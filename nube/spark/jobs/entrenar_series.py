import argparse
import json
import os
import shutil
import time
from datetime import datetime
from itertools import product

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import (FEATURES_MODELO, INDICADORES, LAKE_DIR, MODELOS_DIR, con_rezagos,
                   crear_spark, log, push)


# Entrenamiento de las series de tiempo. Base de S10, más lo que pide un
# modelo de producción:
#   1. dataset supervisado con lead(indicador, 1) en orden cronológico, sobre
#      el histórico TRCU.csv + lo que ya llegó en vivo
#   2. rezagos (lag 1-3 y media móvil de 5 min de cada indicador)
#   3. test = último 20 % en el tiempo, que no se toca hasta el final
#   4. selección de hiperparámetros con validación cruzada temporal (rolling
#      origin, 3 cortes: entrenar hasta 50/60/70 % y validar el 10 % siguiente);
#      nunca randomSplit
#   5. la escala también es un hiperparámetro: el tráfico tiene cola pesada
#      (minutos tranquilos y picos de MB), así que se prueba aprender en
#      log1p() y volver con expm1(); se evalúa siempre en la escala original
#   6. persistencia como baseline obligatorio; LinearRegression, RandomForest
#      y GBT compiten; gana el de menor RMSE promedio en validación
#   7. el ganador se reentrena con todo lo anterior al test y se guarda como
#      PipelineModel; streaming_nids.py lo recarga solo
# Si ningún modelo le gana a la persistencia en test, se reporta así.
MINIMO_FILAS = int(os.getenv("MINIMO_FILAS", "60"))
MINUTOS_ENTRE_ENTRENAMIENTOS = float(os.getenv("MINUTOS_ENTRE_ENTRENAMIENTOS", "360"))

FUENTES = ["historico_minuto", "trafico_minuto"]  # TRCU.csv (batch) + stream en vivo
# El plan B de la demo reproduce TRCU.csv: ya está en historico_minuto, no es dato nuevo.
SENSORES_EXCLUIDOS = ["replay-trcu"]
COLUMNAS_MINUTO = ["ventana_inicio", "ventana_fin", "sensorId", "flujos", "paquetes", "bytes",
                   "hosts", "tcp", "udp", "dns", "bytes_tls", "syn", "bytes_externos"]

ESCALAS = ["original", "log"]
GRILLAS = {
    "linear_regression": (LinearRegression, {"regParam": [0.0, 0.1]}),
    "random_forest": (RandomForestRegressor, {"numTrees": [40], "maxDepth": [6, 10]}),
    "gbt": (GBTRegressor, {"maxIter": [50], "maxDepth": [3, 5]}),
}
CORTES_VALIDACION = [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8)]
INICIO_TEST = 0.8

# Variables que no se pasan a log: ya están en escalas chicas.
SIN_LOG = {"hora", "dia_semana", "gap_min"}
COLUMNAS_LOG = [c for c in FEATURES_MODELO if c not in SIN_LOG]


def dataset_supervisado(spark):
    partes = [spark.read.parquet(f"{LAKE_DIR}/{fuente}").select(*COLUMNAS_MINUTO)
              for fuente in FUENTES if os.path.exists(f"{LAKE_DIR}/{fuente}")]
    if not partes:
        return None
    union = partes[0]
    for parte in partes[1:]:
        union = union.unionByName(parte)
    df = (
        union
        .where(~F.col("sensorId").isin(SENSORES_EXCLUIDOS))
        # foreachBatch es "al menos una vez": un reintento puede duplicar una ventana.
        .dropDuplicates(["sensorId", "ventana_inicio"])
        .withColumn("hora", F.hour("ventana_inicio").cast("double"))
        .withColumn("dia_semana", F.dayofweek("ventana_inicio").cast("double"))
    )
    df = con_rezagos(df)
    orden = Window.partitionBy("sensorId").orderBy("ventana_inicio")
    df = df.withColumn("siguiente_inicio", F.lead("ventana_inicio", 1).over(orden))
    for indicador in INDICADORES:
        objetivo = F.lead(indicador, 1).over(orden).cast("double")
        df = (df.withColumn(f"objetivo_{indicador}", objetivo)
                .withColumn(f"objetivo_log_{indicador}", F.log1p(objetivo)))
    # Solo pares de minutos consecutivos: si el sensor estuvo apagado, el
    # "minuto siguiente" real no existe y la fila no sirve para entrenar.
    df = df.where(F.col("siguiente_inicio") == F.col("ventana_fin"))
    for columna in FEATURES_MODELO:
        df = df.withColumn(columna, F.col(columna).cast("double"))
    posicion = F.row_number().over(Window.orderBy("ventana_inicio", "sensorId"))
    return df.withColumn("posicion", posicion)


def tramo(df, total, desde, hasta):
    return df.where((F.col("posicion") > int(total * desde)) & (F.col("posicion") <= int(total * hasta)))


def armar_pipeline(clase, parametros, escala, indicador):
    if escala == "log":
        sentencia = ("SELECT *, " + ", ".join(f"log1p(GREATEST({c}, 0)) AS l_{c}" for c in COLUMNAS_LOG)
                     + " FROM __THIS__")
        entradas = [f"l_{c}" for c in COLUMNAS_LOG] + sorted(SIN_LOG)
        etapas = [SQLTransformer(statement=sentencia)]
        etiqueta = f"objetivo_log_{indicador}"
    else:
        entradas = FEATURES_MODELO
        etapas = []
        etiqueta = f"objetivo_{indicador}"
    estimador = clase(featuresCol="features", labelCol=etiqueta, **parametros)
    if clase is not LinearRegression:
        estimador.setSeed(42)
    return Pipeline(stages=etapas + [VectorAssembler(inputCols=entradas, outputCol="features"), estimador])


def a_escala_original(predicciones, escala):
    if escala == "log":
        return predicciones.withColumn("prediction", F.expm1("prediction"))
    return predicciones


def evaluar(predicciones, objetivo):
    return {nombre: RegressionEvaluator(labelCol=objetivo, predictionCol="prediction",
                                        metricName=nombre).evaluate(predicciones)
            for nombre in ["rmse", "mae", "r2"]}


def importancias(modelo, escala):
    estimador = modelo.stages[-1]
    nombres = ([f"log({c})" for c in COLUMNAS_LOG] + sorted(SIN_LOG)) if escala == "log" else FEATURES_MODELO
    if hasattr(estimador, "featureImportances"):
        valores = estimador.featureImportances.toArray()
    elif hasattr(estimador, "coefficients"):
        valores = [abs(c) for c in estimador.coefficients.toArray()]
    else:
        return {}
    pares = sorted(zip(nombres, valores), key=lambda p: -p[1])[:8]
    return {nombre: round(float(valor), 4) for nombre, valor in pares}


def entrenar_indicador(df, total, indicador):
    objetivo = f"objetivo_{indicador}"
    test = tramo(df, total, INICIO_TEST, 1.0)
    previo_test = tramo(df, total, 0.0, INICIO_TEST)

    # 1. Validación cruzada temporal: cada configuración se entrena 3 veces.
    busqueda = []
    for nombre, (clase, grilla) in GRILLAS.items():
        claves = list(grilla)
        for valores in product(*(grilla[c] for c in claves)):
            for escala in ESCALAS:
                parametros = dict(zip(claves, valores))
                rmses = []
                for hasta, valida_hasta in CORTES_VALIDACION:
                    modelo = armar_pipeline(clase, parametros, escala, indicador).fit(tramo(df, total, 0.0, hasta))
                    validacion = a_escala_original(modelo.transform(tramo(df, total, hasta, valida_hasta)), escala)
                    rmses.append(evaluar(validacion, objetivo)["rmse"])
                busqueda.append({"modelo": nombre, "parametros": parametros, "escala": escala,
                                 "rmse_cv": sum(rmses) / len(rmses), "rmse_por_corte": rmses})
    mejores = {}
    for config in busqueda:
        actual = mejores.get(config["modelo"])
        if actual is None or config["rmse_cv"] < actual["rmse_cv"]:
            mejores[config["modelo"]] = config
    ganador = min(mejores, key=lambda n: mejores[n]["rmse_cv"])

    # 2. Test: cada algoritmo con su mejor configuración, reentrenado con todo
    #    lo anterior al test. El test no participó en ninguna decisión.
    resultados = {"persistencia": evaluar(test.withColumn("prediction", F.col(indicador)), objetivo)}
    finales = {}
    for nombre, config in mejores.items():
        modelo = armar_pipeline(GRILLAS[nombre][0], config["parametros"], config["escala"], indicador).fit(previo_test)
        finales[nombre] = modelo
        resultados[nombre] = evaluar(a_escala_original(modelo.transform(test), config["escala"]), objetivo)

    rmse_base = resultados["persistencia"]["rmse"]
    mejora = 100 * (rmse_base - resultados[ganador]["rmse"]) / rmse_base if rmse_base else 0.0

    destino = f"{MODELOS_DIR}/{indicador}"
    temporal = f"{destino}/modelo_nuevo"
    shutil.rmtree(temporal, ignore_errors=True)
    finales[ganador].write().overwrite().save(temporal)
    shutil.rmtree(f"{destino}/modelo", ignore_errors=True)
    os.replace(temporal, f"{destino}/modelo")

    metricas = {
        "indicador": indicador,
        "entrenado": datetime.now().isoformat(timespec="seconds"),
        "filas_total": total,
        "filas_test": test.count(),
        "cortes_validacion": CORTES_VALIDACION,
        "features": FEATURES_MODELO,
        "busqueda_hiperparametros": busqueda,
        "mejor_configuracion": mejores,
        "resultados_test": resultados,
        "ganador": ganador,
        "escala": mejores[ganador]["escala"],
        "rmse_ganador": resultados[ganador]["rmse"],
        "mejora_vs_persistencia_pct": mejora,
        "le_gana_a_persistencia": mejora > 0,
        "importancia_variables": importancias(finales[ganador], mejores[ganador]["escala"]),
    }
    # metricas.json se escribe al final: es la señal de "modelo listo" para streaming.
    with open(f"{destino}/metricas.json", "w", encoding="utf-8") as archivo:
        json.dump(metricas, archivo, indent=2)
    return metricas


def entrenar_todo(spark):
    df = dataset_supervisado(spark)
    if df is None:
        log(componente="entrenamiento", status="sin_datos")
        return False
    df = df.cache()
    total = df.count()
    if total < MINIMO_FILAS:
        log(componente="entrenamiento", status="datos_insuficientes", filas=total, minimo=MINIMO_FILAS)
        df.unpersist()
        return False

    lineas = [f"nids_modelo_filas_entrenamiento {total}"]
    for indicador in INDICADORES:
        os.makedirs(f"{MODELOS_DIR}/{indicador}", exist_ok=True)
        metricas = entrenar_indicador(df, total, indicador)
        for modelo, valores in metricas["resultados_test"].items():
            for nombre, valor in valores.items():
                lineas.append(f'nids_modelo_{nombre}{{indicador="{indicador}",modelo="{modelo}"}} {valor}')
        lineas.append(f'nids_modelo_mejora_vs_persistencia_pct{{indicador="{indicador}"}} '
                      f'{metricas["mejora_vs_persistencia_pct"]}')
        lineas.append(f'nids_modelo_ganador_info{{indicador="{indicador}",ganador="{metricas["ganador"]}",'
                      f'escala="{metricas["escala"]}"}} 1')
        log(componente="entrenamiento", status="modelo_entrenado", indicador=indicador,
            ganador=metricas["ganador"], escala=metricas["escala"], filas=total,
            resultadosTest=metricas["resultados_test"],
            configuracion=metricas["mejor_configuracion"][metricas["ganador"]]["parametros"],
            mejoraVsPersistenciaPct=round(metricas["mejora_vs_persistencia_pct"], 2),
            importancia=metricas["importancia_variables"])
    lineas.append(f"nids_modelo_ultimo_entrenamiento_timestamp {time.time()}")
    push("nids_modelo", "entrenamiento", "\n".join(lineas) + "\n")
    df.unpersist()
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucle", action="store_true", help="reentrena cada MINUTOS_ENTRE_ENTRENAMIENTOS")
    args = parser.parse_args()

    spark = crear_spark("nids-entrenamiento")
    while True:
        entrenado = False
        try:
            entrenado = entrenar_todo(spark)
        except Exception as error:  # el bucle no debe morir por un lote raro de datos
            log(componente="entrenamiento", status="error", error=str(error))
        if not args.bucle:
            break
        # Mientras no haya datos suficientes para el primer modelo, reintenta seguido.
        time.sleep(MINUTOS_ENTRE_ENTRENAMIENTOS * 60 if entrenado else 300)


if __name__ == "__main__":
    main()
