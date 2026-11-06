import argparse
import json
import os
import shutil
import time
from datetime import datetime
from itertools import product

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from comun import (FEATURES_MODELO, INDICADORES, LAKE_DIR, MODELOS_DIR, con_rezagos,
                   crear_spark, log, push)


# Entrenamiento de las series de tiempo. Base de S10, más lo que pide un
# modelo de producción que vive en un punto concreto de la red:
#   1. dataset supervisado con lead(indicador, 1) en orden cronológico, sobre
#      el histórico TRCU.csv + lo que ya llegó en vivo del sensor
#   2. rezagos (lag 1-3 y media móvil de 5 min de cada indicador)
#   3. test = último 20 % del tiempo del sensor, que no se toca hasta el final
#   4. selección con validación cruzada temporal (rolling origin, 3 cortes sobre
#      el sensor); nunca randomSplit. Se valida siempre con datos del sensor,
#      porque es donde el modelo va a predecir
#   5. dos estrategias compiten: solo el sensor, o histórico + sensor
#   6. la escala también es un hiperparámetro: se prueba aprender en log1p() y
#      volver con expm1(); se evalúa siempre en la escala original
#   7. persistencia como baseline; LinearRegression, RandomForest y GBT compiten;
#      gana la configuración de menor RMSE promedio en validación
#   8. el ganador solo reemplaza al modelo en producción si, en el mismo test, le
#      gana a la persistencia, mejora el RMSE actual al menos 1 % y no empeora el
#      MAE más de 10 %. Si no, producción no cambia
#   9. se publica el mismo modelo que se midió en el test, que por eso nunca vio
#      ese tramo: la comparación con el siguiente candidato sigue siendo justa.
#      streaming_nids.py lo recarga solo y usa ese RMSE para su umbral de anomalía
MINIMO_FILAS = int(os.getenv("MINIMO_FILAS", "1440"))  # filas del sensor: al menos un día
MEJORA_MINIMA_RMSE = 0.01  # evita cambiar de modelo por diferencias de ruido
EMPEORA_MAXIMO_MAE = 0.10  # un pico puede bajar el RMSE y doblar el error típico
MINUTOS_ENTRE_ENTRENAMIENTOS = float(os.getenv("MINUTOS_ENTRE_ENTRENAMIENTOS", "360"))
# Tras publicar modelos a mano, evita repetir de inmediato un entrenamiento de ~1 h.
ESPERA_INICIAL_MIN = float(os.getenv("ESPERA_INICIAL_MIN", "0"))

FUENTES = ["historico_minuto", "trafico_minuto"]  # TRCU.csv (batch) + stream en vivo
# El plan B de la demo reproduce TRCU.csv: ya está en historico_minuto, no es dato nuevo.
SENSORES_EXCLUIDOS = os.getenv("SENSORES_EXCLUIDOS", "replay-trcu,prueba-vm").split(",")
# Sensor donde va a predecir el modelo y desde cuándo está en su punto de captura.
# Si el sensor cambia de puerto (p. ej. al espejo), hay que mover INICIO_SENSOR:
# el tráfico anterior ya no representa lo que va a ver.
SENSOR_OBJETIVO = os.getenv("SENSOR_OBJETIVO", "nids-pi-01")
INICIO_SENSOR = os.getenv("INICIO_SENSOR", "2026-10-30 10:40:00")
COLUMNAS_MINUTO = ["ventana_inicio", "ventana_fin", "sensorId", "flujos", "paquetes", "bytes",
                   "hosts", "tcp", "udp", "dns", "bytes_tls", "syn", "bytes_externos"]

ESTRATEGIAS = ["solo_sensor", "historico_y_sensor"]
ESCALAS = ["original", "log"]
GRILLAS = {
    "linear_regression": (LinearRegression, {"regParam": [0.0, 0.1]}),
    "random_forest": (RandomForestRegressor, {"numTrees": [40], "maxDepth": [6, 10]}),
    "gbt": (GBTRegressor, {"maxIter": [50], "maxDepth": [3, 5]}),
}
# Fracciones del tramo del sensor anterior al test: entrenar hasta el corte y validar lo siguiente.
CORTES_VALIDACION = [(0.55, 0.70), (0.70, 0.85), (0.85, 1.0)]
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
    # Lo capturado antes de llegar al punto actual no representa esta red.
    inicio = F.lit(INICIO_SENSOR).cast("timestamp")
    return df.where((F.col("sensorId") != SENSOR_OBJETIVO) | (F.col("ventana_inicio") >= inicio))


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


def metricas_en_produccion(destino, test, objetivo):
    """Métricas del modelo que hoy corre en vivo, medidas en el mismo test que el candidato."""
    if not os.path.exists(f"{destino}/metricas.json"):
        return None
    with open(f"{destino}/metricas.json", encoding="utf-8") as archivo:
        escala = json.load(archivo)["escala"]
    modelo = PipelineModel.load(f"{destino}/modelo")
    return evaluar(a_escala_original(modelo.transform(test), escala), objetivo)


def entrenar_indicador(df, tiempos, indicador):
    objetivo = f"objetivo_{indicador}"
    ventana = F.col("ventana_inicio")
    es_sensor = F.col("sensorId") == SENSOR_OBJETIVO
    previos = int(len(tiempos) * INICIO_TEST)
    inicio_test = tiempos[previos]
    test = df.where(es_sensor & (ventana >= F.lit(inicio_test)))

    def entrenamiento(estrategia, condicion_sensor):
        filtro = es_sensor & condicion_sensor
        return df.where(filtro if estrategia == "solo_sensor" else (filtro | ~es_sensor))

    # 1. Validación cruzada temporal: las dos estrategias se validan en los mismos tramos del sensor.
    busqueda = []
    for estrategia in ESTRATEGIAS:
        for nombre, (clase, grilla) in GRILLAS.items():
            claves = list(grilla)
            for valores in product(*(grilla[c] for c in claves)):
                for escala in ESCALAS:
                    parametros = dict(zip(claves, valores))
                    rmses = []
                    for desde, hasta in CORTES_VALIDACION:
                        corte = tiempos[int(previos * desde)]
                        fin = tiempos[int(previos * hasta)]
                        modelo = armar_pipeline(clase, parametros, escala, indicador).fit(
                            entrenamiento(estrategia, ventana < F.lit(corte)))
                        validacion = df.where(es_sensor & (ventana >= F.lit(corte)) & (ventana < F.lit(fin)))
                        rmses.append(evaluar(a_escala_original(modelo.transform(validacion), escala),
                                             objetivo)["rmse"])
                    busqueda.append({"estrategia": estrategia, "modelo": nombre, "parametros": parametros,
                                     "escala": escala, "rmse_cv": sum(rmses) / len(rmses), "rmse_por_corte": rmses})
    mejor_global = min(busqueda, key=lambda c: c["rmse_cv"])
    estrategia = mejor_global["estrategia"]
    mejores = {}
    for config in busqueda:
        if config["estrategia"] != estrategia:
            continue
        actual = mejores.get(config["modelo"])
        if actual is None or config["rmse_cv"] < actual["rmse_cv"]:
            mejores[config["modelo"]] = config
    ganador = mejor_global["modelo"]

    # 2. Test: cada algoritmo con su mejor configuración, reentrenado con todo
    #    lo anterior al test. El test no participó en ninguna decisión.
    previo_test = entrenamiento(estrategia, ventana < F.lit(inicio_test))
    resultados = {"persistencia": evaluar(test.withColumn("prediction", F.col(indicador)), objetivo)}
    finales = {}
    for nombre, config in mejores.items():
        modelo = armar_pipeline(GRILLAS[nombre][0], config["parametros"], config["escala"], indicador).fit(previo_test)
        finales[nombre] = modelo
        resultados[nombre] = evaluar(a_escala_original(modelo.transform(test), config["escala"]), objetivo)

    rmse_base = resultados["persistencia"]["rmse"]
    rmse_nuevo = resultados[ganador]["rmse"]
    mejora = 100 * (rmse_base - rmse_nuevo) / rmse_base if rmse_base else 0.0

    # 3. Compuerta: producción solo cambia si el candidato gana en el mismo test.
    destino = f"{MODELOS_DIR}/{indicador}"
    actual = metricas_en_produccion(destino, test, objetivo)
    mae_nuevo = resultados[ganador]["mae"]
    publicar = rmse_nuevo < rmse_base and (actual is None or (
        rmse_nuevo < actual["rmse"] * (1 - MEJORA_MINIMA_RMSE)
        and mae_nuevo <= actual["mae"] * (1 + EMPEORA_MAXIMO_MAE)))
    decision = {"indicador": indicador, "ganador": ganador, "escala": mejor_global["escala"],
                "estrategia": estrategia, "rmseCandidato": rmse_nuevo, "maeCandidato": mae_nuevo,
                "produccion": actual, "rmsePersistencia": rmse_base, "publicado": publicar}
    if not publicar:
        log(componente="entrenamiento", status="modelo_conservado", **decision)
        return

    # 4. metricas.json va al final y de forma atómica: es la señal de "modelo
    #    listo" para streaming.
    final = finales[ganador]
    temporal = f"{destino}/modelo_nuevo"
    shutil.rmtree(temporal, ignore_errors=True)
    final.write().overwrite().save(temporal)
    shutil.rmtree(f"{destino}/modelo", ignore_errors=True)
    os.replace(temporal, f"{destino}/modelo")

    metricas = {
        "indicador": indicador,
        "entrenado": datetime.now().isoformat(timespec="seconds"),
        "estrategia": estrategia,
        "sensor_objetivo": SENSOR_OBJETIVO,
        "filas_total": previo_test.count() + test.count(),
        "filas_test": test.count(),
        "cortes_validacion": CORTES_VALIDACION,
        "features": FEATURES_MODELO,
        "busqueda_hiperparametros": busqueda,
        "mejor_configuracion": mejores,
        "resultados_test": resultados,
        "ganador": ganador,
        "escala": mejor_global["escala"],
        "rmse_ganador": rmse_nuevo,
        "mejora_vs_persistencia_pct": mejora,
        "le_gana_a_persistencia": mejora > 0,
        "importancia_variables": importancias(final, mejor_global["escala"]),
    }
    with open(f"{destino}/metricas.tmp", "w", encoding="utf-8") as archivo:
        json.dump(metricas, archivo, indent=2)
    os.replace(f"{destino}/metricas.tmp", f"{destino}/metricas.json")
    log(componente="entrenamiento", status="modelo_publicado", importancia=metricas["importancia_variables"],
        configuracion=mejor_global["parametros"], mejoraVsPersistenciaPct=round(mejora, 2), **decision)


def lineas_metricas(indicador):
    """Lo que publica Grafana: siempre el modelo que de verdad está en producción."""
    ruta = f"{MODELOS_DIR}/{indicador}/metricas.json"
    if not os.path.exists(ruta):
        return []
    with open(ruta, encoding="utf-8") as archivo:
        metricas = json.load(archivo)
    lineas = []
    for modelo, valores in metricas["resultados_test"].items():
        for nombre in ["rmse", "mae", "r2"]:
            lineas.append(f'nids_modelo_{nombre}{{indicador="{indicador}",modelo="{modelo}"}} {valores[nombre]}')
    lineas.append(f'nids_modelo_mejora_vs_persistencia_pct{{indicador="{indicador}"}} '
                  f'{metricas["mejora_vs_persistencia_pct"]}')
    lineas.append(f'nids_modelo_ganador_info{{indicador="{indicador}",ganador="{metricas["ganador"]}",'
                  f'escala="{metricas["escala"]}"}} 1')
    return lineas


def entrenar_todo(spark):
    df = dataset_supervisado(spark)
    if df is None:
        log(componente="entrenamiento", status="sin_datos")
        return False
    df = df.cache()
    tiempos = [fila.ventana_inicio for fila in
               df.where(F.col("sensorId") == SENSOR_OBJETIVO).select("ventana_inicio")
                 .orderBy("ventana_inicio").collect()]
    if len(tiempos) < MINIMO_FILAS:
        log(componente="entrenamiento", status="datos_insuficientes", filas=len(tiempos), minimo=MINIMO_FILAS)
        df.unpersist()
        return False

    lineas = [f"nids_modelo_filas_entrenamiento {df.count()}"]
    for indicador in INDICADORES:
        os.makedirs(f"{MODELOS_DIR}/{indicador}", exist_ok=True)
        entrenar_indicador(df, tiempos, indicador)
        lineas += lineas_metricas(indicador)
    lineas.append(f"nids_modelo_ultimo_entrenamiento_timestamp {time.time()}")
    push("nids_modelo", "entrenamiento", "\n".join(lineas) + "\n")
    df.unpersist()
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucle", action="store_true", help="reentrena cada MINUTOS_ENTRE_ENTRENAMIENTOS")
    args = parser.parse_args()

    spark = crear_spark("nids-entrenamiento")
    time.sleep(ESPERA_INICIAL_MIN * 60)
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
