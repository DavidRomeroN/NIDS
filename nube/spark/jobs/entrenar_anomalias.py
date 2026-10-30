import json
import os
import shutil
from datetime import datetime

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql import types as T

from comun import FEATURES_FLUJO, LAKE_DIR, MODELOS_DIR, crear_spark, features_flujo, log, push


# Detector de anomalías por flujo (el "NIDS" propiamente dicho), no supervisado
# porque TRCU.csv no trae etiquetas (label = NeedLabel en todas las filas):
#   1. features de comportamiento de cada flujo (escala log), escaladas
#   2. K-Means: se prueban varios k y se elige por silhouette
#   3. puntaje de anomalía = distancia del flujo al centro de su cluster
#   4. umbral = percentil 99.5 de esa distancia en el histórico: en vivo se
#      marca como anómalo lo que se aleja más que el 99.5 % de lo ya visto
# Se corre una vez después de preparar_historico.py:
#   docker compose run --rm spark-entrenamiento python /jobs/entrenar_anomalias.py
VALORES_K = [4, 6, 8, 10]
PERCENTIL_UMBRAL = 0.995
DESTINO = f"{MODELOS_DIR}/anomalias_flujo"


def distancia_udf(centros):
    centros = [np.array(c) for c in centros]

    @F.udf(T.DoubleType())
    def distancia(vector, cluster):
        return float(np.linalg.norm(vector.toArray() - centros[cluster]))
    return distancia


def main():
    spark = crear_spark("nids-entrenar-anomalias")
    flujos = features_flujo(spark.read.parquet(f"{LAKE_DIR}/historico_flujos")).cache()
    total = flujos.count()

    ensamblador = VectorAssembler(inputCols=FEATURES_FLUJO, outputCol="features_crudas")
    escalador = StandardScaler(inputCol="features_crudas", outputCol="features",
                               withMean=True, withStd=True)
    muestra = flujos.sample(fraction=min(1.0, 60_000 / total), seed=42).cache()

    # Elección de k por silhouette sobre una muestra (es caro sobre 400 mil filas).
    evaluador = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
    busqueda = {}
    for k in VALORES_K:
        modelo = Pipeline(stages=[ensamblador, escalador,
                                  KMeans(k=k, seed=42, featuresCol="features",
                                         predictionCol="cluster")]).fit(muestra)
        busqueda[k] = evaluador.evaluate(modelo.transform(muestra))
        log(componente="anomalias", status="k_evaluado", k=k, silhouette=round(busqueda[k], 4))
    mejor_k = max(busqueda, key=busqueda.get)

    modelo = Pipeline(stages=[ensamblador, escalador,
                              KMeans(k=mejor_k, seed=42, featuresCol="features",
                                     predictionCol="cluster")]).fit(flujos)
    centros = modelo.stages[-1].clusterCenters()
    puntuados = modelo.transform(flujos).withColumn(
        "puntaje", distancia_udf(centros)(F.col("features"), F.col("cluster")))
    umbral = puntuados.approxQuantile("puntaje", [PERCENTIL_UMBRAL], 0.001)[0]
    tamanos = {int(f.cluster): int(f["count"]) for f in puntuados.groupBy("cluster").count().collect()}
    anomalos = puntuados.where(F.col("puntaje") > umbral).count()

    temporal = f"{DESTINO}/modelo_nuevo"
    shutil.rmtree(temporal, ignore_errors=True)
    modelo.write().overwrite().save(temporal)
    shutil.rmtree(f"{DESTINO}/modelo", ignore_errors=True)
    os.replace(temporal, f"{DESTINO}/modelo")

    metricas = {
        "entrenado": datetime.now().isoformat(timespec="seconds"),
        "flujos_entrenamiento": total,
        "features": FEATURES_FLUJO,
        "silhouette_por_k": busqueda,
        "k_elegido": mejor_k,
        "percentil_umbral": PERCENTIL_UMBRAL,
        "umbral_puntaje": umbral,
        "tamano_clusters": tamanos,
        "flujos_sobre_umbral_historico": anomalos,
        "centros": [list(map(float, c)) for c in centros],
    }
    with open(f"{DESTINO}/metricas.json", "w", encoding="utf-8") as archivo:
        json.dump(metricas, archivo, indent=2)

    lineas = [f'nids_anomalias_silhouette{{k="{k}"}} {s}' for k, s in busqueda.items()]
    lineas += [f"nids_anomalias_k_elegido {mejor_k}", f"nids_anomalias_umbral {umbral}",
               f"nids_anomalias_flujos_entrenamiento {total}"]
    push("nids_anomalias_modelo", "entrenamiento", "\n".join(lineas) + "\n")
    log(componente="anomalias", status="modelo_entrenado", k=mejor_k,
        silhouette=round(busqueda[mejor_k], 4), umbral=round(umbral, 3),
        flujos=total, anomalosHistorico=anomalos, clusters=tamanos)


if __name__ == "__main__":
    main()
