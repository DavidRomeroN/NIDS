"""Funciones compartidas por los notebooks 01-04 (una dimensión por integrante).

Cada notebook responde una dimensión de la pregunta central con los datos del
Data Lake que deja 00_etl_batch_trcu.ipynb:
  - U1 (batch): descriptiva/diagnóstica + un modelo predictivo por hora, sobre
    el histórico.
  - U2 (streaming): el modelo por minuto que corre en vivo en Spark Structured
    Streaming; aquí se leen sus métricas reales de validación.
"""
import json

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

LAKE = "/data/lake_notebooks"
MODELOS = "/data/modelos"


def cargar(spark):
    silver = spark.read.parquet(f"{LAKE}/silver_flujos")
    gold = (spark.read.parquet(f"{LAKE}/gold_minuto")
            .withColumn("hora", F.hour("ventana_inicio"))
            .withColumn("dia_semana", F.dayofweek("ventana_inicio"))
            .withColumn("tipo_dia", F.when(F.dayofweek("ventana_inicio").isin(1, 7), "fin de semana")
                        .otherwise("día de semana")))
    return silver, gold


def resumen(gold, columna):
    return (gold.select(F.count("*").alias("minutos"),
                        F.sum(columna).alias("total"),
                        F.avg(columna).alias("promedio_min"),
                        F.expr(f"percentile_approx({columna}, 0.5)").alias("mediana_min"),
                        F.expr(f"percentile_approx({columna}, 0.99)").alias("p99_min"),
                        F.max(columna).alias("max_min"))
            .toPandas().T.rename(columns={0: columna}))


def perfil_horario(gold, columna):
    return (gold.groupBy("hora", "tipo_dia").agg(F.avg(columna).alias("promedio"))
            .toPandas().pivot(index="hora", columns="tipo_dia", values="promedio").fillna(0))


def picos(gold, columna, n=10):
    """Minutos más alejados de lo normal para su hora (z-score por hora del día)."""
    por_hora = Window.partitionBy("hora")
    return (gold.withColumn("media_hora", F.avg(columna).over(por_hora))
            .withColumn("desv_hora", F.stddev(columna).over(por_hora))
            # try_divide: una hora sin variación (desviación 0) da NULL, no error ANSI.
            .withColumn("z", F.try_divide(F.col(columna) - F.col("media_hora"), F.col("desv_hora")))
            .where(F.col("z").isNotNull())
            .orderBy(F.col("z").desc())
            .select("ventana_inicio", columna, F.round("media_hora", 1).alias("media_de_esa_hora"),
                    F.round("z", 1).alias("z"))
            .limit(n).toPandas())


def modelo_horario(gold, columna):
    """U1: predice el total de la PRÓXIMA HORA (batch, sin tiempo real).

    Rezago de 1 h y de 24 h (misma hora del día anterior), hora y día de la
    semana. División cronológica 80/20 y baseline de persistencia, igual que S10.
    """
    horas = (gold.withColumn("hora_inicio", F.date_trunc("hour", "ventana_inicio"))
             .groupBy("hora_inicio").agg(F.sum(columna).cast("double").alias("valor"))
             .withColumn("hora", F.hour("hora_inicio").cast("double"))
             .withColumn("dia_semana", F.dayofweek("hora_inicio").cast("double")))
    orden = Window.orderBy("hora_inicio")
    horas = (horas.withColumn("lag1", F.lag("valor", 1).over(orden))
             .withColumn("lag24", F.lag("valor", 24).over(orden))
             .withColumn("objetivo", F.lead("valor", 1).over(orden))
             .withColumn("siguiente", F.lead("hora_inicio", 1).over(orden))
             .where(F.col("siguiente") == F.col("hora_inicio") + F.expr("INTERVAL 1 HOUR"))
             .na.fill(0.0, ["lag1", "lag24"])
             .withColumn("posicion", F.row_number().over(orden)))
    total = horas.count()
    corte = int(total * 0.8)
    train, test = horas.where(F.col("posicion") <= corte), horas.where(F.col("posicion") > corte)

    evaluar = lambda df: {m: RegressionEvaluator(labelCol="objetivo", metricName=m).evaluate(df)  # noqa: E731
                          for m in ["rmse", "mae", "r2"]}
    filas = {"persistencia": evaluar(test.withColumn("prediction", F.col("valor")))}
    entradas = VectorAssembler(inputCols=["valor", "lag1", "lag24", "hora", "dia_semana"], outputCol="features")
    for nombre, estimador in [("linear_regression", LinearRegression(labelCol="objetivo")),
                              ("random_forest", RandomForestRegressor(labelCol="objetivo", numTrees=50,
                                                                      maxDepth=6, seed=42))]:
        filas[nombre] = evaluar(Pipeline(stages=[entradas, estimador]).fit(train).transform(test))
    tabla = pd.DataFrame(filas).T
    tabla["mejora_vs_persistencia_%"] = 100 * (tabla.loc["persistencia", "rmse"] - tabla["rmse"]) / tabla.loc["persistencia", "rmse"]
    return tabla.round(3), total


def metricas_streaming(indicador):
    """U2: métricas reales del modelo que corre en vivo (entrenar_series.py)."""
    with open(f"{MODELOS}/{indicador}/metricas.json", encoding="utf-8") as archivo:
        m = json.load(archivo)
    test = pd.DataFrame(m["resultados_test"]).T.round(3)
    test["mejora_vs_persistencia_%"] = (100 * (test.loc["persistencia", "rmse"] - test["rmse"])
                                        / test.loc["persistencia", "rmse"]).round(2)
    cv = pd.DataFrame([{**{"modelo": b["modelo"], "escala": b["escala"]}, **b["parametros"],
                        "rmse_cv": round(b["rmse_cv"], 3)} for b in m["busqueda_hiperparametros"]])
    return m, test, cv.sort_values("rmse_cv")
