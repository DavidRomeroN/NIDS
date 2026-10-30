import argparse

from pyspark.sql import functions as F

from comun import COLUMNAS_FLUJO, LAKE_DIR, agregar_por_minuto, crear_spark, es_ip_privada, log


# Capa batch: convierte el histórico de flujos capturado en la U (TRCU.csv,
# formato NTLFlowLyzer, 1-jul a 1-sep-2026) en la serie de tiempo por minuto
# que usa entrenar_series.py. Se corre una vez:
#   docker compose run --rm spark-entrenamiento python /jobs/preparar_historico.py --csv /data/historico/TRCU.csv
# Las IP reales del CSV no salen de aquí: la salida son solo agregados por minuto.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--sensor", default="trcu-historico")
    args = parser.parse_args()

    spark = crear_spark("nids-preparar-historico")
    crudo = spark.read.option("header", True).csv(args.csv)
    total = crudo.count()

    flujos = (
        crudo.select(
            (F.col("timestamp").cast("double") / 1e6).cast("timestamp").alias("ts_inicio"),
            F.lit(args.sensor).alias("sensorId"),
            F.col("src_addr").alias("src"),
            F.col("dst_addr").alias("dst"),
            es_ip_privada("src_addr").alias("src_interna"),
            es_ip_privada("dst_addr").alias("dst_interna"),
            F.col("ip_prot").cast("int").alias("ip_prot"),
            F.col("src_port").cast("int").alias("sport"),
            F.col("dst_port").cast("int").alias("dport"),
            F.col("fwd_pkt_cnt").cast("long").alias("paquetes_fwd"),
            F.col("bwd_pkt_cnt").cast("long").alias("paquetes_bwd"),
            F.col("fwd_pkt_len_tot").cast("long").alias("bytes_fwd"),
            F.col("bwd_pkt_len_tot").cast("long").alias("bytes_bwd"),
            F.col("flag_SYN").cast("long").alias("syn"),
            F.col("flow_duration").cast("long").alias("duracion_us"),
        )
        # Calidad: filas sin tiempo o con contadores negativos/vacíos no entran.
        .where(F.col("ts_inicio").isNotNull() & (F.col("paquetes_fwd") >= 0)
               & (F.col("bytes_fwd") >= 0) & F.col("ip_prot").isNotNull())
        .select(*COLUMNAS_FLUJO)
    )
    validos = flujos.count()

    # Flujos individuales para el detector de anomalías (entrenar_anomalias.py),
    # SIN las IP: el detector solo usa el comportamiento del flujo.
    (flujos.drop("src", "dst")
           .withColumn("fecha", F.to_date("ts_inicio"))
           .write.mode("overwrite").partitionBy("fecha").parquet(f"{LAKE_DIR}/historico_flujos"))

    por_minuto = agregar_por_minuto(flujos).withColumn("fecha", F.to_date("ventana_inicio"))
    por_minuto.write.mode("overwrite").partitionBy("fecha").parquet(f"{LAKE_DIR}/historico_minuto")

    resumen = spark.read.parquet(f"{LAKE_DIR}/historico_minuto").agg(
        F.count("*").alias("minutos"),
        F.min("ventana_inicio").alias("desde"),
        F.max("ventana_inicio").alias("hasta"),
        F.avg("flujos").alias("flujos_prom"),
        F.avg("bytes").alias("bytes_prom"),
        F.avg("syn").alias("syn_prom"),
    ).first().asDict()
    log(componente="historico", status="preparado", filasCsv=total, filasValidas=validos,
        descartadas=total - validos, **resumen)


if __name__ == "__main__":
    main()
