import json
import os
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


DATA_DIR = os.getenv("DATA_DIR", "/data")
LAKE_DIR = f"{DATA_DIR}/lake"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints"
MODELOS_DIR = f"{DATA_DIR}/modelos"
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")

# Indicadores de serie de tiempo: uno por integrante (dimensión U2 de cada uno).
# Cada uno tiene su propio modelo entrenado y su propio baseline de persistencia.
INDICADORES = os.getenv("INDICADORES", "bytes,flujos,syn,dns").split(",")

# Variables de una ventana de 1 minuto (el estado actual del minuto).
FEATURES = ["paquetes", "bytes", "flujos", "hosts", "tcp", "udp", "dns",
            "bytes_tls", "syn", "bytes_externos", "hora", "dia_semana"]

# Rezagos: los minutos anteriores de cada indicador. Le dan memoria al modelo
# (autorregresivo) para que se adapte al nivel reciente de tráfico en vez de
# solo al promedio histórico. gap_min = minutos desde la fila anterior.
REZAGOS = 3
VENTANA_MEDIA = 5
FEATURES_REZAGO = ([f"lag{k}_{i}" for i in INDICADORES for k in range(1, REZAGOS + 1)]
                   + [f"media{VENTANA_MEDIA}_{i}" for i in INDICADORES] + ["gap_min"])
FEATURES_MODELO = FEATURES + FEATURES_REZAGO

# Columnas de flujo que comparten el histórico (TRCU.csv, NTLFlowLyzer) y el
# stream en vivo (ingestor_pcap.py). Ambos lados se normalizan a este esquema
# antes de agregar, para que el modelo vea exactamente las mismas variables.
COLUMNAS_FLUJO = ["ts_inicio", "sensorId", "src", "dst", "src_interna", "dst_interna",
                  "ip_prot", "sport", "dport", "paquetes_fwd", "paquetes_bwd",
                  "bytes_fwd", "bytes_bwd", "syn", "duracion_us"]

# Variables por flujo del detector de anomalías (K-Means). Escala logarítmica:
# bytes y paquetes van de 1 a millones y sin log dominarían la distancia.
FEATURES_FLUJO = ["log_paq_fwd", "log_paq_bwd", "log_bytes_fwd", "log_bytes_bwd",
                  "log_duracion_ms", "syn_d", "es_tcp", "es_udp", "puerto_conocido",
                  "destino_interno"]


def con_rezagos(df):
    """Batch: agrega FEATURES_REZAGO con funciones de ventana sobre el orden temporal."""
    orden = Window.partitionBy("sensorId").orderBy("ventana_inicio")
    previo = F.lag("ventana_inicio", 1).over(orden)
    df = df.withColumn(
        "gap_min",
        F.least(F.coalesce((F.unix_timestamp("ventana_inicio") - F.unix_timestamp(previo)) / 60,
                           F.lit(60.0)), F.lit(60.0)).cast("double"))
    for indicador in INDICADORES:
        actual = F.col(indicador).cast("double")
        for k in range(1, REZAGOS + 1):
            df = df.withColumn(f"lag{k}_{indicador}",
                               F.coalesce(F.lag(indicador, k).over(orden).cast("double"), actual))
        df = df.withColumn(f"media{VENTANA_MEDIA}_{indicador}",
                           F.avg(actual).over(orden.rowsBetween(-(VENTANA_MEDIA - 1), 0)))
    return df


def rezagos_fila(fila, historial):
    """Streaming: lo mismo que con_rezagos() para una fila, a partir de las
    filas anteriores del mismo sensor (lista de dicts, la más reciente al final)."""
    extra = {}
    if historial:
        gap = (fila["ventana_inicio"] - historial[-1]["ventana_inicio"]).total_seconds() / 60
        extra["gap_min"] = float(min(gap, 60.0))
    else:
        extra["gap_min"] = 60.0
    for indicador in INDICADORES:
        actual = float(fila[indicador])
        for k in range(1, REZAGOS + 1):
            extra[f"lag{k}_{indicador}"] = (float(historial[-k][indicador])
                                            if len(historial) >= k else actual)
        ultimos = [float(h[indicador]) for h in historial[-(VENTANA_MEDIA - 1):]] + [actual]
        extra[f"media{VENTANA_MEDIA}_{indicador}"] = sum(ultimos) / len(ultimos)
    return extra


def features_flujo(flujos):
    """Flujos (COLUMNAS_FLUJO) -> FEATURES_FLUJO, igual en batch y en streaming."""
    return (
        flujos
        .withColumn("log_paq_fwd", F.log1p(F.col("paquetes_fwd").cast("double")))
        .withColumn("log_paq_bwd", F.log1p(F.col("paquetes_bwd").cast("double")))
        .withColumn("log_bytes_fwd", F.log1p(F.col("bytes_fwd").cast("double")))
        .withColumn("log_bytes_bwd", F.log1p(F.col("bytes_bwd").cast("double")))
        .withColumn("log_duracion_ms", F.log1p(F.greatest(F.col("duracion_us"), F.lit(0)) / 1000.0))
        .withColumn("syn_d", F.least(F.col("syn"), F.lit(10)).cast("double"))
        .withColumn("es_tcp", (F.col("ip_prot") == 6).cast("double"))
        .withColumn("es_udp", (F.col("ip_prot") == 17).cast("double"))
        .withColumn("puerto_conocido",
                    (F.least(F.coalesce(F.col("dport"), F.lit(0)),
                             F.coalesce(F.col("sport"), F.lit(0))) < 1024).cast("double"))
        .withColumn("destino_interno", F.col("dst_interna").cast("double"))
    )


def contar_si(condicion, valor=None):
    # valor=None y no F.lit(1) como default: F.lit necesita un SparkContext
    # activo y los defaults se evalúan al importar el módulo.
    return F.sum(F.when(condicion, F.lit(1) if valor is None else valor).otherwise(0))


def agregar_por_minuto(flujos):
    """Flujos (COLUMNAS_FLUJO) -> una fila por minuto y sensor.

    La MISMA función agrega el histórico en batch (preparar_historico.py) y el
    stream de Kafka (streaming_nids.py): capa batch y capa de velocidad de la
    arquitectura Lambda calculan los indicadores de una sola forma. Cada flujo
    cuenta en el minuto en que empezó, igual que NTLFlowLyzer.
    """
    puerto = lambda p: (F.col("sport") == p) | (F.col("dport") == p)  # noqa: E731
    bytes_flujo = F.col("bytes_fwd") + F.col("bytes_bwd")
    return (
        flujos
        .groupBy(F.window("ts_inicio", "1 minute"), "sensorId")
        .agg(
            F.count("*").alias("flujos"),
            F.sum(F.col("paquetes_fwd") + F.col("paquetes_bwd")).alias("paquetes"),
            F.sum(bytes_flujo).alias("bytes"),
            F.approx_count_distinct("src").alias("hosts"),
            contar_si(F.col("ip_prot") == 6).alias("tcp"),
            contar_si(F.col("ip_prot") == 17).alias("udp"),
            contar_si(puerto(53)).alias("dns"),
            contar_si(puerto(443), bytes_flujo).alias("bytes_tls"),
            F.sum("syn").alias("syn"),
            contar_si(~(F.col("src_interna") & F.col("dst_interna")), bytes_flujo).alias("bytes_externos"),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "sensorId", "flujos", "paquetes", "bytes", "hosts", "tcp", "udp", "dns",
            "bytes_tls", "syn", "bytes_externos",
        )
    )


def es_ip_privada(columna):
    return F.col(columna).rlike(r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|fe80:|fd)")


def crear_spark(nombre, con_kafka=False):
    builder = (
        SparkSession.builder
        .appName(nombre)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        # 2g por driver (streaming + entrenamiento) entra en una VM de 8 GB.
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "America/Lima")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if con_kafka:
        builder = builder.config("spark.jars.packages",
                                 "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def push(job, instancia, cuerpo):
    # Mismo atajo de S11: Pushgateway para un job que no termina (no es la
    # forma recomendada en producción, ver S12 pregunta 7). POST (no PUT):
    # reemplaza solo las métricas enviadas y conserva las demás del grupo.
    url = f"{PUSHGATEWAY_URL}/metrics/job/{job}/instance/{instancia}"
    try:
        peticion = urllib.request.Request(url, data=cuerpo.encode("utf-8"), method="POST")
        urllib.request.urlopen(peticion, timeout=5).close()
    except OSError as error:
        log(componente=job, status="pushgateway_no_disponible", error=str(error))


def formato_prometheus(metricas):
    # {(nombre, 'etiquetas'): valor} -> texto. Pushgateway rechaza el envío
    # entero si una misma serie aparece dos veces, por eso se pasa por un dict.
    return "".join(f"{nombre}{{{etiquetas}}} {float(valor)}\n"
                   for (nombre, etiquetas), valor in metricas.items())


def log(**campos):
    print(json.dumps({"service": "nids-spark", **campos}, default=str), flush=True)
