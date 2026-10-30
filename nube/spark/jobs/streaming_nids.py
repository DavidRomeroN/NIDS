import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import numpy as np
from pyspark.ml import PipelineModel
from pyspark.sql import Row, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from comun import (CHECKPOINT_DIR, FEATURES_FLUJO, FEATURES_MODELO, INDICADORES, LAKE_DIR,
                   MODELOS_DIR, VENTANA_MEDIA, agregar_por_minuto, crear_spark, features_flujo,
                   formato_prometheus, log, push, rezagos_fila)


# Job de Spark Structured Streaming del NIDS. Cuatro consultas sobre tres
# tópicos de Kafka:
#   trafico-flujos        -> ventanas fijas de 1 min con watermark de 4 min,
#                            KPIs + inferencia de series de tiempo (S8, S10, S11)
#   pi-telemetria         -> ventanas de 1 min de la salud del sensor (S7, S9)
#   incidentes-seguridad  -> KPIs del evento empresarial (S6) y ventanas
#                            deslizantes de 5 min cada 1 min (S8)
# Todas con checkpoint propio y su salida al Data Lake (Parquet) para la capa batch.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRIGGER = os.getenv("TRIGGER", "20 seconds")

spark = crear_spark("nids-streaming", con_kafka=True)


def leer_topico(topico):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topico)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 2_000_000)
        .load()
    )


# ---------------------------------------------------------------------------
# 1. Tráfico de red: flujos agregados en ventanas de 1 minuto
# ---------------------------------------------------------------------------
# Contrato v2 de trafico-flujos (ingestor_pcap.py): mismas columnas de flujo
# que el histórico TRCU.csv, con las IP ya seudonimizadas.
ESQUEMA_FLUJO = T.StructType([
    T.StructField("v", T.IntegerType()),
    T.StructField("sensorId", T.StringType()),
    T.StructField("timestamp", T.LongType()),       # inicio del flujo, microsegundos
    T.StructField("flow_duration", T.LongType()),
    T.StructField("src_addr", T.StringType()),
    T.StructField("dst_addr", T.StringType()),
    T.StructField("srcInterna", T.BooleanType()),
    T.StructField("dstInterna", T.BooleanType()),
    T.StructField("src_port", T.IntegerType()),
    T.StructField("dst_port", T.IntegerType()),
    T.StructField("ip_prot", T.IntegerType()),
    T.StructField("fwd_pkt_cnt", T.LongType()),
    T.StructField("bwd_pkt_cnt", T.LongType()),
    T.StructField("fwd_pkt_len_tot", T.LongType()),
    T.StructField("bwd_pkt_len_tot", T.LongType()),
    T.StructField("flag_SYN", T.LongType()),
])

flujos = (
    leer_topico("trafico-flujos")
    .select(F.from_json(F.col("value").cast("string"), ESQUEMA_FLUJO).alias("e"))
    .select("e.*")
    # Validación de esquema y rango: un flujo tiene al menos un paquete y
    # contadores no negativos.
    .where(F.col("timestamp").isNotNull() & (F.col("fwd_pkt_cnt") >= 1)
           & (F.col("fwd_pkt_len_tot") >= 0) & (F.col("bwd_pkt_len_tot") >= 0))
    .select(
        (F.col("timestamp") / 1e6).cast("timestamp").alias("ts_inicio"),
        "sensorId",
        F.col("src_addr").alias("src"),
        F.col("dst_addr").alias("dst"),
        F.col("flow_duration").alias("duracion_us"),
        F.col("srcInterna").alias("src_interna"),
        F.col("dstInterna").alias("dst_interna"),
        "ip_prot",
        F.col("src_port").alias("sport"),
        F.col("dst_port").alias("dport"),
        F.col("fwd_pkt_cnt").alias("paquetes_fwd"),
        F.col("bwd_pkt_cnt").alias("paquetes_bwd"),
        F.col("fwd_pkt_len_tot").alias("bytes_fwd"),
        F.col("bwd_pkt_len_tot").alias("bytes_bwd"),
        F.col("flag_SYN").alias("syn"),
    )
)

# Un flujo se publica cuando termina (FIN/RST o 30 s sin paquetes, máximo
# 2 min) y el PCAP llega cada 30 s: el watermark espera hasta 4 min de atraso.
ventanas_trafico = agregar_por_minuto(flujos.withWatermark("ts_inicio", "4 minutes"))


class Modelos:
    """Carga los PipelineModel de entrenar_series.py y los recarga cuando cambian."""

    def __init__(self):
        self.modelos = {}
        self.metricas = {}
        self.versiones = {}

    def refrescar(self):
        for indicador in INDICADORES:
            ruta_metricas = f"{MODELOS_DIR}/{indicador}/metricas.json"
            if not os.path.exists(ruta_metricas):
                continue
            version = os.path.getmtime(ruta_metricas)
            if self.versiones.get(indicador) == version:
                continue
            with open(ruta_metricas, encoding="utf-8") as archivo:
                metricas = json.load(archivo)
            self.modelos[indicador] = PipelineModel.load(f"{MODELOS_DIR}/{indicador}/modelo")
            self.metricas[indicador] = metricas
            self.versiones[indicador] = version
            log(componente="trafico", status="modelo_cargado", indicador=indicador,
                ganador=metricas["ganador"], rmseTest=metricas["rmse_ganador"])


modelos = Modelos()
# Predicciones hechas para la ventana siguiente, esperando su valor real:
# (sensor, indicador, inicio_ventana_siguiente) -> {"ml": x, "persistencia": y}
pendientes = {}
# Últimas VENTANA_MEDIA filas por sensor: la memoria que necesitan los rezagos.
historial = defaultdict(lambda: deque(maxlen=VENTANA_MEDIA))


def con_calendario(df):
    return (df.withColumn("hora", F.hour("ventana_inicio").cast("double"))
              .withColumn("dia_semana", F.dayofweek("ventana_inicio").cast("double")))


def precargar_historial():
    # Al reiniciar, los rezagos se reconstruyen desde el Data Lake, no desde cero.
    ruta = f"{LAKE_DIR}/trafico_minuto"
    if not os.path.exists(ruta):
        return
    orden = Window.partitionBy("sensorId").orderBy(F.col("ventana_inicio").desc())
    ultimas = (spark.read.parquet(ruta).dropDuplicates(["sensorId", "ventana_inicio"])
               .withColumn("n", F.row_number().over(orden)).where(F.col("n") <= VENTANA_MEDIA)
               .orderBy("ventana_inicio").collect())
    for fila in ultimas:
        historial[fila.sensorId].append(fila.asDict())
    log(componente="trafico", status="historial_precargado", sensores=sorted(historial))


def procesar_trafico(df, batch_id):
    if df.isEmpty():
        return
    df = con_calendario(df).cache()
    (df.withColumn("fecha", F.to_date("ventana_inicio"))
       .write.mode("append").partitionBy("fecha").parquet(f"{LAKE_DIR}/trafico_minuto"))

    # Un batch puede traer varias ventanas del mismo sensor (p. ej. al
    # recuperar atraso): se recorren en orden, cada una con sus rezagos.
    filas = [fila.asDict() for fila in df.orderBy("ventana_inicio").collect()]
    con_features = []
    for fila in filas:
        previas = list(historial[fila["sensorId"]])
        con_features.append({**fila, **rezagos_fila(fila, previas)})
        historial[fila["sensorId"]].append(fila)

    modelos.refrescar()
    predicciones = {}
    if modelos.modelos:
        entrada = spark.createDataFrame([
            {**{c: float(f[c]) for c in FEATURES_MODELO},
             "sensorId": f["sensorId"], "ventana_inicio": f["ventana_inicio"]}
            for f in con_features])
        for indicador, modelo in modelos.modelos.items():
            salida = modelo.transform(entrada)
            # Si el ganador aprendió en log1p (entrenar_series.py), se vuelve a la escala real.
            if modelos.metricas[indicador].get("escala") == "log":
                salida = salida.withColumn("prediction", F.expm1("prediction"))
            for fila in salida.select("sensorId", "ventana_inicio", "prediction").collect():
                predicciones[(fila.sensorId, fila.ventana_inicio, indicador)] = max(fila.prediction, 0.0)

    metricas = {}
    for fila in filas:
        fila = Row(**fila)
        sensor = fila.sensorId
        siguiente = fila.ventana_inicio + timedelta(minutes=1)
        for indicador in INDICADORES:
            real = float(fila[indicador])
            etiqueta = f'sensor="{sensor}",indicador="{indicador}"'
            metricas[("nids_trafico_valor", etiqueta)] = real

            # ¿Había una predicción hecha un minuto antes para esta ventana?
            previa = pendientes.pop((sensor, indicador, fila.ventana_inicio), None)
            if previa:
                for nombre, valor in previa.items():
                    metricas[("nids_trafico_error_abs", f'{etiqueta},modelo="{nombre}"')] = abs(real - valor)
                if "ml" in previa and indicador in modelos.metricas:
                    umbral = 3 * modelos.metricas[indicador]["rmse_ganador"]
                    anomalia = real - previa["ml"] > umbral
                    metricas[("nids_trafico_anomalia", etiqueta)] = int(anomalia)
                    metricas[("nids_trafico_umbral_anomalia", etiqueta)] = previa["ml"] + umbral
                    if anomalia:
                        abrir_incidente(
                            f"pico_{indicador}", "media" if indicador != "syn" else "alta",
                            "anomalia_trafico" if indicador != "syn" else "escaneo_red",
                            f"{indicador}={real:.0f} supera la predicción {previa['ml']:.0f} "
                            f"+ 3xRMSE ({umbral:.0f}) en {fila.ventana_inicio:%H:%M}")

            # Predicción para el minuto siguiente: modelo entrenado vs persistencia.
            nueva = {"persistencia": real}
            prediccion_ml = predicciones.get((sensor, fila.ventana_inicio, indicador))
            if prediccion_ml is not None:
                nueva["ml"] = prediccion_ml
            pendientes[(sensor, indicador, siguiente)] = nueva
            for nombre, valor in nueva.items():
                metricas[("nids_trafico_prediccion", f'{etiqueta},modelo="{nombre}"')] = valor

        # KPIs de negocio del flujo (S11): lo que lee el responsable de la red.
        bytes_totales = max(float(fila.bytes), 1.0)
        kpis = {
            "paquetes_min": fila.paquetes,
            "mbps": fila.bytes * 8 / 60 / 1e6,
            "hosts_activos": fila.hosts,
            "consultas_dns_min": fila.dns,
            "pct_trafico_cifrado": 100 * fila.bytes_tls / bytes_totales,
            "pct_trafico_externo": 100 * fila.bytes_externos / bytes_totales,
            "pct_tcp": 100 * fila.tcp / max(fila.flujos, 1),  # % de flujos TCP
        }
        for kpi, valor in kpis.items():
            metricas[("nids_trafico_kpi", f'sensor="{sensor}",kpi="{kpi}"')] = valor
        metricas[("nids_trafico_latencia_ventana_segundos", f'sensor="{sensor}"')] = \
            time.time() - fila.ventana_fin.timestamp()
        metricas[("nids_trafico_ventana_fin_timestamp", f'sensor="{sensor}"')] = fila.ventana_fin.timestamp()

    # Las predicciones que nunca reciben su ventana real (sensor apagado) se descartan.
    limite = datetime.now() - timedelta(minutes=30)
    for clave in [c for c in pendientes if c[2] < limite]:
        del pendientes[clave]

    push("nids_trafico", "spark", formato_prometheus(metricas))
    log(componente="trafico", status="batch_procesado", batchId=batch_id, ventanas=len(filas),
        modelos=sorted(modelos.modelos))
    df.unpersist()


# ---------------------------------------------------------------------------
# 1b. Detección de anomalías por flujo (K-Means de entrenar_anomalias.py)
# ---------------------------------------------------------------------------
class DetectorFlujos:
    def __init__(self):
        self.modelo = None
        self.metricas = None
        self.version = None

    def refrescar(self):
        ruta = f"{MODELOS_DIR}/anomalias_flujo/metricas.json"
        if not os.path.exists(ruta) or os.path.getmtime(ruta) == self.version:
            return
        with open(ruta, encoding="utf-8") as archivo:
            self.metricas = json.load(archivo)
        self.modelo = PipelineModel.load(f"{MODELOS_DIR}/anomalias_flujo/modelo")
        self.version = os.path.getmtime(ruta)
        centros = [np.array(c) for c in self.metricas["centros"]]

        @F.udf(T.DoubleType())
        def distancia(vector, cluster):
            return float(np.linalg.norm(vector.toArray() - centros[cluster]))
        self.distancia = distancia
        log(componente="anomalias", status="modelo_cargado", k=self.metricas["k_elegido"],
            umbral=self.metricas["umbral_puntaje"])


detector = DetectorFlujos()
NOMBRE_PROTOCOLO = {6: "tcp", 17: "udp", 1: "icmp"}


def procesar_flujos(df, batch_id):
    if df.isEmpty():
        return
    detector.refrescar()
    if detector.modelo is None:
        return
    umbral = detector.metricas["umbral_puntaje"]
    puntuados = (detector.modelo.transform(features_flujo(df))
                 .withColumn("puntaje", detector.distancia(F.col("features"), F.col("cluster")))
                 .withColumn("anomalo", F.col("puntaje") > umbral)
                 .drop("features", "features_crudas", *FEATURES_FLUJO)
                 .cache())
    total = puntuados.count()
    anomalos = puntuados.where("anomalo").orderBy(F.col("puntaje").desc())
    cantidad = anomalos.count()
    if cantidad:
        (anomalos.withColumn("fecha", F.to_date("ts_inicio"))
                 .write.mode("append").partitionBy("fecha").parquet(f"{LAKE_DIR}/anomalias_flujo"))

    detector_etiqueta = 'detector="kmeans"'
    metricas = {
        ("nids_flujos_puntuados", detector_etiqueta): total,
        ("nids_flujos_anomalos", detector_etiqueta): cantidad,
        ("nids_flujos_pct_anomalos", detector_etiqueta): 100 * cantidad / max(total, 1),
        ("nids_flujos_puntaje_max", detector_etiqueta):
            puntuados.agg(F.max("puntaje")).first()[0] or 0.0,
        ("nids_flujos_umbral", detector_etiqueta): umbral,
    }
    # Top 10 flujos más raros del batch, con su contexto, para la tabla de Grafana.
    top = anomalos.limit(10).collect()
    for posicion, fila in enumerate(top, start=1):
        etiquetas = (f'posicion="{posicion}",src="{fila.src}",dst="{fila.dst}",'
                     f'dport="{fila.dport}",proto="{NOMBRE_PROTOCOLO.get(fila.ip_prot, fila.ip_prot)}",'
                     f'cluster="{fila.cluster}",sensor="{fila.sensorId}"')
        metricas[("nids_flujo_sospechoso_puntaje", etiquetas)] = fila.puntaje
    # La tabla se reemplaza entera en cada batch con anomalías (POST por nombre).
    cuerpo = formato_prometheus(metricas)
    push("nids_flujos", "spark", cuerpo)

    if cantidad:
        peor = top[0]
        abrir_incidente(
            "flujos_anomalos", "alta" if cantidad >= 5 else "media",
            "escaneo_red" if peor.syn and not (peor.bytes_fwd + peor.bytes_bwd) else "anomalia_trafico",
            f"{cantidad} flujos anómalos de {total}; el peor {peor.src}->{peor.dst}:{peor.dport} "
            f"puntaje {peor.puntaje:.1f} (umbral {umbral:.1f})")
    log(componente="anomalias", status="batch_procesado", batchId=batch_id, flujos=total,
        anomalos=cantidad, umbral=round(umbral, 2))
    puntuados.unpersist()


# ---------------------------------------------------------------------------
# Incidentes automáticos: el modelo genera el evento empresarial
# ---------------------------------------------------------------------------
ultimo_incidente = {}
MINUTOS_ENTRE_INCIDENTES = 10


def abrir_incidente(motivo, severidad, categoria, evidencia):
    # Un incidente por motivo cada 10 min: una ráfaga de anomalías es un solo
    # ticket para la mesa de TI, no cincuenta.
    if time.time() - ultimo_incidente.get(motivo, 0) < MINUTOS_ENTRE_INCIDENTES * 60:
        return
    ultimo_incidente[motivo] = time.time()
    evento = {
        "version": "1.1",
        "tipoEvento": "incidente.abierto",
        "incidenteId": f"INC-{datetime.now():%Y%m%d}-{uuid.uuid4().hex[:6]}",
        "severidad": severidad,
        "categoria": categoria,
        "laboratorio": "sensor-nids",
        "reportadoPor": "nids-modelo",
        "evidencia": evidencia,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    (spark.createDataFrame([(evento["incidenteId"], json.dumps(evento))], ["key", "value"])
          .write.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
          .option("topic", "incidentes-seguridad")
          .save())
    log(componente="incidentes", status="incidente_automatico", **evento)


# ---------------------------------------------------------------------------
# 2. Telemetría del sensor (IoT, llega por MQTT -> puente -> Kafka)
# ---------------------------------------------------------------------------
ESQUEMA_TELEMETRIA = T.StructType([
    T.StructField("version", T.StringType()),
    T.StructField("tipoEvento", T.StringType()),
    T.StructField("sensorId", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("cpuTempC", T.DoubleType()),
    T.StructField("cpuPct", T.DoubleType()),
    T.StructField("memPct", T.DoubleType()),
    T.StructField("rxBytes", T.LongType()),
    T.StructField("rxPaquetes", T.LongType()),
    T.StructField("rxDescartados", T.LongType()),
    T.StructField("discoLibreGB", T.DoubleType()),
    T.StructField("discoUsoPct", T.DoubleType()),
    T.StructField("pendientesSubida", T.IntegerType()),
    T.StructField("capturaActiva", T.BooleanType()),
])

telemetria = (
    leer_topico("pi-telemetria")
    .select(F.from_json(F.col("value").cast("string"), ESQUEMA_TELEMETRIA).alias("e"))
    .select("e.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    # Rango físico plausible de un Raspberry Pi 5 (S7: validar antes de agregar).
    .where(F.col("event_time").isNotNull() & F.col("cpuTempC").between(-20, 110))
)

ventanas_telemetria = (
    telemetria
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "1 minute"), "sensorId")
    .agg(
        F.count("*").alias("lecturas"),
        F.avg("cpuTempC").alias("temp_prom"),
        F.max("cpuTempC").alias("temp_max"),
        F.avg("cpuPct").alias("cpu_prom"),
        F.avg("memPct").alias("mem_prom"),
        ((F.max("rxBytes") - F.min("rxBytes")) * 8 / 60 / 1e6).alias("mbps_interfaz"),
        (F.max("rxDescartados") - F.min("rxDescartados")).alias("descartados"),
        F.min("discoLibreGB").alias("disco_libre_gb"),
        F.max("discoUsoPct").alias("disco_uso_pct"),
        F.max("pendientesSubida").alias("pendientes_subida"),
        F.min(F.col("capturaActiva").cast("int")).alias("captura_activa"),
    )
    .select(F.col("window.start").alias("ventana_inicio"), F.col("window.end").alias("ventana_fin"), "*")
    .drop("window")
)


def procesar_telemetria(df, batch_id):
    if df.isEmpty():
        return
    (df.withColumn("fecha", F.to_date("ventana_inicio"))
       .write.mode("append").partitionBy("fecha").parquet(f"{LAKE_DIR}/pi_telemetria_minuto"))
    lineas = []
    ultima = {}
    for fila in df.orderBy("ventana_inicio").collect():
        ultima[fila.sensorId] = fila
    for sensor, fila in ultima.items():
        for campo in ["lecturas", "temp_prom", "temp_max", "cpu_prom", "mem_prom", "mbps_interfaz",
                      "descartados", "disco_libre_gb", "disco_uso_pct", "pendientes_subida", "captura_activa"]:
            valor = fila[campo]
            if valor is not None:
                lineas.append(f'nids_pi_{campo}{{sensor="{sensor}"}} {float(valor)}')
        lineas.append(f'nids_pi_ultima_ventana_timestamp{{sensor="{sensor}"}} {fila.ventana_fin.timestamp()}')
    push("nids_pi", "spark", "\n".join(lineas) + "\n")
    log(componente="telemetria", status="batch_procesado", batchId=batch_id, sensores=sorted(ultima))


# ---------------------------------------------------------------------------
# 3. Incidentes de seguridad (evento empresarial)
# ---------------------------------------------------------------------------
ESQUEMA_INCIDENTE = T.StructType([
    T.StructField("version", T.StringType()),
    T.StructField("tipoEvento", T.StringType()),
    T.StructField("incidenteId", T.StringType()),
    T.StructField("severidad", T.StringType()),
    T.StructField("categoria", T.StringType()),
    T.StructField("laboratorio", T.StringType()),
    T.StructField("reportadoPor", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("minutosResolucion", T.DoubleType()),
    T.StructField("evidencia", T.StringType()),
])

incidentes = (
    leer_topico("incidentes-seguridad")
    .select(F.from_json(F.col("value").cast("string"), ESQUEMA_INCIDENTE).alias("e"))
    .select("e.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .where(F.col("event_time").isNotNull() & F.col("incidenteId").isNotNull()
           & F.col("tipoEvento").isin("incidente.abierto", "incidente.cerrado"))
)

SEVERIDADES = ["baja", "media", "alta", "critica"]
estado_incidentes = {"abiertos": {}, "resoluciones": [], "automaticos": 0}


def procesar_incidentes(df, batch_id):
    # Estado de negocio (tickets abiertos, MTTR) mantenido en el driver: es
    # chico (decenas de tickets) y se reconstruye solo desde Kafka al reiniciar.
    if df.isEmpty():
        return
    (df.withColumn("fecha", F.to_date("event_time"))
       .write.mode("append").partitionBy("fecha").parquet(f"{LAKE_DIR}/incidentes"))
    for fila in df.orderBy("event_time").collect():
        if fila.tipoEvento == "incidente.abierto":
            estado_incidentes["abiertos"][fila.incidenteId] = (
                fila.severidad, fila.reportadoPor or "mesa-ti", fila.event_time.timestamp())
            if fila.reportadoPor == "nids-modelo":
                estado_incidentes["automaticos"] += 1
        else:
            estado_incidentes["abiertos"].pop(fila.incidenteId, None)
            if fila.minutosResolucion is not None:
                estado_incidentes["resoluciones"] = (estado_incidentes["resoluciones"] + [fila.minutosResolucion])[-50:]
    # Los tickets que abre el modelo no los cierra nadie en esta demo: se
    # consideran atendidos a la hora (equivale a un SLA de 60 min).
    limite = time.time() - 3600
    for incidente_id, (_, origen, abierto) in list(estado_incidentes["abiertos"].items()):
        if origen == "nids-modelo" and abierto < limite:
            del estado_incidentes["abiertos"][incidente_id]

    lineas = []
    for severidad in SEVERIDADES:
        for origen in ["mesa-ti", "nids-modelo"]:
            abiertos = sum(1 for s, o, _ in estado_incidentes["abiertos"].values()
                           if s == severidad and o == origen)
            lineas.append(f'nids_incidentes_abiertos{{severidad="{severidad}",origen="{origen}"}} {abiertos}')
    lineas.append(f"nids_incidentes_automaticos_total {estado_incidentes['automaticos']}")
    resoluciones = estado_incidentes["resoluciones"]
    if resoluciones:
        lineas.append(f"nids_incidentes_mttr_minutos {sum(resoluciones) / len(resoluciones)}")
    push("nids_incidentes", "spark", "\n".join(lineas) + "\n")
    log(componente="incidentes", status="batch_procesado", batchId=batch_id,
        abiertos=len(estado_incidentes["abiertos"]))


# Ventana deslizante: incidentes abiertos en los últimos 5 minutos, cada 1 minuto.
ventanas_incidentes = (
    incidentes
    .where(F.col("tipoEvento") == "incidente.abierto")
    .withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "5 minutes", "1 minute"), "severidad")
    .count()
    .select(F.col("window.end").alias("ventana_fin"), "severidad", "count")
)


def procesar_ventanas_incidentes(df, batch_id):
    if df.isEmpty():
        return
    ultima = df.agg(F.max("ventana_fin")).first()[0]
    conteos = {fila.severidad: fila["count"] for fila in df.where(F.col("ventana_fin") == ultima).collect()}
    lineas = [f'nids_incidentes_ultimos_5min{{severidad="{s}"}} {conteos.get(s, 0)}' for s in SEVERIDADES]
    push("nids_incidentes_ventana", "spark", "\n".join(lineas) + "\n")
    log(componente="incidentes_ventana", status="batch_procesado", batchId=batch_id,
        ventanaFin=ultima, conteos=conteos)


# ---------------------------------------------------------------------------
# Arranque de las consultas + métricas de observabilidad del propio pipeline
# ---------------------------------------------------------------------------
def iniciar(df, nombre, funcion, modo="append"):
    return (
        df.writeStream
        .queryName(nombre)
        .outputMode(modo)
        .foreachBatch(funcion)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{nombre}")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def como_dict(valor):
    # Spark 4 devuelve StreamingQueryProgress (con .json) y los offsets de
    # Kafka pueden venir como dict o como texto JSON según la versión.
    if valor is None or isinstance(valor, dict):
        return valor
    if isinstance(valor, str):
        return json.loads(valor)
    return json.loads(valor.json)


def lag_kafka(fuente):
    # Registros que Kafka ya tiene y Spark todavía no procesó (backpressure, S9).
    try:
        ultimo = como_dict(fuente.get("latestOffset")) or {}
        procesado = como_dict(fuente.get("endOffset")) or {}
    except (TypeError, ValueError, AttributeError):
        return None
    total = 0
    for topico, particiones in ultimo.items():
        for particion, offset in particiones.items():
            total += max(offset - procesado.get(topico, {}).get(particion, 0), 0)
    return total


def observar_consultas():
    while True:
        time.sleep(15)
        lineas = []
        for consulta in spark.streams.active:
            progreso = como_dict(consulta.lastProgress)
            if not progreso:
                continue
            etiqueta = f'consulta="{consulta.name}"'
            duracion = progreso.get("durationMs", {}).get("triggerExecution", 0)
            lineas += [
                f"nids_spark_filas_entrada{{{etiqueta}}} {progreso.get('numInputRows', 0)}",
                f"nids_spark_filas_entrada_por_seg{{{etiqueta}}} {progreso.get('inputRowsPerSecond') or 0}",
                f"nids_spark_filas_procesadas_por_seg{{{etiqueta}}} {progreso.get('processedRowsPerSecond') or 0}",
                f"nids_spark_duracion_batch_ms{{{etiqueta}}} {duracion}",
                f"nids_spark_consulta_activa{{{etiqueta}}} {int(consulta.isActive)}",
            ]
            lag = lag_kafka((progreso.get("sources") or [{}])[0])
            if lag is not None:
                lineas.append(f"nids_spark_lag_kafka_registros{{{etiqueta}}} {lag}")
            watermark = progreso.get("eventTime", {}).get("watermark")
            if watermark:
                marca = datetime.fromisoformat(watermark.replace("Z", "+00:00")).timestamp()
                lineas.append(f"nids_spark_watermark_timestamp{{{etiqueta}}} {marca}")
            log(componente="observabilidad", consulta=consulta.name, batchId=progreso.get("batchId"),
                filas=progreso.get("numInputRows"), duracionMs=duracion, watermark=watermark,
                lagKafka=lag, checkpoint=f"{CHECKPOINT_DIR}/{consulta.name}")
        if lineas:
            push("nids_spark", "observabilidad", "\n".join(lineas) + "\n")


precargar_historial()
consultas = [
    iniciar(ventanas_trafico, "trafico_ventanas_1min", procesar_trafico),
    iniciar(flujos, "flujos_anomalias", procesar_flujos),
    iniciar(ventanas_telemetria, "telemetria_ventanas_1min", procesar_telemetria),
    iniciar(incidentes, "incidentes_eventos", procesar_incidentes),
    iniciar(ventanas_incidentes, "incidentes_deslizante_5min", procesar_ventanas_incidentes),
]
log(componente="arranque", status="consultas_iniciadas", consultas=[c.name for c in consultas])
threading.Thread(target=observar_consultas, daemon=True).start()
spark.streams.awaitAnyTermination()
