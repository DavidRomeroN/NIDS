import gzip
import hashlib
import hmac
import ipaddress
import json
import os
import shutil
import socket
import time
import urllib.request
from pathlib import Path

import dpkt
from kafka import KafkaProducer


# Lee los PCAP (solo cabeceras) que el Pi sube cada 30 s, reconstruye los
# flujos (conexiones) y publica un evento por flujo terminado en Kafka, con las
# mismas columnas clave que el histórico TRCU.csv (NTLFlowLyzer). Así el modelo
# entrenado con el histórico recibe en vivo exactamente las mismas variables.
# Es el único lugar donde se tocan IP reales: salen de aquí seudonimizadas con
# HMAC (Ley 29733), nunca en claro.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trafico-flujos")
DIR_ENTRADA = Path(os.getenv("DIR_ENTRADA", "/srv/nids/incoming"))
DIR_PROCESADOS = Path(os.getenv("DIR_PROCESADOS", "/srv/nids/procesados"))
DIAS_RETENCION_PCAP = float(os.getenv("DIAS_RETENCION_PCAP", "3"))
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
SECRETO = os.getenv("SECRETO_SEUDONIMO", "").encode("utf-8")

# Fin de flujo: FIN/RST, o 30 s sin paquetes, o 2 min de duración máxima
# (después sigue como un flujo nuevo). Si el sensor deja de enviar archivos
# 2 min, se cierran todos los flujos abiertos.
INACTIVIDAD_US = 30 * 1_000_000
DURACION_MAX_US = 120 * 1_000_000
SEGUNDOS_SIN_ARCHIVOS_PARA_VACIAR = 120

if not SECRETO or SECRETO == b"cambiar-esto":
    raise SystemExit("SECRETO_SEUDONIMO no está configurado en .env")

FIN, SYN, RST = 0x01, 0x02, 0x04


def log(**campos):
    print(json.dumps({"service": "nids-ingestor", **campos}), flush=True)


def crear_productor():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
                linger_ms=50,
                batch_size=256 * 1024,
                compression_type="gzip",
            )
        except Exception as error:
            log(status="kafka_no_disponible", error=str(error))
            time.sleep(5)


cache_seudonimos = {}


def seudonimo(ip_bytes):
    # Mismo IP -> mismo seudónimo (se pueden seguir hosts sin saber quién es).
    if ip_bytes not in cache_seudonimos:
        if len(cache_seudonimos) > 200_000:
            cache_seudonimos.clear()
        digest = hmac.new(SECRETO, ip_bytes, hashlib.sha256).hexdigest()[:12]
        cache_seudonimos[ip_bytes] = f"h-{digest}"
    return cache_seudonimos[ip_bytes]


def lector_pcap(archivo):
    # editcap/tcpdump pueden dejar pcap clásico o pcapng según la versión.
    try:
        return dpkt.pcap.Reader(archivo)
    except ValueError:
        archivo.seek(0)
        return dpkt.pcapng.Reader(archivo)


def leer_paquete(ts_us, buf):
    """Cabeceras -> (src, dst, sport, dport, proto, payload, flags) o None.

    payload = largo real según las cabeceras IP/TCP/UDP, no lo capturado: es
    correcto aunque el Pi haya recortado el paquete a 128 bytes. Es la misma
    medida que pkt_len de NTLFlowLyzer (bytes de carga útil).
    """
    ip = dpkt.ethernet.Ethernet(buf).data
    if isinstance(ip, dpkt.ip.IP):
        restante = ip.len - ip.hl * 4
        proto = ip.p
    elif isinstance(ip, dpkt.ip6.IP6):
        restante = ip.plen
        proto = ip.nxt
    else:
        return None
    capa4 = ip.data
    sport = dport = 0
    flags = 0
    if isinstance(capa4, dpkt.tcp.TCP):
        sport, dport, flags = capa4.sport, capa4.dport, capa4.flags
        payload = restante - capa4.off * 4
    elif isinstance(capa4, dpkt.udp.UDP):
        sport, dport = capa4.sport, capa4.dport
        payload = restante - 8
    else:
        payload = restante
    return ip.src, ip.dst, sport, dport, proto, max(payload, 0), flags


class TablaFlujos:
    """Flujos bidireccionales abiertos, por 5-tupla, en tiempo de evento."""

    def __init__(self):
        self.abiertos = {}

    def agregar(self, ts_us, src, dst, sport, dport, proto, payload, flags):
        terminados = []
        directa = (src, dst, sport, dport, proto)
        inversa = (dst, src, dport, sport, proto)
        clave = directa if directa in self.abiertos else inversa if inversa in self.abiertos else None
        flujo = self.abiertos.get(clave) if clave else None

        if flujo and (ts_us - flujo["ultimo"] > INACTIVIDAD_US
                      or ts_us - flujo["inicio"] > DURACION_MAX_US):
            terminados.append(self.abiertos.pop(clave))
            flujo = None
        if flujo is None:
            clave = directa
            flujo = {"clave": directa, "inicio": ts_us, "ultimo": ts_us,
                     "fwd_pkt_cnt": 0, "bwd_pkt_cnt": 0, "fwd_pkt_len_tot": 0,
                     "bwd_pkt_len_tot": 0, "flag_SYN": 0, "fin": 0}
            self.abiertos[clave] = flujo

        sentido = "fwd" if (src, dst, sport, dport, proto) == flujo["clave"] else "bwd"
        flujo[f"{sentido}_pkt_cnt"] += 1
        flujo[f"{sentido}_pkt_len_tot"] += payload
        flujo["ultimo"] = ts_us
        if flags & SYN:
            flujo["flag_SYN"] += 1
        if flags & FIN:
            flujo["fin"] += 1
        if flags & RST or flujo["fin"] >= 2:
            terminados.append(self.abiertos.pop(clave))
        return terminados

    def vencidos(self, ahora_us):
        claves = [c for c, f in self.abiertos.items() if ahora_us - f["ultimo"] > INACTIVIDAD_US]
        return [self.abiertos.pop(c) for c in claves]

    def vaciar(self):
        todos = list(self.abiertos.values())
        self.abiertos.clear()
        return todos


def a_evento(sensor_id, flujo):
    src, dst, sport, dport, proto = flujo["clave"]
    return {
        "v": 2,
        "sensorId": sensor_id,
        "timestamp": flujo["inicio"],
        "flow_duration": flujo["ultimo"] - flujo["inicio"],
        "src_addr": seudonimo(src),
        "dst_addr": seudonimo(dst),
        "srcInterna": ipaddress.ip_address(src).is_private,
        "dstInterna": ipaddress.ip_address(dst).is_private,
        "src_port": sport,
        "dst_port": dport,
        "ip_prot": proto,
        "fwd_pkt_cnt": flujo["fwd_pkt_cnt"],
        "bwd_pkt_cnt": flujo["bwd_pkt_cnt"],
        "fwd_pkt_len_tot": flujo["fwd_pkt_len_tot"],
        "bwd_pkt_len_tot": flujo["bwd_pkt_len_tot"],
        "flag_SYN": flujo["flag_SYN"],
    }


def publicar(producer, sensor_id, flujos):
    for flujo in flujos:
        evento = a_evento(sensor_id, flujo)
        producer.send(KAFKA_TOPIC, key=evento["src_addr"], value=evento)
    return len(flujos)


def push_metricas(cuerpo):
    url = f"{PUSHGATEWAY_URL}/metrics/job/nids_ingestor/instance/{socket.gethostname()}"
    try:
        peticion = urllib.request.Request(url, data=cuerpo.encode("utf-8"), method="POST")
        urllib.request.urlopen(peticion, timeout=5).close()
    except OSError as error:
        log(status="pushgateway_no_disponible", error=str(error))


tablas = {}  # una tabla de flujos por sensor


def procesar(producer, ruta):
    # Nombre del archivo: <sensorId>_<AAAAMMDD>_<HHMMSS>.pcap.gz
    sensor_id = ruta.name.split("_")[0]
    tabla = tablas.setdefault(sensor_id, TablaFlujos())
    inicio = time.time()
    paquetes = errores = publicados = 0
    ultimo_us = None

    with gzip.open(ruta, "rb") as archivo:
        for ts, buf in lector_pcap(archivo):
            try:
                paquete = leer_paquete(int(ts * 1e6), buf)
            except (dpkt.UnpackError, ValueError, IndexError, AttributeError):
                errores += 1
                continue
            if paquete is None:
                continue
            ultimo_us = int(ts * 1e6)
            publicados += publicar(producer, sensor_id, tabla.agregar(ultimo_us, *paquete))
            paquetes += 1
    if ultimo_us:
        publicados += publicar(producer, sensor_id, tabla.vencidos(ultimo_us))
    producer.flush()

    duracion = time.time() - inicio
    retraso = time.time() - ultimo_us / 1e6 if ultimo_us else 0
    pendientes = len(list(DIR_ENTRADA.glob("*.pcap.gz"))) - 1
    log(status="procesado", archivo=ruta.name, paquetes=paquetes, errores=errores,
        flujosPublicados=publicados, flujosAbiertos=len(tabla.abiertos),
        duracionSeg=round(duracion, 2), retrasoSeg=round(retraso, 1))
    push_metricas(
        f'nids_ingestor_paquetes_archivo{{sensor="{sensor_id}"}} {paquetes}\n'
        f'nids_ingestor_flujos_archivo{{sensor="{sensor_id}"}} {publicados}\n'
        f'nids_ingestor_flujos_abiertos{{sensor="{sensor_id}"}} {len(tabla.abiertos)}\n'
        f'nids_ingestor_errores_archivo{{sensor="{sensor_id}"}} {errores}\n'
        f'nids_ingestor_duracion_segundos{{sensor="{sensor_id}"}} {duracion:.3f}\n'
        f'nids_ingestor_retraso_segundos{{sensor="{sensor_id}"}} {retraso:.1f}\n'
        f'nids_ingestor_archivos_pendientes {max(pendientes, 0)}\n'
        f'nids_ingestor_ultimo_archivo_timestamp {time.time():.0f}\n'
    )
    DIR_PROCESADOS.mkdir(parents=True, exist_ok=True)
    shutil.move(str(ruta), DIR_PROCESADOS / ruta.name)


def limpiar_procesados():
    limite = time.time() - DIAS_RETENCION_PCAP * 86400
    for ruta in DIR_PROCESADOS.glob("*.pcap.gz"):
        if ruta.stat().st_mtime < limite:
            ruta.unlink()


def main():
    DIR_ENTRADA.mkdir(parents=True, exist_ok=True)
    producer = crear_productor()
    log(status="starting", dirEntrada=str(DIR_ENTRADA), kafkaTopic=KAFKA_TOPIC)
    ultima_limpieza = 0
    ultimo_archivo = time.time()
    while True:
        # El Pi sube como .tmp y renombra al terminar: solo se leen archivos completos.
        archivos = sorted(DIR_ENTRADA.glob("*.pcap.gz"))
        for ruta in archivos:
            try:
                procesar(producer, ruta)
            except (OSError, EOFError, ValueError) as error:
                log(status="archivo_invalido", archivo=ruta.name, error=str(error))
                DIR_PROCESADOS.mkdir(parents=True, exist_ok=True)
                shutil.move(str(ruta), DIR_PROCESADOS / f"invalido_{ruta.name}")
            ultimo_archivo = time.time()

        # Sensor sin enviar (apagado o sin red): se cierran sus flujos abiertos
        # para que no queden contados a medias.
        if time.time() - ultimo_archivo > SEGUNDOS_SIN_ARCHIVOS_PARA_VACIAR:
            for sensor_id, tabla in tablas.items():
                if tabla.abiertos:
                    total = publicar(producer, sensor_id, tabla.vaciar())
                    producer.flush()
                    log(status="flujos_vaciados", sensorId=sensor_id, flujos=total)

        if time.time() - ultima_limpieza > 3600:
            limpiar_procesados()
            ultima_limpieza = time.time()
        if not archivos:
            time.sleep(2)


if __name__ == "__main__":
    main()
