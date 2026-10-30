import csv
import hashlib
import hmac
import ipaddress
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kafka import KafkaProducer


# Plan B de la demo: si el Pi no tiene salida a internet en el laboratorio,
# reproduce en tiempo real el tráfico del histórico TRCU.csv como si llegara
# del sensor. Toma el día del histórico con el mismo día de la semana y más
# tráfico a la hora actual, corre sus timestamps a hoy y publica cada flujo en
# `trafico-flujos` con el mismo contrato v2 del ingestor. Va con sensorId
# "replay-trcu": nunca se confunde con tráfico vivo y el reentrenamiento lo
# excluye. Solo se levanta a mano:
#   docker compose --profile respaldo up -d reproductor
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trafico-flujos")
CSV_HISTORICO = os.getenv("CSV_HISTORICO", "/historico/TRCU.csv")
SENSOR_ID = os.getenv("SENSOR_ID", "replay-trcu")
SECRETO = os.getenv("SECRETO_SEUDONIMO", "").encode("utf-8")
LIMA = ZoneInfo("America/Lima")

if not SECRETO or SECRETO == b"cambiar-esto":
    raise SystemExit("SECRETO_SEUDONIMO no está configurado en .env")


def log(**campos):
    print(json.dumps({"service": "nids-reproductor", **campos}), flush=True)


def seudonimo(ip_texto):
    # Mismo HMAC que el ingestor (sobre los bytes de la IP): un host del
    # histórico recibe el mismo seudónimo que tendría capturado en vivo.
    digest = hmac.new(SECRETO, ipaddress.ip_address(ip_texto).packed, hashlib.sha256).hexdigest()[:12]
    return f"h-{digest}"


def cargar():
    """TRCU.csv -> {fecha Lima: [(timestamp_us, evento sin timestamp), ...]}."""
    por_dia = defaultdict(list)
    seudonimos = {}
    with open(CSV_HISTORICO, newline="") as archivo:
        for fila in csv.DictReader(archivo):
            ts = int(fila["timestamp"])
            for ip in (fila["src_addr"], fila["dst_addr"]):
                if ip not in seudonimos:
                    seudonimos[ip] = (seudonimo(ip), ipaddress.ip_address(ip).is_private)
            src, src_interna = seudonimos[fila["src_addr"]]
            dst, dst_interna = seudonimos[fila["dst_addr"]]
            evento = {
                "v": 2,
                "sensorId": SENSOR_ID,
                "flow_duration": int(float(fila["flow_duration"])),
                "src_addr": src,
                "dst_addr": dst,
                "srcInterna": src_interna,
                "dstInterna": dst_interna,
                "src_port": int(fila["src_port"]),
                "dst_port": int(fila["dst_port"]),
                "ip_prot": int(fila["ip_prot"]),
                "fwd_pkt_cnt": int(fila["fwd_pkt_cnt"]),
                "bwd_pkt_cnt": int(fila["bwd_pkt_cnt"]),
                "fwd_pkt_len_tot": int(float(fila["fwd_pkt_len_tot"])),
                "bwd_pkt_len_tot": int(float(fila["bwd_pkt_len_tot"])),
                "flag_SYN": int(float(fila["flag_SYN"])),
            }
            por_dia[datetime.fromtimestamp(ts / 1e6, LIMA).date()].append((ts, evento))
    for flujos in por_dia.values():
        flujos.sort(key=lambda par: par[0])
    return por_dia


def elegir_dia(por_dia, conteo_hora, ahora):
    # Mismo día de la semana y con tráfico a esta hora; si no hay, cualquier día.
    candidatos = [d for d in por_dia if d.weekday() == ahora.weekday() and conteo_hora[(d, ahora.hour)]]
    if not candidatos:
        candidatos = [d for d in por_dia if conteo_hora[(d, ahora.hour)]]
    return max(candidatos, key=lambda d: conteo_hora[(d, ahora.hour)], default=None)


def main():
    inicio = time.time()
    por_dia = cargar()
    conteo_hora = Counter()
    for dia, flujos in por_dia.items():
        for ts, _ in flujos:
            conteo_hora[(dia, datetime.fromtimestamp(ts / 1e6, LIMA).hour)] += 1
    log(status="historico_cargado", dias=len(por_dia),
        flujos=sum(len(f) for f in por_dia.values()), segundos=round(time.time() - inicio, 1))

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        linger_ms=50,
        compression_type="gzip",
    )

    while True:
        ahora = datetime.now(LIMA)
        fin_hora = ahora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        dia = elegir_dia(por_dia, conteo_hora, ahora)
        if dia is None:
            time.sleep((fin_hora - ahora).total_seconds())
            continue

        desfase_us = (ahora.date() - dia).days * 86_400_000_000
        ahora_us = int(ahora.timestamp() * 1e6)
        fin_us = int(fin_hora.timestamp() * 1e6)
        cola = [(ts + desfase_us, evento) for ts, evento in por_dia[dia]
                if ahora_us <= ts + desfase_us < fin_us]
        log(status="reproduciendo_hora", diaOrigen=str(dia), hora=ahora.hour, flujos=len(cola))

        publicados = 0
        for ts, evento in cola:
            espera = ts / 1e6 - time.time()
            if espera > 0:
                time.sleep(espera)
            producer.send(KAFKA_TOPIC, key=evento["src_addr"], value={**evento, "timestamp": ts})
            publicados += 1
            if publicados % 200 == 0:
                producer.flush()
        producer.flush()
        espera = fin_hora.timestamp() - time.time()
        if espera > 0:
            time.sleep(espera)


if __name__ == "__main__":
    main()
