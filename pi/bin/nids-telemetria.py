#!/usr/bin/env python3
# Telemetría del sensor (S7): cada 5 s publica su estado por MQTT. El broker
# vive en la nube y se alcanza por el túnel SSH (nids-tunel.service), así que
# desde aquí siempre es 127.0.0.1:1883.
import json
import os
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


def leer_conf(ruta="/etc/nids/nids.conf"):
    conf = {}
    with open(ruta, encoding="utf-8") as archivo:
        for linea in archivo:
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, valor = linea.split("=", 1)
                conf[clave] = valor.strip('"')
    return conf


CONF = leer_conf()
SENSOR_ID = CONF.get("SENSOR_ID", "nids-pi-01")
INTERFAZ = CONF.get("INTERFAZ_CAPTURA", "eth0")
DIR_CAPTURA = CONF.get("DIR_CAPTURA", "/mnt/nids/pcap")
TOPICO = f"nids/{SENSOR_ID}/telemetria"
INTERVALO = 5


def leer(ruta, defecto=0):
    try:
        with open(ruta, encoding="utf-8") as archivo:
            return int(archivo.read().strip())
    except (OSError, ValueError):
        return defecto


def tiempos_cpu():
    with open("/proc/stat", encoding="utf-8") as archivo:
        valores = [int(v) for v in archivo.readline().split()[1:]]
    return sum(valores), valores[3] + valores[4]  # total, idle + iowait


def memoria_pct():
    info = {}
    with open("/proc/meminfo", encoding="utf-8") as archivo:
        for linea in archivo:
            clave, valor = linea.split(":", 1)
            info[clave] = int(valor.split()[0])
    return 100 * (1 - info["MemAvailable"] / info["MemTotal"])


def captura_activa():
    return subprocess.run(["systemctl", "is-active", "--quiet", "nids-captura"]).returncode == 0


def pendientes_subida():
    try:
        nombres = os.listdir(DIR_CAPTURA)
    except OSError:
        return 0
    pcaps = [n for n in nombres if n.endswith(".pcap")]
    marcados = {n.rsplit(".", 1)[0] for n in nombres if n.endswith((".subido", ".omitido"))}
    return max(len([p for p in pcaps if p not in marcados]) - 1, 0)


def lectura(cpu_anterior):
    total, idle = tiempos_cpu()
    delta_total = max(total - cpu_anterior[0], 1)
    cpu_pct = 100 * (1 - (idle - cpu_anterior[1]) / delta_total)
    estadisticas = f"/sys/class/net/{INTERFAZ}/statistics"
    try:
        disco = shutil.disk_usage(DIR_CAPTURA)
        libre_gb, uso_pct = disco.free / 1e9, 100 * disco.used / disco.total
    except OSError:
        libre_gb, uso_pct = 0.0, 100.0
    evento = {
        "version": "1.0",
        "tipoEvento": "sensor.telemetria",
        "sensorId": SENSOR_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "cpuTempC": leer("/sys/class/thermal/thermal_zone0/temp") / 1000,
        "cpuPct": round(cpu_pct, 1),
        "memPct": round(memoria_pct(), 1),
        "rxBytes": leer(f"{estadisticas}/rx_bytes"),
        "rxPaquetes": leer(f"{estadisticas}/rx_packets"),
        "rxDescartados": leer(f"{estadisticas}/rx_dropped") + leer(f"{estadisticas}/rx_missed_errors"),
        "discoLibreGB": round(libre_gb, 2),
        "discoUsoPct": round(uso_pct, 1),
        "pendientesSubida": pendientes_subida(),
        "capturaActiva": captura_activa(),
    }
    return evento, (total, idle)


def crear_cliente():
    identificador = f"{SENSOR_ID}-{uuid.uuid4().hex[:6]}"
    # Raspberry Pi OS trae paho-mqtt 1.6 (bookworm) o 2.x (trixie).
    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=identificador)
    return mqtt.Client(client_id=identificador)


def main():
    cliente = crear_cliente()
    cliente.reconnect_delay_set(min_delay=1, max_delay=30)
    while True:
        try:
            cliente.connect("127.0.0.1", 1883, keepalive=30)
            break
        except OSError:
            time.sleep(5)  # el túnel todavía no está arriba
    cliente.loop_start()

    cpu_anterior = tiempos_cpu()
    while True:
        time.sleep(INTERVALO)
        evento, cpu_anterior = lectura(cpu_anterior)
        cliente.publish(TOPICO, json.dumps(evento), qos=1)


if __name__ == "__main__":
    main()
