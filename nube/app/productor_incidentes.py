import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


# Evento empresarial (S6): tickets de incidentes de seguridad que registra la
# mesa de TI. Simula el ritmo real de una mesa de ayuda (más incidentes en
# horario de clases) y cierra cada ticket tras un tiempo de resolución
# realista. Con --uno publica un solo incidente a mano, para la demo en vivo.
#
# Contrato de evento v1 (documentado en docs/contratos.md):
#   version, tipoEvento (incidente.abierto | incidente.cerrado), incidenteId,
#   severidad, categoria, laboratorio, reportadoPor, timestamp (ISO-8601 UTC),
#   minutosResolucion (solo en incidente.cerrado)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "incidentes-seguridad")

SEVERIDADES = [("baja", 0.45), ("media", 0.35), ("alta", 0.15), ("critica", 0.05)]
CATEGORIAS = ["malware", "phishing", "acceso_no_autorizado", "escaneo_red",
              "uso_indebido", "disponibilidad"]
LABORATORIOS = ["lab-01", "lab-02", "lab-03", "sala-docentes"]
# Minutos medianos de resolución por severidad (los críticos se atienden primero).
MEDIANA_RESOLUCION = {"baja": 240, "media": 120, "alta": 45, "critica": 20}


def crear_productor():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
        except Exception as error:
            print(json.dumps({"service": "nids-incidentes", "status": "kafka_no_disponible", "error": str(error)}))
            time.sleep(5)


def ahora_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def elegir(pesos):
    return random.choices([v for v, _ in pesos], weights=[p for _, p in pesos])[0]


def publicar(producer, evento):
    metadata = producer.send(KAFKA_TOPIC, key=evento["incidenteId"], value=evento).get(timeout=10)
    print(json.dumps({
        "service": "nids-incidentes",
        "kafkaTopic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        **evento,
        "status": "published",
    }), flush=True)


def nuevo_incidente(severidad=None, categoria=None, laboratorio=None):
    return {
        "version": "1.0",
        "tipoEvento": "incidente.abierto",
        "incidenteId": f"INC-{datetime.now():%Y%m%d}-{uuid.uuid4().hex[:6]}",
        "severidad": severidad or elegir(SEVERIDADES),
        "categoria": categoria or random.choice(CATEGORIAS),
        "laboratorio": laboratorio or random.choice(LABORATORIOS),
        "reportadoPor": "mesa-ti",
        "timestamp": ahora_iso(),
    }


def tasa_por_hora():
    # Incidentes/hora esperados según el calendario académico (hora de Lima).
    momento = datetime.now()
    if momento.weekday() < 5 and 7 <= momento.hour < 22:
        return 4.0
    if momento.weekday() == 5 and 8 <= momento.hour < 13:
        return 1.5
    return 0.3


def bucle(producer):
    abiertos = {}
    while True:
        if random.random() < tasa_por_hora() / 3600:
            evento = nuevo_incidente()
            publicar(producer, evento)
            minutos = random.lognormvariate(0, 0.6) * MEDIANA_RESOLUCION[evento["severidad"]]
            abiertos[evento["incidenteId"]] = (time.time() + minutos * 60, evento, minutos)

        for incidente_id, (cierre, evento, minutos) in list(abiertos.items()):
            if time.time() >= cierre:
                publicar(producer, {**evento, "tipoEvento": "incidente.cerrado",
                                    "timestamp": ahora_iso(),
                                    "minutosResolucion": round(minutos, 1)})
                del abiertos[incidente_id]
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uno", action="store_true", help="publica un solo incidente y termina")
    parser.add_argument("--severidad", choices=[s for s, _ in SEVERIDADES])
    parser.add_argument("--categoria", choices=CATEGORIAS)
    parser.add_argument("--laboratorio")
    args = parser.parse_args()

    producer = crear_productor()
    if args.uno:
        publicar(producer, nuevo_incidente(args.severidad, args.categoria, args.laboratorio))
        producer.flush()
    else:
        bucle(producer)


if __name__ == "__main__":
    main()
