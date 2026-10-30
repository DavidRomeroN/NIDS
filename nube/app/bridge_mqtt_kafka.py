import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# Patrón IoT de S7: el sensor (Pi) nunca habla Kafka directo:
# publica su telemetría por MQTT y este puente la reenvía a Kafka sin
# transformarla. La validación de esquema/rango se hace en Spark.
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "nids/+/telemetria")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pi-telemetria")


def crear_productor():
    # Kafka puede tardar en aceptar conexiones justo después de un reinicio de la VM.
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: value,
            )
        except Exception as error:
            print(json.dumps({"service": "nids-bridge", "status": "kafka_no_disponible", "error": str(error)}))
            time.sleep(5)


producer = crear_productor()


def on_connect(client, userdata, flags, reason_code, properties=None):
    print(json.dumps({
        "service": "nids-bridge",
        "mqttTopic": MQTT_TOPIC,
        "status": "connected" if reason_code == 0 else f"connect_failed:{reason_code}",
    }))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    raw = msg.payload
    try:
        sensor_id = json.loads(raw.decode("utf-8")).get("sensorId") or "desconocido"
    except (json.JSONDecodeError, UnicodeDecodeError):
        sensor_id = "desconocido"

    metadata = producer.send(KAFKA_TOPIC, key=sensor_id, value=raw).get(timeout=10)
    print(json.dumps({
        "service": "nids-bridge",
        "mqttTopic": msg.topic,
        "kafkaTopic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "sensorId": sensor_id,
        "status": "forwarded",
    }))


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"nids-bridge-{uuid.uuid4().hex[:8]}")
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        break
    except OSError as error:
        print(json.dumps({"service": "nids-bridge", "status": "mqtt_no_disponible", "error": str(error)}))
        time.sleep(5)

client.loop_forever()
