# Contratos de eventos y tópicos de Kafka

Tres tópicos, uno por tipo de fuente. Cada contrato lleva su número de versión
dentro del propio evento para poder evolucionarlo sin romper a los consumidores.

**Tabla 1. Tópicos del pipeline**

| Tópico | Tipo de fuente | Productor | Key | Particiones | Retención |
|---|---|---|---|---:|---|
| `incidentes-seguridad` | Evento empresarial (S6) | `productor_incidentes.py` (mesa de TI) | `incidenteId` | 3 | 72 h |
| `pi-telemetria` | IoT/sensor (S7) | Raspberry Pi → MQTT → `bridge_mqtt_kafka.py` | `sensorId` | 3 | 72 h |
| `trafico-flujos` | Sensor de red (lectura del Pi) | `ingestor_pcap.py` (PCAP de cabeceras → flujos); plan B de la demo: `reproductor_historico.py` (sensor `replay-trcu`) | `src_addr` (seudónimo) | 3 | 72 h |

## 1. `incidentes-seguridad` — evento empresarial v1.1

Ticket de incidente. Se publica un evento al abrir el ticket y otro al
cerrarlo. Tiene **dos productores**: la mesa de TI (`productor_incidentes.py`)
y el propio sistema (Spark abre un ticket cuando un modelo detecta una
anomalía). La v1.1 agrega `evidencia` y el origen `nids-modelo`; los
consumidores de la v1.0 siguen funcionando porque solo se agregaron campos opcionales.

| Campo | Tipo | Obligatorio | Descripción |
|---|---|---|---|
| `version` | string | sí | `"1.0"` (mesa de TI) o `"1.1"` (modelo) |
| `tipoEvento` | string | sí | `incidente.abierto` o `incidente.cerrado` |
| `incidenteId` | string | sí | `INC-AAAAMMDD-xxxxxx`. Es también la key: abrir y cerrar caen en la misma partición, en orden. |
| `severidad` | string | sí | `baja`, `media`, `alta` o `critica` |
| `categoria` | string | sí | `malware`, `phishing`, `acceso_no_autorizado`, `escaneo_red`, `uso_indebido`, `disponibilidad` o `anomalia_trafico` |
| `laboratorio` | string | sí | Ambiente afectado (`lab-01`, `sala-docentes`, `sensor-nids`, ...) |
| `reportadoPor` | string | sí | `mesa-ti` o `nids-modelo` |
| `timestamp` | string ISO-8601 UTC | sí | Momento del evento (tiempo de evento para Spark) |
| `minutosResolucion` | number | solo en `cerrado` | Minutos entre apertura y cierre (base del MTTR) |
| `evidencia` | string | no (v1.1) | Qué detectó el modelo y contra qué umbral |

```json
{"version":"1.0","tipoEvento":"incidente.abierto","incidenteId":"INC-20261030-3f9a1c",
 "severidad":"alta","categoria":"escaneo_red","laboratorio":"lab-01",
 "reportadoPor":"mesa-ti","timestamp":"2026-10-30T14:05:12.311+00:00"}
```

Publicar uno a mano en la demo:

```bash
docker compose exec incidentes python productor_incidentes.py --uno --severidad critica --categoria escaneo_red
```

## 2. `pi-telemetria` — sensor IoT v1.0

El Pi publica por MQTT (`nids/<sensorId>/telemetria`) cada 5 s y el puente lo
reenvía a Kafka sin tocarlo (patrón de S7: el dispositivo nunca habla Kafka directo).

| Campo | Tipo | Rango válido | Descripción |
|---|---|---|---|
| `version` | string | | `"1.0"` |
| `tipoEvento` | string | | `sensor.telemetria` |
| `sensorId` | string | | Identificador del Pi (key) |
| `timestamp` | string ISO-8601 UTC | | Hora del Pi (sincronizada por NTP) |
| `cpuTempC` | number | -20 a 110 | Temperatura del SoC. Fuera de rango, Spark la descarta. |
| `cpuPct`, `memPct` | number | 0 a 100 | Uso de CPU y RAM |
| `rxBytes`, `rxPaquetes` | integer | contador | Contadores de la interfaz espejo |
| `rxDescartados` | integer | contador | Paquetes que el kernel no alcanzó a capturar |
| `discoLibreGB`, `discoUsoPct` | number | | Espacio del disco de captura |
| `pendientesSubida` | integer | | PCAP terminados aún no enviados a la nube |
| `capturaActiva` | boolean | | Estado del servicio `nids-captura` |

## 3. `trafico-flujos` — lectura del sensor de red v2

Un evento por **flujo** (conexión bidireccional por 5-tupla) observado en el
puerto espejo. Usa los mismos nombres de columna que el histórico `TRCU.csv`
(NTLFlowLyzer), para que el modelo entrenado con el histórico reciba en vivo
exactamente las mismas variables. Solo metadatos de cabecera, nunca contenido.
Las IP se reemplazan por un seudónimo HMAC-SHA256 antes de entrar a Kafka (Ley
29733); la misma IP produce siempre el mismo seudónimo.

Un flujo se publica al terminar: FIN en ambos sentidos o RST, 30 s sin
paquetes, o 2 min de duración (lo que sigue cuenta como un flujo nuevo).

| Campo | Tipo | Descripción |
|---|---|---|
| `v` | integer | `2` |
| `sensorId` | string | Pi que lo capturó |
| `timestamp` | integer | Inicio del flujo, epoch en microsegundos (tiempo de evento) |
| `flow_duration` | integer | Microsegundos entre el primer y el último paquete |
| `src_addr`, `dst_addr` | string | Seudónimos `h-xxxxxxxxxxxx` (quien inició = `src`) |
| `srcInterna`, `dstInterna` | boolean | Si la IP es privada (red del campus) |
| `src_port`, `dst_port` | integer | Puertos (0 si no es TCP/UDP) |
| `ip_prot` | integer | 6 = TCP, 17 = UDP, 1 = ICMP |
| `fwd_pkt_cnt`, `bwd_pkt_cnt` | integer | Paquetes en cada sentido |
| `fwd_pkt_len_tot`, `bwd_pkt_len_tot` | integer | Bytes de carga útil en cada sentido, según las cabeceras IP/TCP/UDP (correcto aunque el paquete llegue recortado a 128 B) |
| `flag_SYN` | integer | Paquetes con SYN (SYN + SYN/ACK) |

**Correspondencia con el histórico.** `preparar_historico.py` lee esas mismas
columnas de `TRCU.csv` y ambos lados pasan por la misma función
`agregar_por_minuto()` (`spark/jobs/comun.py`). Diferencia conocida: NTLFlowLyzer
puede cortar los flujos largos con otros tiempos de espera, así que en flujos de
más de 2 min los conteos pueden diferir levemente.
