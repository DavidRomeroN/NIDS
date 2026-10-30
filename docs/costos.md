# Costos y criterios de escalado

## 1. Costo actual

La nube corre en **Azure for Students** (crédito de 100 USD, sin tarjeta), en
una VM **Standard B2as_v2** (2 vCPU AMD, 8 GB RAM) en North Central US.

**Tabla 1. Costo mensual de la configuración actual (precio de lista)**

| Recurso | Detalle | USD/mes |
|---|---|---:|
| VM B2as_v2 | 0,0752 USD/h × 730 h | 54,90 |
| Disco del SO | 64 GiB SSD estándar (E6) | ~4,80 |
| IP pública estática | Standard | ~3,65 |
| Tráfico de entrada (subida del Pi) | gratis en Azure | 0 |
| Tráfico de salida (Grafana) | < 5 GB/mes, dentro de lo gratuito | 0 |
| **Total** | | **~63 USD/mes** |

Con el crédito de 100 USD alcanza para **~6,5 semanas 24/7** (hasta mediados de
diciembre de 2026), que cubre el semestre. El sensor (Raspberry Pi 5 + disco)
es una inversión única del equipo.

**Alternativa evaluada:** Oracle Cloud Always Free (4 OCPU / 24 GB, S/ 0 para
siempre). Se descartó en la práctica porque la región de Santiago no tenía
capacidad ARM al momento de desplegar ("Out of host capacity"); el mismo
`compose.yml` corre allí sin cambios si se libera capacidad.

## 2. Carga real medida

**Tabla 2. Carga observada (Prometheus, 30-oct-2026, ventana de 30 min)**

| Métrica | Valor | Fuente |
|---|---:|---|
| CPU de la VM (con entrenamiento y notebooks corriendo a la vez) | 58,7 % | `node_cpu_seconds_total` |
| RAM de la VM | 71,4 % | `node_memory_*` |
| Duración máxima de micro-batch | 5,6 - 22,9 s según la consulta | `nids_spark_duracion_batch_ms` |
| Lag Kafka → Spark | 0 en las 5 consultas | `nids_spark_lag_kafka_registros` |
| Volumen del histórico | 397 354 flujos en 43 días ≈ 9 200 flujos/día; ~12,7 KB/min de carga útil | TRCU.csv |
| Data Lake + modelos | 25 MB | volumen `spark_data` |

El volumen del puerto espejo del laboratorio se mide con la primera jornada de
captura (paneles "Mensajes/s publicados por tópico" e "Interfaz espejo vs
pipeline") y se agrega a esta tabla.

## 3. Criterios de escalado

El cuello de botella es la cantidad de **paquetes por segundo** que lee el
ingestor; él los agrupa en flujos (decenas de veces menos eventos) antes de
Kafka, y Spark agrega por ventana.

**Tabla 3. Señales que obligan a escalar y qué hacer**

| Señal (panel de Grafana) | Umbral | Acción |
|---|---|---|
| Lag de Kafka → Spark crece sin bajar | > 0 sostenido 10 min | Más núcleos para Spark (`SPARK_MASTER=local[4]`) o separar el entrenamiento a otra VM |
| Duración del micro-batch | > 20 s (el trigger) | Igual que la anterior; o subir el trigger a 30-60 s |
| CPU de la VM | > 80 % sostenido | La B2as_v2 es *burstable* (créditos de CPU): pasar a D2as_v5 (~70 USD/mes) o D4as_v5 (~140 USD/mes) |
| RAM de la VM | > 85 % | Entrenar de noche o en otra VM |
| Paquetes descartados en el Pi | > 0 sostenido | Bajar `SNAPLEN_LOCAL` o restringir el filtro BPF |
| Disco del Pi | > 90 % | Cambiar a un disco más grande |

## 4. Escalar a todo el campus

Con *N* laboratorios hay *N* sensores (inversión única, ~S/ 600 cada uno con
disco) y el volumen en la nube crece ~*N* veces.

**Tabla 4. Estimación de orden de magnitud**

| Escenario | Nube | USD/mes |
|---|---|---:|
| 1 laboratorio (hoy) | 1 VM B2as_v2 | ~63 |
| 5 laboratorios | 1 VM D4as_v5 (4 vCPU / 16 GB) | ~150 |
| 20 laboratorios | Kafka de 3 brokers + Spark en 2-3 nodos (5 VMs D4as_v5), o servicios gestionados (Event Hubs + Databricks) | ~600-900 |

**Por qué streaming cuesta distinto que batch:** el batch paga por corrida y se
puede programar de noche; el streaming paga 24/7 y se dimensiona por el
**pico** (horario de clases), no por el promedio. Una optimización posible es
concentrar el reentrenamiento de noche.
