# Procesamiento streaming con Spark Structured Streaming

Código: `nube/spark/jobs/streaming_nids.py` (+ `comun.py`). Un solo proceso de
Spark 4.2 corre **5 consultas** independientes, cada una con su propio
checkpoint, leyendo de Kafka con `startingOffsets=earliest` y `failOnDataLoss=false`.

**Tabla 1. Consultas en streaming**

| Consulta | Tópico | Operación | Ventana | Watermark | Salida |
|---|---|---|---|---|---|
| `trafico_ventanas_1min` | `trafico-flujos` | `agregar_por_minuto()` + rezagos + 4 modelos | fija 1 min | 4 min | Parquet `trafico_minuto` + métricas + incidentes |
| `flujos_anomalias` | `trafico-flujos` | K-Means por flujo (sin agregación) | — | — | Parquet `anomalias_flujo` + tabla de sospechosos + incidentes |
| `telemetria_ventanas_1min` | `pi-telemetria` | salud del sensor | fija 1 min | 1 min | Parquet `pi_telemetria_minuto` + métricas |
| `incidentes_eventos` | `incidentes-seguridad` | estado de tickets, MTTR | — | — | Parquet `incidentes` + KPIs |
| `incidentes_deslizante_5min` | `incidentes-seguridad` | conteo por severidad | **deslizante 5 min cada 1 min** | 2 min | métricas |

Todas usan `outputMode("append")` con `foreachBatch` y `trigger(processingTime="20 seconds")`.

## 1. Ventanas

- **Fija (tumbling) de 1 minuto** para el tráfico: cada flujo cuenta en el
  minuto en que empezó, igual que en el histórico. Es la unidad de la serie de tiempo.
- **Deslizante de 5 minutos cada 1 minuto** para incidentes: "¿cuántos tickets
  se abrieron en los últimos 5 minutos?", actualizado cada minuto.

## 2. Watermarking: por qué 4 minutos

Un flujo llega a Kafka tarde por diseño:

1. el PCAP se cierra cada **30 s**;
2. el flujo se publica cuando **termina** (FIN/RST, 30 s sin paquetes, o 2 min);
3. la subida y el ingestor suman unos segundos.

El peor caso es ~2,5-3 min de atraso respecto al inicio del flujo. Un watermark
de **4 min** espera ese atraso; un flujo que llega después se descarta (y queda
igual en el disco del Pi y en el Parquet de la capa batch). Sin watermark, Spark
guardaría el estado de todas las ventanas abiertas para siempre y la memoria
crecería sin límite.

## 3. Checkpointing y semántica de entrega

- Cada consulta guarda offsets y estado en `/data/checkpoints/<consulta>`: si el
  contenedor se reinicia, sigue desde donde quedó, sin perder ni repetir lo ya
  confirmado.
- `foreachBatch` es **al menos una vez**: un micro-batch reintentado puede
  escribir dos veces la misma ventana en Parquet. Por eso el entrenamiento
  deduplica por `(sensorId, ventana_inicio)` antes de usar el Data Lake.
- Los incidentes automáticos tienen además un limitador de 10 minutos por
  motivo: una ráfaga de anomalías es un solo ticket, no cincuenta.

## 4. Inferencia en vivo

Por cada ventana de 1 minuto:

1. se calculan los **rezagos** con las 5 filas anteriores del mismo sensor
   (memoria en el driver, precargada desde el Data Lake al reiniciar);
2. cada uno de los 4 `PipelineModel` predice su indicador para el minuto
   siguiente (si el ganador aprendió en escala log, se aplica `expm1`);
3. un minuto después llega el valor real y se publica el **error absoluto en
   vivo** del modelo y de la persistencia, lado a lado;
4. si `real > predicción + 3·RMSE_test`, se marca anomalía y se abre un incidente.

Los modelos se **recargan solos** cuando `entrenar_series.py` deja uno nuevo
(se detecta por la fecha de `metricas.json`), sin reiniciar el streaming.

## 5. Evidencia en logs

```bash
# watermark, batchId, lag de Kafka y checkpoint de cada consulta
docker compose logs spark-streaming | grep observabilidad | tail -5

# ventanas procesadas con los modelos cargados
docker compose logs spark-streaming | grep batch_procesado | tail -5

# incidentes que abrió el modelo
docker compose logs spark-streaming | grep incidente_automatico
```

Ejemplo real de una línea de observabilidad:

```json
{"service": "nids-spark", "componente": "observabilidad", "consulta": "trafico_ventanas_1min",
 "batchId": 42, "filas": 88, "duracionMs": 1834, "watermark": "2026-10-30T05:09:00.000Z",
 "lagKafka": 0, "checkpoint": "/data/checkpoints/trafico_ventanas_1min"}
```
