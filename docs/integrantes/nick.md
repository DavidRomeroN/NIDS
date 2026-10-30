# Nick Saim Mayta Jara — Conexiones (flujos)

**Énfasis en el equipo:** ingesta en Kafka y evento empresarial (incidentes).

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Cuántas conexiones se abren, hacia qué servicios y cuánto duran? | ¿Se está disparando la cantidad de conexiones ahora? |
| Indicador | flujos por hora, top puertos/servicios, duración (p50/p90/p99) | flujos nuevos por minuto |
| Instrumento | Silver/Gold (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | flujos de la próxima hora | flujos del minuto siguiente (RandomForest) |
| Dónde se ve | [Notebook 02](../notebooks/02_nick_conexiones.ipynb) | Grafana, fila "Dimensión 2 (Nick Mayta)" |

## Resultado del modelo en vivo (test)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 10,32 | 6,76 | −0,24 |
| LinearRegression | 6,67 | 3,61 | 0,48 |
| **RandomForest (profundidad 10) — ganador** | **5,60** | **2,93** | **0,63** |
| GBT | 6,29 | 3,39 | 0,54 |

**+45,8 % de RMSE frente a la persistencia**: el indicador más predecible del proyecto.

## El evento empresarial: `incidentes-seguridad`

- **Contrato v1.1** (ver [Ingesta](../ingesta.md)): `incidenteId` como key, para
  que apertura y cierre del mismo ticket caigan en la misma partición y en orden.
- **Dos productores:** la mesa de TI (`productor_incidentes.py`, con el ritmo
  de un horario de clases) y **el propio modelo** (Spark, `reportadoPor:
  nids-modelo`).
- **Procesamiento:** tickets abiertos por severidad y origen, MTTR de los
  últimos 50, y ventana **deslizante** de 5 min cada 1 min.

## Qué muestro en mi demo (3 min)

1. Publico un incidente en vivo y lo sigo por todo el pipeline:
   ```bash
   docker compose exec incidentes python productor_incidentes.py --uno --severidad critica --categoria escaneo_red
   docker compose logs --tail 3 spark-streaming | grep incidentes
   ```
   Se ve el offset/partición en el log del productor, el `batch_procesado` en
   Spark y el panel "Incidentes abiertos" sube a los pocos segundos.
2. Kafka UI (túnel SSH): los 3 tópicos, sus particiones y mensajes.
3. Grafana "Dimensión 2": flujos reales vs predicción vs persistencia.
4. Notebook 02: servicios más usados y duración de las conexiones.

## Mi decisión propia

Que el **modelo genere el evento de negocio**: un incidente automático con
evidencia legible y un limitador de 10 minutos por motivo, para que una ráfaga
de anomalías sea un ticket y no cincuenta.

## Preguntas que me pueden hacer

- **¿Diferencia entre el contrato empresarial y el del sensor?** El incidente es
  poco frecuente, de alto valor y cambia de estado (abierto → cerrado): key =
  `incidenteId`. La telemetría es frecuente (5 s) y de bajo valor individual:
  key = `sensorId`, para mantener el orden por dispositivo.
- **¿Qué es un consumer group y un offset en tu pipeline?** Spark lleva sus
  propios offsets en el checkpoint de cada consulta; por eso al reiniciar sigue
  donde quedó sin perder ni repetir lo confirmado.
- **¿Por qué 3 particiones?** Permite paralelizar la lectura y mantiene el
  orden por key; para el volumen de un laboratorio sobra.
