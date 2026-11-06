# David Romero Nina — Volumen de tráfico

**Rol en el equipo:** BI/ML, sensor (Raspberry Pi) y despliegue en la nube.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Cuánto tráfico mueve el laboratorio, por qué servicios y en qué horario? ¿Cuánto moverá la próxima hora? | ¿Cuánto tráfico hay ahora y cuánto habrá el minuto siguiente? |
| Indicador | bytes por hora / día, MB por servicio, % hacia internet | bytes por minuto |
| Instrumento | Gold `gold_minuto` + Silver `silver_flujos` (TRCU.csv) | Kafka `trafico-flujos` → Spark (ventana 1 min) |
| Predicción | total de la próxima hora (LR / RF vs persistencia) | bytes del minuto siguiente (GBT log vs persistencia) |
| Dónde se ve | [Notebook 01](../notebooks/01_david_volumen_trafico.ipynb) | Grafana, fila "Dimensión 1 (David Romero)" |

## Resultado del modelo en vivo (test del histórico, 6-nov-2026)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 146 021 | 23 982 | −1,04 |
| LinearRegression (log) | 101 750 | 12 870 | 0,01 |
| RandomForest (log) | 101 422 | 12 225 | 0,02 |
| **GBT (log, profundidad 3) — ganador** | **99 893** | **12 560** | **0,05** |

**+31,6 % de RMSE y −48 % de MAE frente a la persistencia.** En la corrida del PPT
fue +16,2 %: quitar 2 filas de prueba movió mucho el RMSE, porque es muy sensible
a los picos. En los últimos dos días del Pi, el mismo modelo mejora +21,2 %. El
candidato reentrenado el 6-nov no superó la compuerta, así que este sigue en producción.

## Qué muestro en mi demo (3 min)

1. **Grafana → "Dimensión 1":** el bytes/min real, la predicción del modelo,
   la persistencia y el umbral de anomalía, actualizándose.
2. Explico cuándo se abre un incidente: si el valor real supera predicción +
   3·RMSE, aparece la anomalía y un incidente en "Incidentes abiertos por origen →
   nids-modelo". Con el Pi en un puerto normal, la descarga de otra PC no se ve:
   para provocarlo hay que generar el tráfico desde el propio Pi.
3. **Notebook 01:** perfil por hora del día (clase vs fin de semana) y MB por
   servicio, que explican por qué el modelo usa hora y rezagos.
4. **Contraste U1 vs U2:** en U1 predigo la próxima hora desde el histórico (foto
   fija); en U2 el modelo predice cada minuto con lo que acaba de llegar.

## Mi decisión propia

Usar **escala logarítmica como hiperparámetro**. El primer GBT en escala
original quedó 66 % peor que la persistencia en test: perseguía los picos de
MB. Con validación cruzada temporal y la opción log, el ganador le ganó a la persistencia.

## Preguntas que me pueden hacer

- **¿Por qué el R² es tan bajo si le ganas a la persistencia?** Porque los picos
  de descarga son casi impredecibles un minuto antes; el R² castiga mucho esos
  picos. El MAE (error típico) baja 48 %: el modelo acierta mucho mejor el
  comportamiento normal, que es lo que define el umbral de anomalía.
- **¿Por qué el Pi no procesa nada?** Porque captura es lo único sensible al
  tiempo real: si el análisis se cuelga, la captura sigue. Y 4 núcleos ARM no
  sostienen captura + Spark.
- **¿Cuánto cuesta?** Ver [costos](../costos.md): ~63 USD/mes en Azure; en el
  crédito de estudiante, S/ 0.
