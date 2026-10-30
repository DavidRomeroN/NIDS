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

## Resultado del modelo en vivo (test, último 20 % del tiempo)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 148 264 | 24 340 | −0,33 |
| LinearRegression (log) | 126 100 | 14 319 | 0,03 |
| RandomForest (log) | 126 945 | 13 735 | 0,02 |
| **GBT (log, profundidad 3) — ganador** | **124 294** | **13 997** | **0,06** |

**+16,2 % de RMSE y −43 % de MAE frente a la persistencia.**

## Qué muestro en mi demo (3 min)

1. **Grafana → "Dimensión 1":** el bytes/min real, la predicción del modelo,
   la persistencia y el umbral de anomalía, actualizándose.
2. Abro un video o una descarga en el laboratorio y muestro cómo sube la curva
   real: si supera predicción + 3·RMSE, aparece la anomalía y un incidente
   automático en "Incidentes abiertos por origen → nids-modelo".
3. **Notebook 01:** perfil por hora del día (clase vs fin de semana) y MB por
   servicio, que explican por qué el modelo usa hora y rezagos.
4. **Contraste U1 vs U2:** en U1 predigo la próxima hora desde el histórico (foto
   fija); en U2 el modelo predice cada minuto con lo que acaba de llegar.

## Mi decisión propia

Usar **escala logarítmica como hiperparámetro**. El primer GBT en escala
original quedó 66 % peor que la persistencia en test: perseguía los picos de
MB. Con validación cruzada temporal y la opción log, el ganador quedó 16 % mejor.

## Preguntas que me pueden hacer

- **¿Por qué el R² es tan bajo si le ganas a la persistencia?** Porque los picos
  de descarga son casi impredecibles un minuto antes; el R² castiga mucho esos
  picos. El MAE (error típico) baja 43 %: el modelo acierta mucho mejor el
  comportamiento normal, que es lo que define el umbral de anomalía.
- **¿Por qué el Pi no procesa nada?** Porque captura es lo único sensible al
  tiempo real: si el análisis se cuelga, la captura sigue. Y 4 núcleos ARM no
  sostienen captura + Spark.
- **¿Cuánto cuesta?** Ver [costos](../costos.md): ~63 USD/mes en Azure; en el
  crédito de estudiante, S/ 0.
