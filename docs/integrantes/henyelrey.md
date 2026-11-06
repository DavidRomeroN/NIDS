# Henyelrey Lucio Garcia Chura — Intentos de conexión (SYN)

**Rol en el equipo:** batch con Spark: ETL del histórico, calidad y Data Lake.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Qué intentos de conexión quedan sin respuesta y quién escanea puertos? | ¿Hay un pico de intentos de conexión (posible escaneo) ahora? |
| Indicador | SYN por hora, % de SYN sin respuesta, puertos distintos por origen y minuto | SYN por minuto |
| Instrumento | Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | SYN de la próxima hora | SYN del minuto siguiente (regresión lineal) |
| Dónde se ve | [Notebook 03](../notebooks/03_henyelrey_intentos_syn.ipynb) | Grafana, fila "Dimensión 3 (Henyelrey Garcia)" |

## Resultado del modelo en vivo (test con datos del Pi, 6-nov-2026)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 2,81 | 1,18 | −0,16 |
| **LinearRegression, histórico + Pi — ganador** | **2,14** | **0,98** | **0,33** |
| RandomForest | 2,24 | 0,98 | 0,27 |
| GBT | 2,28 | 0,93 | 0,24 |

**+24,1 % de RMSE frente a la persistencia.** El +11,6 % del PPT se midió solo con el
histórico, así que las dos cifras no se comparan. El conteo de SYN también incluye
respuestas de servidores: un pico es una señal para revisar, no un escaneo confirmado.

## Qué muestro en mi demo (3 min)

1. Grafana "Dimensión 3": SYN reales vs predicción y el umbral.
2. Tabla "Flujos más sospechosos" del K-Means: un escaneo (muchos SYN a puertos
   distintos sin respuesta) cae lejos de cualquier cluster normal.
3. Si el SYN real supera predicción + 3·RMSE, Spark abre un incidente
   **alta / escaneo_red** solo.
4. Notebook 03: % de SYN sin respuesta y la heurística de escaneo (orígenes
   que tocan muchos puertos en un mismo minuto).
5. Capa batch, mi rol ([notebook 00](../notebooks/00_etl_batch_trcu.ipynb)):
   calidad del histórico (0 nulos, 0 duplicados, 0 negativos), Silver y Gold
   en Parquet particionado por fecha, y el `PartitionFilters` del *partition pruning*.

## Mi decisión propia

La **validación cruzada temporal** (3 cortes rolling-origin) en vez de un solo
bloque de validación: con un solo bloque, el primer modelo de bytes elegía un
GBT que en test quedaba 66 % peor que la persistencia.

## Preguntas que me pueden hacer

- **¿Por qué comparar primero contra la persistencia?** Porque en tráfico de red
  el minuto siguiente se parece mucho al actual; si un modelo no le gana a
  "repetir el último valor", su complejidad no se justifica. En SYN, dos de
  tres algoritmos NO le ganan.
- **¿Por qué la división es cronológica y no aleatoria?** Con `randomSplit` el
  modelo vería minutos del futuro al entrenar (fuga de información) y las
  métricas serían optimistas e irreales.
- **¿Para qué particionar por fecha?** Para que una consulta de un día lea
  solo esa carpeta y no los 43 días: el plan físico lo muestra en `PartitionFilters`.
- **¿Qué pasa si el modelo deja de servir?** Se reentrena cada 6 h y solo se
  reemplaza si gana en el mismo test; el panel "Error absoluto en vivo" muestra si el modelo empieza a
  equivocarse más que la persistencia en producción.
