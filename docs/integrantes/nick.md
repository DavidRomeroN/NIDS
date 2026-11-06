# Nick Saim Mayta Jara — Conexiones (flujos)

**Rol en el equipo:** arquitectura Lambda y observabilidad general; coordinación técnica.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Cuántas conexiones se abren, hacia qué servicios y cuánto duran? | ¿Se está disparando la cantidad de conexiones ahora? |
| Indicador | flujos por hora, top puertos/servicios, duración (p50/p90/p99) | flujos nuevos por minuto |
| Instrumento | Silver/Gold (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | flujos de la próxima hora | flujos del minuto siguiente (regresión lineal) |
| Dónde se ve | [Notebook 02](../notebooks/02_nick_conexiones.ipynb) | Grafana, fila "Dimensión 2 (Nick Mayta)" |

## Resultado del modelo en vivo (test con datos del Pi, 6-nov-2026)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 14,15 | 8,80 | 0,53 |
| **LinearRegression, solo Pi — ganador** | **12,07** | **7,58** | **0,66** |
| RandomForest | 12,30 | 7,78 | 0,64 |
| GBT | 12,67 | 7,79 | 0,62 |

**+14,7 % de RMSE frente a la persistencia.** El +45,8 % del PPT se midió solo con el
histórico; con datos del Pi, ese modelo quedaba 30,5 % peor que la persistencia.

## Arquitectura y observabilidad (mi rol)

La arquitectura es Lambda: una sola función, `agregar_por_minuto()`, que usan
la capa batch y la de velocidad ([Arquitectura](../arquitectura.md)). Para
saber si el pipeline está sano, estas son las señales del tablero:

| Señal | Panel | Qué indica |
|---|---|---|
| Lag Kafka → Spark | "Lag Kafka -> Spark" | Primera alarma de backpressure: Kafka ya tiene datos que Spark no procesó |
| Duración del micro-batch | "Duración de micro-batch" | Si supera el trigger (20 s), el pipeline no da abasto |
| Latencia extremo a extremo | "Latencia extremo a extremo" | Desde que termina la ventana hasta que llega a Grafana |
| Último archivo del sensor | "Segundos desde el último archivo" | Verde < 2 min, rojo > 5 min: el Pi se cayó o perdió la red |
| Interfaz espejo vs pipeline | "Interfaz espejo vs pipeline" | Si divergen, se pierden datos entre el Pi y Spark |
| CPU / RAM de la VM | "Uso de la VM" | Base de la estimación de costos y del escalado |

## Qué muestro en mi demo (3 min)

1. Grafana "Dimensión 2": flujos reales vs predicción vs persistencia.
2. Fila de observabilidad: lag en 0, duración de batch y latencia; explico qué
   alarma sonaría primero ante backpressure.
3. [Costos](../costos.md): cuánto cuesta hoy y cuánto costaría escalar a todo el campus.
4. Notebook 02: servicios más usados y duración de las conexiones.

## Mi decisión propia

Medir la **salud del sensor** como parte de la observabilidad: si el Pi deja de
enviar, el pipeline "se ve sano" (lag 0) pero está ciego. El panel "Segundos
desde el último archivo" detecta eso.

## Preguntas que me pueden hacer

- **¿Qué métrica te avisa primero de backpressure?** El lag de Kafka hacia Spark:
  mide directamente lo pendiente. La duración del batch avisa después.
- **¿Por qué Pushgateway?** Spark Streaming en Python no expone un endpoint que
  Prometheus pueda leer; Pushgateway es un atajo pragmático. Lo recomendado
  sería el PrometheusServlet de Spark o un exporter propio.
- **¿Costo streaming vs batch?** El batch paga por corrida y puede ir de noche;
  el streaming paga 24/7 y se dimensiona por el pico, no por el promedio.
- **¿Por qué Lambda y no Kappa?** Reentrenar sobre meses de historia es batch
  pesado (cada 6 h), mientras la inferencia tiene que tardar segundos.
