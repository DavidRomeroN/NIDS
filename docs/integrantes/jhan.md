# Jhan Logan Ramos Quispe — Consultas DNS

**Énfasis en el equipo:** observabilidad y costos.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Quién consulta DNS, cuánto y con qué tamaño de pregunta? | ¿Hay una ráfaga de DNS anómala (beaconing, túnel DNS)? |
| Indicador | flujos DNS por hora y por host, tamaño de pregunta (p99) | flujos DNS por minuto |
| Instrumento | Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | DNS de la próxima hora | DNS del minuto siguiente (RandomForest log) |
| Dónde se ve | [Notebook 04](../notebooks/04_jhan_consultas_dns.ipynb) | Grafana, fila "Dimensión 4 (Jhan Ramos)" |

## Resultado del modelo en vivo (test)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 0,734 | 0,170 | −0,23 |
| LinearRegression (log) | 0,595 | 0,148 | 0,19 |
| **RandomForest (log, profundidad 6) — ganador por validación** | **0,604** | **0,144** | **0,17** |
| GBT (log) | 0,593 | 0,143 | 0,20 |

**+17,6 % de RMSE frente a la persistencia.** La validación eligió RF; en test
GBT quedó apenas mejor. No se cambia el ganador mirando el test: eso sería
contaminar la evaluación.

## Observabilidad (mi énfasis)

| Señal | Panel | Qué indica |
|---|---|---|
| Lag Kafka → Spark | "Lag Kafka -> Spark" | Primera alarma de backpressure: Kafka ya tiene datos que Spark no procesó |
| Duración del micro-batch | "Duración de micro-batch" | Si supera el trigger (20 s), el pipeline no da abasto |
| Latencia extremo a extremo | "Latencia extremo a extremo" | Desde que termina la ventana hasta Grafana |
| Último archivo del sensor | "Segundos desde el último archivo" | Verde < 2 min, rojo > 5 min: el Pi se cayó o perdió red |
| Interfaz espejo vs pipeline | "Interfaz espejo vs pipeline" | Si divergen, se están perdiendo datos entre el Pi y Spark |
| CPU / RAM de la VM | "Uso de la VM" | Base de la estimación de costos y del escalado |

## Qué muestro en mi demo (3 min)

1. Grafana "Dimensión 4": DNS real vs predicción vs persistencia.
2. Fila de observabilidad: lag en 0, duración de batch y latencia; explico qué
   alarma sonaría primero ante backpressure.
3. [Costos](../costos.md): cuánto cuesta hoy y cuánto costaría escalar a todo el campus.
4. Notebook 04: hosts que más consultan DNS y el p99 del tamaño de pregunta.

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
