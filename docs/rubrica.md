# Rúbrica y evidencias

Cada criterio de evaluación del curso, con **dónde se verifica** en el sistema.
Todo es verificable en vivo (logs, tablero, Kafka) o en el repositorio.

## Unidad 2 — Pipeline en tiempo real

| # | Criterio | Evidencia verificable | Dónde |
|---|---|---|---|
| 1 | Ingesta de eventos **empresariales** con Kafka | Tópico `incidentes-seguridad`, contrato v1.1 versionado, dos productores (mesa de TI y modelo); publicación manual en vivo con offset/partición en el log | [Ingesta](ingesta.md), `productor_incidentes.py`, Grafana "Evento empresarial" |
| 2 | Ingesta **IoT/sensores** | Raspberry Pi real publicando telemetría por **MQTT → puente → Kafka** (`pi-telemetria`) cada 5 s, con esquema y validación de rango físico; más el sensor de red (`trafico-flujos`) | [Sensor](sensor.md), `bridge_mqtt_kafka.py`, Grafana "Sensor IoT" |
| 3 | Streaming con **ventanas, watermarking y checkpointing** | 5 consultas; ventana fija 1 min (watermark 4 min) y **deslizante** 5/1 min (watermark 2 min); checkpoint por consulta; líneas `observabilidad` del log con `watermark` y `checkpoint` | [Streaming](streaming.md), `docker compose logs spark-streaming` |
| 4 | **Observabilidad** con métricas correlacionadas | Lag Kafka→Spark, duración de batch, latencia extremo a extremo, último archivo del sensor, interfaz espejo vs pipeline, CPU/RAM de la VM | [Observabilidad](observabilidad.md), Grafana |
| 5 | **Costos** y criterios de escalado | Costo real en Azure, carga medida, umbrales de escalado, escenario de campus completo | [Costos](costos.md) |
| 6 | Salidas **BI/ML** con KPIs en vivo | Tablero único con KPIs de negocio (Mbps, hosts, % cifrado, incidentes, MTTR) junto a la predicción | [Tablero](dashboard.md) |
| 7 | Serie de tiempo **validada** e integrada al tablero | 4 modelos con validación cruzada temporal, test ciego del último 20 %, comparación contra persistencia (+11,6 % a +45,8 %), error medido en vivo | [Modelos](modelos.md), notebooks 01-04 |
| 8 | Sustentación individual | Una página por integrante con su dimensión U1/U2, su demo y sus respuestas | [Integrantes](integrantes/david.md), [Guion](sustentacion.md) |

## Unidad 3 — Sistema integrado (producto final)

| # | Criterio | Evidencia verificable | Dónde |
|---|---|---|---|
| 1 | Integración **batch + streaming + BI/ML** | Una sola función de agregación para ambas capas; el batch entrena, el streaming aplica y reentrena con lo nuevo; un solo tablero | [Arquitectura](arquitectura.md) |
| 2 | Prácticas **DataOps/DevOps** | Docker Compose con versiones fijas, scripts idempotentes, tablero como código, docs publicadas por GitHub Actions, secretos fuera del repo | [Despliegue](despliegue.md), `.github/workflows/docs.yml` |
| 3 | **Documentación** operativa completa | Este sitio: arquitectura, cada componente, contratos, despliegue paso a paso, verificación | todo el sitio |
| 4 | **Hardening** y revisión técnica | Checklist de 10 controles y tabla de fallos encontrados y corregidos | [Seguridad](seguridad.md) |
| 5 | **Demo reproducible** | `desplegar.sh` + `instalar.sh` levantan todo desde cero; verificación con comandos | [Despliegue](despliegue.md) |
| 6 | Sustentación con evidencias y **valor para la decisión** | Incidentes automáticos con evidencia legible: el sistema le dice a la mesa de TI qué revisar y por qué | [Modelos §3](modelos.md#3-cierre-del-ciclo-del-modelo-al-negocio) |

## Capa batch (Unidad 1, reforzada)

| Criterio | Evidencia |
|---|---|
| Pipeline batch con Spark (extracción, transformación, agregación) | Notebook 00 y `preparar_historico.py` |
| Validación de calidad | Nulos, duplicados, negativos, rango temporal, etiquetas (notebook 00) |
| Salida en Parquet particionado | `silver_flujos`, `gold_minuto` por `fecha`, con *partition pruning* demostrado |
| Modelo predictivo batch | Predicción de la próxima hora por integrante (notebooks 01-04) |
