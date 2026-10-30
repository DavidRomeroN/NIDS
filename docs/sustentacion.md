# Guion de la sustentación

**Pregunta central:** ¿cómo detectar a tiempo comportamientos anómalos en el
tráfico de red del laboratorio para que la mesa de TI reaccione antes de que se
conviertan en incidentes?

## 1. Distribución del tiempo

| Momento | Tiempo | Quién | Qué mostrar |
|---|---|---|---|
| Alcance | 2-3 min | Grupal | El problema, la pregunta central, para quién ([Brief](brief.md)) |
| Arquitectura | 2-3 min | Grupal | Diagrama Lambda: Pi → nube → Kafka → Spark → Grafana ([Arquitectura](arquitectura.md)) |
| Asignación | 2-3 min | Grupal | Las 4 dimensiones (tabla del [Brief](brief.md#3-dimensiones-de-analisis)) |
| Demo individual | 3 min c/u | David → Nick → Henyelrey → Jhan | Cada uno su fila de Grafana + su notebook ([páginas de integrantes](integrantes/david.md)) |
| Preguntas | 3 min c/u | Individual | Ver abajo |

## 2. Secuencia de la demo

1. **Evolución:** del histórico TRCU.csv (batch, U1) al sensor en vivo (U2).
2. **Evento empresarial en vivo** (Nick):
   ```bash
   docker compose exec incidentes python productor_incidentes.py --uno --severidad critica --categoria escaneo_red
   ```
3. **Evento IoT en vivo:** `docker compose logs -f bridge` (telemetría del Pi cada 5 s).
4. **Ventana + watermark + checkpoint en logs:**
   ```bash
   docker compose logs spark-streaming | grep observabilidad | tail -5
   ```
5. **Observabilidad y costos** (Jhan): filas de observabilidad y [costos](costos.md).
6. **Modelos validados** (Henyelrey): fila "Modelos" del tablero y [Modelos](modelos.md).
7. **BI con predicción junto a KPIs** (David): Resumen + Dimensión 1.
8. **Decisión propia del equipo:** el modelo abre incidentes solo, con
   evidencia legible y limitador de 10 min; y la escala log + validación
   cruzada temporal que corrigió un modelo que en test era 66 % peor que la persistencia.

## 3. Prueba en vivo de una anomalía

Desde una PC del laboratorio:

- **Volumen (David):** descargar un archivo grande o reproducir video en alta
  calidad → sube bytes/min sobre el umbral.
- **Conexiones/SYN (Nick, Henyelrey):** abrir muchas pestañas a sitios
  distintos a la vez → sube flujos y SYN.
- **DNS (Jhan):** `for /L %i in (1,1,200) do nslookup sitio%i.example.com`
  en una consola de Windows → ráfaga de DNS.

En 1-5 minutos (bloque de 30 s + cierre del flujo + watermark) se ve la curva
real sobre el umbral, la anomalía y el incidente `nids-modelo` en Grafana.

## 4. Preguntas de referencia y respuestas

1. **Contrato empresarial vs IoT y por qué importa en el tópico.** El incidente
   es poco frecuente, de alto valor y cambia de estado: key `incidenteId` para
   que abrir y cerrar caigan en orden en la misma partición. La telemetría es
   frecuente y de bajo valor individual: key `sensorId`.
2. **Evento tardío y watermarking.** Un flujo llega hasta ~3 min tarde (bloque
   de 30 s + cierre del flujo). El watermark de 4 min lo espera; después se
   descarta (queda igual en el disco del Pi). Sin watermark el estado crecería
   sin límite.
3. **Primera alarma de backpressure.** `nids_spark_lag_kafka_registros`: mide lo
   que Kafka ya tiene y Spark no procesó. La duración del batch avisa después.
4. **Costo streaming vs batch.** Batch paga por corrida y puede ir de noche;
   streaming paga 24/7 y se dimensiona por el pico.
5. **Predicción junto a KPIs.** La mesa de TI ve en un panel cuánto hay, cuánto
   se esperaba y si se pasó del umbral; separado, tendría que cruzarlo a mano.
6. **Baseline de persistencia.** En tráfico, el minuto siguiente se parece al
   actual; si el modelo no le gana, su complejidad no se justifica. En SYN, LR y
   RF NO le ganan; solo GBT.
7. **KPI vs métrica técnica, y Pushgateway.** "% cifrado" o "incidentes abiertos"
   son de negocio; "duración del batch" es técnica. Pushgateway es un atajo
   porque Spark Streaming en Python no expone un endpoint para Prometheus; lo
   recomendado es el PrometheusServlet de Spark o un exporter.

## 5. Antes de presentar (checklist)

- [ ] Grafana abre (http://&lt;IP_VM&gt;:3000) y el panel "Segundos desde el último archivo" está en verde.
- [ ] `docker compose ps`: todo `Up` (kafka-topicos aparece como `Exited (0)`, es normal).
- [ ] Túnel SSH listo para Kafka UI si lo piden.
- [ ] Cada integrante abrió su notebook y su página.
