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
2. **Evento empresarial en vivo** (Jhan, streaming y Kafka):
   ```bash
   docker compose exec incidentes python productor_incidentes.py --uno --severidad critica --categoria escaneo_red
   ```
3. **Evento IoT en vivo** (David, sensor): `docker compose logs -f bridge` (telemetría del Pi cada 5 s).
4. **Ventana + watermark + checkpoint en logs** (Jhan):
   ```bash
   docker compose logs spark-streaming | grep observabilidad | tail -5
   ```
5. **Arquitectura, observabilidad y costos** (Nick): filas de observabilidad y [costos](costos.md).
6. **Capa batch** (Henyelrey): notebook 00, calidad del histórico, Parquet particionado y *partition pruning*.
7. **Modelos validados y BI con predicción junto a KPIs** (David): fila "Modelos", Resumen y Dimensión 1.
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
8. **¿Por qué la inferencia guarda historial si podría ser sin estado?** Sin
   rezagos el modelo perdía contra la persistencia (66 % peor en bytes). Con
   5 filas por sensor en memoria le gana en los 4 indicadores, y esas filas se
   reconstruyen del Data Lake al reiniciar ([Modelos](modelos.md#11-dataset-supervisado)).
9. **¿Cómo predice si el tráfico real cambia?** Cada 6 h se reentrena con el
   histórico más lo que llegó en vivo; si un modelo deja de ganarle a la
   persistencia, el tablero lo muestra (mejora negativa).

## 5. Plan B: el Pi no tiene internet en el laboratorio

`eth0` está en el puerto espejo, que solo escucha, así que la subida a la nube
va por WiFi. El Pi ya guarda la red del hotspot del celular (`pi/bin/nids-wifi.sh`).

1. **Primero:** encender el hotspot del celular con el mismo nombre y la misma
   clave. En 1 minuto el panel "Segundos desde el último archivo" vuelve a verde.
2. **Si aun así no conecta:** encender el reproductor del histórico en la VM:
   ```bash
   docker compose --profile respaldo up -d reproductor
   ```
   Reproduce en tiempo real el tráfico de TRCU.csv del mismo día de la semana
   y la misma hora, como sensor `replay-trcu`, por el mismo tópico y contrato.
   Hay que **decirlo al jurado**: es tráfico real de la U, pero de julio.
   El reentrenamiento lo excluye. Apagarlo al terminar:
   ```bash
   docker compose --profile respaldo stop reproductor
   ```

## 6. Antes de presentar (checklist)

- [ ] Grafana abre (http://&lt;IP_VM&gt;:3000) y el panel "Segundos desde el último archivo" está en verde.
- [ ] `docker compose ps`: todo `Up` (kafka-topicos aparece como `Exited (0)`, es normal).
- [ ] Hotspot del celular encendido si el Pi no tiene otra salida a internet.
- [ ] Túnel SSH listo para Kafka UI si lo piden.
- [ ] Cada integrante abrió su notebook y su página.
