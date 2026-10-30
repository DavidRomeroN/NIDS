# Jhan Logan Ramos Quispe — Consultas DNS

**Rol en el equipo:** streaming y Kafka: tópicos, contratos y evento empresarial.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Quién consulta DNS, cuánto y con qué tamaño de pregunta? | ¿Hay una ráfaga de DNS anómala (beaconing, túnel DNS)? |
| Indicador | flujos DNS por hora y por host, tamaño de pregunta (p99) | flujos DNS por minuto |
| Instrumento | Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | DNS de la próxima hora | DNS del minuto siguiente (RandomForest log) |
| Dónde se ve | [Notebook 04](../notebooks/04_jhan_consultas_dns.ipynb) | Grafana, fila "Dimensión 4 (Jhan Ramos)" |

> **Nota:** "tamaño de pregunta" es `bytes_fwd` por flujo; un flujo puede
> contener varias consultas. Los flujos DNS sin respuesta (`bytes_bwd = 0`)
> pueden ser un artefacto de la captura y no necesariamente un fallo del servidor.

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

## Cómo se detecta una ráfaga DNS

El modelo predice los flujos DNS del minuto siguiente. Si el valor real supera
la predicción en más de **3 × RMSE** del test, se abre un incidente automático
(`severidad: media`, `categoria: anomalia_trafico`) con la evidencia escrita
(valor real, predicción, umbral y minuto). Como las ventanas usan un watermark
de 4 min, el incidente aparece **unos 4–5 min después** de la ráfaga. El ticket
dice cuándo y cuánto se desvió el tráfico; la causa la investiga TI.

## El evento empresarial: `incidentes-seguridad` (mi rol)

- **Contrato v1.1** (ver [Ingesta](../ingesta.md)): `incidenteId` como key, para
  que apertura y cierre del mismo ticket caigan en la misma partición y en orden.
- **Dos productores:** la mesa de TI (`productor_incidentes.py`, con el ritmo
  de un horario de clases) y **el propio modelo** (Spark, `reportadoPor:
  nids-modelo`).
- **Procesamiento:** tickets abiertos por severidad y origen, MTTR de los
  últimos 50, y ventana **deslizante** de 5 min cada 1 min.

## Qué muestro en mi demo (3 min)

> Para mostrar la detección de DNS, lanzo la ráfaga de `nslookup` al menos
> 6–8 min antes de exponer.

1. Publico un incidente en vivo y lo sigo por todo el pipeline:
   ```bash
   docker compose exec incidentes python productor_incidentes.py --uno --severidad critica --categoria escaneo_red
   docker compose logs --tail 3 spark-streaming | grep incidentes
   ```
   Se ve el offset/partición en el log del productor, el `batch_procesado` en
   Spark y el panel "Incidentes abiertos" sube a los pocos segundos.
2. Kafka UI (túnel SSH): los 3 tópicos, sus particiones y mensajes.
3. Grafana "Dimensión 4": DNS real vs predicción vs persistencia, y el panel de incidentes abiertos por el modelo.
4. Notebook 04: hosts que más consultan DNS y el p99 del tamaño de pregunta.

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
