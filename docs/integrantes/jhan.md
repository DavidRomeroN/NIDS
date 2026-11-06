# Jhan Logan Ramos Quispe — Consultas DNS

**Rol en el equipo:** streaming y Kafka: tópicos, contratos y evento empresarial.

## Mis dos dimensiones

| | U1 — batch | U2 — streaming |
|---|---|---|
| Pregunta | ¿Quién consulta DNS, cuánto y con qué tamaño de pregunta? | ¿Hay una ráfaga de DNS anómala (beaconing, túnel DNS)? |
| Indicador | flujos DNS por hora y por host, tamaño de pregunta (p99) | flujos DNS por minuto |
| Instrumento | Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |
| Predicción | DNS de la próxima hora | DNS del minuto siguiente (regresión lineal) |
| Dónde se ve | [Notebook 04](../notebooks/04_jhan_consultas_dns.ipynb) | Grafana, fila "Dimensión 4 (Jhan Ramos)" |

## Resultado del modelo en vivo (test con datos del Pi, 6-nov-2026)

| Modelo | RMSE | MAE | R² |
|---|---:|---:|---:|
| Persistencia | 2,92 | 1,16 | −0,16 |
| **LinearRegression, histórico + Pi — ganador** | **2,22** | **0,96** | **0,33** |
| RandomForest | 2,27 | 0,93 | 0,30 |
| GBT | 2,46 | 0,92 | 0,18 |

**+23,9 % de RMSE frente a la persistencia.** El +17,6 % del PPT se midió solo con el
histórico, así que las dos cifras no se comparan. La validación eligió el modelo:
no se cambia el ganador mirando el test.

## El evento empresarial: `incidentes-seguridad` (mi rol)

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
3. Grafana "Dimensión 4": DNS real vs predicción vs persistencia.
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
