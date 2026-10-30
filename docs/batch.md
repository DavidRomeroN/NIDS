# Capa batch: Data Lake y ETL del histórico

## 1. Fuente

`TRCU.csv`: **397 354 flujos** capturados en la universidad entre el 1 de julio
y el 1 de septiembre de 2026 (43 días con datos, 34 751 minutos), extraídos con
NTLFlowLyzer. 83 columnas por flujo: 5-tupla, tiempos, conteos y largos de
paquetes por sentido, tiempos entre llegadas, flags TCP, ventanas TCP. Todas
las filas tienen `label = NeedLabel`: **no hay etiquetas**.

El CSV **no se versiona** (tiene IP reales): vive en la VM en
`/srv/nids/historico/TRCU.csv` y se monta de solo lectura en el contenedor.

## 2. Pipeline batch

Hay dos caminos sobre el mismo dato, con la misma lógica:

| Camino | Dónde | Para qué |
|---|---|---|
| `preparar_historico.py` | job de Spark en la VM | Alimenta el entrenamiento de producción (`historico_minuto`, `historico_flujos`) |
| `00_etl_batch_trcu.ipynb` | notebook PySpark | Muestra paso a paso la calidad, la limpieza y el particionado (evidencia) |

**Tabla 1. Capas del Data Lake**

| Capa | Ruta | Contenido | Partición |
|---|---|---|---|
| Bronze | `/srv/nids/historico/TRCU.csv` | CSV crudo de NTLFlowLyzer | — |
| Silver | `lake/historico_flujos`, `lake_notebooks/silver_flujos` | un flujo por fila, tipado, IP seudonimizadas o eliminadas | `fecha` |
| Gold | `lake/historico_minuto`, `lake/trafico_minuto` | un minuto por fila con los 10 indicadores | `fecha` |
| Gold (eventos) | `lake/anomalias_flujo`, `lake/incidentes`, `lake/pi_telemetria_minuto` | salidas del streaming | `fecha` |

## 3. Calidad

Validaciones del notebook 00 sobre las 397 354 filas:

- nulos en columnas clave;
- duplicados por `(flow_id, timestamp)`;
- contadores y duraciones negativas;
- rango temporal;
- distribución de la etiqueta (`NeedLabel` en el 100 %, lo que define el
  enfoque no supervisado).

`preparar_historico.py` reporta en su log las filas válidas y descartadas;
en la corrida real: **397 354 válidas, 0 descartadas**.

## 4. Formato analítico

Parquet particionado por `fecha`. Filtrar por un día lee solo esa carpeta: el
plan físico muestra `PartitionFilters: [isnotnull(fecha), (fecha = 2026-08-11)]`
(ver el notebook 00).

## 5. Notebooks

| Notebook | Autor | Contenido |
|---|---|---|
| [00 ETL batch](notebooks/00_etl_batch_trcu.ipynb) | Equipo | Calidad, limpieza, seudonimización, Silver/Gold, partition pruning |
| [01 Volumen](notebooks/01_david_volumen_trafico.ipynb) | David Romero | Bytes por servicio y horario; modelo de la próxima hora |
| [02 Conexiones](notebooks/02_nick_conexiones.ipynb) | Nick Mayta | Flujos por servicio, duración; modelo de la próxima hora |
| [03 SYN](notebooks/03_henyelrey_intentos_syn.ipynb) | Henyelrey Garcia | SYN sin respuesta, heurística de escaneo; modelo |
| [04 DNS](notebooks/04_jhan_consultas_dns.ipynb) | Jhan Ramos | DNS por host, tamaño de pregunta; modelo |

Se ejecutan en la VM, con el mismo contenedor de Spark del pipeline:

```bash
docker compose run --rm -v "$PWD/../notebooks:/notebooks" spark-entrenamiento \
  bash -c "pip install -q jupytext nbconvert ipykernel matplotlib pandas && \
           cd /notebooks && jupytext --to ipynb --execute 00_etl_batch_trcu.py"
```
