# %% [markdown]
# # 03 · Henyelrey Lucio Garcia Chura — Intentos de conexión (SYN)
#
# **Pregunta central del proyecto:** ¿cómo detectar a tiempo comportamientos
# anómalos en el tráfico de red del laboratorio para que la mesa de TI reaccione
# antes de que se conviertan en incidentes?
#
# **Mi dimensión:** ¿hay intentos de conexión anómalos (escaneos de puertos,
# conexiones que nadie responde)?
#
# | | U1 (batch, este notebook) | U2 (streaming, Grafana) |
# |---|---|---|
# | Indicador | SYN por hora, SYN sin respuesta por host | SYN por minuto |
# | Predicción | SYN de la próxima hora | SYN del minuto siguiente |
# | Fuente | Data Lake Gold/Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |

# %%
import sys
sys.path.insert(0, "/jobs")
sys.path.insert(0, "/notebooks")

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from analisis_comun import cargar, metricas_streaming, modelo_horario, perfil_horario, picos, resumen
from comun import crear_spark

spark = crear_spark("nb-03-henyelrey")
silver, gold = cargar(spark)
IND = "syn"

# %% [markdown]
# ## 1. Descriptivo

# %%
print(resumen(gold, IND).to_string())
perfil_horario(gold, IND).plot(kind="bar", figsize=(13, 4), title="SYN por minuto, promedio por hora del día")
plt.show()

# %% [markdown]
# ## 2. Diagnóstico: conexiones que nadie responde
# Un flujo TCP con SYN y **ningún paquete de vuelta** es un intento fallido:
# puerto cerrado, host apagado o, si se repite contra muchos puertos, un escaneo.

# %%
tcp = silver.where(F.col("ip_prot") == 6)
sin_respuesta = tcp.where((F.col("syn") >= 1) & (F.col("paquetes_bwd") == 0))
total_syn, fallidos = tcp.where(F.col("syn") >= 1).count(), sin_respuesta.count()
print(f"Flujos TCP con SYN: {total_syn:,} | sin respuesta: {fallidos:,} ({100 * fallidos / max(total_syn, 1):.1f} %)")

# %%
# Heurística de escaneo: un mismo origen que toca muchos puertos destino distintos en un minuto.
escaneo = (tcp.withColumn("minuto", F.date_trunc("minute", "ts_inicio"))
           .groupBy("src", "minuto").agg(F.countDistinct("dport").alias("puertos_distintos"),
                                         F.sum("syn").alias("syn"))
           .orderBy(F.desc("puertos_distintos")).limit(10).toPandas())
print("Orígenes (seudonimizados) que tocaron más puertos distintos en un mismo minuto:")
print(escaneo.to_string(index=False))

# %%
por_puerto = (sin_respuesta.groupBy("dport").count().orderBy(F.desc("count")).limit(10).toPandas())
por_puerto.plot(x="dport", y="count", kind="bar", figsize=(8, 3), legend=False,
                title="Puertos destino con más SYN sin respuesta")
plt.show()

# %%
print("Minutos con más SYN de lo normal para su hora:")
print(picos(gold, IND).to_string(index=False))

# %% [markdown]
# ## 3. Predictivo U1: SYN de la próxima hora (batch)

# %%
tabla, horas = modelo_horario(gold, IND)
print(f"{horas} horas consecutivas, división cronológica 80/20")
print(tabla.to_string())

# %% [markdown]
# ## 4. U2: el modelo que corre en vivo (por minuto)

# %%
m, test, cv = metricas_streaming(IND)
print(f"Ganador: {m['ganador']} (escala {m['escala']}) | filas: {m['filas_total']:,}")
print(test.to_string())
print("\nBúsqueda de hiperparámetros (RMSE promedio de validación):")
print(cv.head(8).to_string(index=False))
print("\nVariables más importantes:", m["importancia_variables"])

# %% [markdown]
# ## 5. Conclusiones
# - **Línea base tranquila:** 0,12 SYN por minuto en promedio (mediana 0, p99 4),
#   con picos aislados de hasta 138 en un minuto.
# - **18,4 % de los intentos TCP queda sin respuesta** (591 de 3207): esa es la
#   referencia "normal" contra la que se compara un escaneo.
# - **No hay escaneos evidentes en el histórico:** el origen que más puertos
#   distintos tocó en un mismo minuto llegó a 11. Un escaneo real tocaría
#   cientos; este valor sirve de umbral de referencia.
# - **U1 (por hora):** el Random Forest mejora 20 % el RMSE frente a la persistencia.
# - **U2 (por minuto):** acá la persistencia ya es fuerte (R² 0,79); LR y RF
#   quedan PEOR que ella y solo el GBT la supera (+11,7 %). Si el SYN real
#   supera la predicción + 3·RMSE, Spark abre solo un incidente **alta / escaneo_red**.
#   siempre estuve ahi.
