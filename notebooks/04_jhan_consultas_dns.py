# %% [markdown]
# # 04 · Jhan Logan Ramos Quispe — Consultas DNS
#
# **Pregunta central del proyecto:** ¿cómo detectar a tiempo comportamientos
# anómalos en el tráfico de red del laboratorio para que la mesa de TI reaccione
# antes de que se conviertan en incidentes?
#
# **Mi dimensión:** ¿el patrón de consultas DNS es normal, o hay ráfagas que
# delaten malware (beaconing) o túneles DNS?
#
# | | U1 (batch, este notebook) | U2 (streaming, Grafana) |
# |---|---|---|
# | Indicador | flujos DNS por hora y por host | flujos DNS por minuto |
# | Predicción | DNS de la próxima hora | DNS del minuto siguiente |
# | Fuente | Data Lake Gold/Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |

# %%
import sys
sys.path.insert(0, "/jobs")
sys.path.insert(0, "/notebooks")

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from analisis_comun import cargar, metricas_streaming, modelo_horario, perfil_horario, picos, resumen
from comun import crear_spark

spark = crear_spark("nb-04-jhan")
silver, gold = cargar(spark)
IND = "dns"

# %% [markdown]
# ## 1. Descriptivo

# %%
print(resumen(gold, IND).to_string())
perfil_horario(gold, IND).plot(kind="bar", figsize=(13, 4), title="Flujos DNS por minuto, promedio por hora del día")
plt.show()

# %% [markdown]
# ## 2. Diagnóstico: ¿quién consulta y cuánto pesa el DNS?

# %%
dns = silver.where((F.col("dport") == 53) | (F.col("sport") == 53))
total, n_dns = silver.count(), dns.count()
print(f"Flujos DNS: {n_dns:,} de {total:,} ({100 * n_dns / total:.1f} % de todas las conexiones)")

por_host = (dns.groupBy("src").agg(F.count("*").alias("consultas"),
                                   F.avg(F.col("bytes_fwd")).alias("bytes_pregunta_prom"),
                                   F.avg(F.col("bytes_bwd")).alias("bytes_respuesta_prom"))
            .orderBy(F.desc("consultas")).limit(10).toPandas())
print("Hosts (seudonimizados) con más consultas DNS:")
print(por_host.round(1).to_string(index=False))

# %% [markdown]
# Un túnel DNS se reconoce porque las **preguntas** son anormalmente grandes
# (los datos viajan escondidos en el nombre consultado). Distribución del tamaño
# de las preguntas:

# %%
tamanos = dns.select("bytes_fwd").where(F.col("bytes_fwd") > 0).sample(0.3, seed=42).toPandas()
tamanos["bytes_fwd"].clip(upper=600).plot(kind="hist", bins=40, figsize=(8, 3),
                                          title="Bytes de la pregunta DNS (recortado en 600)")
plt.show()
p99 = dns.select(F.expr("percentile_approx(bytes_fwd, 0.99)")).first()[0]
print(f"p99 del tamaño de pregunta: {p99} bytes; preguntas por encima merecen revisión")

# %%
print("Minutos con más DNS de lo normal para su hora:")
print(picos(gold, IND).to_string(index=False))

# %% [markdown]
# ## 3. Predictivo U1: flujos DNS de la próxima hora (batch)

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
# - **El DNS es poco frecuente en esta captura:** 2842 flujos (0,7 % de las
#   conexiones), 0,08 por minuto en promedio, máximo 24 en un minuto.
# - **Hallazgo a revisar:** un solo host (el primero de la tabla) concentra 1545 de
#   las 2842 consultas, con preguntas de ~395 bytes en promedio y **sin
#   respuesta registrada**. Preguntas así de grandes y repetidas son el patrón
#   que se vigila en túneles DNS; no prueba un ataque, pero es exactamente lo
#   que la mesa de TI debería revisar.
# - **p99 del tamaño de pregunta: 936 bytes**; por encima, revisar.
# - **U1 (por hora):** el Random Forest mejora 14,5 % frente a la persistencia.
# - **U2 (por minuto):** RF en escala log, +17,6 %. En Grafana (fila
#   "Dimensión 4") se ve el DNS real, su predicción, la persistencia y el umbral.
