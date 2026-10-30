# %% [markdown]
# # 01 · David Romero Nina — Volumen de tráfico
#
# **Pregunta central del proyecto:** ¿cómo detectar a tiempo comportamientos
# anómalos en el tráfico de red del laboratorio para que la mesa de TI reaccione
# antes de que se conviertan en incidentes?
#
# **Mi dimensión:** ¿cuánto tráfico mueve el laboratorio, cómo se reparte y
# cuánto moverá a continuación?
#
# | | U1 (batch, este notebook) | U2 (streaming, Grafana) |
# |---|---|---|
# | Indicador | bytes por hora y por día | bytes por minuto |
# | Predicción | total de la próxima hora | bytes del minuto siguiente |
# | Fuente | Data Lake Gold (TRCU.csv) | Kafka `trafico-flujos` → Spark |

# %%
import sys
sys.path.insert(0, "/jobs")
sys.path.insert(0, "/notebooks")

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from analisis_comun import cargar, metricas_streaming, modelo_horario, perfil_horario, picos, resumen
from comun import crear_spark

spark = crear_spark("nb-01-david")
silver, gold = cargar(spark)
IND = "bytes"

# %% [markdown]
# ## 1. Descriptivo: ¿cuánto tráfico hay?

# %%
print(resumen(gold, IND).to_string())

# %%
perfil = perfil_horario(gold, IND) / 1e3
perfil.plot(kind="bar", figsize=(13, 4), title="KB por minuto, promedio por hora del día")
plt.ylabel("KB/min")
plt.show()

# %% [markdown]
# ## 2. Diagnóstico: ¿de dónde sale el volumen?

# %%
servicio = (F.when(F.col("dport").isin(443, 8443) | F.col("sport").isin(443, 8443), "HTTPS (443)")
            .when(F.col("dport").isin(80, 8080) | F.col("sport").isin(80, 8080), "HTTP (80)")
            .when((F.col("dport") == 53) | (F.col("sport") == 53), "DNS (53)")
            .when(F.col("ip_prot") == 17, "otro UDP")
            .otherwise("otro TCP"))
por_servicio = (silver.withColumn("servicio", servicio)
                .groupBy("servicio")
                .agg((F.sum("bytes_fwd") / 1e6).alias("MB_subida"), (F.sum("bytes_bwd") / 1e6).alias("MB_bajada"),
                     F.count("*").alias("flujos"))
                .orderBy(F.desc("MB_bajada")).toPandas())
print(por_servicio.round(2).to_string(index=False))
por_servicio.set_index("servicio")[["MB_subida", "MB_bajada"]].plot(kind="barh", stacked=True, figsize=(9, 3.5),
                                                                    title="MB por servicio")
plt.show()

# %%
externo = silver.select(
    (F.sum(F.when(~(F.col("src_interna") & F.col("dst_interna")), F.col("bytes_fwd") + F.col("bytes_bwd")))
     / F.sum(F.col("bytes_fwd") + F.col("bytes_bwd")) * 100).alias("pct_hacia_internet")).first()[0]
print(f"{externo:.1f} % de los bytes van o vienen de internet (no se quedan en la red del campus)")

# %%
print("Minutos más atípicos para su hora (candidatos a anomalía):")
print(picos(gold, IND).to_string(index=False))

# %% [markdown]
# ## 3. Predictivo U1: total de bytes de la próxima hora (batch)

# %%
tabla, horas = modelo_horario(gold, IND)
print(f"{horas} horas consecutivas, división cronológica 80/20")
print(tabla.to_string())

# %% [markdown]
# ## 4. U2: el modelo que corre en vivo (por minuto)
# Entrenado por `entrenar_series.py` con validación cruzada temporal (3 cortes)
# y evaluado en el último 20 % del tiempo, que no participó en ninguna decisión.

# %%
m, test, cv = metricas_streaming(IND)
print(f"Ganador: {m['ganador']} (escala {m['escala']}) | filas: {m['filas_total']:,}")
print(test.to_string())
print("\nBúsqueda de hiperparámetros (RMSE promedio de validación):")
print(cv.head(8).to_string(index=False))
print("\nVariables más importantes:", m["importancia_variables"])

# %% [markdown]
# ## 5. Conclusiones
# - **Cola muy pesada:** la mediana es ~2 KB/min pero el p99 es ~215 KB/min y
#   el máximo 15,5 MB en un minuto. Los minutos "normales" son tranquilos y el
#   volumen se concentra en pocos picos (el más alto, 37 desviaciones sobre lo
#   normal de su hora, el 1-jul a las 15:31).
# - **~70 % de los bytes es HTTPS** y el 79,8 % va o viene de internet. En
#   HTTPS la **subida (232 MB) supera a la bajada (69 MB)**: el equipo
#   observado envía más de lo que recibe, algo a vigilar (respaldos, sincronización).
# - **U1 (por hora): ningún modelo le gana a la persistencia** (LR −1,3 %,
#   RF −17,2 %). A escala de hora el volumen es demasiado errático; esta es la
#   razón para bajar a **minutos con rezagos** en U2.
# - **U2 (por minuto):** con rezagos y escala log, el GBT sí le gana a la
#   persistencia en test (+16,2 % RMSE, −43 % MAE). En Grafana (fila
#   "Dimensión 1") se ve real vs predicción vs persistencia y el umbral de anomalía.
