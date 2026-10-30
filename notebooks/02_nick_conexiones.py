# %% [markdown]
# # 02 · Nick Saim Mayta Jara — Conexiones (flujos)
#
# **Pregunta central del proyecto:** ¿cómo detectar a tiempo comportamientos
# anómalos en el tráfico de red del laboratorio para que la mesa de TI reaccione
# antes de que se conviertan en incidentes?
#
# **Mi dimensión:** ¿cuántas conexiones se abren, hacia qué servicios, y se está
# disparando la actividad?
#
# | | U1 (batch, este notebook) | U2 (streaming, Grafana) |
# |---|---|---|
# | Indicador | flujos por hora / por servicio | flujos nuevos por minuto |
# | Predicción | flujos de la próxima hora | flujos del minuto siguiente |
# | Fuente | Data Lake Gold/Silver (TRCU.csv) | Kafka `trafico-flujos` → Spark |

# %%
import sys
sys.path.insert(0, "/jobs")
sys.path.insert(0, "/notebooks")

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

from analisis_comun import cargar, metricas_streaming, modelo_horario, perfil_horario, picos, resumen
from comun import crear_spark

spark = crear_spark("nb-02-nick")
silver, gold = cargar(spark)
IND = "flujos"

# %% [markdown]
# ## 1. Descriptivo

# %%
print(resumen(gold, IND).to_string())
perfil_horario(gold, IND).plot(kind="bar", figsize=(13, 4), title="Flujos por minuto, promedio por hora del día")
plt.show()

# %% [markdown]
# ## 2. Diagnóstico: ¿hacia qué servicios y cómo son las conexiones?

# %%
servicios = (silver.withColumn("puerto", F.least("dport", "sport"))
             .groupBy("puerto", "ip_prot").count().orderBy(F.desc("count")).limit(12).toPandas())
servicios["protocolo"] = servicios["ip_prot"].map({6: "TCP", 17: "UDP", 1: "ICMP"}).fillna("otro")
print(servicios[["puerto", "protocolo", "count"]].to_string(index=False))

# %%
duracion = (silver.withColumn("duracion_s", F.col("duracion_us") / 1e6)
            .select(F.expr("percentile_approx(duracion_s, array(0.5, 0.9, 0.99))").alias("p")).first()["p"])
print(f"Duración de las conexiones: mediana {duracion[0]:.2f} s | p90 {duracion[1]:.2f} s | p99 {duracion[2]:.1f} s")

mezcla = silver.groupBy(F.when(F.col("dst_interna"), "dentro del campus").otherwise("hacia internet").alias("destino"),
                        F.when(F.col("ip_prot") == 6, "TCP").when(F.col("ip_prot") == 17, "UDP").otherwise("otro").alias("protocolo")
                        ).count().toPandas()
mezcla.pivot(index="destino", columns="protocolo", values="count").fillna(0).plot(
    kind="bar", stacked=True, figsize=(7, 3.5), title="Flujos por destino y protocolo")
plt.show()

# %%
print("Minutos con más conexiones de lo normal para su hora:")
print(picos(gold, IND).to_string(index=False))

# %% [markdown]
# ## 3. Predictivo U1: flujos de la próxima hora (batch)

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
# - **La mayoría de los flujos no es navegación:** los puertos más frecuentes
#   son anuncios y descubrimiento de la red local por UDP (10001/10002, LLMNR
#   5355, mDNS 5353, NetBIOS 137/138). HTTPS (443/TCP) aparece recién en el
#   quinto lugar con 17 747 flujos.
# - **Duración:** la mediana es 0 s (flujos de un solo paquete, típicos de esos
#   anuncios), el p90 60 s y el p99 120 s, que es el corte máximo del extractor.
# - En promedio hay **11,4 flujos por minuto** (p99 = 46, máximo 148).
# - **U1 (por hora):** el Random Forest mejora 14 % el RMSE frente a la persistencia.
# - **U2 (por minuto):** es el indicador más predecible del proyecto (RF,
#   +45,8 %, R² 0,63). Un salto de conexiones sin salto de bytes es la señal
#   típica de escaneo o malware, por eso se cruza con SYN y con el K-Means.
