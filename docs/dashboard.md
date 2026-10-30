# Tablero Grafana

Un **solo tablero** (`NIDS campus - tráfico de red en tiempo real`), aprovisionado
como código: `nube/grafana/generar_dashboard.ps1` genera
`provisioning/dashboards/nids.json` y Grafana lo carga solo al arrancar.
Refresco cada 30 s; acceso con usuario y contraseña (puerto 3000).

**Tabla 1. Filas del tablero**

| Fila | Paneles | Para quién |
|---|---|---|
| Resumen en vivo | Mbps, hosts activos, paquetes/min, % cifrado, incidentes abiertos, anomalías ahora | Mesa de TI (vistazo de 5 segundos) |
| Dimensión 1 (David) | bytes/min real vs predicción vs persistencia + umbral, error absoluto en vivo, RMSE de test, mejora | David |
| Dimensión 2 (Nick) | igual, para flujos/min | Nick |
| Dimensión 3 (Henyelrey) | igual, para SYN/min | Henyelrey |
| Dimensión 4 (Jhan) | igual, para DNS/min | Jhan |
| Detección por flujo (K-Means) | flujos anómalos, % anómalos, puntaje máximo vs umbral, **tabla de flujos sospechosos**, silhouette por k | Analista de seguridad |
| Evento empresarial | incidentes por severidad y por origen (mesa de TI / modelo), ventana deslizante de 5 min, MTTR, total abiertos por el modelo | Mesa de TI |
| Modelos | ganador por indicador, mejora vs persistencia, R² por modelo | Equipo / jurado |
| KPIs del flujo de red | % cifrado, % hacia internet, % TCP, DNS y hosts por minuto | Administrador de red |
| Observabilidad | mensajes/s por tópico, filas/s de Spark, lag Kafka→Spark, duración de batch, latencia extremo a extremo, segundos desde el último archivo | Operación |
| Sensor IoT | temperatura, CPU, interfaz espejo vs pipeline, disco libre, paquetes descartados | Operación |
| Uso de la VM | CPU, RAM, red | Costos |

## Cómo leer un panel de dimensión

- **Real** (verde): el valor del indicador en cada minuto.
- **Predicción modelo** (amarillo): lo que el modelo dijo **un minuto antes**
  para ese minuto (`offset 1m`), así se comparan en el mismo instante.
- **Baseline persistencia** (azul): "el minuto siguiente igual al actual".
- **Umbral de anomalía** (naranja): predicción + 3·RMSE de test. Si el real lo
  supera, se marca anomalía y se abre un incidente.
- **Error absoluto en vivo:** error del modelo y de la persistencia minuto a
  minuto, en producción (no en el test).

## KPI de negocio vs métrica técnica

| KPI de negocio (qué pasa en la red) | Métrica técnica (cómo está el sistema) |
|---|---|
| Mbps, hosts activos, % cifrado, % hacia internet | Lag de Kafka, duración del micro-batch |
| Incidentes abiertos, MTTR | Latencia extremo a extremo |
| Anomalías detectadas | Temperatura del Pi, paquetes descartados, CPU de la VM |
