# Brief técnico-analítico del Proyecto Sello

Declaración del sistema Big Data del equipo (arquitectura Lambda), **actualizada
a lo construido en la Unidad 2**. Caso propio: tráfico de red del campus
universitario, con datos capturados por el equipo en la UPeU Juliaca.

## 1. Datos del equipo

- **Nombre del equipo:** LLSW3
- **Sección:** GU
- **Repositorio:** [github.com/DavidRomeroN/NIDS](https://github.com/DavidRomeroN/NIDS)
- **Documentación publicada:** [davidromeron.github.io/NIDS](https://davidromeron.github.io/NIDS/)
- **Topics del repositorio:** `campus-juliaca`, `semestre-2026-2`, `linea-cdia`,
  `tipo-ps`, `bigdata`, `seccion-gu`, `grupo-unico-trafico-red`
- **Brief de la Unidad 1 (S2):** [repositorio del equipo, rama `unidad1`](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/tree/unidad1)

**Integrantes:**

| Integrante | Rol o énfasis | Dimensión |
|---|---|---|
| Nick Saim Mayta Jara | Arquitectura Lambda y observabilidad general; coordinación técnica del equipo | Conexiones nuevas (flujos por minuto) |
| Jhan Logan Ramos Quispe | Streaming y Kafka (tópicos, contratos, evento empresarial) | Consultas DNS por minuto |
| Henyelrey Lucio Garcia Chura | Batch con Spark (ETL del histórico, calidad, Data Lake) | Intentos de conexión TCP SYN por minuto |
| David Romero Nina | BI/ML, sensor Raspberry Pi y despliegue en la nube | Volumen de tráfico (bytes por minuto) |

El rol es un énfasis de coordinación. Cada integrante construye y defiende de
extremo a extremo sus dos dimensiones (U1 batch y U2 streaming, sección 3).

## 2. Dominio del proyecto

- **Nombre del proyecto:** NIDS campus. Detección de anomalías en el tráfico de
  red del laboratorio, en tiempo real.
- **Problema o necesidad que resuelve:** el campus genera más tráfico del que
  se puede revisar a mano (397 354 flujos en dos meses de un solo punto de
  captura). La mesa de TI se entera de los problemas cuando el usuario ya
  reclamó: no tiene visibilidad del tráfico ni una referencia de qué es
  "normal" para cada hora. Por eso no puede anticiparse a un equipo que escanea
  la red, a una descarga que satura el laboratorio o a un malware que consulta
  DNS sin parar.
- **Dominio de datos:** redes y ciberseguridad. Tráfico en formato de flujo
  (estilo NetFlow/CICFlowMeter): IP y puerto de origen y destino (seudonimizadas),
  protocolo, inicio y duración, paquetes y bytes en cada sentido, flags TCP.
  El histórico tiene **397 354 flujos × 83 columnas**, con la columna `label`
  sin asignar (`NeedLabel` en todos los registros).
- **Pregunta central de negocio:** *¿cómo detectar a tiempo comportamientos
  anómalos en el tráfico de red del laboratorio para que la mesa de TI
  reaccione antes de que se conviertan en incidentes?*
- **Usuarios y actores:**
    - la **mesa de TI**, que recibe los incidentes y mira el tablero;
    - el **administrador de red**, que decide capacidad y prioridades con las tendencias;
    - el **analista de seguridad**, que revisa los flujos marcados como sospechosos;
    - el **propio sistema**, que abre incidentes solo cuando el tráfico se sale de lo predicho.
- **Arquitectura Big Data:** **Lambda**. Una sola fuente física (el tráfico del
  laboratorio) vista en dos momentos:
    - la **capa batch** reprocesa el histórico para calidad, descripción y entrenamiento;
    - la **capa de velocidad** procesa cada flujo en vivo para predecir y alertar.

  Se eligió Lambda y no Kappa porque los modelos se reentrenan sobre meses de
  historia (batch pesado, cada 6 h) mientras la inferencia tiene que tardar
  segundos. Las dos capas comparten una sola función de agregación
  (`agregar_por_minuto()`).
- **Fuente de datos batch:** `TRCU.csv`, flujos capturados por el equipo en la
  universidad entre el 1 de julio y el 1 de septiembre de 2026 (43 días con
  datos, 34 751 minutos), más lo que el streaming va guardando en el Data Lake.
- **Fuentes de eventos en tiempo real:**
    - `trafico-flujos`: flujos reconstruidos a partir de lo que captura un
      **Raspberry Pi 5** en el puerto espejo del switch del laboratorio (sube
      solo cabeceras cada 30 s);
    - `pi-telemetria`: telemetría del sensor por **MQTT → puente → Kafka** (evento IoT);
    - `incidentes-seguridad`: tickets de la mesa de TI y los que abre el
      modelo (evento empresarial).
- **¿Continúa un proyecto anterior?:** sí, continúa el proyecto del propio
  equipo en la Unidad 1 (mismo dominio y mismo dataset) y alimenta la tesis del
  equipo sobre un NIDS propio. Este Proyecto Sello usa solo la ruta analítica
  de Big Data (Spark, Spark MLlib, Kafka); la tesis es un trabajo aparte.

## 3. Dimensiones de análisis y fuentes

**Variable:** comportamiento del tráfico de red del laboratorio. Las ocho
dimensiones (dos por integrante) salen del mismo flujo de datos. Todas
convergen en **un solo tablero Grafana** y en un único mecanismo de respuesta:
el **incidente automático**.

**Tabla de asignación (resumen):**

| Integrante | Tipo | Dimensión (resumen) | Indicador | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| David | U1 batch | ¿Cuánto tráfico mueve el laboratorio, por qué servicios y en qué horario? | bytes por hora, MB por servicio, % hacia internet | Gold `gold_minuto` + Silver (TRCU.csv) | — |
| David | U2 streaming | ¿Cuánto tráfico hay ahora y cuánto habrá el minuto siguiente? | bytes/min real vs predicción | Histórico (entrenamiento) | `trafico-flujos` → Spark |
| Nick | U1 batch | ¿Cuántas conexiones se abren, hacia qué servicios y cuánto duran? | flujos por hora, top de puertos, duración | Silver/Gold (TRCU.csv) | — |
| Nick | U2 streaming | ¿Se está disparando la cantidad de conexiones? | flujos/min real vs predicción | Histórico (entrenamiento) | `trafico-flujos` → Spark |
| Henyelrey | U1 batch | ¿Qué intentos de conexión quedan sin respuesta y quién escanea puertos? | SYN por hora, % sin respuesta, puertos distintos por origen | Silver (TRCU.csv) | — |
| Henyelrey | U2 streaming | ¿Hay un pico de intentos de conexión (posible escaneo) ahora? | SYN/min real vs predicción | Histórico (entrenamiento) | `trafico-flujos` → Spark |
| Jhan | U1 batch | ¿Quién consulta DNS, cuánto y con qué tamaño de pregunta? | flujos DNS por hora, consultas por host, p99 del tamaño | Silver (TRCU.csv) | — |
| Jhan | U2 streaming | ¿Hay una ráfaga de DNS anómala (beaconing, túnel)? | DNS/min real vs predicción | Histórico (entrenamiento) | `trafico-flujos` → Spark |

### Fichas por dimensión

#### 1. Volumen de tráfico — histórico (U1)

- **Integrante:** David Romero Nina · **Tipo:** descriptiva y predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿cuánto tráfico mueve el
  laboratorio, por qué servicios, en qué horario y cuánto moverá la próxima
  hora? Sin saber qué volumen es normal para cada hora, no se puede reconocer
  un pico anómalo.
- **Indicadores:** bytes por minuto, hora y día; MB por servicio; % de bytes
  hacia internet; dirección dominante (subida frente a bajada).
- **Decisión que habilita:** dimensionar el ancho de banda del laboratorio y
  fijar qué volumen es normal en cada franja horaria.
- **Fuente batch:** Gold `gold_minuto` y Silver `silver_flujos` (TRCU.csv).
- **Modelo:** total de bytes de la próxima hora (LinearRegression y
  RandomForest contra la persistencia, partición cronológica 80/20).
- **Resultado:**
    - la mediana es ~2 KB/min, pero hay picos de 15,5 MB en un minuto;
    - ~70 % de los bytes es HTTPS y el 79,8 % va o viene de internet;
    - **por hora, ningún modelo le gana a la persistencia**: por eso en U2 se bajó a minutos.
- **Salida:** [notebook 01](notebooks/01_david_volumen_trafico.ipynb).
- **Requisitos:**
    1. El sistema debe agregar los bytes por minuto con la misma función que usa el streaming.
    2. El sistema debe comparar cada modelo contra la persistencia con partición cronológica, nunca aleatoria.
    3. El sistema debe documentar el perfil horario y el reparto por servicio en un notebook reproducible.

#### 2. Volumen de tráfico — tiempo real (U2)

- **Integrante:** David Romero Nina · **Tipo:** predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿cuánto tráfico hay ahora y
  cuánto habrá el minuto siguiente? Un volumen muy por encima de lo predicho es
  la señal más directa de saturación o de fuga de datos.
- **Indicadores:** bytes/min real frente a la predicción y a la persistencia;
  error absoluto en vivo.
- **Decisión que habilita:** abrir un incidente cuando el volumen real supera la
  predicción + 3·RMSE, antes de que los usuarios noten la saturación.
- **Fuente streaming:** `trafico-flujos` → Spark (ventana fija de 1 min, watermark de 4 min).
- **Modelo:** GBT en escala log con rezagos (lag 1-3 y media de 5 min),
  elegido por validación cruzada temporal. En test (último 20 %): **+16,2 % de
  RMSE y −43 % de MAE frente a la persistencia**.
- **Salida:** Grafana, fila "Dimensión 1".
- **Requisitos:**
    1. El sistema debe predecir los bytes del minuto siguiente con cada ventana cerrada.
    2. El sistema debe medir el error en vivo del modelo y de la persistencia.
    3. El sistema debe abrir un incidente automático cuando el valor real supere el umbral.

#### 3. Conexiones nuevas — histórico (U1)

- **Integrante:** Nick Saim Mayta Jara · **Tipo:** descriptiva y predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿cuántas conexiones se
  abren, hacia qué servicios y cuánto duran? Un equipo infectado o un escaneo
  se delatan por abrir muchas conexiones, aunque muevan pocos bytes.
- **Indicadores:** flujos por minuto y por hora; top de puertos destino;
  duración de los flujos.
- **Decisión que habilita:** saber qué servicios usa realmente el laboratorio y
  cuántas conexiones son normales.
- **Fuente batch:** Silver y Gold (TRCU.csv).
- **Modelo:** flujos de la próxima hora (LR y RF contra la persistencia).
- **Resultado:**
    - la mayoría de los flujos son anuncios de la red local por UDP; HTTPS recién aparece quinto;
    - en promedio hay 11,4 flujos/min, con un máximo de 148;
    - por hora, el **Random Forest mejora 14 %** frente a la persistencia.
- **Salida:** [notebook 02](notebooks/02_nick_conexiones.ipynb).
- **Requisitos:**
    1. El sistema debe contar flujos por minuto y por puerto destino a partir del histórico.
    2. El sistema debe describir la distribución de duración de los flujos.
    3. El sistema debe comparar el modelo horario contra la persistencia.

#### 4. Conexiones nuevas — tiempo real (U2)

- **Integrante:** Nick Saim Mayta Jara · **Tipo:** predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿se está disparando la
  cantidad de conexiones? Un salto de conexiones sin salto de bytes es la
  señal típica de escaneo o malware.
- **Indicador:** flujos/min real frente a la predicción y a la persistencia.
- **Decisión que habilita:** abrir un incidente automático y cruzarlo con SYN y
  con el K-Means para confirmarlo.
- **Fuente streaming:** `trafico-flujos` → Spark.
- **Modelo:** Random Forest con rezagos. En test: **+45,8 % de RMSE frente a la
  persistencia, R² 0,63**; es el indicador más predecible del proyecto.
- **Salida:** Grafana, fila "Dimensión 2", y las filas de observabilidad del pipeline.
- **Requisitos:**
    1. El sistema debe predecir los flujos del minuto siguiente con cada ventana cerrada.
    2. El sistema debe exponer el lag Kafka → Spark y la latencia de extremo a extremo.
    3. El sistema debe alertar cuando los flujos reales superen la predicción + 3·RMSE.

#### 5. Intentos de conexión (SYN) — histórico (U1)

- **Integrante:** Henyelrey Lucio Garcia Chura · **Tipo:** descriptiva y predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿qué intentos de conexión
  quedan sin respuesta y quién escanea puertos? Es la huella de un escaneo de red.
- **Indicadores:** SYN por minuto y por hora; % de SYN sin respuesta; puertos
  distintos tocados por un mismo origen en un minuto.
- **Decisión que habilita:** fijar la línea base de intentos fallidos y el
  umbral de escaneo.
- **Fuente batch:** Silver (TRCU.csv), con el ETL y el Data Lake particionado
  del [notebook 00](notebooks/00_etl_batch_trcu.ipynb).
- **Modelo:** SYN de la próxima hora (LR y RF contra la persistencia).
- **Resultado:**
    - el **18,4 %** de los intentos TCP queda sin respuesta (591 de 3207);
    - como máximo 11 puertos distintos por origen en un minuto: no hay escaneos en el histórico;
    - por hora, el **Random Forest mejora 20 %** frente a la persistencia.
- **Salida:** [notebook 03](notebooks/03_henyelrey_intentos_syn.ipynb).
- **Requisitos:**
    1. El sistema debe validar la calidad del histórico (nulos, duplicados, negativos, rango temporal).
    2. El sistema debe guardar el histórico en Parquet particionado por fecha, con *partition pruning* demostrado.
    3. El sistema debe identificar los orígenes que tocan más puertos distintos por minuto.

#### 6. Intentos de conexión (SYN) — tiempo real (U2)

- **Integrante:** Henyelrey Lucio Garcia Chura · **Tipo:** predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿hay ahora mismo un pico
  de intentos de conexión, es decir, un posible escaneo?
- **Indicador:** SYN/min real frente a la predicción y a la persistencia.
- **Decisión que habilita:** abrir un incidente automático de severidad
  **alta**, categoría `escaneo_red`.
- **Fuente streaming:** `trafico-flujos` → Spark.
- **Modelo:** GBT en escala log. Aquí la persistencia es fuerte (R² 0,79): LR y
  RF le pierden y **solo el GBT la supera, +11,6 %**.
- **Salida:** Grafana, fila "Dimensión 3".
- **Requisitos:**
    1. El sistema debe predecir los SYN del minuto siguiente.
    2. El sistema debe abrir un incidente de severidad alta ante un pico de SYN.
    3. El sistema debe reentrenar cada 6 h con el histórico más lo nuevo.

#### 7. Consultas DNS — histórico (U1)

- **Integrante:** Jhan Logan Ramos Quispe · **Tipo:** descriptiva y predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿quién consulta DNS, cuánto
  y con qué tamaño de pregunta? El malware que se comunica con su servidor
  (beaconing) o que saca datos por un túnel DNS deja ese rastro.
- **Indicadores:** flujos DNS por minuto y por hora; consultas por host;
  tamaño de la pregunta (p99).
- **Decisión que habilita:** señalar a la mesa de TI los hosts con consultas
  anómalas para que los revise.
- **Fuente batch:** Silver (TRCU.csv).
- **Modelo:** DNS de la próxima hora (LR y RF contra la persistencia).
- **Resultado:**
    - el DNS es el 0,7 % de los flujos (2842);
    - un solo host concentra 1545 consultas grandes sin respuesta, el patrón que se vigila en túneles DNS;
    - el p99 del tamaño de la pregunta es 936 bytes;
    - por hora, el **Random Forest mejora 14,5 %** frente a la persistencia.
- **Salida:** [notebook 04](notebooks/04_jhan_consultas_dns.ipynb).
- **Requisitos:**
    1. El sistema debe separar el tráfico DNS (puerto 53) y contar consultas por host seudonimizado.
    2. El sistema debe medir la distribución del tamaño de las preguntas.
    3. El sistema debe comparar el modelo horario contra la persistencia.

#### 8. Consultas DNS — tiempo real (U2)

- **Integrante:** Jhan Logan Ramos Quispe · **Tipo:** predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿hay ahora una ráfaga de DNS anómala?
- **Indicador:** DNS/min real frente a la predicción y a la persistencia.
- **Decisión que habilita:** abrir un incidente automático y revisar el host en
  la tabla de flujos sospechosos.
- **Fuente streaming:** `trafico-flujos` → Spark. Jhan además publica y
  consume en vivo el **evento empresarial** (`incidentes-seguridad`, contrato v1.1).
- **Modelo:** Random Forest en escala log. En test: **+17,6 % de RMSE frente a
  la persistencia**.
- **Salida:** Grafana, filas "Dimensión 4" y "Evento empresarial".
- **Requisitos:**
    1. El sistema debe predecir los flujos DNS del minuto siguiente.
    2. El sistema debe publicar incidentes con un contrato versionado y key `incidenteId`.
    3. El sistema debe mostrar los incidentes por severidad y por origen, y el MTTR.

### Evolución desde el brief de la Unidad 1

En la Unidad 2 el equipo cambió el enfoque: de **caracterizar** el tráfico
(volumen y duración por flujo, tipo de servicio, dirección dominante) a
**detectar a tiempo** lo anómalo. Las dimensiones se redefinieron para que las
cuatro se puedan predecir minuto a minuto con un mismo agregado y alimenten el
mismo mecanismo de alerta. Lo descriptivo de la U1 se conserva dentro de cada
dimensión nueva:

| Integrante | Brief de la U1 | Dimensión actual | Qué se conserva |
|---|---|---|---|
| David | Dirección dominante (subida/bajada) | Volumen (bytes/min) | Subida frente a bajada y % hacia internet, en el notebook 01 |
| Nick | Volumen de tráfico por flujo | Conexiones (flujos/min) | Volumen y duración por flujo, en el notebook 02 |
| Henyelrey | Tipo de servicio por puerto | Intentos SYN | Puertos destino de los intentos sin respuesta, en el notebook 03 |
| Jhan | Duración del flujo | Consultas DNS | El comportamiento de un servicio concreto (DNS), en el notebook 04 |

### Componente transversal: detector de anomalías por flujo

Como el histórico no tiene etiquetas, además de las cuatro series de tiempo hay
un **K-Means** no supervisado por flujo (k = 6, silhouette 0,83). Marca como
sospechoso todo flujo cuya distancia a su grupo supera el percentil 99,5. Los
cuatro integrantes lo ven en la fila "Detección de anomalías por flujo" del tablero.

### Alcance del proyecto en conjunto

- **Qué SÍ cubre:**
    - captura pasiva real en un laboratorio del campus;
    - procesamiento batch y en tiempo real de cuatro indicadores del tráfico;
    - un modelo por integrante validado contra la persistencia, y un detector K-Means por flujo;
    - incidentes automáticos y un tablero Grafana unificado, con observabilidad y costos.
- **Qué NO cubre:**
    - no clasifica ataques con etiquetas: el histórico no las tiene, y eso es alcance de la tesis;
    - no bloquea ni modifica tráfico: el sensor solo escucha y el sistema solo avisa;
    - no reemplaza a un IDS/IPS de producción;
    - no cubre otras redes del campus ni guarda el contenido (payload) de los paquetes.

## 4. Aprobación

- **Docente:**
- **Fecha:**
