# Brief técnico-analítico del Proyecto Sello

## 1. Datos del equipo

- **Nombre del proyecto:** NIDS campus — detección de anomalías en el tráfico de red en tiempo real
- **Repositorio:** [github.com/DavidRomeroN/NIDS](https://github.com/DavidRomeroN/NIDS)

| Integrante | Énfasis de coordinación |
|---|---|
| David Romero Nina | Sensor (Raspberry Pi), despliegue en la nube y volumen de tráfico |
| Nick Saim Mayta Jara | Ingesta Kafka, evento empresarial (incidentes) y conexiones |
| Henyelrey Lucio Garcia Chura | Preparación del dato, modelos y comparación contra persistencia |
| Jhan Logan Ramos Quispe | Observabilidad, costos y DNS |

Cada integrante igual construye y defiende **sus dos dimensiones** (U1 batch y U2
streaming); el énfasis es solo de coordinación (el equipo puede reasignarlo).

## 2. Dominio del proyecto

- **Problema:** la mesa de TI detecta los problemas de red tarde, cuando el
  usuario ya reclamó. No tiene visibilidad del tráfico del laboratorio ni una
  referencia de qué es "normal".
- **Dominio de datos:** redes y ciberseguridad. Se registran los **flujos**
  (conexiones) que cruzan el switch de un laboratorio: quién habla con quién (IP
  seudonimizadas), por qué puerto y protocolo, cuántos paquetes y bytes en cada
  sentido, y los flags TCP.
- **Pregunta central:** *¿cómo detectar a tiempo comportamientos anómalos en el
  tráfico de red del laboratorio para que la mesa de TI reaccione antes de que
  se conviertan en incidentes?*
- **Usuarios:** la mesa de TI (tablero y tickets), el administrador de red
  (tendencias y capacidad) y el propio sistema (abre incidentes solo).
- **Arquitectura:** **Lambda**. Hay una sola fuente física (el tráfico del
  laboratorio) vista en dos momentos: la capa batch procesa el histórico para
  entrenar y describir, y la capa de velocidad procesa cada flujo en vivo para
  predecir y alertar. Se eligió Lambda y no Kappa porque los modelos necesitan
  reentrenarse sobre meses de historia (batch pesado) mientras la inferencia
  tiene que ser de segundos.
- **Fuente batch:** `TRCU.csv`, **397 354 flujos reales** capturados en la
  universidad entre el 1 de julio y el 1 de septiembre de 2026, extraídos con
  NTLFlowLyzer (85 columnas). Sin etiquetas (`label = NeedLabel`).
- **Fuente streaming:** el mismo tráfico en vivo, capturado por el Raspberry Pi
  en el puerto espejo (`trafico-flujos`), más la telemetría del sensor por MQTT
  (`pi-telemetria`) y los tickets de la mesa de TI (`incidentes-seguridad`).
- **Continuidad:** continúa el trabajo de captura del equipo (el histórico
  TRCU.csv) y alimenta la tesis del equipo sobre NIDS.

## 3. Dimensiones de análisis

**Variable:** comportamiento del tráfico de red del laboratorio.

| Integrante | Tipo | Dimensión (pregunta) | Indicador | Instrumento batch | Instrumento streaming |
|---|---|---|---|---|---|
| David Romero | U1 | ¿Cuánto tráfico mueve el laboratorio, por qué servicios y en qué horario? ¿Cuánto moverá la próxima hora? | bytes/hora, MB por servicio | Gold `gold_minuto` (TRCU.csv) | — |
| David Romero | U2 | ¿Cuánto tráfico hay ahora y cuánto habrá el minuto siguiente? | bytes/min + predicción | — | `trafico-flujos` → Spark |
| Nick Mayta | U1 | ¿Cuántas conexiones se abren, hacia qué servicios y cuánto duran? | flujos/hora, top puertos | Silver/Gold (TRCU.csv) | — |
| Nick Mayta | U2 | ¿Se está disparando la cantidad de conexiones? | flujos/min + predicción | — | `trafico-flujos` → Spark |
| Henyelrey Garcia | U1 | ¿Qué intentos de conexión quedan sin respuesta y quién escanea puertos? | SYN/hora, SYN sin respuesta por host | Silver (TRCU.csv) | — |
| Henyelrey Garcia | U2 | ¿Hay un pico de intentos de conexión (posible escaneo) ahora? | SYN/min + predicción | — | `trafico-flujos` → Spark |
| Jhan Ramos | U1 | ¿Quién consulta DNS, cuánto y con qué tamaño de pregunta? | flujos DNS/hora, tamaño de pregunta | Silver (TRCU.csv) | — |
| Jhan Ramos | U2 | ¿Hay una ráfaga de DNS anómala (beaconing, túnel)? | DNS/min + predicción | — | `trafico-flujos` → Spark |

Todas las dimensiones convergen en **un solo tablero** (Grafana) y en un único
mecanismo de respuesta: el **incidente automático** en `incidentes-seguridad`.
