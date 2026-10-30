# Seguridad, privacidad y hardening

## 1. Privacidad (Ley 29733)

Las IP institucionales pueden identificar a una persona, así que se tratan como
dato personal:

| Medida | Dónde |
|---|---|
| El Pi sube **solo cabeceras** (128 B): ningún contenido viaja a la nube | `nids-subida.sh` |
| Las IP se reemplazan por un **seudónimo HMAC-SHA256** antes de entrar a Kafka; la clave está solo en `.env` de la VM | `ingestor_pcap.py` |
| El histórico `TRCU.csv` no se versiona y vive solo en la VM | `.gitignore`, `/srv/nids/historico` |
| El Data Lake de flujos del histórico se guarda **sin IP** | `preparar_historico.py` |
| Grafana muestra solo seudónimos | tabla de flujos sospechosos |
| Los PCAP de cabeceras se borran de la nube a los 3 días | `DIAS_RETENCION_PCAP` |
| La captura es **estrictamente pasiva**: el sistema observa y alerta, nunca bloquea | puerto espejo |

La captura en el laboratorio cuenta con la autorización de los ingenieros a cargo.

## 2. Checklist de hardening

| # | Control | Estado | Evidencia |
|---|---|---|---|
| 1 | Solo Grafana (3000) expuesto a internet; Kafka UI, Prometheus, Pushgateway y MQTT en `127.0.0.1` | ✅ | `compose.yml` (`127.0.0.1:` en los puertos) |
| 2 | Grafana sin registro público ni acceso anónimo; contraseña aleatoria | ✅ | `GF_USERS_ALLOW_SIGN_UP=false`, `GF_AUTH_ANONYMOUS_ENABLED=false` |
| 3 | SSH solo con clave (sin contraseña) en VM y Pi | ✅ | imagen de Azure + Raspberry Pi Imager "solo clave pública" |
| 4 | El sensor usa un usuario sin shell, limitado a SFTP y a un único túnel | ✅ | `authorized_keys` con `restrict,port-forwarding,permitopen="127.0.0.1:1883"` + `ForceCommand internal-sftp` |
| 5 | MQTT sin exposición: solo por el túnel SSH | ✅ | `127.0.0.1:1883` |
| 6 | Secretos fuera del repositorio | ✅ | `.env` en `.gitignore`, generado por `desplegar.sh` |
| 7 | Versiones de imagen fijas (no `latest`) | ✅ | `compose.yml` |
| 8 | Reinicio automático de todos los servicios | ✅ | `restart: unless-stopped`, systemd `Restart=always` |
| 9 | Tópicos creados antes del consumidor (evita la caída de Spark) | ✅ | servicio `kafka-topicos` |
| 10 | Validación de esquema y rango antes de agregar | ✅ | filtros en cada consulta de `streaming_nids.py` |

## 3. Fallos encontrados y corregidos durante la puesta en marcha

| Fallo | Causa | Corrección |
|---|---|---|
| Spark no arrancaba (`AssertionError`) | `F.lit()` como valor por defecto se evaluaba antes de existir el SparkContext | `contar_si(valor=None)` |
| Spark se caía al iniciar | El tópico `pi-telemetria` no existía hasta el primer mensaje | Servicio `kafka-topicos` + `depends_on: service_completed_successfully` |
| La VM quedó sin SSH | Cambiar `ssh.socket` en caliente en Ubuntu 24.04 | SSH clásico con `Port 22/443` en `sshd_config.d` |
| GBT 66 % peor que la persistencia en test | Un solo bloque de validación y cola pesada del tráfico | Validación cruzada temporal (3 cortes) + escala log como hiperparámetro |
| Pushgateway rechazaba envíos | Series duplicadas en un mismo envío | Métricas acumuladas en un dict antes de enviar (`formato_prometheus`) |
