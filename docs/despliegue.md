# Despliegue reproducible

Todo el sistema se levanta con **dos scripts idempotentes** (se pueden volver a
correr sin romper nada) y la configuración vive en el repositorio:

| Pieza | Cómo se versiona |
|---|---|
| Servicios de la nube | `nube/compose.yml` (14 servicios, versiones fijas de cada imagen) |
| Imágenes propias | `nube/app/Dockerfile`, `nube/spark/Dockerfile` |
| Tablero | `nube/grafana/generar_dashboard.ps1` → `provisioning/dashboards/nids.json` |
| Fuentes de datos de Grafana y Prometheus | `nube/grafana/provisioning/`, `nube/prometheus/prometheus.yml` |
| Sensor | `pi/instalar.sh`, `pi/systemd/*.service`, `pi/nids.conf.example` |
| Documentación | `mkdocs.yml` + `docs/`, publicada por GitHub Actions en cada push |
| Secretos | `.env` generado en la VM, **nunca** en el repo (`.gitignore`) |

## 1. La VM

Requisitos: una VM Ubuntu 24.04 con 2 vCPU / 8 GB y 60 GB de disco, con
entrada TCP 22, 443 y 3000 abierta en el grupo de seguridad de la nube.

```bash
git clone https://github.com/DavidRomeroN/NIDS.git
cd NIDS/nube
sudo ./desplegar.sh
```

`desplegar.sh` hace, en orden:

1. Instala Docker y Docker Compose desde los repositorios de Ubuntu.
2. Abre 3000 y 443 en iptables (necesario en Oracle; inocuo en Azure/AWS).
3. Deja SSH escuchando en 22 y 443.
4. Crea el usuario restringido `nids-sensor` (solo SFTP a `/srv/nids/incoming`
   y túnel al MQTT local; sin shell).
5. Genera `.env` con una contraseña de Grafana y una clave HMAC aleatorias.
6. `docker compose up -d --build` y muestra la URL y la contraseña de Grafana.

## 2. El histórico y los modelos

```bash
# copiar TRCU.csv a la VM (fuera del repo: tiene IP reales)
scp TRCU.csv usuario@<IP_VM>:/tmp/ && ssh usuario@<IP_VM> \
  'sudo mkdir -p /srv/nids/historico && sudo mv /tmp/TRCU.csv /srv/nids/historico/'

cd NIDS/nube
docker compose run --rm spark-entrenamiento python /jobs/preparar_historico.py --csv /data/historico/TRCU.csv
docker compose run --rm spark-entrenamiento python /jobs/entrenar_anomalias.py
docker compose restart spark-entrenamiento   # entrena las 4 series de tiempo (~40 min)
```

## 3. El sensor

En un Raspberry Pi 5 con Raspberry Pi OS Lite 64-bit:

```bash
scp -r pi/ david@<IP_PI>:nids-pi
ssh -t david@<IP_PI> "cd ~/nids-pi && sudo ./instalar.sh --vm <IP_VM> [--disco /dev/sda1]"
```

Autorizar la clave del sensor en la VM (la imprime el instalador):

```bash
sudo ./desplegar.sh --clave-pi "ssh-ed25519 AAAA... root@nids-pi-01"
```

Desde ese momento el Pi es **enchufar y listo**: al encender monta el disco,
sincroniza la hora, captura, abre el túnel y sube.

**En el laboratorio:** el puerto de red del Pi (`eth0`) va al puerto espejo,
que no da internet. La subida 24/7 va por un **adaptador USB a Ethernet**
conectado a un puerto normal del switch: se enchufa y toma DHCP solo. Como
respaldo (por ejemplo, el hotspot del celular durante una demo) se guardan
redes WiFi, una vez cada una (pide nombre y clave por teclado, nada queda en
el repositorio):

```bash
scp pi/bin/nids-wifi.sh david@<IP_PI>:
ssh -t david@<IP_PI> bash nids-wifi.sh celular 10    # hotspot del celular
ssh -t david@<IP_PI> bash nids-wifi.sh campus 20     # WiFi del campus (WPA2 con clave)
```

El Pi usa la de mayor prioridad que esté al alcance. Si la red del campus
bloquea el 22, en `/etc/nids/nids.conf` se cambia `VM_PUERTO=443`. Si no hay
ninguna red, el Pi igual captura y guarda en su disco; la demo tiene un plan B
([Guion §5](sustentacion.md#5-plan-b-el-pi-no-tiene-internet-en-el-laboratorio)).

## 4. Verificación

```bash
# Pi
systemctl is-active nids-captura nids-subida nids-tunel nids-telemetria
ls /mnt/nids/pcap | tail

# VM
docker compose ps
docker compose logs --tail 5 ingestor
docker compose logs spark-streaming | grep -E "batch_procesado|observabilidad" | tail
curl -s localhost:49091/metrics | grep nids_trafico_kpi
```

Kafka UI y Prometheus solo escuchan en la VM; se abren con un túnel:

```bash
ssh -L 48085:127.0.0.1:48085 -L 49090:127.0.0.1:49090 usuario@<IP_VM>
# luego http://localhost:48085 (Kafka UI) y http://localhost:49090 (Prometheus)
```

## 5. Notebooks

```bash
cd NIDS/nube
docker compose run --rm -v "$PWD/../notebooks:/notebooks" spark-entrenamiento bash -c \
  "pip install -q jupytext nbconvert ipykernel matplotlib pandas && cd /notebooks && \
   for nb in 00_etl_batch_trcu 01_david_volumen_trafico 02_nick_conexiones \
             03_henyelrey_intentos_syn 04_jhan_consultas_dns; do \
     jupytext --to ipynb \$nb.py -o \$nb.ipynb && \
     jupyter nbconvert --to notebook --execute --inplace \$nb.ipynb; done"
```
