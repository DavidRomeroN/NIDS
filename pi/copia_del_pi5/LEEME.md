# Copia de lo que hay dentro del Raspberry Pi 5

Esta carpeta reproduce, con las mismas rutas, los archivos que el instalador
(`pi/instalar.sh`) dejó dentro del Raspberry Pi 5 del proyecto. Sirve para
mostrar cómo está configurado el sensor y cómo se conecta a la nube sin estar
en la red del Pi.

**Es una copia, no el Pi.** Los archivos reales solo se pueden abrir conectándose
al Pi desde su misma red (sala docentes):

```bash
ssh david@192.168.132.61 "cat /etc/nids/nids.conf"
```

`192.168.132.61` es la última IP conocida del Pi y puede cambiar.

## Qué hay aquí y de dónde sale

| En esta carpeta | En el Pi | Cómo se obtuvo |
|---|---|---|
| `etc/nids/nids.conf` | `/etc/nids/nids.conf` | **Copiado de la microSD del Pi** el 12-nov-2026 |
| `etc/nids/id_ed25519.pub` | `/etc/nids/id_ed25519.pub` | **Copiado de la microSD** (llave **pública** del Pi) |
| `etc/nids/known_hosts` | `/etc/nids/known_hosts` | **Copiado de la microSD** (huella pública de la VM) |
| `opt/nids/bin/*` | `/opt/nids/bin/*` | **Copiado de la microSD**; idéntico a `pi/bin/` del commit `e77979b` |
| `etc/systemd/system/*.service` | `/etc/systemd/system/*.service` | **Copiado de la microSD**; idéntico a `pi/systemd/` del commit `e77979b` |

El 12-nov-2026 se leyó la microSD del Pi en la laptop (WSL, montada en solo
lectura) y estos archivos se reemplazaron por los reales. Lo único que se
escribió en la tarjeta fue la llave nueva de la laptop en
`/home/david/.ssh/authorized_keys`.

No se incluye la llave **privada** (`/etc/nids/id_ed25519`): solo existe dentro del
Pi y nunca debe salir de ahí. Tampoco se incluyen los PCAP capturados
(`/mnt/nids/pcap/`).

## Qué valores se comprobaron y cómo

| Valor en `nids.conf` | Evidencia |
|---|---|
| `VM_HOST=40.116.67.213` | Es la IP pública de la VM `nids-vm` (portal de Azure, Configuración de red) y la VM registra al Pi conectándose a ella |
| `VM_USUARIO=nids-sensor` | La VM registra `Accepted publickey for nids-sensor` desde la IP de la universidad |
| `VM_PUERTO=22` | Las conexiones del Pi llegan al puerto 22 de la VM |
| `SENSOR_ID=nids-pi-01` | La llave del Pi se llama `root@nids-pi-01` (creada por `instalar.sh` línea 43) y los datos llegan como `nids-pi-01` |
| `CLAVE_SSH=/etc/nids/id_ed25519` | Es la ruta donde `instalar.sh` línea 43 crea la llave |
| `LIMITE_USO_DISCO_PCT=70` | **Comprobado** en la microSD. `instalar.sh` línea 59 lo baja a 70 cuando no hay disco USB, y el Pi captura en la microSD |

La huella de la llave pública de esta carpeta coincide con la que la VM tiene
autorizada:

```text
256 SHA256:VJY8dPYHQqCn2w8td0a0UZJ0kZGEgPdZkz2Zgi6uzac root@nids-pi-01 (ED25519)
```

Se puede comprobar en la VM:

```bash
sudo journalctl -u ssh --since "1 hour ago" | grep "Accepted publickey for nids-sensor" | tail -3
```

Y en el Pi, desde su red, la huella de su llave debe ser la misma:

```bash
ssh-keygen -lf /etc/nids/id_ed25519.pub
```

Los scripts de `pi/bin/` y `pi/systemd/` no han cambiado desde que se instaló el
Pi (único commit: `e77979b`, 30-oct-2026), por eso la copia corresponde a la
versión instalada. Si alguien editó un archivo directamente dentro del Pi, esa
edición no está aquí.

## Cómo se conecta el Pi a la nube

No hay API ni URL: el Pi se conecta a la VM de Azure por **SSH**, al puerto 22 de
`40.116.67.213`, con su llave privada. Los valores salen de `etc/nids/nids.conf`.

| Camino | Archivo | Línea | Qué hace |
|---|---|---|---|
| Archivos de tráfico | `opt/nids/bin/nids-subida.sh` | 6 | Lee la configuración (`source /etc/nids/nids.conf`) |
| | | 8-9 | Opciones SSH: llave (`CLAVE_SSH`) y puerto (`VM_PUERTO`) |
| | | 19 | `sftp ... nids-sensor@40.116.67.213`: sube los PCAP recortados |
| | | 20-21 | Los deja en `/srv/nids/incoming` de la VM |
| Telemetría | `etc/systemd/system/nids-tunel.service` | 7 | Lee la configuración (`EnvironmentFile=/etc/nids/nids.conf`) |
| | | 13 | `-L 127.0.0.1:1883:127.0.0.1:1883`: el túnel hacia Mosquitto en la VM |
| | | 14 | Destino `nids-sensor@40.116.67.213` |
| | `opt/nids/bin/nids-telemetria.py` | 114 y 124 | Publica por MQTT en `127.0.0.1:1883`, que el túnel lleva a la VM |

Nota: la línea 22 de `nids.conf` dice "IP pública de Oracle" porque es el
comentario de la plantilla original; la VM del proyecto está en Azure.

## Inventario completo de la microSD (12-nov-2026)

Además de lo anterior, se copió todo lo demás que el NIDS tiene en el Pi:

| En esta carpeta | En el Pi | Qué es |
|---|---|---|
| `home/david/nids-pi/` | `/home/david/nids-pi/` | Carpeta con que se instaló el sensor (`instalar.sh`, `bin/`, `systemd/`, `nids.conf.example`, `clave/`). Idéntica a `pi/` del commit `e77979b` |
| `home/david/nids-wifi.sh` | `/home/david/nids-wifi.sh` | Script para guardar redes WiFi. Idéntico a `pi/bin/nids-wifi.sh` |
| `etc/fstab` | `/etc/fstab` | Incluye `/var/lib/nids /mnt/nids none bind,nofail`: los PCAP van a la microSD |
| `etc/hostname` | `/etc/hostname` | `nids-pi` |
| `etc/ssh/sshd_config.d/50-cloud-init.conf` | igual | `PasswordAuthentication no`: al Pi solo se entra con llave |
| `sistema_del_pi.txt` | (resumen) | Sistema, servicios y timers habilitados, NTP, versiones de los paquetes y conteo de PCAP |

**No se copiaron, a propósito:**

- Llaves privadas: `/etc/nids/id_ed25519`, `/home/david/nids-pi/clave/id_ed25519`
  (la misma llave del sensor) y `/etc/ssh/ssh_host_*_key`. Con ellas cualquiera
  podría hacerse pasar por el sensor o por el Pi.
- Archivos del escritorio sin relación con el NIDS que contienen secretos de
  sesión (`.Xauthority`, cookie de audio) o son los de fábrica (`.bashrc`, `.profile`).

**Los PCAP** (39 140 archivos y 38 123 marcas `.subido`/`.omitido`, del
30-oct al 12-nov-2026) se copiaron fuera del repo, en
`Documents\respaldo_pi_20261112\todos_los_pcap\`.
