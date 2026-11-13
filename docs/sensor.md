# Sensor Raspberry Pi 5

## 1. Rol

El Pi es un **sensor pasivo**: escucha una copia del tráfico en el puerto espejo
del switch y no puede bloquear ni modificar nada. Es el único equipo del
proyecto que está físicamente en el campus.

**Estado actual:** este documento describe el diseño con puerto espejo. Hoy el Pi
está en un **puerto normal** de la sala docentes: ve anuncios de la red, su propio
tráfico y fragmentos del de otras PC. El 99,8 % de los flujos revisados el 6-nov-2026
se vio en una sola dirección.

## 2. Hardware

| Equipo | Especificación | Rol |
|---|---|---|
| Raspberry Pi 5 | 4× Cortex-A76 2,4 GHz, 8 GB RAM, 1 GbE | Captura |
| microSD 128 GB | Raspberry Pi OS Lite 64-bit | Sistema operativo y PCAP mientras no haya disco USB (tope 70 %) |
| Disco USB 3.0 | Disco externo de escritorio (6 meses de tesis) | PCAP locales |
| `eth0` (puerto del Pi) | Puerto espejo (sin IP, modo promiscuo) | Interfaz de captura |
| Adaptador USB 3.0 a Ethernet | Puerto normal del switch, con DHCP e internet | Subida a la nube 24/7 |
| `wlan0` | Hotspot del celular o WiFi del campus (`pi/bin/nids-wifi.sh`) | Respaldo de la subida |

El Pi 5 trae un solo puerto de red y ese queda en el espejo, que no da
internet. Por eso la subida va por un segundo cable con un adaptador USB: no
necesita configuración (NetworkManager le da DHCP al enchufarlo) y la captura
igual excluye el tráfico hacia la nube (`FILTRO_BPF="not host <VM>"`). Al
switch se le piden dos puertos: el espejo y uno normal con salida TCP al 22
(o 443) hacia la VM.

## 3. Servicios (systemd)

Todos arrancan solos al enchufar el Pi y se reinician si fallan.

| Servicio | Script | Qué hace |
|---|---|---|
| `nids-captura` | `pi/bin/nids-captura.sh` | `tcpdump -s 512 -G 30` al disco USB, filtrando el propio canal hacia la nube |
| `nids-subida` | `pi/bin/nids-subida.sh` | `editcap -s 128`, gzip y SFTP de cada archivo terminado; retención en anillo |
| `nids-tunel` | `systemd/nids-tunel.service` | `autossh -L 1883` hacia el MQTT de la nube |
| `nids-telemetria` | `pi/bin/nids-telemetria.py` | Telemetría cada 5 s por MQTT |
| `nids-vigia` (timer, cada 2 min) | `pi/bin/nids-vigia.sh` | Reinicia la captura, la subida o el túnel si dejaron de avanzar |
| `nids-respaldo` | `pi/bin/nids-respaldo.sh` | Tráfico de respaldo marcado cuando el sensor no escucha tráfico |

### Cortes de la subida

**12-nov-2026, 10:52: la subida se detuvo sin ningún error.** La causa real
(confirmada con los registros del Pi y una prueba con 39 200 archivos): la
subida buscaba los PCAP con `ls "$DIR_CAPTURA"/*.pcap`. El comodín mete todos
los nombres en un solo comando y Linux lo limita a 2 MB (`ARG_MAX`), unos
38 100 archivos. El Pi guarda todos sus PCAP en la microSD y a las 10:52 tenía
38 123: `ls` falló con `Argument list too long`, la lista salió vacía y la
subida creyó que no había nada que subir. La captura y la telemetría seguían
funcionando. Ahora `nids-subida.sh`, `nids-vigia.sh` y `nids-respaldo.sh`
buscan con `find`, que no tiene ese límite, y la subida revisa solo los PCAP
de los últimos 30 minutos (los más viejos los marca `.omitido` una vez al
arrancar).

Además, `sftp` corría sin `ServerAlive` ni tope de tiempo: si la conexión
quedaba a medias, esperaba para siempre (cortes de 17 a 20 min). Ahora la
subida tiene estas defensas:

1. `ServerAliveInterval=10` y `ServerAliveCountMax=3` en `nids-subida.sh`:
   ssh detecta la conexión muerta en unos 30 s.
2. `timeout` de `SEGUNDOS_MAX_SUBIDA` (90 s) por archivo: si una subida se
   pasa de ese tiempo, se corta y se reintenta.
3. `nids-vigia`: si hay PCAP pendientes y ninguna subida correcta en 5 min,
   reinicia `nids-subida`. También reinicia `nids-captura` sin archivos
   nuevos y `nids-tunel` si `127.0.0.1:1883` no responde.

Los registros del Pi ahora se guardan entre reinicios (Raspberry Pi OS trae
`Storage=volatile`): `/etc/systemd/journald.conf.d/50-nids-persistente.conf`
con `Storage=persistent` y un tope de 200 MB. Ver con
`journalctl -u nids-subida -u nids-vigia -b -1`.

### Tráfico de respaldo

Si en el último minuto el sensor escuchó menos de `UMBRAL_PAQUETES_MIN` (10)
paquetes de **otros equipos**, el Pi genera un poco de tráfico normal: 3
consultas DNS y una petición HTTPS liviana a un sitio de `SITIOS_RESPALDO`.
Así el pipeline no queda sin datos.

- **Tamaño:** pensado para parecerse a un minuto típico del sensor (mediana
  real ~5 KB y ~24 flujos por minuto en 18 972 minutos): solo DNS y una
  petición HTTPS de cabeceras, sin descargar páginas, para no fabricar picos
  que el modelo marque como anomalía. El tamaño exacto falta medirlo en el Pi.
- **Marcado, nunca como tráfico real:** cada generación queda en
  `/run/nids/trafico_respaldo`, en `journalctl -u nids-respaldo` y en la
  telemetría como `"traficoRespaldo": true`.
- **Límite:** solo funciona cuando `eth0` tiene IP, como hoy. En un puerto
  espejo de verdad lo generado saldría por otra interfaz y el sensor no lo
  escucharía.
- **Apagarlo:** `TRAFICO_RESPALDO=0` en `/etc/nids/nids.conf` y
  `sudo systemctl restart nids-respaldo`.

## 4. Decisiones

- **512 bytes por paquete en el disco local:** conserva cabeceras y el saludo
  TLS (útil para la tesis: huellas JA3/JA4) ocupando una fracción del tráfico completo.
- **128 bytes por paquete hacia la nube:** solo cabeceras, el mínimo para
  reconstruir flujos; el largo real del paquete sale de la cabecera IP.
- **Archivos de 30 s:** equilibrio entre latencia y cantidad de archivos.
- **Retención en anillo:** sobre el 90 % de disco se borra el PCAP más viejo;
  la captura nunca se detiene. Con el disco grande de la tesis se sube a 97 %.
- **Nube caída:** los archivos de más de 30 min se marcan `.omitido` y quedan
  solo en el disco local (llegarían fuera del watermark de Spark).
- **NTP activo:** sin hora exacta, las ventanas de Spark no coinciden con la realidad.

## 5. Telemetría (IoT)

Cada 5 s el Pi publica en `nids/<sensorId>/telemetria` (contrato en
[Ingesta](ingesta.md#2-pi-telemetria-sensor-iot-v10)): temperatura del SoC, CPU,
RAM, contadores de la interfaz espejo, paquetes descartados por el kernel,
disco libre y archivos pendientes de subir. En Grafana se ve en la fila
"Sensor IoT", y el panel "Interfaz espejo vs pipeline" compara lo que ve la
tarjeta de red con lo que procesó Spark (si divergen, se están perdiendo datos).

## 6. Instalación

```bash
sudo ./instalar.sh --vm <IP_PUBLICA_VM> --disco /dev/sda1
```

El script instala paquetes, crea `/etc/nids/nids.conf`, genera la clave SSH
del sensor, monta el disco por UUID (sin formatearlo) y habilita los servicios.
Al final imprime la clave pública que hay que autorizar en la VM
(ver [Despliegue](despliegue.md)).

Para un Pi que ya tiene el sensor instalado, `actualizar.sh` copia solo los
scripts y servicios nuevos y los reinicia, sin tocar `nids.conf`, la llave
ni el disco:

```bash
sudo ./actualizar.sh
```
