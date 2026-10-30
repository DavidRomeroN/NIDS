# Sensor Raspberry Pi 5

## 1. Rol

El Pi es un **sensor pasivo**: escucha una copia del tráfico en el puerto espejo
del switch y no puede bloquear ni modificar nada. Es el único equipo del
proyecto que está físicamente en el campus.

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
