#!/usr/bin/env bash
# Instala el sensor en un Raspberry Pi 5 con Raspberry Pi OS Lite 64-bit.
# Uso (desde la carpeta pi/ copiada al Pi):
#   sudo ./instalar.sh --vm <IP_PUBLICA_VM> [--puerto 22] [--disco /dev/sda1] [--sensor nids-pi-01]
# No formatea ningún disco: usa el sistema de archivos que el USB ya tiene.
# Sin --disco (o si no hay USB) captura en la microSD, solo para pruebas.
set -euo pipefail

VM_HOST="" ; VM_PUERTO=22 ; DISCO="" ; SENSOR_ID=nids-pi-01
while [[ $# -gt 0 ]]; do
  case "$1" in
    --vm) VM_HOST="$2"; shift 2 ;;
    --puerto) VM_PUERTO="$2"; shift 2 ;;
    --disco) DISCO="$2"; shift 2 ;;
    --sensor) SENSOR_ID="$2"; shift 2 ;;
    *) echo "opción desconocida: $1"; exit 1 ;;
  esac
done
[[ -z "$VM_HOST" ]] && { echo "falta --vm <IP>"; exit 1; }
cd "$(dirname "$0")"

echo "== 1. Paquetes"
export DEBIAN_FRONTEND=noninteractive
echo "wireshark-common wireshark-common/install-setuid boolean false" | debconf-set-selections
apt-get update
apt-get install -y tcpdump wireshark-common autossh python3-paho-mqtt gzip exfatprogs

echo "== 2. Archivos del sensor"
install -d /opt/nids/bin /etc/nids
install -m 755 bin/nids-captura.sh bin/nids-subida.sh bin/nids-telemetria.py /opt/nids/bin/
if [[ ! -f /etc/nids/nids.conf ]]; then
  sed -e "s/^SENSOR_ID=.*/SENSOR_ID=${SENSOR_ID}/" \
      -e "s/^VM_HOST=.*/VM_HOST=${VM_HOST}/" \
      -e "s/^VM_PUERTO=.*/VM_PUERTO=${VM_PUERTO}/" \
      nids.conf.example > /etc/nids/nids.conf
fi
if [[ -f clave/id_ed25519 ]]; then
  # Clave generada y autorizada en la VM de antemano (se instala sin pasos extra).
  install -m 600 clave/id_ed25519 /etc/nids/id_ed25519
  install -m 644 clave/id_ed25519.pub /etc/nids/id_ed25519.pub
  [[ -f clave/known_hosts ]] && install -m 644 clave/known_hosts /etc/nids/known_hosts
elif [[ ! -f /etc/nids/id_ed25519 ]]; then
  ssh-keygen -t ed25519 -N "" -C "root@${SENSOR_ID}" -f /etc/nids/id_ed25519 >/dev/null
fi

echo "== 3. Almacenamiento en /mnt/nids"
install -d /mnt/nids
sed -i '\#/mnt/nids#d' /etc/fstab
if [[ -n "$DISCO" && -b "$DISCO" ]]; then
  UUID=$(blkid -s UUID -o value "$DISCO")
  TIPO=$(blkid -s TYPE -o value "$DISCO")
  OPCIONES="defaults,nofail,noatime,x-systemd.device-timeout=15"
  [[ "$TIPO" == "vfat" || "$TIPO" == "exfat" ]] && OPCIONES="${OPCIONES},uid=0,gid=0,umask=022"
  echo "UUID=${UUID} /mnt/nids ${TIPO} ${OPCIONES} 0 2" >> /etc/fstab
  echo "   disco ${DISCO} (${TIPO}) montado por UUID"
else
  install -d /var/lib/nids
  echo "/var/lib/nids /mnt/nids none bind,nofail 0 0" >> /etc/fstab
  sed -i 's/^LIMITE_USO_DISCO_PCT=.*/LIMITE_USO_DISCO_PCT=70/' /etc/nids/nids.conf
  echo "   SIN disco USB: capturando en la microSD (solo pruebas, límite 70%)"
fi
systemctl daemon-reload
mount /mnt/nids || mount -a

echo "== 4. Servicios"
install -m 644 systemd/*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now nids-captura nids-subida nids-tunel nids-telemetria
# Hora exacta: sin NTP, las ventanas de Spark no calzan con la realidad.
timedatectl set-ntp true

echo
echo "Listo. Autoriza esta clave en la VM con:"
echo "  sudo ./desplegar.sh --clave-pi \"$(cat /etc/nids/id_ed25519.pub)\""
