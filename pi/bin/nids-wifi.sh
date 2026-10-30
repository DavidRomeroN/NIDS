#!/usr/bin/env bash
# Guarda una red WiFi para la SUBIDA a la nube (eth0 queda solo para el puerto
# espejo, que no da internet). Pide nombre y clave por teclado: la clave no
# queda en el historial ni en el repositorio. Uso: bash nids-wifi.sh [nombre-perfil] [prioridad]
set -euo pipefail

PERFIL="${1:-celular}"
PRIORIDAD="${2:-10}"

read -r -p "Nombre de la red WiFi: " RED
read -r -s -p "Clave de esa red: " CLAVE
echo

sudo nmcli radio wifi on
sudo nmcli connection delete "$PERFIL" >/dev/null 2>&1 || true
sudo nmcli connection add type wifi ifname wlan0 con-name "$PERFIL" ssid "$RED" \
  wifi-sec.key-mgmt wpa-psk wifi-sec.psk "$CLAVE" \
  connection.autoconnect yes connection.autoconnect-priority "$PRIORIDAD" >/dev/null

echo "Red guardada como '$PERFIL'. Perfiles del Pi:"
nmcli -t -f NAME,TYPE,AUTOCONNECT-PRIORITY connection show
echo "Esperando 20 s por si la red ya esta encendida..."
sleep 20
nmcli -t -f DEVICE,STATE,CONNECTION device
