#!/usr/bin/env bash
# Vigía del sensor: lo corre nids-vigia.timer cada 2 min. Revisa que cada
# servicio esté avanzando de verdad (no solo "activo") y reinicia el que se
# quedó colgado. Cada reinicio queda en el journal: journalctl -u nids-vigia
set -uo pipefail
source /etc/nids/nids.conf

SEGUNDOS_POR_ARCHIVO=${SEGUNDOS_POR_ARCHIVO:-30}
MARCA_SUBIDA=/run/nids-subida/ultima_ok
ahora=$(date +%s)

edad() {  # segundos desde la última modificación de un archivo (enorme si no existe)
  if [[ -e "$1" ]]; then echo $(( ahora - $(stat -c %Y "$1") )); else echo 999999; fi
}

reiniciar() {
  local servicio="$1" motivo="$2"
  # Un servicio que alguien detuvo o deshabilitó a propósito no se toca.
  systemctl is-enabled --quiet "$servicio" || return 0
  echo "vigia: reinicio $servicio — $motivo"
  systemctl restart "$servicio"
}

# 1. Captura: tcpdump crea un archivo nuevo cada SEGUNDOS_POR_ARCHIVO.
ultimo_pcap=$(ls -1 "$DIR_CAPTURA"/*.pcap 2>/dev/null | sort | tail -1)
limite_captura=$(( 3 * SEGUNDOS_POR_ARCHIVO + 30 ))
if [[ -z "$ultimo_pcap" ]] || (( $(edad "$ultimo_pcap") > limite_captura )); then
  reiniciar nids-captura "sin PCAP nuevo en más de ${limite_captura} s"
fi

# 2. Subida: hay archivos terminados sin subir (de 2 a 30 min de antigüedad;
#    los más viejos se omiten a propósito) y ninguna subida correcta en 5 min.
if [[ "${HABILITAR_SUBIDA:-1}" == "1" ]]; then
  pendientes=0
  while read -r archivo; do
    [[ -e "${archivo}.subido" || -e "${archivo}.omitido" ]] && continue
    e=$(edad "$archivo")
    (( e > 120 && e < 1800 )) && pendientes=$(( pendientes + 1 ))
  done < <(ls -1 "$DIR_CAPTURA"/*.pcap 2>/dev/null | sort | head -n -1)
  if (( pendientes > 0 && $(edad "$MARCA_SUBIDA") > 300 )); then
    reiniciar nids-subida "${pendientes} PCAP pendientes y ninguna subida correcta en 5 min"
  fi
fi

# 3. Túnel: el MQTT de la nube debe responder en 127.0.0.1:1883 del Pi.
if ! timeout 5 bash -c '</dev/tcp/127.0.0.1/1883' 2>/dev/null; then
  reiniciar nids-tunel "127.0.0.1:1883 no responde"
fi

# 4. Servicios caídos que systemd dejó de reintentar.
for servicio in nids-captura nids-subida nids-tunel nids-telemetria nids-respaldo; do
  if systemctl is-failed --quiet "$servicio"; then
    systemctl reset-failed "$servicio"
    reiniciar "$servicio" "estaba en estado failed"
  fi
done
exit 0
