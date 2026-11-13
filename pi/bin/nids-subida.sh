#!/usr/bin/env bash
# Sube a la nube una copia recortada (solo cabeceras) de cada PCAP terminado,
# y aplica la retención local del disco. No analiza nada: solo copia y borra.
# Si la nube no responde, los archivos quedan pendientes y se suben después.
set -uo pipefail
source /etc/nids/nids.conf

# ServerAlive: si la conexión queda a medias (corte de red, NAT del campus),
# ssh lo detecta en ~30 s y sale con error. Sin esto, sftp esperaba respuesta
# para siempre y la subida quedaba colgada desde 17 min hasta varias horas.
SSH_OPCIONES=(-i "$CLAVE_SSH" -P "$VM_PUERTO" -o BatchMode=yes -o ConnectTimeout=15
              -o ServerAliveInterval=10 -o ServerAliveCountMax=3
              -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/etc/nids/known_hosts)
# Tope duro por archivo: un PCAP recortado de 30 s pesa pocos KB y sube en
# segundos; si una subida pasa de este tiempo se corta y se reintenta.
SEGUNDOS_MAX_SUBIDA=${SEGUNDOS_MAX_SUBIDA:-90}
TMP=/run/nids-subida
mkdir -p "$TMP"

# PCAP locales ordenados por nombre (= por hora). Con find y no con
# `ls "$DIR_CAPTURA"/*.pcap`: el comodín mete todos los nombres en un solo
# comando, y pasados ~38 000 archivos supera el límite de Linux (ARG_MAX, 2 MB).
# Entonces ls falla con "Argument list too long", la lista sale vacía y la
# subida se detiene sin ningún error visible (pasó el 12-nov-2026 a las 10:52).
listar_pcap() {
  find "$DIR_CAPTURA" -maxdepth 1 -type f -name '*.pcap' 2>/dev/null | sort
}

subir() {
  local origen="$1" base
  base=$(basename "$origen" .pcap)
  timeout 60 editcap -F pcap -s "$SNAPLEN_NUBE" "$origen" "$TMP/$base.pcap" || return 1
  gzip -1 -f "$TMP/$base.pcap" || return 1
  # Se sube como .tmp y se renombra al final: el ingestor nunca lee un archivo a medias.
  timeout --kill-after=10 "$SEGUNDOS_MAX_SUBIDA" \
    sftp "${SSH_OPCIONES[@]}" -b - "${VM_USUARIO}@${VM_HOST}" >/dev/null <<EOF || { rm -f "$TMP/$base.pcap.gz"; return 1; }
put $TMP/$base.pcap.gz incoming/$base.pcap.gz.tmp
rename incoming/$base.pcap.gz.tmp incoming/$base.pcap.gz
EOF
  rm -f "$TMP/$base.pcap.gz"
  touch "${origen}.subido"
  # Marca para el vigía (nids-vigia.sh): hora de la última subida correcta.
  touch "$TMP/ultima_ok"
}

aplicar_retencion() {
  # Modo anillo: con el disco sobre el límite, se borra el PCAP más viejo.
  while true; do
    uso=$(df --output=pcent "$DIR_CAPTURA" | tail -1 | tr -dc '0-9')
    (( uso <= LIMITE_USO_DISCO_PCT )) && break
    viejo=$(listar_pcap | head -1)
    [[ -z "$viejo" ]] && break
    rm -f "$viejo" "${viejo}.subido" "${viejo}.omitido"
    echo "retencion: borrado $(basename "$viejo") (disco al ${uso}%)"
  done
}

# Al arrancar, una sola vez: lo que quedó sin subir hace más de 30 min (Pi
# apagado, nube caída) se marca .omitido y queda solo en el disco local.
# Así cada vuelta del ciclo revisa solo los PCAP recientes y no los miles
# que se acumulan en la microSD (unos 2 800 por día).
if [[ "$HABILITAR_SUBIDA" == "1" ]]; then
  omitidos=0
  while read -r archivo; do
    [[ -e "${archivo}.subido" || -e "${archivo}.omitido" ]] && continue
    touch "${archivo}.omitido"
    omitidos=$(( omitidos + 1 ))
  done < <(find "$DIR_CAPTURA" -maxdepth 1 -type f -name '*.pcap' -mmin +30 2>/dev/null)
  echo "arranque: ${omitidos} PCAP viejos marcados como omitidos"
fi

while true; do
  if [[ "$HABILITAR_SUBIDA" == "1" ]]; then
    # Solo los de los últimos ~31 min, todos menos el último: ese es el que
    # tcpdump está escribiendo ahora.
    mapfile -t terminados < <(find "$DIR_CAPTURA" -maxdepth 1 -type f -name '*.pcap' -mmin -31 2>/dev/null | sort | head -n -1)
    for archivo in "${terminados[@]}"; do
      [[ -e "${archivo}.subido" || -e "${archivo}.omitido" ]] && continue
      # La nube es para tiempo real: lo que quedó atrasado más de 30 min (nube
      # caída) llegaría fuera del watermark de Spark. Queda solo en el disco local.
      if (( $(date +%s) - $(stat -c %Y "$archivo") > 1800 )); then
        touch "${archivo}.omitido"
        continue
      fi
      if subir "$archivo"; then
        echo "subido: $(basename "$archivo")"
      else
        echo "nube no disponible, se reintenta en 15 s"
        break
      fi
    done
  fi
  aplicar_retencion
  sleep 15
done
