#!/usr/bin/env bash
# Sube a la nube una copia recortada (solo cabeceras) de cada PCAP terminado,
# y aplica la retención local del disco. No analiza nada: solo copia y borra.
# Si la nube no responde, los archivos quedan pendientes y se suben después.
set -uo pipefail
source /etc/nids/nids.conf

SSH_OPCIONES=(-i "$CLAVE_SSH" -P "$VM_PUERTO" -o BatchMode=yes -o ConnectTimeout=15
              -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/etc/nids/known_hosts)
TMP=/run/nids-subida
mkdir -p "$TMP"

subir() {
  local origen="$1" base
  base=$(basename "$origen" .pcap)
  editcap -F pcap -s "$SNAPLEN_NUBE" "$origen" "$TMP/$base.pcap" || return 1
  gzip -1 -f "$TMP/$base.pcap" || return 1
  # Se sube como .tmp y se renombra al final: el ingestor nunca lee un archivo a medias.
  sftp "${SSH_OPCIONES[@]}" -b - "${VM_USUARIO}@${VM_HOST}" >/dev/null <<EOF || { rm -f "$TMP/$base.pcap.gz"; return 1; }
put $TMP/$base.pcap.gz incoming/$base.pcap.gz.tmp
rename incoming/$base.pcap.gz.tmp incoming/$base.pcap.gz
EOF
  rm -f "$TMP/$base.pcap.gz"
  touch "${origen}.subido"
}

aplicar_retencion() {
  # Modo anillo: con el disco sobre el límite, se borra el PCAP más viejo.
  while true; do
    uso=$(df --output=pcent "$DIR_CAPTURA" | tail -1 | tr -dc '0-9')
    (( uso <= LIMITE_USO_DISCO_PCT )) && break
    viejo=$(ls -1 "$DIR_CAPTURA"/*.pcap 2>/dev/null | sort | head -1)
    [[ -z "$viejo" ]] && break
    rm -f "$viejo" "${viejo}.subido" "${viejo}.omitido"
    echo "retencion: borrado $(basename "$viejo") (disco al ${uso}%)"
  done
}

while true; do
  if [[ "$HABILITAR_SUBIDA" == "1" ]]; then
    # Todos menos el último: ese es el que tcpdump está escribiendo ahora.
    mapfile -t terminados < <(ls -1 "$DIR_CAPTURA"/*.pcap 2>/dev/null | sort | head -n -1)
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
