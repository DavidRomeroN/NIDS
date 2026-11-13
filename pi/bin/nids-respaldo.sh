#!/usr/bin/env bash
# Tráfico de respaldo: si en el último minuto el sensor no escuchó tráfico de
# otros equipos, genera un poco de tráfico normal desde el propio Pi (consultas
# DNS y una petición HTTPS) para que el pipeline no quede sin datos.
#
# Tamaño: parecido a un minuto típico del sensor (mediana real ~5 KB y ~24
# flujos por minuto), para no inventar picos que el modelo marque como anomalía.
#
# Queda MARCADO, no se hace pasar por tráfico real:
#   - /run/nids/trafico_respaldo guarda la hora y el sitio de cada generación,
#   - la telemetría publica "traficoRespaldo": true mientras está activo,
#   - cada generación queda en el journal: journalctl -u nids-respaldo
set -uo pipefail
source /etc/nids/nids.conf

TRAFICO_RESPALDO=${TRAFICO_RESPALDO:-1}
UMBRAL_PAQUETES_MIN=${UMBRAL_PAQUETES_MIN:-10}
SITIOS_RESPALDO=${SITIOS_RESPALDO:-"www.google.com www.wikipedia.org www.cloudflare.com www.microsoft.com www.upeu.edu.pe"}
SEGUNDOS_POR_ARCHIVO=${SEGUNDOS_POR_ARCHIVO:-30}
ESTADO=/run/nids/trafico_respaldo
mkdir -p /run/nids

if [[ "$TRAFICO_RESPALDO" != "1" ]]; then
  echo "respaldo: desactivado (TRAFICO_RESPALDO=$TRAFICO_RESPALDO)"
  exec sleep infinity
fi

ip_propia() {
  ip -4 -o addr show "$INTERFAZ_CAPTURA" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -1
}

# Paquetes de OTROS equipos en el último minuto: los PCAP terminados más
# recientes (sin el que tcpdump está escribiendo), sin contar al propio Pi.
paquetes_ajenos_ultimo_minuto() {
  local ip="$1" archivos=$(( 60 / SEGUNDOS_POR_ARCHIVO )) total=0 n archivo
  (( archivos < 1 )) && archivos=1
  while read -r archivo; do
    n=$(timeout 30 tcpdump -nn -r "$archivo" "not host $ip" 2>/dev/null | wc -l)
    total=$(( total + n ))
  done < <(find "$DIR_CAPTURA" -maxdepth 1 -type f -name '*.pcap' -mmin -5 2>/dev/null | sort | head -n -1 | tail -n "$archivos")
  echo "$total"
}

captura_al_dia() {
  local ultimo
  # find y no `ls *.pcap`: con ~38 000 archivos el comodín supera ARG_MAX (ver nids-subida.sh).
  ultimo=$(find "$DIR_CAPTURA" -maxdepth 1 -type f -name '*.pcap' -mmin -5 2>/dev/null | sort | tail -1)
  [[ -n "$ultimo" ]] && (( $(date +%s) - $(stat -c %Y "$ultimo") < 3 * SEGUNDOS_POR_ARCHIVO ))
}

generar() {
  local sitios sitio
  read -r -a sitios <<< "$SITIOS_RESPALDO"
  # 3 consultas DNS repartidas en el minuto...
  for _ in 1 2 3; do
    getent ahosts "${sitios[RANDOM % ${#sitios[@]}]}" >/dev/null 2>&1
    sleep $(( RANDOM % 8 + 2 ))
  done
  # ...y una petición HTTPS liviana (solo cabeceras, -I): unos pocos KB con el
  # handshake TLS, sin descargar la página.
  sitio="${sitios[RANDOM % ${#sitios[@]}]}"
  curl -sS -I --http1.1 --max-time 10 -o /dev/null \
       -A "Mozilla/5.0 (X11; Linux aarch64)" "https://${sitio}/" 2>/dev/null
  echo "$(date +%s) ${sitio}" > "$ESTADO"
  echo "respaldo: minuto sin tráfico ajeno, generado DNS + HTTPS a ${sitio}"
}

while true; do
  ip=$(ip_propia)
  if [[ -z "$ip" ]]; then
    # En un puerto espejo de verdad eth0 no tiene IP: lo generado saldría por
    # otra interfaz y el sensor no lo escucharía. En ese caso no tiene sentido.
    sleep 60; continue
  fi
  # Si la captura está detenida, generar no sirve: eso lo arregla nids-vigia.
  if captura_al_dia; then
    paquetes=$(paquetes_ajenos_ultimo_minuto "$ip")
    if (( paquetes < UMBRAL_PAQUETES_MIN )); then
      generar
    fi
  fi
  sleep 60
done
