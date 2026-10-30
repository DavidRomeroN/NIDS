#!/usr/bin/env bash
# Único proceso de análisis de red del Pi: tcpdump escribiendo PCAP rotados
# cada SEGUNDOS_POR_ARCHIVO al disco USB. Lo arranca nids-captura.service.
set -euo pipefail
source /etc/nids/nids.conf

mkdir -p "$DIR_CAPTURA"
# Modo promiscuo y sin IP: en el puerto espejo la interfaz solo escucha.
ip link set "$INTERFAZ_CAPTURA" up promisc on

exec tcpdump -i "$INTERFAZ_CAPTURA" -n \
  -s "$SNAPLEN_LOCAL" -B 32768 \
  -G "$SEGUNDOS_POR_ARCHIVO" \
  -w "${DIR_CAPTURA}/${SENSOR_ID}_%Y%m%d_%H%M%S.pcap" \
  -Z root \
  $FILTRO_BPF
