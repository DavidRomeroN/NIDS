#!/usr/bin/env bash
# Actualiza un Pi que YA tiene el sensor instalado: copia los scripts y
# servicios nuevos y los reinicia. No toca /etc/nids/nids.conf (los valores
# nuevos tienen un valor por defecto dentro de cada script), ni la llave, ni
# el disco ni el fstab. Uso (desde la carpeta pi/ copiada al Pi):
#   sudo ./actualizar.sh
set -euo pipefail
cd "$(dirname "$0")"

echo "== 1. Dependencias nuevas"
command -v curl >/dev/null || { apt-get update && apt-get install -y curl; }

echo "== 2. Scripts"
install -m 755 bin/nids-captura.sh bin/nids-subida.sh bin/nids-telemetria.py \
               bin/nids-vigia.sh bin/nids-respaldo.sh /opt/nids/bin/

echo "== 3. Servicios"
install -m 644 systemd/*.service systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable nids-respaldo nids-vigia.timer
systemctl restart nids-subida nids-telemetria nids-respaldo
systemctl start nids-vigia.timer

echo
systemctl --no-pager --plain list-units 'nids-*' | head -n 12
echo
echo "Listo. Revisa con:  journalctl -u nids-subida -u nids-vigia -u nids-respaldo -n 30"
