#!/usr/bin/env bash
# Prepara la VM de Oracle Cloud (Ubuntu 24.04, ARM64) y levanta el stack.
# Uso, desde proyecto-nids/nube/ en la VM:
#   sudo ./desplegar.sh                      # primera vez
#   sudo ./desplegar.sh --clave-pi "ssh-ed25519 AAAA... root@nids-pi"
# Es idempotente: se puede volver a correr sin romper nada.
set -euo pipefail

CLAVE_PI=""
if [[ "${1:-}" == "--clave-pi" ]]; then
  CLAVE_PI="${2:?falta la clave pública del Pi}"
fi

cd "$(dirname "$0")"

echo "== 1. Docker (paquetes oficiales de Ubuntu)"
if ! command -v docker >/dev/null; then
  apt-get update
  apt-get install -y docker.io docker-compose-v2
  systemctl enable --now docker
fi
usermod -aG docker "${SUDO_USER:-ubuntu}" || true

echo "== 2. Firewall del sistema operativo"
# Las imágenes Ubuntu de Oracle traen iptables con un REJECT al final de INPUT:
# abrir el puerto en la consola de Oracle (Security List) no basta. En Azure/AWS
# INPUT viene vacío y esto no cambia nada (ahí manda el grupo de seguridad).
for puerto in 3000 443; do
  if ! iptables -C INPUT -p tcp --dport "$puerto" -m state --state NEW -j ACCEPT 2>/dev/null; then
    iptables -I INPUT 1 -p tcp --dport "$puerto" -m state --state NEW -j ACCEPT
  fi
done
if command -v netfilter-persistent >/dev/null; then
  netfilter-persistent save
fi

echo "== 3. SSH también en el puerto 443"
# Muchas redes de campus bloquean la salida al 22 pero no al 443. Ubuntu 24.04
# arranca sshd por socket (ssh.socket); cambiar ese socket en caliente dejó la
# VM sin SSH. Se pasa a sshd clásico con los puertos en sshd_config.d.
printf 'Port 22\nPort 443\n' > /etc/ssh/sshd_config.d/10-puertos.conf
sshd -t
systemctl disable --now ssh.socket 2>/dev/null || true
systemctl enable ssh
systemctl restart ssh

echo "== 4. Usuario restringido para el sensor"
# El Pi solo puede: subir archivos por SFTP a /srv/nids/incoming y abrir un
# túnel hacia el MQTT local (127.0.0.1:1883). Nada de shell.
if ! id nids-sensor >/dev/null 2>&1; then
  useradd --system --home-dir /srv/nids --shell /usr/sbin/nologin nids-sensor
fi
mkdir -p /srv/nids/incoming /srv/nids/procesados /srv/nids/.ssh
chown nids-sensor:nids-sensor /srv/nids /srv/nids/incoming
chmod 700 /srv/nids/.ssh
touch /srv/nids/.ssh/authorized_keys
if [[ -n "$CLAVE_PI" ]] && ! grep -qF "$CLAVE_PI" /srv/nids/.ssh/authorized_keys; then
  echo "restrict,port-forwarding,permitopen=\"127.0.0.1:1883\" $CLAVE_PI" >> /srv/nids/.ssh/authorized_keys
  echo "   clave del Pi autorizada"
fi
chown -R nids-sensor:nids-sensor /srv/nids/.ssh
chmod 600 /srv/nids/.ssh/authorized_keys
# nologin bloquea también sftp/túnel: se fuerza el sftp interno para este usuario.
if ! grep -q "Match User nids-sensor" /etc/ssh/sshd_config; then
  cat >> /etc/ssh/sshd_config <<'EOF'

Match User nids-sensor
    ForceCommand internal-sftp
    AllowTcpForwarding local
    PermitTTY no
EOF
  systemctl reload ssh || true
fi

echo "== 5. Secretos (.env)"
if [[ ! -f .env ]]; then
  cat > .env <<EOF
GRAFANA_ADMIN_PASSWORD=$(openssl rand -base64 18 | tr -d '/+=' | cut -c1-16)
SECRETO_SEUDONIMO=$(openssl rand -hex 32)
EOF
  chmod 600 .env
  chown "${SUDO_USER:-ubuntu}" .env
fi

echo "== 6. Stack"
docker compose up -d --build
docker compose ps

IP_PUBLICA=$(curl -s --max-time 5 https://ifconfig.me || echo "<ip-publica>")
echo
echo "Grafana: http://${IP_PUBLICA}:3000   usuario: admin"
echo "Contraseña: $(grep GRAFANA_ADMIN_PASSWORD .env | cut -d= -f2)"
