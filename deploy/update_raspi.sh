#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/vip"
RUN_USER="pi"
PYTHON_BIN="python3"
SKIP_RESTART=0

usage() {
  cat <<'EOF'
Uso:
  sudo bash deploy/update_raspi.sh [opcoes]

Opcoes:
  --install-dir DIR      Diretorio de instalacao (padrao: /opt/vip)
  --user USER            Usuario dono da aplicacao (padrao: pi)
  --python-bin BIN       Python para instalar deps (padrao: python3)
  --skip-restart         Nao reinicia o service no final
  -h, --help             Mostra ajuda
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    --user)
      RUN_USER="$2"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --skip-restart)
      SKIP_RESTART=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Opcao invalida: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Execute como root: sudo bash deploy/update_raspi.sh"
  exit 1
fi

if [[ ! -d "$INSTALL_DIR" ]]; then
  echo "Diretorio nao encontrado: $INSTALL_DIR"
  exit 1
fi

if ! id "$RUN_USER" >/dev/null 2>&1; then
  echo "Usuario nao encontrado: $RUN_USER"
  exit 1
fi

HAS_GIT=0
if [[ -d "$INSTALL_DIR/.git" ]]; then
  HAS_GIT=1
fi

if [[ "$HAS_GIT" -eq 1 ]]; then
  echo "[1/5] Atualizando codigo (git pull)..."
  sudo -u "$RUN_USER" git -C "$INSTALL_DIR" pull --ff-only
else
  echo "[1/5] Repositorio sem .git; pulando git pull."
fi

echo "[2/5] Instalando/atualizando dependencias Python..."
if [[ -x "$INSTALL_DIR/.venv/bin/pip" ]]; then
  sudo -u "$RUN_USER" "$INSTALL_DIR/.venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"
else
  sudo -u "$RUN_USER" "$PYTHON_BIN" -m pip install --break-system-packages -r "$INSTALL_DIR/requirements.txt"
fi

echo "[3/5] Verificando sintaxe Python..."
if [[ -x "$INSTALL_DIR/.venv/bin/python" ]]; then
  sudo -u "$RUN_USER" "$INSTALL_DIR/.venv/bin/python" -m compileall "$INSTALL_DIR/app"
else
  sudo -u "$RUN_USER" "$PYTHON_BIN" -m compileall "$INSTALL_DIR/app"
fi

echo "[4/5] Aplicando migracoes (init DB)..."
if [[ -x "$INSTALL_DIR/.venv/bin/python" ]]; then
  sudo -u "$RUN_USER" "$INSTALL_DIR/.venv/bin/python" - <<PY
from app.db import init_db
init_db()
print("DB OK")
PY
else
  sudo -u "$RUN_USER" "$PYTHON_BIN" - <<PY
from app.db import init_db
init_db()
print("DB OK")
PY
fi

if [[ "$SKIP_RESTART" -eq 0 ]]; then
  echo "[5/5] Reiniciando vip-dashboard.service..."
  systemctl restart vip-dashboard.service
  systemctl status vip-dashboard.service --no-pager || true
else
  echo "[5/5] Reinicio ignorado (--skip-restart)."
fi

echo "Atualizacao concluida com sucesso."
