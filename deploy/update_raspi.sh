#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

INSTALL_DIR="/opt/vip"
RUN_USER="pi"
PYTHON_BIN="python3"
SKIP_RESTART=0
USE_HERE=0
USER_EXPLICIT=0

usage() {
  cat <<'EOF'
Uso:
  sudo bash deploy/update_raspi.sh [opcoes]

  # Instalacao em /opt/vip (service systemd, user pi):
  sudo bash deploy/update_raspi.sh

  # Clone em home (ex.: /home/admin/VIP), SSH como admin:
  cd /home/admin/VIP && sudo bash deploy/update_raspi.sh --here

  # So atualizar codigo em home sem reiniciar o service (producao em /opt/vip):
  sudo bash deploy/update_raspi.sh --here --skip-restart

Opcoes:
  --here                 Usa este repositorio (pai de deploy/) como INSTALL_DIR;
                         com sudo a partir de um login normal, RUN_USER fica SUDO_USER (ex. admin).
  --install-dir DIR      Diretorio de instalacao (padrao: /opt/vip; ignorado se --here veio antes)
  --user USER            Usuario dono da aplicacao (padrao: pi; sobrescreve o deduzido por --here)
  --python-bin BIN       Python para instalar deps (padrao: python3)
  --skip-restart         Nao reinicia o service no final
  -h, --help             Mostra ajuda
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --here)
      USE_HERE=1
      INSTALL_DIR="$REPO_ROOT"
      shift
      ;;
    --install-dir)
      INSTALL_DIR="$2"
      USE_HERE=0
      shift 2
      ;;
    --user)
      RUN_USER="$2"
      USER_EXPLICIT=1
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

if [[ "$USE_HERE" -eq 1 ]]; then
  INSTALL_DIR="$REPO_ROOT"
  if [[ "$USER_EXPLICIT" -eq 0 ]] && [[ -n "${SUDO_USER:-}" ]]; then
    RUN_USER="$SUDO_USER"
  fi
  if [[ "$USER_EXPLICIT" -eq 0 ]] && [[ "$RUN_USER" == "pi" ]] && [[ "$INSTALL_DIR" != "/opt/vip" ]]; then
    echo "Com --here, corre como: sudo bash deploy/update_raspi.sh --here"
    echo "(assim RUN_USER fica o teu login, ex. admin) ou passa explicitamente: --here --user admin"
    exit 1
  fi
fi

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

echo "[1/5] Atualizando codigo (fetch + limpar nao rastreados + reset para origin)..."
# pull --ff-only falha com modificacoes locais ou arquivos soltos que conflitam com o repo.
# Em instalacao tipo appliance, o codigo deve espelhar o GitHub; dados ficam em data/ (gitignore).
# -c safe.directory=... evita "dubious ownership" (ex.: repo dono pi, SSH como admin).
GSAFE=(git -c "safe.directory=$INSTALL_DIR")
BRANCH=$(sudo -u "$RUN_USER" "${GSAFE[@]}" -C "$INSTALL_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)
if [[ -z "$BRANCH" || "$BRANCH" == "HEAD" ]]; then
  BRANCH="main"
fi
sudo -u "$RUN_USER" "${GSAFE[@]}" -C "$INSTALL_DIR" fetch origin "$BRANCH"
sudo -u "$RUN_USER" "${GSAFE[@]}" -C "$INSTALL_DIR" clean -fd
sudo -u "$RUN_USER" "${GSAFE[@]}" -C "$INSTALL_DIR" reset --hard "origin/$BRANCH"

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
# stdin sem cwd deixa de incluir o pacote app no path; e obrigatorio cd ao INSTALL_DIR.
if [[ -x "$INSTALL_DIR/.venv/bin/python" ]]; then
  _vip_py="$INSTALL_DIR/.venv/bin/python"
else
  _vip_py="$PYTHON_BIN"
fi
sudo -u "$RUN_USER" bash -c 'cd "$1" && exec "$2" -c "from app.db import init_db; init_db(); print(\"DB OK\")"' bash "$INSTALL_DIR" "$_vip_py"

if [[ "$SKIP_RESTART" -eq 0 ]]; then
  echo "[5/5] Reiniciando vip-dashboard.service..."
  systemctl restart vip-dashboard.service
  systemctl status vip-dashboard.service --no-pager || true
else
  echo "[5/5] Reinicio ignorado (--skip-restart)."
fi

echo "Atualizacao concluida com sucesso."
