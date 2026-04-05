#!/usr/bin/env bash
set -euo pipefail

APP_NAME="vip-dashboard"
SERVICE_NAME="${APP_NAME}.service"
DEFAULT_INSTALL_DIR="/opt/vip"
DEFAULT_HOST="0.0.0.0"
DEFAULT_PORT="8000"
DEFAULT_USER="pi"
DEFAULT_CAMERA_DEVICE="/dev/video0"
DEFAULT_CAMERA_LABEL="Entrada principal"
DEFAULT_CAMERA_WIDTH="640"
DEFAULT_CAMERA_HEIGHT="360"
DEFAULT_CAMERA_FPS="8"
DEFAULT_CULTO_ANTECEDENCIA_MIN="30"
DEFAULT_CULTO_DURACAO_MIN="150"
DEFAULT_ESTIMAR_FAIXA_ETARIA="1"

INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
RUN_USER="${RUN_USER:-$DEFAULT_USER}"
APP_HOST="${APP_HOST:-$DEFAULT_HOST}"
APP_PORT="${APP_PORT:-$DEFAULT_PORT}"
CAMERA_DEVICE="${CAMERA_DEVICE:-$DEFAULT_CAMERA_DEVICE}"
CAMERA_LABEL="${CAMERA_LABEL:-$DEFAULT_CAMERA_LABEL}"
CAMERA_WIDTH="${CAMERA_WIDTH:-$DEFAULT_CAMERA_WIDTH}"
CAMERA_HEIGHT="${CAMERA_HEIGHT:-$DEFAULT_CAMERA_HEIGHT}"
CAMERA_FPS="${CAMERA_FPS:-$DEFAULT_CAMERA_FPS}"
CAMERA_ENABLED="${CAMERA_ENABLED:-1}"
CULTO_ANTECEDENCIA_MIN="${CULTO_ANTECEDENCIA_MIN:-$DEFAULT_CULTO_ANTECEDENCIA_MIN}"
CULTO_DURACAO_MIN="${CULTO_DURACAO_MIN:-$DEFAULT_CULTO_DURACAO_MIN}"
ESTIMAR_FAIXA_ETARIA="${ESTIMAR_FAIXA_ETARIA:-$DEFAULT_ESTIMAR_FAIXA_ETARIA}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"

usage() {
  cat <<EOF
Uso: sudo bash deploy/setup_raspi.sh [opcoes]

Opcoes:
  --install-dir PATH        Diretorio do app (padrao: $DEFAULT_INSTALL_DIR)
  --user USER               Usuario para executar o service (padrao: $DEFAULT_USER)
  --host HOST               Host do uvicorn (padrao: $DEFAULT_HOST)
  --port PORT               Porta do uvicorn (padrao: $DEFAULT_PORT)
  --camera-device DEVICE    Dispositivo de camera (padrao: $DEFAULT_CAMERA_DEVICE)
  --camera-label LABEL      Nome amigavel da camera (padrao: "$DEFAULT_CAMERA_LABEL")
  --camera-width PX         Largura inferencia (padrao: $DEFAULT_CAMERA_WIDTH)
  --camera-height PX        Altura inferencia (padrao: $DEFAULT_CAMERA_HEIGHT)
  --camera-fps FPS          FPS de processamento (padrao: $DEFAULT_CAMERA_FPS)
  --camera-enabled 0|1      Habilita camera inicial (padrao: 1)
  --culto-antecedencia-min N Janela de inicio do culto (min antes, padrao: $DEFAULT_CULTO_ANTECEDENCIA_MIN)
  --culto-duracao-min N     Duracao da janela do culto (min, padrao: $DEFAULT_CULTO_DURACAO_MIN)
  --estimar-faixa-etaria 0|1 Habilita estimativa de faixas etarias (padrao: $DEFAULT_ESTIMAR_FAIXA_ETARIA)
  --skip-system-deps        Nao roda apt-get update/install
  --yes                     Nao perguntar confirmacoes
  --help                    Exibe esta ajuda

Exemplo:
  sudo bash deploy/setup_raspi.sh --install-dir /opt/vip --user pi --port 8000 --yes
EOF
}

SKIP_SYSTEM_DEPS=0

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
    --host)
      APP_HOST="$2"
      shift 2
      ;;
    --port)
      APP_PORT="$2"
      shift 2
      ;;
    --camera-device)
      CAMERA_DEVICE="$2"
      shift 2
      ;;
    --camera-label)
      CAMERA_LABEL="$2"
      shift 2
      ;;
    --camera-width)
      CAMERA_WIDTH="$2"
      shift 2
      ;;
    --camera-height)
      CAMERA_HEIGHT="$2"
      shift 2
      ;;
    --camera-fps)
      CAMERA_FPS="$2"
      shift 2
      ;;
    --camera-enabled)
      CAMERA_ENABLED="$2"
      shift 2
      ;;
    --culto-antecedencia-min)
      CULTO_ANTECEDENCIA_MIN="$2"
      shift 2
      ;;
    --culto-duracao-min)
      CULTO_DURACAO_MIN="$2"
      shift 2
      ;;
    --estimar-faixa-etaria)
      ESTIMAR_FAIXA_ETARIA="$2"
      shift 2
      ;;
    --skip-system-deps)
      SKIP_SYSTEM_DEPS=1
      shift
      ;;
    --yes)
      NON_INTERACTIVE=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Opcao desconhecida: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_root() {
  if [[ "$EUID" -ne 0 ]]; then
    echo "Execute como root: sudo bash deploy/setup_raspi.sh" >&2
    exit 1
  fi
}

confirm_or_exit() {
  if [[ "$NON_INTERACTIVE" == "1" ]]; then
    return
  fi
  echo "Resumo de setup:"
  echo "  install_dir: $INSTALL_DIR"
  echo "  run_user: $RUN_USER"
  echo "  host: $APP_HOST"
  echo "  port: $APP_PORT"
  echo "  camera_device: $CAMERA_DEVICE"
  echo "  camera_label: $CAMERA_LABEL"
  echo "  camera_width x height: ${CAMERA_WIDTH}x${CAMERA_HEIGHT}"
  echo "  camera_fps: $CAMERA_FPS"
  echo "  culto_antecedencia_min: $CULTO_ANTECEDENCIA_MIN"
  echo "  culto_duracao_min: $CULTO_DURACAO_MIN"
  echo "  estimar_faixa_etaria: $ESTIMAR_FAIXA_ETARIA"
  read -r -p "Continuar? [y/N] " answer
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    echo "Cancelado."
    exit 1
  fi
}

ensure_user_exists() {
  if ! id "$RUN_USER" >/dev/null 2>&1; then
    echo "Usuario '$RUN_USER' nao existe." >&2
    exit 1
  fi
}

# OpenCV/V4L2 no Pi precisa ler /dev/video* — o usuario do servico deve estar em 'video'.
ensure_video_group() {
  if ! getent group video >/dev/null 2>&1; then
    echo "AVISO: grupo 'video' nao encontrado (camera V4L2 pode falhar)." >&2
    return
  fi
  if id -nG "$RUN_USER" | tr ' ' '\n' | grep -qx video; then
    echo "Usuario $RUN_USER ja esta no grupo 'video'."
    return
  fi
  echo "Adicionando $RUN_USER ao grupo 'video' (acesso a cameras USB / modulo Pi)..."
  usermod -aG video "$RUN_USER"
  echo "Reinicie o servico ou o Raspberry apos isto se a camera nao abrir: sudo systemctl restart $SERVICE_NAME"
}

install_dependencies() {
  if [[ "$SKIP_SYSTEM_DEPS" == "1" ]]; then
    echo "[1/6] Pulando dependencias de sistema (--skip-system-deps)."
    return
  fi
  echo "[1/6] Instalando dependencias do sistema..."
  apt-get update
  apt-get install -y python3 python3-pip rsync
}

copy_project() {
  echo "[2/6] Copiando projeto para $INSTALL_DIR ..."
  mkdir -p "$INSTALL_DIR"
  rsync -a --delete \
    --exclude ".git" \
    --exclude ".venv" \
    --exclude "__pycache__" \
    --exclude "*.pyc" \
    "$REPO_ROOT"/ "$INSTALL_DIR"/
  chown -R "$RUN_USER":"$RUN_USER" "$INSTALL_DIR"
}

install_python_deps() {
  echo "[3/6] Instalando dependencias Python..."
  sudo -u "$RUN_USER" "$PYTHON_BIN" -m pip install -r "$INSTALL_DIR/requirements.txt" --break-system-packages
  echo "Opcional (idade/sexo no HOG): como $RUN_USER, correr $INSTALL_DIR/scripts/download_demographics_models.sh"
}

generate_service_file() {
  echo "[4/6] Gerando service systemd..."
  cat > "/etc/systemd/system/$SERVICE_NAME" <<EOF
[Unit]
Description=VIP Dashboard (FastAPI)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$PYTHON_BIN -m uvicorn app.main:app --host $APP_HOST --port $APP_PORT
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
}

enable_service() {
  echo "[5/6] Habilitando e iniciando service..."
  systemctl daemon-reload
  systemctl enable "$SERVICE_NAME"
  systemctl restart "$SERVICE_NAME"
}

apply_initial_camera_config() {
  echo "[6/6] Aplicando configuracao inicial da camera..."
  # sudo strips the caller environment by default; env forwards these to Python.
  sudo -u "$RUN_USER" \
    env \
      VIP_INSTALL_DIR="$INSTALL_DIR" \
      VIP_CAMERA_DEVICE="$CAMERA_DEVICE" \
      VIP_CAMERA_LABEL="$CAMERA_LABEL" \
      VIP_CAMERA_ENABLED="$CAMERA_ENABLED" \
      VIP_CAMERA_WIDTH="$CAMERA_WIDTH" \
      VIP_CAMERA_HEIGHT="$CAMERA_HEIGHT" \
      VIP_CAMERA_FPS="$CAMERA_FPS" \
      VIP_CULTO_ANTECEDENCIA_MIN="$CULTO_ANTECEDENCIA_MIN" \
      VIP_CULTO_DURACAO_MIN="$CULTO_DURACAO_MIN" \
      VIP_ESTIMAR_FAIXA_ETARIA="$ESTIMAR_FAIXA_ETARIA" \
    "$PYTHON_BIN" - <<'PY'
import os
import sqlite3
from pathlib import Path

db = Path(os.environ["VIP_INSTALL_DIR"]) / "data" / "app.db"
db.parent.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(db)
conn.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
updates = {
    "camera_device": os.environ["VIP_CAMERA_DEVICE"],
    "camera_label": os.environ["VIP_CAMERA_LABEL"],
    "camera_enabled": os.environ["VIP_CAMERA_ENABLED"],
    "camera_inference_width": os.environ["VIP_CAMERA_WIDTH"],
    "camera_inference_height": os.environ["VIP_CAMERA_HEIGHT"],
    "camera_fps": os.environ["VIP_CAMERA_FPS"],
    "culto_antecedencia_min": os.environ["VIP_CULTO_ANTECEDENCIA_MIN"],
    "culto_duracao_min": os.environ["VIP_CULTO_DURACAO_MIN"],
    "estimar_faixa_etaria": os.environ["VIP_ESTIMAR_FAIXA_ETARIA"],
}
for key, value in updates.items():
    conn.execute(
        """
        INSERT INTO config (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )
conn.commit()
conn.close()
print("Camera configurada com sucesso no banco local.")
PY
}

print_final_status() {
  echo
  echo "Setup concluido."
  echo "Service: $SERVICE_NAME"
  systemctl --no-pager --full status "$SERVICE_NAME" || true
  echo
  echo "Acesse no navegador:"
  echo "  http://<ip-do-raspi>:$APP_PORT"
}

require_root
ensure_user_exists
ensure_video_group
confirm_or_exit
install_dependencies
copy_project
install_python_deps
generate_service_file
enable_service
apply_initial_camera_config
print_final_status
