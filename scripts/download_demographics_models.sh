#!/usr/bin/env bash
# Descarrega modelos para deteccao facial, re-identificacao e demographics.
# Idempotente: se os ficheiros ja existirem com tamanho plausivel, nao volta a baixar.
#
# Modelos:
#   1. age_net.caffemodel      (~44 MB)  — estimativa de idade (OpenCV DNN)
#   2. gender_net.caffemodel   (~44 MB)  — estimativa de genero (OpenCV DNN)
#   3. face_detection_yunet    (~230 KB) — deteccao facial DNN (substitui Haar)
#   4. face_recognition_sface  (~37 MB)  — re-id facial 128-dim (substitui DCT)
#
# Os prototxt (age_deploy, gender_deploy) ja estao em data/opencv_dnn_models/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/data/opencv_dnn_models"
mkdir -p "$DIR"

MIN_BYTES=$((1024 * 1024))
MIN_BYTES_SMALL=$((100 * 1024))

_file_ok() {
  local f="$1" min="${2:-$MIN_BYTES}"
  [[ -f "$f" ]] || return 1
  local sz
  sz=$(wc -c <"$f" | tr -d '[:space:]')
  [[ "${sz:-0}" -ge "$min" ]]
}

# --- Demographics (Caffe) ---
AGE_BASE="https://raw.githubusercontent.com/GilLevi/AgeGenderDeepLearning/master/models"
AGE_F="$DIR/age_net.caffemodel"
GEN_F="$DIR/gender_net.caffemodel"

# --- YuNet face detector (OpenCV Zoo) ---
YUNET_URL="https://github.com/opencv/opencv_zoo/raw/main/models/face_detection_yunet/face_detection_yunet_2023mar.onnx"
YUNET_F="$DIR/face_detection_yunet_2023mar.onnx"

# --- SFace face recognizer (OpenCV Zoo) ---
SFACE_URL="https://github.com/opencv/opencv_zoo/raw/main/models/face_recognition_sface/face_recognition_sface_2021dec.onnx"
SFACE_F="$DIR/face_recognition_sface_2021dec.onnx"

need=0

echo "Verificando modelos em $DIR ..."
echo ""

if _file_ok "$AGE_F"; then
  echo "  [OK] age_net.caffemodel"
else
  echo "  [--] age_net.caffemodel (a descarregar)"
  need=1
fi

if _file_ok "$GEN_F"; then
  echo "  [OK] gender_net.caffemodel"
else
  echo "  [--] gender_net.caffemodel (a descarregar)"
  need=1
fi

if _file_ok "$YUNET_F" "$MIN_BYTES_SMALL"; then
  echo "  [OK] face_detection_yunet_2023mar.onnx"
else
  echo "  [--] face_detection_yunet_2023mar.onnx (a descarregar)"
  need=1
fi

if _file_ok "$SFACE_F"; then
  echo "  [OK] face_recognition_sface_2021dec.onnx"
else
  echo "  [--] face_recognition_sface_2021dec.onnx (a descarregar)"
  need=1
fi

echo ""

if [[ "$need" -eq 0 ]]; then
  echo "Todos os modelos ja presentes. Nada a descarregar."
  exit 0
fi

echo "A descarregar modelos em falta ..."
echo ""

if ! _file_ok "$AGE_F"; then
  echo ">> age_net.caffemodel (~44 MB)"
  curl -fL --progress-bar -o "$AGE_F" "$AGE_BASE/age_net.caffemodel"
  echo ""
fi

if ! _file_ok "$GEN_F"; then
  echo ">> gender_net.caffemodel (~44 MB)"
  curl -fL --progress-bar -o "$GEN_F" "$AGE_BASE/gender_net.caffemodel"
  echo ""
fi

if ! _file_ok "$YUNET_F" "$MIN_BYTES_SMALL"; then
  echo ">> face_detection_yunet_2023mar.onnx (~230 KB)"
  curl -fL --progress-bar -o "$YUNET_F" "$YUNET_URL"
  echo ""
fi

if ! _file_ok "$SFACE_F"; then
  echo ">> face_recognition_sface_2021dec.onnx (~37 MB)"
  curl -fL --progress-bar -o "$SFACE_F" "$SFACE_URL"
  echo ""
fi

echo "Concluido. Modelos em $DIR"
