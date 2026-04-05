#!/usr/bin/env bash
# Descarrega pesos Caffe para idade/sexo (OpenCV DNN). Prototxt ficam em data/opencv_dnn_models/.
# Idempotente: se os ficheiros ja existirem com tamanho plausivel (~> 1 MiB), nao volta a baixar.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/data/opencv_dnn_models"
mkdir -p "$DIR"
BASE="https://github.com/spmallick/learnopencv/raw/master/AgeGender"
MIN_BYTES=$((1024 * 1024))

_file_ok() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  local sz
  sz=$(wc -c <"$f" | tr -d '[:space:]')
  [[ "${sz:-0}" -ge "$MIN_BYTES" ]]
}

AGE_F="$DIR/age_net.caffemodel"
GEN_F="$DIR/gender_net.caffemodel"
need_age=0
need_gender=0
_file_ok "$AGE_F" || need_age=1
_file_ok "$GEN_F" || need_gender=1

if [[ "$need_age" -eq 0 && "$need_gender" -eq 0 ]]; then
  echo "Modelos DNN ja presentes em $DIR; nada a descarregar."
  exit 0
fi

echo "A descarregar para $DIR ..."
if [[ "$need_age" -eq 1 ]]; then
  curl -fL --progress-bar -o "$AGE_F" "$BASE/age_net.caffemodel"
fi
if [[ "$need_gender" -eq 1 ]]; then
  curl -fL --progress-bar -o "$GEN_F" "$BASE/gender_net.caffemodel"
fi
echo "Concluido."
