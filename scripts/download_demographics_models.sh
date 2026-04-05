#!/usr/bin/env bash
# Descarrega pesos Caffe para idade/sexo (OpenCV DNN). Prototxt ficam em data/opencv_dnn_models/.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/data/opencv_dnn_models"
mkdir -p "$DIR"
BASE="https://github.com/spmallick/learnopencv/raw/master/AgeGender"
echo "A descarregar para $DIR ..."
curl -fL --progress-bar -o "$DIR/age_net.caffemodel" "$BASE/age_net.caffemodel"
curl -fL --progress-bar -o "$DIR/gender_net.caffemodel" "$BASE/gender_net.caffemodel"
echo "Concluido."
