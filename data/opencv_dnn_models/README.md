# Modelos DNN (deteccao facial, re-identificacao, idade/sexo)

Os ficheiros `*.caffemodel` e `*.onnx` sao grandes e **nao** estao no Git.

Modelos utilizados:
- `face_detection_yunet_2023mar.onnx` — detector facial YuNet (primario)
- `face_recognition_sface_2021dec.onnx` — re-identificacao anonima SFace (128-d)
- `age_net.caffemodel` — estimativa de idade (Caffe DNN)
- `gender_net.caffemodel` — estimativa de genero (Caffe DNN)

No Pi, o **`deploy/update_raspi.sh`**, o **`deploy/setup_raspi.sh`** e a **atualizacao pela interface**
chamam automaticamente `scripts/download_demographics_models.sh` (so descarrega o que faltar; precisa de rede).

Manualmente (qualquer maquina):

```bash
chmod +x scripts/download_demographics_models.sh
./scripts/download_demographics_models.sh
```

Requisitos: `curl`. Os ficheiros `age_deploy.prototxt` e `gender_deploy.prototxt` ja vao com o projeto.

Sem os modelos ONNX, o sistema usa fallbacks (Haar Cascade para deteccao, DCT para re-id) com menor acuracia.
Sem os `.caffemodel`, a deteccao facial continua a funcionar; apenas **nao** ha estimativa de idade/sexo.

Origem dos pesos Caffe: [GilLevi/AgeGenderDeepLearning](https://github.com/GilLevi/AgeGenderDeepLearning).
Modelos ONNX: [opencv/opencv_zoo](https://github.com/opencv/opencv_zoo).
