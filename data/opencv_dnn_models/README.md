# Modelos DNN (idade / sexo)

Os ficheiros `*.caffemodel` sao grandes (~40 MB cada) e **nao** estao no Git.

No Pi, o **`deploy/update_raspi.sh`** e o **`deploy/setup_raspi.sh`** chamam automaticamente
`scripts/download_demographics_models.sh` (so descarrega o que faltar; precisa de rede).

Manualmente (qualquer maquina):

```bash
chmod +x scripts/download_demographics_models.sh
./scripts/download_demographics_models.sh
```

Requisitos: `curl`. Os ficheiros `age_deploy.prototxt` e `gender_deploy.prototxt` ja vao com o projeto.

Sem estes pesos, a deteccao HOG continua a funcionar; apenas **nao** ha estimativa de idade/sexo no `ingest` (o log regista uma vez que faltam modelos).

Origem dos pesos: [GilLevi/AgeGenderDeepLearning](https://github.com/GilLevi/AgeGenderDeepLearning) (`models/*.caffemodel` via `raw.githubusercontent.com`). Os `.prototxt` no VIP seguem o tutorial learnopencv/OpenCV DNN; sao compativeis com estes pesos.
