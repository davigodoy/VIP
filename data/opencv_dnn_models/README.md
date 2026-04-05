# Modelos DNN (idade / sexo)

Os ficheiros `*.caffemodel` sao grandes (~40 MB cada) e **nao** estao no Git.

No Raspberry Pi ou na maquina de desenvolvimento:

```bash
chmod +x scripts/download_demographics_models.sh
./scripts/download_demographics_models.sh
```

Requisitos: `curl`. Os ficheiros `age_deploy.prototxt` e `gender_deploy.prototxt` ja vao com o projeto.

Sem estes pesos, a deteccao HOG continua a funcionar; apenas **nao** ha estimativa de idade/sexo no `ingest` (o log regista uma vez que faltam modelos).

Origem dos pesos: repositorio [learnopencv AgeGender](https://github.com/spmallick/learnopencv/tree/master/AgeGender) (OpenCV DNN + Caffe).
