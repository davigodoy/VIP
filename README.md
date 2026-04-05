# VIP

Sistema web (FastAPI) para contagem anonima de fluxo em cultos, com operacao em
Raspberry Pi 4, painel responsivo, reconciliacao de dados e sincronizacao com
Google Sheets.

## Visao geral

O projeto registra eventos de entrada e saida, consolida metricas por culto e
disponibiliza monitoramento em tempo real via interface web.

Escopo funcional:
- contagem de entradas e saidas por culto
- ocupacao atual e pico de ocupacao
- reentrada considerada apenas na janela do mesmo culto
- distribuicoes opcionais por faixa etaria e genero (estimativas)
- sincronizacao opcional com Google Sheets
- reconciliacao manual para recomputar metricas a partir dos eventos brutos

## Funcionalidades principais

- Agenda de cultos:
  - nome, dia da semana, horario e status ativo/inativo
- Configuracao de camera:
  - dispositivo (ex.: `/dev/video0`), nome, resolucao de inferencia e FPS
- Dashboard em tempo real:
  - entradas, saidas, retornos, unicos, ocupacao atual e pico
  - graficos de fluxo, ocupacao, faixa etaria e genero
- Retencao e limpeza:
  - politicas configuraveis e execucao manual (dry-run/real)
  - limpeza automatica diaria
- Conciliacao:
  - execucao sob demanda no painel, com progresso e historico
- Atualizacao do sistema:
  - via painel web (check + update em background)
  - via script bash para manutencao operacional
- Auto start:
  - inicializacao via systemd no boot do Raspberry

## Execucao local

Requisitos minimos:

```bash
python3 --version
pip3 --version
```

Passos:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Acesso local:

- `http://localhost:8000`

## Deploy no Raspberry Pi

Instalacao automatizada:

```bash
sudo bash deploy/setup_raspi.sh --yes
```

Principais opcoes:

```bash
sudo bash deploy/setup_raspi.sh \
  --install-dir /opt/vip \
  --user pi \
  --port 8000 \
  --camera-device /dev/video0 \
  --camera-label "Entrada principal" \
  --camera-width 640 \
  --camera-height 360 \
  --camera-fps 8 \
  --camera-enabled 1 \
  --yes
```

Opcoes adicionais:
- `--skip-system-deps`
- `--host 0.0.0.0`

**Raspberry Pi 4:** o preview no painel e apenas fluxo de video (sem deteccao local no servidor), o que reduz CPU em relacao a classificadores OpenCV no proprio Pi. Em Linux o `pip` nao instala `pyobjc-framework-AVFoundation` (so macOS). Mantenha resolucao/FPS moderados na configuracao (ex.: 640x360, 8 FPS) se notar carga alta; o utilizador do servico deve pertencer ao grupo `video` para V4L2.

Acesso remoto:
- `http://IP_DO_RASPBERRY:8000`

## Atualizacao do sistema

### Via script bash

```bash
sudo bash deploy/update_raspi.sh --install-dir /opt/vip --user pi
```

Fluxo executado:
- `git fetch` + `git clean -fd` + `git reset --hard origin/<branch>` (espelha o remoto; descarta alteracoes locais em ficheiros rastreados e remove ficheiros/dirs nao rastreados que conflituem; **nao** apaga paths ignorados como `.venv/` e `data/`)
- atualizacao de dependencias Python
- validacao basica com `compileall`
- `init_db()` para aplicar migracoes
- restart do `vip-dashboard.service` (opcional)

Opcoes uteis:
- `--skip-restart`
- `--python-bin python3`
- `--install-dir /opt/vip`
- `--user pi`

### Via painel web

Secao **Atualizacoes**:
- checagem de branch/commit local/remoto e contadores ahead/behind
- execucao de update em background
- barra de progresso, etapa atual e historico de execucoes

O job equivale, em termos de passos, ao `deploy/update_raspi.sh`: `git fetch`, `git clean -fd`, `git reset --hard origin/<branch>`, `pip install -r requirements.txt` com o **mesmo interpretador Python** do processo do uvicorn, `compileall app`, `init_db()` e tentativa de `systemctl restart vip-dashboard.service`. Os passos de systemd sao **tolerantes a falha** (avisos no log) se o utilizador do servico nao tiver permissao para reiniciar a unidade.

**Desenvolvimento no PC:** este fluxo **apaga** commits locais nao enviados no Pi se correres update pelo painel ou `update_raspi.sh`. No laptop usa `git pull` normalmente.

**Se o update falhou antes desta correcao** (merge bloqueado), no Pi como `pi`: `cd /opt/vip && git fetch origin && git clean -fd && git reset --hard origin/main` (ajusta `main` se a branch for outra).

Observacao operacional: durante o update pode ocorrer indisponibilidade breve
por reinicio do servico.

## Instalacao manual do service

```bash
sudo cp deploy/vip-dashboard.service /etc/systemd/system/vip-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable vip-dashboard.service
sudo systemctl start vip-dashboard.service
sudo systemctl status vip-dashboard.service
```

## API

### Eventos e metricas

- `POST /api/events/ingest`
  - campos:
    - `person_id` (string tecnica de track)
    - `direction` (`entrada` ou `saida`)
    - `event_ts` (opcional, ISO datetime)
    - `age_band` (opcional: `crianca|junior|adolescente|jovem|adulto|idoso`)
    - `age_estimate` (opcional, inteiro)
    - `gender` (opcional: `homem|mulher`)
  - regras:
    - eventos gravam só fluxo e `event_ts`; `culto_id` no banco fica sempre `NULL` (horario cadastrado nao particiona dados operacionais)
    - reentrada usa `janela_reentrada_min` no estado global por `person_id`
    - classificacao por `age_estimate` usa limites configurados no painel
    - resposta inclui `culto_id: null`, `report_culto_id` (chave sintetica se o instante cair na janela da agenda), `scheduled` e `service_name` (rotulos para UI / relatorios)

- `GET /api/metrics/live` — agregados do registro `__global__` em `service_event_stats`; `scheduled` / `report_culto_id` vêm só da agenda (exibicao)

- `GET /api/metrics/charts` — todos os eventos na janela de tempo; query opcional `window_minutes`, `bucket_seconds` (padrao 300 s, minimo 300 s); parametro `culto_id` ignorado (legado)

**Atualizacao de banco:** na primeira subida apos esta versao, uma migracao pode esvaziar `service_event_stats` e `service_event_people` e ajustar o schema; rode `POST /api/reconciliation/run` uma vez para recomputar a partir de `events`.

### Sincronizacao Google Sheets

- `GET /api/sync/status`
- `POST /api/sync/run`

### Conciliacao

- `GET /api/reconciliation/status`
- `GET /api/reconciliation/runs`
- `POST /api/reconciliation/run`

### Atualizacao

- `GET /api/update/status`
- `GET /api/update/history`
- `POST /api/update/run`

## Credenciais Google Sheets (Service Account)

1. Criar Service Account no Google Cloud.
2. Gerar chave JSON.
3. Configurar a fonte de credencial no painel:
   - variavel de ambiente (recomendado)
   - arquivo JSON no Raspberry
   - JSON inline
4. Compartilhar a planilha com o email do Service Account (editor).
5. Informar `Spreadsheet ID` e nome da aba no painel.
