# VIP

Sistema web (FastAPI) para contagem anonima de fluxo em cultos, com operacao em
Raspberry Pi 4, painel responsivo, reconciliacao de dados e sincronizacao com
Google Sheets.

## Visao geral

O projeto **deteta fluxo (opcionalmente no proprio servidor via HOG OpenCV) e/ou
via edge** e **grava cada evento na base SQLite** (`events`, estado por
`(culto_id, person_id)` em `service_event_people`, agregados em `service_event_stats` por `culto_id`, incluindo `__global__`). Deteccao integrada e `POST /api/events/ingest` usam a **mesma
funcao interna** de ingestao — mesma gravacao e mesmas metricas ao vivo.
A tabela **`events`** e o arquivo minimo para **reconciliar a qualquer momento**
(ou semanas depois), desde que a politica de retencao ainda os mantenha.

Escopo funcional:
- contagem de entradas e saidas (agregado **global** e, na janela da agenda, particao **por culto** com a mesma chave sintetica `report_culto_id`)
- ocupacao atual e pico de ocupacao
- reentrada: mesma `person_id` **na mesma particao** (`__global__` ou culto da agenda), dentro da **janela em minutos** configurada
- distribuicoes opcionais por faixa etaria e genero (quando o **edge** envia no `ingest`; o HOG integrado nao estima)
- sincronizacao opcional com Google Sheets
- reconciliacao manual (no Pi ou a posteriori noutro equipamento) para recomputar
  metricas a partir dos **eventos brutos** guardados na tabela `events`
- **Envolvimento** (visitante / frequentador / membro): dias de calendario distintos com
  entrada numa janela movel (ex. 30 dias); **visitante** ate N dias distintos,
  **frequentador** ate M (M > N), acima disso **membro**; N e M configuraveis no painel.
  Requer `person_id` estavel entre visitas (edge). Ver `GET /api/people/involvement`

## Conferencia com o rascunho de objetivos (igreja / Pi 4)

Tabela rapida: o que o rascunho pede vs o estado atual do VIP.

| Objetivo no rascunho | Estado no VIP |
|----------------------|---------------|
| Pi 4 + camera USB (ex. C920), operacao continua | Sim: V4L2, preview, HOG opcional, `systemd` no deploy |
| Entradas, saidas, ocupacao, pico | Sim: dashboard + `events` / `service_event_stats` |
| Reentradas **no mesmo culto** | Sim na particao do culto: mesmo `person_id`, saida anterior dentro de **N minutos**, contagem isolada por `culto_id` da agenda |
| **Pessoas unicas por culto** | Sim: estado ao vivo em particoes por chave de culto (agenda + horario do evento); na tabela `events` so ha `event_ts` — o culto nao e colado ao registo de deteccao |
| Cultos por nome, dia, horario | Sim: `service_schedules` + configuracao no painel |
| Dados **organizados por evento fixo** | Sim: particoes em `service_event_*` e filtros de grafico derivam culto a partir de `event_ts` + agenda; `events` guarda fluxo e horario |
| Painel responsivo (celular / PC) | Sim |
| Sync Google Sheets opcional | Sim |
| Configuracao completa via web | Sim: camera, agenda, retencao, regras de envolvimento, Sheets, etc. |
| Faixa etaria e genero (estimativas) | **Opcional via API** (`ingest`); sem modelo de idade/sexo no HOG do servidor |
| Horarios / picos de entrada | Sim: graficos de fluxo e ocupacao (janela configuravel) |
| Conciliacao pelo navegador (outro PC/Mac) | Sim: `GET .../events` + `POST .../apply` + botoes no painel |
| Privacidade: sem nome, IDs tecnicos | Sim: `person_id` / track, sem cadastro nominal no fluxo padrao |
| Categorizar frequencia (visitante / frequentador / membro) | Sim: **Regras envolvimento** (janela + dois limites de dias distintos) + dashboard + lista; depende de `person_id` **estavel** (edge) |

## Funcionalidades principais

- Agenda de cultos:
  - nome, dia da semana, horario e status ativo/inativo
- Configuracao de camera:
  - dispositivo (ex.: `/dev/video0`), nome, resolucao de inferencia e FPS
  - deteccao HOG em segundo plano opcional (`live_detection_enabled`): gera
    entradas/saidas gravadas como os demais eventos (sem preview obrigatorio)
- Dashboard em tempo real:
  - entradas, saidas, retornos, unicos, ocupacao atual e pico
  - graficos de fluxo, ocupacao, faixa etaria e genero
- Retencao e limpeza:
  - politicas configuraveis e execucao manual (dry-run/real)
  - limpeza automatica diaria (remove apenas dados **mais velhos** que a janela; nao zera o dia)
  - **Historico longo:** por defeito cada entrada/saida continua na tabela **`events`** por **180 dias** (`retencao_eventos_dias` no painel, ate 3650). Para ver tendencias em meses, exporte/reconcilie a partir de `events` ou Sheets; os totais do dashboard sao **do momento**, nao um arquivo diario automatico.
  - **Envolvimento:** janela em dias + limite maximo de dias distintos para visitante
    e para frequentador (membro = acima do segundo limite); painel **Regras envolvimento**,
    resumo no dashboard e lista
- Conciliacao:
  - execucao sob demanda no painel (servidor ou browser), com progresso e historico
  - **Fonte para conciliar mais tarde:** cada `ingest` grava ja em `events` o necessario
    (`temp_id` / track, `event_type`, `event_ts`, `age_band`, `gender` quando existirem).
    Os agregados do painel sao derivados; basta manter os eventos na BD (ajuste
    **retencao de eventos** nos dias se precisar de reconciliar semanas depois).
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

**Raspberry Pi 4:** o preview no painel e so video (JPEG). Opcionalmente, em **Configuracao da camera**, podes ligar **Deteccao automatica** (OpenCV HOG no mesmo thread de captura): corre em **segundo plano** sem abrir o preview, gera `entrada`/`saida` via a mesma logica que `ingest` — com o custo de CPU e imprecisao tipicos do HOG. Em Linux o `pip` nao instala `pyobjc-framework-AVFoundation` (so macOS). Mantenha resolucao/FPS moderados (ex.: 640x360, 8 FPS) se notar carga alta; o utilizador do servico deve pertencer ao grupo `video` para V4L2.

Acesso remoto:
- `http://IP_DO_RASPBERRY:8000`

## Atualizacao do sistema

### Via script bash

```bash
sudo bash deploy/update_raspi.sh --install-dir /opt/vip --user pi
```

**Clone em `/home/admin/VIP` (SSH como `admin`):** a partir da raiz do repo:

```bash
cd /home/admin/VIP
sudo bash deploy/update_raspi.sh --here
```

`--here` usa este diretorio como instalacao e, com `sudo`, associa o Git/pip ao utilizador `SUDO_USER` (ex.: `admin`), evitando *dubious ownership*. O `safe.directory` ja vai no script.

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

**Se o update falhou antes desta correcao** (merge bloqueado), no Pi (como utilizador dono do repo, normalmente `pi`):

```bash
sudo -u pi bash -c 'cd /opt/vip && git -c safe.directory=/opt/vip fetch origin main && git -c safe.directory=/opt/vip clean -fd && git -c safe.directory=/opt/vip reset --hard origin/main'
```

(ajusta `main` se a branch for outra). O `-c safe.directory=...` evita o erro **dubious ownership** quando o SSH e com outro user (ex. `admin`) mas o dono de `/opt/vip` e o `pi`. Alternativa: `git config --global --add safe.directory /opt/vip` **no utilizador que corre o git**.

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

### Painel com metricas sempre a zero

As entradas/saidas vêm de **(A)** deteccao HOG opcional no servidor (`live_detection_enabled` no painel, requer OpenCV e camera ligada) e/ou **(B)** um **edge** externo que chama `POST /api/events/ingest` com `person_id` e `direction`. O preview so mostra imagem; nao e obrigatorio para (A).

Teste rápido da API no Pi (ajusta host/porta ao teu serviço):

```bash
curl -sS -X POST "http://127.0.0.1:8000/api/events/ingest" \
  -H "Content-Type: application/json" \
  -d '{"person_id":"diag1","direction":"entrada"}'
```

Se as métricas no painel subirem, a API e a base estão corretas; em produção falta garantir que o serviço de visão/track corre e aponta para a mesma URL.

O dashboard atualiza as **métricas ao vivo** aproximadamente a cada **0,8 s** e os **gráficos** a cada **2,5 s** (polling sobre `GET /api/metrics/live` e `.../charts`). Cada `POST /api/events/ingest` fica visível no painel dentro desse intervalo, sem precisar recarregar a página.

### Eventos e metricas

- **Deteccao no servidor (HOG):** com camera e `live_detection_enabled` ativos,
  o processo do painel escreve na base pelo mesmo caminho que abaixo; corre **a qualquer hora** (a agenda **nao** desliga a camera nem o HOG). Por defeito `live_detection_enabled` vem **desligado** na base — ative no painel e grave. Nao e obrigatorio abrir o preview no browser.

- `POST /api/events/ingest`
  - campos:
    - `person_id` (string tecnica de track)
    - `direction` (`entrada` ou `saida`)
    - `event_ts` (opcional, ISO datetime)
    - `age_band` (opcional: `crianca|junior|adolescente|jovem|adulto|idoso`)
    - `age_estimate` (opcional, inteiro)
    - `gender` (opcional: `homem|mulher`)
  - regras:
    - `events` guarda deteccao e **horario** (`event_ts`); `culto_id` na linha do evento fica `NULL` — o culto e so contexto da agenda, derivado quando necessario
    - ingest atualiza **sempre** `__global__` e, se o instante cair na janela de um culto, tambem a particao desse culto (stats + pessoa por `(culto_id, person_id)`)
    - reentrada usa `janela_reentrada_min` **por particao** (estado separado por culto e global)
    - classificacao por `age_estimate` usa limites configurados no painel
    - resposta inclui `culto_id: null` no evento, mais `report_culto_id`, `scheduled` e `service_name` para UI / relatorios

- `GET /api/people/involvement` — `person_id` com entrada na janela; campos
  `visit_days`, `envolvimento` (`visitante` | `frequentador` | `membro`), `last_entrada`;
  query `limit`, `offset`. Regras e `nota_identidade` no JSON.

- `GET /api/metrics/live` — agregados da particao escolhida: sem query `culto_id`, usa o culto **atual na agenda** se `scheduled`, senao `__global__`. Query `culto_id` forca uma particao. Inclui `involvement` (global). `culto_id` no JSON e `null` quando a particao e o agregado global.

- `GET /api/metrics/charts` — eventos na janela de tempo; mesma regra de particao que `live` (para um culto, filtra linhas cuja derivacao agenda+`event_ts` coincide com a particao). Queries `window_minutes`, `bucket_seconds`, `culto_id` opcionais.

**Atualizacao de banco:** na primeira subida apos esta versao, uma migracao pode esvaziar `service_event_stats` e `service_event_people` e ajustar o schema; rode `POST /api/reconciliation/run` uma vez para recomputar a partir de `events`.

### Sincronizacao Google Sheets

- `GET /api/sync/status`
- `POST /api/sync/run`

### Conciliacao

- `GET /api/reconciliation/status`
- `GET /api/reconciliation/runs`
- `POST /api/reconciliation/run` — recomputo **no servidor** (thread em background)
- `GET /api/reconciliation/events` — lista eventos (ordem cronologica) para recomputo **no browser**
- `POST /api/reconciliation/apply` — corpo JSON com `stats` + `people` (resultado do recomputo no PC/Mac); o servidor **so grava** na particao `__global__` (nao apaga particoes por culto). Nao usar em paralelo com `.../run` no servidor.

No painel: botoes **Rodar no servidor (Pi)** e **Rodar neste browser**.

**Conciliar depois (ideal no Pi, quando couber):** nao e preciso guardar ficheiros
extra. O registo permanente para recomputo e a tabela **`events`**. Podes
adiar a conciliacao por limites de CPU; quando correres (no Pi ou via browser +
`apply`), o algoritmo so le `events`. Garante **retencao de eventos** (dias no
painel) suficiente para o periodo que queres voltar a reconciliar; a limpeza
automatica apaga eventos antigos e remove essa possibilidade para essas datas.

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
