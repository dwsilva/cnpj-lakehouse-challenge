# CNPJ Lakehouse Challenge

Pipeline **dbt + Prefect + DuckDB** para o teste técnico de Engenheiro de Dados (Franq). Roda 100% local via Docker. Em produção a mesma modelagem iria para BigQuery.

Repositório: https://github.com/dwsilva/cnpj-lakehouse-challenge

| Documento | Conteúdo |
|-----------|----------|
| [Enunciado](docs/teste_desafio_tecnico_engenheiro_de_dados_franq.md) | Transcrição do PDF do desafio |
| [Requisitos](docs/REQUISITOS.md) | Checklist do que foi entregue |
| [FinOps BigQuery](docs/finops_bigquery.md) | Item 5 — 4 a 7 páginas |
| [Entrega](docs/ENTREGA.md) | Checklist antes de enviar |

---

## Como avaliar (recrutador)

Único pré-requisito: [Docker Desktop](https://www.docker.com/products/docker-desktop/) (ou Docker Engine + Compose v2). **Make não é necessário.** Copiar `.env` **não é necessário** — os defaults já estão no `docker-compose.yml`.

Internet é obrigatória na primeira execução (download da partição 0 da Receita Federal).

```bash
git clone https://github.com/dwsilva/cnpj-lakehouse-challenge.git
cd cnpj-lakehouse-challenge
docker compose build
docker compose run --rm pipeline
```

Isso baixa a **partição 0** da RF, ingere uma amostra de **10.000 linhas** por arquivo, consulta a ReceitaWS (5 CNPJs) e executa `dbt run` + `dbt snapshot` + `dbt test` (**39 testes**).

| Etapa | Tempo típico |
|-------|----------------|
| Build da imagem | 2–4 min (só na primeira vez) |
| Download dos ZIPs (partição 0) | 5–15 min — ~3 GB; as próximas execuções reutilizam os arquivos em `data/raw/` |
| Ingestão 10k + dbt | 2–5 min |

**Sucesso:** o container termina com exit code 0 e o log mostra os 39 testes dbt em `PASS`. O banco fica em `duckdb/cnpj.duckdb`.

Windows (PowerShell) e Linux/macOS usam os **mesmos** comandos `docker compose`.

---

## Permissões de diretório

O clone já traz `data/raw/` e `duckdb/` vazios (`.gitkeep`). O container cria o restante sozinho.

O entrypoint aplica `umask 000` e `chmod a+rwX` em `data/`, `duckdb/` e artefatos dbt **ao sair**. No Linux isso evita o clássico “Permission denied” / arquivos criados como `root` que o avaliador não consegue apagar.

Se ainda assim aparecer erro de permissão no host:

```bash
# Linux/macOS
chmod -R a+rwX data duckdb dbt/target dbt/logs dbt/dbt_packages 2>/dev/null || true
```

PowerShell (Windows):

```powershell
icacls data, duckdb -grant "${env:USERNAME}:(OI)(CI)F" /T
```

Não é preciso criar pastas na mão nem rodar o container com `sudo`.

**DuckDB locked / Permission denied em `cnpj.duckdb`:** feche DBeaver (ou qualquer cliente) com o arquivo aberto. O pipeline detecta o lock, trabalha em `/tmp` e sincroniza no final.

---

## O que o pipeline faz

```
Internet (RF Casados Dados)  →  ZIP em data/raw/{vintage}/
                              ↓
                    Prefect: ingestao_particao (amostra 10k)
                              ↓
                    DuckDB bronze (raw.* + _partition_id, _vintage)
                              ↓
                    ReceitaWS API (JSON complementar, 5 CNPJs)
                              ↓
                    dbt: staging → intermediate → marts + snapshot SCD2
                              ↓
                    dbt test (39 testes)
```

Fontes: [Casados Dados — CNPJ](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) (`Empresas0`, `Estabelecimentos0`, `Socios0`, `Simples`, `Cnaes`).

---

## Prefect UI (opcional)

O comando `docker compose run --rm pipeline` **não** registra runs na UI — é o caminho mais simples para validar o código.

Para ver deployments e logs em http://localhost:4200:

```bash
docker compose --profile prefect up prefect-server prefect-worker -d --build
docker compose --profile prefect run --rm pipeline-prefect
```

Na UI: **Deployments → `cnpj-pipeline` → Quick Run**.

| Deployment | Função | Schedule |
|------------|--------|----------|
| `ingestao-particao-cnpj` | Download + bronze + ReceitaWS (1 partição) | Manual |
| `ingestao-completa-cnpj` | Todas as partições do vintage | Mensal (dia 5, 06:00 BRT) |
| `transformacao-cnpj` | dbt run / snapshot / test | Encadeado ou manual |
| `cnpj-pipeline` | End-to-end (partição 0 + amostra 10k) | Manual |

Para limpar deployments antigos (`*-demo`):

```bash
docker compose --profile prefect down -v
docker compose --profile prefect up prefect-server prefect-worker -d --build
```

---

## dbt docs (opcional)

Rode **depois** do pipeline (precisa do DuckDB já materializado):

```bash
docker compose --profile docs up --build dbt-docs
```

Abra http://localhost:8080

---

## Customizar (opcional)

```bash
cp .env.example .env   # Linux/macOS / Git Bash
copy .env.example .env  # Windows cmd
```

| Variável | Default | Uso |
|----------|---------|-----|
| `SAMPLE_N_ROWS` | `10000` | Amostra local (item 4 do enunciado) |
| `RF_VINTAGE` | `2026-07-12` | Pasta `data/raw/{vintage}/` — altere se a RF publicar mês novo |
| `ENABLE_RECEITAWS` | `true` | `false` pula a API (mais rápido) |
| `RECEITAWS_SAMPLE_SIZE` | `5` | Quantos CNPJs consultar |
| `RECEITAWS_REQUEST_DELAY_SECONDS` | `3` | Respeito ao rate limit |

Outros comandos:

```bash
docker compose run --rm pipeline ingestao-particao   # só bronze
docker compose run --rm pipeline transform           # só dbt run/snapshot/test
docker compose run --rm pipeline dbt-test            # só testes (banco já existe)
docker compose run --rm --no-deps pipeline shell -c "bash scripts/ci_build.sh"
```

`ingestao-completa` baixa **todas** as partições — volume grande, não é necessário para avaliar o desafio.

---

## Estrutura

```
orchestration/flows/
  ingestao_particao_cnpj.py   # 1 partição (amostra ou full)
  ingestao_completa_cnpj.py   # loop de partições + schedule mensal
  transformacao_cnpj.py       # dbt run + snapshot + test
  cnpj_pipeline.py            # end-to-end + serve dos deployments

scripts/
  rf_download.py              # HTTP → data/raw/{vintage}/
  rf_ingest.py                # ZIP → DuckDB bronze
  receitaws_client.py         # API → raw.receitaws_enrichment
  duckdb_conn.py              # fallback se o .duckdb estiver lockado
  entrypoint.sh               # permissões + comandos do container

dbt/models/                   # staging → intermediate → marts
dbt/snapshots/                # SCD2 capital social
dbt/macros/                   # 6 macros de negócio + generate_schema_name
docs/finops_bigquery.md       # proposta cloud (item 5)
```

---

## Troubleshooting

| Sintoma | O que fazer |
|---------|-------------|
| `env file .env not found` | Atualize o Docker Compose (v2). Este repo **não** exige `.env`. |
| `Permission denied` em pasta `data`/`duckdb` (Linux) | O entrypoint já faz `chmod a+rwX`. Se persistir, use o `chmod` da seção de permissões. |
| `Permission denied` / lock em `cnpj.duckdb` | Feche DBeaver e rode de novo. |
| Download lento ou interrompido | Re-execute: o downloader retoma via HTTP Range. |
| ReceitaWS 429 | Aumente `RECEITAWS_REQUEST_DELAY_SECONDS` no `.env`. |
| Vintage 404 | Liste o diretório em [Casados Dados](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) e ajuste `RF_VINTAGE`. |
| `Aguardando Prefect Server` e timeout | No `.env`, deixe `PREFECT_API_URL` **vazio** para o comando simples. A URL só vale com o profile `prefect`. |
| dbt docs vazio / erro | Rode o pipeline completo antes do profile `docs`. |
| `scripts/entrypoint.sh: not found` | CRLF — o Dockerfile já converte para LF; faça rebuild (`docker compose build --no-cache`). |

---

## Licença

Uso livre para avaliação do teste técnico.
