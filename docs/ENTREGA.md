# Checklist de entrega — Código e execução (10 pts)

Use antes de enviar o repositório. O avaliador só precisa do [README](../README.md) (três comandos Docker).

## 1. Git limpo

- Sem `.env` com tokens reais
- Sem `duckdb/*.duckdb` commitado
- Sem `data/raw/**/*.zip` commitado
- Sem `data/demo` (pipeline baixa a RF; o CI usa só `data/ci/`)

## 2. Execução que o avaliador vai rodar

```bash
docker compose build
docker compose run --rm pipeline
```

Não exige `.env`, Make nem Python no host. Esperado: download partição 0, bronze 10k, dbt run/snapshot/test, **39 PASS**.

Permissões: o entrypoint dá `chmod a+rwX` em `data/` e `duckdb/` ao sair — o avaliador no Linux não fica preso a arquivos `root`.

## 3. Smoke extra (opcional)

```bash
docker compose run --rm --no-deps pipeline shell -c "bash scripts/ci_build.sh"
```

Mesmo smoke do GitHub Actions: fixtures em `data/ci/`, sem download da RF.

## 4. Prefect UI (opcional)

```bash
docker compose --profile prefect up prefect-server prefect-worker -d --build
docker compose --profile prefect run --rm pipeline-prefect
```

UI: http://localhost:4200 — deployment `cnpj-pipeline`.

## 5. Documentação

- [x] README com quick start cross-platform
- [x] `docs/REQUISITOS.md`
- [x] `docs/finops_bigquery.md` (link do repo + partitioning/clustering)
- [x] dbt docs: `docker compose --profile docs up dbt-docs` (depois do pipeline)

## 6. Variáveis (todas opcionais)

Defaults no `docker-compose.yml`. Copiar `.env.example` → `.env` só se quiser mudar vintage, amostra ou ReceitaWS.

## 7. O que o avaliador não precisa fazer

- Criar diretórios (`data/raw`, `duckdb` já vêm no clone)
- Baixar ZIPs na mão
- Instalar Python, dbt ou Prefect no host
- Rodar ingestão completa multi-partição
- Configurar GCP
