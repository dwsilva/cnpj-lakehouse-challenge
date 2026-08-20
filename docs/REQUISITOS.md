# Atendimento aos requisitos do desafio

Referência: [`teste_desafio_tecnico_engenheiro_de_dados_franq.md`](teste_desafio_tecnico_engenheiro_de_dados_franq.md)

| Requisito | Status | Onde está | Nota de eficiência |
|-----------|--------|-----------|-------------------|
| Arquitetura em camadas dbt | OK | `dbt/models/{staging,intermediate,marts}/` | staging/int = views; marts = tables |
| Staging (padronização) | OK | `stg_empresas`, `stg_estabelecimentos`, `stg_socios`, `stg_simples`, `stg_cnae`, `stg_receitaws` | macros de parse/limpeza; datas RF `YYYYMMDD` |
| Fato empresas ativas | OK | `marts/fct_empresas_ativas.sql` | incremental; no BQ: partition `data_referencia` + cluster `uf, cnae, situacao` |
| Dimensões (CNAE, empresa) | OK | `dim_cnae.sql`, `dim_empresa.sql` | hierarquia CNAE derivada do código; cluster BQ em `dim_empresa` |
| Snapshot SCD2 capital social | OK | `snapshots/snap_empresas_capital_social.sql` | strategy `timestamp` em `dbt_updated_at` |
| ≥ 3 macros aplicadas | OK (6 + schema) | `dbt/macros/` | `clean_text`, `format_cnpj`, `parse_capital_social`, `standardize_situacao_cadastral`, `audit_row_hash`, `parse_rf_date` |
| ≥ 8 testes dbt | OK (**39**) | YAML + 2 testes singular | nativos + `dbt_utils` + regras de negócio |
| Testes nativos | OK | unique, not_null, accepted_values, relationships | fato → dims |
| Teste customizado / pacote | OK | `assert_capital_social_non_negative`, `assert_dim_empresa_fill_rates`, `dbt_utils.*` | capital ≥ 0; fill-rate; `qty_socios >= 0` |
| Prefect: extração + carga ~10k | OK | `ingestao_particao_cnpj` + `scripts/rf_ingest.py` | `SAMPLE_N_ROWS=10000`; bronze idempotente por partição |
| Prefect: dbt run + dbt test | OK | `transformacao_cnpj.py` | também `dbt snapshot` |
| FinOps BigQuery | OK | [`finops_bigquery.md`](finops_bigquery.md) | partitioning, clustering, custo, onboarding |
| Execução local | OK | Docker Compose, **sem `.env` obrigatório** | ver README |
| ReceitaWS | OK | `receitaws_client.py`, `stg_receitaws` | API live, delay configurável, 5 CNPJs no default |
| Resiliência | OK | retries Prefect; `duckdb_conn.py` (lock → `/tmp`) | chmod no exit (permissões no host) |
| CI | OK | `.github/workflows/dbt-test.yml` | partição 0 + amostra + dbt test |

## Testes (39)

Confirmado em `dbt test`: *Found 10 models, 39 data tests, 1 snapshot*.

| Tipo | Exemplos |
|------|----------|
| Nativos | `unique` / `not_null` em chaves; `accepted_values` (situação, Simples S/N); `relationships` fato → `dim_empresa` / `dim_cnae` |
| `dbt_utils` | `accepted_range` (capital ≥ 0), `not_null_proportion`, `unique_combination_of_columns` (sócios), `expression_is_true` (`qty_socios >= 0`) |
| Singular | `assert_capital_social_non_negative`, `assert_dim_empresa_fill_rates` |

## Como o avaliador valida

```bash
docker compose build
docker compose run --rm pipeline
```

Esperado: exit 0 e 39 testes `PASS`. Detalhes de permissão, tempo e Prefect UI: [README](../README.md).

## Critérios de avaliação (100 pts)

| Critério | Pts | Evidência |
|----------|-----|-----------|
| dbt Mastery | 30 | 6 macros, incremental, snapshot SCD2, 39 testes |
| Arquitetura | 25 | Prefect (4 deployments) → DuckDB → dbt; retries; bronze particionado |
| Data Quality | 20 | testes + staging + nulos/duplicatas tratados |
| BigQuery FinOps | 15 | `docs/finops_bigquery.md` |
| Código/Execução | 10 | Docker one-command, Git sem ZIPs/DuckDB, README para avaliador |
