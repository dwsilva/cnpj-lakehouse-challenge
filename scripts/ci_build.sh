#!/usr/bin/env bash
# CI local e GitHub Actions — baixa partição 0, ingere amostra e roda dbt.
# Isola DuckDB em /tmp: o compose injeta DUCKDB_PATH no bind mount e
# dbt test com 4 threads aborta (SIGABRT) nesse caminho.
set -euo pipefail

export DUCKDB_PATH=/tmp/cnpj_ci.duckdb
export ENABLE_RECEITAWS=false
export SAMPLE_N_ROWS="${CI_SAMPLE_N_ROWS:-1000}"
export RF_VINTAGE="${RF_VINTAGE:-2026-07-12}"
export DBT_THREADS=1

mkdir -p "data/raw/${RF_VINTAGE}" /tmp
rm -f "${DUCKDB_PATH}"

python scripts/rf_ingest.py \
  --partition 0 \
  --vintage "${RF_VINTAGE}" \
  --sample-n-rows "${SAMPLE_N_ROWS}"

cd dbt
dbt deps
dbt run --threads 1
dbt snapshot --threads 1
dbt test --threads 1
