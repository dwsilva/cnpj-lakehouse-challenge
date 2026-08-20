#!/usr/bin/env bash
# CI local e GitHub Actions — baixa partição 0, ingere amostra e roda dbt.
set -euo pipefail

export DUCKDB_PATH="${DUCKDB_PATH:-/tmp/cnpj_ci.duckdb}"
export ENABLE_RECEITAWS="${ENABLE_RECEITAWS:-false}"
export SAMPLE_N_ROWS="${SAMPLE_N_ROWS:-1000}"
export RF_VINTAGE="${RF_VINTAGE:-2026-07-12}"

mkdir -p "data/raw/${RF_VINTAGE}" duckdb dbt/target dbt/logs dbt/dbt_packages
chmod -R a+rwX data duckdb dbt/target dbt/logs dbt/dbt_packages 2>/dev/null || true

rm -f "${DUCKDB_PATH}"

python scripts/rf_ingest.py \
  --partition 0 \
  --vintage "${RF_VINTAGE}" \
  --sample-n-rows "${SAMPLE_N_ROWS}"

if [[ "${ENABLE_RECEITAWS}" == "true" ]]; then
  python -c "from scripts.receitaws_client import fetch_receitaws_enrichment; fetch_receitaws_enrichment(limit=int('${RECEITAWS_SAMPLE_SIZE:-3}'))"
fi

cd dbt
dbt deps
dbt run
dbt snapshot
dbt test
