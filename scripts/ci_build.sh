#!/usr/bin/env bash
# Smoke do dbt no GitHub Actions (vale rodar local também).
#
# Não baixa ZIP da Receita. Isso é o pipeline do avaliador, com internet e paciência.
# Aqui a gente só joga os CSVs de data/ci/ no DuckDB e manda um dbt run/snapshot/test.
# Se alguém quebrar um modelo ou um teste, o PR fica vermelho — e pronto.

set -euo pipefail

# Bind mount do compose + dbt em paralelo já abortou o runner uma vez.
# /tmp é chato, mas é estável.
export DUCKDB_PATH=/tmp/cnpj_ci.duckdb
rm -f "$DUCKDB_PATH"

echo "==> fixtures (data/ci) -> DuckDB"
python -c "from scripts.load_duckdb import load_ci_fixtures; load_ci_fixtures()"

echo "==> dbt deps + run + snapshot + test"
cd dbt
dbt deps
dbt run --threads 1
dbt snapshot --threads 1
dbt test --threads 1

echo "OK — dbt passou nas fixtures."
