#!/usr/bin/env bash
# Checklist rápido antes de entregar o desafio.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Git status"
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
  echo "AVISO: working tree com alterações não commitadas."
  git status -sb
else
  echo "OK — working tree limpo."
fi

echo ""
echo "==> CI build (amostra partição 0)"
export DUCKDB_PATH="${DUCKDB_PATH:-/tmp/cnpj_validate.duckdb}"
export SAMPLE_N_ROWS="${SAMPLE_N_ROWS:-500}"
export ENABLE_RECEITAWS="${ENABLE_RECEITAWS:-false}"
bash scripts/ci_build.sh

echo ""
echo "==> Smoke Prefect (import flows)"
python - <<'PY'
from orchestration.flows.cnpj_pipeline import cnpj_pipeline
from orchestration.flows.ingestao_particao_cnpj import ingestao_particao_cnpj
from orchestration.flows.ingestao_completa_cnpj import ingestao_completa_cnpj
print("Prefect flows OK")
PY

echo ""
echo "Checklist concluído. Revise docs/ENTREGA.md antes de enviar."
