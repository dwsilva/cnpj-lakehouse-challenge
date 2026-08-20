#!/usr/bin/env bash
set -euo pipefail

CMD="${1:-pipeline}"

# Bind mounts no Linux ficam owned by root se o container rodar como root.
# umask + chmod no exit deixam data/, duckdb/ e artefatos dbt graváveis no host.
umask 000

_prepare_writable_dirs() {
  mkdir -p \
    /app/data/raw \
    /app/duckdb \
    /app/dbt/target \
    /app/dbt/logs \
    /app/dbt/dbt_packages
  chmod -R a+rwX \
    /app/data \
    /app/duckdb \
    /app/dbt/target \
    /app/dbt/logs \
    /app/dbt/dbt_packages \
    2>/dev/null || true
}

_prepare_writable_dirs
trap _prepare_writable_dirs EXIT

_wait_prefect_api() {
  if [[ -n "${PREFECT_API_URL:-}" ]]; then
    echo "Aguardando Prefect Server em ${PREFECT_API_URL}..."
    for _ in $(seq 1 30); do
      if curl -sf "${PREFECT_API_URL}/health" >/dev/null 2>&1; then
        return 0
      fi
      sleep 2
    done
    echo "Prefect Server não respondeu a tempo." >&2
    exit 1
  fi
}

case "$CMD" in
  pipeline)
    _wait_prefect_api
    python -m orchestration.flows.cnpj_pipeline
    ;;
  ingestao-particao)
    _wait_prefect_api
    python -m orchestration.flows.ingestao_particao_cnpj
    ;;
  ingestao-completa)
    _wait_prefect_api
    python -m orchestration.flows.ingestao_completa_cnpj
    ;;
  transform)
    _wait_prefect_api
    python -m orchestration.flows.transformacao_cnpj
    ;;
  prefect-serve)
    _wait_prefect_api
    echo "Worker Prefect — deployments em http://localhost:4200"
    python -m orchestration.flows.cnpj_pipeline serve
    ;;
  prefect-reset)
    _wait_prefect_api
    python - <<'PY'
from orchestration.deployments_config import cleanup_deprecated_deployments
removed = cleanup_deprecated_deployments()
print(f"Deployments obsoletos removidos: {removed}")
PY
    ;;
  prefect-server)
    prefect server start --host 0.0.0.0 --port 4200
    ;;
  dbt-run)
    cd /app/dbt && dbt deps && dbt run && dbt snapshot
    ;;
  dbt-test)
    cd /app/dbt && dbt deps && dbt test
    ;;
  dbt-docs)
    cd /app/dbt && dbt deps && dbt docs generate
    echo ""
    echo "=========================================="
    echo " dbt docs: http://localhost:8080"
    echo "=========================================="
    echo ""
    dbt docs serve --host 0.0.0.0 --port 8080
    ;;
  shell)
    shift || true
    exec bash "$@"
    ;;
  *)
    echo "Comando desconhecido: $CMD"
    echo "Uso: entrypoint.sh [pipeline|ingestao-particao|ingestao-completa|transform|prefect-serve|prefect-server|dbt-run|dbt-test|dbt-docs|shell]"
    exit 1
    ;;
esac
