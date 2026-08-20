from __future__ import annotations

from prefect import flow

from orchestration.run_names import transformacao_run_name
from orchestration.tasks.cnpj_tasks import dbt_run_task, dbt_snapshot_task, dbt_test_task
from scripts.config import RF_VINTAGE
from scripts.duckdb_conn import resolve_duckdb_path, sync_duckdb_to_primary


@flow(
    name="transformacao_cnpj",
    flow_run_name=transformacao_run_name,
    log_prints=True,
)
def transformacao_cnpj(
    run_label: str = "manual",
    partition_id: int | None = None,
    vintage: str = RF_VINTAGE,
) -> None:
    resolve_duckdb_path()
    try:
        dbt_run_task()
        dbt_snapshot_task()
        dbt_test_task()
    finally:
        sync_duckdb_to_primary()


if __name__ == "__main__":
    from datetime import datetime, timezone

    run_label = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    transformacao_cnpj(run_label=run_label)
