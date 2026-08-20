from __future__ import annotations

import os

from prefect import flow

from orchestration.deployments_config import DEPLOYMENT_TRANSFORMACAO
from orchestration.run_names import particao_run_name
from orchestration.tasks.cnpj_tasks import (
    enrich_receitaws_task,
    rf_download_partition_task,
    rf_ingest_partition_task,
    trigger_downstream_task,
)
from scripts.config import RF_VINTAGE, SAMPLE_N_ROWS
from scripts.duckdb_conn import resolve_duckdb_path, sync_duckdb_to_primary

ENABLE_RECEITAWS = os.getenv("ENABLE_RECEITAWS", "true").lower() == "true"


@flow(
    name="ingestao_particao_cnpj",
    flow_run_name=particao_run_name,
    log_prints=True,
)
def ingestao_particao_cnpj(
    partition_id: int = 0,
    vintage: str = RF_VINTAGE,
    sample_n_rows: int | None = SAMPLE_N_ROWS,
    run_receitaws: bool = ENABLE_RECEITAWS,
    trigger_downstream: bool = False,
    run_label: str = "manual",
    sync_on_complete: bool = True,
) -> dict[str, int]:
    resolve_duckdb_path()
    try:
        rf_download_partition_task(vintage, partition_id)
        counts = rf_ingest_partition_task(
            partition_id=partition_id,
            vintage=vintage,
            sample_n_rows=sample_n_rows,
            download=False,
        )
        enrich_receitaws_task(
            run_receitaws=run_receitaws,
            partition_id=partition_id,
            vintage=vintage,
        )
        if trigger_downstream:
            downstream_label = f"p{partition_id}-{run_label}"
            trigger_downstream_task(
                DEPLOYMENT_TRANSFORMACAO,
                {
                    "run_label": downstream_label,
                    "partition_id": partition_id,
                    "vintage": vintage,
                },
                flow_run_name=f"transformacao-cnpj-{downstream_label}",
            )
        return counts
    finally:
        if sync_on_complete and not trigger_downstream:
            sync_duckdb_to_primary()


if __name__ == "__main__":
    from datetime import datetime, timezone

    run_label = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    ingestao_particao_cnpj(run_label=run_label)
