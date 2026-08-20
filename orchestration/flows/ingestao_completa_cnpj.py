from __future__ import annotations

import os

from loguru import logger
from prefect import flow

from orchestration.deployments_config import DEPLOYMENT_TRANSFORMACAO
from orchestration.flows.ingestao_particao_cnpj import ingestao_particao_cnpj
from orchestration.flows.transformacao_cnpj import transformacao_cnpj
from orchestration.run_names import completa_run_name
from orchestration.tasks.cnpj_tasks import rf_detect_partitions_task, trigger_downstream_task
from scripts.config import RF_VINTAGE
from scripts.duckdb_conn import resolve_duckdb_path, sync_duckdb_to_primary

ENABLE_RECEITAWS = os.getenv("ENABLE_RECEITAWS", "true").lower() == "true"


@flow(
    name="ingestao_completa_cnpj",
    flow_run_name=completa_run_name,
    log_prints=True,
)
def ingestao_completa_cnpj(
    vintage: str = RF_VINTAGE,
    partitions: list[int] | None = None,
    sample_n_rows: int | None = None,
    run_receitaws: bool = ENABLE_RECEITAWS,
    transform_per_partition: bool = False,
    trigger_downstream: bool = True,
    run_label: str = "manual",
) -> None:
    resolve_duckdb_path()
    partition_ids = partitions or rf_detect_partitions_task(vintage)
    if not partition_ids:
        raise RuntimeError(f"Nenhuma partição detectada para vintage {vintage}.")

    logger.info("Ingestão completa: vintage={} partições={}", vintage, partition_ids)

    try:
        for pid in partition_ids:
            ingestao_particao_cnpj.with_options(
                flow_run_name=f"ingestao-particao-{pid}-{run_label}"
            )(
                partition_id=pid,
                vintage=vintage,
                sample_n_rows=sample_n_rows,
                run_receitaws=run_receitaws,
                trigger_downstream=False,
                run_label=f"{run_label}-p{pid}",
                sync_on_complete=False,
            )
            if transform_per_partition:
                transformacao_cnpj.with_options(
                    flow_run_name=f"transformacao-p{pid}-{run_label}"
                )(
                    run_label=f"{run_label}-p{pid}",
                    partition_id=pid,
                    vintage=vintage,
                )

        if trigger_downstream and not transform_per_partition:
            downstream_label = f"full-{run_label}"
            trigger_downstream_task(
                DEPLOYMENT_TRANSFORMACAO,
                {
                    "run_label": downstream_label,
                    "vintage": vintage,
                },
                flow_run_name=f"transformacao-cnpj-{downstream_label}",
            )
        elif not trigger_downstream and not transform_per_partition:
            transformacao_cnpj.with_options(
                flow_run_name=f"transformacao-{run_label}"
            )(run_label=run_label, vintage=vintage)
    finally:
        sync_duckdb_to_primary()


if __name__ == "__main__":
    from datetime import datetime, timezone

    run_label = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    ingestao_completa_cnpj(run_label=run_label)
