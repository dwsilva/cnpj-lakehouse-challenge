from __future__ import annotations

import os
from datetime import datetime, timezone

from loguru import logger
from prefect import flow, serve

from orchestration.deployments_config import cleanup_deprecated_deployments, coleta_monthly_schedule
from orchestration.flows.ingestao_completa_cnpj import ingestao_completa_cnpj
from orchestration.flows.ingestao_particao_cnpj import ingestao_particao_cnpj
from orchestration.flows.transformacao_cnpj import transformacao_cnpj
from orchestration.run_names import pipeline_run_name
from scripts.config import RF_VINTAGE, SAMPLE_N_ROWS
from scripts.duckdb_conn import sync_duckdb_to_primary

ENABLE_RECEITAWS = os.getenv("ENABLE_RECEITAWS", "true").lower() == "true"


@flow(
    name="cnpj_pipeline",
    flow_run_name=pipeline_run_name,
    log_prints=True,
)
def cnpj_pipeline(
    partition_id: int = 0,
    vintage: str = RF_VINTAGE,
    sample_n_rows: int | None = SAMPLE_N_ROWS,
    run_receitaws: bool = ENABLE_RECEITAWS,
    run_label: str = "manual",
) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = run_label if run_label not in ("manual", "ui", "scheduled") else f"{run_label}-{ts}"

    logger.info("Pipeline CNPJ — partição {} vintage {}.", partition_id, vintage)
    ingestao_particao_cnpj.with_options(flow_run_name=f"ingestao-particao-{suffix}")(
        partition_id=partition_id,
        vintage=vintage,
        sample_n_rows=sample_n_rows,
        run_receitaws=run_receitaws,
        run_label=run_label,
        trigger_downstream=False,
        sync_on_complete=False,
    )
    transformacao_cnpj.with_options(flow_run_name=f"transformacao-cnpj-{suffix}")(
        run_label=run_label,
        partition_id=partition_id,
        vintage=vintage,
    )
    sync_duckdb_to_primary()
    logger.info("Pipeline concluído.")


def _default_run_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def serve_deployments() -> None:
    cleanup_deprecated_deployments()
    logger.info(
        "Deployments: ingestao-particao-cnpj | ingestao-completa-cnpj | "
        "transformacao-cnpj | cnpj-pipeline"
    )
    serve(
        ingestao_particao_cnpj.to_deployment(
            name="ingestao-particao-cnpj",
            tags=["ingestao", "bronze", "rf"],
            parameters={
                "partition_id": 0,
                "vintage": RF_VINTAGE,
                "sample_n_rows": SAMPLE_N_ROWS,
                "run_receitaws": ENABLE_RECEITAWS,
                "run_label": "ui",
                "trigger_downstream": True,
            },
        ),
        ingestao_completa_cnpj.to_deployment(
            name="ingestao-completa-cnpj",
            tags=["ingestao", "bronze", "rf", "scheduled"],
            schedule=coleta_monthly_schedule(),
            parameters={
                "vintage": RF_VINTAGE,
                "sample_n_rows": None,
                "run_receitaws": ENABLE_RECEITAWS,
                "transform_per_partition": False,
                "trigger_downstream": True,
                "run_label": "scheduled",
            },
        ),
        transformacao_cnpj.to_deployment(
            name="transformacao-cnpj",
            tags=["dbt", "gold"],
            parameters={"run_label": "ui"},
        ),
        cnpj_pipeline.to_deployment(
            name="cnpj-pipeline",
            tags=["full", "lakehouse"],
            parameters={
                "partition_id": 0,
                "vintage": RF_VINTAGE,
                "sample_n_rows": SAMPLE_N_ROWS,
                "run_receitaws": ENABLE_RECEITAWS,
                "run_label": "ui",
            },
        ),
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        serve_deployments()
    else:
        cnpj_pipeline(run_label=_default_run_label())
