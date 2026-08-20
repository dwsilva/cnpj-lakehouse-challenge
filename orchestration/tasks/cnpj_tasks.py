from __future__ import annotations

import os
import subprocess
from pathlib import Path

from loguru import logger
from prefect import task

from orchestration.deployments_config import trigger_deployment
from scripts.config import DUCKDB_PATH, RF_VINTAGE, SAMPLE_N_ROWS
from scripts.receitaws_client import fetch_receitaws_enrichment
from scripts.rf_download import detect_partitions, download_partition
from scripts.rf_ingest import ingest_partition

DBT_DIR = Path("/app/dbt") if Path("/app/dbt").exists() else Path(__file__).resolve().parents[2] / "dbt"
ENABLE_RECEITAWS = os.getenv("ENABLE_RECEITAWS", "true").lower() == "true"


@task(name="rf-download-partition", retries=2, retry_delay_seconds=30)
def rf_download_partition_task(vintage: str, partition_id: int) -> list[str]:
    paths = download_partition(vintage, partition_id, include_shared=(partition_id == 0))
    return [str(p) for p in paths]


@task(name="rf-ingest-partition", retries=2, retry_delay_seconds=15)
def rf_ingest_partition_task(
    partition_id: int,
    vintage: str = RF_VINTAGE,
    sample_n_rows: int | None = SAMPLE_N_ROWS,
    download: bool = False,
) -> dict[str, int]:
    return ingest_partition(
        partition_id=partition_id,
        vintage=vintage,
        sample_n_rows=sample_n_rows,
        db_path=DUCKDB_PATH,
        download=download,
    )


@task(name="rf-detect-partitions", retries=1, retry_delay_seconds=10)
def rf_detect_partitions_task(vintage: str) -> list[int]:
    return detect_partitions(vintage)


@task(name="enrich-receitaws", retries=1, retry_delay_seconds=30)
def enrich_receitaws_task(
    run_receitaws: bool = True,
    partition_id: int | None = None,
    vintage: str | None = None,
    limit: int | None = None,
) -> int:
    if not run_receitaws or not ENABLE_RECEITAWS:
        logger.info("ReceitaWS desabilitado (run_receitaws=false ou ENABLE_RECEITAWS=false).")
        return 0
    return fetch_receitaws_enrichment(
        DUCKDB_PATH,
        limit=limit,
        partition_id=str(partition_id) if partition_id is not None else None,
        vintage=vintage,
    )


@task(name="dbt-run", retries=1, retry_delay_seconds=15, log_prints=True)
def dbt_run_task(select: str | None = None) -> None:
    args = ["run"]
    if select:
        args.extend(["--select", select])
    _run_dbt(["deps"])
    _run_dbt(args)


@task(name="dbt-snapshot", retries=1, retry_delay_seconds=15, log_prints=True)
def dbt_snapshot_task() -> None:
    _run_dbt(["snapshot"])


@task(name="dbt-test", retries=0, log_prints=True)
def dbt_test_task(select: str | None = None) -> None:
    args = ["test"]
    if select:
        args.extend(["--select", select])
    _run_dbt(args)


@task(name="trigger-downstream-deployment", retries=1, retry_delay_seconds=5)
def trigger_downstream_task(
    deployment_name: str,
    parameters: dict,
    flow_run_name: str | None = None,
) -> str:
    return trigger_deployment(deployment_name, parameters, flow_run_name=flow_run_name)


def _run_dbt(args: list[str]) -> None:
    from prefect.logging import get_run_logger

    run_logger = get_run_logger()
    cmd = ["dbt", *args]
    cmd_str = " ".join(cmd)
    run_logger.info("=== dbt %s ===", " ".join(args))
    run_logger.info("Comando: %s", cmd_str)
    run_logger.info("Diretório: %s", DBT_DIR)

    process = subprocess.Popen(
        cmd,
        cwd=DBT_DIR,
        env={**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is None:
        raise RuntimeError(f"Falha ao capturar stdout de: {cmd_str}")

    for line in process.stdout:
        line = line.rstrip()
        if line:
            run_logger.info(line)

    returncode = process.wait()
    if returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} falhou com código {returncode}")

    run_logger.info("=== dbt %s concluído (exit 0) ===", " ".join(args))
