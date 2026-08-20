from __future__ import annotations

import asyncio
import os

from loguru import logger
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import run_deployment

DEPLOYMENT_PARTICAO = os.getenv(
    "PREFECT_DEPLOYMENT_PARTICAO", "ingestao_particao_cnpj/ingestao-particao-cnpj"
)
DEPLOYMENT_COMPLETA = os.getenv(
    "PREFECT_DEPLOYMENT_COMPLETA", "ingestao_completa_cnpj/ingestao-completa-cnpj"
)
DEPLOYMENT_TRANSFORMACAO = os.getenv(
    "PREFECT_DEPLOYMENT_TRANSFORMACAO", "transformacao_cnpj/transformacao-cnpj"
)

# Deployments do refactor anterior — ficam presos no volume prefect-data até apagar
DEPRECATED_DEPLOYMENT_NAMES = frozenset(
    {
        "coleta-cnpj-demo",
        "ingestao-cnpj-demo",
        "transformacao-cnpj-demo",
        "cnpj-pipeline-demo",
    }
)
DEPRECATED_FLOW_NAMES = frozenset(
    {
        "coleta_cnpj",
        "ingestao_cnpj",
    }
)

# Dia 5 às 06:00 BRT, janela D+2 após publicação mensal da RF
COLETA_CRON = os.getenv("PREFECT_COLETA_CRON", "0 6 5 * *")
COLETA_TIMEZONE = os.getenv("PREFECT_COLETA_TIMEZONE", "America/Sao_Paulo")


def coleta_monthly_schedule() -> CronSchedule:
    return CronSchedule(cron=COLETA_CRON, timezone=COLETA_TIMEZONE)


async def _cleanup_deprecated_deployments_async() -> int:
    from prefect import get_client

    removed = 0
    async with get_client() as client:
        for deployment in await client.read_deployments():
            if (
                deployment.name in DEPRECATED_DEPLOYMENT_NAMES
                or deployment.name.endswith("-demo")
                or deployment.flow_name in DEPRECATED_FLOW_NAMES
            ):
                logger.info(
                    "Removendo deployment obsoleto: {}/{}",
                    deployment.flow_name,
                    deployment.name,
                )
                await client.delete_deployment(deployment.id)
                removed += 1
    return removed


def cleanup_deprecated_deployments() -> int:
    """Apaga deployments legados da UI (refactor demo → ingestao_particao)."""
    try:
        removed = asyncio.run(_cleanup_deprecated_deployments_async())
        if removed:
            logger.info("Deployments obsoletos removidos: {}", removed)
        return removed
    except Exception as exc:
        logger.warning("Limpeza de deployments ignorada (Prefect API indisponível?): {}", exc)
        return 0


def trigger_deployment(    deployment_name: str,
    parameters: dict,
    *,
    flow_run_name: str | None = None,
) -> str:
    logger.info("Encadeando deployment {} parameters={}", deployment_name, parameters)
    flow_run = run_deployment(
        name=deployment_name,
        parameters=parameters,
        flow_run_name=flow_run_name,
        timeout=0,
        as_subflow=False,
    )
    run_id = str(flow_run.id)
    logger.info("Deployment {} agendado — flow_run_id={}", deployment_name, run_id)
    return run_id
