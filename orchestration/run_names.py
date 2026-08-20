from __future__ import annotations

from datetime import datetime, timezone


def format_run_name(prefix: str, label_param: str = "run_label"):
    def _name() -> str:
        from prefect.runtime import flow_run

        label = flow_run.parameters.get(label_param, "manual")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        if label in ("manual", "ui", "scheduled"):
            return f"{prefix}-{label}-{ts}"
        return f"{prefix}-{label}"

    return _name


def particao_run_name() -> str:
    from prefect.runtime import flow_run

    label = flow_run.parameters.get("run_label", "manual")
    pid = flow_run.parameters.get("partition_id", 0)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    if label in ("manual", "ui", "scheduled"):
        return f"ingestao-particao-p{pid}-{label}-{ts}"
    return f"ingestao-particao-p{pid}-{label}"


def completa_run_name() -> str:
    return format_run_name("ingestao-completa")()


def transformacao_run_name() -> str:
    return format_run_name("transformacao-cnpj")()


def pipeline_run_name() -> str:
    return format_run_name("cnpj-pipeline")()
