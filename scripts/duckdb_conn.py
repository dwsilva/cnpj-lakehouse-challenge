from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
from loguru import logger

from scripts.config import DUCKDB_PATH

_session_tmp: Path | None = None


def _can_open_for_write(path: Path) -> bool:
    try:
        conn = duckdb.connect(str(path))
        conn.execute("SELECT 1")
        conn.close()
        return True
    except duckdb.IOException as exc:
        if "Permission denied" in str(exc) or "being used by another process" in str(exc):
            return False
        raise


def resolve_duckdb_path(preferred: Path | None = None) -> Path:
    global _session_tmp

    current = os.environ.get("DUCKDB_PATH")
    if current and _can_open_for_write(Path(current)):
        return Path(current)

    if _session_tmp is not None and _session_tmp.exists() and _can_open_for_write(_session_tmp):
        os.environ["DUCKDB_PATH"] = str(_session_tmp)
        return _session_tmp

    primary = Path(preferred or DUCKDB_PATH)
    primary.parent.mkdir(parents=True, exist_ok=True)

    if _can_open_for_write(primary):
        os.environ["DUCKDB_PATH"] = str(primary)
        return primary

    tmp = Path("/tmp") / primary.name
    if primary.exists():
        shutil.copy2(primary, tmp)
        logger.warning(
            "DuckDB bloqueado em {} (feche conexões no DBeaver). "
            "Usando cópia de trabalho: {}",
            primary,
            tmp,
        )
    else:
        logger.warning(
            "DuckDB indisponível em {}. Criando banco em {}.",
            primary,
            tmp,
        )

    _session_tmp = tmp
    os.environ["DUCKDB_PATH"] = str(tmp)
    return tmp


def connect_duckdb(preferred: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = resolve_duckdb_path(preferred)
    return duckdb.connect(str(path))


def sync_duckdb_to_primary(preferred: Path | None = None) -> None:
    global _session_tmp

    if _session_tmp is None:
        return

    primary = Path(preferred or DUCKDB_PATH)
    try:
        primary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_session_tmp, primary)
        logger.info("DuckDB sincronizado: {} → {}", _session_tmp, primary)
    except OSError as exc:
        logger.error(
            "Falha ao sincronizar DuckDB para {} (feche DBeaver e tente de novo): {}",
            primary,
            exc,
        )
        raise RuntimeError(
            f"Não foi possível gravar em {primary}. Feche conexões externas (DBeaver)."
        ) from exc
    finally:
        _session_tmp = None
