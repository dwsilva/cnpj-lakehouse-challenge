from __future__ import annotations

import json
import os
import time
from pathlib import Path

import duckdb
import httpx
from loguru import logger

from scripts.config import DUCKDB_PATH
from scripts.duckdb_conn import connect_duckdb

RECEITAWS_BASE_URL = os.getenv("RECEITAWS_BASE_URL", "https://receitaws.com.br/v1/cnpj")
RECEITAWS_SAMPLE_SIZE = int(os.getenv("RECEITAWS_SAMPLE_SIZE", "5"))
RECEITAWS_REQUEST_DELAY_SECONDS = float(os.getenv("RECEITAWS_REQUEST_DELAY_SECONDS", "3"))


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.receitaws_enrichment (
            cnpj VARCHAR,
            payload JSON,
            fetched_at TIMESTAMP,
            source VARCHAR
        )
        """
    )


def _existing_cnpjs(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("SELECT cnpj FROM raw.receitaws_enrichment").fetchall()
    return {row[0] for row in rows}


def _insert_payload(
    conn: duckdb.DuckDBPyConnection,
    cnpj: str,
    payload: str,
    source: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw.receitaws_enrichment (cnpj, payload, fetched_at, source)
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [cnpj, payload, source],
    )


def _list_target_cnpjs(
    conn: duckdb.DuckDBPyConnection,
    limit: int | None,
    partition_id: str | None = None,
    vintage: str | None = None,
) -> list[str]:
    filters = [
        "identificador_matriz_filial = '1'",
        "situacao_cadastral = '02'",
        "cnpj_basico IS NOT NULL",
    ]
    params: list[object] = []

    if partition_id is not None:
        filters.append("_partition_id = ?")
        params.append(partition_id)
    if vintage is not None:
        filters.append("_vintage = ?")
        params.append(vintage)

    sql = f"""
        SELECT DISTINCT
            LPAD(cnpj_basico, 8, '0')
            || LPAD(cnpj_ordem, 4, '0')
            || LPAD(cnpj_dv, 2, '0') AS cnpj
        FROM raw.raw_estabelecimentos
        WHERE {' AND '.join(filters)}
        ORDER BY 1
    """
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    return [row[0] for row in rows]


def fetch_receitaws_enrichment(
    db_path: Path | None = None,
    limit: int | None = None,
    partition_id: str | None = None,
    vintage: str | None = None,
) -> int:
    db_path = db_path or DUCKDB_PATH
    limit = limit if limit is not None else RECEITAWS_SAMPLE_SIZE

    conn = connect_duckdb(db_path)
    _ensure_table(conn)

    cnpjs = _list_target_cnpjs(conn, limit, partition_id, vintage)
    if not cnpjs:
        logger.warning("Nenhum CNPJ matriz ATIVA para enriquecer via ReceitaWS.")
        conn.close()
        return 0

    existing = _existing_cnpjs(conn)
    inserted = 0

    logger.info("ReceitaWS: {} CNPJs alvo (limit={}).", len(cnpjs), limit)

    with httpx.Client(timeout=30.0) as client:
        for cnpj in cnpjs:
            if cnpj in existing:
                logger.info("ReceitaWS cache hit: {}", cnpj)
                continue

            url = f"{RECEITAWS_BASE_URL}/{cnpj}"
            try:
                response = client.get(url)
                if response.status_code == 429:
                    logger.warning("ReceitaWS rate limit (429). Aguardando retry...")
                    time.sleep(RECEITAWS_REQUEST_DELAY_SECONDS * 2)
                    response = client.get(url)
                response.raise_for_status()
                payload = response.text
                try:
                    json.loads(payload)
                except json.JSONDecodeError:
                    logger.error("Resposta inválida ReceitaWS {}: {}", cnpj, payload[:120])
                    continue
                _insert_payload(conn, cnpj, payload, "receitaws_api")
                inserted += 1
                logger.info("ReceitaWS API OK: {}", cnpj)
            except httpx.HTTPError as exc:
                logger.error("Falha ReceitaWS {}: {}", cnpj, exc)

            time.sleep(RECEITAWS_REQUEST_DELAY_SECONDS)

    conn.close()
    return inserted
