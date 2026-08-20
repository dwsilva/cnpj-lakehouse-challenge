from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger

from scripts.config import BRONZE_METADATA, DUCKDB_PATH, TABLE_COLUMNS
from scripts.duckdb_conn import connect_duckdb


def _ensure_schema(conn) -> None:
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


def _table_column_names(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"DESCRIBE raw.{table_name}").fetchall()
    return {row[0] for row in rows}


def _ensure_raw_table(conn, table_name: str, columns: list[str]) -> None:
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
    meta_defs = ", ".join(
        f'"{c}" {"TIMESTAMP" if c == "_loaded_at" else "VARCHAR"}' for c in BRONZE_METADATA
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS raw.{table_name} (
            {col_defs},
            {meta_defs}
        )
        """
    )


def _migrate_raw_table(conn, table_name: str, columns: list[str]) -> None:
    """Tabelas criadas antes do refactor não tinham _partition_id / _vintage."""
    _ensure_raw_table(conn, table_name, columns)
    existing = _table_column_names(conn, table_name)

    for meta_col in BRONZE_METADATA:
        if meta_col in existing:
            continue
        col_type = "TIMESTAMP" if meta_col == "_loaded_at" else "VARCHAR"
        logger.info("Migrando raw.{}: adicionando coluna {}", table_name, meta_col)
        conn.execute(f'ALTER TABLE raw.{table_name} ADD COLUMN "{meta_col}" {col_type}')

    legacy_count = conn.execute(
        f"SELECT COUNT(*) FROM raw.{table_name} WHERE _partition_id IS NULL"
    ).fetchone()[0]
    if legacy_count:
        logger.info(
            "Removendo {} linhas legadas (sem partição) de raw.{}",
            legacy_count,
            table_name,
        )
        conn.execute(f"DELETE FROM raw.{table_name} WHERE _partition_id IS NULL")


def _upsert_partition(
    conn,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
    partition_id: str,
    vintage: str,
    source_file: str,
) -> int:
    _migrate_raw_table(conn, table_name, columns)

    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA

    df = df[columns].copy()
    df["_partition_id"] = partition_id
    df["_vintage"] = vintage
    df["_loaded_at"] = datetime.now(timezone.utc)
    df["_source_file"] = source_file

    all_columns = columns + list(BRONZE_METADATA)
    cols_sql = ", ".join(f'"{c}"' for c in all_columns)

    conn.execute(
        f"""
        DELETE FROM raw.{table_name}
        WHERE _partition_id = ? AND _vintage = ?
        """,
        [partition_id, vintage],
    )

    conn.register("_tmp_df", df[all_columns])
    conn.execute(
        f"INSERT INTO raw.{table_name} ({cols_sql}) SELECT {cols_sql} FROM _tmp_df"
    )
    conn.unregister("_tmp_df")

    count = conn.execute(
        f"""
        SELECT COUNT(*) FROM raw.{table_name}
        WHERE _partition_id = ? AND _vintage = ?
        """,
        [partition_id, vintage],
    ).fetchone()[0]
    logger.info("raw.{} partição {} / {}: {} linhas", table_name, partition_id, vintage, count)
    return count


def load_partition_to_duckdb(
    frames: dict[str, pd.DataFrame],
    partition_id: str,
    vintage: str,
    db_path: Path | None = None,
) -> dict[str, int]:
    db_path = db_path or DUCKDB_PATH
    conn = connect_duckdb(db_path)
    _ensure_schema(conn)

    counts: dict[str, int] = {}
    for table_name, df in frames.items():
        columns = TABLE_COLUMNS[table_name]
        row_partition = "shared" if table_name in {"raw_simples", "raw_cnae"} else partition_id
        source_file = f"{vintage}/partition_{row_partition}/{table_name}"
        counts[table_name] = _upsert_partition(
            conn, table_name, df, columns, row_partition, vintage, source_file
        )

    conn.close()
    return counts
