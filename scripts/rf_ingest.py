"""Carga bronze DuckDB a partir dos ZIPs baixados (por partição)."""

from __future__ import annotations

import argparse
import re
import zipfile
from pathlib import Path

import pandas as pd
from loguru import logger

from scripts.config import (
    DUCKDB_PATH,
    PREFIX_ALIASES,
    RAW_TABLES,
    RF_VINTAGE,
    SAMPLE_N_ROWS,
    TABLE_COLUMNS,
    raw_vintage_dir,
)
from scripts.load_duckdb import load_partition_to_duckdb
from scripts.rf_download import download_partition

SAMPLE_ORDER = ["Estabelecimentos", "Empresas", "Socios", "Simples", "CNAE"]
CHUNK_SIZE = 100_000


def _partition_token(path: Path) -> str | None:
    match = re.search(r"(\d+)\s*$", path.stem)
    return match.group(1) if match else None


def _find_source_file(raw_dir: Path, prefix: str, partition: str | None = None) -> Path | None:
    prefixes = PREFIX_ALIASES.get(prefix, [prefix])
    candidates: list[Path] = []
    for name_prefix in prefixes:
        candidates.extend(sorted(raw_dir.glob(f"{name_prefix}*")))

    for path in candidates:
        if path.suffix.lower() not in {".csv", ".zip"}:
            continue
        if partition is not None and _partition_token(path) != partition:
            continue
        return path
    return None


def _iter_csv_chunks(path: Path, columns: list[str], chunksize: int = CHUNK_SIZE):
    read_kwargs = {
        "sep": ";",
        "dtype": str,
        "encoding": "latin-1",
        "header": None,
        "names": columns,
        "chunksize": chunksize,
    }
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            inner_names = [n for n in zf.namelist() if not n.endswith("/")]
            if not inner_names:
                raise ValueError(f"ZIP vazio: {path}")
            with zf.open(inner_names[0]) as handle:
                yield from pd.read_csv(handle, **read_kwargs)
    else:
        yield from pd.read_csv(path, **read_kwargs)


def _read_csv_head(path: Path, expected_cols: list[str], n_rows: int) -> pd.DataFrame:
    for chunk in _iter_csv_chunks(path, expected_cols, chunksize=n_rows):
        return chunk.head(n_rows)
    return pd.DataFrame(columns=expected_cols)


def _read_csv_full(path: Path, columns: list[str]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for chunk in _iter_csv_chunks(path, columns):
        parts.append(chunk)
    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)


def _filter_by_cnpj_basico(
    path: Path,
    columns: list[str],
    cnpj_keys: set[str],
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    found_keys: set[str] = set()

    for chunk in _iter_csv_chunks(path, columns):
        filtered = chunk[chunk["cnpj_basico"].isin(cnpj_keys)]
        if filtered.empty:
            continue
        parts.append(filtered)
        found_keys.update(filtered["cnpj_basico"].dropna().unique())
        if found_keys >= cnpj_keys:
            break

    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)


def build_partition_frames(
    vintage: str,
    partition_id: int,
    sample_n_rows: int | None = None,
) -> dict[str, pd.DataFrame]:
    raw_dir = raw_vintage_dir(vintage)
    partition = str(partition_id)
    frames: dict[str, pd.DataFrame] = {}

    estab_source = _find_source_file(raw_dir, "Estabelecimentos", partition=partition)
    if estab_source is None:
        raise FileNotFoundError(
            f"Estabelecimentos{partition_id}.zip não encontrado em {raw_dir}. "
            "Rode o download antes."
        )

    estab_cols = TABLE_COLUMNS["raw_estabelecimentos"]
    if sample_n_rows:
        estab_df = _read_csv_head(estab_source, estab_cols, sample_n_rows)
    else:
        estab_df = _read_csv_full(estab_source, estab_cols)

    frames["raw_estabelecimentos"] = estab_df
    cnpj_keys = set(estab_df["cnpj_basico"].dropna().unique())
    logger.info(
        "Estabelecimentos{}: {} linhas, {} cnpj_basico",
        partition_id,
        len(estab_df),
        len(cnpj_keys),
    )

    for prefix in SAMPLE_ORDER[1:]:
        if prefix in {"Simples", "CNAE"} and partition_id > 0:
            continue

        table_name = RAW_TABLES[prefix]
        source = _find_source_file(raw_dir, prefix, partition=partition)
        if source is None and prefix in {"Simples", "CNAE"}:
            source = _find_source_file(raw_dir, prefix)

        if source is None:
            logger.warning("Arquivo ausente para {} (partição {}).", prefix, partition_id)
            continue

        columns = TABLE_COLUMNS[table_name]
        if sample_n_rows:
            if prefix == "CNAE":
                df = _read_csv_full(source, columns)
            elif prefix == "Simples":
                df = _filter_by_cnpj_basico(source, columns, cnpj_keys)
            else:
                logger.info("Filtrando {} por {} cnpj_basico...", source.name, len(cnpj_keys))
                df = _filter_by_cnpj_basico(source, columns, cnpj_keys)
        else:
            df = _read_csv_full(source, columns)

        if df.empty:
            logger.warning("{} sem linhas para partição {}.", table_name, partition_id)
            continue

        frames[table_name] = df
        logger.info("{}: {} linhas", table_name, len(df))

    return frames


def ingest_partition(
    partition_id: int,
    vintage: str | None = None,
    sample_n_rows: int | None = None,
    db_path: Path | None = None,
    download: bool = True,
) -> dict[str, int]:
    vintage = vintage or RF_VINTAGE
    db_path = db_path or DUCKDB_PATH

    if download:
        download_partition(vintage, partition_id, include_shared=(partition_id == 0))

    frames = build_partition_frames(vintage, partition_id, sample_n_rows)
    counts = load_partition_to_duckdb(
        frames,
        partition_id=str(partition_id),
        vintage=vintage,
        db_path=db_path,
    )
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestão bronze CNPJ por partição")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--vintage", default=RF_VINTAGE)
    parser.add_argument("--sample-n-rows", type=int, default=None)
    parser.add_argument("--no-download", action="store_true")
    args = parser.parse_args()

    sample = args.sample_n_rows if args.sample_n_rows else SAMPLE_N_ROWS
    ingest_partition(
        partition_id=args.partition,
        vintage=args.vintage,
        sample_n_rows=sample,
        download=not args.no_download,
    )


if __name__ == "__main__":
    main()
