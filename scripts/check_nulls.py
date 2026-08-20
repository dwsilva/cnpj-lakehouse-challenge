"""Checa taxa de nulos nas marts — uso local/debug."""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

from scripts.config import DUCKDB_PATH

COLS = [
    "razao_social",
    "nome_fantasia",
    "capital_social",
    "situacao_cadastral",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "uf",
    "municipio",
]


def main() -> None:
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DUCKDB_PATH
    if not db.exists():
        print(f"Banco não encontrado: {db}")
        sys.exit(1)

    conn = duckdb.connect(str(db), read_only=True)

    print(f"\n=== DuckDB cast test ===")
    for val in ("20250710", "2015-01-01", ""):
        r = conn.execute(
            "select try_cast(? as date) as iso, strptime(?, '%Y%m%d')::date as rf",
            [val, val],
        ).fetchone()
        print(f"  {val!r} -> try_cast={r[0]!r}, strptime={r[1]!r}")

    print(f"\n=== raw.raw_estabelecimentos (amostra) ===")
    print(
        conn.execute(
            """
            select data_inicio_atividade, count(*)
            from raw.raw_estabelecimentos
            group by 1
            order by 2 desc
            limit 5
            """
        ).fetchdf()
    )

    for table in (
        "staging.stg_estabelecimentos",
        "intermediate.int_empresas_enriquecidas",
        "marts.dim_empresa",
    ):
        try:
            total = conn.execute(f"select count(*) from {table}").fetchone()[0]
            print(f"\n=== {table} ({total} linhas) ===")
            for col in COLS:
                if col not in {r[0] for r in conn.execute(f"describe {table}").fetchall()}:
                    continue
                filled = conn.execute(
                    f"select count({col}) from {table} where {col} is not null and cast({col} as varchar) != ''"
                ).fetchone()[0]
                pct = (filled / total * 100) if total else 0
                print(f"  {col:26} {filled:6}/{total} ({pct:.1f}%)")
        except duckdb.CatalogException as exc:
            print(f"\n{table}: {exc}")

    conn.close()


if __name__ == "__main__":
    main()
