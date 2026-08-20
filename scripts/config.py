from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(PROJECT_ROOT / "duckdb" / "cnpj.duckdb")))

SAMPLE_N_ROWS = int(os.getenv("SAMPLE_N_ROWS", "10000"))
RF_VINTAGE = os.getenv("RF_VINTAGE", "2026-07-12")
RF_BASE_URL = os.getenv(
    "RF_BASE_URL",
    "https://dados-abertos-rf-cnpj.casadosdados.com.br",
)

# Mapeamento: prefixo do arquivo ZIP/CSV da RF -> nome da tabela raw
RAW_TABLES: dict[str, str] = {
    "Empresas": "raw_empresas",
    "Estabelecimentos": "raw_estabelecimentos",
    "Socios": "raw_socios",
    "Simples": "raw_simples",
    "CNAE": "raw_cnae",
}

PREFIX_ALIASES: dict[str, list[str]] = {
    "CNAE": ["CNAE", "Cnaes"],
}

PARTITIONED_PREFIXES = ("Empresas", "Estabelecimentos", "Socios")
SHARED_PREFIXES = ("Simples", "CNAE")

EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
]

ESTABELECIMENTOS_COLUMNS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

SOCIOS_COLUMNS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cnpj_cpf_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

SIMPLES_COLUMNS = [
    "cnpj_basico",
    "opcao_pelo_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_pelo_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
]

CNAE_COLUMNS = ["codigo", "descricao"]

TABLE_COLUMNS: dict[str, list[str]] = {
    "raw_empresas": EMPRESAS_COLUMNS,
    "raw_estabelecimentos": ESTABELECIMENTOS_COLUMNS,
    "raw_socios": SOCIOS_COLUMNS,
    "raw_simples": SIMPLES_COLUMNS,
    "raw_cnae": CNAE_COLUMNS,
}

BRONZE_METADATA = ("_partition_id", "_vintage", "_loaded_at", "_source_file")


def raw_vintage_dir(vintage: str | None = None) -> Path:
    return PROJECT_ROOT / "data" / "raw" / (vintage or RF_VINTAGE)
