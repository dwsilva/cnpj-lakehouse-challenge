{%- if target.type == 'bigquery' -%}
{{
    config(
        materialized='table',
        cluster_by=['cnpj_basico', 'porte_empresa'],
    )
}}
{%- endif -%}

select
    cnpj,
    cnpj_basico,
    razao_social,
    nome_fantasia,
    porte_empresa,
    capital_social,
    situacao_cadastral,
    data_inicio_atividade,
    cnae_fiscal_principal,
    uf,
    municipio,
    dbt_updated_at
from {{ ref('int_empresas_enriquecidas') }}
