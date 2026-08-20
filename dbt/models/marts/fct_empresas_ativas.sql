{%- if target.type == 'bigquery' -%}
{{
    config(
        materialized='incremental',
        unique_key='cnpj',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        partition_by={
            'field': 'data_referencia',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['uf', 'cnae_fiscal_principal', 'situacao_cadastral'],
        require_partition_filter=True,
    )
}}
{%- else -%}
{{
    config(
        materialized='incremental',
        unique_key='cnpj',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
    )
}}
{%- endif -%}

with base as (
    select *
    from {{ ref('int_empresas_enriquecidas') }}
    where situacao_cadastral = 'ATIVA'
)

select
    cnpj,
    cnpj_basico,
    razao_social,
    nome_fantasia,
    capital_social,
    porte_empresa,
    situacao_cadastral,
    data_inicio_atividade,
    cnae_fiscal_principal,
    uf,
    municipio,
    qty_socios,
    is_simples,
    is_mei,
    row_hash,
    current_date as data_referencia,
    dbt_updated_at
from base

{% if is_incremental() %}
where dbt_updated_at > (select coalesce(max(dbt_updated_at), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
