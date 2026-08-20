{% snapshot snap_empresas_capital_social %}

{{
    config(
        target_schema='snapshots',
        unique_key='cnpj_basico',
        strategy='timestamp',
        updated_at='dbt_updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    cnpj_basico,
    razao_social,
    capital_social,
    porte_empresa,
    dbt_updated_at
from {{ ref('stg_empresas') }}

{% endsnapshot %}
