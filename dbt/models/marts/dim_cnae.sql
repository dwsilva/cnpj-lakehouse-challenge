select
    codigo_cnae,
    descricao_cnae,
    secao_cnae,
    divisao_cnae,
    grupo_cnae,
    classe_cnae,
    dbt_updated_at
from {{ ref('stg_cnae') }}
