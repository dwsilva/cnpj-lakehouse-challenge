-- Falha se colunas críticas da dim_empresa estiverem majoritariamente nulas.
-- Complementa os not_null_proportion do schema.yml com um check agregado legível.

with metricas as (
    select
        count(*) as total,
        count(data_inicio_atividade) as com_data_inicio,
        count(razao_social) as com_razao,
        count(situacao_cadastral) as com_situacao,
        count(cnae_fiscal_principal) as com_cnae,
        count(uf) as com_uf
    from {{ ref('dim_empresa') }}
)

select *
from metricas
where total = 0
    or com_data_inicio::double / total < 0.95
    or com_razao::double / total < 0.99
    or com_situacao::double / total < 0.99
    or com_cnae::double / total < 0.90
    or com_uf::double / total < 0.95
