-- Teste customizado: empresas ativas devem ter capital social não negativo
select
    cnpj,
    capital_social
from {{ ref('fct_empresas_ativas') }}
where capital_social < 0
