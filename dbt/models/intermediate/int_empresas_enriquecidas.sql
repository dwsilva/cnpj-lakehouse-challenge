-- depends_on: {{ ref('stg_estabelecimentos') }}
-- depends_on: {{ ref('stg_empresas') }}
-- depends_on: {{ ref('stg_socios') }}
-- depends_on: {{ ref('stg_simples') }}
-- depends_on: {{ ref('stg_receitaws') }}

with estabelecimentos as (
    select * from {{ ref('stg_estabelecimentos') }}
),

empresas as (
    select * from {{ ref('stg_empresas') }}
),

socios_agg as (
    select
        cnpj_basico,
        count(*) as qty_socios
    from {{ ref('stg_socios') }}
    group by 1
),

simples as (
    select * from {{ ref('stg_simples') }}
),

receitaws as (
    select * from {{ ref('stg_receitaws') }}
),

joined as (
    select
        e.cnpj,
        e.cnpj_basico,
        emp.razao_social,
        emp.capital_social,
        emp.porte_empresa,
        e.nome_fantasia,
        e.situacao_cadastral,
        e.situacao_cadastral_codigo,
        e.data_inicio_atividade,
        e.cnae_fiscal_principal,
        e.uf,
        e.municipio,
        coalesce(s.qty_socios, 0) as qty_socios,
        coalesce(sim.opcao_pelo_simples, 'N') = 'S' as is_simples,
        coalesce(sim.opcao_pelo_mei, 'N') = 'S' as is_mei,
        rw.situacao_api,
        rw.capital_social_api,
        rw.porte_api,
        rw.cnae_api,
        rw.uf_api,
        rw.municipio_api,
        rw.receitaws_source,
        (rw.cnpj is not null) as has_receitaws_enrichment,
        (
            rw.cnpj is not null
            and upper(trim(coalesce(rw.situacao_api, ''))) = upper(trim(coalesce(e.situacao_cadastral, '')))
        ) as receitaws_situacao_match,
        (
            rw.cnpj is not null
            and rw.capital_social_api is not null
            and abs(coalesce(emp.capital_social, 0) - rw.capital_social_api) < 0.01
        ) as receitaws_capital_match,
        {{ audit_row_hash(['emp.capital_social', 'e.situacao_cadastral', 'coalesce(s.qty_socios, 0)']) }} as row_hash,
        greatest(
            coalesce(e.dbt_updated_at, current_timestamp::timestamp),
            coalesce(emp.dbt_updated_at, current_timestamp::timestamp)
        ) as dbt_updated_at
    from estabelecimentos e
    inner join empresas emp
        on e.cnpj_basico = emp.cnpj_basico
    left join socios_agg s
        on e.cnpj_basico = s.cnpj_basico
    left join simples sim
        on e.cnpj_basico = sim.cnpj_basico
    left join receitaws rw
        on e.cnpj = rw.cnpj
    where e.identificador_matriz_filial = '1'
)

select * from joined
