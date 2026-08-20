with source as (
    select * from {{ source('raw', 'raw_empresas') }}
),

renamed as (
    select
        {{ clean_text('cnpj_basico') }} as cnpj_basico,
        {{ clean_text('razao_social') }} as razao_social,
        {{ clean_text('natureza_juridica') }} as natureza_juridica,
        {{ clean_text('qualificacao_responsavel') }} as qualificacao_responsavel,
        {{ parse_capital_social('capital_social') }} as capital_social,
        {{ clean_text('porte_empresa') }} as porte_empresa,
        {{ clean_text('ente_federativo_responsavel') }} as ente_federativo_responsavel,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from renamed
