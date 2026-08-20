with source as (
    select * from {{ source('raw', 'receitaws_enrichment') }}
),

parsed as (
    select
        cnpj,
        json_extract_string(payload, '$.nome') as razao_social_api,
        json_extract_string(payload, '$.fantasia') as nome_fantasia_api,
        json_extract_string(payload, '$.situacao') as situacao_api,
        json_extract_string(payload, '$.tipo') as tipo_api,
        json_extract_string(payload, '$.porte') as porte_api,
        json_extract_string(payload, '$.natureza_juridica') as natureza_juridica_api,
        json_extract_string(payload, '$.abertura') as abertura_api,
        try_cast(
            replace(json_extract_string(payload, '$.capital_social'), ',', '.') as double
        ) as capital_social_api,
        json_extract_string(payload, '$.atividade_principal[0].code') as cnae_api,
        json_extract_string(payload, '$.atividade_principal[0].text') as cnae_descricao_api,
        json_extract_string(payload, '$.municipio') as municipio_api,
        json_extract_string(payload, '$.uf') as uf_api,
        json_extract_string(payload, '$.cep') as cep_api,
        json_extract_string(payload, '$.email') as email_api,
        json_extract_string(payload, '$.telefone') as telefone_api,
        source as receitaws_source,
        fetched_at,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from parsed
