with source as (
    select * from {{ source('raw', 'raw_socios') }}
),

renamed as (
    select
        {{ clean_text('cnpj_basico') }} as cnpj_basico,
        {{ clean_text('identificador_socio') }} as identificador_socio,
        {{ clean_text('nome_socio') }} as nome_socio,
        {{ clean_text('cnpj_cpf_socio') }} as cnpj_cpf_socio,
        {{ clean_text('qualificacao_socio') }} as qualificacao_socio,
        {{ parse_rf_date('data_entrada_sociedade') }} as data_entrada_sociedade,
        {{ clean_text('pais') }} as pais,
        {{ clean_text('faixa_etaria') }} as faixa_etaria,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from renamed
