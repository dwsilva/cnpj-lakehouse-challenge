with source as (
    select * from {{ source('raw', 'raw_estabelecimentos') }}
),

renamed as (
    select
        {{ clean_text('cnpj_basico') }} as cnpj_basico,
        {{ clean_text('cnpj_ordem') }} as cnpj_ordem,
        {{ clean_text('cnpj_dv') }} as cnpj_dv,
        {{ format_cnpj('cnpj_basico', 'cnpj_ordem', 'cnpj_dv') }} as cnpj,
        {{ clean_text('identificador_matriz_filial') }} as identificador_matriz_filial,
        {{ clean_text('nome_fantasia') }} as nome_fantasia,
        {{ clean_text('situacao_cadastral') }} as situacao_cadastral_codigo,
        {{ standardize_situacao_cadastral('situacao_cadastral') }} as situacao_cadastral,
        {{ parse_rf_date('data_situacao_cadastral') }} as data_situacao_cadastral,
        {{ clean_text('motivo_situacao_cadastral') }} as motivo_situacao_cadastral,
        {{ parse_rf_date('data_inicio_atividade') }} as data_inicio_atividade,
        lpad({{ clean_text('cnae_fiscal_principal') }}, 7, '0') as cnae_fiscal_principal,
        {{ clean_text('cnae_fiscal_secundaria') }} as cnae_fiscal_secundaria,
        {{ clean_text('uf') }} as uf,
        {{ clean_text('municipio') }} as municipio,
        {{ clean_text('cep') }} as cep,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from renamed
