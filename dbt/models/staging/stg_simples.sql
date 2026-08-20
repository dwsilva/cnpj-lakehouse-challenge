with source as (
    select * from {{ source('raw', 'raw_simples') }}
),

renamed as (
    select
        {{ clean_text('cnpj_basico') }} as cnpj_basico,
        upper({{ clean_text('opcao_pelo_simples') }}) as opcao_pelo_simples,
        {{ parse_rf_date('data_opcao_simples') }} as data_opcao_simples,
        {{ parse_rf_date('data_exclusao_simples') }} as data_exclusao_simples,
        upper({{ clean_text('opcao_pelo_mei') }}) as opcao_pelo_mei,
        {{ parse_rf_date('data_opcao_mei') }} as data_opcao_mei,
        {{ parse_rf_date('data_exclusao_mei') }} as data_exclusao_mei,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from renamed
