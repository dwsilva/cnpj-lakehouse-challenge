with source as (
    select * from {{ source('raw', 'raw_cnae') }}
),

renamed as (
    select
        lpad({{ clean_text('codigo') }}, 7, '0') as codigo_cnae,
        {{ clean_text('descricao') }} as descricao_cnae,
        left(lpad({{ clean_text('codigo') }}, 7, '0'), 1) as secao_cnae,
        left(lpad({{ clean_text('codigo') }}, 7, '0'), 2) as divisao_cnae,
        left(lpad({{ clean_text('codigo') }}, 7, '0'), 3) as grupo_cnae,
        left(lpad({{ clean_text('codigo') }}, 7, '0'), 5) as classe_cnae,
        current_timestamp::timestamp as dbt_updated_at
    from source
)

select * from renamed
