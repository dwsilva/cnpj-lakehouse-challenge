{% macro standardize_situacao_cadastral(column_name) %}
    case trim(cast({{ column_name }} as varchar))
        when '01' then 'NULA'
        when '02' then 'ATIVA'
        when '03' then 'SUSPENSA'
        when '04' then 'INAPTA'
        when '08' then 'BAIXADA'
        else 'DESCONHECIDA'
    end
{% endmacro %}
