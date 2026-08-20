{% macro parse_rf_date(column_name) %}
    {%- if target.type == 'bigquery' -%}
        safe.parse_date('%Y%m%d', nullif(trim(cast({{ column_name }} as string)), ''))
    {%- else -%}
        case
            when nullif(trim(cast({{ column_name }} as varchar)), '') is null then null
            when length(trim(cast({{ column_name }} as varchar))) = 8
                then strptime(trim(cast({{ column_name }} as varchar)), '%Y%m%d')::date
            else try_cast(trim(cast({{ column_name }} as varchar)) as date)
        end
    {%- endif -%}
{% endmacro %}
