{% macro audit_row_hash(columns) %}
    MD5(
        CONCAT_WS(
            '|',
            {%- for col in columns -%}
                COALESCE(CAST({{ col }} AS VARCHAR), '')
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%}
        )
    )
{% endmacro %}
