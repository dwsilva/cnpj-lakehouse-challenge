{% macro parse_capital_social(column_name) %}
    COALESCE(
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(TRIM({{ column_name }}), '.', ''), ',', '.'), ' ', '')
            AS DOUBLE
        ),
        0
    )
{% endmacro %}
