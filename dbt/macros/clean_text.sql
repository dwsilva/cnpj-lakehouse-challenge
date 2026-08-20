{% macro clean_text(column_name) %}
    trim(cast({{ column_name }} as varchar))
{% endmacro %}
