{% macro format_cnpj(cnpj_basico, cnpj_ordem, cnpj_dv) %}
    lpad(trim(cast({{ cnpj_basico }} as varchar)), 8, '0')
    || lpad(trim(cast({{ cnpj_ordem }} as varchar)), 4, '0')
    || lpad(trim(cast({{ cnpj_dv }} as varchar)), 2, '0')
{% endmacro %}
