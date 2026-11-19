{# Render config lists passed via --vars as inline relations, so the model follows the config. #}

{% macro sql_str(value) -%}
    {%- if value is none -%} null {%- else -%} '{{ value | string | replace("'", "''") }}' {%- endif -%}
{%- endmacro %}

{% macro sql_num(value) -%}
    {%- if value is none -%} null {%- else -%} {{ value }} {%- endif -%}
{%- endmacro %}

{% macro config_countries() -%}
    select * from (values
    {%- for c in var('countries') %}
        ({{ sql_str(c) }}, {{ loop.index }}){{ "," if not loop.last }}
    {%- endfor %}
    ) as t(country_iso3, config_order)
{%- endmacro %}

{% macro config_indicators() -%}
    select * from (values
    {%- for i in var('indicators') %}
        ({{ sql_str(i.code) }}, {{ sql_str(i.name) }}, {{ sql_str(i.short_name) }},
         {{ sql_str(i.unit) }}, {{ sql_num(i.min_value) }}::double, {{ sql_num(i.max_value) }}::double){{ "," if not loop.last }}
    {%- endfor %}
    ) as t(indicator_code, indicator_name, indicator_short_name, unit, min_value, max_value)
{%- endmacro %}
