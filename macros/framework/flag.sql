{% macro flag(condition_sql) -%}
  case when {{ condition_sql }} then 1 else 0 end
{%- endmacro %}
