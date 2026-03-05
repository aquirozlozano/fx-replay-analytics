{% macro date_in_reporting_tz(ts_col) -%}
  date({{ ts_col }}, '{{ var("reporting_timezone", "UTC") }}')
{%- endmacro %}
