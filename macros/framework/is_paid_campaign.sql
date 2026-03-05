{% macro is_paid_campaign(campaign_col) -%}
  regexp_contains(lower(coalesce({{ campaign_col }}, '')), r'(paid|brand|retarget|acq|prospecting)')
{%- endmacro %}
