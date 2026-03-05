{% macro is_paid_traffic(source_col, medium_col, campaign_col) -%}
  (
    regexp_contains(lower(coalesce({{ medium_col }}, '')), r'(cpc|ppc|paid|display|affiliate)')
    or regexp_contains(lower(coalesce({{ source_col }}, '')), r'(google_ads|facebook|instagram|linkedin|tiktok|bing)')
    or regexp_contains(lower(coalesce({{ campaign_col }}, '')), r'(brand|retarget|acq|prospecting)')
  )
{%- endmacro %}
