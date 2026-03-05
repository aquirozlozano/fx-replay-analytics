{% macro subscription_status_priority(status_col) -%}
  case {{ status_col }}
    when 'active' then 1
    when 'trialing' then 2
    when 'non_renewing' then 3
    when 'cancelled' then 4
    else 99
  end
{%- endmacro %}
