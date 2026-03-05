{{ config(materialized='table') }}

with eod as (
  select *
  from {{ ref('int_subscriptions_eod_enriched') }}
),

daily_subscription_metrics as (
  select
    date,
    count(distinct case when is_active then subscription_id end) as daily_active_subscriptions,
    count(distinct case when new_vs_returning_customer_flag = 'new_customer' and status in ('trialing', 'active', 'non_renewing') then subscription_id end) as new_subscriptions,
    count(distinct case when churn_flag = 1 then subscription_id end) as churned_subscriptions,
    sum(coalesce(mrr, 0) - coalesce(prev_mrr, 0)) as net_mrr_change,
    sum(case when status = 'trialing' and coalesce(prev_status, 'none') != 'trialing' then 1 else 0 end) as trial_starts,
    sum(case when prev_status = 'trialing' and status = 'active' then 1 else 0 end) as trial_to_paid_conversions
  from eod
  group by 1
),

daily_spend as (
  select
    spend_date as date,
    sum(cost) as total_spend,
    sum(case
          when regexp_contains(lower(campaign), r'(paid|brand|retarget|acq|prospecting)') then cost
          else 0
        end) as paid_spend
  from {{ ref('stg_marketing_spend') }}
  group by 1
),

daily_new_customers as (
  select
    first_touch_date as date,
    count(distinct customer_id) as new_customers,
    count(distinct case when first_touch_is_paid then customer_id end) as paid_new_customers
  from {{ ref('mart_fact_user_attribution') }}
  group by 1
),

combined as (
  select
    m.date,
    m.daily_active_subscriptions,
    m.new_subscriptions,
    m.churned_subscriptions,
    m.net_mrr_change,
    m.trial_starts,
    m.trial_to_paid_conversions,
    safe_divide(m.trial_to_paid_conversions, nullif(m.trial_starts, 0)) as trial_to_paid_conversion_rate,
    s.total_spend,
    s.paid_spend,
    n.new_customers,
    n.paid_new_customers,
    safe_divide(s.total_spend, nullif(n.new_customers, 0)) as cac_blended,
    safe_divide(s.paid_spend, nullif(n.paid_new_customers, 0)) as cac_paid_only,
    safe_divide(
      sum(m.net_mrr_change) over (order by m.date rows between 29 preceding and current row),
      nullif(avg(m.daily_active_subscriptions) over (order by m.date rows between 29 preceding and current row), 0)
    ) as arpu_30d,
    safe_divide(
      sum(m.churned_subscriptions) over (order by m.date rows between 29 preceding and current row),
      nullif(avg(m.daily_active_subscriptions) over (order by m.date rows between 29 preceding and current row), 0)
    ) as churn_rate_30d
  from daily_subscription_metrics m
  left join daily_spend s
    on m.date = s.date
  left join daily_new_customers n
    on m.date = n.date
)

select
  *,
  safe_divide(arpu_30d, nullif(churn_rate_30d, 0)) as ltv_estimate
from combined

