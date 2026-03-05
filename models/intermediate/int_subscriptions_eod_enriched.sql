{{ config(materialized='table', alias='subscriptions_eod_enriched') }}

with base as (
  select *
  from {{ ref('int_subscriptions_eod_status') }}
),

sequenced as (
  select
    customer_id,
    date,
    subscription_id,
    plan_id,
    status,
    mrr,
    is_active,
    is_trial,
    current_term_start_date,
    current_term_end_date,
    lag(status) over (partition by customer_id order by date) as prev_status,
    lag(mrr) over (partition by customer_id order by date) as prev_mrr,
    lag(current_term_start_date) over (partition by customer_id order by date) as prev_term_start_date,
    lag(date) over (partition by customer_id order by date) as prev_date,
    row_number() over (partition by customer_id order by date) as customer_day_number
  from base
),

status_islands as (
  select
    *,
    sum(case when status != coalesce(prev_status, status) then 1 else 0 end)
      over (partition by customer_id order by date rows between unbounded preceding and current row) as status_segment
  from sequenced
),

enriched as (
  select
    customer_id,
    date,
    subscription_id,
    plan_id,
    status,
    mrr,
    is_active,
    is_trial,
    current_term_start_date,
    current_term_end_date,
    dense_rank() over (partition by customer_id, status_segment order by current_term_start_date) as billing_cycle_number,
    case
      when coalesce(prev_status, 'none') in ('active', 'non_renewing', 'trialing') and status = 'cancelled' then 1
      else 0
    end as churn_flag,
    case
      when prev_status = 'cancelled'
       and status in ('trialing', 'active', 'non_renewing')
       and date_diff(date, prev_date, day) <= 60
      then 1
      else 0
    end as reactivation_flag,
    case
      when prev_mrr is not null and mrr > prev_mrr and is_active then 1
      else 0
    end as upgrade_flag,
    case
      when prev_mrr is not null and mrr < prev_mrr and mrr > 0 then 1
      else 0
    end as downgrade_flag,
    case
      when customer_day_number = 1 then 'new_customer'
      when prev_status = 'cancelled' and status in ('trialing', 'active', 'non_renewing') then 'returning_customer'
      else 'existing_customer'
    end as new_vs_returning_customer_flag,
    prev_status,
    prev_mrr
  from status_islands
)

select *
from enriched
