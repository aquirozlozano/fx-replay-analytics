{{
  config(
    materialized='incremental',
    alias='subscriptions_eod_status',
    unique_key=['customer_id', 'date'],
    incremental_strategy='merge',
    partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['customer_id', 'subscription_id'],
    on_schema_change='sync_all_columns'
  )
}}

-- Assumption: a subscription snapshot is valid from current_term_start_date to current_term_end_date (inclusive).
-- To handle late-arriving updates safely, the model recomputes a rolling lookback window.
with params as (
  select
    {% if is_incremental() %}
      date_sub(
        coalesce(
          (select max(date) from {{ this }}),
          (select min(current_term_start_date) from {{ ref('stg_chargebee_subscriptions') }})
        ),
        interval {{ var('eod_incremental_lookback_days', 60) }} day
      ) as start_date,
    {% else %}
      (select min(current_term_start_date) from {{ ref('stg_chargebee_subscriptions') }}) as start_date,
    {% endif %}
    current_date('{{ var('reporting_timezone', 'UTC') }}') as end_date
),

calendar as (
  select date_day as date
  from {{ dbt_utils.date_spine(
      datepart='day',
      start_date="(select start_date from params)",
      end_date="date_add((select end_date from params), interval 1 day)"
  ) }}
),

valid_scd as (
  select
    subscription_id,
    customer_id,
    plan_id,
    status,
    mrr,
    current_term_start_date,
    current_term_end_date,
    updated_at,
    greatest(current_term_start_date, (select start_date from params)) as valid_from,
    least(coalesce(current_term_end_date, (select end_date from params)), (select end_date from params)) as valid_to
  from {{ ref('stg_chargebee_subscriptions') }}
  where current_term_start_date <= (select end_date from params)
    and coalesce(current_term_end_date, (select end_date from params)) >= (select start_date from params)
),

expanded as (
  select
    s.customer_id,
    c.date,
    s.subscription_id,
    s.plan_id,
    s.status,
    s.mrr,
    s.current_term_start_date,
    s.current_term_end_date,
    s.updated_at,
    row_number() over (
      partition by s.customer_id, c.date
      order by
        case s.status
          when 'active' then 1
          when 'trialing' then 2
          when 'non_renewing' then 3
          when 'cancelled' then 4
          else 99
        end,
        s.mrr desc,
        s.updated_at desc,
        s.subscription_id desc
    ) as rn
  from valid_scd s
  join calendar c
    on c.date between s.valid_from and s.valid_to
),

resolved as (
  select
    customer_id,
    date,
    subscription_id,
    plan_id,
    status,
    mrr,
    status in ('active', 'non_renewing') as is_active,
    status = 'trialing' as is_trial,
    current_term_start_date,
    current_term_end_date,
    updated_at
  from expanded
  where rn = 1
)

select *
from resolved
