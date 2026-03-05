{{ config(materialized='table', alias='fact_user_attribution') }}

with ga as (
  select
    user_id as customer_id,
    event_timestamp,
    event_date,
    event_name,
    utm_source,
    utm_medium,
    utm_campaign,
    {{ is_paid_traffic('utm_source', 'utm_medium', 'utm_campaign') }} as is_paid
  from {{ ref('stg_ga4_events') }}
),

first_touch as (
  select
    customer_id,
    event_timestamp as first_touch_ts,
    event_date as first_touch_date,
    utm_source as first_touch_source,
    utm_medium as first_touch_medium,
    utm_campaign as first_touch_campaign,
    is_paid as first_touch_is_paid
  from ga
  qualify row_number() over (
    partition by customer_id
    order by event_timestamp asc, is_paid desc, coalesce(utm_campaign, '') desc
  ) = 1
),

trial_touch as (
  select
    customer_id,
    event_timestamp as trial_start_ts,
    event_date as trial_start_date,
    utm_source as trial_source,
    utm_medium as trial_medium,
    utm_campaign as trial_campaign,
    is_paid as trial_is_paid
  from ga
  where event_name = 'start_trial'
  qualify row_number() over (
    partition by customer_id
    order by event_timestamp asc, is_paid desc
  ) = 1
),

first_purchase as (
  select
    customer_id,
    min(transaction_timestamp) as purchase_ts
  from {{ ref('stg_transactions') }}
  group by 1
),

purchase_candidates as (
  select
    p.customer_id,
    p.purchase_ts,
    g.event_timestamp,
    g.event_date,
    g.utm_source,
    g.utm_medium,
    g.utm_campaign,
    g.is_paid,
    case when g.event_date = date(p.purchase_ts, '{{ var('reporting_timezone', 'UTC') }}') then 1 else 0 end as is_same_day
  from first_purchase p
  join ga g
    on g.customer_id = p.customer_id
   and g.event_timestamp <= p.purchase_ts
   and g.event_timestamp >= timestamp_sub(p.purchase_ts, interval {{ var('attribution_lookback_days', 30) }} day)
),

purchase_touch as (
  select
    customer_id,
    purchase_ts,
    event_timestamp as purchase_touch_ts,
    event_date as purchase_touch_date,
    utm_source as purchase_source,
    utm_medium as purchase_medium,
    utm_campaign as purchase_campaign,
    is_paid as purchase_is_paid
  from purchase_candidates
  -- Paid touches win over organic, and same-day touch wins over older lookback touch.
  qualify row_number() over (
    partition by customer_id
    order by is_same_day desc, is_paid desc, event_timestamp desc, coalesce(utm_campaign, '') desc
  ) = 1
)

select
  ft.customer_id,
  ft.first_touch_ts,
  ft.first_touch_date,
  ft.first_touch_source,
  ft.first_touch_medium,
  ft.first_touch_campaign,
  ft.first_touch_is_paid,
  tr.trial_start_ts,
  tr.trial_start_date,
  tr.trial_source,
  tr.trial_medium,
  tr.trial_campaign,
  tr.trial_is_paid,
  pt.purchase_ts,
  pt.purchase_touch_ts,
  pt.purchase_touch_date,
  pt.purchase_source,
  pt.purchase_medium,
  pt.purchase_campaign,
  pt.purchase_is_paid,
  case when ft.first_touch_is_paid then 'paid' else 'organic' end as first_touch_channel_group,
  case when tr.trial_is_paid then 'paid' else 'organic' end as trial_channel_group,
  case when pt.purchase_is_paid then 'paid' else 'organic' end as purchase_channel_group
from first_touch ft
left join trial_touch tr
  on ft.customer_id = tr.customer_id
left join purchase_touch pt
  on ft.customer_id = pt.customer_id
