with base as (
  select
    cast(subscription_id as string) as subscription_id,
    cast(customer_id as string) as customer_id,
    cast(plan_id as string) as plan_id,
    lower(cast(status as string)) as status,
    cast(current_term_start as timestamp) as current_term_start,
    cast(current_term_end as timestamp) as current_term_end,
    cast(mrr as numeric) as mrr,
    cast(updated_at as timestamp) as updated_at,
    {{ date_in_reporting_tz('current_term_start') }} as current_term_start_date,
    {{ date_in_reporting_tz('current_term_end') }} as current_term_end_date,
    {{ date_in_reporting_tz('updated_at') }} as updated_date
  from {{ source('raw', 'raw_chargebee_subscriptions') }}
)

select *
from base
where subscription_id is not null
qualify row_number() over (
  partition by subscription_id, customer_id, plan_id, status, current_term_start, current_term_end, mrr
  order by updated_at desc
) = 1
