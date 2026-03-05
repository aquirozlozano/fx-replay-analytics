with base as (
  select
    cast(event_timestamp as timestamp) as event_timestamp,
    cast(user_id as string) as user_id,
    lower(cast(event_name as string)) as event_name,
    nullif(lower(cast(utm_source as string)), '') as utm_source,
    nullif(lower(cast(utm_medium as string)), '') as utm_medium,
    nullif(lower(cast(utm_campaign as string)), '') as utm_campaign,
    {{ date_in_reporting_tz('event_timestamp') }} as event_date
  from {{ source('raw', 'raw_ga4_events') }}
)

select *
from base
qualify row_number() over (
  partition by user_id, event_timestamp, event_name, coalesce(utm_source, ''), coalesce(utm_medium, ''), coalesce(utm_campaign, '')
  order by event_timestamp desc
) = 1
