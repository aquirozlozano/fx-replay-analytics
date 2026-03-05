select
  cast(date as date) as spend_date,
  cast(campaign as string) as campaign,
  cast(cost as numeric) as cost,
  cast(clicks as int64) as clicks,
  cast(impressions as int64) as impressions
from {{ source('raw', 'raw_marketing_spend') }}
qualify row_number() over (
  partition by date, campaign
  order by date desc
) = 1
