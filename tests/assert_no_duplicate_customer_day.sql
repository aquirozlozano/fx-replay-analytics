select
  customer_id,
  date,
  count(*) as records
from {{ ref('int_subscriptions_eod_status') }}
group by 1, 2
having count(*) > 1
