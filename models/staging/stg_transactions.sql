select
  cast(transaction_id as string) as transaction_id,
  cast(customer_id as string) as customer_id,
  cast(transaction_date as timestamp) as transaction_timestamp,
  {{ date_in_reporting_tz('transaction_date') }} as transaction_date,
  cast(amount as numeric) as amount,
  lower(cast(type as string)) as type
from {{ source('raw', 'raw_transactions') }}
where lower(cast(type as string)) = 'purchase'
qualify row_number() over (
  partition by transaction_id
  order by transaction_date desc
) = 1
