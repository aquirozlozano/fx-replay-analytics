select *
from {{ ref('int_subscriptions_eod_status') }}
where mrr < 0
