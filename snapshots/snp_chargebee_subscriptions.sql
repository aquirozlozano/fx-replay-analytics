{% snapshot snp_chargebee_subscriptions %}

{{
  config(
    target_schema='snapshots',
    unique_key='subscription_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}

select
  subscription_id,
  customer_id,
  plan_id,
  status,
  current_term_start,
  current_term_end,
  mrr,
  updated_at
from {{ source('raw', 'raw_chargebee_subscriptions') }}

{% endsnapshot %}
