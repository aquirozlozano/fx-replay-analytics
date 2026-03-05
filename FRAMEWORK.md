# dbt Implementation Framework

This repository is structured as a reusable implementation framework for SaaS analytics in BigQuery.

## 1) Add New Raw Inputs

1. Register raw table in `models/staging/_sources.yml`.
2. Define `loaded_at_field` + freshness.
3. Create a staging model in the correct domain folder:
   - `models/staging/billing`
   - `models/staging/marketing`
   - `models/staging/product`

## 2) Add Core Business Logic

1. Build `int_*` models under:
   - `models/intermediate/subscriptions`
   - `models/intermediate/attribution`
2. Keep logic stateful and deterministic (window functions + explicit tie-breakers).
3. For incremental models, always include a late-arrival lookback window.

## 3) Add Consumption Marts

1. Build `mart_*` models under:
   - `models/marts/subscriptions`
   - `models/marts/growth`
2. Keep metric grain explicit and avoid duplicate joins between spend and user-level data.

## 4) Reuse Macros

Use framework macros from `macros/framework`:

- `subscription_status_priority`
- `is_paid_campaign`
- `flag`
- `date_in_reporting_tz`
- `is_paid_traffic`

## 5) Quality Gates

For every new model:

1. Add `not_null` tests for keys and core metric fields.
2. Add `accepted_values` for statuses/flags.
3. Add uniqueness tests where grain is strict.
4. Add at least one singular test for key business invariants.

## 6) Execution Patterns

- By layer:
  - `dbt build --selector layer_staging`
  - `dbt build --selector layer_intermediate`
  - `dbt build --selector layer_marts`
- By domain:
  - `dbt build --selector domain_subscriptions`
  - `dbt build --selector domain_growth`
