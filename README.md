# fx-replay-analytics (dbt + BigQuery SQL)

Production-style dbt framework for SaaS subscription analytics at FX Replay.

## What This Repository Is

This is not only a challenge solution; it is an implementation framework for adding new SaaS analytics domains with consistent standards.

Implemented deliverables:

- `subscriptions_eod_status` (incremental state model)
- `subscriptions_eod_enriched` (lifecycle intelligence)
- `fact_user_attribution` (first-touch, trial-touch, purchase-touch)
- `mart_subscription_metrics_daily` (daily KPI mart)

## Framework Structure

```text
models/
  staging/
    shared/
    billing/
    marketing/
    product/
    sales/
    account_management/
    finance/
    support/
  intermediate/
    shared/
    subscriptions/
    attribution/
    sales/
    marketing/
    account_management/
    finance/
    support/
  marts/
    subscriptions/
    growth/
    sales/
    marketing/
    account_management/
    finance/
    support/
    executive/
macros/
  framework/
```

## Setup

1. Configure `~/.dbt/profiles.yml` with profile `fx_replay_analytics`.
2. Install dependencies:
   ```bash
   dbt deps
   ```
3. Build all:
   ```bash
   dbt build
   ```
4. Run snapshot (optional):
   ```bash
   dbt snapshot
   ```

## Framework Execution (Selectors)

Selectors are defined in `selectors.yml`.

- Layer runs:
  - `dbt build --selector layer_staging`
  - `dbt build --selector layer_intermediate`
  - `dbt build --selector layer_marts`
- Domain runs:
  - `dbt build --selector domain_subscriptions`
  - `dbt build --selector domain_growth`
  - `dbt build --selector domain_billing`
  - `dbt build --selector domain_marketing`
  - `dbt build --selector domain_product`
  - `dbt build --selector domain_sales`
  - `dbt build --selector domain_account_management`
  - `dbt build --selector domain_finance`
  - `dbt build --selector domain_support`
- Area runs:
  - `dbt build --selector area_sales`
  - `dbt build --selector area_marketing`
  - `dbt build --selector area_account_management`
  - `dbt build --selector area_finance`
  - `dbt build --selector area_support`
  - `dbt build --selector area_product`
  - `dbt build --selector area_executive`
  - `dbt build --selector area_shared`

## Assumptions

- Reporting timezone: `UTC` (`var('reporting_timezone')`).
- `raw_ga4_events.user_id` maps to `customer_id`.
- Subscription validity is treated as inclusive between `current_term_start` and `current_term_end`.
- Purchase attribution lookback window: `var('attribution_lookback_days')`.

## Incremental Framework Logic

`subscriptions_eod_status` uses:

- `merge` incremental strategy
- partition by `date`
- clustering by `customer_id, subscription_id`
- rolling lookback window for late-arriving updates (`var('eod_incremental_lookback_days')`)

This balances speed and correctness for mutable SCD-like subscription feeds.

## Reusable Macros

Framework macros in `macros/framework`:

- `subscription_status_priority`
- `is_paid_campaign`
- `flag`

Shared macros:

- `date_in_reporting_tz`
- `is_paid_traffic`

## Data Quality Contract

- `not_null`, `unique`, `accepted_values` tests in schema YAML
- singular invariant tests in `tests/`
- source freshness configuration in `models/staging/_sources.yml`

## Tradeoffs

- Customer-day EOD chooses one deterministic subscription record; this simplifies exec reporting but does not expose parallel-subscription detail by default.
- Attribution currently centers on first purchase path; multi-purchase pathing can be added as a follow-up domain extension.

## Multi-Area Governance

- `domain:*` tags are used for data-logic grouping (billing, subscriptions, attribution, etc.).
- `area:*` tags are used for business ownership and execution slicing (sales, marketing, account management, finance, support, product, executive).
- New models should include both perspectives through folder placement and inherited tags.

## Implementation Guide

Detailed step-by-step framework usage is documented in `FRAMEWORK.md`.

Business ownership guidance is documented in `AREA_OWNERSHIP.md`.
