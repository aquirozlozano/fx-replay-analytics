# fx-replay-analytics (dbt + BigQuery SQL)

FX Replay's corporate analytics repository built on `dbt`, designed to scale with a layered model (`staging`, `intermediate`, `marts`) and strong data governance standards.

## About FX Replay

FX Replay is a backtesting platform focused on helping traders validate strategies with historical data and realistic market simulation, with a vision to help traders become consistently profitable.

## dbt Objective in This Project

Implement a reliable transformation and modeling layer to standardize metrics, improve data traceability, and enable consistent analytics for product and business decision-making.

## Challenge Coverage

This repository implements:

- End-of-day subscription state as an incremental model
- Lifecycle enrichment (churn/reactivation/upgrade/downgrade)
- Customer-level attribution (first-touch, trial-touch, purchase-touch)
- Daily subscription metrics mart (active subs, MRR movement, CAC, trial->paid, LTV)
- Data Lead write-up in `analyses/data_lead_writeup.md`

## Project Structure

```text
models/
  staging/
  intermediate/
  marts/
```

Additional folders:

- `snapshots/` for SCD capture
- `tests/` for singular data quality checks
- `macros/` for reusable SQL logic

## Setup

1. Configure `~/.dbt/profiles.yml` with profile `fx_replay_analytics`.
2. Install dependencies:
   ```bash
   dbt deps
   ```
3. Run models:
   ```bash
   dbt run
   ```
4. Run tests:
   ```bash
   dbt test
   ```
5. Run snapshot (optional):
   ```bash
   dbt snapshot
   ```

## Raw Inputs

Modeled as dbt sources in `models/staging/_sources.yml`:

- `raw_ga4_events`
- `raw_chargebee_subscriptions` (SCD Type 2 behavior)
- `raw_transactions`
- `raw_marketing_spend`

## Key Models

- `int_subscriptions_eod_status` (alias: `subscriptions_eod_status`)
- `int_subscriptions_eod_enriched` (alias: `subscriptions_eod_enriched`)
- `mart_fact_user_attribution` (alias: `fact_user_attribution`)
- `mart_subscription_metrics_daily`

## Assumptions

- Reporting timezone is `UTC` (configurable with `var('reporting_timezone')`).
- `raw_ga4_events.user_id` maps to `customer_id`.
- Subscription validity is treated as inclusive between `current_term_start` and `current_term_end` dates.
- Purchase attribution lookback window is 30 days (`var('attribution_lookback_days')`).

## Incremental Logic

`subscriptions_eod_status` is incremental with `merge`, partitioned by `date` and clustered by `customer_id, subscription_id`.

Late-arriving updates are handled by recomputing a rolling window (`var('eod_incremental_lookback_days')`, default 60 days). This avoids full refresh while backfilling recent corrected SCD states.

## Defensive Metric Definitions

- Churn: transition from active/trialing/non_renewing to cancelled.
- Reactivation: cancelled to active/trialing/non_renewing within 60 days.
- Upgrade/Downgrade: positive/negative MRR deltas vs prior customer-day state.
- CAC blended: total spend / total new customers (by day).
- CAC paid-only: paid spend / paid-attributed new customers (by day).
- LTV estimate: `ARPU_30d / churn_rate_30d`.

## Tradeoffs

- Deterministic tie-breakers prioritize `active` status and higher MRR to avoid double counting at customer-day grain.
- The EOD grain is customer-day (single chosen subscription per customer-day), which simplifies executive metrics but can hide parallel subscriptions unless a multi-subscription mart is added.
- Current attribution uses first purchase only; extending to multi-purchase paths is straightforward but costlier.

## Testing

Included test coverage:

- `not_null`, `unique`, `accepted_values` in schema YAML
- Singular test to assert no duplicate `(customer_id, date)` in EOD status

## Layer Convention

- `models/staging`: source cleaning and standardization.
- `models/intermediate`: intermediate business transformations.
- `models/marts`: final analytics-ready models.