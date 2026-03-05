# Data Lead Write-up

## 1) Monitoring strategy

- Failed loads: monitor dbt job status, model runtime deltas, and test failure trends; page on repeated failures in critical marts.
- Spend not updating: add source freshness checks for `raw_marketing_spend`; trigger alert if freshness breaches 36h warning / 72h error.
- Sudden drop in subscriptions: set anomaly checks on daily active subscriptions and net MRR change with day-of-week baselines.

## 2) dbt project growth plan

- Keep strict layering: `stg_` (source contracts), `int_` (state and business logic), `mart_` (consumption models).
- Split domains as team grows: `subscriptions`, `marketing`, `finance`, `product` folders inside each layer.
- Enforce code owners and CI gates: style checks, tests, freshness, and slim-CI state comparison.

## 3) Trial abuse mitigation

- Build heuristics using repeated device fingerprints, shared payment instruments, IP clustering, and rapid trial churn loops.
- Score abuse risk and feed product controls: stricter limits, manual review, and delayed premium features for high-risk cohorts.
- Track false positive rate to avoid harming legitimate users.

## 4) Speed vs correctness

- Default to correctness on stateful subscription models and financial metrics.
- Use incremental + lookback windows for speed while preserving late-arrival safety.
- Ship fast in marts with clear assumptions, but keep contract-tested intermediate models as the single source of truth.

## 5) First audit priorities

- Validate raw table contracts and late-arriving behavior in Chargebee SCD records.
- Reconcile transactions vs subscription MRR movements for financial consistency.
- Audit attribution logic for paid/organic bias and lookback leakage.
- Verify metric definitions used by dashboards are identical to dbt mart definitions.
