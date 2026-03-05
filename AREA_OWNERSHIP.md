# Area Ownership Guide

Use this guide to place models by business ownership while preserving technical layering.

## Sales

- Pipeline, opportunities, win/loss, outbound performance.
- Typical marts: conversion funnel, rep productivity, revenue by segment.

## Marketing

- Acquisition, attribution, campaign efficiency, funnel top/mid.
- Typical marts: CAC, channel mix, lead quality.

## Account Management

- Retention, expansion, renewals, churn, health scoring.
- Typical marts: renewal pipeline, expansion potential, at-risk accounts.

## Finance

- Revenue recognition support, invoicing, collections, margin views.
- Typical marts: finance reconciliations and executive finance snapshots.

## Support

- Tickets, SLA, resolution quality, support impact on churn/retention.
- Typical marts: ticket backlog, SLA breaches, issue drivers.

## Product

- Usage analytics, activation, trial behavior, feature adoption.
- Typical marts: activation cohorts, feature stickiness.

## Executive

- Cross-area KPI layer for leadership reporting.
- Should consume only curated marts from each area to avoid duplicate logic.

