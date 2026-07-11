# HYDRA fail-closed forward shadow feed — immutable task

Date: 2026-07-11 UTC  
Trigger: four active shadows waiting for fresh forward data

## Objective

Implement a restart-safe, read-only forward-bar ingestion contract and determine
whether the current installation can lawfully supply fresh GLBX 1-minute bars. The
feed must never create orders, broker connections or synthetic freshness. Expected
decision information value: `0.95`.

## Required behavior

- immutable heartbeat schema with source, dataset, explicit raw contract, completed
  bar timestamp, observed timestamp, checksum and sequence;
- UTC monotonicity, future timestamp rejection, stale rejection, duplicate/conflict
  rejection and missing-bar accounting;
- restart-safe single-writer forward bar store;
- explicit current-contract resolver from dated definitions/symbology only;
- CME session/holiday/DST-aware market-closed status;
- closed-bar-only 5m/15m/30m/60m aggregation with availability timestamp equal to
  or later than the source bar close;
- atomic per-candidate heartbeat publication compatible with the existing shadow
  pipeline;
- no separate shadow runner or service.

The source audit is offline first. It may verify that an API key and Databento Live
client are installed, but it may not connect, subscribe or purchase data in this
task. If current dated definitions/entitlement are not already proven, output
`FORWARD_DATA_SOURCE_REQUIRED` with the smallest exact next action and keep all
shadows fail-closed while Discovery and Promotion continue.

## Allowed paths

- `hydra/shadow/data_feed.py`;
- `hydra/shadow/data_heartbeat.py`;
- `hydra/shadow/contract_resolver.py`;
- `hydra/shadow/forward_bar_store.py`;
- `hydra/shadow/feed_health.py`;
- `scripts/shadow_feed_status.py`;
- controller/runner integration and tests;
- mission shadow state through existing atomic/single-writer paths.

## Protected behavior

No network request, secret output, Databento spend, Q4 access, broker code, execution
adapter, order API or second process. Roll back on false freshness, continuous-symbol
execution, incomplete-bar publication, duplicate acceptance, non-atomic state or any
outbound capability.

