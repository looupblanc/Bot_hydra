# HYDRA append-only forward shadow feed v1

## Scientific objective

Collect genuinely post-freeze market observations for immutable shadow
configurations without any broker or order-submission capability. The feed must
turn data uncertainty into forward evidence while leaving Discovery and
Promotion independent.

## Required behavior

- Databento historical market-data reads only.
- Current explicit contracts from dated definitions and symbology.
- A verified CME session/calendar manifest.
- Strict candidate-specific post-freeze boundaries.
- Append-only 1-minute bar storage with one writer.
- Duplicate, divergent, stale, missing, roll and incomplete-bar rejection.
- Closed-bar-only 5m/15m/30m/60m derivation.
- Candidate heartbeat only after a genuine post-freeze completed bar.
- Conservative fail-closed behavior when the market is closed or data stale.
- Budget estimate before every paid request and at least USD 30 reserved.
- Zero broker connections and zero outbound orders.

## Allowed paths

- `hydra/data/current_contract_map.py`
- `hydra/shadow/forward_feed_manifest.py`
- `hydra/shadow/databento_forward_feed.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `scripts/run_shadow_forward_update.py`
- relevant tests and lightweight reports/manifests
- runtime `shadow/state/forward_data/` and existing data/budget ledgers

## Protected paths

- Q4 data and Q4 one-shot closure artifacts
- governance semantics unrelated to the already-consumed Q4 transaction
- mission/registry schemas
- broker, order and credential surfaces
- immutable strategy configurations

## Acceptance tests

- post-freeze-only request bounds;
- read-only manifest with no credentials;
- exact current contract resolution;
- verified calendar/DST/weekend handling;
- append-only and one-writer enforcement;
- stale/duplicate/missing-bar rejection;
- no broker/order capability;
- budget reserve preservation;
- restart-safe scheduling;
- full pytest, compileall, integrity, governance and secret scan.

## Rollback conditions

- any pre-freeze bar enters forward evidence;
- any broker/order surface appears;
- current contract resolution is ambiguous;
- budget reserve would fall below USD 30;
- Q4 audit changes;
- deterministic reference tests regress.

## Expected decision information value

High: the five existing immutable configurations currently have zero forward
observations. The first post-freeze sessions directly distinguish historical
selection artifacts, execution/staleness failures and genuinely reproducible
signal behavior at a cost near USD 0.023 per bounded session.
