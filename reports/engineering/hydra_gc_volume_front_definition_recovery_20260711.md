# Immutable Engineering Task — GC/MGC Volume-Front Definition Recovery

## Scientific objective

- Task ID: `hydra_gc_volume_front_definition_recovery_20260711`.
- Parent task: `hydra_gc_volume_front_data_repair_20260711`.
- Failure observed after the governed bar download: the volume-ranked mapping contains `GC instrument_id=393` and `MGC instrument_id=1974`, neither present in the historical calendar-nearest crosswalk.
- Objective: recover only those missing explicit definitions and finish the already-paid representation repair without downloading the OHLCV request again.
- Expected decision information value: `0.995`; unresolved definitions currently prevent all legitimate GC volume-front research.

## Frozen supplemental request

- Dataset: `GLBX.MDP3`.
- Schema: `definition`.
- Input/output symbology: `instrument_id`.
- Instrument IDs: `393`, `1974` only.
- Period: `[2023-01-01, 2024-10-01)`; Q4 excluded.
- Official estimate observed before implementation:
  - records: `1,100`;
  - estimated cost: `USD 0.000626966357`;
  - billable bytes: `396,000`.
- Maximum supplemental cost: `USD 0.005`.
- Minimum remaining reserve after both the already-paid OHLCV request and this request: `USD 30.00`.

## Required behavior

1. Reuse and checksum the existing `GC.v.0`/`MGC.v.0` raw and Parquet caches; never issue the OHLCV request again.
2. Detect missing crosswalk IDs deterministically from the DBN mapping and frozen base map.
3. Refuse any supplemental definition set other than exactly `393` and `1974` for this recovery task.
4. Estimate official definition cost immediately before purchase and fail above `USD 0.005` or below the protected reserve.
5. Append the definition estimate before download and actual cost exactly once after validation.
6. Download the supplemental definition DBN atomically to a distinct ignored cache path.
7. Require date-aware, unambiguous outright-future definitions with correct root, tick, multiplier, expiry, activation and raw symbol.
8. Combine the supplemental definitions with the existing verified crosswalk in a new immutable volume-front roll map; do not mutate predecessor maps.
9. Recover the already-paid OHLCV ledger entry exactly once, and only after bars, definitions and roll map all validate.
10. A retry after completion must be a cache hit with zero incremental actual spend.
11. Q4 access remains zero and no strategy status changes.

## Allowed paths

- `hydra/data/databento_volume_front.py`;
- `scripts/download_volume_front_development.py`;
- `tests/test_databento_volume_front.py`;
- this immutable task file;
- append-only persistent budget ledger through the existing budget interface;
- ignored Databento, definition, roll-map and report outputs.

## Protected paths

- `config/governance/**`;
- Q4/lockbox files and role ledgers;
- existing calendar-front files and roll maps;
- mission/registry schemas;
- credentials, broker, order and live-trading paths.

## Acceptance tests

- deterministic missing-ID detection;
- rejection of unexpected missing IDs;
- date-aware supplemental contract construction;
- definition cost and reserve guard before network use;
- interrupted-download ledger recovery exactly once;
- no second OHLCV download;
- cache-hit retry costs zero;
- complete volume map round trip and checksum;
- Q4 remains excluded;
- full tests, compilation, governance/integrity checks and secret scan.

## Rollback conditions

- the cached OHLCV checksum or request mapping changes;
- more or different definition IDs are missing;
- official definition estimate exceeds `USD 0.005`;
- any definition is ambiguous, not an outright future, has the wrong root/tick, or lacks expiry;
- the budget cannot be reconciled exactly once;
- Q4 access, a protected-file change, or an API-key disclosure occurs.

## Interpretation boundary

This recovers data provenance only. It does not validate a strategy or authorize holdout, broker, order, PAPER_SHADOW_READY, or funded status.
