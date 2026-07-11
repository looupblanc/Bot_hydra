# Immutable Engineering Task — GC/MGC Volume-Front Development Data Repair

## Scientific objective

- Task ID: `hydra_gc_volume_front_data_repair_20260711`
- Objective: repair the metal-ecology development lane after proving that calendar-nearest `GC.c.0` contains only 17,411 one-minute records while `MGC.c.0` contains 298,979, making all prior GC structural candidates event-starved.
- Official Databento distinction: calendar `c` ranks by nearest expiration; volume `v` ranks by prior-day volume. The proposed source is `GC.v.0` and `MGC.v.0`.
- Decision resolved: whether prior zero metal survival was caused by a genuine economic failure or by an illiquid calendar-front representation.
- Expected decision information value: `0.99`.

## Frozen request contract

- Dataset: `GLBX.MDP3`.
- Schema: `ohlcv-1m`.
- Symbols: `GC.v.0`, `MGC.v.0`.
- Input symbology: `continuous`.
- Output symbology: `instrument_id`.
- Period: `[2023-01-01, 2024-10-01)`; Q4 excluded.
- Official metadata estimate observed before task implementation:
  - records: `1,208,477`;
  - estimated cost: `USD 4.411889091134`;
  - billable bytes: `67,674,712`.
- Current verified remaining budget: `USD 77.03675423189901`.
- Projected post-request budget: `USD 72.62486514076501`.
- Minimum reserve: `USD 30.00`.
- No trades, TBBO, MBP-1, or MBO data.

## Required behavior

1. Build a dedicated volume-front request and cache path; never overwrite or masquerade as the existing calendar-front cache.
2. Estimate official cost immediately before purchase and reject any estimate above `USD 5.00` or projected reserve below `USD 30.00`.
3. Check for the exact volume-front raw/parquet cache and request hash before network use.
4. Append an immutable planned request to the persistent budget ledger before download, with purpose and expected information value.
5. Download once, redact the API key, retain raw DBN/ZST, normalize logical symbols to GC/MGC, and write Parquet atomically.
6. Validate row counts, date coverage, timestamps, duplicates, OHLC validity, symbols, request metadata, and SHA-256 checksums.
7. Require materially improved GC coverage; otherwise mark the repair insufficient and do not research it.
8. Construct an explicit volume-front roll map from the DBN continuous mappings and the existing verified definition crosswalk. Every mapped interval must resolve to a raw contract, multiplier, tick, expiry, and root.
9. Use a new map type and hash. Do not replace or mutate the existing calendar-front map.
10. Append actual spend and validation result to the persistent spend ledger exactly once.
11. Q4 access remains zero; the acquired period ends before Q4.
12. No strategy status changes in the acquisition task.

## Allowed paths

- `hydra/data/databento_volume_front.py`
- `scripts/download_volume_front_development.py`
- `tests/test_databento_volume_front.py`
- append-only `reports/data_budget/databento_spend_ledger.jsonl` through the existing budget interface;
- ignored `data/cache/databento/**` and `data/cache/contract_maps/**` outputs;
- generated acquisition report under ignored/mission report state.

## Protected paths

- `config/governance/**`
- Q4/lockbox files and role ledgers;
- existing calendar-front data and roll maps;
- registry and mission DB schemas;
- API keys, credentials, broker, order, or live-trading paths.

## Acceptance tests

- deterministic request ID and distinct volume-front paths;
- cache hit prevents a second network request;
- budget reserve and max-cost guards;
- planned-before-download and actual-once ledger behavior;
- secret redaction;
- normalized symbol mapping;
- checksum and coverage verification;
- volume roll-map intervals, contract crosswalk, multipliers, ticks, expiries, and hash;
- Q4 exclusion;
- corrupted/partial cache rejection;
- full pytest, compileall, governance, integrity, and secret scan.

## Rollback conditions

- estimate exceeds `USD 5.00` or reserve would fall below `USD 30.00`;
- Q4/end-date drift;
- cache collision with calendar-front data;
- unresolved instrument IDs/definitions;
- GC coverage remains insufficient;
- checksum, OHLC, duplicate, session, or roll-map failure;
- API key appears in any output;
- protected governance change.

## Interpretation boundary

This task repairs a development representation; it does not validate a metal strategy. Any subsequent metal candidate needs a new preregistration, new ID, 2023-only selection, unchanged 2024 Q1–Q3 confirmation, realistic costs, and candidate-level evidence.
