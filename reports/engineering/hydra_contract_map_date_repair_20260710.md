# Immutable Engineering Task — HYDRA Date-Aware Contract Map Repair

Task ID: `eng_hydra_contract_map_date_repair_20260710_v1`

Status at authoring: preregistered before implementation.

## Scientific objective

Repair the confirmed date-insensitive `instrument_id -> raw_symbol` flattening
defect without mutating the historical map or rerunning candidate evidence.
Produce a new explicit, roll-aware map from the already cached Databento
definition history and prove that every continuous interval resolves to a valid
outright futures contract for its declared root.

Frozen diagnostic source:

- integrity-pilot result hash:
  `8f5ad99e452acb522516331dc9d861ac77cbf51cc184d0bd3ab4ca3e5e4894b4`
- frozen defective-map SHA-256:
  `9126ebb8b957b14bb6cb93a90b729e36bff23087f1276865e6a4917630079a2b`
- cached definition DBN SHA-256:
  `a5374a723fdf442f8c5e31f98af26ff813070a61008d24e46f848df69c819dcc`
- implementation baseline commit:
  `eaa3d7f8863d50f72c9568ac622d8fb82850406f`

## Required behavior

1. Verify all frozen hashes before reading definition metadata.
2. Resolve each segment using the final definition event available by the end
   of its start UTC calendar day, using the first cached event only for an
   initial segment that begins before the definition cache.
3. Require `instrument_class=F`, `security_type=FUT`, matching asset/root, and
   a root/month-code/year outright futures symbol for every segment.
4. Preserve exactly the 141 roots, instrument IDs, active intervals, roll
   boundaries, unsafe windows, and economic contract specifications.
5. Write a new immutable map; never overwrite, rename, or delete the frozen
   defective map.
6. Correct the reusable builder so a future global, date-insensitive mapping
   cannot silently recreate this defect.
7. Publish a deterministic audit identifying every changed interval, source
   definition timestamp, checksums, and the new map hash.
8. Read cached definition metadata only. Read zero market-observation rows,
   rerun zero candidates, make zero network requests, spend zero dollars, and
   access no Q4 or future lockbox data.
9. Leave the mission fail-closed until a separate fresh-retest design freezes
   the new map and assigns new atom IDs.

## Allowed paths

- `hydra/data/contract_mapping.py`
- `scripts/build_databento_contract_map.py`
- `hydra/validation/contract_map_date_repair.py`
- `hydra/mission/experiment_runner.py`
- `hydra/mission/controller.py`
- `tests/test_contract_map_date_repair.py`
- `tests/test_roll_mapping_and_clustering.py`
- `tests/test_mission_scheduler.py`
- new immutable map/report artifacts produced by the governed runtime handler

## Protected paths

- `data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_500ac1ef6c622950.json`
- `data/cache/contract_maps/definitions_GLBX-MDP3_2023-01-01_2024-10-01_1faa319bb6354a47.dbn.zst`
- `reports/mission_experiments/post_calibration_retest_pilot_v1/**`
- `reports/data_access/data_access_ledger.jsonl`
- `reports/data_budget/databento_spend_ledger.jsonl`
- `mission/state/hydra_mission.db`
- `registry/hydra_registry.db`
- `config/governance/**`
- all Q4 2024 and future-lockbox data

## Acceptance tests

- Pure date-aware mapping controls reproduce one reused-ID failure and repair it.
- Real cached smoke resolves 141/141 segments to valid outright futures and
  changes exactly the 40 invalid frozen symbols.
- Old-map SHA and raw-definition SHA remain byte-for-byte unchanged.
- Root, instrument ID, interval, roll boundary, and economic-spec invariants
  match exactly between old and new maps.
- A deliberately ambiguous or invalid definition fails closed.
- Deterministic reruns produce the same new map hash and report result hash.
- Full pytest, no-lookahead, compileall, budget, Q4, process-lock, registry,
  secret-scan, and SQLite integrity checks pass.
- The existing queued repair is executed once by the same mission writer after
  restart; no duplicate or consumed retry is permitted.

## Rollback conditions

- Any protected file changes.
- Any segment lacks exactly one valid date-aware definition.
- Any active interval or instrument identity changes.
- Any network, paid-data, Q4, market-observation, or live path is reached.
- Any candidate evidence is rerun before a separately frozen fresh retest.
- Two implementation attempts fail acceptance.

Maximum automatic implementation retries: 2.

## Expected decision information value

`1.0` (highest current priority). Contract identity is a prerequisite for all
contractual transfer and roll-aware evidence. Until this defect is repaired,
no atom result can legitimately change a decision and no strategy can approach
promotion.
