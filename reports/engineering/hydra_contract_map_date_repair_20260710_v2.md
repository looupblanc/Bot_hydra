# Immutable Engineering Task — HYDRA Date-Aware Contract Map Repair v2

Task ID: `eng_hydra_contract_map_date_repair_20260710_v2`

Status at authoring: preregistered before implementation.

This task supersedes, but does not modify, v1. The pre-implementation audit
proved that v1's requirement to preserve every old economic field was
internally inconsistent: `23/141` frozen segments also carry a foreign
`tick_size`. The date-aware definition and the versioned root specification
agree on the correct tick for `141/141` segments.

## Scientific objective

Repair the confirmed date-insensitive `instrument_id -> raw_symbol` and
definition-field flattening defects without mutating the historical map or
rerunning candidate evidence. Produce a new explicit roll map solely from the
already cached Databento definition history.

Frozen sources:

- integrity-pilot result hash:
  `8f5ad99e452acb522516331dc9d861ac77cbf51cc184d0bd3ab4ca3e5e4894b4`
- defective-map SHA-256:
  `9126ebb8b957b14bb6cb93a90b729e36bff23087f1276865e6a4917630079a2b`
- definition DBN SHA-256:
  `a5374a723fdf442f8c5e31f98af26ff813070a61008d24e46f848df69c819dcc`
- implementation baseline commit:
  `e48edc1c018d525bd0efea5d81219640724cb292`

## Required behavior

1. Verify every frozen hash before metadata parsing.
2. Resolve each segment with the final definition event available by the end
   of its start UTC calendar day; use the first cached event only when the
   segment predates the cache.
3. Require `instrument_class=F`, `security_type=FUT`, asset equal to root, and
   a root/month/year outright futures symbol.
4. Preserve exactly: root, instrument ID, continuous symbol, active start/end,
   roll date, unsafe-window policy, tick value, point value, multiplier, and
   micro/full classification.
5. Replace from the date-aware definition: raw contract symbol, month/year,
   expiry/last-trade date, activation, and tick size. Require the resolved tick
   to equal the versioned root specification.
6. Write a new immutable map. Never modify/delete the defective map or raw DBN.
7. Correct the reusable builder so it cannot accept a global flattened raw
   mapping when date-aware definition history is available, and fail closed on
   invalid or ambiguous mappings.
8. Publish all 40 symbol corrections and all 23 tick corrections with source
   timestamps and checksums.
9. Read definition metadata only: zero market bars, candidate reruns, network
   requests, paid spend, Q4 access, or live/broker actions.
10. Keep the mission fail-closed until a separate fresh-retest task freezes the
    repaired map and creates new atom IDs.

## Allowed paths

- `hydra/data/contract_mapping.py`
- `scripts/build_databento_contract_map.py`
- `hydra/validation/contract_map_date_repair.py`
- `hydra/mission/experiment_runner.py`
- `hydra/mission/controller.py`
- `tests/test_contract_map_date_repair.py`
- `tests/test_roll_mapping_and_clustering.py`
- `tests/test_mission_scheduler.py`
- new immutable runtime map/report artifacts

## Protected paths

- the defective map SHA `9126ebb8...0079a2b`
- the raw DBN SHA `a5374a72...19dcc`
- integrity-pilot result hash `8f5ad99e...e4894b4`
- governance, role, access, spend, mission, and registry ledgers/databases
- all Q4 2024 and future-lockbox data

## Acceptance tests

- Real cached smoke resolves 141/141 valid outright futures.
- Exactly 40 symbols and 23 tick sizes change; every resolved tick equals the
  versioned root spec.
- Preserved identity/interval/economic fields match exactly as enumerated.
- Old map, raw DBN, pilot result, ledgers, spend, and Q4 counters are unchanged.
- Reused-ID, ambiguity, wrong-root, non-future, and wrong-tick controls fail
  closed; deterministic reruns produce identical hashes.
- Full pytest, no-lookahead, compileall, budget, Q4, lock, registry, secret,
  SQLite, and governance checks pass.
- The same mission writer executes one durable repair experiment after restart,
  with no duplicate and no consumed retry.

## Rollback conditions

Any protected mutation; any segment unresolved/ambiguous; any invariant field
drift outside the explicit replacement set; any market-bar read, candidate
rerun, network/paid/Q4/live path; or two failed implementation attempts.

Maximum automatic implementation retries: 2.

Expected decision information value: `1.0`.
