# Immutable Engineering and Research Task — HYDRA Calibration Retest v3

Task ID: `eng_hydra_calibration_retest_v3_20260710`

Status at authoring: preregistered before implementation.

## Scientific objective

Repeat only the six highest-information calibration-affected decisions under a
new immutable preregistration after replacing the confirmed date-flattened
contract map. Determine whether the previous invariant-sentinel insufficiency
was caused by defective contract identity, while retaining calibrated null
rejection and without inheriting any v2 atom decision.

This is not a broad atom rerun. It preserves the four calibration-sensitive
candidates and two calibration-invariant sentinels already selected by frozen
EDIG ranking. Every tested atom receives a new v3 ID and starts untested.

Frozen sources:

- invalid v2 execution result hash:
  `22123708ac5ce71d89a75b73d7f3b5ee03cfd87d48655f5e28e1d828ddb12de9`
- invalid v2 execution file SHA-256:
  `34e4f5d937971f277d8b86d64c69e8078bb8ffbb7e5c9ed841a4409a42c75233`
- contract-map repair result hash:
  `a932819f1eb0b72557b39ea867d3e930fd7d9e9dcad3e4cb64e10a0bbe2abb0d`
- contract-map repair file SHA-256:
  `9137d0850efae03a00c139b9628063a6b7237d4614979491956dca7063e5e1a9`
- repaired roll-map semantic hash:
  `705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208`
- repaired roll-map file SHA-256:
  `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`
- implementation baseline commit:
  `920ca2f035c5d317afeafa90d522bca3459aca87`

## Required behavior

1. Verify every frozen result, file, preregistration, data-manifest, and repaired
   map hash before reading market observations.
2. Generate a fresh v3 design and preregistration from the frozen historical
   ranking, with four calibration-sensitive candidates, two invariant failure
   sentinels, five positive controls, and five negative controls.
3. Give every atom a deterministic v3 ID distinct from all historical and v2
   IDs. Recompute atom, preregistration, and design hashes after freezing the
   repaired-map provenance and final implementation commit.
4. Explicitly record that the v2 execution was integrity-invalid and lends no
   status, effect, pass, failure, or insufficiency decision to v3.
5. Freeze and use only the repaired map type
   `EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2`.
   The execution path must take its map from the v3 development manifest, not
   from the legacy historical report.
6. Retain the calibrated v2 causal evaluation contract: training-fold-only
   thresholds, group-safe targets, explicit contracts, full matched nulls,
   clustered uncertainty, full selection-universe correction, tri-state
   mandatory attacks, and separate diagnostic/informational attacks.
7. Re-run positive and negative controls. Validation may not be weakened to
   produce a pass; a failed calibration or non-rejected invariant sentinel
   invalidates decision change.
8. Publish complete per-atom results and explicit outcome routing. A survivor
   may only reopen a family for new replication; it is not a validated atom or
   strategy. Zero survival may only authorize a research-grammar pivot.
9. Use cached development/falsification data ending exclusively at
   `2024-10-01`. Q4, final lockbox, network, paid data, live trading, broker
   execution, and strategy assembly remain prohibited.
10. Queue and execute exactly one durable v3 design and one durable v3
    execution through the existing controller, database, lock, and service.

## Allowed paths

- `hydra/mission/calibration_retest.py`
- `hydra/mission/calibration_retest_v3.py`
- `hydra/mission/calibration_retest_execution.py`
- `hydra/mission/experiment_runner.py`
- `hydra/mission/controller.py`
- `tests/test_calibration_retest_v3.py`
- `tests/test_calibration_retest_execution.py`
- `tests/test_mission_scheduler.py`
- this immutable task and new immutable runtime v3 reports

## Protected paths

- every v1/v2 design, preregistration, execution result, and worker envelope
- repaired and defective roll maps
- raw definition DBN
- governance, role, spend, access, evidence, decision, mission, and registry
  databases or ledgers except append-only development-access evidence generated
  by the official v3 execution
- all Q4 2024 and future-lockbox data

## Acceptance tests

- Design determinism: identical frozen inputs produce identical v3 atom IDs,
  preregistration hash, and design hash.
- All six IDs are new, version 3, unique, and have no inherited status.
- The manifest freezes the repaired path, semantic hash, SHA-256, and repair
  result hash; tampering with any one fails closed before market reads.
- The execution provenance reports the repaired map type/hash and never the
  defective map hash.
- Prefix invariance remains exact; controls satisfy the frozen calibration
  thresholds or no scientific decision changes.
- Q4/access-role tests show zero Q4 reads; spend remains unchanged; no network,
  live, broker, or strategy path is reachable.
- Restart reconciliation queues no duplicates and consumes no retry before
  execution.
- Full pytest, no-lookahead, compileall, budget, Q4, lock, single-writer,
  registry, secret, SQLite, deterministic smoke, and governance checks pass.

## Rollback conditions

Any protected mutation; old or duplicate atom ID; defective-map use; map/hash
ambiguity; future information; Q4/network/paid/live path; validator weakening;
unfrozen code; duplicate durable experiment; or two failed implementation
attempts.

Maximum automatic implementation retries: 2.

Expected decision information value: `0.98`.

The experiment directly resolves the remaining integrity-conditioned decision:
whether the selected mechanisms were scientifically insufficient, or merely
made undecidable by the defective contract representation.
