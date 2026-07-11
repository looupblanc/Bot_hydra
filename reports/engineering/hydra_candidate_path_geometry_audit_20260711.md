# Immutable Research and Engineering Task — Candidate Path-Geometry Audit

Task ID: `eng_hydra_candidate_path_geometry_audit_20260711_v1`

This task is frozen before any new candidate-level decision is made. It does
not promote the historical screen candidate and it does not authorize Q4,
paid data, broker access, or strategy assembly.

## Frozen candidate

- candidate: `intraday_range_migration_path_asymmetry_09_01`
- market: `NQ`
- mechanism: `path_asymmetry + range_relocation`
- horizon: `30` one-minute bars
- historical screen only: `2024 Q1-Q3`, five-minute research sample
- repaired contract map:
  `data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json`

The screen result is only a prioritisation signal. Its reported positive PnL,
matched-null status, and Topstep heuristic status are not inherited.

## Scientific objective

Determine whether the two-component path-geometry mechanism has a stable,
one-leg, cost-resilient directional effect after exact candidate-level
recalculation from raw governed development data. The implementation must use
past-only, contract-segment-safe features and must not read any future target
until the immutable candidate manifest is written.

Discovery market is NQ. Fresh transfer markets are ES, MNQ, and YM. Temporal
evaluation is 2023 H2 calibration followed by 2024 Q1, Q2, and Q3. Q4 and all
future lockboxes remain sealed.

## Mandatory gates

- exact repaired-map and development-manifest checks;
- prefix-invariance and target-boundary proofs;
- candidate-level matched nulls and delayed/sign-flip controls;
- all three primary quarters positive after conservative NQ costs;
- at least 30 independent event days per decisive market;
- explicit-contract replication with at least three eligible contracts;
- parameter-neighborhood stability for the frozen 30-bar horizon and its
  preregistered 20/40-bar neighbors;
- no single day, contract, or event contributes more than 25% of net effect;
- multiple-testing correction over the frozen candidate universe;
- conservative Topstep shared-account replay only after economic gates pass.

Any mandatory failure kills the candidate. Insufficient sample or unresolved
contract/execution ambiguity returns `INSUFFICIENT`, never a pass.

## Governance and protected paths

Q4 access, paid data, network, live/broker execution, registry/mission ledgers,
and historical atoms are prohibited. The implementation may add only the
candidate audit module, its tests, immutable preregistration/result artifacts,
and controlled mission runner/controller wiring.

## Acceptance and rollback

Run targeted tests, full pytest, no-lookahead, compileall, SQLite/governance/
budget/Q4/lock/single-writer/registry/secret checks, and a deterministic smoke.
Rollback on any future-data use, gate weakening, source mutation, duplicate
queue identity, or two failed implementation attempts.

Expected decision information value: `0.99`.
