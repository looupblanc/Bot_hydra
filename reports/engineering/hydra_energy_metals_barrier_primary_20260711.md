# Immutable Engineering Task — Energy/Metals Barrier Primary Tournament

## Scientific objective

- Task ID: `hydra_energy_metals_barrier_primary_20260711`.
- Objective: test whether a causal target-before-stop hazard transfers in energy or metals after repairing the GC/MGC volume-front representation.
- Decision resolved: whether the current equity-index concentration reflects a true ecology limitation or the previous invalid metal representation.
- Expected decision information value: `0.998`.
- This is a fresh single-primary experiment. No prior candidate, status, p-value or pass is inherited.

## Frozen population and selection

- Markets: `CL/MCL` energy and repaired volume-front `GC/MGC` metals only.
- Population: exactly 48 new hypotheses = 2 mini markets × 4 past-only path/hazard features × 6 preregistered session/context/barrier recipes.
- Candidate IDs and structural fingerprints are new and include representation version `energy_metals_barrier_primary_v1`.
- Round 1: `[2023-01-01, 2023-07-01)`.
- Round 2: `[2023-07-01, 2024-01-01)`.
- Maximum one promotion primary, selected before any 2024 row is exposed.
- Diagnostics and archive members other than the frozen primary inherit no status.
- Confirmation folds, unchanged: 2024 Q1, Q2 and Q3; Q4 excluded.

## Frozen data contracts

### Energy

- Parquet SHA-256: `07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374`.
- Explicit calendar-front map file SHA-256: `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`.
- Use only `CL/MCL` rows.

### Metals

- Volume-front Parquet SHA-256: `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`.
- Volume-front roll-map file SHA-256: `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`.
- Roll-map content hash: `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`.
- Use only `GC/MGC` rows.

Both periods end at `2024-10-01` exclusive. Q4 access is forbidden.

## Statistical and execution contract

1. All features and thresholds use past data only; higher-timeframe context uses completed bars only.
2. Entry occurs after the completed decision bar.
3. Symmetric target/stop barriers use past median range scaled by horizon.
4. Same-bar target/stop ambiguity is always resolved stop-first.
5. Stop gaps use the worse of open and stop; targets never receive price improvement.
6. Complete mini and micro costs, correct point values and explicit contracts are mandatory.
7. Round 1 requires at least 8 events, positive net, positive 1.5× cost stress and at least 5 resolved barriers.
8. Round 2 requires at least 10 events, positive net and 1.5× cost stress, concentration at most 40%, at least 8 resolved barriers and target-first probability above 50%.
9. Frozen-primary eligibility also requires positive mini and micro Round-2 economics and non-negative micro target-first probability relative to 50%.
10. Candidate promotion null: exact one-sided target-before-stop binomial, prospective alpha `0.03`, one primary only.
11. Zero-risk shadow research support may use the separately calibrated threshold `p <= 0.20`; it never confers promotion or PAPER_SHADOW_READY.
12. Run delayed-entry, barrier-neighbor, temporal fold, concentration, MLL and Topstep path diagnostics on the exact primary.

## Allowed paths

- `hydra/research/energy_metals_barrier_primary.py`;
- `hydra/mission/controller.py`;
- `tests/test_energy_metals_barrier_primary.py`;
- relevant scheduler tests;
- this task and generated ignored experiment artifacts.

## Protected paths

- `config/governance/**`;
- Q4/lockbox data and role ledgers;
- existing candidate artifacts and manifests;
- raw/normalized market data and roll maps;
- registry/mission schemas;
- live, broker, credentials and order paths.

## Acceptance tests

- exact 48 fresh hypotheses and unique fingerprints;
- only CL/MCL and GC/MGC;
- both frozen data/map hashes enforced;
- early selection cannot access 2024;
- maximum one immutable primary;
- diagnostic candidates cannot inherit status;
- explicit mini/micro transfer and costs;
- stop-first ambiguity and adverse stop gaps;
- exact primary null and calibrated shadow separation;
- no Q4, network, paid data, broker or order capability;
- controller queue/restart/reconciliation and generic shadow activation;
- full tests, compile, integrity, governance and secret scan.

## Rollback and kill conditions

- any frozen data/map/task hash mismatch;
- any 2024 exposure before the primary manifest is written;
- lookahead, partial higher-timeframe bar, invalid contract or execution ambiguity;
- no eligible 2023 primary;
- exact primary loses net economics, cost resilience or contract transfer in 2024;
- hard validation failure or Q4 access.

## Interpretation boundary

A positive result can create at most a `SHADOW_RESEARCH_CANDIDATE`. It cannot create PAPER_SHADOW_READY, validate a mechanism, authorize Q4, or enable real orders.
