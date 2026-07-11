# Immutable Engineering Task — GC Volume-Front Direct-State Primary

## Scientific objective

- Task ID: `hydra_gc_volume_front_direct_primary_20260711`.
- Objective: test interpretable direct state-transition policies on the newly repaired GC/MGC volume-front representation after the cross-ecology barrier-hazard family produced zero early survivor.
- Decision resolved: whether repaired metals support a direct return/distributional state mechanism even though the NQ-style symmetric barrier hazard does not transfer.
- Expected decision information value: `0.999`.
- No prior GC calendar-front result, candidate status or barrier result is inherited.

## Frozen population

- Market: `GC`; execution transfer: `MGC`.
- Exactly 90 new hypotheses: 9 past-only features × continuation/reversal × 5 session/horizon profiles.
- Representation and IDs: `gc_volume_front_direct_primary_v1`; all structural fingerprints must be disjoint from historical QD candidates.
- Features: old-region reentry, directional pressure without progress, shared-loss risk state, failed expansion, extreme dwell, short/long realized-volatility ratio, past 60-minute return, past volatility and past participation.
- Profiles: open 15m, open 30m, middle 30m, late 60m and all-session 60m, using the frozen QD profile definitions.
- No threshold/stop/target grid.

## Selection and confirmation

- Selection period only: `[2023-01-01, 2024-01-01)`.
- Selection thresholds use only the previous 20 completed sessions and require at least 500 historical feature observations.
- Candidate selection gate:
  - at least 25 GC events;
  - positive GC net and positive 1.5× cost-stress net;
  - best positive-event share at most 35%;
  - at least one supportive 2023 half;
  - no half-year loss larger than 1.5 times the other half's positive net;
  - positive MGC net and positive MGC 1.5× cost-stress net.
- Freeze at most one primary before loading any 2024 row.
- Rank eligible candidates by minimum GC/MGC net-to-drawdown, half-year balance, cost resilience and deterministic fingerprint tie-break.
- Confirmation folds unchanged: 2024 Q1, Q2 and Q3; Q4 excluded.
- Exact one-primary block sign-flip test, prospective alpha `0.03`.
- Separately calibrated zero-risk shadow support threshold: `p <= 0.20`; this cannot confer promotion or PAPER_SHADOW_READY.

## Frozen data

- Volume-front Parquet SHA-256: `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`.
- Volume-front map file SHA-256: `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`.
- Roll-map hash: `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`.
- Period ends `2024-10-01` exclusive; Q4 forbidden.

## Execution and validation

1. Explicit contracts and unsafe roll windows are mandatory.
2. Feature at completed bar close, then one complete-bar execution delay.
3. Correct GC/MGC multipliers and conservative round-turn costs.
4. Candidate-level 2024 temporal replay, MGC transfer, parameter-neighbor diagnostics, best-event/fold concentration, delayed execution, MLL and Topstep path.
5. Diagnostic archive candidates inherit no evidence.
6. A candidate may reach at most `SHADOW_RESEARCH_CANDIDATE` in this development experiment.
7. No network, paid data, Q4, broker, live or order capability.

## Allowed paths

- `hydra/research/gc_volume_front_direct_primary.py`;
- `hydra/mission/controller.py`;
- `tests/test_gc_volume_front_direct_primary.py`;
- scheduler tests;
- this task and ignored experiment artifacts.

## Protected paths

- `config/governance/**`;
- Q4/lockbox data and ledgers;
- historical candidates/manifests;
- market-data and roll-map caches;
- registry/mission schemas;
- live, broker, credentials and order paths.

## Acceptance tests

- exact 90 fresh, unique hypotheses;
- frozen hashes and volume map enforced;
- selection loader cannot read 2024;
- maximum one primary frozen before confirmation;
- GC/MGC selection transfer;
- causal threshold availability and delayed fills;
- candidate-level null and calibrated shadow distinction;
- parameter diagnostics and account replay;
- diagnostics inherit no status;
- zero Q4/network/order capability;
- controller queue/recovery/activation;
- full tests, compile, governance, integrity and secret scan.

## Kill and pivot conditions

- no eligible 2023 primary;
- 2024 net or cost stress becomes non-positive;
- catastrophic temporal transfer;
- MGC transfer fails;
- candidate-level null does not support even zero-risk shadow research;
- hard integrity or execution failure.

## Interpretation boundary

This is development confirmation on a corrected representation. It cannot validate a mechanism, open Q4, grant PAPER_SHADOW_READY, or authorize orders.
