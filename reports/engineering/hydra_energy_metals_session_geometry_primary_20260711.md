# Immutable Engineering Task — Energy/Metals Session Geometry Primary

## Scientific objective

- Task ID: `hydra_energy_metals_session_geometry_primary_20260711`.
- Objective: pivot from failed bar-level barrier/direct-state transfer to ecology-specific overnight and RTH-opening geometry in CL/MCL and repaired GC/MGC.
- Decision resolved: whether daily inventory transfer, opening acceptance or opening-path efficiency contains a causal, executable effect in energy or metals.
- Expected decision information value: `0.999`.
- No barrier, legacy session-pilot or QD status is inherited.

## Frozen structural population

- Markets: `CL/MCL`, `GC/MGC` volume-front.
- Exactly 432 hypotheses: 2 markets × 6 session features × continuation/reversal × 2 causal quantiles × 3 holding horizons × 3 prior-session contexts.
- Features:
  1. overnight displacement from prior RTH close;
  2. opening 15-minute impulse;
  3. opening 30-minute impulse;
  4. opening location inside the overnight range;
  5. signed opening 15-minute path efficiency;
  6. opening 15-minute volume surprise, direction anchored by the opening impulse.
- Quantiles: 0.65 and 0.80 from the previous 20 completed sessions only.
- Holding horizons: 30, 60 and 120 completed minutes, always flattened before RTH end.
- Contexts: none, prior-RTH trend agrees, prior-RTH trend disagrees.
- One event maximum per market/session/hypothesis.
- New versioned IDs and fingerprints: `energy_metals_session_geometry_primary_v1`.

## Selection protocol

- Round 1 screen: `[2023-01-01, 2023-07-01)` only.
- Round 2 selection: `[2023-07-01, 2024-01-01)` only.
- Round 1 gate: at least 10 events, positive net, positive 1.5× cost-stress net, best positive event share at most 35%.
- Round 2 gate: at least 10 events, positive mini and micro net, positive mini and micro 1.5× cost stress, best event share at most 35%, consistent policy direction and no impossible session exit.
- Quality-diversity archive: at most one member per ecology × feature × portfolio role.
- Freeze at most one promotion primary before loading any 2024 row.
- Ranking: minimum mini/micro net-to-drawdown, Round-2 effect, concentration, deterministic fingerprint.
- Archive diagnostics inherit no evidence.

## Confirmation and null

- Unchanged confirmation: 2024 Q1, Q2 and Q3; Q4 excluded.
- At least two supportive quarters for shadow admission; no catastrophic quarter.
- Candidate-level five-session block sign-flip, one frozen primary, prospective promotion alpha `0.03`.
- Calibrated zero-risk shadow support may use `p <= 0.20`; this cannot confer PAPER_SHADOW_READY.
- Remove best event/day/month, delay entry one completed minute, nearby quantiles and horizons, cost stress, MLL and Topstep path.

## Data contracts

- Energy Parquet SHA-256: `07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374`.
- Energy map SHA-256: `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`.
- Metals volume-front Parquet SHA-256: `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`.
- Metals volume-front map SHA-256: `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`.
- Metals roll-map hash: `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`.
- Period ends `2024-10-01` exclusive; Q4 prohibited.

## Causal and execution contract

1. CME trading-day labels and Chicago DST-aware clocks.
2. Prior-session values are shifted by one completed session.
3. Overnight metrics use only bars preceding the current RTH open.
4. Opening-window features are available only after the entire 15m/30m window closes.
5. Entry is the next complete one-minute executable proxy; no decision-bar fill.
6. Mini/micro thresholds are computed independently from their own past data.
7. Explicit contracts, roll exclusions, correct multipliers and complete costs.
8. No overnight position and mandatory RTH flatten.
9. No Q4, paid/network request, broker, live or order capability.

## Allowed paths

- `hydra/research/energy_metals_session_geometry_primary.py`;
- `hydra/mission/controller.py`;
- `hydra/mission/experiment_runner.py`;
- `tests/test_energy_metals_session_geometry_primary.py`;
- scheduler tests;
- this task and ignored experiment artifacts.

## Protected paths

- `config/governance/**`;
- Q4/lockbox data and ledgers;
- historical candidates/manifests;
- data and map caches;
- mission/registry schemas;
- live, broker, credential and order paths.

## Acceptance tests

- exact 432 unique, fresh hypotheses and family/niche caps;
- DST-aware deterministic session aggregation;
- no prior-session or opening-window leakage;
- one event maximum per session;
- mini/micro independent thresholds and full costs;
- frozen primary before 2024;
- candidate null/diagnostic separation;
- best-event/day/month and delay stress;
- no inherited archive evidence;
- zero Q4/network/orders;
- queue/restart/reconciliation and generic shadow activation;
- full tests, compilation, governance, integrity and secret scan.

## Kill and pivot conditions

- no eligible 2023 primary;
- 2024 economics or cost stress non-positive;
- fewer than two supportive quarters or catastrophic quarter;
- mini/micro transfer failure;
- candidate null gives no calibrated shadow support;
- hard timing, session, contract, sizing or integrity failure.

## Interpretation boundary

At most one SHADOW_RESEARCH_CANDIDATE can emerge. No development result can grant PAPER_SHADOW_READY, open Q4, or authorize an order.
