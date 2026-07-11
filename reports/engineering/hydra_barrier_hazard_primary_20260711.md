# Immutable Engineering Task — Barrier-Hazard Primary v1

## Scientific objective

- Task ID: `hydra_barrier_hazard_primary_20260711`
- Status: preregistered before any barrier-hazard candidate result is read.
- Objective: test whether past-only path states predict that a symmetric executable profit barrier is reached before an equally distant invalidation barrier, and whether the resulting bracket strategy transfers across 2023 selection folds, 2024 Q1–Q3, and mini/micro contracts.
- Uncertainty addressed: terminal-return strategies can hide path-dependent failure and a favorable endpoint reached only after an account-threatening excursion.
- Expected decision information value: `0.995`.
- Incremental data/network cost: `USD 0.00`.

## Frozen representation

1. Use new candidate IDs and fingerprints; inherit no prior status.
2. Use explicit-contract, roll-guarded 1-minute OHLCV ending before `2024-10-01`.
3. Derive all state variables and volatility/range scales from completed past bars, shifted before decision.
4. Use a bounded multi-asset population over ES/MES, NQ/MNQ, RTY/M2K, YM/MYM, GC/MGC, and CL/MCL.
5. Research new path-state families including signed close-location persistence, range acceleration, return-sign persistence/curvature, and recovery from a past extreme.
6. At an event, decide after a completed bar and enter no earlier than the following 1-minute bar open.
7. Set symmetric target and stop distances from a frozen past-only median-range scale. No arbitrary target/stop grid.
8. Scan explicit 1-minute paths from entry through the frozen horizon.
9. If target and stop are touched in the same 1-minute bar, classify the stop as first.
10. For a stop gap, use the worse of the stop level and bar open. For a target gap, do not credit improvement beyond the target level.
11. If neither barrier is hit, exit at the frozen horizon close.
12. Include realistic round-turn commissions and two ticks of slippage.
13. Report target-first, stop-first, ambiguous-stop-first, timeout, MFE/MAE, net PnL, and intraday MLL path.

## Sequential selection and validation

- Round 1: `2023-01-01` through `2023-06-30` cheap validity/economics screen.
- Round 2: `2023-07-01` through `2023-12-31` target-before-stop hazard, cost stress, concentration, and mini/micro selection.
- Freeze at most one primary before evaluating its 2024 result.
- All other archive candidates remain diagnostic-only.
- Confirmation: unchanged primary on 2024 Q1–Q3, with no 2024 optimization.
- Candidate-level mandatory null: exact one-sided binomial test of target-first outcomes against probability `0.5` among resolved symmetric-barrier events, prospectively at `p <= 0.03`.
- Also require positive net after costs, positive 1.5x cost stress, at least two supportive temporal folds, non-catastrophic transfer, parameter-neighborhood support, and positive mini/micro evidence.
- Q4 is prohibited.
- `PAPER_SHADOW_READY` is prohibited.
- A no-order shadow package may be exported only if the calibrated shadow policy is satisfied without a hard invalidation.

## Bounded population

- Target: 144 structurally unique candidates, 24 per primary market.
- Six mechanism recipes per feature/market combination, coupled to a causal economic story rather than a free parameter grid.
- Horizons: 15, 30, and 60 completed 1-minute bars.
- Sessions: open, middle, late, or full eligible market session.
- Completed context bars: optional 5m, 15m, or 30m state; incomplete bars prohibited.
- Barrier scale multipliers: bounded structural choices tied to horizon/state, target always equal to stop for the mandatory hazard test.
- One promotion primary maximum.

## Required artifacts

- immutable preregistration and population manifest;
- early-fold archive and primary freeze manifest;
- complete primary barrier trade ledger;
- barrier-resolution and ambiguity ledger;
- mini/micro and quarterly confirmation evidence;
- exact-binomial null evidence;
- cost, delay, best-event/day/fold, MLL, Topstep, and parameter-neighborhood diagnostics;
- immutable zero-order shadow configuration if eligible;
- integrity proof, result hash, report, and append-only development-access record.

## Allowed paths

- `hydra/research/barrier_hazard_primary.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_barrier_hazard_primary.py`
- `tests/test_mission_scheduler.py`
- ignored experiment output/state directories.

## Protected paths

- `config/governance/**`
- Q4/holdout governance and role ledgers except authorized append-only development records;
- existing immutable manifests and results;
- mission/registry database schemas unless separately specified and snapshotted;
- all broker, credential, order, and live-execution paths.

## Acceptance tests

- deterministic population and fingerprints;
- future-row invariance of every feature and barrier scale;
- decision/entry timestamps respect completed bars;
- target-first, stop-first, same-bar ambiguity, gap-stop, and timeout cases;
- same-bar ambiguity always loses conservatively;
- exact-contract and roll/session boundaries;
- 2024 absent from selection/freeze;
- zero or one primary;
- diagnostic candidates cannot promote;
- exact-binomial false-positive control and useful injected-effect power;
- mini/micro equivalence and correct multipliers/costs;
- Q4 delta zero and no network/paid/live/order access;
- full pytest, compileall, no-lookahead, integrity, governance, secret, scheduler, and one-writer checks.

## Rollback conditions

- any future or incomplete-bar information;
- optimistic same-bar ordering or gap fill;
- wrong contract/multiplier/cost/session;
- more than one promotion test;
- primary freeze after 2024 evaluation;
- insufficient null calibration;
- Q4, paid-data, network, broker, or order access;
- memory/throughput regression that makes the persistent service unsafe;
- protected governance modification.

## Interpretation boundary

A passing primary is a development-confirmed path-hazard strategy candidate, not a sealed-holdout result and not `PAPER_SHADOW_READY`. A failed primary kills only that exact frozen version. No strategy is made viable by simulator PnL or a Topstep path alone.
