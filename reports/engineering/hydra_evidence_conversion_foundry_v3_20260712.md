# HYDRA Evidence Conversion Foundry V3 — immutable engineering task

- Created UTC: 2026-07-12
- Baseline commit: `b16e44b77091ebad2abdefe3b828f277a71883f3`
- Scientific objective: convert the existing frozen promotion inventory into
  behaviorally distinct, role-specific complete decisions and a small
  pre-holdout cohort; raw Discovery becomes a secondary supply lane.
- Expected decision information value: 0.99. The current funnel contains more
  than 270 promising records but no Paper-ready strategy, while the finite
  Turbo grammar is exhausted.

## Required behavior

1. Build deterministic Evidence Debt records from immutable completed
   promotion results only; never read Q4 or later data.
2. Cluster execution-equivalent and economically redundant candidates using
   frozen specification, market/session/timeframe, lineage and behavioral
   evidence. Select one primary and no more than two backups per cluster.
3. Freeze one primary role per representative and log separate Combine, XFA,
   defensive and portfolio utility components.
4. Rank representatives by transparent value, decision-impact, distinctness
   and closure-cost components.
5. Run a bounded complete-validation cohort without parameter mutation. Emit
   only `PROMOTION_FAILED`, `SHADOW_RESEARCH_ONLY`, or `PRE_HOLDOUT_READY`.
6. Distinguish FULL_ECONOMIC_REPLAY, FULL_RISK_REPLAY and
   FULL_PROMOTION_VALIDATION counters.
7. Keep Q4 sealed. A pre-holdout classification is not a Q4 pass, Paper status
   or funded authorization.
8. On finite Turbo inventory exhaustion, queue this conversion cohort instead
   of another identical discovery batch. Preserve the single controller and
   writer.
9. Apply queue allocation 70% Promotion, 15% Shadow/feed engineering, 10%
   targeted mutation and 5% Discovery until a cohort is frozen.
10. Materialize conservative OHLC intraday risk paths and replay shared-account
    roles under an explicit micro-first contract policy. Optimize only the
    grouping hot path, with identical deterministic account outputs.

## Allowed paths

- `hydra/promotion/`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `hydra/mission/evidence_conversion_scheduler.py`
- `hydra/portfolio/account_contribution.py`
- `hydra/research/turbo_feature_builder.py`
- `scripts/build_evidence_debt_queue.py`
- `scripts/hydra_mission_status.py`
- relevant tests and lightweight engineering reports

## Protected paths

- governance kernel and protected manifests
- mission and registry databases
- Q4 and post-2024-10-01 market data
- raw market data and secrets
- active shadow configurations

## Acceptance tests

- deterministic debt ranking and clustering;
- exact clone merge and distinct-tail separation;
- role-specific evidence and transparent score components;
- immutable source hashes and no status inheritance;
- complete validation vocabulary and three allowed decisions only;
- queue priority and Turbo-exhaustion pivot;
- one writer, restart preservation and no duplicate cohort;
- full pytest, compileall, no-lookahead, Q4/budget/shadow tests, SQLite
  integrity, governance checksum and secret scan;
- deterministic smoke cohort with Q4, paid data, network and order deltas zero.

## Rollback conditions

- any Q4 or protected-data access;
- any broker/order capability;
- non-deterministic cohort selection;
- inherited strategy status;
- duplicate registry or mission writer;
- regression of protected tests or database integrity;
- source result/hash mismatch.
