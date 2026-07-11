# Immutable Engineering Task — Session Geometry Micro-Execution Repair

## Scientific objective

- Task ID: `hydra_session_geometry_micro_execution_repair_20260711`.
- Parent candidate: `strategy_session_geometry_CL_overnight_extreme_position_continuation_q65_h60_prior_trend_agree_v1`.
- Parent result hash: `651d2a3bfb1d2ab56ac6ccaceaf067a8767389a809e153853f309ceb0ed6f69f`.
- Parent result SHA-256: `b4f3158085697e63778849ec2b525f8c74b390fce308d1979a30c1164e4df630`.
- Parent primary-manifest hash: `f11a6f657e018f2d8b137eddb64cf497dcf63ed0ee17848744667fa968201d96`.
- Parent primary-manifest SHA-256: `e62d8b03dd74173c66183d9bca25d27006e5c2b799d2c2aa93f544cbb2fd89d8`.
- Objective: distinguish a false economic transfer failure from a contract-implementation error. A liquid CL signal should be executable on MCL at the same session and timestamp; recomputing a separate MCL signal changes the hypothesis.
- Expected decision information value: `0.999`.

## Fresh child contract

- Child ID: `strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_position_continuation_q65_h60_prior_trend_agree_v2`.
- New lineage version and structural fingerprint; no parent pass status is inherited.
- Signal source remains exactly CL:
  - overnight extreme position;
  - continuation;
  - causal 0.65 threshold from the prior 20 completed CL sessions;
  - 60-minute horizon;
  - prior CL RTH trend agrees;
  - one completed-minute execution delay.
- Execution changes only:
  - match MCL by the same CME trading-session ID;
  - use the MCL bar at the exact CL signal entry timestamp;
  - exit MCL after the same 60 completed minutes;
  - preserve CL side and signal decision;
  - never recompute the signal, threshold or context from MCL.
- Parent and child trade-session overlap and deviations must be reported explicitly.

## Evidence scope

- 2023 and 2024 Q1–Q3 are development/falsification data.
- The parent was already observed on 2024; therefore this child cannot claim independent temporal confirmation from 2024.
- A positive child may reach at most `SHADOW_RESEARCH_CANDIDATE`, where forward evidence is genuinely unseen.
- Q4 remains sealed and PAPER_SHADOW_READY is impossible in this task.

## Frozen data and execution

- Energy Parquet SHA-256: `07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374`.
- Energy map SHA-256: `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`.
- Period: `[2023-01-01, 2024-10-01)`.
- Correct CL/MCL multipliers, explicit contracts, roll exclusions and MCL costs.
- No price interpolation. Missing exact timestamps are skipped and counted.
- Maximum missing matched-session rate: 10%.
- MCL entry uses the exact synchronized bar open; delayed-entry stress uses the following completed minute.
- Mandatory RTH flatten, complete MAE/MLL path and no overnight position.

## Preregistered decisions

- Report 2023 H1, 2023 H2, 2024 Q1, Q2 and Q3 separately.
- Required for zero-risk shadow support:
  - pooled 2023 MCL net and 1.5× cost stress positive;
  - pooled 2024 MCL net and 1.5× cost stress positive;
  - at least two supportive 2024 quarters;
  - no catastrophic quarter;
  - candidate-level five-session block sign-flip `p <= 0.20`;
  - best-event share at most 35%;
  - remove-best-event and remove-best-month net remain positive;
  - one-minute additional delay net remains positive;
  - simulated one-contract MCL MLL safe;
  - immutable zero-order shadow configuration complete.
- Promotion alpha remains `0.03`; failure to reach it is recorded and cannot be relabeled.

## Allowed paths

- `hydra/research/energy_metals_session_execution_repair.py`;
- `hydra/mission/controller.py`;
- `hydra/mission/experiment_runner.py`;
- `tests/test_energy_metals_session_execution_repair.py`;
- scheduler tests;
- this task and ignored experiment artifacts.

## Protected paths

- `config/governance/**`;
- Q4/lockbox data and ledgers;
- parent result, manifest and trade ledger;
- market data and roll maps;
- mission/registry schemas;
- live, broker, credentials and order paths.

## Acceptance tests

- exact parent/result/task hashes;
- fresh child fingerprint;
- CL signal sessions and sides preserved exactly;
- no MCL signal recomputation;
- exact timestamp match only and missing-match audit;
- correct MCL costs/multiplier/roll/MAE;
- fold, null, concentration, delay and MLL evidence;
- no inherited status or independent-validation claim;
- immutable fail-closed shadow configuration with zero orders;
- no Q4/network/paid data;
- queue/recovery/reconciliation/activation;
- full tests, compile, governance, integrity and secret scan.

## Kill conditions

- synchronized MCL economics or cost stress non-positive;
- fewer than two supportive 2024 quarters;
- missing match rate above 10%;
- null, concentration, delay or MLL requirement fails;
- timing, contract, data or governance error.

## Interpretation boundary

This test may repair execution semantics and authorize only no-risk forward shadow research. It cannot validate persistence, open Q4, grant PAPER_SHADOW_READY or submit orders.
