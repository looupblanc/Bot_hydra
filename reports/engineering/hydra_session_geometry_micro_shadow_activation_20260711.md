# Immutable Engineering Task — Session Geometry Micro Shadow Activation

## Objective

- Task ID: `hydra_session_geometry_micro_shadow_activation_20260711`.
- Candidate: `strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_position_continuation_q65_h60_prior_trend_agree_v2`.
- Activate only if the preceding synchronized-execution experiment concludes `SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND`, the exact candidate is `SHADOW_RESEARCH_CANDIDATE`, and the immutable configuration permits zero-risk shadow.
- Expected decision information value: `1.0`; forward evidence is the first genuinely unseen evidence available to this parent-informed child.

## Required behavior

1. Verify the runtime-frozen source result SHA-256/result hash and exact child ID.
2. Verify the runtime-frozen shadow configuration SHA-256/configuration hash.
3. Audit the complete shadow code surface and prove no outbound-order capability.
4. Register one immutable active version; refuse in-place mutation or duplicate activation.
5. Initial state is `WAITING_FOR_FRESH_FORWARD_DATA` and fail-closed.
6. Signal source is CL; virtual execution instrument is MCL; exact session/timestamp matching is mandatory.
7. Missing/stale CL or MCL data, clock mismatch, roll mismatch, duplicate signal or MLL breach produces no signal/fill.
8. Broker connections and outbound orders remain zero.
9. Q4 access, network purchase, PAPER_SHADOW_READY and funded status remain unchanged.

## Allowed paths

- existing generic shadow activation/runner modules without weakening them;
- `hydra/mission/controller.py`;
- `hydra/mission/experiment_runner.py`;
- scheduler/shadow tests;
- this task and ignored activation artifacts/state.

## Protected paths

- `config/governance/**`;
- Q4/lockbox data and ledgers;
- source result/configuration;
- broker, credential, order and live paths;
- mission/registry schemas.

## Acceptance tests

- exact source/configuration identity;
- zero broker/order capability;
- stale/missing dual-market feed fails closed;
- duplicate signal and restart reconciliation;
- immutable active registry entry;
- one writer;
- no Q4/network/paid data;
- full tests, compile, governance, integrity and secret scan.

## Rollback conditions

- source candidate is not shadow-eligible;
- any hash/config/code-surface mismatch;
- any order or broker capability;
- non-deterministic restart or registry conflict;
- governance or Q4 change.

## Interpretation boundary

Activation collects forward virtual evidence only. It does not prove persistence, confer PAPER_SHADOW_READY, authorize Q4 or enable an order.
