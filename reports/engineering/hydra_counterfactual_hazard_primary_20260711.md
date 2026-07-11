# Immutable Engineering Task — Counterfactual Hazard Primary v1

## Status and scientific objective

- Task ID: `hydra_counterfactual_hazard_primary_20260711`
- Status: preregistered before any result from this experiment is read.
- Objective: determine whether a completed-bar market context changes the probability of a positive net outcome for a sparse state transition relative to past-only matched counterfactual events, and convert at most one prospectively frozen primary into an executable zero-order strategy candidate.
- Decision uncertainty addressed: the prior direct-rule search can select profitable development paths without proving that the activation context adds a stable conditional effect.
- Expected decision information value: `0.995`.
- Expected data cost: `USD 0.00`; cached governed development data only.

## Frozen scientific contract

1. Create new candidate IDs and new structural fingerprints. No prior status is inherited.
2. Use explicit-contract, roll-guarded 1-minute OHLCV ending before `2024-10-01`.
3. Derive all features from completed bars and shift rolling statistics before the decision timestamp.
4. Generate a bounded multi-asset population across equity indices, metals, and energy using new path-efficiency, recovery, compression, and accepted-migration representations.
5. A treatment is a state event whose preregistered completed higher-timeframe context is present. Its counterfactual pool contains the same base state without that context.
6. Match without outcome information using explicit contract, session phase, past volatility, prior displacement, past participation, and past opportunity frequency. A matched control must be from a different event/session and available in the same frozen temporal partition.
7. Use `2023-01-01` through `2023-06-30` for cheap screening and `2023-07-01` through `2023-12-31` for primary selection.
8. Freeze at most one primary, its matching policy, costs, thresholds, and alpha before reading its 2024 confirmation result.
9. All non-primary archive members are diagnostic-only and cannot be promoted from this run.
10. Confirm the frozen primary unchanged on 2024 Q1–Q3. Q4 is prohibited.
11. The candidate-level mandatory statistic is the paired difference in positive-net-outcome probability between treated and matched controls, tested with a preregistered paired sign/permutation procedure.
12. The prospective primary threshold is `p <= 0.03`, with positive treatment uplift, positive net after costs, positive 1.5x cost stress, at least two supportive temporal folds, non-catastrophic transfer, and mini/micro support.
13. Report direct economics separately from counterfactual hazard uplift. Neither may substitute for the other.
14. Run best-event, best-day/fold concentration, one-bar delay, cost stress, parameter-neighborhood, explicit-contract transfer, MLL, and Topstep path diagnostics on the primary.
15. `PAPER_SHADOW_READY` is prohibited because Q4 remains sealed.
16. A zero-risk shadow package may be exported only when the frozen primary has no hard invalidation and satisfies the calibrated shadow policy. It must contain no broker or order capability.

## Population and allocation

- Target executable population: 96–192 unique structures after pre-backtest deduplication.
- Markets: ES/MES, NQ/MNQ, RTY/M2K, YM/MYM, GC/MGC, CL/MCL.
- New representation families: path efficiency, failed-displacement recovery, range compression/release, accepted-price migration.
- Horizons: bounded 15, 30, and 60 completed 1-minute bars.
- Contexts: completed 5m/15m/30m trend agreement or disagreement and completed volatility expansion where structurally compatible.
- Maximum one primary test. Family/ecology archive members remain diagnostics.
- Exact trade-path clones of historical candidates must be rejected or explicitly marked diagnostic-only.

## Required artifacts

- immutable preregistration and population manifest;
- selection/freeze manifest written from 2023 only;
- matched-pair ledger with covariates and pair identifiers;
- primary trade ledger;
- counterfactual balance diagnostics;
- validation and Topstep evidence;
- quality-diversity archive summary;
- machine-readable result and human-readable report;
- data-access record proving Q4 count delta zero;
- integrity proof and deterministic result hash.

## Allowed paths

- `hydra/research/counterfactual_hazard_primary.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_counterfactual_hazard_primary.py`
- `tests/test_mission_scheduler.py`
- generated artifacts below the existing ignored mission/report state directories.

## Protected paths

- `config/governance/**`
- protected data-role and Q4 access ledgers except append-only authorized development-access records;
- existing immutable candidate manifests;
- existing experiment results;
- mission and registry database schemas unless separately specified and snapshotted.

## Acceptance tests

- deterministic new structural fingerprints and population size;
- feature values are invariant to changes in future rows;
- higher-timeframe context is joined only after bar availability;
- matching never reads outcomes and is deterministic;
- matched control is distinct and respects explicit contract/session partitioning;
- 2024 data is absent from primary selection and freeze artifacts;
- exactly zero or one promotion primary;
- diagnostic archive members cannot inherit promotion status;
- paired-null implementation rejects calibrated null controls and detects injected meaningful uplift with useful power;
- Q4 access delta is zero;
- no paid/network/live/broker/order path;
- controller resume and one-writer tests pass;
- full pytest, compileall, no-lookahead, integrity, governance, and secret checks pass.

## Rollback conditions

- any future-bar or incomplete higher-timeframe use;
- contract/roll mismatch;
- matching uses outcome or confirmation-period information during primary selection;
- more than one promotion test;
- primary manifest written after 2024 result access;
- false-positive behavior exceeds the calibrated contract;
- memory regression prevents safe coexistence with the controller and shadow pipeline;
- Q4, paid data, network, broker, or outbound-order access;
- protected governance change.

## Interpretation boundary

A positive result supports a conditional probability mechanism and an executable development candidate only. It is not a sealed-holdout pass, not `PAPER_SHADOW_READY`, and not funded evidence. A negative result kills the exact frozen primary and informs a representation/market-ecology pivot; it does not kill every counterfactual or hazard model.
