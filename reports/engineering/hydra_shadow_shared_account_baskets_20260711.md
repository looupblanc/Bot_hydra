# HYDRA Shadow Shared-Account Baskets

- Task ID: `hydra_shadow_shared_account_baskets_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Portfolio evaluation while Shadow remains active
- Expected decision information value: 0.99

## Scientific objective

Evaluate the four immutable active shadow-research candidates on one shared
Topstep 150K account using their exact 2024 Q1-Q3 trade ledgers.  Measure signal
overlap, same-market conflicts, shared loss days, tail overlap, combined contract
usage, intraday unrealized risk, shared MLL, consistency and target progress.

Do not sum standalone payouts.  Recompute every basket at account scope.

Candidates:

1. `strategy_open_gap_continuation_YM_v1`;
2. `strategy_barrier_hazard_NQ_signed_extreme_recovery_60_middle_q65_h30_s100_15m_expansion_v1`;
3. `strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_position_continuation_q65_h60_prior_trend_agree_v2`;
4. `strategy_daily_cross_CL_to_YM_source_prior_trend_continuation_q80_h120_v1`.

## Frozen sources

YM result/hash:

- file SHA-256 `17921561a4b464d961bd23f2f469052a89dbc9f4551202a3c4a325a6efca2a31`;
- semantic hash `89c63a68d52a8b3a1277df0cbe8553c2382bf057c8bc2e1fd3ccfb9707c2eecf`;
- ledger SHA-256 `fbb20c9cf5a33f8867b48e0fba8d75a6ebdf083950765187cc5e6fc8a2f63826`.

NQ barrier result/hash:

- file SHA-256 `17c4f1bbae092901e408f1f1d03a15d5afcab358cfd66b64a5145e62858fc553`;
- semantic hash `9243e40d8f08fadec401004f752b0c69bf53800262d5af08a081ea4a075e4bbf`;
- ledger SHA-256 `3b8ac95ccebc754c8a87b6b6d4f3f8eb52d20bdbe659b97176f277d186ef8e02`.

CL/MCL result/hash:

- file SHA-256 `34f7ceaba8128d9491451762e266b422886b1545b637042ef8c49defcc8ec2eb`;
- semantic hash `8336f9231adf63828707b2c31e17d247cfd4ea0d614a330dd83bff0817eceb3b`;
- ledger SHA-256 `735a01c3f1b1c8585c872d83e5e6986da06fec8ca8424e8dfcc4a30fd4887cb1`.

CL→YM daily result/hash:

- file SHA-256 `717c088194f9a377c8bc045e9e5b6fcb364f8a8a38209242df5f836505a877a5`;
- semantic hash `a76176fc6619dfb669343c65650e0a5b09f795a1715ec3385c7b59d44069b553`;
- ledger SHA-256 `98e5da466bc7e594d781370ab8bc169a44b26757ac545709df4502c055abc01b`.

## Required behavior

1. Verify every result and trade-ledger hash and exact candidate identity.
2. Use only 2024-01-01 through 2024-09-30 development confirmation.
3. Normalize trades without changing entries, exits, sides, costs, PnL or MAE.
4. Filter the NQ ledger to the exact primary micro execution and the daily
   ledger to the exact active candidate.
5. Calculate pairwise daily-PnL correlation, interval overlap, same-underlying
   signal conflicts, shared loss days and joint tail days.
6. Enumerate every unique subset of two to four candidates; one micro contract
   per strategy, one shared account and one shared MLL.
7. Reconstruct a conservative intraday path: while positions overlap, assume
   their recorded adverse excursions can co-occur.
8. Run the current versioned Topstep 150K Combine simulation, 1.5x cost stress,
   consistency and target progress at account scope.
9. Select at most three versioned basket roles: maximum MLL survival, balanced
   progress, and low-correlation diversity.
10. Mark a basket executable only if contract usage is valid, shared MLL does not
    breach, minimum buffer is at least USD 1,000, and no data/execution ambiguity
    remains.
11. Do not claim standalone payouts, Paper readiness, funded readiness or
    independent validation.

## Allowed paths

- `hydra/portfolio/shadow_shared_account.py`;
- mission controller and experiment runner integration;
- targeted tests;
- generated basket manifest, trade attribution and report.

## Protected paths

- source results, ledgers and active shadow configurations;
- governance/Q4/lockbox policy;
- market data and budget ledgers;
- registry and mission schemas;
- live, broker, credentials and order paths.

## Acceptance tests

- all frozen hashes and candidate filters;
- exact normalized trade totals against candidate results;
- deterministic overlap/correlation/tail metrics;
- one shared MLL and contract limit;
- conservative overlapping-MAE path;
- 1.5x cost stress and account-level consistency;
- no standalone payout summation;
- deterministic three-role selection with no duplicate basket ID;
- Q4/network/paid/live/order deltas zero;
- queue/recovery/routing, full tests, compile, SQLite, governance and secret scan.

## Kill conditions

Reject a basket for source mismatch, invalid attribution, duplicate candidate,
shared MLL breach, buffer below USD 1,000, contract conflict, non-finite path or
non-positive cost-stressed account economics.  A rejected basket does not kill
its component strategy.

## Interpretation boundary

An executable shadow basket is a deterministic no-order account configuration
for forward research.  It is not `PAPER_SHADOW_READY`, a Topstep payout claim,
holdout evidence, funded eligibility or permission to trade.
