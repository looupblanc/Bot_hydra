# HYDRA Cross-Asset Daily Shadow Activation

- Task ID: `hydra_cross_asset_daily_shadow_activation_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Shadow
- Expected decision information value: 1.0

## Objective

Activate the exact immutable candidate
`strategy_daily_cross_CL_to_YM_source_prior_trend_continuation_q80_h120_v1`
for fail-closed forward shadow research if and only if the official frozen
cross-asset daily tournament reproduces the prospective shadow admission.

This activation does not alter the candidate, repeat selection, open Q4 or
confer `PAPER_SHADOW_READY`.

## Required behavior

1. Verify the official source-result file and semantic hash recorded by the
   completed mission experiment.
2. Verify exactly one candidate with the authorized ID and
   `SHADOW_RESEARCH_CANDIDATE` status.
3. Verify the candidate has no fatal invalidation, positive mini/micro net,
   MLL-safe one-contract execution, deterministic real-time features and a
   complete observability package.
4. Verify exactly one immutable shadow configuration with matching file and
   semantic hashes.
5. Audit the code surface for broker imports, credentials and order-submission
   calls.
6. Register the candidate once; reject any in-place mutation or duplicate.
7. Start in `WAITING_FOR_FRESH_FORWARD_DATA`; generate no historical signal or
   fill during activation.
8. Keep outbound orders, broker connections, Q4 access, network requests and
   incremental Databento spend at zero.
9. Preserve the three already active immutable shadow candidates unchanged.

## Allowed paths

- generic immutable activation and existing shadow-pipeline modules;
- mission controller and experiment runner integration;
- targeted activation/recovery tests;
- generated activation manifest and report.

## Protected paths

- governance and Q4/lockbox policy;
- candidate source result, elite manifest and configuration;
- market data and maps;
- budget, registry and mission schemas;
- broker, credentials, execution adapters and order paths.

## Acceptance tests

- immutable source/configuration/task hashes;
- exact candidate identity and evidence tier;
- no pass inheritance or Paper promotion;
- one writer and idempotent registry update;
- safe restart and missing/stale feed fail-closed behavior;
- signals/fills/orders/broker connections all zero on activation;
- Q4/network/paid data deltas zero;
- full tests, no-lookahead, compile, SQLite, governance and secret scan.

## Rollback conditions

Do not activate if the official tournament differs from the prospective gates,
the candidate/configuration is absent or changed, any hard invalidation appears,
the surface audit fails, an existing registry entry conflicts, or any protected
invariant changes.

## Interpretation boundary

`SHADOW_RESEARCH_ACTIVE` means zero-risk forward observation only.  It is not a
funded edge claim, Topstep eligibility, holdout pass, `PAPER_SHADOW_READY`, or
permission to submit orders.
