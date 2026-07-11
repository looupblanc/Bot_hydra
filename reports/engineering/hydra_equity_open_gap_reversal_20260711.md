# Immutable Research/Engineering Task — Equity RTH Open-Gap Reversal v1

Task ID: `eng_equity_open_gap_reversal_20260711_v1`

Frozen before inspecting any result from this formulation.

## Scientific objective

Test whether an unusually large, explicitly contracted overnight displacement
in US equity-index futures is partially rejected after the 08:30
America/Chicago RTH open. This is a direct, sparse, executable event strategy,
not another Edge Atom or a nearby repair of a previously killed NQ proxy.

The mechanism is one economic family. ES/MES, NQ/MNQ, RTY/M2K and YM/MYM are
contractual replications and sizing alternatives; mini/micro copies must not be
counted as independent mechanisms.

## Frozen formulation

- development/falsification data only, end exclusive `2024-10-01`;
- Q4 access, network requests, paid data and broker/live execution prohibited;
- explicit-contract 1-minute OHLCV and repaired date-aware roll map required;
- RTH open decision at the close of the 08:30 CT source bar;
- reference price is the last completed 1-minute bar at or before 15:00 CT in
  the previous trading session and within the same explicit contract;
- gap is current RTH open minus that previous completed RTH close;
- direction is contrarian to the signed gap;
- primary holding horizon: 60 completed one-minute bars;
- event threshold: past-only expanding 75th percentile of absolute gap,
  calculated separately by root symbol, with at least 40 prior sessions;
- primary costs: conservative round-turn commissions plus two ticks of
  slippage, using correct multipliers and tick values for each contract;
- exactly one primary event per market/session; roll-crossing and incomplete
  horizon events are excluded;
- rolling evaluation folds: 2023 H2, 2024 Q1, 2024 Q2, 2024 Q3;
- 30- and 90-minute horizons and 65th/85th-percentile thresholds are
  robustness diagnostics only and cannot replace a failed primary result;
- a one-bar entry delay, sign-flip control, block-bootstrap null and
  best-event/best-period concentration are preregistered attacks;
- Topstep-path replay is permitted only after positive net development
  economics and non-catastrophic temporal transfer.

## Decision policy

Hard failures include leakage, invalid contract/roll mapping, future bars,
wrong multipliers, impossible fills, duplicate events, Q4 access, uncontrolled
sizing/MLL or any outbound-order capability.

`PAPER_SHADOW_READY` requires the calibrated Foundry policy: positive net
economics after primary costs, at least two supportive external-like folds,
candidate null probability at most 0.05, at least 30 events, parameter
neighborhood and mini/micro contract evidence, acceptable conservative MLL,
complete deterministic shadow package and no hard invalidation.

A safe but statistically weaker result may be
`SHADOW_RESEARCH_CANDIDATE`. A positive but incomplete result remains
`PROMISING_RESEARCH_CANDIDATE` or `INSUFFICIENT_EVIDENCE`. No component status
is inherited and no Q4 access is authorized by this pilot.

## Allowed implementation paths

- `hydra/research/equity_open_gap_reversal.py`
- Foundry controller/runner/status integration
- focused tests and immutable experiment artifacts

## Protected paths

Q4/future lockboxes, governance roles, registry and mission databases,
credentials, raw market data, broker/order modules and historical results.

## Acceptance and rollback

The implementation must prove prefix/closed-bar invariance, explicit contract
continuity, deterministic output, correct cost/multiplier arithmetic, bounded
development access and zero outbound orders. Roll back on any Q4 read, future
bar dependency, contract contamination, non-determinism, weakened fatal gate or
two failed implementation attempts.

Expected decision information value: `0.94`; data cost: `$0`; one sparse event
mechanism directly tests whether the prior broad zero-survivor result was partly
caused by strategy synthesis rather than absence of monetizable structure.
