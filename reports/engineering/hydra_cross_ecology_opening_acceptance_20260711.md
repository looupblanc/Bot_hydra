# Immutable Research Task — Cross-Ecology Opening Acceptance v1

Task ID: `eng_cross_ecology_opening_acceptance_20260711_v1`

Frozen before executing this formulation.

## Scientific objective

Test whether overnight displacement that is confirmed during the first
completed 15-minute primary-liquidity window continues in two genuinely
different futures ecologies: COMEX gold and NYMEX crude oil. This is not an
equity-index clone for independence counting; session clocks, multipliers,
tick economics and participant ecology are explicit.

## Frozen formulation

- GC/MGC primary session: open 07:20 CT, prior completed close 12:29 CT;
- CL/MCL primary session: open 08:00 CT, prior completed close 13:29 CT;
- decision after the 15th source bar closes (07:35 or 08:15 CT);
- overnight gap: first session open minus prior primary-session close in the
  same explicit contract;
- opening acceptance: signed gap and first-15-minute close displacement have
  the same non-zero direction;
- primary event also requires absolute gap >= its past-only expanding 65th
  percentile by root, with at least 40 prior sessions;
- side follows the accepted displacement;
- entry at the completed 15-minute-window close; exit after 60 additional
  completed one-minute bars; no overnight holding;
- development/falsification ends before 2024-10-01; Q4, paid data, network,
  broker and live paths prohibited;
- exact date-aware contracts, roll exclusions, multipliers and tick values;
- conservative round-turn commission plus two ticks of slippage;
- folds: 2023 H2, 2024 Q1, Q2 and Q3;
- 55th/75th-percentile gaps, 30/90-minute holds, one-bar delay, sign flip,
  1.5x costs, block sign null, best-event/fold concentration and mini/micro
  transfer are preregistered attacks;
- GC and CL are separate candidate strategies but one broad opening-acceptance
  mechanism family; micros are contractual/sizing replications only.

## Decision policy

All leakage, partial-window use, incorrect session/DST clock, cross-contract
reference/target, future bar, multiplier, execution, sizing, MLL, governance or
order-capability failures are fatal.

Positive pooled net alone cannot promote. Shadow research requires temporal,
candidate-null, parameter, contract, cost, MLL and executable shadow evidence.
`PAPER_SHADOW_READY` remains impossible without an untouched holdout.

## Allowed paths

- `hydra/research/cross_ecology_opening_acceptance.py`
- minimal controller/runner/status integration
- focused tests and immutable artifacts

## Protected paths

All governance files, Q4/future lockboxes, registry/mission databases, raw
market data, credentials and broker/order modules.

Expected decision information value: `0.91`; data cost `$0`. This is the first
post-takeover strategy-level test outside equity indices and directly reduces
market-ecology uncertainty.
