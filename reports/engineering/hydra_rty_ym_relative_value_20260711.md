# Immutable Research Task — RTY/YM Relative-Value Residual v1

Task ID: `eng_rty_ym_relative_value_20260711_v1`

Frozen before executing this formulation.

## Scientific objective

Test a market-neutral-ish, two-leg relative-value mechanism between US small-cap
and price-weighted large-cap index futures. This is not the killed directional
NQ proxy lineage. It uses synchronized RTY/YM contracts, a past-only hedge
ratio, integer micro sizing and explicit two-leg costs.

## Frozen formulation

- signal markets: RTY and YM; execution markets: M2K and MYM;
- deterministic closed 30-minute bars during 08:30–14:00 America/Chicago;
- each decision uses a fully closed 30-minute return on both legs;
- past-only hedge beta: rolling covariance(RTY,YM)/variance(YM) over 120 prior
  synchronized returns, shifted one bar; beta clipped to [0.25, 2.50];
- current residual return: RTY return minus past beta times YM return;
- residual z-score uses the prior 40 residual observations only, shifted one
  bar; trade when absolute z >= 2.0;
- enter on the next synchronized one-minute source-bar open after the 30-minute
  decision; exit at the close of the 120th subsequent synchronized minute;
- mean-revert the residual: positive z shorts RTY/M2K and buys YM/MYM; negative
  z does the reverse;
- frozen integer micro sizing: 2 M2K versus 1 MYM; report beta/dollar exposure
  mismatch and reject uncontrolled imbalance;
- mini-contract replay 2 RTY versus 1 YM is contractual evidence only;
- both leg contracts must remain unchanged through the holding period;
- primary two-leg micro cost: each leg's conservative commission plus two ticks,
  multiplied by integer quantity; no legging benefit assumed;
- folds: 2023 H2, 2024 Q1, Q2 and Q3; Q4 excluded;
- z=1.75/2.25, beta windows 80/160, holds 60/180 minutes, one-bar delay,
  sign flip, 1.5x cost, block null, leave-one-contract-out, exposure mismatch,
  event/fold concentration and synchronized-fill failure are diagnostics;
- development/falsification only; paid data, network, broker/live and outbound
  orders prohibited.

## Decision policy

Contemporaneous/future beta, incomplete 30m bars, unsynchronized legs,
cross-contract targets, lookahead, wrong multipliers, impossible simultaneous
fills, uncontrolled dollar/beta exposure, cost, MLL, governance or order-path
failures are fatal.

Positive pooled net alone cannot promote. Shadow research requires temporal,
candidate-null, parameter, contract, exposure, cost, MLL and complete two-leg
virtual-execution evidence. `PAPER_SHADOW_READY` remains impossible without an
untouched holdout.

## Allowed paths

- `hydra/research/rty_ym_relative_value.py`
- minimal two-leg/MTF/controller integration
- focused tests and immutable artifacts

## Protected paths

All governance files, Q4/future lockboxes, registry/mission databases, raw
market data, credentials and broker/order modules.

Expected decision information value: `0.94`; data cost `$0`. The mechanism is
behaviorally distinct from directional gap and trend-confirmation strategies
and could diversify shared MLL if it survives.
