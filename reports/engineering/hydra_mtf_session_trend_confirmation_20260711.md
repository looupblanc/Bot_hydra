# Immutable Research Task — MTF Session Trend Confirmation v1

Task ID: `eng_mtf_session_trend_confirmation_20260711_v1`

Frozen before executing this formulation.

## Scientific objective

Test a causal multi-timeframe invariant: an efficient prior completed RTH
session whose direction is confirmed by the next session's fully completed
first 30 minutes may continue for another hour. The higher-timeframe state is
session-level; execution remains on explicit-contract one-minute bars.

## Frozen formulation

- ES/MES, NQ/MNQ, RTY/M2K and YM/MYM;
- prior RTH session: 08:30–14:59 America/Chicago, aggregated only after its
  final source bar closes;
- prior efficiency: absolute prior open-to-close displacement divided by prior
  high-low range;
- prior efficiency must exceed its root-specific past-only expanding 65th
  percentile, with at least 40 earlier completed sessions;
- current opening window: source bars 08:30–08:59 CT, decision at 09:00 only;
- current 30-minute absolute displacement must exceed its root-specific
  past-only expanding 55th percentile;
- prior session and current 30-minute displacement must have the same non-zero
  sign; side follows that sign;
- entry at the completed 08:59 bar close; primary exit after 60 additional
  completed one-minute bars; no overnight hold;
- explicit date-aware contract, roll, session/DST and multiplier guards;
- development/falsification data end exclusive 2024-10-01; Q4, paid data,
  network, broker and live execution prohibited;
- conservative round-turn commission plus two ticks;
- folds: 2023 H2, 2024 Q1, Q2 and Q3;
- efficiency q55/q75, opening-displacement q45/q65, 30/90-minute holds,
  one-bar delay, sign flip, 1.5x cost, block null, event/fold concentration and
  mini/micro transfer are diagnostic attacks;
- each mini market is a candidate strategy, micros are contract/sizing
  replications, and all four share one MTF confirmation mechanism family.

## Decision policy

Incomplete session/30m bars, future higher-timeframe joins, leakage, wrong
session clock, contract/roll crossover, target contamination, multiplier,
execution, sizing, MLL, governance or outbound-order failures are fatal.

Positive net alone cannot promote. Shadow research requires calibrated
candidate-null, temporal, parameter, contractual, cost, MLL and immutable
zero-order shadow evidence. `PAPER_SHADOW_READY` remains impossible without an
untouched holdout.

## Allowed paths

- `hydra/research/mtf_session_trend_confirmation.py`
- minimal reusable MTF/event helpers
- controller/runner/status integration
- focused tests and immutable artifacts

## Protected paths

All governance files, Q4/future lockboxes, registry/mission databases, raw
market data, credentials and broker/order modules.

Expected decision information value: `0.92`; data cost `$0`. This is the first
strategy-level use of the canonical completed-session/30m/1m architecture.
