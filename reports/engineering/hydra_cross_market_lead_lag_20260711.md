# Immutable Research Task — Cross-Market Lead/Lag Pilot

Task ID: `eng_hydra_cross_market_lead_lag_20260711_v1`

Frozen before outcome construction. Development only (`2023-01-01` through
`2024-09-30`); Q4, paid data, network and live execution prohibited.

Hypothesis: a past-only five-bar NQ repricing leads a one-leg ES/RTY/YM
response over the next 30 bars, after conservative costs. NQ is the leader;
ES, RTY and YM are transfer laggers. The leader return is shifted one bar and
must be available before the lagger entry. Require positive Q1/Q2/Q3 transfer,
contract replication, sign-flip and delayed controls, and no single-day
concentration. A failure kills the mechanism; insufficient evidence never
passes.

No strategy or Topstep status is inherited from historical screens.

Expected decision information value: `0.97`. Maximum automatic retries: 2.
