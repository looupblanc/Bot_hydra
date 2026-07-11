# Immutable Research Task — Volatility Transition Pilot

Task ID: `eng_hydra_volatility_transition_20260711_v1`

Frozen before target construction. Development only through 2024-09-30; Q4,
paid data, network, broker and live execution prohibited.

Hypothesis: a past-only transition in `rv_short_long_ratio` predicts a
30-minute continuation move when the ratio crosses above 1.20, with direction
given by past 60-bar return. The rule is event-based (one entry per crossing),
low-turnover, and evaluated with conservative round-turn costs. Require
positive Q1/Q2/Q3, contract and market replication, sign-flip and delayed
controls, and no concentration. Failure kills the mechanism; insufficiency
never passes.

Expected decision information value: `0.96`. Maximum retries: 2.
