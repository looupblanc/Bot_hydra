# Immutable Research Task — Opening Direction Hazard Policy v1

Task ID: `eng_opening_direction_hazard_20260711_v1`

Frozen before executing this representation.

## Scientific objective

Replace unconditional gap continuation/reversal with a calibrated probability
that the 60-minute outcome continues the signed overnight displacement. Convert
only sufficiently confident probabilities into a sparse bidirectional policy;
otherwise abstain. This is Engine B (distributional/hazard), not a threshold
mutation of either prior directional candidate.

## Frozen model and policy

- markets: ES/MES, NQ/MNQ, RTY/M2K, YM/MYM;
- explicit-contract development data, end exclusive 2024-10-01;
- completed 08:30 CT opening bar is the decision source;
- target: signed-gap continuation at the 60-minute completed-bar horizon;
- features available at 08:31 CT only:
  - absolute gap divided by past-only expanding q75 gap;
  - signed opening-bar body aligned with the gap, divided by gap magnitude;
  - opening range divided by gap magnitude;
  - opening volume divided by its past-only expanding median;
  - past-only expanding continuation rate;
  - gap sign and causal weekday sine/cosine;
- model: standardized L2 logistic regression, `C=0.10`, balanced classes,
  deterministic solver/seed;
- rolling-origin folds: train strictly before and test on 2023 H2, 2024 Q1,
  Q2 and Q3; minimum 120 pooled mini-contract training events;
- primary event still requires absolute gap >= past-only q75 and 40 prior
  root sessions;
- continue when probability >= 0.62, reverse when <= 0.38, abstain otherwise;
- primary 60-minute hold and the same conservative commission-plus-two-tick
  costs as the directional pilots;
- mini roots train the model; micro roots are contract/execution transfer only;
- family-wise adjustment covers the four market candidates;
- confidence 0.58/0.42 and 0.66/0.34, 30/90-minute outcomes, one-bar delay,
  label permutation, 1.5x costs, leave-one-market-out coefficients and
  event/fold concentration are diagnostics, never replacements.

## Admission boundary

Hard integrity, contract, cost, sizing, MLL and zero-order requirements remain
fatal. A rolling model must expose scalers, coefficients, calibration/Brier
score, feature availability and a final all-development model artifact before
shadow completeness can be true. No in-sample model score can promote.

`PAPER_SHADOW_READY` remains impossible without untouched holdout. Q4 remains
sealed and this experiment does not inherit status from prior gap candidates.

## Allowed paths

- `hydra/research/opening_direction_hazard.py`
- minimal reusable gap-event helpers
- controller/runner/status integration
- focused tests and immutable artifacts

## Protected paths

All governance files, Q4/future lockboxes, mission/registry databases, raw
market data, credentials and broker/order modules.

Expected decision information value: `0.93`; data cost `$0`. It directly tests
whether predictable failure states explain the directional instability seen in
the preceding experiments.
