# HYDRA Distributional Survival-Hazard Search

- Task ID: `hydra_distributional_survival_hazard_20260711`
- Created: 2026-07-11 UTC
- Pipeline: Discovery / defensive risk research
- Parallel-safe: yes; sole market-data access-ledger writer in its batch
- Expected decision information value: 0.97

## Scientific objective

Predict the probability that the next 120 completed one-minute bars enter an
extreme adverse-range state on MYM, M2K, MCL or MGC.  This is a calibrated
hazard/risk model, not direct alpha.  It becomes strategy research only after a
separate immutable policy proves avoided loss or portfolio utility.

## Frozen representation

At each target market's RTH open, use only completed prior-session features from
YM, RTY, CL and GC:

- prior-session signed return;
- signed prior-session range shock;
- prior-session close location.

The target is the realized worst absolute adverse excursion over the following
120 completed one-minute bars, expressed in micro-contract dollars.  A tail
event is defined against the target market's rolling 80th percentile of the
previous 40 realized excursions with at least 20 observations.  The current
outcome never enters its own threshold or features.

Models:

- one fixed L2 logistic model per target market;
- `C=0.10`, balanced class weights, maximum 1,000 iterations;
- past-only median imputation and standardization;
- minimum 120 training sessions;
- expanding rolling origin: train through 2023 for Q1 2024, through Q1 for Q2,
  and through Q2 for Q3;
- one-session purge at every boundary;
- no hyperparameter search after validation.

## Prospective evidence

Report per market and pooled:

- ROC AUC;
- Brier score and skill versus training prevalence;
- log loss;
- calibration error;
- top-20% hazard lift and capture rate;
- avoided tail severity for a hypothetical fail-closed deactivation alert;
- 4,096 random-deactivation controls;
- quarter-by-quarter transfer;
- feature coefficients and stability.

A model may become `ROBUST_RESEARCH_CANDIDATE` only if pooled/market AUC is at
least 0.60, Brier skill is positive, top-bin lift is at least 1.35, at least two
quarters are supportive, and random-control p is at most 0.10.  It cannot become
`SHADOW_RESEARCH_CANDIDATE` until a separate policy construction proves account
utility on exact strategy trades.

Validator controls:

- permuted-label negative controls must remain near chance;
- injected monotonic weak-real controls must achieve at least 0.75 AUC;
- failed calibration blocks scientific interpretation.

## Frozen data

- core data SHA-256:
  `07b3093ed8ef5888898abc3e531e0b522273a6c2047489b60eb36b33afeaf374`;
- core map SHA-256:
  `401ca56ebab606c3eb2cbcf6ed244204f264ed2894c2ee0eb2310998f9244fda`;
- core roll hash:
  `705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208`;
- metals data SHA-256:
  `6bca31351820713016426286de8ae3ce9f0350b6886f780cccc5565fd65da78d`;
- metals map SHA-256:
  `2ac275f4043ef210afa092be8e7f6676c0409c6e2ec5e41a01aecb37427f3815`;
- metals roll hash:
  `01ba149449a494a7a118884813abe10de8845c215b7390dbfbfa9d9dff89de13`.

Selection/training ends before each validation fold.  Latest permitted data is
2024-09-30.  Q4 and later are prohibited.

## Allowed paths

- `hydra/research/distributional_survival_hazard.py`;
- experiment runner/controller integration;
- targeted tests and generated reports.

## Protected paths

- governance/Q4/lockbox and budget policy;
- market data/maps;
- existing candidate results and shadow configurations;
- registry/mission schemas;
- live, broker, credentials and order paths.

## Acceptance and kill conditions

- closed prior sessions and future outcome separation are proven;
- rolling threshold is shifted and past-only;
- train imputation/scaling never fits validation data;
- quarter boundaries are purged;
- calibration controls pass;
- role-specific metrics and random controls are complete;
- no direct-alpha, Shadow or Paper promotion;
- Q4/network/paid/live/order deltas zero;
- full tests, no-lookahead, compile, SQLite, governance and secret scan.

Kill exact models for leakage, invalid contracts/timing, failed controls,
nonpositive Brier skill, AUC below 0.55 or unstable direction.  Insufficient but
calibrated discrimination remains diagnostic evidence for the next pivot.

## Interpretation boundary

This experiment researches risk-state prediction.  It does not itself trade,
deactivate an active strategy, prove portfolio benefit, open Q4, grant shadow or
Paper status, or authorize orders.
