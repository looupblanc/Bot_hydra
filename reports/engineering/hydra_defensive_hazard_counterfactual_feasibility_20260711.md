# HYDRA Defensive Hazard Counterfactual — Outcome-Blind Feasibility Decision

Decision time: `2026-07-11T00:55:00Z`

Task: `eng_hydra_defensive_hazard_counterfactual_20260711_v1`

## Decision

`FREEZE_NOT_IDENTIFIABLE_UNDER_PREREGISTERED_COUNTERFACTUAL`

The v4 defensive shared-loss-risk hypothesis is frozen before any new target or
future return was constructed or inspected. It is not falsified economically;
it is not identifiable under the preregistered counterfactual contract because
the high-risk event is too close to the required past-volatility and lagged
opportunity-frequency covariates.

No official v4 atom is created, no old status is inherited, and no strategy is
assembled. Continuing to the outcome stage would be guaranteed to return
`COUNTERFACTUAL_INSUFFICIENT` and therefore has lower expected decision value
than an immediate representation/market-ecology pivot.

## Frozen source and governance

- v3 result hash: `09e65c7f242b5aaee2f5ab9b08ee2891a752f99975aa7718c3cd595c4e8b29fe`
- repaired roll-map hash: `705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208`
- development boundary: `< 2024-10-01`
- Q4 access: `0`
- future outcome/target reads by the feasibility smoke: `0`
- network requests: `0`
- incremental Databento spend: `USD 0`
- live/broker actions: `0`

## Outcome-blind evidence

Both smokes used event labels and past-only covariates only. The target column
was never built. The frozen acceptance thresholds were event support coverage
`>= 0.70`, maximum weighted SMD `<= 0.10`, and effective sample size `>= 50`
in every decisive market.

### Preregistered implementation

- population: `72,631` rows (`28,698` events, `43,933` controls)
- weighted rows in common support: `15,619`
- market event coverage range: `0.2124` to `0.3375`
- market maximum-SMD range: `0.1752` to `0.2337`
- decisive markets passing all gates: `0 / 7`

A ridge sensitivity from `2` through `500`, chosen without outcomes, showed the
expected support/balance tradeoff. At ridge `100`, minimum market coverage first
exceeded `0.70`, but maximum market SMD rose to `1.0658`. At ridge `200`,
coverage rose to `0.8179` while maximum SMD rose to `1.3323`. Regularization
therefore cannot honestly satisfy both constraints.

### Transition-control ablation

The only defensible design ablation restored adjacent below-threshold controls
that had been removed by a 30-bar event-label embargo. Both arms remained
independently thinned to non-overlapping 30-bar observations, and no outcome was
read.

- population: `83,442` rows (`28,698` events, `54,744` controls)
- weighted rows in common support: `29,566`
- market event coverage range: `0.3316` to `0.5866`
- market maximum-SMD range: `0.0783` to `0.1281`
- decisive markets passing all gates: `0 / 7`
- severe Q3 positivity failures remained, including ES coverage `0.00087` and
  YM coverage `0.08535` at cell level

The ablation improves balance and population overlap but still cannot reach the
frozen 70% coverage gate. Weakening that gate or omitting the confounding
covariates would manufacture identifiability and is prohibited.

## Scientific interpretation

The earlier hazard association is likely dominated by, or inseparable from,
the definition's own volatility/opportunity state. It cannot currently support
a causal activation rule, a strategy, a Topstep replay, or holdout access.

## Highest-information next action

Pivot across both representation and market ecology. Use a small preregistered
tournament of genuinely distinct mechanisms:

1. cross-index leadership transitions expressed as a one-leg lagger signal,
   avoiding the two-leg cost failure of prior relative-value prototypes;
2. interpretable path-geometry/state-transition hazards rather than mean-return
   atoms;
3. metal and energy session-transition mechanisms on GC/MGC and CL/MCL, which
   were not covered by the prior equity-heavy representation screen.

Use 2023 for discovery/method calibration and 2024 Q1-Q3 for sequential
replication. Screen mechanism evidence before execution optimization; build a
sparse executable strategy only from a survivor.
