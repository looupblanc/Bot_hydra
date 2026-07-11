# HYDRA Engineering Task — Single-Primary Alpha Calibration v3

## Objective

Calibrate a prospective, high-power shadow-promotion null lane with exactly one
candidate selected and frozen on an earlier development fold before an independent
confirmation fold. This follows v2, where no 20-elite family policy met both the
false-positive and power requirements.

## Frozen design

- Experiment ID: `single_primary_alpha_calibration_v3`
- Candidate count eligible for promotion per confirmation fold: exactly 1.
- Diagnostic archive candidates: unlimited by this test, but never promotion
  eligible on the same confirmation fold.
- Candidate alpha grid: 0.01, 0.02, 0.025, 0.03 and 0.04.
- Event counts: 80, 120 and 360.
- Standardized net effects: 0.00, 0.25 and 0.40.
- Five-event block sign-flip statistic.
- Replications: 500 per condition.
- Null draws: 2,048.
- Explicit cost ratio: 0.05 standard deviations per event.
- Random seed: 774101.

For every alpha, report maximum null false admission across sample sizes and power
by effect/sample size. A policy is eligible only if:

- maximum point false-admission rate is at most 0.05;
- the upper 95% Wilson bound is at most 0.07;
- power for effect 0.40 at 120 events is at least 0.80.

Select lexicographically:

1. eligible policies only;
2. highest power for effect 0.25 at 120 events;
3. highest power for effect 0.40 at 80 events;
4. lower alpha;
5. simpler fixed-threshold policy.

## Prospective contract

If calibrated, future tournaments must:

- select exactly one primary using only the earlier development fold;
- freeze its exact ID, code, data, parameters, costs and null policy;
- test it once on the later confirmation fold;
- retain all other archive elites as diagnostics only;
- require new IDs and a new independent confirmation for any other elite;
- never reinterpret historical QD/accelerated p-values under this policy.

## Allowed paths

- `hydra/calibration/single_primary_alpha.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_single_primary_alpha.py`
- immutable calibration artifacts

## Protected paths

- prior results, statuses and candidate manifests
- governance/Q4, market data, budget, registry DB outside the single writer
- secrets, broker, live and order surfaces

## Acceptance tests

- exact frozen alpha grid and replication design;
- deterministic outcomes;
- only candidate index zero can admit;
- Wilson bounds and selection order are correct;
- no policy is selected unless every constraint passes;
- no market/Q4 access, spend, historical mutation or execution.

## Rollback conditions

Rollback on result-dependent grid expansion, historical reclassification,
nondeterminism, constraint bypass, Q4/market access or order capability.

## Expected decision information value

`0.995`: this is the narrowest prospective repair that preserves the useful power
identified by v2 while enforcing a shadow-grade false-positive ceiling.
