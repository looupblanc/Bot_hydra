# HYDRA Engineering Task — Selection-Adjusted Null Power Calibration

## Objective

Calibrate the exact candidate-level five-event block sign-flip plus BH/FDR policy
used by the 20-elite quality-diversity tournaments. Determine whether the current
absence of shadow admission reflects weak mechanisms or inadequate power at
economically meaningful effect sizes.

## Frozen design

- Experiment ID: `selection_null_power_calibration_v1`
- Search family size: 20 frozen validation elites.
- Event counts: 80, 120 and 360.
- Block size: 5.
- Synthetic tournament replications: 200 per condition.
- Candidate null draws: 1,024 per candidate.
- Negative controls: all 20 candidates have zero gross directional effect before
  explicit costs.
- Positive controls: one preregistered candidate per tournament receives a
  standardized net effect of 0.25 or 0.40; the other 19 remain null.
- Cost ratio: 0.05 residual standard deviations per event.
- Admission evidence threshold: BH-adjusted probability at most 0.20, positive
  net economics and negative sign-flipped net.
- Random seed: 773401.

The calibration must report family false-positive behavior, per-candidate false
positive behavior, power by sample size/effect, sensitivity to event frequency,
and Monte-Carlo uncertainty. Useful power requires at least 0.80 for effect 0.40
at 120 events and above. Family false admission must remain at most 0.05 after
the complete economics/null policy.

If power is insufficient, do not relax the already observed candidate results.
Recommend a new, single-candidate preregistered confirmation design or a more
powerful calibrated statistic with fresh candidate IDs. Historical exact versions
retain their recorded status.

## Allowed paths

- `hydra/calibration/selection_null_power.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_selection_null_power.py`
- immutable calibration reports

## Protected paths

- governance kernel and protected manifests
- prior candidate results, p-values and statuses
- Q4/later market data, registry/mission DB outside the single writer
- budget, secrets, broker and order code

## Acceptance tests

- deterministic simulation;
- exact family size and block structure;
- BH implementation agrees with known examples;
- null and injected controls remain separated;
- no market data, Q4, network, spend or execution;
- result cannot promote a strategy or rewrite prior evidence.

## Rollback conditions

Rollback on nondeterminism, result-dependent thresholds, prior-status mutation,
market-data access, Q4 access or an uncalibrated recommendation to weaken gates.

## Expected decision information value

`0.96`: all new candidates currently fail primarily at adjusted null evidence;
this test determines whether to keep generating mechanisms or change the
confirmation design, at negligible data and compute cost.
