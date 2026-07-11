# HYDRA Engineering Task — Selection Null Policy Repair v2

## Objective

Repair the prospective promotion-null policy after calibrated controls showed
that BH at q=0.20 produces roughly 0.20 family-level false admission even though
per-candidate false admission is low. Preserve useful power without rewriting any
observed candidate result.

## Frozen policy tournament

- Experiment ID: `selection_null_policy_repair_v2`
- Synthetic design, costs, block dependence, event counts, effects, replications
  and seed family inherit calibration v1.
- Compare prospectively:
  1. current BH q=0.20 (diagnostic baseline only);
  2. BH q=0.05 across 20 elites;
  3. Holm family-wise alpha=0.05 across 20 elites;
  4. one primary candidate selected and frozen on the earlier development fold,
     tested at alpha=0.05 on the later fold; 19 elites are diagnostics only;
  5. one frozen primary per five mechanism families, Bonferroni alpha=0.01 each.

Every policy also requires positive net economics and negative sign-flipped net.
Policy selection is lexicographic:

1. family false-admission rate at most 0.05 at every tested event count;
2. power at least 0.80 for standardized net effect 0.40 with 120 events;
3. higher power for effect 0.25;
4. more independently preregistered promotable families;
5. simpler policy.

If only the single-primary policy satisfies calibration, future tournaments must
freeze one promotion-primary candidate before the validation fold; all other
elites remain quality-diversity diagnostics and cannot promote from that fold.
They may receive new IDs and an independent later confirmation.

## Scope boundary

- No historical p-value, status or evidence is changed.
- No candidate is promoted by this engineering experiment.
- Q4, market data, paid data, network, broker and orders are prohibited.
- The repaired policy applies only to manifests created after this result.

## Allowed paths

- `hydra/calibration/selection_null_policy_repair.py`
- `hydra/mission/controller.py`
- `hydra/mission/experiment_runner.py`
- `tests/test_selection_null_policy_repair.py`
- immutable calibration artifacts

## Acceptance tests

- all five policies run on identical simulated tournaments;
- Holm implementation is correct and monotonic;
- selection follows the frozen lexicographic rule;
- negative and positive controls remain separate;
- results are deterministic;
- no prior result/status mutation, market/Q4 read, spend or order surface.

## Rollback conditions

Rollback on result-dependent threshold changes, policy-selection drift,
nondeterminism, historical mutation, Q4/market access or a selected policy that
fails its preregistered false-positive/power constraints.

## Expected decision information value

`0.99`: it repairs the decisive validation bottleneck while preserving scientific
honesty and converts the 20-elite archive from an over-broad promotion family into
a high-throughput diagnostic archive plus a statistically defensible primary
promotion lane.
