# Immutable Research Task — Feasible Quality-Diversity Selector v2

Task ID: `eng_qd_selector_v2_20260711`

Frozen after the v1 2023 Stage-1 disposition counts were known and before any
2024 Q1–Q3 result for the 540-prototype population was read.

## Defect being repaired

Selector v1 returned zero elites despite 87 valid Stage-1 survivors because its
hard 35% ecology share was mathematically infeasible when metals had no
survivor. This is an allocation defect, not scientific evidence against the 87
survivors.

## Frozen v2 policy

- candidate ranking may use 2023 Stage-1 evidence only;
- any validation, 2024, Q4, holdout, future, or test-result field in selector
  input is a fatal leakage error;
- maximum promotion elites: 20;
- only Stage-1 survivors may be promotion elites;
- exact structural fingerprints and lineages remain unique;
- with index and energy survivors, initial target allocation is 15 index and 5
  energy elites (75% / 25%);
- energy receives at least 15% when enough valid energy survivors exist;
- a missing ecology receives no mandatory elite quota;
- unused ecology quota is redistributed to the best remaining valid survivor;
- family 25%, market 40%, and ecology target shares are soft feasibility caps;
- soft caps are applied in the first passes, then relaxed deterministically only
  when required to fill the maximum feasible elite set;
- every relaxation is recorded in the selection audit;
- one direct lineage may contribute at most one elite;
- ranking is deterministic and lexicographic over:
  - positive 1.5x-cost resilience;
  - net economics;
  - lower maximum drawdown;
  - lower best-event concentration;
  - event count;
  - structural fingerprint/candidate ID tie break;
- negative controls are selected separately and never count as elites;
- reserve up to two negative controls (approximately 10% of 20) from missing or
  failed ecologies, prioritizing metal failures when metals have no survivor;
- a control cannot be promoted regardless of its later result;
- manifest contains policy hash, input hash, elite IDs/fingerprints, control IDs,
  allocation audit, relaxation audit, and an explicit `uses_2024_results=false`;
- the frozen elite manifest is immutable before 2024 replay.

## Scientific and governance boundary

- the 540 structural population and Stage-1 gates remain unchanged;
- this change does not alter any signal, threshold, trade, PnL, cost, or null;
- development/falsification data end exclusive 2024-10-01;
- Q4, 2025, network, paid data, live/broker and outbound orders prohibited;
- protected governance files and state databases are protected paths;
- expected data cost: `$0`;
- expected decision information value: `0.99` because it unlocks unchanged 2024
  promotion for the 87 survivors without weakening any scientific gate.
