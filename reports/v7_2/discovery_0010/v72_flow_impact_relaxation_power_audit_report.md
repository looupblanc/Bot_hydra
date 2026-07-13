# HYDRA V7.2 — Delayed flow-impact power-aware audit

[HYDRA-V7] phase=4 step=193 verdict=GREEN
gate=V72_G10_POWER_AUDIT preuve=reports/v7_2/discovery_0010/v72_flow_impact_relaxation_power_audit_result.json#e141def0 tests=4_frozen_candidates
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265107 burned=1
diff_validation=hydra/validation/v72_flow_impact_relaxation_power_audit.py,tests/test_v72_flow_impact_relaxation_power_audit.py CONTRE=biais_de_selection_D1_et_deux_annees
prochaine_action=retain_nonfragile_underpowered_candidates_without_promotion

- Statuts: `{"PROMISING_UNDERPOWERED": 2, "WF_POSITIVE_BUT_FRAGILE": 2}`
- Powered: `0`
- Sous-puissants: `2`
- Fragiles: `2`

## CONTRE

The four candidates were selected on positive D1 walk-forward results and only two calendar years are available; power classification is post-selection development evidence, not fresh confirmation.
