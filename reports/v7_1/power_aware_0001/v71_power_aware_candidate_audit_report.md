# HYDRA V7.1 — Power-aware candidate audit

[HYDRA-V7] phase=4 step=144 verdict=GREEN
gate=V71_POWER_AWARE_CANDIDATES preuve=reports/v7_1/power_aware_0001/v71_power_aware_candidate_audit_result.json#f0eb2311 tests=16_frozen_candidates
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263604 burned=1
diff_validation=hydra/validation/v71_power_aware_candidate_audit.py CONTRE=le_biais_de_selection_persiste
prochaine_action=preregister_and_run_bounded_rolling_combine_for_powered_and_principal_named_diagnostics

- Statuts: `{"PROMISING_UNDERPOWERED": 13, "WF_POSITIVE_BUT_FRAGILE": 3}`
- Powered: `0`
- Rolling Combine éligibles: `0`
- Diagnostics nommés: `2`

## CONTRE

All sixteen candidates were selected for positive walk-forward expectancy before this audit; even a powered classification is post-selection research evidence, not independent proof.
