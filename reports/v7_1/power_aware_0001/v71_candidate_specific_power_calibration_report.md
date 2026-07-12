# HYDRA V7.1 — Candidate-specific power calibration

[HYDRA-V7] phase=4 step=143 verdict=GREEN
gate=V71_POWER_AWARE_CALIBRATION preuve=reports/v7_1/power_aware_0001/v71_candidate_specific_power_calibration_result.json#edd3bcdb tests=6400_control_replications
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263604 burned=1
diff_validation=hydra/calibration/v71_candidate_specific_power_calibration.py CONTRE=les_controles_ne_suppriment_pas_le_biais_de_selection
prochaine_action=run_frozen_16_candidate_power_audit

- SYNTHETIC_AR1_GAUSSIAN: FPR max `0.0`, power 50/120 `1.0`, power 25/240 `0.96`
- SEMI_SYNTHETIC_D1_DAILY_BLOCK_RESIDUAL: FPR max `0.0`, power 50/120 `0.965`, power 25/240 `0.965`

## CONTRE

Controls validate the decision rule at fixed synthetic dependence and D1 residual scale, but candidate post-selection remains a separate source of optimism addressed by shrinkage and multiplicity.
