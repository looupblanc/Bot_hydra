# HYDRA V7.1 — Multiple-testing power audit

[HYDRA-V7] phase=4 step=110 verdict=RED
gate=V71_POWER preuve=reports/v7_1/calibration/v71_power_audit_result.json#8c21d981 tests=10240_controles
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=258132 burned=1
diff_validation=hydra/validation/v71_hierarchical_multiplicity.py,hydra/calibration/v71_power_audit.py CONTRE=des_controles_calibres_ne_prouvent_aucun_edge_D1
prochaine_action=stop_real_candidate_validation_and_report_power_failure

- FPR maximal: `0.0`
- Puissance minimale effets significatifs: `0.4625`
- SYNTHETIC_GAUSSIAN: FPR `0.0`, power `0.4625`, MDE `75.0`
- SEMI_SYNTHETIC_D1_ES_RESIDUAL_BOOTSTRAP: FPR `0.0`, power `0.684375`, MDE `75.0`

## CONTRE

The controls calibrate multiplicity power under frozen synthetic and empirical-residual worlds; they cannot prove that any D1 mechanism has positive expectancy.
