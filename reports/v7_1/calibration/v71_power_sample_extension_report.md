# HYDRA V7.1 — Power sample-size extension

[HYDRA-V7] phase=4 step=111 verdict=GREEN
gate=V71_POWER_SAMPLE preuve=reports/v7_1/calibration/v71_power_sample_extension_result.json#15039df4 tests=3840_controles
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=261972 burned=1
diff_validation=hydra/calibration/v71_power_sample_extension.py CONTRE=la_taille_requise_peut_depasser_la_capacite_D1
prochaine_action=freeze_power_aware_minimum_and_run_D1_stage0_stage2

- Minimum requis: `320` événements
- SYNTHETIC_GAUSSIAN: `{'null_false_positive_rate': 0.0, 'minimum_required_event_count': 320, 'power_at_required_count': 0.9625, 'passed': True}`
- SEMI_SYNTHETIC_D1_ES_RESIDUAL_BOOTSTRAP: `{'null_false_positive_rate': 0.0, 'minimum_required_event_count': 240, 'power_at_required_count': 0.875, 'passed': True}`

## CONTRE

A larger required sample can make D1 unable to decide many rare mechanisms; those cases must remain INSUFFICIENT_POWER.
