# HYDRA V7.1 — Cross-clock flow grammar tripwire

[HYDRA-V7] phase=4 step=152 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=V71_G4_TRIPWIRE preuve=reports/v7_1/discovery_0004/v71_cross_clock_flow_tripwire_result.json#95a9ba4d tests=12_real_plus_36_null_paths
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263664 burned=1
diff_validation=hydra/validation/v71_cross_clock_flow_tripwire.py CONTRE=deux_horloges_issues_des_memes_prints
prochaine_action=audit_power_of_two_walk_forward_positive_candidates

- Réel: `33/240`
- Null: `63/720`
- NULL_RATIO: `0.6363636363636362`
- P-value exacte unilatérale: `0.006598661291902388`
- Force: `VERT_NET`

## CONTRE

Both event clocks use identical prints and only two D1 calendar blocks exist; even a green tripwire cannot establish forward persistence.
