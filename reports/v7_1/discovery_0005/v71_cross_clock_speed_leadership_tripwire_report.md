# HYDRA V7.1 — Speed-leadership grammar tripwire

[HYDRA-V7] phase=4 step=157 verdict=ARTEFACT_GEOMETRY_ONLY
gate=V71_G5_TRIPWIRE preuve=reports/v7_1/discovery_0005/v71_cross_clock_speed_leadership_tripwire_result.json#ea7755aa tests=12_real_plus_36_null_paths
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263732 burned=1
diff_validation=hydra/validation/v71_cross_clock_speed_leadership_tripwire.py CONTRE=deux_blocs_D1_et_durees_preservees
prochaine_action=freeze_speed_leadership_grammar_as_geometry_contaminated

- Réel: `13/240`
- Null: `45/720`
- NULL_RATIO: `1.1538461538461537`
- P-value exacte unilatérale: `0.7404294667980698`
- Force: `ARTEFACT`

## CONTRE

Event durations and flow are preserved in the price nulls and only two D1 blocks exist; even a green result cannot establish forward persistence.
