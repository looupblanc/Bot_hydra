# HYDRA V7 — D1 candidate tribunal

[HYDRA-V7] phase=D step=5 verdict=NULL
gate=D1_CANDIDATES preuve=reports/v7/data/d1_candidate_tribunal_result.json#fcdb9477 tests=659/659
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials=247684 burned=1
diff_validation=hydra/validation/v7_d1_candidate_tribunal.py,scripts/run_v7_d1_candidate_tribunal.py,tests/test_v7_d1_candidate_tribunal.py CONTRE=deux_blocs_aout_limitent_la_puissance_DSR
prochaine_action=tombstone_D1_classes_and_report_current_data_scope_null

- Structures : `8`
- Signaux : `9095`
- Stage 1 : `1`
- Stage 2 : `0`
- Null suite : `0`
- DSR positifs : `0`
- BH : `0`
- SIM_EXPLOIT : `1`
- File shadow : `0`

## CONTRE

Only two August blocks provide daily observations, so DSR has low power under 247684 historical trials. A survivor would still need a WORM fiche before any untouched gap ingestion.
