# HYDRA V7.1 — Event-time outcome-free manifest

[HYDRA-V7] phase=4 step=140 verdict=GREEN
gate=V71_G3_SIGNAL_FREEZE preuve=reports/v7_1/discovery_0003/v71_event_time_signal_manifest.json#e515a0ab tests=outcome_free_generation
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262868 burned=1
diff_validation=aucun CONTRE=la_vitesse_de_completion_peut_seulement_mesurer_la_liquidite_de_session
prochaine_action=freezer_le_manifest_puis_appliquer_stage0_stage2_inchange

- Candidats: `128`
- Signaux: `103828`
- Prévision >=320 signaux: `88`
- Barres cross-date exclues: `122`

## CONTRE

Event completion speed can be a session-liquidity clock rather than directional edge; economic replay and a new-grammar tripwire remain mandatory.
