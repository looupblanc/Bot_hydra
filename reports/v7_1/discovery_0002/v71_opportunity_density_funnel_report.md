# HYDRA V7.1 — Opportunity-density Stage 0–2

[HYDRA-V7] phase=4 step=131 verdict=GREEN
gate=V71_G2_STAGE0_STAGE2 preuve=reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json#2a45c4da tests=128_structures
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262356 burned=1
diff_validation=hydra/validation/v71_opportunity_density_funnel.py CONTRE=la_couverture_structurelle_peut_seulement_ajouter_des_trades_faibles
prochaine_action=classify_density_grammar_and_select_new_mechanism

- Stage 0 valides/novel: `120`
- Stage 1: `4`
- Walk-forward positifs: `3`
- Walk-forward positifs et >=320 événements: `0`

## CONTRE

Structural unions can raise opportunity count by adding weak trades; only powered positive walk-forward candidates may reach the grammar tripwire and relevant nulls.
