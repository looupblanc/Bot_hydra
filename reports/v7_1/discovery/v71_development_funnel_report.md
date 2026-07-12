# HYDRA V7.1 — D1 development funnel

[HYDRA-V7] phase=4 step=121 verdict=GREEN
gate=V71_STAGE0_STAGE2 preuve=reports/v7_1/discovery/v71_development_funnel_result.json#b8767eb9 tests=256_structures
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262228 burned=1
diff_validation=hydra/validation/v71_event_funnel.py CONTRE=deux_blocs_D1_ne_suffisent_pas_a_une_preuve_finale
prochaine_action=classify_underpowered_mechanisms_and_preregister_next_information_gain_action

- Stage 0 valides/novel: `246`
- Stage 1: `18`
- Walk-forward positifs: `11`
- Walk-forward positifs et >=320 événements: `0`

## CONTRE

D1 contains only two date-matched calendar blocks; positive walk-forward expectancy below 320 retained events remains underpowered and cannot support DSR/BH.
