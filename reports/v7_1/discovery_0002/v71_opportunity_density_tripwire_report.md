# HYDRA V7.1 — Opportunity-density grammar tripwire

[HYDRA-V7] phase=4 step=132 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=V71_G2_TRIPWIRE preuve=reports/v7_1/discovery_0002/v71_opportunity_density_tripwire_result.json#dddabdad tests=128_real_plus_384_null_paths
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=262868 burned=1
diff_validation=hydra/validation/v71_opportunity_density_tripwire.py CONTRE=deux_blocs_D1_ne_prouvent_pas_la_stabilite
prochaine_action=retain_null_adjusted_baseline_and_classify_underpowered_mechanisms

- Réel: `117/2560`
- Null: `274/7680`
- NULL_RATIO: `0.7806267806267806`
- P-value exacte unilatérale: `0.004788107006395571`
- Force: `VERT_NET`

## CONTRE

Only two D1 calendar blocks are available; even a green grammar tripwire would not promote any underpowered candidate.
