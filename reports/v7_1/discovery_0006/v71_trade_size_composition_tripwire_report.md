# HYDRA V7.1 — Trade-size composition permanent tripwire

[HYDRA-V7] phase=4 step=162 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=V71_G6_TRIPWIRE preuve=reports/v7_1/discovery_0006/v71_trade_size_composition_tripwire_result.json#c3a0a531 tests=real_vs_3_null_worlds
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263762 burned=1
diff_validation=hydra/validation/v71_trade_size_composition_tripwire.py CONTRE=deux_blocs_calendaires_seulement
prochaine_action=audit_power_of_trade_size_walk_forward_positive_candidates

- Réel: `22/120`
- Null: `20/360`
- NULL_RATIO: `0.303030`
- Force: `VERT_NET`

## CONTRE

Only two D1 calendar blocks exist and trade composition is held fixed in price nulls; even a green result remains development evidence requiring power audit and fresh confirmation.
