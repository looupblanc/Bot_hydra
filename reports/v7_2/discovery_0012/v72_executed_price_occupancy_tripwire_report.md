# HYDRA V7.2 — Executed-price occupancy permanent tripwire

[HYDRA-V7] phase=4 step=208 verdict=ARTEFACT_GEOMETRY_ONLY
gate=V72_G12_TRIPWIRE preuve=reports/v7_2/discovery_0012/v72_executed_price_occupancy_tripwire_result.json#8da24185 tests=real_vs_3_reconstructed_print_null_worlds
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265347 burned=1
diff_validation=hydra/validation/v72_executed_price_occupancy_tripwire.py,tests/test_v72_executed_price_occupancy_tripwire.py CONTRE=la_geometrie_residuelle_intra_minute_est_preserve
prochaine_action=tombstone_executed_price_occupancy_class_as_geometry_only

- Réel: `2/480`
- Null: `57/1440`
- NULL_RATIO: `9.500000`
- Force: `ARTEFACT`

## CONTRE

Rank-residual null reconstruction intentionally preserves within-minute event ordering geometry, so it can be conservative; raw counts and exact-binomial evidence must be read separately from the already negative walk-forward economics.
