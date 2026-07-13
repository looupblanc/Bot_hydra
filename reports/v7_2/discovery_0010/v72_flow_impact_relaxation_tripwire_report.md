# HYDRA V7.2 — Delayed flow-impact permanent tripwire

[HYDRA-V7] phase=4 step=191 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=V72_G10_TRIPWIRE preuve=reports/v7_2/discovery_0010/v72_flow_impact_relaxation_tripwire_result.json#1e6f26a2 tests=real_vs_3_null_worlds
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=264947 burned=1
diff_validation=hydra/validation/v72_flow_impact_relaxation_tripwire.py,tests/test_v72_flow_impact_relaxation_tripwire.py CONTRE=deux_blocs_malgre_un_denominator_plus_large
prochaine_action=audit_power_and_relevant_nulls_for_2x_surviving_walk_forward_candidates

- Réel: `56/720`
- Null: `109/2160`
- NULL_RATIO: `0.648810`
- Force: `VERT_NET`

## CONTRE

The larger episode denominator improves the geometry test but still derives from only two calendar-year blocks; a green tripwire cannot establish an edge.
