# HYDRA V7.1 — Intraminute-flow power-aware audit

[HYDRA-V7] phase=4 step=175 verdict=GREEN
gate=V71_G8_POWER_AUDIT preuve=reports/v7_1/discovery_0008/v71_intraminute_flow_power_audit_result.json#170f56a0 tests=2_frozen_candidates
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263882 burned=1
diff_validation=hydra/validation/v71_intraminute_flow_power_audit.py CONTRE=biais_de_selection_D1_et_tripwire_vert_mince
prochaine_action=classify_fragile_or_underpowered_candidates_without_promotion_and_review_next_distinct_hypothesis

- Statuts: `{"WF_POSITIVE_BUT_FRAGILE": 2}`
- Powered: `0`
- Sous-puissants: `0`
- Fragiles: `2`

## CONTRE

Both candidates were selected after tiny positive pooled D1 walk-forward results with negative early folds and a VERT_MINCE tripwire; this audit is selected development evidence rather than confirmation.
