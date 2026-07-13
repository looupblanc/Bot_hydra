# HYDRA V7.2 — Static basket search freeze

[HYDRA-V7] phase=4 step=184 verdict=GREEN
gate=V72_BASKET_SEARCH_FREEZE preuve=reports/v7_2/crossfit_0001/v72_basket_search_freeze_result.json#44d98926 tests=1009_structures_frozen_before_results
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263902 burned=1
diff_validation=hydra/account_policy/basket.py,hydra/account_policy/v72_static_basket.py,hydra/validation/v72_basket_search_freeze.py CONTRE=quatre_blocs_independants_courts
prochaine_action=commit_manifest_then_append_multiplicity_reservation_then_evaluate_design_blocks

- Composants primaires: `11`
- Structures panier/allocation: `1009`
- UNIT_EQUAL: `550`
- TARGET_VELOCITY_TILT: `459`
- N_trials effectif campagne: `1513.5`
- Résultats panier lus: `0`
- Achats data/Q4/ordres: `0/0/0`

## CONTRE

The search contains 1,009 preregistered basket-allocation structures but only four short independent blocks; cross-fitting controls direct selection leakage, not the low precision of account-level estimates.
