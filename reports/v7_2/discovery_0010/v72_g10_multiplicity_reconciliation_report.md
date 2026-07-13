# HYDRA V7.2 — Réconciliation de multiplicité G10

[HYDRA-V7] phase=4 step=192 verdict=GREEN
gate=V72_G10_MULTIPLICITY_RECONCILIATION preuve=mission/state/proof_registry.json#d1bd1c3b tests=registry_chain_verified
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265091 burned=1
diff_validation=aucun CONTRE=le_compteur_a_ete_corrige_apres_tripwire_et_non_avant
prochaine_action=freeze_and_reserve_G10_candidate_power_audit_before_any_power_result

- Sous-comptage détecté : `144` évaluations candidat-monde.
- Compteur avant/après : `264947 → 265091`.
- Seuils, nulls et signal paths : inchangés et déjà WORM avant le tripwire.
- Verdict du tripwire : non re-jugé ; il ne dépendait pas de `N_trials`.

## CONTRE

Because the increment was not recorded before execution, this entry repairs audit completeness but cannot honestly be described as a preregistration; any later candidate inference must use the corrected campaign count.
