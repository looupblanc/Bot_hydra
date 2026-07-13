# HYDRA V7.2 — G11 integrated regression

[HYDRA-V7] phase=4 step=202 verdict=GREEN
gate=V72_G11_INTEGRATED_REGRESSION preuve=reports/v7_2/checkpoints/v72_g11_integrated_regression_evidence_step_0202.json#9647948e tests=780/780_full_55/55_ruleset
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265227 burned=1
diff_validation=hydra/validation/v72_trade_arrival_renewal_funnel.py,hydra/validation/v72_trade_arrival_renewal_tripwire.py,tests/test_v72_trade_arrival_renewal_funnel.py,tests/test_v72_trade_arrival_renewal_tripwire.py CONTRE=regression_verte_ne_prouve_aucun_edge_et_G11_reste_GEOMETRY_ONLY
prochaine_action=fusion_fast_forward_snapshot_redemarrage_du_meme_service

- Régression complète : `780 passed in 897.25s`
- Ruleset, no-lookahead, Q4 et budget : `55 passed`
- Tests ciblés G11 et contrôleur : `16 passed`
- Compileall : `ok`
- Mission DB, registry, copie mission-registry et cimetière : `ok`
- Gouvernance : `GREEN`
- Q4 : une transaction historique `COMMITTED`, aucun nouvel accès G11
- Replay de référence G11 : hashes funnel et tripwire identiques
- Achat de données G11 : `0`
- Ordres broker : `0`
- Secret ou artefact runtime committé : `0`

## CONTRE

Une régression logicielle verte prouve l’application déterministe des règles et la sûreté de l’intégration, pas un edge économique. G11 reste irréversiblement classé `GEOMETRY_ONLY` pour cette grammaire figée.
