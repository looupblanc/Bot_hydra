# HYDRA V7.2 — G10 integrated regression checkpoint

[HYDRA-V7] phase=4 step=196 verdict=GREEN
gate=V72_G10_INTEGRATED_REGRESSION preuve=reports/v7_2/checkpoints/v72_g10_integrated_regression_evidence_step_0196.json#ff7624b7 tests=770/770
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265107 burned=1
diff_validation=hydra/validation/v72_flow_impact_relaxation_funnel.py,hydra/validation/v72_flow_impact_relaxation_tripwire.py,hydra/validation/v72_flow_impact_relaxation_power_audit.py CONTRE=tripwire_de_classe_vert_mais_aucun_candidat_ne_passe_la_puissance_gelee
prochaine_action=merge_push_restart_same_service_with_g10_controller_v2

- Régression complète : `770/770`.
- Tests ciblés no-lookahead/ruleset/Q4/budget/déterminisme : `55/55`.
- Replay déterministe G10 : `8/8`; compileall vert.
- Mission DB / registre / cimetière : `ok / ok / ok`.
- Gouvernance : verte, hash sémantique `05810bc1`; Q4 reste `1`, fermé et BURNED.
- G10 : `36` structures valides, `6` Stage-1, `5` walk-forward positives, `4` résistantes aux coûts ×2.
- Tripwire : réel `56/720`, null `109/2160`, `NULL_RATIO=0,648810`, p binomiale `0,00109843`, `VERT_NET`.
- Audit candidat : `2` sous-puissantes non fragiles, `2` fragiles, `0` powered.
- Nulls candidats / DSR-BH / Rolling Combine / shadow : `0 / 0 / 0 / 0`, conformément aux gates gelés.
- Achat data / nouvel accès Q4 / ordre broker : `0 / 0 / 0`.
- La correction de tests isole les scénarios historiques de l'état runtime ; aucun code de gouvernance de production ni seuil scientifique n'a changé.

## CONTRE

Le tripwire distingue la classe G10 des mondes contrefactuels, mais les candidats ne couvrent que deux années et huit blocs hebdomadaires. Aucun ne satisfait le gate de puissance à 80 % : ce checkpoint autorise le déploiement du contrôleur de recherche, pas une promotion scientifique.
