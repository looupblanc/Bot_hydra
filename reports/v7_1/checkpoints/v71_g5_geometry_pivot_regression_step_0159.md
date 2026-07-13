# HYDRA V7.1 — Speed-leadership geometry pivot regression

[HYDRA-V7] phase=4 step=159 verdict=ARTEFACT
gate=V71_G5_TRIPWIRE preuve=reports/v7_1/discovery_0005/v71_cross_clock_speed_leadership_tripwire_result.json#ea7755aa tests=717/717_full+100/100_focused
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials=263732 burned=1
diff_validation=hydra/validation/v71_cross_clock_speed_leadership_funnel.py,hydra/validation/v71_cross_clock_speed_leadership_tripwire.py CONTRE=deux_blocs_D1_limitent_la_portee_mais_le_null_depasse_le_reel
prochaine_action=deploy_controller_v6_and_pivot_to_a_distinct_preregistered_class

## Verdict scientifique

- Grammaire limitée 0005 : `12` structures, `2 072` signaux, `12` chemins uniques.
- Représentation : `2 471` transitions de leadership de vitesse entre event clocks volume et dollar.
- Stage 1 : `3` survivants ; walk-forward positif : `2`.
- Tripwire réel : `13/240` passages (`5.4167 %`).
- Tripwire null : `45/720` passages (`6.25 %`).
- `NULL_RATIO = 1.1538461538461537`, seuil artefact inclusif `0.8`.
- Verdict : `ARTEFACT_GEOMETRY_ONLY`; badge `GEOMETRY_ONLY` sur les 12 versions.
- Audit de puissance, DSR/BH, Rolling Combine et shadow : non exécutés, car interdits après le tripwire rouge.
- Cimetière : +1 signature de classe et +12 objets, cause `GEOMETRY_ONLY_NULL_RATIO_GTE_0_8`; aucune colonne paramètre/candidat.
- Achat de données : `0` ; accès holdout : `0` ; ordre broker : `0`.

## Régression et intégrité

- Full pytest : `717 passed in 731.76s`.
- Suite ciblée ruleset/no-lookahead/Q4/budget/shadow/V7.1 : `100 passed in 28.25s`.
- Compileall : vert.
- Mission DB, graveyard, hydra registry, hydra DB et strategy registry : `ok`.
- Registre de preuve : `24` entrées, head `d9169787`, `N_trials=263732`, Q4 seul BURNED.
- Gouvernance : verte ; hash sémantique `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b`.
- Secret scan du diff : aucune correspondance.
- Replay déterministe : manifeste 0005 régénéré SHA `fdae549a`, identique au manifeste WORM.
- Snapshot : `mission/state/snapshots/v71_g5_pre_deploy_20260713T012604Z`.

## CONTRE

Les nulls conservent les durées et le flux des event bars et ne détruisent que leur relation directionnelle au prix. Ils ne testent que deux blocs calendaires D1. Néanmoins, leur taux de passage dépasse celui du réel ; poursuivre cette classe ou auditer ses deux walk-forward positifs violerait le tripwire pré-enregistré.
