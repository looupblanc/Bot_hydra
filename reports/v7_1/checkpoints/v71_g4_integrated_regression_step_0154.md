# HYDRA V7.1 — Bounded grammar 0004 integrated regression

[HYDRA-V7] phase=4 step=154 verdict=GREEN
gate=V71_G4_INTEGRATED_REGRESSION preuve=reports/v7_1/discovery_0004/v71_cross_clock_flow_power_audit_result.json#204b79bc tests=712/712_full+95/95_focused
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials=263672 burned=1
diff_validation=hydra/validation/v71_cross_clock_flow_funnel.py,hydra/validation/v71_cross_clock_flow_tripwire.py,hydra/validation/v71_cross_clock_flow_power_audit.py CONTRE=la_meilleure_formulation_reste_post_selection_et_sous_puissante
prochaine_action=deploy_same_persistent_service_with_controller_v5_then_verify_one_writer

## Verdict scientifique

- Grammaire bornée : `12` structures, `4 514` signaux, `12` valides et nouvelles.
- Stage 1 : `2` survivants ; walk-forward positif : `2`.
- Tripwire : `GREEN_NULL_ADJUSTED_BASELINE`, force `VERT_NET`.
- Comptes bruts du tripwire : réel `33/240`, null `63/720`.
- `NULL_RATIO` : `0.6363636363636362` ; p-value exacte unilatérale : `0.006598661291902388`.
- Audit de puissance : `0 POWERED_WF_POSITIVE`, `1 PROMISING_UNDERPOWERED`, `1 WF_POSITIVE_BUT_FRAGILE`.
- Meilleur candidat 60 minutes : `161` événements bruts, `127.6128` effectifs, moyenne stress ×1,5 `+73.1992 USD/trade`, CI95 `[+13.6271, +127.0138]`, probabilité positive `0.98955`, puissance `0.3381` contre seuil gelé `0.8`.
- Promotions Rolling Combine : `0` ; shadow : `0` ; données achetées : `0` ; ordres : `0`.

## Régression et intégrité

- Full pytest : `712 passed in 718.63s`.
- Suite ciblée no-lookahead/ruleset/Q4/budget/shadow : `95 passed in 9.73s`.
- Compileall `hydra scripts tests` : vert.
- Mission DB, graveyard, hydra registry, hydra DB et strategy registry : `PRAGMA integrity_check = ok`.
- Registre de preuve : `22` entrées, head `cedeb016`, Q4 seul BURNED, `N_trials=263672`.
- Gouvernance : verte ; hash sémantique `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b`.
- YAML gouvernance : `3c9fd63f43037c65d79ecd688ce76bc126c42cc6eeaceb6bed8636548ffaff57`.
- Q4 : une transaction atomique historique, status `COMMITTED`, aucun nouvel accès.
- Secret scan du diff : aucun motif de credential trouvé.
- Replay déterministe : manifeste régénéré SHA `35393b8b`, identique au manifeste WORM.
- Snapshot : `mission/state/snapshots/v71_g4_pre_deploy_20260713T003457Z`.

## CONTRE

Le tripwire vert montre seulement que la géométrie du compte explique moins de passages que le monde réel sur ces deux blocs D1. Le candidat 60 minutes reste sélectionné sur ces mêmes données et sa puissance calibrée n'est que de 33,81 % ; il ne peut donc être ni promu, ni envoyé au shadow, ni utilisé comme preuve d'edge.
