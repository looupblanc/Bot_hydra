# HYDRA V7.1 — G6 et diagnostic Combine sous-puissant

[HYDRA-V7] phase=4 step=166 verdict=GREEN
gate=V71_G6_COMBINE_RESEARCH_INTEGRATION preuve=reports/v7_1/checkpoints/v71_g6_combine_research_integration_evidence_step_0166.json#e8239985 tests=725/725
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263814 burned=1
diff_validation=hydra/validation/v71_trade_size_composition_funnel.py,hydra/validation/v71_trade_size_composition_tripwire.py,hydra/validation/v71_trade_size_composition_power_audit.py,hydra/validation/v71_underpowered_combine_selection.py,hydra/validation/v71_underpowered_combine_diagnostic.py CONTRE=24_starts_ne_valent_que_4_blocs_effectifs_et_zero_pass_ne_prouve_pas_l_impossibilite
prochaine_action=merge_fast_forward_puis_restart_unique_apres_controles_autoritaires

## Verdicts scientifiques

- G6 : 6 structures, 1 524 signaux, 2 Stage 1 et 2 walk-forward positives.
- Tripwire G6 : réel `22/120`, null `20/360`, `NULL_RATIO=0,303030`, p exacte `7,49e-07`, `VERT_NET`.
- Puissance G6 : 2 `PROMISING_UNDERPOWERED`, 0 `POWERED_WF_POSITIVE`.
- Diagnostic Combine : 5 familles distinctes, 24 départs bruts, 4 blocs non chevauchants, 0 passage candidat, 0 passage panier.
- Population réconciliée : 22 walk-forward positifs = 16 sous-puissants + 4 fragiles + 2 terminaux G5 `GEOMETRY_ONLY`; aucun candidat perdu.
- Promotion scientifique, shadow et ordre broker : `0`.

## Sécurité et reproductibilité

- Régression complète : `725/725`.
- No-lookahead et ruleset ciblés : `41/41`.
- SQLite : mission, cimetière et deux registries `ok`.
- Q4 : une transaction historique `COMMITTED`, fenêtre `BURNED`, aucune réutilisation.
- Achats de données : `0`; solde réel : `37,15261161327402 $`.
- Replays déterministes : manifeste `ea883cbf`, diagnostic `6dff583d`.
- Secret scan : `0` détection.

## CONTRE

Le tripwire G6 distingue nettement le réel des nulls sur les données de développement, mais les deux candidats restent loin du seuil de puissance final à 80 %. Les 24 départs Combine se recouvrent fortement et ne représentent que quatre blocs effectifs ; les jours projetés vers la cible sont des extrapolations, jamais des passages ni une validation.
