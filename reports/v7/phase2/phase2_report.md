# HYDRA V7 — Phase 2 multiplicité et déduplication

[HYDRA-V7] phase=2 step=1 verdict=NULL
gate=P2 preuve=reports/v7/phase2/phase2_result.json#586a9957 tests=581/581
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=246706 burned=1
diff_validation=hydra/validation/v7_phase2_multiplicity.py,scripts/run_v7_phase2.py,tests/ruleset/test_v7_phase2_multiplicity.py CONTRE=les_55_paniers_sont_selectionnes_sur_developpement_et_le_recensement_historique_est_reconstruit
prochaine_action=allocate_all_shadow_slots_to_track_B_after_hypothesis_preregistration

## Résultat

- Candidats : `55`
- Familles comportementales : `19`
- DSR z > 0 : `1`
- Rejets BH FDR 10 % : `0`
- SIM_EXPLOIT : `0`
- Éligibles : `0`
- Représentants retenus : `0`

Le replay de référence a été exécuté deux fois avec un résultat byte-identique
(`sha256 586a9957fbe5a1c35050d9c52b8bba01f2e00ec93100b471326748624badac9e`).
Q4 n'a pas été relu, le gap forward n'a pas été ingéré, et aucun ordre n'a été
émis.

## Meilleur dossier hérité

- Politique : `basket_v6_057b7f015e7d1624b2`.
- Cluster : `P2_CLUSTER_002`.
- DSR : `z=0.100054`, probabilité `0.539849`.
- Test BH : `p=0.460151`, seuil au rang 1 `0.001818`, donc non rejeté.
- Espérance par trade : `142.60 $` aux coûts de base, `130.28 $` en
  walk-forward purgé aux coûts ×1,5, et `117.07 $` aux coûts ×2.
- Événements walk-forward retenus : `495`.
- Seul gate échoué : `BH_FDR_10pct_rejected`.

Ce résultat ne dit pas que l'économie brute est négative. Il dit qu'après
avoir compté la recherche massive qui a sélectionné ces paniers, aucun des 55
ne fournit une preuve statistique promotionnelle honnête.

## Représentants

- Aucun : verdict null propre pour la piste héritée.

## CONTRE

Les 55 paniers ont été sélectionnés sur le développement hérité et le
recensement des tests historiques est reconstruit plutôt qu'atomiquement exact.
Le facteur conservateur peut être trop sévère ou encore sous-compter des essais ;
seule une hypothèse nouvelle, pré-enregistrée puis testée sur preuve fraîche,
peut trancher sans réinterpréter ces résultats.
