# HYDRA V7 — Checkpoint frontière Phase 2

[HYDRA-V7] phase=2 step=25 verdict=NULL
gate=P2 preuve=reports/v7/phase2/phase2_result.json#586a9957 tests=581/581
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=246706 burned=1
diff_validation=hydra/governance/proof_registry.py,hydra/data/v7_manifest.py,hydra/validation/v7_phase2_multiplicity.py,scripts/run_v7_phase2.py,tests/ruleset/test_proof_registry.py,tests/ruleset/test_v7_data_manifest.py,tests/ruleset/test_v7_phase2_multiplicity.py CONTRE=le_compteur_historique_est_une_reconstruction_conservatrice_et_non_un_recensement_atomique
prochaine_action=preregister_hypotheses_V7_and_estimate_D1_without_ingesting_forward_gap

## Justification contractuelle

La clause 1 impose d'accepter le null des 55 paniers comme livrable ; les
clauses 2, 3 et 5 interdisent d'abaisser BH/DSR, d'optimiser le pass-rate ou de
consommer le gap sans fiche WORM. La Phase 4 peut maintenant commencer puisque
G0 et G1 sont verts et que le verdict P1 n'est pas ARTEFACT.

## Auto-audit

Le moyen le plus probable de se tromper maintenant serait de transformer le
besoin de remplir les slots shadow en prétexte pour écrire des hypothèses après
avoir observé leurs résultats ou pour ingérer le gap avant la fiche candidat.

## CONTRE

Le meilleur panier conserve une espérance élevée sous coûts ×2 et échoue BH
uniquement après une correction de multiplicité très sévère. Le null est donc
solide au regard du contrat WORM, mais sensible à l'exactitude de la
rétro-estimation de `N_trials`; aucune réinterprétation post hoc n'est permise.

