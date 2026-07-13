# HYDRA Economic Evolution — archive de semences du pilote 0001

```text
[HYDRA-V7] phase=4 step=pilot-0001-seed-archive verdict=GREEN
gate=DEVELOPMENT_PROVENANCE preuve=reports/economic_evolution/pilot_0001/seed_archive.json#ca4b09a2 tests=4/4
budget_llm=usage_API_non_exposee/solde budget_data=87.847388386726/125.00 N_trials=318954 burned=1
diff_validation=hydra/economic_evolution/seed_archive.py,scripts/build_economic_evolution_seed_archive.py,tests/test_economic_evolution_seed_archive.py CONTRE=les_semences_sont_selectionnees_sur_le_developpement_et_ne_constituent_aucune_preuve_independante
prochaine_action=preregistrer_la_premiere_campagne_persistante_de_mutation_et_assemblage
```

L'archive compacte contient 60 composants, dont 22 micro-edges utiles au sens
incrémental du pilote, 20 politiques diagnostiques et 8 mutations améliorantes.
Les statuts bruts erronés sont réconciliés en
`ACCOUNT_POLICY_DIAGNOSTIC_ONLY`. Aucun chemin Combine n'est revendiqué.

## CONTRE

Cette archive transmet une sélection issue du développement. Elle sert à éviter
les relectures des gros fichiers temporaires et à diriger la recherche, jamais à
valider ou promouvoir un candidat.
