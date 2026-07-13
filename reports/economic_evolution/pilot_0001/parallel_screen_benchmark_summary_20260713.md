# HYDRA Economic Evolution — sélection du compute plane

```text
[HYDRA-V7] phase=4 step=parallel-screen-benchmark verdict=GREEN
gate=ENGINEERING_DETERMINISM preuve=reports/economic_evolution/pilot_0001/parallel_screen_benchmark_summary_20260713.json#91021555 tests=7/7
budget_llm=usage_API_non_exposee/solde budget_data=87.847388386726/125.00 N_trials=318954 burned=1
diff_validation=hydra/economic_evolution/parallel_screen.py,scripts/benchmark_economic_evolution_parallel_screen.py,tests/test_economic_evolution_parallel_screen.py CONTRE=le_cache_est_chaud_et_un_cycle_persistant_peut_avoir_un_profil_different
prochaine_action=integrer_3_processus_plus_coordinateur_writer_unique_au_controleur_persistant
```

Les quatre configurations ont évalué les mêmes 45 056 lignes et produit le
même hash canonique `93d7e8b3`. Aucun seuil ni résultat scientifique n'a changé.

| Configuration | Écrans/s | Temps | Accélération |
|---|---:|---:|---:|
| Série | 312,39 | 111,53 s | 1,00× |
| 3 threads | 599,81 | 58,09 s | 1,92× |
| 2 processus | 490,93 | 70,99 s | 1,57× |
| 3 processus | 675,90 | 51,57 s | 2,16× |
| 4 processus | 697,84 | 49,95 s | 2,23× |

Configuration retenue : **3 processus de calcul + coordinateur/writer unique**.
Le quatrième worker ne gagne que 3,24 % et retirerait la capacité réservée au
contrôle, au heartbeat et à l'écriture autoritaire.

## CONTRE

Le cache était chaud et l'utilisation moyenne sélectionnée reste à 61,1 % de
l'hôte à cause du déséquilibre entre marchés et de la sérialisation des résultats.
La mesure persistante reste obligatoire.
