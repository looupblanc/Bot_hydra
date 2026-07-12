# HYDRA V7 — Gate G1 / tripwire null

[HYDRA-V7] phase=1 step=25 verdict=GREEN
gate=G1 preuve=reports/v7/phase1/null_tripwire_result.json#ea909645 tests=571/571
budget_llm=0.000000/12.00 budget_data=0.000000/0.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/validation/v7_null_tripwire.py,scripts/run_v7_null_tripwire.py,tests/ruleset/test_v7_null_tripwire.py CONTRE=le_null_gèle_les_signaux_sélectionnés_et_la_multiplicité_des_115388_essais_reste_à_appliquer
prochaine_action=Phase_2_clustering_55_paniers_registre_de_multiplicité_DSR_et_BH_FDR_10pct

## Verdict

G1 est **VERT** selon le seuil WORM fixé avant run :

- réel : 9 144 passes / 25 680 épisodes = **35,6075 %** ;
- null poolé : 1 359 / 77 040 = **1,7640 %** ;
- `NULL_RATIO = 0,0495407`, contre seuil d'artefact `0,8`.

Les pass-rates hérités ne sont donc pas expliqués uniquement par la géométrie 9 000 $ / 4 500 $ sous ces trois contrôles. Aucun badge `GEOMETRY_ONLY` n'est attribué.

Ce verdict n'établit toujours pas un edge exploitable : la multiplicité, le DSR, le walk-forward purgé et la preuve fraîche restent à franchir.

## Contrôles

| Contrôle | Pass-rate null | NULL_RATIO | Breach MLL |
|---|---:|---:|---:|
| Block shuffle 5 jours | 1,8653 % | 0,05238 | 77,3910 % |
| Marche aléatoire vol-matched | 1,5382 % | 0,04320 | 74,0343 % |
| Années permutées | 1,8886 % | 0,05304 | 77,3910 % |

Décomposition :

- 55 paniers : réel 45,1894 %, null 0,0631 %, ratio 0,00140 ;
- 960 contrôleurs : réel 34,5095 %, null 1,9589 %, ratio 0,05676.

Les 1 015 objets réels ont été reproduits sans mismatch avant génération des nulls. Les 18 chemins synthétiques (3 contrôles × 6 produits) ont chacun un hash déterministe.

## Intégrité

- Pré-enregistrement : `WORM/phase1-null-tripwire-2026-07-12.json` # `4ca94dd3`.
- Résumé : `reports/v7/phase1/null_tripwire_result.json` # `ea909645`.
- Évidence complète compressée : `reports/v7/phase1/null_tripwire_full_evidence.json.gz` # `73bcc543`; contenu original # `cc4d3e48`.
- Tests : 571/571 ; ciblés no-lookahead/Q4/budget/null : 23/23.
- Q4 : 1 accès historique, BURNED ; gap forward : 0 accès ; ordres broker : 0 ; dépense data P1 : 0 $.

## Limite diagnostique explicitement neutralisée

Le champ d'espérance nette des nulls n'est pas retenu : lors d'un breach MLL latent, l'objet historique termine avant de matérialiser la liquidation dans son PnL terminal. Cela ne change ni le breach, ni la passe, ni le `NULL_RATIO`, mais rend cette espérance impropre à une conclusion économique.

## CONTRE

Le tripwire teste les signaux sélectionnés contre des chemins de marché contrefactuels communs ; il ne réexécute pas toute la sélection de features sur chaque raw dataset synthétique. Surtout, les 115 388 prototypes et toutes les recherches ultérieures n'ont pas encore été déflatés : un fort écart réel/null peut encore être du data-mining. P2 est donc obligatoire avant la moindre fiche candidate WORM.
