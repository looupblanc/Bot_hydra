# HYDRA V7 — Economic Evolution deployment readiness

```text
[HYDRA-V7] phase=4 step=pre_deploy verdict=GREEN
gate=ECONOMIC_EVOLUTION_DEPLOYMENT_READINESS preuve=config/v7/economic_evolution_persistent_0002.json#12b1a797 tests=833/833
budget_llm=usage_api_non_exposee/solde_P4 budget_data=87.84738838672598/125 N_trials=318954 burned=1
diff_validation=hydra/validation/v72_executed_price_occupancy_funnel.py,hydra/validation/v72_executed_price_occupancy_tripwire.py,hydra/economic_evolution/account_evaluation.py,hydra/economic_evolution/null_calibration.py,hydra/mission/economic_evolution_runtime.py CONTRE=Le pilote a produit 22 micro-edges utiles mais aucun passage Combine; la campagne persistante reste une validation scientifique à exécuter, pas une preuve d'edge.
prochaine_action=Fusionner puis relancer le service unique; réserver atomiquement 51600 essais avant tout résultat et exécuter la campagne persistante 0002.
```

## Périmètre figé

- Configuration WORM : `hydra_economic_evolution_persistent_0002`.
- SHA-256 de configuration : `12b1a797bcda92fcc8943cb6975a07022a07e86990545e78c2ad72983441a863`.
- Tag WORM : `worm/economic-evolution-persistent-0002-2026-07-13`.
- Commit d'implémentation figé par la configuration : `b75612392ea60a5490414cf7dc5a2087dbc7d3e7`.
- Réservation prospective : 51 600 essais, écrite par l'unique contrôleur avant le premier artefact de résultat.
- Données : caches existants en lecture seule; aucun achat et aucun accès Q4.
- Exécution : aucun broker, aucun ordre, aucun chemin d'ordre.

## Contrôles achevés

- Régression complète : 833 tests passés en 1 303,10 s.
- Contrôles ciblés no-lookahead, budget, gouvernance et Q4 : 32 passés.
- Contrôles ciblés Economic Evolution et reproductibilité : 35 passés.
- Contrôles de multiplicité et WORM supplémentaires : 5 passés.
- `compileall hydra scripts tests` : vert.
- Mission DB : `quick_check=ok`, `integrity_check=ok`.
- Registre stratégies : `quick_check=ok`, `integrity_check=ok`.
- Registre de preuve : 46 entrées, chaîne valide, tête `5cc617ad`, Q4 irréversiblement `BURNED`.
- Gouvernance : verte; hash sémantique `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b`.
- YAML gouvernance : `3c9fd63f43037c65d79ecd688ce76bc126c42cc6eeaceb6bed8636548ffaff57`.
- Q4 : un accès historique, transaction atomique `COMMITTED`, aucun nouvel accès autorisé.
- Databento : dépense 87,84738838672598 USD; reste 37,15261161327402 USD.
- Secret scan : aucun secret; seules les valeurs factices documentées de `.env.example` et du message d'aide sont détectées.
- Diff : `git diff --check` vert.

## Séparation des changements

Les changements de validation/simulation et les changements de génération ont été introduits dans des commits distincts. La campagne persistante ne modifie aucun seuil après observation : elle réutilise les gates WORM du pilote et calibre le validateur avant les résultats.

## État avant déploiement

Le service existant reste actif sur le contrôleur `hydra_v7_2_arrival_renewal_controller_v3`. Il n'exécute pas encore la campagne Economic Evolution. Le déploiement doit conserver le même mission ID, le même service systemd, le même verrou et l'unique writer.
