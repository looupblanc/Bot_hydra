[HYDRA-V7] phase=4 step=0007 verdict=ARTEFACT
gate=DENSITY_DIVERSIFICATION_0007 preuve=reports/economic_evolution/density_diversification_0007/density_diversification_result.json#bd64d7e tests=888/888
budget_llm=usage_API_non_exposee/solde budget_data=87.84738838672598/125_USD N_trials=454503 burned=1
diff_validation=hydra/mission/economic_evolution_failure_runtime.py,tests/test_economic_evolution_failure_runtime.py CONTRE=Le_correctif_de_reprise_ne_prouve_pas_que_la_classe_0007_possede_un_edge;_son_verdict_reste_ARTEFACT_GEOMETRY_ONLY.
prochaine_action=REDÉMARRER_LE_MÊME_SERVICE_ET_PERSISTER_LE_VERDICT_SANS_REJOUER_0007

# Incident de persistance après la campagne 0007

## Faits

- Déploiement initial V10 : commit `38abea4694b3909ef81e77c38eb06739abd033fe`.
- Réservation prospective : `452628 → 454503`, une seule entrée, avant tout artefact 0007.
- Résultat scientifique immuable : `bd64d7e4e70596a5fca62502f394559b597c231504053ef504d3b69b7a28bbc7`.
- Tripwire : réel `5/22`, null apparié `6/22`, `NULL_RATIO=1.2`, p binomiale unilatérale `0.7574336943091392`.
- Verdict : `ARTEFACT_GEOMETRY_ONLY`; politiques compte évaluées `0`; promotions `0`.
- Données/proof/Q4/ordres : `0/0/0/0` delta.

## Incident

Après production atomique du résultat, le contrôleur a reconstruit le review
rétrospectif 0006. Son ancien garde-fou exigeait que le compteur global reste
éternellement égal à `452628`, bien que la campagne aval 0007 ait légitimement
réservé `1875` essais. Le contrôleur a échoué fermé avant de persister le verdict
0007 dans la DB mission. Le worker scientifique n'a pas été rejoué.

## Correction bornée

Le commit `6611226` distingue désormais deux états :

- avant résultat 0006, le compteur doit être exactement `452628` ;
- après vérification cryptographique du résultat 0006, les réservations aval
  append-only sont autorisées, mais toute réservation attribuée au review 0006
  lui-même reste interdite.

Aucun seuil, candidat, population, coût, null, résultat ou statut scientifique
n'a été modifié. Le correctif est uniquement un invariant de reprise monotone.

## Vérifications

- régression complète : `888/888` ;
- no-lookahead et ruleset : `41/41` ;
- SQLite : `7/7 integrity_check=ok` ;
- gouvernance : verte, hash sémantique
  `05810bc193e51e3c40722163a1ee3ae82fd3a8d7762c48eafa25c3a4cba1102b` ;
- YAML : `3c9fd63f43037c65d79ecd688ce76bc126c42cc6eeaceb6bed8636548ffaff57` ;
- Q4 : accès `1`, fenêtre `Q4_2024` toujours `BURNED` ;
- budget : dépense `87.84738838672598`, solde `37.15261161327402` USD ;
- secret scan : vert ; broker/ordres : `0/0`.

## CONTRE

La reprise technique correcte ne transforme pas un résultat développement en
preuve. Le null apparié surpasse le réel et la classe exacte 0007 doit rester
tombstonée ou faire l'objet d'une nouvelle représentation avec de nouveaux IDs,
jamais d'un sauvetage paramétrique.
