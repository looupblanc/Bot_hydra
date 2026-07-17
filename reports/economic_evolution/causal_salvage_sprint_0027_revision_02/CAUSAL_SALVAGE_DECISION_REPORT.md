# HYDRA Causal Salvage Sprint — rapport décisionnel

Émis le 2026-07-17 UTC. Source économique causale : commit `aeeb02f3ad25d6f86128c92ecd40c22331cc3576`. Les montants ci-dessous sont des agrégats diagnostiques sur 48 départs de développement potentiellement chevauchants ; ils ne constituent ni un rendement de compte unique ni 48 observations indépendantes.

## Verdict

`CAUSAL_SALVAGE_GATE_FALSIFIED`.

L'avantage Combine de `OPERATING_PACKAGE_V1` ne survit pas à la correction causale. Dix-sept sleeves sur dix-huit restent positives après coûts stressés, mais elles sont trop lentes : zéro passage normal et stressé pour les dix-huit sleeves, zéro passage pour les six anciens books, y compris au plein horizon chronologique. Aucun book n'atteint la voie A (au moins 3/48 passages normaux et 2/48 stressés) ni la voie B (médiane de progression au moins 60 % avec économie stressée positive et MLL acceptable).

V1 reste terminal sous `RETRACTED_DEVELOPMENT_EVIDENCE_CONTAMINATED`. Aucun résultat Combine, XFA, payout ou rôle historique n'est réutilisé comme preuve économique.

## Contrat causal appliqué

- Éligibilité : `forward_move_h`, sa finitude et toute disponibilité de label futur sont exclus de la décision. Une décision émise sans couverture future complète subsiste et reçoit ensuite `CENSORED_FUTURE_COVERAGE`.
- Entrée : signal, décision et soumission après disponibilité de la barre achevée `t`; premier fill admissible à l'open de la prochaine barre tradable, avec slippage adverse et coûts gelés. Le compte demeure plat jusqu'au `fill_time`.
- Sortie : même chronologie causale; aucune interpolation à travers intervalle manquant, frontière de session ou roll. Les champs `signal_time`, `decision_time`, `order_submit_time`, `earliest_executable_time`, `fill_time` et leurs équivalents de sortie sont persistés séparément.
- Batch et streaming utilisent le même `CausalSleeveStreamingKernel.step`; les dix-huit décisions se réconcilient exactement.

Le scan borné des seuls chemins atteignables classe 5 occurrences comme `OUTCOME_LABEL_ONLY`, 1 comme `KNOWN_CALENDAR_INFORMATION`, 0 comme `LOOKAHEAD_DEFECT` et 0 comme `UNRESOLVED`. Il n'existe donc plus de dépendance future bloquante sur les chemins exécutés.

## Phase A — dix-huit sleeves propres

Horizon principal : 90 jours de trading, 48 départs (12 par bloc), coûts normaux et 1,5× stressés. `TP` est la progression médiane vers la cible de 9 000 USD. `Cons.` est le taux d'épisodes satisfaisant la règle de cohérence. La colonne delta compare seulement au contrôle standalone V1 retiré; cette ancienne valeur est contaminée et n'est pas une référence valide.

| Sleeve | Net normal | Net stressé | Delta stressé vs V1 retiré | TP N / S | Pass N / S | MLL min stressé | Cons. stressée | Décision |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `0d5f4ce40cc3e8f002657965` | 1 294,57 | 1 173,45 | +361,32 | 0,30 % / 0,26 % | 0 / 0 | 4 434,33 | 22,92 % | conserver utile, trop lent |
| `1488a965163f4de09bca7cf3` | 198,24 | 117,24 | +580,55 | 0,00 % / 0,00 % | 0 / 0 | 4 332,64 | 62,50 % | conserver utile, trop lent |
| `352b3dd8e17fd32021bf837b` | 577,89 | 507,02 | −3 111,51 | 0,00 % / 0,00 % | 0 / 0 | 3 901,05 | 81,25 % | conserver utile, trop lent |
| `39eb74b174e7bdd240520b9d` | 2 039,44 | 1 948,87 | −4 966,13 | 0,57 % / 0,56 % | 0 / 0 | 4 329,47 | 45,83 % | conserver utile, trop lent |
| `3ba7422174d7e5b1c2a01d58` | 3 058,71 | 2 900,84 | +1 162,02 | 0,48 % / 0,43 % | 0 / 0 | 4 294,24 | 27,08 % | conserver utile, trop lent |
| `454819bd8caec8581b6016d2` | 400,35 | 15,98 | −1 996,27 | 0,40 % / 0,25 % | 0 / 0 | 3 753,38 | 54,17 % | conserver fragile, trop lent |
| `4de0a5d3b0ef1762cc9e4086` | 3 938,69 | 3 819,07 | +284,78 | 0,08 % / 0,05 % | 0 / 0 | 4 294,33 | 54,17 % | conserver utile, trop lent |
| `57125a2989d8eac29398af60` | 3 835,76 | 3 589,19 | +368,06 | 0,56 % / 0,52 % | 0 / 0 | 4 241,45 | 39,58 % | conserver utile, trop lent |
| `5c45e73e41fac686c67a0443` | 5 641,32 | 5 412,82 | +120,18 | 1,59 % / 1,56 % | 0 / 0 | 4 339,81 | 43,75 % | conserver utile, trop lent |
| `5c79759104e5765769be23ea` | 3 407,25 | 3 379,13 | +190,50 | 0,57 % / 0,56 % | 0 / 0 | 4 344,70 | 43,75 % | conserver utile, trop lent |
| `617236bb4c8b7cca8a472bac` | −37,36 | −181,86 | −2 550,32 | −0,02 % / −0,02 % | 0 / 0 | 4 033,87 | 66,67 % | **démotion économique** |
| `624310a1de865cef4db2f8f4` | 469,79 | 446,92 | −7 260,80 | 0,00 % / 0,00 % | 0 / 0 | 4 099,97 | 79,17 % | conserver utile, trop lent |
| `64d6fd6dd9e5f74b920ac99b` | 9 603,36 | 9 496,11 | +2 040,74 | 1,94 % / 1,94 % | 0 / 0 | 4 209,10 | 47,92 % | conserver utile, trop lent |
| `65bad2088913fc9fca0a145d` | 3 364,17 | 3 308,39 | −4 216,20 | 0,48 % / 0,47 % | 0 / 0 | 4 289,89 | 50,00 % | conserver utile, trop lent |
| `7ecd76490fa9fb34e1af5820` | 3 332,56 | 3 136,81 | +508,97 | 0,44 % / 0,40 % | 0 / 0 | 4 381,38 | 43,75 % | conserver utile, trop lent |
| `8af4e061229c8ce7ae335f63` | 12 636,11 | 12 565,42 | +212,33 | 2,57 % / 2,53 % | 0 / 0 | 4 039,97 | 25,00 % | conserver utile, trop lent |
| `c5da4b5a67abadeb7d68eabe` | 2 186,31 | 1 889,12 | −2 371,73 | 0,36 % / 0,30 % | 0 / 0 | 4 107,07 | 62,50 % | conserver utile, trop lent |
| `e017bb45b0937aef46657631` | 1 623,09 | 1 589,72 | −3 769,16 | 0,37 % / 0,37 % | 0 / 0 | 3 978,97 | 58,33 % | conserver utile, trop lent |

Résultat du gate gelé à 90 jours : 17 sleeves économiquement positives sous stress sont conservées comme briques propres, sans statut Combine; `617236…` est démotée mais pas supprimée. Cette décision n'est pas retunée avec le plein horizon : celui-ci révèle une instabilité de censure (`352b…` devient négative à −4 282,06 USD stressés, tandis que `617236…` redevient positive à +3 592,62 USD). Ces deux cas restent donc explicitement diagnostiques et aucun n'obtient de statut Combine. Aucune sleeve n'est finaliste Combine.

## Six anciens books, rejoués sans avantage de sélection

| Book | Net normal | Net stressé | TP N / S à 90 j | Pass N / S | MLL min stressé | Cons. stressée | TP stressée plein horizon | Différence V1 retirée |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `active_pool_014dffb40e99814612d78c51` | 31 127,62 | 29 573,30 | 4,31 % / 3,56 % | 0 / 0 | 2 121,18 | 27,08 % | 4,54 % | pass 17/48 → 0/48; TP 33,17 % → 3,56 % |
| `active_pool_070e391c7586ba1fac2f5494` | 37 825,38 | 36 447,94 | 4,38 % / 4,08 % | 0 / 0 | 1 928,55 | 25,00 % | 6,56 % | pass 17/48 → 0/48; TP 43,62 % → 4,08 % |
| `active_pool_14e275fa8d869c28b1f27f78` | 35 755,00 | 33 851,48 | 4,38 % / 4,08 % | 0 / 0 | 1 928,55 | 27,08 % | 5,86 % | pass 17/48 → 0/48; TP 46,15 % → 4,08 % |
| `active_pool_186a4177401aab223b0a21fa` | 38 972,51 | 37 586,89 | 4,38 % / 4,08 % | 0 / 0 | 1 869,73 | 27,08 % | 6,56 % | pass 17/48 → 0/48; TP 46,35 % → 4,08 % |
| `active_pool_2287bfb0b1c6f07930150102` | 34 131,01 | 32 924,37 | 4,38 % / 4,08 % | 0 / 0 | 1 928,55 | 29,17 % | 6,56 % | pass 17/48 → 0/48; TP 44,52 % → 4,08 % |
| `active_pool_2377af7025aadf9aaf456a7e` | 38 778,15 | 37 384,03 | 4,38 % / 4,08 % | 0 / 0 | 1 908,32 | 27,08 % | 6,56 % | pass 17/48 → 0/48; TP 44,15 % → 4,08 % |

Tous les 288 épisodes-book par scénario sont censurés à 90 jours; aucun n'atteint la cible et aucun ne franchit le MLL. Même au plein horizon chronologique, le meilleur TP médian stressé est seulement 6,56 %.

## Résultat par bloc

Médiane sur les six références-book à 90 jours; les nets sont des sommes diagnostiques de départs chevauchants et ne sont pas additionnables comme rendement indépendant.

| Bloc | Pass normaux | Pass stressés | TP médiane normale | TP médiane stressée | Net normal diagnostique | Net stressé diagnostique |
|---|---:|---:|---:|---:|---:|---:|
| B1 | 0 | 0 | 4,25 % | 4,02 % | 56 236,00 | 54 822,25 |
| B2 | 0 | 0 | 9,18 % | 9,12 % | 68 190,14 | 65 163,05 |
| B3 | 0 | 0 | 0,74 % | 0,64 % | 14 829,05 | 12 935,68 |
| B4 | 0 | 0 | 5,25 % | 5,07 % | 77 334,48 | 74 847,01 |

La correction élimine notamment l'ancienne concentration de passages en B4 : aucun bloc ne produit maintenant de passage.

## Intégrité, volume et gates

- EvidenceBundle causal scellé : 2 110 signaux, 2 092 entrées, 1 885 sorties/trades, 126 memberships, 11 520 épisodes et 294 704 états journaliers.
- 24 replays de politique exacts en Phase A : 18 standalone + 6 références-book, cinq horizons et deux scénarios. Phase B : 0 politique screenée, 0 replay exact, 0 finaliste, car le gate preregistré est fermé.
- MLL : 0 brèche sur 11 520 épisodes. Sur les résumés politique/scénario/horizon, buffer minimum global 1 845,35 USD, médiane 4 107,07 USD. L'absence de brèche ne compense pas la vitesse cible insuffisante.
- XFA/payout : 0 chemin lancé, conformément à l'interdiction avant au moins trois books Combine propres.
- Scan causal : `PASS`, 0 `LOOKAHEAD_DEFECT`, 0 `UNRESOLVED`.
- Gate technique ciblé : 18/18 batch-streaming identiques; session, roll, couverture future manquante, duplicate/restart/idempotence passent. Après les deux corrections bornées d'adaptateur, la suite ciblée combinée passe 68/68.
- L'unique régression complète a été exécutée une fois : 1 527 tests passent, 7 échecs historiques V5/V6 sont isolés au compteur Q4 global legacy, 0 échec causal; la réconciliation ciblée associée passe. Aucune deuxième régression complète n'a été lancée.
- Temps du coordinateur revision 02 : 124,13 s de replay économique sur 473,02 s, soit 26,24 %. Le scellement/validateur profond unique a consommé 344,58 s. Cette surcharge n'a pas modifié les checkpoints scientifiques.
- Sécurité : delta Q4 0, achat 0 USD, broker 0, ordre 0, XFA 0.

## Décision et action autonome

Le former edge Combine est **falsifié après correction causale**. La banque est classée `POSITIVE_BUT_INSUFFICIENT_TARGET_VELOCITY` : 17 briques positives restent récupérables, mais aucun ancien book ni Package V1 n'est sauvable.

La prochaine action autorisée est une seule campagne causale étroite de découverte `TARGET_BEFORE_ADVERSE_EXCURSION_HAZARD / OPPORTUNITY_DENSITY`, utilisant ce moteur causal, les données existantes et les 17 sleeves propres comme contrôles. Aucun manifest n'a été injecté artificiellement dans la queue : le runtime V17 ne possède pas encore un moteur enregistré qui exprime honnêtement ce mécanisme. L'extension minimale du runner partagé, puis un manifest WORM vérifié et une révision append-only de la queue, sont requis; aucun nouveau contrôleur, service, DB, Q4, achat ou XFA n'est autorisé.

Le service autoritaire `hydra-autonomous-mission.service` reste actif sous V17 et attend l'append du manifest causal distinct.
