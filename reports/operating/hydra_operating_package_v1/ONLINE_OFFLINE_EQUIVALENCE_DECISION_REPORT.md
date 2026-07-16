# OPERATING_PACKAGE_V1 — décision F0

Snapshot durable : 2026-07-16 23:33:59 UTC
Package interne : `9fd671d43127004d9ab11d82a42f07a71e9d846e559b1317c53c488dd1bc0d87`

## Décision

Statut : `ONLINE_OFFLINE_EQUIVALENCE_NOT_PROVEN_FAIL_CLOSED`.

La reconstruction des données brutes vers les features canoniques est byte-exacte et les 2 052 événements théoriques du bundle gelé sont réconciliés. Cela ne suffit pas à autoriser le mode forward : l’ancien replay filtre les décisions selon la disponibilité d’un mouvement futur, ses prix théoriques ne sont pas ceux des fills forward conservateurs et le bundle ne contient pas d’oracle d’état de compte par barre.

Le reçu d’autorisation F0 reste absent. Le service peut archiver les barres append-only, mais ne peut ni émettre un signal, ni créer un fill, ni muter un compte.

## Parité par sleeve et par livre

Le replay théorique contient 2 052 signaux. Le replay causal, avec les seuils, sessions et règles de non-chevauchement inchangés mais sans le masque de résultat futur, en produit 2 073 : 21 décisions supplémentaires et aucune décision théorique manquante.

| Sleeve | Divergences causales |
|---|---:|
| `sleeve_39eb74b174e7bdd240520b9d` | 9 |
| `sleeve_65bad2088913fc9fca0a145d` | 3 |
| `sleeve_7ecd76490fa9fb34e1af5820` | 1 |
| `sleeve_c5da4b5a67abadeb7d68eabe` | 6 |
| `sleeve_e017bb45b0937aef46657631` | 2 |
| 13 autres sleeves | 0 |

Les six livres utilisent les mêmes 18 bindings : 21 divergences par livre, 126 comparaisons répliquées et 6/6 timelines affectées. Les 21 nouveaux événements n’ont pas de sortie, PnL, MAE/MFE ou chemin MLL immuable ; leur replay de compte est donc `NOT_EVALUABLE` sans fabrication.

Quinze exclusions correspondent à des frontières connues de session/roll. Six dépendent de ruptures intraday imprévisibles : YM aux 2023-03-14 19:30, 2023-03-15 19:40, 2023-09-13 19:30 et 2024-09-18 19:20 UTC ; CL au 2023-06-16 15:52 UTC ; NQ au 2024-03-13 19:35 UTC.

## Sémantique de fill et état de compte

- 2 052/2 052 entrées théoriques utilisent le close de la ligne suivante.
- 1 993/2 052 diffèrent de l'open de la barre suivante.
- 1 988/2 052 diffèrent de l'open suivant plus un tick adverse.
- 2 052/2 052 sorties diffèrent du close d'horizon moins un tick adverse.
- Lower bound total : 4 062 écarts de contrat moteur (21 causalité + 4 040 fills + oracle per-bar absent).

Le bundle contient 2 304 oracles d'épisodes 90 jours pour les six livres et des chemins de compte quotidiens, mais aucun oracle par barre permettant de comparer exactement position, PnL latent, MLL, consistency et hash final. Aucun état manquant n'a été synthétisé.

## Gardes déterministes

Duplicate, indisponibilité, out-of-order, intervalle manquant, transition de session, roll, hash de checkpoint et reprise ciblée : `PASS`. La réconciliation restart des 18 bindings n'a pas été scellée avant l'arrêt borné du scan lourd ; elle ne peut donc pas contribuer à un statut F0 positif.

## Feed append-only et métriques forward

Diagnostic fournisseur : `DATA_AVAILABLE`.

- Dataset/schéma/symbologie : `GLBX.MDP3` / `ohlcv-1m` / `raw_symbol`.
- Contrats : `CLQ6 ESU6 M2KU6 MCLQ6 MESU6 MNQU6 MYMU6 NQU6 RTYU6 YMU6`.
- Première barre post-freeze : `2026-07-16T15:07:00Z`.
- Dernière clôture du snapshot : `2026-07-16T15:34:00Z`.
- Barres persistées : 280, soit 28 par racine ; 0 manquante, 0 rejetée, DB `ok`.
- Sessions F1 complètes : 0 ; une session partielle est archivée.
- Signaux / fills virtuels / mutations de compte : 0 / 0 / 0.
- PnL, MLL, consistency et attribution forward : non évalués, compte non muté.
- Événements historiques de chauffe : 48 `WARMUP_PENDING`, conservés immuables ; 0 nouvel événement après le verrou F0.

Aucun défaut de fenêtre, symbole, contrat, schéma ou licence n'est détecté. La source est retardée d'environ huit heures, donc aucun heartbeat économique candidat n'est publié à partir de ces données.

## Core et Backup

La hiérarchie reste inchangée.

- Core `active_pool_186a4177401aab223b0a21fa` : développement 66/192 passes normaux et 65/192 stressés ; net normal 1 049 067,495 USD, net stressé 984 142,2525 USD ; MLL breach 0 ; buffer minimum 875,49 / 873,885 USD.
- Backup `active_pool_014dffb40e99814612d78c51` : développement 64/192 passes normaux et 64/192 stressés ; net normal 923 732,69 USD, net stressé 876 943,2175 USD ; MLL breach 0 ; buffer minimum 1 123,84 / 924,09 USD.
- Forward : égalité opérationnelle à zéro, car F0 interdit toute décision. Aucun reranking ni basculement n'est effectué.

Le chemin XFA reste `CONSISTENCY` avec le profil post-payout 0,25x gelé. Standard reste seulement un diagnostic alternatif ; les EV ne sont pas additionnées.

## Budget, gates et prochaine action

- Dépense append-only depuis l'activation : 0,002590589224 USD, dont 0,00040011853 USD de definitions.
- Dépense Databento cumulée : 87,84997897595 USD.
- Budget restant : 37,15002102405 USD ; réserve protégée de 25 USD intacte.
- Broker / ordres / Q4 : 0 / 0 / 0 delta.
- F0 : fail-closed ; F1, F2 et F3 : non démarrés.
- Lane regime-gap : `RESERVED_NO_SPECIALIST_ADMITTED_NOT_BLOCKING_FORWARD`, 0 candidat, plafond 10 %.

Action autonome exacte : continuer l'acquisition brute append-only et les checkpoints du service V17, sans traiter de décision économique. L'activation des six livres exigerait un nouveau contrat de preuve causal et per-bar explicitement gelé ; elle est incompatible avec l'instruction actuelle de ne modifier ni livres ni sémantique et n'est donc pas réalisée implicitement.

Audit machine : `online_offline_equivalence_audit.json`, proof hash `a9bbaf74d080024001ad733511df942c05807fe768d9d5d6a61b481b744f7210`.
