# HYDRA V7 — AVENANT DU PRINCIPAL 004 — ECONOMIC EVOLUTION ENGINE

**Émis par le principal (humain), le 2026-07-13, avant tout résultat du nouveau
moteur.** Cet avenant complète le `MISSION_CONTRACT.md` et les avenants 001 à
003. Il prime uniquement sur les clauses et l'ordonnancement explicitement
amendés ci-dessous. Toutes les protections non amendées restent intégralement en
vigueur.

## 1. Objet du pivot

La succession de petites grammaires isolées a atteint une saturation
opérationnelle : elle falsifie proprement des formulations, mais dépense trop tôt
du calcul en walk-forward, tripwire et validation d'objets qui n'ont pas encore
prouvé leur utilité économique incrémentale.

L'objet évolué devient une **politique de compte composable**, construite à
partir de micro-composants typés. Un micro-composant peut être utile sans être
une stratégie autonome s'il améliore, de façon mesurable et après coûts, une
politique de compte par rapport à un contrôle apparié.

## 2. Amendement borné de la clause 3 — fitness

La clause 3 est amendée uniquement comme suit pour les campagnes lancées après
ce commit WORM :

1. La valeur économique primaire reste l'espérance nette au niveau compte après
   coûts réalistes. Aucun pass-rate brut ne constitue une preuve d'edge.
2. La découverte et la sélection précoce des composants utilisent leur valeur
   économique marginale : uplift net, réduction de MLL, diversification,
   amélioration de target velocity ou réduction des coûts, toujours face à un
   contrôle apparié.
3. L'évolution des politiques de compte utilise une archive **Pareto** et peut
   inclure simultanément : net après coûts, progression vers la cible, temps à
   la cible, probabilité diagnostique de passage, probabilité de breach MLL,
   consistance, survie XFA et trajectoire de payout.
4. La probabilité de passage Combine ne peut jamais être la fitness unique, ne
   peut jamais remplacer la preuve économique, et ne peut pas promouvoir un
   objet dont l'espérance nette stressée est négative.
5. DSR, BH, puissance calibrée et preuve indépendante restent obligatoires au
   stade de confirmation finale. Ils ne sont plus exécutés sur la population de
   masse avant qu'une politique soit opérationnellement crédible.

Cette modification est prospective. Aucun résultat historique n'est rejugé et
aucun tombstone n'est retiré.

## 3. Nouveau funnel obligatoire

Pour le moteur `ECONOMIC_EVOLUTION_ENGINE_V1`, l'ordre pré-enregistré est :

1. **STRUCTURAL_GENERATION** — composants typés et assemblages compatibles,
   dédupliqués avant replay ;
2. **ULTRA_CHEAP_ECONOMIC_SCREEN** — événements, marge brute et après coûts,
   concentration, dispersion temporelle, turnover, exécution possible ;
3. **COMPONENT_INCREMENTAL_VALUE** — add-one, leave-one-out, contrôles appariés,
   contribution nette, MLL, target velocity et diversification ;
4. **ASSEMBLY_TOURNAMENT** — beam search, évolution bornée et synthèse par rôle,
   sur chronologie de compte partagée ;
5. **ROLLING_COMBINE_PARETO** — diagnostic réaliste multi-départs, MLL, coûts,
   consistance, target velocity, XFA et payout ;
6. **EXPENSIVE_VALIDATION** — walk-forward, nulls pertinents, DSR/BH, puissance,
   bootstrap, stress et preuve indépendante, réservés à la petite élite.

La cible du funnel est de tuer la majorité des objets avant les étapes 3 à 6.
Les suites de tests complètes sont réservées aux changements matériels, aux
fusions et aux accès de preuve protégée ; elles ne sont pas exécutées par
candidat.

## 4. Architecture normative

Le moteur doit fournir, avec interfaces déterministes et versionnées :

- composants typés (`context`, `trigger`, `direction`, `eligibility`, `sizing`,
  `stop`, `target`, `time_exit`, `veto`, `portfolio_role`,
  `account_state_response`) ;
- registre, empreinte structurelle, empreinte sémantique et dépendances de
  features ;
- écran économique vectorisé ;
- tests de contribution incrémentale ;
- assemblage par beam search, évolution compatible et rôles de portefeuille ;
- politique de compte avec budgets de risque, arbitrage de signaux, limites de
  concurrence, protection MLL et modes Combine/Funded/Payout ;
- vecteur de causes d'échec et mutation dirigée par la cause dominante ;
- archives Pareto et qualité-diversité ;
- cache d'évaluations déterministes ;
- orchestration persistante avec un contrôleur et un writer.

Les politiques de compte sont rejouées sur une chronologie réellement fusionnée.
Il est interdit de sommer des statistiques de backtests autonomes pour simuler
un compte partagé.

## 5. Statuts autorisés

- `MICRO_EDGE_USEFUL`
- `ACCOUNT_POLICY_RESEARCH_CANDIDATE`
- `COMBINE_PATH_CANDIDATE`
- `PRE_HOLDOUT_READY`
- `PAPER_SHADOW_READY`
- `FUNDED_RESEARCH_CANDIDATE`

Un statut supérieur n'est attribué que lorsque toutes ses preuves versionnées
sont présentes. Les objectifs quantitatifs de population sont des cibles de
production, jamais des quotas autorisant l'affaiblissement des gates.

## 6. Pilotage et débit

Le premier pilote vise au minimum :

- 50 000 propositions de composants/assemblages ;
- plusieurs milliers d'écrans économiques bon marché ;
- plusieurs centaines de tests de valeur incrémentale ;
- au moins 100 politiques de compte assemblées ;
- Rolling Combine uniquement pour la petite élite.

Avant déploiement persistant, un benchmark comparable mesure génération/s,
screen/s, replay/s, épisodes Combine/s, cache, doublons, CPU, RAM et contention
writer. Le pilote peut rendre un verdict nul ; les nombres ci-dessus ne sont pas
des quotas de réussite.

Allocation initiale : 35 % mutation dirigée, 25 % assemblage, 15 % découverte,
10 % multi-actifs/rôles, 10 % contrôleurs de compte, 5 % représentations
nouvelles. L'allocation s'adapte à l'information gagnée et au taux
d'amélioration.

## 7. Sort du tournoi dual-track 0001

Le tournoi `hydra_v7_boosted_dual_track_tournament_0001`, pré-enregistré mais
n'ayant encore produit aucun nouveau feature, signal, PnL ou résultat de compte,
est **supersédé avant résultat** par le présent moteur. Sa réservation de
multiplicité de 1 607 essais demeure irréversible dans le registre global. Il ne
sera ni exécuté, ni compté comme un résultat scientifique, ni relancé.

Les résultats G11 et G12 restent terminaux et leurs tombstones restent actifs.
Les composants G10 non tombstonés peuvent entrer comme parents de recherche,
sans hériter d'un statut de validation.

## 8. Protections inchangées

Restent absolus : zéro broker, zéro ordre, zéro secret ou donnée brute commitée,
no-lookahead, contrats explicites, coûts réalistes et stressés, une seule écriture
autoritaire, reprise idempotente, Q4 BURNED, fenêtres de preuve rationnées,
manifests immuables et séparation validation/génération par commit.

Aucun nouvel achat de données n'est autorisé pour le pilote. Les 37,15261161327402
USD restants restent réservés à une confirmation ou une question d'exécution
ayant une valeur d'information démontrée.

Tout changement futur de seuil est WORM avant résultat. Toute situation non
couverte reste soumise à la clause d'escalade.

## 9. Déploiement

Le service systemd existant n'est redémarré qu'après régression complète,
no-lookahead, replays déterministes, intégrité DB/registre, gouvernance, budget,
secret scan, crash/reprise, single-writer et no-order verts. Le même mission ID,
les mêmes registres et les mêmes files sont conservés.

## CONTRE

L'assemblage d'un grand nombre de composants peut créer un nouvel espace de
sur-ajustement bien plus vaste que les anciennes grammaires. La déduplication,
les contrôles appariés, la multiplicité prospective, les comparaisons sur blocs
identiques et la validation indépendante restent donc indispensables, même si
elles sont déplacées plus tard dans le funnel.
