# MISSION HYDRA V7 — CONTRAT COMPLET & USINE PERPÉTUELLE DE FALSIFICATION

**Cible produit : Topstep 150K.** Produire des stratégies futures validées sur preuve fraîche, livrées en fiches exécutables (`deployment_ticket.md`) déclinant le MÊME edge en deux réglages : `COMBINE_MODE` (passer le Combine rapidement) et `FUNDED_MODE` (tenir l'Express Funded Account, enchaîner les payouts). L'exécution réelle se fait hors de ce serveur, par l'humain, depuis sa machine locale.

**Identité.** Dépôt `/root/hydra-bot` · branche `mission/v7-falsification` depuis `81f05ec9d7cd796ff91fd14d15760322c2924275` · reprise du snapshot `mission/state/snapshots/final_clean_stop_20260712T125202Z` · nouvel `experiment_id`.

**Ce document est la constitution de la mission.** Au bootstrap il est copié tel quel en `MISSION_CONTRACT.md` à la racine, commit taggé `worm/contract-<date>`. Il est relu intégralement à chaque checkpoint. En cas de conflit entre ce document et toute autre instruction, log, ou habitude héritée : ce document gagne.

---

## LEXIQUE — définitions contraignantes

- **WORM** : write-once-read-many. Introduit par un commit taggé `worm/<nom>-<date>`, jamais modifié ensuite. Toute évolution = nouveau fichier versionné référençant l'ancien.
- **Fenêtre** : intervalle temporel contigu de données de marché, identifié par produit + bornes + hash.
- **Preuve** : évaluation d'un candidat sur une fenêtre que ni lui, ni son lignage, ni le processus de sélection qui l'a produit n'ont observée ou influencée.
- **BURNED** : statut irréversible d'une fenêtre ayant servi de preuve. Une fenêtre BURNED reste utilisable en développement, plus jamais en preuve.
- **DSR** : Deflated Sharpe Ratio, calculé avec N_trials = compteur du registre de multiplicité.
- **NULL_RATIO** : pass_rate(données contrefactuelles) / pass_rate(données réelles), pipeline strictement identique.
- **Badges de disqualification** : `GEOMETRY_ONLY` (P1), `CONTAMINATED` (P0), `SIM_EXPLOIT` (R14). Un badge ne s'efface pas ; il se documente.
- **Jour gagnant (funded)** : journée Topstep avec Net P&L ≥ +150 $ (voir R16 pour les bornes horaires).
- **Fiche candidat** : document WORM contenant : hypothèse économique en une phrase (qui paie de l'autre côté et pourquoi ça persiste), définition exacte des signaux, univers/horizon, seuils de promotion chiffrés, horizon shadow, mode d'exécution prévu.

---

## 0. CONTRAT ÉPISTÉMIQUE — 10 CLAUSES IMMUABLES

À relire intégralement tous les 25 steps et à chaque frontière de phase. Toute action doit être justifiable par une clause. Aucune clause n'est modifiable par la mission elle-même.

1. **Succès = verdict honnête.** Un null propre est un livrable de même valeur qu'un survivant.
2. **Honest-green.** Aucun seuil, gate ou critère ne peut être affaibli après observation des résultats. Tout seuil est pré-enregistré WORM AVANT le run qui le teste.
3. **Fitness interdite : P(passer le Combine).** Fitness obligatoire : espérance nette après coûts au niveau compte, déflatée pour la multiplicité (DSR). Le Combine est un module de sizing AVAL appliqué aux survivants — jamais un objectif d'optimisation.
4. **Séparation stricte.** Interdit de modifier le code de validation/sim/gates ET le code de stratégie/génération dans le même commit. Tout diff sous `hydra/account_policy/`, `tests/ruleset/` ou les modules de validation est listé en tête du rapport suivant.
5. **Preuve fraîche rationnée.** Toute fenêtre consommée en preuve est marquée BURNED dans `mission/state/proof_registry.json` (append-only, hash chaîné). Q4 est BURNED au bootstrap.
6. **Cimetière → générateur : niveau CLASSE uniquement** (famille de mécanisme × régime × cause de mort). Jamais de feedback au niveau paramètre depuis l'OOS ou le shadow.
7. **« Goal achieved » interdit** sans gate pré-enregistré passé VERT, chemin de preuve + sha256 dans le rapport.
8. **Zéro ordre broker depuis cette mission, en permanence** (`ordres/broker = 0`). Le livrable exécutable est un ticket que l'humain signe ; l'exécution se fera hors serveur (R13).
9. **Section CONTRE obligatoire** : chaque rapport de phase inclut le meilleur argument contre sa propre conclusion (limite, anomalie, test manquant). Section vide = rapport invalide.
10. **Escalade plutôt qu'improvisation** : situation non couverte par une clause ou une phase ⇒ STOP propre + question en tête de rapport. Jamais d'interprétation créative des gates.

---

## ÉTAT INITIAL HÉRITÉ (du snapshot)

- 115 388 prototypes, 290 candidats, 55 paniers élites dev, 6 politiques XFA dev, 960 contrôleurs évalués — **tous non validés, multiplicité non corrigée**.
- Grammaire V6 épuisée (53/480) : l'espace combinatoire V6 est saturé. C'est une donnée, pas une panne.
- 5 shadow slots configurés, feed absent. Q4 : 1 accès déjà consommé.
- Tests : 543/543. Budget LLM : ≈ 72 $. Budget data (Databento, crédits d'inscription) : ≈ 125 $, distinct.

---

## RULESET TOPSTEP 150K — à encoder en tests exécutables (`tests/ruleset/`)

Chaque règle : `{énoncé, source, date, statut ∈ {VERIFIED_HUMAN, WEB_SOURCED, CONFLICT, ASSUMED}}`.
Une règle CONFLICT ou ASSUMED **bloque tout deployment_ticket** — jamais la recherche.

### Combine 150K
- **R1** Profit target : 9 000 $ net. Pas de limite de temps. `[WEB_SOURCED]`
- **R2** MLL : 4 500 $. Implémentation par défaut : **niveau mis à jour en fin de journée sur le high-water mark du solde ; breach vérifié en temps réel sur Net P&L total (réalisé + latent) ; touch = liquidation immédiate ; le niveau ne redescend jamais ; lock au solde initial une fois atteint.** `[CONFLICT : une source décrit un niveau trailing intraday sur l'équité live → implémenter les DEUX variantes derrière le flag mll_mode ∈ {eod_level_rt_breach, intraday_hwm}, publier la sensibilité des pass-rates aux deux, trancher par vérification humaine dans TopstepX sim avant tout ticket]`
- **R3** DLL optionnel : 3 000 $ (soft : liquide la session, pas le compte). Simuler avec et sans. `[WEB_SOURCED]`
- **R4** Consistance Combine : meilleur jour < 50 % du profit total ; dépassement = objectif repoussé, pas un échec. `[WEB_SOURCED]`
- **R5** Contrats : 15 max (scaling plan). `[WEB_SOURCED]`
- **R6** Jours de trading minimum : incertain. `[ASSUMED → vérifier avant ticket]`
- **R7** Coût d'attente : abonnement ≈ 149 $/mois pendant le Combine → le time-to-pass a un prix réel, intégré à la fonction de valeur du sizing.

### XFA (funded)
- **R8** Départ à solde 0 $, plancher −4 500 $, verrouillé à 0 $ une fois atteint. `[WEB_SOURCED]`
- **R9** Payout voie Standard : 5 jours gagnants ≥ 150 $ chacun + net positif depuis le dernier payout ; cap par demande ≈ 5 000 $. `[WEB_SOURCED]`
- **R10** Après le 1er payout : plancher = 0 $ définitivement → le seul buffer post-payout est le profit conservé au-dessus de 0. `[WEB_SOURCED]`
- **R11** Voie Consistency : 3 jours gagnants, meilleur jour ≤ 30–40 % du profit. `[CONFLICT sur le pourcentage → vérifier]`
- **R12** Split : 90/10 (traders entrés depuis le 12 jan 2026) — appliqué à tous les calculs d'EV de payout. `[WEB_SOURCED]`

### Conduite, exécution, temps
- **R13** Automatisation permise via TopstepX API, MAIS : exécution sur la machine locale du trader, supervisée activement ; VPS/VPN/serveur distant = suspension. **La recherche ici est libre ; l'exécution ne partira jamais de ce serveur.** `[WEB_SOURCED]`
- **R14** Interdits : HFT, latency arb, exploitation du SIM (fills irréalistes, brackets serrés exploitant l'absence de slippage, rafales exploitant la file d'attente), account-rolling. `[WEB_SOURCED — officiel]`
  → **Conséquence recherche** : tout candidat dont l'edge disparaît sous slippage stressé ×2 est badgé `SIM_EXPLOIT` et tué — overfit ET violation de conduite, double cause de mort.
- **R15** CME Rule 575 : ordres automatisés taggés `isAutomated` (à porter dans chaque ticket). `[WEB_SOURCED]`
- **R16** Bornes de journée : la journée de trading s'ouvre à 17:00 CT ; le jour gagnant se verrouille à 16:00 CT. **La sim aligne toutes ses frontières EOD sur le fuseau CT**, pas sur UTC ni sur le calendrier civil. `[WEB_SOURCED]`

---

## ARCHITECTURE & CHEMINS

Deux pistes parallèles convergeant sur la file shadow :
- **Piste A — valider l'existant** : P0 → P1 → P2 → P3 (les 55 paniers passent au tribunal).
- **Piste B — usine perpétuelle** : DATA_UPGRADE (dès G0) + P4 (génération V7), alimentant la même file shadow, sous les mêmes gates.

```
MISSION_CONTRACT.md                      # ce document, WORM
mission/state/proof_registry.json        # preuve + multiplicité, append-only, hash chaîné
mission/state/graveyard.db               # cimetière : classe × régime × cause de mort
mission/state/shadow/                    # slots, file d'attente, verdicts
data/manifest.json                       # data lake : sources, plages, cutoff, checksums
tests/ruleset/                           # R1–R16 exécutables
WORM/                                    # seuils, fiches candidats, verdicts pré-enregistrés
reports/                                 # rapports au schéma fixe
tickets/                                 # deployment_ticket.md signables
```

---

## PHASE 0 — VÉRITÉ DE LA SIMULATION (génération gelée)

*Budget LLM max : 8 $. Aucune génération tant que G0 et G1 ne sont pas verts.*

- **0.0 Manifest du data lake** : produire `data/manifest.json` — pour chaque source : plage, dernier timestamp (= le **cutoff**), checksum. Ce manifest définit le gap vierge de la Phase 3.
- **0.1** Encoder R1–R16 en tests exécutables. Les deux variantes MLL coexistent derrière `mll_mode`. Seeds loggés partout ; tout run doit être rejouable à l'identique.
- **0.2 Test de divergence** : rejouer les 55 paniers + 6 politiques XFA sous les deux variantes MLL. Si l'implémentation historique diverge de la variante par défaut ⇒ badge `CONTAMINATED` sur tous les pass-rates historiques (rien n'est effacé, tout est requalifié). Publier l'écart chiffré variante A vs B.
- **0.3 Modèle de coûts** par produit et horizon (commissions + slippage), profils stressés ×1,5 et ×2. Publier `costs_model.md`.
- **0.4 Gate G0** (pré-enregistré) : tests ≥ 543 verts + ruleset encodé + manifest publié + rapport de divergence publié.

---

## PHASE 1 — CALIBRATION DU NULL (tripwire fondateur)

*Budget LLM max : 12 $.*

- **1.1** Trois jeux contrefactuels par produit : (a) block-shuffle des retours (blocs journaliers), (b) marche aléatoire à volatilité calquée, (c) années permutées. Seeds WORM.
- **1.2** Rejouer le pipeline COMPLET figé (55 paniers + contrôleurs → épisodes Combine) sur ces jeux, sans aucune modification. Au moins autant d'épisodes null que réels.
- **1.3** Verdict pré-enregistré AVANT run :
  - `NULL_RATIO ≥ 0,8` ⇒ **ARTEFACT** : le pass-rate mesure la géométrie 9k/4,5k, pas un edge. Badge `GEOMETRY_ONLY` sur les 55. Les métriques pass-rate sont retirées de tous les tableaux de bord et remplacées par les métriques d'espérance.
  - `NULL_RATIO < 0,8` ⇒ la baseline null-ajustée devient le zéro de toute métrique future.
- **1.4 Tripwire permanent** : re-run obligatoire à chaque nouvelle grammaire, nouveau jeu de données, ou modification de la sim. Résultat en tête de chaque rapport de génération.

---

## PHASE 2 — DÉDUPLICATION & MULTIPLICITÉ

*Budget LLM max : 8 $.*

- **2.1** Corrélation des retours quotidiens des 55 → clustering hiérarchique (coupure pré-enregistrée : distance 1−ρ = 0,3) → **N familles réellement distinctes**.
- **2.2** Étendre `proof_registry.json` en **registre de multiplicité** : compteur cumulatif de tous les tests statistiques jamais exécutés, rétro-estimation documentée (méthode écrite) pour les 115 388 prototypes. Toute promotion applique Benjamini-Hochberg à FDR 10 % avec ce compteur.
- **2.3** Sélectionner **≤ 3 représentants** (max 1 par famille), fiche candidat WORM chacun. Seuils de promotion (WORM avant tout run) :
  - DSR > 0 avec N_trials du registre ;
  - espérance nette par trade > 0 sous coûts ×1,5, en walk-forward purgé (purge = horizon du label) + embargo ≥ 5 jours ;
  - conformité R1–R16 = 100 % sur trajectoire simulée ;
  - survie au test `SIM_EXPLOIT` (edge conservé sous slippage ×2).
- **2.4** Si aucun représentant ne survit à BH : c'est le verdict, pas un problème — les slots shadow reviennent intégralement à la Piste B.
- **2.5** Q4 = BURNED : interdit comme preuve. La preuve des représentants = Phase 3 uniquement.

---

## PHASE 3 — PREUVE FORWARD + BACKFILL DU GAP VIERGE

*Coût marginal quasi nul. Priorité absolue dès G0 vert.*

- **3.0 Backfill immédiat — l'accélérateur.** Au premier démarrage du feed : ingérer les données depuis le cutoff du manifest jusqu'à aujourd'hui. Virginité vérifiée par : absence du manifest + aucun accès loggé. Ce gap est découpé en fenêtres de confirmation **immédiatement consommables** — une fois chacune, puis BURNED.
  **Ordre strict et non négociable** : les fiches candidats sont commitées WORM AVANT l'ingestion du gap. Toute fiche postérieure à l'ingestion ne peut pas consommer le gap (elle attend le feed forward). Le timestamp des commits fait foi.
- **3.1 Feed append-only** : ingestion quotidienne pour les 5 shadow slots, barres horodatées, hash chaîné dans le registre. Jamais différée (priorité 1 de l'ordonnanceur).
- **3.2 Slots = jetons.** Machine d'états : `EMPTY → LOADED(fiche WORM) → RUNNING → VERDICT(GREEN|RED) → fenêtre BURNED, slot FREED`. Horizon par défaut 20 jours de bourse, ajustable par fiche (WORM). Aucun candidat ne repasse sur une fenêtre déjà vue par lui ou sa famille.
- **3.3 Embargo roulant** : chaque semaine de données neuves sert UNE fois comme fenêtre de confirmation puis bascule en dev. Statut de chaque semaine tracé au registre. C'est ce qui rend l'usine perpétuelle sans jamais épuiser la preuve.
- **3.4 Gate G3 — sortie vers le réel.** Conditions : ≥ 1 candidat shadow-VERT + DSR > 0 + conformité 100 % + R2 tranché VERIFIED_HUMAN. Alors le module de sizing produit deux profils du MÊME edge :
  - **`COMBINE_MODE`** : multiplicateur de risque k* = argmax P(atteindre 9 000 $ en ≤ 30 jours de bourse) sous contraintes P(breach MLL) ≤ 20 %, consistance R4 respectée en trajectoire, contrats ≤ 15 (R5). Objectif secondaire : minimiser E[mois d'abonnement] (R7).
  - **`FUNDED_MODE`** : politique de sizing maximisant la cadence de payouts — fabriquer des jours gagnants ≥ 150 $ (R9, bornes R16), rester au-dessus du plancher (R8), simulation du calendrier de retraits sous cap ≈ 5 000 $/demande et split 90/10 (R12). **Règle post-payout obligatoire** : après le 1er payout simulé, plancher = 0 (R10) ⇒ réduction automatique du risque (k divisé par un facteur pré-enregistré) tant que le coussin de profit < seuil pré-enregistré.
  - Livrable : `tickets/<candidat>.md` — paramètres exacts, mode d'exécution local conforme R13–R15 (isAutomated inclus), garde-fous chiffrés, et la mention : « aucun ordre n'a été ni ne sera émis par cette mission ». L'humain signe.

---

## PHASE 4 — USINE PERPÉTUELLE : DATA_UPGRADE + GÉNÉRATION V7

*Débloquée si G0 + G1 verts ET verdict P1 ≠ ARTEFACT non résolu. Budget LLM : le solde. DATA_UPGRADE peut démarrer dès G0, en parallèle de P1–P3.*

### 4.0 Module DATA_UPGRADE (budget data distinct)
- Source : **Databento, dataset `GLBX.MDP3`** (CME/CBOT/NYMEX/COMEX). Clé API via variable d'environnement `DATABENTO_API_KEY` — jamais en clair dans le repo, jamais dans un log. Contrats continus et rollovers gérés via la symbologie du fournisseur ; timestamps nanoseconde.
- **Palier D1** (immédiat, ≤ 60 $ des crédits) : schéma `trades` — prints signés côté agresseur — MES/ES, 2–3 ans, RTH d'abord. Intégration au manifest (hashes, plages). Débloque : delta/CVD, profils de volume, distribution des tailles, détection de sweeps sur prints réels.
- **Palier D2** (déblocage conditionnel : ≥ 1 classe d'hypothèses survivante nécessitant la profondeur, justification écrite) : schéma `mbp-10`, fenêtres ciblées. Débloque : imbalance, absorption, dynamique de file. ~5× plus lourd que le top-of-book — échelonner.
- **`mbp-1` ponctuel** pour mesurer le spread réellement observé → le modèle de coûts passe de forfaitaire à `slippage = f(spread observé, taille, heure)`.
- **Représentation native** : barres event-based (volume bars, imbalance bars, dollar bars) construites depuis les prints, en plus du temps calendaire.
- **Palier D3 (MBO)** : interdit sans justification écrite par une classe survivante précise.

### 4.1 Grammaire V7 — hypothèses avant mécanismes
- L'épuisement V6 (53/480) est un signal : V7 n'est **pas** une expansion combinatoire. Chaque mécanisme nouveau exige une fiche d'hypothèse économique AVANT implémentation. Pas d'hypothèse → pas de slot de génération. `GRAMMAR_EXHAUSTED` = pivot non fatal (revue d'hypothèses), jamais un crash.
- **Primitives microstructure candidates** (chacune avec sa fiche « qui paie ») : divergence de delta aux extrêmes, absorption au niveau balayé, persistance d'imbalance, qualité de sweep sur prints (volume déclenché vs continuation), profil d'agression par session.
- **Note cimetière** : les nulls SMC historiques (OHLC 15m) sont enregistrés comme classe « SMC-sur-OHLC », pas « SMC » — ils testaient l'ombre du mécanisme, pas le mécanisme. Les classes microstructure partent vierges.

### 4.2 Cimetière actif
Indexer les 115 388 morts + tous les nouveaux par signature (classe × régime × cause) dans `graveyard.db`. Le générateur échantillonne à distance des clusters morts. Firewall clause 6.

### 4.3 Multi-horizons libre
1-minute → daily, tous styles admissibles (momentum, mean-reversion, saisonnalité, microstructure, flux…) s'ils portent une hypothèse et passent l'entonnoir. Chaque horizon a son modèle de coûts propre.

### 4.4 Entonnoir imposé (du moins cher au plus cher)
`sanité in-sample → coûts stressés → conformité ruleset → check SIM_EXPLOIT → walk-forward purgé + BH → file shadow`
Cible : ≥ 95 % tués avant le walk-forward. Chaque étage incrémente le registre de multiplicité.

### 4.5 La boucle
Générer quand la file shadow a de la capacité · tuer en continu · promouvoir au compte-gouttes · re-déclencher le tripwire (P1) à chaque grammaire ou nouveau jeu de données. La mission ne « termine » pas : elle rend des verdicts.

---

## ORDONNANCEMENT & PRÉEMPTION

1. Ingestion feed quotidienne + backfill — jamais différée.
2. Verdicts shadow arrivés à échéance.
3. Phases dans l'ordre P0 → P4 ; DATA_UPGRADE D1 en parallèle dès G0.
4. Génération V7 seulement si la file shadow n'est pas saturée ET le tripwire est vert.

**Préemption** : toute anomalie d'intégrité (hash cassé, test rouge, divergence sim, écriture hors registre) suspend la génération jusqu'à résolution documentée.

---

## GOUVERNEUR & CHECKPOINTS

- **Budget LLM** : P0 ≤ 8 $ · P1 ≤ 12 $ · P2 ≤ 8 $ · P3 ≈ 0 · P4 = solde. **Budget data** : D1 ≤ 60 $ des crédits ; D2/D3 sur justification écrite.
- Dépassement ⇒ STOP propre + snapshot (mécanique `final_clean_stop`) + rapport. Reprise : `python scripts/hydra_mission_resume.py --state-dir mission/state --start-service`.
- **Checkpoint tous les 25 steps et à chaque frontière de phase** : (a) relire `MISSION_CONTRACT.md` ; (b) justifier l'étape courante par une clause, ≤ 3 lignes ; (c) auto-audit une ligne : « quel est le moyen le plus probable par lequel je suis en train de me tromper en ce moment ? ». Injustifiable ⇒ STOP + rapport.
- **Interactions humaines requises — exactement trois** : ① fournir `DATABENTO_API_KEY` ; ② trancher R2 dans TopstepX sim (VERIFIED_HUMAN) ; ③ signer les tickets. Tout le reste est autonome.

---

## INTERDICTIONS ABSOLUES (rappel opérationnel)

Émettre un ordre broker · affaiblir un seuil après observation · réutiliser une fenêtre BURNED comme preuve · faire remonter du feedback paramétrique du forward vers le générateur · modifier validation et stratégie dans le même commit · écrire « Goal achieved » sans gate vert · committer ou logger une clé API · émettre un ticket avec une règle CONFLICT/ASSUMED non tranchée · rapport sans section CONTRE.

---

## SCHÉMA DE RAPPORT (fixe — checkpoints et fins de phase, lisible en tmux)

```
[HYDRA-V7] phase=<0-4|D> step=<n> verdict=<GREEN|RED|ARTEFACT|BLOCKED|NULL>
gate=<id> preuve=<chemin>#<sha256:8> tests=<x/y>
budget_llm=<consommé/max> budget_data=<consommé/max> N_trials=<registre> burned=<n>
diff_validation=<liste|aucun> CONTRE=<meilleur argument contre cette conclusion>
prochaine_action=<une ligne>
```

---

## ÉTATS STABLES DE LA MISSION (il n'y a pas de « fin »)

- **LIVRAISON** : ≥ 1 ticket signable en attente → la boucle continue en arrière-plan.
- **NULL PROPRE** : P1 = ARTEFACT non résolu OU P4 épuise les hypothèses économiques → rapport final : « le champ de vision actuel des données ne contient pas d'edge exploitable », avec recommandation chiffrée du prochain palier data (D2/D3, autres venues). **C'est un succès de mission.**
- **BLOQUÉ** : question humaine en attente → STOP propre, question en tête de rapport.

---

## ACCEPTANCE TESTS DU BOOTSTRAP (auto-vérification avant P0)

1. `python scripts/hydra_mission_doctor.py` vert.
2. `MISSION_CONTRACT.md` présent, commit taggé `worm/contract-<date>`.
3. `mission/state/proof_registry.json` créé, Q4 = BURNED, hash chaîné initialisé.
4. Branche `mission/v7-falsification` active sur `81f05ec`.
5. Tests : 543/543 verts.
6. `data/manifest.json` généré, cutoff identifié.

Un seul échec ⇒ état BLOQUÉ, pas de Phase 0.

---

## BOOTSTRAP (ordre exact)

1. `python scripts/hydra_mission_doctor.py` — état sain requis.
2. Lire `mission/state/snapshots/final_clean_stop_20260712T125202Z`.
3. `git checkout -b mission/v7-falsification 81f05ec`.
4. Copier ce document en `MISSION_CONTRACT.md` ; commit taggé `worm/contract-<date>`.
5. Créer `mission/state/proof_registry.json` avec Q4 = BURNED.
6. Générer `data/manifest.json` (cutoff du data lake).
7. Exécuter les acceptance tests ci-dessus.
8. Phase 0. La mission rend des verdicts.
