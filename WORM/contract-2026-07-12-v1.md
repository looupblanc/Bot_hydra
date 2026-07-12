# MISSION HYDRA V7 — CONTRAT DE FALSIFICATION & USINE PERPÉTUELLE

**Cible produit : Topstep 150K — passer le Combine rapidement, tenir l'Express Funded Account, enchaîner les payouts.**
Dépôt : `/root/hydra-bot` · Branche : `mission/v7-falsification` depuis `81f05ec9d7cd796ff91fd14d15760322c2924275`
Reprise depuis : `mission/state/snapshots/final_clean_stop_20260712T125202Z`

---

## 0. CONTRAT ÉPISTÉMIQUE — IMMUABLE

À relire intégralement tous les 25 steps. Toute action doit être justifiable par une clause. Aucune clause n'est modifiable par la mission elle-même.

1. **Succès = verdict honnête.** Un null propre est un livrable de même valeur qu'un survivant.
2. **Honest-green.** Aucun seuil, gate ou critère ne peut être affaibli après observation des résultats. Tout seuil est pré-enregistré (commit dédié taggé `WORM/`) AVANT le run qui le teste.
3. **Fitness interdite : P(passer le Combine).** Fitness obligatoire : espérance nette après coûts au niveau compte, déflatée pour la multiplicité (Deflated Sharpe Ratio, N_trials = compteur du registre). Le Combine est un module de **sizing aval** appliqué aux survivants — jamais un objectif d'optimisation.
4. **Séparation stricte.** Interdit de modifier le code de validation/sim/gates ET le code de stratégie/génération dans le même commit. Tout diff sous `hydra/account_policy/`, `tests/ruleset/` ou les modules de validation est listé en tête du rapport suivant.
5. **Preuve fraîche rationnée.** Toute fenêtre OOS ou shadow consommée est marquée `BURNED` dans `mission/state/proof_registry.json` (append-only, hash chaîné). Q4 est BURNED dès le bootstrap. Une fenêtre BURNED ne redevient jamais une preuve.
6. **Cimetière → générateur : niveau CLASSE uniquement** (famille de mécanisme × régime × cause de mort). Jamais de feedback au niveau paramètre.
7. **« Goal achieved » interdit** sans gate pré-enregistré passé VERT, chemin de preuve + sha256 inclus dans le rapport.
8. **Zéro ordre broker depuis cette mission**, en permanence (`ordres/broker = 0`). Le livrable exécutable est un `deployment_ticket.md` que l'humain signe ; l'exécution réelle se fera hors de ce serveur (voir R13).

---

## ÉTAT INITIAL HÉRITÉ (du snapshot)

- 115 388 prototypes, 290 candidats, 55 paniers élites dev, 6 politiques XFA dev, 960 contrôleurs évalués — **tous non validés, multiplicité non corrigée**.
- Grammaire V6 épuisée (53/480) : l'espace combinatoire V6 est saturé. C'est une donnée, pas une panne.
- 5 shadow slots configurés, feed absent. Q4 : 1 accès déjà consommé.
- Tests : 543/543. Budget : ≈ 72 $.

---

## RULESET TOPSTEP 150K — à encoder en tests exécutables (`tests/ruleset/`)

Chaque règle : `{énoncé, source, date, statut ∈ {VERIFIED_HUMAN, WEB_SOURCED, CONFLICT, ASSUMED}}`.
Une règle CONFLICT ou ASSUMED **bloque tout deployment_ticket** — pas la recherche.

### Combine 150K

- **R1** Profit target : 9 000 $ net. Pas de limite de temps. `[WEB_SOURCED]`
- **R2** MLL : 4 500 $. Implémentation par défaut : **niveau mis à jour en fin de journée sur le high-water mark du solde ; breach vérifié en temps réel sur Net P&L total (réalisé + latent) ; touch = liquidation immédiate ; le niveau ne redescend jamais ; lock au solde initial une fois atteint.** `[CONFLICT : une source décrit un niveau trailing intraday sur l'équité live → implémenter les DEUX variantes derrière un flag, mesurer la sensibilité des pass-rates aux deux, trancher par vérification humaine dans TopstepX sim avant tout ticket]`
- **R3** DLL optionnel : 3 000 $ (soft breach : liquide la session, pas le compte). Simuler avec et sans. `[WEB_SOURCED]`
- **R4** Consistance Combine : meilleur jour < 50 % du profit total ; dépassement = objectif repoussé, pas un échec. `[WEB_SOURCED]`
- **R5** Contrats : 15 max (scaling plan). `[WEB_SOURCED]`
- **R6** Jours de trading minimum : incertain. `[ASSUMED → vérifier avant ticket]`
- **R7** Coût d'attente : abonnement ≈ 149 $/mois pendant le Combine → le time-to-pass a un prix réel, à intégrer dans la fonction de valeur du sizing.

### XFA (funded)

- **R8** Départ à solde 0 $, plancher −4 500 $, plancher verrouillé à 0 $ une fois atteint. `[WEB_SOURCED]`
- **R9** Payout voie Standard : 5 jours gagnants ≥ 150 $ chacun + net positif depuis le dernier payout ; cap par demande ≈ 5 000 $ (150K). `[WEB_SOURCED]`
- **R10** Après le 1er payout : plancher = 0 $ définitivement → le seul buffer post-payout est le profit conservé au-dessus de 0. `[WEB_SOURCED]`
- **R11** Voie Consistency : 3 jours gagnants, meilleur jour ≤ 30–40 % du profit. `[CONFLICT sur le pourcentage → vérifier]`
- **R12** Split : 90/10 (traders entrés depuis le 12 jan 2026). `[WEB_SOURCED]`

### Conduite & exécution

- **R13** Automatisation permise via TopstepX API, MAIS : exécution sur la machine locale du trader, supervisée activement ; VPS/VPN/serveur distant = suspension. **La recherche sur ce serveur est libre ; l'exécution ne partira jamais d'ici.** `[WEB_SOURCED]`
- **R14** Interdits : HFT, latency arb, exploitation du SIM (fills irréalistes, brackets serrés exploitant l'absence de slippage, rafales exploitant la file d'attente), account-rolling (griller des comptes en série à haut risque). `[WEB_SOURCED — source officielle]`
- **R15** CME Rule 575 : ordres automatisés taggés `isAutomated`. `[WEB_SOURCED]`

> ⚠ **Conséquence directe de R14 sur la recherche** : tout candidat dont l'edge disparaît sous slippage stressé ×2 est classé `SIM_EXPLOIT` et tué. C'est simultanément de l'overfit et une violation de conduite — double raison de mort.

---

## PHASE 0 — VÉRITÉ DE LA SIMULATION (génération gelée)

*Budget max : 8 $. Aucune génération tant que G0 et G1 ne sont pas verts.*

- **0.1** Encoder le RULESET en tests exécutables. Les deux variantes MLL de R2 coexistent derrière un flag de config.
- **0.2** Test de divergence : rejouer les 55 paniers + 6 politiques XFA sous les deux variantes. Si l'implémentation historique diverge de la variante par défaut ⇒ badge `CONTAMINATED` sur tous les pass-rates historiques (rien n'est effacé, tout est requalifié). Publier l'écart chiffré variante A vs variante B.
- **0.3** Modèle de coûts par produit et par horizon (commissions + slippage), avec profils stressés ×1,5 et ×2. Publier `costs_model.md`.
- **0.4** **Gate G0** (pré-enregistré) : tests ≥ 543 verts + ruleset encodé + rapport de divergence publié.

---

## PHASE 1 — CALIBRATION DU NULL (tripwire fondateur)

*Budget max : 12 $.*

- **1.1** Trois jeux contrefactuels par produit : (a) block-shuffle des retours (blocs journaliers), (b) marche aléatoire à volatilité calquée, (c) années permutées.
- **1.2** Rejouer le pipeline COMPLET figé (55 paniers + contrôleurs → épisodes Combine) sur ces jeux, sans aucune modification.
- **1.3** Verdict pré-enregistré AVANT run : `NULL_RATIO = pass_rate(null) / pass_rate(réel)`.
  - Si NULL_RATIO ≥ 0,8 ⇒ **ARTEFACT** : le pass-rate mesure la géométrie 9k/4,5k, pas un edge. Badge `GEOMETRY_ONLY` sur les 55 élites. La clause 3 du contrat est confirmée définitivement.
  - Si NULL_RATIO < 0,8 ⇒ la baseline null-ajustée devient le zéro de toute métrique future.
- **1.4** Ce test devient un **tripwire permanent** : re-run obligatoire à chaque nouvelle grammaire ou modification de la sim ; résultat en tête de chaque rapport de génération.

---

## PHASE 2 — DÉDUPLICATION & MULTIPLICITÉ

*Budget max : 8 $.*

- **2.1** Matrice de corrélation des retours quotidiens des 55 paniers → clustering hiérarchique (coupure pré-enregistrée : distance 1−ρ = 0,3) → **N familles réellement distinctes**.
- **2.2** Étendre `proof_registry.json` en **registre de multiplicité** : compteur cumulatif de tous les tests statistiques jamais exécutés (rétro-estimation documentée pour les 115 388 prototypes). Toute promotion applique Benjamini-Hochberg à FDR 10 % avec ce compteur.
- **2.3** Sélectionner **≤ 3 représentants** (max 1 par famille), chacun avec fiche pré-enregistrée : hypothèse économique en une phrase (qui paie de l'autre côté et pourquoi ça persiste), seuils, horizon shadow, mode d'exécution prévu.
  **Seuils de promotion (commit WORM avant tout run) :**
  - DSR > 0 avec N_trials du registre ;
  - espérance nette par trade > 0 sous coûts stressés ×1,5, en walk-forward purgé (purge = horizon du label) + embargo ≥ 5 jours ;
  - conformité R1–R15 = 100 % sur la trajectoire simulée ;
  - survie au test `SIM_EXPLOIT` (edge conservé sous slippage ×2).
- **2.4** Q4 = BURNED : interdit comme preuve. La preuve des représentants = Phase 3 uniquement.

---

## PHASE 3 — PREUVE FORWARD (cœur de l'usine)

*Coût marginal quasi nul. Priorité absolue dès G0 vert.*

- **3.1** **Feed append-only** : activer l'ingestion quotidienne pour les 5 shadow slots. Barres horodatées, hash chaîné dans le registre, aucune réécriture possible. C'est désormais l'unique canal de preuve propre.
- **3.2** **Slots = jetons.** 1 slot = 1 candidat pré-enregistré = 1 verdict binaire à horizon fixe (20 jours de bourse par défaut). Verdict rendu → slot libéré → candidat suivant dans la file. Aucun candidat ne repasse sur une fenêtre déjà vue.
- **3.3** **Embargo roulant** : chaque semaine de données neuves sert UNE fois comme fenêtre de confirmation, puis bascule en dev. Statut de chaque semaine tracé dans le registre. C'est ce mécanisme qui permet à l'usine de tourner en permanence sans jamais épuiser la preuve.
- **3.4** **Gate G3 — sortie vers le réel** : ≥ 1 candidat shadow-VERT + DSR > 0 + conformité 100 % + R2 tranché par l'humain ⇒ le module de sizing produit **deux profils du MÊME edge** :
  - `COMBINE_MODE` : multiplicateur de risque maximisant P(pass ≤ 30 jours de bourse) sous contraintes P(breach MLL) ≤ 20 % et consistance R4 respectée. Objectif secondaire : minimiser les mois d'abonnement (R7).
  - `FUNDED_MODE` : sizing visant la cadence de payout — fabriquer des jours gagnants ≥ 150 $ (R9), rester au-dessus du plancher, **réduction structurelle du risque après le 1er payout** (R10 : plancher à 0), simulation du calendrier de retraits sous cap 5 000 $ par demande et split 90/10 (R12).
  - Livrable : `deployment_ticket.md` par candidat (paramètres, mode d'exécution local conforme R13–R15, garde-fous). Rien ne part vers un broker : l'humain signe.

---

## PHASE 4 — GÉNÉRATION V7 & BOUCLE PERPÉTUELLE

*Débloquée seulement si G0 + G1 verts ET verdict Phase 1 ≠ ARTEFACT non résolu. Budget : le solde.*

- **4.1** L'épuisement 53/480 est un signal : V7 n'est **pas** une expansion combinatoire. Chaque mécanisme nouveau exige une hypothèse économique écrite d'une phrase. Pas d'hypothèse → pas de slot de génération. `GRAMMAR_EXHAUSTED` = pivot non fatal (revue d'hypothèses), jamais un crash de mission.
- **4.2** **Cimetière actif** : indexer les 115 388 morts par signature (classe × régime × cause de mort) dans `graveyard.db`. Le générateur V7 échantillonne à distance des clusters morts. Firewall clause 6 : jamais de feedback paramètre.
- **4.3** **Multi-horizons libre** : 1-minute → daily, tous styles admissibles (momentum, mean-reversion, saisonnalité, microstructure, flux…) s'ils portent une hypothèse et passent l'entonnoir. Chaque horizon a son modèle de coûts propre.
- **4.4** **Entonnoir imposé**, du moins cher au plus cher :
  `sanité in-sample → coûts stressés → conformité ruleset → check SIM_EXPLOIT → walk-forward purgé + BH → file d'attente shadow`.
  Cible : ≥ 95 % tués avant le walk-forward. Chaque étage incrémente le registre de multiplicité.
- **4.5** **La boucle** : générer quand la file shadow a de la capacité, tuer en continu, promouvoir au compte-gouttes, re-déclencher le tripwire à chaque grammaire. La mission ne « termine » pas : elle rend des verdicts.

---

## GOUVERNEUR

- Budgets : P0 ≤ 8 $ · P1 ≤ 12 $ · P2 ≤ 8 $ · P3 ≈ 0 · P4 = solde. Dépassement ⇒ STOP propre + snapshot (même mécanique que `final_clean_stop`).
- Checkpoint tous les 25 steps : relire le contrat, écrire ≤ 3 lignes justifiant l'étape courante par une clause. Injustifiable ⇒ STOP + rapport.
- Nouvel `experiment_id`. Relance du service : `python scripts/hydra_mission_resume.py --state-dir mission/state --start-service` — seulement après G0.

---

## SCHÉMA DE RAPPORT (fixe — checkpoints et fins de phase, lisible en tmux)

```text
[HYDRA-V7] phase=<0-4> step=<n> verdict=<GREEN|RED|ARTEFACT|BLOCKED|NULL>
gate=<id> preuve=<chemin>#<sha256:8> tests=<x/y>
budget_phase=<consommé/max> N_trials=<registre> burned=<fenêtres>
diff_validation=<liste|aucun> prochaine_action=<une ligne>
```

---

## BOOTSTRAP (ordre exact)

1. `python scripts/hydra_mission_doctor.py` — état sain requis.
2. Lire `mission/state/snapshots/final_clean_stop_20260712T125202Z`.
3. `git checkout -b mission/v7-falsification 81f05ec`.
4. Copier ce document en `MISSION_CONTRACT.md` à la racine ; commit initial = référence WORM.
5. Créer `mission/state/proof_registry.json` avec Q4 = BURNED.
6. Phase 0.
