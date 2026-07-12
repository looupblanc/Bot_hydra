# AVENANT-001 AU MISSION_CONTRACT — PIVOT ORDERFLOW & CORRECTIFS DE RUN

**Émis par le principal (humain).** À committer tel quel : tag `worm/amendment-001-<date>`.
Le `MISSION_CONTRACT.md` reste intégralement en vigueur. Cet avenant prime uniquement sur les sections qu'il amende explicitement. Rappel : le contrat n'est pas modifiable par la mission (clause 0) — un avenant signé par le principal est l'unique voie d'évolution, et celui-ci en est une.

---

## A. CORRECTIFS DE RUN — immédiats

- **A1. Cimetière — nouvelle signature de classe : `ARB_INTRA_PRODUIT`.** La divergence mini/micro (MES vs ES, même sous-jacent) est de l'arbitrage intra-produit : un « écart » exploitable y est un artefact de quotes stales / latence, non capturable à notre latence d'exécution et contraire à l'esprit de R14. Badge SIM_EXPLOIT-adjacent au niveau classe (clause 6 : jamais au niveau paramètre). Le générateur échantillonne loin de cette classe ; le collapse IS→WF observé (+22,59 → −48,64 $/trade) est archivé comme signature de la classe.
- **A2. Schéma de rapport : réintégrer `budget_llm=<consommé/max>`**, conformément au contrat. Aucune ligne du schéma n'est optionnelle.
- **A3. Tripwire durci (pré-enregistré) :** en plus de `NULL_RATIO`, publier les comptes bruts (x/n réel vs x/n null) et la p-value binomiale exacte, test unilatéral, H0 = taux observé sous le null. Le verdict textuel doit qualifier la force de l'évidence (« vert-mince » vs « vert-net »).
- **A4. Contrôleur persistant V7 :** redémarrage sous systemd uniquement après régression complète verte + scan intégrité/gouvernance/secret vert. Ta discipline actuelle devient la règle écrite.

---

## B. PIVOT ORDERFLOW — DÉBLOCAGE D2 (décision du principal, justification écrite)

- **B1. Justification** (vaut « justification écrite » exigée par §4.0 du contrat) : les campagnes D1-0001/0002 ont testé les hypothèses visibles par les **prints** — le côté agressif du marché. Les hypothèses pré-enregistrées *absorption*, *imbalance*, *dynamique de file* sont des affirmations sur le côté **passif** — structurellement intestables sans profondeur. D1 n'a donc pas falsifié ces classes : il ne pouvait pas les voir. Le déblocage D2 est un changement d'instrument d'observation, pas un affaiblissement de gate : **aucun seuil de promotion ne change.**
- **B2. Scope D2 :** dataset `GLBX.MDP3`, schéma `mbp-10`, **ES uniquement** (le book informatif est sur le full-size ; l'exécution dans les tickets reste MES/ES, au choix du module de sizing). Fenêtre : les N mois **RTH les plus récents** que le budget autorise — les régimes de microstructure décaient vite, la récence prime sur la longueur.
- **B3. Budget data :** interroger l'**API de coût Databento AVANT tout achat** ; cap D2 ≤ **28 $** ; réserve ≥ **9 $** pour le feed quotidien et les backfills. La fenêtre s'ajuste au cap, jamais l'inverse. Rapport une ligne : coût estimé + fenêtre retenue, avant achat.
- **B4. Manifest & preuve :** le dataset D2 entre au manifest (plages, hashes, **cutoff propre**). Le gap entre la fin du D2 historique et aujourd'hui suit la règle du gap vierge (§3.0) : **fiches WORM d'abord, ingestion ensuite**, fenêtres consommables une fois puis BURNED. L'embargo roulant s'applique au flux D2 comme au reste.
- **B5. Primitives profondeur** — chacune exige sa fiche « qui paie de l'autre côté et pourquoi ça persiste » AVANT implémentation :
  - imbalance de book : ratio de profondeur bid/ask sur top-k niveaux, et sa persistance ;
  - absorption au niveau balayé : flux agressif absorbé sans déplacement du prix ;
  - retrait de liquidité / vacuum : annulations massives précédant le mouvement ;
  - iceberg / reload : volume exécuté à un niveau > taille affichée ;
  - microprice (mid pondéré par la profondeur) vs prix des prints : drift et divergence ;
  - **coût réel** : spread observé au top du book → `slippage = f(spread, taille, heure)` remplace le forfaitaire. Mise à jour de `costs_model.md` en commit séparé de toute stratégie (clause 4).
- **B6. Détection ≠ pratique :** ces primitives LISENT le comportement d'autrui (icebergs, retraits, spoof d'autrui). Aucune stratégie ne manipule elle-même le book — la mission n'émet de toute façon aucun ordre (clause 8), et tout ticket reste conforme R13–R15.
- **B7. Réorientation de la génération :** la boucle P4 privilégie les classes profondeur tant que la file shadow a de la capacité. Les classes prints-only encore vivantes continuent leur route aux mêmes gates.

---

## C. MULTIPLICITÉ STRATIFIÉE — pré-enregistrée AVANT toute génération D2

*C'est l'antériorité qui rend cet amendement légitime : il est commité avant que la moindre donnée D2 ne soit vue.*

- **C1. Constat :** appliquer le N_trials global (247 892) à une campagne neuve de ~10² tests rend toute promotion arithmétiquement impossible à jamais. Ce n'est plus de la rigueur, c'est une machine à faux négatifs : la déflation DSR doit compter les essais du processus de sélection **qui a produit le candidat**, pas l'histoire entière du projet.
- **C2. Règle amendée** (applicable uniquement aux campagnes lancées APRÈS cet avenant) : `N_trials(candidat) = essais de sa campagne (même lignée de générateur + même pool de sélection) × 1,5` — le coefficient d'inflation couvre la réutilisation d'information inter-campagnes via le cimetière. Le compteur global reste tenu au registre, à titre d'audit.
- **C3. BH :** FDR 10 % appliqué **par campagne** avec le N_trials stratifié. Aucun seuil de promotion ne change : DSR > 0, espérance nette > 0 sous coûts ×1,5, conformité R1–R16 = 100 %, survie SIM_EXPLOIT.
- **C4. Interdiction explicite de rétroactivité :** les campagnes closes (D1-0001, D1-0002) restent NULL. Aucun re-jugement du passé sous la nouvelle règle, jamais.

---

## D. ORDONNANCEMENT — mise à jour

1. Feed quotidien + backfills (D1 **et** D2) — jamais différés.
2. Verdicts shadow arrivés à échéance.
3. Cost-check puis achat/ingestion D2 (B3 → B4).
4. Fiches d'hypothèses profondeur (B5) → génération D2-0001.
5. Tripwire (P1) re-déclenché au premier jeu D2 et à chaque nouvelle grammaire, comme au contrat.

Préemption inchangée : toute anomalie d'intégrité suspend la génération.

---

## E. EXÉCUTION DE L'AVENANT (ordre exact)

1. Committer ce fichier : `worm/amendment-001-<date>`.
2. Appliquer A1–A4 (commits validation séparés de tout le reste, clause 4).
3. Cost-check Databento (B3) → rapport une ligne : coût estimé / fenêtre retenue.
4. Rédiger et committer les fiches WORM d'au moins 3 primitives B5.
5. Ingestion D2 + manifest + traitement du gap vierge (B4).
6. Lancer la campagne D2-0001 sous multiplicité stratifiée (C).
7. Rapport au schéma complet — `budget_llm` réintégré, section CONTRE incluse.

**Rappel final :** les états stables du contrat demeurent. Un NULL propre sur les classes profondeur serait un livrable — il dirait que l'edge n'est pas non plus dans le book visible, et chiffrerait le palier suivant. Mais ces classes partent vierges : c'est la première fois que le système regarde là où tes hypothèses vivent.
