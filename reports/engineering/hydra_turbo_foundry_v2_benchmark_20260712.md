# HYDRA Turbo Foundry V2 — benchmark avant/après

Benchmark UTC: 2026-07-12

Baseline code: `2598e19ccaeef9f59cf2b6660aec63ae14b634f8`
Jeu de référence réel: données de développement gouvernées, du 2023-01-01 au
2024-09-30 inclus, Q4 exclu.

## Diagnostic avant optimisation

| Cycle | Structures | Replays larges | Temps | Débit structurel |
|---|---:|---:|---:|---:|
| Pre-close primaire | 32 | 1 | 83 s | 0,386/s |
| Mini/micro primaire | 96 | 3 | 317 s | 0,303/s |
| Role epoch | 6 politiques | 120 replays account | 88 s | 0,068 politique/s |

Le cycle mini/micro réévaluait en plus 31 fois les 96 structures dans son null
familial, soit 2 976 pseudo-évaluations. Les moteurs pre-close et mini/micro
étaient `parallel_safe=False` et n'utilisaient qu'un cœur. Les écritures de
rapport représentaient moins de 0,5 seconde et n'étaient pas le goulet.

## Architecture mesurée

- cache content-addressé et mmap, 1m + contextes fermés 5m/15m/30m/60m ;
- support causal existant conservé pour session/daily ;
- DSL structurel immuable, empreinte économique indépendante des noms ;
- compilation structure-of-arrays et Stage-1 NumPy par micro-lots ;
- replays exacts purs sur trois workers persistants ;
- unique writer d'artefacts, aucune écriture SQLite par worker ;
- dimensionnement de puissance : 5 990 propositions ;
- méta-screen registry train/calibration/OOS avec 20 % d'exploration minimum ;
- fallback exploration activé lorsque le méta-screen OOS n'est pas discriminant ;
- Promotion séparée : null candidat 2 048 tirages, BH/FDR, voisinage,
  mini/micro, délai et replay Topstep par rôle.

## Résultats après optimisation

Smoke réel immuable : `/tmp/hydra-turbo-v2-smoke-20260712T0320Z`.

| Mesure | Résultat |
|---|---:|
| Propositions / Stage-0 valides | 5 990 / 5 990 |
| Survivants Stage-1 | 602 |
| Replays exacts | 180 |
| Candidats prometteurs | 15 |
| Candidats envoyés à Promotion | 11 |
| Temps total chaud | 64,50 s |
| Débit Stage-1 complet | 191,22 candidats/s |
| Débit structurel end-to-end | 92,87 structures/s |
| Débit replays exacts (pool) | 54,09 candidats/s |
| Cache features chaud | 2,98 s |
| Stage-1 | 31,33 s |
| Replays exacts parallèles | 3,33 s |
| Utilisation workers | 89,50 % |
| Idle scheduler avec travail éligible | 0,00 % |
| RSS parent après cycle | 556,7 Mio |
| Pic RSS agrégé estimé des 3 workers | 1 958,3 Mio |

Comparaison A/B sur les mêmes matrices et les mêmes spécifications :

- Stage-1 scalaire/vectorisé : minimum `9,19x`, sorties bit-identiques ;
- replay exact scalaire/vectorisé : `99,20x`, sorties identiques ;
- débit structurel end-to-end contre le dernier cycle primaire : environ
  `306x` (92,87/s contre 0,303/s), sans comparer un simple compteur synthétique
  à une validation complète.

Le cache froid a consommé environ 1,6 Gio de RSS dans le processus de
construction et a été écrit une fois. Le bundle erroné découvert pendant le
smoke a été invalidé par version ; le bundle courant encode explicitement les
jours CME en `datetime64[D]`.

## Promotion réelle du smoke

Smoke Promotion : `/tmp/hydra-turbo-promotion-smoke-20260712T0325Z`.

- 11 candidats gelés évalués ;
- 3 `SHADOW_RESEARCH_CANDIDATE` ;
- 0 Topstep-path dans ce batch ;
- 0 `PAPER_SHADOW_READY` ;
- Q4 : 0 ;
- ordre/broker : 0.

Ces trois candidats ne sont ni actifs ni Paper-ready. Ils requièrent un package
shadow immuable/no-order et restent soumis au contrat protégé de promotion.

## Limites et décisions honnêtes

- Le métal ne possède pas assez d'observations de calibration valides dans le
  cache actuel. Son quota est donc redistribué avant Stage-1, sans lire les
  résultats économiques : 50 % indices, 50 % énergie. Cette relaxation est
  enregistrée ; elle n'est pas comptée comme diversité métal.
- Le méta-screen registry a obtenu un OOS mono-classe et un rappel à mi-budget
  insuffisant. Il est conservé comme diagnostic mais désactivé pour l'allocation.
- Aucun gain de débit ne constitue une preuve d'edge.
- Aucun candidat n'a accédé à Q4 ni reçu un statut hérité.

## Verdict d'acceptation

- Stage-0/Stage-1 >= 5x : **PASS** (`9,19x` A/B ; `306x` end-to-end structurel).
- Replay exact >= 3x : **PASS** (`99,20x`).
- Utilisation workers >= 80 % : **PASS** (`89,50 %`).
- Idle scheduler < 5 % : **PASS** (`0 %`).
- Sorties déterministes : **PASS**.
- Writer unique / Q4 / ordres / budget : **PASS**.
