# V7.1 RESEARCH-STAGE GATE CORRECTION

**Émis par le principal (humain), le 2026-07-13.** Cet avenant complète le
`MISSION_CONTRACT.md` et l'avenant power-aware. Il ne modifie pas le seuil de
puissance final, les gates d'intégrité, le tripwire, la multiplicité, les règles
de preuve, ni l'interdiction permanente d'ordres broker.

Keep the 80% calibrated power requirement for final confirmation, but do not use
it as a universal gate before diagnostic Rolling Combine evaluation.

Create the status:

`PROMISING_UNDERPOWERED_COMBINE_RESEARCH`

Select the 3–5 highest-information candidates among the current
`PROMISING_UNDERPOWERED` population using:

- stressed walk-forward economics;
- effective independent sample size;
- block stability;
- concentration;
- behavioral distinctness;
- expected Topstep relevance.

Freeze their exact specifications and run bounded Rolling Combine diagnostics
with 24–60 block-aware starts.

Report:

- pass rate;
- target-progress percentiles;
- MLL breach rate;
- consistency;
- days projected to target;
- net after costs;
- account-basket contribution.

Do not promote these candidates as validated. Do not buy additional data yet.
Continue the distinct preregistered G6 discovery campaign in parallel.

Also reconcile why 20 walk-forward-positive candidates produced only 18 power
decisions. No candidate may disappear from the evidence ledger without an
explicit terminal or pending status.

## Interprétation normative bornée

1. `POWERED_WF_POSITIVE` demeure requis pour toute confirmation finale,
   validation statistique aval, shadow admission ou claim de preuve.
2. Le Rolling Combine autorisé ici est un diagnostic de compte, jamais une
   fitness de génération, un gate de promotion ou une preuve indépendante.
3. Le statut nouveau est terminal pour ce diagnostic précis mais reste
   `PROMISING_UNDERPOWERED` au niveau scientifique.
4. Les départs, horizons, règles de compte, critères de sélection et
   spécifications candidates sont WORM avant le premier diagnostic.
5. Aucun résultat du diagnostic ne peut modifier les paramètres de la version
   testée ni réhabiliter une classe `GEOMETRY_ONLY`.
6. Les deux candidats walk-forward positifs de G5 sont explicitement classés
   `GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT`; ils ne sont ni perdus ni
   éligibles au diagnostic.

## CONTRE

Le diagnostic Combine est post-sélection et peut sembler convaincant sur des
épisodes corrélés. Ses résultats restent descriptifs, portent un effectif
indépendant explicite et ne remplacent jamais le seuil final de puissance à
80 %, le tripwire, les nulls, DSR/BH ou une confirmation fraîche.
