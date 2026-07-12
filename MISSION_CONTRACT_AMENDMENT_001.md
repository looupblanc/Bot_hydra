# HYDRA V7 — AMENDEMENT UTILISATEUR 001

Date d'autorisation : `2026-07-12`  
Objet : bootstrap initial du feed de preuve forward.

## Autorisation

Au premier démarrage du feed append-only de la Phase 3.1, HYDRA doit backfiller
le gap compris entre le dernier timestamp effectivement présent dans le data
lake canonique et le dernier bar fermé disponible au moment du démarrage.

Ce gap peut fournir des fenêtres de confirmation immédiatement consommables si,
et seulement si, toutes les conditions suivantes sont satisfaites :

1. le manifest d'ingestion prouve que chaque barre du gap était absente avant le
   bootstrap ;
2. le registre de preuve et le registre d'accès prouvent que la fenêtre n'a
   jamais été inspectée comme preuve, holdout ou shadow ;
3. la fenêtre commence strictement après le freeze du candidat qui la consomme ;
4. seules des barres de marché réellement fermées sont ingérées ; aucune barre
   de week-end, de marché fermé ou future n'est fabriquée ;
5. l'ingestion est append-only, hash-chaînée, idempotente et accompagnée des
   empreintes de données, contrats explicites, calendrier et symbologie ;
6. le gap est découpé en fenêtres hebdomadaires non chevauchantes, conformément
   à l'embargo roulant de la clause 3.3 ;
7. une fenêtre est attribuée une seule fois à un candidat pré-enregistré ; dès
   son verdict, elle est marquée irréversiblement `BURNED` dans
   `mission/state/proof_registry.json` ;
8. toute portion antérieure au freeze du candidat peut devenir donnée de
   développement, mais ne compte jamais comme preuve forward pour ce candidat ;
9. aucune fenêtre `BURNED`, partiellement vue ou contaminée n'est réhabilitée ;
10. les contraintes de budget, d'absence d'ordre broker et de séparation des
    rôles de données restent inchangées.

Cet amendement est une autorisation humaine additive. Il ne modifie ni les
seuils de promotion, ni les gates G0/G1/G3, ni le contrat de falsification.
