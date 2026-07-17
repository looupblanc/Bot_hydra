# HYDRA F0 Single-Source Engine Parity — décision

## Décision

Le statut final autorisé est **`DEVELOPMENT_EVIDENCE_CONTAMINATED`**. Aucun reçu `ONLINE_OFFLINE_EQUIVALENCE_PROVEN` n'a été créé. Les six livres sont placés en quarantaine opérationnelle sans modifier leurs manifests, leurs rangs, leurs manches ou leurs paramètres. Le service V17 reste actif, mais son chemin forward est verrouillé dans `DEVELOPMENT_EVIDENCE_CONTAMINATED_FAIL_CLOSED` avant tout appel d'acquisition ou de traitement économique.

La cause n'est pas une dérive du moteur en ligne. Le sélecteur de développement utilisait `np.isfinite(forward_move__h)` dans l'éligibilité du signal. Ce champ dépend de la clôture et de la continuité futures jusqu'à l'horizon de sortie. Retirer uniquement ce masque, en conservant les déclencheurs, contextes, sessions et l'état de non-chevauchement gelés, produit 2 073 signaux contre 2 052 dans l'EvidenceBundle : 21 signaux supplémentaires, aucun signal manquant.

Parmi ces 21 exclusions, 15 correspondent à des frontières de session connues, mais 6 dépendent de minutes manquantes intrajournalières impossibles à connaître lors de la décision. Cela suffit à classer l'algorithme de développement `DEVELOPMENT_LOOKAHEAD_DEFECT`.

## Sous-contrats F0

| Contrat | Résultat | Écarts | Autorisation |
|---|---:|---:|---:|
| F0-A — compatibilité de décision développement | `LEGACY_COMPONENT_COMPATIBILITY_EXACT_BUT_SCIENTIFICALLY_CONTAMINATED` | 0 sur 2 052 signaux, entrées, sorties et trades sous le modèle legacy | Non |
| F0-B — batch/streaming sous le modèle forward | `NOT_RUN_CONTAMINATED_PACKAGE` | non applicable | Non |
| F0-C — état compte par barre sous le modèle forward | `NOT_RUN_CONTAMINATED_PACKAGE` | non applicable | Non |

F0-B/F0-C et le nouvel oracle par barre n'ont pas été fabriqués : supprimer le masque futur aurait créé une nouvelle politique de 2 073 signaux, tandis que le conserver aurait encodé le lookahead dans le moteur forward. Aucune de ces deux options ne respecte l'immuabilité des six livres.

Les modèles de fill sont désormais explicitement séparés :

- `DEVELOPMENT_FILL_MODEL`, hash `6d840dff2004f9976452ba8224c8c4f539e12dde879f43326513003e4808e623`, reproduit la clôture de la ligne suivante et la sortie historique ; il est réservé à la compatibilité legacy.
- `FORWARD_CONSERVATIVE_FILL_MODEL`, hash `77b53399932b436ec520371e3bb3a1c4748dc737b4f5d8f958bc178bb523c910`, définit exactement un tick adverse à l'entrée et à la sortie ; il n'a pas été exécuté pour ce package contaminé.

## Causes par manche

| Manche | Marché / horizon | Écarts | Première divergence | Classification |
|---|---:|---:|---|---|
| `sleeve_39eb74b174e7bdd240520b9d` | YM / 60m | 9 = 5 frontières + 4 trous imprévisibles | 2023-02-24 21:00Z, ligne 53 080 | mixte session + lookahead |
| `sleeve_65bad2088913fc9fca0a145d` | ES / 60m | 3 frontières | 2023-09-29 20:00Z, ligne 244 837 | frontière de session |
| `sleeve_7ecd76490fa9fb34e1af5820` | CL / 60m | 1 trou imprévisible | 2023-06-16 15:52Z, ligne 126 120 | lookahead |
| `sleeve_c5da4b5a67abadeb7d68eabe` | NQ / 30m | 6 frontières | 2023-01-16 17:35Z, ligne 13 531 | frontière de session |
| `sleeve_e017bb45b0937aef46657631` | NQ / 60m | 2 = 1 frontière + 1 trou imprévisible | 2023-06-01 20:00Z, ligne 140 864 | mixte session + lookahead |

Pour les 21 événements, le premier champ différent est exactement `eligibility.horizon_available` (`False` dans le legacy, `True` dans l'évaluation causale). Les vecteurs de features, triggers, contextes, sessions, contrats, rolls et états de position antérieurs concordent. Les 13 autres manches ont zéro divergence.

Les six trous imprévisibles sont : YM 2023-03-14 (20:22 absente), YM 2023-03-15 (20:31), CL 2023-06-16 (16:08), YM 2023-09-13 (premier trou 20:22), NQ 2024-03-13 (20:32) et YM 2024-09-18 (20:15). Aucun changement de contrat n'explique ces premières ruptures.

Un second défaut distinct affecte les 2 052 entrées legacy : `entry_time` vaut `decision_ns`, mais le prix attaché est `close.shift(-1)` et ne devient disponible qu'une minute plus tard. C'est un `AVAILABILITY_SEMANTICS_MISMATCH` qui peut changer l'ordre des conflits et l'exposition du gouverneur ; il ne justifie pas de rendre le fill forward identique au fill de développement.

Le détail exhaustif des 21 événements et les cinq traces causales est scellé dans `F0_ROOT_CAUSE_FORENSICS.json`, SHA-256 `87a499f8acf6489668d75826cdffd2d6776398c96b45e50f35db1dfd8df2fb1f`.

## Impact et quarantaine

- 21 / 2 052 omissions causales, soit 1,023 % ; 126 comparaisons de livres divergentes par réplication des 21 événements dans les six livres.
- Les cinq manches affectées représentent 884 / 2 052 signaux canoniques ; les trois manches touchées par un trou imprévisible en représentent 447.
- L'impact économique exact n'est pas identifiable : les 21 événements absents n'ont ni fill, ni PnL, ni MAE/MFE gelé et peuvent modifier conflits, exposition, MLL, cohérence et cible. Aucun delta de pass ou de PnL n'a été inventé.
- Les cinq manches affectées sont mises en quarantaine stricte ; les six livres contiennent les trois manches indiscutablement contaminées et sont donc tous bloqués.
- Core `active_pool_186a4177401aab223b0a21fa` et Backup `active_pool_014dffb40e99814612d78c51` restent inchangés, mais ne sont pas activables sous ce package.

## Tests d'ordre et de reprise

Les gardes déjà indépendantes passent pour : doublon, retard, ordre inversé, intervalle manquant, changement de session, roll et hash de checkpoint. Le test ciblé checkpoint/reprise legacy passe également. La réconciliation complète 18 bindings sous un nouveau noyau forward n'a pas été exécutée après la contamination : elle aurait validé une politique différente ou reproduit un état non causal. Aucun troisième garde ni régression large n'a été lancé.

## Reçu et service persistant

- Reçu terminal : `mission/state/operating_package_v1_parity/f0_single_source_engine_parity_receipt.json`.
- Hash du reçu : `d6c52ed16fb3310ccb61f8b2505ca046d7c0154a0b2e5877a9e1f79a6b76c3e7`.
- Commit de l'implémentation scellé dans le reçu : `d5936ee5ce42f94b01e4ee2d6aeef9c1b4c28a57`.
- Reçu d'autorisation `online_offline_equivalence_receipt.json` : absent.
- Service : `hydra-autonomous-mission.service`, V17, actif, un contrôleur et un writer, PID 2 944 445 au scellement.
- État persistant : `DEVELOPMENT_EVIDENCE_CONTAMINATED_FAIL_CLOSED`, prochaine vérification 2026-07-18 00:30:53Z.
- Mission DB, registre, strategy registry, cemetery et forward-bar DB : `PRAGMA integrity_check = ok`.
- Broker : 0 ; ordres : 0 ; delta Q4 : 0.

## Backlog et F1

- Barres post-freeze archivées : 690 sur 10 racines.
- Première clôture : 2026-07-16 15:07:00Z.
- Dernière clôture : 2026-07-16 16:15:00Z.
- Barres traitées économiquement : 0.
- Signaux : 0 ; fills virtuels : 0 ; mutations de compte : 0.
- Ledgers forward historiques : 48 événements de warm-up à zéro action, 8 par livre, inchangés.
- Sessions forward complètes admises : 0.
- `POST_FREEZE_BACKLOG_ACTIVATION` : non émis.
- F1 : `NOT_AUTHORIZED_DEVELOPMENT_EVIDENCE_CONTAMINATED`.

Le poll d'acquisition déjà parti entre le checkpoint et la maintenance a ajouté 100 barres et coûté exactement 0,000730156898 USD. Le cumul est 87,85289960354199 USD et le budget restant 37,147100396458015 USD. Ce dépassement ponctuel du « zéro nouvel achat » est déclaré, non masqué. Le verrou terminal empêche désormais tout nouvel appel d'acquisition avant le prochain contrôle et chaque contrôle reste sans achat tant que le reçu demeure terminal.

## Action autonome suivante

V17 maintient le service, les six livres et les 690 barres intacts, revalide le reçu terminal et reste sans acquisition, signal, fill ou mutation. Aucune activation F1 n'est possible pour `OPERATING_PACKAGE_V1` inchangé. Une reprise économique exigerait une nouvelle autorité explicite pour reconstruire et sélectionner un package causal — action interdite dans le présent sprint et donc non entreprise.
