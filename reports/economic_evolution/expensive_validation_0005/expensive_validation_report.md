[HYDRA-V7] phase=4 step=4477 verdict=NULL
gate=EV0005_DEVELOPMENT_VALIDATION preuve=reports/economic_evolution/expensive_validation_0005/expensive_validation_result.json#1c8796a3 tests=860/860
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.000000_USD N_trials=452628 burned=1
diff_validation=hydra/validation/economic_evolution_expensive_validation.py,tests/test_economic_evolution_expensive_validation.py CONTRE=la_politique_a_ete_selectionnee_sur_le_meme_developpement_et_154_jours_ne_donnent_aucune_confirmation_independante
prochaine_action=preregistrer_une_revue_failure_directed_au_niveau_classe_0006_sans_achat_de_donnees

# HYDRA Economic Evolution — validation coûteuse 0005

## Verdict

`account_policy_child_9a7e901bda15207c0e7ba25d` est classée
`EXPENSIVE_VALIDATION_UNDERPOWERED`.

La politique conserve une économie positive et ne breach pas le MLL dans les
quatre blocs de développement, y compris sous coûts multipliés par deux. Elle
ne satisfait toutefois ni le null de signes, ni DSR/BH, ni le seuil de
puissance final préenregistré. Elle n'est donc pas autorisée à consommer une
fenêtre indépendante, à entrer en shadow ou à hériter d'un statut de
promotion.

## Résultats account-level gelés

| Profil | Net groupé | Blocs positifs | Progression médiane | Progression max | Breach MLL | Buffer MLL min | Consistance |
|---|---:|---:|---:|---:|---:|---:|---:|
| Contrôleur, coûts normaux | 4 633,97 $ | 3/4 | 17,69 % | 28,39 % | 0/4 | 2 597,51 $ | 25 % |
| Contrôleur, coûts ×1,5 | 3 846,61 $ | 3/4 | 15,43 % | 24,63 % | 0/4 | 2 615,07 $ | 25 % |
| Contrôleur, coûts ×2 | 3 043,96 $ | 3/4 | 13,16 % | 22,10 % | 0/4 | 2 506,32 $ | 25 % |
| Statique, coûts ×1,5 | 3 707,34 $ | 3/4 | 15,43 % | 29,05 % | 0/4 | 1 967,50 $ | 50 % |

Le contrôleur n'est pas dominé au sens Pareto par le contrôle statique : il
gagne légèrement en net et protège mieux le buffer MLL, mais il réduit la
progression maximale et la consistance. Aucun retrait d'une sleeve ne domine
la politique complète.

## Statistique et puissance

- Observations quotidiennes : 154 ; effectif indépendant estimé : 154.
- Net quotidien moyen sous coûts ×1,5 : +24,98 $ ; médiane : 0 $.
- Bootstrap par blocs, IC 95 % : [−9,15 $ ; +61,41 $].
- Probabilité bootstrap d'une moyenne positive : 92,24 %.
- Null de signes par blocs : p unilatérale = 0,07658, donc non rejeté à 5 %.
- Sharpe annualisé observé : 1,4377 ; Sharpe maximal attendu après recherche : 4,4131.
- DSR : z = −2,4613 ; p unilatérale = 0,99308.
- BH, famille de 1 278 politiques : seuil rang 1 = 0,00007825 ; rejet = faux.
- Calibration : faux positifs null = 0 %, puissance sur +50 $/jour = 0 % après DSR/BH.
- Part du meilleur jour positif : 8,81 % ; net après retrait : +2 577,03 $.
- Inversion de signe : −5 772,04 $, donc l'orientation originale reste supérieure.

## Intégrité et incident de bootstrap

Les trois premiers sous-processus ont échoué avant import Python à cause de
l'absence de `PYTHONPATH`. Aucun validateur, donnée ou résultat n'avait alors
été chargé. La récupération unique est auditée dans
`reports/economic_evolution/expensive_validation_0005_bootstrap_recovery.json`
(sha256 `8ea75930`) et n'a ajouté aucun essai au registre.

- Compteur : 452 604 → 452 628, exactement une réservation de 24 comparaisons.
- Q4 : accès inchangé à 1 ; fenêtre `Q4_2024` toujours `BURNED`.
- Achat de données : 0.
- Fenêtre de preuve consommée : 0.
- Connexions broker : 0 ; ordres : 0.
- PRE_HOLDOUT_READY : 0 ; PAPER_SHADOW_READY : 0.

## CONTRE

Le signal économique brut est encourageant mais reste compatible avec une
sélection chanceuse parmi 1 278 politiques. L'IC recouvre zéro, le null de
signes n'est pas rejeté et la puissance finale est nulle sous le correctif de
multiplicité gelé. Utiliser maintenant une preuve fraîche gaspillerait une
fenêtre BURNED sans probabilité raisonnable de décision positive.

## Décision suivante

L'exacte politique est gelée comme composant de recherche sous-puissant ; elle
ne sera ni mutée paramétriquement depuis ce résultat ni envoyée en
confirmation. La mission prépare une revue 0006 au niveau classe pour choisir
entre davantage d'opportunités indépendantes, une représentation économique
différente ou l'abandon de cette classe, sans achat de données.
