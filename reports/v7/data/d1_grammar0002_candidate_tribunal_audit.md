# HYDRA V7 — Audit du tribunal D1 grammaire 0002

[HYDRA-V7] phase=4 step=3 verdict=NULL
gate=D1_GRAMMAR0002_CANDIDATES preuve=reports/v7/data/d1_grammar0002_candidate_tribunal_result.json#a5b6c8ac tests=2/2_cibles;659/659_dernier_global
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_D1=59.859323/60.00 N_trials=247892 burned=1
diff_validation=hydra/validation/v7_d1_grammar0002_candidate_tribunal.py,scripts/run_v7_d1_grammar0002_candidate_tribunal.py,tests/test_v7_d1_grammar0002_candidate_tribunal.py CONTRE=les_deux_meilleurs_PnL_pooles_sont_negatifs_apres_embargo_walk_forward
prochaine_action=tombstoner_les_classes_0002_et_preregistrer_une_nouvelle_hypothese_economique_distincte

## Entonnoir

- Structures : `8`
- Signaux : `1 289`
- Stage 1 : `4`
- Stage 2 : `2`
- Null suite : `6`
- DSR positifs : `0`
- Rejets BH : `0`
- `SIM_EXPLOIT` : `2`
- File shadow : `0`
- Accès Q4/gap : `0/0`
- Ordres broker : `0`

## Deux survivants aux coûts et nulls

`v7d1g2_cross_contract_participation_divergence_ES` conserve
`+22,588889 $/trade` à coûts ×1,5 et `+10,088889 $/trade` à ×2, mais son
walk-forward purgé/embargo est `-48,636957 $/trade` et son DSR `-4,432182`.

`v7d1g2_delta_extreme_rejection_ES` conserve `+41,557143 $/trade` à coûts
×1,5 et `+29,057143 $/trade` à ×2, mais son walk-forward est
`-2,133333 $/trade` et son DSR `-4,203801`.

Ces résultats montrent une économie poolée attractive mais une instabilité
temporelle incompatible avec les gates WORM. Ils ne sont ni promus ni envoyés
sur le gap frais.

## CONTRE

Avec seulement deux blocs d'août, le walk-forward et le DSR ont peu de puissance
pour distinguer une petite instabilité d'un véritable changement de régime.
Cette limite n'autorise toutefois ni baisse du gate ni consommation du gap.
