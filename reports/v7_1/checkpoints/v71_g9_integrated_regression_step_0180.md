# HYDRA V7.1 — G9 integrated regression

[HYDRA-V7] phase=4 step=180 verdict=NULL
gate=V71_G9_INTEGRATED_REGRESSION preuve=reports/v7_1/checkpoints/v71_g9_integrated_regression_evidence_step_0180.json#3220f965 tests=749/749
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263902 burned=1
diff_validation=hydra/validation/v71_aggressor_run_topology_funnel.py,hydra/validation/v71_aggressor_run_topology_tripwire.py CONTRE=tripwire_sous_puissant_et_formulations_possiblement_trop_etroites
prochaine_action=deploy_controller_v10_then_begin_v72_cross_fitted_account_synthesis_without_Q4_or_data_purchase

- G9 gelé : `4` structures, `1 573` signaux et `0` doublon.
- Stage 0 : `4`; Stage 1 : `0`; walk-forward positif : `0`; DSR/BH : `0`; Combine : `0`; shadow : `0`.
- Économie sous coûts `1,5x` : de `-16,38` à `-64,30 USD/trade` selon la formulation.
- Tripwire : réel `0/80`, null `9/240`, p binomiale exacte `1,0`; verdict `BLOCKED_UNDERPOWERED`.
- Décision terminale : `G9_FORMULATION_FALSIFIED` pour les quatre formulations exactes. La classe générale de topologie des runs n'est pas déclarée morte et aucune expansion G9 n'est autorisée.
- Store outcome-free : `17 200` minutes ES issues d'environ `33,98 M` points locaux; hash déterministe `f7edf987` sous deux tailles de chunks.
- Régression : `749/749`; tests no-lookahead/ruleset/Q4/budget ciblés : `55/55`; compileall vert.
- Mission DB : `ok`; registre : `ok`; gouvernance : verte; Q4 : `1`, déjà BURNED; ordres broker : `0`.
- Achat de données G9 : `0`; solde data : `37,152612 USD`.

## CONTRE

Le tripwire ne possède aucune réussite réelle et seulement neuf réussites nulles : il ne peut pas distinguer proprement une classe intrinsèquement nulle d'une formulation trop étroite. Le verdict est donc limité aux quatre graphes gelés; il ne prouve pas que toute information de topologie intraminute soit absente.
