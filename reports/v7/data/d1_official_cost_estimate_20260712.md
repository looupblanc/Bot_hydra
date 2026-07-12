# HYDRA V7 — Estimation officielle DATA_UPGRADE D1

[HYDRA-V7] phase=D step=1 verdict=BLOCKED
gate=D1 preuve=reports/v7/data/d1_official_cost_estimate_20260712.json#82169564 tests=metadata_only
budget_llm=0.000000/solde budget_data=0.000000/60.00 N_trials=246706 burned=1
diff_validation=aucun CONTRE=un_achat_evenementiel_plus_etroit_pourrait_etre_informatif_mais_ne_satisferait_pas_le_scope_WORM_D1
prochaine_action=continuer_P4_sur_OHLCV_1m_sans_achat_et_preregisterer_toute_future_reduction_de_scope

## Verdict

Le téléchargement D1 n'est pas exécuté. L'estimation officielle Databento pour
`ES.c.0 + MES.c.0`, schéma `trades`, du 1er janvier 2023 au 1er octobre 2024,
est de **357,490859 $** pour **285 604 827** enregistrements. Le plafond WORM
D1 est de **60 $**.

ES seul coûte 215,962954 $, MES seul 141,527905 $, et même une seule année des
deux contrats coûte 200,256988 $. Aucun cache `trades` correspondant n'existe.
Aucune donnée n'a été téléchargée et la dépense réelle est de 0 $.

## CONTRE

Un échantillonnage d'événements ciblés pourrait probablement tenir sous 60 $.
Mais choisir ces événements après les résultats OHLCV introduirait une sélection
supplémentaire et ne constituerait pas les deux à trois années D1 promises. Une
telle réduction de scope devra être une nouvelle hypothèse data WORM, jamais un
ajustement silencieux.
