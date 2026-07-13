# HYDRA V7.1 — G7 integrated falsification deployment

[HYDRA-V7] phase=4 step=171 verdict=ARTEFACT
gate=V71_G7_INTEGRATED_DEPLOYMENT preuve=reports/v7_1/checkpoints/v71_g7_integrated_deployment_evidence_step_0171.json#771098f3 tests=731/731
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263844 burned=1
diff_validation=hydra/validation/v71_flow_sign_sequence_funnel.py,hydra/validation/v71_flow_sign_sequence_tripwire.py CONTRE=deux_blocs_D1_ne_falsifient_que_la_classe_ordinale_preregistered
prochaine_action=preregister_hydra_v7_1_independent_confirmation_and_distinct_hypothesis_review_0008_without_data_purchase

- Gate de puissance finale inchangé : `80%`.
- Population WF réconciliée : `22/22`; sous-puissants `16`, fragiles `4`, terminaux géométrie `2`.
- Diagnostic Combine sous-puissant : `5` candidats, `24` starts, `4` blocs effectifs, `0` pass candidat, `0` pass panier, `0` promotion.
- G7 : `6` structures, `1 889` signaux, `0` doublon, `0` Stage-1, `0` WF positif.
- Tripwire G7 : réel `5/120`, null `17/360`, `NULL_RATIO=1.133333`, verdict `ARTEFACT_GEOMETRY_ONLY`.
- Classe G7 tombstonée : `6`; aucun audit de puissance, Combine, shadow ou holdout.
- Données achetées : `0`; budget restant : `37.152612 USD`.
- Service persistant : actif, PID `456785`, contrôleur v8, un writer, zéro ordre.

## CONTRE

Ce null propre ne falsifie pas toute séquence order-flow imaginable : il falsifie la classe ordinale exactement pré-enregistrée sur seulement deux blocs calendaires D1. Les candidats positifs antérieurs restent sous le gate final de puissance inchangé.
