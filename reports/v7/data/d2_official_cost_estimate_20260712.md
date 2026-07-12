# HYDRA V7 — D2 mbp-10 official cost estimate

[HYDRA-V7] phase=D step=101 verdict=GREEN
gate=D2_OFFICIAL_COST_CHECK preuve=WORM/v7-d2-mbp10-acquisition-plan-2026-07-12.json#6cf55c62 tests=API_officielle_sans_achat
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_D2_projete=27.394909/28.00_reserve=9.757703 N_trials=247892 burned=1
diff_validation=aucun CONTRE=un_mois_RTH_et_des_sessions_creuses_ne_couvrent_pas_plusieurs_regimes_de_book
prochaine_action=committer_au_moins_trois_fiches_WORM_de_profondeur_avant_ingestion

Coût officiel estimé : **27,394908636809 $** ; fenêtre retenue : **22 sessions RTH ES mbp-10, du 2026-06-11 08:30 CT au 2026-07-10 15:10 CT**.

## CONTRE

La fenêtre est la plus longue sous le cap de 28 $, mais elle ne couvre qu'un mois et certaines sessions de rollover ou fériées sont très creuses ; un verdict négatif ne suffira donc pas à épuiser toutes les classes de profondeur.
