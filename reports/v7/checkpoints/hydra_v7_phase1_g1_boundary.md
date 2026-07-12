[HYDRA-V7] phase=1 step=25 verdict=GREEN
gate=G1 preuve=reports/v7/phase1/null_tripwire_result.json#ea909645 tests=571/571
budget_llm=0.000000/12.00 budget_data=0.000000/0.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/validation/v7_null_tripwire.py,scripts/run_v7_null_tripwire.py,tests/ruleset/test_v7_null_tripwire.py CONTRE=le_signal_conditionnel_peut_encore_refléter_la_sélection_multiple
prochaine_action=construire_P2_sans_génération_et_appliquer_DSR_BH_FDR_10pct

Justification (clauses 1–5 et 9) : le seuil 0,8 et les seeds étaient WORM ; le résultat brut et son contre-argument sont conservés ; aucune preuve fraîche n'a été consommée.
Auto-audit : le risque principal est de confondre rejet du null géométrique et preuve d'un edge malgré 115 388 essais de sélection.
