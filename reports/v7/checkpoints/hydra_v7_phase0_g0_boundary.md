[HYDRA-V7] phase=0 step=25 verdict=GREEN
gate=G0 preuve=reports/v7/phase0_v2/phase0_result.json#8de76b03 tests=565/565
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1
diff_validation=config/rulesets/topstep_150k_v7.json,hydra/account_policy/,hydra/data/v7_manifest.py,hydra/execution/v7_cost_model.py,hydra/propfirm/,hydra/validation/v7_phase0_divergence.py,scripts/,tests/ruleset/ CONTRE=R2_non_tranché_et_ordre_intrabar_non_identifié
prochaine_action=préenregistrer_P1_et_ses_seeds_avant_tout_null

Justification (clauses 1, 2, 4 et 5) : G0 atteste seulement la fidélité du simulateur et du manifest ; la génération reste gelée ; aucune fenêtre fraîche n'a été lue.
Auto-audit : le moyen le plus probable de se tromper est de prendre l'équivalence historique pour une validation économique alors qu'elle ne corrige ni le null ni la multiplicité.
