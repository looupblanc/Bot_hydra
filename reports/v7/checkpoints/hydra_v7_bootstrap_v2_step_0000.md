[HYDRA-V7] phase=0 step=0 verdict=NULL
gate=BOOTSTRAP_V2 preuve=MISSION_CONTRACT.md#35cca363 tests=557/557(preliminary)
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/account_policy/basket.py,hydra/account_policy/xfa.py,hydra/propfirm/combine_episode.py,hydra/propfirm/intraday_mll.py,hydra/propfirm/mll_variants.py,hydra/propfirm/topstep_150k.py,hydra/propfirm/xfa_episode.py,hydra/validation/v7_phase0_divergence.py,tests/ruleset/ CONTRE=le résultat P0 préliminaire peut masquer une erreur de journée CT et aucun cutoff de data lake n'est encore publié
prochaine_action=publier_le_manifest_avant_toute_ingestion_puis_encoder_R16_sans_génération

Justification (clauses 2, 4 et 5) : le résultat antérieur n'est pas déclaré vert ; les changements restent exclusivement dans la validation ; aucune fenêtre du gap n'est lue avant son manifest.
Auto-audit : le risque dominant est de confondre la fin nominale d'une requête Databento avec le dernier timestamp réellement présent.
