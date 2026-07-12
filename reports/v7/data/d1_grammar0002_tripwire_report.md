# HYDRA V7 — D1 grammar 0002 tripwire

[HYDRA-V7] phase=4 step=1 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=D1_GRAMMAR0002_TRIPWIRE preuve=reports/v7/data/d1_grammar0002_tripwire_result.json#119b58e9 tests=659/659
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials=247892 burned=1
diff_validation=hydra/validation/v7_d1_grammar0002_tripwire.py,scripts/run_v7_d1_grammar0002_tripwire.py,tests/test_v7_d1_grammar0002_tripwire.py CONTRE=deux_blocs_aout_ne_prouvent_pas_la_stabilite
prochaine_action=run_separately_committed_grammar0002_candidate_tribunal

- Real episodes: `160`
- Real passes: `8`
- Null episodes: `480`
- Null passes: `10`
- NULL_RATIO: `0.41666666666666663`

## CONTRE

Only two August blocks are available. A GREEN tripwire rejects gross account geometry but does not validate any candidate edge.
