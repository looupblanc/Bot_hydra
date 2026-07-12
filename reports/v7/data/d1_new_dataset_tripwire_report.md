# HYDRA V7 — D1 new-dataset tripwire

[HYDRA-V7] phase=D step=4 verdict=GREEN_NULL_ADJUSTED_BASELINE
gate=D1_TRIPWIRE preuve=reports/v7/data/d1_new_dataset_tripwire_result.json#99caf188 tests=642/642
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials=247684 burned=1
diff_validation=hydra/validation/v7_d1_new_dataset_tripwire.py,scripts/run_v7_d1_new_dataset_tripwire.py,tests/test_v7_d1_new_dataset_tripwire.py CONTRE=deux_blocs_aout_limitent_la_portee_du_tripwire
prochaine_action=run_separately_committed_D1_candidate_tribunal

- NULL_RATIO : `0.000000000000`
- Episodes réels : `160`
- Passes réelles : `6`
- Episodes null : `480`
- Passes null : `0`
- Combine : diagnostic uniquement, jamais fitness ni gate candidat.

## CONTRE

The tripwire has only two matched August year blocks. A GREEN verdict would reject gross account geometry but would not establish seasonal or forward stability of any candidate.
