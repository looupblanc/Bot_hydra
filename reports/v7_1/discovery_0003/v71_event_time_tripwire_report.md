# HYDRA V7.1 — Event-time grammar tripwire

[HYDRA-V7] phase=4 step=142 verdict=ARTEFACT_GEOMETRY_ONLY
gate=V71_G3_TRIPWIRE preuve=reports/v7_1/discovery_0003/v71_event_time_tripwire_result.json#ae22d7a4 tests=128_real_plus_384_null_paths
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263508 burned=1
diff_validation=hydra/validation/v71_event_time_tripwire.py CONTRE=deux_blocs_D1_limitent_la_stabilite
prochaine_action=freeze_event_time_grammar_as_geometry_contaminated

- Réel: `110/2560`
- Null: `269/7680`
- NULL_RATIO: `0.8151515151515152`
- P-value exacte unilatérale: `0.01881859886601479`
- Force: `ARTEFACT`

## CONTRE

Event duration and aggressor flow are preserved in the price nulls; the tripwire isolates directional relation to price but cannot prove stability beyond the two available D1 calendar blocks.
