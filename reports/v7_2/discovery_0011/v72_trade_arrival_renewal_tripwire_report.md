# HYDRA V7.2 — Trade-arrival renewal permanent tripwire

[HYDRA-V7] phase=4 step=200 verdict=ARTEFACT_GEOMETRY_ONLY
gate=V72_G11_TRIPWIRE preuve=reports/v7_2/discovery_0011/v72_trade_arrival_renewal_tripwire_result.json#80a4c4a9 tests=real_vs_3_null_worlds
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=265227 burned=1
diff_validation=hydra/validation/v72_trade_arrival_renewal_tripwire.py,tests/test_v72_trade_arrival_renewal_tripwire.py CONTRE=les_timestamps_invariants_peuvent_conserver_la_saisonnalite
prochaine_action=tombstone_trade_arrival_renewal_class_as_geometry_only

- Réel: `6/480`
- Null: `45/1440`
- NULL_RATIO: `2.500000`
- Force: `ARTEFACT`

## CONTRE

Arrival timestamps are intentionally invariant in the price nulls, so time-of-day seasonality can remain; raw counts and exact-binomial evidence must be read separately from candidate economics.
