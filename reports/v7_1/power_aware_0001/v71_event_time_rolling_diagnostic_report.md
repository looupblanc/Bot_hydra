# HYDRA V7.1 — Event-time Rolling Combine diagnostic

[HYDRA-V7] phase=4 step=145 verdict=NULL
gate=V71_EVENT_TIME_ROLLING_DIAGNOSTIC preuve=reports/v7_1/power_aware_0001/v71_event_time_rolling_diagnostic_result.json#0c4203c0 tests=2_strategies_plus_1_shared_basket
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263604 burned=1
diff_validation=hydra/validation/v71_event_time_rolling_diagnostic.py CONTRE=trop_peu_de_starts_et_tripwire_GEOMETRY_ONLY
prochaine_action=freeze_diagnostic_and_keep_candidates_underpowered_pending_independent_confirmation

- Starts: `5` / minimum sérieux `24`
- Statut: `INSUFFICIENT_EPISODE_STARTS`
- fast_volume_completion_60m Q1/EOD: pass `0.4`, MLL `0.6`, progrès médian `-0.0993277777776756`
- high_rate_low_progress_60m Q1/EOD: pass `0.0`, MLL `0.4`, progrès médian `0.8540222222223205`
- Panier Q1/EOD: pass `0.0`, MLL `1.0`, progrès médian `-0.40398333333324665`

## CONTRE

Only a handful of 20-day starts exist in the two frozen one-month D1 blocks, and the grammar tripwire is GEOMETRY_ONLY; these account paths measure mechanics, not reliable pass probability.
