# HYDRA V7.2 — Rolling Combine semantics audit

[HYDRA-V7] phase=4 step=182 verdict=GREEN
gate=V72_COMBINE_SEMANTICS preuve=reports/v7_2/semantics/v72_combine_semantics_audit_result.json#08200f30 tests=ruleset_plus_censoring_probes
budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263902 burned=1
diff_validation=hydra/propfirm/censored_combine.py,hydra/validation/v72_combine_semantics_audit.py CONTRE=couverture_D1_trop_courte_pour_horizons_longs
prochaine_action=freeze_reconciled_component_bank_before_basket_results

- Timeout officiel: `aucun`
- Horizons gelés: `20/40/60/90/full_available`
- Profitable encore vivant à l'horizon: `censuré, jamais échec`
- Variantes MLL: `eod_level_rt_breach` et `intraday_hwm`
- Ordres broker: `0`

## CONTRE

The existing D1 coverage is too short for uncensored 40/60/90-day headlines on every frozen block; V7.2 can measure leakage-free short-horizon progress, but confirmation still requires untouched or forward data.
