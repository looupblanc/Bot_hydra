# HYDRA V7 — Phase 0 MLL divergence

[HYDRA-V7] phase=0 step=2 verdict=NULL
gate=G0 preuve=reports/v7/phase0_v2/phase0_result.json#pending tests=pending
budget_llm=0.000000/8.00 budget_data=0.000000/60.00 N_trials=pending_retro_estimate burned=1
diff_validation=hydra/account_policy/basket.py,hydra/account_policy/xfa.py,hydra/propfirm/combine_episode.py,hydra/propfirm/intraday_mll.py,hydra/propfirm/mll_variants.py,hydra/propfirm/topstep_150k.py,hydra/propfirm/trading_day.py,hydra/propfirm/xfa_episode.py,hydra/validation/v7_phase0_divergence.py,hydra/data/v7_manifest.py,config/rulesets/topstep_150k_v7.json,tests/ruleset/ CONTRE=The source paths contain OHLC extrema without their intrabar order; the intraday-HWM sensitivity therefore remains conservative and cannot resolve the human R2 conflict.
prochaine_action=exécuter_la_régression_totale_puis_rendre_le_verdict_G0

Validation/simulation diff: hydra/account_policy/basket.py, hydra/account_policy/xfa.py, hydra/propfirm/combine_episode.py, hydra/propfirm/intraday_mll.py, hydra/propfirm/mll_variants.py, hydra/propfirm/topstep_150k.py, hydra/propfirm/trading_day.py, hydra/propfirm/xfa_episode.py, hydra/validation/v7_phase0_divergence.py, hydra/data/v7_manifest.py, config/rulesets/topstep_150k_v7.json, tests/ruleset/

Historical classification: `HISTORICAL_DEFAULT_EQUIVALENT`.
Frozen scope: `55` baskets + `6` XFA policies.
Default episodes/pass/breach: `2640` / `0.451894` / `0.120455`.
Intraday-HWM episodes/pass/breach: `2640` / `0.429924` / `0.215530`.
Pass-rate delta: `-0.021970`; MLL-breach delta: `+0.095076`.
Terminal transitions: `251`; unresolved MFE/MAE order observations: `30987`.
XFA mean payout probability default/sensitivity: `0.888889` / `0.875000`.
Q4 BURNED: `True`; orders: `0`; paid spend: `$0.000000`.

These are development-only sensitivity results, not promotion evidence.

## CONTRE

The source paths contain OHLC extrema without their intrabar order; the intraday-HWM sensitivity therefore remains conservative and cannot resolve the human R2 conflict.
