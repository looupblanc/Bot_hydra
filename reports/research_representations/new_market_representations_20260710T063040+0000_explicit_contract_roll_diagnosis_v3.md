# new_market_representations_20260710T063040+0000_explicit_contract_roll_diagnosis_v3.md

Historical research only. No live trading approval.

```json
[
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "paired OHLCV plus explicit roll map",
    "economic_mechanism": "Past-window hedge ratio residual between synchronized NQ and ES contracts.",
    "execution_requirement": "market or synthetic paired execution; no signal inside roll exclusion",
    "expected_failure_regime": "macro directional breakouts where both legs trend without convergence",
    "expected_regime": "high correlation regimes with temporary index composition dislocation",
    "falsification_test": "Q1/Q2 residual sign and magnitude must transfer after excluding roll windows",
    "hypothesis": "Mean-reverting relative overextension after synchronized roll filtering.",
    "likely_topstep_role": "portfolio diversifier / relative-value sleeve",
    "minimal_parameter_count": 5,
    "name": "roll_aware_beta_neutral_nq_es_residual_divergence",
    "roll_sensitivity": "high"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "paired OHLCV",
    "economic_mechanism": "Rolling beta estimated only from prior bars and applied to current residual.",
    "execution_requirement": "requires spread slippage model",
    "expected_failure_regime": "beta instability and event shocks",
    "expected_regime": "stable intraday covariance and moderate volatility",
    "falsification_test": "beta residual should outperform raw NQ momentum and survive parameter plateaus",
    "hypothesis": "Changing NQ/ES beta creates exploitable residual only when estimation is stable.",
    "likely_topstep_role": "low-directional-beta Topstep candidate",
    "minimal_parameter_count": 6,
    "name": "dynamic_hedge_ratio_relative_value",
    "roll_sensitivity": "high"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV session features",
    "economic_mechanism": "Overnight range/location pressure resolved during early RTH.",
    "execution_requirement": "marketable orders during liquid RTH only",
    "expected_failure_regime": "quiet overnight sessions and news shock opens",
    "expected_regime": "large overnight imbalance with early confirmation",
    "falsification_test": "January-derived rules must transfer month-to-month without one-day concentration",
    "hypothesis": "Overnight positioning can unwind or continue after cash open.",
    "likely_topstep_role": "controlled opening-session climber",
    "minimal_parameter_count": 4,
    "name": "overnight_inventory_rth_resolution",
    "roll_sensitivity": "medium"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV RTH features",
    "economic_mechanism": "Opening displacement that fails to maintain auction direction.",
    "execution_requirement": "strict cutoff and same-bar conservative fills",
    "expected_failure_regime": "trend days with persistent breadth",
    "expected_regime": "first 60-120 RTH minutes",
    "falsification_test": "must reduce worst-day loss versus naive opening breakout",
    "hypothesis": "Failed continuation traps early momentum and creates bounded reversal.",
    "likely_topstep_role": "Topstep consistency-safe short horizon",
    "minimal_parameter_count": 5,
    "name": "opening_auction_displacement_failed_continuation",
    "roll_sensitivity": "low"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV volatility windows",
    "economic_mechanism": "Transition in realized-vol shape, not simple range expansion.",
    "execution_requirement": "market orders with volatility-scaled stops",
    "expected_failure_regime": "headline volatility and whipsaw expansion",
    "expected_regime": "compression resolving into directional liquidity",
    "falsification_test": "shape feature must add value over range_expansion alone",
    "hypothesis": "Convexity/decay profile distinguishes actionable expansion from chop.",
    "likely_topstep_role": "target velocity with MLL control",
    "minimal_parameter_count": 6,
    "name": "volatility_shape_transition",
    "roll_sensitivity": "low"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "paired OHLCV and explicit rolls",
    "economic_mechanism": "Lagged ES/NQ confirmation only in stable covariance states.",
    "execution_requirement": "requires synchronized contract pair",
    "expected_failure_regime": "roll windows and high-vol news",
    "expected_regime": "moderate vol and high index correlation",
    "falsification_test": "lagged feature must beat simultaneous correlation proxy",
    "hypothesis": "Lead-lag appears only when one index reprices first under constrained vol.",
    "likely_topstep_role": "relative-value signal filter",
    "minimal_parameter_count": 6,
    "name": "cross_market_lead_lag_conditioned_on_vol_regime",
    "roll_sensitivity": "high"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV path geometry",
    "economic_mechanism": "Where the range migrates inside the session and how price spends time near extremes.",
    "execution_requirement": "time stop plus daily lock",
    "expected_failure_regime": "featureless rotational days",
    "expected_regime": "structured trend or failed trend days",
    "falsification_test": "ablation must show path terms matter beyond momentum",
    "hypothesis": "Path asymmetry can identify controlled continuation versus exhaustion.",
    "likely_topstep_role": "session role / diversifier",
    "minimal_parameter_count": 5,
    "name": "intraday_range_migration_path_asymmetry",
    "roll_sensitivity": "low"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV sessions and calendar",
    "economic_mechanism": "State changes around Globex/RTH boundaries with no post-cutoff holds.",
    "execution_requirement": "strict session scheduler",
    "expected_failure_regime": "holiday/low-volume sessions",
    "expected_regime": "session boundary repricing",
    "falsification_test": "must transfer across months and respect 3:10 CT flatten",
    "hypothesis": "Liquidity regime change creates repeatable but time-bounded opportunity.",
    "likely_topstep_role": "low-overlap portfolio component",
    "minimal_parameter_count": 4,
    "name": "session_transition_state_models",
    "roll_sensitivity": "medium"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV expansion and path confirmation",
    "economic_mechanism": "Expansion that fails after limited continuation, with tail-risk stop.",
    "execution_requirement": "conservative same-bar ordering",
    "expected_failure_regime": "true trend days",
    "expected_regime": "false breakout regimes",
    "falsification_test": "must improve MLL buffer versus raw reversal",
    "hypothesis": "Breakout failure can be monetized if tail exposure is capped.",
    "likely_topstep_role": "MLL-safe reversal sleeve",
    "minimal_parameter_count": 5,
    "name": "failed_directional_expansion_controlled_tail",
    "roll_sensitivity": "low"
  },
  {
    "available_information": "Only past and current completed OHLCV bars plus explicit contract map; no future roll or Q4 information.",
    "data_requirement": "OHLCV plus portfolio scheduler",
    "economic_mechanism": "Micro-first sizing across MES/MNQ to tune account-level risk.",
    "execution_requirement": "micro contracts only until safety proven",
    "expected_failure_regime": "large gap days and high correlation stops",
    "expected_regime": "low-to-moderate opportunity regimes",
    "falsification_test": "portfolio scheduler must beat standalone naive sum under shared MLL",
    "hypothesis": "Fine sizing can preserve MLL while combining small independent edges.",
    "likely_topstep_role": "one-account risk smoother",
    "minimal_parameter_count": 5,
    "name": "mes_mnq_micro_first_portfolio_roles",
    "roll_sensitivity": "medium"
  }
]
```
