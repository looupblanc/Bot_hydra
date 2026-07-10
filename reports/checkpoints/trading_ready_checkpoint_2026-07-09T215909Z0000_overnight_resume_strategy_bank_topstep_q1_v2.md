# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T21:59:09+00:00

- runtime_seconds: 6447.1
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 3
- candidates_tested_this_run: 8700
- total_candidates_in_registry: 11963
- target_80000_progress: 0.149537
- trading_ready_count: 0
- economically_viable_count: 270
- topstep_viable_count: 13
- near_miss_count: 97
- target_50_reached: False
- status_distribution: {'DEAD_STRATEGY': 9583, 'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'ECONOMICALLY_VIABLE': 270, 'PROMISING_NEEDS_MUTATION': 78, 'TOPSTEP_NEAR_MISS': 19, 'TOPSTEP_VIABLE': 13}
- failure_reasons: {'no_economic_signal': 8616, 'combine_profit_target_not_reached': 1357, 'fragile_trade_order': 921, 'combine_mll_breached': 669, 'viable_only_in_one_split': 182, 'weak_but_mutatable_economic_profile': 181, 'duplicate_or_near_duplicate_equity_curve': 20, 'march_oos_weak': 11, 'combine_target_not_reached': 3, 'reshuffle_robustness_soft_fail': 2, 'funded_mll_or_tail_failure': 1}
- promotion_gate_distribution: {'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 9945, 'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 9943, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 9934, 'OOS:SOFT_FAIL:march_oos_weak': 9919, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 9069, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 8616, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 6189, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 6079, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 5514, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 3867, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 1134, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 92, 'CORRELATION:HARD_FAIL:duplicate_or_near_duplicate_equity_curve': 20, 'CORRELATION:SOFT_FAIL:high_correlation_needs_portfolio_role': 19, 'TOPSTEP_COMBINE:SOFT_FAIL:topstep_near_miss_target_velocity': 2}
- target_reached_count: 15
- mll_respected_count: 5241
- consistency_respected_count: 10905
- funded_survival_count: 5124
- payout_eligible_count: 217
- payout_cycles_survived: 247
- gross_payout_estimate: 383870.63216602267
- trader_net_payout_estimate: 345483.5689494204
- best_topstep_score: 1.0
- best_promotion_score: 0.884426
- best_economic_score: 0.906913
- best_families: {'topstep_nq_es_divergence_controlled': 206, 'topstep_opening_range_controlled_runner': 18, 'topstep_volatility_expansion_limited_risk': 1}
- near_miss_map: {'topstep_nq_es_divergence_controlled_v2': 57, 'near_miss_adaptive_mutator': 34, 'creative_market_representation_lane': 3, 'topstep_vwap_exhaustion_payout_engine_v2': 2, 'topstep_volatility_expansion_limited_risk_v2': 1}
- branches_to_kill: ['topstep_nq_es_divergence_controlled_v2', 'consistency_safe_runner_v1', 'topstep_volatility_expansion_limited_risk_v2', 'payout_cycle_smooth_climber_v1', 'creative_market_representation_lane', 'portfolio_diversification_lane', 'topstep_vwap_exhaustion_payout_engine_v2', 'topstep_opening_range_controlled_runner_v2', 'near_miss_adaptive_mutator', 'topstep_prior_level_reclaim_smooth_pnl_v2']
- branches_to_expand: ['topstep_nq_es_divergence_controlled_v2', 'near_miss_adaptive_mutator', 'portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2
