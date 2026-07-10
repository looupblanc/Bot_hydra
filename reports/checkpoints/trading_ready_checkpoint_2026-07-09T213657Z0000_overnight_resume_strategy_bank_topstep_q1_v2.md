# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T21:36:57+00:00

- runtime_seconds: 5115.22
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 3
- candidates_tested_this_run: 7700
- total_candidates_in_registry: 10963
- target_80000_progress: 0.137038
- trading_ready_count: 0
- economically_viable_count: 246
- topstep_viable_count: 10
- near_miss_count: 85
- target_50_reached: False
- status_distribution: {'DEAD_STRATEGY': 8622, 'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'ECONOMICALLY_VIABLE': 246, 'PROMISING_NEEDS_MUTATION': 67, 'TOPSTEP_NEAR_MISS': 18, 'TOPSTEP_VIABLE': 10}
- failure_reasons: {'no_economic_signal': 7766, 'combine_profit_target_not_reached': 1357, 'fragile_trade_order': 814, 'combine_mll_breached': 668, 'viable_only_in_one_split': 164, 'weak_but_mutatable_economic_profile': 163, 'duplicate_or_near_duplicate_equity_curve': 17, 'march_oos_weak': 9, 'reshuffle_robustness_soft_fail': 2, 'combine_target_not_reached': 2, 'funded_mll_or_tail_failure': 1}
- promotion_gate_distribution: {'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 8946, 'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 8945, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 8937, 'OOS:SOFT_FAIL:march_oos_weak': 8921, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 8162, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 7766, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 5573, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 5473, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 4984, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 3475, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 1006, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 84, 'CORRELATION:SOFT_FAIL:high_correlation_needs_portfolio_role': 18, 'CORRELATION:HARD_FAIL:duplicate_or_near_duplicate_equity_curve': 17, 'TOPSTEP_COMBINE:SOFT_FAIL:topstep_near_miss_target_velocity': 2}
- target_reached_count: 13
- mll_respected_count: 4847
- consistency_respected_count: 10004
- funded_survival_count: 4740
- payout_eligible_count: 203
- payout_cycles_survived: 231
- gross_payout_estimate: 361318.5476996394
- trader_net_payout_estimate: 325186.69292967545
- best_topstep_score: 0.897938
- best_promotion_score: 0.846643
- best_economic_score: 0.892537
- best_families: {'topstep_nq_es_divergence_controlled': 179, 'topstep_opening_range_controlled_runner': 16, 'topstep_volatility_expansion_limited_risk': 1}
- near_miss_map: {'topstep_nq_es_divergence_controlled_v2': 49, 'near_miss_adaptive_mutator': 30, 'creative_market_representation_lane': 3, 'topstep_vwap_exhaustion_payout_engine_v2': 2, 'topstep_volatility_expansion_limited_risk_v2': 1}
- branches_to_kill: ['topstep_nq_es_divergence_controlled_v2', 'consistency_safe_runner_v1', 'topstep_volatility_expansion_limited_risk_v2', 'payout_cycle_smooth_climber_v1', 'creative_market_representation_lane', 'topstep_opening_range_controlled_runner_v2', 'portfolio_diversification_lane', 'topstep_vwap_exhaustion_payout_engine_v2', 'near_miss_adaptive_mutator', 'topstep_prior_level_reclaim_smooth_pnl_v2']
- branches_to_expand: ['topstep_nq_es_divergence_controlled_v2', 'near_miss_adaptive_mutator', 'portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2
