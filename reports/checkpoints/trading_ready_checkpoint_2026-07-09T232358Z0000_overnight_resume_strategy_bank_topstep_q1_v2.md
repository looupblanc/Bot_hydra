# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T23:23:58+00:00

- runtime_seconds: 11536.43
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 3
- candidates_tested_this_run: 12000
- total_candidates_in_registry: 15263
- target_80000_progress: 0.190787
- trading_ready_count: 0
- economically_viable_count: 351
- topstep_viable_count: 17
- near_miss_count: 137
- target_50_reached: False
- status_distribution: {'DEAD_STRATEGY': 12758, 'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'ECONOMICALLY_VIABLE': 351, 'PROMISING_NEEDS_MUTATION': 112, 'TOPSTEP_NEAR_MISS': 25, 'TOPSTEP_VIABLE': 17}
- failure_reasons: {'no_economic_signal': 11440, 'combine_profit_target_not_reached': 1357, 'fragile_trade_order': 1246, 'combine_mll_breached': 676, 'viable_only_in_one_split': 245, 'weak_but_mutatable_economic_profile': 232, 'duplicate_or_near_duplicate_equity_curve': 39, 'march_oos_weak': 17, 'combine_target_not_reached': 8, 'reshuffle_robustness_soft_fail': 2, 'funded_mll_or_tail_failure': 1}
- promotion_gate_distribution: {'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 13243, 'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 13230, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 13228, 'OOS:SOFT_FAIL:march_oos_weak': 13200, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 12058, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 11440, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 8238, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 8087, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 7297, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 5149, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 1531, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 123, 'CORRELATION:SOFT_FAIL:high_correlation_needs_portfolio_role': 42, 'CORRELATION:HARD_FAIL:duplicate_or_near_duplicate_equity_curve': 39, 'TOPSTEP_COMBINE:SOFT_FAIL:topstep_near_miss_target_velocity': 3}
- target_reached_count: 24
- mll_respected_count: 6533
- consistency_respected_count: 13876
- funded_survival_count: 6375
- payout_eligible_count: 284
- payout_cycles_survived: 328
- gross_payout_estimate: 526245.1168923106
- trader_net_payout_estimate: 473620.6052030796
- best_topstep_score: 1.0
- best_promotion_score: 0.884426
- best_economic_score: 0.906913
- best_families: {'topstep_nq_es_divergence_controlled': 268, 'topstep_opening_range_controlled_runner': 27, 'topstep_volatility_expansion_limited_risk': 2}
- near_miss_map: {'topstep_nq_es_divergence_controlled_v2': 85, 'near_miss_adaptive_mutator': 46, 'creative_market_representation_lane': 3, 'topstep_vwap_exhaustion_payout_engine_v2': 2, 'topstep_volatility_expansion_limited_risk_v2': 1}
- branches_to_kill: ['topstep_nq_es_divergence_controlled_v2', 'consistency_safe_runner_v1', 'payout_cycle_smooth_climber_v1', 'topstep_volatility_expansion_limited_risk_v2', 'creative_market_representation_lane', 'topstep_vwap_exhaustion_payout_engine_v2', 'portfolio_diversification_lane', 'topstep_opening_range_controlled_runner_v2', 'near_miss_adaptive_mutator', 'topstep_prior_level_reclaim_smooth_pnl_v2']
- branches_to_expand: ['topstep_nq_es_divergence_controlled_v2', 'near_miss_adaptive_mutator', 'topstep_opening_range_controlled_runner_v2', 'portfolio_diversification_lane', 'creative_market_representation_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2
