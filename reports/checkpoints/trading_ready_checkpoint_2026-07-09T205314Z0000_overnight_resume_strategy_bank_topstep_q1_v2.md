# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T20:53:14+00:00

- runtime_seconds: 2492.9
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 3
- candidates_tested_this_run: 5300
- total_candidates_in_registry: 8563
- target_80000_progress: 0.107037
- trading_ready_count: 0
- economically_viable_count: 180
- topstep_viable_count: 8
- near_miss_count: 59
- target_50_reached: False
- status_distribution: {'DEAD_STRATEGY': 6316, 'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'ECONOMICALLY_VIABLE': 180, 'PROMISING_NEEDS_MUTATION': 46, 'TOPSTEP_NEAR_MISS': 13, 'TOPSTEP_VIABLE': 8}
- failure_reasons: {'no_economic_signal': 5712, 'combine_profit_target_not_reached': 1357, 'combine_mll_breached': 662, 'fragile_trade_order': 573, 'viable_only_in_one_split': 122, 'weak_but_mutatable_economic_profile': 116, 'duplicate_or_near_duplicate_equity_curve': 12, 'march_oos_weak': 6, 'reshuffle_robustness_soft_fail': 1, 'funded_mll_or_tail_failure': 1, 'combine_target_not_reached': 1}
- promotion_gate_distribution: {'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 6552, 'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 6549, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 6542, 'OOS:SOFT_FAIL:march_oos_weak': 6534, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 5979, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 5712, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 4089, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 4018, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 3684, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 2533, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 709, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 60, 'CORRELATION:HARD_FAIL:duplicate_or_near_duplicate_equity_curve': 12, 'CORRELATION:SOFT_FAIL:high_correlation_needs_portfolio_role': 11, 'TOPSTEP_COMBINE:SOFT_FAIL:topstep_near_miss_target_velocity': 1}
- target_reached_count: 11
- mll_respected_count: 3902
- consistency_respected_count: 7840
- funded_survival_count: 3824
- payout_eligible_count: 150
- payout_cycles_survived: 169
- gross_payout_estimate: 265260.76003684604
- trader_net_payout_estimate: 238734.68403316144
- best_topstep_score: 0.806147
- best_promotion_score: 0.814577
- best_economic_score: 0.867094
- best_families: {'topstep_nq_es_divergence_controlled': 128, 'topstep_opening_range_controlled_runner': 14, 'topstep_volatility_expansion_limited_risk': 1}
- near_miss_map: {'topstep_nq_es_divergence_controlled_v2': 32, 'near_miss_adaptive_mutator': 21, 'creative_market_representation_lane': 3, 'topstep_vwap_exhaustion_payout_engine_v2': 2, 'topstep_volatility_expansion_limited_risk_v2': 1}
- branches_to_kill: ['topstep_nq_es_divergence_controlled_v2', 'consistency_safe_runner_v1', 'topstep_volatility_expansion_limited_risk_v2', 'payout_cycle_smooth_climber_v1', 'creative_market_representation_lane', 'topstep_opening_range_controlled_runner_v2', 'portfolio_diversification_lane', 'topstep_vwap_exhaustion_payout_engine_v2', 'near_miss_adaptive_mutator', 'topstep_prior_level_reclaim_smooth_pnl_v2']
- branches_to_expand: ['topstep_nq_es_divergence_controlled_v2', 'near_miss_adaptive_mutator', 'portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2
