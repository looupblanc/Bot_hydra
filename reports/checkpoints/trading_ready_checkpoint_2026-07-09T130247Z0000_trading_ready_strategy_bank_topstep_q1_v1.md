# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T13:02:47+00:00

- runtime_seconds: 228.61
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 1
- candidates_tested_this_run: 600
- total_candidates_in_registry: 2600
- target_80000_progress: 0.0325
- trading_ready_count: 0
- economically_viable_count: 14
- topstep_viable_count: 1
- near_miss_count: 3
- target_50_reached: False
- status_distribution: {'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'DEAD_STRATEGY': 582, 'ECONOMICALLY_VIABLE': 14, 'PROMISING_NEEDS_MUTATION': 3, 'TOPSTEP_VIABLE': 1}
- failure_reasons: {'combine_profit_target_not_reached': 1357, 'combine_mll_breached': 644, 'no_economic_signal': 534, 'fragile_trade_order': 47, 'weak_but_mutatable_economic_profile': 10, 'viable_only_in_one_split': 7, 'march_oos_weak': 1}
- promotion_gate_distribution: {'OOS:SOFT_FAIL:march_oos_weak': 600, 'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 599, 'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 599, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 598, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 568, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 534, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 397, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 391, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 372, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 209, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 57, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 2}
- target_reached_count: 0
- mll_respected_count: 1566
- consistency_respected_count: 2446
- funded_survival_count: 1553
- payout_eligible_count: 27
- payout_cycles_survived: 31
- gross_payout_estimate: 36559.005344553596
- trader_net_payout_estimate: 32903.10481009824
- best_topstep_score: 0.687537
- best_promotion_score: 0.646813
- best_economic_score: 0.756091
- best_families: {'topstep_nq_es_divergence_controlled': 8, 'topstep_opening_range_controlled_runner': 3}
- near_miss_map: {'creative_market_representation_lane': 2, 'topstep_vwap_exhaustion_payout_engine_v2': 1}
- branches_to_kill: ['topstep_opening_range_controlled_runner_v2', 'topstep_nq_es_divergence_controlled_v2', 'topstep_volatility_expansion_limited_risk_v2', 'topstep_prior_level_reclaim_smooth_pnl_v2', 'payout_cycle_smooth_climber_v1', 'near_miss_adaptive_mutator', 'consistency_safe_runner_v1', 'topstep_micro_scaling_mes_mnq_v2', 'portfolio_diversification_lane', 'creative_market_representation_lane']
- branches_to_expand: ['portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 1049 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 1 --max-runtime-hours 0.08 --continue-until-quality --report-tag trading_ready_strategy_bank_topstep_q1_v1
