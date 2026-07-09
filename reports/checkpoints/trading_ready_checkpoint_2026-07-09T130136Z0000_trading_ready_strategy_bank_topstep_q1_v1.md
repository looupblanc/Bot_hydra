# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T13:01:36+00:00

- runtime_seconds: 158.13
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 1
- candidates_tested_this_run: 400
- total_candidates_in_registry: 2400
- target_80000_progress: 0.03
- trading_ready_count: 0
- economically_viable_count: 10
- topstep_viable_count: 1
- near_miss_count: 3
- target_50_reached: False
- status_distribution: {'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'DEAD_STRATEGY': 386, 'ECONOMICALLY_VIABLE': 10, 'PROMISING_NEEDS_MUTATION': 3, 'TOPSTEP_VIABLE': 1}
- failure_reasons: {'combine_profit_target_not_reached': 1357, 'combine_mll_breached': 643, 'no_economic_signal': 358, 'fragile_trade_order': 28, 'weak_but_mutatable_economic_profile': 9, 'viable_only_in_one_split': 4, 'march_oos_weak': 1}
- promotion_gate_distribution: {'OOS:SOFT_FAIL:march_oos_weak': 400, 'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 399, 'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 399, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 398, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 379, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 358, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 261, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 258, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 241, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 142, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 37, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 1}
- target_reached_count: 0
- mll_respected_count: 1499
- consistency_respected_count: 2259
- funded_survival_count: 1489
- payout_eligible_count: 21
- payout_cycles_survived: 25
- gross_payout_estimate: 28499.6591181593
- trader_net_payout_estimate: 25649.69320634337
- best_topstep_score: 0.687537
- best_promotion_score: 0.646813
- best_economic_score: 0.709711
- best_families: {'topstep_nq_es_divergence_controlled': 7, 'topstep_opening_range_controlled_runner': 1}
- near_miss_map: {'creative_market_representation_lane': 2, 'topstep_vwap_exhaustion_payout_engine_v2': 1}
- branches_to_kill: ['topstep_opening_range_controlled_runner_v2', 'topstep_nq_es_divergence_controlled_v2', 'topstep_volatility_expansion_limited_risk_v2', 'topstep_prior_level_reclaim_smooth_pnl_v2', 'payout_cycle_smooth_climber_v1', 'near_miss_adaptive_mutator', 'consistency_safe_runner_v1', 'topstep_micro_scaling_mes_mnq_v2', 'portfolio_diversification_lane', 'creative_market_representation_lane']
- branches_to_expand: ['portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: running
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 1049 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 1 --max-runtime-hours 0.08 --continue-until-quality --report-tag trading_ready_strategy_bank_topstep_q1_v1
