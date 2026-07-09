# HYDRA Trading-Ready Factory Checkpoint

Generated: 2026-07-09T13:03:46+00:00

- runtime_seconds: 288.11
- cache_only_respected: True
- new_databento_request_made: False
- workers_used: 1
- candidates_tested_this_run: 770
- total_candidates_in_registry: 2770
- target_80000_progress: 0.034625
- trading_ready_count: 0
- economically_viable_count: 18
- topstep_viable_count: 1
- near_miss_count: 8
- target_50_reached: False
- status_distribution: {'TOPSTEP_COMBINE_FAILED_TARGET': 1357, 'DEAD_STRATEGY': 743, 'TOPSTEP_COMBINE_FAILED_MLL': 643, 'ECONOMICALLY_VIABLE': 18, 'PROMISING_NEEDS_MUTATION': 7, 'TOPSTEP_VIABLE': 1, 'TOPSTEP_NEAR_MISS': 1}
- failure_reasons: {'combine_profit_target_not_reached': 1357, 'no_economic_signal': 682, 'combine_mll_breached': 646, 'fragile_trade_order': 58, 'weak_but_mutatable_economic_profile': 15, 'viable_only_in_one_split': 11, 'march_oos_weak': 1}
- promotion_gate_distribution: {'WALK_FORWARD:SOFT_FAIL:viable_only_in_one_split': 769, 'OOS:SOFT_FAIL:march_oos_weak': 769, 'PORTFOLIO_INTERACTION:SOFT_FAIL:portfolio_role_needs_retest': 769, 'PAYOUT_SURVIVAL:SOFT_FAIL:payout_profile_weak': 768, 'MONTE_CARLO:HARD_FAIL:fragile_trade_order': 724, 'ECONOMIC_PROFILE:HARD_FAIL:no_economic_signal': 682, 'FUNDED_XFA:SOFT_FAIL:funded_mll_or_tail_failure': 513, 'TOPSTEP_COMBINE:HARD_FAIL:combine_mll_breached': 507, 'PARAMETER_SENSITIVITY:SOFT_FAIL:parameter_zone_too_fragile': 485, 'TOPSTEP_COMBINE:SOFT_FAIL:combine_target_not_reached': 263, 'ECONOMIC_PROFILE:SOFT_FAIL:weak_but_mutatable_economic_profile': 74, 'MONTE_CARLO:SOFT_FAIL:reshuffle_robustness_soft_fail': 3}
- target_reached_count: 0
- mll_respected_count: 1620
- consistency_respected_count: 2601
- funded_survival_count: 1607
- payout_eligible_count: 28
- payout_cycles_survived: 32
- gross_payout_estimate: 38623.21269573654
- trader_net_payout_estimate: 34760.89142616289
- best_topstep_score: 0.687537
- best_promotion_score: 0.646813
- best_economic_score: 0.756091
- best_families: {'topstep_nq_es_divergence_controlled': 11, 'topstep_opening_range_controlled_runner': 3}
- near_miss_map: {'creative_market_representation_lane': 3, 'topstep_vwap_exhaustion_payout_engine_v2': 2, 'topstep_volatility_expansion_limited_risk_v2': 1, 'topstep_nq_es_divergence_controlled_v2': 1, 'near_miss_adaptive_mutator': 1}
- branches_to_kill: ['topstep_opening_range_controlled_runner_v2', 'topstep_nq_es_divergence_controlled_v2', 'topstep_prior_level_reclaim_smooth_pnl_v2', 'consistency_safe_runner_v1', 'portfolio_diversification_lane', 'payout_cycle_smooth_climber_v1', 'topstep_volatility_expansion_limited_risk_v2', 'topstep_micro_scaling_mes_mnq_v2', 'near_miss_adaptive_mutator', 'creative_market_representation_lane']
- branches_to_expand: ['portfolio_diversification_lane']
- exported_configs: []
- checkpoint_folder: /root/hydra-bot/reports/checkpoints
- stop_reason: max_runtime_reached
- resume_command: python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 1049 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 1 --max-runtime-hours 0.08 --continue-until-quality --report-tag trading_ready_strategy_bank_topstep_q1_v1
