# HYDRA Trading-Ready Topstep Factory Interrupted Run Finalization

Generated: 2026-07-10T00:45:07.451678+00:00
Tag: `stopped_for_gate_aware_remediation_v1`

## Stop Discipline
- Current research process was stopped on request before launching another factory command.
- This report uses the latest consistent checkpoint plus the current live SQLite registry.
- Only candidates already committed in SQLite are counted; incomplete in-flight worker results are not counted.
- Validation thresholds and candidate classifications were not modified.
- This is historical research only. It is not live trading approval.

## Integrity And Resume State
- SQLite integrity check: `ok`
- Registry path: `registry/hydra_registry.db`
- Latest consistent checkpoint: `reports/checkpoints/trading_ready_checkpoint_2026-07-10T002909Z0000_overnight_resume_strategy_bank_topstep_q1_v2.md`
- Final stop checkpoint: `reports/checkpoints/trading_ready_checkpoint_2026-07-10T004507Z0000_stopped_for_gate_aware_remediation_v1.md`
- Resume from total registry candidates: 17994
- Completed candidates since pre-long-run state (3263): 14731
- Exact resume command: `python scripts/run_trading_ready_topstep_factory.py --symbols ES MES NQ MNQ --start 2024-01-01 --end 2024-03-28 --schema ohlcv-1m --dataset GLBX.MDP3 --use-cache-only --strict --seed 2050 --workers auto --min-total-candidates 80000 --target-trading-ready 50 --target-economically-viable 30 --target-topstep-viable 20 --target-portfolio-candidates 10 --account-size 150000 --profit-target 9000 --mll 4500 --no-daily-loss-limit --simulate-funded --simulate-payouts --checkpoint-every-minutes 20 --max-runtime-hours 6 --continue-until-quality --report-tag overnight_resume_strategy_bank_topstep_q1_v2`

## Core Counts
- Total candidates: 17994
- Progress toward 80,000: 22.4925%
- Economically viable count: 403
- Topstep viable count: 21
- Trading-ready count: 0
- Near-miss count: 160
- Target reached count: 27
- MLL respected count: 7607
- Consistency respected count: 16347
- Funded survival count: 7424
- Payout eligible count: 337
- Gross payout estimate: $613,798.71
- Trader net payout estimate after 90/10: $552,418.84
- Best Topstep score: 1.000000
- Best promotion score: 0.884426
- Best economic score: 0.906913

## Status Distribution
- DEAD_STRATEGY: 15410
- TOPSTEP_COMBINE_FAILED_TARGET: 1357
- TOPSTEP_COMBINE_FAILED_MLL: 643
- ECONOMICALLY_VIABLE: 403
- PROMISING_NEEDS_MUTATION: 129
- TOPSTEP_NEAR_MISS: 31
- TOPSTEP_VIABLE: 21

## Rejection Reason Distribution
- no_economic_signal: 13825
- fragile_trade_order: 1496
- combine_profit_target_not_reached: 1357
- combine_mll_breached: 682
- viable_only_in_one_split: 275
- weak_but_mutatable_economic_profile: 274
- duplicate_or_near_duplicate_equity_curve: 50
- march_oos_weak: 20
- combine_target_not_reached: 11
- reshuffle_robustness_soft_fail: 2
- topstep_near_miss_target_velocity: 1
- funded_mll_or_tail_failure: 1

## Gate Failure Distribution
- PORTFOLIO_INTERACTION | SOFT_FAIL | portfolio_role_needs_retest: 15973
- PAYOUT_SURVIVAL | SOFT_FAIL | payout_profile_weak: 15952
- WALK_FORWARD | SOFT_FAIL | viable_only_in_one_split: 15951
- OOS | SOFT_FAIL | march_oos_weak: 15913
- MONTE_CARLO | HARD_FAIL | fragile_trade_order: 14558
- ECONOMIC_PROFILE | HARD_FAIL | no_economic_signal: 13825
- FUNDED_XFA | SOFT_FAIL | funded_mll_or_tail_failure: 9920
- TOPSTEP_COMBINE | HARD_FAIL | combine_mll_breached: 9744
- PARAMETER_SENSITIVITY | SOFT_FAIL | parameter_zone_too_fragile: 8821
- TOPSTEP_COMBINE | SOFT_FAIL | combine_target_not_reached: 6219
- ECONOMIC_PROFILE | SOFT_FAIL | weak_but_mutatable_economic_profile: 1836
- MONTE_CARLO | SOFT_FAIL | reshuffle_robustness_soft_fail: 150
- CORRELATION | SOFT_FAIL | high_correlation_needs_portfolio_role: 56
- CORRELATION | HARD_FAIL | duplicate_or_near_duplicate_equity_curve: 50
- TOPSTEP_COMBINE | SOFT_FAIL | topstep_near_miss_target_velocity: 4

## Gate Failure Counts By Gate
- PORTFOLIO_INTERACTION: 15973
- TOPSTEP_COMBINE: 15967
- PAYOUT_SURVIVAL: 15952
- WALK_FORWARD: 15951
- OOS: 15913
- ECONOMIC_PROFILE: 15661
- MONTE_CARLO: 14708
- FUNDED_XFA: 9920
- PARAMETER_SENSITIVITY: 8821
- CORRELATION: 106

## Gate Failure Counts By Gate And Severity
- PORTFOLIO_INTERACTION:SOFT_FAIL: 15973
- PAYOUT_SURVIVAL:SOFT_FAIL: 15952
- WALK_FORWARD:SOFT_FAIL: 15951
- OOS:SOFT_FAIL: 15913
- MONTE_CARLO:HARD_FAIL: 14558
- ECONOMIC_PROFILE:HARD_FAIL: 13825
- FUNDED_XFA:SOFT_FAIL: 9920
- TOPSTEP_COMBINE:HARD_FAIL: 9744
- PARAMETER_SENSITIVITY:SOFT_FAIL: 8821
- TOPSTEP_COMBINE:SOFT_FAIL: 6223
- ECONOMIC_PROFILE:SOFT_FAIL: 1836
- MONTE_CARLO:SOFT_FAIL: 150
- CORRELATION:SOFT_FAIL: 56
- CORRELATION:HARD_FAIL: 50

## Failure Distribution By Family
| family | failure | count |
| --- | --- | --- |
| topstep_vwap_exhaustion_payout_engine | no_economic_signal | 3146 |
| topstep_volatility_expansion_limited_risk | no_economic_signal | 3144 |
| topstep_nq_es_divergence_controlled | no_economic_signal | 3015 |
| topstep_opening_range_controlled_runner | no_economic_signal | 2830 |
| topstep_prior_level_reclaim_smooth_pnl | no_economic_signal | 1575 |
| topstep_nq_es_divergence_controlled | fragile_trade_order | 1060 |
| topstep_nq_es_divergence_controlled | combine_profit_target_not_reached | 334 |
| topstep_opening_range_controlled_runner | fragile_trade_order | 319 |
| topstep_opening_range_controlled_runner | combine_profit_target_not_reached | 314 |
| topstep_prior_level_reclaim_smooth_pnl | combine_profit_target_not_reached | 311 |
| topstep_nq_es_divergence_controlled | weak_but_mutatable_economic_profile | 265 |
| topstep_volatility_expansion_limited_risk | combine_mll_breached | 257 |
| topstep_nq_es_divergence_controlled | viable_only_in_one_split | 250 |
| topstep_micro_scaling_mes_mnq | combine_profit_target_not_reached | 215 |
| topstep_vwap_exhaustion_payout_engine | combine_mll_breached | 215 |
| topstep_vwap_exhaustion_payout_engine | combine_profit_target_not_reached | 133 |
| topstep_micro_scaling_mes_mnq | combine_mll_breached | 121 |
| topstep_micro_scaling_mes_mnq | no_economic_signal | 115 |
| topstep_prior_level_reclaim_smooth_pnl | fragile_trade_order | 72 |
| topstep_nq_es_divergence_controlled | combine_mll_breached | 53 |
| topstep_nq_es_divergence_controlled | duplicate_or_near_duplicate_equity_curve | 50 |
| topstep_volatility_expansion_limited_risk | combine_profit_target_not_reached | 50 |
| topstep_vwap_exhaustion_payout_engine | fragile_trade_order | 33 |
| topstep_opening_range_controlled_runner | viable_only_in_one_split | 24 |
| topstep_opening_range_controlled_runner | combine_mll_breached | 23 |
| topstep_nq_es_divergence_controlled | march_oos_weak | 18 |
| topstep_prior_level_reclaim_smooth_pnl | combine_mll_breached | 13 |
| topstep_volatility_expansion_limited_risk | fragile_trade_order | 10 |
| topstep_nq_es_divergence_controlled | combine_target_not_reached | 9 |
| topstep_opening_range_controlled_runner | weak_but_mutatable_economic_profile | 5 |
| topstep_vwap_exhaustion_payout_engine | weak_but_mutatable_economic_profile | 3 |
| topstep_micro_scaling_mes_mnq | fragile_trade_order | 2 |
| topstep_nq_es_divergence_controlled | reshuffle_robustness_soft_fail | 2 |
| topstep_opening_range_controlled_runner | march_oos_weak | 2 |
| topstep_nq_es_divergence_controlled | funded_mll_or_tail_failure | 1 |
| topstep_nq_es_divergence_controlled | topstep_near_miss_target_velocity | 1 |
| topstep_opening_range_controlled_runner | combine_target_not_reached | 1 |
| topstep_volatility_expansion_limited_risk | combine_target_not_reached | 1 |
| topstep_volatility_expansion_limited_risk | viable_only_in_one_split | 1 |
| topstep_volatility_expansion_limited_risk | weak_but_mutatable_economic_profile | 1 |

## Failure Distribution By Lane
| lane | failure | count |
| --- | --- | --- |
| topstep_nq_es_divergence_controlled_v2 | no_economic_signal | 1999 |
| topstep_volatility_expansion_limited_risk_v2 | no_economic_signal | 1579 |
| creative_market_representation_lane | no_economic_signal | 1577 |
| payout_cycle_smooth_climber_v1 | no_economic_signal | 1569 |
| topstep_vwap_exhaustion_payout_engine_v2 | no_economic_signal | 1564 |
| consistency_safe_runner_v1 | no_economic_signal | 1519 |
| portfolio_diversification_lane | no_economic_signal | 1419 |
| topstep_opening_range_controlled_runner_v2 | no_economic_signal | 1416 |
| unknown | combine_profit_target_not_reached | 1357 |
| near_miss_adaptive_mutator | no_economic_signal | 1058 |
| topstep_nq_es_divergence_controlled_v2 | fragile_trade_order | 702 |
| unknown | combine_mll_breached | 643 |
| near_miss_adaptive_mutator | fragile_trade_order | 340 |
| topstep_nq_es_divergence_controlled_v2 | viable_only_in_one_split | 176 |
| topstep_nq_es_divergence_controlled_v2 | weak_but_mutatable_economic_profile | 171 |
| portfolio_diversification_lane | fragile_trade_order | 161 |
| topstep_opening_range_controlled_runner_v2 | fragile_trade_order | 156 |
| near_miss_adaptive_mutator | weak_but_mutatable_economic_profile | 82 |
| consistency_safe_runner_v1 | fragile_trade_order | 72 |
| near_miss_adaptive_mutator | viable_only_in_one_split | 68 |
| topstep_micro_scaling_mes_mnq_v2 | no_economic_signal | 63 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | no_economic_signal | 62 |
| topstep_nq_es_divergence_controlled_v2 | duplicate_or_near_duplicate_equity_curve | 32 |
| topstep_nq_es_divergence_controlled_v2 | combine_mll_breached | 21 |
| payout_cycle_smooth_climber_v1 | fragile_trade_order | 20 |
| near_miss_adaptive_mutator | duplicate_or_near_duplicate_equity_curve | 18 |
| topstep_vwap_exhaustion_payout_engine_v2 | fragile_trade_order | 18 |
| topstep_opening_range_controlled_runner_v2 | viable_only_in_one_split | 13 |
| near_miss_adaptive_mutator | combine_mll_breached | 11 |
| creative_market_representation_lane | fragile_trade_order | 9 |
| portfolio_diversification_lane | viable_only_in_one_split | 9 |
| topstep_nq_es_divergence_controlled_v2 | march_oos_weak | 9 |
| topstep_volatility_expansion_limited_risk_v2 | fragile_trade_order | 9 |
| near_miss_adaptive_mutator | march_oos_weak | 8 |
| topstep_nq_es_divergence_controlled_v2 | combine_target_not_reached | 5 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | fragile_trade_order | 5 |
| topstep_vwap_exhaustion_payout_engine_v2 | weak_but_mutatable_economic_profile | 5 |
| near_miss_adaptive_mutator | combine_target_not_reached | 4 |
| topstep_micro_scaling_mes_mnq_v2 | fragile_trade_order | 4 |
| topstep_opening_range_controlled_runner_v2 | weak_but_mutatable_economic_profile | 4 |
| topstep_micro_scaling_mes_mnq_v2 | weak_but_mutatable_economic_profile | 3 |
| topstep_vwap_exhaustion_payout_engine_v2 | viable_only_in_one_split | 3 |
| creative_market_representation_lane | viable_only_in_one_split | 2 |
| creative_market_representation_lane | weak_but_mutatable_economic_profile | 2 |
| near_miss_adaptive_mutator | reshuffle_robustness_soft_fail | 2 |
| payout_cycle_smooth_climber_v1 | weak_but_mutatable_economic_profile | 2 |
| portfolio_diversification_lane | weak_but_mutatable_economic_profile | 2 |
| topstep_opening_range_controlled_runner_v2 | combine_mll_breached | 2 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | combine_mll_breached | 2 |
| topstep_volatility_expansion_limited_risk_v2 | viable_only_in_one_split | 2 |
| topstep_volatility_expansion_limited_risk_v2 | weak_but_mutatable_economic_profile | 2 |
| topstep_vwap_exhaustion_payout_engine_v2 | combine_mll_breached | 2 |
| consistency_safe_runner_v1 | viable_only_in_one_split | 1 |
| creative_market_representation_lane | combine_target_not_reached | 1 |
| payout_cycle_smooth_climber_v1 | march_oos_weak | 1 |
| portfolio_diversification_lane | combine_mll_breached | 1 |
| portfolio_diversification_lane | combine_target_not_reached | 1 |
| portfolio_diversification_lane | march_oos_weak | 1 |
| topstep_nq_es_divergence_controlled_v2 | funded_mll_or_tail_failure | 1 |
| topstep_nq_es_divergence_controlled_v2 | topstep_near_miss_target_velocity | 1 |
| topstep_opening_range_controlled_runner_v2 | march_oos_weak | 1 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | viable_only_in_one_split | 1 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | weak_but_mutatable_economic_profile | 1 |

## Top 100 Near-Misses
| candidate_id | family | symbol | validation_status | rejection_reason | research_lane | promotion_score | topstep_score | economic_score | net_profit | combine_min_mll_buffer | combine_profit_target_hit | combine_mll_breached | combine_consistency_ok | funded_sim_survived | payout_eligible | recommended_action | branch_action |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cand_73dfc9dbbd04 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.884426 | 1 | 0.796343 | 8148.26 | 3259.91 | 1 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_7d0b38decded | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.846643 | 0.897938 | 0.818094 | 7189.11 | 2106.55 | 1 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_c246d0ee5c7e | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.814577 | 0.806147 | 0.810801 | 6746.26 | 3484.28 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_c7dee5d0df2e | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.809194 | 0.772719 | 0.849481 | 5838.88 | 2394.91 | 1 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_ed88f3d3724e | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | topstep_near_miss_target_velocity | topstep_nq_es_divergence_controlled_v2 | 0.803491 | 0.692785 | 0.886408 | 7042.5 | 2027.24 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_2ae4cb1a81df | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.741953 | 0.676352 | 0.842575 | 6035.55 | 3012.91 | 1 | 0 | 1 | 0 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_64e9fe6e77d1 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.715793 | 0.633544 | 0.774886 | 3230.35 | 2579.28 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_e0428bdd8a02 | topstep_opening_range_controlled_runner | NQ | TOPSTEP_VIABLE | march_oos_weak | topstep_opening_range_controlled_runner_v2 | 0.715218 | 0.627265 | 0.783612 | 3206.64 | 3168.93 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_16b3e9449578 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.714705 | 0.597999 | 0.802326 | 4432.9 | 2951.99 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_a15a83d8f323 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | march_oos_weak | near_miss_adaptive_mutator | 0.701059 | 0.592325 | 0.83493 | 12562.4 | 2871.11 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_3bf8c5b5f2fb | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | weak_but_mutatable_economic_profile | near_miss_adaptive_mutator | 0.695121 | 0.690585 | 0.672422 | 4309.42 | 3397.34 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_84aabeac28f9 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.687339 | 0.476604 | 0.906913 | 7754.14 | 2914.3 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_ca84ada596f1 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | funded_mll_or_tail_failure | topstep_nq_es_divergence_controlled_v2 | 0.683346 | 0.555194 | 0.805518 | 20445.8 | 2737.11 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_0a3d0e770b75 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.682157 | 0.57553 | 0.761058 | 3642.79 | 2319.72 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_857f73be18ce | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | weak_but_mutatable_economic_profile | near_miss_adaptive_mutator | 0.679218 | 0.642778 | 0.70651 | 4341.3 | 3431.61 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_e037e90e5142 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.676428 | 0.517667 | 0.863678 | 7911.91 | 2248.15 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_df97f2ac5cad | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.675689 | 0.553805 | 0.800283 | 8963.75 | 2201.85 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_2d371a60c839 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | combine_target_not_reached | topstep_nq_es_divergence_controlled_v2 | 0.674832 | 0.530401 | 0.79007 | 3455.35 | 3510.54 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_366fe22ca205 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.672197 | 0.483562 | 0.865631 | 7093.14 | 1800.28 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_c61c340b2cc6 | topstep_volatility_expansion_limited_risk | NQ | TOPSTEP_VIABLE | combine_target_not_reached | creative_market_representation_lane | 0.657904 | 0.668731 | 0.585699 | 1470.32 | 4271.68 | 0 | 0 | 1 | 1 | 0 | deepen_validation_and_portfolio_test | expand |
| cand_32ce229d68d7 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.657167 | 0.444739 | 0.867094 | 6620.92 | 767.159 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_45a07f2779c7 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.656894 | 0.568879 | 0.71393 | 1578.18 | 3340.82 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_3df28d2ab2dc | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | combine_target_not_reached | near_miss_adaptive_mutator | 0.650849 | 0.683266 | 0.5665 | 2110.23 | 3550.7 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_19e9aab70e8b | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.649451 | 0.632717 | 0.68002 | 4454.52 | 3318.83 | 1 | 0 | 1 | 0 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_78724dc9f509 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | viable_only_in_one_split | portfolio_diversification_lane | 0.646813 | 0.687537 | 0.547111 | 1315.14 | 3803.34 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_1b14d6c426a5 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.645007 | 0.525211 | 0.709947 | 3207.29 | 1912.83 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_d6243b018c31 | topstep_nq_es_divergence_controlled | MNQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.644209 | 0.633652 | 0.630381 | 2351.66 | 3089.28 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_e962e629500f | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.644124 | 0.484539 | 0.737616 | 4210.48 | 809.994 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_1b20208ed4d2 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | march_oos_weak | topstep_nq_es_divergence_controlled_v2 | 0.642335 | 0.512369 | 0.782657 | 7939.58 | 1689.99 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_e6724984107d | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.641022 | 0.668483 | 0.535902 | 1511.22 | 3676.97 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_f35423abe7f3 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.639307 | 0.563771 | 0.655772 | 950.221 | 3814.77 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_e781dcff173b | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | march_oos_weak | near_miss_adaptive_mutator | 0.638893 | 0.647401 | 0.611139 | 3614.07 | 3673.32 | 1 | 0 | 1 | 0 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_73fe0ee950b6 | topstep_nq_es_divergence_controlled | MNQ | TOPSTEP_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.63771 | 0.643762 | 0.594117 | 2087.13 | 3391.82 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_a397f272514a | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.63253 | 0.527561 | 0.740021 | 8790.34 | 2571.91 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_9c59cf6a5af9 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.631952 | 0.512825 | 0.699505 | 2375.11 | 3258.43 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_63a3ab97350f | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | near_miss_adaptive_mutator | 0.631193 | 0.53061 | 0.690366 | 1880.69 | 3205.96 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_124c889df7af | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.628886 | 0.443085 | 0.790159 | 7199.67 | 2334.89 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_1bf1f079ad25 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.626704 | 0.551869 | 0.64346 | 720.433 | 3773.48 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_311bf131a81f | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.624379 | 0.470463 | 0.716239 | 2990.51 | 1752.13 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_5001854906c1 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | combine_target_not_reached | near_miss_adaptive_mutator | 0.623448 | 0.625895 | 0.54315 | 1631.91 | 3182.63 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_48ebb4a8e69f | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.621477 | 0.343682 | 0.893849 | 6340.96 | 2224.51 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_b800937b7fda | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.619762 | 0.576366 | 0.579851 | 2830.43 | 2027.67 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_996fc4b5c1f8 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | combine_target_not_reached | near_miss_adaptive_mutator | 0.616884 | 0.64704 | 0.513958 | 820.171 | 3823.97 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_4fc4c934fcd6 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | march_oos_weak | payout_cycle_smooth_climber_v1 | 0.615669 | 0.442578 | 0.709711 | 3490.14 | 2773.88 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_341a2a2366c7 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.615416 | 0.363045 | 0.865831 | 6062.28 | 1114.42 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_6006e94dc95a | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.615098 | 0.474333 | 0.749111 | 7231.19 | 336.956 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_eed5000f146e | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.613235 | 0.338156 | 0.892537 | 6707.84 | 2133.97 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_a66f823dbbdf | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_VIABLE | weak_but_mutatable_economic_profile | topstep_nq_es_divergence_controlled_v2 | 0.613128 | 0.643209 | 0.504501 | 1115.79 | 3629.35 | 0 | 0 | 1 | 1 | 1 | deepen_validation_and_portfolio_test | expand |
| cand_9874719ab043 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_vwap_exhaustion_payout_engine_v2 | 0.609677 | 0.517215 | 0.649452 | 632.152 | 4088.39 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_0ea339accc63 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.607461 | 0.428734 | 0.765505 | 5000.34 | 2450.35 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_4fc21817d909 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.606297 | 0.439874 | 0.763563 | 6323.08 | 700.084 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_7331a4d59e88 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | near_miss_adaptive_mutator | 0.605874 | 0.56622 | 0.570469 | 1170.04 | 2971.29 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_7c5c5ab3d5db | topstep_nq_es_divergence_controlled | MNQ | TOPSTEP_NEAR_MISS | weak_but_mutatable_economic_profile | near_miss_adaptive_mutator | 0.603533 | 0.596432 | 0.578076 | 2357.52 | 3615.17 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_35d06c4c0043 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.601917 | 0.426597 | 0.749851 | 5657.06 | 1848.5 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_bc2a23c8b345 | topstep_opening_range_controlled_runner | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_opening_range_controlled_runner_v2 | 0.601836 | 0.378486 | 0.771351 | 3808.49 | 2180.28 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_410c44a33878 | topstep_nq_es_divergence_controlled | ES | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.600951 | 0.493227 | 0.661058 | 681.369 | 4048.09 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_af9d58c9ba7c | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.599288 | 0.439875 | 0.74377 | 7054.5 | 921.272 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_2ece7585c8ad | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | weak_but_mutatable_economic_profile | topstep_nq_es_divergence_controlled_v2 | 0.599182 | 0.562006 | 0.552121 | 3330.74 | 1314.43 | 0 | 0 | 1 | 1 | 1 | mutate_weak_dimension | mutate |
| cand_c2bcfcd6dd4f | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.5974 | 0.603629 | 0.518134 | 1080.28 | 3669.84 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_2e4b267abd3c | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.592019 | 0.57672 | 0.542526 | 2038.19 | 3094.71 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_471ba9935d7c | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | weak_but_mutatable_economic_profile | topstep_nq_es_divergence_controlled_v2 | 0.589524 | 0.582074 | 0.519893 | 763.106 | 4034.86 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_dfd6751ff2e8 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | combine_target_not_reached | near_miss_adaptive_mutator | 0.588547 | 0.289512 | 0.902383 | 5332.42 | 3047.29 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_2262a32218e3 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.583782 | 0.389184 | 0.723821 | 4200.99 | 2607.95 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_366301cfe1ac | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | march_oos_weak | near_miss_adaptive_mutator | 0.580151 | 0.404555 | 0.711591 | 4666.84 | 1413.53 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_2fb3d6627b59 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.577798 | 0.475681 | 0.635144 | 4112.15 | 2081.16 | 1 | 0 | 1 | 0 | 1 | mutate_weak_dimension | mutate |
| cand_61ae7de937d3 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.575558 | 0.432855 | 0.686059 | 4378.16 | 1991.28 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_c461cec36879 | topstep_nq_es_divergence_controlled | MNQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.565551 | 0.444759 | 0.6228 | 2484.45 | 2806.42 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_10183f0425c9 | topstep_nq_es_divergence_controlled | NQ | TOPSTEP_NEAR_MISS | weak_but_mutatable_economic_profile | topstep_nq_es_divergence_controlled_v2 | 0.561688 | 0.579259 | 0.503211 | 797.862 | 4255.61 | 0 | 0 | 1 | 1 | 0 | mutate_weak_dimension | mutate |
| cand_a55e0fa58501 | topstep_opening_range_controlled_runner | NQ | ECONOMICALLY_VIABLE | combine_target_not_reached | portfolio_diversification_lane | 0.560037 | 0.372009 | 0.667125 | 1874.97 | 3341.69 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_9811372217d8 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.555605 | 0.388373 | 0.673111 | 3972.39 | 809.7 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_6f1ad9e34d88 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.554622 | 0.307469 | 0.780716 | 5087.66 | 3343.3 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_4cbd5d17ab20 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | march_oos_weak | near_miss_adaptive_mutator | 0.551863 | 0.250704 | 0.819703 | 4251.03 | 3380.67 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_f6d645f12da5 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.551444 | 0.336311 | 0.674656 | 2064.73 | 2500.48 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_0d39f20190fa | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.551087 | 0.302924 | 0.700212 | 3332.85 | 1969.83 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_b59eca9f2596 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.549566 | 0.335239 | 0.661859 | 3154.58 | 2201.57 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_a611977c6609 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.543548 | 0.369201 | 0.632707 | 802.293 | 3881.72 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_43edb524d48c | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.54258 | 0.291405 | 0.707606 | 2879.53 | 2445.7 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_2fbcb966b40a | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.540727 | 0.367804 | 0.633693 | 880.786 | 3445.44 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_35d379398749 | topstep_opening_range_controlled_runner | NQ | ECONOMICALLY_VIABLE | march_oos_weak | portfolio_diversification_lane | 0.540339 | 0.318847 | 0.681891 | 1874.6 | 3123.66 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_135d53169173 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.53668 | 0.441937 | 0.590435 | 2835.09 | 1294.71 | 1 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_10335a516f15 | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.535944 | 0.336783 | 0.642035 | 903.281 | 3515.43 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_dd8bccd3c91d | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | march_oos_weak | near_miss_adaptive_mutator | 0.535056 | 0.248414 | 0.790355 | 4815.54 | 3053.75 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_9a4f9ec534da | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.529092 | 0.298735 | 0.654467 | 2586.92 | 2046.35 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_fbf7a1401684 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.528102 | 0.29086 | 0.676027 | 2064.62 | 3142.9 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_629b61f5a633 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | weak_but_mutatable_economic_profile | near_miss_adaptive_mutator | 0.527238 | 0.38166 | 0.578879 | 3795.2 | 2330.14 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_c6f96a78cd80 | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.527091 | 0.356964 | 0.606989 | 786.505 | 3350 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_86964eec3cd2 | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.52703 | 0.352931 | 0.610249 | 772.721 | 3405.88 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_698c45e11fdc | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.525924 | 0.274182 | 0.684049 | 3054.07 | 1970.54 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_728d32bbddde | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.525103 | 0.314379 | 0.633108 | 1769.42 | 1870.08 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_0b19cae866f5 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.52489 | 0.20614 | 0.815062 | 5163.87 | 1011.56 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_2e7608c8f2f3 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.524709 | 0.376169 | 0.555089 | 2667.98 | 1981.81 | 0 | 0 | 0 | 1 | 1 | improve_topstep_path | mutate |
| cand_d9e6190666f7 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | weak_but_mutatable_economic_profile | topstep_nq_es_divergence_controlled_v2 | 0.523656 | 0.398203 | 0.561888 | 2524.67 | 3364.93 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_29a66235af7f | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.522023 | 0.359056 | 0.593419 | 402.305 | 3796.45 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_add3b8a00713 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | near_miss_adaptive_mutator | 0.521831 | 0.306234 | 0.64368 | 1319.46 | 3382.3 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_dc3b6a4363dc | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.519103 | 0.330348 | 0.611516 | 1076.23 | 3327.68 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_e7ce8a01e4c0 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.517649 | 0.281833 | 0.654819 | 2384.38 | 2465.36 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_c3c4b71da8c8 | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.515604 | 0.302342 | 0.634402 | 1112.56 | 3162.8 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_6a147a30e3c8 | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.51517 | 0.200925 | 0.736149 | 4045.02 | 1346.33 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |
| cand_e8183fd1edbb | topstep_nq_es_divergence_controlled | NQ | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.514838 | 0.238524 | 0.753083 | 3947.7 | 2838.47 | 0 | 0 | 1 | 0 | 1 | improve_topstep_path | mutate |
| cand_1eeb06c4f756 | topstep_nq_es_divergence_controlled | ES | ECONOMICALLY_VIABLE | viable_only_in_one_split | topstep_nq_es_divergence_controlled_v2 | 0.513866 | 0.349917 | 0.584359 | 307.228 | 3879.74 | 0 | 0 | 0 | 1 | 0 | improve_topstep_path | mutate |

## Candidates Passing Every Gate Except One
- None.

## Target-Profit Versus MLL-Risk Pareto Frontier
| candidate_id | family | symbol | research_lane | validation_status | net_profit | combine_min_mll_buffer | topstep_score | promotion_score | combine_profit_target_hit | combine_consistency_ok | funded_sim_survived | payout_eligible | rejection_reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cand_ca84ada596f1 | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | TOPSTEP_NEAR_MISS | 20445.8 | 2737.11 | 0.555194 | 0.683346 | 1 | 1 | 0 | 1 | funded_mll_or_tail_failure |
| cand_a15a83d8f323 | topstep_nq_es_divergence_controlled | NQ | near_miss_adaptive_mutator | TOPSTEP_NEAR_MISS | 12562.4 | 2871.11 | 0.592325 | 0.701059 | 1 | 1 | 0 | 1 | march_oos_weak |
| cand_73dfc9dbbd04 | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | TOPSTEP_VIABLE | 8148.26 | 3259.91 | 1 | 0.884426 | 1 | 1 | 1 | 1 | viable_only_in_one_split |
| cand_c246d0ee5c7e | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | TOPSTEP_VIABLE | 6746.26 | 3484.28 | 0.806147 | 0.814577 | 0 | 1 | 1 | 1 | viable_only_in_one_split |
| cand_e781dcff173b | topstep_nq_es_divergence_controlled | NQ | near_miss_adaptive_mutator | TOPSTEP_VIABLE | 3614.07 | 3673.32 | 0.647401 | 0.638893 | 1 | 1 | 0 | 1 | march_oos_weak |
| cand_e6724984107d | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | TOPSTEP_VIABLE | 1511.22 | 3676.97 | 0.668483 | 0.641022 | 0 | 1 | 1 | 1 | viable_only_in_one_split |
| cand_c61c340b2cc6 | topstep_volatility_expansion_limited_risk | NQ | creative_market_representation_lane | TOPSTEP_VIABLE | 1470.32 | 4271.68 | 0.668731 | 0.657904 | 0 | 1 | 1 | 0 | combine_target_not_reached |
| cand_696312cabf7f | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 560.882 | 4276.53 | 0.292874 | 0.452553 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_9cdc13ea9410 | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 484.434 | 4306.98 | 0.293179 | 0.451398 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_b5194b4f0e46 | topstep_nq_es_divergence_controlled | NQ | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 413.679 | 4335.18 | 0.293461 | 0.450329 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_e3bc541ee1ff | topstep_nq_es_divergence_controlled | ES | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 305.105 | 4354.67 | 0.218623 | 0.418939 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_b8c965e19e43 | topstep_nq_es_divergence_controlled | NQ | near_miss_adaptive_mutator | DEAD_STRATEGY | 288.487 | 4375.86 | 0.436391 | 0.507774 | 0 | 1 | 1 | 0 | no_economic_signal |
| cand_a73928e8c62a | topstep_nq_es_divergence_controlled | ES | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 254.647 | 4377.24 | 0.243441 | 0.42714 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_fee83a0f35a0 | topstep_nq_es_divergence_controlled | MNQ | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 220.21 | 4381.93 | 0.265875 | 0.421073 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_c9066c2b7ac0 | topstep_nq_es_divergence_controlled | ES | near_miss_adaptive_mutator | DEAD_STRATEGY | 199.541 | 4408.55 | 0.243556 | 0.42574 | 0 | 0 | 1 | 0 | no_economic_signal |
| cand_47670eb896b3 | topstep_nq_es_divergence_controlled | ES |  | TOPSTEP_COMBINE_FAILED_TARGET | 171.06 | 4416.64 | 0.393824 | 0 | 0 | 1 | 1 | 0 | combine_profit_target_not_reached |
| cand_086dad1201f1 | topstep_nq_es_divergence_controlled | NQ |  | TOPSTEP_COMBINE_FAILED_TARGET | 163.598 | 4429.6 | 0.436367 | 0 | 0 | 1 | 1 | 0 | combine_profit_target_not_reached |
| cand_7b9ed5e618c0 | topstep_nq_es_divergence_controlled | ES | topstep_nq_es_divergence_controlled_v2 | DEAD_STRATEGY | 110.305 | 4439.24 | 0.295937 | 0.431156 | 0 | 0 | 1 | 0 | duplicate_or_near_duplicate_equity_curve |
| cand_15491c04c7c9 | topstep_nq_es_divergence_controlled | ES | near_miss_adaptive_mutator | DEAD_STRATEGY | 102.667 | 4439.56 | 0.198835 | 0.401466 | 0 | 0 | 1 | 0 | no_economic_signal |

## Lane Performance
| lane | total | dead | dead_rate | economic | economic_rate | topstep_viable | topstep_rate | near_miss | near_miss_rate | target_hits | mll_ok | payout_eligible | best_promotion_score | best_topstep_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| topstep_nq_es_divergence_controlled_v2 | 3117 | 2754 | 0.8835 | 252 | 0.0808 | 11 | 0.0035 | 100 | 0.0321 | 21 | 2464 | 147 | 0.884426 | 1 |
| unknown | 2000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1357 | 13 | 0 | 0.575299 |
| portfolio_diversification_lane | 1594 | 1581 | 0.9918 | 12 | 0.0075 | 1 | 0.0006 | 0 | 0 | 0 | 808 | 32 | 0.646813 | 0.687537 |
| topstep_vwap_exhaustion_payout_engine_v2 | 1592 | 1584 | 0.995 | 6 | 0.0038 | 0 | 0 | 2 | 0.0013 | 0 | 111 | 6 | 0.609677 | 0.517215 |
| topstep_volatility_expansion_limited_risk_v2 | 1592 | 1588 | 0.9975 | 3 | 0.0019 | 0 | 0 | 1 | 0.0006 | 0 | 57 | 19 | 0.481089 | 0.351626 |
| topstep_opening_range_controlled_runner_v2 | 1592 | 1574 | 0.9887 | 17 | 0.0107 | 1 | 0.0006 | 0 | 0 | 0 | 839 | 30 | 0.715218 | 0.627265 |
| payout_cycle_smooth_climber_v1 | 1592 | 1589 | 0.9981 | 3 | 0.0019 | 0 | 0 | 0 | 0 | 0 | 116 | 5 | 0.615669 | 0.442578 |
| consistency_safe_runner_v1 | 1592 | 1591 | 0.9994 | 1 | 0.0006 | 0 | 0 | 0 | 0 | 0 | 490 | 3 | 0.430109 | 0.306658 |
| near_miss_adaptive_mutator | 1591 | 1427 | 0.8969 | 103 | 0.0647 | 7 | 0.0044 | 54 | 0.0339 | 6 | 1251 | 61 | 0.715793 | 0.690585 |
| creative_market_representation_lane | 1591 | 1586 | 0.9969 | 1 | 0.0006 | 1 | 0.0006 | 3 | 0.0019 | 0 | 68 | 18 | 0.657904 | 0.668731 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | 71 | 69 | 0.9718 | 2 | 0.0282 | 0 | 0 | 0 | 0 | 0 | 20 | 1 | 0.439617 | 0.269316 |
| topstep_micro_scaling_mes_mnq_v2 | 70 | 67 | 0.9571 | 3 | 0.0429 | 0 | 0 | 0 | 0 | 0 | 26 | 2 | 0.440322 | 0.275099 |

## Branch Actions Already Recorded
| lane | branch_action | count |
| --- | --- | --- |
| topstep_nq_es_divergence_controlled_v2 | kill | 2754 |
| consistency_safe_runner_v1 | kill | 1591 |
| payout_cycle_smooth_climber_v1 | kill | 1589 |
| topstep_volatility_expansion_limited_risk_v2 | kill | 1588 |
| creative_market_representation_lane | kill | 1586 |
| topstep_vwap_exhaustion_payout_engine_v2 | kill | 1584 |
| portfolio_diversification_lane | kill | 1581 |
| topstep_opening_range_controlled_runner_v2 | kill | 1574 |
| near_miss_adaptive_mutator | kill | 1427 |
| topstep_nq_es_divergence_controlled_v2 | mutate | 352 |
| near_miss_adaptive_mutator | mutate | 157 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | kill | 69 |
| topstep_micro_scaling_mes_mnq_v2 | kill | 67 |
| topstep_opening_range_controlled_runner_v2 | mutate | 17 |
| portfolio_diversification_lane | mutate | 12 |
| topstep_nq_es_divergence_controlled_v2 | expand | 11 |
| topstep_vwap_exhaustion_payout_engine_v2 | mutate | 8 |
| near_miss_adaptive_mutator | expand | 7 |
| creative_market_representation_lane | mutate | 4 |
| topstep_volatility_expansion_limited_risk_v2 | mutate | 4 |
| payout_cycle_smooth_climber_v1 | mutate | 3 |
| topstep_micro_scaling_mes_mnq_v2 | mutate | 3 |
| topstep_prior_level_reclaim_smooth_pnl_v2 | mutate | 2 |
| consistency_safe_runner_v1 | mutate | 1 |
| creative_market_representation_lane | expand | 1 |
| portfolio_diversification_lane | expand | 1 |
| topstep_opening_range_controlled_runner_v2 | expand | 1 |

## Best Families
| family | strong_count | best_promotion_score | best_topstep_score | topstep_viable | economic | near_miss |
| --- | --- | --- | --- | --- | --- | --- |
| topstep_nq_es_divergence_controlled | 317 | 0.884426 | 1 | 19 | 211 | 44 |
| topstep_opening_range_controlled_runner | 29 | 0.715218 | 0.627265 | 1 | 24 | 0 |
| topstep_volatility_expansion_limited_risk | 2 | 0.657904 | 0.668731 | 1 | 1 | 0 |

## Branches Showing Diminishing Returns
| lane | total | dead_rate | economic | topstep_viable | near_miss | best_promotion_score | best_topstep_score |
| --- | --- | --- | --- | --- | --- | --- | --- |
| topstep_volatility_expansion_limited_risk_v2 | 1592 | 0.9975 | 3 | 0 | 1 | 0.481089 | 0.351626 |
| consistency_safe_runner_v1 | 1592 | 0.9994 | 1 | 0 | 0 | 0.430109 | 0.306658 |

## Branches Worth Repairing
| lane | total | dead_rate | economic | topstep_viable | near_miss | target_hits | mll_ok | payout_eligible | best_promotion_score | best_topstep_score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| topstep_nq_es_divergence_controlled_v2 | 3117 | 0.8835 | 252 | 11 | 100 | 21 | 2464 | 147 | 0.884426 | 1 |
| portfolio_diversification_lane | 1594 | 0.9918 | 12 | 1 | 0 | 0 | 808 | 32 | 0.646813 | 0.687537 |
| topstep_opening_range_controlled_runner_v2 | 1592 | 0.9887 | 17 | 1 | 0 | 0 | 839 | 30 | 0.715218 | 0.627265 |
| near_miss_adaptive_mutator | 1591 | 0.8969 | 103 | 7 | 54 | 6 | 1251 | 61 | 0.715793 | 0.690585 |
| creative_market_representation_lane | 1591 | 0.9969 | 1 | 1 | 3 | 0 | 68 | 18 | 0.657904 | 0.668731 |

## Branches Worth Killing Or Freezing
| lane | total | dead_rate | economic | topstep_viable | near_miss | best_promotion_score | best_topstep_score |
| --- | --- | --- | --- | --- | --- | --- | --- |
| topstep_volatility_expansion_limited_risk_v2 | 1592 | 0.9975 | 3 | 0 | 1 | 0.481089 | 0.351626 |
| consistency_safe_runner_v1 | 1592 | 0.9994 | 1 | 0 | 0 | 0.430109 | 0.306658 |

## Gate-Aware Remediation Notes
- Do not loosen Topstep MLL, consistency, no-lookahead, or payout survival gates to manufacture success.
- Highest-value remediation should target gate-aware mutation around target velocity, split/OOS stability, Monte Carlo fragility, and portfolio/correlation behavior.
- Near-misses should be mutated on the single weakest gate where possible; broad random expansion is lower priority than repairing lanes with demonstrated signal.
- Branches with high dead-rate and no near-miss/topstep viable output should be frozen until their feature representation or exit policy is revised.
