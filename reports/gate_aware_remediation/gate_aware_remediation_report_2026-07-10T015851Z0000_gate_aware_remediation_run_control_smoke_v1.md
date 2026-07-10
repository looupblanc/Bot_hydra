# HYDRA Gate-Aware Remediation Report

Generated: 2026-07-10T01:58:51+00:00

## Warning
- This is historical research only. It is not live trading approval.
- Q1/Q2 evidence cannot create TRADING_READY_CANDIDATE status.
- Q4 lockbox remains unavailable for tuning.

## Summary
- baseline_commit: bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1
- current_commit: a5b3656e9060a799ef7226cf271c243e41a99caf
- registry_integrity: ok
- runtime_seconds: 392.66
- workers_used: 3
- starting_candidates: 18401
- total_candidates: 20001
- remediation_children_tested: 1600
- remediation_children_generated: 1600
- cycles_completed: 40
- duplicate_rate: 0.0
- parent_to_child_improvement_rate: 0.325625
- policy_counts: `{"consistency_daily_lock": 23, "mll_buffer_derisk": 42, "oos_simplify": 913, "payout_frequency": 23, "portfolio_role_shift": 22, "sequence_fragility_smooth": 24, "target_velocity_runner": 553}`
- policy_improvements: `{"consistency_daily_lock": 6, "mll_buffer_derisk": 18, "oos_simplify": 280, "payout_frequency": 8, "portfolio_role_shift": 9, "sequence_fragility_smooth": 10, "target_velocity_runner": 190}`
- policies_frozen: `[]`
- last_stop_reason: max_runtime_reached
- last_stop_diagnostics: `{"continue_until_deadline": true, "cycles_completed": 40, "elapsed_seconds": 342.22, "eligible_parents": 750, "max_runtime_met": true, "min_children_met": true, "min_cycles_met": true, "min_runtime_met": true, "minimum_cycles": 3, "minimum_remediation_children": 200, "proven_work_exhaustion": false, "provisional_quality_target_reached": true, "queue_size": 0, "remediation_children_completed": 1600, "valid_quality_target_reached": false}`
- dossiers_generated: 460
- topstep_viable_reaudited: 29
- near_misses_analyzed: 200
- economically_viable_analyzed: 200
- hard_invalid_count: 31
- repairable_count: 429
- candidates_failing_exactly_one_gate: 2
- candidates_failing_exactly_two_gates: 19
- q1_repair_parent_pool_size: 460
- q1_promotion_finalists: 0
- q1_promotion_finalists_status: not_assigned_by_dossier_or_repairability_selection
- q2_confirmed_candidates: 0
- q3_blind_validation_passes: not_applicable_q3_quarantined
- q3_confirmation_passes: 0
- execution_validation_passes: 0
- q4_lockbox_passes: 0
- trading_ready_candidates: 0
- economic_strategy_units: 0
- economic_strategy_units_status: not_validated_without_trade_level_behavioral_clustering
- equivalence_clusters: 16208
- best_pareto_candidates: `["rem_2d934df793f1", "rem_f34088292746", "rem_6678663d5ef1", "rem_2c17d7bdc174", "rem_0d41adb34041", "rem_527908ceb6b1", "rem_bff983e62462", "rem_4386fda323f8", "rem_c7bfb5b46fd5", "rem_c17d988d0a0b", "cand_73dfc9dbbd04", "rem_9dcd813b8ed7", "rem_e49fceafba04", "rem_1299b5c29e4c", "rem_ad2e79e821be", "rem_6da2dd9ef1a7", "cand_7d0b38decded", "rem_4c409d78da4c", "cand_c246d0ee5c7e", "rem_0edb27c7c777"]`
- portfolio_baskets: `[{"candidate_ids": ["rem_2d934df793f1", "rem_f34088292746", "rem_6678663d5ef1", "rem_0d41adb34041", "rem_527908ceb6b1", "rem_c7bfb5b46fd5", "rem_c17d988d0a0b", "cand_7d0b38decded", "rem_4c409d78da4c", "cand_c246d0ee5c7e"], "estimated_net_profit": 75852.61415214447, "estimated_trader_net_payout": 50109.40222618589, "executable": true, "notes": ["approximation_from_registry_metrics_not_trade_level_schedule"], "policy": "balanced_pareto_one_account", "shared_mll_respected": true}]`
- status_distribution: `{"DEAD_STRATEGY": 15966, "ECONOMICALLY_VIABLE": 1105, "PROMISING_NEEDS_MUTATION": 309, "TOPSTEP_COMBINE_FAILED_MLL": 643, "TOPSTEP_COMBINE_FAILED_TARGET": 1357, "TOPSTEP_NEAR_MISS": 362, "TOPSTEP_VIABLE": 259}`
- family_fdr_proxy: `{"topstep_micro_scaling_mes_mnq": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 453.0}, "topstep_nq_es_divergence_controlled": {"false_discovery_risk_proxy": 0.4434731572904707, "signal_rate": 0.08352468427095293, "strong_count": 363.0, "topstep_viable_count": 219.0, "trial_count": 6968.0}, "topstep_opening_range_controlled_runner": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0005592841163310962, "strong_count": 1.0, "topstep_viable_count": 1.0, "trial_count": 3576.0}, "topstep_prior_level_reclaim_smooth_pnl": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 1971.0}, "topstep_volatility_expansion_limited_risk": {"false_discovery_risk_proxy": 0.920308409934342, "signal_rate": 0.0219811590065658, "strong_count": 38.0, "topstep_viable_count": 39.0, "trial_count": 3503.0}, "topstep_vwap_exhaustion_payout_engine": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 3530.0}}`
- effective_independent_trials_proxy: 18673.45
- selection_adjusted_best_promotion_proxy: 0.8803272775783093
- cache_audit: `{"cache_dir": "/root/hydra-bot/data/cache/databento", "checksums": {"/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst": "f34c04855f60d9eb0fefcec03711272d55915859a0f5bd162c0a38468dee388e", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet": "4540b7585caa2798998bd39a7d787c948f1f7c82f1d692e55118c4d4fffc5911", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst": "1604633e2056345005180c40295d83985f85e90641c0194156f5cf952b11a885", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet": "c1af5bb43aa0ac5b83d9d7bab8d20b2ef52c97089f7acccc8d19231ce9e647dc", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst": "d609fccf70bf533102cf8c8e5406a60643decc77f4573505edeab47db5e3e889", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet": "97914227a87a5cd6b1c90f11de2061054a66f0a2b313f8120b8831af06b6c85e", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst": "0a7fbddf8cdcf0e3db1f3fb1774524465ef4905c3fd8142a713d480255c1cd6b"}, "file_count": 7, "files": ["/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst"]}`
- data_acquisition: `{"q2": {"cache_hit": true, "checksum": "c1af5bb43aa0ac5b83d9d7bab8d20b2ef52c97089f7acccc8d19231ce9e647dc", "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "reason": "complete_cache_hit", "request_id": "96620a64bd782e0bb5eb"}, "q3": {"cache_hit": true, "checksum": "97914227a87a5cd6b1c90f11de2061054a66f0a2b313f8120b8831af06b6c85e", "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "reason": "complete_cache_hit", "request_id": "582a5e3148596e41b8be"}, "q4": {"cache_hit": true, "checksum": null, "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.parquet", "reason": "duplicate_request_blocked_by_ledger", "request_id": "6d78163e69760f304b04"}}`
- budget_ledger_records: 13
- q1_manifest_path: /root/hydra-bot/reports/lockbox/q1_remediation_freeze_1ca6943485c73fd8.json
- q3_manifest_path: /root/hydra-bot/reports/lockbox/q3_quarantined_confirmation_manifest_8f398a42aa6ef606.json
- q3_lockbox_contaminated: True
- lockbox_integrity: Q3 contaminated for affected lineages; Q4 raw-only remains uninspected
- rule_snapshot_path: config/prop_firms/topstep_150k_2026-07-10.yaml
- budget_ledger_path: reports/data_budget/databento_spend_ledger.jsonl
- data_access_ledger_path: reports/data_access/data_access_ledger.jsonl
- checkpoint_folder: reports/checkpoints/gate_aware_remediation
- resume_command: python scripts/run_gate_aware_remediation_factory.py --registry registry/hydra_registry.db --dataset GLBX.MDP3 --symbols ES MES NQ MNQ --development-start 2024-01-01 --development-end 2024-03-29 --q2-start 2024-04-01 --q2-end 2024-07-01 --q3-start 2024-07-01 --q3-end 2024-10-01 --q4-start 2024-10-01 --q4-end 2025-01-01 --schema ohlcv-1m --databento-budget-usd 100 --databento-budget-start 2026-07-10 --auto-purchase-under-budget --budget-safety-ceiling-usd 98 --primary-topstep-mode no-dll --evaluate-xfa-standard --evaluate-xfa-consistency --evaluate-optional-dll-sensitivity --account-size 150000 --profit-target 9000 --mll-distance 4500 --workers 3 --single-writer-registry --min-runtime-hours 0.083 --max-runtime-hours 0.095 --continue-until-deadline --minimum-cycles 3 --minimum-remediation-children 200 --stop-only-on-valid-quality-target --checkpoint-every-minutes 1 --target-economic-strategy-units 50 --max-remediation-children 0 --cycle-size 40 --creative-exploration-ratio 0.1 --strict-lockbox --conservative-intrabar --seed 8050 --report-tag gate_aware_remediation_run_control_smoke_v1
- checkpoint_path: /root/hydra-bot/reports/checkpoints/gate_aware_remediation/gate_aware_checkpoint_2026-07-10T015851Z0000_gate_aware_remediation_run_control_smoke_v1.md
