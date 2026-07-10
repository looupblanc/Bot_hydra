# HYDRA Gate-Aware Remediation Report

Generated: 2026-07-10T01:27:25+00:00

## Warning
- This is historical research only. It is not live trading approval.
- Q1/Q2 evidence cannot create TRADING_READY_CANDIDATE status.
- Q4 lockbox remains unavailable for tuning.

## Summary
- baseline_commit: bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1
- current_commit: bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1
- registry_integrity: ok
- runtime_seconds: 77.11
- workers_used: 3
- starting_candidates: 17994
- total_candidates: 18010
- remediation_children_tested: 16
- parent_to_child_improvement_rate: 0.25
- dossiers_generated: 381
- topstep_viable_reaudited: 21
- near_misses_analyzed: 160
- economically_viable_analyzed: 200
- hard_invalid_count: 0
- repairable_count: 381
- candidates_failing_exactly_one_gate: 0
- candidates_failing_exactly_two_gates: 11
- q1_promotion_finalists: 381
- q2_confirmed_candidates: 0
- q3_blind_validation_passes: 0
- execution_validation_passes: 0
- q4_lockbox_passes: 0
- trading_ready_candidates: 0
- economic_strategy_units: 14460
- equivalence_clusters: 14460
- best_pareto_candidates: `["rem_2c17d7bdc174", "cand_73dfc9dbbd04", "rem_e49fceafba04", "rem_ad2e79e821be", "cand_7d0b38decded", "cand_c246d0ee5c7e", "rem_1347685ea766", "cand_2ae4cb1a81df", "rem_8f755cb1944d", "cand_a15a83d8f323", "rem_bbabbb693c88", "cand_84aabeac28f9", "cand_ca84ada596f1", "cand_e037e90e5142", "cand_2d371a60c839", "cand_c61c340b2cc6", "cand_3df28d2ab2dc", "cand_19e9aab70e8b", "cand_78724dc9f509", "cand_e6724984107d"]`
- portfolio_baskets: `[{"candidate_ids": ["rem_2c17d7bdc174", "cand_73dfc9dbbd04", "cand_7d0b38decded", "cand_c246d0ee5c7e", "cand_c7dee5d0df2e", "cand_ed88f3d3724e", "cand_2ae4cb1a81df", "cand_a15a83d8f323", "rem_bbabbb693c88", "cand_84aabeac28f9"], "estimated_net_profit": 78426.61847550306, "estimated_trader_net_payout": 52789.692872911764, "executable": true, "notes": ["approximation_from_registry_metrics_not_trade_level_schedule"], "policy": "balanced_pareto_one_account", "shared_mll_respected": true}]`
- status_distribution: `{"DEAD_STRATEGY": 15420, "ECONOMICALLY_VIABLE": 404, "PROMISING_NEEDS_MUTATION": 129, "TOPSTEP_COMBINE_FAILED_MLL": 643, "TOPSTEP_COMBINE_FAILED_TARGET": 1357, "TOPSTEP_NEAR_MISS": 33, "TOPSTEP_VIABLE": 24}`
- family_fdr_proxy: `{"topstep_micro_scaling_mes_mnq": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 453.0}, "topstep_nq_es_divergence_controlled": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.010644589000591367, "strong_count": 32.0, "topstep_viable_count": 22.0, "trial_count": 5073.0}, "topstep_opening_range_controlled_runner": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0005683432793407218, "strong_count": 1.0, "topstep_viable_count": 1.0, "trial_count": 3519.0}, "topstep_prior_level_reclaim_smooth_pnl": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 1971.0}, "topstep_volatility_expansion_limited_risk": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0005773672055427252, "strong_count": 1.0, "topstep_viable_count": 1.0, "trial_count": 3464.0}, "topstep_vwap_exhaustion_payout_engine": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 3530.0}}`
- effective_independent_trials_proxy: 16767.5
- selection_adjusted_best_promotion_proxy: 0.8776419522258947
- cache_audit: `{"cache_dir": "/root/hydra-bot/data/cache/databento", "checksums": {"/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst": "f34c04855f60d9eb0fefcec03711272d55915859a0f5bd162c0a38468dee388e", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet": "4540b7585caa2798998bd39a7d787c948f1f7c82f1d692e55118c4d4fffc5911", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst": "1604633e2056345005180c40295d83985f85e90641c0194156f5cf952b11a885", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet": "c1af5bb43aa0ac5b83d9d7bab8d20b2ef52c97089f7acccc8d19231ce9e647dc", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst": "d609fccf70bf533102cf8c8e5406a60643decc77f4573505edeab47db5e3e889", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet": "97914227a87a5cd6b1c90f11de2061054a66f0a2b313f8120b8831af06b6c85e", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst": "0a7fbddf8cdcf0e3db1f3fb1774524465ef4905c3fd8142a713d480255c1cd6b"}, "file_count": 7, "files": ["/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.dbn.zst"]}`
- data_acquisition: `{"q2": {"cache_hit": true, "checksum": "c1af5bb43aa0ac5b83d9d7bab8d20b2ef52c97089f7acccc8d19231ce9e647dc", "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "reason": "complete_cache_hit", "request_id": "96620a64bd782e0bb5eb"}, "q3": {"cache_hit": true, "checksum": "97914227a87a5cd6b1c90f11de2061054a66f0a2b313f8120b8831af06b6c85e", "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "reason": "complete_cache_hit", "request_id": "043a8ee0983dbdc3c8b5"}, "q4": {"cache_hit": true, "checksum": null, "estimated_cost_usd": 0.0, "may_download": false, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-10-01_2025-01-01.parquet", "reason": "duplicate_request_blocked_by_ledger", "request_id": "6d78163e69760f304b04"}}`
- budget_ledger_records: 9
- q1_manifest_path: /root/hydra-bot/reports/lockbox/q1_remediation_freeze_fb5f678a6814a5c8.json
- q3_manifest_path: /root/hydra-bot/reports/lockbox/q3_blind_validation_freeze_89f241f790900d79.json
- q3_lockbox_contaminated: True
- lockbox_integrity: Q3 contaminated for affected lineages; Q4 raw-only remains uninspected
- rule_snapshot_path: config/prop_firms/topstep_150k_2026-07-10.yaml
- budget_ledger_path: reports/data_budget/databento_spend_ledger.jsonl
- data_access_ledger_path: reports/data_access/data_access_ledger.jsonl
- checkpoint_folder: reports/checkpoints/gate_aware_remediation
- resume_command: python scripts/run_gate_aware_remediation_factory.py --registry registry/hydra_registry.db --dataset GLBX.MDP3 --symbols ES MES NQ MNQ --development-start 2024-01-01 --development-end 2024-03-29 --q2-start 2024-04-01 --q2-end 2024-07-01 --q3-start 2024-07-01 --q3-end 2024-10-01 --q4-start 2024-10-01 --q4-end 2025-01-01 --schema ohlcv-1m --databento-budget-usd 100 --databento-budget-start 2026-07-10 --auto-purchase-under-budget --budget-safety-ceiling-usd 98 --primary-topstep-mode no-dll --evaluate-xfa-standard --evaluate-xfa-consistency --evaluate-optional-dll-sensitivity --account-size 150000 --profit-target 9000 --mll-distance 4500 --workers auto --single-writer-registry --runtime-hours 0.08 --checkpoint-every-minutes 0.2 --target-economic-strategy-units 50 --strict-lockbox --conservative-intrabar --seed 6050 --report-tag gate_aware_remediation_smoke_v2
- checkpoint_path: /root/hydra-bot/reports/checkpoints/gate_aware_remediation/gate_aware_checkpoint_2026-07-10T012725Z0000_gate_aware_remediation_smoke_v2.md
