# HYDRA Gate-Aware Remediation Report

Generated: 2026-07-10T01:24:20+00:00

## Warning
- This is historical research only. It is not live trading approval.
- Q1/Q2 evidence cannot create TRADING_READY_CANDIDATE status.
- Q4 lockbox remains unavailable for tuning.

## Summary
- baseline_commit: bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1
- current_commit: bad3b5e77400ae676f1d7c0c047eb9fd3e41e9c1
- registry_integrity: ok
- runtime_seconds: 190.59
- workers_used: 3
- starting_candidates: 17994
- total_candidates: 17994
- remediation_children_tested: 0
- parent_to_child_improvement_rate: 0.0
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
- economic_strategy_units: 14444
- equivalence_clusters: 14444
- best_pareto_candidates: `["cand_73dfc9dbbd04", "cand_7d0b38decded", "cand_c246d0ee5c7e", "cand_c7dee5d0df2e", "cand_ed88f3d3724e", "cand_2ae4cb1a81df", "cand_a15a83d8f323", "cand_84aabeac28f9", "cand_ca84ada596f1", "cand_e037e90e5142", "cand_2d371a60c839", "cand_366fe22ca205", "cand_c61c340b2cc6", "cand_32ce229d68d7", "cand_3df28d2ab2dc", "cand_19e9aab70e8b", "cand_78724dc9f509", "cand_e6724984107d", "cand_f35423abe7f3", "cand_e781dcff173b"]`
- portfolio_baskets: `[{"candidate_ids": ["cand_73dfc9dbbd04", "cand_7d0b38decded", "cand_c246d0ee5c7e", "cand_c7dee5d0df2e", "cand_ed88f3d3724e", "cand_2ae4cb1a81df", "cand_a15a83d8f323", "cand_84aabeac28f9", "cand_ca84ada596f1", "cand_e037e90e5142"], "estimated_net_profit": 89674.7958692688, "estimated_trader_net_payout": 50382.208729249716, "executable": true, "notes": ["approximation_from_registry_metrics_not_trade_level_schedule"], "policy": "balanced_pareto_one_account", "shared_mll_respected": true}]`
- status_distribution: `{"DEAD_STRATEGY": 15410, "ECONOMICALLY_VIABLE": 403, "PROMISING_NEEDS_MUTATION": 129, "TOPSTEP_COMBINE_FAILED_MLL": 643, "TOPSTEP_COMBINE_FAILED_TARGET": 1357, "TOPSTEP_NEAR_MISS": 31, "TOPSTEP_VIABLE": 21}`
- family_fdr_proxy: `{"topstep_micro_scaling_mes_mnq": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 453.0}, "topstep_nq_es_divergence_controlled": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.007908264136022143, "strong_count": 21.0, "topstep_viable_count": 19.0, "trial_count": 5058.0}, "topstep_opening_range_controlled_runner": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0005685048322910744, "strong_count": 1.0, "topstep_viable_count": 1.0, "trial_count": 3518.0}, "topstep_prior_level_reclaim_smooth_pnl": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 1971.0}, "topstep_volatility_expansion_limited_risk": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0005773672055427252, "strong_count": 1.0, "topstep_viable_count": 1.0, "trial_count": 3464.0}, "topstep_vwap_exhaustion_payout_engine": {"false_discovery_risk_proxy": 1.0, "signal_rate": 0.0, "strong_count": 0.0, "topstep_viable_count": 0.0, "trial_count": 3530.0}}`
- effective_independent_trials_proxy: 16751.5
- selection_adjusted_best_promotion_proxy: 0.8611767631576086
- cache_audit: `{"cache_dir": "/root/hydra-bot/data/cache/databento", "checksums": {"/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst": "f34c04855f60d9eb0fefcec03711272d55915859a0f5bd162c0a38468dee388e", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet": "4540b7585caa2798998bd39a7d787c948f1f7c82f1d692e55118c4d4fffc5911"}, "file_count": 2, "files": ["/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.dbn.zst", "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet"]}`
- data_acquisition: `{"q2": {"cache_hit": false, "checksum": null, "download": {"cache_hit": false, "dry_run": false, "estimate": {"billable_size_bytes": 19806024, "estimated_cost_usd": 1.29120580852, "record_count": 353679}, "network_request_made": true, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "raw_output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst", "record_count": null, "request": {"api_symbols": ["ES.c.0", "MES.c.0", "NQ.c.0", "MNQ.c.0"], "cache_folder": "data/cache/databento", "dataset": "GLBX.MDP3", "end": "2024-07-01", "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "raw_output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.dbn.zst", "schema": "ohlcv-1m", "start": "2024-04-01", "stype_in": "continuous", "stype_out": "instrument_id", "symbol_map": {"ES.c.0": "ES", "MES.c.0": "MES", "MNQ.c.0": "MNQ", "NQ.c.0": "NQ"}, "symbols": ["ES", "MES", "NQ", "MNQ"], "timeframe": "1m"}, "validation": {"columns": ["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "session_id"], "duplicate_timestamp_symbol_rows": 0, "future_timestamp_rows": 0, "missing_intervals": {"ES": {"gap_count_gt_1m": 407, "max_gap_seconds": 203460.0}, "MES": {"gap_count_gt_1m": 366, "max_gap_seconds": 203460.0}, "MNQ": {"gap_count_gt_1m": 121, "max_gap_seconds": 203460.0}, "NQ": {"gap_count_gt_1m": 339, "max_gap_seconds": 203460.0}}, "row_count": 353679, "rows_by_symbol": {"ES": 88257, "MES": 88404, "MNQ": 88709, "NQ": 88309}}}, "estimated_cost_usd": 1.29120580852, "ledger": {"actual_cost_usd": 1.29120580852, "approval_mode": "AUTO_UNDER_HARD_CAP", "cache_hit": false, "candidate_tier": "validation_package", "checksum": "c1af5bb43aa0ac5b83d9d7bab8d20b2ef52c97089f7acccc8d19231ce9e647dc", "cumulative_actual_spend_usd": 1.29120580852, "cumulative_estimated_spend_usd": 1.29120580852, "dataset": "GLBX.MDP3", "download_status": "DOWNLOADED", "end": "2024-07-01", "estimated_cost_usd": 0.0, "request_id": "96620a64bd782e0bb5eb", "research_purpose": "Q2 confirmation OHLCV", "resulting_file": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "schema": "ohlcv-1m", "start": "2024-04-01", "stype_in": "continuous", "symbols": ["ES", "MES", "NQ", "MNQ"], "timestamp_utc": "2026-07-10T01:21:33.918892+00:00"}, "may_download": true, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet", "reason": "cache_miss_under_budget", "request_id": "96620a64bd782e0bb5eb", "status": "downloaded"}, "q3": {"cache_hit": false, "checksum": null, "download": {"cache_hit": false, "dry_run": false, "estimate": {"billable_size_bytes": 20101480, "estimated_cost_usd": 1.310467347503, "record_count": 358955}, "network_request_made": true, "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "raw_output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst", "record_count": null, "request": {"api_symbols": ["ES.c.0", "MES.c.0", "NQ.c.0", "MNQ.c.0"], "cache_folder": "data/cache/databento", "dataset": "GLBX.MDP3", "end": "2024-10-01", "output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet", "raw_output_path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.dbn.zst", "schema": "ohlcv-1m", "start": "2024-07-01", "stype_in": "continuous", "stype_out": "instrument_id", "symbol_map": {"ES.c.0": "ES", "MES.c.0": "MES", "MNQ.c.0": "MNQ", "NQ.c.0": "NQ"}, "symbols": ["ES", "MES", "NQ", "MNQ"], "timeframe": "1m"}, "validation": {"columns": ["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume",`
- budget_ledger_records: 6
- q1_manifest_path: /root/hydra-bot/reports/lockbox/q1_remediation_freeze_021433c697d1462b.json
- q3_manifest_path: /root/hydra-bot/reports/lockbox/q3_blind_validation_freeze_b886434ff3b40460.json
- rule_snapshot_path: config/prop_firms/topstep_150k_2026-07-10.yaml
- budget_ledger_path: reports/data_budget/databento_spend_ledger.jsonl
- data_access_ledger_path: reports/data_access/data_access_ledger.jsonl
- checkpoint_folder: reports/checkpoints/gate_aware_remediation
- resume_command: python scripts/run_gate_aware_remediation_factory.py --registry registry/hydra_registry.db --dataset GLBX.MDP3 --symbols ES MES NQ MNQ --development-start 2024-01-01 --development-end 2024-03-29 --q2-start 2024-04-01 --q2-end 2024-07-01 --q3-start 2024-07-01 --q3-end 2024-10-01 --q4-start 2024-10-01 --q4-end 2025-01-01 --schema ohlcv-1m --databento-budget-usd 100 --databento-budget-start 2026-07-10 --auto-purchase-under-budget --budget-safety-ceiling-usd 98 --primary-topstep-mode no-dll --evaluate-xfa-standard --evaluate-xfa-consistency --evaluate-optional-dll-sensitivity --account-size 150000 --profit-target 9000 --mll-distance 4500 --workers auto --single-writer-registry --runtime-hours 0.03 --checkpoint-every-minutes 0.2 --target-economic-strategy-units 50 --strict-lockbox --conservative-intrabar --seed 5050 --report-tag gate_aware_remediation_smoke_v1
- checkpoint_path: /root/hydra-bot/reports/checkpoints/gate_aware_remediation/gate_aware_checkpoint_2026-07-10T012420Z0000_gate_aware_remediation_smoke_v1.md
