# OOS Failure Forensics

This is historical research only. No live trading approval is implied.

## created_at
```json
"2026-07-10T05:00:49+00:00"
```

## runtime_seconds
```json
262.73
```

## registry_integrity
```json
"ok"
```

## registry_total
```json
75312
```

## q3_status
```json
"quarantined_contaminated_not_used"
```

## q4_status
```json
"sealed_uninspected_not_loaded"
```

## new_databento_purchase
```json
false
```

## pytest_required
```json
"run separately by CI/preflight; this script does not invoke pytest"
```

## oos_threshold
```json
0.35
```

## oos_failure_distribution
```json
{
  "SPLIT_CONCENTRATION": 26096,
  "THRESHOLD_TOO_STRICT": 533,
  "TRUE_EDGE_DECAY": 45831
}
```

## oos_failure_count
```json
72460
```

## strata_counts
```json
{
  "best_500_topstep_viable": 500,
  "closest_to_oos_threshold": 500,
  "non_nq_es_families": 500,
  "parents_children_by_policy": 400,
  "q1_core_robust": 4099,
  "random_oos_failures": 500,
  "top_nq_es_divergence_lineages": 500
}
```

## full_recompute_count
```json
101
```

## recomputed_oos_distribution
```json
{
  "INSUFFICIENT_OOS_TRADES": 2,
  "SPLIT_CONCENTRATION": 3,
  "THRESHOLD_TOO_STRICT": 11,
  "TRUE_EDGE_DECAY": 85
}
```

## oos_gate_bug
```json
{
  "contains_bug": false,
  "finding": "No OOS metric-direction bug detected in persisted split scores.",
  "metric_direction_bug_count": 0,
  "status_inheritance_bug_count": 0
}
```

## threshold_defensibility
```json
{
  "defensible": true,
  "finding": "Do not weaken OOS. The zero-pass result is driven mostly by low March split scores, not a narrow threshold cliff.",
  "missing_split_evidence_count": 0,
  "near_threshold_count": 533,
  "near_threshold_share": 0.007356
}
```

## status_provenance_audit
```json
{
  "canary_status_counts": {
    "ECONOMICALLY_VIABLE": 77,
    "TOPSTEP_NEAR_MISS": 10,
    "TOPSTEP_VIABLE": 123
  },
  "child_status_inheritance_detected": "not_provable_from_legacy_rows; new pipeline recomputes and fingerprints inputs",
  "existing_statuses_final_promotion_usable": 0,
  "finding": "Historical statuses are legacy-unversioned for final promotion. New canary/recompute statuses record version, input fingerprint, mode, and evidence strength.",
  "legacy_registry_evidence_strength": {
    "LEGACY_UNVERSIONED": 75312
  },
  "legacy_rows_with_validation_version": 0,
  "recomputed_computation_modes_sample": {
    "PROXY": 101
  },
  "recomputed_true_status_counts_sample": {
    "DEAD_STRATEGY": 4,
    "ECONOMICALLY_VIABLE": 23,
    "TOPSTEP_NEAR_MISS": 21,
    "TOPSTEP_VIABLE": 53
  }
}
```

## reward_delta_definition
```json
"child promotion_score - parent promotion_score is local-only; corrected reward uses component-weighted promotion advancement where positive is beneficial."
```

## corrected_reward_components
```json
[
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.001020892380014023,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": 0.0,
    "total": 1.168979
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0010574835050162316,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": 0.0,
    "total": 1.168943
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0009548525749899757,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -0.080955
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0009723797966580605,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -0.080972
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.000947794226667611,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 0.0,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -1.330948
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0009767141116511387,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": 0.0,
    "total": 1.169023
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0009649725433458419,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": 0.0,
    "total": 1.169035
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0010238932616630336,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -0.081024
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0009675996933219722,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 1.25,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -0.080968
  },
  {
    "complexity_penalty": -0.08,
    "compute_cost_penalty": -0.0013200043133353271,
    "duplicate": 0.0,
    "gate_distance_reduction": 0.0,
    "hard_invalid": 0.0,
    "local_improvement": 0.0,
    "new_behavior_cluster": 0.0,
    "oos_pass": 0.0,
    "passed_failed_gate": 0.0,
    "policy": "target_velocity_runner",
    "provisional_status_penalty": 0.0,
    "q1_core_robust": 0.0,
    "q2_confirmation": 0.0,
    "tail_or_mll_regression": -1.25,
    "total": -1.33132
  }
]
```

## bandit_allocation_caps
```json
{
  "max_family_share": 0.3,
  "max_lineage_share": 0.02,
  "max_policy_share": 0.35,
  "minimum_controlled_exploration_share": 0.15
}
```

## roll_audit
```json
{
  "contract_definitions_available": false,
  "explicit_roll_mapping_available": false,
  "notes": [
    "Cached OHLCV uses continuous symbols; explicit raw contract mapping and definitions were not present in the registry.",
    "Q4 was not loaded or inspected."
  ],
  "roll_artifact_suspected": true,
  "status": "partial_continuous_ohlcv_only",
  "symbol_windows": {
    "ES": {
      "2024-03-15": {
        "bars": 4826,
        "gap_suspected": false,
        "max_abs_close_pct_change": 0.013144
      },
      "2024-06-21": {
        "bars": 0,
        "gap_suspected": true
      }
    },
    "MES": {
      "2024-03-15": {
        "bars": 4891,
        "gap_suspected": false,
        "max_abs_close_pct_change": 0.013094
      },
      "2024-06-21": {
        "bars": 0,
        "gap_suspected": true
      }
    },
    "MNQ": {
      "2024-03-15": {
        "bars": 5063,
        "gap_suspected": false,
        "max_abs_close_pct_change": 0.010583
      },
      "2024-06-21": {
        "bars": 0,
        "gap_suspected": true
      }
    },
    "NQ": {
      "2024-03-15": {
        "bars": 4681,
        "gap_suspected": false,
        "max_abs_close_pct_change": 0.009764
      },
      "2024-06-21": {
        "bars": 0,
        "gap_suspected": true
      }
    }
  },
  "timestamp_range": {
    "end": "2024-03-28 20:59:00+00:00",
    "start": "2024-01-01 23:00:00+00:00"
  }
}
```

## nq_es_divergence_family_diagnosis
```json
{
  "diagnosis": "Current top candidates are NQ/ES variants failing March OOS; treat as a concentrated lineage until trade-overlap evidence proves distinct behavior.",
  "family_rows": 61844,
  "oos_failure_distribution": {
    "SPLIT_CONCENTRATION": 26089,
    "THRESHOLD_TOO_STRICT": 452,
    "TRUE_EDGE_DECAY": 34586
  },
  "roll_note": "Continuous OHLCV alone cannot rule out roll-generated relative-value artifacts; explicit contract mapping remains required before trusting this family.",
  "top50_failed_gate_patterns": {
    "('OOS',)": 50
  }
}
```

## behavioral_evidence_manifest
```json
{
  "created_at": "2026-07-10T04:58:22+00:00",
  "ledger_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T045821+0000_oos_failure_forensics_v2/trade_ledgers.jsonl.gz",
  "ledger_row_count": 17096,
  "ledger_sha256": "8af285c9c79e98605359664abb8d2843fb7157cdf9d2f757276b4a3a44af83b3",
  "sketch_count": 101,
  "sketch_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T045821+0000_oos_failure_forensics_v2/behavioral_sketches.jsonl.gz",
  "sketch_schema": {
    "candidate_id": "str",
    "daily_pnl_hash": "sha256",
    "direction_signature": "sha256",
    "entry_overlap_signature": "sha256",
    "holding_time_histogram": "dict[str,int]",
    "parent_candidate_id": "str|null",
    "regime_exposure": "dict[str,int]",
    "session_histogram": "dict[str,int]",
    "symbol_exposure": "dict[str,int]",
    "tail_event_signature": "sha256",
    "trade_timestamp_signature": "sha256",
    "validation_hash": "sha256"
  },
  "sketch_sha256": "440fcc3fc99d5685fcc2d10ee928f489cde298016dfc3bf9db368019e6c3cd83",
  "storage_note": "Full behavioral evidence is under ignored data/cache and must not be committed.",
  "tag": "20260710T045821+0000_oos_failure_forensics_v2",
  "trade_ledger_schema": {
    "candidate_id": "str",
    "commissions": "float",
    "contract": "str|null",
    "direction": "int",
    "entry_price": "float",
    "entry_timestamp": "iso8601",
    "exit_price": "float",
    "exit_timestamp": "iso8601",
    "gross_pnl": "float",
    "mae": "float",
    "mfe": "float",
    "net_pnl": "float",
    "parent_candidate_id": "str|null",
    "quantity": "float",
    "reason_for_entry": "str",
    "reason_for_exit": "str",
    "regime": "str",
    "session": "str",
    "slippage": "float",
    "symbol": "str",
    "validation_period": "str"
  }
}
```

## valid_economic_clusters
```json
{
  "cluster_sizes": [
    4,
    4,
    3,
    3,
    3,
    2,
    2,
    2,
    2,
    2,
    2,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1
  ],
  "dominant_cluster_size": 4,
  "evidence_backed_candidates": 101,
  "representatives": [
    "rem_fa32ae139043",
    "rem_6678663d5ef1",
    "rem_c17d988d0a0b",
    "rem_9dcd813b8ed7",
    "rem_527908ceb6b1",
    "rem_d99d433e0157",
    "rem_ffde48550282",
    "rem_39461ae7c4bc",
    "rem_9efd9cb7573a",
    "rem_5b133897fcd7",
    "rem_0c1d1d1cff52",
    "rem_881afc27c862",
    "rem_24110e8619f6",
    "rem_cfb7ef7331fe",
    "rem_0cc911d43c29",
    "rem_676f8f367a6d",
    "rem_fdbfa24363da",
    "rem_692cec6cc02d",
    "rem_fda03e858f8e",
    "rem_69701a3e21ca",
    "rem_4bfc09c992cf",
    "rem_85afc78a1b5e",
    "rem_1f8a0039139f",
    "rem_161e9a1d24b4",
    "cand_cf9e14131fa2"
  ],
  "uncertainty": "Exact clustering is limited to recomputed candidates with persisted sketches.",
  "valid_economic_clusters": 83
}
```

## canary
```json
{
  "allocation_note": "Balanced canary; no registry rows written.",
  "completed_children": 210,
  "duplicates": 0,
  "mean_reward_by_policy": {
    "consistency_daily_lock": -0.164389,
    "mll_buffer_derisk": -0.042741,
    "oos_simplify": -0.459412,
    "payout_frequency": 0.33521,
    "portfolio_role_shift": -0.417912,
    "sequence_fragility_smooth": -1.206017,
    "target_velocity_runner": 0.085578
  },
  "oos_passes": 0,
  "policy_allocation": {
    "consistency_daily_lock": 30,
    "mll_buffer_derisk": 30,
    "oos_simplify": 30,
    "payout_frequency": 30,
    "portfolio_role_shift": 30,
    "sequence_fragility_smooth": 30,
    "target_velocity_runner": 30
  },
  "promotion_stage_progress_by_policy": {},
  "requested_children": 210,
  "reward_components": [
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.001020892380014023,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.168979
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0010574835050162316,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.168943
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0009548525749899757,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -0.080955
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0009723797966580605,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -0.080972
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.000947794226667611,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 0.0,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -1.330948
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0009767141116511387,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.169023
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0009649725433458419,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.169035
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0010238932616630336,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -0.081024
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0009675996933219722,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -0.080968
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0013200043133353271,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 0.0,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -1.33132
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.001426926766653196,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.168573
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0015010289599983178,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": 0.0,
      "total": 1.168499
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.001353391631661604,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regression": -1.25,
      "total": -0.081353
    },
    {
      "complexity_penalty": -0.08,
      "compute_cost_penalty": -0.0013875387383450288,
      "duplicate": 0.0,
      "gate_distance_reduction": 0.0,
      "hard_invalid": 0.0,
      "local_improvement": 0.0,
      "new_behavior_cluster": 0.0,
      "oos_pass": 0.0,
      "passed_failed_gate": 0.0,
      "policy": "target_velocity_runner",
      "provisional_status_penalty": 0.0,
      "q1_core_robust": 1.25,
      "q2_confirmation": 0.0,
      "tail_or_mll_regre
```

## q2_candidate_set
```json
{
  "candidate_ids": [
    "rem_676f8f367a6d",
    "rem_1f8a0039139f",
    "rem_114a17fb1c8c",
    "rem_cf1d9d72b129",
    "rem_b4043f2fd47b",
    "rem_7ce984309750",
    "rem_d2381920815d",
    "rem_485400b5bafb",
    "rem_ad6fc048113e",
    "rem_507619173f35",
    "rem_ad5dc854fc28",
    "rem_f0c9f85ab78e",
    "rem_f9c28f3d2f44",
    "rem_db423a52b802",
    "rem_0d6051ec48c4",
    "rem_48ae528c05e0",
    "rem_bc2634aeee51",
    "rem_689faa5a4a44",
    "rem_b35d2049c890",
    "rem_d074c5aca1c0",
    "rem_dbfbb652e74c",
    "rem_0318d5c0c44a",
    "rem_82392d7b0080",
    "rem_3f0579d5a8d8",
    "rem_387acb0c27a6",
    "rem_c6b31a6b9ed1",
    "rem_36c013ab62d2",
    "rem_9af4b5386cc5",
    "rem_f9d76f78bb63",
    "rem_ee6491b3140b",
    "rem_4878c1d7ab7c",
    "rem_7a6980e8025e",
    "rem_60bfd595ae6b",
    "rem_4c396414d726",
    "rem_ba645bcc7096",
    "rem_31c9c690606b",
    "rem_eebdacff42b1",
    "rem_3d2ce569eaa8",
    "rem_d0fea828361d",
    "rem_0fbf559ca207",
    "rem_5b133897fcd7",
    "rem_b1a99af6ec72",
    "rem_c125d8dba696",
    "rem_2180b2720e06",
    "rem_f23c42603dd3",
    "rem_12bc4167b90b",
    "rem_f8b24ef8f57a",
    "rem_e5ecf52d7e70",
    "rem_41bc75888c12",
    "rem_7925b343db64",
    "rem_c1fb3005131c",
    "rem_0f4b0132bc01",
    "rem_d74f31102f30",
    "rem_652a9dd7bb99",
    "rem_4957b68da62e",
    "rem_7e2847848ce5",
    "rem_c3eaffacc926",
    "rem_b93b10a2af8e",
    "rem_f1eeac2e57e0",
    "rem_41c809447e23",
    "rem_3084165e6753",
    "rem_0c1d1d1cff52",
    "rem_1b6c9599067d",
    "rem_a616afc9aa2d",
    "rem_f20e2ab55e61",
    "rem_47e50dc855cb",
    "rem_39461ae7c4bc",
    "rem_fb44b8e6f616",
    "rem_cfb7ef7331fe",
    "rem_5a2b3bf704f9",
    "rem_d7915f9dcef7",
    "rem_b9d84ed4f334",
    "rem_b5f396eb1596",
    "rem_58078cd9129c",
    "rem_b2f44e7dd9d4",
    "rem_736c872da6b3",
    "rem_c998af58f980",
    "rem_8da33e246e09",
    "rem_1377b7527d14"
  ],
  "count": 79,
  "manifest_hash": "c256dadbd42bd9a62d30b5c8e70374b1b5c17a98c8b7b30c3775d40296a78392",
  "manifest_path": "/root/hydra-bot/reports/lockbox/q2_confirmation_freeze_c256dadbd42bd9a6.json"
}
```

## next_command
```json
"python scripts/audit_oos_failures.py --registry registry/hydra_registry.db --full-recompute-limit 300 --canary-children 280 --q2-candidate-limit 100 --report-tag oos_failure_forensics_v2"
```

## files_note
```json
"Raw behavioral evidence is stored under ignored data/cache/behavioral_evidence; commit only this report, source, tests, and manifests."
```
