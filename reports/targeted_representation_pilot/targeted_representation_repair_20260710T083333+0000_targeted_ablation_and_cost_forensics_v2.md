# Targeted Representation Repair Lab targeted_ablation_and_cost_forensics_v2

Historical research only. No live trading approval.

- Q4 seal: PASSED_NO_Q4_ACCESS
- Cost classification: SIZING_BUG
- Corrected paired mean cost: 669.808333
- Corrected paired median cost: 727.75
- Prototypes proposed: 80
- Pre-backtest rejected: 63
- Representation evidence passes: 4
- Topstep compatible: 1

```json
{
  "baseline_commit": "390855724e92153b0a243ed04f5809b909918f7f",
  "behavioral_clusters": {
    "calibration": {
      "cluster_stability": 1.0,
      "control_pairs": 17,
      "false_merge_rate": 0.0,
      "false_split_rate": 0.0,
      "negative_controls": 11,
      "pair_decisions": [
        {
          "control_type": "exact_duplicate",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_exact_duplicate",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "metadata_only_change",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_metadata_only_change",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "tiny_stop_neighbor",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_tiny_stop_neighbor",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "tiny_target_neighbor",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_tiny_target_neighbor",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "same_signal_neighboring_parameter",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_same_signal_neighboring_parameter",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "parent_child_high_trade_overlap",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_parent_child_high_trade_overlap",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "different_session",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_session",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "opposite_direction",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_opposite_direction",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_instrument",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_instrument",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_holding_horizon",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_holding_horizon",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_tail_behavior",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_tail_behavior",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "low_trade_overlap",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_low_trade_overlap",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "different_regime",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_regime",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "different_portfolio_role",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "overnight_inventory_rth_resolution_09_00_different_portfolio_role",
          "should_cluster": false,
          "similarity": 0.442857
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "intraday_range_migration_path_asymmetry_02_00",
          "should_cluster": false,
          "similarity": 0.068389
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_09_00",
          "right": "intraday_range_migration_path_asymmetry_06_00",
          "should_cluster": false,
          "similarity": 0.053571
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "intraday_range_migration_path_asymmetry_02_00",
          "right": "intraday_range_migration_path_asymmetry_06_00",
          "should_cluster": false,
          "similarity": 0.335714
        }
      ],
      "positive_controls": 6,
      "precision_known_clones": 1.0,
      "recall_known_clones": 1.0,
      "recommended_thresholds": {
        "holding_similarity_threshold": 0.85,
        "overlap_threshold": 0.95,
        "pnl_correlation_threshold": 0.95,
        "tail_overlap_threshold": 0.8
      },
      "thresholds": {
        "holding_similarity_threshold": 0.85,
        "overlap_threshold": 0.95,
        "pnl_correlation_threshold": 0.95,
        "tail_overlap_threshold": 0.8
      },
      "uncertainty": "Controls are synthetic/local until a larger trade-ledger set exists."
    },
    "cluster_count": 3,
    "valid_economic_units": 3
  },
  "behavioral_rejected_count": 0,
  "checkpoint_path": null,
  "cost_forensics": {
    "classification": "SIZING_BUG",
    "contract_mix": {
      "MNQ/MES": 180
    },
    "corrected_cost": {
      "max": 952.5,
      "mean": 669.808333,
      "median": 727.75,
      "p25": 500.125,
      "p75": 864.25,
      "p90": 922.0
    },
    "cost_bug_existed": false,
    "hedge_ratio_distribution": {
      "max": 0.999976,
      "mean": 0.862714,
      "median": 0.918539,
      "p25": 0.785554,
      "p75": 0.963879,
      "p90": 0.986033
    },
    "legacy_mislabeled_cost": {
      "max": 1648.61015,
      "mean": 1135.891543,
      "median": 1217.096175,
      "p25": 824.688675,
      "p75": 1461.39035,
      "p90": 1571.02774
    },
    "legacy_reported_mean_cost_usd": 806.2583,
    "paired_lane_unfairly_penalized_by_cost": false,
    "quantity_distribution": {
      "max": 150.0,
      "mean": 105.916667,
      "median": 115.0,
      "p25": 79.0,
      "p75": 137.0,
      "p90": 145.0
    },
    "report_path": "/root/hydra-bot/reports/execution_cost_forensics/paired_execution_cost_forensics_20260710T083421+0000_targeted_ablation_and_cost_forensics_v2.md"
  },
  "created_at_utc": "2026-07-10T08:37:57+00:00",
  "falsified_formulations": [],
  "final_report_path": "/root/hydra-bot/reports/targeted_representation_pilot/targeted_representation_repair_20260710T083333+0000_targeted_ablation_and_cost_forensics_v2.md",
  "future_q4_freeze_candidates": [],
  "insufficient_evidence_formulations": [],
  "lane_dispositions": {
    "intraday_range_migration_path_asymmetry": "REPRESENTATION_EVIDENCE_PASS",
    "overnight_inventory_rth_resolution": "REPRESENTATION_EVIDENCE_PASS"
  },
  "matched_null_passes": 13,
  "new_databento_requests": [],
  "overnight_components_tested": [
    "overnight_displacement",
    "overnight_participation",
    "prior_value_position",
    "opening_response",
    "acceptance_rejection",
    "regime_context"
  ],
  "overnight_final_disposition": "REPRESENTATION_EVIDENCE_PASS",
  "overnight_incremental_components": [
    "overnight_displacement",
    "overnight_participation",
    "prior_value_position",
    "acceptance_rejection",
    "regime_context",
    "opening_response"
  ],
  "overnight_matched_null_result": {
    "best_probability_beats_null": 0.8888888888888888,
    "matched_null_beaten": 7,
    "tests": 18
  },
  "parameter_variants_tested": 0,
  "pre_backtest_duplicates_rejected": 63,
  "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
  "range_components_tested": [
    "accepted_center_migration",
    "time_at_extremes",
    "effort_vs_progress",
    "path_asymmetry",
    "range_relocation",
    "session_phase"
  ],
  "range_final_disposition": "REPRESENTATION_EVIDENCE_PASS",
  "range_incremental_components": [
    "accepted_center_migration",
    "session_phase",
    "time_at_extremes",
    "path_asymmetry",
    "range_relocation",
    "effort_vs_progress"
  ],
  "range_matched_null_result": {
    "best_probability_beats_null": 0.8888888888888888,
    "matched_null_beaten": 3,
    "tests": 18
  },
  "raw_economic_screen_passes": 3,
  "raw_positive_net_prototypes": 5,
  "remaining_budget_usd": 96.106305,
  "representation_evidence_passes": 4,
  "research_sample_step_minutes": 5,
  "spend_this_phase_usd": 0.0,
  "status_counts": {
    "FALSIFIED": 3,
    "MATCHED_NULL_BEATEN": 13,
    "RAW_ECONOMIC_SCREEN_PASS": 3,
    "RAW_POSITIVE_NET": 5,
    "REPRESENTATION_EVIDENCE_PASS": 4,
    "TOPSTEP_COMPATIBLE": 1,
    "TOPSTEP_PATH_CANDIDATE": 1
  },
  "structural_prototypes_tested": 17,
  "surviving_formulations": [
    "overnight_inventory_rth_resolution",
    "intraday_range_migration_path_asymmetry"
  ],
  "topstep_compatible_candidates": 1,
  "topstep_path_candidates": 1,
  "total_prototypes_proposed": 80,
  "warning": "Q1-Q3 are development/falsification data. Q4 remains sealed. Historical research only."
}
```
