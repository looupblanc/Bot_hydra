# Market Representation Lab bounded_market_representation_pilot_v1

Historical research only. This is not live trading approval.

- Q4 seal: PASSED_NO_Q4_ACCESS_BY_LAB
- Prototypes generated: 150
- Economically viable prototypes: 33
- Topstep-compatible prototypes: 0
- Future Q4 freeze candidates: 0

```json
{
  "baseline_commit": "792797245229361c752f342cfca4aaeb119b5eec",
  "behavioral_clusters": {
    "calibration": {
      "cluster_stability": 1.0,
      "control_pairs": 60,
      "false_merge_rate": 0.0,
      "false_split_rate": 0.0,
      "negative_controls": 54,
      "pair_decisions": [
        {
          "control_type": "exact_duplicate",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_exact_duplicate",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "metadata_only_change",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_metadata_only_change",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "tiny_stop_neighbor",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_tiny_stop_neighbor",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "tiny_target_neighbor",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_tiny_target_neighbor",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "same_signal_neighboring_parameter",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_same_signal_neighboring_parameter",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "parent_child_high_trade_overlap",
          "did_cluster": true,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_parent_child_high_trade_overlap",
          "should_cluster": true,
          "similarity": 1.0
        },
        {
          "control_type": "different_session",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_session",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "opposite_direction",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_opposite_direction",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_instrument",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_instrument",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_holding_horizon",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_holding_horizon",
          "should_cluster": false,
          "similarity": 0.571429
        },
        {
          "control_type": "different_tail_behavior",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_tail_behavior",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "low_trade_overlap",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_low_trade_overlap",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "different_regime",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_regime",
          "should_cluster": false,
          "similarity": 0.714286
        },
        {
          "control_type": "different_portfolio_role",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_00_different_portfolio_role",
          "should_cluster": false,
          "similarity": 0.431487
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_00_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_01_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_01_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_01_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_01_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_01_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_02_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_02_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_02_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_02_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_02_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_03_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_03_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_03_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_03_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_00",
          "right": "overnight_inventory_rth_resolution_03_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_00_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_00_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_00_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_01_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_01_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_01_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_01_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_01_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_02_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_02_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_02_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_02_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_02_04",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_03_00",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_03_01",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_03_02",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_03_03",
          "should_cluster": false,
          "similarity": 0.285714
        },
        {
          "control_type": "negative_behavioral_difference",
          "did_cluster": false,
          "left": "overnight_inventory_rth_resolution_00_01",
          "right": "overnight_inventory_rth_resolution_03_04",
          "should_cluster": false,
          "similarity": 0.285714
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
    "clusters": [
      {
        "cluster_id": "behavior_cluster_0003",
        "level": "LEVEL_1_EXECUTION_EQUIVALENT",
        "member_count": 11,
        "members": [
          "overnight_inventory_rth_resolution_00_03",
          "overnight_inventory_rth_resolution_01_03",
          "overnight_inventory_rth_resolution_01_04",
          "overnight_inventory_rth_resolution_02_02",
          "overnight_inventory_rth_resolution_02_03",
          "overnight_inventory_rth_resolution_03_00",
          "overnight_inventory_rth_resolution_03_01",
          "overnight_inventory_rth_resolution_03_04",
          "overnight_inventory_rth_resolution_05_00",
          "overnight_inventory_rth_resolution_05_03",
          "overnight_inventory_rth_resolution_05_04"
        ],
        "portfolio_role": "micro_risk_control",
        "representative": "overnight_inventory_rth_resolution_00_03"
      },
      {
        "cluster_id": "behavior_cluster_0002",
        "level": "LEVEL_1_EXECUTION_EQUIVALENT",
        "member_count": 9,
        "members": [
          "overnight_inventory_rth_resolution_00_02",
          "overnight_inventory_rth_resolution_01_02",
          "overnight_inventory_rth_resolution_02_01",
          "overnight_inventory_rth_resolution_02_04",
          "overnight_inventory_rth_resolution_03_03",
          "overnight_inventory_rth_resolution_04_00",
          "overnight_inventory_rth_resolution_04_02",
          "overnight_inventory_rth_resolution_04_04",
          "overnight_inventory_rth_resolution_05_02"
        ],
        "portfolio_role": "nasdaq_momentum_or_relative_value",
        "representative": "overnight_inventory_rth_resolution_00_02"
      },
      {
        "cluster_id": "behavior_cluster_0004",
        "level": "LEVEL_1_EXECUTION_EQUIVALENT",
        "member_count": 6,
        "members": [
          "overnight_inventory_rth_resolution_00_04",
          "overnight_inventory_rth_resolution_01_00",
          "overnight_inventory_rth_resolution_01_01",
          "overnight_inventory_rth_resolution_02_00",
          "overnight_inventory_rth_resolution_03_02",
          "overnight_inventory_rth_resolution_05_01"
        ],
        "portfolio_role": "sp500_index_exposure",
        "representative": "overnight_inventory_rth_resolution_00_04"
      },
      {
        "cluster_id": "behavior_cluster_0001",
        "level": "LEVEL_1_EXECUTION_EQUIVALENT",
        "member_count": 4,
        "members": [
          "overnight_inventory_rth_resolution_00_00",
          "overnight_inventory_rth_resolution_00_01",
          "overnight_inventory_rth_resolution_04_01",
          "overnight_inventory_rth_resolution_04_03"
        ],
        "portfolio_role": "micro_risk_control",
        "representative": "overnight_inventory_rth_resolution_00_00"
      },
      {
        "cluster_id": "behavior_cluster_0005",
        "level": "LEVEL_1_EXECUTION_EQUIVALENT",
        "member_count": 3,
        "members": [
          "intraday_range_migration_path_asymmetry_00_03",
          "intraday_range_migration_path_asymmetry_01_00",
          "intraday_range_migration_path_asymmetry_02_02"
        ],
        "portfolio_role": "sp500_index_exposure",
        "representative": "intraday_range_migration_path_asymmetry_00_03"
      }
    ],
    "valid_economic_units": 5
  },
  "behavioral_evidence": {
    "sketch_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T070236+0000_bounded_market_representation_pilot_v1/prototype_behavioral_sketches.jsonl.gz",
    "trade_ledger_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T070236+0000_bounded_market_representation_pilot_v1/prototype_trade_ledgers.jsonl.gz"
  },
  "created_at_utc": "2026-07-10T07:06:47+00:00",
  "data_access_records": [
    {
      "candidate_ids": [],
      "code_commit": "792797245229361c752f342cfca4aaeb119b5eec",
      "data_role": "DEVELOPMENT",
      "freeze_manifest_hash": null,
      "parameters_mutable": true,
      "period_accessed": "2024-01-01:2024-03-29",
      "process_id": 68324,
      "reason_for_access": "market representation development/falsification; Q4 not accessed",
      "requesting_module": "scripts/run_market_representation_lab.py",
      "timestamp_utc": "2026-07-10T07:02:36.146298+00:00"
    },
    {
      "candidate_ids": [],
      "code_commit": "792797245229361c752f342cfca4aaeb119b5eec",
      "data_role": "DIAGNOSTIC_CONFIRMATION_CONSUMED",
      "freeze_manifest_hash": null,
      "parameters_mutable": true,
      "period_accessed": "2024-04-01:2024-07-01",
      "process_id": 68324,
      "reason_for_access": "market representation development/falsification; Q4 not accessed",
      "requesting_module": "scripts/run_market_representation_lab.py",
      "timestamp_utc": "2026-07-10T07:02:36.325079+00:00"
    },
    {
      "candidate_ids": [],
      "code_commit": "792797245229361c752f342cfca4aaeb119b5eec",
      "data_role": "CONTAMINATED_DEVELOPMENT",
      "freeze_manifest_hash": null,
      "parameters_mutable": true,
      "period_accessed": "2024-07-01:2024-10-01",
      "process_id": 68324,
      "reason_for_access": "market representation development/falsification; Q4 not accessed",
      "requesting_module": "scripts/run_market_representation_lab.py",
      "timestamp_utc": "2026-07-10T07:02:36.451182+00:00"
    }
  ],
  "data_roles": {
    "q1": "DEVELOPMENT",
    "q2": "DIAGNOSTIC_CONFIRMATION_CONSUMED",
    "q3": "CONTAMINATED_DEVELOPMENT",
    "q4": "SEALED_BLIND_HOLDOUT"
  },
  "databento_spend_this_phase_usd": 0.0,
  "dataset": "GLBX.MDP3",
  "directional_beta_audit": {
    "audits": 135,
    "directional_dominance_count": 0,
    "finding": "No paired candidate may be called relative value if directional dominance is present."
  },
  "economically_viable_prototypes": 33,
  "falsified_representations": [
    "roll_aware_beta_neutral_nq_es_residual_divergence",
    "opening_auction_displacement_failed_continuation",
    "volatility_shape_transition",
    "overnight_inventory_rth_resolution",
    "intraday_range_migration_path_asymmetry"
  ],
  "family_dispositions": {
    "intraday_range_migration_path_asymmetry": {
      "costs_erase_effect": false,
      "disposition": "FALSIFIED",
      "null_beats_effect": true,
      "periods_with_signal": 3,
      "positive_after_costs": true,
      "representation": "intraday_range_migration_path_asymmetry",
      "roll_artifact": false,
      "stable_direction": false,
      "trade_count": 3600
    },
    "opening_auction_displacement_failed_continuation": {
      "costs_erase_effect": false,
      "disposition": "FALSIFIED",
      "null_beats_effect": true,
      "periods_with_signal": 3,
      "positive_after_costs": false,
      "representation": "opening_auction_displacement_failed_continuation",
      "roll_artifact": false,
      "stable_direction": true,
      "trade_count": 5820
    },
    "overnight_inventory_rth_resolution": {
      "costs_erase_effect": false,
      "disposition": "FALSIFIED",
      "null_beats_effect": true,
      "periods_with_signal": 3,
      "positive_after_costs": true,
      "representation": "overnight_inventory_rth_resolution",
      "roll_artifact": false,
      "stable_direction": false,
      "trade_count": 7200
    },
    "roll_aware_beta_neutral_nq_es_residual_divergence": {
      "costs_erase_effect": false,
      "disposition": "FALSIFIED",
      "null_beats_effect": true,
      "periods_with_signal": 3,
      "positive_after_costs": false,
      "representation": "roll_aware_beta_neutral_nq_es_residual_divergence",
      "roll_artifact": false,
      "stable_direction": true,
      "trade_count": 8100
    },
    "volatility_shape_transition": {
      "costs_erase_effect": true,
      "disposition": "FALSIFIED",
      "null_beats_effect": true,
      "periods_with_signal": 3,
      "positive_after_costs": false,
      "representation": "volatility_shape_transition",
      "roll_artifact": false,
      "stable_direction": false,
      "trade_count": 7200
    }
  },
  "feature_level_evidence": {
    "intraday_range_migration_path_asymmetry": [
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -7.66813140997577e-06,
        "feature_forward_correlation": -0.011047948912386838,
        "magnitude_monotonicity": -1.0577992549262056e-05,
        "null_effect_abs": 0.0005297550710473585,
        "period": "q1",
        "rows": 342970,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 7.203419853747491e-06,
        "feature_forward_correlation": 0.002125068145230053,
        "magnitude_monotonicity": -2.813925248408095e-06,
        "null_effect_abs": 0.0005533370568409898,
        "period": "q2",
        "rows": 353559,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 3.6410044996522986e-05,
        "feature_forward_correlation": 0.018201037488586067,
        "magnitude_monotonicity": 8.621308823536092e-05,
        "null_effect_abs": 0.0007608865609076258,
        "period": "q3",
        "rows": 358835,
        "status": "OK"
      }
    ],
    "opening_auction_displacement_failed_continuation": [
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 9.198380026993744e-05,
        "feature_forward_correlation": 0.08984941304463234,
        "magnitude_monotonicity": 0.0002294244744788768,
        "null_effect_abs": 9.93278809128937e-05,
        "period": "q1",
        "rows": 342970,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 6.572040345512948e-05,
        "feature_forward_correlation": 0.08428792873672072,
        "magnitude_monotonicity": 0.00030921687153537367,
        "null_effect_abs": 6.71664802671603e-05,
        "period": "q2",
        "rows": 353559,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 0.00013871445378525431,
        "feature_forward_correlation": 0.09683168280357074,
        "magnitude_monotonicity": 0.00046763534934639956,
        "null_effect_abs": 0.00014440216434840518,
        "period": "q3",
        "rows": 358835,
        "status": "OK"
      }
    ],
    "overnight_inventory_rth_resolution": [
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -4.995385519332643e-06,
        "feature_forward_correlation": -0.006965187680537481,
        "magnitude_monotonicity": -4.515854740982056e-05,
        "null_effect_abs": 0.00022336419590894232,
        "period": "q1",
        "rows": 342970,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 2.8097257444151252e-05,
        "feature_forward_correlation": -0.005667729105919318,
        "magnitude_monotonicity": 5.3708535230605565e-05,
        "null_effect_abs": 0.00026619652838076846,
        "period": "q2",
        "rows": 353559,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -4.8387207600048085e-06,
        "feature_forward_correlation": -0.014552694845178384,
        "magnitude_monotonicity": -5.305138055515586e-05,
        "null_effect_abs": 0.00030992219352296975,
        "period": "q3",
        "rows": 358835,
        "status": "OK"
      }
    ],
    "roll_aware_beta_neutral_nq_es_residual_divergence": [
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -1.1584708173100566e-05,
        "feature_forward_correlation": -0.02022296004902447,
        "magnitude_monotonicity": -3.0180510214243138e-05,
        "null_effect_abs": 1.884658277805591e-05,
        "period": "q1",
        "rows": 85317,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -1.0928246015090765e-05,
        "feature_forward_correlation": -0.025510439575008467,
        "magnitude_monotonicity": -2.9194612387526833e-05,
        "null_effect_abs": 1.641044838913875e-05,
        "period": "q2",
        "rows": 87900,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -1.3240104769358281e-05,
        "feature_forward_correlation": -0.02574622922494229,
        "magnitude_monotonicity": -4.973612518535813e-05,
        "null_effect_abs": 1.8310650263383134e-05,
        "period": "q3",
        "rows": 89329,
        "status": "OK"
      }
    ],
    "volatility_shape_transition": [
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 2.1085255056480326e-05,
        "feature_forward_correlation": 0.012278772288028598,
        "magnitude_monotonicity": 3.6753053285379823e-05,
        "null_effect_abs": 0.00012105560177808793,
        "period": "q1",
        "rows": 342930,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "negative",
        "effect_size": -2.4610831183866913e-06,
        "feature_forward_correlation": -0.0015708591310240705,
        "magnitude_monotonicity": 8.03982432188469e-06,
        "null_effect_abs": 0.0001330022032292242,
        "period": "q2",
        "rows": 353519,
        "status": "OK"
      },
      {
        "beats_null": false,
        "direction": "positive",
        "effect_size": 2.417014366945763e-05,
        "feature_forward_correlation": 0.012978291701323419,
        "magnitude_monotonicity": 8.896062213770095e-05,
        "null_effect_abs": 0.0002103018689928924,
        "period": "q3",
        "rows": 358795,
        "status": "OK"
      }
    ]
  },
  "future_q4_freeze_candidates": [],
  "future_tick_tbbo_candidates": [
    "roll_aware_beta_neutral_nq_es_residual_divergence_00_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_00_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_00_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_00_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_00_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_01_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_01_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_01_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_01_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_01_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_02_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_02_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_02_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_02_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_02_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_03_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_03_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_03_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_03_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_03_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_04_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_04_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_04_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_04_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_04_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_05_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_05_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_05_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_05_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_05_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_06_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_06_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_06_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_06_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_06_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_07_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_07_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_07_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_07_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_07_04",
    "roll_aware_beta_neutral_nq_es_residual_divergence_08_00",
    "roll_aware_beta_neutral_nq_es_residual_divergence_08_01",
    "roll_aware_beta_neutral_nq_es_residual_divergence_08_02",
    "roll_aware_beta_neutral_nq_es_residual_divergence_08_03",
    "roll_aware_beta_neutral_nq_es_residual_divergence_08_04"
  ],
  "insufficient_evidence_representations": [],
  "legging_risk_findings": {
    "mode_used_for_conclusions": "ATOMIC_CONSERVATIVE",
    "sequential_stress_status": "implemented_and_unit_tested_not_used_for_final_viability"
  },
  "new_databento_requests": [],
  "null_model_summary": {
    "intraday_range_migration_path_asymmetry": {
      "beats_null_count": 0,
      "effective_trial_count": 3,
      "periods": 3,
      "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only."
    },
    "opening_auction_displacement_failed_continuation": {
      "beats_null_count": 0,
      "effective_trial_count": 3,
      "periods": 3,
      "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only."
    },
    "overnight_inventory_rth_resolution": {
      "beats_null_count": 0,
      "effective_trial_count": 3,
      "periods": 3,
      "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only."
    },
    "roll_aware_beta_neutral_nq_es_residual_divergence": {
      "beats_null_count": 0,
      "effective_trial_count": 3,
      "periods": 3,
      "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only."
    },
    "volatility_shape_transition": {
      "beats_null_count": 0,
      "effective_trial_count": 3,
      "periods": 3,
      "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only."
    }
  },
  "parameter_variants": 120,
  "prototype_results_top": [
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_00_04",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_00",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_04"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_01_00",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_01",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_00"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_01_01",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_01",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_01"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_02_00",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_02",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_00"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_03_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_03",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 16862.5,
      "max_drawdown": -2879.5,
      "net_pnl": 14702.5,
      "period_net_pnl": {
        "q1": 767.5,
        "q2": 6105.0,
        "q3": 7830.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_05_01",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_05",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_01"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_00_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_00",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_01_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_01",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_02_01",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_02",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_01"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_02_04",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_02",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_04"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_03_03",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_03",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_03"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_04_00",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_04",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_00"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_04_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_04",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_04_04",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_04",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_04"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 15980.0,
      "max_drawdown": -10739.0,
      "net_pnl": 13820.0,
      "period_net_pnl": {
        "q1": 6990.0,
        "q2": 2350.0,
        "q3": 4480.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_05_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_05",
      "symbol": "NQ",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "intraday_range_migration_path_asymmetry",
      "gross_pnl": 4375.0,
      "max_drawdown": -1293.5,
      "net_pnl": 2215.0,
      "period_net_pnl": {
        "q1": 730.0,
        "q2": 1080.0,
        "q3": 405.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "intraday_range_migration_path_asymmetry_00_03",
      "requires_future_tick_tbbo": false,
      "structural_id": "intraday_range_migration_path_asymmetry_struct_00",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_03"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "intraday_range_migration_path_asymmetry",
      "gross_pnl": 4375.0,
      "max_drawdown": -1293.5,
      "net_pnl": 2215.0,
      "period_net_pnl": {
        "q1": 730.0,
        "q2": 1080.0,
        "q3": 405.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "intraday_range_migration_path_asymmetry_01_00",
      "requires_future_tick_tbbo": false,
      "structural_id": "intraday_range_migration_path_asymmetry_struct_01",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_00"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "intraday_range_migration_path_asymmetry",
      "gross_pnl": 4375.0,
      "max_drawdown": -1293.5,
      "net_pnl": 2215.0,
      "period_net_pnl": {
        "q1": 730.0,
        "q2": 1080.0,
        "q3": 405.0
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "intraday_range_migration_path_asymmetry_02_02",
      "requires_future_tick_tbbo": false,
      "structural_id": "intraday_range_migration_path_asymmetry_struct_02",
      "symbol": "ES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_02"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 1680.0,
      "max_drawdown": -525.75,
      "net_pnl": 600.0,
      "period_net_pnl": {
        "q1": -250.0,
        "q2": 452.5,
        "q3": 397.5
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_00_00",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_00",
      "symbol": "MES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_00"
    },
    {
      "economically_viable": true,
      "eligible_for_future_q4_freeze": false,
      "family": "overnight_inventory_rth_resolution",
      "gross_pnl": 1680.0,
      "max_drawdown": -525.75,
      "net_pnl": 600.0,
      "period_net_pnl": {
        "q1": -250.0,
        "q2": 452.5,
        "q3": 397.5
      },
      "period_trade_count": {
        "q1": 80,
        "q2": 80,
        "q3": 80
      },
      "prototype_id": "overnight_inventory_rth_resolution_00_01",
      "requires_future_tick_tbbo": false,
      "structural_id": "overnight_inventory_rth_resolution_struct_00",
      "symbol": "MES",
      "topstep_compatible": false,
      "trade_count": 240,
      "variant_id": "variant_01"
    }
  ],
  "prototypes_by_family": {
    "intraday_range_migration_path_asymmetry": 15,
    "opening_auction_displacement_failed_continuation": 30,
    "overnight_inventory_rth_resolution": 30,
    "roll_aware_beta_neutral_nq_es_residual_divergence": 45,
    "volatility_shape_transition": 30
  },
  "prototypes_generated": 150,
  "q4_seal_verification": "PASSED_NO_Q4_ACCESS_BY_LAB",
  "rejected_representations": [
    {
      "name": "dynamic_hedge_ratio_relative_value",
      "reason": "Folded into the core paired lane as hedge-ratio methods, not a separate compute lane."
    },
    {
      "name": "cross_market_lead_lag_conditioned_on_vol_regime",
      "reason": "Deferred until paired residuals survive; high spurious-correlation risk."
    },
    {
      "name": "session_transition_state_models",
      "reason": "Deferred due calendar/session complexity relative to pilot budget."
    },
    {
      "name": "failed_directional_expansion_controlled_tail",
      "reason": "Overlaps with opening and volatility-shape lanes; keep as future ablation."
    },
    {
      "name": "mes_mnq_micro_first_portfolio_roles",
      "reason": "Risk/portfolio sizing role, not a standalone representation until underlying edges survive."
    }
  ],
  "roll_map_hash": "eddda493c5fdaaa91dfabbfcd6b1e07eab9a4e0d26961a58468e59fa69eaed98",
  "roll_map_path": "data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_eddda493c5fdaaa9.json",
  "roll_map_period": {
    "continuous_symbols": [
      "ES.c.0",
      "MES.c.0",
      "NQ.c.0",
      "MNQ.c.0"
    ],
    "definition_record_count": 16,
    "period_end": "2024-10-01",
    "period_start": "2024-01-01"
  },
  "schema": "ohlcv-1m",
  "selected_representations": [
    {
      "economic_hypothesis": "Temporary residual dislocation between synchronized NQ and ES contracts may normalize after beta-neutral shocks.",
      "expected_failure_regime": "Directional macro trend, beta instability, or roll transition.",
      "expected_regime": "Stable covariance and moderate volatility.",
      "mechanism": "Past-only hedge-ratio residual, two-leg execution, roll exclusion.",
      "name": "roll_aware_beta_neutral_nq_es_residual_divergence",
      "rank": 1,
      "reason": "Mandatory core lane and cleanly falsifiable after old directional proxy was killed.",
      "roll_sensitivity": "high",
      "selected": true,
      "topstep_role": "Relative-value diversifier if true beta neutrality survives."
    },
    {
      "economic_hypothesis": "Opening auction effort that fails to maintain direction may expose trapped early momentum.",
      "expected_failure_regime": "Persistent trend day.",
      "expected_regime": "First 60-120 RTH minutes after abnormal opening displacement.",
      "mechanism": "Opening range geometry, effort/progress, failed continuation.",
      "name": "opening_auction_displacement_failed_continuation",
      "rank": 2,
      "reason": "Low parameter count and not a renamed indicator.",
      "roll_sensitivity": "low",
      "selected": true,
      "topstep_role": "Short-horizon MLL-contained sleeve."
    },
    {
      "economic_hypothesis": "Realized-volatility shape can distinguish actionable expansion from chop better than simple ATR expansion.",
      "expected_failure_regime": "Headline volatility and whipsaw expansion.",
      "expected_regime": "Compression resolving into directional liquidity.",
      "mechanism": "Short/medium/long vol curvature plus path asymmetry.",
      "name": "volatility_shape_transition",
      "rank": 3,
      "reason": "Falsifies a structural state, not a stop/target grid.",
      "roll_sensitivity": "low",
      "selected": true,
      "topstep_role": "Target-velocity research lane with explicit cost hurdle."
    },
    {
      "economic_hypothesis": "Overnight inventory imbalance may resolve during the regular-session open through acceptance or rejection.",
      "expected_failure_regime": "Quiet overnight sessions or news shock opens.",
      "expected_regime": "Large overnight move with early cash-session confirmation.",
      "mechanism": "Overnight displacement and early RTH response.",
      "name": "overnight_inventory_rth_resolution",
      "rank": 4,
      "reason": "Distinct mechanism from paired residuals and volatility shape.",
      "roll_sensitivity": "medium",
      "selected": true,
      "topstep_role": "Opening-session climber if costs and concentration are controlled."
    },
    {
      "economic_hypothesis": "The migration of accepted price zones through the session may encode continuation/exhaustion states.",
      "expected_failure_regime": "Featureless rotation.",
      "expected_regime": "Structured trend or failed-trend days.",
      "mechanism": "Time near rolling extremes and range-location imbalance.",
      "name": "intraday_range_migration_path_asymmetry",
      "rank": 5,
      "reason": "Adds path geometry without broad indicator stacking.",
      "roll_sensitivity": "low",
      "selected": true,
      "topstep_role": "Portfolio diversifier / session role."
    }
  ],
  "structural_prototypes": 30,
  "surviving_representations": [],
  "symbols": [
    "ES",
    "MES",
    "NQ",
    "MNQ"
  ],
  "topstep_compatible_prototypes": 0,
  "transfer_results": {
    "q1": {
      "positive_net_count": 45,
      "trade_count": 10680
    },
    "q2": {
      "positive_net_count": 81,
      "trade_count": 10140
    },
    "q3": {
      "positive_net_count": 70,
      "trade_count": 11100
    }
  },
  "true_paired_candidates": 45,
  "two_leg_cost_findings": {
    "mean_two_leg_cost": 806.2583,
    "mode": "ATOMIC_CONSERVATIVE",
    "trade_count": 8100
  },
  "warning": "Historical research only. This is not live trading approval. Q1-Q3 are development/falsification data."
}
```
