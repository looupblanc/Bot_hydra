# explicit_roll_diagnosis_20260710T063040+0000_explicit_contract_roll_diagnosis_v3.md

Historical research only. No live trading approval.

```json
{
  "behavioral_evidence_manifest": {
    "created_at": "2026-07-10T06:30:39+00:00",
    "ledger_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T063039+0000_explicit_contract_roll_diagnosis_v3/trade_ledgers.jsonl.gz",
    "ledger_row_count": 4852,
    "ledger_sha256": "11204b479afeaf5db13a9c7c1cf8447f437c51e902e3a0524f8629c0415880d8",
    "sketch_count": 79,
    "sketch_path": "/root/hydra-bot/data/cache/behavioral_evidence/20260710T063039+0000_explicit_contract_roll_diagnosis_v3/behavioral_sketches.jsonl.gz",
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
    "sketch_sha256": "a8acccd049c0c5f689aafa2944fb3e087d4b658a4fc91bb2146dfc8a7f4d063b",
    "storage_note": "Full behavioral evidence is under ignored data/cache and must not be committed.",
    "tag": "20260710T063039+0000_explicit_contract_roll_diagnosis_v3",
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
  },
  "budget_state": {
    "cumulative_actual_spend_usd": 3.892145,
    "cumulative_estimated_spend_usd": 3.892145,
    "remaining_hard_cap_budget_usd": 96.107855,
    "remaining_safety_budget_usd": 94.107855
  },
  "calibrated_economic_clusters": 77,
  "clustering_calibration": {
    "cluster_stability": 1.0,
    "control_pairs": 60,
    "false_merge_rate": 0.0,
    "false_split_rate": 0.0,
    "negative_controls": 54,
    "pair_decisions": [
      {
        "control_type": "exact_duplicate",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_exact_duplicate",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "metadata_only_change",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_metadata_only_change",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "tiny_stop_neighbor",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_tiny_stop_neighbor",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "tiny_target_neighbor",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_tiny_target_neighbor",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "same_signal_neighboring_parameter",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_same_signal_neighboring_parameter",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "parent_child_high_trade_overlap",
        "did_cluster": true,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_parent_child_high_trade_overlap",
        "should_cluster": true,
        "similarity": 1.0
      },
      {
        "control_type": "different_session",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_session",
        "should_cluster": false,
        "similarity": 0.571429
      },
      {
        "control_type": "opposite_direction",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_opposite_direction",
        "should_cluster": false,
        "similarity": 0.571429
      },
      {
        "control_type": "different_instrument",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_instrument",
        "should_cluster": false,
        "similarity": 0.571429
      },
      {
        "control_type": "different_holding_horizon",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_holding_horizon",
        "should_cluster": false,
        "similarity": 0.571429
      },
      {
        "control_type": "different_tail_behavior",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_tail_behavior",
        "should_cluster": false,
        "similarity": 0.714286
      },
      {
        "control_type": "low_trade_overlap",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_low_trade_overlap",
        "should_cluster": false,
        "similarity": 0.714286
      },
      {
        "control_type": "different_regime",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_regime",
        "should_cluster": false,
        "similarity": 0.714286
      },
      {
        "control_type": "different_portfolio_role",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0318d5c0c44a_different_portfolio_role",
        "should_cluster": false,
        "similarity": 0.428571
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0c1d1d1cff52",
        "should_cluster": false,
        "similarity": 0.394063
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0d6051ec48c4",
        "should_cluster": false,
        "similarity": 0.413285
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0f4b0132bc01",
        "should_cluster": false,
        "similarity": 0.394414
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_0fbf559ca207",
        "should_cluster": false,
        "similarity": 0.408923
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_114a17fb1c8c",
        "should_cluster": false,
        "similarity": 0.393582
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_12bc4167b90b",
        "should_cluster": false,
        "similarity": 0.248414
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_1377b7527d14",
        "should_cluster": false,
        "similarity": 0.238697
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_1b6c9599067d",
        "should_cluster": false,
        "similarity": 0.250814
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_1f8a0039139f",
        "should_cluster": false,
        "similarity": 0.269691
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_2180b2720e06",
        "should_cluster": false,
        "similarity": 0.247916
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_3084165e6753",
        "should_cluster": false,
        "similarity": 0.238006
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_31c9c690606b",
        "should_cluster": false,
        "similarity": 0.564732
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_36c013ab62d2",
        "should_cluster": false,
        "similarity": 0.401285
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_387acb0c27a6",
        "should_cluster": false,
        "similarity": 0.264192
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_39461ae7c4bc",
        "should_cluster": false,
        "similarity": 0.249604
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_3d2ce569eaa8",
        "should_cluster": false,
        "similarity": 0.389392
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_3f0579d5a8d8",
        "should_cluster": false,
        "similarity": 0.553709
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_41bc75888c12",
        "should_cluster": false,
        "similarity": 0.247916
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0318d5c0c44a",
        "right": "rem_41c809447e23",
        "should_cluster": false,
        "similarity": 0.242371
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_0d6051ec48c4",
        "should_cluster": false,
        "similarity": 0.396125
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_0f4b0132bc01",
        "should_cluster": false,
        "similarity": 0.373322
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_0fbf559ca207",
        "should_cluster": false,
        "similarity": 0.412911
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_114a17fb1c8c",
        "should_cluster": false,
        "similarity": 0.366152
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_12bc4167b90b",
        "should_cluster": false,
        "similarity": 0.262654
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_1377b7527d14",
        "should_cluster": false,
        "similarity": 0.246263
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_1b6c9599067d",
        "should_cluster": false,
        "similarity": 0.265179
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_1f8a0039139f",
        "should_cluster": false,
        "similarity": 0.26149
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_2180b2720e06",
        "should_cluster": false,
        "similarity": 0.256941
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_3084165e6753",
        "should_cluster": false,
        "similarity": 0.245516
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_31c9c690606b",
        "should_cluster": false,
        "similarity": 0.400163
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_36c013ab62d2",
        "should_cluster": false,
        "similarity": 0.380643
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_387acb0c27a6",
        "should_cluster": false,
        "similarity": 0.260504
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_39461ae7c4bc",
        "should_cluster": false,
        "similarity": 0.263906
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_3d2ce569eaa8",
        "should_cluster": false,
        "similarity": 0.393768
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_3f0579d5a8d8",
        "should_cluster": false,
        "similarity": 0.410714
      },
      {
        "control_type": "negative_behavioral_difference",
        "did_cluster": false,
        "left": "rem_0c1d1d1cff52",
        "right": "rem_41bc75888c12",
        "should_cluster": false,
        "similarity": 0.25572
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
  "contract_map_path": "data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_ecee1e2e1a91511b.json",
  "created_at": "2026-07-10T06:30:40+00:00",
  "data_role_correction": {
    "future_2025": "POTENTIAL_FINAL_LOCKBOX",
    "q1": "DEVELOPMENT",
    "q2": "DIAGNOSTIC_CONFIRMATION_CONSUMED",
    "q3": "CONTAMINATED_DEVELOPMENT",
    "q4": "SEALED_BLIND_HOLDOUT"
  },
  "eligible_future_tick_tbbo": [
    "rem_0318d5c0c44a",
    "rem_0c1d1d1cff52",
    "rem_0f4b0132bc01",
    "rem_0fbf559ca207",
    "rem_12bc4167b90b",
    "rem_1377b7527d14",
    "rem_1b6c9599067d",
    "rem_1f8a0039139f",
    "rem_2180b2720e06",
    "rem_3084165e6753",
    "rem_31c9c690606b",
    "rem_36c013ab62d2",
    "rem_387acb0c27a6",
    "rem_39461ae7c4bc",
    "rem_3d2ce569eaa8",
    "rem_3f0579d5a8d8",
    "rem_41bc75888c12",
    "rem_41c809447e23",
    "rem_47e50dc855cb",
    "rem_485400b5bafb",
    "rem_4878c1d7ab7c",
    "rem_48ae528c05e0",
    "rem_4957b68da62e",
    "rem_4c396414d726",
    "rem_507619173f35",
    "rem_58078cd9129c",
    "rem_5a2b3bf704f9",
    "rem_5b133897fcd7",
    "rem_60bfd595ae6b",
    "rem_652a9dd7bb99",
    "rem_676f8f367a6d",
    "rem_689faa5a4a44",
    "rem_736c872da6b3",
    "rem_7925b343db64",
    "rem_7a6980e8025e",
    "rem_7e2847848ce5",
    "rem_82392d7b0080",
    "rem_8da33e246e09",
    "rem_9af4b5386cc5",
    "rem_a616afc9aa2d",
    "rem_ad5dc854fc28",
    "rem_ad6fc048113e",
    "rem_b2f44e7dd9d4",
    "rem_b35d2049c890",
    "rem_b4043f2fd47b",
    "rem_b5f396eb1596",
    "rem_b93b10a2af8e",
    "rem_b9d84ed4f334",
    "rem_ba645bcc7096",
    "rem_bc2634aeee51",
    "rem_c125d8dba696",
    "rem_c1fb3005131c",
    "rem_c3eaffacc926",
    "rem_c6b31a6b9ed1",
    "rem_cfb7ef7331fe",
    "rem_d074c5aca1c0",
    "rem_d0fea828361d",
    "rem_d74f31102f30",
    "rem_d7915f9dcef7",
    "rem_dbfbb652e74c",
    "rem_e5ecf52d7e70",
    "rem_ee6491b3140b",
    "rem_eebdacff42b1",
    "rem_f1eeac2e57e0",
    "rem_f20e2ab55e61",
    "rem_f23c42603dd3",
    "rem_f8b24ef8f57a",
    "rem_f9d76f78bb63",
    "rem_fb44b8e6f616"
  ],
  "exact_next_milestone": "Retire the current NQ/ES parameter-neighbor lineage, then implement one or two roll-aware beta-neutral representations with explicit ES/NQ paired inputs before any new search.",
  "explicit_contract_map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
  "hidden_directional_beta": {
    "detected": true,
    "diagnosis": "This is hidden single-market directional beta, not a tradable ES/NQ relative-value spread.",
    "es_input_used": false,
    "mechanism": "The current topstep_nq_es_divergence_controlled implementation uses only the candidate symbol dataframe: momentum_20 minus session_return, then direction from that same symbol momentum."
  },
  "lineage_disposition": {
    "disposition": "KILL_EXISTING_LINEAGE",
    "family": "topstep_nq_es_divergence_controlled",
    "freeze_or_kill_parameter_neighbors": true,
    "q1_pass_count": 0,
    "q2_diagnostic_transfer_count": 0,
    "reason": "no representative passed explicit-contract Q1 recomputation",
    "retain_representatives": []
  },
  "manifest_audit": {
    "candidate_count": 79,
    "computed_hash": "c256dadbd42bd9a62d30b5c8e70374b1b5c17a98c8b7b30c3775d40296a78392",
    "current_commit": "966a8d307a5561a7967b93dbe28116b53152bec3",
    "manifest_hash": "c256dadbd42bd9a62d30b5c8e70374b1b5c17a98c8b7b30c3775d40296a78392",
    "manifest_hash_valid": true,
    "missing_candidate_ids": [],
    "source_commit": "955dbfb4a57e1f329f7139729ec4e7a5a67c446c",
    "source_commit_matches_current": false,
    "source_commit_note": "Current source differs because provenance/forensic infrastructure was added after freeze; candidate parameters/specifications were not altered.",
    "spec_mismatch_count": 0,
    "spec_mismatches": []
  },
  "materially_affected_candidates": [
    "rem_0318d5c0c44a",
    "rem_0c1d1d1cff52",
    "rem_0f4b0132bc01",
    "rem_0fbf559ca207",
    "rem_12bc4167b90b",
    "rem_1377b7527d14",
    "rem_1b6c9599067d",
    "rem_1f8a0039139f",
    "rem_2180b2720e06",
    "rem_3084165e6753",
    "rem_31c9c690606b",
    "rem_36c013ab62d2",
    "rem_387acb0c27a6",
    "rem_39461ae7c4bc",
    "rem_3d2ce569eaa8",
    "rem_3f0579d5a8d8",
    "rem_41bc75888c12",
    "rem_41c809447e23",
    "rem_47e50dc855cb",
    "rem_485400b5bafb",
    "rem_4878c1d7ab7c",
    "rem_48ae528c05e0",
    "rem_4957b68da62e",
    "rem_4c396414d726",
    "rem_507619173f35",
    "rem_58078cd9129c",
    "rem_5a2b3bf704f9",
    "rem_5b133897fcd7",
    "rem_60bfd595ae6b",
    "rem_652a9dd7bb99",
    "rem_676f8f367a6d",
    "rem_689faa5a4a44",
    "rem_736c872da6b3",
    "rem_7925b343db64",
    "rem_7a6980e8025e",
    "rem_7e2847848ce5",
    "rem_82392d7b0080",
    "rem_8da33e246e09",
    "rem_9af4b5386cc5",
    "rem_a616afc9aa2d",
    "rem_ad5dc854fc28",
    "rem_ad6fc048113e",
    "rem_b2f44e7dd9d4",
    "rem_b35d2049c890",
    "rem_b4043f2fd47b",
    "rem_b5f396eb1596",
    "rem_b93b10a2af8e",
    "rem_b9d84ed4f334",
    "rem_ba645bcc7096",
    "rem_bc2634aeee51",
    "rem_c125d8dba696",
    "rem_c1fb3005131c",
    "rem_c3eaffacc926",
    "rem_c6b31a6b9ed1",
    "rem_cfb7ef7331fe",
    "rem_d074c5aca1c0",
    "rem_d0fea828361d",
    "rem_d74f31102f30",
    "rem_d7915f9dcef7",
    "rem_dbfbb652e74c",
    "rem_e5ecf52d7e70",
    "rem_ee6491b3140b",
    "rem_eebdacff42b1",
    "rem_f1eeac2e57e0",
    "rem_f20e2ab55e61",
    "rem_f23c42603dd3",
    "rem_f8b24ef8f57a",
    "rem_f9d76f78bb63",
    "rem_fb44b8e6f616"
  ],
  "new_representation_names": [
    "roll_aware_beta_neutral_nq_es_residual_divergence",
    "dynamic_hedge_ratio_relative_value",
    "overnight_inventory_rth_resolution",
    "opening_auction_displacement_failed_continuation",
    "volatility_shape_transition",
    "cross_market_lead_lag_conditioned_on_vol_regime",
    "intraday_range_migration_path_asymmetry",
    "session_transition_state_models",
    "failed_directional_expansion_controlled_tail",
    "mes_mnq_micro_first_portfolio_roles"
  ],
  "q1_invalidations": [
    "rem_0318d5c0c44a",
    "rem_0c1d1d1cff52",
    "rem_0f4b0132bc01",
    "rem_0fbf559ca207",
    "rem_12bc4167b90b",
    "rem_1377b7527d14",
    "rem_1b6c9599067d",
    "rem_1f8a0039139f",
    "rem_2180b2720e06",
    "rem_3084165e6753",
    "rem_31c9c690606b",
    "rem_36c013ab62d2",
    "rem_387acb0c27a6",
    "rem_39461ae7c4bc",
    "rem_3d2ce569eaa8",
    "rem_3f0579d5a8d8",
    "rem_41bc75888c12",
    "rem_41c809447e23",
    "rem_47e50dc855cb",
    "rem_485400b5bafb",
    "rem_4878c1d7ab7c",
    "rem_48ae528c05e0",
    "rem_4957b68da62e",
    "rem_4c396414d726",
    "rem_507619173f35",
    "rem_58078cd9129c",
    "rem_5a2b3bf704f9",
    "rem_5b133897fcd7",
    "rem_60bfd595ae6b",
    "rem_652a9dd7bb99",
    "rem_676f8f367a6d",
    "rem_689faa5a4a44",
    "rem_736c872da6b3",
    "rem_7925b343db64",
    "rem_7a6980e8025e",
    "rem_7e2847848ce5",
    "rem_82392d7b0080",
    "rem_8da33e246e09",
    "rem_9af4b5386cc5",
    "rem_a616afc9aa2d",
    "rem_ad5dc854fc28",
    "rem_ad6fc048113e",
    "rem_b2f44e7dd9d4",
    "rem_b35d2049c890",
    "rem_b4043f2fd47b",
    "rem_b5f396eb1596",
    "rem_b93b10a2af8e",
    "rem_b9d84ed4f334",
    "rem_ba645bcc7096",
    "rem_bc2634aeee51",
    "rem_c125d8dba696",
    "rem_c1fb3005131c",
    "rem_c3eaffacc926",
    "rem_c6b31a6b9ed1",
    "rem_cfb7ef7331fe",
    "rem_d074c5aca1c0",
    "rem_d0fea828361d",
    "rem_d74f31102f30",
    "rem_d7915f9dcef7",
    "rem_dbfbb652e74c",
    "rem_e5ecf52d7e70",
    "rem_ee6491b3140b",
    "rem_eebdacff42b1",
    "rem_f1eeac2e57e0",
    "rem_f20e2ab55e61",
    "rem_f23c42603dd3",
    "rem_f8b24ef8f57a",
    "rem_f9d76f78bb63",
    "rem_fb44b8e6f616"
  ],
  "q1_status_counts": {
    "EXPLICIT_Q1_FAIL": 10,
    "EXPLICIT_Q1_ROLL_INVALIDATED": 69
  },
  "q2_diagnostic_records": [],
  "q2_status_counts": {},
  "q3_quarantine_verification": {
    "evidence_path": "/root/hydra-bot/reports/data_access/lockbox_contamination_events.jsonl",
    "q3_quarantined": true
  },
  "q4_seal_verification": {
    "q4_access_entries_total": 0,
    "q4_loaded_or_inspected_this_phase": false,
    "q4_prior_raw_only_cache_exists": true,
    "status": "SEALED_RAW_ONLY_UNINSPECTED_BY_THIS_PHASE"
  },
  "registry_integrity": "ok",
  "roll_impact_counts": {
    "ROLL_MATERIAL_IMPACT": 69,
    "ROLL_MINOR_IMPACT": 10
  },
  "rule_proxy_comparison_q1": {
    "by_symbol": {
      "ES": {
        "disagreement_count": 4,
        "disagreement_rate": 0.008,
        "samples": [
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 01:22:00+00:00"
          },
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 05:21:00+00:00"
          },
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 08:49:00+00:00"
          },
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 11:46:00+00:00"
          }
        ],
        "timestamps_checked": 500
      },
      "MES": {
        "disagreement_count": 4,
        "disagreement_rate": 0.008,
        "samples": [
          {
            "new_contract": "MESH4",
            "old_contract": "MESM4",
            "symbol": "MES",
            "timestamp": "2024-03-15 00:24:00+00:00"
          },
          {
            "new_contract": "MESH4",
            "old_contract": "MESM4",
            "symbol": "MES",
            "timestamp": "2024-03-15 03:44:00+00:00"
          },
          {
            "new_contract": "MESH4",
            "old_contract": "MESM4",
            "symbol": "MES",
            "timestamp": "2024-03-15 07:36:00+00:00"
          },
          {
            "new_contract": "MESH4",
            "old_contract": "MESM4",
            "symbol": "MES",
            "timestamp": "2024-03-15 10:38:00+00:00"
          }
        ],
        "timestamps_checked": 500
      },
      "MNQ": {
        "disagreement_count": 4,
        "disagreement_rate": 0.008,
        "samples": [
          {
            "new_contract": "MNQH4",
            "old_contract": "MNQM4",
            "symbol": "MNQ",
            "timestamp": "2024-03-15 02:34:00+00:00"
          },
          {
            "new_contract": "MNQH4",
            "old_contract": "MNQM4",
            "symbol": "MNQ",
            "timestamp": "2024-03-15 05:26:00+00:00"
          },
          {
            "new_contract": "MNQH4",
            "old_contract": "MNQM4",
            "symbol": "MNQ",
            "timestamp": "2024-03-15 08:19:00+00:00"
          },
          {
            "new_contract": "MNQH4",
            "old_contract": "MNQM4",
            "symbol": "MNQ",
            "timestamp": "2024-03-15 11:11:00+00:00"
          }
        ],
        "timestamps_checked": 500
      },
      "NQ": {
        "disagreement_count": 3,
        "disagreement_rate": 0.006,
        "samples": [
          {
            "new_contract": "NQH4",
            "old_contract": "NQM4",
            "symbol": "NQ",
            "timestamp": "2024-03-15 01:27:00+00:00"
          },
          {
            "new_contract": "NQH4",
            "old_contract": "NQM4",
            "symbol": "NQ",
            "timestamp": "2024-03-15 06:37:00+00:00"
          },
          {
            "new_contract": "NQH4",
            "old_contract": "NQM4",
            "symbol": "NQ",
            "timestamp": "2024-03-15 10:14:00+00:00"
          }
        ],
        "timestamps_checked": 500
      }
    },
    "disagreement_count": 15,
    "disagreement_rate": 0.0075,
    "new_map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
    "old_map_type": "RULE_BASED_CME_EQUITY_INDEX_QUARTERLY_PROXY",
    "samples": [
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 01:22:00+00:00"
      },
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 05:21:00+00:00"
      },
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 08:49:00+00:00"
      },
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 11:46:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 00:24:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 03:44:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 07:36:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 10:38:00+00:00"
      },
      {
        "new_contract": "NQH4",
        "old_contract": "NQM4",
        "symbol": "NQ",
        "timestamp": "2024-03-15 01:27:00+00:00"
      },
      {
        "new_contract": "NQH4",
        "old_contract": "NQM4",
        "symbol": "NQ",
        "timestamp": "2024-03-15 06:37:00+00:00"
      },
      {
        "new_contract": "NQH4",
        "old_contract": "NQM4",
        "symbol": "NQ",
        "timestamp": "2024-03-15 10:14:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 02:34:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 05:26:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 08:19:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 11:11:00+00:00"
      }
    ],
    "timestamps_checked": 2000
  },
  "runtime_seconds": 98.39
}
```
