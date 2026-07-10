# Edge Atom Discovery Lab edge_atom_discovery_replication_v1_final_corrected

Historical research only. This is not live trading approval.

Q4 remained sealed; the lab used development/falsification data ending before 2024-10-01. This corrected report fixes the phase Q4-access counter boundary semantics; no atom result was recomputed or changed.

```json
{
  "account_utilities": [],
  "adversarial_passes": 0,
  "assembly_decisions": [
    {
      "accepted": false,
      "reason": "no_fully_validated_atoms_available_for_assembly",
      "strategy": null
    }
  ],
  "atom_families_represented": [
    "accepted_price_migration",
    "calendar_participation_structure",
    "contract_roll_invariant_relative_state",
    "cross_market_risk_transfer",
    "defensive_portfolio_atom",
    "distribution_tail_state",
    "effort_vs_progress",
    "session_inventory_acceptance",
    "session_transition_state",
    "volatility_path_shape"
  ],
  "atom_hypotheses_proposed": 120,
  "atoms_falsified": 104,
  "atoms_insufficient": 16,
  "atoms_preregistered": 120,
  "atoms_screened": 120,
  "baseline_commit_actual": "579ad08b621232ca62bc4a022757a6abb8a5de56",
  "baseline_commit_expected": "579ad08b621232ca62bc4a022757a6abb8a5de56",
  "behavioral_clusters": 0,
  "best_validated_mechanisms": [
    {
      "adjusted_evidence": 4.639409900099716,
      "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "mechanism": "Large signed path effort with weak realized progress can identify absorption or exhaustion visible in OHLCV path geometry.",
      "raw_effect": 7.49947130153992e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.512730286266663,
      "atom_id": "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "accepted_price_migration",
      "mechanism": "Movement or failure of accepted price regions can precede continuation or return to prior value.",
      "raw_effect": 0.0001089883007264411,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.3675205442509557,
      "atom_id": "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "defensive_portfolio_atom",
      "mechanism": "States predicting poor future path quality can be valuable as risk-off atoms even without direct alpha.",
      "raw_effect": -3.6033406421662145e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.287842137548147,
      "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_MES_60_moderate_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "distribution_tail_state",
      "mechanism": "Recent asymmetry and tail clustering may predict elevated future path risk or rebound behavior.",
      "raw_effect": 6.291010947735956e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.269286229506916,
      "atom_id": "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "volatility_path_shape",
      "mechanism": "Volatility shape contains information about whether risk transfer is building or decaying.",
      "raw_effect": -4.6357861300464405e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.228530333977451,
      "atom_id": "atom_calendar_participation_structure_calendar_volatility_interaction_MGC_60_moderate_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "calendar_participation_structure",
      "mechanism": "Calendar effects are only admissible when interacting with state-dependent participation or volatility.",
      "raw_effect": 0.0001384472971393612,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.2077760802863793,
      "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "mechanism": "Large signed path effort with weak realized progress can identify absorption or exhaustion visible in OHLCV path geometry.",
      "raw_effect": 5.5265455912028406e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.0147729101931096,
      "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_YM_60_low_v1",
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "distribution_tail_state",
      "mechanism": "Recent asymmetry and tail clustering may predict elevated future path risk or rebound behavior.",
      "raw_effect": 4.297626701029669e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 3.0126458885152405,
      "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_60_high_v1",
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "session_transition_state",
      "mechanism": "Return efficiency and participation changes across session boundaries can persist briefly after transition.",
      "raw_effect": -9.039198331224849e-05,
      "status": "ATOM_FALSIFIED"
    },
    {
      "adjusted_evidence": 2.9373393661409777,
      "atom_id": "atom_volatility_path_shape_compression_persistence_MGC_60_high_v1",
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "volatility_path_shape",
      "mechanism": "Volatility shape contains information about whether risk transfer is building or decaying.",
      "raw_effect": -0.00017834127483207864,
      "status": "ATOM_FALSIFIED"
    }
  ],
  "cached_coverage": [
    {
      "end": "2023-12-29 21:59:00+00:00",
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2023-01-01_2024-01-01.parquet",
      "rows": 1403681,
      "start": "2023-01-02 23:00:00+00:00",
      "status": "LOADED_DEVELOPMENT_ONLY",
      "symbols": [
        "ES",
        "MES",
        "MNQ",
        "NQ"
      ]
    },
    {
      "end": "2024-03-28 20:59:00+00:00",
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-01-01_2024-03-31.parquet",
      "rows": 343090,
      "start": "2024-01-01 23:00:00+00:00",
      "status": "LOADED_DEVELOPMENT_ONLY",
      "symbols": [
        "ES",
        "MES",
        "MNQ",
        "NQ"
      ]
    },
    {
      "end": "2024-06-30 23:59:00+00:00",
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-04-01_2024-07-01.parquet",
      "rows": 353679,
      "start": "2024-04-01 00:00:00+00:00",
      "status": "LOADED_DEVELOPMENT_ONLY",
      "symbols": [
        "ES",
        "MES",
        "MNQ",
        "NQ"
      ]
    },
    {
      "end": "2024-09-30 23:59:00+00:00",
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2024-07-01_2024-10-01.parquet",
      "rows": 358955,
      "start": "2024-07-01 00:00:00+00:00",
      "status": "LOADED_DEVELOPMENT_ONLY",
      "symbols": [
        "ES",
        "MES",
        "MNQ",
        "NQ"
      ]
    },
    {
      "end": "2024-09-30 23:59:00+00:00",
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_2023-01-01_2024-10-01.parquet",
      "rows": 3812778,
      "start": "2023-01-02 23:00:00+00:00",
      "status": "LOADED_DEVELOPMENT_ONLY",
      "symbols": [
        "CL",
        "GC",
        "M2K",
        "MCL",
        "MGC",
        "MYM",
        "RTY",
        "YM"
      ]
    }
  ],
  "candidate_level_complete_null_passes": 0,
  "contract_map_completeness": {
    "mapped_symbols": [
      "CL",
      "ES",
      "GC",
      "M2K",
      "MCL",
      "MES",
      "MGC",
      "MNQ",
      "MYM",
      "NQ",
      "RTY",
      "YM"
    ],
    "roll_map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
    "roll_unsafe_rows_excluded": 590666,
    "status": "PARTIAL_OR_COMPLETE_ROLL_MAP_APPLIED",
    "unmapped_symbols": []
  },
  "contract_map_path": "/root/hydra-bot/data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_500ac1ef6c622950.json",
  "contract_replicated_atoms": 95,
  "cost_resilient_strategies": 0,
  "created_at_utc": "2026-07-10T10:14:56+00:00",
  "cross_market_replicated_atoms": 6,
  "cumulative_spend_usd": 22.963245768100997,
  "data_access_record": {
    "candidate_ids": [],
    "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
    "data_role": "DEVELOPMENT",
    "freeze_manifest_hash": null,
    "parameters_mutable": true,
    "period_accessed": "2023-01-01:2024-10-01",
    "process_id": 76424,
    "reason_for_access": "edge atom development/falsification across Q1-Q3 and optional 2023 folds; Q4 excluded",
    "requesting_module": "scripts/run_edge_atom_discovery_lab.py",
    "timestamp_utc": "2026-07-10T10:11:04.530149+00:00"
  },
  "data_roles": {
    "future_2025": "POTENTIAL_FINAL_LOCKBOX",
    "q1_2024": "DEVELOPMENT",
    "q2_2024": "DIAGNOSTIC_CONFIRMATION_CONSUMED",
    "q3_2024": "CONTAMINATED_DEVELOPMENT",
    "q4_2024": "SEALED_BLIND_HOLDOUT"
  },
  "databento_requests": [
    {
      "estimated_cost_usd": 0.0,
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_ES_MES_NQ_MNQ_2023-01-01_2024-01-01.parquet",
      "purpose": "missing_2023_existing_markets",
      "status": "complete_cache_hit",
      "symbols": [
        "ES",
        "MES",
        "NQ",
        "MNQ"
      ]
    },
    {
      "estimated_cost_usd": 0.0,
      "path": "/root/hydra-bot/data/cache/databento/GLBX-MDP3_ohlcv-1m_RTY_M2K_YM_MYM_GC_MGC_CL_MCL_2023-01-01_2024-10-01.parquet",
      "purpose": "new_market_ecology_development",
      "status": "complete_cache_hit",
      "symbols": [
        "RTY",
        "M2K",
        "YM",
        "MYM",
        "GC",
        "MGC",
        "CL",
        "MCL"
      ]
    }
  ],
  "dataset": "GLBX.MDP3",
  "development_period": {
    "end": "2024-10-01",
    "start": "2023-01-01"
  },
  "estimated_combine_pass_probability": 0.0,
  "estimated_repeat_payout_probability": 0.0,
  "estimated_xfa_survival": 0.0,
  "executable_account_baskets": [],
  "families_killed": [
    "accepted_price_migration",
    "calendar_participation_structure",
    "contract_roll_invariant_relative_state",
    "cross_market_risk_transfer",
    "defensive_portfolio_atom",
    "distribution_tail_state",
    "effort_vs_progress",
    "session_inventory_acceptance",
    "session_transition_state",
    "volatility_path_shape"
  ],
  "families_requiring_more_evidence": [],
  "family_results": {
    "accepted_price_migration": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.512730286266663,
      "mean_raw_effect": 0.000118720122825925,
      "status_counts": {
        "ATOM_FALSIFIED": 12
      },
      "temporal_passes": 8
    },
    "calendar_participation_structure": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.228530333977451,
      "mean_raw_effect": 5.587061599872624e-05,
      "status_counts": {
        "ATOM_FALSIFIED": 11,
        "ATOM_INSUFFICIENT_EVIDENCE": 1
      },
      "temporal_passes": 6
    },
    "contract_roll_invariant_relative_state": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 1.1285273055179497,
      "mean_raw_effect": -7.350919949956032e-06,
      "status_counts": {
        "ATOM_FALSIFIED": 8,
        "ATOM_INSUFFICIENT_EVIDENCE": 4
      },
      "temporal_passes": 1
    },
    "cross_market_risk_transfer": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 1.1870631516879784,
      "mean_raw_effect": -5.285209884697903e-06,
      "status_counts": {
        "ATOM_FALSIFIED": 12
      },
      "temporal_passes": 7
    },
    "defensive_portfolio_atom": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.3675205442509557,
      "mean_raw_effect": 2.5376394130251863e-07,
      "status_counts": {
        "ATOM_FALSIFIED": 8,
        "ATOM_INSUFFICIENT_EVIDENCE": 4
      },
      "temporal_passes": 5
    },
    "distribution_tail_state": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.287842137548147,
      "mean_raw_effect": 1.1717950642445017e-05,
      "status_counts": {
        "ATOM_FALSIFIED": 8,
        "ATOM_INSUFFICIENT_EVIDENCE": 4
      },
      "temporal_passes": 6
    },
    "effort_vs_progress": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 4.639409900099716,
      "mean_raw_effect": -5.644609362286233e-06,
      "status_counts": {
        "ATOM_FALSIFIED": 12
      },
      "temporal_passes": 12
    },
    "session_inventory_acceptance": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 2.4268553964578303,
      "mean_raw_effect": -0.0001585740101465583,
      "status_counts": {
        "ATOM_FALSIFIED": 9,
        "ATOM_INSUFFICIENT_EVIDENCE": 3
      },
      "temporal_passes": 3
    },
    "session_transition_state": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.0126458885152405,
      "mean_raw_effect": 0.0003515229156601909,
      "status_counts": {
        "ATOM_FALSIFIED": 12
      },
      "temporal_passes": 8
    },
    "volatility_path_shape": {
      "adversarial_passes": 0,
      "atoms": 12,
      "disposition": "FALSIFIED",
      "max_adjusted_evidence": 3.269286229506916,
      "mean_raw_effect": -0.00015333476473970505,
      "status_counts": {
        "ATOM_FALSIFIED": 12
      },
      "temporal_passes": 4
    }
  },
  "fully_validated_edge_atoms": 0,
  "historical_q4_access_records_before_phase": 0,
  "new_markets_acquired": [],
  "portfolio_only_strategies": 0,
  "preregistration_path": "/root/hydra-bot/reports/edge_atom_lab/edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
  "q4_access_count": 0,
  "q4_freeze_recommendations": [],
  "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
  "registry_integrity": "ok",
  "remaining_budget_usd": 77.03675423189901,
  "report_correction": "Corrected Q4 access accounting: development period ending exactly at 2024-10-01 is exclusive and is not Q4 access.",
  "schema": "ohlcv-1m",
  "single_writer_registry": true,
  "spend_this_phase_usd": 19.069551210850996,
  "status_scope_violations_detected": 0,
  "strategies_assembled": 0,
  "strategy_specs": [],
  "temporal_transfer_strategy_passes": 0,
  "temporally_replicated_atoms": 60,
  "tombstoned_prior_formulations": [
    "topstep_nq_es_divergence_controlled",
    "previous_paired_es_nq_residual_formulation",
    "previous_opening_auction_formulation",
    "previous_volatility_shape_formulation",
    "previous_overnight_inventory_strategy_formulations",
    "previous_intraday_range_migration_strategy_formulations",
    "strict_replay_12_candidate_formulations"
  ],
  "top_atom_results": [
    {
      "adversarial": {
        "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 7.49142800737072e-05,
            "block_shuffled_signal": 7.502836895691306e-05,
            "cost_stress": 1.5028368956913062e-05,
            "delayed_signal": 8.433044879298628e-05,
            "event_time_jitter": 7.501919886932527e-05,
            "mean_reversion_baseline": -3.78230467936787e-05,
            "momentum_baseline": 3.78230467936787e-05,
            "opportunity_count_matched_random": 1.3418011430954994e-06,
            "session_only_baseline": 0.0001683562547522378,
            "sign_flipped_signal": -7.502836895691306e-05,
            "volatility_only_baseline": 5.412077731892925e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": true,
            "delayed_signal": false,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 249408,
          "real_effect": 7.502836895691306e-05
        },
        "effect_retention_after_best_event_removed": 0.9984793900654912,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 7.502836895691306e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,session_only_baseline"
      },
      "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
      "confidence_high": 8.414075837590564e-05,
      "confidence_low": 6.584866765489275e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.00012582555670064149,
          "observations": 43817
        },
        "2023Q2": {
          "effect": 0.00011833329388499686,
          "observations": 30801
        },
        "2023Q3": {
          "effect": -6.141917900327726e-05,
          "observations": 31950
        },
        "2023Q4": {
          "effect": 0.00012919885358834263,
          "observations": 32544
        },
        "2024Q1": {
          "effect": 0.0001474699391094757,
          "observations": 28841
        },
        "2024Q2": {
          "effect": 5.9123204459455e-05,
          "observations": 34754
        },
        "2024Q3": {
          "effect": 2.1332906533375725e-05,
          "observations": 46707
        }
      },
      "contracts_positive": 6,
      "cost_hurdle": 0.0009905621440167295,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0009155674310013303,
      "evidence_strength": 16.071387328221515,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "fdr_adjusted_evidence": 4.639409900099716,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 0.00012273288218531448,
          "observations": 74618
        },
        "2023_h2": {
          "effect": 3.476764849481058e-05,
          "observations": 64494
        },
        "2024_q1": {
          "effect": 0.0001474699391094757,
          "observations": 28841
        },
        "2024_q2": {
          "effect": 5.9123204459455e-05,
          "observations": 34754
        },
        "2024_q3": {
          "effect": 2.1332906533375725e-05,
          "observations": 46707
        }
      },
      "folds_positive": 5,
      "market_count": 1,
      "market_results": {
        "MES": {
          "effect": 7.49947130153992e-05,
          "observations": 249414
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:41.947437+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 4.639409900099716,
        "input_hash": "56237ced34ba8e7ebe40e870e75d7050a452cbd5bdb7523ab44c1cec4d3df26a",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 7.49947130153992e-05,
      "replication": {
        "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_MES_60_low_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 6,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 5,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.4369635714773761,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0001353296552559595,
      "valid_observations": 249414
    },
    {
      "adversarial": {
        "atom_id": "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "event_time_jitter",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:event_time_jitter,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 0.00010786120875287744,
            "block_shuffled_signal": -6.951134264902237e-06,
            "cost_stress": 4.82706627987056e-05,
            "delayed_signal": 1.5839791190494402e-05,
            "event_time_jitter": 0.0001086393633590573,
            "mean_reversion_baseline": -0.00010800972380714968,
            "momentum_baseline": 0.00010800972380714968,
            "opportunity_count_matched_random": -4.724687042060236e-06,
            "session_only_baseline": 0.00013920621969383206,
            "sign_flipped_signal": -0.0001082706627987056,
            "volatility_only_baseline": 8.36718542481979e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": true,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 85678,
          "real_effect": 0.0001082706627987056
        },
        "effect_retention_after_best_event_removed": 0.9962182364525707,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 0.0001082706627987056,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:event_time_jitter,session_only_baseline"
      },
      "atom_id": "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
      "confidence_high": 0.00012654328822683661,
      "confidence_low": 9.143331322604558e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.00022410440170938001,
          "observations": 11456
        },
        "2023Q2": {
          "effect": 0.00024047276940421968,
          "observations": 10939
        },
        "2023Q3": {
          "effect": 0.00011640371835384496,
          "observations": 13107
        },
        "2023Q4": {
          "effect": 9.809743296597398e-05,
          "observations": 13074
        },
        "2024Q1": {
          "effect": 4.878870727275467e-05,
          "observations": 12647
        },
        "2024Q2": {
          "effect": 7.98172189810108e-05,
          "observations": 11621
        },
        "2024Q3": {
          "effect": -1.6277453653378972e-05,
          "observations": 12865
        }
      },
      "contracts_positive": 6,
      "cost_hurdle": 0.00028349581843667806,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.00017450751771023695,
      "evidence_strength": 12.168454658199654,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "accepted_price_migration",
      "fdr_adjusted_evidence": 3.512730286266663,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 0.00023209964949745106,
          "observations": 22395
        },
        "2023_h2": {
          "effect": 0.00010726211279404872,
          "observations": 26181
        },
        "2024_q1": {
          "effect": 4.878870727275467e-05,
          "observations": 12647
        },
        "2024_q2": {
          "effect": 7.98172189810108e-05,
          "observations": 11621
        },
        "2024_q3": {
          "effect": -1.6277453653378972e-05,
          "observations": 12865
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "MNQ": {
          "effect": 0.0001089883007264411,
          "observations": 85709
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:43.174556+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.512730286266663,
        "input_hash": "5c80443ac25bcbacedf4e2451ecaf534653f0dae7328b18a0868111c5d1fdcc6",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 0.0001089883007264411,
      "replication": {
        "atom_id": "atom_accepted_price_migration_old_region_reentry_MNQ_60_extreme_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 6,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.15000848850376558,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0004657120996425953,
      "valid_observations": 85709
    },
    {
      "adversarial": {
        "atom_id": "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -3.598482827864884e-05,
            "block_shuffled_signal": -3.606226082634827e-05,
            "cost_stress": -2.393773917365173e-05,
            "delayed_signal": -3.1117537107205365e-05,
            "event_time_jitter": -3.576774963466694e-05,
            "mean_reversion_baseline": 3.025412747720756e-06,
            "momentum_baseline": -3.025412747720756e-06,
            "opportunity_count_matched_random": -7.982770289450664e-08,
            "session_only_baseline": 0.00012840287581584583,
            "sign_flipped_signal": 3.606226082634827e-05,
            "volatility_only_baseline": -7.800010584017814e-08
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 583270,
          "real_effect": -3.606226082634827e-05
        },
        "effect_retention_after_best_event_removed": 0.9978528093933906,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -3.606226082634827e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
      "confidence_high": -2.9979154133810575e-05,
      "confidence_low": -4.208765870951371e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -6.832288478382676e-05,
          "observations": 109544
        },
        "2023Q2": {
          "effect": -7.708182979042745e-05,
          "observations": 80030
        },
        "2023Q3": {
          "effect": 3.1017459288801176e-05,
          "observations": 73293
        },
        "2023Q4": {
          "effect": -8.011864926081409e-05,
          "observations": 74909
        },
        "2024Q1": {
          "effect": -6.334572345016123e-05,
          "observations": 72411
        },
        "2024Q2": {
          "effect": 6.943733362237683e-07,
          "observations": 68610
        },
        "2024Q3": {
          "effect": 8.637076969633499e-06,
          "observations": 104500
        }
      },
      "contracts_positive": 4,
      "cost_hurdle": 0.0017780784407688674,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0017420450343472052,
      "evidence_strength": 11.665433356349306,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "defensive_portfolio_atom",
      "fdr_adjusted_evidence": 3.3675205442509557,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -7.202053514135604e-05,
          "observations": 189574
        },
        "2023_h2": {
          "effect": -2.5156511071538965e-05,
          "observations": 148202
        },
        "2024_q1": {
          "effect": -6.334572345016123e-05,
          "observations": 72411
        },
        "2024_q2": {
          "effect": 6.943733362237683e-07,
          "observations": 68610
        },
        "2024_q3": {
          "effect": 8.637076969633499e-06,
          "observations": 104500
        }
      },
      "folds_positive": 3,
      "market_count": 3,
      "market_results": {
        "MYM": {
          "effect": -2.4271240584576273e-05,
          "observations": 120902
        },
        "NQ": {
          "effect": -5.863212411539484e-05,
          "observations": 211048
        },
        "RTY": {
          "effect": -2.2715782587713276e-05,
          "observations": 251347
        }
      },
      "markets_positive": 3,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:27.425853+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.3675205442509557,
        "input_hash": "20ab32fa9f8f3c8e67191a68bdf85d75662f9a42e0571b1a7f7fbe977be380f4",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -3.6033406421662145e-05,
      "replication": {
        "atom_id": "atom_defensive_portfolio_atom_shared_loss_risk_state_MYM_30_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 4,
        "cross_market_pass": true,
        "cross_market_required": true,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 3,
        "market_count": 3,
        "markets_positive": 3,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.35000627050289435,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 7.069739971557857e-05,
      "valid_observations": 583297
    },
    {
      "adversarial": {
        "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_MES_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 6.319051631355498e-05,
            "block_shuffled_signal": 6.333302219045666e-05,
            "cost_stress": 3.333022190456663e-06,
            "delayed_signal": 7.861066996605489e-05,
            "event_time_jitter": 6.363183933537804e-05,
            "mean_reversion_baseline": -3.855977586913836e-05,
            "momentum_baseline": 3.855977586913836e-05,
            "opportunity_count_matched_random": -8.955869781893524e-06,
            "session_only_baseline": 0.0001722610335065318,
            "sign_flipped_signal": -6.333302219045666e-05,
            "volatility_only_baseline": 5.412077731892925e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": true,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 199756,
          "real_effect": 6.333302219045666e-05
        },
        "effect_retention_after_best_event_removed": 0.9977498961525452,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 6.333302219045666e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,session_only_baseline"
      },
      "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_MES_60_moderate_v1",
      "confidence_high": 7.373628171421047e-05,
      "confidence_low": 5.208393724050864e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 9.987229265451309e-05,
          "observations": 42311
        },
        "2023Q2": {
          "effect": 0.00010497743369710227,
          "observations": 27450
        },
        "2023Q3": {
          "effect": -8.092025699557389e-05,
          "observations": 27238
        },
        "2023Q4": {
          "effect": 0.00013364440826111942,
          "observations": 27478
        },
        "2024Q1": {
          "effect": 0.00017955780533859478,
          "observations": 20316
        },
        "2024Q2": {
          "effect": 6.378587900479033e-05,
          "observations": 22975
        },
        "2024Q3": {
          "effect": -3.4959751776557356e-05,
          "observations": 32028
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.0010104979509347107,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0009475878414573511,
      "evidence_strength": 11.389419258998503,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "distribution_tail_state",
      "fdr_adjusted_evidence": 3.287842137548147,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 0.00010188109587721736,
          "observations": 69761
        },
        "2023_h2": {
          "effect": 2.6832646577849204e-05,
          "observations": 54716
        },
        "2024_q1": {
          "effect": 0.00017955780533859478,
          "observations": 20316
        },
        "2024_q2": {
          "effect": 6.378587900479033e-05,
          "observations": 22975
        },
        "2024_q3": {
          "effect": -3.4959751776557356e-05,
          "observations": 32028
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "MES": {
          "effect": 6.291010947735956e-05,
          "observations": 199796
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:40.347637+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.287842137548147,
        "input_hash": "45ef7d1d4d4179e11745e903eb0477d16550a4231f40f7237587c0acd4ec392a",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 6.291010947735956e-05,
      "replication": {
        "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_MES_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.350034776423512,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00015914156496038597,
      "valid_observations": 199796
    },
    {
      "adversarial": {
        "atom_id": "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline",
          "volatility_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -4.6238459988509746e-05,
            "block_shuffled_signal": -4.638071130376267e-05,
            "cost_stress": -1.3619288696237333e-05,
            "delayed_signal": -3.1411901177785466e-05,
            "event_time_jitter": -4.661500242344302e-05,
            "mean_reversion_baseline": -6.3933523229496545e-06,
            "momentum_baseline": 6.3933523229496545e-06,
            "opportunity_count_matched_random": 5.612869517449919e-06,
            "session_only_baseline": 6.79054228973049e-05,
            "sign_flipped_signal": 4.638071130376267e-05,
            "volatility_only_baseline": 5.43107023189453e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": false
          },
          "event_count": 198985,
          "real_effect": -4.638071130376267e-05
        },
        "effect_retention_after_best_event_removed": 0.996932963914217,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -4.638071130376267e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
      "confidence_high": -3.8334877876704384e-05,
      "confidence_low": -5.4380844724224425e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -0.00010558213673172595,
          "observations": 28526
        },
        "2023Q2": {
          "effect": -4.2667098768913434e-05,
          "observations": 29213
        },
        "2023Q3": {
          "effect": 9.374608933817793e-06,
          "observations": 27352
        },
        "2023Q4": {
          "effect": -9.57312358968892e-05,
          "observations": 29067
        },
        "2024Q1": {
          "effect": -5.513319815589606e-05,
          "observations": 27970
        },
        "2024Q2": {
          "effect": -8.776985581238618e-06,
          "observations": 28818
        },
        "2024Q3": {
          "effect": -2.3005675707242726e-05,
          "observations": 28040
        }
      },
      "contracts_positive": 1,
      "cost_hurdle": 0.001973467821510799,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.0019271099602103346,
      "evidence_strength": 11.325139707982526,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "volatility_path_shape",
      "fdr_adjusted_evidence": 3.269286229506916,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -7.375032454225883e-05,
          "observations": 57739
        },
        "2023_h2": {
          "effect": -4.477579415191858e-05,
          "observations": 56419
        },
        "2024_q1": {
          "effect": -5.513319815589606e-05,
          "observations": 27970
        },
        "2024_q2": {
          "effect": -8.776985581238618e-06,
          "observations": 28818
        },
        "2024_q3": {
          "effect": -2.3005675707242726e-05,
          "observations": 28040
        }
      },
      "folds_positive": 0,
      "market_count": 1,
      "market_results": {
        "ES": {
          "effect": -4.6357861300464405e-05,
          "observations": 198986
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:33.988764+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.269286229506916,
        "input_hash": "cf1bf0ea4ad566dbbf70bdd292e2879bf0dd887684bcb446dc4ab06a61906e9d",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -4.6357861300464405e-05,
      "replication": {
        "atom_id": "atom_volatility_path_shape_failed_expansion_ES_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 1,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 0,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.34865355940756265,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00015884954538319411,
      "valid_observations": 198986
    },
    {
      "adversarial": {
        "atom_id": "atom_calendar_participation_structure_calendar_volatility_interaction_MGC_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 0.00013739865204113953,
            "block_shuffled_signal": 0.0001384472971393612,
            "cost_stress": 7.84472971393612e-05,
            "delayed_signal": 0.00017717499582455842,
            "event_time_jitter": 0.00013787615499870497,
            "mean_reversion_baseline": 3.208430819435544e-05,
            "momentum_baseline": -3.208430819435544e-05,
            "opportunity_count_matched_random": -1.2268438948375689e-05,
            "session_only_baseline": 0.00032475105228597765,
            "sign_flipped_signal": -0.0001384472971393612,
            "volatility_only_baseline": -8.306583105845329e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": true,
            "delayed_signal": false,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 93753,
          "real_effect": 0.0001384472971393612
        },
        "effect_retention_after_best_event_removed": 0.992425673018621,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 0.0001384472971393612,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,session_only_baseline"
      },
      "atom_id": "atom_calendar_participation_structure_calendar_volatility_interaction_MGC_60_moderate_v1",
      "confidence_high": 0.00016271032845182885,
      "confidence_low": 0.00011418426582689354,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.0002213867634753213,
          "observations": 22311
        },
        "2023Q2": {
          "effect": -0.00032784798330110166,
          "observations": 9751
        },
        "2023Q3": {
          "effect": 9.279270892421508e-05,
          "observations": 10275
        },
        "2023Q4": {
          "effect": 0.00039679766889413713,
          "observations": 6333
        },
        "2024Q1": {
          "effect": 0.00012170003822989901,
          "observations": 15343
        },
        "2024Q2": {
          "effect": 0.00012517138435373594,
          "observations": 12096
        },
        "2024Q3": {
          "effect": 0.00024879015988271745,
          "observations": 17644
        }
      },
      "contracts_positive": 6,
      "cost_hurdle": 0.0022246391140992683,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.002086191816959907,
      "evidence_strength": 11.183957144452522,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "calendar_participation_structure",
      "fdr_adjusted_evidence": 3.228530333977451,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 5.434827505236265e-05,
          "observations": 32062
        },
        "2023_h2": {
          "effect": 0.00020871656558904629,
          "observations": 16608
        },
        "2024_q1": {
          "effect": 0.00012170003822989901,
          "observations": 15343
        },
        "2024_q2": {
          "effect": 0.00012517138435373594,
          "observations": 12096
        },
        "2024_q3": {
          "effect": 0.00024879015988271745,
          "observations": 17644
        }
      },
      "folds_positive": 5,
      "market_count": 1,
      "market_results": {
        "MGC": {
          "effect": 0.0001384472971393612,
          "observations": 93753
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:41.454608+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.228530333977451,
        "input_hash": "5f86aa318487c1c12fe78581cf1223f6e146f0a3eaac3eed4f7cedd43192d790",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 0.0001384472971393612,
      "replication": {
        "atom_id": "atom_calendar_participation_structure_calendar_volatility_interaction_MGC_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 6,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 5,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.3498781530010188,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0010788671679825685,
      "valid_observations": 93753
    },
    {
      "adversarial": {
        "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress",
        "details": {
          "attack_effects": {
            "best_event_removed": 5.519997155327877e-05,
            "block_shuffled_signal": 5.529156522187174e-05,
            "cost_stress": -4.708434778128264e-06,
            "delayed_signal": 6.488017437914893e-05,
            "event_time_jitter": 5.534947283069143e-05,
            "mean_reversion_baseline": -1.6137744396020724e-05,
            "momentum_baseline": 1.6137744396020724e-05,
            "opportunity_count_matched_random": 2.5297289316140334e-06,
            "session_only_baseline": 0.00018749677343412834,
            "sign_flipped_signal": -5.529156522187174e-05,
            "volatility_only_baseline": 1.624215026939314e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 196310,
          "real_effect": 5.529156522187174e-05
        },
        "effect_retention_after_best_event_removed": 0.9983434422913255,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 5.529156522187174e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress"
      },
      "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
      "confidence_high": 6.501344783793348e-05,
      "confidence_low": 4.551746398612333e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 5.585459797218073e-05,
          "observations": 35363
        },
        "2023Q2": {
          "effect": 9.667785040741138e-05,
          "observations": 25780
        },
        "2023Q3": {
          "effect": -6.461602844391055e-05,
          "observations": 24411
        },
        "2023Q4": {
          "effect": 0.0001715389130909977,
          "observations": 25350
        },
        "2024Q1": {
          "effect": 9.945436471883487e-05,
          "observations": 22816
        },
        "2024Q2": {
          "effect": -6.4950911327007515e-06,
          "observations": 27698
        },
        "2024Q3": {
          "effect": 4.359942642812178e-05,
          "observations": 34898
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.0002550369803621525,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0001997715244501241,
      "evidence_strength": 11.112062300720302,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "fdr_adjusted_evidence": 3.2077760802863793,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 7.306709078051932e-05,
          "observations": 61143
        },
        "2023_h2": {
          "effect": 5.568958775974139e-05,
          "observations": 49761
        },
        "2024_q1": {
          "effect": 9.945436471883487e-05,
          "observations": 22816
        },
        "2024_q2": {
          "effect": -6.4950911327007515e-06,
          "observations": 27698
        },
        "2024_q3": {
          "effect": 4.359942642812178e-05,
          "observations": 34898
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "YM": {
          "effect": 5.5265455912028406e-05,
          "observations": 196316
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:31.471154+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.2077760802863793,
        "input_hash": "ebcefb693fcf03bea6ae71268b8b68e1a3990d84aecdbb94cb955f5bd46feeda",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 5.5265455912028406e-05,
      "replication": {
        "atom_id": "atom_effort_vs_progress_directional_pressure_without_progress_YM_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.34885613655991554,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00011286540920917994,
      "valid_observations": 196316
    },
    {
      "adversarial": {
        "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_YM_60_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 4.3183870251669063e-05,
            "block_shuffled_signal": 4.3254928934907426e-05,
            "cost_stress": -1.6745071065092575e-05,
            "delayed_signal": 5.061489606483604e-05,
            "event_time_jitter": 4.307580104463775e-05,
            "mean_reversion_baseline": -1.4389151650083307e-05,
            "momentum_baseline": 1.4389151650083307e-05,
            "opportunity_count_matched_random": 8.170791113512891e-07,
            "session_only_baseline": 0.00010894346413250729,
            "sign_flipped_signal": -4.3254928934907426e-05,
            "volatility_only_baseline": 1.624215026939314e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 253210,
          "real_effect": 4.3254928934907426e-05
        },
        "effect_retention_after_best_event_removed": 0.9983572118834065,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 4.3254928934907426e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_YM_60_low_v1",
      "confidence_high": 5.104192000140681e-05,
      "confidence_low": 3.4910614019186566e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 2.211779913202682e-05,
          "observations": 48107
        },
        "2023Q2": {
          "effect": 6.785876465855653e-05,
          "observations": 36478
        },
        "2023Q3": {
          "effect": -4.2805963439250926e-05,
          "observations": 34993
        },
        "2023Q4": {
          "effect": 0.00015085673320619264,
          "observations": 34410
        },
        "2024Q1": {
          "effect": 7.861048213996792e-05,
          "observations": 28342
        },
        "2024Q2": {
          "effect": -3.894519923015563e-05,
          "observations": 32754
        },
        "2024Q3": {
          "effect": 7.070897410555738e-05,
          "observations": 38175
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.00025888850535036245,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.00021591223834006574,
      "evidence_strength": 10.443479707473498,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "distribution_tail_state",
      "fdr_adjusted_evidence": 3.0147729101931096,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 4.1843979193228575e-05,
          "observations": 84585
        },
        "2023_h2": {
          "effect": 5.321198090853969e-05,
          "observations": 69403
        },
        "2024_q1": {
          "effect": 7.861048213996792e-05,
          "observations": 28342
        },
        "2024_q2": {
          "effect": -3.894519923015563e-05,
          "observations": 32754
        },
        "2024_q3": {
          "effect": 7.070897410555738e-05,
          "observations": 38175
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "YM": {
          "effect": 4.297626701029669e-05,
          "observations": 253259
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:52.904619+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.0147729101931096,
        "input_hash": "7ece6c4188ea381532cc32b221c8f11e0ae077bec582a6f929b5dbe5bfd8e9e0",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 4.297626701029669e-05,
      "replication": {
        "atom_id": "atom_distribution_tail_state_recovery_speed_after_loss_YM_60_low_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.45004460303300625,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 9.596138777863987e-05,
      "valid_observations": 253259
    },
    {
      "adversarial": {
        "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_60_high_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "event_time_jitter",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "cost_stress",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:event_time_jitter,momentum_baseline,mean_reversion_baseline,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -9.013989309671695e-05,
            "block_shuffled_signal": -3.500834218455966e-05,
            "cost_stress": 3.0391983312248487e-05,
            "delayed_signal": 3.81154697421413e-05,
            "event_time_jitter": -9.079968011704929e-05,
            "mean_reversion_baseline": -0.00010772847570902824,
            "momentum_baseline": 0.00010772847570902824,
            "opportunity_count_matched_random": 8.96551362114778e-07,
            "session_only_baseline": 0.00022079576730886602,
            "sign_flipped_signal": 9.039198331224849e-05,
            "volatility_only_baseline": 8.579276656517448e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": true,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": false,
            "momentum_baseline": false,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 142739,
          "real_effect": -9.039198331224849e-05
        },
        "effect_retention_after_best_event_removed": 0.9972111441048847,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -9.039198331224849e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:event_time_jitter,momentum_baseline,mean_reversion_baseline,session_only_baseline"
      },
      "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_60_high_v1",
      "confidence_high": -7.341551774922854e-05,
      "confidence_low": -0.00010736844887526844,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -0.0001771657505204381,
          "observations": 28934
        },
        "2023Q2": {
          "effect": -0.00013400164010492506,
          "observations": 18348
        },
        "2023Q3": {
          "effect": 4.932747980081259e-05,
          "observations": 19588
        },
        "2023Q4": {
          "effect": -0.00013506420681748776,
          "observations": 17633
        },
        "2024Q1": {
          "effect": 4.9143250936878255e-06,
          "observations": 16900
        },
        "2024Q2": {
          "effect": -7.200395051491566e-05,
          "observations": 15298
        },
        "2024Q3": {
          "effect": -0.00011075584001978715,
          "observations": 26038
        }
      },
      "contracts_positive": 2,
      "cost_hurdle": 0.0005840931953142746,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.0004937012120020261,
      "evidence_strength": 10.43611148824376,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "session_transition_state",
      "fdr_adjusted_evidence": 3.0126458885152405,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -0.0001604157167252553,
          "observations": 47282
        },
        "2023_h2": {
          "effect": -3.802585864094044e-05,
          "observations": 37221
        },
        "2024_q1": {
          "effect": 4.9143250936878255e-06,
          "observations": 16900
        },
        "2024_q2": {
          "effect": -7.200395051491566e-05,
          "observations": 15298
        },
        "2024_q3": {
          "effect": -0.00011075584001978715,
          "observations": 26038
        }
      },
      "folds_positive": 1,
      "market_count": 1,
      "market_results": {
        "NQ": {
          "effect": -9.039198331224849e-05,
          "observations": 142739
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:13:03.969711+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 3.0126458885152405,
        "input_hash": "44172c18ba144e0017259c234f9aebe06d0eab2b491fa1e2da3df80b01004a86",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -9.039198331224849e-05,
      "replication": {
        "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_60_high_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 2,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 1,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.24998686482680837,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0002113094623301967,
      "valid_observations": 142739
    },
    {
      "adversarial": {
        "atom_id": "atom_volatility_path_shape_compression_persistence_MGC_60_high_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "event_time_jitter",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -0.00017687397089373173,
            "block_shuffled_signal": -0.00017834127483207864,
            "cost_stress": 0.00011834127483207864,
            "delayed_signal": -8.909861045616967e-05,
            "event_time_jitter": -0.00018070381318025688,
            "mean_reversion_baseline": 0.00013668191659424372,
            "momentum_baseline": -0.00013668191659424372,
            "opportunity_count_matched_random": 9.263994999890998e-06,
            "session_only_baseline": 0.0005055919670776569,
            "sign_flipped_signal": 0.00017834127483207864,
            "volatility_only_baseline": -8.306583105845329e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": true,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 66976,
          "real_effect": -0.00017834127483207864
        },
        "effect_retention_after_best_event_removed": 0.9917724938339233,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -0.00017834127483207864,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,session_only_baseline"
      },
      "atom_id": "atom_volatility_path_shape_compression_persistence_MGC_60_high_v1",
      "confidence_high": -0.00014398839190188738,
      "confidence_low": -0.0002126941577622699,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -1.91471078789779e-05,
          "observations": 13756
        },
        "2023Q2": {
          "effect": 0.0003038367621617021,
          "observations": 7692
        },
        "2023Q3": {
          "effect": -0.00029223467770568253,
          "observations": 8966
        },
        "2023Q4": {
          "effect": -0.0005364538978486399,
          "observations": 6706
        },
        "2024Q1": {
          "effect": -0.0001838744239383957,
          "observations": 12325
        },
        "2024Q2": {
          "effect": -0.0001690581198232508,
          "observations": 6933
        },
        "2024Q3": {
          "effect": -0.00041161934352334747,
          "observations": 10598
        }
      },
      "contracts_positive": 1,
      "cost_hurdle": 0.002233472304943419,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.00205513103011134,
      "evidence_strength": 10.175242042456668,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "volatility_path_shape",
      "fdr_adjusted_evidence": 2.9373393661409777,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 9.668615994799479e-05,
          "observations": 21448
        },
        "2023_h2": {
          "effect": -0.0003967353215468433,
          "observations": 15672
        },
        "2024_q1": {
          "effect": -0.0001838744239383957,
          "observations": 12325
        },
        "2024_q2": {
          "effect": -0.0001690581198232508,
          "observations": 6933
        },
        "2024_q3": {
          "effect": -0.00041161934352334747,
          "observations": 10598
        }
      },
      "folds_positive": 1,
      "market_count": 1,
      "market_results": {
        "MGC": {
          "effect": -0.00017834127483207864,
          "observations": 66976
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:13:17.490559+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.9373393661409777,
        "input_hash": "e3183aaca35efd4455a7c4ae25701ca02b3fb0f175bb2b390431ead860bca0ce",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -0.00017834127483207864,
      "replication": {
        "atom_id": "atom_volatility_path_shape_compression_persistence_MGC_60_high_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 1,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 1,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.24994868617960211,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0009597672253645851,
      "valid_observations": 66976
    },
    {
      "adversarial": {
        "atom_id": "atom_calendar_participation_structure_weekday_state_interaction_MYM_15_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress",
        "details": {
          "attack_effects": {
            "best_event_removed": -2.5685838874809317e-05,
            "block_shuffled_signal": -2.5595962172257648e-05,
            "cost_stress": -3.4404037827742354e-05,
            "delayed_signal": -2.7862477696608715e-05,
            "event_time_jitter": -2.561033177140696e-05,
            "mean_reversion_baseline": -1.4846068521466974e-06,
            "momentum_baseline": 1.4846068521466974e-06,
            "opportunity_count_matched_random": 8.312901426009143e-07,
            "session_only_baseline": 3.8362960921412695e-05,
            "sign_flipped_signal": 2.5595962172257648e-05,
            "volatility_only_baseline": -2.31860229223368e-06
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 102271,
          "real_effect": -2.5595962172257648e-05
        },
        "effect_retention_after_best_event_removed": 1.0035113625323717,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -2.5595962172257648e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress"
      },
      "atom_id": "atom_calendar_participation_structure_weekday_state_interaction_MYM_15_moderate_v1",
      "confidence_high": -2.0463200047956238e-05,
      "confidence_low": -3.072872429655906e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -4.463192558784583e-05,
          "observations": 15638
        },
        "2023Q2": {
          "effect": -6.322367877131468e-05,
          "observations": 14274
        },
        "2023Q3": {
          "effect": -9.12141902409431e-06,
          "observations": 14732
        },
        "2023Q4": {
          "effect": -2.0457744609461913e-05,
          "observations": 14474
        },
        "2024Q1": {
          "effect": 4.401426223691196e-06,
          "observations": 13855
        },
        "2024Q2": {
          "effect": -3.5920696107930983e-05,
          "observations": 14703
        },
        "2024Q3": {
          "effect": -8.199618301370758e-06,
          "observations": 14595
        }
      },
      "contracts_positive": 1,
      "cost_hurdle": 0.00012714019325309374,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.0001015442310808361,
      "evidence_strength": 9.774091345496183,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "calendar_participation_structure",
      "fdr_adjusted_evidence": 2.8215371347031066,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -5.350390622908796e-05,
          "observations": 29912
        },
        "2023_h2": {
          "effect": -1.47395103930805e-05,
          "observations": 29206
        },
        "2024_q1": {
          "effect": 4.401426223691196e-06,
          "observations": 13855
        },
        "2024_q2": {
          "effect": -3.5920696107930983e-05,
          "observations": 14703
        },
        "2024_q3": {
          "effect": -8.199618301370758e-06,
          "observations": 14595
        }
      },
      "folds_positive": 1,
      "market_count": 1,
      "market_results": {
        "MYM": {
          "effect": -2.5595962172257648e-05,
          "observations": 102271
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:10.200734+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.8215371347031066,
        "input_hash": "543b6162669857982154c44a908a702bfe8e037a861361d00e56b4bd6192380b",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -2.5595962172257648e-05,
      "replication": {
        "atom_id": "atom_calendar_participation_structure_weekday_state_interaction_MYM_15_moderate_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 1,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 1,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.1869735877480205,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00035930693944084406,
      "valid_observations": 102271
    },
    {
      "adversarial": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_MES_60_high_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "cost_stress",
          "session_only_baseline",
          "volatility_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:cost_stress,session_only_baseline,volatility_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 4.745442132740208e-05,
            "block_shuffled_signal": -5.89872636494377e-06,
            "cost_stress": -1.23483646425043e-05,
            "delayed_signal": -3.837222863012524e-05,
            "event_time_jitter": 4.707287639471668e-05,
            "mean_reversion_baseline": -4.230254680117778e-05,
            "momentum_baseline": 4.230254680117778e-05,
            "opportunity_count_matched_random": 3.236957419028733e-06,
            "session_only_baseline": 9.29591006909618e-05,
            "sign_flipped_signal": -4.76516353574957e-05,
            "volatility_only_baseline": 5.412077731892925e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": false
          },
          "event_count": 139611,
          "real_effect": 4.76516353574957e-05
        },
        "effect_retention_after_best_event_removed": 0.9958613376306171,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 4.76516353574957e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:cost_stress,session_only_baseline,volatility_only_baseline"
      },
      "atom_id": "atom_accepted_price_migration_extreme_dwell_MES_60_high_v1",
      "confidence_high": 5.77310775547717e-05,
      "confidence_low": 3.810183667752732e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.00028399048698846336,
          "observations": 18914
        },
        "2023Q2": {
          "effect": 0.000115181057961496,
          "observations": 17824
        },
        "2023Q3": {
          "effect": 4.563203919313507e-05,
          "observations": 21361
        },
        "2023Q4": {
          "effect": -4.9501742242465704e-05,
          "observations": 20797
        },
        "2024Q1": {
          "effect": -8.123646250562503e-05,
          "observations": 19292
        },
        "2024Q2": {
          "effect": 5.520736186663445e-06,
          "observations": 19912
        },
        "2024Q3": {
          "effect": 3.614467233973897e-05,
          "observations": 21528
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.0009856532690833424,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0009377368119671929,
      "evidence_strength": 9.569015585979946,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "accepted_price_migration",
      "fdr_adjusted_evidence": 2.7623368622226234,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 0.00020209002253812134,
          "observations": 36738
        },
        "2023_h2": {
          "effect": -1.2984900662270784e-06,
          "observations": 42158
        },
        "2024_q1": {
          "effect": -8.123646250562503e-05,
          "observations": 19292
        },
        "2024_q2": {
          "effect": 5.520736186663445e-06,
          "observations": 19912
        },
        "2024_q3": {
          "effect": 3.614467233973897e-05,
          "observations": 21528
        }
      },
      "folds_positive": 3,
      "market_count": 1,
      "market_results": {
        "MES": {
          "effect": 4.791645711614951e-05,
          "observations": 139628
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:13:16.429255+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.7623368622226234,
        "input_hash": "c89de85731256d53171e79ca94f656de74a67ee30a679bda8c0c3b2f53bd4b01",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 4.791645711614951e-05,
      "replication": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_MES_60_high_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 3,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.24462279406225418,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00032259567828237087,
      "valid_observations": 139628
    },
    {
      "adversarial": {
        "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_30_high_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -4.162472009272645e-05,
            "block_shuffled_signal": -4.194096512539291e-05,
            "cost_stress": -1.8059034874607093e-05,
            "delayed_signal": -9.605390995266107e-06,
            "event_time_jitter": -4.081192875070746e-05,
            "mean_reversion_baseline": -1.261859202395451e-05,
            "momentum_baseline": 1.261859202395451e-05,
            "opportunity_count_matched_random": -2.6427928855801297e-06,
            "session_only_baseline": 7.231278717734197e-05,
            "sign_flipped_signal": 4.194096512539291e-05,
            "volatility_only_baseline": 1.623713786743773e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 142836,
          "real_effect": -4.194096512539291e-05
        },
        "effect_retention_after_best_event_removed": 0.9924597578591488,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -4.194096512539291e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_30_high_v1",
      "confidence_high": -3.249422989404818e-05,
      "confidence_low": -5.130770503204983e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -5.566086522010903e-05,
          "observations": 20234
        },
        "2023Q2": {
          "effect": -0.00011367393313879637,
          "observations": 19598
        },
        "2023Q3": {
          "effect": 4.505567567466063e-05,
          "observations": 20956
        },
        "2023Q4": {
          "effect": -6.599054950203919e-05,
          "observations": 19734
        },
        "2024Q1": {
          "effect": -6.655744374458573e-05,
          "observations": 20501
        },
        "2024Q2": {
          "effect": 1.2155477026306725e-05,
          "observations": 20160
        },
        "2024Q3": {
          "effect": -5.32684168841439e-05,
          "observations": 21654
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.00028367006020109056,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.00024176909273804155,
      "evidence_strength": 8.730539746130006,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "fdr_adjusted_evidence": 2.520289736299443,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -8.420425006321091e-05,
          "observations": 39832
        },
        "2023_h2": {
          "effect": -8.79996963467813e-06,
          "observations": 40690
        },
        "2024_q1": {
          "effect": -6.655744374458573e-05,
          "observations": 20501
        },
        "2024_q2": {
          "effect": 1.2155477026306725e-05,
          "observations": 20160
        },
        "2024_q3": {
          "effect": -5.32684168841439e-05,
          "observations": 21654
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "MNQ": {
          "effect": -4.1900967463049006e-05,
          "observations": 142837
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:45.692994+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.520289736299443,
        "input_hash": "f6dff535da6d700cc525e3736ff0ba739cc531096d947632b6f0d00ca466592c",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -4.1900967463049006e-05,
      "replication": {
        "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_30_high_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.24998118626299679,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00029585569933377284,
      "valid_observations": 142837
    },
    {
      "adversarial": {
        "atom_id": "atom_defensive_portfolio_atom_drawdown_risk_state_NQ_15_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -1.8688421017939354e-05,
            "block_shuffled_signal": -1.8763361344544157e-05,
            "cost_stress": -4.123663865545584e-05,
            "delayed_signal": -1.6902352318703195e-05,
            "event_time_jitter": -1.8730170747305707e-05,
            "mean_reversion_baseline": 9.605434006591976e-06,
            "momentum_baseline": -9.605434006591976e-06,
            "opportunity_count_matched_random": 8.493539610012655e-07,
            "session_only_baseline": 6.107448426730019e-05,
            "sign_flipped_signal": 1.8763361344544157e-05,
            "volatility_only_baseline": -8.458439647304193e-06
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 588828,
          "real_effect": -1.8763361344544157e-05
        },
        "effect_retention_after_best_event_removed": 0.996006028705161,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -1.8763361344544157e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_defensive_portfolio_atom_drawdown_risk_state_NQ_15_moderate_v1",
      "confidence_high": -1.4447727271872749e-05,
      "confidence_low": -2.3078995417215565e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -4.2056422983680564e-05,
          "observations": 110509
        },
        "2023Q2": {
          "effect": -3.654374688829276e-05,
          "observations": 80793
        },
        "2023Q3": {
          "effect": 2.180823625901594e-05,
          "observations": 74051
        },
        "2023Q4": {
          "effect": -4.2421403884434204e-05,
          "observations": 75486
        },
        "2024Q1": {
          "effect": -2.942350952618007e-05,
          "observations": 73084
        },
        "2024Q2": {
          "effect": 1.6153094113150268e-06,
          "observations": 69469
        },
        "2024Q3": {
          "effect": 1.6803927234043068e-06,
          "observations": 105436
        }
      },
      "contracts_positive": 4,
      "cost_hurdle": 0.0018215407356867851,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.001802777374342241,
      "evidence_strength": 8.521618750808925,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "defensive_portfolio_atom",
      "fdr_adjusted_evidence": 2.4599794398554478,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -3.972824220263976e-05,
          "observations": 191302
        },
        "2023_h2": {
          "effect": -1.0614766849702823e-05,
          "observations": 149537
        },
        "2024_q1": {
          "effect": -2.942350952618007e-05,
          "observations": 73084
        },
        "2024_q2": {
          "effect": 1.6153094113150268e-06,
          "observations": 69469
        },
        "2024_q3": {
          "effect": 1.6803927234043068e-06,
          "observations": 105436
        }
      },
      "folds_positive": 3,
      "market_count": 3,
      "market_results": {
        "NQ": {
          "effect": -3.088049529067777e-05,
          "observations": 213896
        },
        "RTY": {
          "effect": -1.2143191755952012e-05,
          "observations": 254633
        },
        "YM": {
          "effect": -1.1231354929775855e-05,
          "observations": 120299
        }
      },
      "markets_positive": 3,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:12.708037+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.4599794398554478,
        "input_hash": "bd36474a4fae2599fcf0a5d43f5c14feab3189686898d388b64a105eaf7b793e",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -1.8763361344544157e-05,
      "replication": {
        "atom_id": "atom_defensive_portfolio_atom_drawdown_risk_state_NQ_15_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 4,
        "cross_market_pass": true,
        "cross_market_required": true,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 3,
        "market_count": 3,
        "markets_positive": 3,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.3499961958744258,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00010979012119632406,
      "valid_observations": 588828
    },
    {
      "adversarial": {
        "atom_id": "atom_session_inventory_acceptance_acceptance_rejection_RTY_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "event_time_jitter",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -0.0001001221033266703,
            "block_shuffled_signal": 6.531773594724173e-05,
            "cost_stress": 4.061061012804738e-05,
            "delayed_signal": -0.0001285997467046082,
            "event_time_jitter": -0.00010255044684629832,
            "mean_reversion_baseline": 4.282528689062007e-05,
            "momentum_baseline": -4.282528689062007e-05,
            "opportunity_count_matched_random": 3.3670501073998244e-06,
            "session_only_baseline": 0.00019570300987883528,
            "sign_flipped_signal": 0.00010061061012804738,
            "volatility_only_baseline": 2.610366458947677e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": true,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 68599,
          "real_effect": -0.00010061061012804738
        },
        "effect_retention_after_best_event_removed": 0.995144579674496,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -0.00010061061012804738,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,session_only_baseline"
      },
      "atom_id": "atom_session_inventory_acceptance_acceptance_rejection_RTY_60_moderate_v1",
      "confidence_high": -7.715399558602204e-05,
      "confidence_low": -0.00012406722467007272,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -0.00020040584819217572,
          "observations": 12663
        },
        "2023Q2": {
          "effect": -0.00029928030664686997,
          "observations": 5803
        },
        "2023Q3": {
          "effect": 0.0003597233176623753,
          "observations": 10794
        },
        "2023Q4": {
          "effect": -0.0006165868564669097,
          "observations": 10689
        },
        "2024Q1": {
          "effect": 0.0002001347028009417,
          "observations": 10737
        },
        "2024Q2": {
          "effect": 1.6445599239919786e-05,
          "observations": 5006
        },
        "2024Q3": {
          "effect": -0.00016662694079850048,
          "observations": 12907
        }
      },
      "contracts_positive": 3,
      "cost_hurdle": 0.004644442150892765,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.004543831540764718,
      "evidence_strength": 8.406873698575346,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "session_inventory_acceptance",
      "fdr_adjusted_evidence": 2.4268553964578303,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -0.00023147746534871156,
          "observations": 18466
        },
        "2023_h2": {
          "effect": -0.00012604586966099323,
          "observations": 21483
        },
        "2024_q1": {
          "effect": 0.0002001347028009417,
          "observations": 10737
        },
        "2024_q2": {
          "effect": 1.6445599239919786e-05,
          "observations": 5006
        },
        "2024_q3": {
          "effect": -0.00016662694079850048,
          "observations": 12907
        }
      },
      "folds_positive": 2,
      "market_count": 1,
      "market_results": {
        "RTY": {
          "effect": -0.00010061061012804738,
          "observations": 68599
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:30.162310+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.4268553964578303,
        "input_hash": "4a00f5994817c4f44cf041579742d70ddc3f3a69b3d43bf3d08df7cec076f1a0",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -0.00010061061012804738,
      "replication": {
        "atom_id": "atom_session_inventory_acceptance_acceptance_rejection_RTY_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 3,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 2,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.1250617569792223,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00032887012086055213,
      "valid_observations": 68599
    },
    {
      "adversarial": {
        "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_30_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -3.392723856807413e-05,
            "block_shuffled_signal": -7.842871422515843e-06,
            "cost_stress": -2.590938417885794e-05,
            "delayed_signal": -3.5009612712870305e-05,
            "event_time_jitter": -3.458832067285012e-05,
            "mean_reversion_baseline": -1.4135408471548965e-05,
            "momentum_baseline": 1.4135408471548965e-05,
            "opportunity_count_matched_random": 4.43266964616384e-06,
            "session_only_baseline": 6.73014022611557e-05,
            "sign_flipped_signal": 3.409061582114206e-05,
            "volatility_only_baseline": 1.5968576229767966e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 256930,
          "real_effect": -3.409061582114206e-05
        },
        "effect_retention_after_best_event_removed": 0.9952075593493208,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -3.409061582114206e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_30_low_v1",
      "confidence_high": -2.6092011471895157e-05,
      "confidence_low": -4.208922017038896e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -7.184735086000711e-05,
          "observations": 46906
        },
        "2023Q2": {
          "effect": -3.5110746213659346e-05,
          "observations": 34702
        },
        "2023Q3": {
          "effect": -7.071457167551454e-06,
          "observations": 37051
        },
        "2023Q4": {
          "effect": -2.137884397717267e-05,
          "observations": 32443
        },
        "2024Q1": {
          "effect": -6.278282220549706e-06,
          "observations": 32701
        },
        "2024Q2": {
          "effect": -8.48182587608616e-06,
          "observations": 29939
        },
        "2024Q3": {
          "effect": -6.380418361386103e-05,
          "observations": 43188
        }
      },
      "contracts_positive": 0,
      "cost_hurdle": 0.0005801959773078907,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.0005461053614867486,
      "evidence_strength": 8.353658224853884,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "session_transition_state",
      "fdr_adjusted_evidence": 2.411493412418761,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -5.62259209213055e-05,
          "observations": 81608
        },
        "2023_h2": {
          "effect": -1.3750804309240538e-05,
          "observations": 69494
        },
        "2024_q1": {
          "effect": -6.278282220549706e-06,
          "observations": 32701
        },
        "2024_q2": {
          "effect": -8.48182587608616e-06,
          "observations": 29939
        },
        "2024_q3": {
          "effect": -6.380418361386103e-05,
          "observations": 43188
        }
      },
      "folds_positive": 0,
      "market_count": 1,
      "market_results": {
        "NQ": {
          "effect": -3.409061582114206e-05,
          "observations": 256930
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:28.973974+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.411493412418761,
        "input_hash": "650036190bf3d6bb21f2a8273151c9ab63daf794ead7111960eeb97ca3544ab2",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -3.409061582114206e-05,
      "replication": {
        "atom_id": "atom_session_transition_state_mid_to_late_pressure_decay_NQ_30_low_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 0,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 0,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.449952365607969,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00016540590320301407,
      "valid_observations": 256930
    },
    {
      "adversarial": {
        "atom_id": "atom_session_transition_state_transition_participation_shift_GC_60_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 0.0032395606741126167,
            "block_shuffled_signal": 0.003270819091230077,
            "cost_stress": 0.002616655272984062,
            "delayed_signal": 0.004192531591704871,
            "event_time_jitter": 0.0032857446894790977,
            "mean_reversion_baseline": -0.0025341221981273403,
            "momentum_baseline": 0.0025341221981273403,
            "opportunity_count_matched_random": 0.00013503942489971832,
            "session_only_baseline": 0.004463198649149989,
            "sign_flipped_signal": -0.003270819091230077,
            "volatility_only_baseline": 0.0007745634782991972
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": true,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 1980,
          "real_effect": 0.003270819091230077
        },
        "effect_retention_after_best_event_removed": 0.9904432448736549,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 0.003270819091230077,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,session_only_baseline"
      },
      "atom_id": "atom_session_transition_state_transition_participation_shift_GC_60_moderate_v1",
      "confidence_high": 0.004094064663135751,
      "confidence_low": 0.0024856732524247806,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.0025333594151265033,
          "observations": 245
        },
        "2023Q2": {
          "effect": -0.006642704072685152,
          "observations": 403
        },
        "2023Q3": {
          "effect": -0.0037837419613647783,
          "observations": 358
        },
        "2023Q4": {
          "effect": 0.01044883617335758,
          "observations": 445
        },
        "2024Q1": {
          "effect": 0.018192224943511734,
          "observations": 219
        },
        "2024Q2": {
          "effect": 0.0015507333656670141,
          "observations": 161
        },
        "2024Q3": {
          "effect": 0.006964328536478931,
          "observations": 150
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.004547751389590703,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.001257882431810437,
      "evidence_strength": 8.018126824488569,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "session_transition_state",
      "fdr_adjusted_evidence": 2.3146338402575175,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -0.0031733590811514256,
          "observations": 648
        },
        "2023_h2": {
          "effect": 0.004103552272696802,
          "observations": 803
        },
        "2024_q1": {
          "effect": 0.018192224943511734,
          "observations": 219
        },
        "2024_q2": {
          "effect": 0.0015507333656670141,
          "observations": 161
        },
        "2024_q3": {
          "effect": 0.006964328536478931,
          "observations": 150
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "GC": {
          "effect": 0.0032898689577802657,
          "observations": 1981
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:34.866017+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.3146338402575175,
        "input_hash": "d4e4c71766134e1a187c76feaff8e4f83613c06d2945408d938009a08c0bdce1",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 0.0032898689577802657,
      "replication": {
        "atom_id": "atom_session_transition_state_transition_participation_shift_GC_60_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.35186500888099465,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.003968647674524561,
      "valid_observations": 1981
    },
    {
      "adversarial": {
        "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_15_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -1.9209882063145528e-05,
            "block_shuffled_signal": -1.937640135169808e-05,
            "cost_stress": -4.062359864830192e-05,
            "delayed_signal": -1.2542680821461132e-05,
            "event_time_jitter": -1.8822068951434826e-05,
            "mean_reversion_baseline": 2.9367118015812824e-06,
            "momentum_baseline": -2.9367118015812824e-06,
            "opportunity_count_matched_random": 1.077633285939504e-06,
            "session_only_baseline": 3.225905943426749e-05,
            "sign_flipped_signal": 1.937640135169808e-05,
            "volatility_only_baseline": -2.2862382875381284e-06
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 257114,
          "real_effect": -1.937640135169808e-05
        },
        "effect_retention_after_best_event_removed": 0.9914060776544578,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -1.937640135169808e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_15_low_v1",
      "confidence_high": -1.4542182387437991e-05,
      "confidence_low": -2.4167055666650452e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -2.208193882444169e-05,
          "observations": 36687
        },
        "2023Q2": {
          "effect": -5.410700206923569e-05,
          "observations": 35930
        },
        "2023Q3": {
          "effect": 2.229614247960471e-05,
          "observations": 37561
        },
        "2023Q4": {
          "effect": -2.4568105968951827e-05,
          "observations": 35654
        },
        "2024Q1": {
          "effect": -2.2567315562623696e-05,
          "observations": 36490
        },
        "2024Q2": {
          "effect": -2.139309930857363e-05,
          "observations": 36442
        },
        "2024Q3": {
          "effect": -1.5139205880284465e-05,
          "observations": 38351
        }
      },
      "contracts_positive": 6,
      "cost_hurdle": 0.0002840326321935209,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.00026467801316647666,
      "evidence_strength": 7.882712258651294,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "effort_vs_progress",
      "fdr_adjusted_evidence": 2.275543022238344,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -3.792754691050209e-05,
          "observations": 72617
        },
        "2023_h2": {
          "effect": -5.256551600160602e-07,
          "observations": 73215
        },
        "2024_q1": {
          "effect": -2.2567315562623696e-05,
          "observations": 36490
        },
        "2024_q2": {
          "effect": -2.139309930857363e-05,
          "observations": 36442
        },
        "2024_q3": {
          "effect": -1.5139205880284465e-05,
          "observations": 38351
        }
      },
      "folds_positive": 5,
      "market_count": 1,
      "market_results": {
        "MNQ": {
          "effect": -1.9354619027044222e-05,
          "observations": 257115
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:09.009004+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.275543022238344,
        "input_hash": "3fc29429acef220258c9ec3eedcf6a03228bc24b0c58cad4cfe5c20fc73483a2",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -1.9354619027044222e-05,
      "replication": {
        "atom_id": "atom_effort_vs_progress_effort_progress_ratio_MNQ_15_low_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 6,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 5,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.44996902377643916,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0001759568602039719,
      "valid_observations": 257115
    },
    {
      "adversarial": {
        "atom_id": "atom_volatility_path_shape_failed_expansion_NQ_30_extreme_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -4.964302845742033e-05,
            "block_shuffled_signal": -5.016936702013783e-05,
            "cost_stress": -9.830632979862168e-06,
            "delayed_signal": -3.659009656675631e-05,
            "event_time_jitter": -5.020491021020497e-05,
            "mean_reversion_baseline": 4.1700351981340155e-07,
            "momentum_baseline": -4.1700351981340155e-07,
            "opportunity_count_matched_random": -8.720598598210111e-06,
            "session_only_baseline": 6.338865489159422e-05,
            "sign_flipped_signal": 5.016936702013783e-05,
            "volatility_only_baseline": 1.5968576229767966e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 85218,
          "real_effect": -5.016936702013783e-05
        },
        "effect_retention_after_best_event_removed": 0.989508766126026,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -5.016936702013783e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_volatility_path_shape_failed_expansion_NQ_30_extreme_v1",
      "confidence_high": -3.7352346151262504e-05,
      "confidence_low": -6.285369479794211e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -0.00011984058419578209,
          "observations": 11955
        },
        "2023Q2": {
          "effect": -5.671212049638181e-05,
          "observations": 11965
        },
        "2023Q3": {
          "effect": -2.0289162878710893e-05,
          "observations": 11855
        },
        "2023Q4": {
          "effect": -5.651132797566234e-05,
          "observations": 12884
        },
        "2024Q1": {
          "effect": -2.230679769932488e-06,
          "observations": 11342
        },
        "2024Q2": {
          "effect": -5.391353272854259e-05,
          "observations": 12894
        },
        "2024Q3": {
          "effect": -3.808772797041526e-05,
          "observations": 12324
        }
      },
      "contracts_positive": 0,
      "cost_hurdle": 0.0005684150693150598,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.0005183120488404576,
      "evidence_strength": 7.701704054229058,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "volatility_path_shape",
      "fdr_adjusted_evidence": 2.2232904544639895,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -8.826315659698091e-05,
          "observations": 23920
        },
        "2023_h2": {
          "effect": -3.9153562212116545e-05,
          "observations": 24739
        },
        "2024_q1": {
          "effect": -2.230679769932488e-06,
          "observations": 11342
        },
        "2024_q2": {
          "effect": -5.391353272854259e-05,
          "observations": 12894
        },
        "2024_q3": {
          "effect": -3.808772797041526e-05,
          "observations": 12324
        }
      },
      "folds_positive": 0,
      "market_count": 1,
      "market_results": {
        "NQ": {
          "effect": -5.010302047460231e-05,
          "observations": 85219
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:27.604709+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.2232904544639895,
        "input_hash": "174ec661e8f7ef4cad5cfeabb84b9cbadbd2b02286bc60ee10e6bd3b9bd1cc92",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -5.010302047460231e-05,
      "replication": {
        "atom_id": "atom_volatility_path_shape_failed_expansion_NQ_30_extreme_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 0,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 0,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.14924100200344648,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0003788126919321076,
      "valid_observations": 85219
    },
    {
      "adversarial": {
        "atom_id": "atom_session_inventory_acceptance_prior_value_distance_NQ_15_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -1.9588734790509718e-05,
            "block_shuffled_signal": 4.058703161600974e-06,
            "cost_stress": -4.0227513919632625e-05,
            "delayed_signal": -1.5145739294017851e-05,
            "event_time_jitter": -1.956904021532647e-05,
            "mean_reversion_baseline": 4.506395232499096e-06,
            "momentum_baseline": -4.506395232499096e-06,
            "opportunity_count_matched_random": 1.5780985349424031e-07,
            "session_only_baseline": 4.134778355783882e-05,
            "sign_flipped_signal": 1.9772486080367376e-05,
            "volatility_only_baseline": -2.4441950415958842e-06
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 199844,
          "real_effect": -1.9772486080367376e-05
        },
        "effect_retention_after_best_event_removed": 0.9907067179559119,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -1.9772486080367376e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:cost_stress,session_only_baseline"
      },
      "atom_id": "atom_session_inventory_acceptance_prior_value_distance_NQ_15_moderate_v1",
      "confidence_high": -1.4634559303555736e-05,
      "confidence_low": -2.4854583801605294e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -2.2049554481743553e-07,
          "observations": 30052
        },
        "2023Q2": {
          "effect": -6.151602651742622e-07,
          "observations": 28176
        },
        "2023Q3": {
          "effect": -2.6193588338632273e-06,
          "observations": 31092
        },
        "2023Q4": {
          "effect": -2.0502139932039197e-05,
          "observations": 25359
        },
        "2024Q1": {
          "effect": -1.7121711333339827e-05,
          "observations": 28873
        },
        "2024Q2": {
          "effect": -1.9728230920912683e-05,
          "observations": 24888
        },
        "2024Q3": {
          "effect": -7.435722210777888e-05,
          "observations": 31405
        }
      },
      "contracts_positive": 0,
      "cost_hurdle": 0.0005694670737301675,
      "direction_ok": false,
      "effect_after_cost_hurdle": -0.000549722502177587,
      "evidence_strength": 7.573242167950458,
      "failure_reason": "effect_direction_opposes_preregistered_direction",
      "family": "session_inventory_acceptance",
      "fdr_adjusted_evidence": 2.186206702152211,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -4.114702161228891e-07,
          "observations": 58228
        },
        "2023_h2": {
          "effect": -1.065268766539224e-05,
          "observations": 56451
        },
        "2024_q1": {
          "effect": -1.7121711333339827e-05,
          "observations": 28873
        },
        "2024_q2": {
          "effect": -1.9728230920912683e-05,
          "observations": 24888
        },
        "2024_q3": {
          "effect": -7.435722210777888e-05,
          "observations": 31405
        }
      },
      "folds_positive": 0,
      "market_count": 1,
      "market_results": {
        "NQ": {
          "effect": -1.9744571552580515e-05,
          "observations": 199845
        }
      },
      "markets_positive": 0,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:44.350665+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.186206702152211,
        "input_hash": "6992ef52a34a7c6ca94bbc7ab54c7f6c5206d22816130688ebc45ca0d65cc7b1",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -1.9744571552580515e-05,
      "replication": {
        "atom_id": "atom_session_inventory_acceptance_prior_value_distance_NQ_15_moderate_v1",
        "contract_count": 7,
        "contract_pass": false,
        "contracts_positive": 0,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "temporal_replication_failed",
        "fold_count": 5,
        "folds_positive": 0,
        "market_count": 1,
        "markets_positive": 0,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": false
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.3499722431881982,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00037291152197801075,
      "valid_observations": 199845
    },
    {
      "adversarial": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_ES_30_moderate_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 2.1280365592670113e-05,
            "block_shuffled_signal": 8.684993243469214e-06,
            "cost_stress": -3.8592894573688366e-05,
            "delayed_signal": 1.680982000038368e-05,
            "event_time_jitter": 2.1660070194929184e-05,
            "mean_reversion_baseline": -1.7364661927835886e-05,
            "momentum_baseline": 1.7364661927835886e-05,
            "opportunity_count_matched_random": 1.9792411781377895e-06,
            "session_only_baseline": 4.14198564545632e-05,
            "sign_flipped_signal": -2.1407105426311636e-05,
            "volatility_only_baseline": 1.7205700941788202e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 193131,
          "real_effect": 2.1407105426311636e-05
        },
        "effect_retention_after_best_event_removed": 0.9940795436320061,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 2.1407105426311636e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_accepted_price_migration_extreme_dwell_ES_30_moderate_v1",
      "confidence_high": 2.7145284237507146e-05,
      "confidence_low": 1.5668926615116125e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.00011091186904215251,
          "observations": 27179
        },
        "2023Q2": {
          "effect": 4.3855758231673716e-05,
          "observations": 25158
        },
        "2023Q3": {
          "effect": 1.4985382246069998e-05,
          "observations": 29490
        },
        "2023Q4": {
          "effect": -3.1120854438560298e-06,
          "observations": 27646
        },
        "2024Q1": {
          "effect": -2.079715865570566e-05,
          "observations": 26253
        },
        "2024Q2": {
          "effect": 4.499551231419956e-06,
          "observations": 27670
        },
        "2024Q3": {
          "effect": 2.7638082784251532e-06,
          "observations": 29735
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.001972278529556785,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0019508714241304737,
      "evidence_strength": 7.312063289786041,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "accepted_price_migration",
      "fdr_adjusted_evidence": 2.1108108543447757,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 7.867850381735884e-05,
          "observations": 52337
        },
        "2023_h2": {
          "effect": 6.2286860868062245e-06,
          "observations": 57136
        },
        "2024_q1": {
          "effect": -2.079715865570566e-05,
          "observations": 26253
        },
        "2024_q2": {
          "effect": 4.499551231419956e-06,
          "observations": 27670
        },
        "2024_q3": {
          "effect": 2.7638082784251532e-06,
          "observations": 29735
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "ES": {
          "effect": 2.1407105426311636e-05,
          "observations": 193131
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:12:17.260636+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.1108108543447757,
        "input_hash": "564f70bd866b5b2df26891717776a46d4950d94ca538fca056323173a66f1fdd",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 2.1407105426311636e-05,
      "replication": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_ES_30_moderate_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.3383769274840256,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.00030972042985607785,
      "valid_observations": 193131
    },
    {
      "adversarial": {
        "atom_id": "atom_session_inventory_acceptance_overnight_displacement_MYM_60_extreme_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 5.4488451113146876e-05,
            "block_shuffled_signal": -1.0076908821154231e-05,
            "cost_stress": -5.29313365408293e-06,
            "delayed_signal": 9.662388898705361e-05,
            "event_time_jitter": 5.5501379057796364e-05,
            "mean_reversion_baseline": -3.2270079733398894e-05,
            "momentum_baseline": 3.2270079733398894e-05,
            "opportunity_count_matched_random": -2.0575957601806733e-06,
            "session_only_baseline": 0.00037019112038475833,
            "sign_flipped_signal": -5.470686634591707e-05,
            "volatility_only_baseline": 2.0977682445187423e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 82050,
          "real_effect": 5.470686634591707e-05
        },
        "effect_retention_after_best_event_removed": 0.9960075352993327,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 5.470686634591707e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_session_inventory_acceptance_overnight_displacement_MYM_60_extreme_v1",
      "confidence_high": 6.959978455045987e-05,
      "confidence_low": 3.981394814137427e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 0.0002889675498993942,
          "observations": 15793
        },
        "2023Q2": {
          "effect": -4.4815381967056e-05,
          "observations": 11004
        },
        "2023Q3": {
          "effect": 0.00013097879873929074,
          "observations": 10814
        },
        "2023Q4": {
          "effect": -0.00013531012430859234,
          "observations": 10020
        },
        "2024Q1": {
          "effect": 9.091504241771655e-05,
          "observations": 7009
        },
        "2024Q2": {
          "effect": 4.812121602849986e-06,
          "observations": 11527
        },
        "2024Q3": {
          "effect": -2.109852110618739e-05,
          "observations": 15883
        }
      },
      "contracts_positive": 4,
      "cost_hurdle": 0.00012887705129306643,
      "direction_ok": true,
      "effect_after_cost_hurdle": -7.417018494714935e-05,
      "evidence_strength": 7.199761427904061,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "session_inventory_acceptance",
      "fdr_adjusted_evidence": 2.078392099250747,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 0.00015190193127572664,
          "observations": 26797
        },
        "2023_h2": {
          "effect": 2.908576557290709e-06,
          "observations": 20834
        },
        "2024_q1": {
          "effect": 9.091504241771655e-05,
          "observations": 7009
        },
        "2024_q2": {
          "effect": 4.812121602849986e-06,
          "observations": 11527
        },
        "2024_q3": {
          "effect": -2.109852110618739e-05,
          "observations": 15883
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "MYM": {
          "effect": 5.470686634591707e-05,
          "observations": 82050
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:13:55.409692+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.078392099250747,
        "input_hash": "e694abb9486226fcf30a86363a624bf1425d38dfeabf96f4746bf37e9759cbab",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 5.470686634591707e-05,
      "replication": {
        "atom_id": "atom_session_inventory_acceptance_overnight_displacement_MYM_60_extreme_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 4,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.15001755232787747,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.000285518070469678,
      "valid_observations": 82050
    },
    {
      "adversarial": {
        "atom_id": "atom_distribution_tail_state_downside_upside_variance_ratio_ES_15_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "block_shuffled_signal",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "delayed_signal",
          "sign_flipped_signal",
          "event_time_jitter",
          "best_event_removed",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,momentum_baseline,mean_reversion_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": -1.256846990937956e-05,
            "block_shuffled_signal": -1.2674081760826255e-05,
            "cost_stress": -4.732591823917375e-05,
            "delayed_signal": -4.868289655565273e-06,
            "event_time_jitter": -1.2331241679322738e-05,
            "mean_reversion_baseline": 1.3808707500182828e-05,
            "momentum_baseline": -1.3808707500182828e-05,
            "opportunity_count_matched_random": 6.875998772814859e-07,
            "session_only_baseline": 2.5690423082770295e-05,
            "sign_flipped_signal": 1.2674081760826255e-05,
            "volatility_only_baseline": -1.2297489993530138e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": true,
            "event_time_jitter": true,
            "mean_reversion_baseline": false,
            "momentum_baseline": false,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 256834,
          "real_effect": -1.2674081760826255e-05
        },
        "effect_retention_after_best_event_removed": 0.9916671003517489,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": -1.2674081760826255e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:block_shuffled_signal,cost_stress,momentum_baseline,mean_reversion_baseline"
      },
      "atom_id": "atom_distribution_tail_state_downside_upside_variance_ratio_ES_15_low_v1",
      "confidence_high": -9.145344277052431e-06,
      "confidence_low": -1.6202819244600078e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": -1.1378203640500782e-05,
          "observations": 37003
        },
        "2023Q2": {
          "effect": -2.0243125543320425e-05,
          "observations": 37908
        },
        "2023Q3": {
          "effect": -1.3530417161958898e-06,
          "observations": 37767
        },
        "2023Q4": {
          "effect": -1.7007982319330755e-05,
          "observations": 36516
        },
        "2024Q1": {
          "effect": -2.5003055213101334e-05,
          "observations": 35232
        },
        "2024Q2": {
          "effect": -1.103964993465296e-05,
          "observations": 36558
        },
        "2024Q3": {
          "effect": -3.0703506306033023e-06,
          "observations": 35850
        }
      },
      "contracts_positive": 7,
      "cost_hurdle": 0.0019783480793537397,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0019656739975929135,
      "evidence_strength": 7.039684976693967,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "distribution_tail_state",
      "fdr_adjusted_evidence": 2.0321820081522133,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": -1.5864213165031053e-05,
          "observations": 74911
        },
        "2023_h2": {
          "effect": -9.048689590730745e-06,
          "observations": 74283
        },
        "2024_q1": {
          "effect": -2.5003055213101334e-05,
          "observations": 35232
        },
        "2024_q2": {
          "effect": -1.103964993465296e-05,
          "observations": 36558
        },
        "2024_q3": {
          "effect": -3.0703506306033023e-06,
          "observations": 35850
        }
      },
      "folds_positive": 5,
      "market_count": 1,
      "market_results": {
        "ES": {
          "effect": -1.2674081760826255e-05,
          "observations": 256834
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:18.444750+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 2.0321820081522133,
        "input_hash": "e0bd2e52bb121ff9f236b0d01f39ef358abc3f1b3e7f75dbe0641386ac6d4533",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": -1.2674081760826255e-05,
      "replication": {
        "atom_id": "atom_distribution_tail_state_downside_upside_variance_ratio_ES_15_low_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 7,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 5,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.44997652302495567,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0001520235913630121,
      "valid_observations": 256834
    },
    {
      "adversarial": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_NQ_15_extreme_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "block_shuffled_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline",
        "details": {
          "attack_effects": {
            "best_event_removed": 3.112392464083016e-05,
            "block_shuffled_signal": 1.6102379858497838e-07,
            "cost_stress": -2.8561923371852713e-05,
            "delayed_signal": 3.5266862750364976e-05,
            "event_time_jitter": 3.166009710583473e-05,
            "mean_reversion_baseline": 2.39283271854753e-06,
            "momentum_baseline": -2.39283271854753e-06,
            "opportunity_count_matched_random": 6.395870615777634e-06,
            "session_only_baseline": 6.172509227818943e-05,
            "sign_flipped_signal": -3.143807662814729e-05,
            "volatility_only_baseline": -2.4441950415958842e-06
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": true,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 79601,
          "real_effect": 3.143807662814729e-05
        },
        "effect_retention_after_best_event_removed": 0.990007277129802,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 3.143807662814729e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,event_time_jitter,cost_stress,session_only_baseline"
      },
      "atom_id": "atom_accepted_price_migration_extreme_dwell_NQ_15_extreme_v1",
      "confidence_high": 4.0334779693469006e-05,
      "confidence_low": 2.2541373562825574e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 9.466076480364506e-05,
          "observations": 11329
        },
        "2023Q2": {
          "effect": 7.440654543862365e-05,
          "observations": 10025
        },
        "2023Q3": {
          "effect": 1.1448758586659944e-05,
          "observations": 11914
        },
        "2023Q4": {
          "effect": 4.269692467520973e-05,
          "observations": 11715
        },
        "2024Q1": {
          "effect": 2.525156650726314e-05,
          "observations": 11529
        },
        "2024Q2": {
          "effect": 2.1118728816385386e-05,
          "observations": 10781
        },
        "2024Q3": {
          "effect": -3.828699974071991e-05,
          "observations": 12308
        }
      },
      "contracts_positive": 6,
      "cost_hurdle": 0.0005670094974090816,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.0005355714207809344,
      "evidence_strength": 6.926007279185336,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "accepted_price_migration",
      "fdr_adjusted_evidence": 1.9993660835234808,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 8.515207560563345e-05,
          "observations": 21354
        },
        "2023_h2": {
          "effect": 2.6941257876827143e-05,
          "observations": 23629
        },
        "2024_q1": {
          "effect": 2.525156650726314e-05,
          "observations": 11529
        },
        "2024_q2": {
          "effect": 2.1118728816385386e-05,
          "observations": 10781
        },
        "2024_q3": {
          "effect": -3.828699974071991e-05,
          "observations": 12308
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "NQ": {
          "effect": 3.143807662814729e-05,
          "observations": 79601
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:10.351580+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 1.9993660835234808,
        "input_hash": "7de625523472c72eb62f96580d13fe822dd9d6bf20f0adfa6f38dae3a012df44",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 3.143807662814729e-05,
      "replication": {
        "atom_id": "atom_accepted_price_migration_extreme_dwell_NQ_15_extreme_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 6,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.1393987366710389,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 0.0007630163234653071,
      "valid_observations": 79601
    },
    {
      "adversarial": {
        "atom_id": "atom_session_transition_state_transition_participation_shift_MYM_60_low_v1",
        "attacks_attempted": [
          "delayed_signal",
          "sign_flipped_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "best_event_removed",
          "cost_stress",
          "momentum_baseline",
          "mean_reversion_baseline",
          "session_only_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "attacks_failed": [
          "delayed_signal",
          "block_shuffled_signal",
          "event_time_jitter",
          "cost_stress",
          "session_only_baseline"
        ],
        "attacks_survived": [
          "sign_flipped_signal",
          "best_event_removed",
          "momentum_baseline",
          "mean_reversion_baseline",
          "volatility_only_baseline",
          "opportunity_count_matched_random"
        ],
        "decision_reason": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress",
        "details": {
          "attack_effects": {
            "best_event_removed": 2.869844320653829e-05,
            "block_shuffled_signal": 2.877138535787364e-05,
            "cost_stress": -3.122861464212636e-05,
            "delayed_signal": 3.473494472594129e-05,
            "event_time_jitter": 2.887063646045688e-05,
            "mean_reversion_baseline": -9.946385108595818e-06,
            "momentum_baseline": 9.946385108595818e-06,
            "opportunity_count_matched_random": 1.1148041340292661e-06,
            "session_only_baseline": 0.00017276519971566508,
            "sign_flipped_signal": -2.877138535787364e-05,
            "volatility_only_baseline": 2.0977682445187423e-05
          },
          "attack_passes": {
            "best_event_removed": "True",
            "block_shuffled_signal": false,
            "cost_stress": false,
            "delayed_signal": false,
            "event_time_jitter": false,
            "mean_reversion_baseline": true,
            "momentum_baseline": true,
            "opportunity_count_matched_random": true,
            "session_only_baseline": false,
            "sign_flipped_signal": true,
            "volatility_only_baseline": true
          },
          "event_count": 246041,
          "real_effect": 2.877138535787364e-05
        },
        "effect_retention_after_best_event_removed": 0.9974647674963142,
        "passed": false,
        "policy_version": "atom_adversarial_policy_v1",
        "real_effect": 2.877138535787364e-05,
        "simplest_competing_explanation": "mandatory_attack_explains_or_matches_effect:delayed_signal,block_shuffled_signal,event_time_jitter,cost_stress"
      },
      "atom_id": "atom_session_transition_state_transition_participation_shift_MYM_60_low_v1",
      "confidence_high": 3.6791299403168816e-05,
      "confidence_low": 2.0321686215243195e-05,
      "contract_count": 7,
      "contract_results": {
        "2023Q1": {
          "effect": 1.3012786441199591e-05,
          "observations": 46543
        },
        "2023Q2": {
          "effect": 5.481147404765737e-05,
          "observations": 36449
        },
        "2023Q3": {
          "effect": -3.593328626107685e-05,
          "observations": 32408
        },
        "2023Q4": {
          "effect": 0.00014988552244807083,
          "observations": 31629
        },
        "2024Q1": {
          "effect": 6.121831147161668e-05,
          "observations": 31324
        },
        "2024Q2": {
          "effect": -4.813163258413772e-05,
          "observations": 33595
        },
        "2024Q3": {
          "effect": 1.6023257934767317e-05,
          "observations": 34134
        }
      },
      "contracts_positive": 5,
      "cost_hurdle": 0.00012865597392572263,
      "direction_ok": true,
      "effect_after_cost_hurdle": -0.00010009948111651663,
      "evidence_strength": 6.796847657245239,
      "failure_reason": "effect_below_cost_or_minimum_effect",
      "family": "session_transition_state",
      "fdr_adjusted_evidence": 1.9620809122757081,
      "fold_count": 5,
      "fold_results": {
        "2023_h1": {
          "effect": 3.137022287564845e-05,
          "observations": 82992
        },
        "2023_h2": {
          "effect": 5.58458898505716e-05,
          "observations": 64037
        },
        "2024_q1": {
          "effect": 6.121831147161668e-05,
          "observations": 31324
        },
        "2024_q2": {
          "effect": -4.813163258413772e-05,
          "observations": 33595
        },
        "2024_q3": {
          "effect": 1.6023257934767317e-05,
          "observations": 34134
        }
      },
      "folds_positive": 4,
      "market_count": 1,
      "market_results": {
        "MYM": {
          "effect": 2.8556492809206006e-05,
          "observations": 246082
        }
      },
      "markets_positive": 1,
      "provenance": {
        "code_commit": "579ad08b621232ca62bc4a022757a6abb8a5de56",
        "computation_mode": "FULL",
        "computed_at_utc": "2026-07-10T10:14:45.719822+00:00",
        "data_fingerprint": "0b37dd801adc83e7fe115f0e265969efeabfbe7a0c683636d9dc2d5250b90f32",
        "evidence_strength": 1.9620809122757081,
        "input_hash": "3abc5c332b67fa759455c2ad9a054d0a1ce1e400ef3f2f5ac3fdba8d2f74e220",
        "passed": false,
        "policy_version": "edge_atom_policy_v1",
        "scope": "EDGE_ATOM",
        "status": "ATOM_FALSIFIED",
        "validation_version": "edge_atom_tester_v1"
      },
      "raw_effect": 2.8556492809206006e-05,
      "replication": {
        "atom_id": "atom_session_transition_state_transition_participation_shift_MYM_60_low_v1",
        "contract_count": 7,
        "contract_pass": true,
        "contracts_positive": 5,
        "cross_market_pass": true,
        "cross_market_required": false,
        "decision_reason": "replication_policy_passed",
        "fold_count": 5,
        "folds_positive": 4,
        "market_count": 1,
        "markets_positive": 1,
        "policy_version": "atom_replication_policy_v1",
        "temporal_pass": true
      },
      "simplest_competing_explanation": "unresolved_until_adversarial_validation",
      "state_frequency": 0.4499283279945003,
      "status": "ATOM_FALSIFIED",
      "top_event_concentration": 9.743646352154175e-05,
      "valid_observations": 246082
    }
  ],
  "topstep_compatible_strategies": 0,
  "topstep_path_candidates": 0,
  "warning": "Historical research only. No live trading approval. Q4 remains sealed and was not accessed.",
  "workers_requested": 3
}
```
