# Strict Unsampled Temporal Transfer Replay strict_unsampled_temporal_transfer_v1

Historical research only. Q4 remains sealed. No live trading approval.

- Baseline commit: 7a05294ff2d2fd2154ec7e4e470b80a7cac4fdac
- Q4 seal: PASSED_NO_Q4_ACCESS
- Frozen candidates: 12
- Candidate-level null passes: 0
- Topstep replay count: 0
- Future Q4 freeze recommendations: 0

```json
{
  "baseline_commit": "7a05294ff2d2fd2154ec7e4e470b80a7cac4fdac",
  "candidate_level_matched_null_passes": 0,
  "candidate_results": [
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
        "components": [
          "accepted_center_migration",
          "time_at_extremes"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "RAW_POSITIVE_NET",
          "RAW_ECONOMIC_SCREEN_PASS",
          "MATCHED_NULL_BEATEN",
          "REPRESENTATION_EVIDENCE_PASS",
          "TOPSTEP_PATH_CANDIDATE",
          "TOPSTEP_COMPATIBLE"
        ],
        "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_06",
        "symbol": "NQ",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "time_at_extremes"
        ],
        "full_pooled_net_pnl": -1190.0,
        "harmful_components": [
          "accepted_center_migration"
        ],
        "minimal_stable_formulation": [
          "time_at_extremes"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "HARMFUL_COMPONENT",
            "component": "accepted_center_migration",
            "full_minus_removed": -1990.0,
            "removed_net_pnl": 800.0
          },
          {
            "classification": "ESSENTIAL_COMPONENT",
            "component": "time_at_extremes",
            "full_minus_removed": 7820.0,
            "removed_net_pnl": -9010.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -5.427124485521969e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.723333,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": -0.00026727137850238034,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
          "effect_size": 0.00015774424322861433,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -210.0,
          "average_win": 256.57142857142856,
          "best_day": 287.0,
          "best_trade": 1431.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": -470.0
          },
          "executed_trades": 80,
          "expectancy": -5.875,
          "gross_pnl": 250.0,
          "losing_streak": 12,
          "max_adverse_excursion": -1225.0,
          "max_drawdown": 3432.0,
          "max_favorable_excursion": 1510.0,
          "net_pnl": -470.0,
          "opportunities": 2893,
          "period": "q1",
          "profit_factor": 0.9502645502645503,
          "regime_contribution": {
            "high_vol": 1643.0,
            "normal_vol": -2113.0
          },
          "roll_window_contribution": {
            "normal": -470.0
          },
          "session_contribution": {
            "0": -160.0,
            "1": -491.0,
            "10": 883.0,
            "11": -453.0,
            "12": 947.0,
            "13": -324.0,
            "14": 1049.0,
            "15": 2038.0,
            "16": -367.0,
            "17": -649.0,
            "18": -743.0,
            "19": -824.0,
            "2": 171.0,
            "20": 64.0,
            "21": -487.0,
            "23": -767.0,
            "3": 126.0,
            "4": -187.0,
            "5": -44.0,
            "6": -9.0,
            "7": 86.0,
            "8": -475.0,
            "9": 146.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.1593541202672606,
          "top_3_trade_profit_pct": 0.3216035634743875,
          "top_5_trade_profit_pct": 0.42706013363028955,
          "trade_count": 80,
          "win_rate": 0.4375,
          "worst_day": -338.0,
          "worst_trade": -824.0
        },
        "q2": {
          "average_loss": -194.0,
          "average_win": 157.5625,
          "best_day": 1316.0,
          "best_trade": 531.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": 1355.0
          },
          "executed_trades": 80,
          "expectancy": 16.9375,
          "gross_pnl": 2075.0,
          "losing_streak": 5,
          "max_adverse_excursion": -760.0,
          "max_drawdown": 2108.0,
          "max_favorable_excursion": 710.0,
          "net_pnl": 1355.0,
          "opportunities": 2965,
          "period": "q2",
          "profit_factor": 1.2182667525773196,
          "regime_contribution": {
            "high_vol": 1822.0,
            "normal_vol": -467.0
          },
          "roll_window_contribution": {
            "normal": 1355.0
          },
          "session_contribution": {
            "0": 768.0,
            "1": -418.0,
            "10": -298.0,
            "11": -9.0,
            "13": 67.0,
            "14": 552.0,
            "15": 331.0,
            "16": -122.0,
            "17": -119.0,
            "18": -961.0,
            "19": 1015.0,
            "2": 15.0,
            "20": -279.0,
            "22": 141.0,
            "23": 918.0,
            "3": 189.0,
            "4": 153.0,
            "5": 169.0,
            "6": -451.0,
            "7": -533.0,
            "8": -557.0,
            "9": 784.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.07021023403411344,
          "top_3_trade_profit_pct": 0.19079730265767553,
          "top_5_trade_profit_pct": 0.2895676318921063,
          "trade_count": 80,
          "win_rate": 0.6,
          "worst_day": -458.0,
          "worst_trade": -519.0
        },
        "q3": {
          "average_loss": -275.16279069767444,
          "average_win": 263.7027027027027,
          "best_day": 341.0,
          "best_trade": 826.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": -2075.0
          },
          "executed_trades": 80,
          "expectancy": -25.9375,
          "gross_pnl": -1355.0,
          "losing_streak": 5,
          "max_adverse_excursion": -1110.0,
          "max_drawdown": 4671.0,
          "max_favorable_excursion": 1235.0,
          "net_pnl": -2075.0,
          "opportunities": 2994,
          "period": "q3",
          "profit_factor": 0.8246281271129141,
          "regime_contribution": {
            "high_vol": -647.0,
            "normal_vol": -1428.0
          },
          "roll_window_contribution": {
            "normal": -2075.0
          },
          "session_contribution": {
            "0": 918.0,
            "1": -258.0,
            "10": -823.0,
            "12": -652.0,
            "13": 496.0,
            "15": -647.0,
            "17": -1467.0,
            "18": -70.0,
            "19": 939.0,
            "2": 138.0,
            "20": -89.0,
            "22": 91.0,
            "3": -215.0,
            "4": 184.0,
            "5": 487.0,
            "6": 433.0,
            "7": -1690.0,
            "8": 1130.0,
            "9": -980.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0846571692118479,
          "top_3_trade_profit_pct": 0.22886133032694475,
          "top_5_trade_profit_pct": 0.35769191349800145,
          "trade_count": 80,
          "win_rate": 0.4625,
          "worst_day": -2416.0,
          "worst_trade": -1034.0
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.723333,
        "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
        "decision_reason": "mandatory_null_failed:sign_flipped_effect",
        "effect_size": -0.00015533129989133147,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 5,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -229.08333333333334,
        "average_win": 219.16666666666666,
        "best_day": 1316.0,
        "best_trade": 1431.0,
        "commissions": 2160.0,
        "expectancy": -4.958333333333333,
        "gross_pnl": 970.0,
        "losing_streak": 12,
        "max_drawdown": 4960.0,
        "net_pnl": -1190.0,
        "periods": "q1_to_q3",
        "profit_factor": 0.9567115314659876,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.054410646387832697,
        "top_3_trade_profit_pct": 0.11703422053231939,
        "top_5_trade_profit_pct": 0.170532319391635,
        "trade_count": 240,
        "win_rate": 0.5,
        "worst_day": -2416.0,
        "worst_trade": -1034.0
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -1190.0,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.054411
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "overnight_inventory_rth_resolution_09_00",
        "components": [
          "opening_response",
          "acceptance_rejection"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "overnight_inventory_rth_resolution",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "RAW_POSITIVE_NET",
          "RAW_ECONOMIC_SCREEN_PASS",
          "MATCHED_NULL_BEATEN",
          "REPRESENTATION_EVIDENCE_PASS"
        ],
        "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
        "sizing": "fixed_one_contract",
        "structural_id": "overnight_inventory_rth_resolution_struct_09",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "acceptance_rejection"
        ],
        "full_pooled_net_pnl": 309.5,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "acceptance_rejection"
        ],
        "redundant_components": [
          "opening_response"
        ],
        "removals": [
          {
            "classification": "REDUNDANT_COMPONENT",
            "component": "opening_response",
            "full_minus_removed": 132.0,
            "removed_net_pnl": 177.5
          },
          {
            "classification": "ESSENTIAL_COMPONENT",
            "component": "acceptance_rejection",
            "full_minus_removed": 285.0,
            "removed_net_pnl": 24.5
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.723333,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": 0.00016770067665670315,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
          "effect_size": 0.0001635213644460353,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.0,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -8.062362262355239e-06,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 0,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -3.25,
          "average_win": 11.392857142857142,
          "best_day": 28.0,
          "best_trade": 28.0,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "commissions": 49.5,
          "contract_contribution": {
            "MES": 66.75
          },
          "executed_trades": 11,
          "expectancy": 6.068181818181818,
          "gross_pnl": 116.25,
          "losing_streak": 1,
          "max_adverse_excursion": -22.5,
          "max_drawdown": 7.25,
          "max_favorable_excursion": 38.75,
          "net_pnl": 66.75,
          "opportunities": 11,
          "period": "q1",
          "profit_factor": 6.134615384615385,
          "regime_contribution": {
            "normal_vol": 66.75
          },
          "roll_window_contribution": {
            "normal": 70.0,
            "roll_window": -3.25
          },
          "session_contribution": {
            "0": 37.0,
            "22": 28.0,
            "23": 1.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.3510971786833856,
          "top_3_trade_profit_pct": 0.8025078369905956,
          "top_5_trade_profit_pct": 0.9561128526645768,
          "trade_count": 11,
          "win_rate": 0.6363636363636364,
          "worst_day": -5.75,
          "worst_trade": -5.75
        },
        "q2": {
          "average_loss": -3.09375,
          "average_win": 14.022727272727273,
          "best_day": 34.25,
          "best_trade": 34.25,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "commissions": 85.5,
          "contract_contribution": {
            "MES": 129.5
          },
          "executed_trades": 19,
          "expectancy": 6.815789473684211,
          "gross_pnl": 215.0,
          "losing_streak": 3,
          "max_adverse_excursion": -47.5,
          "max_drawdown": 13.75,
          "max_favorable_excursion": 42.5,
          "net_pnl": 129.5,
          "opportunities": 19,
          "period": "q2",
          "profit_factor": 6.232323232323233,
          "regime_contribution": {
            "normal_vol": 129.5
          },
          "roll_window_contribution": {
            "normal": 129.5
          },
          "session_contribution": {
            "0": 19.75,
            "22": 109.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.22204213938411668,
          "top_3_trade_profit_pct": 0.5607779578606159,
          "top_5_trade_profit_pct": 0.7860615883306321,
          "trade_count": 19,
          "win_rate": 0.5789473684210527,
          "worst_day": -5.75,
          "worst_trade": -5.75
        },
        "q3": {
          "average_loss": -9.5,
          "average_win": 15.34375,
          "best_day": 33.0,
          "best_trade": 33.0,
          "candidate_id": "overnight_inventory_rth_resolution_09_00",
          "commissions": 40.5,
          "contract_contribution": {
            "MES": 113.25
          },
          "executed_trades": 9,
          "expectancy": 12.583333333333334,
          "gross_pnl": 153.75,
          "losing_streak": 1,
          "max_adverse_excursion": -18.75,
          "max_drawdown": 9.5,
          "max_favorable_excursion": 45.0,
          "net_pnl": 113.25,
          "opportunities": 9,
          "period": "q3",
          "profit_factor": 12.921052631578947,
          "regime_contribution": {
            "normal_vol": 113.25
          },
          "roll_window_contribution": {
            "normal": 113.25
          },
          "session_contribution": {
            "0": 95.25,
            "22": 18.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.26883910386965376,
          "top_3_trade_profit_pct": 0.5824847250509165,
          "top_5_trade_profit_pct": 0.824847250509165,
          "trade_count": 9,
          "win_rate": 0.8888888888888888,
          "worst_day": -9.5,
          "worst_trade": -9.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.723333,
        "candidate_id": "overnight_inventory_rth_resolution_09_00",
        "decision_reason": "mandatory_null_failed:sign_flipped_effect",
        "effect_size": 8.791079602178029e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 5,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -3.6346153846153846,
        "average_win": 13.721153846153847,
        "best_day": 34.25,
        "best_trade": 34.25,
        "commissions": 175.5,
        "expectancy": 7.935897435897436,
        "gross_pnl": 485.0,
        "losing_streak": 3,
        "max_drawdown": 13.75,
        "net_pnl": 309.5,
        "periods": "q1_to_q3",
        "profit_factor": 7.550264550264551,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.09600560616678346,
        "top_3_trade_profit_pct": 0.2669936930623686,
        "top_5_trade_profit_pct": 0.4204625087596356,
        "trade_count": 39,
        "win_rate": 0.6666666666666666,
        "worst_day": -9.5,
        "worst_trade": -9.5
      },
      "temporal_transfer": {
        "candidate_id": "overnight_inventory_rth_resolution_09_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": 309.5,
        "pooled_null_passed": false,
        "positive_periods": 3,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.096006
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "overnight_inventory_rth_resolution_05_00",
        "components": [
          "regime_context"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "overnight_inventory_rth_resolution",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "RAW_POSITIVE_NET",
          "MATCHED_NULL_BEATEN",
          "REPRESENTATION_EVIDENCE_PASS"
        ],
        "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
        "sizing": "fixed_one_contract",
        "structural_id": "overnight_inventory_rth_resolution_struct_05",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "regime_context"
        ],
        "full_pooled_net_pnl": -22.0,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "regime_context"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "regime_context",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": -0.00014373776375073888,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
          "effect_size": -0.0001417118566387893,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.39,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -5.324580335085235e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -7.0,
          "average_win": 1.75,
          "best_day": 1.75,
          "best_trade": 1.75,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "commissions": 9.0,
          "contract_contribution": {
            "MES": -5.25
          },
          "executed_trades": 2,
          "expectancy": -2.625,
          "gross_pnl": 3.75,
          "losing_streak": 1,
          "max_adverse_excursion": -16.25,
          "max_drawdown": 7.0,
          "max_favorable_excursion": 18.75,
          "net_pnl": -5.25,
          "opportunities": 2,
          "period": "q1",
          "profit_factor": 0.25,
          "regime_contribution": {
            "normal_vol": -5.25
          },
          "roll_window_contribution": {
            "normal": -5.25
          },
          "session_contribution": {
            "0": -7.0,
            "23": 1.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 1.0,
          "top_3_trade_profit_pct": 1.0,
          "top_5_trade_profit_pct": 1.0,
          "trade_count": 2,
          "win_rate": 0.5,
          "worst_day": -7.0,
          "worst_trade": -7.0
        },
        "q2": {
          "average_loss": -13.875,
          "average_win": 0.0,
          "best_day": -27.75,
          "best_trade": -2.0,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "commissions": 9.0,
          "contract_contribution": {
            "MES": -27.75
          },
          "executed_trades": 2,
          "expectancy": -13.875,
          "gross_pnl": -18.75,
          "losing_streak": 2,
          "max_adverse_excursion": -23.75,
          "max_drawdown": 27.75,
          "max_favorable_excursion": 7.5,
          "net_pnl": -27.75,
          "opportunities": 2,
          "period": "q2",
          "profit_factor": 0.0,
          "regime_contribution": {
            "normal_vol": -27.75
          },
          "roll_window_contribution": {
            "normal": -27.75
          },
          "session_contribution": {
            "0": -25.75,
            "1": -2.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0,
          "top_3_trade_profit_pct": 0.0,
          "top_5_trade_profit_pct": 0.0,
          "trade_count": 2,
          "win_rate": 0.0,
          "worst_day": -27.75,
          "worst_trade": -25.75
        },
        "q3": {
          "average_loss": -4.5,
          "average_win": 15.5,
          "best_day": 11.0,
          "best_trade": 15.5,
          "candidate_id": "overnight_inventory_rth_resolution_05_00",
          "commissions": 9.0,
          "contract_contribution": {
            "MES": 11.0
          },
          "executed_trades": 2,
          "expectancy": 5.5,
          "gross_pnl": 20.0,
          "losing_streak": 1,
          "max_adverse_excursion": -10.0,
          "max_drawdown": 4.5,
          "max_favorable_excursion": 21.25,
          "net_pnl": 11.0,
          "opportunities": 2,
          "period": "q3",
          "profit_factor": 3.4444444444444446,
          "regime_contribution": {
            "normal_vol": 11.0
          },
          "roll_window_contribution": {
            "normal": 11.0
          },
          "session_contribution": {
            "0": 15.5,
            "1": -4.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 1.0,
          "top_3_trade_profit_pct": 1.0,
          "top_5_trade_profit_pct": 1.0,
          "trade_count": 2,
          "win_rate": 0.5,
          "worst_day": 11.0,
          "worst_trade": -4.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.39,
        "candidate_id": "overnight_inventory_rth_resolution_05_00",
        "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,sign_flipped_effect",
        "effect_size": -0.000154360257115564,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 3,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -9.8125,
        "average_win": 8.625,
        "best_day": 11.0,
        "best_trade": 15.5,
        "commissions": 27.0,
        "expectancy": -3.6666666666666665,
        "gross_pnl": 5.0,
        "losing_streak": 3,
        "max_drawdown": 34.75,
        "net_pnl": -22.0,
        "periods": "q1_to_q3",
        "profit_factor": 0.4394904458598726,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.8985507246376812,
        "top_3_trade_profit_pct": 1.0,
        "top_5_trade_profit_pct": 1.0,
        "trade_count": 6,
        "win_rate": 0.3333333333333333,
        "worst_day": -27.75,
        "worst_trade": -25.75
      },
      "temporal_transfer": {
        "candidate_id": "overnight_inventory_rth_resolution_05_00",
        "decision_reason": "total_trade_count_below_minimum",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -22.0,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "INSUFFICIENT_EVIDENCE",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.898551
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
        "components": [
          "path_asymmetry"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "MATCHED_NULL_BEATEN",
          "REPRESENTATION_EVIDENCE_PASS"
        ],
        "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_03",
        "symbol": "MNQ",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "path_asymmetry"
        ],
        "full_pooled_net_pnl": -686.5,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "path_asymmetry"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "path_asymmetry",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.723333,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": 8.317745722384973e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.223333,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "decision_reason": "mandatory_null_failed:mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -6.566064972629718e-06,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 2,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.223333,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -9.99345652929029e-06,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 2,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -15.819148936170214,
          "average_win": 22.625,
          "best_day": 65.0,
          "best_trade": 172.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MNQ": -19.5
          },
          "executed_trades": 80,
          "expectancy": -0.24375,
          "gross_pnl": 340.5,
          "losing_streak": 11,
          "max_adverse_excursion": -159.5,
          "max_drawdown": 239.0,
          "max_favorable_excursion": 182.5,
          "net_pnl": -19.5,
          "opportunities": 3838,
          "period": "q1",
          "profit_factor": 0.9737726967047747,
          "regime_contribution": {
            "high_vol": 239.5,
            "normal_vol": -259.0
          },
          "roll_window_contribution": {
            "normal": -19.5
          },
          "session_contribution": {
            "0": -52.5,
            "1": 5.5,
            "10": 142.5,
            "11": -201.0,
            "13": 39.0,
            "14": 23.0,
            "15": 100.5,
            "17": -98.0,
            "18": 32.0,
            "2": -3.5,
            "20": 172.5,
            "21": -39.5,
            "23": -96.0,
            "3": -3.5,
            "4": -22.0,
            "5": -0.5,
            "6": -48.0,
            "7": -37.5,
            "8": 83.0,
            "9": -15.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.23825966850828728,
          "top_3_trade_profit_pct": 0.47997237569060774,
          "top_5_trade_profit_pct": 0.6319060773480663,
          "trade_count": 80,
          "win_rate": 0.4,
          "worst_day": -60.5,
          "worst_trade": -152.5
        },
        "q2": {
          "average_loss": -15.382978723404255,
          "average_win": 12.919354838709678,
          "best_day": -104.5,
          "best_trade": 39.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MNQ": -322.5
          },
          "executed_trades": 80,
          "expectancy": -4.03125,
          "gross_pnl": 37.5,
          "losing_streak": 6,
          "max_adverse_excursion": -81.0,
          "max_drawdown": 350.0,
          "max_favorable_excursion": 79.0,
          "net_pnl": -322.5,
          "opportunities": 4220,
          "period": "q2",
          "profit_factor": 0.553941908713693,
          "regime_contribution": {
            "high_vol": -78.5,
            "normal_vol": -244.0
          },
          "roll_window_contribution": {
            "normal": -322.5
          },
          "session_contribution": {
            "0": -22.0,
            "1": -63.5,
            "10": 84.5,
            "11": 32.5,
            "12": -19.0,
            "13": -9.0,
            "15": -9.5,
            "16": -61.0,
            "17": -26.5,
            "18": -17.0,
            "19": 3.0,
            "2": -38.5,
            "20": -0.5,
            "22": -51.5,
            "23": 5.0,
            "3": 27.5,
            "4": -11.5,
            "5": -3.0,
            "6": 10.5,
            "7": -113.0,
            "8": 15.0,
            "9": -55.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0986267166042447,
          "top_3_trade_profit_pct": 0.2808988764044944,
          "top_5_trade_profit_pct": 0.42696629213483145,
          "trade_count": 80,
          "win_rate": 0.3875,
          "worst_day": -218.0,
          "worst_trade": -62.0
        },
        "q3": {
          "average_loss": -18.854166666666668,
          "average_win": 18.080645161290324,
          "best_day": -51.0,
          "best_trade": 94.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MNQ": -344.5
          },
          "executed_trades": 80,
          "expectancy": -4.30625,
          "gross_pnl": 15.5,
          "losing_streak": 9,
          "max_adverse_excursion": -126.5,
          "max_drawdown": 457.0,
          "max_favorable_excursion": 104.0,
          "net_pnl": -344.5,
          "opportunities": 2834,
          "period": "q3",
          "profit_factor": 0.6193370165745856,
          "regime_contribution": {
            "high_vol": 23.5,
            "normal_vol": -368.0
          },
          "roll_window_contribution": {
            "normal": -344.5
          },
          "session_contribution": {
            "0": -94.5,
            "1": 6.0,
            "10": -27.5,
            "11": 34.5,
            "12": 26.0,
            "16": -6.5,
            "19": -88.0,
            "2": -28.0,
            "20": -26.5,
            "22": -67.5,
            "23": -5.5,
            "3": -23.0,
            "4": -115.0,
            "5": -128.5,
            "6": 22.0,
            "7": 19.0,
            "8": 134.0,
            "9": 24.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.16859946476360393,
          "top_3_trade_profit_pct": 0.32292595896520965,
          "top_5_trade_profit_pct": 0.463871543264942,
          "trade_count": 80,
          "win_rate": 0.3875,
          "worst_day": -293.5,
          "worst_trade": -95.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.723333,
        "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
        "decision_reason": "mandatory_null_failed:sign_flipped_effect",
        "effect_size": 5.838130565118394e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 5,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -16.700704225352112,
        "average_win": 17.925531914893618,
        "best_day": 65.0,
        "best_trade": 172.5,
        "commissions": 1080.0,
        "expectancy": -2.8604166666666666,
        "gross_pnl": 393.5,
        "losing_streak": 11,
        "max_drawdown": 852.5,
        "net_pnl": -686.5,
        "periods": "q1_to_q3",
        "profit_factor": 0.7105207674467636,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.10237388724035608,
        "top_3_trade_profit_pct": 0.21810089020771514,
        "top_5_trade_profit_pct": 0.30148367952522254,
        "trade_count": 240,
        "win_rate": 0.39166666666666666,
        "worst_day": -293.5,
        "worst_trade": -152.5
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -686.5,
        "pooled_null_passed": false,
        "positive_periods": 0,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.102374
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
        "components": [
          "effort_vs_progress"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "RAW_POSITIVE_NET",
          "RAW_ECONOMIC_SCREEN_PASS",
          "MATCHED_NULL_BEATEN"
        ],
        "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_02",
        "symbol": "NQ",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "effort_vs_progress"
        ],
        "full_pooled_net_pnl": -4820.0,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "effort_vs_progress"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "effort_vs_progress",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": -0.0003745635300856335,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
          "effect_size": -0.00012182937557890801,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -3.2086617548049793e-06,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 0,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -180.17021276595744,
          "average_win": 117.36363636363636,
          "best_day": 393.0,
          "best_trade": 1026.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": -4595.0
          },
          "executed_trades": 80,
          "expectancy": -57.4375,
          "gross_pnl": -3875.0,
          "losing_streak": 8,
          "max_adverse_excursion": -1235.0,
          "max_drawdown": 5870.0,
          "max_favorable_excursion": 1110.0,
          "net_pnl": -4595.0,
          "opportunities": 707,
          "period": "q1",
          "profit_factor": 0.45736891828058573,
          "regime_contribution": {
            "high_vol": -1976.0,
            "normal_vol": -2619.0
          },
          "roll_window_contribution": {
            "normal": -4595.0
          },
          "session_contribution": {
            "0": 97.0,
            "1": -286.0,
            "10": -63.0,
            "11": -459.0,
            "12": 32.0,
            "14": -1957.0,
            "15": 1462.0,
            "16": -69.0,
            "17": -439.0,
            "2": 107.0,
            "20": -582.0,
            "21": -221.0,
            "23": -66.0,
            "3": -829.0,
            "4": 184.0,
            "5": -18.0,
            "6": -232.0,
            "8": -712.0,
            "9": -544.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.26491092176607284,
          "top_3_trade_profit_pct": 0.46553059643687067,
          "top_5_trade_profit_pct": 0.557707203718048,
          "trade_count": 80,
          "win_rate": 0.4125,
          "worst_day": -2345.0,
          "worst_trade": -1209.0
        },
        "q2": {
          "average_loss": -188.4047619047619,
          "average_win": 127.57894736842105,
          "best_day": 784.0,
          "best_trade": 731.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": -3065.0
          },
          "executed_trades": 80,
          "expectancy": -38.3125,
          "gross_pnl": -2345.0,
          "losing_streak": 7,
          "max_adverse_excursion": -820.0,
          "max_drawdown": 3207.0,
          "max_favorable_excursion": 805.0,
          "net_pnl": -3065.0,
          "opportunities": 717,
          "period": "q2",
          "profit_factor": 0.6126627069379502,
          "regime_contribution": {
            "high_vol": -114.0,
            "normal_vol": -2951.0
          },
          "roll_window_contribution": {
            "normal": -3065.0
          },
          "session_contribution": {
            "0": 558.0,
            "1": 124.0,
            "10": 250.0,
            "11": -664.0,
            "12": 27.0,
            "13": -683.0,
            "15": 366.0,
            "16": -27.0,
            "17": -379.0,
            "19": 46.0,
            "2": 435.0,
            "20": -840.0,
            "22": 52.0,
            "23": -459.0,
            "3": -651.0,
            "4": 486.0,
            "5": -251.0,
            "6": -562.0,
            "7": -581.0,
            "9": -312.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.1507838283828383,
          "top_3_trade_profit_pct": 0.34405940594059403,
          "top_5_trade_profit_pct": 0.46720297029702973,
          "trade_count": 80,
          "win_rate": 0.475,
          "worst_day": -1536.0,
          "worst_trade": -664.0
        },
        "q3": {
          "average_loss": -116.72727272727273,
          "average_win": 142.38297872340425,
          "best_day": 1303.0,
          "best_trade": 1061.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
          "commissions": 720.0,
          "contract_contribution": {
            "NQ": 2840.0
          },
          "executed_trades": 80,
          "expectancy": 35.5,
          "gross_pnl": 3560.0,
          "losing_streak": 6,
          "max_adverse_excursion": -725.0,
          "max_drawdown": 1467.0,
          "max_favorable_excursion": 1235.0,
          "net_pnl": 2840.0,
          "opportunities": 496,
          "period": "q3",
          "profit_factor": 1.7372793354101765,
          "regime_contribution": {
            "high_vol": 381.0,
            "normal_vol": 2459.0
          },
          "roll_window_contribution": {
            "normal": 2840.0
          },
          "session_contribution": {
            "0": 59.0,
            "1": 207.0,
            "10": 392.0,
            "11": -690.0,
            "12": 115.0,
            "13": 782.0,
            "14": 381.0,
            "15": 531.0,
            "16": 352.0,
            "19": 212.0,
            "2": -431.0,
            "20": -154.0,
            "22": 63.0,
            "23": -204.0,
            "3": -83.0,
            "4": -116.0,
            "5": 787.0,
            "6": 31.0,
            "7": 460.0,
            "8": 94.0,
            "9": 52.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.1585475194261805,
          "top_3_trade_profit_pct": 0.31649731022115957,
          "top_5_trade_profit_pct": 0.42289300657501494,
          "trade_count": 80,
          "win_rate": 0.5875,
          "worst_day": -1378.0,
          "worst_trade": -614.0
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.556667,
        "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
        "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
        "effect_size": -0.0003179895207636792,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 4,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -165.84426229508196,
        "average_win": 130.61864406779662,
        "best_day": 1303.0,
        "best_trade": 1061.0,
        "commissions": 2160.0,
        "expectancy": -20.083333333333332,
        "gross_pnl": -2660.0,
        "losing_streak": 8,
        "max_drawdown": 8512.0,
        "net_pnl": -4820.0,
        "periods": "q1_to_q3",
        "profit_factor": 0.7617753175505363,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.06883799390125218,
        "top_3_trade_profit_pct": 0.18283267371699216,
        "top_5_trade_profit_pct": 0.25141114643482776,
        "trade_count": 240,
        "win_rate": 0.49166666666666664,
        "worst_day": -2345.0,
        "worst_trade": -1209.0
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -4820.0,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.068838
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "overnight_inventory_rth_resolution_01_00",
        "components": [
          "overnight_participation"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "overnight_inventory_rth_resolution",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "MATCHED_NULL_BEATEN"
        ],
        "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "overnight_inventory_rth_resolution_struct_01",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "overnight_participation"
        ],
        "full_pooled_net_pnl": -139.5,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "overnight_participation"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "overnight_participation",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": -0.0001444079143972831,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": -0.00014701213951145742,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.056667,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -2.6893778806281152e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 1,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -11.464285714285714,
          "average_win": 4.041666666666667,
          "best_day": 10.5,
          "best_trade": 10.5,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "commissions": 58.5,
          "contract_contribution": {
            "MES": -56.0
          },
          "executed_trades": 13,
          "expectancy": -4.3076923076923075,
          "gross_pnl": 2.5,
          "losing_streak": 3,
          "max_adverse_excursion": -36.25,
          "max_drawdown": 56.0,
          "max_favorable_excursion": 18.75,
          "net_pnl": -56.0,
          "opportunities": 13,
          "period": "q1",
          "profit_factor": 0.30218068535825543,
          "regime_contribution": {
            "normal_vol": -56.0
          },
          "roll_window_contribution": {
            "normal": -57.75,
            "roll_window": 1.75
          },
          "session_contribution": {
            "0": -56.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.4329896907216495,
          "top_3_trade_profit_pct": 0.6804123711340206,
          "top_5_trade_profit_pct": 0.9278350515463918,
          "trade_count": 13,
          "win_rate": 0.46153846153846156,
          "worst_day": -37.0,
          "worst_trade": -37.0
        },
        "q2": {
          "average_loss": -12.875,
          "average_win": 10.083333333333334,
          "best_day": 23.0,
          "best_trade": 23.0,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "commissions": 58.5,
          "contract_contribution": {
            "MES": -98.5
          },
          "executed_trades": 13,
          "expectancy": -7.576923076923077,
          "gross_pnl": -40.0,
          "losing_streak": 7,
          "max_adverse_excursion": -60.0,
          "max_drawdown": 119.25,
          "max_favorable_excursion": 32.5,
          "net_pnl": -98.5,
          "opportunities": 13,
          "period": "q2",
          "profit_factor": 0.23495145631067962,
          "regime_contribution": {
            "normal_vol": -98.5
          },
          "roll_window_contribution": {
            "normal": -96.5,
            "roll_window": -2.0
          },
          "session_contribution": {
            "0": -98.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.7603305785123967,
          "top_3_trade_profit_pct": 1.0,
          "top_5_trade_profit_pct": 1.0,
          "trade_count": 13,
          "win_rate": 0.23076923076923078,
          "worst_day": -28.25,
          "worst_trade": -28.25
        },
        "q3": {
          "average_loss": -21.75,
          "average_win": 12.375,
          "best_day": 27.25,
          "best_trade": 23.0,
          "candidate_id": "overnight_inventory_rth_resolution_01_00",
          "commissions": 67.5,
          "contract_contribution": {
            "MES": 15.0
          },
          "executed_trades": 15,
          "expectancy": 1.0,
          "gross_pnl": 82.5,
          "losing_streak": 2,
          "max_adverse_excursion": -125.0,
          "max_drawdown": 60.25,
          "max_favorable_excursion": 33.75,
          "net_pnl": 15.0,
          "opportunities": 15,
          "period": "q3",
          "profit_factor": 1.1379310344827587,
          "regime_contribution": {
            "high_vol": -52.0,
            "normal_vol": 67.0
          },
          "roll_window_contribution": {
            "normal": -6.75,
            "roll_window": 21.75
          },
          "session_contribution": {
            "0": 15.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.18585858585858586,
          "top_3_trade_profit_pct": 0.5171717171717172,
          "top_5_trade_profit_pct": 0.7373737373737373,
          "trade_count": 15,
          "win_rate": 0.6666666666666666,
          "worst_day": -52.0,
          "worst_trade": -52.0
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.556667,
        "candidate_id": "overnight_inventory_rth_resolution_01_00",
        "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
        "effect_size": -0.00015739390335044407,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 4,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -14.443181818181818,
        "average_win": 9.381578947368421,
        "best_day": 27.25,
        "best_trade": 23.0,
        "commissions": 184.5,
        "expectancy": -3.402439024390244,
        "gross_pnl": 45.0,
        "losing_streak": 8,
        "max_drawdown": 175.5,
        "net_pnl": -139.5,
        "periods": "q1_to_q3",
        "profit_factor": 0.5609756097560976,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.12903225806451613,
        "top_3_trade_profit_pct": 0.3800841514726508,
        "top_5_trade_profit_pct": 0.5750350631136045,
        "trade_count": 41,
        "win_rate": 0.4634146341463415,
        "worst_day": -52.0,
        "worst_trade": -52.0
      },
      "temporal_transfer": {
        "candidate_id": "overnight_inventory_rth_resolution_01_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -139.5,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.129032
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
        "components": [
          "accepted_center_migration"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "MATCHED_NULL_BEATEN"
        ],
        "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_00",
        "symbol": "ES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "accepted_center_migration"
        ],
        "full_pooled_net_pnl": -1060.0,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "accepted_center_migration"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "accepted_center_migration",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.723333,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": 5.2504917811728505e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": 4.400487739196472e-06,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 0,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": 5.1253166170084276e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -69.57692307692308,
          "average_win": 79.41463414634147,
          "best_day": 610.5,
          "best_trade": 678.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": 542.5
          },
          "executed_trades": 80,
          "expectancy": 6.78125,
          "gross_pnl": 1262.5,
          "losing_streak": 4,
          "max_adverse_excursion": -325.0,
          "max_drawdown": 813.5,
          "max_favorable_excursion": 775.0,
          "net_pnl": 542.5,
          "opportunities": 2034,
          "period": "q1",
          "profit_factor": 1.1999262944536577,
          "regime_contribution": {
            "high_vol": -198.5,
            "normal_vol": 741.0
          },
          "roll_window_contribution": {
            "normal": 542.5
          },
          "session_contribution": {
            "0": -109.5,
            "1": -70.0,
            "10": 719.5,
            "11": 185.5,
            "13": 53.5,
            "15": 341.0,
            "16": -393.0,
            "17": -146.5,
            "2": 5.0,
            "21": 53.5,
            "23": -256.0,
            "3": -92.0,
            "4": -25.5,
            "5": -11.0,
            "6": 57.0,
            "7": -22.0,
            "8": 80.0,
            "9": 173.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.2083845208845209,
          "top_3_trade_profit_pct": 0.3948095823095823,
          "top_5_trade_profit_pct": 0.515970515970516,
          "trade_count": 80,
          "win_rate": 0.5125,
          "worst_day": -113.0,
          "worst_trade": -296.5
        },
        "q2": {
          "average_loss": -90.08108108108108,
          "average_win": 56.406976744186046,
          "best_day": 388.5,
          "best_trade": 228.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": -907.5
          },
          "executed_trades": 80,
          "expectancy": -11.34375,
          "gross_pnl": -187.5,
          "losing_streak": 4,
          "max_adverse_excursion": -612.5,
          "max_drawdown": 1312.0,
          "max_favorable_excursion": 337.5,
          "net_pnl": -907.5,
          "opportunities": 1986,
          "period": "q2",
          "profit_factor": 0.7277227722772277,
          "regime_contribution": {
            "high_vol": -186.0,
            "normal_vol": -721.5
          },
          "roll_window_contribution": {
            "normal": -907.5
          },
          "session_contribution": {
            "0": 55.0,
            "1": -225.5,
            "13": -464.5,
            "14": -321.5,
            "15": 201.5,
            "16": -243.0,
            "17": 26.5,
            "18": -529.0,
            "19": -202.0,
            "2": -25.5,
            "20": 135.5,
            "22": 16.0,
            "23": 348.0,
            "3": 56.5,
            "4": 48.0,
            "5": 64.0,
            "6": 137.0,
            "7": 69.5,
            "8": 5.0,
            "9": -59.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.09420737992166564,
          "top_3_trade_profit_pct": 0.23623995052566482,
          "top_5_trade_profit_pct": 0.35765821480107196,
          "trade_count": 80,
          "win_rate": 0.5375,
          "worst_day": -681.0,
          "worst_trade": -446.5
        },
        "q3": {
          "average_loss": -106.11538461538461,
          "average_win": 83.98780487804878,
          "best_day": -197.0,
          "best_trade": 341.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": -695.0
          },
          "executed_trades": 80,
          "expectancy": -8.6875,
          "gross_pnl": 25.0,
          "losing_streak": 4,
          "max_adverse_excursion": -600.0,
          "max_drawdown": 1346.5,
          "max_favorable_excursion": 525.0,
          "net_pnl": -695.0,
          "opportunities": 1597,
          "period": "q3",
          "profit_factor": 0.8320647577624743,
          "regime_contribution": {
            "high_vol": 41.0,
            "normal_vol": -736.0
          },
          "roll_window_contribution": {
            "normal": -695.0
          },
          "session_contribution": {
            "0": 430.0,
            "1": -18.0,
            "10": 76.5,
            "12": -30.5,
            "13": -146.5,
            "15": 41.0,
            "17": 108.5,
            "18": 53.5,
            "19": 253.5,
            "2": -97.0,
            "20": -71.5,
            "22": 6.5,
            "3": 140.5,
            "4": -71.5,
            "5": -47.0,
            "6": -139.5,
            "7": -311.0,
            "8": -443.5,
            "9": -429.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0990271526063598,
          "top_3_trade_profit_pct": 0.24626107158414404,
          "top_5_trade_profit_pct": 0.3680847974444606,
          "trade_count": 80,
          "win_rate": 0.5125,
          "worst_day": -290.5,
          "worst_trade": -296.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.39,
        "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
        "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,sign_flipped_effect",
        "effect_size": -2.966589901346346e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 3,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -88.56521739130434,
        "average_win": 73.0,
        "best_day": 610.5,
        "best_trade": 678.5,
        "commissions": 2160.0,
        "expectancy": -4.416666666666667,
        "gross_pnl": 1100.0,
        "losing_streak": 5,
        "max_drawdown": 2177.5,
        "net_pnl": -1060.0,
        "periods": "q1_to_q3",
        "profit_factor": 0.895925380461463,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.07435616438356164,
        "top_3_trade_profit_pct": 0.14909589041095891,
        "top_5_trade_profit_pct": 0.20602739726027397,
        "trade_count": 240,
        "win_rate": 0.5208333333333334,
        "worst_day": -681.0,
        "worst_trade": -446.5
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -1060.0,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.074356
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
        "components": [
          "path_asymmetry",
          "range_relocation"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "MATCHED_NULL_BEATEN"
        ],
        "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_09",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [],
        "full_pooled_net_pnl": -1071.25,
        "harmful_components": [
          "path_asymmetry"
        ],
        "minimal_stable_formulation": [
          "path_asymmetry",
          "range_relocation"
        ],
        "redundant_components": [
          "range_relocation"
        ],
        "removals": [
          {
            "classification": "HARMFUL_COMPONENT",
            "component": "path_asymmetry",
            "full_minus_removed": -1000.5,
            "removed_net_pnl": -70.75
          },
          {
            "classification": "REDUNDANT_COMPONENT",
            "component": "range_relocation",
            "full_minus_removed": -28.75,
            "removed_net_pnl": -1042.5
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
          "effect_size": 5.049878385764421e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.056667,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": 1.1816847483106038e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 1,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.723333,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": 8.383869769688501e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -7.606617647058823,
          "average_win": 2.4791666666666665,
          "best_day": -33.5,
          "best_trade": 4.25,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -487.5
          },
          "executed_trades": 80,
          "expectancy": -6.09375,
          "gross_pnl": -127.5,
          "losing_streak": 29,
          "max_adverse_excursion": -22.5,
          "max_drawdown": 492.25,
          "max_favorable_excursion": 16.25,
          "net_pnl": -487.5,
          "opportunities": 20546,
          "period": "q1",
          "profit_factor": 0.05751570807153214,
          "regime_contribution": {
            "normal_vol": -487.5
          },
          "roll_window_contribution": {
            "normal": -487.5
          },
          "session_contribution": {
            "0": -111.5,
            "1": -194.0,
            "2": -58.0,
            "23": -42.75,
            "3": -62.5,
            "4": -1.75,
            "5": -17.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.14285714285714285,
          "top_3_trade_profit_pct": 0.3865546218487395,
          "top_5_trade_profit_pct": 0.5882352941176471,
          "trade_count": 80,
          "win_rate": 0.15,
          "worst_day": -454.0,
          "worst_trade": -20.75
        },
        "q2": {
          "average_loss": -5.991935483870968,
          "average_win": 2.236111111111111,
          "best_day": -331.25,
          "best_trade": 4.25,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -331.25
          },
          "executed_trades": 80,
          "expectancy": -4.140625,
          "gross_pnl": 28.75,
          "losing_streak": 12,
          "max_adverse_excursion": -22.5,
          "max_drawdown": 331.25,
          "max_favorable_excursion": 15.0,
          "net_pnl": -331.25,
          "opportunities": 21314,
          "period": "q2",
          "profit_factor": 0.10834454912516824,
          "regime_contribution": {
            "normal_vol": -331.25
          },
          "roll_window_contribution": {
            "normal": -331.25
          },
          "session_contribution": {
            "0": -40.75,
            "1": -39.5,
            "2": -31.25,
            "3": -40.0,
            "4": -36.75,
            "5": -52.5,
            "6": -81.0,
            "7": -9.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.10559006211180125,
          "top_3_trade_profit_pct": 0.3167701863354037,
          "top_5_trade_profit_pct": 0.4658385093167702,
          "trade_count": 80,
          "win_rate": 0.225,
          "worst_day": -331.25,
          "worst_trade": -22.0
        },
        "q3": {
          "average_loss": -6.669811320754717,
          "average_win": 3.740740740740741,
          "best_day": -252.5,
          "best_trade": 11.75,
          "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -252.5
          },
          "executed_trades": 80,
          "expectancy": -3.15625,
          "gross_pnl": 107.5,
          "losing_streak": 20,
          "max_adverse_excursion": -21.25,
          "max_drawdown": 255.5,
          "max_favorable_excursion": 22.5,
          "net_pnl": -252.5,
          "opportunities": 21966,
          "period": "q3",
          "profit_factor": 0.2857142857142857,
          "regime_contribution": {
            "normal_vol": -252.5
          },
          "roll_window_contribution": {
            "normal": -252.5
          },
          "session_contribution": {
            "0": -4.5,
            "1": -19.5,
            "2": -52.5,
            "3": -70.75,
            "4": -30.0,
            "5": -66.75,
            "6": -8.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.11633663366336634,
          "top_3_trade_profit_pct": 0.2871287128712871,
          "top_5_trade_profit_pct": 0.4207920792079208,
          "trade_count": 80,
          "win_rate": 0.3375,
          "worst_day": -252.5,
          "worst_trade": -24.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.556667,
        "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
        "decision_reason": "mandatory_null_failed:block_shuffled_effect,sign_flipped_effect",
        "effect_size": -4.75320121592532e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 4,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -6.788251366120218,
        "average_win": 3.0,
        "best_day": -33.5,
        "best_trade": 11.75,
        "commissions": 1080.0,
        "expectancy": -4.463541666666667,
        "gross_pnl": 8.75,
        "losing_streak": 29,
        "max_drawdown": 1078.25,
        "net_pnl": -1071.25,
        "periods": "q1_to_q3",
        "profit_factor": 0.13765345139867177,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.06871345029239766,
        "top_3_trade_profit_pct": 0.1695906432748538,
        "top_5_trade_profit_pct": 0.24853801169590642,
        "trade_count": 240,
        "win_rate": 0.2375,
        "worst_day": -454.0,
        "worst_trade": -24.5
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -1071.25,
        "pooled_null_passed": false,
        "positive_periods": 0,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.068713
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "overnight_inventory_rth_resolution_00_00",
        "components": [
          "overnight_displacement"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "overnight_inventory_rth_resolution",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "MATCHED_NULL_BEATEN"
        ],
        "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "overnight_inventory_rth_resolution_struct_00",
        "symbol": "ES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "overnight_displacement"
        ],
        "full_pooled_net_pnl": -4647.5,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "overnight_displacement"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "overnight_displacement",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.723333,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": -8.549668913519603e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.723333,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "decision_reason": "mandatory_null_failed:sign_flipped_effect",
          "effect_size": -0.0001423754084869983,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 5,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.556667,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": 0.00018436857835107511,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -91.44680851063829,
          "average_win": 62.96969696969697,
          "best_day": 16.0,
          "best_trade": 241.0,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": -2220.0
          },
          "executed_trades": 80,
          "expectancy": -27.75,
          "gross_pnl": -1500.0,
          "losing_streak": 10,
          "max_adverse_excursion": -975.0,
          "max_drawdown": 2330.0,
          "max_favorable_excursion": 387.5,
          "net_pnl": -2220.0,
          "opportunities": 964,
          "period": "q1",
          "profit_factor": 0.48348068869241506,
          "regime_contribution": {
            "high_vol": -892.0,
            "normal_vol": -1328.0
          },
          "roll_window_contribution": {
            "normal": -2220.0
          },
          "session_contribution": {
            "0": -37.0,
            "1": -118.5,
            "10": -152.0,
            "11": 16.0,
            "13": -9.0,
            "14": -946.5,
            "17": -298.5,
            "18": 362.0,
            "2": -213.0,
            "23": 16.0,
            "3": -70.0,
            "4": 253.0,
            "5": -111.0,
            "6": 160.5,
            "7": -230.5,
            "8": -134.0,
            "9": -707.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.11597690086621752,
          "top_3_trade_profit_pct": 0.287776708373436,
          "top_5_trade_profit_pct": 0.3994225216554379,
          "trade_count": 80,
          "win_rate": 0.4125,
          "worst_day": -1625.5,
          "worst_trade": -946.5
        },
        "q2": {
          "average_loss": -125.66666666666667,
          "average_win": 61.1219512195122,
          "best_day": 485.0,
          "best_trade": 303.5,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": -2395.0
          },
          "executed_trades": 80,
          "expectancy": -29.9375,
          "gross_pnl": -1675.0,
          "losing_streak": 7,
          "max_adverse_excursion": -1412.5,
          "max_drawdown": 2754.0,
          "max_favorable_excursion": 525.0,
          "net_pnl": -2395.0,
          "opportunities": 1011,
          "period": "q2",
          "profit_factor": 0.5113242195470312,
          "regime_contribution": {
            "high_vol": -625.5,
            "normal_vol": -1769.5
          },
          "roll_window_contribution": {
            "normal": -2395.0
          },
          "session_contribution": {
            "0": 570.5,
            "1": -102.5,
            "12": -209.0,
            "14": -305.5,
            "17": -105.5,
            "18": -1414.5,
            "19": -223.5,
            "2": 219.0,
            "3": -100.5,
            "4": 137.0,
            "5": -34.0,
            "6": 142.5,
            "7": -606.0,
            "8": -71.5,
            "9": -291.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.12110933758978451,
          "top_3_trade_profit_pct": 0.2685554668794892,
          "top_5_trade_profit_pct": 0.3810853950518755,
          "trade_count": 80,
          "win_rate": 0.5125,
          "worst_day": -1636.5,
          "worst_trade": -1334.0
        },
        "q3": {
          "average_loss": -75.19318181818181,
          "average_win": 91.0,
          "best_day": 163.0,
          "best_trade": 591.0,
          "candidate_id": "overnight_inventory_rth_resolution_00_00",
          "commissions": 720.0,
          "contract_contribution": {
            "ES": -32.5
          },
          "executed_trades": 80,
          "expectancy": -0.40625,
          "gross_pnl": 687.5,
          "losing_streak": 6,
          "max_adverse_excursion": -537.5,
          "max_drawdown": 1347.5,
          "max_favorable_excursion": 662.5,
          "net_pnl": -32.5,
          "opportunities": 700,
          "period": "q3",
          "profit_factor": 0.9901768172888016,
          "regime_contribution": {
            "high_vol": 264.0,
            "normal_vol": -296.5
          },
          "roll_window_contribution": {
            "normal": -32.5
          },
          "session_contribution": {
            "0": -261.5,
            "1": 197.5,
            "10": -98.5,
            "12": 116.0,
            "13": 69.5,
            "14": 369.5,
            "16": -105.5,
            "17": -302.0,
            "18": -523.5,
            "19": 623.0,
            "2": -205.5,
            "23": 173.0,
            "3": 19.5,
            "4": -136.0,
            "5": 18.5,
            "6": -188.0,
            "7": 223.0,
            "8": -21.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.1804029304029304,
          "top_3_trade_profit_pct": 0.36568986568986567,
          "top_5_trade_profit_pct": 0.46321733821733824,
          "trade_count": 80,
          "win_rate": 0.45,
          "worst_day": -407.0,
          "worst_trade": -409.0
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.723333,
        "candidate_id": "overnight_inventory_rth_resolution_00_00",
        "decision_reason": "mandatory_null_failed:sign_flipped_effect",
        "effect_size": -0.00010883134078272927,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 5,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -96.21153846153847,
        "average_win": 71.45454545454545,
        "best_day": 485.0,
        "best_trade": 591.0,
        "commissions": 2160.0,
        "expectancy": -19.364583333333332,
        "gross_pnl": -2487.5,
        "losing_streak": 10,
        "max_drawdown": 5924.5,
        "net_pnl": -4647.5,
        "periods": "q1_to_q3",
        "profit_factor": 0.6284229462322607,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.0751908396946565,
        "top_3_trade_profit_pct": 0.16036895674300256,
        "top_5_trade_profit_pct": 0.22169211195928754,
        "trade_count": 240,
        "win_rate": 0.4583333333333333,
        "worst_day": -1636.5,
        "worst_trade": -1334.0
      },
      "temporal_transfer": {
        "candidate_id": "overnight_inventory_rth_resolution_00_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -4647.5,
        "pooled_null_passed": false,
        "positive_periods": 0,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.075191
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
        "components": [
          "time_at_extremes"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "FALSIFIED"
        ],
        "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_01",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "time_at_extremes"
        ],
        "full_pooled_net_pnl": -1028.75,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "time_at_extremes"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "time_at_extremes",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "decision_reason": "mandatory_null_failed:mean_reversion_baseline_effect,momentum_baseline_effect,sign_flipped_effect",
          "effect_size": 4.5081005033214145e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
          "effect_size": 7.430335330186295e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": 5.7393813729261854e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -10.601694915254237,
          "average_win": 7.583333333333333,
          "best_day": 6.25,
          "best_trade": 24.25,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -466.25
          },
          "executed_trades": 80,
          "expectancy": -5.828125,
          "gross_pnl": -106.25,
          "losing_streak": 10,
          "max_adverse_excursion": -73.75,
          "max_drawdown": 469.25,
          "max_favorable_excursion": 40.0,
          "net_pnl": -466.25,
          "opportunities": 781,
          "period": "q1",
          "profit_factor": 0.2545963229416467,
          "regime_contribution": {
            "high_vol": -134.75,
            "normal_vol": -331.5
          },
          "roll_window_contribution": {
            "normal": -466.25
          },
          "session_contribution": {
            "0": -15.0,
            "1": -58.25,
            "10": 12.25,
            "11": 2.75,
            "12": -5.75,
            "13": -44.75,
            "14": -29.0,
            "15": 15.25,
            "16": -19.5,
            "17": 23.5,
            "18": -12.75,
            "19": -105.0,
            "2": -35.25,
            "20": 6.75,
            "21": -45.5,
            "23": -39.25,
            "3": -30.5,
            "4": -17.25,
            "5": -6.5,
            "6": -3.0,
            "7": -9.25,
            "8": -31.25,
            "9": -19.0
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.152276295133438,
          "top_3_trade_profit_pct": 0.3783359497645212,
          "top_5_trade_profit_pct": 0.5572998430141287,
          "trade_count": 80,
          "win_rate": 0.2625,
          "worst_day": -121.75,
          "worst_trade": -43.25
        },
        "q2": {
          "average_loss": -10.773584905660377,
          "average_win": 13.74074074074074,
          "best_day": 80.0,
          "best_trade": 71.75,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -200.0
          },
          "executed_trades": 80,
          "expectancy": -2.5,
          "gross_pnl": 160.0,
          "losing_streak": 10,
          "max_adverse_excursion": -51.25,
          "max_drawdown": 216.75,
          "max_favorable_excursion": 93.75,
          "net_pnl": -200.0,
          "opportunities": 805,
          "period": "q2",
          "profit_factor": 0.649737302977233,
          "regime_contribution": {
            "high_vol": 112.75,
            "normal_vol": -312.75
          },
          "roll_window_contribution": {
            "normal": -200.0
          },
          "session_contribution": {
            "0": -16.25,
            "1": -59.25,
            "10": -38.0,
            "11": 11.0,
            "12": 42.75,
            "13": 35.25,
            "14": 28.0,
            "15": 26.0,
            "16": -86.0,
            "17": 0.0,
            "18": 0.75,
            "19": 10.25,
            "2": -18.75,
            "20": 20.0,
            "22": -12.25,
            "23": 22.25,
            "3": -1.0,
            "4": 1.75,
            "5": 9.75,
            "6": -2.75,
            "7": -21.0,
            "8": -75.75,
            "9": -76.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.19339622641509435,
          "top_3_trade_profit_pct": 0.3881401617250674,
          "top_5_trade_profit_pct": 0.5188679245283019,
          "trade_count": 80,
          "win_rate": 0.3375,
          "worst_day": -181.0,
          "worst_trade": -48.25
        },
        "q3": {
          "average_loss": -9.879464285714286,
          "average_win": 7.947916666666667,
          "best_day": -8.25,
          "best_trade": 33.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
          "commissions": 360.0,
          "contract_contribution": {
            "MES": -362.5
          },
          "executed_trades": 80,
          "expectancy": -4.53125,
          "gross_pnl": -2.5,
          "losing_streak": 9,
          "max_adverse_excursion": -57.5,
          "max_drawdown": 362.5,
          "max_favorable_excursion": 67.5,
          "net_pnl": -362.5,
          "opportunities": 764,
          "period": "q3",
          "profit_factor": 0.3447808404880253,
          "regime_contribution": {
            "high_vol": 16.75,
            "normal_vol": -379.25
          },
          "roll_window_contribution": {
            "normal": -362.5
          },
          "session_contribution": {
            "0": -48.0,
            "1": -35.0,
            "10": -68.5,
            "11": -28.75,
            "12": 13.0,
            "13": 47.5,
            "14": -27.25,
            "15": 9.75,
            "16": -8.5,
            "17": 15.75,
            "18": -126.25,
            "19": 48.5,
            "2": -13.75,
            "20": -16.0,
            "22": -24.25,
            "23": -29.25,
            "3": -1.5,
            "4": -8.0,
            "5": -14.75,
            "6": -9.0,
            "7": -24.75,
            "8": 9.25,
            "9": -22.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.17300131061598953,
          "top_3_trade_profit_pct": 0.3486238532110092,
          "top_5_trade_profit_pct": 0.4980340760157274,
          "trade_count": 80,
          "win_rate": 0.3,
          "worst_day": -126.0,
          "worst_trade": -45.75
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.0,
        "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
        "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
        "effect_size": 1.9933422248519993e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 0,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -10.415178571428571,
        "average_win": 10.01388888888889,
        "best_day": 80.0,
        "best_trade": 71.75,
        "commissions": 1080.0,
        "expectancy": -4.286458333333333,
        "gross_pnl": 51.25,
        "losing_streak": 10,
        "max_drawdown": 1028.75,
        "net_pnl": -1028.75,
        "periods": "q1_to_q3",
        "profit_factor": 0.41205886555222176,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.09951456310679611,
        "top_3_trade_profit_pct": 0.19972260748959778,
        "top_5_trade_profit_pct": 0.2843273231622746,
        "trade_count": 240,
        "win_rate": 0.3,
        "worst_day": -181.0,
        "worst_trade": -48.25
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -1028.75,
        "pooled_null_passed": false,
        "positive_periods": 0,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.099515
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
        "components": [
          "range_relocation"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "FALSIFIED"
        ],
        "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_04",
        "symbol": "ES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "range_relocation"
        ],
        "full_pooled_net_pnl": -139.5,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "range_relocation"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "range_relocation",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.056667,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,sign_flipped_effect",
          "effect_size": 5.283089830465474e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 1,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "decision_reason": "mandatory_null_failed:delayed_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": -2.4469894259691704e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.223333,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": 6.064122492583464e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 2,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -71.5,
          "average_win": 0.0,
          "best_day": -71.5,
          "best_trade": -71.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "commissions": 9.0,
          "contract_contribution": {
            "ES": -71.5
          },
          "executed_trades": 1,
          "expectancy": -71.5,
          "gross_pnl": -62.5,
          "losing_streak": 1,
          "max_adverse_excursion": -87.5,
          "max_drawdown": 71.5,
          "max_favorable_excursion": 12.5,
          "net_pnl": -71.5,
          "opportunities": 1,
          "period": "q1",
          "profit_factor": 0.0,
          "regime_contribution": {
            "normal_vol": -71.5
          },
          "roll_window_contribution": {
            "normal": -71.5
          },
          "session_contribution": {
            "23": -71.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0,
          "top_3_trade_profit_pct": 0.0,
          "top_5_trade_profit_pct": 0.0,
          "trade_count": 1,
          "win_rate": 0.0,
          "worst_day": -71.5,
          "worst_trade": -71.5
        },
        "q2": {
          "average_loss": -71.5,
          "average_win": 0.0,
          "best_day": -71.5,
          "best_trade": -71.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "commissions": 9.0,
          "contract_contribution": {
            "ES": -71.5
          },
          "executed_trades": 1,
          "expectancy": -71.5,
          "gross_pnl": -62.5,
          "losing_streak": 1,
          "max_adverse_excursion": -87.5,
          "max_drawdown": 71.5,
          "max_favorable_excursion": 25.0,
          "net_pnl": -71.5,
          "opportunities": 1,
          "period": "q2",
          "profit_factor": 0.0,
          "regime_contribution": {
            "normal_vol": -71.5
          },
          "roll_window_contribution": {
            "normal": -71.5
          },
          "session_contribution": {
            "0": -71.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.0,
          "top_3_trade_profit_pct": 0.0,
          "top_5_trade_profit_pct": 0.0,
          "trade_count": 1,
          "win_rate": 0.0,
          "worst_day": -71.5,
          "worst_trade": -71.5
        },
        "q3": {
          "average_loss": 0.0,
          "average_win": 3.5,
          "best_day": 3.5,
          "best_trade": 3.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
          "commissions": 9.0,
          "contract_contribution": {
            "ES": 3.5
          },
          "executed_trades": 1,
          "expectancy": 3.5,
          "gross_pnl": 12.5,
          "losing_streak": 0,
          "max_adverse_excursion": 0.0,
          "max_drawdown": 0.0,
          "max_favorable_excursion": 87.5,
          "net_pnl": 3.5,
          "opportunities": 1,
          "period": "q3",
          "profit_factor": 999.0,
          "regime_contribution": {
            "normal_vol": 3.5
          },
          "roll_window_contribution": {
            "normal": 3.5
          },
          "session_contribution": {
            "0": 3.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 1.0,
          "top_3_trade_profit_pct": 1.0,
          "top_5_trade_profit_pct": 1.0,
          "trade_count": 1,
          "win_rate": 1.0,
          "worst_day": 3.5,
          "worst_trade": 3.5
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.0,
        "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
        "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,mean_reversion_baseline_effect,momentum_baseline_effect,random_matched_effect,sign_flipped_effect",
        "effect_size": 1.7037662673099076e-05,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 0,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -71.5,
        "average_win": 3.5,
        "best_day": 3.5,
        "best_trade": 3.5,
        "commissions": 27.0,
        "expectancy": -46.5,
        "gross_pnl": -112.5,
        "losing_streak": 2,
        "max_drawdown": 143.0,
        "net_pnl": -139.5,
        "periods": "q1_to_q3",
        "profit_factor": 0.024475524475524476,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 1.0,
        "top_3_trade_profit_pct": 1.0,
        "top_5_trade_profit_pct": 1.0,
        "trade_count": 3,
        "win_rate": 0.3333333333333333,
        "worst_day": -71.5,
        "worst_trade": -71.5
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
        "decision_reason": "total_trade_count_below_minimum",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -139.5,
        "pooled_null_passed": false,
        "positive_periods": 1,
        "status": "INSUFFICIENT_EVIDENCE",
        "third_period_catastrophic": false,
        "top_trade_concentration": 1.0
      },
      "topstep_sequential": null
    },
    {
      "candidate": {
        "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
        "components": [
          "session_phase"
        ],
        "cost_model": "round_turn_cost_v1",
        "horizon": 20,
        "lane": "intraday_range_migration_path_asymmetry",
        "max_trades_per_period": 80,
        "previous_statuses": [
          "FALSIFIED"
        ],
        "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
        "sizing": "fixed_one_contract",
        "structural_id": "intraday_range_migration_path_asymmetry_struct_05",
        "symbol": "MES",
        "threshold_rank": 0,
        "variant_id": "variant_00"
      },
      "component_necessity": {
        "essential_components": [
          "session_phase"
        ],
        "full_pooled_net_pnl": -906.25,
        "harmful_components": [],
        "minimal_stable_formulation": [
          "session_phase"
        ],
        "redundant_components": [],
        "removals": [
          {
            "classification": "ESSENTIAL_ONLY_COMPONENT",
            "component": "session_phase",
            "removed_net_pnl": 0.0
          }
        ]
      },
      "period_null_decisions": {
        "q1": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,delayed_effect,sign_flipped_effect",
          "effect_size": -0.00014401885479318525,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q2": {
          "adjusted_probability": 0.556667,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "decision_reason": "mandatory_null_failed:random_matched_effect,sign_flipped_effect",
          "effect_size": 5.8981535659829776e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 4,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        },
        "q3": {
          "adjusted_probability": 0.39,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "decision_reason": "mandatory_null_failed:block_shuffled_effect,random_matched_effect,sign_flipped_effect",
          "effect_size": 3.8321276648413116e-05,
          "effective_trial_count": 12,
          "event_count": 1500,
          "null_tests": 6,
          "null_tests_passed": 3,
          "passed": false,
          "policy_version": "candidate_null_policy_v1",
          "required_tests": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ],
          "tested_null_set": [
            "random_matched_effect",
            "block_shuffled_effect",
            "delayed_effect",
            "sign_flipped_effect",
            "momentum_baseline_effect",
            "mean_reversion_baseline_effect"
          ]
        }
      },
      "period_results": {
        "q1": {
          "average_loss": -7.805555555555555,
          "average_win": 5.434210526315789,
          "best_day": 28.0,
          "best_trade": 28.0,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "commissions": 288.0,
          "contract_contribution": {
            "MES": -248.0
          },
          "executed_trades": 64,
          "expectancy": -3.875,
          "gross_pnl": 40.0,
          "losing_streak": 8,
          "max_adverse_excursion": -27.5,
          "max_drawdown": 258.25,
          "max_favorable_excursion": 37.5,
          "net_pnl": -248.0,
          "opportunities": 64,
          "period": "q1",
          "profit_factor": 0.2939501779359431,
          "regime_contribution": {
            "normal_vol": -248.0
          },
          "roll_window_contribution": {
            "normal": -235.5,
            "roll_window": -12.5
          },
          "session_contribution": {
            "0": -237.25,
            "23": -10.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.2711864406779661,
          "top_3_trade_profit_pct": 0.48668280871670705,
          "top_5_trade_profit_pct": 0.6295399515738499,
          "trade_count": 64,
          "win_rate": 0.296875,
          "worst_day": -25.75,
          "worst_trade": -25.75
        },
        "q2": {
          "average_loss": -10.425,
          "average_win": 6.5,
          "best_day": 26.75,
          "best_trade": 26.75,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "commissions": 292.5,
          "contract_contribution": {
            "MES": -423.75
          },
          "executed_trades": 65,
          "expectancy": -6.519230769230769,
          "gross_pnl": -131.25,
          "losing_streak": 13,
          "max_adverse_excursion": -67.5,
          "max_drawdown": 423.75,
          "max_favorable_excursion": 35.0,
          "net_pnl": -423.75,
          "opportunities": 65,
          "period": "q2",
          "profit_factor": 0.18705035971223022,
          "regime_contribution": {
            "normal_vol": -423.75
          },
          "roll_window_contribution": {
            "normal": -373.75,
            "roll_window": -50.0
          },
          "session_contribution": {
            "0": -423.75
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.2743589743589744,
          "top_3_trade_profit_pct": 0.4897435897435897,
          "top_5_trade_profit_pct": 0.6538461538461539,
          "trade_count": 65,
          "win_rate": 0.23076923076923078,
          "worst_day": -60.75,
          "worst_trade": -60.75
        },
        "q3": {
          "average_loss": -15.414634146341463,
          "average_win": 15.9,
          "best_day": 70.5,
          "best_trade": 70.5,
          "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
          "commissions": 297.0,
          "contract_contribution": {
            "MES": -234.5
          },
          "executed_trades": 66,
          "expectancy": -3.553030303030303,
          "gross_pnl": 62.5,
          "losing_streak": 9,
          "max_adverse_excursion": -120.0,
          "max_drawdown": 283.25,
          "max_favorable_excursion": 91.25,
          "net_pnl": -234.5,
          "opportunities": 66,
          "period": "q3",
          "profit_factor": 0.6289556962025317,
          "regime_contribution": {
            "high_vol": -22.5,
            "normal_vol": -212.0
          },
          "roll_window_contribution": {
            "normal": -163.25,
            "roll_window": -71.25
          },
          "session_contribution": {
            "0": -234.5
          },
          "slippage": 0.0,
          "top_1_trade_profit_pct": 0.17735849056603772,
          "top_3_trade_profit_pct": 0.36855345911949683,
          "top_5_trade_profit_pct": 0.5157232704402516,
          "trade_count": 66,
          "win_rate": 0.3787878787878788,
          "worst_day": -77.0,
          "worst_trade": -77.0
        }
      },
      "pooled_null_decision": {
        "adjusted_probability": 0.556667,
        "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
        "decision_reason": "mandatory_null_failed:delayed_effect,sign_flipped_effect",
        "effect_size": -0.00010795042920257702,
        "effective_trial_count": 12,
        "event_count": 2500,
        "null_tests": 6,
        "null_tests_passed": 4,
        "passed": false,
        "policy_version": "candidate_null_policy_v1",
        "required_tests": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ],
        "tested_null_set": [
          "random_matched_effect",
          "block_shuffled_effect",
          "delayed_effect",
          "sign_flipped_effect",
          "momentum_baseline_effect",
          "mean_reversion_baseline_effect"
        ]
      },
      "sequential_result": {
        "average_loss": -11.0625,
        "average_win": 10.139830508474576,
        "best_day": 70.5,
        "best_trade": 70.5,
        "commissions": 877.5,
        "expectancy": -4.647435897435898,
        "gross_pnl": -28.75,
        "losing_streak": 13,
        "max_drawdown": 907.75,
        "net_pnl": -906.25,
        "periods": "q1_to_q3",
        "profit_factor": 0.3976404120970422,
        "slippage": 0.0,
        "top_1_trade_profit_pct": 0.11784371082323443,
        "top_3_trade_profit_pct": 0.24488090263267864,
        "top_5_trade_profit_pct": 0.3447555369828667,
        "trade_count": 195,
        "win_rate": 0.30256410256410254,
        "worst_day": -77.0,
        "worst_trade": -77.0
      },
      "temporal_transfer": {
        "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
        "decision_reason": "pooled_negative_or_nulls_failed_or_period_instability",
        "null_period_passes": 0,
        "policy_version": "temporal_transfer_policy_v1",
        "pooled_net_pnl": -906.25,
        "pooled_null_passed": false,
        "positive_periods": 0,
        "status": "TEMPORAL_TRANSFER_FAILED",
        "third_period_catastrophic": false,
        "top_trade_concentration": 0.117844
      },
      "topstep_sequential": null
    }
  ],
  "candidates_frozen": [
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_06_00",
      "components": [
        "accepted_center_migration",
        "time_at_extremes"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "RAW_POSITIVE_NET",
        "RAW_ECONOMIC_SCREEN_PASS",
        "MATCHED_NULL_BEATEN",
        "REPRESENTATION_EVIDENCE_PASS",
        "TOPSTEP_PATH_CANDIDATE",
        "TOPSTEP_COMPATIBLE"
      ],
      "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_06",
      "symbol": "NQ",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "overnight_inventory_rth_resolution_09_00",
      "components": [
        "opening_response",
        "acceptance_rejection"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "overnight_inventory_rth_resolution",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "RAW_POSITIVE_NET",
        "RAW_ECONOMIC_SCREEN_PASS",
        "MATCHED_NULL_BEATEN",
        "REPRESENTATION_EVIDENCE_PASS"
      ],
      "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
      "sizing": "fixed_one_contract",
      "structural_id": "overnight_inventory_rth_resolution_struct_09",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "overnight_inventory_rth_resolution_05_00",
      "components": [
        "regime_context"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "overnight_inventory_rth_resolution",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "RAW_POSITIVE_NET",
        "MATCHED_NULL_BEATEN",
        "REPRESENTATION_EVIDENCE_PASS"
      ],
      "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
      "sizing": "fixed_one_contract",
      "structural_id": "overnight_inventory_rth_resolution_struct_05",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_03_00",
      "components": [
        "path_asymmetry"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "MATCHED_NULL_BEATEN",
        "REPRESENTATION_EVIDENCE_PASS"
      ],
      "replay_role": "REPRESENTATION_EVIDENCE_CANDIDATE",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_03",
      "symbol": "MNQ",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_02_00",
      "components": [
        "effort_vs_progress"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "RAW_POSITIVE_NET",
        "RAW_ECONOMIC_SCREEN_PASS",
        "MATCHED_NULL_BEATEN"
      ],
      "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_02",
      "symbol": "NQ",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "overnight_inventory_rth_resolution_01_00",
      "components": [
        "overnight_participation"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "overnight_inventory_rth_resolution",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "MATCHED_NULL_BEATEN"
      ],
      "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "overnight_inventory_rth_resolution_struct_01",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_00_00",
      "components": [
        "accepted_center_migration"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "MATCHED_NULL_BEATEN"
      ],
      "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_00",
      "symbol": "ES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_09_00",
      "components": [
        "path_asymmetry",
        "range_relocation"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "MATCHED_NULL_BEATEN"
      ],
      "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_09",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "overnight_inventory_rth_resolution_00_00",
      "components": [
        "overnight_displacement"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "overnight_inventory_rth_resolution",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "MATCHED_NULL_BEATEN"
      ],
      "replay_role": "MATCHED_NULL_POSITIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "overnight_inventory_rth_resolution_struct_00",
      "symbol": "ES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_01_00",
      "components": [
        "time_at_extremes"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "FALSIFIED"
      ],
      "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_01",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_04_00",
      "components": [
        "range_relocation"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "FALSIFIED"
      ],
      "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_04",
      "symbol": "ES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    },
    {
      "candidate_id": "intraday_range_migration_path_asymmetry_05_00",
      "components": [
        "session_phase"
      ],
      "cost_model": "round_turn_cost_v1",
      "horizon": 20,
      "lane": "intraday_range_migration_path_asymmetry",
      "max_trades_per_period": 80,
      "previous_statuses": [
        "FALSIFIED"
      ],
      "replay_role": "MATCHED_NULL_NEGATIVE_CONTROL",
      "sizing": "fixed_one_contract",
      "structural_id": "intraday_range_migration_path_asymmetry_struct_05",
      "symbol": "MES",
      "threshold_rank": 0,
      "variant_id": "variant_00"
    }
  ],
  "created_at_utc": "2026-07-10T09:15:42+00:00",
  "future_q4_freeze_recommendations": [],
  "manifest_hash": "97a40215f1454fe31b5b7ddce858f90f7caa1598d09d0f935b0eefcd25870e60",
  "manifest_path": "/root/hydra-bot/reports/temporal_transfer/strict_replay_manifest_20260710T091039+0000_strict_unsampled_temporal_transfer_v1.json",
  "new_databento_requests": [],
  "portfolio_diagnostic": {
    "reason": "fewer_than_two_temporal_transfer_candidates",
    "status": "NOT_APPLICABLE"
  },
  "previous_13_matched_null_passes_interpretation": "13 prototype labels from shared component-level null evidence; not 13 complete candidate-level null-suite passes.",
  "previous_status_semantics": {
    "MATCHED_NULL_BEATEN": "Prototype label assigned when any component in the prototype overlapped a lane-level component-level null comparison that passed.",
    "REPRESENTATION_EVIDENCE_PASS": "Prototype label assigned when the shared component-level null condition was true and sampled Q1-Q3 net was positive in at least two periods.",
    "TOPSTEP_COMPATIBLE": "Prototype label assigned from sampled pooled net PnL above 9000 plus shared component-level null evidence; no candidate-level Topstep replay was performed.",
    "TOPSTEP_PATH_CANDIDATE": "Prototype label assigned from sampled pooled net PnL above 1500 plus shared component-level null evidence.",
    "defect": "Previous labels were not candidate-level complete-null-suite decisions and cannot support Q4 freeze or promotion."
  },
  "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
  "remaining_budget_usd": 96.106305,
  "spend_this_phase_usd": 0.0,
  "status_distribution": {
    "INSUFFICIENT_EVIDENCE": 2,
    "TEMPORAL_TRANSFER_FAILED": 10
  },
  "status_policy_version": "candidate_null_policy_v1",
  "temporal_policy_version": "temporal_transfer_policy_v1",
  "topstep_compatible_after_strict_replay": 0,
  "topstep_replay_count": 0,
  "warning": "Q1-Q3 are development/falsification data. Q4 remains sealed. Historical research only."
}
```
