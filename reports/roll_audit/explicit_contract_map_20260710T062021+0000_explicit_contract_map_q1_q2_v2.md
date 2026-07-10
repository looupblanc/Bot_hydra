# Explicit Contract Map explicit_contract_map_q1_q2_v2

Historical research only. No live trading approval.

```json
{
  "budget_summary_path": "/root/hydra-bot/reports/data_budget/databento_budget_summary.md",
  "continuous_mapping": {
    "ES.c.0": [
      {
        "d0": "2024-01-01",
        "d1": "2024-03-17",
        "s": "17077"
      },
      {
        "d0": "2024-03-17",
        "d1": "2024-06-23",
        "s": "5602"
      },
      {
        "d0": "2024-06-23",
        "d1": "2024-07-01",
        "s": "118"
      }
    ],
    "MES.c.0": [
      {
        "d0": "2024-01-01",
        "d1": "2024-03-17",
        "s": "763"
      },
      {
        "d0": "2024-03-17",
        "d1": "2024-06-23",
        "s": "13804"
      },
      {
        "d0": "2024-06-23",
        "d1": "2024-07-01",
        "s": "7114"
      }
    ],
    "MNQ.c.0": [
      {
        "d0": "2024-01-01",
        "d1": "2024-03-17",
        "s": "7101"
      },
      {
        "d0": "2024-03-17",
        "d1": "2024-06-23",
        "s": "10"
      },
      {
        "d0": "2024-06-23",
        "d1": "2024-07-01",
        "s": "13941"
      }
    ],
    "NQ.c.0": [
      {
        "d0": "2024-01-01",
        "d1": "2024-03-17",
        "s": "750"
      },
      {
        "d0": "2024-03-17",
        "d1": "2024-06-23",
        "s": "13743"
      },
      {
        "d0": "2024-06-23",
        "d1": "2024-07-01",
        "s": "4358"
      }
    ]
  },
  "contract_map_hash": "ecee1e2e1a91511b5bee5dd8fcf49a22ef07bc5e2d6ccc01d9ffb0ad4dec2d90",
  "contract_map_path": "/root/hydra-bot/data/cache/contract_maps/roll_map_GLBX-MDP3_ohlcv-1m_ecee1e2e1a91511b.json",
  "contracts": [
    {
      "activation_time": "2021-12-17T14:30:00+00:00",
      "active_end": "2024-03-17",
      "active_start": "2024-01-01",
      "continuous_symbol": "ES.c.0",
      "contract": "ESH4",
      "contract_multiplier": 50.0,
      "deactivation_time": "2024-03-17",
      "expiry_date": "2024-03-15",
      "instrument_id": "17077",
      "is_micro": false,
      "last_trade_date": "2024-03-15",
      "month_code": "H",
      "parent_symbol": "ES",
      "point_value": 50.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "ES",
      "tick_size": 0.25,
      "tick_value": 12.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2022-03-18T13:30:00+00:00",
      "active_end": "2024-06-23",
      "active_start": "2024-03-17",
      "continuous_symbol": "ES.c.0",
      "contract": "ESM4",
      "contract_multiplier": 50.0,
      "deactivation_time": "2024-06-23",
      "expiry_date": "2024-06-21",
      "instrument_id": "5602",
      "is_micro": false,
      "last_trade_date": "2024-06-21",
      "month_code": "M",
      "parent_symbol": "ES",
      "point_value": 50.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "ES",
      "tick_size": 0.25,
      "tick_value": 12.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2022-06-17T13:30:00+00:00",
      "active_end": "2024-07-01",
      "active_start": "2024-06-23",
      "continuous_symbol": "ES.c.0",
      "contract": "ESU4",
      "contract_multiplier": 50.0,
      "deactivation_time": "2024-07-01",
      "expiry_date": "2024-09-20",
      "instrument_id": "118",
      "is_micro": false,
      "last_trade_date": "2024-09-20",
      "month_code": "U",
      "parent_symbol": "ES",
      "point_value": 50.0,
      "price_discontinuity": null,
      "roll_date": "2024-06-23",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "ES",
      "tick_size": 0.25,
      "tick_value": 12.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2022-12-16T14:30:00+00:00",
      "active_end": "2024-03-17",
      "active_start": "2024-01-01",
      "continuous_symbol": "MES.c.0",
      "contract": "MESH4",
      "contract_multiplier": 5.0,
      "deactivation_time": "2024-03-17",
      "expiry_date": "2024-03-15",
      "instrument_id": "763",
      "is_micro": true,
      "last_trade_date": "2024-03-15",
      "month_code": "H",
      "parent_symbol": "MES",
      "point_value": 5.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MES",
      "tick_size": 0.25,
      "tick_value": 1.25,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-03-17T13:30:00+00:00",
      "active_end": "2024-06-23",
      "active_start": "2024-03-17",
      "continuous_symbol": "MES.c.0",
      "contract": "MESM4",
      "contract_multiplier": 5.0,
      "deactivation_time": "2024-06-23",
      "expiry_date": "2024-06-21",
      "instrument_id": "13804",
      "is_micro": true,
      "last_trade_date": "2024-06-21",
      "month_code": "M",
      "parent_symbol": "MES",
      "point_value": 5.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MES",
      "tick_size": 0.25,
      "tick_value": 1.25,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-06-16T13:30:00+00:00",
      "active_end": "2024-07-01",
      "active_start": "2024-06-23",
      "continuous_symbol": "MES.c.0",
      "contract": "MESU4",
      "contract_multiplier": 5.0,
      "deactivation_time": "2024-07-01",
      "expiry_date": "2024-09-20",
      "instrument_id": "7114",
      "is_micro": true,
      "last_trade_date": "2024-09-20",
      "month_code": "U",
      "parent_symbol": "MES",
      "point_value": 5.0,
      "price_discontinuity": null,
      "roll_date": "2024-06-23",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MES",
      "tick_size": 0.25,
      "tick_value": 1.25,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2022-12-16T14:30:00+00:00",
      "active_end": "2024-03-17",
      "active_start": "2024-01-01",
      "continuous_symbol": "MNQ.c.0",
      "contract": "MNQH4",
      "contract_multiplier": 2.0,
      "deactivation_time": "2024-03-17",
      "expiry_date": "2024-03-15",
      "instrument_id": "7101",
      "is_micro": true,
      "last_trade_date": "2024-03-15",
      "month_code": "H",
      "parent_symbol": "MNQ",
      "point_value": 2.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MNQ",
      "tick_size": 0.25,
      "tick_value": 0.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-03-17T13:30:00+00:00",
      "active_end": "2024-06-23",
      "active_start": "2024-03-17",
      "continuous_symbol": "MNQ.c.0",
      "contract": "MNQM4",
      "contract_multiplier": 2.0,
      "deactivation_time": "2024-06-23",
      "expiry_date": "2024-06-21",
      "instrument_id": "10",
      "is_micro": true,
      "last_trade_date": "2024-06-21",
      "month_code": "M",
      "parent_symbol": "MNQ",
      "point_value": 2.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MNQ",
      "tick_size": 0.25,
      "tick_value": 0.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-06-16T13:30:00+00:00",
      "active_end": "2024-07-01",
      "active_start": "2024-06-23",
      "continuous_symbol": "MNQ.c.0",
      "contract": "MNQU4",
      "contract_multiplier": 2.0,
      "deactivation_time": "2024-07-01",
      "expiry_date": "2024-09-20",
      "instrument_id": "13941",
      "is_micro": true,
      "last_trade_date": "2024-09-20",
      "month_code": "U",
      "parent_symbol": "MNQ",
      "point_value": 2.0,
      "price_discontinuity": null,
      "roll_date": "2024-06-23",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "MNQ",
      "tick_size": 0.25,
      "tick_value": 0.5,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2022-12-16T14:30:00+00:00",
      "active_end": "2024-03-17",
      "active_start": "2024-01-01",
      "continuous_symbol": "NQ.c.0",
      "contract": "NQH4",
      "contract_multiplier": 20.0,
      "deactivation_time": "2024-03-17",
      "expiry_date": "2024-03-15",
      "instrument_id": "750",
      "is_micro": false,
      "last_trade_date": "2024-03-15",
      "month_code": "H",
      "parent_symbol": "NQ",
      "point_value": 20.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "NQ",
      "tick_size": 0.25,
      "tick_value": 5.0,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-03-17T13:30:00+00:00",
      "active_end": "2024-06-23",
      "active_start": "2024-03-17",
      "continuous_symbol": "NQ.c.0",
      "contract": "NQM4",
      "contract_multiplier": 20.0,
      "deactivation_time": "2024-06-23",
      "expiry_date": "2024-06-21",
      "instrument_id": "13743",
      "is_micro": false,
      "last_trade_date": "2024-06-21",
      "month_code": "M",
      "parent_symbol": "NQ",
      "point_value": 20.0,
      "price_discontinuity": null,
      "roll_date": "2024-03-17",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "NQ",
      "tick_size": 0.25,
      "tick_value": 5.0,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    },
    {
      "activation_time": "2023-06-16T13:30:00+00:00",
      "active_end": "2024-07-01",
      "active_start": "2024-06-23",
      "continuous_symbol": "NQ.c.0",
      "contract": "NQU4",
      "contract_multiplier": 20.0,
      "deactivation_time": "2024-07-01",
      "expiry_date": "2024-09-20",
      "instrument_id": "4358",
      "is_micro": false,
      "last_trade_date": "2024-09-20",
      "month_code": "U",
      "parent_symbol": "NQ",
      "point_value": 20.0,
      "price_discontinuity": null,
      "roll_date": "2024-06-23",
      "roll_reason": "databento_continuous_front_contract_transition",
      "root": "NQ",
      "tick_size": 0.25,
      "tick_value": 5.0,
      "transition_uncertainty": "date_level_symbology_interval",
      "volume_migration_ratio": null,
      "year": 2024
    }
  ],
  "created_at": "2026-07-10T06:20:21+00:00",
  "databento_requests": {
    "definition_timeseries_request": {
      "actual_cost_usd": 0.0,
      "checksum": "14d31bac14212f70eac6fdbbf70c6fb74e9273132d659323ef4c240123555139",
      "download_status": "CACHE_HIT",
      "estimated_cost_usd": 0.0,
      "parsed_path": "/root/hydra-bot/data/cache/contract_maps/definitions_GLBX-MDP3_2024-01-01_2024-07-01_7b550d0d281ff16a.json",
      "raw_path": "/root/hydra-bot/data/cache/contract_maps/definitions_GLBX-MDP3_2024-01-01_2024-07-01_7b550d0d281ff16a.dbn.zst"
    },
    "symbology_continuous_to_instrument_id": true,
    "symbology_instrument_id_to_raw_symbol": true
  },
  "dataset": "GLBX.MDP3",
  "definition_cache": {
    "actual_cost_usd": 0.0,
    "checksum": "14d31bac14212f70eac6fdbbf70c6fb74e9273132d659323ef4c240123555139",
    "download_status": "CACHE_HIT",
    "estimated_cost_usd": 0.0,
    "parsed_path": "/root/hydra-bot/data/cache/contract_maps/definitions_GLBX-MDP3_2024-01-01_2024-07-01_7b550d0d281ff16a.json",
    "raw_path": "/root/hydra-bot/data/cache/contract_maps/definitions_GLBX-MDP3_2024-01-01_2024-07-01_7b550d0d281ff16a.dbn.zst"
  },
  "new_databento_purchase": false,
  "pair_synchronization": {
    "MNQ_MES": {
      "invalid_count": 36,
      "maturity_mismatch_count": 0,
      "pair": [
        "MNQ",
        "MES"
      ],
      "pair_validity_rate": 0.928,
      "roll_transition_exclusion_count": 36,
      "samples": [
        {
          "left_contract": "MNQH4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESH4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T08:19:00+00:00"
        },
        {
          "left_contract": "MNQH4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESH4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T19:56:00+00:00"
        },
        {
          "left_contract": "MNQH4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESH4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-15T10:35:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-18T06:43:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-18T18:20:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-19T06:57:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-19T18:34:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-20T07:12:00+00:00"
        },
        {
          "left_contract": "MNQM4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESM4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-20T18:49:00+00:00"
        },
        {
          "left_contract": "MNQH4",
          "left_symbol": "MNQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "MESH4",
          "right_symbol": "MES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T05:52:00+00:00"
        }
      ],
      "timestamps_checked": 500,
      "valid_count": 464
    },
    "NQ_ES": {
      "invalid_count": 36,
      "maturity_mismatch_count": 0,
      "pair": [
        "NQ",
        "ES"
      ],
      "pair_validity_rate": 0.928,
      "roll_transition_exclusion_count": 36,
      "samples": [
        {
          "left_contract": "NQH4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESH4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T05:15:00+00:00"
        },
        {
          "left_contract": "NQH4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESH4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T17:00:00+00:00"
        },
        {
          "left_contract": "NQH4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESH4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-15T08:06:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-18T04:18:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-18T15:53:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-19T04:28:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-19T16:03:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-20T04:40:00+00:00"
        },
        {
          "left_contract": "NQM4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESM4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-20T16:17:00+00:00"
        },
        {
          "left_contract": "NQH4",
          "left_symbol": "NQ",
          "pair_valid": false,
          "reason": "roll_transition_exclusion",
          "residual_time_to_expiry_days": 0,
          "right_contract": "ESH4",
          "right_symbol": "ES",
          "roll_transition_exclusion": true,
          "synchronized_quarterly_maturity": true,
          "timestamp": "2024-03-14T00:32:00+00:00"
        }
      ],
      "timestamps_checked": 500,
      "valid_count": 464
    }
  },
  "period": {
    "end": "2024-07-01",
    "start": "2024-01-01"
  },
  "raw_symbol_mapping": {
    "10": "MNQM4",
    "118": "ESU4",
    "13743": "NQM4",
    "13804": "MESM4",
    "13941": "MNQU4",
    "17077": "ESH4",
    "4358": "NQU4",
    "5602": "ESM4",
    "7101": "MNQH4",
    "7114": "MESU4",
    "750": "NQH4",
    "763": "MESH4"
  },
  "roll_audit": {
    "explicit_contract_metadata_available": true,
    "roll_artifact_suspected": false,
    "roll_map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
    "symbols": {
      "ES": {
        "active_contracts": {
          "ESH4": 73292,
          "ESM4": 93652,
          "ESU4": 7020
        },
        "bars": 173964,
        "gap_suspected": false,
        "max_abs_return_in_unsafe_window": 0.013452,
        "unsafe_roll_bars": 12338,
        "volume_discontinuity_ratio": 176.652632
      },
      "MES": {
        "active_contracts": {
          "MESH4": 73359,
          "MESM4": 93801,
          "MESU4": 7017
        },
        "bars": 174177,
        "gap_suspected": false,
        "max_abs_return_in_unsafe_window": 0.01336,
        "unsafe_roll_bars": 12543,
        "volume_discontinuity_ratio": 208.44
      },
      "MNQ": {
        "active_contracts": {
          "MNQH4": 73583,
          "MNQM4": 94109,
          "MNQU4": 7020
        },
        "bars": 174712,
        "gap_suspected": false,
        "max_abs_return_in_unsafe_window": 0.01219,
        "unsafe_roll_bars": 12843,
        "volume_discontinuity_ratio": 106.051724
      },
      "NQ": {
        "active_contracts": {
          "NQH4": 73187,
          "NQM4": 93709,
          "NQU4": 7020
        },
        "bars": 173916,
        "gap_suspected": false,
        "max_abs_return_in_unsafe_window": 0.01292,
        "unsafe_roll_bars": 12205,
        "volume_discontinuity_ratio": 132.25
      }
    }
  },
  "roll_map_status": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
  "rule_proxy_comparison": {
    "by_symbol": {
      "ES": {
        "disagreement_count": 4,
        "disagreement_rate": 0.008,
        "samples": [
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 05:51:00+00:00"
          },
          {
            "new_contract": "ESH4",
            "old_contract": "ESM4",
            "symbol": "ES",
            "timestamp": "2024-03-15 12:15:00+00:00"
          },
          {
            "new_contract": "ESM4",
            "old_contract": "ESU4",
            "symbol": "ES",
            "timestamp": "2024-06-21 05:03:00+00:00"
          },
          {
            "new_contract": "ESM4",
            "old_contract": "ESU4",
            "symbol": "ES",
            "timestamp": "2024-06-21 12:53:00+00:00"
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
            "timestamp": "2024-03-15 01:29:00+00:00"
          },
          {
            "new_contract": "MESH4",
            "old_contract": "MESM4",
            "symbol": "MES",
            "timestamp": "2024-03-15 08:48:00+00:00"
          },
          {
            "new_contract": "MESM4",
            "old_contract": "MESU4",
            "symbol": "MES",
            "timestamp": "2024-06-21 04:31:00+00:00"
          },
          {
            "new_contract": "MESM4",
            "old_contract": "MESU4",
            "symbol": "MES",
            "timestamp": "2024-06-21 11:26:00+00:00"
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
            "timestamp": "2024-03-15 02:47:00+00:00"
          },
          {
            "new_contract": "MNQH4",
            "old_contract": "MNQM4",
            "symbol": "MNQ",
            "timestamp": "2024-03-15 08:37:00+00:00"
          },
          {
            "new_contract": "MNQM4",
            "old_contract": "MNQU4",
            "symbol": "MNQ",
            "timestamp": "2024-06-21 04:32:00+00:00"
          },
          {
            "new_contract": "MNQM4",
            "old_contract": "MNQU4",
            "symbol": "MNQ",
            "timestamp": "2024-06-21 10:38:00+00:00"
          }
        ],
        "timestamps_checked": 500
      },
      "NQ": {
        "disagreement_count": 2,
        "disagreement_rate": 0.004,
        "samples": [
          {
            "new_contract": "NQH4",
            "old_contract": "NQM4",
            "symbol": "NQ",
            "timestamp": "2024-03-15 07:11:00+00:00"
          },
          {
            "new_contract": "NQM4",
            "old_contract": "NQU4",
            "symbol": "NQ",
            "timestamp": "2024-06-21 07:09:00+00:00"
          }
        ],
        "timestamps_checked": 500
      }
    },
    "disagreement_count": 14,
    "disagreement_rate": 0.007,
    "new_map_type": "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DEFINITIONS",
    "old_map_type": "RULE_BASED_CME_EQUITY_INDEX_QUARTERLY_PROXY",
    "samples": [
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 05:51:00+00:00"
      },
      {
        "new_contract": "ESH4",
        "old_contract": "ESM4",
        "symbol": "ES",
        "timestamp": "2024-03-15 12:15:00+00:00"
      },
      {
        "new_contract": "ESM4",
        "old_contract": "ESU4",
        "symbol": "ES",
        "timestamp": "2024-06-21 05:03:00+00:00"
      },
      {
        "new_contract": "ESM4",
        "old_contract": "ESU4",
        "symbol": "ES",
        "timestamp": "2024-06-21 12:53:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 01:29:00+00:00"
      },
      {
        "new_contract": "MESH4",
        "old_contract": "MESM4",
        "symbol": "MES",
        "timestamp": "2024-03-15 08:48:00+00:00"
      },
      {
        "new_contract": "MESM4",
        "old_contract": "MESU4",
        "symbol": "MES",
        "timestamp": "2024-06-21 04:31:00+00:00"
      },
      {
        "new_contract": "MESM4",
        "old_contract": "MESU4",
        "symbol": "MES",
        "timestamp": "2024-06-21 11:26:00+00:00"
      },
      {
        "new_contract": "NQH4",
        "old_contract": "NQM4",
        "symbol": "NQ",
        "timestamp": "2024-03-15 07:11:00+00:00"
      },
      {
        "new_contract": "NQM4",
        "old_contract": "NQU4",
        "symbol": "NQ",
        "timestamp": "2024-06-21 07:09:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 02:47:00+00:00"
      },
      {
        "new_contract": "MNQH4",
        "old_contract": "MNQM4",
        "symbol": "MNQ",
        "timestamp": "2024-03-15 08:37:00+00:00"
      },
      {
        "new_contract": "MNQM4",
        "old_contract": "MNQU4",
        "symbol": "MNQ",
        "timestamp": "2024-06-21 04:32:00+00:00"
      },
      {
        "new_contract": "MNQM4",
        "old_contract": "MNQU4",
        "symbol": "MNQ",
        "timestamp": "2024-06-21 10:38:00+00:00"
      }
    ],
    "timestamps_checked": 2000
  },
  "schema": "ohlcv-1m",
  "spend_this_phase_usd": 0.0,
  "symbols": [
    "ES",
    "MES",
    "NQ",
    "MNQ"
  ]
}
```
