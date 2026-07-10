# Representation Ablation

```json
{
  "intraday_range_migration_path_asymmetry": {
    "best_probability_beats_null": 0.8888888888888888,
    "component_counts": {
      "accepted_center_migration": 21,
      "effort_vs_progress": 21,
      "path_asymmetry": 21,
      "range_relocation": 21,
      "session_phase": 21,
      "time_at_extremes": 21
    },
    "incremental_components": [
      "accepted_center_migration",
      "session_phase",
      "time_at_extremes",
      "path_asymmetry",
      "range_relocation",
      "effort_vs_progress"
    ],
    "incremental_value_tests": 18,
    "matched_null_beaten_tests": 18,
    "period_counts": {
      "q1": 22,
      "q2": 22,
      "q3": 22
    },
    "tests": 66,
    "top_examples": [
      {
        "ablation_id": "eb7d51e9faac1d3e",
        "components": [
          "accepted_center_migration"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.00040703496541924375
      },
      {
        "ablation_id": "fa4c105cd496294b",
        "components": [
          "effort_vs_progress"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0008083329824856565
      },
      {
        "ablation_id": "688f4d5fc843a660",
        "components": [
          "path_asymmetry"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0002196371749695192
      },
      {
        "ablation_id": "98a1b4ab4394a9aa",
        "components": [
          "accepted_center_migration",
          "effort_vs_progress"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0004946396684469441
      },
      {
        "ablation_id": "49535e4bc9a2bd3a",
        "components": [
          "accepted_center_migration",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0003745959347110428
      },
      {
        "ablation_id": "4016fb6985d7da7a",
        "components": [
          "time_at_extremes",
          "effort_vs_progress"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0005995875876986734
      },
      {
        "ablation_id": "43dd18f4282a79bf",
        "components": [
          "effort_vs_progress",
          "path_asymmetry"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0004939906959969882
      },
      {
        "ablation_id": "69fede620416f4fa",
        "components": [
          "effort_vs_progress",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.000682689567094894
      },
      {
        "ablation_id": "8e39a20d937914ec",
        "components": [
          "path_asymmetry",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0003871782116743545
      },
      {
        "ablation_id": "100ac113dbcd3811",
        "components": [
          "accepted_center_migration",
          "time_at_extremes",
          "effort_vs_progress",
          "path_asymmetry",
          "range_relocation",
          "session_phase"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.00044682021867077637
      },
      {
        "ablation_id": "69fede620416f4fa",
        "components": [
          "effort_vs_progress",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q3",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0006305760233564079
      },
      {
        "ablation_id": "8e39a20d937914ec",
        "components": [
          "path_asymmetry",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q3",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.00015943274262263763
      },
      {
        "ablation_id": "f945e24c2cedcf21",
        "components": [
          "path_asymmetry",
          "session_phase"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q3",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.00045951800569259496
      },
      {
        "ablation_id": "fa4c105cd496294b",
        "components": [
          "effort_vs_progress"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0008828337769993054
      },
      {
        "ablation_id": "11eafa05e7552e55",
        "components": [
          "session_phase"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0006343583940268858
      },
      {
        "ablation_id": "0655898e8a40a1be",
        "components": [
          "time_at_extremes",
          "path_asymmetry"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.00023818078151148424
      },
      {
        "ablation_id": "43dd18f4282a79bf",
        "components": [
          "effort_vs_progress",
          "path_asymmetry"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0005001334963620549
      },
      {
        "ablation_id": "8e39a20d937914ec",
        "components": [
          "path_asymmetry",
          "range_relocation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0002139538470909357
      },
      {
        "ablation_id": "4bfca5fb0c44fa6c",
        "components": [
          "time_at_extremes"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.00040344770615198945
      },
      {
        "ablation_id": "ab5734e8761c50a5",
        "components": [
          "accepted_center_migration",
          "time_at_extremes"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.00035259742757983494
      }
    ]
  },
  "overnight_inventory_rth_resolution": {
    "best_probability_beats_null": 0.8888888888888888,
    "component_counts": {
      "acceptance_rejection": 21,
      "opening_response": 21,
      "overnight_displacement": 21,
      "overnight_participation": 21,
      "prior_value_position": 21,
      "regime_context": 21
    },
    "incremental_components": [
      "overnight_displacement",
      "overnight_participation",
      "prior_value_position",
      "acceptance_rejection",
      "regime_context",
      "opening_response"
    ],
    "incremental_value_tests": 17,
    "matched_null_beaten_tests": 17,
    "period_counts": {
      "q1": 22,
      "q2": 22,
      "q3": 22
    },
    "tests": 66,
    "top_examples": [
      {
        "ablation_id": "4f7f492a23e4d61e",
        "components": [
          "overnight_displacement"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.000569254969781893
      },
      {
        "ablation_id": "3c5bda98e3e7076d",
        "components": [
          "overnight_participation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0008332747109724758
      },
      {
        "ablation_id": "bf26eccc633d71fb",
        "components": [
          "prior_value_position"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0007889703441354701
      },
      {
        "ablation_id": "85bf9cb9ef28e232",
        "components": [
          "acceptance_rejection"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0007889703441354701
      },
      {
        "ablation_id": "b3146999e709dffb",
        "components": [
          "regime_context"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0008808880903153828
      },
      {
        "ablation_id": "0bb4c3575602ac98",
        "components": [
          "overnight_displacement",
          "overnight_participation"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0007098847958722023
      },
      {
        "ablation_id": "bad9f7e68cccd009",
        "components": [
          "overnight_displacement",
          "regime_context"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0006518303512455688
      },
      {
        "ablation_id": "cedd75bdacba4d64",
        "components": [
          "overnight_displacement",
          "overnight_participation",
          "prior_value_position",
          "opening_response",
          "acceptance_rejection",
          "regime_context"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0006019695622886212
      },
      {
        "ablation_id": "b3146999e709dffb",
        "components": [
          "regime_context"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0008430522508867897
      },
      {
        "ablation_id": "f0b194c6a2232528",
        "components": [
          "prior_value_position",
          "regime_context"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0005081900842958031
      },
      {
        "ablation_id": "7f8f539bc3442e79",
        "components": [
          "overnight_participation",
          "prior_value_position"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q3",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": 0.0005259890969670176
      },
      {
        "ablation_id": "fa3cc904e081fadd",
        "components": [
          "prior_value_position",
          "acceptance_rejection"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q3",
        "probability_beats_matched_null": 0.8888888888888888,
        "real_effect": -0.0004345668987541307
      },
      {
        "ablation_id": "bd78c649db66d32a",
        "components": [
          "prior_value_position",
          "opening_response"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0004594888993698769
      },
      {
        "ablation_id": "fa3cc904e081fadd",
        "components": [
          "prior_value_position",
          "acceptance_rejection"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0007889703441354701
      },
      {
        "ablation_id": "efc80e624526956d",
        "components": [
          "opening_response",
          "acceptance_rejection"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0003835751559746019
      },
      {
        "ablation_id": "973f26d208ab0145",
        "components": [
          "opening_response",
          "regime_context"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q1",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0004817351402951513
      },
      {
        "ablation_id": "4f7f492a23e4d61e",
        "components": [
          "overnight_displacement"
        ],
        "incremental_value": true,
        "matched_null_status": "MATCHED_NULL_BEATEN",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0006100089496519475
      },
      {
        "ablation_id": "3c5bda98e3e7076d",
        "components": [
          "overnight_participation"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0007696061218823722
      },
      {
        "ablation_id": "0bb4c3575602ac98",
        "components": [
          "overnight_displacement",
          "overnight_participation"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": -0.0007253978183212579
      },
      {
        "ablation_id": "af6f99747ff6969c",
        "components": [
          "overnight_displacement",
          "opening_response"
        ],
        "incremental_value": false,
        "matched_null_status": "FALSIFIED",
        "period": "q2",
        "probability_beats_matched_null": 0.7777777777777778,
        "real_effect": 0.0010463013140793542
      }
    ]
  }
}
```
