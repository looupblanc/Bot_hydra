# HYDRA Validator Calibration

Historical research only. No live trading approval.

```json
{
  "attack_classification": {
    "best_event_removed": "ROBUSTNESS_DIAGNOSTIC",
    "block_shuffled_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "cost_stress": "ROBUSTNESS_DIAGNOSTIC",
    "delayed_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "event_time_jitter": "ROBUSTNESS_DIAGNOSTIC",
    "lookahead": "FATAL_MANDATORY",
    "mean_reversion_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "momentum_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "opportunity_count_matched_random": "FATAL_MANDATORY",
    "placebo_market": "INFORMATIONAL_ONLY",
    "session_only_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "sign_flipped_signal": "HYPOTHESIS_SPECIFIC_MANDATORY",
    "target_leakage": "FATAL_MANDATORY",
    "volatility_only_baseline": "HYPOTHESIS_SPECIFIC_MANDATORY"
  },
  "cost_policy": {
    "atom_monetizability_buffer": 0.0,
    "atom_statistical_hurdle_multiplier": 0.1,
    "defensive_atom_cost_mode": "risk_information_not_direct_round_trip_cost",
    "policy_version": "atom_cost_hurdle_calibration_v1",
    "strategy_execution_cost_required": false
  },
  "created_at_utc": "2026-07-10T10:34:53+00:00",
  "false_positive_rate": 0.0,
  "negative_controls": [
    {
      "control_id": "independent_noise",
      "effect": 0.00012725468092356502,
      "expected_positive": false,
      "null_effect": 6.01993228206122e-05,
      "observations": 720,
      "passed": false,
      "reason": "null_control_rejected",
      "signal_to_noise": 0.9348018699880414
    },
    {
      "control_id": "autocorrelated_no_edge",
      "effect": -2.2465111352922847e-05,
      "expected_positive": false,
      "null_effect": 4.973674568574817e-05,
      "observations": 668,
      "passed": false,
      "reason": "null_control_rejected",
      "signal_to_noise": 0.15911882685704093
    },
    {
      "control_id": "random_session_effect",
      "effect": -2.863361317215543e-05,
      "expected_positive": false,
      "null_effect": 0.00021881853925627052,
      "observations": 314,
      "passed": false,
      "reason": "null_control_rejected",
      "signal_to_noise": 0.14498947883918495
    },
    {
      "control_id": "block_shuffled_real_returns",
      "effect": 8.28326929059491e-05,
      "expected_positive": false,
      "null_effect": 0.00017525833639698722,
      "observations": 2987,
      "passed": false,
      "reason": "null_control_rejected",
      "signal_to_noise": 1.2360392332594496
    },
    {
      "control_id": "opportunity_matched_random",
      "effect": -3.0643850935929695e-06,
      "expected_positive": false,
      "null_effect": 0.0003676955128358409,
      "observations": 732,
      "passed": false,
      "reason": "null_control_rejected",
      "signal_to_noise": 0.023292426101921032
    }
  ],
  "passed": true,
  "positive_controls": [
    {
      "control_id": "mean_shift_strong",
      "effect": 0.003255220706000429,
      "expected_positive": true,
      "null_effect": 3.70639068071406e-05,
      "observations": 733,
      "passed": true,
      "reason": "known_injected_effect_detected",
      "signal_to_noise": 23.262652195812176
    },
    {
      "control_id": "path_asymmetry_medium",
      "effect": 0.0014904915586690525,
      "expected_positive": true,
      "null_effect": -0.00044655198383840625,
      "observations": 587,
      "passed": true,
      "reason": "known_injected_effect_detected",
      "signal_to_noise": 8.05227370893133
    },
    {
      "control_id": "tail_risk_defensive",
      "effect": 0.002903057000404244,
      "expected_positive": true,
      "null_effect": 0.0002968489451072422,
      "observations": 588,
      "passed": true,
      "reason": "known_injected_effect_detected",
      "signal_to_noise": 17.34057371372311
    },
    {
      "control_id": "volatility_prediction",
      "effect": -0.00024317137452237796,
      "expected_positive": true,
      "null_effect": 0.00045768385251396417,
      "observations": 704,
      "passed": false,
      "reason": "known_injected_effect_missed",
      "signal_to_noise": 1.3758097404340068
    },
    {
      "control_id": "regime_conditional",
      "effect": 0.004308181319061185,
      "expected_positive": true,
      "null_effect": 0.0005308442256158376,
      "observations": 538,
      "passed": true,
      "reason": "known_injected_effect_detected",
      "signal_to_noise": 23.354191641217856
    }
  ],
  "power_on_meaningful_effects": 0.8,
  "precision": 1.0,
  "recall": 0.8,
  "version": "validator_calibration_v1",
  "zero_pass_diagnosis": {
    "adversarial_passes": 0,
    "atoms_reported": 120,
    "attack_policy_diagnosis": "Previous validator required all listed attacks for all atom families; calibrated policy separates fatal, hypothesis-specific, diagnostic, and informational attacks.",
    "cause": "MULTIPLE_CAUSES_COST_HURDLE_AND_OVERSTRICT_ATTACK_POLICY",
    "common_failed_attacks": [
      [
        "session_only_baseline",
        25
      ],
      [
        "cost_stress",
        17
      ],
      [
        "block_shuffled_signal",
        16
      ],
      [
        "event_time_jitter",
        15
      ],
      [
        "delayed_signal",
        12
      ],
      [
        "volatility_only_baseline",
        2
      ],
      [
        "momentum_baseline",
        2
      ],
      [
        "mean_reversion_baseline",
        2
      ]
    ],
    "cost_hurdle_failures_in_top_atoms": 25,
    "cost_policy_diagnosis": "Previous atom-stage hurdle compared raw atom effects against an executable strategy-like cost envelope; calibrated policy separates atom statistical evidence from strategy execution cost.",
    "direction_failures_in_top_atoms": 8,
    "status": "audited",
    "top_atoms_audited": 25
  }
}
```
