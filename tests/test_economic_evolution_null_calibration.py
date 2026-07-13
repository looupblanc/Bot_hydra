from __future__ import annotations

from hydra.economic_evolution.incremental_value import IncrementalValuePolicy
from hydra.economic_evolution.null_calibration import calibrate_incremental_validator


def _policy() -> IncrementalValuePolicy:
    return IncrementalValuePolicy(
        minimum_matched_starts=16,
        minimum_independent_blocks=4,
        minimum_stressed_net_uplift=50.0,
        minimum_target_progress_uplift=0.005,
        minimum_mll_breach_reduction=0.05,
        minimum_consistency_uplift=0.10,
        minimum_shared_loss_day_reduction=0.5,
        maximum_net_sacrifice_for_defensive_role=100.0,
        maximum_cost_increase=50.0,
        minimum_positive_block_fraction=0.75,
    )


def test_fixed_incremental_validator_rejects_null_and_retains_injected_power() -> None:
    first = calibrate_incremental_validator(
        _policy(),
        seed=20_260_713,
        repetitions=256,
        starts_per_block=4,
        noise_scale=50.0,
    )
    second = calibrate_incremental_validator(
        _policy(),
        seed=20_260_713,
        repetitions=256,
        starts_per_block=4,
        noise_scale=50.0,
    )

    assert first == second
    assert first.null_false_positive_rate <= 0.10
    assert first.meaningful_effect_power >= 0.80
    assert first.thresholds_changed is False
