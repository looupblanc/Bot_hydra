from __future__ import annotations

import numpy as np

from hydra.calibration.selection_null_power import (
    _family_block_probabilities,
    _synthetic_gross_returns,
    simulate_condition,
)


def test_selection_calibration_is_deterministic_and_injected_power_improves() -> None:
    null_first = simulate_condition(
        event_count=80,
        standardized_net_effect=0.0,
        replications=30,
        null_draws=128,
        seed=17,
    )
    null_second = simulate_condition(
        event_count=80,
        standardized_net_effect=0.0,
        replications=30,
        null_draws=128,
        seed=17,
    )
    injected = simulate_condition(
        event_count=80,
        standardized_net_effect=0.40,
        replications=30,
        null_draws=128,
        seed=17,
    )

    assert null_first == null_second
    assert injected["injected_candidate_power"] > 0
    assert 0 <= null_first["family_false_admission_rate"] <= 1
    assert 0 <= null_first["per_null_candidate_false_admission_rate"] <= 1


def test_block_probability_uses_candidate_specific_blocks_and_costs() -> None:
    gross = np.zeros((20, 10), dtype=float)
    gross[0] = 2.0
    probabilities, net = _family_block_probabilities(
        gross,
        cost=0.05,
        draws=256,
        rng=np.random.default_rng(123),
    )

    assert probabilities.shape == (20,)
    assert net.shape == (20,)
    assert probabilities[0] < probabilities[1]
    assert net[0] > 0 > net[1]


def test_negative_controls_have_zero_gross_effect_before_costs() -> None:
    gross = _synthetic_gross_returns(
        np.random.default_rng(91),
        event_count=10_000,
        net_effects=np.zeros(20),
    )

    assert abs(float(gross.mean())) < 0.02
