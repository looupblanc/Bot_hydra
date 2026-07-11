from __future__ import annotations

import numpy as np

from hydra.calibration.single_primary_alpha import (
    _primary_probability,
    _summaries,
    simulate_primary_condition,
)


def test_primary_probability_detects_large_positive_effect() -> None:
    probability, net, flipped = _primary_probability(
        np.full(80, 1.0),
        draws=512,
        rng=np.random.default_rng(7),
    )

    assert probability < 0.01
    assert net > 0
    assert flipped < 0


def test_single_primary_simulation_is_deterministic() -> None:
    first = simulate_primary_condition(
        event_count=80,
        standardized_net_effect=0.40,
        replications=30,
        null_draws=128,
        seed=81,
    )
    second = simulate_primary_condition(
        event_count=80,
        standardized_net_effect=0.40,
        replications=30,
        null_draws=128,
        seed=81,
    )

    assert first == second
    assert set(first["alphas"]) == {"0.01", "0.02", "0.025", "0.03", "0.04"}


def test_summary_never_marks_alpha_when_power_constraint_fails() -> None:
    conditions = []
    for event_count in (80, 120, 360):
        for effect in (0.0, 0.25, 0.40):
            conditions.append(
                {
                    "event_count": event_count,
                    "standardized_net_effect": effect,
                    "alphas": {
                        str(alpha): {
                            "admission_rate": 0.01 if effect == 0 else 0.50,
                            "interval_95": [0.0, 0.03],
                        }
                        for alpha in (0.01, 0.02, 0.025, 0.03, 0.04)
                    },
                }
            )

    assert not any(row["constraints_passed"] for row in _summaries(conditions))
