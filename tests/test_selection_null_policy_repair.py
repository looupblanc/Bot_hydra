from __future__ import annotations

import numpy as np

from hydra.calibration.selection_null_policy_repair import (
    _holm_rejections,
    policy_decisions,
    simulate_policy_condition,
)


def test_holm_is_step_down_and_monotonic() -> None:
    assert _holm_rejections(np.asarray([0.04, 0.001, 0.01]), 0.05).tolist() == [
        True,
        True,
        True,
    ]
    assert not _holm_rejections(np.asarray([0.03, 0.04, 0.05]), 0.05).any()


def test_single_primary_never_promotes_diagnostic_elites() -> None:
    probabilities = np.full(20, 0.001)
    economic = np.ones(20, dtype=bool)

    decisions = policy_decisions(probabilities, economic)

    primary = decisions["SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05"]
    family = decisions["FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01"]
    assert primary.sum() == 1 and primary[0]
    assert family.sum() == 5 and family[:5].all()


def test_policy_simulation_is_deterministic() -> None:
    first = simulate_policy_condition(
        event_count=80,
        standardized_net_effect=0.40,
        replications=20,
        null_draws=128,
        seed=991,
    )
    second = simulate_policy_condition(
        event_count=80,
        standardized_net_effect=0.40,
        replications=20,
        null_draws=128,
        seed=991,
    )

    assert first == second
    assert set(first["policies"]) == {
        "BH_Q_0_20_BASELINE",
        "BH_Q_0_05",
        "HOLM_FWER_0_05",
        "SINGLE_PRESELECTED_PRIMARY_ALPHA_0_05",
        "FIVE_FAMILY_PRIMARIES_BONFERRONI_0_01",
    }
