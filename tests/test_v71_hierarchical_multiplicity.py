from __future__ import annotations

import numpy as np
import pytest

from hydra.validation.v71_hierarchical_multiplicity import (
    family_bh,
    hierarchical_trial_accounting,
)


def test_effective_trials_collapse_correlated_clones_without_erasing_history() -> None:
    base = np.tile(np.array([1.0, -1.0, 1.0, -1.0]), 32)
    matrix = np.vstack([base for _ in range(32)])

    accounting = hierarchical_trial_accounting(
        matrix,
        raw_global_trials=247_892,
    )

    assert accounting.raw_family_candidates == 32
    assert accounting.effective_signal_trials == pytest.approx(1.0)
    assert accounting.campaign_inflated_trials == 2
    assert accounting.global_search_history_penalty == 18
    assert accounting.DSR_N_trials == 20


def test_effective_trials_retain_independent_structures() -> None:
    matrix = np.eye(32)
    accounting = hierarchical_trial_accounting(
        matrix,
        raw_global_trials=247_892,
        prior_family_grammar_versions=2,
    )

    assert accounting.effective_signal_trials > 15.0
    assert accounting.DSR_N_trials > accounting.global_search_history_penalty
    assert accounting.prior_family_grammar_penalty == 2


def test_bh_is_applied_per_family() -> None:
    result = family_bh(
        {
            "A": {"a1": 0.001, "a2": 0.9},
            "B": {"b1": 0.08, "b2": 0.09},
        },
        q=0.10,
    )

    assert result["A"]["a1"]["rejected"] is True
    assert result["A"]["a2"]["rejected"] is False
    assert result["B"]["b1"]["rejected"] is True
    assert result["B"]["b2"]["rejected"] is True
