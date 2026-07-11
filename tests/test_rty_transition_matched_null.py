from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hydra.research.rty_transition_matched_null import (
    RTYTransitionMatchedNullError,
    match_covariates_only,
    matched_validator_controls,
    paired_sign_flip_probability,
)


def _covariates() -> pd.DataFrame:
    rows = []
    for index in range(8):
        treated = index < 4
        rows.append(
            {
                "session_id": f"2024-01-{index + 2:02d}",
                "calendar_quarter": "2024Q1",
                "absolute_trend_ratio": 1.0 + 0.1 * (index % 4),
                "source_range_ratio": 1.1 + 0.05 * (index % 4),
                "source_prior_close_location": 0.1 * (index % 4),
                "session_ordinal_within_quarter": index % 4,
                "treatment": treated,
            }
        )
    return pd.DataFrame(rows)


def test_matching_is_deterministic_outcome_blind_and_without_reuse() -> None:
    frame = _covariates()

    first = match_covariates_only(frame, caliper=2.0)
    second = match_covariates_only(frame, caliper=2.0)

    pd.testing.assert_frame_equal(first, second)
    assert len(first) == 4
    assert first["control_session_id"].is_unique
    assert (first["distance"] <= 2.0).all()


def test_matching_rejects_any_outcome_column() -> None:
    frame = _covariates().assign(net_pnl=100.0)

    with pytest.raises(RTYTransitionMatchedNullError, match="Outcome columns"):
        match_covariates_only(frame)


def test_caliper_fails_closed_instead_of_forcing_pairs() -> None:
    frame = _covariates()
    frame.loc[~frame["treatment"], "absolute_trend_ratio"] += 100.0

    pairs = match_covariates_only(frame, caliper=0.01)

    assert pairs.empty


def test_paired_sign_flip_is_deterministic_and_detects_positive_effect() -> None:
    positive = np.linspace(0.5, 2.0, 40)
    null = np.array([-1.0, 1.0] * 20)

    assert paired_sign_flip_probability(positive) <= 0.01
    assert paired_sign_flip_probability(positive) == paired_sign_flip_probability(
        positive
    )
    assert paired_sign_flip_probability(null) == 1.0


def test_matched_validator_controls_are_calibrated() -> None:
    controls = matched_validator_controls()

    assert controls["negative_control_passed"] is True
    assert controls["injected_control_passed"] is True
    assert controls["passed"] is True

