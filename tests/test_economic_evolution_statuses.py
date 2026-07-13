from __future__ import annotations

from types import SimpleNamespace

import pytest

from hydra.economic_evolution.statuses import rolling_research_status


GATE = {
    "minimum_pass_count": 1,
    "maximum_mll_breach_rate": 0.20,
    "minimum_stressed_median_net": 0.0,
    "minimum_consistency_pass_rate": 0.50,
}


def _result(*, passes: int) -> SimpleNamespace:
    return SimpleNamespace(
        controlled_base=SimpleNamespace(
            pass_count=passes,
            mll_breach_rate=0.0,
            consistency_pass_rate=0.75,
        ),
        controlled_stress_1_5x=SimpleNamespace(median_episode_net_pnl=500.0),
    )


def test_rolling_replay_does_not_invent_research_candidate_status() -> None:
    assert (
        rolling_research_status(
            _result(passes=0),
            GATE,
            fallback_status="ACCOUNT_POLICY_DIAGNOSTIC_ONLY",
        )
        == "ACCOUNT_POLICY_DIAGNOSTIC_ONLY"
    )


def test_rolling_replay_promotes_only_an_actual_combine_path() -> None:
    assert (
        rolling_research_status(
            _result(passes=1),
            GATE,
            fallback_status="ACCOUNT_POLICY_DIAGNOSTIC_ONLY",
        )
        == "COMBINE_PATH_CANDIDATE"
    )


def test_rolling_replay_rejects_unknown_fallback_status() -> None:
    with pytest.raises(ValueError, match="invalid rolling fallback status"):
        rolling_research_status(
            _result(passes=0), GATE, fallback_status="VALIDATED"
        )
