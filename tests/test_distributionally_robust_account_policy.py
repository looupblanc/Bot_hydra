from __future__ import annotations

from hydra.research.distributionally_robust_account_policy import (
    SUBSTANTIAL_LOWER_QUARTILE_PROGRESS,
    _decision,
    _select_memberships,
)


def _result(*, net: float, p25: float, passes: int = 0, mll: int = 0):
    summary = {
        "pass_count": passes,
        "target_progress_p25": p25,
        "target_progress_median": p25,
        "net_total_usd": net,
        "minimum_mll_buffer_usd": 1000.0,
        "mll_breach_count": mll,
        "terminal_distribution": {"TIMEOUT": 1},
    }
    return {
        "evaluation": {
            "summaries": {
                "NORMAL": {"5": summary},
                "STRESSED_1_5X": {"5": dict(summary)},
            }
        }
    }


def test_g_requires_passes_in_two_eras_and_positive_stress_everywhere():
    rows = [
        _result(net=10.0, p25=0.2, passes=1),
        _result(net=11.0, p25=0.2, passes=1),
        _result(net=12.0, p25=0.2, passes=0),
    ]
    value = _decision(rows, 5)
    assert value["status"] == "TIER_G_DISTRIBUTIONALLY_ROBUST_DEVELOPMENT_BOOK"

    rows[-1] = _result(net=-1.0, p25=0.2, passes=0)
    assert _decision(rows, 5)["status"] == "DRO_GATE_REJECTED"


def test_progress_only_never_inherits_a_tier():
    rows = [
        _result(net=10.0, p25=SUBSTANTIAL_LOWER_QUARTILE_PROGRESS),
        _result(net=11.0, p25=0.2),
        _result(net=12.0, p25=0.3),
    ]
    value = _decision(rows, 5)
    assert value["status"] == "ROBUST_PROGRESS_ONLY_NO_PROMOTION"
    assert value["evidence_tier"] is None


def test_metadata_beam_excludes_closed_memberships(monkeypatch):
    inventory = {
        f"c{index:02d}": {
            "niches": {
                "markets": ["NQ" if index % 3 else "CL"],
                "sessions": [index % 3],
                "mechanisms": [f"m{index % 7}"],
                "timeframes": [f"t{index % 5}"],
                "holding_horizons": [str(index % 4)],
            }
        }
        for index in range(33)
    }
    monkeypatch.setattr(
        "hydra.research.distributionally_robust_account_policy._closed_memberships",
        lambda: {("c00", "c01")},
    )
    selected = _select_memberships(inventory)
    assert len(selected) == 40
    assert len(set(selected)) == 40
    assert ("c00", "c01") not in selected
