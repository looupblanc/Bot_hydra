from __future__ import annotations

from hydra.research.near_pass_bank_salvage import _pass_cells, _recovery_verdict


def _candidate(normal_pass: int, normal_mll: int, stressed_pass: int, stressed_mll: int):
    scenario = {
        "pass_count": 0,
        "mll_breach_count": 0,
        "full_coverage_start_count": 1,
    }
    summaries = {
        "NORMAL": {str(h): dict(scenario) for h in (5, 10, 20)},
        "STRESSED_1_5X": {str(h): dict(scenario) for h in (5, 10, 20)},
    }
    summaries["NORMAL"]["10"].update(
        pass_count=normal_pass, mll_breach_count=normal_mll
    )
    summaries["STRESSED_1_5X"]["10"].update(
        pass_count=stressed_pass, mll_breach_count=stressed_mll
    )
    return {
        "account_results": [{
            "account_label": "50K",
            "selected_profile_id": "pnl_state_guarded",
            "base_integer_quantity_tier": 4,
            "all_block_result": {"summaries": summaries},
        }]
    }


def test_normal_pass_is_not_erased_by_missing_stressed_pass() -> None:
    cells = _pass_cells(_candidate(1, 0, 0, 0))
    assert len(cells) == 1
    assert cells[0]["classification_status"] == "COMBINE_PASS_OBSERVED_DEVELOPMENT"
    assert cells[0]["stress_pass_required_for_status"] is False


def test_normal_mll_is_a_hard_rejection() -> None:
    assert _pass_cells(_candidate(1, 1, 1, 0)) == []


def test_stressed_mll_is_a_hard_rejection() -> None:
    assert _pass_cells(_candidate(1, 0, 1, 1)) == []


def test_partial_recovery_has_exact_exhaustion_verdict() -> None:
    assert _recovery_verdict(1) == "INVENTORY_NEAR_PASS_EXHAUSTED"
    assert _recovery_verdict(6) == "TARGETED_NEAR_PASS_RECOVERY_COMPLETE"
