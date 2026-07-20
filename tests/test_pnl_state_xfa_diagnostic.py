from __future__ import annotations

from hydra.research.pnl_state_xfa_diagnostic import (
    _aggregate_paths,
    _continuation_days,
    _post_payout_survival,
)


def test_continuation_days_stops_before_first_unavailable_day() -> None:
    assert _continuation_days(
        (1, 2, 3, 4, 5, 6), after_day=2, unavailable=frozenset({5})
    ) == (3, 4)


def test_standard_and_consistency_ev_are_aggregated_separately() -> None:
    handoffs = [
        {
            "policy_id": "policy-a",
            "horizon_trading_days": 5,
            "normal_full_coverage_start_count": 10,
        }
    ]
    records = [
        {
            "policy_id": "policy-a",
            "horizon_trading_days": 5,
            "path": "STANDARD",
            "first_payout_count": 1,
            "payout_cycles": 1,
            "trader_net_payout_usd": 900.0,
            "minimum_mll_buffer_usd": 100.0,
            "terminal": "DATA_CENSORED",
        },
        {
            "policy_id": "policy-a",
            "horizon_trading_days": 5,
            "path": "CONSISTENCY",
            "first_payout_count": 1,
            "payout_cycles": 2,
            "trader_net_payout_usd": 1_800.0,
            "minimum_mll_buffer_usd": 50.0,
            "terminal": "DATA_CENSORED",
        },
    ]
    result = _aggregate_paths(records, handoffs)
    assert len(result) == 2
    by_path = {row["path"]: row for row in result}
    assert by_path["STANDARD"]["expected_trader_payout_per_new_combine_attempt_usd"] == 90.0
    assert by_path["CONSISTENCY"]["expected_trader_payout_per_new_combine_attempt_usd"] == 180.0
    assert all(row["alternative_value_not_additive"] is True for row in result)


def test_post_payout_survival_separates_failures_from_censoring() -> None:
    alternatives = [
        {
            "first_payout_day": 5,
            "observed_days": 40,
            "terminal": "DATA_CENSORED",
        },
        {
            "first_payout_day": 5,
            "observed_days": 35,
            "terminal": "MLL_BREACHED",
        },
        {
            "first_payout_day": 5,
            "observed_days": 70,
            "terminal": "DATA_CENSORED",
        },
        {
            "first_payout_day": None,
            "observed_days": 90,
            "terminal": "DATA_CENSORED",
        },
    ]
    result = _post_payout_survival(alternatives)
    assert result["first_payout_path_count"] == 3
    assert result["checkpoints"]["30"] == {
        "survived_count": 2,
        "failed_before_or_on_checkpoint_count": 1,
        "data_censored_before_checkpoint_count": 0,
        "evaluable_count": 3,
        "survival_rate_among_evaluable": 2 / 3,
        "demonstrated_survival_rate_all_first_payout_paths": 2 / 3,
    }
    assert result["checkpoints"]["60"]["survived_count"] == 1
    assert result["checkpoints"]["60"]["data_censored_before_checkpoint_count"] == 1
    assert result["checkpoints"]["90"]["survived_count"] == 0
    assert result["checkpoints"]["90"]["data_censored_before_checkpoint_count"] == 2
