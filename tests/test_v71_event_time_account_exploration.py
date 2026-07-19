from __future__ import annotations

from hydra.execution.v7_cost_model import CostStress
from hydra.production.v71_event_time_account_exploration import (
    G4_CANDIDATE_ID,
    G6_CANDIDATE_IDS,
    build_complete_rolling_start_grid,
    evaluate_candidate_frontier,
    run_v71_event_time_account_exploration,
)
from hydra.propfirm.combine_episode import TradePathEvent


def _event(day: int, *, scenario: str, net: float) -> TradePathEvent:
    return TradePathEvent(
        event_id=f"candidate:{day}:{scenario}",
        decision_ns=day * 1_000_000,
        exit_ns=day * 1_000_000 + 100,
        session_day=day,
        net_pnl=net,
        gross_pnl=1_000.0,
        worst_unrealized_pnl=-50.0,
        best_unrealized_pnl=max(net, 0.0),
        quantity=1,
        mini_equivalent=1.0,
    )


def _rule() -> dict[str, object]:
    return {
        "account_label": "50K",
        "account_size_usd": 50_000,
        "profit_target_usd": 3_000,
        "maximum_loss_limit_usd": 2_000,
        "maximum_mini_contracts": 5,
        "consistency_target_fraction": 0.5,
        "minimum_trading_days": 2,
        "optional_daily_loss_limit_usd": 1_000,
    }


def test_complete_grid_uses_only_full_year_local_windows() -> None:
    grid = build_complete_rolling_start_grid(
        {2023: tuple(range(1, 11)), 2024: tuple(range(101, 107))}
    )

    assert len(grid[5]) == 8
    assert len(grid[10]) == 1
    assert len(grid[20]) == 0
    assert grid[5][0] == (1, 2023)
    assert grid[5][-1] == (102, 2024)


def test_exact_frontier_runs_normal_and_stressed_chronologically() -> None:
    calendar = tuple(range(1, 21))
    normal = tuple(_event(day, scenario="BASE", net=1_000.0) for day in calendar)
    stressed = tuple(
        _event(day, scenario="STRESS_1_5X", net=800.0) for day in calendar
    )
    starts = {horizon: ((1, 2023),) for horizon in (5, 10, 20)}

    cells, replay_count = evaluate_candidate_frontier(
        candidate_id="candidate",
        scenario_events={
            CostStress.BASE.value: normal,
            CostStress.STRESS_1_5X.value: stressed,
        },
        calendar=calendar,
        starts=starts,
        rule=_rule(),
        quantities=(1,),
    )

    assert replay_count == 6
    assert len(cells) == 3
    assert cells[0]["normal"]["pass_count"] == 1
    assert cells[0]["stressed"]["pass_count"] == 1
    assert cells[0]["normal"]["mll_breach_count"] == 0
    assert cells[0]["normal"]["consistency_compliance_rate"] == 1.0
    assert cells[0]["evidence_tier"] == "E"
    assert cells[0]["promotion_status"] is None


def test_real_frozen_population_is_bounded_read_only_tier_e() -> None:
    result = run_v71_event_time_account_exploration(
        ".", maximum_quantity_per_account=1
    )

    assert result["status"] == "COMPLETE_BOUNDED_READ_ONLY_EVENT_TIME_EXPLORATION"
    assert result["source_population"]["selected_candidate_ids"] == [
        G4_CANDIDATE_ID,
        *G6_CANDIDATE_IDS,
    ]
    assert result["source_population"]["integration"]["G6"] == (
        "G6_INTEGRATED_BOUNDED_DEVELOPMENT_ONLY"
    )
    assert result["source_population"]["g3_or_g5_candidate_count"] == 0
    assert result["evidence_tier"] == "E"
    assert result["promotion_status"] is None
    assert result["counters"]["candidate_count"] == 3
    assert result["counters"]["exact_chronological_account_replays"] > 0
    assert result["counters"]["data_purchase_count"] == 0
    assert result["counters"]["q4_access_count_delta"] == 0
    assert result["counters"]["broker_connections"] == 0
    assert result["counters"]["orders"] == 0
    assert all(
        cell["evidence_tier"] == "E" and cell["promotion_status"] is None
        for candidate in result["candidate_results"]
        for account in candidate["account_size_matrix"]
        for cell in account["frontier"]
    )
