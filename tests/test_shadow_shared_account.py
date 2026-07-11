from __future__ import annotations

from pathlib import Path

import pandas as pd

from hydra.portfolio.shadow_shared_account import (
    YM_ID,
    conservative_shared_daily_path,
    normalize_candidate_trades,
    pairwise_interactions,
    select_basket_roles,
)


def test_normalization_filters_exact_micro_and_development_period() -> None:
    rows = [
        {
            "symbol": "YM",
            "decision_timestamp": "2024-01-02T14:31:00Z",
            "exit_timestamp_60": "2024-01-02T15:30:00Z",
            "event_session_id": "2024-01-02",
            "side": 1,
            "cost": 14.5,
            "net_pnl_60": 100.0,
            "mae_dollars": -50.0,
        },
        {
            "symbol": "MYM",
            "decision_timestamp": "2024-01-02T14:31:00Z",
            "exit_timestamp_60": "2024-01-02T15:30:00Z",
            "event_session_id": "2024-01-02",
            "side": 1,
            "cost": 3.0,
            "net_pnl_60": 10.0,
            "mae_dollars": -5.0,
        },
        {
            "symbol": "MYM",
            "decision_timestamp": "2024-10-01T14:31:00Z",
            "exit_timestamp_60": "2024-10-01T15:30:00Z",
            "event_session_id": "2024-10-01",
            "side": -1,
            "cost": 3.0,
            "net_pnl_60": 999.0,
            "mae_dollars": -5.0,
        },
    ]

    normalized = normalize_candidate_trades(YM_ID, rows)

    assert len(normalized) == 1
    assert normalized.iloc[0]["symbol"] == "MYM"
    assert normalized.iloc[0]["underlying"] == "YM"
    assert normalized.iloc[0]["net_pnl"] == 10.0


def test_conservative_path_coincides_overlapping_adverse_excursions() -> None:
    events = pd.DataFrame(
        [
            {
                "trade_id": "a",
                "event_session_id": "2024-01-02",
                "entry_timestamp": pd.Timestamp("2024-01-02T14:00:00Z"),
                "exit_timestamp": pd.Timestamp("2024-01-02T15:00:00Z"),
                "net_pnl": 100.0,
                "mae_dollars": -50.0,
            },
            {
                "trade_id": "b",
                "event_session_id": "2024-01-02",
                "entry_timestamp": pd.Timestamp("2024-01-02T14:30:00Z"),
                "exit_timestamp": pd.Timestamp("2024-01-02T15:30:00Z"),
                "net_pnl": -20.0,
                "mae_dollars": -80.0,
            },
        ]
    )

    daily = conservative_shared_daily_path(events)

    assert len(daily) == 1
    assert daily.iloc[0]["pnl"] == 80.0
    assert daily.iloc[0]["worst_intraday_pnl"] == -130.0
    assert daily.iloc[0]["trades"] == 2


def test_role_selection_uses_three_distinct_executable_baskets() -> None:
    evaluations = []
    for index in range(4):
        evaluations.append(
            {
                "basket_id": f"basket-{index}",
                "candidate_ids": [f"candidate-{index}", f"candidate-{index + 1}"],
                "executable": True,
                "cost_stress_1_5x_net": 100.0 + index,
                "maximum_absolute_daily_correlation": 0.4 - index * 0.05,
                "shared_account_combine": {
                    "min_mll_buffer": 2000.0 + index,
                    "total_profit": 200.0 + index,
                },
            }
        )

    selected = select_basket_roles(evaluations)

    assert len(selected) == 3
    assert len({row["basket_id"] for row in selected}) == 3
    assert {row["role"] for row in selected} == {
        "maximum_mll_survival",
        "balanced_progress",
        "low_correlation_diversity",
    }


def test_joint_tail_does_not_count_mutually_inactive_zero_days() -> None:
    left = pd.DataFrame(
        [
            {
                "entry_timestamp": pd.Timestamp("2024-01-02T14:00:00Z"),
                "exit_timestamp": pd.Timestamp("2024-01-02T15:00:00Z"),
                "event_session_id": "2024-01-02",
                "underlying": "YM",
                "side": 1.0,
                "net_pnl": -10.0,
            }
        ]
    )
    right = pd.DataFrame(
        [
            {
                "entry_timestamp": pd.Timestamp("2024-01-03T14:00:00Z"),
                "exit_timestamp": pd.Timestamp("2024-01-03T15:00:00Z"),
                "event_session_id": "2024-01-03",
                "underlying": "NQ",
                "side": 1.0,
                "net_pnl": -20.0,
            }
        ]
    )

    interaction = pairwise_interactions({"left": left, "right": right})[0]

    assert interaction["joint_tail_days"] == 0
    assert interaction["shared_loss_days"] == 0


def test_task_prohibits_standalone_payout_sum_and_orders() -> None:
    task = Path(
        "reports/engineering/hydra_shadow_shared_account_baskets_20260711.md"
    ).read_text(encoding="utf-8")

    assert "Do not sum standalone payouts" in task
    assert "one shared MLL" in task
    assert "order paths" in task
    assert "not `PAPER_SHADOW_READY`" in task
