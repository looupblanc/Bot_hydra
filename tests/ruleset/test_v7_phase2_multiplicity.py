from __future__ import annotations

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.validation.v7_phase2_multiplicity import (
    behavioral_clusters,
    benjamini_hochberg,
    deflated_sharpe_statistics,
    select_representatives,
    stress_trade,
)


def test_deflated_sharpe_uses_full_trial_count_and_sign() -> None:
    positive = np.tile(np.asarray([3.0, 2.0, 4.0, 1.0]), 100)
    negative = -positive

    good = deflated_sharpe_statistics(positive, n_trials=246_706)
    bad = deflated_sharpe_statistics(negative, n_trials=246_706)

    assert good["deflated_z"] > 0.0
    assert good["DSR_probability"] > 0.5
    assert bad["deflated_z"] < 0.0
    assert bad["one_sided_p_value"] > 0.5


def test_bh_is_deterministic_and_controls_family() -> None:
    result = benjamini_hochberg(
        {"b": 0.03, "a": 0.001, "c": 0.20}, q=0.10
    )

    assert result["a"]["rank"] == 1
    assert result["a"]["rejected"] is True
    assert result["b"]["rejected"] is True
    assert result["c"]["rejected"] is False


def test_behavioral_clustering_merges_equivalent_paths() -> None:
    ids = ["a", "b", "c"]
    matrix = np.asarray(
        [
            [1.0, -1.0, 2.0, -2.0],
            [2.0, -2.0, 4.0, -4.0],
            [-1.0, 1.0, -2.0, 2.0],
        ]
    )

    correlations, clusters = behavioral_clusters(ids, matrix, cut_distance=0.3)

    assert correlations[0, 1] == 1.0
    assert len(clusters) == 2
    assert clusters[0]["member_ids"] == ["a", "b"]
    assert clusters[1]["member_ids"] == ["c"]


def test_cost_stress_deducts_only_incremental_frozen_cost() -> None:
    trade = RoutedTrade(
        component_id="c",
        market="ES",
        side=1,
        event=TradePathEvent(
            event_id="event",
            decision_ns=1,
            exit_ns=2,
            session_day=1,
            net_pnl=90.0,
            gross_pnl=100.0,
            worst_unrealized_pnl=-30.0,
            best_unrealized_pnl=120.0,
            quantity=1,
            mini_equivalent=1.0,
        ),
    )

    stressed = stress_trade(trade, 2.0)

    assert stressed.event.net_pnl == 80.0
    assert stressed.event.worst_unrealized_pnl == -40.0
    assert stressed.event.best_unrealized_pnl == 110.0


def test_representative_selection_keeps_one_per_cluster() -> None:
    clusters = [
        {"cluster_id": "one", "member_ids": ["a", "b"]},
        {"cluster_id": "two", "member_ids": ["c"]},
    ]
    rows = [
        {
            "policy_id": "a",
            "promotion_eligible": True,
            "DSR": {"deflated_z": 3.0},
            "walk_forward": {"pooled_expectancy_per_trade_1_5x": 5.0},
            "cost_stress": {"maximum_drawdown_2x": 10.0},
        },
        {
            "policy_id": "b",
            "promotion_eligible": True,
            "DSR": {"deflated_z": 2.0},
            "walk_forward": {"pooled_expectancy_per_trade_1_5x": 6.0},
            "cost_stress": {"maximum_drawdown_2x": 9.0},
        },
        {
            "policy_id": "c",
            "promotion_eligible": True,
            "DSR": {"deflated_z": 1.0},
            "walk_forward": {"pooled_expectancy_per_trade_1_5x": 4.0},
            "cost_stress": {"maximum_drawdown_2x": 8.0},
        },
    ]

    assert select_representatives(rows, clusters, maximum=3) == ["a", "c"]
