from __future__ import annotations

from pathlib import Path

from hydra.research.clean_cross_asset_daily_tripwire import (
    BRANCH_ID,
    _best_safe_cell,
    _gate,
    _trade_path_event,
    load_decision_card,
)


def _input() -> dict[str, object]:
    return {
        "event_id": "candidate:20240103",
        "session_day": 20240103,
        "entry_ns": 100,
        "exit_ns": 200,
        "side": 1,
        "gross_one_micro": 100.0,
        "favorable_one_micro": 130.0,
        "adverse_one_micro": -40.0,
        "base_cost_one_micro": 4.0,
        "session_compliant": True,
    }


def test_card_is_self_hashed_and_fail_closed() -> None:
    card = load_decision_card(
        Path("config/research/clean_cross_asset_daily_tripwire_decision_card_v1.json")
    )
    assert card["selected_branch"] == BRANCH_ID
    assert card["smallest_decisive_falsification_experiment"]["promotion_allowed"] is False
    assert card["smallest_decisive_falsification_experiment"]["q4_access_allowed"] is False
    assert card["smallest_decisive_falsification_experiment"]["data_purchase_allowed"] is False


def test_stress_and_direction_flip_preserve_clock_and_change_economics() -> None:
    primary = _trade_path_event(
        _input(), quantity=2, scenario="STRESSED_1_5X", direction_flip=False
    )
    flip = _trade_path_event(
        _input(), quantity=2, scenario="STRESSED_1_5X", direction_flip=True
    )
    assert primary.decision_ns == flip.decision_ns == 100
    assert primary.exit_ns == flip.exit_ns == 200
    assert primary.quantity == flip.quantity == 2
    assert primary.net_pnl == 188.0
    assert flip.net_pnl == -212.0
    assert primary.worst_unrealized_pnl == -86.0
    assert flip.worst_unrealized_pnl == -266.0


def test_gate_requires_headline_passes_blocks_mll_consistency_and_control() -> None:
    summary = {
        "pass_count": 2,
        "blocks_with_passes": ["2024_Q1", "2024_Q2"],
        "mll_breach_rate": 0.0,
        "net_total_usd": 1200.0,
        "all_passing_paths_consistency_compliant": True,
    }
    primary = {
        "horizon_trading_days": 20,
        "normal": dict(summary),
        "stressed": dict(summary),
    }
    control = {"stressed": {**summary, "pass_count": 0, "net_total_usd": -100.0}}
    frozen = {
        "headline_horizon_trading_days": 20,
        "minimum_normal_passes": 2,
        "minimum_stressed_passes": 2,
        "minimum_passing_temporal_blocks": 2,
        "maximum_stressed_mll_breach_rate": 0.1,
    }
    assert _gate(primary, control, frozen)["passed"] is True
    broken = {**primary, "stressed": {**summary, "blocks_with_passes": ["2024_Q1"]}}
    assert _gate(broken, control, frozen)["passed"] is False


def test_safe_cell_rejects_passes_bought_with_excessive_mll() -> None:
    base = {
        "account_size_usd": 50_000,
        "account_label": "50K",
        "micro_quantity": 1,
        "horizon_trading_days": 20,
        "normal": {
            "pass_count": 0,
            "mll_breach_rate": 0.0,
            "all_passing_paths_consistency_compliant": True,
        },
        "stressed": {
            "pass_count": 0,
            "mll_breach_rate": 0.0,
            "target_progress_median": 0.20,
            "net_total_usd": 100.0,
            "all_passing_paths_consistency_compliant": True,
        },
    }
    unsafe = {
        **base,
        "micro_quantity": 8,
        "normal": {**base["normal"], "pass_count": 2, "mll_breach_rate": 0.25},
        "stressed": {
            **base["stressed"],
            "pass_count": 2,
            "mll_breach_rate": 0.30,
            "target_progress_median": 0.80,
        },
    }
    selected = _best_safe_cell(
        [unsafe, base],
        headline_horizon_trading_days=20,
        maximum_mll_breach_rate=0.10,
    )
    assert selected["micro_quantity"] == 1
