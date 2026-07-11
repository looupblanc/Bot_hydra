from __future__ import annotations

from collections import Counter

import pandas as pd

from hydra.research.accelerated_context_tournament import (
    _apply_context,
    _context_shadow_specification,
    _round_gate,
    generate_executable_hypotheses,
)


def test_executable_population_is_exact_balanced_and_deterministic() -> None:
    first = generate_executable_hypotheses()
    second = generate_executable_hypotheses()
    markets = Counter(item["market"] for item in first)
    families = Counter(item["mechanism_family"] for item in first)

    assert first == second
    assert len(first) == 300
    assert len({item["candidate_id"] for item in first}) == 300
    assert len({item["structural_fingerprint"] for item in first}) == 300
    assert set(markets.values()) == {50}
    assert max(families.values()) / len(first) <= 0.25
    assert all(item["candidate_id"].endswith("_v2") for item in first)


def test_next_executable_batch_is_new_version_and_structurally_disjoint() -> None:
    first = generate_executable_hypotheses(0)
    second = generate_executable_hypotheses(1)

    assert len(second) == 300
    assert all(item["candidate_id"].endswith("_v3") for item in second)
    assert not (
        {item["structural_fingerprint"] for item in first}
        & {item["structural_fingerprint"] for item in second}
    )


def test_context_join_uses_only_completed_higher_timeframe_bar() -> None:
    timestamps = pd.date_range("2024-01-02T00:00:00Z", periods=20, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": "ES",
            "active_contract": "ESH4",
            "open": range(20),
            "high": [value + 1 for value in range(20)],
            "low": [value - 1 for value in range(20)],
            "close": [value + 0.5 for value in range(20)],
            "volume": 1.0,
        }
    )
    events = pd.DataFrame(
        {
            "symbol": ["ES"],
            "active_contract": ["ESH4"],
            "decision_timestamp": [pd.Timestamp("2024-01-02T00:11:00Z")],
            "entry_timestamp": [pd.Timestamp("2024-01-02T00:12:00Z")],
            "side": [1],
        }
    )

    selected = _apply_context(
        events, frame, "completed_5m_trend_agree", {}
    )

    assert len(selected) == 1
    assert selected.loc[0, "context_availability_timestamp"] == pd.Timestamp(
        "2024-01-02T00:10:00Z"
    )
    assert (
        selected["context_availability_timestamp"] <= selected["decision_timestamp"]
    ).all()


def test_round_gate_and_shadow_spec_preserve_fail_closed_context() -> None:
    metrics = {
        "events": 12,
        "net_pnl": 100.0,
        "cost_stress_1_5x_net": 50.0,
        "best_positive_event_share": 0.2,
        "finite": True,
    }
    hypothesis = next(
        item
        for item in generate_executable_hypotheses()
        if item["activation_context"] != "none"
    )
    specification = _context_shadow_specification(hypothesis, "a" * 64)

    assert _round_gate(metrics, minimum_events=10, maximum_concentration=0.4)
    specification.validate()
    assert len(specification.timeframes) == 2
    assert specification.entry_rules["missing_context_policy"] == "fail_closed_skip_signal"
    assert not specification.outbound_orders_enabled
