from __future__ import annotations

import pandas as pd
import pytest

from hydra.production import autonomous_event_time_matched_controls as controls
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.v71_event_mechanism_grammar import V71Signal


def _event() -> TradePathEvent:
    return TradePathEvent(
        event_id="candidate:100:BASE",
        decision_ns=100,
        exit_ns=200,
        session_day=1,
        gross_pnl=125.0,
        net_pnl=100.0,
        worst_unrealized_pnl=-75.0,
        best_unrealized_pnl=175.0,
        quantity=1,
        mini_equivalent=1.0,
    )


def _signal(position: int, side: int) -> V71Signal:
    minute = position * 60_000_000_000
    return V71Signal(
        candidate_id=controls.FROZEN_CANDIDATE_ID,
        family_id="TEST",
        motif="TEST",
        response_policy="CONTINUATION",
        holding_minutes=60,
        calendar_year=2024,
        session_day="2024-01-02",
        source_position=position,
        availability_ns=minute,
        decision_ns=minute,
        entry_minute_start_ns=minute,
        exit_minute_start_ns=minute + 60 * 60_000_000_000,
        side=side,
        contract="ESH4",
        feature_snapshot_hash=f"source-{position}",
    )


def _states() -> pd.DataFrame:
    minute = 60_000_000_000
    rows = []
    for position in range(181):
        decision = (position + 1) * minute
        rows.append(
            {
                "contract": "ESH4",
                "session_day": "2024-01-02",
                "calendar_year": 2024,
                "minute_start_ns": position * minute,
                "availability_ns": decision,
                "entry_ns_60": decision,
                "exit_ns_60": decision + 60 * minute,
                "executable_60": True,
            }
        )
    return pd.DataFrame(rows)


def test_direction_flip_keeps_cost_timing_and_exposure() -> None:
    source = _event()
    flipped = controls._direction_flip_event(source, "DIRECTION_FLIP")
    assert flipped.decision_ns == source.decision_ns
    assert flipped.exit_ns == source.exit_ns
    assert flipped.quantity == source.quantity
    assert flipped.mini_equivalent == source.mini_equivalent
    assert source.gross_pnl - source.net_pnl == pytest.approx(
        flipped.gross_pnl - flipped.net_pnl
    )
    assert flipped.gross_pnl == -source.gross_pnl
    assert flipped.net_pnl == pytest.approx(-150.0)
    assert flipped.worst_unrealized_pnl == pytest.approx(-225.0)
    assert flipped.best_unrealized_pnl == pytest.approx(25.0)


def test_matched_signal_controls_are_deterministic_and_count_matched() -> None:
    source = (_signal(10, 1), _signal(80, -1), _signal(150, 1))
    first, audit = controls._matched_signal_controls(source, _states())
    second, second_audit = controls._matched_signal_controls(source, _states())
    assert first == second
    assert audit == second_audit
    assert audit["source_signal_count"] == 3
    assert audit["session_count"] == 1
    assert audit["session_and_exposure_count_matched"] is True
    permuted = first["SESSION_EXPOSURE_MATCHED_DIRECTION_PERMUTATION"]
    timing = first["SESSION_MATCHED_TIMING_GRID"]
    assert sorted(row.side for row in permuted) == sorted(row.side for row in source)
    assert [row.decision_ns for row in permuted] == [
        row.decision_ns for row in source
    ]
    assert len(timing) == len(source)
    assert [row.side for row in timing] == [row.side for row in source]
    assert all(
        right.decision_ns >= left.exit_minute_start_ns
        for left, right in zip(timing, timing[1:], strict=False)
    )


def test_control_verdict_requires_two_passing_heldout_blocks() -> None:
    summary = {
        "pass_count": 2,
        "net_total_usd": 5_000.0,
        "mll_breach_rate": 0.0,
        "by_block": {
            "B3": {"pass_count": 2},
            "B4": {"pass_count": 0},
        },
    }
    primary = {
        "HELD_OUT_DEVELOPMENT": {"10": {"STRESS_1_5X": summary}}
    }
    control_summary = replace_dict(
        summary, pass_count=0, net_total_usd=-1_000.0
    )
    arms = {
        "PRIMARY": {"evaluation": primary},
        **{
            control_id: {
                "evaluation": {
                    "HELD_OUT_DEVELOPMENT": {
                        "10": {"STRESS_1_5X": control_summary}
                    }
                }
            }
            for control_id in controls.CONTROL_IDS
        },
    }
    assert controls._control_verdict(primary, arms) == (
        "EVENT_TIME_MATCHED_CONTROLS_NOT_DISTINCT"
    )
    primary["HELD_OUT_DEVELOPMENT"]["10"]["STRESS_1_5X"]["by_block"][
        "B4"
    ]["pass_count"] = 1
    assert controls._control_verdict(primary, arms) == (
        "EVENT_TIME_CONTROL_DISTINCT_DEVELOPMENT_ONLY"
    )


def replace_dict(value: dict, **updates: object) -> dict:
    return {**value, **updates}
