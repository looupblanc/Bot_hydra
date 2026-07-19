from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from hydra.research.scheduled_macro_release_tripwire import (
    BRANCH_ID,
    EVALUATION_ROLES,
    SCENARIOS,
    _build_release_event,
    _matched_control_time,
    _primary_mll_cell_rates,
    load_decision_card,
    load_official_calendar,
)


CARD = Path(
    "config/research/scheduled_macro_release_causal_reaction_tripwire_v1.json"
)
CALENDAR = Path(
    "config/research/scheduled_macro_release_calendar_2023_2024_v1.json"
)


def _market_frame(symbol: str, contract: str) -> pd.DataFrame:
    index = pd.date_range("2024-01-05T13:30:00Z", periods=31, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": index,
            "active_contract": contract,
            "trading_session_id": "2024-01-05",
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": 100.0,
        },
        index=index,
    )
    if symbol == "YM":
        frame.loc[index[0], "close"] = 102.0
    else:
        frame.loc[index[16], "open"] = 105.0
    return frame


def _release() -> dict[str, object]:
    return {
        "release_id": "BLS_EMPLOYMENT_SITUATION:20240105T1330Z:00",
        "family": "BLS_EMPLOYMENT_SITUATION",
        "role": "VALIDATION",
        "release_time": pd.Timestamp("2024-01-05T13:30:00Z"),
    }


def _specification(mode: str = "CONTINUATION") -> dict[str, object]:
    return {
        "candidate_id": "macro_test",
        "signal_market": "YM",
        "execution_market": "MYM",
        "reaction_mode": mode,
        "observation_minutes": 1,
        "holding_minutes": 15,
    }


def test_card_and_official_calendar_are_frozen_and_pre_q4() -> None:
    card = load_decision_card(CARD)
    calendar = load_official_calendar(CALENDAR)
    assert card["selected_branch"] == BRANCH_ID
    assert card["governance"]["read_only"] is True
    assert card["governance"]["tier_q_allowed"] is False
    assert card["governance"]["q4_access_allowed"] is False
    assert card["governance"]["data_purchase_allowed"] is False
    assert len(calendar["events"]) == 56
    assert all(
        pd.Timestamp(row["release_utc"]) < pd.Timestamp("2024-10-01T00:00:00Z")
        for row in calendar["events"]
    )
    assert not any("eia" in str(row["source"]).lower() for row in calendar["events"])


def test_release_decision_uses_completed_bar_and_next_exact_open() -> None:
    frames = {
        "YM": _market_frame("YM", "YMH4"),
        "MYM": _market_frame("MYM", "MYMH4"),
    }
    costs = {"MYM": {"NORMAL": 3.0, "STRESSED_1_5X": 4.5}}
    event, reason = _build_release_event(
        _release(),
        _specification(),
        frames,
        costs,
        anchor_time=pd.Timestamp("2024-01-05T13:30:00Z"),
    )
    assert reason == "OK"
    assert event is not None
    assert event["decision_time"] == "2024-01-05T13:31:00+00:00"
    assert event["fill_time"] == "2024-01-05T13:31:00+00:00"
    assert event["exit_time"] == "2024-01-05T13:46:00+00:00"
    assert event["side"] == 1
    assert event["entry_price"] == 100.0
    assert event["exit_price"] == 105.0
    assert event["gross_one_micro"] == 2.5

    rejected, reason = _build_release_event(
        _release(),
        _specification("REJECTION"),
        frames,
        costs,
        anchor_time=pd.Timestamp("2024-01-05T13:30:00Z"),
    )
    assert reason == "OK"
    assert rejected is not None
    assert rejected["side"] == -1
    assert rejected["gross_one_micro"] == -2.5


def test_contract_cycle_mismatch_fails_closed() -> None:
    frames = {
        "YM": _market_frame("YM", "YMH4"),
        "MYM": _market_frame("MYM", "MYMM4"),
    }
    event, reason = _build_release_event(
        _release(),
        _specification(),
        frames,
        {"MYM": {"NORMAL": 3.0, "STRESSED_1_5X": 4.5}},
        anchor_time=pd.Timestamp("2024-01-05T13:30:00Z"),
    )
    assert event is None
    assert reason == "MINI_MICRO_CONTRACT_CYCLE_MISMATCH"


def test_session_matched_control_uses_prior_non_release_session_same_clock() -> None:
    frame = pd.DataFrame(
        {
            "trading_session_id": ["2024-01-03", "2024-01-04", "2024-01-05"]
        }
    )
    matched = _matched_control_time(
        _release(), frame, {"2024-01-03", "2024-01-05"}
    )
    assert matched == pd.Timestamp("2024-01-04T13:30:00Z")


def test_mll_gate_cells_are_not_diluted_across_horizons() -> None:
    safe = SimpleNamespace(mll_breached=False)
    breach = SimpleNamespace(mll_breached=True)
    store = {
        ("PRIMARY", role, horizon, scenario): [safe] * 10
        for role in EVALUATION_ROLES
        for horizon in (5, 10, 20)
        for scenario in SCENARIOS
    }
    store[("PRIMARY", "FINAL_DEVELOPMENT", 20, "STRESSED_1_5X")] = [
        breach,
        *([safe] * 8),
    ]
    cells = _primary_mll_cell_rates(store, (5, 10, 20))
    assert cells["VALIDATION:5D:NORMAL"]["mll_breach_rate"] == 0.0
    assert (
        cells["FINAL_DEVELOPMENT:20D:STRESSED_1_5X"]["mll_breach_rate"]
        > 0.10
    )

