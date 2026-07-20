from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.cbot_grain_split_session_inventory_transfer import (
    Cell,
    SessionContext,
    _candidate_id,
    _cells,
    _economic_result_hash,
    _first_row_strictly_after,
    _roll_guard_days_chicago,
    _summary,
    _signal,
    _trade_event,
    _wasde_days,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/cbot_grain_split_session_inventory_transfer_v1.json"


def _manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _frame() -> pd.DataFrame:
    timestamps = pd.date_range("2024-06-13T13:34:00Z", periods=10, freq="1min")
    rows = []
    for index, timestamp in enumerate(timestamps):
        open_price = 450.0
        if index == 1:
            high, low, close = 453.0, 447.0, 450.0
        else:
            high, low, close = 450.25, 449.75, 450.0
        rows.append(
            {
                "timestamp": timestamp,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "instrument_id": "ZCN4",
                "symbol": "ZC.c.0",
            }
        )
    return pd.DataFrame(rows).set_index("timestamp", drop=False)


def _context(*, response_direction: int = 1) -> SessionContext:
    return SessionContext(
        symbol="ZC",
        session_day=date(2024, 6, 13),
        role="VALIDATION",
        decision_bars=1,
        overnight_direction=1,
        response_direction=response_direction,
        overnight_ratio=1.25,
        response_ratio=0.5,
        prior_range=4.0,
        decision_time=pd.Timestamp("2024-06-13T13:34:00Z"),
        entry_time=pd.Timestamp("2024-06-13T13:35:00Z"),
        entry_price=450.0,
        instrument_id="ZCN4",
    )


def test_manifest_is_self_hashed_pre_q4_and_zero_spend() -> None:
    payload = _manifest()
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    assert payload["data_contract"]["end_exclusive"] == "2024-10-01"
    assert payload["data_contract"]["q4_2024_access"] is False
    assert payload["data_contract"]["incremental_data_spend_usd"] == 0.0
    assert payload["governance"]["maximum_cpu_workers"] == 1
    frozen = _manifest()
    cells = _cells(frozen)
    assert len(cells) == 72
    assert len({_candidate_id(cell, frozen) for cell in cells}) == 72


def test_wasde_and_contract_roll_exclusions_are_stable() -> None:
    assert date(2024, 6, 12) in _wasde_days(ROOT)
    frame = _frame().copy()
    frame.loc[pd.Timestamp("2024-06-13T13:39:00Z") :, "instrument_id"] = "ZCU4"
    guarded = _roll_guard_days_chicago(frame)
    assert date(2024, 6, 12) in guarded
    assert date(2024, 6, 13) in guarded
    assert date(2024, 6, 14) in guarded


def test_economic_hash_excludes_only_runtime_telemetry() -> None:
    first = {"status": "FALSIFIED", "value": 12.5, "runtime_seconds": 1.0}
    second = {"status": "FALSIFIED", "value": 12.5, "runtime_seconds": 9.0}
    changed = {"status": "FALSIFIED", "value": 12.6, "runtime_seconds": 1.0}
    assert _economic_result_hash(first) == _economic_result_hash(second)
    assert _economic_result_hash(first) != _economic_result_hash(changed)


def test_signal_requires_frozen_inventory_and_reopen_relation() -> None:
    continuation = Cell("INVENTORY_CONTINUATION", 1, 1.0, 30, "ZC")
    rejection = Cell("INVENTORY_REJECTION", 1, 1.0, 30, "ZC")
    assert _signal(_context(), continuation, _manifest()) == 1
    assert _signal(_context(), rejection, _manifest()) is None
    assert _signal(_context(response_direction=-1), rejection, _manifest()) == -1


def test_trade_enters_at_frozen_next_open_and_stops_first_on_ambiguous_bar() -> None:
    cell = Cell("INVENTORY_CONTINUATION", 1, 1.0, 30, "ZC")
    context = _context()
    event = _trade_event(
        _frame(),
        context,
        context,
        cell,
        _manifest(),
        direction_flip=False,
        timing_control=False,
    )
    assert event is not None
    assert pd.Timestamp(event["entry_time"]) == context.entry_time
    assert event["exit_reason"] == "STOP_FIRST"
    assert event["direction"] == 1
    assert event["normal_cost_usd"] == 30.28
    assert event["stressed_cost_usd"] == 55.28


def test_entry_lookup_is_strictly_after_decision_time() -> None:
    frame = _frame()
    decision = pd.Timestamp("2024-06-13T13:34:00Z")
    row = _first_row_strictly_after(frame, decision, limit_minutes=2)
    assert row is not None
    assert pd.Timestamp(row["timestamp"]) == pd.Timestamp("2024-06-13T13:35:00Z")


def test_missing_future_entry_is_persisted_as_censored_signal() -> None:
    cell = Cell("INVENTORY_CONTINUATION", 1, 1.0, 30, "ZC")
    signal_context = _context()
    censored_context = SessionContext(
        **{
            **{
                field: getattr(signal_context, field)
                for field in signal_context.__dataclass_fields__
            },
            "entry_time": None,
            "entry_price": None,
            "instrument_id": None,
        }
    )
    event = _trade_event(
        _frame(),
        signal_context,
        censored_context,
        cell,
        _manifest(),
        direction_flip=False,
        timing_control=False,
    )
    assert event is not None
    assert event["outcome_state"] == "DATA_CENSORED"
    assert event["censor_reason"] == "MISSING_EXECUTABLE_ENTRY"


def test_summary_preserves_stressed_cost_and_concentration() -> None:
    events = [
        {
            "role": "DISCOVERY",
            "outcome_state": "FULL_COVERAGE",
            "gross_pnl_usd": 200.0,
            "normal_net_usd": 170.0,
            "stressed_net_usd": 145.0,
            "stressed_cost_usd": 55.0,
            "minimum_open_pnl_stressed_usd": -80.0,
            "exit_reason": "TARGET",
            "event_hash": "a",
        },
        {
            "role": "DISCOVERY",
            "outcome_state": "FULL_COVERAGE",
            "gross_pnl_usd": -100.0,
            "normal_net_usd": -130.0,
            "stressed_net_usd": -155.0,
            "stressed_cost_usd": 55.0,
            "minimum_open_pnl_stressed_usd": -180.0,
            "exit_reason": "STOP_FIRST",
            "event_hash": "b",
        },
    ]
    summary = _summary(events, "DISCOVERY")
    assert summary["event_count"] == 2
    assert summary["stressed_net_usd"] == -10.0
    assert summary["stressed_edge_to_cost_ratio"] == 100.0 / 110.0
    assert summary["maximum_single_event_positive_profit_share"] == 1.0
