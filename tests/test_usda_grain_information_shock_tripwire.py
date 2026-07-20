from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.usda_grain_information_shock_tripwire import (
    Cell,
    _release_timestamps,
    _trade_event,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/usda_grain_information_shock_tripwire_v1.json"


def _manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _bars() -> dict[str, pd.DataFrame]:
    timestamps = pd.date_range("2024-06-12T15:20:00Z", "2024-06-12T17:30:00Z", freq="1min")
    output = {}
    for number, symbol in enumerate(("ZC", "ZS", "ZW"), start=1):
        rows = []
        price = 450.0 + 20.0 * number
        for timestamp in timestamps:
            open_price = price
            if timestamp == pd.Timestamp("2024-06-12T16:00:00Z"):
                close_price = price + 4.0
                high = price + 6.0
                low = price - 1.0
            elif timestamp == pd.Timestamp("2024-06-12T16:01:00Z"):
                close_price = price
                high = price + 8.0
                low = price - 8.0
            else:
                close_price = price + 0.25
                high = max(open_price, close_price) + 0.25
                low = min(open_price, close_price) - 0.25
            rows.append(
                {
                    "timestamp": timestamp,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close_price,
                    "instrument_id": number,
                    "symbol": f"{symbol}.c.0",
                }
            )
            price = close_price
        frame = pd.DataFrame(rows)
        frame["local_timestamp"] = frame["timestamp"].dt.tz_convert("America/New_York")
        frame["local_date"] = frame["local_timestamp"].dt.date
        output[symbol] = frame.set_index("timestamp", drop=False)
    return output


def test_manifest_is_self_hashed_pre_q4_and_official_calendar_is_stable() -> None:
    payload = _manifest()
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    assert payload["data_contract"]["end_exclusive"] == "2024-10-01"
    assert payload["data_contract"]["q4_2024_access"] is False
    assert len(payload["release_contract"]["release_dates"]) == 80
    assert "2019-01-01" not in payload["release_contract"]["release_dates"]


def test_release_timestamps_apply_new_york_dst() -> None:
    releases = _release_timestamps(_manifest())
    assert pd.Timestamp("2018-01-12T17:00:00Z") in releases
    assert pd.Timestamp("2018-08-10T16:00:00Z") in releases
    assert pd.Timestamp("2024-09-12T16:00:00Z") in releases


def test_trade_is_next_open_and_conservative_same_bar_stop_first() -> None:
    cell = Cell(
        mechanism="RESPONSE_CONTINUATION",
        signal_mode="CROSS_GRAIN_BREADTH",
        decision_bars=1,
        minimum_response_to_prior_range=0.5,
        holding_minutes=15,
        stop_prior_range_fraction=0.5,
        target_stop_multiple=1.5,
        execution_symbol="ZC",
    )
    event = _trade_event(
        _bars(), pd.Timestamp("2024-06-12T16:00:00Z"), cell, _manifest()
    )
    assert event is not None
    assert pd.Timestamp(event["decision_time"]) == pd.Timestamp("2024-06-12T16:01:00Z")
    assert pd.Timestamp(event["entry_time"]) >= pd.Timestamp(event["decision_time"])
    assert event["exit_reason"] == "STOP_FIRST"
    assert event["direction"] == 1
