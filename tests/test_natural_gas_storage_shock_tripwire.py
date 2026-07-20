from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.research.natural_gas_storage_shock_tripwire import (
    Cell,
    _release_timestamps,
    _trade_event,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "config/research/natural_gas_storage_shock_tripwire_v1.json"


def _manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _bars() -> dict[str, pd.DataFrame]:
    timestamps = pd.date_range("2024-06-06T13:55:00Z", "2024-06-06T15:45:00Z", freq="1min")
    rows = []
    price = 2.5
    for timestamp in timestamps:
        open_price = price
        if timestamp == pd.Timestamp("2024-06-06T14:30:00Z"):
            close_price = price + 0.030
            high = price + 0.060
            low = price - 0.060
        elif timestamp == pd.Timestamp("2024-06-06T14:31:00Z"):
            close_price = price
            high = price + 0.100
            low = price - 0.100
        else:
            close_price = price + 0.001
            high = max(open_price, close_price) + 0.001
            low = min(open_price, close_price) - 0.001
        rows.append(
            {
                "timestamp": timestamp,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
                "instrument_id": 1,
                "symbol": "NG.c.0",
            }
        )
        price = close_price
    frame = pd.DataFrame(rows)
    frame["local_timestamp"] = frame["timestamp"].dt.tz_convert("America/New_York")
    frame["local_date"] = frame["local_timestamp"].dt.date
    frame = frame.set_index("timestamp", drop=False)
    micro = frame.copy()
    micro["symbol"] = "MNG.c.0"
    return {"NG": frame, "MNG": micro}


def test_manifest_is_self_hashed_and_pre_q4() -> None:
    payload = _manifest()
    claimed = payload.pop("manifest_hash")
    assert stable_hash(payload) == claimed
    assert payload["data_contract"]["end_exclusive"] == "2024-10-01"
    assert payload["data_contract"]["q4_access"] is False


def test_known_release_calendar_excludes_frozen_holiday_shift() -> None:
    manifest = _manifest()
    bars = _bars()["NG"]
    releases = _release_timestamps(bars, manifest)
    assert releases == [pd.Timestamp("2024-06-06T14:30:00Z")]


def test_trade_is_filled_after_completed_response_and_same_bar_stop_is_first() -> None:
    manifest = _manifest()
    bars = _bars()
    cell = Cell(
        mechanism="RELEASE_RESPONSE_CONTINUATION",
        decision_bars=1,
        minimum_response_to_prior_range=0.5,
        holding_minutes=15,
        stop_prior_range_fraction=0.5,
        target_stop_multiple=1.5,
        execution_symbol="NG",
    )
    event = _trade_event(
        bars,
        pd.Timestamp("2024-06-06T14:30:00Z"),
        cell,
        manifest,
    )
    assert event is not None
    assert pd.Timestamp(event["decision_time"]) == pd.Timestamp("2024-06-06T14:31:00Z")
    assert pd.Timestamp(event["entry_time"]) >= pd.Timestamp(event["decision_time"])
    # The synthetic entry bar crosses both boundaries; conservative ordering wins.
    assert event["exit_reason"] == "STOP_FIRST"
    assert event["direction"] == 1
