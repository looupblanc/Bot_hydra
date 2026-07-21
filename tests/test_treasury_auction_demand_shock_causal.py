from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

from hydra.research.treasury_auction_demand_shock_causal import (
    MarketBars,
    build_causal_events,
    control_direction,
    replay_event,
    robust_z,
    simulate_account_window,
)


def _record(day: str, term: str, btc: float, indirect: float, dealer: float) -> dict:
    total = 1_000.0
    return {
        "cusip": f"{term}-{day}",
        "auctionDate": f"{day}T00:00:00",
        "securityTerm": term,
        "updatedTimestamp": f"{day}T13:03:00",
        "bidToCoverRatio": str(btc),
        "indirectBidderAccepted": str(indirect * total),
        "primaryDealerAccepted": str(dealer * total),
        "totalAccepted": str(total),
    }


def _manifests() -> tuple[dict, dict]:
    manifest = {
        "chronological_roles": {"DISCOVERY": ["2023-01-01", "2024-01-01"]},
        "feature_contract": {"individual_z_clip": 3.0, "minimum_history": 6},
        "policy": {
            "trade_threshold_absolute_score_inclusive": 1.5,
            "quantity_tiers": [
                {"minimum_absolute_score": 1.5, "contracts": 3},
                {"minimum_absolute_score": 3.0, "contracts": 5},
            ],
        },
    }
    predecessor = {
        "official_event_sources": {"allowed_terms": ["2-Year"]},
        "term_to_market": {"2-Year": "ZT"},
    }
    return manifest, predecessor


def test_trailing_six_excludes_current_and_future_observations() -> None:
    manifest, predecessor = _manifests()
    records = [
        _record(f"2023-01-{day:02d}", "2-Year", 2.0, 0.5, 0.3)
        for day in range(1, 7)
    ]
    records.append(_record("2023-01-07", "2-Year", 3.0, 0.8, 0.1))
    records.append(_record("2023-01-08", "2-Year", 999.0, 0.0, 1.0))
    events = build_causal_events(records, manifest, predecessor)
    assert len(events) == 2
    first = events[0]
    assert first["history_count"] == 6
    assert first["bid_to_cover_z"] == 3.0
    assert first["demand_score"] == 9.0
    assert first["quantity"] == 5
    # Removing the later eighth observation cannot alter the seventh event.
    without_future = build_causal_events(records[:-1], manifest, predecessor)
    assert without_future[0]["history_fingerprint"] == first["history_fingerprint"]
    assert without_future[0]["demand_score"] == first["demand_score"]


def test_robust_z_uses_exact_population_scale_and_clip() -> None:
    history = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    center = np.median(history)
    scale = max(1.4826 * np.median(np.abs(np.asarray(history) - center)), np.std(history), 1e-9)
    assert robust_z(4.0, history) == (4.0 - center) / scale
    assert robust_z(100.0, history) == 3.0


def test_next_open_strictly_after_availability_and_same_bar_stop_first() -> None:
    times = pd.to_datetime(
        ["2023-01-10T18:03:00Z", "2023-01-10T18:04:00Z", "2023-01-10T18:05:00Z", "2023-01-10T19:04:00Z"],
        utc=True,
    )
    frame = pd.DataFrame(
        {
            "timestamp": times,
            "open": [100.0, 100.0, 100.0, 100.0],
            "high": [100.0, 100.0, 103.0, 100.0],
            "low": [100.0, 100.0, 97.0, 100.0],
            "close": [100.0, 100.0, 100.0, 100.0],
            "contract": ["ZT"] * 4,
            "session_id": ["2023-01-10"] * 4,
        }
    )
    bars = MarketBars(frame, frame["timestamp"].astype("int64").to_numpy())
    event = {
        "event_id": "event",
        "auction_date": "2023-01-10",
        "available_at": pd.Timestamp("2023-01-10T18:03:00Z"),
        "action": "DEMAND_CONTINUATION",
        "direction": 1,
        "quantity": 3,
    }
    result = replay_event(
        event,
        bars,
        {"tick": 0.25, "point_value": 1.0, "round_turn_fee_usd": 0.0},
        "CAUSAL_DEMAND",
        "NORMAL",
        60,
        8,
        12,
        1,
    )
    assert result["entry_time"] == "2023-01-10T18:04:00+00:00"
    assert result["exit_reason"] == "STOP"
    assert result["exit_price"] == 98.0


def test_controls_preserve_exposure_and_flip_direction() -> None:
    assert control_direction("abc", "DIRECTION_FLIP", 1) == -1
    random = control_direction("abc", "DETERMINISTIC_RANDOM_DIRECTION_EXPOSURE_MATCHED", 1)
    assert random in (-1, 1)
    assert random == control_direction("abc", "DETERMINISTIC_RANDOM_DIRECTION_EXPOSURE_MATCHED", -1)


def test_account_uses_eod_trailing_mll_and_consistency() -> None:
    trades = [
        {
            "auction_date": "2023-01-02",
            "status": "TRADE_COMPLETED",
            "entry_time": "2023-01-02T18:00:00Z",
            "event_id": "a",
            "net_pnl_usd": 1600.0,
            "minimum_trade_pnl_usd": -100.0,
        },
        {
            "auction_date": "2023-01-03",
            "status": "TRADE_COMPLETED",
            "entry_time": "2023-01-03T18:00:00Z",
            "event_id": "b",
            "net_pnl_usd": 1600.0,
            "minimum_trade_pnl_usd": -100.0,
        },
    ]
    result = simulate_account_window(
        ["2023-01-02", "2023-01-03"],
        trades,
        {"profit_target_usd": 3000, "maximum_loss_limit_usd": 2000, "consistency_fraction": 0.5},
    )
    assert result["passed"]
    assert result["pass_day"] == 2
    assert result["required_profit_usd"] == 3200.0
    assert result["minimum_mll_buffer_usd"] > 0
