from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.research.ym_strict_promotion import (
    YMStrictPromotionError,
    _block_bootstrap_probability,
    _concentration_attacks,
    _load_ledger,
    _matched_direction_probability,
)


def _events() -> pd.DataFrame:
    rows = []
    for index in range(30):
        side = 1 if index % 2 == 0 else -1
        directionless = 50.0 if side > 0 else -40.0
        rows.append(
            {
                "symbol": "YM",
                "active_contract": "YMH4" if index < 15 else "YMM4",
                "decision_timestamp": pd.Timestamp("2024-01-02T14:31:00Z")
                + pd.Timedelta(days=index),
                "event_session_id": f"2024-01-{index + 1:02d}",
                "gap_points": float((index % 7) + 1) * side,
                "side": side,
                "gross_pnl_60": side * directionless,
                "net_pnl_60": side * directionless - 4.5,
                "cost": 4.5,
            }
        )
    return pd.DataFrame(rows)


def test_strict_statistics_are_deterministic_and_concentration_is_exact() -> None:
    events = _events()
    first = _block_bootstrap_probability(
        events["net_pnl_60"].to_numpy(dtype=float), seed=42, draws=256
    )
    second = _block_bootstrap_probability(
        events["net_pnl_60"].to_numpy(dtype=float), seed=42, draws=256
    )
    matched_first = _matched_direction_probability(events, seed=43, draws=256)
    matched_second = _matched_direction_probability(events, seed=43, draws=256)
    attacks = _concentration_attacks(events)

    assert first == second
    assert matched_first == matched_second
    assert first["draws"] == 256
    assert 0.0 < matched_first["one_sided_probability"] <= 1.0
    assert np.isclose(
        attacks["remove_best_trade_net"],
        events["net_pnl_60"].sum() - events["net_pnl_60"].max(),
    )
    assert attacks["best_month"] == "2024-01"


def test_parent_ledger_loader_fails_closed_on_q4(tmp_path: Path) -> None:
    row = _events().iloc[0].to_dict()
    row["decision_timestamp"] = "2024-10-01T00:00:00+00:00"
    path = tmp_path / "ledger.jsonl"
    path.write_text(json.dumps(row, default=str) + "\n", encoding="utf-8")

    with pytest.raises(YMStrictPromotionError, match="Q4"):
        _load_ledger(path)


def test_parent_ledger_loader_rejects_duplicate_events(tmp_path: Path) -> None:
    row = _events().iloc[0].to_dict()
    content = json.dumps(row, default=str) + "\n"
    path = tmp_path / "ledger.jsonl"
    path.write_text(content + content, encoding="utf-8")

    with pytest.raises(YMStrictPromotionError, match="Duplicate"):
        _load_ledger(path)
