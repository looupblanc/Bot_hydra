from __future__ import annotations

import pandas as pd
import pytest

from hydra.research.treasury_auction_concession_curve_relative_value import (
    _active_policy_audit,
    _path_outcome,
    frozen_specs,
)


def test_frozen_lattice_is_bounded_and_unique() -> None:
    specs = frozen_specs()

    assert len(specs) == 24
    assert len({row.policy_id for row in specs}) == 24


def test_pair_outcome_enters_and_exits_at_frozen_opens() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-02T17:00:00Z", periods=4, freq="1min"),
            "segment": [1, 1, 1, 1],
            "target_session_id": ["2024-01-02"] * 4,
            "target_open": [100.0, 100.0, 100.1, 100.2],
            "target_high": [100.0, 100.1, 100.2, 100.3],
            "target_low": [99.9, 99.9, 100.0, 100.1],
            "hedge_open": [110.0, 110.0, 110.0, 110.0],
            "hedge_high": [110.1] * 4,
            "hedge_low": [109.9] * 4,
        }
    )

    result = _path_outcome(
        frame,
        entry_index=1,
        holding_minutes=2,
        target="ZN",
        hedge="TN",
        target_qty=1,
        hedge_qty=1,
        direction=1,
    )

    assert result is not None
    assert result["entry_time"] == frame.at[1, "timestamp"].isoformat()
    assert result["exit_time"] == frame.at[3, "timestamp"].isoformat()
    assert result["gross_usd"] == pytest.approx(200.0)


def test_activity_audit_never_calls_zero_trade_policy_economic_best() -> None:
    def candidate(policy_id: str, discovery_trades: int, discovery_net: float) -> dict:
        roles = {
            "DISCOVERY": {"trade_count": discovery_trades, "stressed_net_usd": discovery_net},
            "VALIDATION": {"trade_count": 1, "stressed_net_usd": -1.0},
            "FINAL_DEVELOPMENT": {"trade_count": 1, "stressed_net_usd": -1.0},
        }
        return {"policy_id": policy_id, "by_role": roles, "_trades": []}

    audit = _active_policy_audit(
        [candidate("inactive", 0, 0.0), candidate("active", 2, -10.0)]
    )

    assert audit["inactive_policy_count"] == 1
    assert audit["best_active_policy"]["policy_id"] == "active"
    assert audit["selection_changed"] is False
