from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.fx_causal_ecology import (
    BasePolicy,
    ExactPolicy,
    build_panel,
    materialize_trades,
    normalize_bars,
    opportunities,
)


def _bars() -> pd.DataFrame:
    timestamps = pd.date_range("2022-01-03", periods=1_500, freq="min", tz="UTC")
    rows = []
    for offset, root in enumerate(("6E", "6B", "6J", "6A")):
        scale = {"6E": 1.10, "6B": 1.30, "6J": 0.009, "6A": 0.72}[root]
        moves = np.sin(np.arange(len(timestamps)) / (17 + offset)) * scale * 0.0001
        if root == "6E":
            moves[800:820] += np.linspace(0.0, 0.01, 20)
        close = scale + np.cumsum(moves)
        for index, timestamp in enumerate(timestamps):
            rows.append(
                {
                    "ts_event": timestamp,
                    "symbol": f"{root}H2",
                    "instrument_id": 10_000 + offset,
                    "open": close[index] - scale * 0.00002,
                    "high": close[index] + scale * 0.0002,
                    "low": close[index] - scale * 0.0002,
                    "close": close[index],
                    "volume": 100 + offset,
                }
            )
    return pd.DataFrame(rows)


def test_decisions_are_completed_bar_and_entries_are_next_bar() -> None:
    bars = normalize_bars(_bars())
    panel = build_panel(bars)
    base = BasePolicy("USD_FACTOR_RESIDUAL_REVERSION", "US_12_20", 15, 1.0)
    events = opportunities(base, panel, start="2022-01-03", end="2022-01-04")
    assert events
    policy = ExactPolicy(base, 30, 1.0, 1.5)
    trades = materialize_trades(policy, panel, ("2022-01-03", "2022-01-04"))
    assert trades
    assert all(row.entry_ns > row.decision_ns for row in trades)
    assert all(row.exit_ns >= row.entry_ns for row in trades)
    assert all(row.stop_distance > 0.0 for row in trades)


def test_continuous_roll_transition_is_not_filled() -> None:
    bars = normalize_bars(_bars())
    panel = build_panel(bars)
    # The raw-symbol change itself is a hard causal boundary.  This mutation is
    # confined to the synthetic read/write panel used by the test.
    panel["contract_id"].loc[panel["contract_id"].index[800]:, "6E"] = 20_000
    base = BasePolicy("USD_FACTOR_RESIDUAL_CONTINUATION", "US_12_20", 15, 1.0)
    policy = ExactPolicy(base, 30, 1.0, 1.5)
    trades = materialize_trades(policy, panel, ("2022-01-03", "2022-01-04"))
    for row in trades:
        decision = int(np.searchsorted(panel["timestamp_ns"], row.decision_ns))
        entry = int(np.searchsorted(panel["timestamp_ns"], row.entry_ns))
        assert panel["contract_id"][row.root].iat[decision] == panel["contract_id"][row.root].iat[entry]


def test_flatten_veto_ends_at_new_1700_chicago_session() -> None:
    panel = build_panel(normalize_bars(_bars()))
    chicago = panel["timestamps"].tz_convert("America/Chicago")
    close_index = int(np.flatnonzero((chicago.hour == 15) & (chicago.minute == 15))[0])
    reopen_index = int(np.flatnonzero((chicago.hour == 17) & (chicago.minute == 0))[0])
    overnight_index = int(np.flatnonzero((chicago.hour == 18) & (chicago.minute == 0))[0])
    assert bool(panel["flatten"][close_index]) is True
    assert bool(panel["flatten"][reopen_index]) is False
    assert bool(panel["flatten"][overnight_index]) is False
