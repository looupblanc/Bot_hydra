from __future__ import annotations

import pandas as pd

from hydra.research.energy_metals_session_execution_repair import (
    CHILD_ID,
    PARENT_ID,
    child_specification,
    synchronize_mcl_execution,
)


def _mcl_row(session_id: str, timestamp: str) -> dict[str, object]:
    entry = pd.Timestamp(timestamp)
    return {
        "session_id": session_id,
        "active_contract": "MCLH4",
        "overnight_entry_timestamp": entry,
        "overnight_entry_price": 70.0,
        "overnight_exit_timestamp_60": entry + pd.Timedelta(minutes=60),
        "overnight_exit_60": 70.2,
        "overnight_long_mae_60": -0.1,
        "overnight_short_mae_60": -0.3,
        "overnight_entry_timestamp_delay1": entry + pd.Timedelta(minutes=1),
        "overnight_entry_price_delay1": 70.05,
        "overnight_exit_timestamp_60_delay1": entry + pd.Timedelta(minutes=61),
        "overnight_exit_60_delay1": 70.25,
        "overnight_long_mae_60_delay1": -0.1,
        "overnight_short_mae_60_delay1": -0.3,
    }


def test_child_is_fresh_and_changes_execution_semantics_only() -> None:
    child = child_specification()

    assert child["candidate_id"] == CHILD_ID
    assert child["parent_candidate_id"] == PARENT_ID
    assert child["signal_market"] == "CL"
    assert child["execution_market"] == "MCL"
    assert child["signal_semantics"] == "CL_SIGNAL_MCL_SYNCHRONIZED_EXECUTION"
    assert len(child["structural_fingerprint"]) == 64


def test_synchronized_execution_preserves_signal_side_and_audits_missing() -> None:
    signals = pd.DataFrame(
        [
            {
                "trading_session_id": "2024-01-03",
                "entry_timestamp": pd.Timestamp("2024-01-03T14:01:00Z"),
                "side": 1.0,
            },
            {
                "trading_session_id": "2024-01-04",
                "entry_timestamp": pd.Timestamp("2024-01-04T14:01:00Z"),
                "side": -1.0,
            },
        ]
    )
    table = pd.DataFrame([_mcl_row("2024-01-03", "2024-01-03T14:01:00Z")])

    events, missing = synchronize_mcl_execution(signals, table)
    delayed, delayed_missing = synchronize_mcl_execution(
        signals, table, entry_delay_bars=1
    )

    assert len(events) == 1
    assert events.iloc[0]["side"] == 1.0
    assert events.iloc[0]["symbol"] == "MCL"
    assert events.iloc[0]["signal_symbol"] == "CL"
    assert events.iloc[0]["signal_recomputed_from_mcl"] is False or not bool(
        events.iloc[0]["signal_recomputed_from_mcl"]
    )
    assert events.iloc[0]["net_pnl"] < events.iloc[0]["gross_pnl"]
    assert missing == [
        {"session_id": "2024-01-04", "reason": "missing_mcl_session"}
    ]
    assert len(delayed) == 1
    assert delayed.iloc[0]["entry_timestamp"] == pd.Timestamp(
        "2024-01-03T14:02:00Z"
    )
    assert delayed_missing == missing
