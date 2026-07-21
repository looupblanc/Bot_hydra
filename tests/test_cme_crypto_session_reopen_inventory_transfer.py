from __future__ import annotations

from copy import deepcopy

import pandas as pd

from hydra.research.cme_crypto_session_reopen_inventory_transfer import (
    _opportunities,
    _session_table,
    frozen_specs,
)


def _manifest() -> dict[str, object]:
    import json
    from pathlib import Path

    return json.loads(
        Path("config/research/cme_crypto_session_reopen_inventory_transfer_v1.json").read_text()
    )


def _synthetic_market() -> pd.DataFrame:
    rows = []
    for index, day in enumerate(pd.date_range("2022-01-03", periods=30, freq="B")):
        session = pd.Timestamp(day, tz="UTC")
        rows.extend(
            [
                {
                    "session_day": session,
                    "ts_recv": session - pd.Timedelta(hours=6),
                    "sequence": index * 2,
                    "mid": 100.0 + index,
                    "spread": 1.0,
                },
                {
                    "session_day": session,
                    "ts_recv": session + pd.Timedelta(hours=20),
                    "sequence": index * 2 + 1,
                    "mid": 100.5 + index,
                    "spread": 1.0,
                },
            ]
        )
    return pd.DataFrame(rows)


def test_frozen_lattice_is_bounded_and_unique() -> None:
    specs = frozen_specs(_manifest())
    assert len(specs) == 16
    assert len({(row.mechanism, row.decision_delay_minutes, row.holding_minutes) for row in specs}) == 16


def test_trailing_gap_state_cannot_see_later_session_and_selects_one_market() -> None:
    manifest = _manifest()
    base = _synthetic_market()
    changed = deepcopy(base)
    changed.loc[changed.index[-1], "mid"] = 1_000_000.0
    left = _session_table(base, manifest)
    right = _session_table(changed, manifest)
    assert left.iloc[:-1]["gap_score"].tolist() == right.iloc[:-1]["gap_score"].tolist()

    mbt = left.rename(columns={column: f"MBT_{column}" for column in left.columns if column != "session_day"})
    met = left.rename(columns={column: f"MET_{column}" for column in left.columns if column != "session_day"})
    sessions = mbt.merge(met, on="session_day")
    sessions["role"] = "DISCOVERY"
    opportunities = _opportunities(sessions, frozen_specs(manifest)[0], manifest)
    assert len({row["session_day"] for row in opportunities}) == len(opportunities)


def test_dst_shifted_labels_collapse_to_one_exchange_session() -> None:
    manifest = _manifest()
    frame = _synthetic_market()
    duplicate = frame.iloc[[0]].copy()
    duplicate["session_day"] = duplicate["session_day"] + pd.Timedelta(hours=1)
    duplicate["ts_recv"] = duplicate["ts_recv"] + pd.Timedelta(minutes=1)
    duplicate["sequence"] = 10_000
    combined = pd.concat([frame, duplicate], ignore_index=True)

    table = _session_table(combined, manifest)

    assert table["session_day"].map(lambda value: pd.Timestamp(value).date()).is_unique
