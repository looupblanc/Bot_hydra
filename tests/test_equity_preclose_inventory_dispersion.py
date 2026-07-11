from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from hydra.research.equity_preclose_inventory_dispersion import (
    ALL_SYMBOLS,
    DECISION_MINUTES,
    MINI_TO_MICRO,
    _bh_family,
    _materialize_events,
    _matched_event_probability,
    _session_symbol_rows,
    _score_rows,
    apply_causal_thresholds,
    build_causal_contexts,
    select_frozen_elites,
    structural_manifest,
)


def _causal_input(session_count: int = 90) -> pd.DataFrame:
    records = []
    dates = pd.bdate_range("2023-01-03", periods=session_count)
    for day_index, day in enumerate(dates):
        for decision in DECISION_MINUTES:
            decision_ts = pd.Timestamp(day).tz_localize("America/Chicago") + pd.Timedelta(
                minutes=decision
            )
            for symbol_index, symbol in enumerate(ALL_SYMBOLS):
                is_micro = symbol in MINI_TO_MICRO.values()
                base = 1000.0 + 20 * symbol_index + day_index
                displacement = 0.002 * np.sin(day_index / 5 + symbol_index)
                records.append(
                    {
                        "event_session_id": day.strftime("%Y-%m-%d"),
                        "symbol": symbol,
                        "decision_minute": decision,
                        "decision_timestamp": decision_ts.tz_convert("UTC"),
                        "source_bar_start": decision_ts.tz_convert("UTC") - pd.Timedelta(minutes=1),
                        "source_bar_close": decision_ts.tz_convert("UTC"),
                        "availability_timestamp": decision_ts.tz_convert("UTC"),
                        "active_contract": f"{symbol}H3",
                        "displacement": displacement,
                        "path_efficiency": 0.2 + 0.01 * (symbol_index % 4),
                        "cumulative_volume": 1000.0 + day_index * 2 + symbol_index,
                        "session_return": 0.003 * np.cos(day_index / 7 + symbol_index),
                        "entry_price": base,
                        "delay_1m_entry_price": base + (0.25 if is_micro else 0.5),
                        "delay_5m_entry_price": base + (0.5 if is_micro else 1.0),
                        "exit_price": base + 2.0 * np.sign(displacement or 1.0),
                        "path_low": base - 2.0,
                        "path_high": base + 3.0,
                        "exit_timestamp": (
                            pd.Timestamp(day).tz_localize("America/Chicago")
                            + pd.Timedelta(hours=15, minutes=5)
                        ).tz_convert("UTC"),
                    }
                )
    return pd.DataFrame(records)


def test_frozen_population_has_exactly_32_unique_outcome_neutral_structures() -> None:
    rows = structural_manifest()
    assert len(rows) == 32
    assert len({row["candidate_id"] for row in rows}) == 32
    assert len({row["structural_fingerprint"] for row in rows}) == 32
    assert all(row["target_pool"] == "COMBINE_PASSER_POOL" for row in rows)
    assert all(not row["status_inherited"] and not row["inherited_passes"] for row in rows)


def test_causal_cross_market_context_uses_only_shifted_history() -> None:
    source = _causal_input()
    first = build_causal_contexts(source)
    changed = source.copy()
    final_session = changed["event_session_id"].max()
    changed.loc[changed["event_session_id"].eq(final_session), "session_return"] = 1000.0
    second = build_causal_contexts(changed)
    cutoff = first["event_session_id"].lt(final_session)
    columns = ["prior_realized_volatility", "prior_residual_scale", "prior_common_scale"]
    pd.testing.assert_frame_equal(
        first.loc[cutoff, columns].reset_index(drop=True),
        second.loc[cutoff, columns].reset_index(drop=True),
    )
    assert (first["source_bar_close"] <= first["decision_timestamp"]).all()
    assert (first["availability_timestamp"] <= first["decision_timestamp"]).all()


def test_expanding_threshold_excludes_current_and_freezes_2023() -> None:
    scores = pd.DataFrame(
        {
            "event_session_id": pd.bdate_range("2023-01-02", periods=280).strftime("%Y-%m-%d"),
            "score": np.arange(280, dtype=float),
            "structurally_eligible": True,
            "side": 1.0,
        }
    )
    selected, frozen = apply_causal_thresholds(scores, 0.70)
    assert frozen is not None
    first_eligible = scores.iloc[40]
    observed_threshold = selected.loc[
        selected["event_session_id"].eq(first_eligible["event_session_id"]), "threshold"
    ].iloc[0]
    assert observed_threshold == scores.iloc[:40]["score"].quantile(0.70)
    validation = selected.loc[selected["event_session_id"].ge("2024-01-01")]
    assert validation["threshold"].eq(frozen).all()
    assert validation["threshold_fit_cutoff"].eq("2023-12-31").all()


def test_chicago_dst_and_closed_bar_convention() -> None:
    pieces = []
    for date in ("2024-03-08", "2024-03-11"):
        local = pd.date_range(
            f"{date} 08:30", f"{date} 15:04", freq="1min", tz="America/Chicago"
        )
        piece = pd.DataFrame(
            {
                "timestamp": local.tz_convert("UTC"),
                "symbol": "MES",
                "open": 5000.0,
                "high": 5001.0,
                "low": 4999.0,
                "close": 5000.5,
                "volume": 10,
                "active_contract": "MESH4",
            }
        )
        pieces.append(piece)
    rows = _session_symbol_rows(pd.concat(pieces, ignore_index=True))
    decisions = rows.loc[rows["decision_minute"].eq(14 * 60 + 15), "decision_timestamp"].tolist()
    assert decisions == [
        pd.Timestamp("2024-03-08T20:15:00Z"),
        pd.Timestamp("2024-03-11T19:15:00Z"),
    ]
    assert (rows["source_bar_close"] == rows["decision_timestamp"]).all()
    assert rows["exit_timestamp"].dt.tz_convert("America/Chicago").dt.strftime("%H:%M").eq("15:05").all()


def test_qd_selector_deduplicates_and_obeys_mechanism_clock_caps() -> None:
    candidates = []
    for index in range(12):
        candidates.append(
            {
                "candidate_id": f"candidate_{index}",
                "stage1_passed": True,
                "quality": 100 - index,
                "mechanism": "A" if index % 2 == 0 else "B",
                "decision_minute_chicago": 855 if index % 3 else 885,
                "behavior_fingerprint": "duplicate" if index in {0, 1} else f"b{index}",
            }
        )
    selected = select_frozen_elites(candidates)
    assert len(selected) <= 4
    assert max(pd.Series([row["mechanism"] for row in selected]).value_counts()) <= 2
    assert max(pd.Series([row["decision_minute_chicago"] for row in selected]).value_counts()) <= 2
    assert len({row["behavior_fingerprint"] for row in selected}) == len(selected)


def test_family_adjustment_uses_all_32_members() -> None:
    universe = [f"c{i}" for i in range(32)]
    adjusted = _bh_family({"c0": 0.001, "c1": 0.004}, universe)
    assert adjusted["c0"] == 0.032
    assert adjusted["c1"] == 0.064
    assert adjusted["c31"] == 1.0


def test_micro_execution_cost_delay_mae_and_account_fields() -> None:
    contexts = build_causal_contexts(_causal_input())
    specification = next(
        row
        for row in structural_manifest()
        if row["target_market"] == "ES"
        and row["mechanism"] == "RESIDUAL_DISPERSION_CONVERGENCE"
        and row["decision_minute_chicago"] == 855
        and row["threshold_quantile"] == 0.70
    )
    scored = _score_rows(contexts, specification)
    selected, _ = apply_causal_thresholds(scored, 0.70)
    events = _materialize_events(selected, specification)
    assert not events.empty
    assert events["cost"].eq(4.5).all()
    assert (events["mae_dollars"] <= 0.0).all()
    assert (events["entry_timestamp"] < events["exit_timestamp"]).all()
    assert (events["source_bar_close"] <= events["entry_timestamp"]).all()
    assert events["status_inherited"].eq(False).all()
    probability = _matched_event_probability(
        contexts, events, specification, seed=17, draws=7
    )
    assert 0.0 < probability <= 1.0
