from __future__ import annotations

import numpy as np
import pandas as pd

from hydra.research.mini_micro_participation_divergence import (
    _contract_maturity,
    _materialize_events,
    _score_rows,
    _selector_shift_max_probability,
    apply_phase_causal_thresholds,
    build_participation_contexts,
    resample_closed_five_minute_bars,
    select_frozen_elites,
    structural_manifest,
)


def _minute_bars(days: int = 25) -> pd.DataFrame:
    records = []
    for day_index, day in enumerate(pd.bdate_range("2024-02-20", periods=days)):
        for symbol, contract, scale in (("ES", "ESH4", 10), ("MES", "MESH4", 3)):
            local = pd.date_range(
                f"{day.date()} 08:30", f"{day.date()} 15:04", freq="1min", tz="America/Chicago"
            )
            for minute_index, timestamp in enumerate(local):
                price = 5000 + day_index + minute_index * 0.01
                records.append(
                    {
                        "timestamp": timestamp.tz_convert("UTC"),
                        "symbol": symbol,
                        "open": price,
                        "high": price + 0.5,
                        "low": price - 0.5,
                        "close": price + (0.1 if minute_index % 2 else -0.05),
                        "volume": scale * (100 + minute_index % 20 + day_index),
                        "active_contract": contract,
                    }
                )
    return pd.DataFrame(records)


def _scored_contexts(session_count: int = 300) -> pd.DataFrame:
    records = []
    for index, day in enumerate(pd.bdate_range("2023-01-02", periods=session_count)):
        for decision in (540, 545, 550):
            decision_ts = (
                pd.Timestamp(day).tz_localize("America/Chicago") + pd.Timedelta(minutes=decision)
            ).tz_convert("UTC")
            divergence = 0.2 + 0.01 * ((index + decision) % 30)
            records.append(
                {
                    "event_session_id": day.strftime("%Y-%m-%d"),
                    "target_market": "ES",
                    "execution_market": "MES",
                    "decision_minute": decision,
                    "decision_timestamp": decision_ts,
                    "source_bar_start": decision_ts - pd.Timedelta(minutes=5),
                    "source_bar_close": decision_ts,
                    "availability_timestamp": decision_ts,
                    "signal_contract": "ESH3",
                    "execution_contract": "MESH3",
                    "mini_return_5m": 0.001,
                    "micro_return_5m": 0.001,
                    "mini_normalized_participation": 1.0,
                    "micro_normalized_participation": 1.5,
                    "participation_divergence": divergence,
                    "sign_agreement": True,
                    "entry_price": 5000.0,
                    "delay_1m_entry_price": 5000.25,
                    "delay_5m_entry_price": 5000.5,
                    "exit_price_15": 5002.0,
                    "path_low_15": 4998.0,
                    "path_high_15": 5003.0,
                    "exit_timestamp_15": decision_ts + pd.Timedelta(minutes=15),
                    "exit_price_30": 5003.0,
                    "path_low_30": 4997.0,
                    "path_high_30": 5004.0,
                    "exit_timestamp_30": decision_ts + pd.Timedelta(minutes=30),
                    "exit_price_60": 5004.0,
                    "path_low_60": 4996.0,
                    "path_high_60": 5005.0,
                    "exit_timestamp_60": decision_ts + pd.Timedelta(minutes=60),
                }
            )
    return pd.DataFrame(records)


def test_population_is_exactly_96_unique_one_economic_pair_each() -> None:
    population = structural_manifest()
    assert len(population) == 96
    assert len({row["candidate_id"] for row in population}) == 96
    assert len({row["structural_fingerprint"] for row in population}) == 96
    assert all(row["target_pool"] == "COMBINE_PASSER_POOL" for row in population)
    assert all(not row["status_inherited"] for row in population)


def test_closed_five_minute_resampling_and_dst_availability() -> None:
    bars = _minute_bars(days=15)
    five = resample_closed_five_minute_bars(bars)
    assert five["source_row_count"].eq(5).all()
    assert (
        five["source_bar_close"] == five["source_bar_start"] + pd.Timedelta(minutes=5)
    ).all()
    before = five.loc[five["event_session_id"].eq("2024-03-08")].iloc[0]
    after = five.loc[five["event_session_id"].eq("2024-03-11")].iloc[0]
    assert before["source_bar_start"].hour == after["source_bar_start"].hour + 1


def test_participation_history_is_shifted_and_pair_maturity_is_equal() -> None:
    bars = _minute_bars(days=25)
    first = build_participation_contexts(bars)
    changed = bars.copy()
    final_day = changed["timestamp"].dt.tz_convert("America/Chicago").dt.strftime("%Y-%m-%d").max()
    final_mask = changed["timestamp"].dt.tz_convert("America/Chicago").dt.strftime("%Y-%m-%d").eq(final_day)
    changed.loc[final_mask & changed["symbol"].eq("MES"), "volume"] *= 1000
    second = build_participation_contexts(changed)
    cutoff = first["event_session_id"].lt(final_day)
    columns = [
        "mini_normalized_participation",
        "micro_normalized_participation",
        "participation_divergence",
    ]
    pd.testing.assert_frame_equal(
        first.loc[cutoff, columns].reset_index(drop=True),
        second.loc[cutoff, columns].reset_index(drop=True),
    )
    assert (first["source_bar_close"] == first["decision_timestamp"]).all()
    assert _contract_maturity("ES", "ESH4") == _contract_maturity("MES", "MESH4")


def test_entry_contract_mismatch_fails_closed_for_that_event() -> None:
    bars = _minute_bars(days=25)
    local = bars["timestamp"].dt.tz_convert("America/Chicago")
    target_day = local.dt.strftime("%Y-%m-%d").max()
    mismatch = (
        bars["symbol"].eq("MES")
        & local.dt.strftime("%Y-%m-%d").eq(target_day)
        & (local.dt.hour * 60 + local.dt.minute).eq(9 * 60)
    )
    bars.loc[mismatch, "active_contract"] = "MESM4"
    contexts = build_participation_contexts(bars)
    assert contexts.loc[
        contexts["event_session_id"].eq(target_day)
        & contexts["target_market"].eq("ES")
        & contexts["decision_minute"].eq(9 * 60)
    ].empty


def test_phase_threshold_is_shifted_frozen_and_one_event_per_session() -> None:
    specification = structural_manifest()[0]
    scored = _score_rows(_scored_contexts(), specification)
    selected, frozen = apply_phase_causal_thresholds(scored, 0.70)
    assert frozen
    assert not selected["event_session_id"].duplicated().any()
    validation = selected.loc[selected["event_session_id"].ge("2024-01-01")]
    assert validation["threshold_fit_cutoff"].eq("2023-12-31").all()
    for row in validation.itertuples():
        assert row.threshold == frozen[str(row.decision_minute)]

    single_phase = scored.loc[scored["decision_minute"].eq(540)].reset_index(drop=True)
    single_selected, _ = apply_phase_causal_thresholds(single_phase, 0.70)
    first_after_minimum = single_phase.iloc[20]
    threshold = single_selected.loc[
        single_selected["event_session_id"].eq(first_after_minimum["event_session_id"]),
        "threshold",
    ].iloc[0]
    assert threshold == single_phase.iloc[:20]["score"].quantile(0.70)


def test_materialization_uses_micro_cost_and_closed_exit() -> None:
    specification = next(
        row
        for row in structural_manifest()
        if row["target_market"] == "ES"
        and row["participation_state"] == "MICRO_DOMINANT"
        and row["return_policy"] == "CONTINUATION"
        and row["threshold_quantile"] == 0.70
        and row["holding_minutes"] == 15
    )
    selected, _ = apply_phase_causal_thresholds(_score_rows(_scored_contexts(), specification), 0.70)
    events = _materialize_events(selected, specification)
    assert not events.empty
    assert events["cost"].eq(4.5).all()
    assert (events["mae_dollars"] <= 0).all()
    assert (events["source_bar_close"] <= events["entry_timestamp"]).all()
    assert (events["entry_timestamp"] < events["exit_timestamp"]).all()
    assert not events["event_session_id"].duplicated().any()


def test_qd_selector_has_market_cap_and_behavior_dedupe() -> None:
    population = structural_manifest()
    stage1 = []
    for index, row in enumerate(population[:30]):
        stage1.append(
            {
                **row,
                "stage1_passed": True,
                "quality": 100 - index,
                "behavior_fingerprint": "same" if index in {0, 1} else f"b{index}",
            }
        )
    selected = select_frozen_elites(stage1)
    assert len(selected) <= 8
    assert max(pd.Series([row["target_market"] for row in selected]).value_counts()) <= 2
    assert len({row["behavior_fingerprint"] for row in selected}) == len(selected)


def test_selector_shift_null_replays_population_with_prepared_plan() -> None:
    contexts = _scored_contexts()
    contexts["prior_pair_volatility"] = 0.01
    population = [
        row
        for row in structural_manifest()
        if row["target_market"] == "ES"
        and row["threshold_quantile"] == 0.70
        and row["holding_minutes"] == 15
    ][:2]
    probability = _selector_shift_max_probability(
        contexts, population, observed_best_quality=0.0, seed=7, draws=1
    )
    assert probability in {0.5, 1.0}
