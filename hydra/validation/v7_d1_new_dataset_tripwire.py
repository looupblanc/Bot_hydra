from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter
from dataclasses import replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, V7CostModel, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import (
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.v7_d1_microstructure_grammar import (
    D1CandidateSpec,
    D1Signal,
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)


GRAMMAR_SHA256 = "f7b7f3d8d0a43749d31986e4848eeb7285123654dd9ca47eb327284831dd1691"
TRIPWIRE_POLICY_SHA256 = "8bf84b324be6292de1213e5ade0d88bf1f83cae67502ef4ff7321c8c50c4be7d"
VALIDATION_POLICY_SHA256 = "f35fd49a91c38abfb975cacd1270249801d3e6289525559400155232ea7079ab"
EXECUTION_ADDENDUM_SHA256 = "cf4d0a4c355e58279439c1ead1c9dd6bd875a0b3deacec0432315fe716abd5f1"
SIGNAL_MANIFEST_SHA256 = "448017ea1afce1630ff722c8ea1df677775db20253c32e24a9b85429305db235"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
N_TRIALS = 247_684
NULL_THRESHOLD = 0.80
DAY_NS = 86_400_000_000_000
POINT_VALUES = {"ES": 50.0, "MES": 5.0}
MINI_EQUIVALENT = {"ES": 1.0, "MES": 0.1}
QUANTITIES = (1, 2, 4, 8)
RANDOM_SEEDS = {
    (2023, "ES"): 720301,
    (2023, "MES"): 720302,
    (2024, "ES"): 720401,
    (2024, "MES"): 720402,
}
SHUFFLE_SEEDS = {
    (2023, "ES"): 710301,
    (2023, "MES"): 710302,
    (2024, "ES"): 710401,
    (2024, "MES"): 710402,
}


class D1TripwireError(RuntimeError):
    pass


class D1NullControl(StrEnum):
    DAILY_BLOCK_SHUFFLE = "DAILY_BLOCK_SHUFFLE"
    VOLATILITY_MATCHED_RANDOM_WALK = "VOLATILITY_MATCHED_RANDOM_WALK"
    YEAR_BLOCK_PERMUTATION = "YEAR_BLOCK_PERMUTATION"


def run_d1_new_dataset_tripwire(
    *,
    project_root: str | Path,
    grammar_path: str | Path,
    tripwire_policy_path: str | Path,
    validation_policy_path: str | Path,
    execution_addendum_path: str | Path,
    signal_manifest_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    paths = {
        "grammar": Path(grammar_path).resolve(),
        "tripwire policy": Path(tripwire_policy_path).resolve(),
        "validation policy": Path(validation_policy_path).resolve(),
        "execution addendum": Path(execution_addendum_path).resolve(),
        "signal manifest": Path(signal_manifest_path).resolve(),
    }
    _verify_inputs(root, paths, proof_registry_path)
    minute, event = load_feature_store(root)
    real_signals = generate_signal_population(minute, event)
    _verify_signal_manifest(paths["signal manifest"], real_signals)
    specs = {row.candidate_id: row for row in candidate_specs()}
    cost_model = load_cost_model()
    real_events = build_candidate_events(
        minute, event, real_signals, specs, cost_model, stress=CostStress.BASE
    )
    eligible_by_year = _eligible_days_by_year(minute)
    real = _evaluate_world(real_events, eligible_by_year)

    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute, null_event = build_null_feature_world(
            minute, event, control=control
        )
        null_signals = generate_signal_population(null_minute, null_event)
        null_events = build_candidate_events(
            null_minute,
            null_event,
            null_signals,
            specs,
            cost_model,
            stress=CostStress.BASE,
        )
        summary = _evaluate_world(null_events, eligible_by_year)
        summary["signal_count"] = sum(len(rows) for rows in null_signals.values())
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])

    real_rate = float(real["pass_rate"])
    pooled_rate = pooled_passes / max(pooled_episodes, 1)
    if int(real["pass_count"]) == 0:
        verdict = "BLOCKED_UNDERPOWERED"
        null_ratio = None
    else:
        null_ratio = pooled_rate / real_rate
        verdict = (
            "ARTEFACT_GEOMETRY_ONLY"
            if null_ratio >= NULL_THRESHOLD
            else "GREEN_NULL_ADJUSTED_BASELINE"
        )
    if pooled_episodes < int(real["episode_count"]):
        raise D1TripwireError("D1 null episode count is below real")
    result = {
        "schema": "hydra_v7_d1_new_dataset_tripwire_result_v1",
        "tripwire_id": "hydra_v7_d1_new_dataset_tripwire_0001",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "real": {
            **real,
            "signal_count": sum(len(rows) for rows in real_signals.values()),
        },
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": pooled_rate,
        },
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": list(QUANTITIES),
        "feature_and_signal_recomputation": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_validation_executed": False,
        "N_trials": N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "diff_validation": [
            "hydra/validation/v7_d1_new_dataset_tripwire.py",
            "scripts/run_v7_d1_new_dataset_tripwire.py",
            "tests/test_v7_d1_new_dataset_tripwire.py",
        ],
        "CONTRE": (
            "The tripwire has only two matched August year blocks. A GREEN "
            "verdict would reject gross account geometry but would not establish "
            "seasonal or forward stability of any candidate."
        ),
        "prochaine_action": (
            "run_separately_committed_D1_candidate_tribunal"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "freeze_D1_result_and_do_not_validate_candidates"
        ),
    }
    return _write_result(result, output_dir)


def build_candidate_events(
    minute: pd.DataFrame,
    event: pd.DataFrame,
    signals: Mapping[str, Sequence[D1Signal]],
    specs: Mapping[str, D1CandidateSpec],
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> dict[str, tuple[TradePathEvent, ...]]:
    output: dict[str, tuple[TradePathEvent, ...]] = {}
    event_cache = {
        (str(product), str(bar_type), int(year)): frame.reset_index(drop=True)
        for (product, bar_type, year), frame in event.groupby(
            ["product", "bar_type", "calendar_year"], sort=True
        )
    }
    minute_cache = _minute_day_cache(minute)
    for candidate_id, spec in sorted(specs.items()):
        rows: list[TradePathEvent] = []
        for signal in signals[candidate_id]:
            if signal.source_bar_type == "MINUTE_PRINT_FEATURES":
                rows.append(
                    _cash_signal_to_event(
                        signal,
                        spec,
                        minute_cache,
                        cost_model,
                        stress=stress,
                    )
                )
            else:
                rows.append(
                    _event_signal_to_event(
                        signal,
                        spec,
                        event_cache,
                        cost_model,
                        stress=stress,
                    )
                )
        ordered = sorted(rows, key=lambda row: (row.decision_ns, row.event_id))
        if any(
            right.decision_ns < left.exit_ns
            for left, right in zip(ordered, ordered[1:], strict=False)
            if left.session_day == right.session_day
        ):
            raise D1TripwireError("D1 candidate events overlap")
        output[candidate_id] = tuple(ordered)
    return output


def _event_signal_to_event(
    signal: D1Signal,
    spec: D1CandidateSpec,
    cache: Mapping[tuple[str, str, int], pd.DataFrame],
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> TradePathEvent:
    frame = cache[(signal.product, signal.source_bar_type, signal.calendar_year)]
    position = int(signal.source_position)
    if position >= len(frame) or int(frame.iloc[position]["availability_ns"]) != signal.availability_ns:
        raise D1TripwireError("D1 event signal source drift")
    starts = frame["start_event_ns"].to_numpy(dtype=np.int64)
    entry = max(
        position + 1,
        int(np.searchsorted(starts, signal.availability_ns, side="left")),
    )
    last_held = entry + signal.holding_units - 1
    exit_position = last_held + 1
    if exit_position >= len(frame):
        raise D1TripwireError("D1 event signal lacks conservative exit print")
    contracts = frame["contract"].astype(str).to_numpy()
    if len(set(contracts[position : exit_position + 1])) != 1 or contracts[entry] != signal.contract:
        raise D1TripwireError("D1 event crosses explicit contract")
    entry_price = float(frame.iloc[entry]["open"])
    exit_price = float(frame.iloc[exit_position]["open"])
    held = frame.iloc[entry : last_held + 1]
    return _priced_event(
        signal,
        spec,
        entry_price=entry_price,
        exit_price=exit_price,
        path_low=float(min(held["low"].min(), exit_price)),
        path_high=float(max(held["high"].max(), exit_price)),
        exit_ns=int(frame.iloc[exit_position]["start_event_ns"]),
        cost_model=cost_model,
        stress=stress,
    )


def _cash_signal_to_event(
    signal: D1Signal,
    spec: D1CandidateSpec,
    cache: Mapping[tuple[str, int, str], pd.DataFrame],
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> TradePathEvent:
    local_date = _ct_date(signal.decision_ns)
    frame = cache[(signal.product, signal.calendar_year, local_date)]
    entry_rows = frame[frame["local_minute"] == 9 * 60 + 1]
    exit_rows = frame[frame["local_minute"] == 10 * 60 + 1]
    held = frame[
        (frame["local_minute"] >= 9 * 60 + 1)
        & (frame["local_minute"] <= 10 * 60)
    ]
    if len(entry_rows) != 1 or len(exit_rows) != 1 or held.empty:
        raise D1TripwireError("D1 cash-open execution window is incomplete")
    entry_row = entry_rows.iloc[0]
    exit_row = exit_rows.iloc[0]
    if int(entry_row["minute_start_ns"]) < signal.availability_ns:
        raise D1TripwireError("D1 cash entry precedes signal availability")
    if len(set(held["contract"].astype(str))) != 1 or str(entry_row["contract"]) != signal.contract:
        raise D1TripwireError("D1 cash event crosses explicit contract")
    entry_price = float(entry_row["open"])
    exit_price = float(exit_row["open"])
    return _priced_event(
        signal,
        spec,
        entry_price=entry_price,
        exit_price=exit_price,
        path_low=float(min(held["low"].min(), exit_price)),
        path_high=float(max(held["high"].max(), exit_price)),
        exit_ns=int(exit_row["minute_start_ns"]),
        cost_model=cost_model,
        stress=stress,
    )


def _priced_event(
    signal: D1Signal,
    spec: D1CandidateSpec,
    *,
    entry_price: float,
    exit_price: float,
    path_low: float,
    path_high: float,
    exit_ns: int,
    cost_model: V7CostModel,
    stress: CostStress,
) -> TradePathEvent:
    scale = signal.side * POINT_VALUES[signal.product]
    gross = (exit_price - entry_price) * scale
    cost = cost_model.round_turn_cost(
        signal.product, spec.cost_horizon, stress=stress, contracts=1.0
    )
    adverse_price = path_low if signal.side > 0 else path_high
    favorable_price = path_high if signal.side > 0 else path_low
    adverse = (adverse_price - entry_price) * scale - cost
    favorable = (favorable_price - entry_price) * scale - cost
    net = gross - cost
    session_day = _day_id(_ct_date(signal.decision_ns))
    return TradePathEvent(
        event_id=(
            f"{signal.candidate_id}:{signal.calendar_year}:"
            f"{signal.decision_ns}:{stress.value}"
        ),
        decision_ns=signal.decision_ns,
        exit_ns=exit_ns,
        session_day=session_day,
        net_pnl=float(net),
        gross_pnl=float(gross),
        worst_unrealized_pnl=float(min(adverse, net, 0.0)),
        best_unrealized_pnl=float(max(favorable, net, 0.0)),
        quantity=1,
        mini_equivalent=MINI_EQUIVALENT[signal.product],
        regime=spec.hypothesis_id,
        session_compliant=True,
        contract_limit_compliant=True,
        same_bar_ambiguous=False,
    )


def build_null_feature_world(
    minute: pd.DataFrame,
    event: pd.DataFrame,
    *,
    control: D1NullControl,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return (
            _year_permuted_prices(minute, "minute_start_ns"),
            _year_permuted_prices(event, "start_event_ns"),
        )
    if control == D1NullControl.DAILY_BLOCK_SHUFFLE:
        return (
            _within_year_price_null(
                minute, "minute_start_ns", control=control, seeds=SHUFFLE_SEEDS
            ),
            _within_year_price_null(
                event, "start_event_ns", control=control, seeds=SHUFFLE_SEEDS
            ),
        )
    if control == D1NullControl.VOLATILITY_MATCHED_RANDOM_WALK:
        return (
            _within_year_price_null(
                minute, "minute_start_ns", control=control, seeds=RANDOM_SEEDS
            ),
            _within_year_price_null(
                event, "start_event_ns", control=control, seeds=RANDOM_SEEDS
            ),
        )
    raise D1TripwireError(f"unknown D1 null control: {control}")


def _within_year_price_null(
    frame: pd.DataFrame,
    timestamp_column: str,
    *,
    control: D1NullControl,
    seeds: Mapping[tuple[int, str], int],
) -> pd.DataFrame:
    output = _with_local_date(frame, timestamp_column)
    grouping = ["calendar_year", "product"]
    if "bar_type" in output.columns:
        grouping.append("bar_type")
    for key, group in output.groupby(grouping, sort=True):
        year = int(key[0])
        product = str(key[1])
        rng = np.random.default_rng(seeds[(year, product)])
        day_groups = [
            np.asarray(indices, dtype=np.int64)
            for _, indices in group.groupby("_local_date", sort=True).groups.items()
        ]
        if control == D1NullControl.DAILY_BLOCK_SHUFFLE:
            donors = rng.permutation(len(day_groups))
            for recipient, donor in zip(day_groups, donors, strict=True):
                _copy_normalized_price_path(output, recipient, day_groups[int(donor)])
        else:
            for indices in day_groups:
                _random_walk_price_path(output, indices, rng)
    return output.drop(columns=["_local_date"])


def _year_permuted_prices(
    frame: pd.DataFrame, timestamp_column: str
) -> pd.DataFrame:
    output = _with_local_date(frame, timestamp_column)
    grouping = ["product"] + (["bar_type"] if "bar_type" in output.columns else [])
    for _, group in output.groupby(grouping, sort=True):
        by_year = {
            int(year): [
                np.asarray(indices, dtype=np.int64)
                for _, indices in year_frame.groupby("_local_date", sort=True).groups.items()
            ]
            for year, year_frame in group.groupby("calendar_year", sort=True)
        }
        if set(by_year) != {2023, 2024}:
            raise D1TripwireError("D1 year permutation requires both years")
        for recipient_year, donor_year in ((2023, 2024), (2024, 2023)):
            recipients = by_year[recipient_year]
            donors = by_year[donor_year]
            for position, recipient in enumerate(recipients):
                donor = donors[position % len(donors)]
                _copy_normalized_price_path(output, recipient, donor)
    return output.drop(columns=["_local_date"])


def _copy_normalized_price_path(
    frame: pd.DataFrame,
    recipient_indices: np.ndarray,
    donor_indices: np.ndarray,
) -> None:
    recipient_anchor = float(frame.loc[recipient_indices[0], "open"])
    donor_anchor = float(frame.loc[donor_indices[0], "open"])
    source_x = np.linspace(0.0, 1.0, len(donor_indices))
    target_x = np.linspace(0.0, 1.0, len(recipient_indices))
    values: dict[str, np.ndarray] = {}
    for column in ("open", "high", "low", "close", "vwap"):
        normalized = (
            frame.loc[donor_indices, column].to_numpy(dtype=np.float64)
            / donor_anchor
        )
        values[column] = np.interp(target_x, source_x, normalized) * recipient_anchor
    values["high"] = np.maximum.reduce(
        [values["high"], values["open"], values["close"]]
    )
    values["low"] = np.minimum.reduce(
        [values["low"], values["open"], values["close"]]
    )
    values["vwap"] = np.clip(values["vwap"], values["low"], values["high"])
    for column, value in values.items():
        frame.loc[recipient_indices, column] = value
    price_change = values["close"] - values["open"]
    donor_path = (
        frame.loc[donor_indices, "path_length_points"].to_numpy(dtype=np.float64)
        / donor_anchor
    )
    path = np.interp(target_x, source_x, donor_path) * recipient_anchor
    path = np.maximum(path, np.abs(price_change))
    frame.loc[recipient_indices, "price_change_points"] = price_change
    frame.loc[recipient_indices, "path_length_points"] = path
    if "signed_path_efficiency" in frame.columns:
        frame.loc[recipient_indices, "signed_path_efficiency"] = np.divide(
            price_change,
            path,
            out=np.zeros_like(price_change),
            where=path > 0.0,
        )


def _random_walk_price_path(
    frame: pd.DataFrame,
    indices: np.ndarray,
    rng: np.random.Generator,
) -> None:
    original_open = frame.loc[indices, "open"].to_numpy(dtype=np.float64)
    original_close = frame.loc[indices, "close"].to_numpy(dtype=np.float64)
    raw_returns = np.log(np.maximum(original_close, 1.0e-12) / np.maximum(original_open, 1.0e-12))
    sigma = float(np.std(raw_returns, ddof=1)) if len(raw_returns) > 1 else 0.0
    increments = rng.normal(0.0, sigma, size=len(indices))
    open_ = np.empty(len(indices), dtype=np.float64)
    close = np.empty(len(indices), dtype=np.float64)
    open_[0] = original_open[0]
    for position, increment in enumerate(increments):
        if position:
            open_[position] = close[position - 1]
        close[position] = open_[position] * math.exp(float(increment))
    original_range = (
        frame.loc[indices, "high"].to_numpy(dtype=np.float64)
        - frame.loc[indices, "low"].to_numpy(dtype=np.float64)
    )
    relative_range = original_range / np.maximum(original_open, 1.0e-12)
    relative_range = relative_range[rng.permutation(len(relative_range))]
    half_range = 0.5 * relative_range * open_
    high = np.maximum(open_, close) + half_range
    low = np.maximum(1.0e-9, np.minimum(open_, close) - half_range)
    price_change = close - open_
    path = np.maximum(np.abs(price_change), high - low)
    frame.loc[indices, "open"] = open_
    frame.loc[indices, "high"] = high
    frame.loc[indices, "low"] = low
    frame.loc[indices, "close"] = close
    frame.loc[indices, "vwap"] = (high + low + close) / 3.0
    frame.loc[indices, "price_change_points"] = price_change
    frame.loc[indices, "path_length_points"] = path
    if "signed_path_efficiency" in frame.columns:
        frame.loc[indices, "signed_path_efficiency"] = np.divide(
            price_change,
            path,
            out=np.zeros_like(price_change),
            where=path > 0.0,
        )


def _evaluate_world(
    events: Mapping[str, Sequence[TradePathEvent]],
    eligible_by_year: Mapping[int, Sequence[int]],
) -> dict[str, Any]:
    pass_count = breach_count = timeout_count = compliance_count = episode_count = 0
    candidate_rows: dict[str, Any] = {}
    for candidate_id, candidate_events in sorted(events.items()):
        per_quantity: dict[str, Any] = {}
        for quantity in QUANTITIES:
            scaled = tuple(_scale_event(row, quantity) for row in candidate_events)
            terminal = Counter()
            target_progress: list[float] = []
            minimum_buffers: list[float] = []
            for year in sorted(eligible_by_year):
                days = tuple(eligible_by_year[year])
                starts = days[: max(0, len(days) - 20 + 1)]
                for start in starts:
                    episode = run_combine_episode(
                        scaled,
                        days,
                        start_day=int(start),
                        maximum_duration_days=20,
                        config=Topstep150KConfig(
                            mll_mode=MllMode.EOD_LEVEL_RT_BREACH
                        ),
                        maximum_mini_equivalent=15.0,
                    )
                    terminal[episode.terminal.value] += 1
                    target_progress.append(float(episode.target_progress))
                    minimum_buffers.append(float(episode.minimum_mll_buffer))
            count = sum(terminal.values())
            row = {
                "episode_count": count,
                "pass_count": terminal[CombineTerminal.PASSED.value],
                "mll_breach_count": terminal[CombineTerminal.MLL_BREACH.value],
                "timeout_count": terminal[CombineTerminal.TIMEOUT.value],
                "compliance_failure_count": terminal[
                    CombineTerminal.COMPLIANCE_FAILURE.value
                ],
                "median_target_progress": float(
                    np.median(target_progress) if target_progress else 0.0
                ),
                "minimum_mll_buffer": float(
                    min(minimum_buffers) if minimum_buffers else 0.0
                ),
            }
            per_quantity[str(quantity)] = row
            episode_count += count
            pass_count += int(row["pass_count"])
            breach_count += int(row["mll_breach_count"])
            timeout_count += int(row["timeout_count"])
            compliance_count += int(row["compliance_failure_count"])
        candidate_rows[candidate_id] = per_quantity
    return {
        "episode_count": episode_count,
        "pass_count": pass_count,
        "pass_rate": pass_count / max(episode_count, 1),
        "mll_breach_count": breach_count,
        "mll_breach_rate": breach_count / max(episode_count, 1),
        "timeout_count": timeout_count,
        "compliance_failure_count": compliance_count,
        "candidate_quantity_results": candidate_rows,
    }


def _scale_event(event: TradePathEvent, quantity: int) -> TradePathEvent:
    return replace(
        event,
        event_id=f"{event.event_id}:Q{quantity}",
        net_pnl=event.net_pnl * quantity,
        gross_pnl=event.gross_pnl * quantity,
        worst_unrealized_pnl=event.worst_unrealized_pnl * quantity,
        best_unrealized_pnl=event.best_unrealized_pnl * quantity,
        quantity=quantity,
        mini_equivalent=event.mini_equivalent * quantity,
    )


def _eligible_days_by_year(minute: pd.DataFrame) -> dict[int, tuple[int, ...]]:
    timestamps = pd.to_datetime(
        minute["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    dates = np.asarray([value.isoformat() for value in timestamps.date])
    years = minute["calendar_year"].to_numpy(dtype=np.int64)
    return {
        year: tuple(sorted({_day_id(day) for day in dates[years == year]}))
        for year in sorted(set(years))
    }


def _minute_day_cache(
    minute: pd.DataFrame,
) -> dict[tuple[str, int, str], pd.DataFrame]:
    output = _with_local_date(minute, "minute_start_ns")
    timestamps = pd.to_datetime(
        output["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    output["local_minute"] = timestamps.hour * 60 + timestamps.minute
    return {
        (str(product), int(year), str(local_date)): frame.reset_index(drop=True)
        for (product, year, local_date), frame in output.groupby(
            ["product", "calendar_year", "_local_date"], sort=True
        )
    }


def _with_local_date(frame: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
    output = frame.copy()
    timestamps = pd.to_datetime(
        output[timestamp_column].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    output["_local_date"] = [value.isoformat() for value in timestamps.date]
    return output


def _ct_date(timestamp_ns: int) -> str:
    return (
        pd.Timestamp(timestamp_ns, unit="ns", tz="UTC")
        .tz_convert("America/Chicago")
        .date()
        .isoformat()
    )


def _day_id(date_value: str) -> int:
    return int(pd.Timestamp(str(date_value), tz="UTC").value // DAY_NS)


def _verify_inputs(
    root: Path,
    paths: Mapping[str, Path],
    proof_registry_path: str | Path,
) -> None:
    expected = {
        "grammar": GRAMMAR_SHA256,
        "tripwire policy": TRIPWIRE_POLICY_SHA256,
        "validation policy": VALIDATION_POLICY_SHA256,
        "execution addendum": EXECUTION_ADDENDUM_SHA256,
        "signal manifest": SIGNAL_MANIFEST_SHA256,
    }
    drift = [
        name for name, path in paths.items() if _sha256(path) != expected[name]
    ]
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        drift.append("mission contract")
    if drift:
        raise D1TripwireError("D1 frozen input hash mismatch: " + ",".join(drift))
    proof = load_and_verify(proof_registry_path)
    if multiplicity_trial_count(proof) != N_TRIALS:
        raise D1TripwireError("D1 multiplicity reservation mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise D1TripwireError("D1 unexpected proof-window state")


def _verify_signal_manifest(
    path: Path, signals: Mapping[str, Sequence[D1Signal]]
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise D1TripwireError("D1 signal manifest logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload.get("signals"):
        raise D1TripwireError("D1 signal regeneration drift")
    if payload.get("contains_future_outcomes_or_pnl") is not False:
        raise D1TripwireError("D1 signal manifest contains outcomes")


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "d1_new_dataset_tripwire_result.json"
    report_path = destination / "d1_new_dataset_tripwire_report.md"
    temporary = result_path.with_name(result_path.name + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    report_path.write_text(_render_report(result), encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    result["report_path"] = str(report_path)
    return result


def _render_report(result: Mapping[str, Any]) -> str:
    ratio = "NA" if result["NULL_RATIO"] is None else f"{result['NULL_RATIO']:.12f}"
    return "\n".join(
        [
            "# HYDRA V7 — D1 new-dataset tripwire",
            "",
            f"[HYDRA-V7] phase=D step=4 verdict={result['verdict']}",
            "gate=D1_TRIPWIRE preuve=reports/v7/data/d1_new_dataset_tripwire_result.json#pending tests=pending",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v7_d1_new_dataset_tripwire.py,scripts/run_v7_d1_new_dataset_tripwire.py,tests/test_v7_d1_new_dataset_tripwire.py CONTRE=deux_blocs_aout_limitent_la_portee_du_tripwire",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- NULL_RATIO : `{ratio}`",
            f"- Episodes réels : `{result['real']['episode_count']}`",
            f"- Passes réelles : `{result['real']['pass_count']}`",
            f"- Episodes null : `{result['pooled_null']['episode_count']}`",
            f"- Passes null : `{result['pooled_null']['pass_count']}`",
            "- Combine : diagnostic uniquement, jamais fitness ni gate candidat.",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "D1NullControl",
    "D1TripwireError",
    "build_candidate_events",
    "build_null_feature_world",
    "run_d1_new_dataset_tripwire",
]
