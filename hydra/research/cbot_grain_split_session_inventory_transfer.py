"""Bounded CBOT grain split-session inventory-transfer tripwire."""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.economic_evolution.schema import stable_hash
from hydra.research.usda_grain_information_shock_tripwire import (
    SYMBOLS,
    _load_bars,
    audit_inputs as audit_usda_inputs,
)


MANIFEST = Path("config/research/cbot_grain_split_session_inventory_transfer_v1.json")
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
CHICAGO = "America/Chicago"


class GrainSplitSessionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Cell:
    mechanism: str
    decision_bars: int
    minimum_overnight_displacement_to_pre_pause_range: float
    holding_minutes: int
    execution_symbol: str

@dataclass(frozen=True, slots=True)
class SessionContext:
    symbol: str
    session_day: date
    role: str
    decision_bars: int
    overnight_direction: int
    response_direction: int
    overnight_ratio: float
    response_ratio: float
    prior_range: float
    decision_time: pd.Timestamp
    entry_time: pd.Timestamp | None
    entry_price: float | None
    instrument_id: str | None


def _candidate_id(cell: Cell, manifest: Mapping[str, Any]) -> str:
    """Fingerprint the complete frozen executable specification, not only its cell."""

    return "grain_reopen_" + stable_hash(
        {
            "cell": asdict(cell),
            "manifest_hash": manifest["manifest_hash"],
            "execution": manifest["execution"],
            "session_contract": manifest["session_contract"],
            "causal_contract": manifest["causal_contract"],
        }
    )[:20]


def _economic_result_hash(result: Mapping[str, Any]) -> str:
    return stable_hash(
        {key: value for key, value in result.items() if key != "runtime_seconds"}
    )


def _read_manifest(root: Path) -> dict[str, Any]:
    path = root / MANIFEST
    manifest = json.loads(path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise GrainSplitSessionError("frozen manifest hash drift")
    if (
        manifest["data_contract"]["symbols"] != ["ZC.c.0", "ZS.c.0", "ZW.c.0"]
        or manifest["data_contract"]["incremental_data_spend_usd"] != 0.0
        or manifest["data_contract"]["q4_2024_access"] is not False
        or manifest["governance"]["maximum_cpu_workers"] != 1
    ):
        raise GrainSplitSessionError("data or governance contract drift")
    for frozen in manifest["frozen_inputs"].values():
        source = (root / frozen["path"]).resolve()
        if not source.is_file() or sha256_file(source) != frozen["sha256"]:
            raise GrainSplitSessionError(f"frozen input drift: {source}")
    return manifest


def _role_for_day(day: date, manifest: Mapping[str, Any]) -> str | None:
    stamp = pd.Timestamp(day)
    for role in manifest["chronological_roles"]:
        if pd.Timestamp(role["start"]) <= stamp < pd.Timestamp(role["end"]):
            return str(role["role"])
    return None


def _utc(day: date, clock: str) -> pd.Timestamp:
    return pd.Timestamp(f"{day.isoformat()} {clock}").tz_localize(CHICAGO).tz_convert("UTC")


def _roll_guard_days_chicago(frame: pd.DataFrame) -> set[date]:
    ordered = frame.sort_index(kind="mergesort")
    changed = ordered["instrument_id"].astype(str).ne(ordered["instrument_id"].astype(str).shift())
    boundaries = {
        pd.Timestamp(value).tz_convert(CHICAGO).date()
        for value in ordered.loc[changed, "timestamp"].iloc[1:].tolist()
    }
    guarded: set[date] = set()
    for day in boundaries:
        guarded.update({day - timedelta(days=1), day, day + timedelta(days=1)})
    return guarded


def _wasde_days(root: Path) -> set[date]:
    payload = json.loads(
        (root / "config/research/usda_grain_information_shock_tripwire_v1.json").read_text(
            encoding="utf-8"
        )
    )
    return {date.fromisoformat(value) for value in payload["release_contract"]["release_dates"]}


def _first_row_strictly_after(
    frame: pd.DataFrame, timestamp: pd.Timestamp, *, limit_minutes: int
) -> pd.Series | None:
    rows = frame.loc[timestamp : timestamp + pd.Timedelta(minutes=limit_minutes)]
    rows = rows.loc[rows.index > timestamp]
    return None if rows.empty else rows.iloc[0]


def _build_contexts(
    root: Path,
    bars: Mapping[str, pd.DataFrame],
    manifest: Mapping[str, Any],
) -> tuple[dict[tuple[str, int], list[SessionContext]], dict[str, Any]]:
    excluded = _wasde_days(root)
    output: dict[tuple[str, int], list[SessionContext]] = {}
    diagnostics: dict[str, Any] = {}
    minimum_overnight = int(manifest["session_contract"]["minimum_overnight_bars"])
    minimum_pre = int(manifest["session_contract"]["minimum_pre_pause_bars"])
    for symbol in SYMBOLS:
        frame = bars[symbol]
        chicago = frame["timestamp"].dt.tz_convert(CHICAGO)
        reopen_days = sorted(
            set(chicago.loc[chicago.dt.hour.eq(8) & chicago.dt.minute.eq(30)].dt.date)
        )
        roll_guard = _roll_guard_days_chicago(frame)
        built = 0
        skipped: dict[str, int] = {
            "WASDE": 0,
            "ROLL_GUARD": 0,
            "ROLE": 0,
            "OVERNIGHT_COVERAGE": 0,
            "OVERNIGHT_BOUNDARY_COVERAGE": 0,
            "PRE_PAUSE_COVERAGE": 0,
            "REOPEN_COVERAGE": 0,
        }
        for decision_bars in manifest["candidate_lattice"]["decision_bars"]:
            contexts: list[SessionContext] = []
            for day in reopen_days:
                role = _role_for_day(day, manifest)
                if role is None:
                    skipped["ROLE"] += 1
                    continue
                if day in excluded:
                    skipped["WASDE"] += 1
                    continue
                if day in roll_guard:
                    skipped["ROLL_GUARD"] += 1
                    continue
                overnight = frame.loc[
                    _utc(day - timedelta(days=1), "19:00") : _utc(day, "07:45")
                    - pd.Timedelta(nanoseconds=1)
                ]
                if len(overnight) < minimum_overnight:
                    skipped["OVERNIGHT_COVERAGE"] += 1
                    continue
                overnight_open = _utc(day - timedelta(days=1), "19:00")
                overnight_last = _utc(day, "07:44")
                if (
                    pd.Timestamp(overnight.index[0]) != overnight_open
                    or pd.Timestamp(overnight.index[-1])
                    < overnight_last - pd.Timedelta(minutes=5)
                ):
                    skipped["OVERNIGHT_BOUNDARY_COVERAGE"] += 1
                    continue
                pre = frame.loc[
                    _utc(day, "07:15") : _utc(day, "07:45") - pd.Timedelta(nanoseconds=1)
                ]
                if len(pre) < minimum_pre:
                    skipped["PRE_PAUSE_COVERAGE"] += 1
                    continue
                release = _utc(day, "08:30")
                expected = [release + pd.Timedelta(minutes=index) for index in range(decision_bars)]
                if any(timestamp not in frame.index for timestamp in expected):
                    skipped["REOPEN_COVERAGE"] += 1
                    continue
                response = frame.loc[expected]
                prior_range = float(pre["high"].max() - pre["low"].min())
                overnight_move = float(overnight.iloc[-1]["close"] - overnight.iloc[0]["open"])
                reopen_move = float(response.iloc[-1]["close"] - response.iloc[0]["open"])
                if prior_range < float(manifest["execution"]["tick_size"]) or overnight_move == 0.0 or reopen_move == 0.0:
                    continue
                decision_time = expected[-1] + pd.Timedelta(minutes=1)
                entry = _first_row_strictly_after(frame, decision_time, limit_minutes=2)
                contexts.append(
                    SessionContext(
                        symbol=symbol,
                        session_day=day,
                        role=role,
                        decision_bars=int(decision_bars),
                        overnight_direction=1 if overnight_move > 0.0 else -1,
                        response_direction=1 if reopen_move > 0.0 else -1,
                        overnight_ratio=abs(overnight_move) / prior_range,
                        response_ratio=abs(reopen_move) / prior_range,
                        prior_range=prior_range,
                        decision_time=decision_time,
                        entry_time=(
                            pd.Timestamp(entry["timestamp"]) if entry is not None else None
                        ),
                        entry_price=float(entry["open"]) if entry is not None else None,
                        instrument_id=str(entry["instrument_id"]) if entry is not None else None,
                    )
                )
            output[(symbol, int(decision_bars))] = contexts
            built += len(contexts)
        diagnostics[symbol] = {
            "reopen_days_observed": len(reopen_days),
            "context_count_across_decision_windows": built,
            "unique_session_count": len(
                {context.session_day for key, values in output.items() if key[0] == symbol for context in values}
            ),
            "roll_guard_day_count": len(roll_guard),
            "skipped_counts_across_decision_windows": skipped,
        }
    return output, diagnostics


def _cells(manifest: Mapping[str, Any]) -> list[Cell]:
    lattice = manifest["candidate_lattice"]
    return [
        Cell(mechanism, decision_bars, threshold, hold, symbol)
        for mechanism in lattice["mechanisms"]
        for decision_bars in lattice["decision_bars"]
        for threshold in lattice["minimum_overnight_displacement_to_pre_pause_range"]
        for hold in lattice["holding_minutes"]
        for symbol in lattice["execution_symbols"]
    ]


def _signal(context: SessionContext, cell: Cell, manifest: Mapping[str, Any]) -> int | None:
    if context.overnight_ratio < cell.minimum_overnight_displacement_to_pre_pause_range:
        return None
    if context.response_ratio < float(
        manifest["candidate_lattice"]["minimum_reopen_response_to_pre_pause_range"]
    ):
        return None
    if cell.mechanism == "INVENTORY_CONTINUATION":
        if context.overnight_direction != context.response_direction:
            return None
        return context.response_direction
    if cell.mechanism == "INVENTORY_REJECTION":
        if context.overnight_direction == context.response_direction:
            return None
        return context.response_direction
    raise GrainSplitSessionError(f"unknown mechanism: {cell.mechanism}")


def _trade_event(
    frame: pd.DataFrame,
    signal_context: SessionContext,
    trade_context: SessionContext,
    cell: Cell,
    manifest: Mapping[str, Any],
    *,
    direction_flip: bool,
    timing_control: bool,
) -> dict[str, Any] | None:
    direction = _signal(signal_context, cell, manifest)
    if direction is None:
        return None
    if direction_flip:
        direction *= -1
    entry_time = trade_context.entry_time
    entry_price = trade_context.entry_price
    if entry_time is None or entry_price is None:
        return _censored_event(
            signal_context,
            trade_context,
            cell,
            manifest,
            direction,
            reason="MISSING_EXECUTABLE_ENTRY",
            direction_flip=direction_flip,
            timing_control=timing_control,
        )
    if entry_time <= trade_context.decision_time:
        raise GrainSplitSessionError("entry is not strictly after decision_time")
    tick_size = float(manifest["execution"]["tick_size"])
    point_value = float(manifest["execution"]["point_value_usd"])
    stop_ticks = max(
        2,
        int(
            round(
                trade_context.prior_range
                * float(manifest["candidate_lattice"]["stop_pre_pause_range_fraction"])
                / tick_size
            )
        ),
    )
    stop_distance = stop_ticks * tick_size
    target_ticks = max(
        1,
        int(
            round(
                stop_ticks * float(manifest["candidate_lattice"]["target_stop_multiple"])
            )
        ),
    )
    stop_price = entry_price - direction * stop_distance
    target_price = entry_price + direction * target_ticks * tick_size
    deadline = entry_time + pd.Timedelta(minutes=cell.holding_minutes)
    path = frame.loc[entry_time : deadline + pd.Timedelta(minutes=2)]
    if path.empty:
        return _censored_event(
            signal_context,
            trade_context,
            cell,
            manifest,
            direction,
            reason="MISSING_PATH_COVERAGE",
            direction_flip=direction_flip,
            timing_control=timing_control,
        )
    exit_price: float | None = None
    exit_time: pd.Timestamp | None = None
    exit_reason = ""
    minimum_open_pnl_price = 0.0
    for _, row in path.iterrows():
        timestamp = pd.Timestamp(row["timestamp"])
        if timestamp >= deadline:
            exit_price = float(row["open"])
            exit_time = timestamp
            exit_reason = "TIME_EXIT"
            break
        if direction > 0:
            minimum_open_pnl_price = min(minimum_open_pnl_price, float(row["low"]) - entry_price)
            stop_hit = float(row["low"]) <= stop_price
            target_hit = float(row["high"]) >= target_price
        else:
            minimum_open_pnl_price = min(minimum_open_pnl_price, entry_price - float(row["high"]))
            stop_hit = float(row["high"]) >= stop_price
            target_hit = float(row["low"]) <= target_price
        if stop_hit:
            exit_price = stop_price
            exit_time = timestamp + pd.Timedelta(minutes=1)
            exit_reason = "STOP_FIRST"
            break
        if target_hit:
            exit_price = target_price
            exit_time = timestamp + pd.Timedelta(minutes=1)
            exit_reason = "TARGET"
            break
    if exit_price is None or exit_time is None:
        return _censored_event(
            signal_context,
            trade_context,
            cell,
            manifest,
            direction,
            reason="MISSING_EXIT_COVERAGE",
            direction_flip=direction_flip,
            timing_control=timing_control,
        )
    gross = direction * (exit_price - entry_price) * point_value
    commission = float(manifest["execution"]["round_turn_commission_and_fees_usd"])
    normal_cost = commission + 2.0 * float(
        manifest["execution"]["normal_slippage_ticks_per_side"]
    ) * float(manifest["execution"]["tick_value_usd"])
    stressed_cost = commission + 2.0 * float(
        manifest["execution"]["stressed_slippage_ticks_per_side"]
    ) * float(manifest["execution"]["tick_value_usd"])
    return {
        "candidate_id": _candidate_id(cell, manifest),
        "session_day": signal_context.session_day.isoformat(),
        "trade_session_day": trade_context.session_day.isoformat(),
        "release_day": signal_context.session_day.isoformat(),
        "role": signal_context.role,
        "symbol": cell.execution_symbol,
        "mechanism": cell.mechanism,
        "direction": direction,
        "overnight_ratio": signal_context.overnight_ratio,
        "response_ratio": signal_context.response_ratio,
        "decision_time": signal_context.decision_time.isoformat(),
        "entry_time": entry_time.isoformat(),
        "entry_price": entry_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "exit_time": exit_time.isoformat(),
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "gross_pnl_usd": gross,
        "normal_cost_usd": normal_cost,
        "stressed_cost_usd": stressed_cost,
        "normal_net_usd": gross - normal_cost,
        "stressed_net_usd": gross - stressed_cost,
        "minimum_open_pnl_normal_usd": minimum_open_pnl_price * point_value - normal_cost / 2.0,
        "minimum_open_pnl_stressed_usd": minimum_open_pnl_price * point_value - stressed_cost / 2.0,
        "direction_flip_control": direction_flip,
        "next_session_timing_control": timing_control,
        "outcome_state": "FULL_COVERAGE",
        "instrument_id": trade_context.instrument_id,
        "fill_policy_id": "NEXT_TRADABLE_BAR_OPEN_STRICTLY_AFTER_DECISION_V1",
        "event_hash": stable_hash(
            {
                "candidate_id": _candidate_id(cell, manifest),
                "manifest_hash": manifest["manifest_hash"],
                "cell": asdict(cell),
                "signal_day": signal_context.session_day.isoformat(),
                "trade_day": trade_context.session_day.isoformat(),
                "direction": direction,
                "instrument_id": trade_context.instrument_id,
                "entry_time": entry_time.isoformat(),
                "exit_time": exit_time.isoformat(),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "normal_cost_usd": normal_cost,
                "stressed_cost_usd": stressed_cost,
                "fill_policy_id": "NEXT_TRADABLE_BAR_OPEN_STRICTLY_AFTER_DECISION_V1",
            }
        ),
    }


def _censored_event(
    signal_context: SessionContext,
    trade_context: SessionContext,
    cell: Cell,
    manifest: Mapping[str, Any],
    direction: int,
    *,
    reason: str,
    direction_flip: bool,
    timing_control: bool,
) -> dict[str, Any]:
    core = {
        "candidate_id": _candidate_id(cell, manifest),
        "session_day": signal_context.session_day.isoformat(),
        "trade_session_day": trade_context.session_day.isoformat(),
        "release_day": signal_context.session_day.isoformat(),
        "role": signal_context.role,
        "symbol": cell.execution_symbol,
        "mechanism": cell.mechanism,
        "direction": direction,
        "overnight_ratio": signal_context.overnight_ratio,
        "response_ratio": signal_context.response_ratio,
        "decision_time": signal_context.decision_time.isoformat(),
        "entry_time": (
            trade_context.entry_time.isoformat()
            if trade_context.entry_time is not None
            else None
        ),
        "direction_flip_control": direction_flip,
        "next_session_timing_control": timing_control,
        "outcome_state": "DATA_CENSORED",
        "censor_reason": reason,
        "instrument_id": trade_context.instrument_id,
        "fill_policy_id": "NEXT_TRADABLE_BAR_OPEN_STRICTLY_AFTER_DECISION_V1",
    }
    return {
        **core,
        "event_hash": stable_hash(
            {"manifest_hash": manifest["manifest_hash"], "cell": asdict(cell), **core}
        ),
    }


def _summary(events: Sequence[Mapping[str, Any]], role: str) -> dict[str, Any]:
    role_rows = [row for row in events if row["role"] == role]
    rows = [row for row in role_rows if row["outcome_state"] == "FULL_COVERAGE"]
    stressed = [float(row["stressed_net_usd"]) for row in rows]
    gross = [float(row["gross_pnl_usd"]) for row in rows]
    costs = [float(row["stressed_cost_usd"]) for row in rows]
    positive = [value for value in stressed if value > 0.0]
    return {
        "role": role,
        "signal_count": len(role_rows),
        "event_count": len(rows),
        "data_censored_count": len(role_rows) - len(rows),
        "gross_pnl_usd": float(sum(gross)),
        "normal_net_usd": float(sum(float(row["normal_net_usd"]) for row in rows)),
        "stressed_net_usd": float(sum(stressed)),
        "median_stressed_event_usd": float(np.median(stressed)) if stressed else None,
        "lower_quartile_stressed_event_usd": float(np.quantile(stressed, 0.25)) if stressed else None,
        "positive_stressed_event_rate": float(sum(value > 0.0 for value in stressed) / len(stressed)) if stressed else None,
        "stressed_edge_to_cost_ratio": float(sum(gross) / sum(costs)) if sum(costs) > 0.0 else None,
        "maximum_single_event_positive_profit_share": float(max(positive) / sum(positive)) if positive and sum(positive) > 0.0 else None,
        "minimum_open_pnl_stressed_usd": min(
            (float(row["minimum_open_pnl_stressed_usd"]) for row in rows), default=None
        ),
        "exit_reasons": {
            reason: sum(row["exit_reason"] == reason for row in rows)
            for reason in sorted({str(row["exit_reason"]) for row in rows})
        },
        "event_path_hash": stable_hash([row["event_hash"] for row in rows]),
    }


def _evaluate_cell(
    bars: Mapping[str, pd.DataFrame],
    contexts: Mapping[tuple[str, int], Sequence[SessionContext]],
    cell: Cell,
    manifest: Mapping[str, Any],
    *,
    direction_flip: bool = False,
    timing_lag_sessions: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source = contexts[(cell.execution_symbol, cell.decision_bars)]
    events: list[dict[str, Any]] = []
    for index in range(timing_lag_sessions, len(source)):
        signal_context = source[index - timing_lag_sessions]
        trade_context = source[index]
        if timing_lag_sessions and signal_context.role != trade_context.role:
            continue
        event = _trade_event(
            bars[cell.execution_symbol],
            signal_context,
            trade_context,
            cell,
            manifest,
            direction_flip=direction_flip,
            timing_control=bool(timing_lag_sessions),
        )
        if event is not None:
            events.append(event)
    return events, {
        "candidate_id": _candidate_id(cell, manifest),
        "cell": asdict(cell),
        "event_count": len(events),
        "roles": {role: _summary(events, role) for role in ROLES},
        "event_hash": stable_hash([event["event_hash"] for event in events]),
    }


def _evaluate_next_session_control_pair(
    bars: Mapping[str, pd.DataFrame],
    contexts: Mapping[tuple[str, int], Sequence[SessionContext]],
    cell: Cell,
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Replay primary and lagged timing control on identical admissible dates."""

    source = contexts[(cell.execution_symbol, cell.decision_bars)]
    primary_events: list[dict[str, Any]] = []
    control_events: list[dict[str, Any]] = []
    for index in range(1, len(source)):
        prior = source[index - 1]
        current = source[index]
        if prior.role != current.role:
            continue
        primary = _trade_event(
            bars[cell.execution_symbol],
            current,
            current,
            cell,
            manifest,
            direction_flip=False,
            timing_control=False,
        )
        control = _trade_event(
            bars[cell.execution_symbol],
            prior,
            current,
            cell,
            manifest,
            direction_flip=False,
            timing_control=True,
        )
        if primary is not None:
            primary_events.append(primary)
        if control is not None:
            control_events.append(control)

    def pack(events: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "candidate_id": _candidate_id(cell, manifest),
            "cell": asdict(cell),
            "event_count": len(events),
            "roles": {role: _summary(events, role) for role in ROLES},
            "event_hash": stable_hash([event["event_hash"] for event in events]),
        }

    return pack(primary_events), pack(control_events)


def _passes_role(summary: Mapping[str, Any], *, minimum: int, gate: Mapping[str, Any]) -> bool:
    concentration = summary["maximum_single_event_positive_profit_share"]
    return bool(
        int(summary["event_count"]) >= minimum
        and float(summary["stressed_net_usd"]) > 0.0
        and float(summary["stressed_edge_to_cost_ratio"] or -math.inf)
        >= float(gate["minimum_stressed_edge_to_cost_ratio"])
        and concentration is not None
        and float(concentration) <= float(gate["maximum_single_event_profit_concentration"])
    )


def _rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    discovery = row["roles"]["DISCOVERY"]
    return (
        float(discovery["stressed_net_usd"]),
        float(discovery["lower_quartile_stressed_event_usd"] or -math.inf),
        float(discovery["positive_stressed_event_rate"] or 0.0),
        -int(row["cell"]["holding_minutes"]),
        row["candidate_id"],
    )


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    manifest = _read_manifest(project)
    source_audit = audit_usda_inputs(project)
    bars, data_receipt = _load_bars(source_audit)
    contexts, context_receipt = _build_contexts(project, bars, manifest)
    lattice = _cells(manifest)
    evaluated: list[dict[str, Any]] = []
    upper_events: list[dict[str, Any]] = []
    for cell in lattice:
        events, result = _evaluate_cell(bars, contexts, cell, manifest)
        evaluated.append(result)
        upper_events.extend(events)
    gate = manifest["selection_gate"]
    eligible = [
        row
        for row in evaluated
        if int(row["roles"]["DISCOVERY"]["event_count"]) >= int(gate["minimum_discovery_events"])
        and float(row["roles"]["DISCOVERY"]["stressed_net_usd"]) > 0.0
        and float(row["roles"]["DISCOVERY"]["stressed_edge_to_cost_ratio"] or -math.inf)
        >= float(gate["minimum_stressed_edge_to_cost_ratio"])
    ]
    eligible.sort(key=_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    niches: set[tuple[Any, ...]] = set()
    for row in eligible:
        niche = (
            row["cell"]["mechanism"],
            row["cell"]["execution_symbol"],
            row["cell"]["decision_bars"],
            row["cell"]["holding_minutes"],
        )
        if niche in niches:
            continue
        niches.add(niche)
        selected.append(row)
        if len(selected) >= int(gate["maximum_selected_cells"]):
            break
    lookup = {_candidate_id(cell, manifest): cell for cell in lattice}
    selected_results: list[dict[str, Any]] = []
    passers: list[str] = []
    positive_both: list[str] = []
    for row in selected:
        cell = lookup[row["candidate_id"]]
        _events, primary = _evaluate_cell(bars, contexts, cell, manifest)
        _events, flip = _evaluate_cell(
            bars, contexts, cell, manifest, direction_flip=True
        )
        timing_primary, timing = _evaluate_next_session_control_pair(
            bars, contexts, cell, manifest
        )
        role_gate = {
            role: _passes_role(
                primary["roles"][role],
                minimum=int(gate[f"minimum_{role.lower()}_events"]),
                gate=gate,
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        }
        positive = all(
            float(primary["roles"][role]["stressed_net_usd"]) > 0.0
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        if positive:
            positive_both.append(_candidate_id(cell, manifest))
        control_gate = all(
            float(primary["roles"][role]["stressed_net_usd"])
            > float(flip["roles"][role]["stressed_net_usd"])
            and float(timing_primary["roles"][role]["stressed_net_usd"])
            > float(timing["roles"][role]["stressed_net_usd"])
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        passed = all(role_gate.values()) and control_gate
        if passed:
            passers.append(_candidate_id(cell, manifest))
        selected_results.append(
            {
                "candidate_id": _candidate_id(cell, manifest),
                "cell": asdict(cell),
                "primary": primary,
                "controls": {
                    "direction_flip": flip,
                    "next_session_matched_primary": timing_primary,
                    "next_session_signal_timing": timing,
                },
                "role_gate": role_gate,
                "control_gate": control_gate,
                "event_gate_passed": passed,
            }
        )
    by_event: dict[tuple[str, str, str], float] = {}
    for event in upper_events:
        if event["outcome_state"] != "FULL_COVERAGE":
            continue
        key = (str(event["role"]), str(event["session_day"]), str(event["symbol"]))
        by_event[key] = max(by_event.get(key, -math.inf), float(event["stressed_net_usd"]))
    upper_bound = {
        role: {
            "market_session_count": len(values := [
                value for (item_role, _day, _symbol), value in by_event.items() if item_role == role
            ]),
            "non_deployable_hindsight_stressed_net_usd": float(
                sum(max(0.0, value) for value in values)
            ),
            "positive_event_rate": float(sum(value > 0.0 for value in values) / len(values)) if values else None,
            "flat_choice_count": int(sum(value <= 0.0 for value in values)),
            "interpretation": "NON_DEPLOYABLE_UPPER_BOUND_NEVER_PROMOTION_ELIGIBLE",
        }
        for role in ROLES
    }
    if passers:
        status = "GRAIN_SPLIT_SESSION_EVENT_ALPHA_GREEN"
        next_action = "OPEN_EXACT_ACCOUNT_SIZE_MATRIX_FOR_FROZEN_PASSERS"
    elif positive_both:
        status = "GRAIN_SPLIT_SESSION_EVENT_ALPHA_WEAK"
        next_action = "PRESERVE_DIAGNOSTIC_CLOSE_EXACT_SPEC_AND_PIVOT_TO_CME_CRYPTO_EVENT_TIME"
    else:
        status = "GRAIN_SPLIT_SESSION_EVENT_ALPHA_FALSIFIED"
        next_action = "TOMBSTONE_EXACT_SPEC_AND_PIVOT_TO_CME_CRYPTO_EVENT_TIME"
    result: dict[str, Any] = {
        "schema": "hydra_cbot_grain_split_session_inventory_transfer_economic_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "manifest_hash": manifest["manifest_hash"],
        "source_acquisition_receipt_hash": source_audit["receipt"]["receipt_hash"],
        "source_audit_hash": source_audit["audit_hash"],
        "incremental_data_spend_usd": 0.0,
        "data_receipt": data_receipt,
        "context_receipt": context_receipt,
        "proposal_count": len(lattice),
        "discovery_eligible_count": len(eligible),
        "best_diagnostic_by_discovery": sorted(
            evaluated, key=_rank, reverse=True
        )[:5],
        "selected_candidate_ids": [row["candidate_id"] for row in selected],
        "selected_results": selected_results,
        "event_gate_passer_ids": passers,
        "positive_both_heldout_without_full_gate_ids": positive_both,
        "non_deployable_upper_bound": upper_bound,
        "account_matrix_executed": False,
        "account_matrix_block_reason": None if passers else "EVENT_ECONOMICS_GATE_NOT_GREEN",
        "tier_ceiling": "E" if passers else "H_DIAGNOSTIC",
        "runtime_seconds": time.perf_counter() - started,
        "next_action": next_action,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
    }
    result["result_hash"] = _economic_result_hash(result)
    return result


__all__ = ["Cell", "GrainSplitSessionError", "SessionContext", "run_tripwire"]
