"""Causal official-USDA WASDE grain information-shock tripwire.

The report calendar is frozen before acquisition.  Cells are selected on the
discovery era only and replayed unchanged on validation and final-development.
The module deliberately stops before account replay unless the held-out event
economics and matched controls are green.
"""

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

from hydra.data.budget import read_ledger, request_id_for, sha256_file
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_usda_grain_information_shock_tripwire import (
    ACCESS_LEDGER,
    MANIFEST,
    RECEIPT,
    _read_manifest,
)


ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
SYMBOLS = ("ZC", "ZS", "ZW")


class USDAGrainTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Cell:
    mechanism: str
    signal_mode: str
    decision_bars: int
    minimum_response_to_prior_range: float
    holding_minutes: int
    stop_prior_range_fraction: float
    target_stop_multiple: float
    execution_symbol: str

    @property
    def candidate_id(self) -> str:
        return "usda_grain_" + stable_hash(asdict(self))[:20]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    result = path if path.is_absolute() else root / path
    result = result.resolve()
    if result != root and root not in result.parents:
        raise USDAGrainTripwireError("path escapes project root")
    if not result.is_file():
        raise USDAGrainTripwireError(f"required artifact missing: {result}")
    return result


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _read_manifest(project)
    receipt_path = project / RECEIPT
    if not receipt_path.is_file():
        raise USDAGrainTripwireError("governed acquisition receipt unavailable")
    receipt = _read_json(receipt_path)
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("manifest_hash") != manifest["manifest_hash"]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("q4_access_count_delta") != 0
        or receipt.get("broker_connections") != 0
        or receipt.get("orders") != 0
    ):
        raise USDAGrainTripwireError("acquisition receipt semantic drift")
    files: dict[str, dict[str, Any]] = {}
    for row in receipt["files"]:
        path = _inside(project, row["path"])
        if path.stat().st_size != int(row["size_bytes"]) or sha256_file(path) != row["sha256"]:
            raise USDAGrainTripwireError("raw artifact drift")
        files[str(row["kind"])] = {**row, "path": str(path)}
    if set(files) != {"ohlcv-1m", "definition"}:
        raise USDAGrainTripwireError("raw file inventory drift")
    ledger = read_ledger(project / "reports/data_budget/databento_spend_ledger.jsonl")
    bundle_id = str(receipt["bundle_id"])
    for schema in files:
        request_id = request_id_for({"bundle_id": bundle_id, "schema": schema})
        rows = [row for row in ledger if row.get("request_id") == request_id]
        if len(rows) != 1 or rows[0].get("download_status") != "DOWNLOADED":
            raise USDAGrainTripwireError("spend ledger does not reconcile")
    access = [
        json.loads(line)
        for line in (project / ACCESS_LEDGER).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for role in ROLES:
        marker = f"{bundle_id}:{role}"
        if sum(marker in set(row.get("candidate_ids") or ()) for row in access) != 1:
            raise USDAGrainTripwireError("data-role ledger does not reconcile")
    return {
        "manifest": manifest,
        "receipt": receipt,
        "files": files,
        "audit_hash": stable_hash(
            {
                "manifest_hash": manifest["manifest_hash"],
                "receipt_hash": receipt["receipt_hash"],
                "files": {key: value["sha256"] for key, value in sorted(files.items())},
            }
        ),
    }


def _load_bars(audit: Mapping[str, Any]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    store = _import_databento().DBNStore.from_file(audit["files"]["ohlcv-1m"]["path"])
    frame = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    frame = frame.rename(columns={"ts_event": "timestamp"})
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    wanted = [f"{symbol}.c.0" for symbol in SYMBOLS]
    frame = frame.loc[
        frame["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
        & frame["symbol"].isin(wanted)
    ].copy()
    if frame.duplicated(["symbol", "timestamp"]).any():
        raise USDAGrainTripwireError("duplicate grain minute bar")
    frame["local_timestamp"] = frame["timestamp"].dt.tz_convert("America/New_York")
    frame["local_date"] = frame["local_timestamp"].dt.date
    output: dict[str, pd.DataFrame] = {}
    for symbol in SYMBOLS:
        part = frame.loc[frame["symbol"].eq(f"{symbol}.c.0")].copy()
        if part.empty:
            raise USDAGrainTripwireError(f"missing {symbol}.c.0 bars")
        output[symbol] = part.sort_values("timestamp", kind="mergesort").set_index(
            "timestamp", drop=False
        )
    return output, {
        "row_count": len(frame),
        "rows_by_symbol": {symbol: len(output[symbol]) for symbol in SYMBOLS},
        "first_timestamp": str(frame["timestamp"].min()),
        "last_timestamp": str(frame["timestamp"].max()),
        "q4_2024_row_count": int(
            frame["timestamp"].ge(pd.Timestamp("2024-10-01", tz="UTC")).sum()
        ),
    }


def _release_timestamps(manifest: Mapping[str, Any]) -> list[pd.Timestamp]:
    output: list[pd.Timestamp] = []
    for value in manifest["release_contract"]["release_dates"]:
        local = pd.Timestamp(f"{value} {manifest['release_contract']['release_time']}")
        output.append(local.tz_localize(manifest["release_contract"]["timezone"]).tz_convert("UTC"))
    if len(output) != 80 or len(set(output)) != 80 or output != sorted(output):
        raise USDAGrainTripwireError("official release calendar drift")
    return output


def _role_for_day(day: date, manifest: Mapping[str, Any]) -> str | None:
    stamp = pd.Timestamp(day)
    for role in manifest["chronological_roles"]:
        if pd.Timestamp(role["start"]) <= stamp < pd.Timestamp(role["end"]):
            return str(role["role"])
    return None


def _roll_guard_days(frame: pd.DataFrame) -> set[date]:
    ordered = frame.sort_index(kind="mergesort")
    changed = ordered["instrument_id"].astype(str).ne(ordered["instrument_id"].astype(str).shift())
    boundaries = set(ordered.loc[changed, "local_date"].iloc[1:].tolist())
    guarded: set[date] = set()
    for day in boundaries:
        guarded.update({day - timedelta(days=1), day, day + timedelta(days=1)})
    return guarded


def _cells(manifest: Mapping[str, Any]) -> list[Cell]:
    lattice = manifest["candidate_lattice"]
    return [
        Cell(mechanism, mode, decision_bars, threshold, hold, stop, target, symbol)
        for mechanism in lattice["mechanisms"]
        for mode in lattice["signal_modes"]
        for decision_bars in lattice["decision_bars"]
        for threshold in lattice["minimum_response_to_prior_range"]
        for hold in lattice["holding_minutes"]
        for stop in lattice["stop_prior_range_fraction"]
        for target in lattice["target_stop_multiple"]
        for symbol in lattice["execution_symbols"]
    ]


def _response(frame: pd.DataFrame, release: pd.Timestamp, decision_bars: int) -> dict[str, float] | None:
    pre = frame.loc[release - pd.Timedelta(minutes=30) : release - pd.Timedelta(nanoseconds=1)]
    expected = [release + pd.Timedelta(minutes=index) for index in range(decision_bars)]
    if len(pre) < 20 or any(timestamp not in frame.index for timestamp in expected):
        return None
    prior_range = float(pre["high"].max() - pre["low"].min())
    if not math.isfinite(prior_range) or prior_range < 0.5:
        return None
    response = frame.loc[expected]
    open_price = float(response.iloc[0]["open"])
    close_price = float(response.iloc[-1]["close"])
    return {
        "prior_range": prior_range,
        "impulse": close_price - open_price,
        "upward": float(response["high"].max()) - open_price,
        "downward": open_price - float(response["low"].min()),
        "close": close_price,
        "open": open_price,
        "high": float(response["high"].max()),
        "low": float(response["low"].min()),
    }


def _one_market_direction(response: Mapping[str, float], mechanism: str) -> tuple[int, float] | None:
    prior_range = float(response["prior_range"])
    if mechanism == "RESPONSE_CONTINUATION":
        impulse = float(response["impulse"])
        if impulse == 0.0:
            return None
        return (1 if impulse > 0.0 else -1), abs(impulse) / prior_range
    if mechanism == "REJECTION_REVERSAL":
        upward = float(response["upward"])
        downward = float(response["downward"])
        if max(upward, downward) <= 0.0:
            return None
        if upward >= downward:
            rejection = (float(response["high"]) - float(response["close"])) / max(upward, 1e-12)
            direction = -1
            dominant = upward
        else:
            rejection = (float(response["close"]) - float(response["low"])) / max(downward, 1e-12)
            direction = 1
            dominant = downward
        if rejection < 0.5:
            return None
        return direction, dominant / prior_range
    raise USDAGrainTripwireError(f"unknown mechanism: {mechanism}")


def _direction_and_score(
    bars: Mapping[str, pd.DataFrame], release: pd.Timestamp, cell: Cell
) -> tuple[int, float, float] | None:
    responses = {
        symbol: _response(bars[symbol], release, cell.decision_bars) for symbol in SYMBOLS
    }
    execution_response = responses[cell.execution_symbol]
    if execution_response is None:
        return None
    if cell.signal_mode == "OWN_MARKET":
        resolved = _one_market_direction(execution_response, cell.mechanism)
        if resolved is None:
            return None
        direction, score = resolved
    elif cell.signal_mode == "CROSS_GRAIN_BREADTH":
        votes = [
            _one_market_direction(response, cell.mechanism)
            for response in responses.values()
            if response is not None
        ]
        if len(votes) != len(SYMBOLS):
            return None
        signs = [vote[0] for vote in votes if vote is not None]
        direction = 1 if sum(signs) >= 2 else -1
        agreeing = [vote[1] for vote in votes if vote is not None and vote[0] == direction]
        if len(agreeing) < 2:
            return None
        score = float(np.median(agreeing))
    else:
        raise USDAGrainTripwireError(f"unknown signal mode: {cell.signal_mode}")
    if score < cell.minimum_response_to_prior_range:
        return None
    return direction, score, float(execution_response["prior_range"])


def _first_row_at_or_after(
    frame: pd.DataFrame, timestamp: pd.Timestamp, *, limit_minutes: int
) -> pd.Series | None:
    rows = frame.loc[timestamp : timestamp + pd.Timedelta(minutes=limit_minutes)]
    return None if rows.empty else rows.iloc[0]


def _trade_event(
    bars: Mapping[str, pd.DataFrame],
    release: pd.Timestamp,
    cell: Cell,
    manifest: Mapping[str, Any],
    *,
    direction_flip: bool = False,
) -> dict[str, Any] | None:
    resolved = _direction_and_score(bars, release, cell)
    if resolved is None:
        return None
    direction, score, prior_range = resolved
    if direction_flip:
        direction *= -1
    decision_bar = release + pd.Timedelta(minutes=cell.decision_bars - 1)
    decision_time = decision_bar + pd.Timedelta(minutes=1)
    execution = bars[cell.execution_symbol]
    entry_row = _first_row_at_or_after(execution, decision_time, limit_minutes=3)
    if entry_row is None:
        return None
    entry_time = pd.Timestamp(entry_row["timestamp"])
    entry_price = float(entry_row["open"])
    spec = manifest["execution"]["instruments"][cell.execution_symbol]
    tick_size = float(spec["tick_size"])
    stop_ticks = max(2, int(round(prior_range * cell.stop_prior_range_fraction / tick_size)))
    stop_distance = stop_ticks * tick_size
    target_ticks = max(1, int(round(stop_ticks * cell.target_stop_multiple)))
    target_distance = target_ticks * tick_size
    stop_price = entry_price - direction * stop_distance
    target_price = entry_price + direction * target_distance
    deadline = entry_time + pd.Timedelta(minutes=cell.holding_minutes)
    path = execution.loc[entry_time : deadline + pd.Timedelta(minutes=3)]
    if path.empty:
        return None
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
        return None
    point_value = float(spec["point_value_usd"])
    gross = direction * (exit_price - entry_price) * point_value
    commission = float(spec["round_turn_commission_and_fees_usd"])
    normal_cost = commission + 2.0 * float(
        manifest["execution"]["normal_slippage_ticks_per_side"]
    ) * float(spec["tick_value_usd"])
    stressed_cost = commission + 2.0 * float(
        manifest["execution"]["stressed_slippage_ticks_per_side"]
    ) * float(spec["tick_value_usd"])
    local_day = release.tz_convert("America/New_York").date()
    role = _role_for_day(local_day, manifest)
    if role is None:
        return None
    return {
        "candidate_id": cell.candidate_id,
        "release_timestamp": release.isoformat(),
        "release_day": local_day.isoformat(),
        "role": role,
        "mechanism": cell.mechanism,
        "signal_mode": cell.signal_mode,
        "execution_symbol": cell.execution_symbol,
        "direction": direction,
        "response_score": score,
        "prior_range": prior_range,
        "decision_time": decision_time.isoformat(),
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
        "event_hash": stable_hash(
            {
                "cell": asdict(cell),
                "release": release.isoformat(),
                "direction": direction,
                "decision_time": decision_time.isoformat(),
                "entry_time": entry_time.isoformat(),
                "exit_time": exit_time.isoformat(),
                "entry_price": entry_price,
                "exit_price": exit_price,
            }
        ),
    }


def _summary(events: Sequence[Mapping[str, Any]], role: str) -> dict[str, Any]:
    rows = [row for row in events if row["role"] == role]
    normal = [float(row["normal_net_usd"]) for row in rows]
    stressed = [float(row["stressed_net_usd"]) for row in rows]
    gross = [float(row["gross_pnl_usd"]) for row in rows]
    costs = [float(row["stressed_cost_usd"]) for row in rows]
    positive = [value for value in stressed if value > 0.0]
    return {
        "role": role,
        "event_count": len(rows),
        "gross_pnl_usd": float(sum(gross)),
        "normal_net_usd": float(sum(normal)),
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
    releases: Sequence[pd.Timestamp],
    cell: Cell,
    manifest: Mapping[str, Any],
    *,
    direction_flip: bool = False,
    shift_days: int = 0,
    roll_guard: set[date],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for original in releases:
        timestamp = original + pd.Timedelta(days=shift_days)
        local_day = timestamp.tz_convert("America/New_York").date()
        if local_day in roll_guard:
            continue
        event = _trade_event(
            bars, timestamp, cell, manifest, direction_flip=direction_flip
        )
        if event is not None:
            if shift_days:
                original_day = original.tz_convert("America/New_York").date()
                event["role"] = _role_for_day(original_day, manifest)
            events.append(event)
    return events, {
        "candidate_id": cell.candidate_id,
        "cell": asdict(cell),
        "roles": {role: _summary(events, role) for role in ROLES},
        "event_count": len(events),
        "event_hash": stable_hash([row["event_hash"] for row in events]),
    }


def _selection_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    summary = row["roles"]["DISCOVERY"]
    return (
        float(summary["stressed_net_usd"]),
        float(summary["lower_quartile_stressed_event_usd"] or -math.inf),
        float(summary["positive_stressed_event_rate"] or 0.0),
        -int(row["cell"]["holding_minutes"]),
        row["candidate_id"],
    )


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


def _upper_bound(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_event: dict[tuple[str, str, str], float] = {}
    for row in rows:
        for event in row.get("events", []):
            key = (
                str(event["role"]),
                str(event["release_day"]),
                str(event["execution_symbol"]),
            )
            by_event[key] = max(by_event.get(key, -math.inf), float(event["stressed_net_usd"]))
    output: dict[str, Any] = {}
    for role in ROLES:
        values = [value for (item_role, _day, _symbol), value in by_event.items() if item_role == role]
        output[role] = {
            "market_event_count": len(values),
            "non_deployable_hindsight_stressed_net_usd": float(sum(values)),
            "positive_event_rate": float(sum(value > 0.0 for value in values) / len(values)) if values else None,
            "interpretation": "NON_DEPLOYABLE_UPPER_BOUND_NEVER_PROMOTION_ELIGIBLE",
        }
    return output


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    bars, data_receipt = _load_bars(audit)
    releases = _release_timestamps(manifest)
    guard: set[date] = set()
    for symbol in SYMBOLS:
        guard |= _roll_guard_days(bars[symbol])
    role_counts = {
        role: sum(
            _role_for_day(timestamp.tz_convert("America/New_York").date(), manifest) == role
            and timestamp.tz_convert("America/New_York").date() not in guard
            for timestamp in releases
        )
        for role in ROLES
    }
    lattice = _cells(manifest)
    evaluated: list[dict[str, Any]] = []
    all_events_for_upper: list[dict[str, Any]] = []
    for cell in lattice:
        events, result = _evaluate_cell(bars, releases, cell, manifest, roll_guard=guard)
        evaluated.append(result)
        all_events_for_upper.append({"candidate_id": cell.candidate_id, "events": events})

    gate = manifest["selection_gate"]
    discovery_eligible = [
        row
        for row in evaluated
        if int(row["roles"]["DISCOVERY"]["event_count"]) >= int(gate["minimum_discovery_events"])
        and float(row["roles"]["DISCOVERY"]["stressed_net_usd"]) > 0.0
        and float(row["roles"]["DISCOVERY"]["stressed_edge_to_cost_ratio"] or -math.inf)
        >= float(gate["minimum_stressed_edge_to_cost_ratio"])
    ]
    discovery_eligible.sort(key=_selection_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    niches: set[tuple[Any, ...]] = set()
    for row in discovery_eligible:
        cell = row["cell"]
        niche = (
            cell["mechanism"],
            cell["signal_mode"],
            cell["decision_bars"],
            cell["execution_symbol"],
            cell["holding_minutes"],
        )
        if niche in niches:
            continue
        niches.add(niche)
        selected.append(row)
        if len(selected) >= int(gate["maximum_selected_cells"]):
            break

    selected_results: list[dict[str, Any]] = []
    passers: list[str] = []
    positive_both_without_full_gate: list[str] = []
    cell_lookup = {cell.candidate_id: cell for cell in lattice}
    for primary in selected:
        cell = cell_lookup[primary["candidate_id"]]
        _primary_events, primary_full = _evaluate_cell(
            bars, releases, cell, manifest, roll_guard=guard
        )
        _flip_events, flip = _evaluate_cell(
            bars, releases, cell, manifest, direction_flip=True, roll_guard=guard
        )
        _timing_events, timing = _evaluate_cell(
            bars, releases, cell, manifest, shift_days=7, roll_guard=guard
        )
        role_gate = {
            role: _passes_role(
                primary_full["roles"][role],
                minimum=int(gate[f"minimum_{role.lower()}_events"]),
                gate=gate,
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        }
        positive_both = all(
            float(primary_full["roles"][role]["stressed_net_usd"]) > 0.0
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        if positive_both:
            positive_both_without_full_gate.append(cell.candidate_id)
        control_gate = all(
            float(primary_full["roles"][role]["stressed_net_usd"])
            > max(
                float(flip["roles"][role]["stressed_net_usd"]),
                float(timing["roles"][role]["stressed_net_usd"]),
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        passed = all(role_gate.values()) and control_gate
        if passed:
            passers.append(cell.candidate_id)
        selected_results.append(
            {
                "candidate_id": cell.candidate_id,
                "cell": asdict(cell),
                "primary": primary_full,
                "controls": {
                    "direction_flip": flip,
                    "non_release_week_matched": timing,
                },
                "role_gate": role_gate,
                "control_gate": control_gate,
                "event_gate_passed": passed,
            }
        )

    if passers:
        status = "USDA_GRAIN_INFORMATION_SHOCK_EVENT_ALPHA_GREEN"
        next_action = "OPEN_EXACT_ACCOUNT_SIZE_MATRIX_FOR_FROZEN_PASSERS"
    elif positive_both_without_full_gate:
        status = "USDA_GRAIN_INFORMATION_SHOCK_WEAK"
        next_action = "PRESERVE_DIAGNOSTICS_CLOSE_EXACT_SPEC_AND_START_DISTINCT_VOLATILITY_EVENT_POLICY"
    else:
        status = "USDA_GRAIN_INFORMATION_SHOCK_FALSIFIED"
        next_action = "TOMBSTONE_EXACT_SPEC_AND_START_DISTINCT_VOLATILITY_EVENT_POLICY"

    result: dict[str, Any] = {
        "schema": "hydra_usda_grain_information_shock_tripwire_economic_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "evidence_role": manifest["governance"]["evidence_role"],
        "manifest_hash": manifest["manifest_hash"],
        "acquisition_receipt_hash": audit["receipt"]["receipt_hash"],
        "audit_hash": audit["audit_hash"],
        "actual_incremental_spend_usd": audit["receipt"]["actual_incremental_spend_usd"],
        "data_receipt": data_receipt,
        "scheduled_release_count": len(releases),
        "usable_release_count_by_role": role_counts,
        "roll_guard_day_count": len(guard),
        "proposal_count": len(lattice),
        "discovery_eligible_count": len(discovery_eligible),
        "selected_candidate_ids": [row["candidate_id"] for row in selected],
        "selected_results": selected_results,
        "event_gate_passer_ids": passers,
        "positive_both_heldout_without_full_gate_ids": positive_both_without_full_gate,
        "non_deployable_upper_bound": _upper_bound(all_events_for_upper),
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
    core = dict(result)
    result["result_hash"] = stable_hash(core)
    return result


__all__ = ["Cell", "USDAGrainTripwireError", "audit_inputs", "run_tripwire"]
