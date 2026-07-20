"""Causal MBT/MET cross-flow response residual tripwire.

The branch is intentionally small: 24 executable specifications are selected
on 2022 only and then replayed unchanged on 2023.  Account replay and the
conditional 2024 tranche remain closed unless the event-economics gate passes.
"""

from __future__ import annotations

import gc
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
from scripts.acquire_cme_cross_crypto_flow_response_residual import (
    ACCESS_LEDGER,
    MANIFEST,
    RECEIPT,
)


ROLES = ("DISCOVERY", "VALIDATION")
SYMBOLS = ("MBT", "MET")
DEGRADED_UTC_DAYS = {date(2022, 1, 2)}
CHICAGO = "America/Chicago"


class CrossCryptoTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Candidate:
    target: str
    mechanism: str
    event_window_trade_count: int
    holding_minutes: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_manifest(root: Path) -> dict[str, Any]:
    manifest = _read_json(root / MANIFEST)
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise CrossCryptoTripwireError("frozen manifest hash drift")
    return manifest


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    result = path if path.is_absolute() else root / path
    result = result.resolve()
    if result != root and root not in result.parents:
        raise CrossCryptoTripwireError("path escapes project root")
    if not result.is_file():
        raise CrossCryptoTripwireError(f"required artifact missing: {result}")
    return result


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _read_manifest(project)
    receipt = _read_json(project / RECEIPT)
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("manifest_hash") != manifest["manifest_hash"]
        or receipt.get("download_status") != "DOWNLOADED"
        or receipt.get("tranche") != "A"
        or receipt.get("q4_access_count_delta") != 0
        or receipt.get("broker_connections") != 0
        or receipt.get("orders") != 0
    ):
        raise CrossCryptoTripwireError("acquisition receipt semantic drift")
    files: dict[str, dict[str, Any]] = {}
    for row in receipt["files"]:
        path = _inside(project, row["path"])
        if path.stat().st_size != int(row["size_bytes"]) or sha256_file(path) != row["sha256"]:
            raise CrossCryptoTripwireError("raw artifact drift")
        files[str(row["kind"])] = {**row, "path": str(path)}
    if set(files) != {"tbbo", "definition"}:
        raise CrossCryptoTripwireError("raw schema inventory drift")
    ledger = read_ledger(project / "reports/data_budget/databento_spend_ledger.jsonl")
    for schema in files:
        request_id = request_id_for({"bundle_id": receipt["bundle_id"], "schema": schema})
        matches = [row for row in ledger if row.get("request_id") == request_id]
        if len(matches) != 1 or matches[0].get("download_status") != "DOWNLOADED":
            raise CrossCryptoTripwireError("spend ledger does not reconcile")
    access = [
        json.loads(line)
        for line in (project / ACCESS_LEDGER).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for role in ROLES:
        marker = f"{receipt['bundle_id']}:{role}"
        if sum(marker in set(row.get("candidate_ids") or ()) for row in access) != 1:
            raise CrossCryptoTripwireError("data-role ledger does not reconcile")
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


def _session_day(timestamp: pd.Series) -> pd.Series:
    local = timestamp.dt.tz_convert(CHICAGO)
    normalized = local.dt.normalize()
    return normalized + pd.to_timedelta(local.dt.hour.ge(17).astype(int), unit="D")


def _roll_guard_days(frame: pd.DataFrame) -> set[pd.Timestamp]:
    changed = frame["instrument_id"].astype(str).ne(frame["instrument_id"].astype(str).shift())
    boundaries = set(frame.loc[changed, "session_day"].iloc[1:].tolist())
    guarded: set[pd.Timestamp] = set()
    for day in boundaries:
        stamp = pd.Timestamp(day)
        guarded.update(
            {stamp - pd.Timedelta(days=1), stamp, stamp + pd.Timedelta(days=1)}
        )
    return guarded


def _load_events(audit: Mapping[str, Any]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    store = _import_databento().DBNStore.from_file(audit["files"]["tbbo"]["path"])
    raw = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    columns = [
        "ts_recv",
        "ts_event",
        "publisher_id",
        "instrument_id",
        "side",
        "price",
        "size",
        "sequence",
        "bid_px_00",
        "ask_px_00",
        "bid_sz_00",
        "ask_sz_00",
        "symbol",
    ]
    raw = raw[columns].copy()
    raw["ts_recv"] = pd.to_datetime(raw["ts_recv"], utc=True)
    raw["ts_event"] = pd.to_datetime(raw["ts_event"], utc=True)
    source_record_count = len(raw)
    clock_delta_us = (raw["ts_recv"] - raw["ts_event"]).dt.total_seconds() * 1e6
    negative_clock_delta_count = int(clock_delta_us.lt(0.0).sum())
    duplicate_record_count = int(raw.duplicated(keep="first").sum())
    raw = raw.drop_duplicates(keep="first").reset_index(drop=True)
    raw = raw.loc[raw["symbol"].isin(["MBT.c.0", "MET.c.0"])].copy()
    raw["utc_day"] = raw["ts_event"].dt.date
    degraded_count = int(raw["utc_day"].isin(DEGRADED_UTC_DAYS).sum())
    raw = raw.loc[~raw["utc_day"].isin(DEGRADED_UTC_DAYS)].copy()
    raw["session_day"] = _session_day(raw["ts_recv"])
    local = raw["ts_recv"].dt.tz_convert(CHICAGO)
    local_minutes = local.dt.hour * 60 + local.dt.minute
    allowed_session = local_minutes.ge(17 * 60) | local_minutes.lt(15 * 60 + 10)
    valid_quote = (
        raw["bid_px_00"].gt(0.0)
        & raw["ask_px_00"].gt(0.0)
        & raw["ask_px_00"].ge(raw["bid_px_00"])
        & raw["bid_sz_00"].gt(0)
        & raw["ask_sz_00"].gt(0)
    )
    invalid_quote_count = int((~valid_quote).sum())
    raw = raw.loc[allowed_session & valid_quote].copy()
    output: dict[str, pd.DataFrame] = {}
    roll_diagnostics: dict[str, Any] = {}
    for root in SYMBOLS:
        part = raw.loc[raw["symbol"].eq(f"{root}.c.0")].copy()
        part = part.sort_values(["ts_recv", "sequence"], kind="mergesort").reset_index(drop=True)
        guards = _roll_guard_days(part)
        guarded_count = int(part["session_day"].isin(guards).sum())
        part = part.loc[~part["session_day"].isin(guards)].reset_index(drop=True)
        part["mid"] = (part["bid_px_00"] + part["ask_px_00"]) / 2.0
        part["spread"] = part["ask_px_00"] - part["bid_px_00"]
        part["side_sign"] = part["side"].map({"B": 1.0, "A": -1.0}).fillna(0.0)
        part["signed_size"] = part["side_sign"] * part["size"].astype(float)
        output[root] = part
        roll_diagnostics[root] = {
            "retained_event_count": len(part),
            "roll_guard_session_count": len(guards),
            "roll_guard_event_count": guarded_count,
            "first_ts_recv": part["ts_recv"].min().isoformat(),
            "last_ts_recv": part["ts_recv"].max().isoformat(),
        }
    del raw
    gc.collect()
    return output, {
        "source_record_count": source_record_count,
        "degraded_utc_days_excluded": sorted(day.isoformat() for day in DEGRADED_UTC_DAYS),
        "degraded_record_count": degraded_count,
        "invalid_quote_record_count": invalid_quote_count,
        "exact_duplicate_record_count_deduplicated": duplicate_record_count,
        "negative_exchange_to_receive_clock_delta_count": negative_clock_delta_count,
        "minimum_exchange_to_receive_clock_delta_us": float(clock_delta_us.min()),
        "availability_ordering": "TS_RECV_CAPTURE_ORDER; NEGATIVE_CLOCK_DELTAS_REPORTED_NOT_BACKFILLED",
        "symbols": roll_diagnostics,
    }


def _load_contract_spec(audit: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    store = _import_databento().DBNStore.from_file(audit["files"]["definition"]["path"])
    frame = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    output: dict[str, dict[str, float]] = {}
    for root in SYMBOLS:
        part = frame.loc[frame["symbol"].eq(f"{root}.c.0")]
        tick = sorted(set(float(value) for value in part["min_price_increment"] if value > 0))
        quantity = sorted(set(float(value) for value in part["unit_of_measure_qty"] if value > 0))
        if len(tick) != 1 or len(quantity) != 1:
            raise CrossCryptoTripwireError(f"ambiguous contract definition: {root}")
        output[root] = {
            "tick_size": tick[0],
            "point_value_usd": quantity[0],
            "tick_value_usd": tick[0] * quantity[0],
        }
    return output


def _role(timestamp: pd.Series) -> pd.Series:
    output = pd.Series(pd.NA, index=timestamp.index, dtype="object")
    output.loc[timestamp.ge("2022-01-01") & timestamp.lt("2023-01-01")] = "DISCOVERY"
    output.loc[timestamp.ge("2023-01-01") & timestamp.lt("2024-01-01")] = "VALIDATION"
    return output


def _features(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    signed = frame["signed_size"].astype(float)
    volume = frame["size"].astype(float)
    rolling_signed = signed.rolling(window, min_periods=window).sum()
    rolling_volume = volume.rolling(window, min_periods=window).sum()
    imbalance = rolling_signed / rolling_volume.replace(0.0, np.nan)
    center = imbalance.rolling(1000, min_periods=200).median().shift(1)
    deviation = (imbalance - center).abs()
    mad = deviation.rolling(1000, min_periods=200).median().shift(1)
    flow_z = (imbalance - center) / (1.4826 * mad).replace(0.0, np.nan)
    prior_high = frame["mid"].rolling(window, min_periods=window).max().shift(1)
    prior_low = frame["mid"].rolling(window, min_periods=window).min().shift(1)
    prior_range = prior_high - prior_low
    response = frame["mid"] - frame["mid"].shift(window)
    return pd.DataFrame(
        {
            "ts_recv": frame["ts_recv"],
            "session_day": frame["session_day"],
            "instrument_id": frame["instrument_id"],
            "bid": frame["bid_px_00"].astype(float),
            "ask": frame["ask_px_00"].astype(float),
            "bid_size": frame["bid_sz_00"].astype(float),
            "ask_size": frame["ask_sz_00"].astype(float),
            "mid": frame["mid"].astype(float),
            "spread": frame["spread"].astype(float),
            "flow_z": flow_z.astype(float),
            "response": response.astype(float),
            "prior_range": prior_range.astype(float),
            "role": _role(frame["ts_recv"]),
        }
    )


def _paired_features(
    events: Mapping[str, pd.DataFrame], target: str, window: int
) -> pd.DataFrame:
    other = "MET" if target == "MBT" else "MBT"
    own = _features(events[target], window).dropna(subset=["flow_z", "prior_range"]).copy()
    cross = _features(events[other], window)[["ts_recv", "flow_z", "response"]].copy()
    cross = cross.rename(columns={"flow_z": "cross_flow_z", "response": "cross_response"})
    paired = pd.merge_asof(
        own.sort_values("ts_recv"),
        cross.dropna().sort_values("ts_recv"),
        on="ts_recv",
        direction="backward",
        tolerance=pd.Timedelta(minutes=2),
    )
    paired = paired.dropna(subset=["cross_flow_z", "cross_response"]).reset_index(drop=True)
    paired["target"] = target
    return paired


def _candidates(manifest: Mapping[str, Any]) -> list[Candidate]:
    lattice = manifest["candidate_lattice"]
    output = [
        Candidate(target, mechanism, window, hold)
        for target in lattice["targets"]
        for mechanism in lattice["mechanisms"]
        for window in lattice["event_windows_trade_count"]
        for hold in lattice["holding_minutes"]
    ]
    if len(output) != int(lattice["proposal_count"]):
        raise CrossCryptoTripwireError("candidate lattice count drift")
    return output


def _candidate_id(candidate: Candidate, manifest: Mapping[str, Any]) -> str:
    return "cross_crypto_" + stable_hash(
        {
            "candidate": asdict(candidate),
            "manifest_hash": manifest["manifest_hash"],
            "causal_contract": manifest["causal_contract"],
            "execution": manifest["execution"],
        }
    )[:20]


def _thresholds(frame: pd.DataFrame, manifest: Mapping[str, Any]) -> dict[str, float]:
    discovery = frame.loc[frame["role"].eq("DISCOVERY")].copy()
    quantile = float(manifest["candidate_lattice"]["discovery_flow_shock_quantile"])
    efficiency = discovery["response"].abs() / discovery["flow_z"].abs().clip(lower=0.1)
    return {
        "own_abs_flow_z_q90": float(discovery["flow_z"].abs().quantile(quantile)),
        "cross_abs_flow_z_q90": float(discovery["cross_flow_z"].abs().quantile(quantile)),
        "own_abs_flow_z_median": float(discovery["flow_z"].abs().median()),
        "abs_response_q40": float(discovery["response"].abs().quantile(0.4)),
        "response_efficiency_q25": float(efficiency.quantile(0.25)),
    }


def _signal_arrays(
    frame: pd.DataFrame,
    candidate: Candidate,
    thresholds: Mapping[str, float],
    *,
    control: str = "PRIMARY",
) -> tuple[np.ndarray, np.ndarray]:
    own = frame["flow_z"].to_numpy(float)
    cross = frame["cross_flow_z"].to_numpy(float)
    response = frame["response"].to_numpy(float)
    own_shock = np.abs(own) >= float(thresholds["own_abs_flow_z_q90"])
    cross_shock = np.abs(cross) >= float(thresholds["cross_abs_flow_z_q90"])
    if candidate.mechanism == "CROSS_FLOW_CONFIRMATION_CONTINUATION":
        mask = own_shock & cross_shock & (np.sign(own) == np.sign(cross))
        direction = np.sign(own)
    elif candidate.mechanism == "CROSS_CRYPTO_RESPONSE_LAG_CATCHUP":
        mask = (
            cross_shock
            & (np.abs(own) <= float(thresholds["own_abs_flow_z_median"]))
            & (np.abs(response) <= float(thresholds["abs_response_q40"]))
        )
        direction = np.sign(cross)
    elif candidate.mechanism == "OWN_FLOW_REJECTION_REVERSAL":
        efficiency = np.abs(response) / np.clip(np.abs(own), 0.1, None)
        mask = own_shock & (efficiency <= float(thresholds["response_efficiency_q25"]))
        direction = -np.sign(own)
    else:
        raise CrossCryptoTripwireError(f"unknown mechanism: {candidate.mechanism}")
    if control == "DIRECTION_FLIP":
        direction = -direction
    elif control == "OWN_FLOW_ONLY":
        mask = own_shock
        direction = np.sign(own)
    elif control == "MARKET_PERMUTATION":
        mask = cross_shock
        direction = np.sign(cross)
    elif control != "PRIMARY":
        raise CrossCryptoTripwireError(f"unknown control: {control}")
    transition = mask & ~np.r_[False, mask[:-1]]
    indices = np.flatnonzero(transition & np.isfinite(direction) & (direction != 0))
    return indices.astype(np.int64), direction[indices].astype(np.int8)


def _session_flatten_utc(session_day: pd.Timestamp) -> pd.Timestamp:
    day = pd.Timestamp(session_day).date()
    return pd.Timestamp(f"{day.isoformat()} 15:10").tz_localize(CHICAGO).tz_convert("UTC")


def _simulate_signals(
    frame: pd.DataFrame,
    candidate: Candidate,
    indices: np.ndarray,
    directions: np.ndarray,
    contract: Mapping[str, float],
    manifest: Mapping[str, Any],
    *,
    control: str,
) -> list[dict[str, Any]]:
    candidate_id = _candidate_id(candidate, manifest)
    ts = frame["ts_recv"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
    bid = frame["bid"].to_numpy(float)
    ask = frame["ask"].to_numpy(float)
    bid_size = frame["bid_size"].to_numpy(float)
    ask_size = frame["ask_size"].to_numpy(float)
    prior_range = frame["prior_range"].to_numpy(float)
    roles = frame["role"].astype(object).to_numpy()
    sessions = frame["session_day"].to_numpy()
    tick = float(contract["tick_size"])
    point_value = float(contract["point_value_usd"])
    fee = float(manifest["execution"]["normal_round_turn_fees_usd"][candidate.target])
    stress_cost = fee + 2.0 * tick * point_value
    minimum_gap_ns = int(
        float(manifest["candidate_lattice"]["minimum_episode_separation_seconds"]) * 1e9
    )
    hold_ns = int(candidate.holding_minutes * 60 * 1e9)
    next_allowed_ns = -1
    output: list[dict[str, Any]] = []
    for signal_index, direction in zip(indices.tolist(), directions.tolist(), strict=True):
        if roles[signal_index] not in ROLES or ts[signal_index] < next_allowed_ns:
            continue
        entry_index = int(np.searchsorted(ts, ts[signal_index], side="right"))
        if entry_index >= len(frame):
            censored = {
                "candidate_id": candidate_id,
                "role": roles[signal_index],
                "outcome_state": "DATA_CENSORED",
                "censor_reason": "MISSING_STRICTLY_LATER_ENTRY",
                "signal_time": pd.Timestamp(ts[signal_index], unit="ns", tz="UTC").isoformat(),
            }
            output.append({**censored, "event_hash": stable_hash(censored)})
            continue
        if sessions[entry_index] != sessions[signal_index]:
            censored = {
                "candidate_id": candidate_id,
                "role": roles[signal_index],
                "outcome_state": "DATA_CENSORED",
                "censor_reason": "SESSION_CLOSED_BEFORE_ENTRY",
                "signal_time": pd.Timestamp(ts[signal_index], unit="ns", tz="UTC").isoformat(),
            }
            output.append({**censored, "event_hash": stable_hash(censored)})
            continue
        entry_price = ask[entry_index] if direction > 0 else bid[entry_index]
        displayed = ask_size[entry_index] if direction > 0 else bid_size[entry_index]
        if not math.isfinite(entry_price) or displayed < 1.0:
            continue
        risk = max(
            float(manifest["candidate_lattice"]["minimum_stop_ticks"]) * tick,
            float(prior_range[signal_index]),
        ) * float(manifest["candidate_lattice"]["stop_risk_units"])
        stop = entry_price - direction * risk
        target = entry_price + direction * risk * float(
            manifest["candidate_lattice"]["target_stop_multiple"]
        )
        deadline_ns = min(
            ts[entry_index] + hold_ns,
            _session_flatten_utc(pd.Timestamp(sessions[signal_index])).value,
        )
        final_index = int(np.searchsorted(ts, deadline_ns, side="left"))
        final_index = min(final_index, len(frame) - 1)
        exit_index: int | None = None
        exit_price: float | None = None
        reason = ""
        minimum_open = 0.0
        for path_index in range(entry_index, final_index + 1):
            if sessions[path_index] != sessions[signal_index]:
                break
            executable = bid[path_index] if direction > 0 else ask[path_index]
            pnl = direction * (executable - entry_price) * point_value
            minimum_open = min(minimum_open, pnl)
            stop_hit = executable <= stop if direction > 0 else executable >= stop
            target_hit = executable >= target if direction > 0 else executable <= target
            if stop_hit:
                exit_index, exit_price, reason = path_index, executable, "STOP_FIRST"
                break
            if target_hit:
                exit_index, exit_price, reason = path_index, target, "TARGET"
                break
            if ts[path_index] >= deadline_ns:
                exit_index, exit_price, reason = path_index, executable, "TIME_OR_SESSION_EXIT"
                break
        if exit_index is None or exit_price is None:
            censored = {
                "candidate_id": candidate_id,
                "role": roles[signal_index],
                "outcome_state": "DATA_CENSORED",
                "censor_reason": "MISSING_EXIT_COVERAGE",
                "signal_time": pd.Timestamp(ts[signal_index], unit="ns", tz="UTC").isoformat(),
            }
            output.append({**censored, "event_hash": stable_hash(censored)})
            continue
        gross = direction * (exit_price - entry_price) * point_value
        normal = gross - fee
        stressed = gross - stress_cost
        session_label = pd.Timestamp(sessions[signal_index]).date().isoformat()
        core = {
            "candidate_id": candidate_id,
            "role": roles[signal_index],
            "outcome_state": "FULL_COVERAGE",
            "target": candidate.target,
            "mechanism": candidate.mechanism,
            "event_window_trade_count": candidate.event_window_trade_count,
            "holding_minutes": candidate.holding_minutes,
            "control": control,
            "session_day": session_label,
            "instrument_id": str(frame.iloc[entry_index]["instrument_id"]),
            "direction": int(direction),
            "signal_time": pd.Timestamp(ts[signal_index], unit="ns", tz="UTC").isoformat(),
            "decision_time": pd.Timestamp(ts[signal_index], unit="ns", tz="UTC").isoformat(),
            "entry_time": pd.Timestamp(ts[entry_index], unit="ns", tz="UTC").isoformat(),
            "entry_price": float(entry_price),
            "stop_price": float(stop),
            "target_price": float(target),
            "exit_time": pd.Timestamp(ts[exit_index], unit="ns", tz="UTC").isoformat(),
            "exit_price": float(exit_price),
            "exit_reason": reason,
            "gross_pnl_usd": float(gross),
            "normal_cost_usd": fee,
            "stressed_cost_usd": stress_cost,
            "normal_net_usd": float(normal),
            "stressed_net_usd": float(stressed),
            "minimum_open_pnl_stressed_usd": float(minimum_open - stress_cost / 2.0),
            "fill_policy_id": "TBBO_EXECUTABLE_TOP_OF_BOOK_NEXT_TARGET_EVENT_V1",
        }
        output.append({**core, "event_hash": stable_hash(core)})
        next_allowed_ns = ts[exit_index] + minimum_gap_ns
    return output


def _summary(events: Sequence[Mapping[str, Any]], role: str) -> dict[str, Any]:
    role_rows = [row for row in events if row["role"] == role]
    rows = [row for row in role_rows if row["outcome_state"] == "FULL_COVERAGE"]
    gross = [float(row["gross_pnl_usd"]) for row in rows]
    normal = [float(row["normal_net_usd"]) for row in rows]
    stressed = [float(row["stressed_net_usd"]) for row in rows]
    costs = [float(row["stressed_cost_usd"]) for row in rows]
    positives = [value for value in stressed if value > 0.0]
    daily: dict[str, float] = {}
    for row in rows:
        daily[str(row["session_day"])] = daily.get(str(row["session_day"]), 0.0) + float(
            row["stressed_net_usd"]
        )
    positive_days = [value for value in daily.values() if value > 0.0]
    return {
        "role": role,
        "signal_count": len(role_rows),
        "event_count": len(rows),
        "data_censored_count": len(role_rows) - len(rows),
        "gross_pnl_usd": float(sum(gross)),
        "normal_net_usd": float(sum(normal)),
        "stressed_net_usd": float(sum(stressed)),
        "stressed_net_per_event_usd": float(np.mean(stressed)) if stressed else None,
        "median_stressed_event_usd": float(np.median(stressed)) if stressed else None,
        "lower_quartile_stressed_event_usd": float(np.quantile(stressed, 0.25)) if stressed else None,
        "positive_stressed_event_rate": float(sum(value > 0 for value in stressed) / len(stressed)) if stressed else None,
        "stressed_edge_to_cost_ratio": float(sum(gross) / sum(costs)) if sum(costs) > 0 else None,
        "maximum_single_trade_positive_profit_share": float(max(positives) / sum(positives)) if positives and sum(positives) > 0 else None,
        "maximum_positive_day_profit_share": float(max(positive_days) / sum(positive_days)) if positive_days and sum(positive_days) > 0 else None,
        "minimum_open_pnl_stressed_usd": min(
            (float(row["minimum_open_pnl_stressed_usd"]) for row in rows), default=None
        ),
        "target_count": sum(row["exit_reason"] == "TARGET" for row in rows),
        "stop_count": sum(row["exit_reason"] == "STOP_FIRST" for row in rows),
        "session_count": len(daily),
        "event_path_hash": stable_hash([row.get("event_hash") for row in role_rows]),
    }


def _evaluate(
    frame: pd.DataFrame,
    candidate: Candidate,
    thresholds: Mapping[str, float],
    contract: Mapping[str, float],
    manifest: Mapping[str, Any],
    *,
    control: str = "PRIMARY",
    time_shift: bool = False,
) -> dict[str, Any]:
    indices, directions = _signal_arrays(frame, candidate, thresholds, control=control)
    if time_shift and len(indices):
        shifted_ns = (
            frame.loc[indices, "ts_recv"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
            + int(24 * 3600 * 1e9)
        )
        all_ns = frame["ts_recv"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
        indices = np.searchsorted(all_ns, shifted_ns, side="left")
        valid = indices < len(frame)
        indices = indices[valid]
        directions = directions[valid]
        control = "SESSION_TIME_SHIFT"
    events = _simulate_signals(
        frame,
        candidate,
        indices,
        directions,
        contract,
        manifest,
        control=control,
    )
    return {
        "candidate_id": _candidate_id(candidate, manifest),
        "candidate": asdict(candidate),
        "control": control,
        "thresholds_frozen_from_discovery": dict(thresholds),
        "roles": {role: _summary(events, role) for role in ROLES},
        "event_hash": stable_hash([row.get("event_hash") for row in events]),
    }


def _rank(result: Mapping[str, Any]) -> tuple[Any, ...]:
    discovery = result["roles"]["DISCOVERY"]
    return (
        float(discovery["stressed_net_usd"]),
        float(discovery["stressed_net_per_event_usd"] or -math.inf),
        int(discovery["event_count"]),
        result["candidate_id"],
    )


def _gate_role(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> bool:
    gate = manifest["selection_gate"]
    trade_concentration = summary["maximum_single_trade_positive_profit_share"]
    day_concentration = summary["maximum_positive_day_profit_share"]
    return bool(
        int(summary["event_count"]) >= int(gate["minimum_validation_independent_episodes"])
        and float(summary["stressed_net_usd"]) > float(
            gate["validation_stressed_net_usd_minimum_exclusive"]
        )
        and float(summary["stressed_edge_to_cost_ratio"] or -math.inf)
        >= float(gate["minimum_validation_stressed_edge_to_cost_ratio"])
        and trade_concentration is not None
        and day_concentration is not None
        and float(trade_concentration)
        <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
        and float(day_concentration)
        <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
    )


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    events, data_receipt = _load_events(audit)
    contracts = _load_contract_spec(audit)
    paired: dict[tuple[str, int], pd.DataFrame] = {}
    thresholds: dict[tuple[str, int], dict[str, float]] = {}
    for target in SYMBOLS:
        for window in manifest["candidate_lattice"]["event_windows_trade_count"]:
            key = (target, int(window))
            paired[key] = _paired_features(events, target, int(window))
            thresholds[key] = _thresholds(paired[key], manifest)
    del events
    gc.collect()
    lattice = _candidates(manifest)
    evaluated: list[dict[str, Any]] = []
    for candidate in lattice:
        key = (candidate.target, candidate.event_window_trade_count)
        evaluated.append(
            _evaluate(
                paired[key],
                candidate,
                thresholds[key],
                contracts[candidate.target],
                manifest,
            )
        )
    gate = manifest["selection_gate"]
    eligible = [
        row
        for row in evaluated
        if int(row["roles"]["DISCOVERY"]["event_count"])
        >= int(gate["minimum_discovery_independent_episodes"])
        and float(row["roles"]["DISCOVERY"]["stressed_net_usd"]) > 0.0
        and float(row["roles"]["DISCOVERY"]["stressed_edge_to_cost_ratio"] or -math.inf)
        >= float(gate["minimum_validation_stressed_edge_to_cost_ratio"])
    ]
    eligible.sort(key=_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    niches: set[tuple[str, str]] = set()
    for row in eligible:
        niche = (row["candidate"]["target"], row["candidate"]["mechanism"])
        if niche in niches:
            continue
        niches.add(niche)
        selected.append(row)
        if len(selected) >= int(gate["maximum_selected_specs"]):
            break
    by_id = {_candidate_id(candidate, manifest): candidate for candidate in lattice}
    selected_results: list[dict[str, Any]] = []
    passers: list[str] = []
    for row in selected:
        candidate = by_id[row["candidate_id"]]
        key = (candidate.target, candidate.event_window_trade_count)
        controls = {
            "direction_flip": _evaluate(
                paired[key], candidate, thresholds[key], contracts[candidate.target], manifest,
                control="DIRECTION_FLIP",
            ),
            "session_time_shift": _evaluate(
                paired[key], candidate, thresholds[key], contracts[candidate.target], manifest,
                time_shift=True,
            ),
            "own_flow_only": _evaluate(
                paired[key], candidate, thresholds[key], contracts[candidate.target], manifest,
                control="OWN_FLOW_ONLY",
            ),
        }
        other_target = "MET" if candidate.target == "MBT" else "MBT"
        permuted = Candidate(
            target=other_target,
            mechanism=candidate.mechanism,
            event_window_trade_count=candidate.event_window_trade_count,
            holding_minutes=candidate.holding_minutes,
        )
        permuted_key = (other_target, candidate.event_window_trade_count)
        controls["market_permutation"] = _evaluate(
            paired[permuted_key],
            permuted,
            thresholds[permuted_key],
            contracts[other_target],
            manifest,
        )
        validation = row["roles"]["VALIDATION"]
        primary_mean = float(validation["stressed_net_per_event_usd"] or -math.inf)
        controls_beaten = all(
            primary_mean
            > float(control["roles"]["VALIDATION"]["stressed_net_per_event_usd"] or -math.inf)
            for control in controls.values()
        )
        passed = _gate_role(validation, manifest) and controls_beaten
        if passed:
            passers.append(row["candidate_id"])
        selected_results.append(
            {
                "candidate_id": row["candidate_id"],
                "primary": row,
                "controls": controls,
                "controls_beaten": controls_beaten,
                "validation_event_gate_passed": passed,
            }
        )
    if passers:
        status = "CROSS_CRYPTO_FLOW_RESPONSE_VALIDATION_GREEN"
        next_action = "FREEZE_PASSERS_AND_ACQUIRE_CONDITIONAL_2024_TRANCHE_B"
    else:
        status = "CROSS_CRYPTO_FLOW_RESPONSE_FALSIFIED"
        next_action = "STOP_TRANCHE_B_PRESERVE_BUDGET_AND_PIVOT_TO_DISTINCT_REPRESENTATION"
    result: dict[str, Any] = {
        "schema": "hydra_cme_cross_crypto_flow_response_residual_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "manifest_hash": manifest["manifest_hash"],
        "source_audit_hash": audit["audit_hash"],
        "acquisition_receipt_hash": audit["receipt"]["receipt_hash"],
        "actual_incremental_spend_usd": audit["receipt"]["actual_incremental_spend_usd"],
        "remaining_budget_usd": audit["receipt"]["remaining_after_usd"],
        "data_receipt": data_receipt,
        "contract_specs": contracts,
        "proposal_count": len(lattice),
        "discovery_eligible_count": len(eligible),
        "all_candidate_results": evaluated,
        "best_diagnostics_by_discovery": sorted(evaluated, key=_rank, reverse=True)[:6],
        "selected_candidate_ids": [row["candidate_id"] for row in selected],
        "selected_results": selected_results,
        "validation_event_gate_passer_ids": passers,
        "tranche_b_acquired": False,
        "account_replay_executed": False,
        "tier_ceiling": "Q_PENDING_FINAL_DEVELOPMENT" if passers else "H_DIAGNOSTIC",
        "runtime_seconds": time.perf_counter() - started,
        "next_action": next_action,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
    }
    result["result_hash"] = stable_hash(
        {key: value for key, value in result.items() if key != "runtime_seconds"}
    )
    return result


__all__ = [
    "Candidate",
    "CrossCryptoTripwireError",
    "audit_inputs",
    "run_tripwire",
]
