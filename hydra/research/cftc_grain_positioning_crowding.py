"""Causal CFTC positioning/crowding tripwire for ZC, ZS, and ZW.

The report-date fields are never treated as publication times.  Each Tuesday
snapshot becomes usable only on the following Wednesday RTH (eight calendar
days later), a deliberately conservative rule frozen before acquisition.
"""

from __future__ import annotations

import gc
import json
import math
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from scripts.acquire_cftc_grain_positioning_crowding import MANIFEST, RECEIPT


CHICAGO = "America/Chicago"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
MARKETS = ("ZC", "ZS", "ZW")


class CFTCGrainTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Candidate:
    market: str
    mechanism: str
    extreme_percentile: float
    target_stop_multiple: float


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest(root: Path) -> dict[str, Any]:
    payload = _read_json(root / MANIFEST)
    core = dict(payload)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise CFTCGrainTripwireError("frozen manifest hash drift")
    return payload


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _manifest(project)
    receipt = _read_json(project / RECEIPT)
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("manifest_hash") != manifest["manifest_hash"]
        or receipt.get("download_status") != "DOWNLOADED"
        or float(receipt.get("actual_cost_usd", math.nan)) != 0.0
        or int(receipt.get("q4_access_count_delta", -1)) != 0
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
    ):
        raise CFTCGrainTripwireError("CFTC receipt semantic drift")
    cot_path = Path(receipt["output_path"])
    if not cot_path.is_absolute():
        cot_path = project / cot_path
    cot_path = cot_path.resolve()
    if project not in cot_path.parents:
        raise CFTCGrainTripwireError("CFTC input escapes project root")
    if not cot_path.is_file() or sha256_file(cot_path) != receipt["raw_sha256"]:
        raise CFTCGrainTripwireError("raw CFTC artifact drift")
    price = manifest["frozen_price_input"]
    price_files: dict[str, Path] = {}
    for key in ("ohlcv", "definition"):
        path = (project / price[f"{key}_path"]).resolve()
        if project not in path.parents or not path.is_file():
            raise CFTCGrainTripwireError(f"missing {key} artifact")
        if sha256_file(path) != price[f"{key}_sha256"]:
            raise CFTCGrainTripwireError(f"frozen {key} artifact drift")
        price_files[key] = path
    return {
        "manifest": manifest,
        "receipt": receipt,
        "cot_path": cot_path,
        "price_files": price_files,
        "audit_hash": stable_hash(
            {
                "manifest_hash": manifest["manifest_hash"],
                "receipt_hash": receipt["receipt_hash"],
                "cot_sha256": receipt["raw_sha256"],
                "ohlcv_sha256": price["ohlcv_sha256"],
                "definition_sha256": price["definition_sha256"],
            }
        ),
    }


def _role(timestamp: pd.Series) -> pd.Series:
    output = pd.Series(pd.NA, index=timestamp.index, dtype="object")
    output.loc[timestamp.ge("2018-01-02") & timestamp.lt("2022-01-01")] = "DISCOVERY"
    output.loc[timestamp.ge("2022-01-01") & timestamp.lt("2023-01-01")] = "VALIDATION"
    output.loc[timestamp.ge("2023-01-01") & timestamp.lt("2024-10-01")] = "FINAL_DEVELOPMENT"
    return output


def _rolling_percentile(values: pd.Series, window: int, minimum: int) -> pd.Series:
    raw = values.to_numpy(float)
    output = np.full(len(raw), np.nan, dtype=float)
    for index, value in enumerate(raw):
        start = max(0, index - window)
        history = raw[start:index]
        history = history[np.isfinite(history)]
        if len(history) >= minimum and math.isfinite(value):
            output[index] = float((history <= value).sum() / len(history))
    return pd.Series(output, index=values.index)


def _load_cot(audit: Mapping[str, Any]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    manifest = audit["manifest"]
    data = manifest["cftc_data_contract"]
    frame = pd.read_csv(
        audit["cot_path"],
        dtype={
            "id": "string",
            "cftc_contract_market_code": "string",
            "futonly_or_combined": "string",
        },
    )
    if len(frame) != int(data["expected_total_rows"]):
        raise CFTCGrainTripwireError("CFTC row count drift")
    frame["report_date"] = pd.to_datetime(frame["report_date_as_yyyy_mm_dd"], utc=True)
    excluded = manifest["publication_causality"]["excluded_disrupted_report_dates"]
    excluded_mask = frame["report_date"].between(
        pd.Timestamp(excluded["start"], tz="UTC"),
        pd.Timestamp(excluded["end_inclusive"], tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1),
    )
    excluded_count = int(excluded_mask.sum())
    frame = frame.loc[~excluded_mask].copy()
    numeric = [
        "open_interest_all",
        "prod_merc_positions_long",
        "prod_merc_positions_short",
        "swap_positions_long_all",
        "swap__positions_short_all",
        "m_money_positions_long_all",
        "m_money_positions_short_all",
        "m_money_positions_spread",
    ]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
    code_to_market = {value: key for key, value in data["contract_market_codes"].items()}
    frame["market"] = frame["cftc_contract_market_code"].astype(str).map(code_to_market)
    if frame["market"].isna().any():
        raise CFTCGrainTripwireError("unexpected CFTC market code")
    output: dict[str, pd.DataFrame] = {}
    diagnostics: dict[str, Any] = {}
    history = int(manifest["candidate_lattice"]["rolling_position_history_reports"])
    minimum = int(manifest["candidate_lattice"]["minimum_position_history_reports"])
    for market in MARKETS:
        part = frame.loc[frame["market"].eq(market)].sort_values("report_date").reset_index(drop=True)
        oi = part["open_interest_all"].replace(0.0, np.nan)
        part["managed_net_ratio"] = (
            part["m_money_positions_long_all"] - part["m_money_positions_short_all"]
        ) / oi
        part["producer_net_ratio"] = (
            part["prod_merc_positions_long"] - part["prod_merc_positions_short"]
        ) / oi
        part["swap_net_ratio"] = (
            part["swap_positions_long_all"] - part["swap__positions_short_all"]
        ) / oi
        part["managed_change"] = part["managed_net_ratio"].diff()
        part["managed_percentile"] = _rolling_percentile(part["managed_net_ratio"], history, minimum)
        part["managed_change_abs_percentile"] = _rolling_percentile(part["managed_change"].abs(), history, minimum)
        part["managed_producer_divergence"] = part["managed_net_ratio"] - part["producer_net_ratio"]
        part["divergence_percentile"] = _rolling_percentile(
            part["managed_producer_divergence"], history, minimum
        )
        # The source field is a calendar date, not an instant.  Do not convert
        # midnight UTC into the prior Chicago date before applying the frozen
        # eight-calendar-day publication lag.
        local_date = part["report_date"].dt.date + pd.to_timedelta(8, unit="D")
        part["available_at"] = pd.to_datetime(local_date.astype(str) + " 08:30").dt.tz_localize(CHICAGO).dt.tz_convert("UTC")
        if (part["available_at"] <= part["report_date"]).any():
            raise CFTCGrainTripwireError("CFTC availability is not delayed")
        output[market] = part
        diagnostics[market] = {
            "retained_report_count": len(part),
            "first_report_date": part["report_date"].min().isoformat(),
            "last_report_date": part["report_date"].max().isoformat(),
            "first_feature_ready_at": part.loc[part["managed_percentile"].notna(), "available_at"].min().isoformat(),
        }
    return output, {
        "source_row_count": int(data["expected_total_rows"]),
        "shutdown_backlog_rows_excluded": excluded_count,
        "availability_rule": manifest["publication_causality"]["conservative_actionable_time"],
        "markets": diagnostics,
    }


def _load_contracts(audit: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    store = _import_databento().DBNStore.from_file(str(audit["price_files"]["definition"]))
    frame = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    output: dict[str, dict[str, float]] = {}
    for market in MARKETS:
        part = frame.loc[frame["symbol"].eq(f"{market}.c.0")]
        ticks = sorted(set(float(value) for value in part["min_price_increment"] if value > 0))
        quantities = sorted(set(float(value) for value in part["unit_of_measure_qty"] if value > 0))
        units = sorted(set(str(value) for value in part["unit_of_measure"] if str(value)))
        if len(ticks) != 1 or quantities != [5000.0] or units != ["BU"]:
            raise CFTCGrainTripwireError(f"ambiguous grain contract definition: {market}")
        point_value = quantities[0] / 100.0
        output[market] = {
            "tick_size": ticks[0],
            "point_value_usd": point_value,
            "tick_value_usd": ticks[0] * point_value,
        }
    return output


def _session_day(timestamp: pd.Series) -> pd.Series:
    local = timestamp.dt.tz_convert(CHICAGO)
    normalized = local.dt.normalize()
    return normalized + pd.to_timedelta(local.dt.hour.ge(19).astype(int), unit="D")


def _roll_guard_days(frame: pd.DataFrame) -> set[pd.Timestamp]:
    changed = frame["instrument_id"].astype(str).ne(frame["instrument_id"].astype(str).shift())
    boundaries = set(frame.loc[changed, "session_day"].iloc[1:].tolist())
    guarded: set[pd.Timestamp] = set()
    for boundary in boundaries:
        stamp = pd.Timestamp(boundary)
        guarded.update({stamp - pd.Timedelta(days=1), stamp, stamp + pd.Timedelta(days=1)})
    return guarded


def _load_price_sessions(
    audit: Mapping[str, Any]
) -> tuple[dict[str, dict[pd.Timestamp, pd.DataFrame]], dict[str, Any]]:
    store = _import_databento().DBNStore.from_file(str(audit["price_files"]["ohlcv"]))
    raw = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    raw = raw[["ts_event", "instrument_id", "open", "high", "low", "close", "volume", "symbol"]].copy()
    raw["ts_event"] = pd.to_datetime(raw["ts_event"], utc=True)
    raw = raw.loc[raw["symbol"].isin([f"{market}.c.0" for market in MARKETS])].copy()
    raw["session_day"] = _session_day(raw["ts_event"])
    local = raw["ts_event"].dt.tz_convert(CHICAGO)
    minute = local.dt.hour * 60 + local.dt.minute
    rth = minute.between(8 * 60 + 30, 13 * 60 + 19)
    raw = raw.loc[rth].copy()
    output: dict[str, dict[pd.Timestamp, pd.DataFrame]] = {}
    diagnostics: dict[str, Any] = {}
    for market in MARKETS:
        part = raw.loc[raw["symbol"].eq(f"{market}.c.0")].sort_values("ts_event").copy()
        guards = _roll_guard_days(part)
        guarded_rows = int(part["session_day"].isin(guards).sum())
        part = part.loc[~part["session_day"].isin(guards)].copy()
        instrument_counts = part.groupby("session_day")["instrument_id"].nunique()
        mixed_sessions = instrument_counts.loc[instrument_counts.ne(1)].index.tolist()
        if mixed_sessions:
            raise CFTCGrainTripwireError(
                f"mixed instrument ids remain inside {market} sessions after roll guard: "
                f"{[pd.Timestamp(day).isoformat() for day in mixed_sessions[:5]]}"
            )
        sessions = {
            pd.Timestamp(day): group.reset_index(drop=True)
            for day, group in part.groupby("session_day", sort=True)
        }
        output[market] = sessions
        diagnostics[market] = {
            "session_count": len(sessions),
            "roll_guard_session_count": len(guards),
            "roll_guard_row_count": guarded_rows,
            "mixed_instrument_session_count": 0,
            "rth_bar_count": len(part),
        }
    del raw
    gc.collect()
    return output, diagnostics


def _candidates(manifest: Mapping[str, Any]) -> list[Candidate]:
    lattice = manifest["candidate_lattice"]
    output = [
        Candidate(market, mechanism, float(percentile), float(target_multiple))
        for market in lattice["markets"]
        for mechanism in lattice["mechanisms"]
        for percentile in lattice["extreme_percentiles"]
        for target_multiple in lattice["target_stop_multiples"]
    ]
    if len(output) != int(lattice["proposal_count"]):
        raise CFTCGrainTripwireError("candidate lattice drift")
    return output


def _candidate_id(candidate: Candidate, manifest: Mapping[str, Any]) -> str:
    return "cftc_grain_" + stable_hash(
        {
            "candidate": asdict(candidate),
            "manifest_hash": manifest["manifest_hash"],
            "execution": manifest["execution"],
            "publication_causality": manifest["publication_causality"],
        }
    )[:20]


def _cot_direction(row: Mapping[str, Any], candidate: Candidate) -> int:
    q = candidate.extreme_percentile
    low = 1.0 - q
    managed = float(row["managed_percentile"])
    change = float(row["managed_change"])
    if candidate.mechanism == "CROWDING_CONTINUATION":
        return 1 if managed >= q else (-1 if managed <= low else 0)
    if candidate.mechanism == "CROWDING_UNWIND":
        if managed >= q and change < 0:
            return -1
        if managed <= low and change > 0:
            return 1
        return 0
    if candidate.mechanism == "PRODUCER_MANAGED_DIVERGENCE":
        divergence = float(row["divergence_percentile"])
        return -1 if divergence >= q else (1 if divergence <= low else 0)
    if candidate.mechanism == "POSITION_CHANGE_MOMENTUM":
        strength = float(row["managed_change_abs_percentile"])
        return int(np.sign(change)) if strength >= q and change != 0 else 0
    raise CFTCGrainTripwireError(f"unknown mechanism: {candidate.mechanism}")


def _session_context(session: pd.DataFrame) -> dict[str, Any] | None:
    local = session["ts_event"].dt.tz_convert(CHICAGO)
    minutes = local.dt.hour * 60 + local.dt.minute
    opening = session.loc[minutes.between(8 * 60 + 30, 8 * 60 + 44)]
    required_opening_minutes = set(range(8 * 60 + 30, 8 * 60 + 45))
    observed_opening_minutes = set(int(value) for value in minutes.loc[opening.index])
    decision_local = (
        pd.Timestamp(local.iloc[0].date()).tz_localize(CHICAGO)
        + pd.Timedelta(hours=8, minutes=45)
    )
    decision_time = decision_local.tz_convert("UTC")
    entries = session.loc[session["ts_event"].gt(decision_time)]
    if (
        observed_opening_minutes != required_opening_minutes
        or entries.empty
        or (13 * 60 + 19) not in set(int(value) for value in minutes)
        or session["instrument_id"].astype(str).nunique() != 1
    ):
        return None
    first = opening.iloc[0]
    last = opening.iloc[-1]
    entry = entries.iloc[0]
    if pd.Timestamp(entry["ts_event"]) <= decision_time:
        return None
    return {
        "decision_time": decision_time,
        "entry_index": int(entry.name),
        "entry_time": pd.Timestamp(entry["ts_event"]),
        "entry_price": float(entry["open"]),
        "opening_displacement": float(last["close"] - first["open"]),
        "opening_range": float(opening["high"].max() - opening["low"].min()),
        "instrument_id": str(entry["instrument_id"]),
    }


def _simulate_session(
    session: pd.DataFrame,
    context: Mapping[str, Any],
    direction: int,
    candidate: Candidate,
    contract: Mapping[str, float],
    manifest: Mapping[str, Any],
    *,
    control: str,
) -> dict[str, Any]:
    tick = float(contract["tick_size"])
    point_value = float(contract["point_value_usd"])
    entry_index = int(context["entry_index"])
    risk = max(float(context["opening_range"]), 4.0 * tick)

    def replay_path(extra_slippage_ticks_per_side: int) -> dict[str, Any]:
        slippage = float(extra_slippage_ticks_per_side) * tick
        entry_price = float(context["entry_price"]) + direction * slippage
        stop = entry_price - direction * risk
        target = entry_price + direction * risk * candidate.target_stop_multiple
        exit_price: float | None = None
        exit_index: int | None = None
        exit_reason = ""
        minimum_open = 0.0
        for index in range(entry_index, len(session)):
            row = session.iloc[index]
            low, high = float(row["low"]), float(row["high"])
            stop_hit = low <= stop if direction > 0 else high >= stop
            target_hit = high >= target if direction > 0 else low <= target
            if stop_hit:
                open_price = float(row["open"])
                raw_exit = min(open_price, stop) if direction > 0 else max(open_price, stop)
                exit_price = raw_exit - direction * slippage
                exit_index, exit_reason = index, "STOP_FIRST"
                minimum_open = min(
                    minimum_open,
                    direction * (exit_price - entry_price) * point_value,
                )
                break
            mark = low if direction > 0 else high
            minimum_open = min(
                minimum_open,
                direction * (mark - entry_price) * point_value,
            )
            if target_hit:
                exit_price = target - direction * slippage
                exit_index, exit_reason = index, "TARGET"
                break
        if exit_index is None:
            exit_index = len(session) - 1
            exit_price = float(session.iloc[exit_index]["close"]) - direction * slippage
            exit_reason = "SESSION_FLATTEN"
            minimum_open = min(
                minimum_open,
                direction * (exit_price - entry_price) * point_value,
            )
        return {
            "entry_price": entry_price,
            "stop_price": stop,
            "target_price": target,
            "exit_price": float(exit_price),
            "exit_index": int(exit_index),
            "exit_reason": exit_reason,
            "gross_pnl_usd": direction * (float(exit_price) - entry_price) * point_value,
            "minimum_open_pnl_usd": minimum_open,
        }

    normal_path = replay_path(
        int(manifest["execution"]["normal_extra_slippage_ticks_per_side"])
    )
    stressed_ticks = int(manifest["execution"]["stressed_extra_slippage_ticks_per_side"])
    stressed_path = replay_path(stressed_ticks)
    fee = float(manifest["execution"]["normal_round_turn_fees_usd"][candidate.market])
    nominal_stress_cost = fee + 2.0 * stressed_ticks * tick * point_value
    core = {
        "candidate_id": _candidate_id(candidate, manifest),
        "market": candidate.market,
        "mechanism": candidate.mechanism,
        "extreme_percentile": candidate.extreme_percentile,
        "target_stop_multiple": candidate.target_stop_multiple,
        "control": control,
        "role": str(context["role"]),
        "session_day": pd.Timestamp(context["session_day"]).date().isoformat(),
        "cot_report_date": pd.Timestamp(context["cot_report_date"]).isoformat(),
        "cot_available_at": pd.Timestamp(context["cot_available_at"]).isoformat(),
        "decision_time": pd.Timestamp(context["decision_time"]).isoformat(),
        "entry_time": pd.Timestamp(context["entry_time"]).isoformat(),
        "instrument_id": str(context["instrument_id"]),
        "direction": int(direction),
        "entry_price": float(normal_path["entry_price"]),
        "stop_price": float(normal_path["stop_price"]),
        "target_price": float(normal_path["target_price"]),
        "exit_time": (
            pd.Timestamp(session.iloc[int(normal_path["exit_index"])]["ts_event"])
            + pd.Timedelta(minutes=1)
        ).isoformat(),
        "exit_price": float(normal_path["exit_price"]),
        "exit_reason": str(normal_path["exit_reason"]),
        "gross_pnl_usd": float(normal_path["gross_pnl_usd"]),
        "stressed_entry_price": float(stressed_path["entry_price"]),
        "stressed_stop_price": float(stressed_path["stop_price"]),
        "stressed_target_price": float(stressed_path["target_price"]),
        "stressed_exit_time": (
            pd.Timestamp(session.iloc[int(stressed_path["exit_index"])]["ts_event"])
            + pd.Timedelta(minutes=1)
        ).isoformat(),
        "stressed_exit_price": float(stressed_path["exit_price"]),
        "stressed_exit_reason": str(stressed_path["exit_reason"]),
        "stressed_gross_pnl_usd": float(stressed_path["gross_pnl_usd"]),
        "normal_cost_usd": fee,
        "stressed_cost_usd": nominal_stress_cost,
        "normal_net_usd": float(normal_path["gross_pnl_usd"] - fee),
        "stressed_net_usd": float(stressed_path["gross_pnl_usd"] - fee),
        "minimum_open_pnl_stressed_usd": float(
            stressed_path["minimum_open_pnl_usd"] - fee / 2.0
        ),
        "fill_policy_id": manifest["execution"]["fill_model"],
        "stressed_fill_semantics": "ADVERSE_FILL_EACH_SIDE_WITH_RECOMPUTED_STOP_TARGET_GEOMETRY",
    }
    return {**core, "event_hash": stable_hash(core)}


def _contexts(
    sessions: Mapping[pd.Timestamp, pd.DataFrame], cot: pd.DataFrame
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    available_ns = cot["available_at"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
    for session_day, session in sessions.items():
        context = _session_context(session)
        if context is None:
            continue
        decision_ns = pd.Timestamp(context["decision_time"]).value
        index = int(np.searchsorted(available_ns, decision_ns, side="right") - 1)
        if index < 0:
            continue
        cot_row = cot.iloc[index]
        role = _role(pd.Series([pd.Timestamp(session_day).tz_convert("UTC")])).iloc[0]
        if role not in ROLES:
            continue
        output.append(
            {
                **context,
                "session_day": session_day,
                "role": role,
                "cot_index": index,
                "cot_report_date": cot_row["report_date"],
                "cot_available_at": cot_row["available_at"],
            }
        )
    return output


def _summary(rows: Sequence[Mapping[str, Any]], role: str) -> dict[str, Any]:
    selected = [row for row in rows if row["role"] == role]
    gross = [float(row["gross_pnl_usd"]) for row in selected]
    normal = [float(row["normal_net_usd"]) for row in selected]
    stressed = [float(row["stressed_net_usd"]) for row in selected]
    costs = [float(row["stressed_cost_usd"]) for row in selected]
    positives = [value for value in stressed if value > 0]
    daily = {str(row["session_day"]): float(row["stressed_net_usd"]) for row in selected}
    positive_days = [value for value in daily.values() if value > 0]
    return {
        "role": role,
        "event_count": len(selected),
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
        "minimum_open_pnl_stressed_usd": min((float(row["minimum_open_pnl_stressed_usd"]) for row in selected), default=None),
        "target_count": sum(row["exit_reason"] == "TARGET" for row in selected),
        "stop_count": sum(row["exit_reason"] == "STOP_FIRST" for row in selected),
        "event_path_hash": stable_hash([row["event_hash"] for row in selected]),
    }


def _evaluate(
    candidate: Candidate,
    sessions: Mapping[pd.Timestamp, pd.DataFrame],
    contexts: Sequence[Mapping[str, Any]],
    cot: pd.DataFrame,
    contract: Mapping[str, float],
    manifest: Mapping[str, Any],
    *,
    control: str = "PRIMARY",
    roles: Sequence[str] = ROLES[:2],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    ordered_days = sorted(sessions)
    next_day = {day: ordered_days[index + 1] for index, day in enumerate(ordered_days[:-1])}
    for base in contexts:
        if base["role"] not in roles:
            continue
        cot_index = int(base["cot_index"])
        direction_row = cot.iloc[cot_index]
        if control == "ONE_COT_PUBLICATION_LAG":
            if cot_index < 1:
                continue
            direction_row = cot.iloc[cot_index - 1]
        direction = _cot_direction(direction_row, candidate)
        if direction == 0:
            continue
        displacement = float(base["opening_displacement"])
        if control == "DIRECTION_FLIP":
            if int(np.sign(displacement)) != direction:
                continue
            direction = -direction
        elif control == "PRICE_ONLY_OPENING_DISPLACEMENT":
            direction = int(np.sign(displacement))
        elif control == "SESSION_TIME_SHIFT":
            shifted_day = next_day.get(pd.Timestamp(base["session_day"]))
            if shifted_day is None:
                continue
            shifted = _session_context(sessions[shifted_day])
            if shifted is None:
                continue
            shifted["role"] = _role(pd.Series([shifted_day.tz_convert("UTC")])).iloc[0]
            if shifted["role"] not in roles:
                continue
            shifted.update(
                {
                    "session_day": shifted_day,
                    "cot_report_date": base["cot_report_date"],
                    "cot_available_at": base["cot_available_at"],
                }
            )
            if int(np.sign(float(shifted["opening_displacement"]))) != direction:
                continue
            rows.append(
                _simulate_session(
                    sessions[shifted_day], shifted, direction, candidate, contract, manifest,
                    control=control,
                )
            )
            continue
        elif control != "PRIMARY" and control != "ONE_COT_PUBLICATION_LAG":
            raise CFTCGrainTripwireError(f"unknown control: {control}")
        if control != "DIRECTION_FLIP" and int(np.sign(displacement)) != direction:
            continue
        rows.append(
            _simulate_session(
                sessions[pd.Timestamp(base["session_day"])], base, direction,
                candidate, contract, manifest, control=control,
            )
        )
    return {
        "candidate_id": _candidate_id(candidate, manifest),
        "candidate": asdict(candidate),
        "control": control,
        "roles": {role: _summary(rows, role) for role in roles},
        "event_hash": stable_hash([row["event_hash"] for row in rows]),
    }


def _rank(result: Mapping[str, Any]) -> tuple[Any, ...]:
    row = result["roles"]["DISCOVERY"]
    mean = row["stressed_net_per_event_usd"]
    return (
        float(row["stressed_net_usd"]),
        float(-math.inf if mean is None else mean),
        int(row["event_count"]),
        result["candidate_id"],
    )


def _gate(summary: Mapping[str, Any], manifest: Mapping[str, Any], *, discovery: bool) -> bool:
    gate = manifest["selection_gate"]
    minimum = gate["minimum_discovery_events"] if discovery else gate["minimum_validation_events"]
    trade_share = summary["maximum_single_trade_positive_profit_share"]
    day_share = summary["maximum_positive_day_profit_share"]
    edge_to_cost = summary["stressed_edge_to_cost_ratio"]
    return bool(
        int(summary["event_count"]) >= int(minimum)
        and float(summary["stressed_net_usd"]) > 0.0
        and float(-math.inf if edge_to_cost is None else edge_to_cost)
        >= float(gate["minimum_validation_stressed_edge_to_cost_ratio"])
        and trade_share is not None
        and day_share is not None
        and float(trade_share) <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
        and float(day_share) <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
    )


def _controls_beaten(
    primary: Mapping[str, Any],
    controls: Mapping[str, Mapping[str, Any]],
    role: str,
    minimum_events: int,
) -> tuple[bool, bool]:
    """Return (beaten, resolved); an empty/undercovered control never passes."""
    primary_value = primary["stressed_net_per_event_usd"]
    if int(primary["event_count"]) < minimum_events or primary_value is None:
        return False, False
    control_summaries = [control["roles"][role] for control in controls.values()]
    resolved = all(
        int(summary["event_count"]) >= minimum_events
        and summary["stressed_net_per_event_usd"] is not None
        for summary in control_summaries
    )
    if not resolved:
        return False, False
    primary_mean = float(primary_value)
    return (
        all(
            primary_mean > float(summary["stressed_net_per_event_usd"])
            for summary in control_summaries
        ),
        True,
    )


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    cot, cot_receipt = _load_cot(audit)
    contracts = _load_contracts(audit)
    sessions, price_receipt = _load_price_sessions(audit)
    contexts = {market: _contexts(sessions[market], cot[market]) for market in MARKETS}
    candidates = _candidates(manifest)
    by_id = {_candidate_id(candidate, manifest): candidate for candidate in candidates}
    evaluated = [
        _evaluate(
            candidate,
            sessions[candidate.market],
            contexts[candidate.market],
            cot[candidate.market],
            contracts[candidate.market],
            manifest,
            roles=("DISCOVERY",),
        )
        for candidate in candidates
    ]
    eligible = [row for row in evaluated if _gate(row["roles"]["DISCOVERY"], manifest, discovery=True)]
    eligible.sort(key=_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    niches: set[tuple[str, str]] = set()
    for row in eligible:
        niche = (row["candidate"]["market"], row["candidate"]["mechanism"])
        if niche in niches:
            continue
        niches.add(niche)
        selected.append(row)
        if len(selected) >= int(manifest["selection_gate"]["maximum_selected_specs"]):
            break
    selected_results: list[dict[str, Any]] = []
    validation_passers: list[str] = []
    final_passers: list[str] = []
    for row in selected:
        candidate = by_id[row["candidate_id"]]
        validation_primary = _evaluate(
            candidate,
            sessions[candidate.market],
            contexts[candidate.market],
            cot[candidate.market],
            contracts[candidate.market],
            manifest,
            roles=("VALIDATION",),
        )
        controls = {
            name.lower(): _evaluate(
                candidate,
                sessions[candidate.market],
                contexts[candidate.market],
                cot[candidate.market],
                contracts[candidate.market],
                manifest,
                control=name,
                roles=("VALIDATION",),
            )
            for name in (
                "DIRECTION_FLIP",
                "ONE_COT_PUBLICATION_LAG",
                "PRICE_ONLY_OPENING_DISPLACEMENT",
                "SESSION_TIME_SHIFT",
            )
        }
        validation = validation_primary["roles"]["VALIDATION"]
        controls_beaten, controls_resolved = _controls_beaten(
            validation,
            controls,
            "VALIDATION",
            int(manifest["selection_gate"]["minimum_validation_events"]),
        )
        validation_passed = (
            _gate(validation, manifest, discovery=False)
            and controls_resolved
            and controls_beaten
        )
        final_result: dict[str, Any] | None = None
        final_controls: dict[str, Any] = {}
        final_passed = False
        if validation_passed:
            validation_passers.append(row["candidate_id"])
            final_result = _evaluate(
                candidate,
                sessions[candidate.market],
                contexts[candidate.market],
                cot[candidate.market],
                contracts[candidate.market],
                manifest,
                roles=("FINAL_DEVELOPMENT",),
            )
            final_controls = {
                name.lower(): _evaluate(
                    candidate,
                    sessions[candidate.market],
                    contexts[candidate.market],
                    cot[candidate.market],
                    contracts[candidate.market],
                    manifest,
                    control=name,
                    roles=("FINAL_DEVELOPMENT",),
                )
                for name in (
                    "DIRECTION_FLIP",
                    "ONE_COT_PUBLICATION_LAG",
                    "PRICE_ONLY_OPENING_DISPLACEMENT",
                    "SESSION_TIME_SHIFT",
                )
            }
            final_summary = final_result["roles"]["FINAL_DEVELOPMENT"]
            final_controls_beaten, final_controls_resolved = _controls_beaten(
                final_summary,
                final_controls,
                "FINAL_DEVELOPMENT",
                int(manifest["selection_gate"]["minimum_validation_events"]),
            )
            final_passed = (
                _gate(final_summary, manifest, discovery=False)
                and final_controls_resolved
                and final_controls_beaten
            )
            if final_passed:
                final_passers.append(row["candidate_id"])
        selected_results.append(
            {
                "candidate_id": row["candidate_id"],
                "primary_discovery": row,
                "primary_validation": validation_primary,
                "validation_controls": controls,
                "validation_controls_resolved": controls_resolved,
                "validation_controls_beaten": controls_beaten,
                "validation_gate_passed": validation_passed,
                "final_development": final_result,
                "final_controls": final_controls,
                "final_controls_resolved": (
                    final_controls_resolved if validation_passed else False
                ),
                "final_gate_passed": final_passed,
            }
        )
    if final_passers:
        status = "CFTC_GRAIN_POSITIONING_EVENT_GATE_GREEN"
        next_action = "FREEZE_PASSERS_AND_RUN_ACCOUNT_SIZE_MATRIX"
    else:
        status = "CFTC_GRAIN_POSITIONING_CROWDING_FALSIFIED"
        next_action = "PIVOT_TO_FROZEN_SOYBEAN_CRUSH_STRUCTURAL_VALUE_ROUTER_EVI"
    result: dict[str, Any] = {
        "schema": "hydra_cftc_grain_positioning_crowding_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "manifest_hash": manifest["manifest_hash"],
        "source_audit_hash": audit["audit_hash"],
        "cftc_receipt_hash": audit["receipt"]["receipt_hash"],
        "actual_incremental_spend_usd": 0.0,
        "cot_data_receipt": cot_receipt,
        "price_data_receipt": price_receipt,
        "contract_specs": contracts,
        "context_count_by_market": {market: len(contexts[market]) for market in MARKETS},
        "proposal_count": len(candidates),
        "discovery_eligible_count": len(eligible),
        "validation_candidate_count": len(selected),
        "final_development_candidate_count": len(validation_passers),
        "validation_access_policy": "DISCOVERY_SELECT_THEN_SELECTED_VALIDATION_ONLY",
        "all_candidate_results": evaluated,
        "best_diagnostics_by_discovery": sorted(evaluated, key=_rank, reverse=True)[:8],
        "selected_candidate_ids": [row["candidate_id"] for row in selected],
        "selected_results": selected_results,
        "validation_event_gate_passer_ids": validation_passers,
        "final_development_event_gate_passer_ids": final_passers,
        "account_replay_executed": False,
        "combine_pass_count": 0,
        "xfa_paths_started": 0,
        "tier_ceiling": "Q_PENDING_ACCOUNT_REPLAY" if final_passers else "H_DIAGNOSTIC",
        "runtime_seconds": time.perf_counter() - started,
        "next_action": next_action,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    result["result_hash"] = stable_hash(
        {key: value for key, value in result.items() if key != "runtime_seconds"}
    )
    return result


__all__ = [
    "Candidate",
    "CFTCGrainTripwireError",
    "audit_inputs",
    "run_tripwire",
]
