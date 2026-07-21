"""Causal Treasury-auction demand-shock tripwire.

This module intentionally implements only the immutable contract in
``treasury_auction_demand_shock_causal_v1.json``.  It is not a generic search
runner: there are no tunable thresholds and no model fitting on validation or
final-development outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    return _json_scalar(value)


def role_for(day: str, roles: Mapping[str, Sequence[str]]) -> str | None:
    for role, bounds in roles.items():
        if str(bounds[0]) <= day < str(bounds[1]):
            return role
    return None


def parse_available(value: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(str(value))
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(NY)
    return timestamp.tz_convert(UTC)


def load_manifest(root: Path, manifest_path: Path) -> dict[str, Any]:
    absolute = manifest_path if manifest_path.is_absolute() else root / manifest_path
    manifest = json.loads(absolute.read_text(encoding="utf-8"))
    claimed = manifest.get("manifest_hash")
    core = dict(manifest)
    core.pop("manifest_hash", None)
    actual = hashlib.sha256(canonical_json(core)).hexdigest()
    if actual != claimed:
        raise RuntimeError(f"manifest hash drift: expected={claimed} actual={actual}")

    for label in ("note", "bond"):
        item = manifest["frozen_official_inputs"]
        path = root / item[f"{label}_path"]
        actual = sha256_file(path)
        if actual != item[f"{label}_sha256"]:
            raise RuntimeError(f"official {label} input hash drift")

    predecessor_path = root / manifest["predecessor"]["manifest_path"]
    if sha256_file(predecessor_path) != manifest["predecessor"]["manifest_hash"]:
        # Manifest hashes are semantic hashes rather than file-byte hashes.
        predecessor = json.loads(predecessor_path.read_text(encoding="utf-8"))
        core = dict(predecessor)
        claimed_predecessor = core.pop("manifest_hash")
        if hashlib.sha256(canonical_json(core)).hexdigest() != claimed_predecessor:
            raise RuntimeError("predecessor manifest hash drift")

    predecessor = json.loads(predecessor_path.read_text(encoding="utf-8"))
    for item in predecessor["frozen_price_inputs"].values():
        path = root / item["path"]
        if sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"price input hash drift: {path}")
    rule = predecessor["official_rule_snapshot"]
    if sha256_file(root / rule["path"]) != rule["sha256"]:
        raise RuntimeError("official rule snapshot hash drift")
    return manifest


def load_predecessor(root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    path = root / str(manifest["predecessor"]["manifest_path"])
    predecessor = json.loads(path.read_text(encoding="utf-8"))
    if predecessor["manifest_hash"] != manifest["predecessor"]["manifest_hash"]:
        raise RuntimeError("predecessor manifest identity mismatch")
    return predecessor


def _number(record: Mapping[str, Any], field: str) -> float:
    try:
        value = float(str(record.get(field, "")).replace(",", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not math.isfinite(value):
        raise ValueError(f"non-finite {field}")
    return value


def robust_z(current: float, history: Sequence[float], clip: float = 3.0) -> float:
    """Frozen trailing-six robust z-score.

    The population standard deviation is deliberately included in the maximum
    scale, exactly as specified by the immutable manifest.
    """

    values = np.asarray(history, dtype=np.float64)
    if values.shape != (6,):
        raise ValueError("robust_z requires exactly six historical observations")
    center = float(np.median(values))
    mad = float(np.median(np.abs(values - center)))
    population_sd = float(np.std(values, ddof=0))
    scale = max(1.4826 * mad, population_sd, 1e-9)
    return float(np.clip((float(current) - center) / scale, -clip, clip))


def build_causal_events(
    records: Sequence[Mapping[str, Any]], manifest: Mapping[str, Any], predecessor: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Create event features from strictly earlier same-term auctions."""

    allowed = set(predecessor["official_event_sources"]["allowed_terms"])
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in records:
        term = str(raw.get("securityTerm", ""))
        day = str(raw.get("auctionDate", ""))[:10]
        cusip = str(raw.get("cusip", ""))
        if term not in allowed or not day or not cusip:
            continue
        try:
            available = parse_available(str(raw.get("updatedTimestamp", "")))
            total = _number(raw, "totalAccepted")
            if total <= 0:
                continue
            observation = {
                "cusip": cusip,
                "auction_date": day,
                "term": term,
                "market": predecessor["term_to_market"][term],
                "available_at": available,
                "bid_to_cover": _number(raw, "bidToCoverRatio"),
                "indirect_share": _number(raw, "indirectBidderAccepted") / total,
                "primary_dealer_share": _number(raw, "primaryDealerAccepted") / total,
            }
        except (KeyError, ValueError, TypeError):
            continue
        key = (cusip, day, term)
        prior = selected.get(key)
        if prior is None or observation["available_at"] > prior["available_at"]:
            selected[key] = observation

    chronological = sorted(
        selected.values(), key=lambda row: (row["available_at"], row["term"], row["cusip"])
    )
    history: dict[str, list[dict[str, Any]]] = {}
    events: list[dict[str, Any]] = []
    clip = float(manifest["feature_contract"]["individual_z_clip"])
    minimum = int(manifest["feature_contract"]["minimum_history"])
    threshold = float(manifest["policy"]["trade_threshold_absolute_score_inclusive"])
    tiers = sorted(
        manifest["policy"]["quantity_tiers"],
        key=lambda item: float(item["minimum_absolute_score"]),
    )
    for row in chronological:
        previous = history.setdefault(row["term"], [])
        if len(previous) >= minimum:
            trailing = previous[-minimum:]
            btc_z = robust_z(row["bid_to_cover"], [item["bid_to_cover"] for item in trailing], clip)
            indirect_z = robust_z(
                row["indirect_share"], [item["indirect_share"] for item in trailing], clip
            )
            dealer_z = robust_z(
                row["primary_dealer_share"],
                [item["primary_dealer_share"] for item in trailing],
                clip,
            )
            score = btc_z + indirect_z - dealer_z
            absolute_score = abs(score)
            action = "DEMAND_CONTINUATION" if absolute_score >= threshold else "ABSTAIN"
            quantity = 0
            if action != "ABSTAIN":
                for tier in tiers:
                    if absolute_score >= float(tier["minimum_absolute_score"]):
                        quantity = int(tier["contracts"])
            role = role_for(row["auction_date"], manifest["chronological_roles"])
            if role is not None:
                event_id = hashlib.sha256(
                    f"{row['cusip']}|{row['auction_date']}|{row['term']}|{row['available_at'].isoformat()}".encode()
                ).hexdigest()[:24]
                history_fingerprint = hashlib.sha256(
                    canonical_json(
                        [
                            {
                                key: _json_scalar(item[key])
                                for key in (
                                    "auction_date",
                                    "term",
                                    "available_at",
                                    "bid_to_cover",
                                    "indirect_share",
                                    "primary_dealer_share",
                                )
                            }
                            for item in trailing
                        ]
                    )
                ).hexdigest()
                events.append(
                    {
                        **row,
                        "event_id": event_id,
                        "role": role,
                        "history_count": minimum,
                        "history_fingerprint": history_fingerprint,
                        "bid_to_cover_z": btc_z,
                        "indirect_share_z": indirect_z,
                        "primary_dealer_share_z": dealer_z,
                        "demand_score": score,
                        "action": action,
                        "direction": 0 if action == "ABSTAIN" else (1 if score > 0 else -1),
                        "quantity": quantity,
                    }
                )
        # The current auction becomes historical only after its own decision is
        # computed; no current/future value enters the trailing window.
        previous.append(row)
    return events


@dataclass(frozen=True)
class MarketBars:
    frame: pd.DataFrame
    timestamps_ns: np.ndarray

    def __post_init__(self) -> None:
        # Pandas 3 may retain microsecond resolution for parsed ISO strings;
        # force nanoseconds so comparison with ``Timestamp.value`` is exact.
        timestamps = pd.to_datetime(self.frame["timestamp"], utc=True).astype(
            "datetime64[ns, UTC]"
        )
        object.__setattr__(self, "timestamps_ns", timestamps.astype("int64").to_numpy())


def load_market_bars(root: Path, predecessor: Mapping[str, Any], market: str) -> MarketBars:
    path = root / predecessor["frozen_price_inputs"][market]["path"]
    frame = pd.read_parquet(
        path,
        columns=["timestamp", "open", "high", "low", "close", "contract", "session_id"],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).astype(
        "datetime64[ns, UTC]"
    )
    frame = (
        frame.sort_values("timestamp", kind="mergesort")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )
    return MarketBars(frame=frame, timestamps_ns=frame["timestamp"].astype("int64").to_numpy())


def control_direction(event_id: str, policy: str, direction: int) -> int:
    if direction == 0:
        return 0
    if policy == "CAUSAL_DEMAND":
        return direction
    if policy == "DIRECTION_FLIP":
        return -direction
    if policy == "DETERMINISTIC_RANDOM_DIRECTION_EXPOSURE_MATCHED":
        value = hashlib.sha256(f"treasury-causal-v1|{event_id}".encode()).digest()[0]
        return 1 if value & 1 else -1
    raise ValueError(f"unknown policy {policy}")


def replay_event(
    event: Mapping[str, Any],
    bars: MarketBars,
    spec: Mapping[str, Any],
    policy: str,
    scenario: str,
    maximum_holding_minutes: int,
    stop_ticks: int,
    target_ticks: int,
    stressed_extra_ticks_per_side: int,
    maximum_loss_limit_usd: float = 2000.0,
) -> dict[str, Any]:
    """Replay one event with next-open entry and stop-first ambiguity."""

    output = {key: _json_scalar(value) for key, value in event.items()}
    output.update({"policy": policy, "scenario": scenario})
    if event["action"] == "ABSTAIN":
        output.update({"status": "ABSTAIN", "completed": True, "net_pnl_usd": 0.0})
        return output

    start = int(
        np.searchsorted(bars.timestamps_ns, int(pd.Timestamp(event["available_at"]).value), side="right")
    )
    if start >= len(bars.frame):
        output.update({"status": "DATA_CENSORED", "completed": False})
        return output
    entry_row = bars.frame.iloc[start]
    if str(entry_row["session_id"]) != str(event["auction_date"]):
        output.update({"status": "DATA_CENSORED", "completed": False})
        return output

    entry_time = pd.Timestamp(entry_row["timestamp"])
    deadline = entry_time + pd.Timedelta(minutes=int(maximum_holding_minutes))
    end = int(np.searchsorted(bars.timestamps_ns, int(deadline.value), side="right"))
    path = bars.frame.iloc[start:end]
    path = path[path["contract"].astype(str) == str(entry_row["contract"])]
    if path.empty or pd.Timestamp(path.iloc[-1]["timestamp"]) < deadline - pd.Timedelta(minutes=2):
        output.update({"status": "DATA_CENSORED", "completed": False})
        return output

    direction = control_direction(str(event["event_id"]), policy, int(event["direction"]))
    quantity = int(event["quantity"])
    entry_price = float(entry_row["open"])
    tick = float(spec["tick"])
    point_value = float(spec["point_value"])
    stop_price = entry_price - direction * int(stop_ticks) * tick
    target_price = entry_price + direction * int(target_ticks) * tick
    exit_price = float(path.iloc[-1]["close"])
    exit_time = pd.Timestamp(path.iloc[-1]["timestamp"])
    exit_reason = "TIME_EXIT"
    worst_price = entry_price
    for _, bar in path.iterrows():
        low = float(bar["low"])
        high = float(bar["high"])
        if direction > 0:
            worst_price = min(worst_price, low)
            stop_hit = low <= stop_price
            target_hit = high >= target_price
        else:
            worst_price = max(worst_price, high)
            stop_hit = high >= stop_price
            target_hit = low <= target_price
        if stop_hit:  # frozen STOP_FIRST includes same-bar stop/target ambiguity
            exit_price = stop_price
            exit_time = pd.Timestamp(bar["timestamp"])
            exit_reason = "STOP"
            break
        if target_hit:
            exit_price = target_price
            exit_time = pd.Timestamp(bar["timestamp"])
            exit_reason = "TARGET"
            break

    gross = direction * (exit_price - entry_price) * point_value * quantity
    fees = float(spec["round_turn_fee_usd"]) * quantity
    stress = 0.0
    if scenario == "STRESSED":
        stress = 2.0 * int(stressed_extra_ticks_per_side) * tick * point_value * quantity
    elif scenario != "NORMAL":
        raise ValueError(f"unknown scenario {scenario}")
    net = gross - fees - stress
    adverse = direction * (worst_price - entry_price) * point_value * quantity - fees - stress
    output.update(
        {
            "status": "TRADE_COMPLETED",
            "completed": True,
            "effective_direction": direction,
            "quantity": quantity,
            "entry_time": entry_time.isoformat(),
            "entry_contract": str(entry_row["contract"]),
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "exit_time": exit_time.isoformat(),
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "gross_pnl_usd": gross,
            "fees_usd": fees,
            "slippage_usd": stress,
            "net_pnl_usd": net,
            "minimum_trade_pnl_usd": adverse,
            "event_mll_breach": adverse <= -float(maximum_loss_limit_usd),
        }
    )
    return output


def _daily_rows(trades: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    output: dict[str, list[Mapping[str, Any]]] = {}
    for trade in trades:
        if trade.get("status") == "TRADE_COMPLETED":
            output.setdefault(str(trade["auction_date"]), []).append(trade)
    for rows in output.values():
        rows.sort(key=lambda row: (str(row["entry_time"]), str(row["event_id"])))
    return output


def simulate_account_window(
    days: Sequence[str], trades: Sequence[Mapping[str, Any]], account: Mapping[str, Any]
) -> dict[str, Any]:
    target = float(account["profit_target_usd"])
    mll_distance = float(account["maximum_loss_limit_usd"])
    consistency = float(account["consistency_fraction"])
    by_day = _daily_rows(trades)
    equity = 0.0
    eod_high_water = 0.0
    mll_level = -mll_distance
    minimum_buffer = math.inf
    best_day = 0.0
    traded_days = 0
    breached = False
    passed = False
    pass_day: int | None = None
    final_required = target
    daily_pnl: list[dict[str, Any]] = []
    for day_number, day in enumerate(days, start=1):
        start_equity = equity
        rows = by_day.get(day, [])
        if rows:
            traded_days += 1
        for trade in rows:
            worst_equity = equity + float(trade["minimum_trade_pnl_usd"])
            minimum_buffer = min(minimum_buffer, worst_equity - mll_level)
            if worst_equity <= mll_level:
                breached = True
                # Persist the actual intratrade equity at the point the account
                # violates the real-time MLL rather than leaving a deceptively
                # benign pre-trade closing balance in the failed window.
                equity = worst_equity
                break
            equity += float(trade["net_pnl_usd"])
            minimum_buffer = min(minimum_buffer, equity - mll_level)
        day_profit = equity - start_equity
        best_day = max(best_day, day_profit)
        final_required = max(target, best_day / consistency)
        daily_pnl.append({"day": day, "pnl_usd": day_profit, "equity_usd": equity})
        if breached:
            break
        if traded_days >= 2 and equity >= final_required:
            passed = True
            pass_day = day_number
            break
        eod_high_water = max(eod_high_water, equity)
        mll_level = min(0.0, eod_high_water - mll_distance)
        minimum_buffer = min(minimum_buffer, equity - mll_level)

    if not math.isfinite(minimum_buffer):
        minimum_buffer = mll_distance
    return {
        "start": days[0],
        "end": days[-1],
        "session_count": len(days),
        "traded_days": traded_days,
        "net_pnl_usd": equity,
        "best_day_usd": best_day,
        "required_profit_usd": final_required,
        "target_progress_fraction": equity / final_required if final_required > 0 else 0.0,
        "minimum_mll_buffer_usd": minimum_buffer,
        "mll_breached": breached,
        "consistency_compliant": best_day <= consistency * max(equity, target),
        "passed": passed,
        "pass_day": pass_day,
        "daily_pnl": daily_pnl,
    }


def account_windows(
    trades: Sequence[Mapping[str, Any]],
    sessions: Sequence[str],
    width: int,
    roles: Mapping[str, Sequence[str]],
    account: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for role, bounds in roles.items():
        eligible = [day for day in sessions if str(bounds[0]) <= day < str(bounds[1])]
        for offset in range(0, len(eligible) - width + 1, width):
            days = eligible[offset : offset + width]
            result = simulate_account_window(days, trades, account)
            result.update({"role": role, "horizon_sessions": int(width)})
            output.append(result)
    return output


def summarize_trades(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    completed = [row for row in rows if row.get("status") == "TRADE_COMPLETED"]
    evaluated = [row for row in rows if row.get("completed")]
    nets = [float(row.get("net_pnl_usd", 0.0)) for row in completed]
    return {
        "event_count": len(rows),
        "completed_event_count": len(evaluated),
        "trade_count": len(completed),
        "trade_coverage_fraction": len(completed) / len(evaluated) if evaluated else 0.0,
        "net_pnl_usd": float(sum(nets)),
        "median_trade_net_usd": float(np.median(nets)) if nets else None,
        "positive_trade_count": sum(value > 0 for value in nets),
        "target_exit_count": sum(row.get("exit_reason") == "TARGET" for row in completed),
        "stop_exit_count": sum(row.get("exit_reason") == "STOP" for row in completed),
        "time_exit_count": sum(row.get("exit_reason") == "TIME_EXIT" for row in completed),
        "event_mll_breach_count": sum(bool(row.get("event_mll_breach")) for row in completed),
    }


def summarize_windows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    progress = [float(row["target_progress_fraction"]) for row in rows]
    buffers = [float(row["minimum_mll_buffer_usd"]) for row in rows]
    nets = [float(row["net_pnl_usd"]) for row in rows]
    return {
        "window_count": len(rows),
        "pass_count": sum(bool(row["passed"]) for row in rows),
        "pass_rate": sum(bool(row["passed"]) for row in rows) / len(rows) if rows else 0.0,
        "mll_breach_count": sum(bool(row["mll_breached"]) for row in rows),
        "mll_breach_rate": sum(bool(row["mll_breached"]) for row in rows) / len(rows) if rows else 0.0,
        "consistency_compliant_count": sum(bool(row["consistency_compliant"]) for row in rows),
        "consistency_compliance_rate": sum(bool(row["consistency_compliant"]) for row in rows) / len(rows)
        if rows
        else 0.0,
        "median_net_pnl_usd": float(np.median(nets)) if nets else None,
        "total_net_pnl_usd": float(sum(nets)),
        "median_target_progress_fraction": float(np.median(progress)) if progress else None,
        "lower_quartile_target_progress_fraction": float(np.quantile(progress, 0.25)) if progress else None,
        "minimum_mll_buffer_usd": min(buffers) if buffers else None,
        "median_days_to_pass": float(np.median([row["pass_day"] for row in rows if row["passed"]]))
        if any(row["passed"] for row in rows)
        else None,
    }


def evaluate_gate(result: Mapping[str, Any], gate: Mapping[str, Any]) -> tuple[str, dict[str, bool]]:
    causal = result["trade_summary"]["CAUSAL_DEMAND"]
    validation = causal["STRESSED"]["VALIDATION"]
    final = causal["STRESSED"]["FINAL_DEVELOPMENT"]
    controls = result["trade_summary"]
    completed_total = sum(causal["NORMAL"][role]["completed_event_count"] for role in causal["NORMAL"])
    trade_total = sum(causal["NORMAL"][role]["trade_count"] for role in causal["NORMAL"])
    coverage = trade_total / completed_total if completed_total else 0.0
    p20 = result["account_summary"]["CAUSAL_DEMAND"]["STRESSED"]["20"]
    total_trades = sum(causal["STRESSED"][role]["trade_count"] for role in causal["STRESSED"])
    total_breaches = sum(causal["STRESSED"][role]["event_mll_breach_count"] for role in causal["STRESSED"])
    checks = {
        "minimum_validation_completed_events": validation["completed_event_count"] >= int(gate["minimum_validation_completed_events"]),
        "minimum_final_completed_events": final["completed_event_count"] >= int(gate["minimum_final_completed_events"]),
        "trade_coverage_minimum": coverage >= float(gate["minimum_trade_coverage_fraction"]),
        "trade_coverage_maximum": coverage <= float(gate["maximum_trade_coverage_fraction"]),
        "positive_stressed_net_validation": validation["net_pnl_usd"] > 0.0,
        "positive_stressed_net_final": final["net_pnl_usd"] > 0.0,
        "beats_flip_and_random_validation": all(
            validation["net_pnl_usd"] > controls[policy]["STRESSED"]["VALIDATION"]["net_pnl_usd"]
            for policy in ("DIRECTION_FLIP", "DETERMINISTIC_RANDOM_DIRECTION_EXPOSURE_MATCHED")
        ),
        "beats_flip_and_random_final": all(
            final["net_pnl_usd"] > controls[policy]["STRESSED"]["FINAL_DEVELOPMENT"]["net_pnl_usd"]
            for policy in ("DIRECTION_FLIP", "DETERMINISTIC_RANDOM_DIRECTION_EXPOSURE_MATCHED")
        ),
        "event_mll_breach_rate": (total_breaches / total_trades if total_trades else 1.0)
        <= float(gate["maximum_event_mll_breach_rate"]),
        "minimum_stressed_p20_pass_count": p20["pass_count"] >= int(gate["minimum_stressed_p20_pass_count"]),
    }
    if all(checks.values()):
        status = str(gate["success_status"])
    elif (
        validation["net_pnl_usd"] > 0
        or final["net_pnl_usd"] > 0
        or p20["pass_count"] > 0
    ):
        status = str(gate["weak_status"])
    else:
        status = str(gate["failure_status"])
    return status, checks


def run_tripwire(root: Path, manifest_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    manifest = load_manifest(root, manifest_path)
    predecessor = load_predecessor(root, manifest)
    official: list[dict[str, Any]] = []
    for label in ("note", "bond"):
        path = root / manifest["frozen_official_inputs"][f"{label}_path"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise RuntimeError(f"official {label} input must be a list")
        official.extend(payload)
    events = build_causal_events(official, manifest, predecessor)
    bars = {
        market: load_market_bars(root, predecessor, market)
        for market in predecessor["frozen_price_inputs"]
    }
    policy = manifest["policy"]
    account = manifest["account_evaluation"]
    policies = ["CAUSAL_DEMAND", *policy["controls"]]
    scenarios = ["NORMAL", "STRESSED"]
    ledger: list[dict[str, Any]] = []
    for event in events:
        market = str(event["market"])
        for policy_name in policies:
            for scenario in scenarios:
                ledger.append(
                    replay_event(
                        event,
                        bars[market],
                        predecessor["market_specs"][market],
                        policy_name,
                        scenario,
                        int(policy["maximum_holding_minutes"]),
                        int(policy["stop_ticks"]),
                        int(policy["target_ticks"]),
                        int(policy["stressed_extra_ticks_per_side"]),
                        float(account["maximum_loss_limit_usd"]),
                    )
                )
    ledger.sort(key=lambda row: (str(row["available_at"]), row["policy"], row["scenario"], row["event_id"]))

    zn = bars["ZN"].frame
    sessions = sorted(
        day for day in zn["session_id"].astype(str).unique()
        if "2023-01-01" <= day < "2024-10-01"
    )
    window_ledger: list[dict[str, Any]] = []
    trade_summary: dict[str, Any] = {}
    account_summary: dict[str, Any] = {}
    for policy_name in policies:
        trade_summary[policy_name] = {}
        account_summary[policy_name] = {}
        for scenario in scenarios:
            selected = [row for row in ledger if row["policy"] == policy_name and row["scenario"] == scenario]
            trade_summary[policy_name][scenario] = {
                role: summarize_trades([row for row in selected if row["role"] == role])
                for role in manifest["chronological_roles"]
            }
            account_summary[policy_name][scenario] = {}
            for width in account["complete_non_overlapping_windows_sessions"]:
                windows = account_windows(
                    selected,
                    sessions,
                    int(width),
                    manifest["chronological_roles"],
                    account,
                )
                for row in windows:
                    row.update({"policy": policy_name, "scenario": scenario})
                window_ledger.extend(windows)
                summary = summarize_windows(windows)
                summary["by_role"] = {
                    role: summarize_windows([row for row in windows if row["role"] == role])
                    for role in manifest["chronological_roles"]
                }
                account_summary[policy_name][scenario][str(width)] = summary

    result: dict[str, Any] = {
        "schema": "hydra_treasury_auction_demand_shock_causal_result_v1",
        "branch_id": manifest["branch_id"],
        "manifest_hash": manifest["manifest_hash"],
        "predecessor_result_hash": manifest["predecessor"]["upper_bound_result_hash"],
        "official_source_hashes": {
            label: manifest["frozen_official_inputs"][f"{label}_sha256"] for label in ("note", "bond")
        },
        "price_input_hashes": {
            market: item["sha256"] for market, item in predecessor["frozen_price_inputs"].items()
        },
        "raw_official_record_count": len(official),
        "causal_feature_event_count": len(events),
        "causal_feature_event_count_by_role": {
            role: sum(event["role"] == role for event in events)
            for role in manifest["chronological_roles"]
        },
        "action_count": {
            action: sum(event["action"] == action for event in events)
            for action in manifest["policy"]["actions"]
        },
        "trade_summary": trade_summary,
        "account_summary": account_summary,
        "incremental_spend_usd": 0.0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "tier_q_allowed": False,
        "xfa_allowed": False,
    }
    status, checks = evaluate_gate(result, manifest["gate"])
    result["status"] = status
    result["gate_checks"] = checks
    result["gate_pass"] = status == manifest["gate"]["success_status"]
    result["next_action"] = (
        "PRESERVE_TIER_E_AND_QUEUE_DISTINCT_CONFIRMATION"
        if result["gate_pass"]
        else "TOMBSTONE_EXACT_CAUSAL_DEMAND_SCORE_NO_NEIGHBOR_RETRY"
    )
    result["event_ledger_hash"] = hashlib.sha256(
        b"".join(canonical_json(json_ready(row)) + b"\n" for row in ledger)
    ).hexdigest()
    result["account_window_ledger_hash"] = hashlib.sha256(
        b"".join(canonical_json(json_ready(row)) + b"\n" for row in window_ledger)
    ).hexdigest()
    result = json_ready(result)
    result["result_hash"] = hashlib.sha256(canonical_json(result)).hexdigest()
    return result, json_ready(ledger), json_ready(window_ledger)


def write_outputs(
    result: Mapping[str, Any],
    event_ledger: Sequence[Mapping[str, Any]],
    window_ledger: Sequence[Mapping[str, Any]],
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "result": output_dir / "causal_result.json",
        "event_ledger": output_dir / "causal_event_ledger.jsonl",
        "account_windows": output_dir / "account_window_ledger.jsonl",
    }
    paths["result"].write_bytes(canonical_json(json_ready(result)) + b"\n")
    paths["event_ledger"].write_bytes(
        b"".join(canonical_json(json_ready(row)) + b"\n" for row in event_ledger)
    )
    paths["account_windows"].write_bytes(
        b"".join(canonical_json(json_ready(row)) + b"\n" for row in window_ledger)
    )
    return {label: str(path) for label, path in paths.items()}
