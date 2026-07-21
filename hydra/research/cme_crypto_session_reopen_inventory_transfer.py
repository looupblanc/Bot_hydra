"""Bounded CME micro-crypto close-to-reopen inventory-transfer tripwire.

This branch reuses the immutable 2022-2023 MBT/MET TBBO tranche.  It is
materially different from the prior intraday signed-flow shock test: there is
one scheduled opportunity per exchange session, one selected market, and no
decision feature after the frozen decision time.
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

from hydra.economic_evolution.schema import stable_hash
from hydra.research.cme_cross_crypto_flow_response_residual import (
    CHICAGO,
    SYMBOLS,
    _load_contract_spec,
    _load_events,
    audit_inputs as audit_cross_crypto_inputs,
)


MANIFEST = Path("config/research/cme_crypto_session_reopen_inventory_transfer_v1.json")
ROLES = ("DISCOVERY", "VALIDATION")
CONTROLS = ("PRIMARY", "DIRECTION_FLIP", "DETERMINISTIC_RANDOM_DIRECTION")


class CryptoReopenTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PolicySpec:
    mechanism: str
    decision_delay_minutes: int
    holding_minutes: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_manifest(root: Path) -> dict[str, Any]:
    manifest = _read_json(root / MANIFEST)
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise CryptoReopenTripwireError("frozen manifest hash drift")
    return manifest


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _read_manifest(project)
    source = audit_cross_crypto_inputs(project)
    expected = manifest["frozen_inputs"]
    if (
        source["files"]["tbbo"]["sha256"] != expected["tbbo_sha256"]
        or source["files"]["definition"]["sha256"] != expected["definition_sha256"]
        or source["receipt"].get("q4_access_count_delta") != 0
        or source["receipt"].get("broker_connections") != 0
        or source["receipt"].get("orders") != 0
    ):
        raise CryptoReopenTripwireError("immutable source receipt drift")
    return {
        "manifest": manifest,
        "source": source,
        "audit_hash": stable_hash(
            {
                "manifest_hash": manifest["manifest_hash"],
                "source_audit_hash": source["audit_hash"],
                "tbbo_sha256": expected["tbbo_sha256"],
                "definition_sha256": expected["definition_sha256"],
            }
        ),
    }


def frozen_specs(manifest: Mapping[str, Any] | None = None) -> list[PolicySpec]:
    if manifest is None:
        manifest = _read_json(Path(MANIFEST))
    lattice = manifest["candidate_lattice"]
    specs = [
        PolicySpec(str(mechanism), int(delay), int(hold))
        for mechanism in lattice["mechanisms"]
        for delay in lattice["decision_delay_minutes"]
        for hold in lattice["holding_minutes"]
    ]
    if len(specs) != int(lattice["proposal_count"]):
        raise CryptoReopenTripwireError("frozen policy count drift")
    return specs


def _policy_id(spec: PolicySpec, manifest: Mapping[str, Any]) -> str:
    return "crypto_reopen_" + stable_hash(
        {
            "spec": asdict(spec),
            "manifest_hash": manifest["manifest_hash"],
            "causal_contract": manifest["causal_contract"],
            "execution": manifest["execution"],
        }
    )[:20]


def _role(timestamp: pd.Series) -> pd.Series:
    role = pd.Series(pd.NA, index=timestamp.index, dtype="object")
    role.loc[timestamp.ge("2022-01-01") & timestamp.lt("2023-01-01")] = "DISCOVERY"
    role.loc[timestamp.ge("2023-01-01") & timestamp.lt("2024-01-01")] = "VALIDATION"
    return role


def _session_table(frame: pd.DataFrame, manifest: Mapping[str, Any]) -> pd.DataFrame:
    """Build one causal row per observed exchange session.

    ``prev_close`` and ``prior_range`` are shifted by one complete session.
    The trailing gap scale is also shifted, so modifying a later session can
    never change an earlier decision row.
    """

    # The reused source loader labels exchange sessions with timezone-aware
    # timestamps.  Adding a calendar day across the spring DST boundary can
    # yield both 00:00 and 01:00 labels for the same Chicago session date.
    # Canonicalise to the exchange-calendar date before aggregation so the
    # reopen and the following day session remain one opportunity.
    ordered = frame.copy()
    ordered["session_key"] = pd.to_datetime(ordered["session_day"]).map(
        lambda value: pd.Timestamp(value).date()
    )
    ordered = ordered.sort_values(["session_key", "ts_recv", "sequence"], kind="mergesort")
    grouped = ordered.groupby("session_key", sort=True, observed=True)
    first = grouped.first()
    last = grouped.last()
    ranges = grouped["mid"].agg(lambda values: float(values.max() - values.min()))
    table = pd.DataFrame(
        {
            "session_day": first.index,
            "open_ts": pd.to_datetime(first["ts_recv"], utc=True).to_numpy(),
            "open_mid": first["mid"].astype(float).to_numpy(),
            "open_spread": first["spread"].astype(float).to_numpy(),
            "close_ts": pd.to_datetime(last["ts_recv"], utc=True).to_numpy(),
            "close_mid": last["mid"].astype(float).to_numpy(),
            "session_range": ranges.astype(float).to_numpy(),
        }
    ).sort_values("session_day", kind="mergesort").reset_index(drop=True)
    table["prev_close"] = table["close_mid"].shift(1)
    table["prior_range"] = table["session_range"].shift(1)
    table["gap"] = table["open_mid"] - table["prev_close"]
    lattice = manifest["candidate_lattice"]
    trailing = (
        table["gap"]
        .abs()
        .shift(1)
        .rolling(
            int(lattice["trailing_gap_sessions"]),
            min_periods=int(lattice["trailing_minimum_sessions"]),
        )
        .median()
    )
    table["trailing_gap_median"] = trailing
    table["gap_score"] = table["gap"] / trailing.replace(0.0, np.nan)
    table["role"] = _role(pd.to_datetime(table["open_ts"], utc=True))
    return table.dropna(
        subset=["prev_close", "prior_range", "gap_score", "role"]
    ).reset_index(drop=True)


def _paired_sessions(
    events: Mapping[str, pd.DataFrame], manifest: Mapping[str, Any]
) -> pd.DataFrame:
    tables = {}
    for market in SYMBOLS:
        table = _session_table(events[market], manifest)
        tables[market] = table.rename(
            columns={column: f"{market}_{column}" for column in table.columns if column != "session_day"}
        )
    paired = tables["MBT"].merge(tables["MET"], on="session_day", how="inner", validate="one_to_one")
    if not paired["MBT_role"].eq(paired["MET_role"]).all():
        raise CryptoReopenTripwireError("cross-market temporal-role mismatch")
    paired["role"] = paired["MBT_role"]
    return paired.sort_values("session_day", kind="mergesort").reset_index(drop=True)


def _opportunities(
    sessions: pd.DataFrame,
    spec: PolicySpec,
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    threshold = float(manifest["candidate_lattice"]["minimum_abs_gap_over_trailing_median"])
    output: list[dict[str, Any]] = []
    for row in sessions.itertuples(index=False):
        scores = {market: float(getattr(row, f"{market}_gap_score")) for market in SYMBOLS}
        gaps = {market: float(getattr(row, f"{market}_gap")) for market in SYMBOLS}
        if not all(math.isfinite(value) for value in (*scores.values(), *gaps.values())):
            continue
        target: str | None = None
        direction = 0
        raw_score = 0.0
        if spec.mechanism in {"OWN_REOPEN_GAP_CONTINUATION", "OWN_REOPEN_GAP_FADE"}:
            target = max(SYMBOLS, key=lambda market: (abs(scores[market]), market))
            raw_score = abs(scores[target])
            if raw_score < threshold or gaps[target] == 0.0:
                continue
            direction = int(math.copysign(1, gaps[target]))
            if spec.mechanism == "OWN_REOPEN_GAP_FADE":
                direction *= -1
        elif spec.mechanism == "COMMON_REOPEN_GAP_CONTINUATION":
            if (
                min(abs(scores[market]) for market in SYMBOLS) < threshold
                or np.sign(gaps["MBT"]) != np.sign(gaps["MET"])
                or gaps["MBT"] == 0.0
            ):
                continue
            target = max(SYMBOLS, key=lambda market: (abs(scores[market]), market))
            direction = int(math.copysign(1, gaps[target]))
            raw_score = min(abs(scores[market]) for market in SYMBOLS)
        elif spec.mechanism == "RELATIVE_REOPEN_GAP_MEAN_REVERSION":
            residual = scores["MBT"] - scores["MET"]
            if abs(residual) < threshold:
                continue
            target = max(SYMBOLS, key=lambda market: (abs(scores[market]), market))
            other = "MET" if target == "MBT" else "MBT"
            relative = scores[target] - scores[other]
            if relative == 0.0:
                continue
            direction = -int(math.copysign(1, relative))
            raw_score = abs(residual)
        else:
            raise CryptoReopenTripwireError(f"unknown mechanism: {spec.mechanism}")
        decision_time = max(
            pd.Timestamp(getattr(row, "MBT_open_ts")),
            pd.Timestamp(getattr(row, "MET_open_ts")),
        ) + pd.Timedelta(minutes=spec.decision_delay_minutes)
        core = {
            "session_day": pd.Timestamp(row.session_day).date().isoformat(),
            "role": str(row.role),
            "market": target,
            "direction": direction,
            "decision_time": decision_time.isoformat(),
            "gap_score": raw_score,
            "target_gap": gaps[target],
            "other_gap": gaps["MET" if target == "MBT" else "MBT"],
            "prior_session_range": float(getattr(row, f"{target}_prior_range")),
        }
        output.append({**core, "opportunity_hash": stable_hash(core)})
    if len({row["session_day"] for row in output}) != len(output):
        raise CryptoReopenTripwireError("one-market-per-session invariant failed")
    return output


def _flatten_utc(session_day: str) -> pd.Timestamp:
    return pd.Timestamp(f"{session_day} 15:10").tz_localize(CHICAGO).tz_convert("UTC")


def _control_direction(
    direction: int, control: str, policy_id: str, opportunity: Mapping[str, Any]
) -> int:
    if control == "PRIMARY":
        return direction
    if control == "DIRECTION_FLIP":
        return -direction
    if control == "DETERMINISTIC_RANDOM_DIRECTION":
        digest = stable_hash(
            {
                "policy_id": policy_id,
                "session_day": opportunity["session_day"],
                "market": opportunity["market"],
                "control": control,
            }
        )
        return 1 if int(digest[:8], 16) % 2 else -1
    raise CryptoReopenTripwireError(f"unknown control: {control}")


def _simulate(
    opportunities: Sequence[Mapping[str, Any]],
    spec: PolicySpec,
    events: Mapping[str, pd.DataFrame],
    contracts: Mapping[str, Mapping[str, float]],
    manifest: Mapping[str, Any],
    *,
    control: str,
) -> list[dict[str, Any]]:
    policy_id = _policy_id(spec, manifest)
    arrays: dict[str, dict[str, Any]] = {}
    for market in SYMBOLS:
        frame = events[market]
        arrays[market] = {
            "frame": frame,
            "ts": frame["ts_recv"].to_numpy(dtype="datetime64[ns]").astype(np.int64),
            "sessions": frame["session_day"].to_numpy(),
            "bid": frame["bid_px_00"].to_numpy(float),
            "ask": frame["ask_px_00"].to_numpy(float),
            "bid_size": frame["bid_sz_00"].to_numpy(float),
            "ask_size": frame["ask_sz_00"].to_numpy(float),
        }
    output: list[dict[str, Any]] = []
    lattice = manifest["candidate_lattice"]
    for opportunity in opportunities:
        market = str(opportunity["market"])
        data = arrays[market]
        ts = data["ts"]
        decision = pd.Timestamp(str(opportunity["decision_time"])).value
        entry_index = int(np.searchsorted(ts, decision, side="right"))
        session_label = str(opportunity["session_day"])
        if entry_index >= len(ts) or pd.Timestamp(data["sessions"][entry_index]).date().isoformat() != session_label:
            output.append(
                {
                    **dict(opportunity),
                    "policy_id": policy_id,
                    "control": control,
                    "outcome_state": "DATA_CENSORED",
                    "censor_reason": "MISSING_STRICTLY_LATER_ENTRY",
                }
            )
            continue
        direction = _control_direction(int(opportunity["direction"]), control, policy_id, opportunity)
        entry_price = data["ask"][entry_index] if direction > 0 else data["bid"][entry_index]
        displayed = data["ask_size"][entry_index] if direction > 0 else data["bid_size"][entry_index]
        if not math.isfinite(entry_price) or displayed < 1.0:
            continue
        contract = contracts[market]
        tick = float(contract["tick_size"])
        point_value = float(contract["point_value_usd"])
        risk = max(
            int(lattice["minimum_stop_ticks"]) * tick,
            float(lattice["stop_fraction_of_prior_session_range"])
            * float(opportunity["prior_session_range"]),
        )
        stop = entry_price - direction * risk
        target = entry_price + direction * risk * float(lattice["target_stop_multiple"])
        deadline = min(
            ts[entry_index] + int(spec.holding_minutes * 60 * 1e9),
            _flatten_utc(session_label).value,
        )
        final_index = min(int(np.searchsorted(ts, deadline, side="left")), len(ts) - 1)
        exit_index: int | None = None
        exit_price: float | None = None
        exit_reason = ""
        minimum_open_gross = 0.0
        for path_index in range(entry_index, final_index + 1):
            if pd.Timestamp(data["sessions"][path_index]).date().isoformat() != session_label:
                break
            executable = data["bid"][path_index] if direction > 0 else data["ask"][path_index]
            open_gross = direction * (executable - entry_price) * point_value
            minimum_open_gross = min(minimum_open_gross, open_gross)
            stop_hit = executable <= stop if direction > 0 else executable >= stop
            target_hit = executable >= target if direction > 0 else executable <= target
            if stop_hit:
                exit_index, exit_price, exit_reason = path_index, executable, "STOP_FIRST"
                break
            if target_hit:
                exit_index, exit_price, exit_reason = path_index, target, "TARGET"
                break
            if ts[path_index] >= deadline:
                exit_index, exit_price, exit_reason = path_index, executable, "TIME_EXIT"
                break
        if exit_index is None or exit_price is None:
            output.append(
                {
                    **dict(opportunity),
                    "policy_id": policy_id,
                    "control": control,
                    "outcome_state": "DATA_CENSORED",
                    "censor_reason": "MISSING_EXIT_COVERAGE",
                }
            )
            continue
        gross = direction * (exit_price - entry_price) * point_value
        normal_cost = float(manifest["execution"]["normal_round_turn_fees_usd"][market])
        stressed_cost = normal_cost + 2.0 * tick * point_value
        core = {
            **dict(opportunity),
            "policy_id": policy_id,
            "control": control,
            "outcome_state": "FULL_COVERAGE",
            "direction": direction,
            "entry_time": pd.Timestamp(ts[entry_index], unit="ns", tz="UTC").isoformat(),
            "entry_price": float(entry_price),
            "stop_price": float(stop),
            "target_price": float(target),
            "exit_time": pd.Timestamp(ts[exit_index], unit="ns", tz="UTC").isoformat(),
            "exit_price": float(exit_price),
            "exit_reason": exit_reason,
            "gross_pnl_usd": float(gross),
            "normal_cost_usd": normal_cost,
            "stressed_cost_usd": stressed_cost,
            "normal_net_usd": float(gross - normal_cost),
            "stressed_net_usd": float(gross - stressed_cost),
            "minimum_open_gross_pnl_usd": float(minimum_open_gross),
            "fill_policy_id": manifest["execution"]["fill_model"],
        }
        output.append({**core, "event_hash": stable_hash(core)})
    return output


def _summary(events: Sequence[Mapping[str, Any]], role: str) -> dict[str, Any]:
    role_rows = [row for row in events if row["role"] == role]
    rows = [row for row in role_rows if row["outcome_state"] == "FULL_COVERAGE"]
    gross = [float(row["gross_pnl_usd"]) for row in rows]
    normal = [float(row["normal_net_usd"]) for row in rows]
    stressed = [float(row["stressed_net_usd"]) for row in rows]
    costs = [float(row["stressed_cost_usd"]) for row in rows]
    positive_trades = [value for value in stressed if value > 0.0]
    by_day: dict[str, float] = {}
    by_market: dict[str, float] = {market: 0.0 for market in SYMBOLS}
    by_half: dict[str, float] = {"H1": 0.0, "H2": 0.0}
    for row in rows:
        day = str(row["session_day"])
        value = float(row["stressed_net_usd"])
        by_day[day] = by_day.get(day, 0.0) + value
        by_market[str(row["market"])] += value
        by_half["H1" if int(day[5:7]) <= 6 else "H2"] += value
    positive_days = [value for value in by_day.values() if value > 0.0]
    positive_markets = [value for value in by_market.values() if value > 0.0]
    return {
        "role": role,
        "signal_count": len(role_rows),
        "event_count": len(rows),
        "data_censored_count": len(role_rows) - len(rows),
        "session_count": len(by_day),
        "gross_pnl_usd": float(sum(gross)),
        "normal_net_usd": float(sum(normal)),
        "stressed_net_usd": float(sum(stressed)),
        "stressed_net_per_event_usd": float(np.mean(stressed)) if stressed else None,
        "median_stressed_event_usd": float(np.median(stressed)) if stressed else None,
        "lower_quartile_stressed_event_usd": float(np.quantile(stressed, 0.25)) if stressed else None,
        "positive_stressed_event_rate": float(sum(value > 0.0 for value in stressed) / len(stressed)) if stressed else None,
        "stressed_edge_to_cost_ratio": float(sum(gross) / sum(costs)) if sum(costs) else None,
        "maximum_single_trade_positive_profit_share": float(max(positive_trades) / sum(positive_trades)) if positive_trades else None,
        "maximum_positive_day_profit_share": float(max(positive_days) / sum(positive_days)) if positive_days else None,
        "maximum_single_market_positive_profit_share": float(max(positive_markets) / sum(positive_markets)) if positive_markets else None,
        "stressed_net_by_market": by_market,
        "stressed_net_by_half_year": by_half,
        "market_event_counts": {market: sum(row["market"] == market for row in rows) for market in SYMBOLS},
        "target_count": sum(row["exit_reason"] == "TARGET" for row in rows),
        "stop_count": sum(row["exit_reason"] == "STOP_FIRST" for row in rows),
        "minimum_open_gross_pnl_usd": min((float(row["minimum_open_gross_pnl_usd"]) for row in rows), default=None),
        "event_path_hash": stable_hash([row.get("event_hash") for row in role_rows]),
    }


def _evaluate(
    sessions: pd.DataFrame,
    spec: PolicySpec,
    events: Mapping[str, pd.DataFrame],
    contracts: Mapping[str, Mapping[str, float]],
    manifest: Mapping[str, Any],
    *,
    control: str = "PRIMARY",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opportunities = _opportunities(sessions, spec, manifest)
    simulated = _simulate(opportunities, spec, events, contracts, manifest, control=control)
    result = {
        "policy_id": _policy_id(spec, manifest),
        "spec": asdict(spec),
        "control": control,
        "opportunity_count": len(opportunities),
        "roles": {role: _summary(simulated, role) for role in ROLES},
        "event_path_hash": stable_hash([row.get("event_hash") for row in simulated]),
    }
    return result, simulated


def _rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    value = row["roles"]["DISCOVERY"]
    return (
        float(value["stressed_net_usd"]),
        float(value["stressed_net_per_event_usd"] or -math.inf),
        int(value["event_count"]),
        str(row["policy_id"]),
    )


def _validation_gate(
    primary: Mapping[str, Any], controls: Mapping[str, Mapping[str, Any]], manifest: Mapping[str, Any]
) -> dict[str, Any]:
    gate = manifest["selection_gate"]
    held = primary["roles"]["VALIDATION"]
    controls_beaten = all(
        float(held["stressed_net_per_event_usd"] or -math.inf)
        > float(control["roles"]["VALIDATION"]["stressed_net_per_event_usd"] or -math.inf)
        for control in controls.values()
    )
    checks = {
        "minimum_validation_events": int(held["event_count"]) >= int(gate["minimum_validation_events"]),
        "positive_validation_stressed": float(held["stressed_net_usd"]) > float(gate["validation_stressed_net_usd_minimum_exclusive"]),
        "positive_validation_half_years": sum(value > 0.0 for value in held["stressed_net_by_half_year"].values()) >= int(gate["positive_validation_half_years_required"]),
        "positive_validation_markets": sum(value > 0.0 for value in held["stressed_net_by_market"].values()) >= int(gate["positive_validation_markets_required"]),
        "single_trade_concentration": held["maximum_single_trade_positive_profit_share"] is not None and float(held["maximum_single_trade_positive_profit_share"]) <= float(gate["maximum_single_trade_or_day_positive_profit_share"]),
        "single_day_concentration": held["maximum_positive_day_profit_share"] is not None and float(held["maximum_positive_day_profit_share"]) <= float(gate["maximum_single_trade_or_day_positive_profit_share"]),
        "single_market_concentration": held["maximum_single_market_positive_profit_share"] is not None and float(held["maximum_single_market_positive_profit_share"]) <= float(gate["maximum_single_market_positive_profit_share"]),
        "matched_controls_beaten": controls_beaten,
    }
    return {"passed": all(checks.values()), "checks": checks}


def _account_summary(
    events: Sequence[Mapping[str, Any]], calendar: Sequence[str], quantity: int, horizon: int,
    *, scenario: str,
) -> dict[str, Any]:
    by_day = {str(row["session_day"]): row for row in events if row["outcome_state"] == "FULL_COVERAGE"}
    episodes: list[dict[str, Any]] = []
    for offset in range(0, len(calendar) - horizon + 1, horizon):
        days = list(calendar[offset : offset + horizon])
        realized = 0.0
        mll_level = -2000.0
        minimum_buffer = 2000.0
        best_day = 0.0
        traded_days = 0
        terminal = "TIMEOUT"
        days_to_target: int | None = None
        for day_number, day in enumerate(days, start=1):
            row = by_day.get(day)
            daily = 0.0
            if row is not None:
                traded_days += 1
                cost_key = "normal_cost_usd" if scenario == "NORMAL" else "stressed_cost_usd"
                daily = quantity * (float(row["gross_pnl_usd"]) - float(row[cost_key]))
                open_equity = realized + quantity * float(row["minimum_open_gross_pnl_usd"]) - quantity * float(row[cost_key]) / 2.0
                minimum_buffer = min(minimum_buffer, open_equity - mll_level)
                if open_equity <= mll_level:
                    terminal = "MLL_BREACHED"
                    break
                realized += daily
                minimum_buffer = min(minimum_buffer, realized - mll_level)
                if realized <= mll_level:
                    terminal = "MLL_BREACHED"
                    break
                best_day = max(best_day, daily)
                required = max(3000.0, best_day / 0.5)
                if traded_days >= 2 and realized >= required:
                    terminal = "TARGET_REACHED"
                    days_to_target = day_number
                    break
            mll_level = min(0.0, max(mll_level, realized - 2000.0))
        episodes.append(
            {
                "start_day": days[0],
                "terminal": terminal,
                "net_usd": realized,
                "target_progress": realized / max(3000.0, best_day / 0.5),
                "minimum_mll_buffer_usd": minimum_buffer,
                "consistency_compliant": best_day <= 0.5 * max(realized, 3000.0),
                "days_to_target": days_to_target,
            }
        )
    passes = [row for row in episodes if row["terminal"] == "TARGET_REACHED"]
    return {
        "scenario": scenario,
        "quantity": quantity,
        "horizon_trading_days": horizon,
        "episode_count": len(episodes),
        "pass_count": len(passes),
        "pass_rate": len(passes) / len(episodes) if episodes else None,
        "mll_breach_count": sum(row["terminal"] == "MLL_BREACHED" for row in episodes),
        "mll_breach_rate": sum(row["terminal"] == "MLL_BREACHED" for row in episodes) / len(episodes) if episodes else None,
        "net_median_usd": float(np.median([row["net_usd"] for row in episodes])) if episodes else None,
        "target_progress_median": float(np.median([row["target_progress"] for row in episodes])) if episodes else None,
        "target_progress_p25": float(np.quantile([row["target_progress"] for row in episodes], 0.25)) if episodes else None,
        "minimum_mll_buffer_usd": min((row["minimum_mll_buffer_usd"] for row in episodes), default=None),
        "consistency_compliance_rate": sum(row["consistency_compliant"] for row in episodes) / len(episodes) if episodes else None,
        "median_days_to_target": float(np.median([row["days_to_target"] for row in passes])) if passes else None,
        "episode_hash": stable_hash(episodes),
    }


def _account_matrix(
    selected_events: Mapping[str, Sequence[Mapping[str, Any]]], sessions: pd.DataFrame,
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    calendar = [
        pd.Timestamp(day).date().isoformat()
        for day in sessions.loc[sessions["role"].eq("VALIDATION"), "session_day"].tolist()
    ]
    output: list[dict[str, Any]] = []
    for policy_id, events in selected_events.items():
        held = [row for row in events if row["role"] == "VALIDATION"]
        for quantity in manifest["account_contract"]["quantity_tiers"]:
            for horizon in manifest["account_contract"]["horizons_trading_days"]:
                output.append(
                    {
                        "policy_id": policy_id,
                        "account_label": "50K",
                        "quantity": int(quantity),
                        "horizon_trading_days": int(horizon),
                        "NORMAL": _account_summary(held, calendar, int(quantity), int(horizon), scenario="NORMAL"),
                        "STRESSED_1_5X": _account_summary(held, calendar, int(quantity), int(horizon), scenario="STRESSED_1_5X"),
                    }
                )
    return output


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    events, reconstruction = _load_events(audit["source"])
    contracts = _load_contract_spec(audit["source"])
    sessions = _paired_sessions(events, manifest)
    evaluated: list[dict[str, Any]] = []
    event_sets: dict[str, list[dict[str, Any]]] = {}
    for spec in frozen_specs(manifest):
        row, simulated = _evaluate(sessions, spec, events, contracts, manifest)
        evaluated.append(row)
        event_sets[row["policy_id"]] = simulated

    gate = manifest["selection_gate"]
    eligible = [
        row
        for row in evaluated
        if int(row["roles"]["DISCOVERY"]["event_count"]) >= int(gate["minimum_discovery_events"])
        and float(row["roles"]["DISCOVERY"]["stressed_net_usd"]) > 0.0
    ]
    eligible.sort(key=_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    mechanisms: set[str] = set()
    for row in eligible:
        mechanism = str(row["spec"]["mechanism"])
        if mechanism in mechanisms:
            continue
        mechanisms.add(mechanism)
        selected.append(row)
        if len(selected) >= int(gate["maximum_selected_specs"]):
            break

    selected_results: list[dict[str, Any]] = []
    passers: list[str] = []
    passing_events: dict[str, Sequence[Mapping[str, Any]]] = {}
    specs = {_policy_id(spec, manifest): spec for spec in frozen_specs(manifest)}
    for primary in selected:
        policy_id = str(primary["policy_id"])
        controls: dict[str, Mapping[str, Any]] = {}
        for control in CONTROLS[1:]:
            control_row, _ = _evaluate(
                sessions, specs[policy_id], events, contracts, manifest, control=control
            )
            controls[control] = control_row
        gate_result = _validation_gate(primary, controls, manifest)
        if gate_result["passed"]:
            passers.append(policy_id)
            passing_events[policy_id] = event_sets[policy_id]
        selected_results.append(
            {
                "policy_id": policy_id,
                "primary": primary,
                "controls": controls,
                "validation_gate": gate_result,
            }
        )

    account_matrix = _account_matrix(passing_events, sessions, manifest) if passers else []
    exact_pass_count = sum(
        int(cell[scenario]["pass_count"])
        for cell in account_matrix
        for scenario in ("NORMAL", "STRESSED_1_5X")
    )
    if not passers:
        status = "CME_CRYPTO_SESSION_REOPEN_TRANSFER_FALSIFIED"
        next_action = "TOMBSTONE_EXACT_REOPEN_SPEC_AND_REQUIRE_NEW_EXTERNAL_INFORMATION_REPRESENTATION"
        tier = "H_DIAGNOSTIC"
    elif exact_pass_count == 0:
        status = "CME_CRYPTO_SESSION_REOPEN_EVENT_ALPHA_ACCOUNT_VELOCITY_WEAK"
        next_action = "PRESERVE_EVENT_ALPHA_WITHOUT_PROMOTION_AND_REALLOCATE_EXPLORATION"
        tier = "E"
    else:
        status = "CME_CRYPTO_SESSION_REOPEN_TIER_E_ACCOUNT_SIGNAL"
        next_action = "FREEZE_ACCOUNT_SIGNAL_FOR_MATERIALLY_DISTINCT_CHRONOLOGICAL_CONFIRMATION"
        tier = "E"
    result: dict[str, Any] = {
        "schema": "hydra_cme_crypto_session_reopen_inventory_transfer_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "evidence_role": manifest["governance"]["evidence_role"],
        "tier_ceiling": tier,
        "decision_card": manifest["decision_card"],
        "manifest_hash": manifest["manifest_hash"],
        "source_audit_hash": audit["audit_hash"],
        "source_receipt_hash": audit["source"]["receipt"]["receipt_hash"],
        "incremental_data_spend_usd": 0.0,
        "data_reconstruction": reconstruction,
        "paired_session_count": len(sessions),
        "paired_sessions_by_role": {role: int(sessions["role"].eq(role).sum()) for role in ROLES},
        "contract_specs": contracts,
        "proposal_count": len(evaluated),
        "discovery_eligible_count": len(eligible),
        "selected_candidate_ids": [row["policy_id"] for row in selected],
        "validation_event_gate_passer_ids": passers,
        "all_candidate_results": evaluated,
        "selected_results": selected_results,
        "best_diagnostics_by_discovery": sorted(evaluated, key=_rank, reverse=True)[:4],
        "account_replay_executed": bool(passers),
        "account_matrix": account_matrix,
        "exact_account_cell_count": len(account_matrix),
        "exact_normal_and_stressed_pass_count": exact_pass_count,
        "runtime_seconds": time.perf_counter() - started,
        "next_action": next_action,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "xfa_paths_started": 0,
    }
    result["result_hash"] = stable_hash(
        {key: value for key, value in result.items() if key != "runtime_seconds"}
    )
    del events
    gc.collect()
    return result


__all__ = [
    "CryptoReopenTripwireError",
    "PolicySpec",
    "audit_inputs",
    "frozen_specs",
    "run_tripwire",
]
