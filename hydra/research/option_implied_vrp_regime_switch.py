from __future__ import annotations

"""Bounded option-implied VRP action-switch tripwire.

The only trainable object is frozen from Discovery outcomes before held-out
outcomes are materialised.  The module has no acquisition, Combine, XFA,
broker, order, Q4, registry, or mission-database path.
"""

import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from hydra.economic_evolution.schema import stable_hash
from hydra.research.option_settlement_surface_tripwire import (
    CHICAGO,
    MARKET_SPEC,
    OptionSettlementTripwireError,
    _canonical_bytes,
    _inside,
    _rth,
    _sha256_file,
    _write_immutable_json,
    load_futures_bars,
    load_surfaces,
)


MANIFEST = Path("config/research/cross_asset_option_implied_vrp_regime_switch_v1.json")
ROLE_ORDER = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
ACTIONS = ("ABSTAIN", "BREAKOUT_OCO", "FADE_EXTENSION")
FEATURES = (
    "ATM_STRADDLE_VOL_PROXY",
    "LOG_IMPLIED_TO_REALIZED_RATIO",
    "FRONT_NEXT_TERM_SLOPE",
    "DOWNSIDE_UPSIDE_WING_PREMIUM_SKEW",
    "ES_NQ_LOG_VRP_DIFFERENCE",
)


class VrpRegimeSwitchError(OptionSettlementTripwireError):
    pass


def read_and_audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest_path = _inside(project, MANIFEST)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise VrpRegimeSwitchError("manifest hash drift")

    decision = manifest["decision_card"]
    decision_path = _inside(project, decision["path"])
    if _sha256_file(decision_path) != decision["file_sha256"]:
        raise VrpRegimeSwitchError("decision card file drift")
    decision_card = json.loads(decision_path.read_text(encoding="utf-8"))
    decision_core = dict(decision_card)
    decision_hash = str(decision_core.pop("decision_card_hash", ""))
    if stable_hash(decision_core) != decision_hash or decision_hash != decision["decision_card_hash"]:
        raise VrpRegimeSwitchError("decision card hash drift")

    frozen = manifest["frozen_inputs"]
    paths = {
        "statistics": _inside(project, frozen["statistics_path"]),
        "definitions": _inside(project, frozen["definition_path"]),
        "ohlcv": _inside(project, frozen["underlying_ohlcv_path"]),
        "receipt": _inside(project, frozen["acquisition_receipt_path"]),
        "rules": _inside(project, manifest["official_rule_evidence"]["snapshot_path"]),
    }
    expected = {
        "statistics": frozen["statistics_sha256"],
        "definitions": frozen["definition_sha256"],
        "ohlcv": frozen["underlying_ohlcv_sha256"],
        "receipt": frozen["acquisition_receipt_file_sha256"],
        "rules": manifest["official_rule_evidence"]["snapshot_file_sha256"],
    }
    hashes = {key: _sha256_file(path) for key, path in paths.items()}
    if hashes != expected:
        raise VrpRegimeSwitchError("frozen input hash drift")
    receipt = json.loads(paths["receipt"].read_text(encoding="utf-8"))
    receipt_core = dict(receipt)
    receipt_hash = str(receipt_core.pop("receipt_hash", ""))
    if stable_hash(receipt_core) != receipt_hash or receipt_hash != frozen["acquisition_receipt_hash"]:
        raise VrpRegimeSwitchError("acquisition receipt drift")
    if any(receipt.get(field) != 0 for field in ("q4_access_count_delta", "broker_connections", "orders")):
        raise VrpRegimeSwitchError("forbidden acquisition state")
    rules = json.loads(paths["rules"].read_text(encoding="utf-8"))
    if rules.get("parsed_rule_hash") != manifest["official_rule_evidence"]["parsed_rule_hash"]:
        raise VrpRegimeSwitchError("official rule snapshot drift")
    if frozen["end_exclusive"] != "2024-10-01" or not manifest["governance"]["no_q4_access"]:
        raise VrpRegimeSwitchError("Q4 boundary drift")
    if manifest["evidence_role"]["maximum_tier"] != "E_DIAGNOSTIC":
        raise VrpRegimeSwitchError("evidence ceiling drift")
    return {
        "root": project,
        "manifest": manifest,
        "paths": paths,
        "hashes": hashes,
        "audit_hash": stable_hash(
            {"manifest_hash": claimed, "decision_card_hash": decision_hash, "input_hashes": hashes}
        ),
    }


def _realized_session_vol(rows: pd.DataFrame) -> float | None:
    rth = _rth(rows)
    if len(rth) < 30:
        return None
    prices = rth["close"].to_numpy(dtype=float)
    returns = np.diff(np.log(prices))
    value = float(np.sqrt(np.square(returns).sum()))
    return value if math.isfinite(value) and value > 0 else None


def _range_state(rows: pd.DataFrame, family: str, manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    spec = manifest["opportunity_contract"]["families"][family]
    rth = _rth(rows)
    selected = rth.loc[
        rth["local_minute"].ge(spec["range_start_chicago"])
        & rth["local_minute"].lt(spec["range_end_exclusive_chicago"])
    ]
    expected = int(
        (
            pd.Timestamp(f"2000-01-01 {spec['range_end_exclusive_chicago']}")
            - pd.Timestamp(f"2000-01-01 {spec['range_start_chicago']}")
        ).total_seconds()
        // 60
    )
    if len(selected) != expected:
        return None
    local_day = selected.iloc[0]["local_timestamp"].date().isoformat()
    decision = pd.Timestamp(f"{local_day} {spec['decision_chicago']}", tz=CHICAGO).tz_convert("UTC")
    return {
        "decision_time": decision,
        "range_high": float(selected["high"].max()),
        "range_low": float(selected["low"].min()),
        "range_bar_count": len(selected),
    }


def build_opportunities(
    audit: Mapping[str, Any],
    surfaces: Mapping[tuple[str, str], Any],
    bars: Mapping[tuple[str, str], pd.DataFrame],
) -> list[dict[str, Any]]:
    manifest = audit["manifest"]
    sessions = [day for role in manifest["chronological_roles"] for day in role["sessions"]]
    role_by_day = {day: role["role"] for role in manifest["chronological_roles"] for day in role["sessions"]}
    trailing_count = int(manifest["vrp_feature_contract"]["trailing_realized_session_count"])
    families = tuple(manifest["opportunity_contract"]["families"])
    output: list[dict[str, Any]] = []

    session_vol: dict[tuple[str, str], float] = {}
    for (market, day), rows in bars.items():
        value = _realized_session_vol(rows)
        if value is not None:
            session_vol[(market, day)] = value

    for index, day in enumerate(sessions[1:], start=1):
        prior_listed = sessions[index - 1]
        prior_days_by_market: dict[str, list[str]] = {}
        log_ratio_by_market: dict[str, float] = {}
        for market in MARKET_SPEC:
            prior_days = [candidate for candidate in sessions[:index] if (market, candidate) in session_vol]
            prior_days_by_market[market] = prior_days[-trailing_count:]
            snapshot = surfaces.get((prior_listed, market))
            if snapshot is None or len(prior_days_by_market[market]) != trailing_count:
                continue
            front = snapshot.front_term
            if front is None:
                continue
            trailing_realized = float(np.mean([session_vol[(market, prior)] for prior in prior_days_by_market[market]]))
            implied = float(front.atm_straddle_vol_proxy)
            if implied <= 0 or trailing_realized <= 0:
                continue
            log_ratio_by_market[market] = math.log(implied / trailing_realized)

        if set(log_ratio_by_market) != set(MARKET_SPEC):
            continue
        cross_difference = log_ratio_by_market["ES"] - log_ratio_by_market["NQ"]
        for market in MARKET_SPEC:
            rows = bars.get((market, day))
            snapshot = surfaces.get((prior_listed, market))
            if rows is None or snapshot is None or snapshot.front_term is None:
                continue
            for family in families:
                range_state = _range_state(rows, family, manifest)
                if range_state is None or pd.Timestamp(snapshot.available_at) > range_state["decision_time"]:
                    continue
                front = snapshot.front_term
                features = {
                    "ATM_STRADDLE_VOL_PROXY": float(front.atm_straddle_vol_proxy),
                    "LOG_IMPLIED_TO_REALIZED_RATIO": log_ratio_by_market[market],
                    "FRONT_NEXT_TERM_SLOPE": float(snapshot.front_next_term_slope),
                    "DOWNSIDE_UPSIDE_WING_PREMIUM_SKEW": float(front.downside_upside_wing_premium_skew),
                    "ES_NQ_LOG_VRP_DIFFERENCE": cross_difference,
                }
                if not all(math.isfinite(value) for value in features.values()):
                    raise VrpRegimeSwitchError("non-finite decision feature")
                core = {
                    "session": day,
                    "role": role_by_day[day],
                    "prior_listed_session": prior_listed,
                    "trailing_realized_sessions": prior_days_by_market[market],
                    "market": market,
                    "family": family,
                    "decision_time": range_state["decision_time"].isoformat(),
                    "range_high": range_state["range_high"],
                    "range_low": range_state["range_low"],
                    "range_bar_count": range_state["range_bar_count"],
                    "surface_available_at": snapshot.available_at,
                    "surface_snapshot_hash": snapshot.snapshot_hash,
                    "features": features,
                }
                identity = {key: core[key] for key in ("session", "market", "family", "decision_time")}
                core["opportunity_id"] = "vrp_switch_" + stable_hash(identity)[:20]
                core["input_fingerprint"] = stable_hash({**identity, "features": features})
                output.append(core)
    return sorted(output, key=lambda row: (row["decision_time"], row["market"], row["family"]))


def simulate_action(
    opportunity: Mapping[str, Any],
    rows: pd.DataFrame,
    manifest: Mapping[str, Any],
    *,
    action: str,
    scenario: str,
) -> dict[str, Any]:
    if action not in ACTIONS:
        raise VrpRegimeSwitchError(f"unknown action: {action}")
    if action == "ABSTAIN":
        return _zero_outcome(action, "EXECUTABLE_ABSTAIN")
    market = str(opportunity["market"])
    spec = manifest["execution_contract"]["market_parameters"][market]
    tick = float(spec["tick_size"])
    decision = pd.Timestamp(opportunity["decision_time"])
    flatten = pd.Timestamp(
        f"{opportunity['session']} {manifest['execution_contract']['mandatory_flatten_chicago']}",
        tz=CHICAGO,
    ).tz_convert("UTC")
    trigger_deadline = decision + pd.Timedelta(
        minutes=int(manifest["opportunity_contract"]["entry_window_minutes"])
    )
    high_trigger = float(opportunity["range_high"]) + int(manifest["opportunity_contract"]["trigger_offset_ticks"]) * tick
    low_trigger = float(opportunity["range_low"]) - int(manifest["opportunity_contract"]["trigger_offset_ticks"]) * tick
    candidates = rows.loc[
        rows["timestamp"].gt(decision)
        & rows["timestamp"].lt(trigger_deadline)
        & rows["timestamp"].lt(flatten)
    ].sort_values("timestamp", kind="mergesort")
    trigger_row = None
    extension_direction = 0
    for row in candidates.itertuples(index=False):
        high_hit = float(row.high) >= high_trigger
        low_hit = float(row.low) <= low_trigger
        if high_hit and low_hit:
            return _zero_outcome(action, "EXECUTABLE_ABSTAIN_AMBIGUOUS")
        if high_hit or low_hit:
            trigger_row = row
            extension_direction = 1 if high_hit else -1
            break
    if trigger_row is None:
        return _zero_outcome(action, "EXECUTABLE_NO_TRIGGER")

    trigger_time = pd.Timestamp(trigger_row.timestamp)
    entry_candidates = rows.loc[rows["timestamp"].gt(trigger_time) & rows["timestamp"].lt(flatten)].sort_values(
        "timestamp", kind="mergesort"
    )
    if entry_candidates.empty:
        return _zero_outcome(action, "DATA_CENSORED_NO_NEXT_OPEN")
    entry_row = entry_candidates.iloc[0]
    entry_time = pd.Timestamp(entry_row["timestamp"])
    direction = extension_direction if action == "BREAKOUT_OCO" else -extension_direction
    stress_ticks = (
        int(manifest["execution_contract"]["stressed_extra_slippage_ticks_per_side"])
        if scenario == "STRESSED"
        else int(manifest["execution_contract"]["normal_extra_slippage_ticks_per_side"])
    )
    entry = float(entry_row["open"]) + direction * stress_ticks * tick
    stop = entry - direction * int(spec["stop_ticks"]) * tick
    target = entry + direction * int(spec["target_ticks"]) * tick
    hold_deadline = entry_time + pd.Timedelta(minutes=int(manifest["execution_contract"]["maximum_holding_minutes"]))
    path = rows.loc[
        rows["timestamp"].ge(entry_time)
        & rows["timestamp"].lt(hold_deadline)
        & rows["timestamp"].lt(flatten)
    ].sort_values("timestamp", kind="mergesort")
    time_exit = rows.loc[
        rows["timestamp"].ge(hold_deadline) & rows["timestamp"].lt(flatten)
    ].sort_values("timestamp", kind="mergesort")
    if path.empty:
        return _zero_outcome(action, "DATA_CENSORED_EXIT")
    instrument = str(entry_row["instrument_id"])
    if path["instrument_id"].astype(str).ne(instrument).any():
        return _zero_outcome(action, "HARD_FAILURE_INTRATRADE_ROLL")

    exit_price: float | None = None
    exit_time: pd.Timestamp | None = None
    exit_reason = "TIME"
    same_bar = False
    adverse_prices: list[float] = []
    for row in path.itertuples(index=False):
        stop_hit = float(row.low) <= stop if direction > 0 else float(row.high) >= stop
        target_hit = float(row.high) >= target if direction > 0 else float(row.low) <= target
        if stop_hit or target_hit:
            same_bar = stop_hit and target_hit
            if stop_hit:
                exit_reason = "STOP_FIRST"
                raw_exit = min(stop, float(row.open)) if direction > 0 else max(stop, float(row.open))
            else:
                exit_reason = "TARGET"
                raw_exit = target
            exit_price = raw_exit - direction * stress_ticks * tick
            exit_time = pd.Timestamp(row.timestamp)
            adverse_prices.append(exit_price if stop_hit else (float(row.low) if direction > 0 else float(row.high)))
            break
        adverse_prices.append(float(row.low) if direction > 0 else float(row.high))
    if exit_price is None:
        if time_exit.empty:
            return _zero_outcome(action, "DATA_CENSORED_EXIT")
        exit_row = time_exit.iloc[0]
        if str(exit_row["instrument_id"]) != instrument:
            return _zero_outcome(action, "HARD_FAILURE_INTRATRADE_ROLL")
        exit_price = float(exit_row["open"]) - direction * stress_ticks * tick
        exit_time = pd.Timestamp(exit_row["timestamp"])
    fee = float(spec["normal_round_turn_fee_usd"])
    point_value = float(spec["point_value_usd"])
    gross = direction * (exit_price - entry) * point_value
    net = gross - fee
    adverse_price = min(adverse_prices) if direction > 0 else max(adverse_prices)
    adverse_pnl = direction * (adverse_price - entry) * point_value - fee
    buffer = 2000.0 + min(0.0, adverse_pnl)
    return {
        "status": "EXECUTABLE_COMPLETE",
        "action": action,
        "scenario": scenario,
        "traded": True,
        "extension_direction": extension_direction,
        "direction": direction,
        "trigger_time": trigger_time.isoformat(),
        "entry_time": entry_time.isoformat(),
        "exit_time": exit_time.isoformat(),
        "entry_price": entry,
        "exit_price": exit_price,
        "stop_price": stop,
        "target_price": target,
        "exit_reason": exit_reason,
        "same_bar_stop_first": same_bar,
        "gross_pnl": gross,
        "net_pnl": net,
        "minimum_mll_buffer_50k": buffer,
        "mll_breach_50k": buffer <= 0.0,
    }


def _zero_outcome(action: str, status: str) -> dict[str, Any]:
    return {
        "status": status,
        "action": action,
        "traded": False,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "minimum_mll_buffer_50k": 2000.0,
        "mll_breach_50k": False,
        "exit_reason": "NO_TRADE",
    }


def materialize_outcomes(
    opportunities: Sequence[Mapping[str, Any]],
    bars: Mapping[tuple[str, str], pd.DataFrame],
    manifest: Mapping[str, Any],
    *,
    roles: frozenset[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for opportunity in opportunities:
        if opportunity["role"] not in roles:
            continue
        rows = bars[(str(opportunity["market"]), str(opportunity["session"]))]
        output[str(opportunity["opportunity_id"])] = {
            scenario: {
                action: simulate_action(opportunity, rows, manifest, action=action, scenario=scenario)
                for action in ACTIONS
            }
            for scenario in ("NORMAL", "STRESSED")
        }
    return output


@dataclass(frozen=True, slots=True)
class FrozenActionModel:
    features: tuple[str, ...]
    mean: tuple[float, ...]
    scale: tuple[float, ...]
    classes: tuple[str, ...]
    coefficient: tuple[tuple[float, ...], ...]
    intercept: tuple[float, ...]
    training_rows: int
    class_counts: tuple[tuple[str, int], ...]
    model_kind: str

    def predict(self, values: Mapping[str, float]) -> str:
        if len(self.classes) == 1:
            return self.classes[0]
        x = np.asarray([float(values[name]) for name in self.features], dtype=float)
        z = (x - np.asarray(self.mean)) / np.asarray(self.scale)
        coef = np.asarray(self.coefficient)
        intercept = np.asarray(self.intercept)
        if len(self.classes) == 2 and coef.shape[0] == 1:
            score = float(np.dot(coef[0], z) + intercept[0])
            return self.classes[1] if score > 0 else self.classes[0]
        scores = coef @ z + intercept
        return self.classes[int(np.argmax(scores))]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def fit_action_model(
    rows: Sequence[Mapping[str, float]],
    labels: Sequence[str],
    manifest: Mapping[str, Any],
) -> FrozenActionModel:
    x = np.asarray([[float(row[name]) for name in FEATURES] for row in rows], dtype=float)
    y = np.asarray(labels, dtype=str)
    if len(x) == 0 or not np.isfinite(x).all():
        raise VrpRegimeSwitchError("empty or non-finite model matrix")
    mean = x.mean(axis=0)
    scale = x.std(axis=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    z = (x - mean) / scale
    classes, counts = np.unique(y, return_counts=True)
    if len(classes) == 1:
        coefficient = np.zeros((1, x.shape[1]), dtype=float)
        intercept = np.zeros(1, dtype=float)
        kind = "CONSTANT_ACTION"
    else:
        contract = manifest["teacher_policy_contract"]
        model = LogisticRegression(
            C=float(contract["regularization_c"]),
            solver="lbfgs",
            random_state=int(contract["random_state"]),
            max_iter=int(contract["maximum_iterations"]),
        ).fit(z, y)
        classes = model.classes_
        coefficient = model.coef_
        intercept = model.intercept_
        kind = "L2_LOGISTIC_ACTION_CLASSIFIER"
    count_map = {str(label): int(count) for label, count in zip(*np.unique(y, return_counts=True))}
    return FrozenActionModel(
        features=FEATURES,
        mean=tuple(float(value) for value in mean),
        scale=tuple(float(value) for value in scale),
        classes=tuple(str(value) for value in classes),
        coefficient=tuple(tuple(float(value) for value in row) for row in coefficient),
        intercept=tuple(float(value) for value in intercept),
        training_rows=len(y),
        class_counts=tuple((action, count_map.get(action, 0)) for action in ACTIONS),
        model_kind=kind,
    )


def train_and_freeze_policy(
    opportunities: Sequence[Mapping[str, Any]],
    discovery: Mapping[str, Mapping[str, Mapping[str, Any]]],
    manifest: Mapping[str, Any],
) -> tuple[FrozenActionModel, dict[str, str], str, dict[str, Any]]:
    rows = [row for row in opportunities if row["role"] == "DISCOVERY"]
    labels: list[str] = []
    for row in rows:
        action_values = discovery[row["opportunity_id"]]["STRESSED"]
        labels.append(max(ACTIONS, key=lambda action: (float(action_values[action]["net_pnl"]), -ACTIONS.index(action))))
    model = fit_action_model([row["features"] for row in rows], labels, manifest)
    predictions = {row["opportunity_id"]: model.predict(row["features"]) for row in opportunities}
    static_totals = {
        action: sum(float(discovery[row["opportunity_id"]]["STRESSED"][action]["net_pnl"]) for row in rows)
        for action in ACTIONS
    }
    best_static = max(ACTIONS, key=lambda action: (static_totals[action], -ACTIONS.index(action)))
    core = {
        "schema": "hydra_option_implied_vrp_action_policy_freeze_v1",
        "manifest_hash": manifest["manifest_hash"],
        "training_role": "DISCOVERY_ONLY",
        "discovery_opportunity_count": len(rows),
        "discovery_outcome_hash": stable_hash(discovery),
        "model": model.to_dict(),
        "best_static_action": best_static,
        "static_discovery_stressed_net_usd": static_totals,
        "prediction_hash_all_roles_inputs_only": stable_hash(predictions),
        "validation_or_final_outcome_opened_before_freeze": False,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count": 0,
    }
    return model, predictions, best_static, {**core, "policy_freeze_hash": stable_hash(core)}


def _flip(action: str) -> str:
    return {
        "ABSTAIN": "ABSTAIN",
        "BREAKOUT_OCO": "FADE_EXTENSION",
        "FADE_EXTENSION": "BREAKOUT_OCO",
    }[action]


def _random_actions(
    opportunities: Sequence[Mapping[str, Any]], predictions: Mapping[str, str]
) -> dict[str, str]:
    output: dict[str, str] = {}
    for role in ROLE_ORDER:
        rows = [row for row in opportunities if row["role"] == role]
        counts = {action: sum(predictions[row["opportunity_id"]] == action for row in rows) for action in ACTIONS}
        ranked = sorted(rows, key=lambda row: stable_hash({"seed": 73032, "id": row["opportunity_id"]}))
        offset = 0
        for action in ACTIONS:
            for row in ranked[offset : offset + counts[action]]:
                output[row["opportunity_id"]] = action
            offset += counts[action]
    return output


def _policy_summary(
    opportunities: Sequence[Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Mapping[str, Any]]],
    actions: Mapping[str, str],
    *,
    scenario: str,
) -> dict[str, Any]:
    values: list[float] = []
    gross: list[float] = []
    traded_rows: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    hard_failures = 0
    status_counts: dict[str, int] = {}
    action_counts = {action: 0 for action in ACTIONS}
    for row in opportunities:
        action = actions[row["opportunity_id"]]
        action_counts[action] += 1
        result = outcomes[row["opportunity_id"]][scenario][action]
        status_counts[result["status"]] = status_counts.get(result["status"], 0) + 1
        hard_failures += result["status"].startswith("HARD_FAILURE")
        values.append(float(result["net_pnl"]))
        gross.append(float(result["gross_pnl"]))
        if result["traded"]:
            traded_rows.append((row, result))
    positive_trades = [max(float(result["net_pnl"]), 0.0) for _, result in traded_rows]
    positive_trade_total = sum(positive_trades)
    daily: dict[str, float] = {}
    by_market: dict[str, float] = {}
    by_family: dict[str, float] = {}
    for row, result in traded_rows:
        value = float(result["net_pnl"])
        daily[row["session"]] = daily.get(row["session"], 0.0) + value
        by_market[row["market"]] = by_market.get(row["market"], 0.0) + value
        by_family[row["family"]] = by_family.get(row["family"], 0.0) + value
    positive_days = [max(value, 0.0) for value in daily.values()]
    day_total = sum(positive_days)
    return {
        "opportunity_count": len(opportunities),
        "trade_count": len(traded_rows),
        "trade_coverage": len(traded_rows) / len(opportunities) if opportunities else 0.0,
        "action_counts": action_counts,
        "status_counts": dict(sorted(status_counts.items())),
        "gross_pnl_usd": sum(gross),
        "net_pnl_usd": sum(values),
        "mean_net_per_opportunity_usd": sum(values) / len(opportunities) if opportunities else 0.0,
        "target_count": sum(result["exit_reason"] == "TARGET" for _, result in traded_rows),
        "stop_count": sum(result["exit_reason"] == "STOP_FIRST" for _, result in traded_rows),
        "time_count": sum(result["exit_reason"] == "TIME" for _, result in traded_rows),
        "minimum_mll_buffer_50k_usd": min(
            (float(result["minimum_mll_buffer_50k"]) for _, result in traded_rows), default=2000.0
        ),
        "mll_breach_count_50k": sum(bool(result["mll_breach_50k"]) for _, result in traded_rows),
        "hard_failure_count": hard_failures,
        "maximum_single_trade_positive_profit_share": (
            max(positive_trades, default=0.0) / positive_trade_total if positive_trade_total > 0 else 0.0
        ),
        "maximum_single_day_positive_profit_share": (
            max(positive_days, default=0.0) / day_total if day_total > 0 else 0.0
        ),
        "net_by_market_usd": dict(sorted(by_market.items())),
        "net_by_family_usd": dict(sorted(by_family.items())),
    }


def evaluate(
    opportunities: Sequence[Mapping[str, Any]],
    outcomes: Mapping[str, Mapping[str, Mapping[str, Any]]],
    predictions: Mapping[str, str],
    best_static: str,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    random = _random_actions(opportunities, predictions)
    policies = {
        "TEACHER_VRP_SWITCH": dict(predictions),
        "BEST_STATIC_DISCOVERY": {row["opportunity_id"]: best_static for row in opportunities},
        "STATIC_BREAKOUT_OCO": {row["opportunity_id"]: "BREAKOUT_OCO" for row in opportunities},
        "STATIC_FADE_EXTENSION": {row["opportunity_id"]: "FADE_EXTENSION" for row in opportunities},
        "STATIC_ABSTAIN": {row["opportunity_id"]: "ABSTAIN" for row in opportunities},
        "ACTION_FLIP": {key: _flip(action) for key, action in predictions.items()},
        "RANDOM_ACTION": random,
    }
    results: dict[str, dict[str, dict[str, Any]]] = {}
    counts: dict[str, int] = {}
    for role in ROLE_ORDER:
        rows = [row for row in opportunities if row["role"] == role]
        counts[role] = len(rows)
        role_outcomes = {row["opportunity_id"]: outcomes[row["opportunity_id"]] for row in rows}
        results[role] = {}
        for scenario in ("NORMAL", "STRESSED"):
            results[role][scenario] = {
                name: _policy_summary(
                    rows,
                    role_outcomes,
                    {row["opportunity_id"]: actions[row["opportunity_id"]] for row in rows},
                    scenario=scenario,
                )
                for name, actions in policies.items()
            }
        teacher = results[role]["STRESSED"]["TEACHER_VRP_SWITCH"]
        teacher["paired_stressed_uplift_vs_best_static_usd"] = (
            teacher["net_pnl_usd"] - results[role]["STRESSED"]["BEST_STATIC_DISCOVERY"]["net_pnl_usd"]
        )
        teacher["delta_vs_action_flip_usd"] = (
            teacher["net_pnl_usd"] - results[role]["STRESSED"]["ACTION_FLIP"]["net_pnl_usd"]
        )
        teacher["delta_vs_random_action_usd"] = (
            teacher["net_pnl_usd"] - results[role]["STRESSED"]["RANDOM_ACTION"]["net_pnl_usd"]
        )

    gate = manifest["selection_gate"]
    coverage_ok = (
        len(opportunities) >= gate["minimum_total_independent_opportunities"]
        and counts["VALIDATION"] >= gate["minimum_validation_independent_opportunities"]
        and counts["FINAL_DEVELOPMENT"] >= gate["minimum_final_development_independent_opportunities"]
    )
    held_checks: dict[str, dict[str, bool]] = {}
    for role, prefix in (("VALIDATION", "validation"), ("FINAL_DEVELOPMENT", "final_development")):
        row = results[role]["STRESSED"]["TEACHER_VRP_SWITCH"]
        held_checks[role] = {
            "minimum_trades": row["trade_count"] >= gate[f"minimum_{prefix}_trade_count"],
            "minimum_trade_coverage": row["trade_coverage"] >= gate["minimum_trade_coverage_fraction"],
            "positive_stressed_net": row["net_pnl_usd"] > gate[f"{prefix}_stressed_net_minimum_exclusive_usd"],
            "positive_uplift_vs_best_static": row["paired_stressed_uplift_vs_best_static_usd"]
            > gate[f"{prefix}_paired_stressed_uplift_vs_best_static_minimum_exclusive_usd"],
            "beats_action_flip": row["delta_vs_action_flip_usd"]
            > gate[f"{prefix}_delta_vs_action_flip_minimum_exclusive_usd"],
            "beats_random_action": row["delta_vs_random_action_usd"]
            > gate[f"{prefix}_delta_vs_random_action_minimum_exclusive_usd"],
            "no_mll_breach": row["mll_breach_count_50k"] == 0,
            "no_hard_failure": row["hard_failure_count"] == 0,
            "day_concentration": row["maximum_single_day_positive_profit_share"]
            <= gate["maximum_single_day_positive_profit_share"],
        }
    gate_pass = coverage_ok and all(all(checks.values()) for checks in held_checks.values())
    if not coverage_ok:
        status = gate["coverage_failure_status"]
    elif gate_pass:
        status = gate["success_status"]
    else:
        status = gate["failure_status"]
    return {
        "status": status,
        "evidence_tier": "E_DIAGNOSTIC" if gate_pass else "TERMINAL_TRIPWIRE",
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "opportunity_counts": counts,
        "best_static_action": best_static,
        "policy_results_by_role": results,
        "gate": {"coverage_passed": coverage_ok, "held_checks": held_checks, "passed": gate_pass},
        "combine_replay_count": 0,
        "xfa_path_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count": 0,
    }


def run_tripwire(root: str | Path, output_dir: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    audit = read_and_audit_inputs(root)
    output = Path(output_dir).resolve()
    surfaces, surface_audit = load_surfaces(audit)
    if surface_audit["snapshot_hash"] != audit["manifest"]["frozen_inputs"]["predecessor_surface_snapshot_hash"]:
        raise VrpRegimeSwitchError("surface reconstruction drift")
    bars, bar_audit = load_futures_bars(audit)
    opportunities = build_opportunities(audit, surfaces, bars)

    discovery = materialize_outcomes(
        opportunities, bars, audit["manifest"], roles=frozenset({"DISCOVERY"})
    )
    model, predictions, best_static, policy_freeze = train_and_freeze_policy(
        opportunities, discovery, audit["manifest"]
    )
    freeze_path = output / "policy_freeze.json"
    freeze_file_sha = _write_immutable_json(freeze_path, policy_freeze)
    if _sha256_file(freeze_path) != freeze_file_sha:
        raise VrpRegimeSwitchError("policy freeze durability failure")

    heldout = materialize_outcomes(
        opportunities,
        bars,
        audit["manifest"],
        roles=frozenset({"VALIDATION", "FINAL_DEVELOPMENT"}),
    )
    outcomes = {**discovery, **heldout}
    if set(outcomes) != {row["opportunity_id"] for row in opportunities}:
        raise VrpRegimeSwitchError("outcome ledger incomplete")
    evaluation = evaluate(opportunities, outcomes, predictions, best_static, audit["manifest"])
    evidence_rows = [
        {
            "opportunity": row,
            "selected_action": predictions[row["opportunity_id"]],
            "outcomes": outcomes[row["opportunity_id"]],
        }
        for row in opportunities
    ]
    evidence_path = output / "opportunity_evidence.jsonl"
    evidence_raw = b"".join(_canonical_bytes(row) + b"\n" for row in evidence_rows)
    if evidence_path.exists() and evidence_path.read_bytes() != evidence_raw:
        raise VrpRegimeSwitchError("immutable opportunity evidence drift")
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    if not evidence_path.exists():
        evidence_path.write_bytes(evidence_raw)
    evidence_sha = hashlib.sha256(evidence_raw).hexdigest()
    result_core = {
        "schema": "hydra_cross_asset_option_implied_vrp_regime_switch_result_v1",
        "branch_id": audit["manifest"]["branch_id"],
        "manifest_hash": audit["manifest"]["manifest_hash"],
        "input_audit_hash": audit["audit_hash"],
        "surface_audit": surface_audit,
        "bar_audit": bar_audit,
        "policy_freeze_path": freeze_path.name,
        "policy_freeze_file_sha256": freeze_file_sha,
        "policy_freeze_hash": policy_freeze["policy_freeze_hash"],
        "model": model.to_dict(),
        "opportunity_evidence_path": evidence_path.name,
        "opportunity_evidence_sha256": evidence_sha,
        "evaluation": evaluation,
        "runtime_seconds": time.perf_counter() - started,
        "incremental_data_spend_usd": 0.0,
        "evidence_ceiling": "TIER_E_DIAGNOSTIC_VIEWED_DEVELOPMENT_ONLY",
        "frozen_boundaries": {
            "tier_q_allowed": False,
            "combine_allowed": False,
            "xfa_allowed": False,
            "neighbor_retry_allowed": False,
        },
    }
    result = {**result_core, "result_hash": stable_hash(result_core)}
    result_path = output / "result.json"
    result_file_sha = _write_immutable_json(result_path, result)
    return {**result, "result_path": str(result_path), "result_file_sha256": result_file_sha}
