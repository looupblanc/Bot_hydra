"""Causal executable RB/HO/CL crack-spread state tripwire.

The branch is intentionally bounded to 24 frozen cells.  It differs from the
terminal refinery-products pilot: every economic event owns and accounts for
all three futures legs.  Sparse OHLCV is never treated as an atomic spread
fill.  Each leg fills independently at its first observed bar open strictly
after the causal decision; an incomplete basket is liquidated and charged.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_replay as exact
from hydra.propfirm.combine_episode import TradePathEvent, run_combine_episode
from hydra.research.cl_front_second_term_structure_economic_runner import (
    _prior_robust_score,
    _session_fields,
    _true_session_guard_days,
)
from hydra.research.refinery_products_to_mcl_pilot import (
    _roll_guard_from_instrument_ids,
)


MANIFEST = Path("config/research/executable_hedged_crack_spread_state_v1.json")
REPORT_DIR = Path("reports/research_tripwires/executable_hedged_crack_spread_state_v1")
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
LEGS = ("RB", "HO", "CL")


class CrackSpreadTripwireError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PolicySpec:
    ratio_id: str
    mechanism: str
    lookback_minutes: int
    holding_minutes: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    if resolved != root and root not in resolved.parents:
        raise CrackSpreadTripwireError("frozen input escapes project root")
    if not resolved.is_file():
        raise CrackSpreadTripwireError(f"required frozen input missing: {resolved}")
    return resolved


def _manifest(root: Path) -> dict[str, Any]:
    value = _read_json(_inside(root, MANIFEST))
    core = dict(value)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise CrackSpreadTripwireError("manifest hash drift")
    if int(value["candidate_lattice"]["proposal_count"]) != 24:
        raise CrackSpreadTripwireError("frozen proposal cardinality drift")
    return value


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _manifest(project)
    files: dict[str, dict[str, Any]] = {}
    for key, row in sorted(manifest["frozen_inputs"].items()):
        path = _inside(project, row["path"])
        digest = sha256_file(path)
        if digest != str(row["sha256"]):
            raise CrackSpreadTripwireError(f"frozen input hash drift: {key}")
        files[key] = {
            "path": str(path),
            "sha256": digest,
            "size_bytes": path.stat().st_size,
        }
    receipt = _read_json(Path(files["source_receipt"]["path"]))
    if (
        receipt.get("download_status") != "DOWNLOADED"
        or int(receipt.get("q4_access_count_delta", -1)) != 0
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
    ):
        raise CrackSpreadTripwireError("source receipt governance drift")
    rules = _read_json(Path(files["rule_snapshot"]["path"]))
    special = rules.get("product_restrictions", {}).get("special_contract_caps", {}).get(
        "CL_QM_RB_HO", {}
    )
    expected_caps = manifest["account_gate"]["shared_CL_QM_RB_HO_mini_caps"]
    if {key: int(value) for key, value in special.items()} != {
        key: int(value) for key, value in expected_caps.items()
    }:
        raise CrackSpreadTripwireError("shared CL/RB/HO contract-cap drift")
    core = {
        "manifest_hash": manifest["manifest_hash"],
        "file_hashes": {key: row["sha256"] for key, row in sorted(files.items())},
        "rule_snapshot_hash": rules["parsed_rule_hash"],
        "shared_contract_caps": special,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {
        "manifest": manifest,
        "files": files,
        "rule_snapshot": rules,
        **core,
        "audit_hash": stable_hash(core),
    }


def frozen_specs(manifest: Mapping[str, Any]) -> tuple[PolicySpec, ...]:
    lattice = manifest["candidate_lattice"]
    specs = tuple(
        PolicySpec(str(ratio), str(mechanism), int(lookback), int(holding))
        for ratio in lattice["ratio_ids"]
        for mechanism in lattice["mechanisms"]
        for lookback in lattice["lookback_minutes"]
        for holding in lattice["holding_minutes"]
    )
    if len(specs) != 24 or len(set(specs)) != 24:
        raise CrackSpreadTripwireError("frozen policy lattice is not 24 unique cells")
    return specs


def _policy_id(spec: PolicySpec, manifest: Mapping[str, Any]) -> str:
    return "crack_" + stable_hash(
        {
            "spec": asdict(spec),
            "manifest_hash": manifest["manifest_hash"],
            "causal_execution": manifest["causal_execution"],
            "cost_model": manifest["cost_model"],
        }
    )[:20]


def _definition_symbols(path: Path) -> dict[str, str]:
    frame = (
        _import_databento()
        .DBNStore.from_file(str(path))
        .to_df(pretty_ts=True, map_symbols=True, price_type="float")
        .reset_index()
    )
    frame["ts_event"] = pd.to_datetime(frame["ts_event"], utc=True)
    frame = frame.sort_values(["instrument_id", "ts_event"], kind="mergesort")
    latest = frame.groupby("instrument_id", sort=False).tail(1)
    mapping = {
        str(int(row.instrument_id)): str(row.raw_symbol).strip().upper()
        for row in latest.itertuples(index=False)
        if str(row.asset).strip().upper() in {"RB", "HO"}
    }
    if not mapping:
        raise CrackSpreadTripwireError("RB/HO explicit definition map is empty")
    return mapping


def _assign_cl_contract(timestamp: pd.Series, roll_path: Path) -> pd.Series:
    roll_map = load_roll_map(roll_path)
    if not str(roll_map.map_type).startswith("EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY"):
        raise CrackSpreadTripwireError("CL roll map is not explicit Databento symbology")
    contracts = sorted(
        (row for row in roll_map.contracts if row.root == "CL"),
        key=lambda row: pd.Timestamp(row.active_start).value,
    )
    starts = np.asarray([pd.Timestamp(row.active_start).value for row in contracts], dtype=np.int64)
    ends = np.asarray([pd.Timestamp(row.active_end).value for row in contracts], dtype=np.int64)
    values = pd.to_datetime(timestamp, utc=True).astype("datetime64[ns, UTC]").array.asi8
    positions = np.searchsorted(starts, values, side="right") - 1
    clipped = np.clip(positions, 0, len(contracts) - 1)
    valid = (positions >= 0) & (positions < len(contracts)) & (values < ends[clipped])
    names = np.asarray([row.contract for row in contracts], dtype=object)
    output = np.full(len(values), None, dtype=object)
    output[valid] = names[clipped[valid]]
    return pd.Series(output, index=timestamp.index, dtype="string")


def _delivery(contract: pd.Series, root: str) -> pd.Series:
    raw = contract.astype("string").str.upper()
    suffix = raw.str.slice(len(root))
    valid = suffix.str.match(r"^[FGHJKMNQUVXZ]\d{1,2}$", na=False)
    return suffix.where(valid)


def _leg_frame(frame: pd.DataFrame, root: str) -> pd.DataFrame:
    columns = [
        "timestamp",
        "session_day",
        "local_minute",
        "roll_unsafe",
        "contract",
        "open",
        "high",
        "low",
        "close",
    ]
    output = frame[columns].sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    output["timestamp_ns"] = (
        pd.to_datetime(output["timestamp"], utc=True).astype("datetime64[ns, UTC]").array.asi8
    )
    output.attrs["root"] = root
    return output


def _load_inputs(project: Path, audit: Mapping[str, Any]) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, Any]]:
    files = audit["files"]
    definitions = _definition_symbols(Path(files["rb_ho_definition"]["path"]))
    products = (
        _import_databento()
        .DBNStore.from_file(files["rb_ho_ohlcv"]["path"])
        .to_df(pretty_ts=True, map_symbols=True, price_type="float")
        .reset_index()
        .rename(columns={"ts_event": "timestamp"})
    )
    products["timestamp"] = pd.to_datetime(products["timestamp"], utc=True)
    products = products.loc[
        products["timestamp"].ge(pd.Timestamp("2023-01-03", tz="UTC"))
        & products["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
        & products["symbol"].isin(["RB.c.0", "HO.c.0"])
    ].copy()
    products = _session_fields(products)
    products["contract"] = products["instrument_id"].map(
        lambda value: definitions.get(str(int(value)))
    ).astype("string")
    if products["contract"].isna().any():
        raise CrackSpreadTripwireError("unmapped RB/HO instrument_id")
    product_guard = _roll_guard_from_instrument_ids(products)
    products["roll_unsafe"] = products["session_day"].isin(product_guard)

    cl = pd.read_parquet(
        files["cl_ohlcv"]["path"],
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"],
        filters=[("symbol", "=", "CL")],
    )
    cl["timestamp"] = pd.to_datetime(cl["timestamp"], utc=True)
    cl = cl.loc[
        cl["timestamp"].ge(pd.Timestamp("2023-01-03", tz="UTC"))
        & cl["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
    ].copy()
    cl = _session_fields(cl)
    cl["contract"] = _assign_cl_contract(
        cl["timestamp"], Path(files["front_roll_map"]["path"])
    )
    roll_map = _read_json(Path(files["front_roll_map"]["path"]))
    boundaries = {
        str(row["active_start"])[:10]
        for row in roll_map["contracts"]
        if row.get("root") == "CL"
    }
    cl_days = sorted(set(int(value) for value in cl["session_day"]))
    cl_guard = _true_session_guard_days(cl, boundaries, cl_days, radius=1)
    cl["roll_unsafe"] = cl["session_day"].isin(cl_guard) | cl["contract"].isna()

    legs: dict[str, pd.DataFrame] = {}
    for root, symbol in (("RB", "RB.c.0"), ("HO", "HO.c.0")):
        legs[root] = _leg_frame(products.loc[products["symbol"].eq(symbol)].copy(), root)
    legs["CL"] = _leg_frame(cl, "CL")

    def slim(root: str) -> pd.DataFrame:
        return legs[root].rename(
            columns={
                column: f"{root}_{column}"
                for column in legs[root].columns
                if column != "timestamp"
            }
        )

    common = slim("RB").merge(slim("HO"), on="timestamp", how="inner", validate="one_to_one")
    common = common.merge(slim("CL"), on="timestamp", how="inner", validate="one_to_one")
    common = common.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    if not (
        common["RB_session_day"].eq(common["HO_session_day"]).all()
        and common["RB_session_day"].eq(common["CL_session_day"]).all()
    ):
        raise CrackSpreadTripwireError("leg session clocks disagree")
    common["session_day"] = common["CL_session_day"].astype(int)
    common["local_minute"] = common["CL_local_minute"].astype(int)
    for root in LEGS:
        common[f"{root}_delivery"] = _delivery(common[f"{root}_contract"], root)
    same_delivery = (
        common["RB_delivery"].notna()
        & common["RB_delivery"].eq(common["HO_delivery"])
        & common["RB_delivery"].eq(common["CL_delivery"])
    )
    common["same_delivery"] = same_delivery
    common["roll_unsafe"] = common[[f"{root}_roll_unsafe" for root in LEGS]].any(axis=1)
    eligible = common.loc[common["same_delivery"] & ~common["roll_unsafe"]].copy()
    reconstruction = {
        "rb_rows": len(legs["RB"]),
        "ho_rows": len(legs["HO"]),
        "cl_rows": len(legs["CL"]),
        "all_three_common_rows": len(common),
        "same_delivery_rows": int(same_delivery.sum()),
        "post_roll_guard_same_delivery_rows": len(eligible),
        "same_delivery_fraction": float(same_delivery.mean()),
        "product_roll_guard_days": len(product_guard),
        "cl_roll_guard_days": len(cl_guard),
        "q4_rows": 0,
    }
    return eligible.reset_index(drop=True), legs, reconstruction


def _role_bounds(manifest: Mapping[str, Any], role: str) -> tuple[int, int]:
    row = next(value for value in manifest["chronological_roles"] if value["role"] == role)
    return int(str(row["start"]).replace("-", "")), int(str(row["end"]).replace("-", ""))


def _feature_frame(common: pd.DataFrame, spec: PolicySpec, manifest: Mapping[str, Any]) -> pd.DataFrame:
    weights = manifest["physical_contract_ratios"][spec.ratio_id]
    multipliers = manifest["contract_specs"]
    spread = sum(
        float(weights[root])
        * float(multipliers[root]["price_multiplier"])
        * common[f"{root}_close"].astype(float)
        for root in LEGS
    )
    timestamp = pd.to_datetime(common["timestamp"], utc=True)
    discontinuity = timestamp.diff().ne(pd.Timedelta(minutes=1))
    discontinuity |= common["session_day"].ne(common["session_day"].shift())
    for root in LEGS:
        discontinuity |= common[f"{root}_contract"].ne(common[f"{root}_contract"].shift())
    # DBN-backed columns may retain Arrow boolean dtype, whose cumulative sum
    # is not implemented.  Materialise the exact mask as native int64; this is
    # a representation-only conversion and cannot change a boundary.
    segment = pd.Series(
        np.asarray(discontinuity.fillna(True), dtype=bool).astype(np.int64),
        index=common.index,
    ).cumsum()
    movement = spread.groupby(segment, sort=False).diff(spec.lookback_minutes)
    clock = pd.DataFrame(
        {
            "local_minute_chicago": common["local_minute"].map(
                lambda value: f"{int(value)//60:02d}:{int(value)%60:02d}"
            )
        }
    )
    score = _prior_robust_score(movement, clock)
    latest_signal_minute = 15 * 60 + 10 - spec.holding_minutes - 10
    output = common.copy()
    output["spread_close_usd"] = spread
    output["spread_displacement_usd"] = movement
    output["spread_score"] = score
    output["decision_time"] = timestamp + pd.Timedelta(minutes=1)
    output["decision_eligible"] = (
        output["local_minute"].ge(7 * 60)
        & output["local_minute"].le(latest_signal_minute)
        & output["spread_score"].abs().ge(
            float(manifest["candidate_lattice"]["prior_session_robust_z_threshold"])
        )
        & output["spread_displacement_usd"].notna()
    )
    return output


def _first_after(
    leg: pd.DataFrame,
    timestamp: pd.Timestamp,
    *,
    contract: str,
    session_day: int,
    deadline: pd.Timestamp | None,
) -> Mapping[str, Any] | None:
    values = leg["timestamp_ns"].to_numpy(dtype=np.int64, copy=False)
    position = int(np.searchsorted(values, int(timestamp.value), side="right"))
    while position < len(leg):
        row = leg.iloc[position]
        row_time = pd.Timestamp(row["timestamp"])
        if deadline is not None and row_time > deadline:
            return None
        if int(row["session_day"]) != int(session_day):
            return None
        if str(row["contract"]) != str(contract) or bool(row["roll_unsafe"]):
            return None
        return row
    return None


def _cost(
    manifest: Mapping[str, Any], ratio_id: str, scenario: str, *, executed_legs: Sequence[str] = LEGS
) -> dict[str, float]:
    model = manifest["cost_model"][scenario]
    ratios = manifest["physical_contract_ratios"][ratio_id]
    specs = manifest["contract_specs"]
    quantities = {root: abs(int(ratios[root])) for root in executed_legs}
    total_contracts = sum(quantities.values())
    commission = total_contracts * float(model["commission_round_turn_per_contract_usd"])
    slippage = sum(
        quantity
        * 2.0
        * float(model["slippage_ticks_per_side"])
        * float(specs[root]["tick_value_usd"])
        for root, quantity in quantities.items()
    )
    tick_inventory = [
        float(specs[root]["tick_value_usd"])
        for root, quantity in quantities.items()
        for _ in range(quantity)
    ]
    legging = max(sum(tick_inventory) - min(tick_inventory, default=0.0), 0.0) * float(
        model["legging_ticks_per_non_anchor_contract"]
    )
    return {
        "commission_usd": commission,
        "slippage_usd": slippage,
        "legging_usd": legging,
        "total_usd": commission + slippage + legging,
    }


def _pnl(
    entry: Mapping[str, Mapping[str, Any]],
    exit_rows: Mapping[str, Mapping[str, Any]],
    side: int,
    spec: PolicySpec,
    manifest: Mapping[str, Any],
) -> float:
    ratio = manifest["physical_contract_ratios"][spec.ratio_id]
    contract_specs = manifest["contract_specs"]
    return float(
        sum(
            side
            * int(ratio[root])
            * float(contract_specs[root]["price_multiplier"])
            * (float(exit_rows[root]["open"]) - float(entry[root]["open"]))
            for root in entry
        )
    )


def _attempt_event(
    row: Mapping[str, Any],
    spec: PolicySpec,
    manifest: Mapping[str, Any],
    legs: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    displacement = float(row["spread_displacement_usd"])
    base_side = 1 if displacement > 0 else -1
    side = base_side if spec.mechanism.endswith("CONTINUATION") else -base_side
    decision = pd.Timestamp(row["decision_time"])
    session_day = int(row["session_day"])
    entry_deadline = decision + pd.Timedelta(
        minutes=int(manifest["causal_execution"]["maximum_entry_completion_minutes"])
    )
    entries: dict[str, Mapping[str, Any]] = {}
    for root in LEGS:
        fill = _first_after(
            legs[root], decision,
            contract=str(row[f"{root}_contract"]),
            session_day=session_day,
            deadline=entry_deadline,
        )
        if fill is not None:
            entries[root] = fill
    core = {
        "ratio_id": spec.ratio_id,
        "mechanism": spec.mechanism,
        "lookback_minutes": spec.lookback_minutes,
        "holding_minutes": spec.holding_minutes,
        "session_day": session_day,
        "signal_time": pd.Timestamp(row["timestamp"]).isoformat(),
        "decision_time": decision.isoformat(),
        "side": side,
        "spread_score": float(row["spread_score"]),
        "spread_displacement_usd": displacement,
        "contracts": {root: str(row[f"{root}_contract"]) for root in LEGS},
    }
    if len(entries) != len(LEGS):
        exits: dict[str, Mapping[str, Any]] = {}
        for root, fill in entries.items():
            unwind = _first_after(
                legs[root],
                entry_deadline,
                contract=str(row[f"{root}_contract"]),
                session_day=session_day,
                deadline=None,
            )
            if unwind is not None:
                exits[root] = unwind
        executed = tuple(sorted(set(entries) & set(exits)))
        gross = _pnl(
            {root: entries[root] for root in executed},
            {root: exits[root] for root in executed},
            side,
            spec,
            manifest,
        ) if executed else 0.0
        normal_cost = _cost(manifest, spec.ratio_id, "normal", executed_legs=executed)
        stressed_cost = _cost(manifest, spec.ratio_id, "stressed", executed_legs=executed)
        event = {
            **core,
            "outcome_status": "INCOMPLETE_BUNDLE_EXECUTION",
            "filled_legs": sorted(entries),
            "liquidated_legs": list(executed),
            "entry_times": {root: pd.Timestamp(value["timestamp"]).isoformat() for root, value in entries.items()},
            "exit_times": {root: pd.Timestamp(value["timestamp"]).isoformat() for root, value in exits.items()},
            "gross_usd": gross,
            "normal_cost": normal_cost,
            "stressed_cost": stressed_cost,
            "normal_net_usd": gross - normal_cost["total_usd"],
            "stressed_net_usd": gross - stressed_cost["total_usd"],
            "worst_unrealized_usd": min(gross, 0.0) - stressed_cost["total_usd"],
            "best_unrealized_usd": max(gross, 0.0),
        }
        return {**event, "event_id": stable_hash(event)}

    last_entry = max(pd.Timestamp(value["timestamp"]) for value in entries.values())
    exit_intent = last_entry + pd.Timedelta(minutes=spec.holding_minutes)
    exit_deadline = exit_intent + pd.Timedelta(
        minutes=int(manifest["causal_execution"]["maximum_exit_wait_minutes"])
    )
    exits = {}
    for root in LEGS:
        fill = _first_after(
            legs[root],
            exit_intent,
            contract=str(row[f"{root}_contract"]),
            session_day=session_day,
            deadline=exit_deadline,
        )
        if fill is not None:
            exits[root] = fill
    status = "EXECUTABLE_COMPLETE" if len(exits) == len(LEGS) else "INCOMPLETE_BUNDLE_EXIT"
    if len(exits) != len(LEGS):
        # Never censor a live leg for free: extend each missing liquidation to
        # its next observable same-contract bar in the session.
        for root in LEGS:
            if root not in exits:
                fill = _first_after(
                    legs[root],
                    exit_intent,
                    contract=str(row[f"{root}_contract"]),
                    session_day=session_day,
                    deadline=None,
                )
                if fill is not None:
                    exits[root] = fill
    if len(exits) != len(LEGS):
        status = "UNRESOLVED_LIVE_LEG_FAIL_CLOSED"
        gross = -sum(_cost(manifest, spec.ratio_id, "stressed")[key] for key in ("total_usd",))
    else:
        gross = _pnl(entries, exits, side, spec, manifest)
    normal_cost = _cost(manifest, spec.ratio_id, "normal")
    stressed_cost = _cost(manifest, spec.ratio_id, "stressed")
    event = {
        **core,
        "outcome_status": status,
        "filled_legs": sorted(entries),
        "liquidated_legs": sorted(exits),
        "entry_times": {root: pd.Timestamp(value["timestamp"]).isoformat() for root, value in entries.items()},
        "exit_times": {root: pd.Timestamp(value["timestamp"]).isoformat() for root, value in exits.items()},
        "entry_skew_seconds": float(
            (max(pd.Timestamp(v["timestamp"]) for v in entries.values()) - min(pd.Timestamp(v["timestamp"]) for v in entries.values())).total_seconds()
        ),
        "gross_usd": gross,
        "normal_cost": normal_cost,
        "stressed_cost": stressed_cost,
        "normal_net_usd": gross - normal_cost["total_usd"],
        "stressed_net_usd": gross - stressed_cost["total_usd"],
        "worst_unrealized_usd": min(gross, 0.0) - stressed_cost["total_usd"],
        "best_unrealized_usd": max(gross, 0.0),
    }
    return {**event, "event_id": stable_hash(event)}


def _events_for_role(
    common: pd.DataFrame,
    legs: Mapping[str, pd.DataFrame],
    spec: PolicySpec,
    manifest: Mapping[str, Any],
    role: str,
) -> list[dict[str, Any]]:
    lower, upper = _role_bounds(manifest, role)
    features = _feature_frame(common, spec, manifest)
    eligible = features.loc[
        features["decision_eligible"]
        & features["session_day"].ge(lower)
        & features["session_day"].lt(upper)
    ]
    # Exactly one opportunity attempt per session/cell; repeated minute bars
    # are state support, not independent opportunities.
    first = eligible.sort_values("timestamp", kind="mergesort").groupby(
        "session_day", sort=True, observed=True
    ).head(1)
    return [_attempt_event(row, spec, manifest, legs) for row in first.to_dict("records")]


def _summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(events)
    gross = np.asarray([float(row["gross_usd"]) for row in rows], dtype=float)
    normal = np.asarray([float(row["normal_net_usd"]) for row in rows], dtype=float)
    stressed = np.asarray([float(row["stressed_net_usd"]) for row in rows], dtype=float)
    positive = np.maximum(stressed, 0.0)
    by_day: dict[int, float] = {}
    for row, value in zip(rows, stressed, strict=True):
        by_day[int(row["session_day"])] = by_day.get(int(row["session_day"]), 0.0) + float(value)
    positive_days = np.maximum(np.asarray(list(by_day.values()), dtype=float), 0.0)
    total_positive = float(positive.sum())
    statuses: dict[str, int] = {}
    for row in rows:
        key = str(row["outcome_status"])
        statuses[key] = statuses.get(key, 0) + 1
    cost_stressed = float(sum(float(row["stressed_cost"]["total_usd"]) for row in rows))
    return {
        "event_count": len(rows),
        "independent_session_count": len(by_day),
        "outcome_status_counts": dict(sorted(statuses.items())),
        "gross_total_usd": float(gross.sum()),
        "normal_net_total_usd": float(normal.sum()),
        "stressed_net_total_usd": float(stressed.sum()),
        "stressed_cost_total_usd": cost_stressed,
        "gross_to_stressed_cost_ratio": float(gross.sum() / cost_stressed) if cost_stressed > 0 else 0.0,
        "stressed_mean_per_event_usd": float(stressed.mean()) if len(stressed) else 0.0,
        "stressed_median_per_event_usd": float(np.median(stressed)) if len(stressed) else 0.0,
        "stressed_win_rate": float(np.mean(stressed > 0.0)) if len(stressed) else 0.0,
        "maximum_single_event_positive_profit_share": float(positive.max() / total_positive) if total_positive > 0 else 0.0,
        "maximum_single_day_positive_profit_share": float(positive_days.max() / positive_days.sum()) if positive_days.sum() > 0 else 0.0,
        "median_entry_skew_seconds": float(np.median([float(row.get("entry_skew_seconds", 0.0)) for row in rows])) if rows else 0.0,
        "ledger_hash": stable_hash(rows),
    }


def _upper_bound(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(events)
    pnl = []
    for row in rows:
        cost = float(row["stressed_cost"]["total_usd"])
        pnl.append(max(abs(float(row["gross_usd"])) - cost, 0.0))
    return {
        "classification": "NON_DEPLOYABLE_FULL_OUTCOME_DIRECTION_OR_ABSTAIN_UPPER_BOUND",
        "event_count": len(rows),
        "traded_event_count": int(sum(value > 0 for value in pnl)),
        "stressed_net_usd": float(sum(pnl)),
        "positive_event_rate": float(np.mean(np.asarray(pnl) > 0.0)) if pnl else 0.0,
        "not_a_strategy": True,
    }


def _write_immutable(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            raise CrackSpreadTripwireError(f"immutable artifact drift: {path}")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o444)
    try:
        os.write(descriptor, encoded.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _selection(
    specs: Sequence[PolicySpec],
    discovery: Mapping[str, Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> list[str]:
    gate = manifest["selection_and_gate"]
    eligible = [
        _policy_id(spec, manifest)
        for spec in specs
        if int(discovery[_policy_id(spec, manifest)]["event_count"])
        >= int(gate["minimum_discovery_complete_events"])
        and float(discovery[_policy_id(spec, manifest)]["stressed_net_total_usd"]) > 0.0
    ]
    eligible.sort(
        key=lambda policy: (
            float(discovery[policy]["stressed_net_total_usd"]),
            float(discovery[policy]["gross_to_stressed_cost_ratio"]),
            int(discovery[policy]["event_count"]),
            policy,
        ),
        reverse=True,
    )
    selected: list[str] = []
    used_ratio_mechanism: set[tuple[str, str]] = set()
    by_id = {_policy_id(spec, manifest): spec for spec in specs}
    for policy in eligible:
        spec = by_id[policy]
        cell = (spec.ratio_id, spec.mechanism)
        if cell in used_ratio_mechanism:
            continue
        selected.append(policy)
        used_ratio_mechanism.add(cell)
        if len(selected) >= int(gate["maximum_selected_cells"]):
            break
    return selected


def _held_gate(
    validation: Mapping[str, Any], final: Mapping[str, Any], manifest: Mapping[str, Any]
) -> dict[str, Any]:
    gate = manifest["selection_and_gate"]
    checks = {
        "validation_event_density": int(validation["event_count"]) >= int(gate["minimum_validation_complete_events"]),
        "final_event_density": int(final["event_count"]) >= int(gate["minimum_final_development_complete_events"]),
        "combined_held_event_density": int(validation["event_count"]) + int(final["event_count"]) >= int(gate["minimum_combined_held_complete_events"]),
        "positive_validation_stressed": float(validation["stressed_net_total_usd"]) > 0.0,
        "positive_final_stressed": float(final["stressed_net_total_usd"]) > 0.0,
        "event_concentration": max(float(validation["maximum_single_event_positive_profit_share"]), float(final["maximum_single_event_positive_profit_share"])) <= float(gate["maximum_single_event_positive_profit_share"]),
        "day_concentration": max(float(validation["maximum_single_day_positive_profit_share"]), float(final["maximum_single_day_positive_profit_share"])) <= float(gate["maximum_single_day_positive_profit_share"]),
    }
    return {"checks": checks, "passed": all(checks.values())}


def run_tripwire(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    specs = frozen_specs(manifest)
    common, legs, reconstruction = _load_inputs(project, audit)

    discovery_events: dict[str, list[dict[str, Any]]] = {}
    discovery_summary: dict[str, dict[str, Any]] = {}
    discovery_upper: dict[str, dict[str, Any]] = {}
    for spec in specs:
        policy = _policy_id(spec, manifest)
        rows = _events_for_role(common, legs, spec, manifest, "DISCOVERY")
        discovery_events[policy] = rows
        discovery_summary[policy] = _summary(rows)
        discovery_upper[policy] = _upper_bound(rows)
    selected = _selection(specs, discovery_summary, manifest)
    selection_core = {
        "schema": "hydra_executable_hedged_crack_selection_freeze_v1",
        "manifest_hash": manifest["manifest_hash"],
        "audit_hash": audit["audit_hash"],
        "candidate_count": len(specs),
        "selected_policy_ids": selected,
        "selection_role": "DISCOVERY_ONLY",
        "selection_rule": "POSITIVE_STRESSED_AND_100_EVENTS_THEN_BEST_ONE_PER_RATIO_MECHANISM_MAX4",
        "held_outcomes_opened": False,
        "spec_hash": stable_hash([asdict(spec) for spec in specs]),
    }
    selection = {**selection_core, "selection_hash": stable_hash(selection_core)}
    _write_immutable(project / REPORT_DIR / "selection_freeze.json", selection)

    by_id = {_policy_id(spec, manifest): spec for spec in specs}
    held: dict[str, dict[str, Any]] = {}
    held_events: dict[str, dict[str, list[dict[str, Any]]]] = {}
    held_gates: dict[str, dict[str, Any]] = {}
    for policy in selected:
        spec = by_id[policy]
        validation_rows = _events_for_role(common, legs, spec, manifest, "VALIDATION")
        final_rows = _events_for_role(common, legs, spec, manifest, "FINAL_DEVELOPMENT")
        held_events[policy] = {"VALIDATION": validation_rows, "FINAL_DEVELOPMENT": final_rows}
        validation_summary = _summary(validation_rows)
        final_summary = _summary(final_rows)
        held[policy] = {
            "VALIDATION": validation_summary,
            "FINAL_DEVELOPMENT": final_summary,
            "NON_DEPLOYABLE_UPPER_BOUND_VALIDATION": _upper_bound(validation_rows),
            "NON_DEPLOYABLE_UPPER_BOUND_FINAL_DEVELOPMENT": _upper_bound(final_rows),
        }
        held_gates[policy] = _held_gate(validation_summary, final_summary, manifest)
    passers = [policy for policy in selected if held_gates[policy]["passed"]]

    # Exact account replay is intentionally fail-closed until a held-out cell
    # is positive in both roles with enough independent sessions.  No passing
    # cell means no account/XFA work at all.
    account_matrix: list[dict[str, Any]] = []
    if passers:
        # This branch would need a separate composite intrabar trajectory
        # reconciliation before it may claim exact MLL.  The bounded tripwire
        # therefore records the technical gate rather than approximating it.
        raise CrackSpreadTripwireError(
            "held event gate passed; exact composite intrabar account adapter required before account replay"
        )

    discovery_eligible = [
        policy
        for policy, row in discovery_summary.items()
        if int(row["event_count"]) >= int(manifest["selection_and_gate"]["minimum_discovery_complete_events"])
        and float(row["stressed_net_total_usd"]) > 0.0
    ]
    if not discovery_eligible:
        verdict = "EXECUTABLE_HEDGED_CRACK_SPREAD_STATE_FALSIFIED_DISCOVERY"
    elif not passers:
        verdict = "EXECUTABLE_HEDGED_CRACK_SPREAD_STATE_FALSIFIED_HELD_OUT"
    else:
        verdict = "EXECUTABLE_HEDGED_CRACK_SPREAD_STATE_EVENT_GATE_GREEN"
    core = {
        "schema": "hydra_executable_hedged_crack_spread_state_result_v1",
        "branch_id": manifest["branch_id"],
        "verdict": verdict,
        "evidence_role": "PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "manifest_hash": manifest["manifest_hash"],
        "selection_hash": selection["selection_hash"],
        "audit": {
            "audit_hash": audit["audit_hash"],
            "rule_snapshot_hash": audit["rule_snapshot_hash"],
            "shared_contract_caps": audit["shared_contract_caps"],
        },
        "data_reconstruction": reconstruction,
        "counts": {
            "proposal_count": len(specs),
            "discovery_policy_count": len(discovery_summary),
            "discovery_selected_count": len(selected),
            "held_gate_passer_count": len(passers),
            "account_cell_count": len(account_matrix),
            "account_episode_count": 0,
            "exact_normal_passes": 0,
            "exact_stressed_passes": 0,
            "xfa_paths": 0,
        },
        "policy_specs": {policy: asdict(spec) for policy, spec in sorted(by_id.items())},
        "discovery_summaries": discovery_summary,
        "discovery_non_deployable_upper_bounds": discovery_upper,
        "selected_policy_ids": selected,
        "held_summaries": held,
        "held_gates": held_gates,
        "held_gate_passers": passers,
        "account_matrix": account_matrix,
        "contract_limit_matrix": {
            "50K": {"cap": 3, "status": "INELIGIBLE_CONTRACT_CAP_ALL_RATIOS"},
            "100K": {"cap": 6, "status": "ELIGIBLE_IF_EVENT_GATE_PASSES"},
            "150K": {"cap": 9, "status": "ELIGIBLE_IF_EVENT_GATE_PASSES"},
        },
        "governance": {
            "cpu_workers": 1,
            "numeric_threads": 1,
            "new_data_purchase": False,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "xfa_paths": 0,
            "persistent_service_modified": False,
        },
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "next_autonomous_action": (
            "PIVOT_TO_TREASURY_AUCTION_CONCESSION_CURVE_RELATIVE_VALUE"
            if not passers
            else "BUILD_AND_RECONCILE_COMPOSITE_INTRABAR_ACCOUNT_ADAPTER_THEN_EXACT_REPLAY"
        ),
    }
    result = {**core, "result_hash": stable_hash(core)}
    _write_immutable(project / REPORT_DIR / "economic_result.json", result)
    return result


__all__ = [
    "CrackSpreadTripwireError",
    "PolicySpec",
    "audit_inputs",
    "frozen_specs",
    "run_tripwire",
]
