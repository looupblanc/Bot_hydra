"""Bounded causal RB+HO refinery-products signal pilot with MCL-only execution.

The module is intentionally read-only with respect to mission state.  It consumes
one governed acquisition receipt, freezes selection on discovery, evaluates the
two held-out development roles once, and only opens exact account replay when
the preregistered event-economics gate passes.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import read_ledger, sha256_file
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_exact_replay as exact
from hydra.research.cl_front_second_term_structure_economic_runner import (
    _TargetIndex,
    _block,
    _cell_rank,
    _evaluate_cell,
    _nonoverlapping_events,
    _prior_robust_score,
    _replay_at_timestamp,
    _session_fields,
    _true_session_guard_days,
)
from hydra.research.cl_front_second_term_structure_tripwire import CLTermStructureRule
from scripts.acquire_refinery_products_to_mcl_pilot import (
    ACCESS_LEDGER,
    MANIFEST,
    RECEIPT,
    _read_manifest,
    _schema_request_id,
)


ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
CONTROLS = ("PRIMARY", "CL_ONLY_RETURN_SHOCK", "DIRECTION_FLIP", "SESSION_MATCHED_TIMING_NULL")
TARGET_MARKET = "MCL"
DECISION_START_MINUTE = 7 * 60
DECISION_END_MINUTE = 14 * 60
MIN_SOURCE_ROWS_PER_DAY = 360


class RefineryPilotError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    path = path if path.is_absolute() else root / path
    result = path.resolve()
    if result != root and root not in result.parents:
        raise RefineryPilotError("path escapes project root")
    if not result.is_file():
        raise RefineryPilotError(f"required artifact missing: {result}")
    return result


def audit_inputs(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    manifest = _read_manifest(project)
    receipt_path = project / RECEIPT
    if not receipt_path.is_file():
        raise RefineryPilotError("governed acquisition receipt unavailable")
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
        raise RefineryPilotError("acquisition receipt semantic drift")
    files: dict[str, dict[str, Any]] = {}
    for row in receipt.get("files", []):
        artifact = _inside(project, row["path"])
        if artifact.stat().st_size != int(row["size_bytes"]) or sha256_file(artifact) != row["sha256"]:
            raise RefineryPilotError("raw file receipt drift")
        files[str(row["kind"])] = {**dict(row), "path": str(artifact)}
    if set(files) != {"ohlcv-1m", "definition"}:
        raise RefineryPilotError("raw file inventory drift")
    ledger = read_ledger(project / "reports/data_budget/databento_spend_ledger.jsonl")
    for schema in files:
        rid = _schema_request_id(str(receipt["bundle_id"]), schema)
        rows = [row for row in ledger if row.get("request_id") == rid]
        if len(rows) != 1 or rows[0].get("download_status") != "DOWNLOADED":
            raise RefineryPilotError("spend ledger does not reconcile")
    access_path = project / ACCESS_LEDGER
    access = [
        json.loads(line)
        for line in access_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for role in ROLES:
        marker = f"{receipt['bundle_id']}:{role}"
        if sum(marker in set(row.get("candidate_ids") or ()) for row in access) != 1:
            raise RefineryPilotError("data-role ledger does not reconcile")
    return {
        "manifest": manifest,
        "receipt": receipt,
        "files": files,
        "audit_hash": stable_hash(
            {
                "manifest_hash": manifest["manifest_hash"],
                "receipt_hash": receipt["receipt_hash"],
                "file_hashes": {key: value["sha256"] for key, value in sorted(files.items())},
            }
        ),
    }


def _load_market_data(project: Path, audit: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    manifest = audit["manifest"]
    existing = _inside(project, manifest["frozen_inputs"]["front_and_execution"]["path"])
    raw = pd.read_parquet(
        existing,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"],
        filters=[("symbol", "in", ["CL", "MCL"])],
    )
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw = raw.loc[
        raw["timestamp"].ge(pd.Timestamp("2023-01-03", tz="UTC"))
        & raw["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
    ].copy()
    raw = _session_fields(raw)
    if raw.duplicated(["symbol", "timestamp"]).any():
        raise RefineryPilotError("CL/MCL cache contains duplicate bars")
    cl = raw.loc[raw["symbol"].eq("CL")].copy()
    target = raw.loc[raw["symbol"].eq("MCL")].copy()
    if cl.empty or target.empty:
        raise RefineryPilotError("CL/MCL cache empty")
    target["roll_unsafe"] = False

    store = _import_databento().DBNStore.from_file(audit["files"]["ohlcv-1m"]["path"])
    products = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    products = products.rename(columns={"ts_event": "timestamp"})
    products["timestamp"] = pd.to_datetime(products["timestamp"], utc=True)
    products = products.loc[
        products["timestamp"].ge(pd.Timestamp("2023-01-03", tz="UTC"))
        & products["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
        & products["symbol"].isin(["RB.c.0", "HO.c.0"])
    ].copy()
    products = _session_fields(products)
    if products.duplicated(["symbol", "timestamp"]).any():
        raise RefineryPilotError("RB/HO acquisition contains duplicate bars")
    if set(products["symbol"].unique()) != {"RB.c.0", "HO.c.0"}:
        raise RefineryPilotError("RB/HO symbol reconstruction incomplete")

    product_guard = _roll_guard_from_instrument_ids(products)
    roll_map = _read_json(_inside(project, manifest["frozen_inputs"]["front_roll_map"]["path"]))
    front_boundaries = {
        str(row["active_start"])[:10]
        for row in roll_map.get("contracts", [])
        if row.get("root") in {"CL", "MCL"} and "2023-01-03" <= str(row["active_start"])[:10] < "2024-10-01"
    }
    all_target_days = sorted(set(int(value) for value in target["session_day"]))
    target_guard = _true_session_guard_days(target, front_boundaries, all_target_days, radius=1)
    target["roll_unsafe"] = target["session_day"].isin(target_guard)

    source = _align_sources(cl, products)
    source["roll_unsafe"] = source["session_day"].isin(product_guard | target_guard)
    source["available_at"] = source["timestamp"] + pd.Timedelta(minutes=1)
    return source, target, {
        "cl_mcl_cache_sha256": sha256_file(existing),
        "raw_products_sha256": audit["files"]["ohlcv-1m"]["sha256"],
        "raw_definition_sha256": audit["files"]["definition"]["sha256"],
        "source_aligned_rows": len(source),
        "target_rows": len(target),
        "product_roll_guard_days": len(product_guard),
        "cl_mcl_roll_guard_days": len(target_guard),
        "q4_rows": 0,
    }


def _roll_guard_from_instrument_ids(products: pd.DataFrame) -> set[int]:
    boundaries: set[int] = set()
    for _symbol, frame in products.groupby("symbol", sort=True):
        ordered = frame.sort_values("timestamp", kind="mergesort")
        changed = ordered["instrument_id"].astype(str).ne(ordered["instrument_id"].astype(str).shift())
        boundaries.update(int(value) for value in ordered.loc[changed, "session_day"].iloc[1:])
    days = sorted(set(int(value) for value in products["session_day"]))
    positions = {day: index for index, day in enumerate(days)}
    guarded: set[int] = set()
    for boundary in boundaries:
        index = positions.get(boundary)
        if index is not None:
            guarded.update(days[max(0, index - 1) : index + 2])
    return guarded


def _align_sources(cl: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    def slim(frame: pd.DataFrame, suffix: str, *, instrument: bool) -> pd.DataFrame:
        columns = ["timestamp", "close", "session_day", "local_minute"]
        if instrument:
            columns.append("instrument_id")
        out = frame[columns].copy()
        return out.rename(columns={column: f"{column}_{suffix}" for column in columns if column != "timestamp"})

    rb = products.loc[products["symbol"].eq("RB.c.0")]
    ho = products.loc[products["symbol"].eq("HO.c.0")]
    merged = slim(cl, "cl", instrument=False).merge(
        slim(rb, "rb", instrument=True), on="timestamp", how="inner", validate="one_to_one"
    ).merge(slim(ho, "ho", instrument=True), on="timestamp", how="inner", validate="one_to_one")
    merged = merged.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    if not (
        merged["session_day_cl"].eq(merged["session_day_rb"]).all()
        and merged["session_day_cl"].eq(merged["session_day_ho"]).all()
        and merged["local_minute_cl"].eq(merged["local_minute_rb"]).all()
        and merged["local_minute_cl"].eq(merged["local_minute_ho"]).all()
    ):
        raise RefineryPilotError("source session clocks disagree")
    merged["session_day"] = merged["session_day_cl"].astype(int)
    merged["local_minute"] = merged["local_minute_cl"].astype(int)
    return merged


def _segment_diff(values: pd.Series, segments: pd.Series, periods: int) -> pd.Series:
    return values.groupby(segments, sort=False).diff(periods)


def _feature_frame(source: pd.DataFrame, lookback: int) -> pd.DataFrame:
    out = source.copy()
    timestamp = pd.to_datetime(out["timestamp"], utc=True)
    discontinuity = timestamp.diff().ne(pd.Timedelta(minutes=1))
    discontinuity |= out["session_day"].ne(out["session_day"].shift())
    discontinuity |= out["instrument_id_rb"].astype(str).ne(out["instrument_id_rb"].astype(str).shift())
    discontinuity |= out["instrument_id_ho"].astype(str).ne(out["instrument_id_ho"].astype(str).shift())
    segment = discontinuity.cumsum()
    rb = out["close_rb"].astype(float)
    ho = out["close_ho"].astype(float)
    cl = out["close_cl"].astype(float)
    rb_return = _segment_diff(np.log(rb), segment, lookback)
    ho_return = _segment_diff(np.log(ho), segment, lookback)
    cl_return = _segment_diff(np.log(cl), segment, lookback)
    product_return = (2.0 * rb_return + ho_return) / 3.0
    residual_return = product_return - cl_return
    implied_crude = (2.0 * 42.0 * rb + 42.0 * ho) / 3.0
    crack_margin = implied_crude - cl
    crack_innovation = _segment_diff(crack_margin, segment, lookback)
    score_frame = pd.DataFrame({
        "local_minute_chicago": out["local_minute"].map(lambda value: f"{int(value)//60:02d}:{int(value)%60:02d}")
    })
    result = pd.DataFrame({
        "timestamp": timestamp,
        "available_at": pd.to_datetime(out["available_at"], utc=True),
        "session_day": out["session_day"].astype(int),
        "local_minute": out["local_minute"].astype(int),
        "roll_unsafe": out["roll_unsafe"].astype(bool),
        "rb_return": rb_return,
        "ho_return": ho_return,
        "cl_return": cl_return,
        "product_return": product_return,
        "product_residual_return": residual_return,
        "crack_margin_usd_per_barrel": crack_margin,
        "crack_innovation_usd_per_barrel": crack_innovation,
    })
    result["crack_score_prior_sessions"] = _prior_robust_score(crack_innovation, score_frame)
    result["residual_score_prior_sessions"] = _prior_robust_score(residual_return, score_frame)
    result["cl_score_prior_sessions"] = _prior_robust_score(cl_return, score_frame)
    result["decision_eligible"] = (
        result["local_minute"].ge(DECISION_START_MINUTE)
        & result["local_minute"].lt(DECISION_END_MINUTE)
        & ~result["roll_unsafe"]
        & result["available_at"].eq(result["timestamp"] + pd.Timedelta(minutes=1))
    )
    return result


def _rule(row: Mapping[str, Any]) -> CLTermStructureRule:
    mechanism = str(row["mechanism"])
    lookback = int(row["lookback_minutes"])
    holding = int(row["holding_minutes"])
    return CLTermStructureRule(
        rule_id=f"refinery_v1:{mechanism}:lb{lookback}:h{holding}",
        mechanism=mechanism,
        lookback_minutes=lookback,
        holding_minutes=holding,
        trigger_score=float(row["trigger_z"]),
        target_r_multiple=2.5 if holding == 15 else 3.0,
        stop_r_multiple=1.0,
    )


def _direction(value: Any, rule: CLTermStructureRule) -> tuple[int, float]:
    if not bool(value.decision_eligible):
        return 0, float("nan")
    if rule.mechanism == "CRACK_INNOVATION_CONTINUATION":
        score = float(value.crack_score_prior_sessions)
        raw = float(value.crack_innovation_usd_per_barrel)
        side = int(np.sign(raw)) if np.isfinite(raw) and np.isfinite(score) and abs(score) >= rule.trigger_score else 0
        return side, score
    if rule.mechanism == "PRODUCT_BREADTH_CL_LAG":
        score = float(value.residual_score_prior_sessions)
        product = float(value.product_return)
        rb = float(value.rb_return)
        ho = float(value.ho_return)
        cl = float(value.cl_return)
        aligned = np.isfinite(rb) and np.isfinite(ho) and rb * ho > 0.0
        lagged = np.isfinite(product) and np.isfinite(cl) and abs(cl) <= 0.75 * abs(product)
        side = int(np.sign(product)) if aligned and lagged and np.isfinite(score) and abs(score) >= rule.trigger_score else 0
        return side, score
    raise RefineryPilotError("unknown frozen mechanism")


def _build_events(
    features: Mapping[int, pd.DataFrame], target: pd.DataFrame, rules: Sequence[CLTermStructureRule]
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], dict[str, dict[str, int]]]:
    target_index = _TargetIndex(target)
    all_events: dict[str, dict[str, list[dict[str, Any]]]] = {}
    statuses: dict[str, dict[str, int]] = {}
    for rule in rules:
        primary_decisions: list[dict[str, Any]] = []
        frame = features[rule.lookback_minutes]
        if rule.mechanism == "CRACK_INNOVATION_CONTINUATION":
            eligible = (
                frame["decision_eligible"]
                & frame["crack_score_prior_sessions"].abs().ge(rule.trigger_score)
                & frame["crack_innovation_usd_per_barrel"].ne(0.0)
            )
        elif rule.mechanism == "PRODUCT_BREADTH_CL_LAG":
            eligible = (
                frame["decision_eligible"]
                & frame["residual_score_prior_sessions"].abs().ge(rule.trigger_score)
                & frame["rb_return"].mul(frame["ho_return"]).gt(0.0)
                & frame["cl_return"].abs().le(0.75 * frame["product_return"].abs())
                & frame["product_return"].ne(0.0)
            )
        else:
            raise RefineryPilotError("unknown frozen mechanism")
        # Consolidate repeated raw triggers before trajectory replay.  The
        # next opportunity can start only after the prior causal position has
        # exited; a censored open path closes the rest of that session.  This
        # is equivalent to the final non-overlap ledger but avoids replaying
        # thousands of execution-ineligible trigger updates.
        next_allowed_ns = -1
        censored_session: int | None = None
        for value in frame.loc[eligible].itertuples(index=False):
            decision_time = pd.Timestamp(value.available_at)
            session_day = int(value.session_day)
            if censored_session is not None and session_day != censored_session:
                censored_session = None
            if int(decision_time.value) <= next_allowed_ns or censored_session == session_day:
                continue
            side, score = _direction(value, rule)
            if side == 0:
                continue
            core = {
                "rule_id": rule.rule_id,
                "signal_time": pd.Timestamp(value.timestamp).isoformat(),
                "decision_time": pd.Timestamp(value.available_at).isoformat(),
                "side": side,
                "score": score,
                "crack": float(value.crack_innovation_usd_per_barrel),
                "product_residual": float(value.product_residual_return),
                "cl_return": float(value.cl_return),
            }
            event = _replay_at_timestamp(
                target_index,
                pd.Timestamp(value.available_at),
                side,
                rule,
                control="PRIMARY",
                source_feature_hash=stable_hash(core),
                source_score=score,
            )
            cl_return = float(value.cl_return)
            event["cl_control_side"] = (
                int(np.sign(cl_return)) if np.isfinite(cl_return) and cl_return != 0.0 else int(side)
            )
            primary_decisions.append(event)
            if event["outcome_status"] == "EXECUTABLE_COMPLETE":
                next_allowed_ns = int(event["exit_ns"])
            elif bool(event.get("position_opened")):
                censored_session = session_day
        status_count = Counter(str(row["outcome_status"]) for row in primary_decisions)
        primary = _nonoverlapping_events(
            [row for row in primary_decisions if row["outcome_status"] == "EXECUTABLE_COMPLETE"]
        )
        controls: dict[str, list[dict[str, Any]]] = {"PRIMARY": primary}
        flip = [
            _replay_at_timestamp(
                target_index,
                pd.Timestamp(row["decision_time"]),
                -int(row["side"]),
                rule,
                control="DIRECTION_FLIP",
                source_feature_hash=stable_hash({"primary": row["event_id"], "control": "flip"}),
            )
            for row in primary
        ]
        cl_only = [
            _replay_at_timestamp(
                target_index,
                pd.Timestamp(row["decision_time"]),
                int(row.get("cl_control_side") or 0) or int(row["side"]),
                rule,
                control="CL_ONLY_RETURN_SHOCK",
                source_feature_hash=stable_hash({"primary": row["event_id"], "control": "cl_only"}),
            )
            for row in primary
        ]
        timing: list[dict[str, Any]] = []
        for row in primary:
            shifted = pd.Timestamp(row["decision_time"]) + pd.Timedelta(minutes=17)
            local = shifted.tz_convert("America/Chicago")
            if local.hour * 60 + local.minute >= DECISION_END_MINUTE:
                continue
            timing.append(
                _replay_at_timestamp(
                    target_index,
                    shifted,
                    int(row["side"]),
                    rule,
                    control="SESSION_MATCHED_TIMING_NULL",
                    source_feature_hash=stable_hash({"primary": row["event_id"], "control": "timing17"}),
                )
            )
        controls["DIRECTION_FLIP"] = _nonoverlapping_events([row for row in flip if row["outcome_status"] == "EXECUTABLE_COMPLETE"])
        controls["CL_ONLY_RETURN_SHOCK"] = _nonoverlapping_events([row for row in cl_only if row["outcome_status"] == "EXECUTABLE_COMPLETE"])
        controls["SESSION_MATCHED_TIMING_NULL"] = _nonoverlapping_events([row for row in timing if row["outcome_status"] == "EXECUTABLE_COMPLETE"])
        all_events[rule.rule_id] = controls
        statuses[rule.rule_id] = dict(sorted(status_count.items()))
    return all_events, statuses


def _role_bounds(manifest: Mapping[str, Any], role: str) -> tuple[int, int]:
    row = next(value for value in manifest["chronological_roles"] if value["role"] == role)
    return int(row["start"].replace("-", "")), int(row["end"].replace("-", ""))


def _event_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    events = list(rows)
    gross = [float(row["gross_one_micro"]) for row in events]
    normal = [float(row["normal_net_one_micro"]) for row in events]
    stressed = [float(row["stressed_net_one_micro"]) for row in events]
    positive = [max(value, 0.0) for value in stressed]
    total_positive = sum(positive)
    by_day: dict[int, float] = {}
    for row, value in zip(events, stressed, strict=True):
        by_day[int(row["session_day"])] = by_day.get(int(row["session_day"]), 0.0) + value
    positive_days = [max(value, 0.0) for value in by_day.values()]
    return {
        "event_count": len(events),
        "independent_session_count": len(by_day),
        "gross_total_usd": float(sum(gross)),
        "normal_net_total_usd": float(sum(normal)),
        "stressed_net_total_usd": float(sum(stressed)),
        "stressed_mean_per_event_usd": float(np.mean(stressed)) if stressed else 0.0,
        "stressed_median_per_event_usd": float(np.median(stressed)) if stressed else 0.0,
        "stressed_win_rate": float(np.mean(np.asarray(stressed) > 0.0)) if stressed else 0.0,
        "favorable_before_adverse_rate": float(
            np.mean([float(row["favorable_one_micro"]) > abs(float(row["adverse_one_micro"])) for row in events])
        ) if events else 0.0,
        "maximum_single_event_profit_concentration": max(positive, default=0.0) / total_positive if total_positive > 0 else 0.0,
        "maximum_single_day_profit_concentration": max(positive_days, default=0.0) / sum(positive_days) if sum(positive_days) > 0 else 0.0,
        "ledger_hash": stable_hash(events),
    }


def _summaries(
    events: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]], manifest: Mapping[str, Any]
) -> dict[str, dict[str, dict[str, Any]]]:
    output: dict[str, dict[str, dict[str, Any]]] = {}
    for candidate, controls in sorted(events.items()):
        output[candidate] = {}
        for role in ROLES:
            lower, upper = _role_bounds(manifest, role)
            output[candidate][role] = {
                control: _event_summary([row for row in rows if lower <= int(row["session_day"]) < upper])
                for control, rows in controls.items()
            }
    return output


def _select_discovery(rules: Sequence[CLTermStructureRule], summaries: Mapping[str, Any]) -> list[str]:
    selected: list[str] = []
    for mechanism in sorted({rule.mechanism for rule in rules}):
        group = [rule for rule in rules if rule.mechanism == mechanism]
        best = max(
            group,
            key=lambda rule: (
                float(summaries[rule.rule_id]["DISCOVERY"]["PRIMARY"]["stressed_net_total_usd"]),
                int(summaries[rule.rule_id]["DISCOVERY"]["PRIMARY"]["event_count"]),
                rule.rule_id,
            ),
        )
        selected.append(best.rule_id)
    return selected[:2]


def _event_gate(candidate: str, summaries: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    gate = manifest["event_gate"]
    validation = summaries[candidate]["VALIDATION"]
    final = summaries[candidate]["FINAL_DEVELOPMENT"]
    checks = {
        "validation_power": validation["PRIMARY"]["event_count"] >= int(gate["minimum_complete_events_validation"]),
        "final_power": final["PRIMARY"]["event_count"] >= int(gate["minimum_complete_events_final_development"]),
        "positive_validation_stressed": validation["PRIMARY"]["stressed_net_total_usd"] > 0.0,
        "positive_final_stressed": final["PRIMARY"]["stressed_net_total_usd"] > 0.0,
        "validation_uplift_cl_only": validation["PRIMARY"]["stressed_net_total_usd"] > validation["CL_ONLY_RETURN_SHOCK"]["stressed_net_total_usd"],
        "final_uplift_cl_only": final["PRIMARY"]["stressed_net_total_usd"] > final["CL_ONLY_RETURN_SHOCK"]["stressed_net_total_usd"],
        "event_concentration": max(
            validation["PRIMARY"]["maximum_single_event_profit_concentration"],
            final["PRIMARY"]["maximum_single_event_profit_concentration"],
        ) <= float(gate["maximum_single_event_profit_concentration"]),
        "day_concentration": max(
            validation["PRIMARY"]["maximum_single_day_profit_concentration"],
            final["PRIMARY"]["maximum_single_day_profit_concentration"],
        ) <= float(gate["maximum_single_day_profit_concentration"]),
    }
    return {"passed": all(checks.values()), "checks": checks}


def _coverage(source: pd.DataFrame, target: pd.DataFrame, manifest: Mapping[str, Any]) -> tuple[dict[str, tuple[int, ...]], dict[str, list[int]]]:
    source_counts = source.loc[
        source["local_minute"].ge(DECISION_START_MINUTE - 60)
        & source["local_minute"].lt(DECISION_END_MINUTE)
    ].groupby("session_day").size()
    target_days = sorted(set(int(value) for value in target["session_day"]))
    calendars: dict[str, tuple[int, ...]] = {}
    censored: dict[str, list[int]] = {}
    for role in ROLES:
        lower, upper = _role_bounds(manifest, role)
        days = tuple(day for day in target_days if lower <= day < upper)
        calendars[role] = days
        censored[role] = [day for day in days if int(source_counts.get(day, 0)) < MIN_SOURCE_ROWS_PER_DAY]
    return calendars, censored


def _account_matrix(
    candidate: str,
    primary_events: Sequence[Mapping[str, Any]],
    source: pd.DataFrame,
    target: pd.DataFrame,
    manifest: Mapping[str, Any],
    rules_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    account_rules, snapshot = exact._load_rule_snapshot(rules_path)
    calendars, censored = _coverage(source, target, manifest)
    matrix: list[dict[str, Any]] = []
    for role in ROLES:
        for label in manifest["account_gate"]["account_sizes"]:
            rule = account_rules[label]
            config = exact._account_config(rule)
            cap = int(rule["special_contract_caps"]["MCL"][label])
            for risk in manifest["account_gate"]["risk_fraction_of_current_mll_buffer"]:
                for horizon in manifest["account_gate"]["horizons_trading_days"]:
                    cell = _evaluate_cell(
                        primary_events,
                        calendar=calendars[role],
                        censored_days=censored[role],
                        censored_signal_days=(),
                        config=config,
                        account_label=label,
                        micro_cap=cap,
                        risk_fraction=float(risk),
                        horizon=int(horizon),
                    )
                    matrix.append({"candidate_id": candidate, "role": role, **cell})
    return matrix, snapshot


def run_pilot(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    source, target, reconstruction = _load_market_data(project, audit)
    rules = tuple(_rule(row) for row in manifest["frozen_cells"])
    if len(rules) != 12 or len({stable_hash(rule.to_dict()) for rule in rules}) != 12:
        raise RefineryPilotError("frozen rule lattice drift")
    features = {lookback: _feature_frame(source, lookback) for lookback in (5, 15, 60)}
    event_sets, decision_statuses = _build_events(features, target, rules)
    summaries = _summaries(event_sets, manifest)
    selected = _select_discovery(rules, summaries)
    gates = {candidate: _event_gate(candidate, summaries, manifest) for candidate in selected}
    passing = [candidate for candidate in selected if gates[candidate]["passed"]]

    account_matrix: list[dict[str, Any]] = []
    rule_snapshot: dict[str, Any] | None = None
    if passing:
        rules_path = _inside(project, manifest["frozen_inputs"]["rule_snapshot"]["path"])
        for candidate in passing:
            cells, snapshot = _account_matrix(
                candidate, event_sets[candidate]["PRIMARY"], source, target, manifest, rules_path
            )
            account_matrix.extend(cells)
            rule_snapshot = snapshot
    exact_passes = sum(
        int(cell[scenario]["pass_count"])
        for cell in account_matrix
        for scenario in ("normal", "stressed")
    )
    if not passing:
        power = any(
            gates[candidate]["checks"]["validation_power"] and gates[candidate]["checks"]["final_power"]
            for candidate in selected
        )
        status = (
            "REFINERY_PRODUCTS_TO_MCL_TRIPWIRE_FALSIFIED"
            if power
            else "REFINERY_PRODUCTS_TO_MCL_UNDERPOWERED"
        )
    elif exact_passes > 0:
        status = "REFINERY_PRODUCTS_TO_MCL_TIER_E_ACCOUNT_SIGNAL"
    else:
        status = "REFINERY_PRODUCTS_TO_MCL_EVENT_ALPHA_ACCOUNT_VELOCITY_WEAK"
    next_action = (
        "KILL_THIS_EXACT_RB_HO_TO_MCL_BRANCH_AND_REALLOCATE_EXPLORATION"
        if not passing
        else "PRESERVE_TIER_E_ONLY_AND_REQUIRE_MATERIALLY_DISTINCT_CONFIRMATION_BEFORE_PROMOTION"
    )
    core = {
        "schema": "hydra_refinery_products_to_mcl_pilot_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "evidence_role": "PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "tier_ceiling": "E",
        "audit": {key: value for key, value in audit.items() if key != "manifest" and key != "receipt" and key != "files"},
        "manifest_hash": manifest["manifest_hash"],
        "acquisition_receipt_hash": audit["receipt"]["receipt_hash"],
        "actual_incremental_spend_usd": float(audit["receipt"]["official_total_cost_usd"]),
        "data_reconstruction": reconstruction,
        "proposal_count": len(rules),
        "selected_candidate_ids": selected,
        "decision_status_counts": decision_statuses,
        "event_summaries": summaries,
        "event_gates": gates,
        "event_gate_passers": passing,
        "account_matrix_executed": bool(passing),
        "account_cell_count": len(account_matrix),
        "account_episode_count": sum(
            int(cell[scenario]["episode_count"])
            for cell in account_matrix
            for scenario in ("normal", "stressed")
        ),
        "account_matrix": account_matrix,
        "rule_snapshot": rule_snapshot,
        "exact_normal_passes": sum(int(cell["normal"]["pass_count"]) for cell in account_matrix),
        "exact_stressed_passes": sum(int(cell["stressed"]["pass_count"]) for cell in account_matrix),
        "governance": {
            "cpu_workers": 1,
            "numeric_threads": 1,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "xfa_paths": 0,
            "promotion_allowed": False,
        },
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "next_autonomous_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


__all__ = ["RefineryPilotError", "audit_inputs", "run_pilot"]
