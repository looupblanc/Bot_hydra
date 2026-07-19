"""Bounded magnitude-only cross-asset volatility-convexity tripwire.

Treasury volatility can arm a target-market OCO, but it can never choose its
direction.  All economic evidence in this module is viewed pre-Q4 development
evidence and is capped at Tier E.  The module never writes mission state,
registries, queues, budget ledgers, services, broker routes, or orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import RollMap, load_roll_map
from hydra.economic_evolution.schema import stable_hash
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _apply_explicit_contract_map
from hydra.production import autonomous_exact_replay as exact
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.mll_variants import advance_end_of_day_floor, advance_intraday_floor
from hydra.propfirm.scaling_plan import mini_equivalent


SCHEMA = "hydra_cross_asset_volatility_convexity_tripwire_v1"
BRANCH_ID = "CROSS_ASSET_VOLATILITY_CONVEXITY_WITHOUT_DIRECTION_TRANSFER_V1"
DEFAULT_CARD = Path(
    "config/research/cross_asset_volatility_convexity_without_direction_transfer_v1.json"
)
DEFAULT_OUTPUT = Path(
    "reports/research_tripwires/cross_asset_volatility_convexity_without_direction_transfer_v1"
)
MAP_TYPE = "EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
PRIMARY = "PRIMARY"


class VolatilityConvexityTripwireError(RuntimeError):
    """A frozen input or causal/economic invariant failed closed."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise VolatilityConvexityTripwireError("decision-card hash drift")
    governance = dict(card.get("governance") or {})
    if (
        card.get("selected_branch") != BRANCH_ID
        or governance.get("status_ceiling") != "TIER_E_EXECUTABLE_DIAGNOSTIC"
        or bool(governance.get("promotion_allowed"))
        or bool(governance.get("tier_q_allowed"))
        or bool(governance.get("q4_access_allowed"))
        or bool(governance.get("data_purchase_allowed"))
        or bool(governance.get("broker_connection_allowed"))
        or bool(governance.get("orders_allowed"))
        or int(governance.get("maximum_cpu_workers") or 0) != 1
    ):
        raise VolatilityConvexityTripwireError("decision-card semantic drift")
    return card


def run_cross_asset_volatility_convexity_tripwire(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Run the one-worker pre-Q4 tripwire and return a self-hashed result."""

    started = time.perf_counter()
    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    inputs = dict(card["frozen_inputs"])
    causal = dict(card["causal_contract"])
    account = dict(card["account_frontier"])

    verified = _verify_inputs(project, inputs)
    cemetery = _cemetery_audit(project, card)
    sources, source_roll_days = _load_sources(project, inputs, causal)
    source, sign_invariance = build_source_composite(
        sources,
        prior_sessions=int(causal["normalization_prior_true_sessions"]),
        rv_minutes=int(causal["source_rv_minutes"]),
    )
    if not bool(sign_invariance["passed"]):
        raise VolatilityConvexityTripwireError("source-sign flip invariance failed")

    targets, raw_targets, mapping_proof, roll_map = _load_targets(
        project, inputs, causal
    )
    target_features = build_target_features(
        targets,
        prior_sessions=int(causal["normalization_prior_true_sessions"]),
    )
    calendars, coverage = build_true_session_calendars(
        raw_targets,
        source,
        roll_map=roll_map,
        source_roll_days=source_roll_days,
        card=card,
    )
    proposals, event_sets, construction = build_candidate_event_sets(
        source,
        target_features,
        card=card,
        source_roll_days=source_roll_days,
        target_roll_days={
            market: _target_roll_days(roll_map, market)
            for market in causal["execution_markets"]
        },
    )
    selected = select_discovery_candidates(proposals, event_sets, card=card)
    power = _power_preflight(selected, event_sets, card)

    rules, rule_receipt = exact._load_rule_snapshot(
        _inside(project, inputs["rule_snapshot_path"])
    )
    candidate_results = [
        evaluate_candidate(
            proposal,
            event_sets[str(proposal["candidate_id"])],
            calendars=calendars,
            coverage=coverage,
            rules=rules,
            card=card,
        )
        for proposal in selected
    ]
    branch_gate = _branch_gate(candidate_results, power, card)
    status = str(branch_gate["status"])
    next_action = str(card["next_branch_rule"][_next_rule_key(status)])
    safety = {
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_mission_writes": 0,
    }
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "decision": status,
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "source_bindings": {
            "decision_card_path": str(card_path.relative_to(project)),
            "decision_card_hash": card["card_hash"],
            "verified_files": verified,
            "rule_snapshot": rule_receipt,
        },
        "integrity": {
            "source_sign_flip_invariance": sign_invariance,
            "source_direction_field_count": 0,
            "oco_levels_frozen_before_activation": True,
            "decision_bar_fill_count": int(construction["decision_bar_fill_count"]),
            "entry_double_touch_policy": "AMBIGUOUS_BOTH_TOUCH_ABSTAIN",
            "exit_double_touch_policy": "STOP_FIRST_CONSERVATIVE",
            "roll_unsafe_trade_count": int(construction["roll_unsafe_trade_count"]),
            "future_outcome_decision_field_count": 0,
            "q4_row_count": 0,
            "target_mapping_proof": mapping_proof,
            **safety,
        },
        "cemetery_audit": cemetery,
        "proposal_count": len(proposals),
        "selected_candidate_count": len(selected),
        "selected_candidates": selected,
        "event_counts": {
            candidate_id: {
                control: len(events)
                for control, events in sorted(control_sets.items())
            }
            for candidate_id, control_sets in sorted(event_sets.items())
        },
        "event_construction": construction,
        "role_calendar_counts": {
            role: {market: len(days) for market, days in sorted(values.items())}
            for role, values in calendars.items()
        },
        "coverage": coverage,
        "power_preflight": power,
        "candidate_results": candidate_results,
        "branch_gate": branch_gate,
        "economic_summary": _economic_summary(candidate_results),
        "runtime_seconds": time.perf_counter() - started,
        **safety,
        "next_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


def persist_tripwire_artifacts(
    root: str | Path,
    result: Mapping[str, Any],
    *,
    output_root: str | Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    """Persist one result, one compact report and one reconciliation receipt."""

    project = Path(root).resolve()
    folder = _inside(project, output_root)
    folder.mkdir(parents=True, exist_ok=True)
    result_path = folder / "economic_result.json"
    report_path = folder / "decision_report.json"
    receipt_path = folder / "evidence_receipt.json"
    report_core = {
        "schema": "hydra_cross_asset_volatility_convexity_decision_report_v1",
        "branch_id": BRANCH_ID,
        "decision": result["decision"],
        "evidence_tier_ceiling": result["evidence_tier_ceiling"],
        "proposal_count": result["proposal_count"],
        "selected_candidates": result["selected_candidates"],
        "power_preflight": result["power_preflight"],
        "economic_summary": result["economic_summary"],
        "branch_gate": result["branch_gate"],
        "runtime_seconds": result["runtime_seconds"],
        "next_action": result["next_action"],
        "safety": {
            key: result[key]
            for key in (
                "q4_access_count_delta",
                "data_purchase_count",
                "incremental_data_spend_usd",
                "broker_connections",
                "orders",
                "authoritative_mission_writes",
            )
        },
    }
    report = {**report_core, "report_hash": stable_hash(report_core)}
    _atomic_json(result_path, result)
    _atomic_json(report_path, report)
    receipt_core = {
        "schema": "hydra_cross_asset_volatility_convexity_evidence_receipt_v1",
        "branch_id": BRANCH_ID,
        "result_path": str(result_path.relative_to(project)),
        "result_sha256": _sha256(result_path),
        "result_hash": result["result_hash"],
        "report_path": str(report_path.relative_to(project)),
        "report_sha256": _sha256(report_path),
        "report_hash": report["report_hash"],
        "decision_card_hash": result["source_bindings"]["decision_card_hash"],
        "tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
    }
    receipt = {**receipt_core, "receipt_hash": stable_hash(receipt_core)}
    _atomic_json(receipt_path, receipt)
    return {
        "result_path": str(result_path.relative_to(project)),
        "result_sha256": _sha256(result_path),
        "report_path": str(report_path.relative_to(project)),
        "report_sha256": _sha256(report_path),
        "receipt_path": str(receipt_path.relative_to(project)),
        "receipt_sha256": _sha256(receipt_path),
        "receipt_hash": receipt["receipt_hash"],
    }


def build_source_composite(
    frames: Mapping[str, pd.DataFrame],
    *,
    prior_sessions: int,
    rv_minutes: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build sign-invariant, same-clock source volatility features."""

    prepared: dict[str, pd.DataFrame] = {}
    flipped: dict[str, pd.DataFrame] = {}
    for market in ("ZN", "TN"):
        prepared[market] = _source_features_one(
            frames[market], prior_sessions=prior_sessions, rv_minutes=rv_minutes,
            return_sign=1.0,
        )
        flipped[market] = _source_features_one(
            frames[market], prior_sessions=prior_sessions, rv_minutes=rv_minutes,
            return_sign=-1.0,
        )
        left = prepared[market]["vol_z"].to_numpy(dtype=float)
        right = flipped[market]["vol_z"].to_numpy(dtype=float)
        if not np.allclose(left, right, equal_nan=True, rtol=0.0, atol=0.0):
            raise VolatilityConvexityTripwireError(
                f"source-sign invariance mismatch for {market}"
            )
    columns = [
        "timestamp", "session_day", "local_minute", "contract",
        "roll_segment_id", "vol_z", "rv15",
    ]
    merged = prepared["ZN"][columns].merge(
        prepared["TN"][columns], on=["timestamp", "session_day", "local_minute"],
        how="inner", validate="one_to_one", suffixes=("_zn", "_tn"),
    )
    merged["rates_vol_score"] = merged[["vol_z_zn", "vol_z_tn"]].median(
        axis=1, skipna=False
    )
    output = merged[
        [
            "timestamp", "session_day", "local_minute", "rates_vol_score",
            "rv15_zn", "rv15_tn", "contract_zn", "contract_tn",
            "roll_segment_id_zn", "roll_segment_id_tn",
        ]
    ].sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    original_hash = _numeric_frame_hash(output, ("rates_vol_score", "rv15_zn", "rv15_tn"))
    flipped_merge = flipped["ZN"][columns].merge(
        flipped["TN"][columns], on=["timestamp", "session_day", "local_minute"],
        how="inner", validate="one_to_one", suffixes=("_zn", "_tn"),
    )
    flipped_merge["rates_vol_score"] = flipped_merge[["vol_z_zn", "vol_z_tn"]].median(
        axis=1, skipna=False
    )
    flipped_hash = _numeric_frame_hash(
        flipped_merge, ("rates_vol_score", "rv15_zn", "rv15_tn")
    )
    return output, {
        "passed": original_hash == flipped_hash,
        "original_feature_hash": original_hash,
        "source_sign_flipped_feature_hash": flipped_hash,
        "compared_row_count": len(output),
        "decision_source_direction_fields": [],
    }


def build_target_features(
    frame: pd.DataFrame, *, prior_sessions: int
) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for (_market, _contract), group in frame.groupby(
        ["symbol", "active_contract"], sort=True
    ):
        ordered = group.sort_values("timestamp", kind="mergesort").copy()
        ordered["gap_segment"] = ordered["timestamp"].diff().ne(
            pd.Timedelta(minutes=1)
        ).cumsum()
        segments: list[pd.DataFrame] = []
        for _segment, segment in ordered.groupby("gap_segment", sort=True):
            segment = segment.copy()
            close = segment["close"].astype(float)
            returns = np.log(close).diff()
            segment["rv15"] = np.sqrt(
                returns.pow(2).rolling(15, min_periods=15).sum()
            )
            segment["range30"] = (
                segment["high"].astype(float).rolling(30, min_periods=30).max()
                - segment["low"].astype(float).rolling(30, min_periods=30).min()
            )
            segment["oco_high_15"] = segment["high"].astype(float).rolling(
                15, min_periods=15
            ).max()
            segment["oco_low_15"] = segment["low"].astype(float).rolling(
                15, min_periods=15
            ).min()
            segment["oco_high_30"] = segment["high"].astype(float).rolling(
                30, min_periods=30
            ).max()
            segment["oco_low_30"] = segment["low"].astype(float).rolling(
                30, min_periods=30
            ).min()
            segments.append(segment)
        pieces.append(pd.concat(segments, ignore_index=True))
    output = pd.concat(pieces, ignore_index=True)
    local = output["timestamp"].dt.tz_convert("America/Chicago")
    output["session_day"] = local.dt.strftime("%Y%m%d").astype(int)
    output["local_minute"] = local.dt.hour * 60 + local.dt.minute
    # Same-clock baselines are chronological within each target.  The explicit
    # sort is part of the causal contract: groupby must never inherit contract
    # ordering from the piece-wise roll-map reconstruction.
    output = output.sort_values(
        ["symbol", "local_minute", "timestamp"], kind="mergesort"
    ).reset_index(drop=True)
    grouped = output.groupby(["symbol", "local_minute"], sort=True, group_keys=False)
    for value in ("rv15", "range30"):
        output[f"{value}_median"] = grouped[value].transform(
            lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).median().shift(1)
        )
        output[f"{value}_q25"] = grouped[value].transform(
            lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.25).shift(1)
        )
        output[f"{value}_q35"] = grouped[value].transform(
            lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.35).shift(1)
        )
        output[f"{value}_q50"] = grouped[value].transform(
            lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.50).shift(1)
        )
        output[f"{value}_q75"] = grouped[value].transform(
            lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.75).shift(1)
        )
    scale = (output["rv15_q75"] - output["rv15_q25"]) / 1.349
    output["target_vol_z"] = (
        output["rv15"] - output["rv15_median"]
    ) / scale.replace(0.0, np.nan)
    output = output.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    output["target_index"] = output.groupby("symbol", sort=False).cumcount()
    return output


def build_true_session_calendars(
    raw_targets: pd.DataFrame,
    source: pd.DataFrame,
    *,
    roll_map: RollMap,
    source_roll_days: set[int],
    card: Mapping[str, Any],
) -> tuple[dict[str, dict[str, tuple[int, ...]]], dict[str, Any]]:
    """Use observed exchange sessions; rolls stay elapsed, gaps are censored."""

    causal = card["causal_contract"]
    minimum = int(causal["minimum_observed_minutes_for_complete_session"])
    start = _clock_minutes(causal["calendar_inventory_window_chicago"][0])
    end = _clock_minutes(causal["calendar_inventory_window_chicago"][1])
    target_view = _session_view(raw_targets)
    source_view = source[["timestamp", "session_day", "local_minute"]].copy()
    target_roll = {
        market: _target_roll_days(roll_map, market)
        for market in causal["execution_markets"]
    }
    calendars: dict[str, dict[str, tuple[int, ...]]] = {}
    audit: dict[str, Any] = {}
    for role in ROLES:
        lower, upper = map(_day_int, card["chronological_roles"][role])
        calendars[role] = {}
        audit[role] = {}
        source_counts = source_view.loc[
            source_view["local_minute"].between(start, end - 1)
        ].groupby("session_day").size()
        for market in causal["execution_markets"]:
            target_counts = target_view.loc[
                target_view["symbol"].eq(market)
                & target_view["local_minute"].between(start, end - 1)
            ].groupby("session_day").size()
            true_days = sorted(
                day for day in set(source_counts.index) | set(target_counts.index)
                if lower <= int(day) < upper
            )
            roll_days = set(source_roll_days) | set(target_roll[market])
            planned = sorted(day for day in true_days if day in roll_days)
            censored: dict[str, list[str]] = {}
            for day in true_days:
                if day in roll_days:
                    continue
                reasons: list[str] = []
                if int(source_counts.get(day, 0)) < minimum:
                    reasons.append("SOURCE_MINUTE_GAP")
                if int(target_counts.get(day, 0)) < minimum:
                    reasons.append("TARGET_MINUTE_GAP")
                if reasons:
                    censored[str(day)] = reasons
            calendars[role][market] = tuple(int(value) for value in true_days)
            audit[role][market] = {
                "status": "DATA_CENSORED" if censored else "FULL_COVERAGE",
                "true_session_count": len(true_days),
                "true_session_days": true_days,
                "roll_unsafe_zero_trade_count": len(planned),
                "roll_unsafe_zero_trade_days": planned,
                "data_censored_day_count": len(censored),
                "data_censored_days": [int(value) for value in censored],
                "data_censored_reasons_by_day": censored,
                "calendar_policy": "OBSERVED_EXCHANGE_SESSION_ORDER_NO_WEEKDAY_SYNTHESIS",
            }
    return calendars, audit


def build_candidate_event_sets(
    source: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    card: Mapping[str, Any],
    source_roll_days: set[int],
    target_roll_days: Mapping[str, set[int]] | None = None,
) -> tuple[
    tuple[dict[str, Any], ...],
    dict[str, dict[str, tuple[dict[str, Any], ...]]],
    dict[str, Any],
]:
    causal = card["causal_contract"]
    proposals: list[dict[str, Any]] = []
    output: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    counters: Counter[str] = Counter()
    for market in causal["execution_markets"]:
        target = targets.loc[targets["symbol"].eq(market)].copy()
        target = target.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
        merged = target.merge(source, on=["timestamp", "session_day", "local_minute"], how="inner", validate="one_to_one")
        merged = _add_shifted_source_controls(merged)
        for mechanism in causal["mechanisms"]:
            for session_role, clocks in causal["session_roles_chicago"].items():
                lower, upper = map(_clock_minutes, clocks)
                candidate_id = (
                    f"volconv_{market}_{str(mechanism['mechanism']).lower()}_"
                    f"{session_role.lower()}_v1"
                )
                proposal = {
                    "candidate_id": candidate_id,
                    "mechanism": mechanism["mechanism"],
                    "source_markets": ["ZN", "TN"],
                    "execution_market": market,
                    "session_role": session_role,
                    "structural_fingerprint": stable_hash(
                        {
                            "branch": BRANCH_ID,
                            "market": market,
                            "session_role": session_role,
                            "mechanism": mechanism,
                            "fill": "FROZEN_OCO_NEXT_TRADABLE_BAR",
                            "entry_ambiguity": "ABSTAIN",
                            "exit_ambiguity": "STOP_FIRST",
                        }
                    ),
                }
                proposals.append(proposal)
                pools: dict[str, tuple[dict[str, Any], ...]] = {}
                raw_pools: dict[str, tuple[dict[str, Any], ...]] = {}
                for control, score_column in (
                    (PRIMARY, "rates_vol_score"),
                    ("SOURCE_SHIFT_5_TRUE_SESSIONS", "rates_vol_score_shift5"),
                    ("SOURCE_MAGNITUDE_PERMUTATION", "rates_vol_score_shift11"),
                    ("TARGET_ONLY_DUTY_MATCHED_OCO", None),
                    ("SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO", None),
                ):
                    triggers = _candidate_triggers(
                        merged,
                        mechanism=mechanism,
                        score_column=score_column,
                        clock_start=lower,
                        clock_end=upper,
                        source_roll_days=(
                            set(source_roll_days)
                            | set((target_roll_days or {}).get(market, set()))
                        ),
                    )
                    events, local_counts = _materialize_trigger_set(
                        triggers,
                        target,
                        proposal=proposal,
                        mechanism=mechanism,
                        causal=causal,
                        control=control,
                    )
                    counters.update(local_counts)
                    raw_pools[control] = events
                primary_events = raw_pools[PRIMARY]
                pools[PRIMARY] = primary_events
                used: set[str] = set()
                for control in card["controls"]:
                    pools[control] = _match_control_events(
                        primary_events,
                        raw_pools[control],
                        used=used if control in {
                            "TARGET_ONLY_DUTY_MATCHED_OCO",
                            "SESSION_TIME_DIRECTION_EXPOSURE_MATCHED_PLACEBO",
                        } else set(),
                    )
                output[candidate_id] = pools
    if len(proposals) != 8 or len({p["structural_fingerprint"] for p in proposals}) != 8:
        raise VolatilityConvexityTripwireError("frozen eight-specification lattice drift")
    counters["decision_bar_fill_count"] = 0
    counters["roll_unsafe_trade_count"] = 0
    return tuple(proposals), output, dict(sorted(counters.items()))


def select_discovery_candidates(
    proposals: Sequence[Mapping[str, Any]],
    event_sets: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    *,
    card: Mapping[str, Any],
) -> list[dict[str, Any]]:
    lower, upper = map(_day_int, card["chronological_roles"]["DISCOVERY"])
    scored: list[dict[str, Any]] = []
    for proposal in proposals:
        events = [
            row for row in event_sets[str(proposal["candidate_id"])][PRIMARY]
            if lower <= int(row["session_day"]) < upper
        ]
        stressed = sum(float(row["stressed_net_one_micro"]) for row in events)
        scored.append(
            {
                **dict(proposal),
                "discovery_event_count": len(events),
                "discovery_stressed_net_one_micro_usd": float(stressed),
                "discovery_normal_net_one_micro_usd": float(
                    sum(float(row["normal_net_one_micro"]) for row in events)
                ),
            }
        )
    selected: list[dict[str, Any]] = []
    for market in card["causal_contract"]["execution_markets"]:
        rows = [row for row in scored if row["execution_market"] == market]
        selected.append(
            max(
                rows,
                key=lambda row: (
                    float(row["discovery_stressed_net_one_micro_usd"]),
                    float(row["discovery_normal_net_one_micro_usd"]),
                    int(row["discovery_event_count"]),
                    str(row["candidate_id"]),
                ),
            )
        )
    return selected


def evaluate_candidate(
    proposal: Mapping[str, Any],
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    calendars: Mapping[str, Mapping[str, Sequence[int]]],
    coverage: Mapping[str, Any],
    rules: Mapping[str, Mapping[str, Any]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    market = str(proposal["execution_market"])
    frontier: list[dict[str, Any]] = []
    for account_label in card["account_frontier"]["account_sizes"]:
        rule = dict(rules[str(account_label)])
        config = exact._account_config(rule)
        cap = int(rule["maximum_micro_contracts"])
        cap = min(
            cap,
            int(card["account_frontier"].get("special_micro_caps", {}).get(market, {}).get(account_label, cap)),
        )
        for risk_fraction in card["account_frontier"]["risk_fraction_of_current_mll_buffer"]:
            for horizon in card["account_frontier"]["horizons_trading_days"]:
                cell = _evaluate_cell(
                    event_sets[PRIMARY],
                    calendar=calendars["DISCOVERY"][market],
                    coverage=coverage["DISCOVERY"][market],
                    config=config,
                    account_label=str(account_label),
                    market=market,
                    micro_cap=cap,
                    risk_fraction=float(risk_fraction),
                    horizon=int(horizon),
                    card=card,
                )
                frontier.append(cell)
    frozen = max(
        frontier,
        key=lambda row: (
            int(row["stressed"]["pass_count"]),
            int(row["normal"]["pass_count"]),
            float(row["stressed"]["target_progress_p25"]),
            float(row["stressed"]["target_progress_median"]),
            float(row["stressed"]["net_total_usd"]),
            -float(row["stressed"]["mll_breach_rate"]),
            -int(row["horizon_trading_days"]),
            -int(row["account_size_usd"]),
            -float(row["risk_fraction"]),
        ),
    )
    rule = dict(rules[str(frozen["account_label"])])
    config = exact._account_config(rule)
    cap = int(rule["maximum_micro_contracts"])
    cap = min(
        cap,
        int(card["account_frontier"].get("special_micro_caps", {}).get(market, {}).get(frozen["account_label"], cap)),
    )
    evaluations: dict[str, Any] = {}
    for role in ROLES:
        evaluations[role] = {}
        for control, events in event_sets.items():
            evaluations[role][control] = _evaluate_cell(
                events,
                calendar=calendars[role][market],
                coverage=coverage[role][market],
                config=config,
                account_label=str(frozen["account_label"]),
                market=market,
                micro_cap=cap,
                risk_fraction=float(frozen["risk_fraction"]),
                horizon=int(frozen["horizon_trading_days"]),
                card=card,
            )
    gate = _candidate_gate(proposal, event_sets, evaluations, card)
    return {
        "candidate_id": proposal["candidate_id"],
        "execution_market": market,
        "mechanism": proposal["mechanism"],
        "session_role": proposal["session_role"],
        "frozen_discovery_cell": {
            key: frozen[key]
            for key in (
                "account_label", "account_size_usd", "risk_fraction",
                "horizon_trading_days", "micro_cap",
            )
        },
        "discovery_frontier_cell_count": len(frontier),
        "evaluations": evaluations,
        "event_attribution": _event_attribution(event_sets[PRIMARY], card),
        "control_match_counts": {
            control: len(events) for control, events in sorted(event_sets.items())
        },
        "gate": gate,
        "evidence_tier": "E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
    }


def _source_features_one(
    frame: pd.DataFrame,
    *,
    prior_sessions: int,
    rv_minutes: int,
    return_sign: float,
) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for segment_id, group in frame.groupby("roll_segment_id", sort=True):
        ordered = group.sort_values("timestamp", kind="mergesort").copy()
        ordered["gap_segment"] = ordered["timestamp"].diff().ne(pd.Timedelta(minutes=1)).cumsum()
        for _gap, segment in ordered.groupby("gap_segment", sort=True):
            segment = segment.copy()
            returns = float(return_sign) * np.log(segment["close"].astype(float)).diff()
            segment["rv15"] = np.sqrt(
                returns.pow(2).rolling(rv_minutes, min_periods=rv_minutes).sum()
            )
            segment["roll_segment_id"] = segment_id
            pieces.append(segment)
    output = pd.concat(pieces, ignore_index=True)
    local = output["timestamp"].dt.tz_convert("America/Chicago")
    output["session_day"] = local.dt.strftime("%Y%m%d").astype(int)
    output["local_minute"] = local.dt.hour * 60 + local.dt.minute
    output = output.sort_values(
        ["roll_segment_id", "local_minute", "timestamp"], kind="mergesort"
    ).reset_index(drop=True)
    grouped = output.groupby(["roll_segment_id", "local_minute"], sort=True, group_keys=False)["rv15"]
    median = grouped.transform(
        lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).median().shift(1)
    )
    q25 = grouped.transform(
        lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.25).shift(1)
    )
    q75 = grouped.transform(
        lambda series: series.rolling(prior_sessions, min_periods=prior_sessions).quantile(0.75).shift(1)
    )
    output["vol_z"] = (output["rv15"] - median) / ((q75 - q25) / 1.349).replace(0.0, np.nan)
    return output.sort_values("timestamp", kind="mergesort").reset_index(drop=True)


def _candidate_triggers(
    merged: pd.DataFrame,
    *,
    mechanism: Mapping[str, Any],
    score_column: str | None,
    clock_start: int,
    clock_end: int,
    source_roll_days: set[int],
) -> pd.DataFrame:
    base = merged.loc[
        merged["local_minute"].between(clock_start, clock_end, inclusive="left")
        & ~merged["unsafe_roll_window"].astype(bool)
        & ~merged["session_day"].isin(source_roll_days)
    ].copy()
    if mechanism["target_range_quantile"] is not None:
        base = base.loc[base["range30"].le(base["range30_q35"])]
    if mechanism["target_vol_quantile"] is not None:
        base = base.loc[base["rv15"].le(base["rv15_q50"])]
    if score_column is not None:
        base = base.loc[
            base[score_column].ge(float(mechanism["source_score_minimum"]))
        ].copy()
        if mechanism["source_target_score_gap_minimum"] is not None:
            base = base.loc[
                (base[score_column] - base["target_vol_z"]).ge(
                    float(mechanism["source_target_score_gap_minimum"])
                )
            ]
        base["decision_source_score"] = base[score_column]
    else:
        base["decision_source_score"] = np.nan
    lookback = int(mechanism["oco_lookback_minutes"])
    required = [f"oco_high_{lookback}", f"oco_low_{lookback}", "range30"]
    return base.dropna(subset=required).sort_values("timestamp", kind="mergesort")


def _materialize_trigger_set(
    triggers: pd.DataFrame,
    target: pd.DataFrame,
    *,
    proposal: Mapping[str, Any],
    mechanism: Mapping[str, Any],
    causal: Mapping[str, Any],
    control: str,
) -> tuple[tuple[dict[str, Any], ...], Counter[str]]:
    counters: Counter[str] = Counter()
    events: list[dict[str, Any]] = []
    per_day: Counter[int] = Counter()
    last_arm = -1
    last_exit = -1
    reset_ns = int(causal["event_reset_minutes"]) * 60 * 1_000_000_000
    maximum = int(causal["maximum_events_per_session"])
    for row in triggers.itertuples(index=False):
        decision_ns = int((pd.Timestamp(row.timestamp) + pd.Timedelta(minutes=1)).value)
        day = int(row.session_day)
        if per_day[day] >= maximum or decision_ns - last_arm < reset_ns or decision_ns <= last_exit:
            counters["opportunity_consolidated_count"] += 1
            continue
        event, status = materialize_frozen_oco_event(
            row,
            target,
            proposal=proposal,
            mechanism=mechanism,
            causal=causal,
            control=control,
        )
        counters[status] += 1
        last_arm = decision_ns
        per_day[day] += 1
        if event is not None:
            events.append(event)
            last_exit = int(event["exit_ns"])
    return tuple(events), counters


def materialize_frozen_oco_event(
    row: Any,
    target: pd.DataFrame,
    *,
    proposal: Mapping[str, Any],
    mechanism: Mapping[str, Any],
    causal: Mapping[str, Any],
    control: str,
) -> tuple[dict[str, Any] | None, str]:
    market = str(proposal["execution_market"])
    tick = float(instrument_spec(market).tick_size)
    point = float(instrument_spec(market).point_value)
    lookback = int(mechanism["oco_lookback_minutes"])
    high = float(getattr(row, f"oco_high_{lookback}"))
    low = float(getattr(row, f"oco_low_{lookback}"))
    risk_points = max(
        float(mechanism["stop_range_fraction"]) * float(row.range30),
        int(mechanism["minimum_stop_ticks"]) * tick,
    )
    buy_entry = high + tick
    sell_entry = low - tick
    multiple = float(mechanism["target_r_multiple"])
    levels = {
        "buy_entry": buy_entry,
        "buy_stop": buy_entry - risk_points,
        "buy_target": buy_entry + multiple * risk_points,
        "sell_entry": sell_entry,
        "sell_stop": sell_entry + risk_points,
        "sell_target": sell_entry - multiple * risk_points,
    }
    decision_time = pd.Timestamp(row.timestamp) + pd.Timedelta(minutes=1)
    start_index = int(row.target_index) + 1
    valid = int(mechanism["oco_valid_minutes"])
    if start_index >= len(target):
        return None, "DATA_CENSORED"
    expected = decision_time
    chosen: tuple[int, int, float] | None = None
    for offset in range(valid):
        index = start_index + offset
        if index >= len(target):
            return None, "DATA_CENSORED"
        bar = target.iloc[index]
        timestamp = pd.Timestamp(bar["timestamp"])
        if timestamp != expected + pd.Timedelta(minutes=offset):
            return None, "DATA_CENSORED"
        if bool(bar["unsafe_roll_window"]) or str(bar["active_contract"]) != str(row.active_contract):
            return None, "ROLL_UNSAFE_ZERO_TRADE"
        buy = float(bar["high"]) >= buy_entry
        sell = float(bar["low"]) <= sell_entry
        if buy and sell:
            return None, "AMBIGUOUS_BOTH_TOUCH_ABSTAIN"
        if buy:
            fill = max(buy_entry, float(bar["open"]))
            if fill >= levels["buy_target"]:
                return None, "GAP_BEYOND_TARGET_ABSTAIN"
            chosen = (index, 1, fill)
            break
        if sell:
            fill = min(sell_entry, float(bar["open"]))
            if fill <= levels["sell_target"]:
                return None, "GAP_BEYOND_TARGET_ABSTAIN"
            chosen = (index, -1, fill)
            break
    if chosen is None:
        return None, "OCO_TIMEOUT_NO_FILL"
    fill_index, side, fill = chosen
    stop = levels["buy_stop"] if side > 0 else levels["sell_stop"]
    target_level = levels["buy_target"] if side > 0 else levels["sell_target"]
    maximum_holding = int(mechanism["maximum_holding_minutes"])
    exit_price: float | None = None
    exit_timestamp: pd.Timestamp | None = None
    exit_reason = ""
    highs: list[float] = []
    lows: list[float] = []
    for offset in range(maximum_holding):
        index = fill_index + offset
        if index >= len(target):
            return None, "DATA_CENSORED"
        bar = target.iloc[index]
        expected_bar = pd.Timestamp(target.iloc[fill_index]["timestamp"]) + pd.Timedelta(minutes=offset)
        if pd.Timestamp(bar["timestamp"]) != expected_bar:
            return None, "DATA_CENSORED"
        if bool(bar["unsafe_roll_window"]) or str(bar["active_contract"]) != str(row.active_contract):
            return None, "ROLL_UNSAFE_ZERO_TRADE"
        highs.append(float(bar["high"]))
        lows.append(float(bar["low"]))
        if side > 0:
            stop_touch = float(bar["low"]) <= stop
            target_touch = float(bar["high"]) >= target_level
            if stop_touch:
                exit_price = min(stop, float(bar["open"]))
                exit_reason = "STOP_FIRST" if target_touch else "STOP"
            elif target_touch:
                exit_price = target_level
                exit_reason = "TARGET"
        else:
            stop_touch = float(bar["high"]) >= stop
            target_touch = float(bar["low"]) <= target_level
            if stop_touch:
                exit_price = max(stop, float(bar["open"]))
                exit_reason = "STOP_FIRST" if target_touch else "STOP"
            elif target_touch:
                exit_price = target_level
                exit_reason = "TARGET"
        if exit_price is not None:
            exit_timestamp = pd.Timestamp(bar["timestamp"]) + pd.Timedelta(minutes=1)
            break
    if exit_price is None:
        exit_index = fill_index + maximum_holding
        if exit_index >= len(target):
            return None, "DATA_CENSORED"
        bar = target.iloc[exit_index]
        expected_exit = pd.Timestamp(target.iloc[fill_index]["timestamp"]) + pd.Timedelta(minutes=maximum_holding)
        if pd.Timestamp(bar["timestamp"]) != expected_exit:
            return None, "DATA_CENSORED"
        if bool(bar["unsafe_roll_window"]) or str(bar["active_contract"]) != str(row.active_contract):
            return None, "ROLL_UNSAFE_ZERO_TRADE"
        exit_price = float(bar["open"])
        exit_timestamp = pd.Timestamp(bar["timestamp"])
        exit_reason = "TIME_STOP_NEXT_OPEN"
    if exit_timestamp.tz_convert("America/Chicago").strftime("%H:%M") > "15:10":
        return None, "SESSION_FLATTEN_REJECT"
    gross = side * (float(exit_price) - fill) * point
    favorable = (
        (max(highs, default=fill) - fill) * point
        if side > 0 else (fill - min(lows, default=fill)) * point
    )
    adverse = (
        (min(lows, default=fill) - fill) * point
        if side > 0 else (fill - max(highs, default=fill)) * point
    )
    normal_cost = float(causal["normal_all_in_cost_per_micro_usd"][market])
    stressed_cost = float(causal["stressed_all_in_cost_per_micro_usd"][market])
    local_fill = pd.Timestamp(target.iloc[fill_index]["timestamp"]).tz_convert("America/Chicago")
    session_day = int(local_fill.strftime("%Y%m%d"))
    feature_core = {
        "rates_vol_score": None if pd.isna(row.decision_source_score) else round(float(row.decision_source_score), 12),
        "target_vol_z": round(float(row.target_vol_z), 12),
        "range30": round(float(row.range30), 12),
        "levels": {key: round(value, 12) for key, value in levels.items()},
        "decision_ns": int(decision_time.value),
        "source_contracts": [str(row.contract_zn), str(row.contract_tn)],
        "target_contract": str(row.active_contract),
    }
    return {
        "event_id": f"{proposal['candidate_id']}:{control}:{decision_time.isoformat()}",
        "candidate_id": proposal["candidate_id"],
        "control": control,
        "session_day": session_day,
        "block": _block(session_day),
        "session_role": proposal["session_role"],
        "local_minute": int(row.local_minute),
        "decision_ns": int(decision_time.value),
        "entry_ns": int(pd.Timestamp(target.iloc[fill_index]["timestamp"]).value),
        "exit_ns": int(exit_timestamp.value),
        "side": side,
        "entry_price": fill,
        "exit_price": float(exit_price),
        "exit_reason": exit_reason,
        "frozen_oco_levels": levels,
        "gross_one_micro": gross,
        "favorable_one_micro": favorable,
        "adverse_one_micro": adverse,
        "normal_net_one_micro": gross - normal_cost,
        "stressed_net_one_micro": gross - stressed_cost,
        "normal_cost_one_micro": normal_cost,
        "stressed_cost_one_micro": stressed_cost,
        "stop_risk_one_micro": abs(fill - stop) * point + stressed_cost,
        "source_contracts": [str(row.contract_zn), str(row.contract_tn)],
        "target_contract": str(row.active_contract),
        "feature_hash": stable_hash(feature_core),
        "same_bar_exit_stop_first": exit_reason == "STOP_FIRST",
        "session_compliant": True,
    }, "TRADE_CREATED"


def _evaluate_cell(
    events: Sequence[Mapping[str, Any]],
    *,
    calendar: Sequence[int],
    coverage: Mapping[str, Any],
    config: Any,
    account_label: str,
    market: str,
    micro_cap: int,
    risk_fraction: float,
    horizon: int,
    card: Mapping[str, Any],
) -> dict[str, Any]:
    starts = non_overlapping_starts(calendar, (horizon,))[horizon]
    censored_days = set(int(value) for value in coverage["data_censored_days"])
    positions = {int(day): index for index, day in enumerate(calendar)}
    full: list[tuple[int, str]] = []
    censored: list[tuple[int, str]] = []
    for start_day, label in starts:
        index = positions[int(start_day)]
        window = set(int(day) for day in calendar[index : index + horizon])
        (censored if window & censored_days else full).append((int(start_day), str(label)))
    summaries: dict[str, Any] = {}
    for scenario in SCENARIOS:
        episodes = [
            (
                _run_dynamic_episode(
                    events,
                    calendar,
                    start_day=start_day,
                    horizon=horizon,
                    config=config,
                    account_label=account_label,
                    market=market,
                    micro_cap=micro_cap,
                    risk_fraction=risk_fraction,
                    scenario=scenario,
                    card=card,
                ),
                _block(start_day),
            )
            for start_day, _label in full
        ]
        summaries[scenario] = _summarize_episodes(episodes, len(censored))
    return {
        "account_label": account_label,
        "account_size_usd": int(round(float(config.combine_starting_balance))),
        "market": market,
        "risk_fraction": risk_fraction,
        "micro_cap": micro_cap,
        "horizon_trading_days": horizon,
        "total_preregistered_start_count": len(starts),
        "full_coverage_start_count": len(full),
        "data_censored_start_count": len(censored),
        "normal": summaries["NORMAL"],
        "stressed": summaries["STRESSED_1_5X"],
    }


def _run_dynamic_episode(
    events: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    *,
    start_day: int,
    horizon: int,
    config: Any,
    account_label: str,
    market: str,
    micro_cap: int,
    risk_fraction: float,
    scenario: str,
    card: Mapping[str, Any],
) -> dict[str, Any]:
    days = tuple(int(value) for value in calendar)
    start_index = days.index(int(start_day))
    episode_days = days[start_index : start_index + int(horizon)]
    end_day = episode_days[-1]
    selected = sorted(
        (row for row in events if start_day <= int(row["session_day"]) <= end_day),
        key=lambda row: (int(row["session_day"]), int(row["decision_ns"]), str(row["event_id"])),
    )
    by_day: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for event in selected:
        by_day[int(event["session_day"])].append(event)
    balance = float(config.combine_starting_balance)
    floor = float(config.combine_starting_mll)
    minimum_buffer = balance - floor
    best_day = 0.0
    required_target = float(config.combine_profit_target)
    traded_days = 0
    event_count = 0
    quantities: list[int] = []
    daily_values: list[float] = []
    terminal = CombineTerminal.TIMEOUT.value
    reason = "maximum_evaluation_duration_reached"
    days_to_target: int | None = None
    consistency_ok = True
    account_frontier = card["account_frontier"]
    daily_loss_guard = float(account_frontier["daily_loss_guard_fraction_of_mll"]) * float(config.combine_max_loss_limit)
    daily_profit_guard = float(account_frontier["daily_profit_guard_fraction_of_base_target"]) * float(config.combine_profit_target)
    for elapsed, day in enumerate(episode_days, start=1):
        day_pnl = 0.0
        day_traded = False
        for event in by_day.get(day, []):
            if day_pnl <= -daily_loss_guard or day_pnl >= daily_profit_guard:
                continue
            current_buffer = max(balance - floor, 0.0)
            remaining_daily = max(daily_loss_guard + min(day_pnl, 0.0), 0.0)
            risk_budget = min(risk_fraction * current_buffer, remaining_daily)
            per_micro = max(float(event["stop_risk_one_micro"]), 1e-12)
            quantity = min(int(math.floor(risk_budget / per_micro)), int(micro_cap))
            if quantity <= 0:
                continue
            # ``micro_cap`` is frozen directly from the official rule snapshot
            # (including the MGC special cap), so quantity <= micro_cap is the
            # authoritative contract-limit invariant.  Topstep150KConfig does
            # not and must not carry a second, potentially divergent cap.
            cost = float(
                event["normal_cost_one_micro"]
                if scenario == "NORMAL"
                else event["stressed_cost_one_micro"]
            )
            gross = float(event["gross_one_micro"]) * quantity
            net = (float(event["gross_one_micro"]) - cost) * quantity
            best = max((float(event["favorable_one_micro"]) - cost / 2.0) * quantity, 0.0)
            worst = min((float(event["adverse_one_micro"]) - cost / 2.0) * quantity, 0.0)
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance + best,
                distance=float(config.combine_max_loss_limit),
                lock=float(config.combine_starting_balance),
                variant=config.resolved_mll_mode,
            )
            minimum_buffer = min(minimum_buffer, balance + worst - floor)
            if balance + worst <= floor:
                terminal = CombineTerminal.MLL_BREACH.value
                reason = "intraday_unrealized_mll_touch_or_breach"
                break
            balance += net
            day_pnl += net
            event_count += 1
            day_traded = True
            quantities.append(quantity)
            floor = advance_intraday_floor(
                floor,
                live_equity_high=balance,
                distance=float(config.combine_max_loss_limit),
                lock=float(config.combine_starting_balance),
                variant=config.resolved_mll_mode,
            )
            minimum_buffer = min(minimum_buffer, balance - floor)
            if balance <= floor:
                terminal = CombineTerminal.MLL_BREACH.value
                reason = "realized_mll_touch_or_breach"
                break
        daily_values.append(day_pnl)
        traded_days += int(day_traded)
        if terminal in {CombineTerminal.MLL_BREACH.value, CombineTerminal.COMPLIANCE_FAILURE.value}:
            break
        total = balance - float(config.combine_starting_balance)
        best_day = max(best_day, day_pnl)
        if best_day > float(config.combine_profit_target) * float(config.consistency_best_day_max_pct_of_profit_target):
            required_target = max(required_target, best_day / float(config.consistency_best_day_max_pct_of_profit_target))
        concentration = best_day / total if total > 0 and best_day > 0 else 0.0
        consistency_ok = total <= 0 or concentration <= float(config.consistency_best_day_max_pct_of_profit_target) + 1e-12
        floor = advance_end_of_day_floor(
            floor,
            closing_balance=balance,
            distance=float(config.combine_max_loss_limit),
            lock=float(config.combine_starting_balance),
        )
        minimum_buffer = min(minimum_buffer, balance - floor)
        if total >= required_target and consistency_ok and traded_days >= int(config.minimum_pass_days):
            terminal = CombineTerminal.PASSED.value
            reason = "target_consistency_and_minimum_days_satisfied"
            days_to_target = elapsed
            break
    net = balance - float(config.combine_starting_balance)
    concentration = best_day / net if net > 0 and best_day > 0 else 0.0
    return {
        "terminal": terminal,
        "terminal_reason": reason,
        "net_pnl": net,
        "target_progress": net / max(required_target, 1e-12),
        "minimum_mll_buffer": minimum_buffer,
        "mll_breached": terminal == CombineTerminal.MLL_BREACH.value,
        "consistency_ok": consistency_ok,
        "best_day_concentration": concentration,
        "days_to_target": days_to_target,
        "event_count": event_count,
        "traded_days": traded_days,
        "maximum_micro_quantity": max(quantities, default=0),
        "maximum_mini_equivalent": max((mini_equivalent(market, value) for value in quantities), default=0.0),
        "worst_day_loss": min(daily_values, default=0.0),
        "account_label": account_label,
    }


def _summarize_episodes(
    values: Sequence[tuple[Mapping[str, Any], str]], data_censored_count: int
) -> dict[str, Any]:
    episodes = [row for row, _block_id in values]
    passes = [row for row in episodes if row["terminal"] == CombineTerminal.PASSED.value]
    nets = [float(row["net_pnl"]) for row in episodes]
    progress = [float(row["target_progress"]) for row in episodes]
    pass_by_block = Counter(
        block for row, block in values if row["terminal"] == CombineTerminal.PASSED.value
    )
    return {
        "episode_count": len(episodes),
        "full_coverage_episode_count": len(episodes),
        "data_censored_count": int(data_censored_count),
        "pass_count": len(passes),
        "pass_rate": len(passes) / max(len(episodes), 1),
        "pass_count_by_block": dict(sorted(pass_by_block.items())),
        "net_total_usd": float(sum(nets)),
        "net_median_usd": float(statistics.median(nets)) if nets else 0.0,
        "target_progress_median": float(statistics.median(progress)) if progress else 0.0,
        "target_progress_p25": float(np.percentile(progress, 25)) if progress else 0.0,
        "mll_breach_count": sum(bool(row["mll_breached"]) for row in episodes),
        "mll_breach_rate": sum(bool(row["mll_breached"]) for row in episodes) / max(len(episodes), 1),
        "minimum_mll_buffer_usd": min((float(row["minimum_mll_buffer"]) for row in episodes), default=0.0),
        "consistency_compliance_rate": sum(bool(row["consistency_ok"]) for row in episodes) / max(len(episodes), 1),
        "all_passing_paths_consistency_compliant": bool(passes) and all(bool(row["consistency_ok"]) for row in passes),
        "median_days_to_target": statistics.median([int(row["days_to_target"]) for row in passes]) if passes else None,
        "terminal_distribution": dict(sorted(Counter(str(row["terminal"]) for row in episodes).items())),
    }


def _candidate_gate(
    proposal: Mapping[str, Any],
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    evaluations: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    frozen = card["frozen_gate"]
    validation = evaluations["VALIDATION"][PRIMARY]
    final = evaluations["FINAL_DEVELOPMENT"][PRIMARY]
    controls = [evaluations["FINAL_DEVELOPMENT"][name] for name in card["controls"]]
    primary = final["stressed"]
    beats: dict[str, bool] = {}
    for name, control in zip(card["controls"], controls, strict=True):
        summary = control["stressed"]
        uplift = float(primary["target_progress_median"]) - float(summary["target_progress_median"])
        beats[name] = (
            uplift >= float(frozen["minimum_median_target_progress_uplift_over_each_control"])
            or int(primary["pass_count"]) > int(summary["pass_count"])
        )
    lower, upper = map(_day_int, card["chronological_roles"]["FINAL_DEVELOPMENT"])
    final_events = [
        row for row in event_sets[PRIMARY]
        if lower <= int(row["session_day"]) < upper
    ]
    positive = [max(float(row["stressed_net_one_micro"]), 0.0) for row in final_events]
    concentration = max(positive, default=0.0) / max(sum(positive), 1e-12)
    quarter_net: dict[str, float] = defaultdict(float)
    for row in final_events:
        quarter_net[str(row["block"])] += float(row["stressed_net_one_micro"])
    checks = {
        "positive_validation_stressed": float(validation["stressed"]["net_total_usd"]) > 0.0,
        "positive_final_stressed": float(primary["net_total_usd"]) > 0.0,
        "controlled_mll": float(primary["mll_breach_rate"]) <= float(frozen["maximum_stressed_mll_breach_rate"]),
        "passing_consistency": int(primary["pass_count"]) == 0 or bool(primary["all_passing_paths_consistency_compliant"]),
        "nonnegative_final_p25": float(primary["target_progress_p25"]) >= float(frozen["minimum_final_stressed_target_progress_p25"]),
        "no_single_trade_domination": concentration <= float(frozen["maximum_single_trade_profit_concentration"]),
        "positive_multiple_quarters": sum(value > 0.0 for value in quarter_net.values()) >= 2,
        "beats_all_controls": all(beats.values()),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "control_beats": beats,
        "single_trade_profit_concentration": concentration,
        "positive_quarter_count": sum(value > 0.0 for value in quarter_net.values()),
        "proposal": dict(proposal),
    }


def _branch_gate(
    candidates: Sequence[Mapping[str, Any]],
    power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(power["passed"]):
        return {"passed": False, "status": "VOL_CONVEXITY_UNDERPOWERED_NO_THRESHOLD_RELAXATION", "checks": {"power": False}}
    final_primary = [row["evaluations"]["FINAL_DEVELOPMENT"][PRIMARY] for row in candidates]
    validation_primary = [row["evaluations"]["VALIDATION"][PRIMARY] for row in candidates]
    normal_passes = sum(int(row["normal"]["pass_count"]) for row in final_primary)
    stressed_passes = sum(int(row["stressed"]["pass_count"]) for row in final_primary)
    gate = card["frozen_gate"]
    checks = {
        "two_positive_targets": sum(
            float(v["stressed"]["net_total_usd"]) > 0.0
            and float(f["stressed"]["net_total_usd"]) > 0.0
            for v, f in zip(validation_primary, final_primary, strict=True)
        ) >= int(gate["minimum_distinct_positive_targets"]),
        "combined_normal_passes": normal_passes >= int(gate["minimum_combined_final_normal_passes"]),
        "combined_stressed_passes": stressed_passes >= int(gate["minimum_combined_final_stressed_passes"]),
        "candidate_gates": all(bool(row["gate"]["passed"]) for row in candidates),
    }
    if all(checks.values()):
        status = "VOL_CONVEXITY_GREEN_TIER_E"
    elif any(
        float(row["stressed"]["net_total_usd"]) > 0.0 for row in final_primary
    ):
        status = "VOL_CONVEXITY_WEAK"
    else:
        status = "VOL_CONVEXITY_FALSIFIED"
    return {
        "passed": status == "VOL_CONVEXITY_GREEN_TIER_E",
        "status": status,
        "checks": checks,
        "combined_final_normal_passes": normal_passes,
        "combined_final_stressed_passes": stressed_passes,
    }


def _power_preflight(
    selected: Sequence[Mapping[str, Any]],
    event_sets: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    required = card["power_preflight"]["minimum_independent_events_per_target"]
    rows: dict[str, Any] = {}
    passed = True
    for proposal in selected:
        candidate = str(proposal["candidate_id"])
        counts: dict[str, int] = {}
        checks: dict[str, bool] = {}
        for role in ROLES:
            lower, upper = map(_day_int, card["chronological_roles"][role])
            count = sum(
                lower <= int(row["session_day"]) < upper
                for row in event_sets[candidate][PRIMARY]
            )
            counts[role] = count
            checks[role] = count >= int(required[role])
        passed = passed and all(checks.values())
        rows[candidate] = {"event_counts": counts, "checks": checks}
    return {"passed": passed, "candidates": rows, "thresholds": required}


def _load_sources(
    project: Path,
    inputs: Mapping[str, Any],
    causal: Mapping[str, Any],
) -> tuple[dict[str, pd.DataFrame], set[int]]:
    output: dict[str, pd.DataFrame] = {}
    transitions: set[int] = set()
    lower = pd.Timestamp(causal["data_start_inclusive"])
    upper = pd.Timestamp(causal["data_end_exclusive"])
    for row in inputs["source_files"]:
        path = _inside(project, row["path"])
        frame = pd.read_parquet(
            path,
            columns=["timestamp", "symbol", "contract", "open", "high", "low", "close", "volume", "session_id", "roll_segment_id"],
        )
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.loc[
            frame["symbol"].eq(row["market"])
            & frame["timestamp"].ge(lower)
            & frame["timestamp"].lt(upper)
        ].sort_values("timestamp", kind="mergesort").reset_index(drop=True)
        if frame.empty or frame["timestamp"].duplicated().any():
            raise VolatilityConvexityTripwireError(f"invalid source {row['market']}")
        local = frame["timestamp"].dt.tz_convert("America/Chicago")
        days = local.dt.strftime("%Y%m%d").astype(int)
        changes = frame["roll_segment_id"].ne(frame["roll_segment_id"].shift())
        transitions.update(int(value) for value in days[changes].iloc[1:])
        output[str(row["market"])] = frame
    observed = sorted(
        set(
            int(value)
            for frame in output.values()
            for value in frame["timestamp"].dt.tz_convert("America/Chicago").dt.strftime("%Y%m%d").astype(int)
        )
    )
    guarded: set[int] = set()
    positions = {day: index for index, day in enumerate(observed)}
    for day in transitions:
        index = positions.get(day)
        if index is None:
            continue
        guarded.update(observed[max(0, index - 1) : min(len(observed), index + 2)])
    return output, guarded


def _load_targets(
    project: Path,
    inputs: Mapping[str, Any],
    causal: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int], RollMap]:
    markets = list(causal["execution_markets"])
    pieces: list[pd.DataFrame] = []
    for row in inputs["target_files"]:
        wanted = sorted(set(markets) & set(row["markets"]))
        if not wanted:
            continue
        frame = pd.read_parquet(
            _inside(project, row["path"]),
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume", "session_id"],
            filters=[("symbol", "in", wanted)],
        )
        pieces.append(frame)
    raw = pd.concat(pieces, ignore_index=True)
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
    raw = raw.loc[
        raw["symbol"].isin(markets)
        & raw["timestamp"].ge(pd.Timestamp(causal["data_start_inclusive"]))
        & raw["timestamp"].lt(pd.Timestamp(causal["data_end_exclusive"]))
    ].drop_duplicates(["symbol", "timestamp"], keep="first")
    raw = raw.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)
    roll_map = load_roll_map(_inside(project, inputs["target_roll_map_path"]))
    mapped, proof = _apply_explicit_contract_map(raw, roll_map, required_map_type=MAP_TYPE)
    if set(markets) - set(mapped["symbol"]):
        raise VolatilityConvexityTripwireError("target map removed required market")
    # The authoritative mapper has already removed every unsafe roll row.
    # Preserve an explicit field so downstream deterministic event replay can
    # fail closed if a future mapper version retains and marks such rows.
    mapped["unsafe_roll_window"] = False
    return mapped, raw, proof, roll_map


def _verify_inputs(project: Path, inputs: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in [*inputs["source_files"], *inputs["target_files"]]:
        path = _inside(project, row["path"])
        actual = _sha256(path)
        if actual != str(row["sha256"]):
            raise VolatilityConvexityTripwireError(f"input SHA drift: {row['path']}")
        rows.append({"path": row["path"], "sha256": actual, "size_bytes": path.stat().st_size})
    for prefix in ("target_roll_map", "treasury_roll_receipt", "rule_snapshot"):
        path = _inside(project, inputs[f"{prefix}_path"])
        actual = _sha256(path)
        if actual != str(inputs[f"{prefix}_sha256"]):
            raise VolatilityConvexityTripwireError(f"input SHA drift: {prefix}")
        rows.append({"path": str(path.relative_to(project)), "sha256": actual, "size_bytes": path.stat().st_size})
    return rows


def _cemetery_audit(project: Path, card: Mapping[str, Any]) -> dict[str, Any]:
    audit = card["cemetery_audit"]
    path = _inside(project, audit["graveyard_path"])
    actual = _sha256(path)
    if actual != audit["graveyard_sha256_at_selection"]:
        raise VolatilityConvexityTripwireError("graveyard SHA drift")
    mechanism = str(audit["canonical_mechanism_class"]).lower()
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        collision = int(
            connection.execute(
                "SELECT COUNT(*) FROM class_tombstones WHERE lower(mechanism_class)=?",
                (mechanism,),
            ).fetchone()[0]
        )
    finally:
        connection.close()
    if collision or int(audit["exact_mechanism_class_collision_count"]) != 0:
        raise VolatilityConvexityTripwireError("exact cemetery collision")
    return {
        "graveyard_sha256": actual,
        "canonical_mechanism_class": mechanism,
        "exact_collision_count": 0,
        "adjacent_tombstones_reviewed": audit["adjacent_tombstones_reviewed"],
        "forbidden_neighbor_features": audit["forbidden_neighbor_features"],
        "resurrection_detected": False,
    }


def _add_shifted_source_controls(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    group = output.groupby("local_minute", sort=True)["rates_vol_score"]
    output["rates_vol_score_shift5"] = group.shift(5)
    output["rates_vol_score_shift11"] = group.shift(11)
    return output


def _match_control_events(
    primary: Sequence[Mapping[str, Any]],
    pool: Sequence[Mapping[str, Any]],
    *,
    used: set[str],
) -> tuple[dict[str, Any], ...]:
    available = [row for row in pool if str(row["event_id"]) not in used]
    matched: list[dict[str, Any]] = []
    for real in primary:
        candidates = [
            row for row in available
            if row["block"] == real["block"]
            and row["session_role"] == real["session_role"]
            and int(row["side"]) == int(real["side"])
            and int(row["decision_ns"]) != int(real["decision_ns"])
            and abs(int(row["local_minute"]) - int(real["local_minute"])) <= 60
        ]
        if not candidates:
            continue
        chosen = min(
            candidates,
            key=lambda row: (
                abs(int(row["local_minute"]) - int(real["local_minute"])),
                abs(int(row["session_day"]) - int(real["session_day"])),
                str(row["event_id"]),
            ),
        )
        matched.append(dict(chosen))
        available.remove(chosen)
        used.add(str(chosen["event_id"]))
    return tuple(matched)


def _event_attribution(
    events: Sequence[Mapping[str, Any]], card: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for role in ROLES:
        lower, upper = map(_day_int, card["chronological_roles"][role])
        rows = [row for row in events if lower <= int(row["session_day"]) < upper]
        output[role] = {
            "event_count": len(rows),
            "normal_net_one_micro_usd": sum(float(row["normal_net_one_micro"]) for row in rows),
            "stressed_net_one_micro_usd": sum(float(row["stressed_net_one_micro"]) for row in rows),
            "long_count": sum(int(row["side"]) > 0 for row in rows),
            "short_count": sum(int(row["side"]) < 0 for row in rows),
            "target_count": sum(row["exit_reason"] == "TARGET" for row in rows),
            "stop_count": sum(str(row["exit_reason"]).startswith("STOP") for row in rows),
        }
    return output


def _economic_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        final = candidate["evaluations"]["FINAL_DEVELOPMENT"][PRIMARY]
        rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "market": candidate["execution_market"],
                "account_label": final["account_label"],
                "horizon_trading_days": final["horizon_trading_days"],
                "risk_fraction": final["risk_fraction"],
                "normal_passes": final["normal"]["pass_count"],
                "stressed_passes": final["stressed"]["pass_count"],
                "normal_net_usd": final["normal"]["net_total_usd"],
                "stressed_net_usd": final["stressed"]["net_total_usd"],
                "stressed_target_progress_median": final["stressed"]["target_progress_median"],
                "stressed_target_progress_p25": final["stressed"]["target_progress_p25"],
                "stressed_mll_breach_rate": final["stressed"]["mll_breach_rate"],
                "minimum_mll_buffer_usd": final["stressed"]["minimum_mll_buffer_usd"],
                "gate_passed": candidate["gate"]["passed"],
            }
        )
    return {
        "candidates": rows,
        "combined_final_normal_passes": sum(int(row["normal_passes"]) for row in rows),
        "combined_final_stressed_passes": sum(int(row["stressed_passes"]) for row in rows),
        "positive_final_stressed_candidate_count": sum(float(row["stressed_net_usd"]) > 0.0 for row in rows),
    }


def _session_view(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame[["timestamp", "symbol"]].copy()
    local = output["timestamp"].dt.tz_convert("America/Chicago")
    output["session_day"] = local.dt.strftime("%Y%m%d").astype(int)
    output["local_minute"] = local.dt.hour * 60 + local.dt.minute
    return output


def _target_roll_days(roll_map: RollMap, market: str) -> set[int]:
    values: set[int] = set()
    for contract in roll_map.contracts:
        if contract.root != market or not contract.roll_date:
            continue
        # ``roll_date`` is a declared exchange calendar date.  Treating its
        # midnight as an instant and converting to Chicago would silently move
        # it to the preceding date.
        stamp = pd.Timestamp(contract.roll_date).tz_localize(None).normalize()
        for offset in range(-int(roll_map.unsafe_window_days), int(roll_map.unsafe_window_days) + 1):
            values.add(int((stamp.normalize() + pd.Timedelta(days=offset)).strftime("%Y%m%d")))
    return values


def _numeric_frame_hash(frame: pd.DataFrame, columns: Sequence[str]) -> str:
    values = frame[list(columns)].to_numpy(dtype=np.float64)
    canonical = np.nan_to_num(values, nan=9.87654321e307, posinf=8.7654321e307, neginf=-8.7654321e307)
    return hashlib.sha256(canonical.tobytes(order="C")).hexdigest()


def _next_rule_key(status: str) -> str:
    if status == "VOL_CONVEXITY_GREEN_TIER_E":
        return "when_green"
    if status == "VOL_CONVEXITY_WEAK":
        return "when_weak"
    if status == "VOL_CONVEXITY_UNDERPOWERED_NO_THRESHOLD_RELAXATION":
        return "when_underpowered"
    return "when_falsified"


def _block(day: int) -> str:
    text = str(int(day))
    month = int(text[4:6])
    return f"{text[:4]}Q{(month - 1) // 3 + 1}"


def _day_int(value: str) -> int:
    return int(str(value)[:10].replace("-", ""))


def _clock_minutes(value: str) -> int:
    hour, minute = str(value).split(":", maxsplit=1)
    return int(hour) * 60 + int(minute)


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise VolatilityConvexityTripwireError("path escapes project root") from exc
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise VolatilityConvexityTripwireError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(payload, encoding="utf-8")
    os.replace(temporary, path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--card", default=str(DEFAULT_CARD))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args(argv)
    result = run_cross_asset_volatility_convexity_tripwire(
        args.root, decision_card_path=args.card
    )
    artifacts = persist_tripwire_artifacts(args.root, result, output_root=args.output)
    print(
        json.dumps(
            {
                "decision": result["decision"],
                "economic_summary": result["economic_summary"],
                "result_hash": result["result_hash"],
                "artifacts": artifacts,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
