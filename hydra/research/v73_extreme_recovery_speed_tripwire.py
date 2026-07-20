"""Bounded ES event-extreme recovery-speed reversal tripwire.

The default path is deliberately metadata-only.  Economic parquet rows are
decoded only by :func:`run_economic_tripwire`, which callers must select
explicitly.  This branch is pre-Q4 development evidence and can reach Tier Q
only through its frozen gate; it cannot inherit or manufacture Tier G/C.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash
from hydra.markets.instruments import instrument_spec
from hydra.production import autonomous_exact_replay as exact
from hydra.research.causal_sleeve_replay import CausalFillPolicy
from hydra.research.cross_asset_volatility_convexity_tripwire import _evaluate_cell
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    generate_signal_population as generate_g2_signal_population,
)


SCHEMA = "hydra_v73_es_event_extreme_recovery_speed_tripwire_v1"
AUDIT_SCHEMA = "hydra_v73_es_event_extreme_recovery_speed_audit_v1"
BRANCH_ID = "V73_ES_EVENT_EXTREME_RECOVERY_SPEED_REVERSAL"
DEFAULT_CARD = Path(
    "config/research/v73_es_event_extreme_recovery_speed_reversal_v1.json"
)
DEFAULT_OUTPUT = Path(
    "reports/research_tripwires/v73_es_event_extreme_recovery_speed_reversal_v1"
)
EXPECTED_CARD_HASH = "7d1dc6b9f33ac9c121e7780f0105852c5c49adbdbe67d880bd8e82ef96e78375"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
PRIMARY = "PRIMARY"
CONTROLS = (
    "STATIC_EXTREME_REJECTION_PARENT",
    "DIRECTION_FLIP_SAME_EVENTS",
    "SESSION_MATCHED_TIMING_NULL",
)
MINUTE_NS = 60_000_000_000


class V73TripwireError(RuntimeError):
    """A frozen input, causal contract, or accounting invariant failed."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed or claimed != EXPECTED_CARD_HASH:
        raise V73TripwireError("V73 decision-card hash drift")
    governance = dict(card.get("governance") or {})
    causal = dict(card.get("causal_contract") or {})
    if (
        card.get("selected_branch") != BRANCH_ID
        or causal.get("execution_market") != "ES"
        or causal.get("q4_end_exclusive") != "2024-10-01"
        or bool(causal.get("future_outcomes_in_decision"))
        or bool(causal.get("future_label_availability_in_eligibility"))
        or not bool(governance.get("audit_only_must_not_decode_parquet"))
        or bool(governance.get("tier_q_allowed"))
        or governance.get("evidence_ceiling") != "TIER_E_EXECUTABLE_DIAGNOSTIC"
        or bool(governance.get("tier_g_allowed"))
        or bool(governance.get("tier_c_allowed"))
        or bool(governance.get("q4_access_allowed"))
        or bool(governance.get("data_purchase_allowed"))
        or bool(governance.get("network_access_allowed"))
        or bool(governance.get("broker_connection_allowed"))
        or bool(governance.get("orders_allowed"))
        or tuple(row["id"] for row in card.get("controls", ())) != CONTROLS
    ):
        raise V73TripwireError("V73 decision-card semantic drift")
    return card


def audit_only(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Verify the immutable preparation without decoding economic rows."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    verified = _verify_frozen_inputs(project, card["frozen_inputs"])
    feature_manifest_path = _inside(
        project, card["frozen_inputs"]["feature_manifest"]["path"]
    )
    feature_manifest = _read_json(feature_manifest_path)
    if feature_manifest.get("outcome_or_pnl_columns") != []:
        raise V73TripwireError("frozen feature manifest contains outcomes or PnL")
    q4_proof = _manifest_q4_proof(
        feature_manifest,
        card["causal_contract"]["q4_end_exclusive"],
        roles=card["chronological_roles"],
    )
    core: dict[str, Any] = {
        "schema": AUDIT_SCHEMA,
        "branch_id": BRANCH_ID,
        "status": "AUDIT_ONLY_GREEN_READY_FOR_EXPLICIT_ECONOMIC_REPLAY",
        "decision_card_path": str(card_path.relative_to(project)),
        "decision_card_hash": card["card_hash"],
        "verified_inputs": verified,
        "candidate_count": len(card["candidate_lattice"]),
        "controls": [row["id"] for row in card["controls"]],
        "chronological_roles": card["chronological_roles"],
        "outcome_columns_declared_by_feature_manifest": [],
        "pre_q4_manifest_proof": q4_proof,
        "parquet_files_decoded": 0,
        "economic_rows_decoded": 0,
        "outcomes_read": 0,
        "q4_rows_read": 0,
        "network_requests": 0,
        "data_purchases": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_mission_writes": 0,
        "economic_replay_started": False,
    }
    return {**core, "audit_hash": stable_hash(core)}


def run_economic_tripwire(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Run the explicitly authorised bounded pre-Q4 economic tripwire."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    verified = _verify_frozen_inputs(project, card["frozen_inputs"])
    minute = _load_economic_minutes(project, card)
    calendars, coverage = build_role_calendars(minute, card=card)
    event_sets: dict[str, dict[str, tuple[dict[str, Any], ...]]] = {}
    construction: dict[str, Any] = {}
    parent_reference, parent_counts = build_static_parent_control(
        minute, holding_minutes=15, card=card, project_root=project
    )
    for spec in card["candidate_lattice"]:
        candidate_id = str(spec["candidate_id"])
        holding = int(spec["holding_minutes"])
        signals = generate_recovery_signals(
            minute, candidate_id=candidate_id, holding_minutes=holding, card=card
        )
        primary, primary_counts = materialize_recovery_trades(
            minute, signals, holding_minutes=holding, card=card
        )
        flipped, flipped_counts = build_direction_flip_control(
            minute, primary, holding_minutes=holding, card=card
        )
        timing, timing_counts = build_session_timing_control(
            minute, primary, holding_minutes=holding, card=card
        )
        event_sets[candidate_id] = {
            PRIMARY: primary,
            "STATIC_EXTREME_REJECTION_PARENT": parent_reference,
            "DIRECTION_FLIP_SAME_EVENTS": flipped,
            "SESSION_MATCHED_TIMING_NULL": timing,
        }
        construction[candidate_id] = {
            "signal_count": len(signals),
            "event_counts": {
                key: len(value)
                for key, value in sorted(event_sets[candidate_id].items())
            },
            "primary_status_counts": primary_counts,
            "static_parent_status_counts": parent_counts,
            "direction_flip_status_counts": flipped_counts,
            "timing_null_status_counts": timing_counts,
        }
    power = _power_preflight(event_sets, card)
    rules, rule_receipt = exact._load_rule_snapshot(
        _inside(project, card["frozen_inputs"]["rule_snapshot"]["path"])
    )
    results = [
        evaluate_candidate(
            dict(spec),
            event_sets[str(spec["candidate_id"])],
            calendars=calendars,
            coverage=coverage,
            rules=rules,
            card=card,
        )
        for spec in card["candidate_lattice"]
    ]
    branch = _branch_gate(results, power, card)
    status = str(branch["status"])
    next_key = {
        "V73_RECOVERY_SPEED_GREEN_TIER_E": "when_green",
        "V73_RECOVERY_SPEED_WEAK": "when_weak",
        "V73_RECOVERY_SPEED_FALSIFIED": "when_falsified",
        "V73_EVENT_EXTREME_RECOVERY_SPEED_UNDERPOWERED_NO_THRESHOLD_RELAXATION": "when_underpowered",
    }[status]
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "decision": status,
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "tier_q_claimed": False,
        "tier_q_eligible": False,
        "complete_causal_evidence_bundle": False,
        "tier_g_or_c_claimed": False,
        "independent_confirmation_claimed": False,
        "account_replay_evidence_role": card["account_frontier"][
            "account_replay_evidence_role"
        ],
        "source_bindings": {
            "decision_card_path": str(card_path.relative_to(project)),
            "decision_card_hash": card["card_hash"],
            "verified_inputs": verified,
            "rule_snapshot": rule_receipt,
            "fill_policy_hashes": {
                str(holding): CausalFillPolicy().resolved_fingerprint("ES", int(holding))
                for holding in card["causal_contract"]["holding_variants_minutes"]
            },
        },
        "event_construction": construction,
        "role_calendar_counts": {
            role: len(calendars[role]["ES"]) for role in ROLES
        },
        "coverage": coverage,
        "power_preflight": power,
        "candidate_results": results,
        "branch_gate": branch,
        "q4_row_count": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_mission_writes": 0,
        "next_action": card["branch_rule"][next_key],
    }
    return {**core, "result_hash": stable_hash(core)}


def persist_economic_result(
    root: str | Path,
    result: Mapping[str, Any],
    *,
    output_root: str | Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    project = Path(root).resolve()
    folder = _inside(project, output_root)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "economic_result.json"
    if path.exists():
        existing = _read_json(path)
        if existing != dict(result):
            raise V73TripwireError("refusing to overwrite divergent V73 result")
    else:
        _atomic_json(path, result)
    return {
        "path": str(path.relative_to(project)),
        "sha256": _sha256(path),
        "result_hash": result["result_hash"],
    }


def generate_recovery_signals(
    minute: pd.DataFrame,
    *,
    candidate_id: str,
    holding_minutes: int,
    card: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Generate decisions only after the causal recovery is fully available."""

    frame = _prepare_minutes(minute, card=card)
    causal = card["causal_contract"]
    lookback = int(causal["prior_extreme_window_minutes"])
    deadlines = tuple(int(value) for value in causal["recovery_deadlines_minutes"])
    output: list[dict[str, Any]] = []
    for (_day, contract), raw in frame.groupby(
        ["local_date", "contract"], sort=True
    ):
        rows = raw.sort_values("minute_start_ns", kind="stable").reset_index(drop=True)
        starts = rows["minute_start_ns"].to_numpy(dtype=np.int64)
        fraction = _signed_fraction(rows)
        for position in range(lookback, len(rows)):
            prior = rows.iloc[position - lookback : position]
            if not _consecutive(prior["minute_start_ns"], expected_count=lookback):
                continue
            breach = rows.iloc[position]
            if int(starts[position]) != int(starts[position - 1]) + MINUTE_NS:
                continue
            prior_high = float(prior["high"].max())
            prior_low = float(prior["low"].min())
            q75 = float(np.quantile(np.abs(fraction[position - lookback : position]), 0.75))
            flow = float(fraction[position])
            side = 0
            breach_extreme = 0.0
            if (
                float(breach["high"]) > prior_high
                and flow > 0.0
                and abs(flow) >= q75
            ):
                side = -1
                breach_extreme = float(breach["high"])
            elif (
                float(breach["low"]) < prior_low
                and flow < 0.0
                and abs(flow) >= q75
            ):
                side = 1
                breach_extreme = float(breach["low"])
            if not side:
                continue
            for recovery_lag in deadlines:
                recovery_position = position + recovery_lag
                if recovery_position >= len(rows):
                    break
                recovery = rows.iloc[recovery_position]
                if int(starts[recovery_position]) != int(starts[position]) + recovery_lag * MINUTE_NS:
                    break
                recovered = (
                    float(recovery["close"]) <= prior_high
                    if side < 0
                    else float(recovery["close"]) >= prior_low
                )
                if not recovered or abs(float(fraction[recovery_position])) >= abs(flow):
                    continue
                causal_window = rows.iloc[
                    position - lookback : recovery_position + 1
                ]
                decision_ns = max(
                    int(causal_window["source_close_ns"].max()),
                    int(causal_window["availability_ns"].max()),
                )
                snapshot = {
                    "prior_high": prior_high,
                    "prior_low": prior_low,
                    "prior_q75_abs_signed_fraction": q75,
                    "breach_signed_fraction": flow,
                    "recovery_signed_fraction": float(fraction[recovery_position]),
                    "recovery_lag_minutes": recovery_lag,
                    "breach_extreme": breach_extreme,
                }
                output.append(
                    {
                        "signal_id": f"{candidate_id}:{decision_ns}",
                        "candidate_id": candidate_id,
                        "contract": str(breach["contract"]),
                        "side": side,
                        "breach_minute_start_ns": int(breach["minute_start_ns"]),
                        "recovery_minute_start_ns": int(recovery["minute_start_ns"]),
                        "decision_ns": decision_ns,
                        "availability_ns": decision_ns,
                        "breach_extreme": breach_extreme,
                        "recovery_lag_minutes": recovery_lag,
                        "holding_minutes": int(holding_minutes),
                        "feature_snapshot_hash": stable_hash(snapshot),
                    }
                )
                break
    unique = {str(row["signal_id"]): row for row in output}
    return tuple(sorted(unique.values(), key=lambda row: (int(row["decision_ns"]), str(row["signal_id"]))))


def materialize_recovery_trades(
    minute: pd.DataFrame,
    signals: Sequence[Mapping[str, Any]],
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], dict[str, int]]:
    frame = _prepare_minutes(minute, card=card)
    lookup = _minute_lookup(frame)
    output: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()
    last_exit = -1
    for signal in sorted(signals, key=lambda row: int(row["decision_ns"])):
        if int(signal["decision_ns"]) <= last_exit:
            counters["OVERLAPPING_SIGNAL_SUPPRESSED"] += 1
            continue
        event, status = _materialize_from_signal(
            lookup,
            signal,
            holding_minutes=holding_minutes,
            card=card,
            control=PRIMARY,
        )
        counters[status] += 1
        if event is not None:
            output.append(event)
            last_exit = int(event["exit_ns"])
    return tuple(output), dict(sorted(counters.items()))


def build_direction_flip_control(
    minute: pd.DataFrame,
    primary: Sequence[Mapping[str, Any]],
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], dict[str, int]]:
    frame = _prepare_minutes(minute, card=card)
    lookup = _minute_lookup(frame)
    raw_events: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()
    for source in primary:
        signal = {
            "signal_id": f"direction-flip:{source['event_id']}",
            "candidate_id": source["candidate_id"],
            "contract": source["target_contract"],
            "side": -int(source["side"]),
            "decision_ns": int(source["decision_ns"]),
            "availability_ns": int(source["decision_ns"]),
            "forced_entry_ns": int(source["entry_minute_start_ns"]),
            "risk_points": float(source["risk_points"]),
            "feature_snapshot_hash": stable_hash(
                {"control": "DIRECTION_FLIP_SAME_EVENTS", "source": source["event_id"]}
            ),
        }
        event, status = _materialize_from_signal(
            lookup,
            signal,
            holding_minutes=holding_minutes,
            card=card,
            control="DIRECTION_FLIP_SAME_EVENTS",
        )
        counters[status] += 1
        if event is not None:
            raw_events.append(event)
    events: list[dict[str, Any]] = []
    last_exit = -1
    for event in sorted(
        raw_events, key=lambda row: (int(row["decision_ns"]), str(row["event_id"]))
    ):
        if int(event["decision_ns"]) <= last_exit:
            counters["DIRECTION_FLIP_OVERLAP_SUPPRESSED"] += 1
            continue
        events.append(event)
        last_exit = int(event["exit_ns"])
    return tuple(events), dict(sorted(counters.items()))


def build_session_timing_control(
    minute: pd.DataFrame,
    primary: Sequence[Mapping[str, Any]],
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], dict[str, int]]:
    """Shift every event cyclically to another same-clock day inside its role."""

    frame = _prepare_minutes(minute, card=card)
    lookup = _minute_lookup(frame)
    by_day_minute: dict[tuple[str, int], pd.Series] = {}
    for row in frame.itertuples(index=False):
        by_day_minute[(str(row.local_date), int(row.local_minute))] = pd.Series(
            row._asdict()
        )
    role_days: dict[str, list[str]] = {}
    for role in ROLES:
        lower, upper = card["chronological_roles"][role]
        role_days[role] = sorted(
            value
            for value in set(frame["local_date"].astype(str))
            if str(lower) <= value < str(upper)
        )
    events: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()
    for source in primary:
        entry_local = pd.Timestamp(
            int(source["entry_ns"]), unit="ns", tz="UTC"
        ).tz_convert("America/Chicago")
        source_day = entry_local.strftime("%Y-%m-%d")
        role = _role_for_date(source_day, card)
        days = role_days.get(role, [])
        if len(days) < 2 or source_day not in days:
            counters["TIMING_NULL_UNDERPOWERED"] += 1
            continue
        target_day = days[(days.index(source_day) + 1) % len(days)]
        target = by_day_minute.get((target_day, entry_local.hour * 60 + entry_local.minute))
        if target is None or str(target["contract"]) != str(source["target_contract"]):
            counters["TIMING_NULL_MISSING_MATCH"] += 1
            continue
        entry_ns = int(target["minute_start_ns"])
        signal = {
            "signal_id": f"timing-null:{source['event_id']}:{entry_ns}",
            "candidate_id": source["candidate_id"],
            "contract": str(target["contract"]),
            "side": int(source["side"]),
            "decision_ns": entry_ns - 1,
            "availability_ns": entry_ns - 1,
            "forced_entry_ns": entry_ns,
            "risk_points": float(source["risk_points"]),
            "feature_snapshot_hash": stable_hash(
                {
                    "control": "SESSION_MATCHED_TIMING_NULL",
                    "source": source["event_id"],
                    "target_day": target_day,
                    "local_minute": entry_local.hour * 60 + entry_local.minute,
                }
            ),
        }
        event, status = _materialize_from_signal(
            lookup,
            signal,
            holding_minutes=holding_minutes,
            card=card,
            control="SESSION_MATCHED_TIMING_NULL",
        )
        counters[status] += 1
        if event is not None:
            events.append(event)
    events.sort(key=lambda row: (int(row["decision_ns"]), str(row["event_id"])))
    retained: list[dict[str, Any]] = []
    last_exit = -1
    for event in events:
        if int(event["decision_ns"]) <= last_exit:
            counters["TIMING_NULL_OVERLAP_SUPPRESSED"] += 1
            continue
        retained.append(event)
        last_exit = int(event["exit_ns"])
    return tuple(retained), dict(sorted(counters.items()))


def build_static_parent_control(
    minute: pd.DataFrame,
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
    project_root: str | Path = ".",
) -> tuple[tuple[dict[str, Any], ...], dict[str, int]]:
    """Recreate the prior five-minute ES delta-extreme rejection reference."""

    if int(holding_minutes) != 15:
        raise V73TripwireError("authoritative D1H5 parent holding must remain 15m")
    parent_frame = _prepare_parent_minutes(minute, card=card)
    frame = parent_frame.loc[parent_frame["product"].eq("ES")].copy()
    lookup = _minute_lookup(frame)
    population = generate_g2_signal_population(
        parent_frame, project_root=project_root
    )
    canonical = population["v7d1g2_delta_extreme_rejection_ES"]
    events: list[dict[str, Any]] = []
    counters: Counter[str] = Counter()
    for signal in canonical:
        mapped = {
            "signal_id": f"canonical-parent:{signal.decision_ns}",
            "candidate_id": signal.candidate_id,
            "contract": signal.contract,
            "side": signal.side,
            "decision_ns": signal.decision_ns,
            "availability_ns": signal.availability_ns,
            "forced_entry_ns": signal.entry_minute_start_ns,
            "forced_exit_ns": signal.exit_minute_start_ns,
            "feature_snapshot_hash": signal.feature_snapshot_hash,
        }
        event, status = _materialize_fixed_parent_signal(
            lookup,
            mapped,
            holding_minutes=holding_minutes,
            card=card,
        )
        counters[status] += 1
        if event is not None:
            events.append(event)
    return tuple(events), dict(sorted(counters.items()))


def build_role_calendars(
    minute: pd.DataFrame, *, card: Mapping[str, Any]
) -> tuple[dict[str, dict[str, tuple[int, ...]]], dict[str, Any]]:
    frame = _prepare_minutes(minute, card=card)
    exact_count = int(
        card["causal_contract"]["exact_observed_minutes_for_complete_session"]
    )
    clock_start, clock_end = map(
        _clock_minute,
        card["causal_contract"]["evaluation_session_window_chicago"],
    )
    calendars: dict[str, dict[str, tuple[int, ...]]] = {}
    coverage: dict[str, Any] = {}
    for role in ROLES:
        lower, upper = card["chronological_roles"][role]
        scoped = frame.loc[
            frame["local_date"].ge(str(lower)) & frame["local_date"].lt(str(upper))
        ]
        full: list[int] = []
        censored: list[int] = []
        for day, raw in scoped.groupby("local_date", sort=True):
            rows = raw.sort_values("minute_start_ns", kind="stable")
            day_int = int(str(day).replace("-", ""))
            complete = (
                len(rows) == exact_count == clock_end - clock_start
                and int(rows.iloc[0]["local_minute"]) == clock_start
                and int(rows.iloc[-1]["local_minute"]) == clock_end - 1
                and len(set(rows["contract"].astype(str))) == 1
                and _consecutive(rows["minute_start_ns"], expected_count=len(rows))
            )
            (full if complete else censored).append(day_int)
        observed = tuple(sorted(full + censored))
        calendars[role] = {"ES": observed}
        coverage[role] = {
            "ES": {
                "status": "FULL_COVERAGE" if not censored else "DATA_CENSORED",
                "full_coverage_days": full,
                "data_censored_days": censored,
                "full_coverage_day_count": len(full),
                "data_censored_day_count": len(censored),
                "preregistered_calendar_day_count": len(observed),
                "exact_required_minutes": exact_count,
            }
        }
    return calendars, coverage


def evaluate_candidate(
    proposal: Mapping[str, Any],
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    calendars: Mapping[str, Mapping[str, Sequence[int]]],
    coverage: Mapping[str, Any],
    rules: Mapping[str, Mapping[str, Any]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    discovery_cells: list[dict[str, Any]] = []
    for account_label in card["account_frontier"]["account_sizes"]:
        rule = dict(rules[str(account_label)])
        config = exact._account_config(rule)
        for risk in card["account_frontier"]["risk_fraction_of_current_mll_buffer"]:
            for horizon in card["account_frontier"]["horizons_trading_days"]:
                discovery_cells.append(
                    _evaluate_cell(
                        event_sets[PRIMARY],
                        calendar=calendars["DISCOVERY"]["ES"],
                        coverage=coverage["DISCOVERY"]["ES"],
                        config=config,
                        account_label=str(account_label),
                        market="ES",
                        micro_cap=int(rule["maximum_mini_contracts"]),
                        risk_fraction=float(risk),
                        horizon=int(horizon),
                        card=card,
                    )
                )
    frozen = max(discovery_cells, key=_discovery_rank)
    rule = dict(rules[str(frozen["account_label"])])
    config = exact._account_config(rule)
    evaluations: dict[str, dict[str, dict[str, Any]]] = {}
    for role in ROLES:
        evaluations[role] = {}
        for control, events in event_sets.items():
            if control == "STATIC_EXTREME_REJECTION_PARENT":
                evaluations[role][control] = {
                    "EVENT_REFERENCE": _event_reference_summary(
                        events, role=role, card=card
                    )
                }
                continue
            evaluations[role][control] = {}
            for horizon in card["account_frontier"]["horizons_trading_days"]:
                evaluations[role][control][str(horizon)] = _evaluate_cell(
                    events,
                    calendar=calendars[role]["ES"],
                    coverage=coverage[role]["ES"],
                    config=config,
                    account_label=str(frozen["account_label"]),
                    market="ES",
                    micro_cap=int(rule["maximum_mini_contracts"]),
                    risk_fraction=float(frozen["risk_fraction"]),
                    horizon=int(horizon),
                    card=card,
                )
    gate = _candidate_gate(event_sets, evaluations, card)
    return {
        "candidate_id": proposal["candidate_id"],
        "holding_minutes": int(proposal["holding_minutes"]),
        "frozen_discovery_cell": {
            key: frozen[key]
            for key in (
                "account_label",
                "account_size_usd",
                "risk_fraction",
                "horizon_trading_days",
                "micro_cap",
            )
        },
        "discovery_frontier_cell_count": len(discovery_cells),
        "evaluations": evaluations,
        "gate": gate,
        "evidence_tier": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "tier_q_claimed": False,
        "tier_q_eligible": False,
        "complete_causal_evidence_bundle": False,
        "tier_g_or_c_claimed": False,
        "independent_confirmation_claimed": False,
    }


def _materialize_fixed_parent_signal(
    lookup: Mapping[int, tuple[pd.DataFrame, int]],
    signal: Mapping[str, Any],
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str]:
    """Materialize the legacy parent's exact fixed open-to-open holding."""

    starts = np.asarray(sorted(lookup), dtype=np.int64)
    if "forced_entry_ns" in signal:
        entry_start_ns = int(signal["forced_entry_ns"])
        if entry_start_ns not in lookup:
            return None, "CENSORED_FUTURE_COVERAGE"
    else:
        position = int(
            np.searchsorted(starts, int(signal["decision_ns"]), side="left")
        )
        while position < len(starts):
            candidate_ns = int(starts[position])
            candidate_frame, candidate_position = lookup[candidate_ns]
            if int(candidate_frame.iloc[candidate_position]["first_trade_ns"]) > int(
                signal["decision_ns"]
            ):
                break
            position += 1
        if position >= len(starts):
            return None, "CENSORED_FUTURE_COVERAGE"
        entry_start_ns = int(starts[position])
    frame, entry_position = lookup[entry_start_ns]
    entry = frame.iloc[entry_position]
    if (
        str(entry["contract"]) != str(signal["contract"])
        or int(entry["first_trade_ns"]) <= int(signal["decision_ns"])
        or entry_position <= 0
        or int(frame.iloc[entry_position - 1]["minute_start_ns"]) + MINUTE_NS
        != entry_start_ns
    ):
        return None, "CENSORED_OR_NONEXECUTABLE_ENTRY"
    if "forced_exit_ns" in signal:
        forced_exit = int(signal["forced_exit_ns"])
        if forced_exit not in lookup:
            return None, "CENSORED_FUTURE_COVERAGE"
        exit_frame, exit_position = lookup[forced_exit]
        if not exit_frame.equals(frame):
            return None, "CENSORED_FUTURE_COVERAGE"
    else:
        exit_position = entry_position + int(holding_minutes)
    if exit_position >= len(frame):
        return None, "CENSORED_FUTURE_COVERAGE"
    held = frame.iloc[entry_position:exit_position]
    exit_row = frame.iloc[exit_position]
    if (
        not _consecutive(held["minute_start_ns"], expected_count=int(holding_minutes))
        or int(exit_row["minute_start_ns"])
        != entry_start_ns + int(holding_minutes) * MINUTE_NS
        or len(set(frame.iloc[entry_position : exit_position + 1]["contract"].astype(str)))
        != 1
        or len(set(frame.iloc[entry_position : exit_position + 1]["local_date"].astype(str)))
        != 1
    ):
        return None, "CENSORED_FUTURE_COVERAGE"
    exit_local = pd.Timestamp(
        int(exit_row["first_trade_ns"]), unit="ns", tz="UTC"
    ).tz_convert("America/Chicago")
    if exit_local.hour * 60 + exit_local.minute > _clock_minute(
        card["causal_contract"]["mandatory_flatten_local"]
    ):
        return None, "SESSION_FLATTEN_VIOLATION"
    instrument = instrument_spec("ES")
    entry_price = float(entry["open"])
    exit_price = float(exit_row["open"])
    side = int(signal["side"])
    payload = CausalFillPolicy().resolved_payload("ES", int(holding_minutes))
    commission = float(payload["commission_round_turn_usd"])
    normal_cost = commission + 2.0 * float(
        payload["normal_slippage_ticks_per_side"]
    ) * float(instrument.tick_value)
    stressed_cost = commission + 2.0 * float(
        payload["stressed_slippage_ticks_per_side"]
    ) * float(instrument.tick_value)
    gross = side * (exit_price - entry_price) * instrument.point_value
    favorable = (
        (float(held["high"].max()) - entry_price) * instrument.point_value
        if side > 0
        else (entry_price - float(held["low"].min())) * instrument.point_value
    )
    adverse = (
        (float(held["low"].min()) - entry_price) * instrument.point_value
        if side > 0
        else (entry_price - float(held["high"].max())) * instrument.point_value
    )
    session_day = int(str(entry["local_date"]).replace("-", ""))
    event_core = {
        "legacy_parent": "v7d1g2_delta_extreme_rejection_ES",
        "source_signal_id": str(signal["signal_id"]),
        "decision_ns": int(signal["decision_ns"]),
        "entry_ns": int(entry["first_trade_ns"]),
        "exit_ns": int(exit_row["minute_start_ns"]),
        "exit_minute_start_ns": int(exit_row["minute_start_ns"]),
        "holding_minutes": int(holding_minutes),
        "feature_snapshot_hash": str(signal["feature_snapshot_hash"]),
        "fill_policy_hash": CausalFillPolicy().resolved_fingerprint(
            "ES", int(holding_minutes)
        ),
    }
    return {
        "event_id": f"{signal['candidate_id']}:STATIC_PARENT:{signal['decision_ns']}",
        "candidate_id": str(signal["candidate_id"]),
        "control": "STATIC_EXTREME_REJECTION_PARENT",
        "session_day": session_day,
        "block": _temporal_context(session_day),
        "local_minute": int(entry["local_minute"]),
        "decision_ns": int(signal["decision_ns"]),
        "entry_ns": int(entry["first_trade_ns"]),
        "entry_minute_start_ns": entry_start_ns,
        "exit_ns": int(exit_row["minute_start_ns"]),
        "exit_minute_start_ns": int(exit_row["minute_start_ns"]),
        "exit_first_trade_ns": int(exit_row["first_trade_ns"]),
        "side": side,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_reason": "LEGACY_FIXED_HOLDING_EXIT",
        "gross_one_micro": gross,
        "favorable_one_micro": favorable,
        "adverse_one_micro": adverse,
        "normal_net_one_micro": gross - normal_cost,
        "stressed_net_one_micro": gross - stressed_cost,
        "normal_cost_one_micro": normal_cost,
        "stressed_cost_one_micro": stressed_cost,
        "target_contract": str(entry["contract"]),
        "feature_hash": stable_hash(event_core),
        "same_bar_exit_stop_first": False,
        "session_compliant": True,
        "account_control_eligible": False,
    }, "TRADE_CREATED"


def _materialize_from_signal(
    lookup: Mapping[int, tuple[pd.DataFrame, int]],
    signal: Mapping[str, Any],
    *,
    holding_minutes: int,
    card: Mapping[str, Any],
    control: str,
) -> tuple[dict[str, Any] | None, str]:
    starts = np.asarray(sorted(lookup), dtype=np.int64)
    if "forced_entry_ns" in signal:
        entry_ns = int(signal["forced_entry_ns"])
        if entry_ns not in lookup:
            return None, "CENSORED_FUTURE_COVERAGE"
    else:
        position = int(np.searchsorted(starts, int(signal["decision_ns"]), side="left"))
        while (
            position < len(starts)
            and int(lookup[int(starts[position])][0].iloc[lookup[int(starts[position])][1]]["first_trade_ns"])
            <= int(signal["decision_ns"])
        ):
            position += 1
        if position >= len(starts):
            return None, "CENSORED_FUTURE_COVERAGE"
        entry_ns = int(starts[position])
    frame, entry_position = lookup[entry_ns]
    entry = frame.iloc[entry_position]
    decision_ns = int(signal["decision_ns"])
    if (
        str(entry["contract"]) != str(signal["contract"])
        or int(entry["first_trade_ns"]) <= decision_ns
        or entry_position <= 0
        or int(frame.iloc[entry_position - 1]["minute_start_ns"]) + MINUTE_NS != entry_ns
    ):
        return None, "CENSORED_OR_NONEXECUTABLE_ENTRY"
    if "recovery_minute_start_ns" in signal:
        recovery_ns = int(signal["recovery_minute_start_ns"])
        recovery_local = pd.Timestamp(
            recovery_ns, unit="ns", tz="UTC"
        ).tz_convert("America/Chicago")
        if (
            entry_ns < recovery_ns + MINUTE_NS
            or str(entry["local_date"]) != recovery_local.strftime("%Y-%m-%d")
        ):
            return None, "CENSORED_OR_NONEXECUTABLE_ENTRY"
    entry_local = pd.Timestamp(entry_ns, unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    )
    flatten_minute = _clock_minute(card["causal_contract"]["mandatory_flatten_local"])
    entry_minute = entry_local.hour * 60 + entry_local.minute
    if entry_minute + int(holding_minutes) > flatten_minute:
        return None, "SESSION_FLATTEN_ENTRY_REJECT"
    instrument = instrument_spec("ES")
    raw_entry = float(entry["open"])
    side = int(signal["side"])
    if "risk_points" in signal:
        risk_points = float(signal["risk_points"])
    else:
        breach_extreme = float(signal["breach_extreme"])
        stop = breach_extreme - instrument.tick_size if side > 0 else breach_extreme + instrument.tick_size
        risk_points = side * (raw_entry - stop)
    if not math.isfinite(risk_points) or risk_points <= 0.0:
        return None, "INVALID_CAUSAL_STOP_GEOMETRY"
    stop = raw_entry - side * risk_points
    target = raw_entry + side * float(card["causal_contract"]["target_r_multiple"]) * risk_points
    exit_price: float | None = None
    exit_ns: int | None = None
    exit_reason = ""
    highs: list[float] = []
    lows: list[float] = []
    for offset in range(int(holding_minutes)):
        position = entry_position + offset
        if position >= len(frame):
            return None, "CENSORED_FUTURE_COVERAGE"
        row = frame.iloc[position]
        if (
            int(row["minute_start_ns"]) != entry_ns + offset * MINUTE_NS
            or str(row["contract"]) != str(entry["contract"])
            or str(row["local_date"]) != str(entry["local_date"])
        ):
            return None, "CENSORED_FUTURE_COVERAGE"
        high = float(row["high"])
        low = float(row["low"])
        highs.append(high)
        lows.append(low)
        if side > 0:
            stop_touch = low <= stop
            target_touch = high >= target
            if stop_touch:
                exit_price = min(stop, float(row["open"]))
                exit_reason = "STOP_FIRST" if target_touch else "STOP"
            elif target_touch:
                exit_price = target
                exit_reason = "TARGET"
        else:
            stop_touch = high >= stop
            target_touch = low <= target
            if stop_touch:
                exit_price = max(stop, float(row["open"]))
                exit_reason = "STOP_FIRST" if target_touch else "STOP"
            elif target_touch:
                exit_price = target
                exit_reason = "TARGET"
        if exit_price is not None:
            exit_ns = int(row["source_close_ns"])
            break
    if exit_price is None:
        position = entry_position + int(holding_minutes)
        if position >= len(frame):
            return None, "CENSORED_FUTURE_COVERAGE"
        row = frame.iloc[position]
        if (
            int(row["minute_start_ns"]) != entry_ns + int(holding_minutes) * MINUTE_NS
            or str(row["contract"]) != str(entry["contract"])
            or str(row["local_date"]) != str(entry["local_date"])
        ):
            return None, "CENSORED_FUTURE_COVERAGE"
        exit_price = float(row["open"])
        exit_ns = int(row["first_trade_ns"])
        exit_reason = "TIME_EXIT_NEXT_MINUTE_OPEN"
    if exit_ns is None:
        raise V73TripwireError("materialized trade has no causal exit timestamp")
    exit_local = pd.Timestamp(exit_ns, unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    )
    if exit_local.hour * 60 + exit_local.minute > flatten_minute:
        return None, "SESSION_FLATTEN_VIOLATION"
    payload = CausalFillPolicy().resolved_payload("ES", int(holding_minutes))
    commission = float(payload["commission_round_turn_usd"])
    normal_cost = commission + 2.0 * float(payload["normal_slippage_ticks_per_side"]) * float(instrument.tick_value)
    stressed_cost = commission + 2.0 * float(payload["stressed_slippage_ticks_per_side"]) * float(instrument.tick_value)
    gross = side * (float(exit_price) - raw_entry) * float(instrument.point_value)
    favorable = (
        (max(highs, default=raw_entry) - raw_entry) * instrument.point_value
        if side > 0
        else (raw_entry - min(lows, default=raw_entry)) * instrument.point_value
    )
    adverse = (
        (min(lows, default=raw_entry) - raw_entry) * instrument.point_value
        if side > 0
        else (raw_entry - max(highs, default=raw_entry)) * instrument.point_value
    )
    session_day = int(str(entry["local_date"]).replace("-", ""))
    event_core = {
        "source_signal_id": str(signal["signal_id"]),
        "control": control,
        "decision_ns": decision_ns,
        "entry_ns": int(entry["first_trade_ns"]),
        "entry_minute_start_ns": entry_ns,
        "exit_ns": exit_ns,
        "side": side,
        "risk_points": risk_points,
        "holding_minutes": int(holding_minutes),
        "feature_snapshot_hash": str(signal["feature_snapshot_hash"]),
        "fill_policy_hash": CausalFillPolicy().resolved_fingerprint("ES", int(holding_minutes)),
    }
    return {
        "event_id": f"{signal['candidate_id']}:{control}:{decision_ns}:{entry_ns}",
        "candidate_id": str(signal["candidate_id"]),
        "control": control,
        "session_day": session_day,
        "block": _temporal_context(session_day),
        "local_minute": entry_minute,
        "decision_ns": decision_ns,
        "entry_ns": int(entry["first_trade_ns"]),
        "entry_minute_start_ns": entry_ns,
        "exit_ns": exit_ns,
        "side": side,
        "entry_price": raw_entry,
        "exit_price": float(exit_price),
        "stop_price": stop,
        "target_price": target,
        "risk_points": risk_points,
        "exit_reason": exit_reason,
        "gross_one_micro": gross,
        "favorable_one_micro": favorable,
        "adverse_one_micro": adverse,
        "normal_net_one_micro": gross - normal_cost,
        "stressed_net_one_micro": gross - stressed_cost,
        "normal_cost_one_micro": normal_cost,
        "stressed_cost_one_micro": stressed_cost,
        "stop_risk_one_micro": risk_points * instrument.point_value + stressed_cost,
        "target_contract": str(entry["contract"]),
        "feature_hash": stable_hash(event_core),
        "same_bar_exit_stop_first": exit_reason == "STOP_FIRST",
        "session_compliant": True,
    }, "TRADE_CREATED"


def _candidate_gate(
    event_sets: Mapping[str, Sequence[Mapping[str, Any]]],
    evaluations: Mapping[str, Mapping[str, Mapping[str, Any]]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    gate = card["frozen_gate"]
    headline = {
        role: _headline_cell(evaluations[role][PRIMARY]) for role in ROLES
    }
    validation = headline["VALIDATION"]["stressed"]
    final = headline["FINAL_DEVELOPMENT"]["stressed"]
    combined_normal = sum(
        max(
            int(evaluations[role][PRIMARY][str(h)]["normal"]["pass_count"])
            for h in gate["qualifying_horizons_trading_days"]
        )
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    )
    combined_stressed = sum(
        max(
            int(evaluations[role][PRIMARY][str(h)]["stressed"]["pass_count"])
            for h in gate["qualifying_horizons_trading_days"]
        )
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    )
    all_final_cells = [
        evaluations[role][PRIMARY][str(h)]
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        for h in gate["qualifying_horizons_trading_days"]
    ]
    contexts = {
        role: _positive_context_count(event_sets[PRIMARY], role, card)
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    }
    concentration = {
        role: _positive_trade_concentration(event_sets[PRIMARY], role, card)
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    }
    combined_events = [
        row
        for row in event_sets[PRIMARY]
        if _role_for_day(int(row["session_day"]), card)
        in {"VALIDATION", "FINAL_DEVELOPMENT"}
    ]
    positive = [max(float(row["stressed_net_one_micro"]), 0.0) for row in combined_events]
    concentration["COMBINED"] = max(positive, default=0.0) / max(sum(positive), 1e-12)
    control_beats: dict[str, bool] = {}
    control_deltas: dict[str, Any] = {}
    primary_metric = _combined_control_metric(evaluations, PRIMARY, gate)
    for control in CONTROLS:
        if control == "STATIC_EXTREME_REJECTION_PARENT":
            primary_reference = _event_reference_summary(
                event_sets[PRIMARY],
                role=("VALIDATION", "FINAL_DEVELOPMENT"),
                card=card,
            )
            parent_reference = _event_reference_summary(
                event_sets[control],
                role=("VALIDATION", "FINAL_DEVELOPMENT"),
                card=card,
            )
            passed = (
                int(primary_reference["event_count"]) > 0
                and int(parent_reference["event_count"]) > 0
                and float(primary_reference["stressed_expectancy_per_event_usd"])
                > float(parent_reference["stressed_expectancy_per_event_usd"])
                and float(primary_reference["stressed_net_total_usd"]) >= 0.0
            )
            control_beats[control] = passed
            control_deltas[control] = {
                "comparison_basis": "EXACT_ONE_CONTRACT_EVENT_EXPECTANCY",
                "stressed_expectancy_per_event_usd": float(
                    primary_reference["stressed_expectancy_per_event_usd"]
                )
                - float(parent_reference["stressed_expectancy_per_event_usd"]),
                "primary_event_count": int(primary_reference["event_count"]),
                "parent_event_count": int(parent_reference["event_count"]),
            }
            continue
        metric = _combined_control_metric(evaluations, control, gate)
        passed = (
            primary_metric["stressed_pass_count"] > metric["stressed_pass_count"]
            or (
                primary_metric["stressed_target_progress_median"]
                > metric["stressed_target_progress_median"]
                and primary_metric["stressed_net_total_usd"]
                >= metric["stressed_net_total_usd"]
            )
        )
        control_beats[control] = passed
        control_deltas[control] = {
            key: primary_metric[key] - metric[key] for key in primary_metric
        }
    checks = {
        "full_coverage_denominator_in_each_heldout_role": all(
            any(
                int(
                    evaluations[role][PRIMARY][str(h)]["stressed"][
                        "full_coverage_episode_count"
                    ]
                )
                > 0
                for h in gate["qualifying_horizons_trading_days"]
            )
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        ),
        "positive_validation_stressed_net": float(validation["net_total_usd"]) > 0.0,
        "positive_final_development_stressed_net": float(final["net_total_usd"]) > 0.0,
        "minimum_combined_normal_passes": combined_normal >= int(gate["minimum_combined_normal_passes"]),
        "minimum_combined_stressed_passes": combined_stressed >= int(gate["minimum_combined_stressed_passes"]),
        "positive_validation_context": contexts["VALIDATION"] >= 1,
        "positive_final_development_context": contexts["FINAL_DEVELOPMENT"] >= 1,
        "controlled_stressed_mll": max(
            float(cell["stressed"]["mll_breach_rate"]) for cell in all_final_cells
        ) <= float(gate["maximum_stressed_mll_breach_rate"]),
        "passing_paths_consistency": all(
            int(cell[scenario]["pass_count"]) == 0
            or bool(cell[scenario]["all_passing_paths_consistency_compliant"])
            for cell in all_final_cells
            for scenario in ("normal", "stressed")
        ),
        "nonnegative_stressed_p25": min(
            float(cell["stressed"]["target_progress_p25"])
            for cell in all_final_cells
        ) >= float(gate["minimum_stressed_target_progress_p25"]),
        "no_single_trade_domination": max(concentration.values())
        <= float(gate["maximum_single_trade_positive_profit_concentration"]),
        "beats_all_controls": all(control_beats.values()),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "conservative_unique_normal_pass_lower_bound": combined_normal,
        "conservative_unique_stressed_pass_lower_bound": combined_stressed,
        "positive_temporal_contexts": contexts,
        "single_trade_positive_profit_concentration": concentration,
        "control_beats": control_beats,
        "control_deltas": control_deltas,
        "headline_horizons": {
            role: int(headline[role]["horizon_trading_days"]) for role in ROLES
        },
    }


def _branch_gate(
    candidates: Sequence[Mapping[str, Any]],
    power: Mapping[str, Any],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    if not bool(power["passed"]):
        status = card["power_preflight"]["when_underpowered"]
    elif any(bool(row["gate"]["passed"]) for row in candidates):
        status = "V73_RECOVERY_SPEED_GREEN_TIER_E"
    elif any(
        float(_headline_cell(row["evaluations"]["FINAL_DEVELOPMENT"][PRIMARY])["stressed"]["net_total_usd"]) > 0.0
        for row in candidates
    ):
        status = "V73_RECOVERY_SPEED_WEAK"
    else:
        status = "V73_RECOVERY_SPEED_FALSIFIED"
    return {
        "passed": status == "V73_RECOVERY_SPEED_GREEN_TIER_E",
        "status": status,
        "tier_e_candidate_ids": [
            str(row["candidate_id"]) for row in candidates if bool(row["gate"]["passed"])
        ],
        "tier_q_candidate_ids": [],
        "tier_g_or_c_claimed": False,
    }


def _power_preflight(
    event_sets: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    required = card["power_preflight"]["minimum_independent_primary_events"]
    rows: dict[str, Any] = {}
    passed = True
    for candidate, controls in sorted(event_sets.items()):
        counts: dict[str, int] = {}
        checks: dict[str, bool] = {}
        for role in ROLES:
            count = sum(
                _role_for_day(int(row["session_day"]), card) == role
                for row in controls[PRIMARY]
            )
            counts[role] = count
            checks[role] = count >= int(required[role])
        control_power: dict[str, dict[str, bool]] = {}
        control_counts: dict[str, dict[str, int]] = {}
        for control in CONTROLS:
            control_counts[control] = {}
            control_power[control] = {}
            for role in ROLES:
                control_count = sum(
                    _role_for_day(int(row["session_day"]), card) == role
                    for row in controls[control]
                )
                control_counts[control][role] = control_count
                threshold = (
                    int(required[role])
                    if control == "STATIC_EXTREME_REJECTION_PARENT"
                    else math.ceil(0.8 * counts[role])
                )
                control_power[control][role] = control_count >= threshold
        checks["role_local_control_power"] = all(
            value
            for by_role in control_power.values()
            for value in by_role.values()
        )
        passed = passed and all(checks.values())
        rows[candidate] = {
            "primary_event_counts": counts,
            "checks": checks,
            "control_event_counts_by_role": control_counts,
            "control_power": control_power,
        }
    return {"passed": passed, "thresholds": required, "candidates": rows}


def _load_economic_minutes(project: Path, card: Mapping[str, Any]) -> pd.DataFrame:
    path = _inside(project, card["frozen_inputs"]["minute_print_store"]["path"])
    columns = [
        "product",
        "contract",
        "calendar_year",
        "minute_start_ns",
        "source_close_ns",
        "availability_ns",
        "first_trade_ns",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        "signed_aggressor_volume",
    ]
    return pd.read_parquet(path, columns=columns)


def _prepare_minutes(minute: pd.DataFrame, *, card: Mapping[str, Any]) -> pd.DataFrame:
    required = {
        "product",
        "contract",
        "calendar_year",
        "minute_start_ns",
        "source_close_ns",
        "availability_ns",
        "first_trade_ns",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        "signed_aggressor_volume",
    }
    missing = required - set(minute.columns)
    if missing:
        raise V73TripwireError(f"minute store missing columns: {sorted(missing)}")
    frame = minute.loc[minute["product"].eq("ES")].copy()
    if frame.empty:
        raise V73TripwireError("minute store has no ES rows")
    frame = frame.sort_values("minute_start_ns", kind="stable").reset_index(drop=True)
    if frame["minute_start_ns"].duplicated().any():
        raise V73TripwireError("duplicate ES minute timestamp")
    local = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["local_date"] = local.strftime("%Y-%m-%d")
    frame["local_minute"] = local.hour * 60 + local.minute
    lower, upper = map(
        _clock_minute,
        card["causal_contract"]["evaluation_session_window_chicago"],
    )
    frame = frame.loc[
        frame["local_minute"].ge(lower) & frame["local_minute"].lt(upper)
    ].reset_index(drop=True)
    if frame["local_date"].ge(card["causal_contract"]["q4_end_exclusive"]).any():
        raise V73TripwireError("Q4 row encountered in V73 economic input")
    return frame


def _prepare_parent_minutes(
    minute: pd.DataFrame, *, card: Mapping[str, Any]
) -> pd.DataFrame:
    """Preserve the canonical ES/MES synchronized parent input universe."""

    required = {
        "product",
        "contract",
        "calendar_year",
        "minute_start_ns",
        "source_close_ns",
        "availability_ns",
        "first_trade_ns",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        "signed_aggressor_volume",
    }
    missing = required - set(minute.columns)
    if missing:
        raise V73TripwireError(f"parent minute store missing columns: {sorted(missing)}")
    frame = minute.loc[minute["product"].isin(("ES", "MES"))].copy()
    if set(frame["product"].astype(str)) != {"ES", "MES"}:
        raise V73TripwireError("canonical parent requires both ES and MES")
    frame = frame.sort_values(
        ["product", "minute_start_ns"], kind="stable"
    ).reset_index(drop=True)
    if frame.duplicated(["product", "minute_start_ns"]).any():
        raise V73TripwireError("duplicate ES/MES parent minute timestamp")
    local = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["local_date"] = local.strftime("%Y-%m-%d")
    frame["local_minute"] = local.hour * 60 + local.minute
    lower, upper = map(
        _clock_minute,
        card["causal_contract"]["evaluation_session_window_chicago"],
    )
    frame = frame.loc[
        frame["local_minute"].ge(lower) & frame["local_minute"].lt(upper)
    ].reset_index(drop=True)
    if frame["local_date"].ge(card["causal_contract"]["q4_end_exclusive"]).any():
        raise V73TripwireError("Q4 row encountered in canonical parent input")
    return frame


def _minute_lookup(frame: pd.DataFrame) -> dict[int, tuple[pd.DataFrame, int]]:
    output: dict[int, tuple[pd.DataFrame, int]] = {}
    for (_day, _contract), raw in frame.groupby(
        ["local_date", "contract"], sort=True
    ):
        rows = raw.sort_values("minute_start_ns", kind="stable").reset_index(drop=True)
        for position, value in enumerate(rows["minute_start_ns"].to_numpy(dtype=np.int64)):
            output[int(value)] = (rows, position)
    return output


def _signed_fraction(frame: pd.DataFrame) -> np.ndarray:
    signed = frame["signed_aggressor_volume"].to_numpy(dtype=float)
    total = frame["total_volume"].to_numpy(dtype=float)
    return np.divide(signed, total, out=np.zeros_like(signed), where=total > 0.0)


def _consecutive(values: Sequence[Any], *, expected_count: int) -> bool:
    array = np.asarray(values, dtype=np.int64)
    return len(array) == int(expected_count) and (
        len(array) < 2 or bool(np.all(np.diff(array) == MINUTE_NS))
    )


def _discovery_rank(cell: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        int(cell["stressed"]["pass_count"]),
        int(cell["normal"]["pass_count"]),
        float(cell["stressed"]["target_progress_p25"]),
        float(cell["stressed"]["target_progress_median"]),
        float(cell["stressed"]["net_total_usd"]),
        -float(cell["stressed"]["mll_breach_rate"]),
        -int(cell["horizon_trading_days"]),
        -int(cell["account_size_usd"]),
        -float(cell["risk_fraction"]),
    )


def _headline_cell(cells: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    eligible = [
        dict(cell)
        for _horizon, cell in cells.items()
        if int(cell["stressed"]["full_coverage_episode_count"]) > 0
    ]
    if not eligible:
        return dict(cells[min(cells, key=lambda value: int(value))])
    return max(eligible, key=lambda cell: int(cell["horizon_trading_days"]))


def _combined_control_metric(
    evaluations: Mapping[str, Mapping[str, Mapping[str, Any]]],
    control: str,
    gate: Mapping[str, Any],
) -> dict[str, float]:
    cells = [
        _headline_cell(evaluations[role][control])
        for role in ("VALIDATION", "FINAL_DEVELOPMENT")
    ]
    return {
        "stressed_pass_count": float(
            sum(int(cell["stressed"]["pass_count"]) for cell in cells)
        ),
        "stressed_target_progress_median": float(
            statistics.median(
                float(cell["stressed"]["target_progress_median"]) for cell in cells
            )
        ),
        "stressed_net_total_usd": float(
            sum(float(cell["stressed"]["net_total_usd"]) for cell in cells)
        ),
    }


def _event_reference_summary(
    events: Sequence[Mapping[str, Any]],
    *,
    role: str | Sequence[str],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    roles = {str(role)} if isinstance(role, str) else {str(value) for value in role}
    selected = [
        row
        for row in events
        if _role_for_day(int(row["session_day"]), card) in roles
    ]
    normal = [float(row["normal_net_one_micro"]) for row in selected]
    stressed = [float(row["stressed_net_one_micro"]) for row in selected]
    return {
        "comparison_basis": "EXACT_ONE_CONTRACT_EVENT_REFERENCE",
        "event_count": len(selected),
        "normal_net_total_usd": float(sum(normal)),
        "stressed_net_total_usd": float(sum(stressed)),
        "normal_expectancy_per_event_usd": float(sum(normal) / len(normal))
        if normal
        else 0.0,
        "stressed_expectancy_per_event_usd": float(sum(stressed) / len(stressed))
        if stressed
        else 0.0,
    }


def _positive_context_count(
    events: Sequence[Mapping[str, Any]], role: str, card: Mapping[str, Any]
) -> int:
    values: dict[str, float] = defaultdict(float)
    for row in events:
        if _role_for_day(int(row["session_day"]), card) == role:
            values[str(row["block"])] += float(row["stressed_net_one_micro"])
    return sum(value > 0.0 for value in values.values())


def _positive_trade_concentration(
    events: Sequence[Mapping[str, Any]], role: str, card: Mapping[str, Any]
) -> float:
    profits = [
        max(float(row["stressed_net_one_micro"]), 0.0)
        for row in events
        if _role_for_day(int(row["session_day"]), card) == role
    ]
    return max(profits, default=0.0) / max(sum(profits), 1e-12)


def _role_for_day(day: int, card: Mapping[str, Any]) -> str | None:
    value = f"{int(day):08d}"
    formatted = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    try:
        return _role_for_date(formatted, card)
    except V73TripwireError:
        return None


def _role_for_date(value: str, card: Mapping[str, Any]) -> str:
    matches = [
        role
        for role in ROLES
        if str(card["chronological_roles"][role][0])
        <= str(value)
        < str(card["chronological_roles"][role][1])
    ]
    if len(matches) != 1:
        raise V73TripwireError(f"date outside unique frozen role: {value}")
    return matches[0]


def _temporal_context(day: int) -> str:
    value = pd.Timestamp(str(int(day)))
    iso = value.isocalendar()
    return f"{iso.year}-W{int(iso.week):02d}"


def _verify_frozen_inputs(
    project: Path, frozen_inputs: Mapping[str, Mapping[str, Any]]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for name, binding in sorted(frozen_inputs.items()):
        path = _inside(project, binding["path"])
        actual = _sha256(path)
        expected = str(binding["sha256"])
        if actual != expected:
            raise V73TripwireError(f"frozen input hash mismatch: {name}")
        output[name] = {
            "path": str(path.relative_to(project)),
            "sha256": actual,
            "byte_count": path.stat().st_size,
        }
    return output


def _manifest_q4_proof(
    manifest: Mapping[str, Any],
    q4_end: str,
    *,
    roles: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    intervals = [
        (
            pd.Timestamp(int(row["slice_start_ns"]), unit="ns", tz="UTC"),
            pd.Timestamp(int(row["slice_end_ns"]), unit="ns", tz="UTC"),
        )
        for row in manifest.get("sources", ())
    ]
    ends = [upper for _lower, upper in intervals]
    boundary = pd.Timestamp(q4_end, tz="UTC")
    role_coverage = {
        role: any(
            lower <= pd.Timestamp(bounds[0], tz="UTC")
            and pd.Timestamp(bounds[1], tz="UTC") <= upper
            for lower, upper in intervals
        )
        for role, bounds in roles.items()
    }
    passed = (
        bool(ends)
        and all(value <= boundary for value in ends)
        and int(manifest.get("q4_access_count_delta", -1)) == 0
        and all(role_coverage.values())
    )
    if not passed:
        raise V73TripwireError("feature manifest does not prove pre-Q4 bounds")
    return {
        "passed": True,
        "maximum_source_end_exclusive": max(ends).isoformat(),
        "q4_boundary": boundary.isoformat(),
        "q4_access_count_delta": 0,
        "chronological_role_coverage": role_coverage,
    }


def _clock_minute(value: str) -> int:
    hour, minute = (int(part) for part in str(value).split(":"))
    return hour * 60 + minute


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise V73TripwireError(f"path escapes project root: {value}") from exc
    if not resolved.is_file() and resolved.suffix:
        # Output directories are checked by callers after this helper.
        if not any(part == "reports" for part in resolved.parts):
            raise V73TripwireError(f"required file missing: {value}")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise V73TripwireError(f"expected JSON object: {path}")
    return value


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


__all__ = [
    "BRANCH_ID",
    "CONTROLS",
    "DEFAULT_CARD",
    "V73TripwireError",
    "audit_only",
    "build_direction_flip_control",
    "build_role_calendars",
    "build_session_timing_control",
    "build_static_parent_control",
    "evaluate_candidate",
    "generate_recovery_signals",
    "load_decision_card",
    "materialize_recovery_trades",
    "persist_economic_result",
    "run_economic_tripwire",
]
