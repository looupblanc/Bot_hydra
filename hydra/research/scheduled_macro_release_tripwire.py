"""Bounded, read-only scheduled macro release reaction tripwire.

This module evaluates one small preregistered lattice around official CPI,
Employment Situation and FOMC statement timestamps.  The release clock is an
exogenous public input; price direction is decided only after the frozen
post-release observation interval has completed.  Exact mapped mini bars drive
the signal and exact mapped micro bars provide next-tradable-open execution.

The 2023 role selects at most one candidate and legal account cell per market.
2024 Q1 validation and 2024 Q2-Q3 final-development never participate in that
selection.  Results are development-only, perform no writes, and cannot create
Tier Q/G/C/F status.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.economic_evolution.schema import stable_hash
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _globex_session_fields,
)
from hydra.production import autonomous_exact_replay as exact
from hydra.production.fresh_confirmation_lane import non_overlapping_starts
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.scaling_plan import mini_equivalent


SCHEMA = "hydra_scheduled_macro_release_causal_reaction_tripwire_v1"
BRANCH_ID = "SCHEDULED_MACRO_RELEASE_CAUSAL_REACTION_TRIPWIRE_V1"
DEFAULT_CARD = Path("config/research/scheduled_macro_release_causal_reaction_tripwire_v1.json")
RELEASE_FAMILIES = (
    "BLS_CPI",
    "BLS_EMPLOYMENT_SITUATION",
    "FOMC_STATEMENT",
)
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
EVALUATION_ROLES = ("VALIDATION", "FINAL_DEVELOPMENT")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
CONTROLS = ("PRIMARY", "DIRECTION_FLIP", "RELEASE_TIME_SESSION_MATCHED")


class ScheduledMacroReleaseTripwireError(RuntimeError):
    """The frozen source, causal contract, or exact account contract drifted."""


def load_decision_card(path: str | Path = DEFAULT_CARD) -> dict[str, Any]:
    card = _read_json(Path(path))
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or stable_hash(core) != claimed:
        raise ScheduledMacroReleaseTripwireError("decision-card hash drift")
    governance = dict(card.get("governance") or {})
    exclusion = dict(card.get("explicit_non_resurrection") or {})
    if (
        card.get("selected_branch") != BRANCH_ID
        or not bool(governance.get("read_only"))
        or any(
            bool(governance.get(key))
            for key in (
                "promotion_allowed",
                "tier_q_allowed",
                "q4_access_allowed",
                "data_purchase_allowed",
                "broker_connection_allowed",
                "orders_allowed",
            )
        )
        or "EIA_PETROLEUM_STATUS_REPORT"
        not in set(exclusion.get("excluded_release_families") or ())
        or bool(exclusion.get("tombstoned_grammar_resurrected"))
    ):
        raise ScheduledMacroReleaseTripwireError("decision-card semantic drift")
    return card


def load_official_calendar(path: str | Path) -> dict[str, Any]:
    payload = _read_json(Path(path))
    events = [dict(row) for row in payload.get("events") or ()]
    if len(events) != 56:
        raise ScheduledMacroReleaseTripwireError("official macro calendar count drift")
    families = Counter(str(row.get("family")) for row in events)
    if families != Counter(
        {"BLS_CPI": 21, "BLS_EMPLOYMENT_SITUATION": 21, "FOMC_STATEMENT": 14}
    ):
        raise ScheduledMacroReleaseTripwireError("official macro family counts drift")
    timestamps = [pd.Timestamp(row["release_utc"]) for row in events]
    if (
        any(value.tzinfo is None for value in timestamps)
        or timestamps != sorted(timestamps)
        or len(set(timestamps)) != len(timestamps)
        or max(timestamps) >= pd.Timestamp("2024-10-01T00:00:00Z")
    ):
        raise ScheduledMacroReleaseTripwireError("calendar is not unique chronological pre-Q4")
    for row, timestamp in zip(events, timestamps, strict=True):
        source = str(row.get("source") or "")
        local = pd.Timestamp(row["release_et"])
        if local.tzinfo is None or local.tz_convert("UTC") != timestamp:
            raise ScheduledMacroReleaseTripwireError("calendar UTC conversion drift")
        if row["family"].startswith("BLS_"):
            if "bls.gov/" not in source or (local.hour, local.minute) != (8, 30):
                raise ScheduledMacroReleaseTripwireError("BLS source/time drift")
        elif row["family"] == "FOMC_STATEMENT":
            if (
                "federalreserve.gov/" not in source
                or (local.hour, local.minute) != (14, 0)
            ):
                raise ScheduledMacroReleaseTripwireError("FOMC source/time drift")
        else:
            raise ScheduledMacroReleaseTripwireError("unsupported macro release family")
    retrieval = dict(payload.get("retrieval") or {})
    source_hashes = dict(retrieval.get("source_response_sha256") or {})
    if len(source_hashes) != 3 or any(
        len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        for value in source_hashes.values()
    ):
        raise ScheduledMacroReleaseTripwireError("official retrieval hashes are incomplete")
    verification = dict(payload.get("verification") or {})
    if (
        int(verification.get("event_source_http_200_count", 0)) != 56
        or int(verification.get("official_archive_content_mismatch_count", -1)) != 0
        or int(verification.get("utc_conversion_mismatch_count", -1)) != 0
        or int(verification.get("duplicate_release_timestamp_count", -1)) != 0
        or not bool(verification.get("chronologically_sorted"))
    ):
        raise ScheduledMacroReleaseTripwireError("official calendar verification drift")
    return payload


def run_scheduled_macro_release_tripwire(
    root: str | Path,
    *,
    decision_card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Execute the preregistered tripwire without authoritative writes."""

    project = Path(root).resolve()
    card_path = _inside(project, decision_card_path)
    card = load_decision_card(card_path)
    inputs = dict(card["frozen_inputs"])
    calendar_path = _verified(project, inputs, "official_calendar")
    core_ohlcv_path = _verified(project, inputs, "core_ohlcv")
    core_map_path = _verified(project, inputs, "core_contract_map")
    metals_ohlcv_path = _verified(project, inputs, "metals_ohlcv")
    metals_map_path = _verified(project, inputs, "metals_contract_map")
    cost_path = _verified(project, inputs, "cost_model")
    rules_path = _verified(project, inputs, "rule_snapshot")
    calendar_payload = load_official_calendar(calendar_path)
    releases = _release_records(calendar_payload, card["chronological_roles"])

    mapped, mapping_receipt = _load_mapped_bars(
        core_ohlcv_path,
        core_map_path,
        metals_ohlcv_path,
        metals_map_path,
        inputs,
    )
    frames = _symbol_frames(mapped)
    cost_schedule = dict(card["causal_contract"]["conservative_round_turn_cost_usd"])
    specifications = _candidate_specifications(card)
    event_cache: dict[str, tuple[dict[str, Any], ...]] = {}
    coverage_cache: dict[str, dict[str, Any]] = {}
    screens: list[dict[str, Any]] = []
    for specification in specifications:
        cache_key = _event_cache_key(specification, "PRIMARY")
        if cache_key not in event_cache:
            events, coverage = _build_spec_events(
                releases,
                specification,
                frames,
                cost_schedule,
                control="PRIMARY",
            )
            event_cache[cache_key] = events
            coverage_cache[cache_key] = coverage
        scoped = _scope_events(event_cache[cache_key], specification["release_scope"])
        screens.append(_screen_candidate(specification, scoped, coverage_cache[cache_key]))

    selected_screens = _select_discovery_candidates(screens, card["discovery_selection"])
    rules, rule_receipt = exact._load_rule_snapshot(rules_path)
    role_calendars = _role_calendars(mapped, card["chronological_roles"])
    frontier_results: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    for screen in selected_screens:
        specification = dict(screen["specification"])
        primary_events = _scope_events(
            event_cache[_event_cache_key(specification, "PRIMARY")],
            specification["release_scope"],
        )
        frontier, chosen = _discovery_account_frontier(
            specification,
            primary_events,
            role_calendars[specification["execution_market"]]["DISCOVERY"],
            rules,
            card["account_frontier"],
        )
        frontier_results.extend(frontier)
        selected.append({**screen, "selected_account_cell": chosen})

    evaluations: list[dict[str, Any]] = []
    for row in selected:
        if row["selected_account_cell"] is None:
            evaluations.append(
                {
                    "candidate_id": row["candidate_id"],
                    "status": "NO_LEGAL_DISCOVERY_ACCOUNT_CELL",
                    "gate": {"passed": False, "reason": "NO_LEGAL_DISCOVERY_ACCOUNT_CELL"},
                }
            )
            continue
        evaluations.append(
            _evaluate_selected_candidate(
                row,
                releases,
                frames,
                event_cache,
                coverage_cache,
                cost_schedule,
                role_calendars,
                rules,
                card,
            )
        )

    passing = [row for row in evaluations if bool(dict(row.get("gate") or {}).get("passed"))]
    if passing:
        status = "SCHEDULED_MACRO_RELEASE_TRIPWIRE_PASSED_TIER_E_DEVELOPMENT_ONLY"
        next_action = card["next_branch_rule"]["when_gate_passes"]
    else:
        status = "SCHEDULED_MACRO_RELEASE_CAUSAL_REACTION_FALSIFIED"
        next_action = card["next_branch_rule"]["when_gate_fails"]
    best = _best_evaluation(evaluations)
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": status,
        "economic_verdict": status,
        "evidence_role": card["evidence_role"],
        "evidence_ceiling": "TIER_E_DEVELOPMENT_ONLY",
        "source_bindings": {
            "decision_card_path": str(card_path.relative_to(project)),
            "decision_card_hash": card["card_hash"],
            "official_calendar_path": str(calendar_path.relative_to(project)),
            "official_calendar_sha256": inputs["official_calendar_sha256"],
            "official_source_response_hashes": calendar_payload["retrieval"][
                "source_response_sha256"
            ],
            "core_ohlcv_sha256": inputs["core_ohlcv_sha256"],
            "core_contract_map_sha256": inputs["core_contract_map_sha256"],
            "core_contract_map_semantic_hash": inputs[
                "core_contract_map_semantic_hash"
            ],
            "metals_ohlcv_sha256": inputs["metals_ohlcv_sha256"],
            "metals_contract_map_sha256": inputs["metals_contract_map_sha256"],
            "metals_contract_map_semantic_hash": inputs[
                "metals_contract_map_semantic_hash"
            ],
            "cost_model_sha256": inputs["cost_model_sha256"],
            "rule_snapshot": rule_receipt,
        },
        "official_release_count": len(releases),
        "official_release_counts": dict(sorted(Counter(row["family"] for row in releases).items())),
        "role_release_counts": dict(sorted(Counter(row["role"] for row in releases).items())),
        "contract_mapping_receipt": mapping_receipt,
        "candidate_lattice_count": len(specifications),
        "candidate_screens": screens,
        "selected_candidate_count": len(selected),
        "selected_candidates": selected,
        "discovery_account_frontier": frontier_results,
        "evaluations": evaluations,
        "best_evaluation": best,
        "passing_candidate_ids": sorted(row["candidate_id"] for row in passing),
        "promotion_status": None,
        "tier_q_created": 0,
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "incremental_data_spend_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "tombstoned_eia_inventory_grammar_resurrected": False,
        "next_action": next_action,
    }
    return {**core, "result_hash": stable_hash(core)}


def _release_records(
    payload: Mapping[str, Any], roles: Mapping[str, Sequence[str]]
) -> tuple[dict[str, Any], ...]:
    bounds = {
        role: (pd.Timestamp(value[0]), pd.Timestamp(value[1]))
        for role, value in roles.items()
    }
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(payload["events"]):
        timestamp = pd.Timestamp(raw["release_utc"])
        matched = [role for role, (start, end) in bounds.items() if start <= timestamp < end]
        if len(matched) != 1:
            raise ScheduledMacroReleaseTripwireError("release role is not uniquely frozen")
        records.append(
            {
                **dict(raw),
                "release_id": f"{raw['family']}:{timestamp.strftime('%Y%m%dT%H%MZ')}:{index:02d}",
                "release_time": timestamp,
                "role": matched[0],
            }
        )
    return tuple(records)


def _candidate_specifications(card: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    contract = dict(card["causal_contract"])
    rows: list[dict[str, Any]] = []
    for pair in contract["signal_execution_pairs"]:
        for scope in contract["release_scopes"]:
            for mode in contract["reaction_modes"]:
                for observation in contract["reaction_observation_minutes"]:
                    for holding in contract["holding_minutes"]:
                        base = {
                            "signal_market": str(pair["signal_market"]),
                            "execution_market": str(pair["execution_market"]),
                            "release_scope": str(scope),
                            "reaction_mode": str(mode),
                            "observation_minutes": int(observation),
                            "holding_minutes": int(holding),
                        }
                        fingerprint = stable_hash(base)
                        rows.append(
                            {
                                **base,
                                "candidate_id": (
                                    f"macro_{base['signal_market']}_{base['execution_market']}_"
                                    f"{scope.lower()}_{mode.lower()}_o{observation}_h{holding}_"
                                    f"{fingerprint[:12]}"
                                ),
                                "structural_fingerprint": fingerprint,
                            }
                        )
    if len(rows) != 96 or len({row["structural_fingerprint"] for row in rows}) != 96:
        raise ScheduledMacroReleaseTripwireError("candidate lattice drift")
    return tuple(sorted(rows, key=lambda row: row["candidate_id"]))


def _load_mapped_bars(
    core_ohlcv_path: Path,
    core_map_path: Path,
    metals_ohlcv_path: Path,
    metals_map_path: Path,
    inputs: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    core, core_receipt = _load_one_mapped_source(
        core_ohlcv_path,
        core_map_path,
        symbols=("YM", "MYM"),
        map_type=str(inputs["core_contract_map_type"]),
        map_semantic_hash=str(inputs["core_contract_map_semantic_hash"]),
    )
    metals, metals_receipt = _load_one_mapped_source(
        metals_ohlcv_path,
        metals_map_path,
        symbols=("GC", "MGC"),
        map_type=str(inputs["metals_contract_map_type"]),
        map_semantic_hash=str(inputs["metals_contract_map_semantic_hash"]),
    )
    mapped = pd.concat([core, metals], ignore_index=True).sort_values(
        ["symbol", "timestamp"]
    )
    mapped["trading_session_id"], mapped["session_phase_15m"] = _globex_session_fields(
        mapped["timestamp"]
    )
    if mapped.duplicated(["symbol", "timestamp"]).any():
        raise ScheduledMacroReleaseTripwireError("mapped source rows duplicate")
    return mapped, {
        "core": core_receipt,
        "metals": metals_receipt,
        "rows_kept": int(len(mapped)),
        "rows_by_symbol": {
            str(symbol): int(len(group)) for symbol, group in mapped.groupby("symbol")
        },
        "sessions_by_symbol": {
            str(symbol): int(group["trading_session_id"].nunique())
            for symbol, group in mapped.groupby("symbol")
        },
        "session_label_contract": "CME_TRADING_DAY_FROM_17_00_AMERICA_CHICAGO",
        "contract_cycles_required_equal": True,
    }


def _load_one_mapped_source(
    ohlcv_path: Path,
    map_path: Path,
    *,
    symbols: Sequence[str],
    map_type: str,
    map_semantic_hash: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = pd.read_parquet(
        ohlcv_path,
        columns=[
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "session_id",
        ],
        filters=[("symbol", "in", list(symbols))],
    )
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.loc[
        frame["timestamp"].ge(pd.Timestamp("2023-01-01T00:00:00Z"))
        & frame["timestamp"].lt(pd.Timestamp("2024-10-01T00:00:00Z"))
    ].sort_values(["symbol", "timestamp"])
    if frame.empty or frame.duplicated(["symbol", "timestamp"]).any():
        raise ScheduledMacroReleaseTripwireError("source OHLCV is empty or duplicated")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != map_type or roll_map.roll_map_hash() != map_semantic_hash:
        raise ScheduledMacroReleaseTripwireError("explicit contract map semantic drift")
    mapped, receipt = _apply_explicit_contract_map(
        frame,
        roll_map,
        required_map_type=map_type,
    )
    if not set(symbols).issubset(set(mapped["symbol"].astype(str))):
        raise ScheduledMacroReleaseTripwireError("mapped mini/micro pair is absent")
    receipt = {
        **receipt,
        "rows_loaded": int(len(frame)),
        "rows_kept": int(len(mapped)),
        "rows_by_symbol": {
            str(symbol): int(len(group)) for symbol, group in mapped.groupby("symbol")
        },
        "sessions_by_symbol": {
            str(symbol): int(group["session_id"].nunique())
            for symbol, group in mapped.groupby("symbol")
        },
    }
    return mapped, receipt


def _symbol_frames(mapped: pd.DataFrame) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for symbol, group in mapped.groupby("symbol", sort=True):
        ordered = group.sort_values("timestamp").copy()
        if ordered["timestamp"].duplicated().any():
            raise ScheduledMacroReleaseTripwireError("mapped symbol timestamps duplicate")
        output[str(symbol)] = ordered.set_index("timestamp", drop=False)
    return output


def _event_cache_key(specification: Mapping[str, Any], control: str) -> str:
    return stable_hash(
        {
            "signal_market": specification["signal_market"],
            "execution_market": specification["execution_market"],
            "reaction_mode": specification["reaction_mode"],
            "observation_minutes": specification["observation_minutes"],
            "holding_minutes": specification["holding_minutes"],
            "control": control,
        }
    )


def _build_spec_events(
    releases: Sequence[Mapping[str, Any]],
    specification: Mapping[str, Any],
    frames: Mapping[str, pd.DataFrame],
    cost_schedule: Mapping[str, Mapping[str, float]],
    *,
    control: str,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    events: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    macro_session_dates = {
        pd.Timestamp(row["release_time"])
        .tz_convert("America/Chicago")
        .strftime("%Y-%m-%d")
        for row in releases
    }
    execution = str(specification["execution_market"])
    for release in releases:
        anchor = pd.Timestamp(release["release_time"])
        if control == "RELEASE_TIME_SESSION_MATCHED":
            anchor = _matched_control_time(
                release,
                frames[execution],
                macro_session_dates,
            )
            if anchor is None:
                reasons["NO_MATCHED_CONTROL_SESSION"] += 1
                continue
        event, reason = _build_release_event(
            release,
            specification,
            frames,
            cost_schedule,
            anchor_time=anchor,
        )
        if event is None:
            reasons[reason] += 1
            continue
        events.append(event)
    if len({row["event_id"] for row in events}) != len(events):
        raise ScheduledMacroReleaseTripwireError("event construction duplicated an ID")
    return tuple(sorted(events, key=lambda row: (row["entry_ns"], row["event_id"]))), {
        "control": control,
        "requested_release_count": len(releases),
        "executable_event_count": len(events),
        "missing_reason_counts": dict(sorted(reasons.items())),
    }


def _build_release_event(
    release: Mapping[str, Any],
    specification: Mapping[str, Any],
    frames: Mapping[str, pd.DataFrame],
    cost_schedule: Mapping[str, Mapping[str, float]],
    *,
    anchor_time: pd.Timestamp,
) -> tuple[dict[str, Any] | None, str]:
    signal_symbol = str(specification["signal_market"])
    execution_symbol = str(specification["execution_market"])
    if signal_symbol not in frames or execution_symbol not in frames:
        return None, "MISSING_MARKET_PAIR"
    observation = int(specification["observation_minutes"])
    holding = int(specification["holding_minutes"])
    signal_frame = frames[signal_symbol]
    execution_frame = frames[execution_symbol]
    observation_times = pd.date_range(anchor_time, periods=observation, freq="1min")
    observed = signal_frame.reindex(observation_times)
    if observed["timestamp"].isna().any():
        return None, "INCOMPLETE_SIGNAL_OBSERVATION"
    if len(observed) != observation or observed["active_contract"].nunique() != 1:
        return None, "SIGNAL_CONTRACT_OR_INTERVAL_BREAK"
    displacement = float(observed.iloc[-1]["close"] - observed.iloc[0]["open"])
    direction = int(np.sign(displacement))
    if direction == 0:
        return None, "ZERO_REACTION_ABSTENTION"
    if specification["reaction_mode"] == "REJECTION":
        direction *= -1
    decision_time = anchor_time + pd.Timedelta(minutes=observation)
    exit_time = decision_time + pd.Timedelta(minutes=holding)
    if decision_time not in execution_frame.index or exit_time not in execution_frame.index:
        return None, "MISSING_EXACT_EXECUTION_OPEN"
    entry_row = execution_frame.loc[decision_time]
    exit_row = execution_frame.loc[exit_time]
    if isinstance(entry_row, pd.DataFrame) or isinstance(exit_row, pd.DataFrame):
        raise ScheduledMacroReleaseTripwireError("execution timestamp is not unique")
    path = execution_frame.loc[
        decision_time : exit_time - pd.Timedelta(minutes=1)
    ]
    expected_times = pd.date_range(decision_time, periods=holding, freq="1min")
    if (
        len(path) != holding
        or not path.index.equals(expected_times)
        or path["active_contract"].nunique() != 1
        or str(entry_row["trading_session_id"])
        != str(exit_row["trading_session_id"])
    ):
        return None, "EXECUTION_PATH_OR_CONTRACT_BREAK"
    signal_contract = str(observed.iloc[-1]["active_contract"])
    execution_contract = str(entry_row["active_contract"])
    if _contract_cycle(signal_contract, signal_symbol) != _contract_cycle(
        execution_contract, execution_symbol
    ):
        return None, "MINI_MICRO_CONTRACT_CYCLE_MISMATCH"
    entry_price = float(entry_row["open"])
    exit_price = float(exit_row["open"])
    point_value = instrument_spec(execution_symbol).point_value
    gross = direction * (exit_price - entry_price) * point_value
    if direction > 0:
        favorable = (float(path["high"].max()) - entry_price) * point_value
        adverse = (float(path["low"].min()) - entry_price) * point_value
    else:
        favorable = (entry_price - float(path["low"].min())) * point_value
        adverse = (entry_price - float(path["high"].max())) * point_value
    local_exit = exit_time.tz_convert("America/Chicago")
    session_compliant = (local_exit.hour, local_exit.minute) <= (15, 10)
    costs = dict(cost_schedule[execution_symbol])
    normal_cost = float(costs["NORMAL"])
    stressed_cost = float(costs["STRESSED_1_5X"])
    return {
        "event_id": (
            f"{specification['candidate_id']}:{release['release_id']}:"
            f"{anchor_time.strftime('%Y%m%dT%H%MZ')}"
        ),
        "release_id": str(release["release_id"]),
        "release_family": str(release["family"]),
        "role": str(release["role"]),
        "official_release_time": str(pd.Timestamp(release["release_time"]).isoformat()),
        "anchor_time": str(anchor_time.isoformat()),
        "decision_time": str(decision_time.isoformat()),
        "fill_time": str(decision_time.isoformat()),
        "exit_time": str(exit_time.isoformat()),
        "entry_ns": int(decision_time.value),
        "exit_ns": int(exit_time.value),
        "session_day": int(str(entry_row["trading_session_id"]).replace("-", "")),
        "execution_market": execution_symbol,
        "side": direction,
        "reaction_displacement": displacement,
        "signal_contract": signal_contract,
        "execution_contract": execution_contract,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_one_micro": float(gross),
        "favorable_one_micro": float(max(favorable, 0.0)),
        "adverse_one_micro": float(min(adverse, 0.0)),
        "normal_cost_one_micro": normal_cost,
        "stressed_cost_one_micro": stressed_cost,
        "session_compliant": session_compliant,
    }, "OK"


def _matched_control_time(
    release: Mapping[str, Any],
    execution_frame: pd.DataFrame,
    macro_session_dates: set[str],
) -> pd.Timestamp | None:
    release_time = pd.Timestamp(release["release_time"])
    local = release_time.tz_convert("America/Chicago")
    sessions = sorted(set(execution_frame["trading_session_id"].astype(str)))
    release_session = local.strftime("%Y-%m-%d")
    candidates = [value for value in sessions if value < release_session and value not in macro_session_dates]
    if not candidates:
        return None
    day = candidates[-1]
    return pd.Timestamp(
        f"{day} {local.hour:02d}:{local.minute:02d}:00",
        tz="America/Chicago",
    ).tz_convert("UTC")


def _contract_cycle(contract: str, root: str) -> str:
    if not contract.startswith(root) or len(contract) <= len(root):
        raise ScheduledMacroReleaseTripwireError("explicit contract root mismatch")
    return contract[len(root) :]


def _scope_events(
    events: Sequence[Mapping[str, Any]], release_scope: str
) -> tuple[dict[str, Any], ...]:
    return tuple(
        dict(row)
        for row in events
        if release_scope == "ALL_RELEASES" or row["release_family"] == release_scope
    )


def _screen_candidate(
    specification: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    role_results = {
        role: {
            scenario: _event_economics(
                [row for row in events if row["role"] == role],
                quantity=1,
                scenario=scenario,
            )
            for scenario in SCENARIOS
        }
        for role in ROLES
    }
    return {
        "candidate_id": specification["candidate_id"],
        "structural_fingerprint": specification["structural_fingerprint"],
        "specification": dict(specification),
        "coverage": dict(coverage),
        "role_results_one_micro": role_results,
        "discovery_selection_fields": {
            "positive_discovery_stressed_net": role_results["DISCOVERY"][
                "STRESSED_1_5X"
            ]["net_total_usd"]
            > 0.0,
            "discovery_stressed_net_usd": role_results["DISCOVERY"][
                "STRESSED_1_5X"
            ]["net_total_usd"],
            "discovery_net_per_event_usd": role_results["DISCOVERY"][
                "STRESSED_1_5X"
            ]["net_per_event_usd"],
            "event_count": role_results["DISCOVERY"]["STRESSED_1_5X"]["event_count"],
        },
    }


def _select_discovery_candidates(
    screens: Sequence[Mapping[str, Any]], selection: Mapping[str, Any]
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    maximum = int(selection["maximum_selected_candidates"])
    per_market = int(selection["maximum_per_execution_market"])
    counts: Counter[str] = Counter()
    eligible = [
        dict(row)
        for row in screens
        if int(row["role_results_one_micro"]["DISCOVERY"]["STRESSED_1_5X"]["event_count"])
        > 0
    ]
    ordered = sorted(
        eligible,
        key=lambda row: (
            -int(row["discovery_selection_fields"]["positive_discovery_stressed_net"]),
            -float(row["discovery_selection_fields"]["discovery_stressed_net_usd"]),
            -float(row["discovery_selection_fields"]["discovery_net_per_event_usd"]),
            -int(row["discovery_selection_fields"]["event_count"]),
            str(row["candidate_id"]),
        ),
    )
    for row in ordered:
        market = str(row["specification"]["execution_market"])
        if counts[market] >= per_market:
            continue
        selected.append(row)
        counts[market] += 1
        if len(selected) >= maximum:
            break
    return selected


def _role_calendars(
    mapped: pd.DataFrame, roles: Mapping[str, Sequence[str]]
) -> dict[str, dict[str, tuple[int, ...]]]:
    output: dict[str, dict[str, tuple[int, ...]]] = {}
    for market in ("MYM", "MGC"):
        market_frame = mapped.loc[mapped["symbol"].astype(str).eq(market)]
        output[market] = {}
        for role, bounds in roles.items():
            start, end = pd.Timestamp(bounds[0]), pd.Timestamp(bounds[1])
            values = sorted(
                {
                    int(value.replace("-", ""))
                    for value in market_frame["trading_session_id"].astype(str)
                    if start <= pd.Timestamp(value, tz="UTC") < end
                }
            )
            output[market][role] = tuple(values)
    return output


def _discovery_account_frontier(
    specification: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    rules: Mapping[str, Mapping[str, Any]],
    frontier: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    starts = non_overlapping_starts(calendar, (20,))[20]
    rows: list[dict[str, Any]] = []
    for account_label in frontier["account_sizes"]:
        rule = dict(rules[str(account_label)])
        config = exact._account_config(rule)
        for quantity in frontier["micro_quantities"]:
            quantity = int(quantity)
            if quantity > int(rule["maximum_micro_contracts"]):
                continue
            results: dict[str, Any] = {}
            for scenario in SCENARIOS:
                path = _trade_path_events(events, quantity=quantity, scenario=scenario)
                episodes = [
                    run_combine_episode(
                        path,
                        calendar,
                        start_day=int(start_day),
                        maximum_duration_days=20,
                        config=config,
                        maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
                    )
                    for start_day, _ in starts
                ]
                results[scenario] = _summarize_episodes(episodes)
            rows.append(
                {
                    "candidate_id": specification["candidate_id"],
                    "account_label": str(account_label),
                    "account_size_usd": int(rule["account_size_usd"]),
                    "micro_quantity": quantity,
                    "mini_equivalent": mini_equivalent(
                        str(specification["execution_market"]), quantity
                    ),
                    "horizon_trading_days": 20,
                    "full_coverage_start_count": len(starts),
                    "normal": results["NORMAL"],
                    "stressed": results["STRESSED_1_5X"],
                }
            )
    safe = [
        row
        for row in rows
        if row["normal"]["mll_breach_rate"] <= float(frontier["maximum_mll_breach_rate"])
        and row["stressed"]["mll_breach_rate"]
        <= float(frontier["maximum_mll_breach_rate"])
        and row["normal"]["all_passing_paths_consistency_compliant"]
        and row["stressed"]["all_passing_paths_consistency_compliant"]
    ]
    if not safe:
        return rows, None
    chosen = max(
        safe,
        key=lambda row: (
            int(row["stressed"]["pass_count"]),
            int(row["normal"]["pass_count"]),
            float(row["stressed"]["target_progress_median"]),
            float(row["stressed"]["net_total_usd"]),
            -int(row["account_size_usd"]),
            -int(row["micro_quantity"]),
        ),
    )
    return rows, dict(chosen)


def _evaluate_selected_candidate(
    selected: Mapping[str, Any],
    releases: Sequence[Mapping[str, Any]],
    frames: Mapping[str, pd.DataFrame],
    event_cache: dict[str, tuple[dict[str, Any], ...]],
    coverage_cache: dict[str, dict[str, Any]],
    cost_schedule: Mapping[str, Mapping[str, float]],
    role_calendars: Mapping[str, Mapping[str, Sequence[int]]],
    rules: Mapping[str, Mapping[str, Any]],
    card: Mapping[str, Any],
) -> dict[str, Any]:
    specification = dict(selected["specification"])
    account = dict(selected["selected_account_cell"])
    account_label = str(account["account_label"])
    quantity = int(account["micro_quantity"])
    rule = dict(rules[account_label])
    config = exact._account_config(rule)
    control_events: dict[str, tuple[dict[str, Any], ...]] = {}
    control_coverage: dict[str, Any] = {}
    primary_key = _event_cache_key(specification, "PRIMARY")
    primary = _scope_events(event_cache[primary_key], specification["release_scope"])
    control_events["PRIMARY"] = primary
    control_coverage["PRIMARY"] = coverage_cache[primary_key]
    control_events["DIRECTION_FLIP"] = tuple(_flip_event(row) for row in primary)
    control_coverage["DIRECTION_FLIP"] = {
        "control": "DIRECTION_FLIP",
        "executable_event_count": len(primary),
        "identical_event_clock": True,
    }
    matched_key = _event_cache_key(specification, "RELEASE_TIME_SESSION_MATCHED")
    if matched_key not in event_cache:
        values, coverage = _build_spec_events(
            releases,
            specification,
            frames,
            cost_schedule,
            control="RELEASE_TIME_SESSION_MATCHED",
        )
        event_cache[matched_key] = values
        coverage_cache[matched_key] = coverage
    matched = _scope_events(event_cache[matched_key], specification["release_scope"])
    primary_release_ids = {row["release_id"] for row in primary}
    matched = tuple(row for row in matched if row["release_id"] in primary_release_ids)
    control_events["RELEASE_TIME_SESSION_MATCHED"] = matched
    control_coverage["RELEASE_TIME_SESSION_MATCHED"] = coverage_cache[matched_key]

    summaries: dict[str, Any] = {}
    episode_store: dict[tuple[str, str, int, str], list[CombineEpisodeResult]] = {}
    event_results: dict[str, Any] = {}
    market = str(specification["execution_market"])
    for control in CONTROLS:
        summaries[control] = {}
        event_results[control] = {}
        for role in EVALUATION_ROLES:
            calendar = role_calendars[market][role]
            role_events = [row for row in control_events[control] if row["role"] == role]
            event_results[control][role] = {
                scenario: _event_economics(
                    role_events,
                    quantity=quantity,
                    scenario=scenario,
                )
                for scenario in SCENARIOS
            }
            summaries[control][role] = {}
            for horizon in card["account_frontier"]["horizons_trading_days"]:
                starts = non_overlapping_starts(calendar, (int(horizon),))[int(horizon)]
                summaries[control][role][str(horizon)] = {}
                for scenario in SCENARIOS:
                    path = _trade_path_events(
                        role_events,
                        quantity=quantity,
                        scenario=scenario,
                    )
                    episodes = [
                        run_combine_episode(
                            path,
                            calendar,
                            start_day=int(start_day),
                            maximum_duration_days=int(horizon),
                            config=config,
                            maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
                        )
                        for start_day, _ in starts
                    ]
                    episode_store[(control, role, int(horizon), scenario)] = episodes
                    summaries[control][role][str(horizon)][scenario] = _summarize_episodes(
                        episodes
                    )
    gate = _macro_gate(
        event_results,
        episode_store,
        card["frozen_gate"],
        card["account_frontier"]["horizons_trading_days"],
    )
    family_attribution = _release_family_attribution(
        control_events["PRIMARY"], quantity=quantity
    )
    return {
        "candidate_id": selected["candidate_id"],
        "specification": specification,
        "selected_account_cell_from_discovery": account,
        "control_coverage": control_coverage,
        "event_economics": event_results,
        "account_results": summaries,
        "release_family_attribution": family_attribution,
        "gate": gate,
        "status": (
            "TIER_E_DEVELOPMENT_TRIPWIRE_PASS"
            if gate["passed"]
            else "DEVELOPMENT_TRIPWIRE_FALSIFIED"
        ),
        "promotion_status": None,
    }


def _macro_gate(
    event_results: Mapping[str, Any],
    episode_store: Mapping[tuple[str, str, int, str], Sequence[CombineEpisodeResult]],
    frozen: Mapping[str, Any],
    horizons: Sequence[int],
) -> dict[str, Any]:
    primary_events = event_results["PRIMARY"]
    checks: dict[str, bool] = {
        "positive_stressed_validation_net": primary_events["VALIDATION"][
            "STRESSED_1_5X"
        ]["net_total_usd"]
        > 0.0,
        "positive_stressed_final_development_net": primary_events[
            "FINAL_DEVELOPMENT"
        ]["STRESSED_1_5X"]["net_total_usd"]
        > 0.0,
    }
    positive_contexts = sum(
        primary_events[role]["STRESSED_1_5X"]["net_total_usd"] > 0.0
        for role in EVALUATION_ROLES
    )
    positive_families = sum(
        1
        for family in RELEASE_FAMILIES
        if sum(
            primary_events[role]["STRESSED_1_5X"]["net_by_release_family_usd"].get(
                family, 0.0
            )
            for role in EVALUATION_ROLES
        )
        > 0.0
    )
    checks["release_or_context_diversity"] = max(positive_contexts, positive_families) >= int(
        frozen["minimum_positive_release_types_or_temporal_contexts"]
    )
    all_primary = [
        result
        for (control, _role, _horizon, _scenario), results in episode_store.items()
        if control == "PRIMARY"
        for result in results
    ]
    primary_mll_cells = _primary_mll_cell_rates(episode_store, horizons)
    checks["controlled_mll"] = all(
        cell["episode_count"] > 0
        and cell["mll_breach_rate"]
        <= float(frozen["maximum_normal_and_stressed_mll_breach_rate"])
        for cell in primary_mll_cells.values()
    )
    passing_primary = [result for result in all_primary if result.passed]
    checks["passing_consistency"] = all(result.consistency_ok for result in passing_primary)

    pass_route_by_horizon: dict[str, Any] = {}
    pass_route = dict(frozen["pass_route"])
    for horizon in horizons:
        combined = {
            control: {
                scenario: _summarize_episodes(
                    [
                        result
                        for role in EVALUATION_ROLES
                        for result in episode_store[(control, role, int(horizon), scenario)]
                    ]
                )
                for scenario in SCENARIOS
            }
            for control in CONTROLS
        }
        primary = combined["PRIMARY"]
        beats = all(
            _beats_control(primary["STRESSED_1_5X"], combined[control]["STRESSED_1_5X"])
            for control in ("DIRECTION_FLIP", "RELEASE_TIME_SESSION_MATCHED")
        )
        route_checks = {
            "normal_passes": primary["NORMAL"]["pass_count"]
            >= int(pass_route["minimum_combined_normal_passes"]),
            "stressed_passes": primary["STRESSED_1_5X"]["pass_count"]
            >= int(pass_route["minimum_combined_stressed_passes"]),
            "beats_both_controls": beats,
        }
        pass_route_by_horizon[str(horizon)] = {
            "passed": all(route_checks.values()),
            "checks": route_checks,
            "combined": combined,
        }
    pass_route_passed = any(row["passed"] for row in pass_route_by_horizon.values())

    uplift = dict(frozen["predefined_target_progress_uplift_route"])
    headline = int(uplift["headline_horizon_trading_days"])
    final = {
        control: _summarize_episodes(
            episode_store[(control, "FINAL_DEVELOPMENT", headline, "STRESSED_1_5X")]
        )
        for control in CONTROLS
    }
    primary_final = final["PRIMARY"]
    uplift_checks = {
        "minimum_median_progress": primary_final["target_progress_median"]
        >= float(uplift["minimum_final_stressed_median_target_progress"]),
        "nonnegative_lower_quartile": primary_final["target_progress_p25"]
        >= float(uplift["minimum_final_stressed_p25_target_progress"]),
        "uplift_over_direction_flip": primary_final["target_progress_median"]
        - final["DIRECTION_FLIP"]["target_progress_median"]
        >= float(uplift["minimum_absolute_median_uplift_over_each_control"]),
        "uplift_over_session_matched": primary_final["target_progress_median"]
        - final["RELEASE_TIME_SESSION_MATCHED"]["target_progress_median"]
        >= float(uplift["minimum_absolute_median_uplift_over_each_control"]),
    }
    uplift_passed = all(uplift_checks.values())
    checks["pass_or_predefined_uplift_route"] = pass_route_passed or uplift_passed
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "positive_temporal_context_count": positive_contexts,
        "positive_release_family_count": positive_families,
        "primary_mll_cells": primary_mll_cells,
        "pass_route_by_horizon": pass_route_by_horizon,
        "predefined_target_progress_uplift_route": {
            "passed": uplift_passed,
            "checks": uplift_checks,
            "final_development_stressed": final,
        },
    }


def _primary_mll_cell_rates(
    episode_store: Mapping[
        tuple[str, str, int, str], Sequence[CombineEpisodeResult]
    ],
    horizons: Sequence[int],
) -> dict[str, dict[str, Any]]:
    """Report MLL separately for every frozen primary evidence cell.

    Pooling horizons or temporal roles could conceal an unsafe small cell.  The
    gate therefore fails closed when any expected cell is empty or exceeds the
    frozen breach ceiling.
    """

    cells: dict[str, dict[str, Any]] = {}
    for role in EVALUATION_ROLES:
        for horizon in horizons:
            for scenario in SCENARIOS:
                results = tuple(
                    episode_store.get(
                        ("PRIMARY", role, int(horizon), scenario), ()
                    )
                )
                key = f"{role}:{int(horizon)}D:{scenario}"
                cells[key] = {
                    "episode_count": len(results),
                    "mll_breach_count": sum(
                        bool(result.mll_breached) for result in results
                    ),
                    "mll_breach_rate": (
                        sum(bool(result.mll_breached) for result in results)
                        / len(results)
                        if results
                        else 1.0
                    ),
                }
    return cells


def _event_economics(
    events: Sequence[Mapping[str, Any]], *, quantity: int, scenario: str
) -> dict[str, Any]:
    cost_key = (
        "normal_cost_one_micro" if scenario == "NORMAL" else "stressed_cost_one_micro"
    )
    values = [
        (float(row["gross_one_micro"]) - float(row[cost_key])) * quantity
        for row in events
    ]
    by_family: dict[str, float] = {}
    for row, value in zip(events, values, strict=True):
        by_family[str(row["release_family"])] = by_family.get(
            str(row["release_family"]), 0.0
        ) + value
    return {
        "event_count": len(values),
        "net_total_usd": float(sum(values)),
        "net_per_event_usd": float(statistics.mean(values)) if values else 0.0,
        "net_median_event_usd": float(statistics.median(values)) if values else 0.0,
        "win_rate": sum(value > 0.0 for value in values) / max(len(values), 1),
        "net_by_release_family_usd": dict(sorted(by_family.items())),
    }


def _trade_path_events(
    events: Sequence[Mapping[str, Any]], *, quantity: int, scenario: str
) -> tuple[TradePathEvent, ...]:
    cost_key = (
        "normal_cost_one_micro" if scenario == "NORMAL" else "stressed_cost_one_micro"
    )
    output = []
    for row in events:
        cost = float(row[cost_key]) * quantity
        output.append(
            TradePathEvent(
                event_id=f"{row['event_id']}:{scenario}:q{quantity}",
                decision_ns=int(row["entry_ns"]),
                exit_ns=int(row["exit_ns"]),
                session_day=int(row["session_day"]),
                net_pnl=float(row["gross_one_micro"]) * quantity - cost,
                gross_pnl=float(row["gross_one_micro"]) * quantity,
                worst_unrealized_pnl=(
                    float(row["adverse_one_micro"]) * quantity - cost / 2.0
                ),
                best_unrealized_pnl=(
                    float(row["favorable_one_micro"]) * quantity - cost / 2.0
                ),
                quantity=quantity,
                mini_equivalent=mini_equivalent(str(row["execution_market"]), quantity),
                regime=str(row["release_family"]),
                session_compliant=bool(row["session_compliant"]),
            )
        )
    return tuple(output)


def _flip_event(row: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(row)
    value["event_id"] = f"{row['event_id']}:direction_flip"
    value["side"] = -int(row["side"])
    value["gross_one_micro"] = -float(row["gross_one_micro"])
    value["favorable_one_micro"] = -float(row["adverse_one_micro"])
    value["adverse_one_micro"] = -float(row["favorable_one_micro"])
    return value


def _summarize_episodes(episodes: Sequence[CombineEpisodeResult]) -> dict[str, Any]:
    values = list(episodes)
    nets = [float(value.net_pnl) for value in values]
    progress = [float(value.target_progress) for value in values]
    passes = [value for value in values if value.terminal is CombineTerminal.PASSED]
    return {
        "episode_count": len(values),
        "pass_count": len(passes),
        "pass_rate": len(passes) / max(len(values), 1),
        "net_total_usd": float(sum(nets)),
        "net_median_usd": float(statistics.median(nets)) if nets else 0.0,
        "target_progress_median": float(statistics.median(progress)) if progress else 0.0,
        "target_progress_p25": float(np.percentile(progress, 25)) if progress else 0.0,
        "mll_breach_count": sum(value.mll_breached for value in values),
        "mll_breach_rate": sum(value.mll_breached for value in values) / max(len(values), 1),
        "minimum_mll_buffer_usd": min(
            (float(value.minimum_mll_buffer) for value in values), default=0.0
        ),
        "consistency_compliance_rate": sum(value.consistency_ok for value in values)
        / max(len(values), 1),
        "all_passing_paths_consistency_compliant": all(
            value.consistency_ok for value in passes
        ),
        "terminal_distribution": dict(
            sorted(Counter(value.terminal.value for value in values).items())
        ),
    }


def _beats_control(primary: Mapping[str, Any], control: Mapping[str, Any]) -> bool:
    return int(primary["pass_count"]) > int(control["pass_count"]) or (
        int(primary["pass_count"]) == int(control["pass_count"])
        and float(primary["net_total_usd"]) > float(control["net_total_usd"])
    )


def _release_family_attribution(
    events: Sequence[Mapping[str, Any]], *, quantity: int
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for family in RELEASE_FAMILIES:
        selected = [row for row in events if row["release_family"] == family]
        output[family] = {
            scenario: _event_economics(selected, quantity=quantity, scenario=scenario)
            for scenario in SCENARIOS
        }
    return output


def _best_evaluation(evaluations: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    valid = [dict(row) for row in evaluations if row.get("account_results")]
    if not valid:
        return None
    return max(
        valid,
        key=lambda row: (
            int(bool(row["gate"]["passed"])),
            max(
                row["gate"]["pass_route_by_horizon"][str(horizon)]["combined"][
                    "PRIMARY"
                ]["STRESSED_1_5X"]["pass_count"]
                for horizon in (5, 10, 20)
            ),
            row["event_economics"]["PRIMARY"]["FINAL_DEVELOPMENT"][
                "STRESSED_1_5X"
            ]["net_total_usd"],
            str(row["candidate_id"]),
        ),
    )


def _verified(project: Path, inputs: Mapping[str, Any], prefix: str) -> Path:
    path = _inside(project, str(inputs[f"{prefix}_path"]))
    if _sha256(path) != str(inputs[f"{prefix}_sha256"]):
        raise ScheduledMacroReleaseTripwireError(f"{prefix} SHA drift")
    return path


def _inside(project: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (project / path).resolve() if not path.is_absolute() else path.resolve()
    if not resolved.is_relative_to(project) or not resolved.is_file():
        raise ScheduledMacroReleaseTripwireError("source path escapes project or is absent")
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ScheduledMacroReleaseTripwireError("expected a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "BRANCH_ID",
    "ScheduledMacroReleaseTripwireError",
    "load_decision_card",
    "load_official_calendar",
    "run_scheduled_macro_release_tripwire",
]
