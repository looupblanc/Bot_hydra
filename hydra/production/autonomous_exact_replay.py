"""Read-only exact account-size race over the immutable FAST-PASS 0029 bank.

This module is deliberately narrower than the autonomous director.  It turns
the SHA-bound event evidence from campaign 0029 back into canonical causal
trade trajectories, applies a small whole-contract risk frontier, and invokes
the authoritative chronological shared-account replay for the 50K, 100K and
150K Combine snapshots.

The function in this module performs no writes and assigns no promotion.  Its
return value is suitable for execution inside one economic worker and durable
persistence by the single parent writer.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.markets.instruments import instrument_spec
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.scaling_plan import mini_equivalent
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.causal_target_velocity import FeatureMatrix, HazardOutcome


EXACT_REPLAY_SCHEMA = "hydra_autonomous_exact_0029_account_race_v1"
SOURCE_CAMPAIGN_ID = "hydra_fast_pass_factory_0029"
DEFAULT_FAST_PASS_MANIFEST = Path("config/v7/fast_pass_factory_0029.json")
DEFAULT_RULE_SNAPSHOT = Path("config/rulesets/topstep_official_2026-07-19.json")
DEFAULT_BANK_ROOT = Path(
    "data/cache/economic_production/hydra_fast_pass_factory_0029"
)
DEFAULT_INTEGER_TIERS = (1, 2, 3, 4)
RISK_GOVERNOR_MODES = (
    "CONTRACT_ONLY_UNIFORM_SCALE",
    "CAUSAL_STATIC_STOP_RISK_GOVERNOR",
)
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
_TRADING_TIMEZONE = ZoneInfo("America/Chicago")
_SESSION_START_LOCAL = datetime_time(17, 0)
_SESSION_FLATTEN_LOCAL = datetime_time(15, 10)


class AutonomousExactReplayError(RuntimeError):
    """Immutable source evidence or exact replay semantics failed closed."""


def run_exact_0029_account_size_race(
    root: str | Path,
    *,
    cohort_maximum: int = 32,
    cohort_offset: int = 0,
    candidate_ids: Sequence[str] | None = None,
    integer_tiers: Sequence[int] = DEFAULT_INTEGER_TIERS,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Run a bounded exact causal account-size/risk race without writing.

    The cohort is selected only from already-viewed development evidence.  A
    result from this function is therefore diagnostic development evidence and
    cannot itself promote a policy or consume confirmation data.
    """

    started = time.perf_counter()
    project = Path(root).resolve()
    if not 1 <= int(cohort_maximum) <= 256:
        raise AutonomousExactReplayError("exact cohort maximum must be in [1,256]")
    if int(cohort_offset) < 0 or int(cohort_offset) >= 256:
        raise AutonomousExactReplayError("exact cohort offset must be in [0,255]")
    tiers = _integer_tiers(integer_tiers)
    manifest_file = _inside(project, fast_pass_manifest_path)
    rules_file = _inside(project, rule_snapshot_path)
    manifest = _load_self_hashed_manifest(manifest_file)
    if manifest.get("campaign_id") != SOURCE_CAMPAIGN_ID:
        raise AutonomousExactReplayError("FAST-PASS source campaign identity drift")
    rules, rule_receipt = _load_rule_snapshot(rules_file)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    bank_entries, bank_receipt = _load_banks(project)
    cohort = select_quality_diverse_cohort(
        bank_entries,
        maximum=cohort_maximum,
        offset=cohort_offset,
        candidate_ids=candidate_ids,
    )
    if not cohort:
        raise AutonomousExactReplayError("0029 quality-diverse exact cohort is empty")

    results: list[dict[str, Any]] = []
    exact_replays = 0
    exact_normal = 0
    exact_stressed = 0
    reconstructed_events = 0
    legal_cells = 0
    illegal_cells = 0
    full_coverage_start_cells = 0
    censored_start_cells = 0
    source_event_hashes: dict[str, str] = {}

    for entry in cohort:
        events, event_receipt = _read_verified_event_evidence(
            project, entry["event_evidence"]
        )
        replay = reconstruct_exact_hazard_replay(
            candidate_payload=entry["candidate"],
            event_mappings=events,
            eligible_session_days=entry["eligible_session_days"],
            expected_hashes=entry["exact_hashes"],
        )
        if (
            str(replay.candidate.candidate_id) != str(entry["candidate_id"])
            or str(replay.candidate.structural_fingerprint)
            != str(entry["candidate_fingerprint"])
            or len(replay.normal_trajectories)
            != int(entry.get("completed_event_count", -1))
            or len(replay.stressed_trajectories)
            != int(entry.get("completed_event_count", -1))
        ):
            raise AutonomousExactReplayError(
                "reconstructed candidate identity differs from frozen bank"
            )
        normal_session_checked, normal_violation_count = _apply_session_contract(
            replay.normal_trajectories
        )
        stressed_session_checked, stressed_violation_count = _apply_session_contract(
            replay.stressed_trajectories
        )
        if normal_violation_count != stressed_violation_count:
            raise AutonomousExactReplayError(
                "normal/stressed session-compliance identity drift"
            )
        declared_risk_charge = _declared_stop_risk_charge_per_mini(
            events, entry["candidate"]
        )
        candidate_id = str(entry["candidate_id"])
        reconstructed_events += len(events)
        source_event_hashes[candidate_id] = event_receipt["sha256"]
        coverage = _candidate_coverage(replay, calendar, starts)
        candidate_result: dict[str, Any] = {
            "candidate_id": candidate_id,
            "candidate_fingerprint": str(entry["candidate_fingerprint"]),
            "realized_behavioral_fingerprint": str(
                entry["realized_behavioral_fingerprint"]
            ),
            "qd_cell": str(entry["qd_cell"]),
            "source_wave": int(entry["_source_wave"]),
            "candidate": dict(entry["candidate"]),
            "source_completed_event_count": len(replay.normal_trajectories),
            "source_censored_event_count": len(events)
            - len(replay.normal_trajectories),
            "source_event_evidence": event_receipt,
            "coverage": coverage["receipt"],
            "session_contract": {
                "timezone": "America/Chicago",
                "session_start_local": "17:00",
                "mandatory_flatten_local": "15:10",
                "overnight_through_flatten_allowed": False,
                "event_violation_count": normal_violation_count,
            },
            "declared_stop_risk_charge_per_mini_usd": declared_risk_charge,
            "frontier": [],
            "promotion_status": None,
            "evidence_tier": "E_DIAGNOSTIC_DEVELOPMENT",
        }
        full_coverage_start_cells += sum(
            len(value) for value in coverage["starts"].values()
        )
        censored_start_cells += sum(
            int(value) for value in coverage["censored"].values()
        )

        for account_label in ("50K", "100K", "150K"):
            rule = rules[account_label]
            config = _account_config(rule)
            account_contract_limit = _market_contract_limit_mini(
                entry["candidate"], rule
            )
            for tier in tiers:
                normal = tuple(
                    scale_causal_trajectory(
                        row, executable_quantity_multiplier=tier
                    )
                    for row in normal_session_checked
                )
                stressed = tuple(
                    scale_causal_trajectory(
                        row, executable_quantity_multiplier=tier
                    )
                    for row in stressed_session_checked
                )
                _require_scenario_identity(normal, stressed)
                maximum_mini = max(
                    (float(row.event.mini_equivalent) for row in normal),
                    default=0.0,
                )
                for governor_mode in RISK_GOVERNOR_MODES:
                    if maximum_mini > account_contract_limit + 1e-12:
                        illegal_cells += len(HORIZONS)
                        for horizon in HORIZONS:
                            candidate_result["frontier"].append(
                                _illegal_cell(
                                    candidate_id,
                                    account_label,
                                    rule,
                                    tier=tier,
                                    horizon=horizon,
                                    maximum_mini=maximum_mini,
                                    account_contract_limit=account_contract_limit,
                                    governor_mode=governor_mode,
                                )
                            )
                        continue
                    policy_charge = (
                        1e-6
                        if governor_mode == "CONTRACT_ONLY_UNIFORM_SCALE"
                        else declared_risk_charge
                    )
                    policy = _standalone_policy(
                        candidate_id,
                        rule,
                        tier=tier,
                        declared_risk_charge_per_mini=policy_charge,
                        account_contract_limit=account_contract_limit,
                        governor_mode=governor_mode,
                    )
                    for horizon in HORIZONS:
                        legal_cells += 1
                        episode_sets: dict[str, list[tuple[Any, str]]] = {
                            "NORMAL": [],
                            "STRESSED_1_5X": [],
                        }
                        for start_day, block in coverage["starts"][horizon]:
                            for scenario, trajectories in (
                                ("NORMAL", normal),
                                ("STRESSED_1_5X", stressed),
                            ):
                                episode = run_causal_shared_account_episode(
                                    {candidate_id: trajectories},
                                    calendar,
                                    policy=policy,
                                    start_day=int(start_day),
                                    maximum_duration_days=horizon,
                                    config=config,
                                )
                                episode_sets[scenario].append((episode, block))
                                exact_replays += 1
                                exact_normal += int(scenario == "NORMAL")
                                exact_stressed += int(scenario == "STRESSED_1_5X")
                        candidate_result["frontier"].append(
                            _exact_cell(
                                candidate_id,
                                account_label,
                                rule,
                                tier=tier,
                                horizon=horizon,
                                maximum_mini=maximum_mini,
                                account_contract_limit=account_contract_limit,
                                declared_risk_charge=policy_charge,
                                governor_mode=governor_mode,
                                policy=policy,
                                requested_starts=len(starts[horizon]),
                                censored_starts=coverage["censored"][horizon],
                                episodes=episode_sets,
                            )
                        )
        candidate_result["candidate_result_hash"] = stable_hash(candidate_result)
        results.append(candidate_result)

    all_cells = [cell for row in results for cell in row["frontier"]]
    exact_cells = [
        cell
        for candidate in results
        if int(candidate["session_contract"]["event_violation_count"]) == 0
        for cell in candidate["frontier"]
        if cell["legally_executable"]
    ]
    best = _best_frontier_point(exact_cells)
    core = {
        "schema": EXACT_REPLAY_SCHEMA,
        "status": "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE",
        "branch_id": "EXACT_0029_ACCOUNT_SIZE_RACE",
        "source_campaign_id": SOURCE_CAMPAIGN_ID,
        "source_manifest": {
            "path": str(manifest_file),
            "file_sha256": _sha256(manifest_file),
            "manifest_hash": str(manifest["manifest_hash"]),
        },
        "source_banks": bank_receipt,
        "source_event_file_hashes": dict(sorted(source_event_hashes.items())),
        "official_rule_snapshot": rule_receipt,
        "frozen_grid": grid_receipt,
        "selection": {
            "policy": (
                "EXPLICIT_FROZEN_IDS_WITH_UNIQUE_QD_AND_BEHAVIOR_V1"
                if candidate_ids is not None
                else "ROUND_ROBIN_MARKET_MECHANISM_WITH_UNIQUE_QD_AND_BEHAVIOR_V1"
            ),
            "maximum": int(cohort_maximum),
            "offset": int(cohort_offset),
            "explicit_candidate_ids": (
                None if candidate_ids is None else [str(value) for value in candidate_ids]
            ),
            "selected_count": len(cohort),
            "selected_candidate_ids": [str(row["candidate_id"]) for row in cohort],
            "outcome_roles": "VIEWED_DEVELOPMENT_ONLY",
        },
        "integer_risk_frontier": list(tiers),
        "risk_governor_modes": list(RISK_GOVERNOR_MODES),
        "results": results,
        "best_exact_frontier_point": best,
        "counters": {
            "source_bank_entry_count": int(bank_receipt["entry_count"]),
            "source_unique_candidate_count": int(
                bank_receipt["unique_candidate_count"]
            ),
            "qd_selected_candidate_count": len(cohort),
            "canonical_candidates_reconstructed": len(results),
            "canonical_event_records_reconstructed": reconstructed_events,
            "legal_account_horizon_cells": legal_cells,
            "contract_illegal_account_horizon_cells": illegal_cells,
            "candidate_horizon_full_coverage_start_count": (
                full_coverage_start_cells
            ),
            "candidate_horizon_data_censored_start_count": censored_start_cells,
            "exact_account_replays": exact_replays,
            "exact_normal_account_replays": exact_normal,
            "exact_stressed_account_replays": exact_stressed,
            "summary_scaled_episode_screens": 0,
            "xfa_paths_started": 0,
            "promotion_count": 0,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "interpretation_boundary": (
            "Exact causal development diagnostics only. No promotion, XFA, "
            "confirmation, protected-data access, or order capability."
        ),
        "decision": (
            "EXACT_COMBINE_PASS_OBSERVED_REQUIRES_CHRONOLOGICAL_VALIDATION"
            if best and int(best["stressed"]["pass_count"]) > 0
            else "EXACT_LEGAL_FRONTIER_NO_PASS"
        ),
        "evidence_tier": "E",
        "promotion_status": None,
        "next_action": (
            "FREEZE_AND_ADVANCE_ANY_EXACT_PASS_TO_CHRONOLOGICAL_VALIDATION"
            if best and int(best["stressed"]["pass_count"]) > 0
            else "REALLOCATE_EXPLORATION_TO_A_MATERIALLY_DISTINCT_INFORMATION_BRANCH"
        ),
        "next_materially_distinct_action": (
            "FREEZE_AND_ADVANCE_ANY_EXACT_PASS_TO_CHRONOLOGICAL_VALIDATION"
            if best and int(best["stressed"]["pass_count"]) > 0
            else "REALLOCATE_EXPLORATION_TO_A_MATERIALLY_DISTINCT_INFORMATION_BRANCH"
        ),
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {
        **core,
        "result_hash": stable_hash(core),
        "runtime_seconds": time.perf_counter() - started,
    }


def exact_0029_account_size_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe worker adapter; it remains read-only."""

    return run_exact_0029_account_size_race(
        str(payload["root"]),
        cohort_maximum=int(payload.get("cohort_maximum", 32)),
        cohort_offset=int(payload.get("cohort_offset", 0)),
        candidate_ids=(
            None
            if payload.get("candidate_ids") is None
            else tuple(str(value) for value in payload["candidate_ids"])
        ),
        integer_tiers=tuple(
            int(value) for value in payload.get("integer_tiers", DEFAULT_INTEGER_TIERS)
        ),
        fast_pass_manifest_path=str(
            payload.get("fast_pass_manifest_path", DEFAULT_FAST_PASS_MANIFEST)
        ),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
        ),
    )


def select_quality_diverse_cohort(
    entries: Sequence[Mapping[str, Any]],
    *,
    maximum: int,
    offset: int = 0,
    candidate_ids: Sequence[str] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Select a deterministic, coarse-niche round-robin cohort."""

    if maximum <= 0:
        return ()
    if offset < 0:
        raise AutonomousExactReplayError("quality-diverse cohort offset is negative")
    if candidate_ids is not None and offset:
        raise AutonomousExactReplayError(
            "explicit candidate IDs and cohort offset are mutually exclusive"
        )
    by_id: dict[str, dict[str, Any]] = {}
    for raw in entries:
        row = dict(raw)
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            raise AutonomousExactReplayError("bank entry has no candidate ID")
        prior = by_id.get(candidate_id)
        if prior is not None:
            provenance_fields = (
                "candidate_fingerprint",
                "candidate",
                "exact_hashes",
                "event_evidence",
                "eligible_session_days",
                "qd_cell",
                "realized_behavioral_fingerprint",
            )
            if any(prior.get(field) != row.get(field) for field in provenance_fields):
                raise AutonomousExactReplayError(
                    f"duplicate candidate evidence drift: {candidate_id}"
                )
            if _entry_rank(row) <= _entry_rank(prior):
                continue
        by_id[candidate_id] = row

    if candidate_ids is not None:
        requested = tuple(str(value) for value in candidate_ids)
        if not requested or len(requested) > maximum or len(set(requested)) != len(
            requested
        ):
            raise AutonomousExactReplayError(
                "explicit exact cohort must be unique, nonempty and within maximum"
            )
        missing = [value for value in requested if value not in by_id]
        if missing:
            raise AutonomousExactReplayError(
                "explicit exact cohort is absent from immutable bank: "
                + ",".join(missing)
            )
        selected = tuple(by_id[value] for value in requested)
        _require_quality_diversity(selected)
        return selected

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in by_id.values():
        candidate = dict(row.get("candidate") or {})
        groups[
            (
                str(candidate.get("market") or "UNKNOWN"),
                str(candidate.get("mechanism") or "UNKNOWN"),
            )
        ].append(row)
    for rows in groups.values():
        rows.sort(key=_entry_rank, reverse=True)
    group_keys = sorted(
        groups,
        key=lambda key: (_entry_rank(groups[key][0]), key),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    used_qd: set[str] = set()
    used_behavior: set[str] = set()
    depth = 0
    target = min(maximum + offset, len(by_id))
    while len(selected) < target:
        progressed = False
        for key in group_keys:
            rows = groups[key]
            if depth >= len(rows):
                continue
            row = rows[depth]
            progressed = True
            qd = str(row.get("qd_cell") or "")
            behavior = str(row.get("realized_behavioral_fingerprint") or "")
            if not qd or not behavior:
                raise AutonomousExactReplayError(
                    "quality-diverse bank entry lacks QD/behavior identity"
                )
            if qd in used_qd or behavior in used_behavior:
                continue
            selected.append(row)
            used_qd.add(qd)
            used_behavior.add(behavior)
            if len(selected) == target:
                break
        if not progressed:
            break
        depth += 1
    return tuple(selected[offset : offset + maximum])


def _require_quality_diversity(entries: Sequence[Mapping[str, Any]]) -> None:
    qd = [str(row.get("qd_cell") or "") for row in entries]
    behavior = [
        str(row.get("realized_behavioral_fingerprint") or "") for row in entries
    ]
    if (
        any(not value for value in qd)
        or any(not value for value in behavior)
        or len(set(qd)) != len(qd)
        or len(set(behavior)) != len(behavior)
    ):
        raise AutonomousExactReplayError(
            "explicit exact cohort is not QD/behaviorally unique"
        )


def _load_banks(project: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base = _inside(project, DEFAULT_BANK_ROOT)
    entries: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    for wave in (1, 2):
        path = base / f"wave_{wave:02d}/causal_executable_bank.json"
        payload = _read_json(path)
        rows = list(payload.get("entries") or ())
        if (
            payload.get("schema") != "hydra_fast_pass_qd_bank_v1"
            or payload.get("campaign_id") != SOURCE_CAMPAIGN_ID
            or int(payload.get("wave", -1)) != wave
            or stable_hash(rows) != str(payload.get("bank_hash") or "")
        ):
            raise AutonomousExactReplayError(f"0029 bank identity drift: wave {wave}")
        for raw in rows:
            row = dict(raw)
            row["_source_wave"] = wave
            entries.append(row)
        files.append(
            {
                "wave": wave,
                "path": str(path),
                "file_sha256": _sha256(path),
                "bank_hash": str(payload["bank_hash"]),
                "entry_count": len(rows),
            }
        )
    return entries, {
        "files": files,
        "entry_count": len(entries),
        "unique_candidate_count": len(
            {str(row["candidate_id"]) for row in entries}
        ),
    }


def _load_frozen_grid(
    project: Path, manifest: Mapping[str, Any]
) -> tuple[
    tuple[int, ...],
    dict[int, tuple[tuple[int, str], ...]],
    dict[str, Any],
]:
    data = dict(manifest["data"])
    calendar: set[int] = set()
    source_hashes: dict[str, str] = {}
    start_ns = _date_ns(str(data["evaluation_start_inclusive"]))
    end_ns = _date_ns(str(data["evaluation_end_exclusive"]))
    for market, binding_value in sorted(
        dict(data["feature_matrix_bindings"]).items()
    ):
        binding = dict(binding_value)
        path = _inside(project, str(binding["path"]))
        digest = _sha256(path)
        if digest != str(binding["file_sha256"]):
            raise AutonomousExactReplayError(
                f"frozen feature-matrix manifest drift: {market}"
            )
        matrix = FeatureMatrix.open(path.parent, mmap=True)
        timestamp = matrix.array("timestamp_ns")
        days = matrix.array("session_day")
        mask = (timestamp >= start_ns) & (timestamp < end_ns)
        calendar.update(int(value) for value in np.unique(days[mask]))
        source_hashes[str(market)] = digest
    ordered = tuple(sorted(calendar))
    grid = dict(manifest["evaluation_grid"])
    starts = {
        horizon: tuple(
            (int(row["session_day"]), str(row["temporal_block"]))
            for row in grid["headline_starts"][str(horizon)]
        )
        for horizon in HORIZONS
    }
    grid_payload = {
        "account_calendar": list(ordered),
        "headline_starts": {
            str(horizon): [
                {"session_day": day, "temporal_block": block}
                for day, block in starts[horizon]
            ]
            for horizon in HORIZONS
        },
    }
    if stable_hash(grid_payload) != str(grid["grid_hash"]):
        raise AutonomousExactReplayError("frozen 0029 account grid hash drift")
    if len(ordered) != int(grid["account_calendar_session_count"]):
        raise AutonomousExactReplayError("frozen 0029 account calendar count drift")
    return ordered, starts, {
        "grid_hash": str(grid["grid_hash"]),
        "account_calendar_session_count": len(ordered),
        "headline_start_counts": {
            str(horizon): len(starts[horizon]) for horizon in HORIZONS
        },
        "feature_manifest_hashes": source_hashes,
    }


def _candidate_coverage(
    replay: Any,
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
) -> dict[str, Any]:
    index = {int(day): position for position, day in enumerate(calendar)}
    eligible = {int(value) for value in replay.eligible_session_days}
    censored_days = {
        int(row.session_day)
        for row in replay.events
        if str(row.outcome) == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
    }
    accepted: dict[int, tuple[tuple[int, str], ...]] = {}
    censored: dict[int, int] = {}
    for horizon in HORIZONS:
        valid: list[tuple[int, str]] = []
        rejected = 0
        for start_day, block in starts[horizon]:
            if int(start_day) not in index:
                raise AutonomousExactReplayError("frozen start is absent from calendar")
            position = index[int(start_day)]
            window = tuple(calendar[position : position + horizon])
            if (
                len(window) != horizon
                or any(int(day) not in eligible for day in window)
                or any(int(day) in censored_days for day in window)
            ):
                rejected += 1
                continue
            valid.append((int(start_day), str(block)))
        accepted[horizon] = tuple(valid)
        censored[horizon] = rejected
    return {
        "starts": accepted,
        "censored": censored,
        "receipt": {
            "full_coverage_start_counts": {
                str(horizon): len(accepted[horizon]) for horizon in HORIZONS
            },
            "data_censored_start_counts": {
                str(horizon): censored[horizon] for horizon in HORIZONS
            },
            "headline_denominator_excludes_censored": True,
        },
    }


def _exact_cell(
    candidate_id: str,
    account_label: str,
    rule: Mapping[str, Any],
    *,
    tier: int,
    horizon: int,
    maximum_mini: float,
    account_contract_limit: float,
    declared_risk_charge: float,
    governor_mode: str,
    policy: ActiveRiskPoolPolicy,
    requested_starts: int,
    censored_starts: int,
    episodes: Mapping[str, Sequence[tuple[Any, str]]],
) -> dict[str, Any]:
    summaries = {
        scenario: _summarize_exact_episodes(values)
        for scenario, values in episodes.items()
    }
    compliance_failures = sum(
        int(summary["terminal_distribution"].get("COMPLIANCE_FAILURE", 0))
        for summary in summaries.values()
    )
    return {
        "candidate_id": candidate_id,
        "account_label": account_label,
        "account_size_usd": int(rule["account_size_usd"]),
        "integer_quantity_tier": int(tier),
        "horizon_trading_days": int(horizon),
        "maximum_scaled_mini_equivalent": maximum_mini,
        "maximum_mini_contracts": float(account_contract_limit),
        "declared_stop_risk_charge_per_mini_usd": float(declared_risk_charge),
        "risk_governor_mode": governor_mode,
        "account_policy": policy.to_dict(),
        "legally_executable": True,
        "account_rule_compliant": compliance_failures == 0,
        "hard_compliance_failure_count": compliance_failures,
        "requested_start_count": int(requested_starts),
        "full_coverage_start_count": len(episodes["NORMAL"]),
        "data_censored_start_count": int(censored_starts),
        "exact_account_replays": sum(len(value) for value in episodes.values()),
        "normal": summaries["NORMAL"],
        "stressed": summaries["STRESSED_1_5X"],
    }


def _illegal_cell(
    candidate_id: str,
    account_label: str,
    rule: Mapping[str, Any],
    *,
    tier: int,
    horizon: int,
    maximum_mini: float,
    account_contract_limit: float,
    governor_mode: str,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "account_label": account_label,
        "account_size_usd": int(rule["account_size_usd"]),
        "integer_quantity_tier": int(tier),
        "horizon_trading_days": int(horizon),
        "maximum_scaled_mini_equivalent": maximum_mini,
        "maximum_mini_contracts": float(account_contract_limit),
        "risk_governor_mode": governor_mode,
        "legally_executable": False,
        "account_rule_compliant": False,
        "reason": "FROZEN_UNIFORM_TIER_EXCEEDS_CONTRACT_LIMIT",
        "exact_account_replays": 0,
        "normal": None,
        "stressed": None,
    }


def _summarize_exact_episodes(
    values: Sequence[tuple[Any, str]],
) -> dict[str, Any]:
    episodes = [row for row, _block in values]
    blocks = [block for _row, block in values]
    if not episodes:
        return {
            "episode_count": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "net_total_usd": 0.0,
            "net_median_usd": 0.0,
            "target_progress_p25": 0.0,
            "target_progress_median": 0.0,
            "minimum_mll_buffer_usd": None,
            "median_days_to_target": None,
            "terminal_distribution": {},
            "by_block": {},
            "episode_path_hash": stable_hash([]),
            "requested_quantity_total": 0,
            "admitted_quantity_total": 0,
            "size_reduced_count": 0,
            "risk_or_contract_rejection_count": 0,
        }
    target = [float(row.target_progress) for row in episodes]
    net = [float(row.net_pnl) for row in episodes]
    passing_days = [
        int(row.days_to_target)
        for row in episodes
        if row.days_to_target is not None
    ]
    terminal = Counter(row.terminal.value for row in episodes)
    by_block: dict[str, Any] = {}
    for block in sorted(set(blocks)):
        selected = [
            episode for episode, observed in values if observed == block
        ]
        by_block[block] = {
            "episode_count": len(selected),
            "pass_count": sum(row.passed for row in selected),
            "mll_breach_count": sum(row.mll_breached for row in selected),
            "net_total_usd": sum(float(row.net_pnl) for row in selected),
            "target_progress_median": statistics.median(
                float(row.target_progress) for row in selected
            ),
        }
    paths = [row.to_dict(include_paths=True) for row in episodes]
    allocation = [
        decision
        for episode in episodes
        for decision in episode.risk_allocation_path
    ]
    requested_total = sum(int(row.get("requested_quantity", 0)) for row in allocation)
    admitted_total = sum(int(row.get("quantity", 0)) for row in allocation)
    size_reduced = sum(bool(row.get("size_reduced")) for row in allocation)
    if size_reduced:
        raise AutonomousExactReplayError(
            "priority exact frontier silently reduced a requested quantity"
        )
    risk_or_contract_rejections = sum(
        str(row.get("decision_status"))
        in {"MLL_RISK_REJECTED", "CONTRACT_LIMIT_REJECTED"}
        for row in allocation
    )
    return {
        "episode_count": len(episodes),
        "pass_count": sum(row.passed for row in episodes),
        "pass_rate": sum(row.passed for row in episodes) / len(episodes),
        "mll_breach_count": sum(row.mll_breached for row in episodes),
        "mll_breach_rate": sum(row.mll_breached for row in episodes)
        / len(episodes),
        "consistency_compliance_rate": sum(
            bool(row.consistency_ok) for row in episodes
        )
        / len(episodes),
        "net_total_usd": sum(net),
        "net_median_usd": statistics.median(net),
        "target_progress_p25": _percentile(target, 25),
        "target_progress_median": statistics.median(target),
        "minimum_mll_buffer_usd": min(
            float(row.minimum_mll_buffer) for row in episodes
        ),
        "median_days_to_target": (
            statistics.median(passing_days) if passing_days else None
        ),
        "terminal_distribution": dict(sorted(terminal.items())),
        "by_block": by_block,
        "episode_path_hash": stable_hash(paths),
        "requested_quantity_total": requested_total,
        "admitted_quantity_total": admitted_total,
        "size_reduced_count": size_reduced,
        "risk_or_contract_rejection_count": risk_or_contract_rejections,
        "silent_size_reduction": size_reduced > 0,
    }


def _standalone_policy(
    candidate_id: str,
    rule: Mapping[str, Any],
    *,
    tier: int,
    declared_risk_charge_per_mini: float,
    account_contract_limit: float,
    governor_mode: str,
) -> ActiveRiskPoolPolicy:
    mll = float(rule["maximum_loss_limit_usd"])
    target = float(rule["profit_target_usd"])
    if declared_risk_charge_per_mini <= 0.0:
        raise AutonomousExactReplayError("declared stop risk charge must be positive")
    return ActiveRiskPoolPolicy(
        policy_id=(
            f"exact-0029:{candidate_id}:{int(rule['account_size_usd'])}:"
            f"{tier}:{governor_mode}"
        ),
        component_priority=(candidate_id,),
        nominal_risk_charge_per_mini=(
            (candidate_id, float(declared_risk_charge_per_mini)),
        ),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=mll,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(account_contract_limit),
        # A requested integer tier is either admitted unchanged or rejected.
        # It is never silently clipped into a different economic policy.
        concurrency_scaling=ConcurrencyScaling.PRIORITY,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll,
        daily_consistency_profit_guard=target,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _apply_session_contract(
    trajectories: Sequence[Any],
) -> tuple[tuple[Any, ...], int]:
    checked: list[Any] = []
    violations = 0
    for trajectory in trajectories:
        compliant = _event_session_compliant(trajectory.event)
        violations += int(not compliant)
        checked.append(
            replace(
                trajectory,
                event=replace(trajectory.event, session_compliant=compliant),
            )
        )
    return tuple(checked), violations


def _event_session_compliant(event: Any) -> bool:
    fill = datetime.fromtimestamp(
        int(event.decision_ns) / 1_000_000_000, tz=UTC
    ).astimezone(_TRADING_TIMEZONE)
    exit_time = datetime.fromtimestamp(
        int(event.exit_ns) / 1_000_000_000, tz=UTC
    ).astimezone(_TRADING_TIMEZONE)
    fill_clock = fill.timetz().replace(tzinfo=None)
    if fill_clock >= _SESSION_START_LOCAL:
        trading_date = fill.date() + timedelta(days=1)
    elif fill_clock < _SESSION_FLATTEN_LOCAL:
        trading_date = fill.date()
    else:
        return False
    expected_session_day = (trading_date - date(1970, 1, 1)).days
    flatten = datetime.combine(
        trading_date, _SESSION_FLATTEN_LOCAL, tzinfo=_TRADING_TIMEZONE
    )
    return (
        int(event.session_day) == expected_session_day
        and exit_time >= fill
        and exit_time <= flatten
    )


def _declared_stop_risk_charge_per_mini(
    events: Sequence[Mapping[str, Any]], candidate: Mapping[str, Any]
) -> float:
    execution_market = str(candidate.get("execution_market") or "")
    adverse_r = float(candidate.get("adverse_r", 0.0))
    if not execution_market or adverse_r <= 0.0:
        raise AutonomousExactReplayError("candidate lacks declared stop-risk inputs")
    point_value = float(instrument_spec(execution_market).point_value)
    charges: list[float] = []
    for row in events:
        quantity = int(row.get("quantity", 0))
        risk_unit = float(row.get("risk_unit_price", 0.0))
        if row.get("fill_time_ns") is None and (quantity <= 0 or risk_unit <= 0.0):
            # A never-filled row with no declared exposure contributes no risk.
            # Outcome/censor status is deliberately not consulted.
            continue
        mini = float(mini_equivalent(execution_market, quantity))
        if quantity <= 0 or risk_unit <= 0.0 or mini <= 0.0:
            raise AutonomousExactReplayError(
                "completed event lacks causal declared stop-risk inputs"
            )
        charges.append(risk_unit * adverse_r * point_value * quantity / mini)
    if not charges or not all(math.isfinite(value) and value > 0.0 for value in charges):
        raise AutonomousExactReplayError("cannot freeze declared stop-risk charge")
    # A single static development policy uses the maximum causal stop risk seen
    # in its viewed development event set; no future episode outcome enters it.
    return max(charges)


def _market_contract_limit_mini(
    candidate: Mapping[str, Any], rule: Mapping[str, Any]
) -> float:
    market = str(candidate.get("execution_market") or "").upper()
    account_label = str(rule.get("account_label") or "")
    limits = dict(rule.get("special_contract_caps") or {})
    group: str | None = None
    for key, members in (
        ("CL_QM_RB_HO", {"CL", "QM", "RB", "HO"}),
        ("MCL", {"MCL"}),
        ("GC", {"GC"}),
        ("MGC", {"MGC"}),
        ("SI_HG_PL", {"SI", "HG", "PL"}),
        ("SIL_MHG", {"SIL", "MHG"}),
    ):
        if market in members:
            group = key
            break
    if group is None:
        return float(rule["maximum_mini_contracts"])
    raw_limit = float(dict(limits.get(group) or {}).get(account_label, 0.0))
    if raw_limit <= 0.0:
        return 0.0
    return float(mini_equivalent(market, raw_limit))


def _account_config(rule: Mapping[str, Any]) -> Topstep150KConfig:
    return Topstep150KConfig(
        account_size=float(rule["account_size_usd"]),
        combine_profit_target=float(rule["profit_target_usd"]),
        combine_max_loss_limit=float(rule["maximum_loss_limit_usd"]),
        combine_starting_balance=float(rule["account_size_usd"]),
        mll_mode="eod_level_rt_breach",
        optional_daily_loss_limit=float(rule["optional_daily_loss_limit_usd"]),
        use_optional_daily_loss_limit=False,
        consistency_best_day_max_pct_of_profit_target=float(
            rule["consistency_target_fraction"]
        ),
        minimum_pass_days=int(rule["minimum_trading_days"]),
    )


def _require_scenario_identity(normal: Sequence[Any], stressed: Sequence[Any]) -> None:
    def identity(row: Any) -> tuple[Any, ...]:
        event_id = str(row.event.event_id)
        for suffix in (":NORMAL", ":STRESSED_1_5X"):
            if event_id.endswith(suffix):
                event_id = event_id[: -len(suffix)]
        return (
            event_id,
            row.component_id,
            row.market,
            row.side,
            row.event.decision_ns,
            row.event.exit_ns,
            row.event.quantity,
            row.event.mini_equivalent,
        )

    if [identity(row) for row in normal] != [identity(row) for row in stressed]:
        raise AutonomousExactReplayError(
            "normal/stressed exact trajectory decision identity drift"
        )


def _read_verified_event_evidence(
    project: Path, receipt_value: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    receipt = dict(receipt_value)
    base = _inside(project, DEFAULT_BANK_ROOT)
    path = _inside(base, str(receipt["relative_path"]))
    digest = _sha256(path)
    if digest != str(receipt["sha256"]):
        raise AutonomousExactReplayError("0029 event evidence compressed SHA drift")
    raw = gzip.decompress(path.read_bytes())
    uncompressed = hashlib.sha256(raw).hexdigest()
    if receipt.get("uncompressed_sha256") and uncompressed != str(
        receipt["uncompressed_sha256"]
    ):
        raise AutonomousExactReplayError("0029 event evidence content SHA drift")
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    if len(rows) != int(receipt["record_count"]):
        raise AutonomousExactReplayError("0029 event evidence record count drift")
    return rows, {
        "relative_path": str(receipt["relative_path"]),
        "record_count": len(rows),
        "sha256": digest,
        "uncompressed_sha256": uncompressed,
    }


def _load_rule_snapshot(
    path: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    payload = _read_json(path)
    combine = dict(payload.get("combine") or {})
    combine_common = dict(payload.get("combine_common") or {})
    restrictions = dict(payload.get("product_restrictions") or {})
    expected = {"50K", "100K", "150K"}
    if set(combine) != expected:
        raise AutonomousExactReplayError("official account-size inventory drift")
    parsed = {
        "combine": payload.get("combine"),
        "combine_common": payload.get("combine_common"),
        "xfa": payload.get("xfa"),
        "product_restrictions": payload.get("product_restrictions"),
    }
    if stable_hash(parsed) != str(payload.get("parsed_rule_hash") or ""):
        raise AutonomousExactReplayError("official parsed-rule hash drift")
    required = {
        "account_size_usd",
        "profit_target_usd",
        "maximum_loss_limit_usd",
        "maximum_mini_contracts",
        "consistency_target_fraction",
        "minimum_trading_days",
        "optional_daily_loss_limit_usd",
    }
    for label, rule in combine.items():
        if not required.issubset(rule):
            raise AutonomousExactReplayError(f"incomplete official account rule: {label}")
    if (
        combine_common.get("maximum_loss_limit_mode")
        != "EOD_BALANCE_LEVEL_REALTIME_REALIZED_AND_UNREALIZED_BREACH"
        or combine_common.get("session_timezone") != "America/Chicago"
        or combine_common.get("session_start_local") != "17:00"
        or combine_common.get("mandatory_flatten_local") != "15:10"
        or combine_common.get("overnight_positions_allowed") is not False
    ):
        raise AutonomousExactReplayError(
            "official account semantics are unsupported by exact replay"
        )
    special_caps = dict(restrictions.get("special_contract_caps") or {})
    resolved_rules: dict[str, dict[str, Any]] = {}
    for label, raw in combine.items():
        rule = dict(raw)
        rule["account_label"] = label
        rule["special_contract_caps"] = special_caps
        resolved_rules[label] = rule
    return resolved_rules, {
        "path": str(path),
        "file_sha256": _sha256(path),
        "snapshot_id": str(payload.get("snapshot_id") or ""),
        "retrieved_at_utc": str(payload.get("retrieved_at_utc") or ""),
        "retrieval_status": str(payload.get("retrieval_status") or ""),
        "parsed_rule_hash": str(payload["parsed_rule_hash"]),
    }


def _load_self_hashed_manifest(path: Path) -> dict[str, Any]:
    payload = _read_json(path)
    claimed = str(payload.get("manifest_hash") or "")
    core = dict(payload)
    core.pop("manifest_hash", None)
    if not claimed or stable_hash(core) != claimed:
        raise AutonomousExactReplayError("FAST-PASS manifest self-hash drift")
    return payload


def _best_frontier_point(cells: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    eligible = [
        row
        for row in cells
        if row.get("account_rule_compliant") is True
        and row.get("normal") is not None
        and row.get("stressed") is not None
        and int(row["normal"].get("episode_count", 0)) > 0
        and int(row["stressed"].get("episode_count", 0)) > 0
        and int(row["normal"].get("admitted_quantity_total", 0)) > 0
        and int(row["stressed"].get("admitted_quantity_total", 0)) > 0
    ]
    if not eligible:
        return None
    selected = max(
        eligible,
        key=lambda row: (
            float(row["stressed"]["pass_rate"]),
            float(row["normal"]["pass_rate"]),
            -float(row["stressed"]["mll_breach_rate"]),
            -int(row["horizon_trading_days"]),
            float(row["stressed"]["target_progress_median"]),
            float(row["normal"]["target_progress_median"]),
            -int(row["account_size_usd"]),
            -int(row["integer_quantity_tier"]),
        ),
    )
    return {
        key: selected[key]
        for key in (
            "candidate_id",
            "account_label",
            "account_size_usd",
            "integer_quantity_tier",
            "risk_governor_mode",
            "horizon_trading_days",
            "full_coverage_start_count",
            "normal",
            "stressed",
        )
    }


def _entry_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        float(row.get("stressed_full_net", -math.inf)),
        int(row.get("positive_stressed_block_count", 0)),
        float(row.get("normal_full_net", -math.inf)),
        float(row.get("stressed_design_net", -math.inf)),
        int(row.get("completed_event_count", 0)),
        int(row.get("_source_wave", 0)),
        str(row.get("candidate_id") or ""),
    )


def _integer_tiers(values: Sequence[int]) -> tuple[int, ...]:
    tiers = tuple(sorted({int(value) for value in values}))
    if not tiers or any(value <= 0 or value > 32 for value in tiers):
        raise AutonomousExactReplayError("integer risk tiers must be unique in [1,32]")
    return tiers


def _percentile(values: Sequence[float], percentile: float) -> float:
    return float(np.percentile(np.asarray(values, dtype=float), percentile))


def _date_ns(value: str) -> int:
    parsed = datetime.fromisoformat(str(value)).replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1_000_000_000)


def _inside(base: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (base / path).resolve()
    try:
        resolved.relative_to(base.resolve())
    except ValueError as exc:
        raise AutonomousExactReplayError("path escaped the authoritative root") from exc
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AutonomousExactReplayError(f"cannot read immutable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AutonomousExactReplayError(f"immutable JSON is not an object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "AutonomousExactReplayError",
    "DEFAULT_INTEGER_TIERS",
    "EXACT_REPLAY_SCHEMA",
    "exact_0029_account_size_worker",
    "run_exact_0029_account_size_race",
    "select_quality_diverse_cohort",
]
