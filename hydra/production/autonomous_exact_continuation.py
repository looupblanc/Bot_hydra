"""Bounded exact continuation and qualification audit for the 0029 bank.

The persistent director originally replayed the first 32 quality-diverse
FAST-PASS 0029 candidates.  This module provides the deliberately narrow
continuation primitive needed to consume the rest of that immutable bank.
It does not write files, update a registry, assign a promotion, start XFA, or
touch data outside the already-viewed development evidence.

The public worker is module-level and accepts/returns plain mappings, so it is
safe to submit to a ``spawn`` process pool.  Persistence remains the sole
responsibility of the parent authoritative writer.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_INTEGER_TIERS,
    DEFAULT_RULE_SNAPSHOT,
    EXACT_REPLAY_SCHEMA,
    _load_banks,
    exact_0029_account_size_worker,
    select_quality_diverse_cohort,
)


CONTINUATION_SCHEMA = "hydra_autonomous_exact_0029_continuation_v1"
CONTINUATION_PLAN_SCHEMA = "hydra_autonomous_exact_0029_continuation_plan_v1"
CONTINUATION_COMPOSITE_SCHEMA = (
    "hydra_autonomous_exact_0029_continuation_composite_v1"
)
AUTONOMOUS_BRANCH_RESULT_SCHEMA = "hydra_autonomous_economic_branch_result_v1"
AUTONOMOUS_DIRECTOR_CAMPAIGN_ID = (
    "hydra_autonomous_economic_discovery_director_0035"
)
QUALIFICATION_AUDIT_SCHEMA = "hydra_autonomous_tier_q_qualification_audit_v1"
COMPACT_EVIDENCE_SCHEMA = "hydra_compact_exact_candidate_evidence_bundle_v1"

INITIAL_EXACT_COHORT_SIZE = 32
DEFAULT_CONTINUATION_COHORT_SIZE = 32
DEFAULT_LANE_COUNT = 2
MAXIMUM_QD_INVENTORY = 256

QUALIFICATION_CANDIDATE_ID = "hazard_19327ab34a21d623c654a6cc"
QUALIFICATION_ACCOUNT_LABEL = "50K"
QUALIFICATION_INTEGER_TIER = 3
QUALIFICATION_GOVERNOR = "CAUSAL_STATIC_STOP_RISK_GOVERNOR"
QUALIFICATION_HORIZONS = (5, 10, 20)
MAXIMUM_ACCEPTABLE_MLL_BREACH_RATE = 0.10
MAXIMUM_ACCEPTABLE_CONCENTRATION_SHARE = 0.25
MAXIMUM_ACCEPTABLE_BLOCK_PASS_SHARE = 0.75

_FORBIDDEN_COUNTERS = (
    "promotion_count",
    "xfa_paths_started",
    "data_purchase_count",
    "q4_access_count_delta",
    "broker_connections",
    "orders",
)
_ADDITIVE_COUNTERS = (
    "qd_selected_candidate_count",
    "canonical_candidates_reconstructed",
    "canonical_event_records_reconstructed",
    "legal_account_horizon_cells",
    "contract_illegal_account_horizon_cells",
    "candidate_horizon_full_coverage_start_count",
    "candidate_horizon_data_censored_start_count",
    "exact_account_replays",
    "exact_normal_account_replays",
    "exact_stressed_account_replays",
    "summary_scaled_episode_screens",
    *_FORBIDDEN_COUNTERS,
)


class AutonomousExactContinuationError(RuntimeError):
    """The immutable continuation or its accounting failed closed."""


def inspect_remaining_0029_source(root: str | Path) -> dict[str, Any]:
    """Return a deterministic inventory of the replayable immutable bank.

    The raw source has duplicate cross-wave entries.  The replayable inventory
    is the same quality/behaviour-unique round-robin ordering used by the
    authoritative exact replay.  Consequently the exhaustion denominator is
    explicit and cannot be inflated by duplicate bank rows.
    """

    project = Path(root).resolve()
    entries, receipt = _load_banks(project)
    selected = select_quality_diverse_cohort(
        entries,
        maximum=MAXIMUM_QD_INVENTORY,
        offset=0,
    )
    candidate_ids = tuple(str(row["candidate_id"]) for row in selected)
    if len(candidate_ids) < INITIAL_EXACT_COHORT_SIZE:
        raise AutonomousExactContinuationError(
            "0029 replayable inventory is smaller than the sealed initial cohort"
        )
    if len(candidate_ids) != len(set(candidate_ids)):
        raise AutonomousExactContinuationError(
            "0029 replayable inventory contains duplicate candidates"
        )
    core = {
        "source_campaign_id": "hydra_fast_pass_factory_0029",
        "source_bank_entry_count": int(receipt["entry_count"]),
        "source_unique_candidate_count": int(receipt["unique_candidate_count"]),
        "source_qd_replayable_candidate_count": len(candidate_ids),
        "source_qd_excluded_candidate_count": (
            int(receipt["unique_candidate_count"]) - len(candidate_ids)
        ),
        "sealed_initial_exact_candidate_count": INITIAL_EXACT_COHORT_SIZE,
        "remaining_exact_candidate_count": (
            len(candidate_ids) - INITIAL_EXACT_COHORT_SIZE
        ),
        "sealed_initial_candidate_ids": list(
            candidate_ids[:INITIAL_EXACT_COHORT_SIZE]
        ),
        "remaining_candidate_ids": list(
            candidate_ids[INITIAL_EXACT_COHORT_SIZE:]
        ),
        "ordered_replayable_candidate_ids_hash": stable_hash(list(candidate_ids)),
        "source_bank_files": list(receipt["files"]),
    }
    return {**core, "source_inventory_hash": stable_hash(core)}


def plan_remaining_0029_exact_jobs(
    root: str | Path,
    *,
    completed_cohort_offsets: Sequence[int] = (),
    cohort_size: int = DEFAULT_CONTINUATION_COHORT_SIZE,
    lane_count: int = DEFAULT_LANE_COUNT,
    integer_tiers: Sequence[int] = DEFAULT_INTEGER_TIERS,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Plan at most two deterministic, disjoint continuation workers."""

    if not 1 <= int(cohort_size) <= 64:
        raise AutonomousExactContinuationError("cohort size must be in [1,64]")
    if not 1 <= int(lane_count) <= 2:
        raise AutonomousExactContinuationError("lane count must be one or two")
    inventory = inspect_remaining_0029_source(root)
    total = int(inventory["source_qd_replayable_candidate_count"])
    valid_offsets = tuple(
        range(INITIAL_EXACT_COHORT_SIZE, total, int(cohort_size))
    )
    completed = tuple(sorted({int(value) for value in completed_cohort_offsets}))
    if any(value not in valid_offsets for value in completed):
        raise AutonomousExactContinuationError(
            "completed continuation offset is not a frozen cohort boundary"
        )
    ordered_ids = tuple(inventory["sealed_initial_candidate_ids"]) + tuple(
        inventory["remaining_candidate_ids"]
    )
    pending = [value for value in valid_offsets if value not in completed]
    jobs: list[dict[str, Any]] = []
    for lane_number, offset in enumerate(pending[: int(lane_count)], start=1):
        maximum = min(int(cohort_size), total - offset)
        expected = ordered_ids[offset : offset + maximum]
        payload = {
            "root": str(Path(root).resolve()),
            "cohort_offset": offset,
            "cohort_maximum": maximum,
            "expected_candidate_ids": list(expected),
            "source_inventory_hash": inventory["source_inventory_hash"],
            "integer_tiers": [int(value) for value in integer_tiers],
            "fast_pass_manifest_path": str(fast_pass_manifest_path),
            "rule_snapshot_path": str(rule_snapshot_path),
        }
        jobs.append(
            {
                "lane_id": f"REMAINING_EXACT_LANE_{lane_number}",
                "cohort_offset": offset,
                "cohort_maximum": maximum,
                "expected_candidate_ids": list(expected),
                "worker_payload": payload,
            }
        )
    core = {
        "schema": CONTINUATION_PLAN_SCHEMA,
        "status": (
            "SOURCE_BANK_EXHAUSTED" if not pending else "CONTINUATION_JOBS_READY"
        ),
        "source_inventory": inventory,
        "cohort_size": int(cohort_size),
        "maximum_concurrent_cpu_workers": 2,
        "requested_lane_count": int(lane_count),
        "completed_cohort_offsets": list(completed),
        "pending_cohort_offsets": pending,
        "jobs": jobs,
        "source_bank_exhausted": not pending,
        "read_only_workers": True,
        "single_authoritative_writer_required": True,
    }
    return {**core, "plan_hash": stable_hash(core)}


def remaining_0029_exact_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe, read-only adapter around the authoritative exact worker."""

    root = str(payload["root"])
    offset = int(payload["cohort_offset"])
    maximum = int(payload["cohort_maximum"])
    if offset < INITIAL_EXACT_COHORT_SIZE:
        raise AutonomousExactContinuationError(
            "continuation cannot replay the sealed first 32 candidates"
        )
    inventory = inspect_remaining_0029_source(root)
    if str(payload.get("source_inventory_hash") or "") != str(
        inventory["source_inventory_hash"]
    ):
        raise AutonomousExactContinuationError("0029 source inventory hash drift")
    ordered_ids = tuple(inventory["sealed_initial_candidate_ids"]) + tuple(
        inventory["remaining_candidate_ids"]
    )
    expected = ordered_ids[offset : offset + maximum]
    declared = tuple(str(value) for value in payload["expected_candidate_ids"])
    if not expected or declared != expected:
        raise AutonomousExactContinuationError(
            "continuation cohort differs from its frozen disjoint inventory slice"
        )

    exact_result = exact_0029_account_size_worker(
        {
            "root": root,
            "cohort_offset": offset,
            "cohort_maximum": maximum,
            "integer_tiers": tuple(
                int(value)
                for value in payload.get("integer_tiers", DEFAULT_INTEGER_TIERS)
            ),
            "fast_pass_manifest_path": str(
                payload.get("fast_pass_manifest_path", DEFAULT_FAST_PASS_MANIFEST)
            ),
            "rule_snapshot_path": str(
                payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
            ),
        }
    )
    _verify_exact_result(exact_result)
    actual = tuple(
        str(value) for value in exact_result["selection"]["selected_candidate_ids"]
    )
    if actual != expected:
        raise AutonomousExactContinuationError(
            "authoritative exact worker returned a different cohort"
        )
    _require_diagnostic_safety(exact_result)

    deterministic_exact = dict(exact_result)
    runtime_seconds = float(deterministic_exact.pop("runtime_seconds", 0.0))
    core = {
        "schema": CONTINUATION_SCHEMA,
        "status": "COMPLETE_READ_ONLY_EXACT_CONTINUATION_COHORT",
        "cohort_offset": offset,
        "cohort_maximum": maximum,
        "candidate_ids": list(actual),
        "source_inventory": inventory,
        "exact_result": deterministic_exact,
        "read_only_worker": True,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "evidence_tier": "E",
        "promotion_status": None,
        "xfa_paths_started": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {
        **core,
        "result_hash": stable_hash(core),
        "runtime_seconds": runtime_seconds,
    }


def compose_remaining_0029_exact_results(
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compose exact cohort receipts without duplicating their bulky evidence."""

    initial = _unwrap_exact_result(initial_exact_result)
    _verify_exact_result(initial)
    _require_diagnostic_safety(initial)
    if int(initial["selection"].get("offset", -1)) != 0:
        raise AutonomousExactContinuationError("initial exact cohort offset drift")
    initial_ids = tuple(
        str(value) for value in initial["selection"]["selected_candidate_ids"]
    )
    if len(initial_ids) != INITIAL_EXACT_COHORT_SIZE:
        raise AutonomousExactContinuationError("initial exact cohort count drift")

    wrappers = sorted(
        (_verify_continuation_result(value) for value in continuation_results),
        key=lambda row: int(row["cohort_offset"]),
    )
    inventory = (
        dict(wrappers[0]["source_inventory"])
        if wrappers
        else _inventory_from_initial(initial, initial_ids)
    )
    if tuple(inventory["sealed_initial_candidate_ids"]) != initial_ids:
        raise AutonomousExactContinuationError(
            "initial exact cohort differs from continuation source inventory"
        )
    inventory_hash = str(inventory["source_inventory_hash"])
    expected_all = tuple(inventory["sealed_initial_candidate_ids"]) + tuple(
        inventory["remaining_candidate_ids"]
    )
    seen: set[str] = set(initial_ids)
    exact_results = [initial]
    cohort_receipts: list[dict[str, Any]] = []
    for wrapper in wrappers:
        if str(wrapper["source_inventory"]["source_inventory_hash"]) != inventory_hash:
            raise AutonomousExactContinuationError(
                "continuation cohorts use different source inventories"
            )
        exact = dict(wrapper["exact_result"])
        _verify_exact_result(exact)
        _require_diagnostic_safety(exact)
        ids = tuple(str(value) for value in wrapper["candidate_ids"])
        offset = int(wrapper["cohort_offset"])
        if ids != expected_all[offset : offset + len(ids)]:
            raise AutonomousExactContinuationError(
                "continuation result is not its deterministic inventory slice"
            )
        overlap = seen.intersection(ids)
        if overlap:
            raise AutonomousExactContinuationError(
                "candidate replayed in more than one exact cohort: "
                + ",".join(sorted(overlap))
            )
        seen.update(ids)
        exact_results.append(exact)
        cohort_receipts.append(
            {
                "cohort_offset": offset,
                "candidate_count": len(ids),
                "candidate_ids_hash": stable_hash(list(ids)),
                "exact_result_hash": str(exact["result_hash"]),
                "continuation_result_hash": str(wrapper["result_hash"]),
            }
        )

    unexpected = seen.difference(expected_all)
    if unexpected:
        raise AutonomousExactContinuationError(
            "exact continuation contains candidates outside the source inventory"
        )
    missing = tuple(value for value in expected_all if value not in seen)
    aggregated = {
        key: sum(int(result["counters"].get(key, 0)) for result in exact_results)
        for key in _ADDITIVE_COUNTERS
    }
    pass_sets = _candidate_pass_sets(exact_results)
    best = _best_exact_point(exact_results)
    core = {
        "schema": CONTINUATION_COMPOSITE_SCHEMA,
        "status": (
            "COMPLETE_SOURCE_BANK_EXHAUSTED"
            if not missing
            else "PARTIAL_EXACT_CONTINUATION_ACTIVE"
        ),
        "source_inventory": inventory,
        "source_bank_exhausted": not missing,
        "initial_exact_result_hash": str(initial["result_hash"]),
        "continuation_cohorts_completed": cohort_receipts,
        "completed_candidate_count": len(seen),
        "remaining_candidate_count": len(missing),
        "remaining_candidate_ids_hash": stable_hash(list(missing)),
        "aggregate_counters": {
            **aggregated,
            "source_bank_entry_count": int(
                inventory["source_bank_entry_count"]
            ),
            "source_unique_candidate_count": int(
                inventory["source_unique_candidate_count"]
            ),
            "source_qd_replayable_candidate_count": int(
                inventory["source_qd_replayable_candidate_count"]
            ),
            "exact_candidate_count": len(seen),
            "remaining_candidate_count": len(missing),
            "candidate_with_normal_pass_count": len(pass_sets["normal"]),
            "candidate_with_stressed_pass_count": len(pass_sets["stressed"]),
            "candidate_with_normal_and_stressed_pass_count": len(
                pass_sets["both"]
            ),
        },
        "candidate_pass_sets": {
            key: sorted(value) for key, value in pass_sets.items()
        },
        "best_exact_frontier_point": best,
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "evidence_tier": "E",
        "promotion_status": None,
        "interpretation_boundary": (
            "Candidate pass counts are unique candidate denominators, not "
            "independent confirmation or automatic Tier-Q promotions."
        ),
        "next_action": (
            "RUN_NEXT_TWO_DISJOINT_EXACT_COHORTS"
            if missing
            else "AUDIT_NO_RETUNE_TIER_Q_ELIGIBILITY_AND_CONCENTRATION"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def audit_hazard_19327_tier_q(
    exact_result: Mapping[str, Any],
    *,
    concentration_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Audit the frozen 50K/tier-3 candidate without retuning outcomes."""

    exact = _unwrap_exact_result(exact_result)
    _verify_exact_result(exact)
    _require_diagnostic_safety(exact)
    candidates = [
        row
        for row in exact.get("results", ())
        if str(row.get("candidate_id")) == QUALIFICATION_CANDIDATE_ID
    ]
    if len(candidates) != 1:
        raise AutonomousExactContinuationError(
            "frozen qualification candidate is absent or duplicated"
        )
    candidate = dict(candidates[0])
    cells = [
        dict(row)
        for row in candidate.get("frontier", ())
        if str(row.get("account_label")) == QUALIFICATION_ACCOUNT_LABEL
        and int(row.get("integer_quantity_tier", -1))
        == QUALIFICATION_INTEGER_TIER
        and str(row.get("risk_governor_mode")) == QUALIFICATION_GOVERNOR
    ]
    by_horizon = {int(row["horizon_trading_days"]): row for row in cells}
    if set(by_horizon) != set(QUALIFICATION_HORIZONS) or len(cells) != len(
        QUALIFICATION_HORIZONS
    ):
        raise AutonomousExactContinuationError(
            "frozen qualification frontier is incomplete or duplicated"
        )

    horizon_results: dict[str, Any] = {}
    block_cleared = True
    for horizon in QUALIFICATION_HORIZONS:
        cell = by_horizon[horizon]
        normal = dict(cell.get("normal") or {})
        stressed = dict(cell.get("stressed") or {})
        block_passes = {
            block: int(row.get("pass_count", 0))
            for block, row in dict(stressed.get("by_block") or {}).items()
        }
        total_passes = sum(block_passes.values())
        positive_blocks = sum(value > 0 for value in block_passes.values())
        maximum_share = (
            max(block_passes.values(), default=0) / total_passes
            if total_passes
            else None
        )
        horizon_block_clear = bool(
            total_passes > 0
            and positive_blocks >= 2
            and maximum_share is not None
            and maximum_share <= MAXIMUM_ACCEPTABLE_BLOCK_PASS_SHARE
        )
        block_cleared = block_cleared and horizon_block_clear
        horizon_results[str(horizon)] = {
            "full_coverage_start_count": int(
                cell.get("full_coverage_start_count", 0)
            ),
            "data_censored_start_count": int(
                cell.get("data_censored_start_count", 0)
            ),
            "normal": _qualification_metrics(normal),
            "stressed": _qualification_metrics(stressed),
            "stressed_passes_by_block": dict(sorted(block_passes.items())),
            "stressed_positive_pass_block_count": positive_blocks,
            "maximum_stressed_pass_block_share": maximum_share,
            "block_concentration_cleared": horizon_block_clear,
            "account_rule_compliant": cell.get("account_rule_compliant") is True,
            "legally_executable": cell.get("legally_executable") is True,
        }

    compact_bundle = _compact_candidate_evidence(exact, candidate, by_horizon)
    external_concentration = _verify_concentration_receipt(
        concentration_receipt,
        exact_result_hash=str(exact["result_hash"]),
    )
    external_cleared = external_concentration["cleared"]
    concentration = {
        "maximum_allowed_block_pass_share": MAXIMUM_ACCEPTABLE_BLOCK_PASS_SHARE,
        "maximum_allowed_day_trade_or_event_profit_share": (
            MAXIMUM_ACCEPTABLE_CONCENTRATION_SHARE
        ),
        "block_concentration_cleared": block_cleared,
        "day_trade_event_concentration": external_concentration,
        "all_concentration_dimensions_cleared": (
            block_cleared and external_cleared
        ),
    }

    all_cells = list(by_horizon.values())
    stressed = [dict(row["stressed"]) for row in all_cells]
    gates = {
        "causal_accounting_valid": all(
            row.get("legally_executable") is True
            and row.get("account_rule_compliant") is True
            and int(row.get("hard_compliance_failure_count", 0)) == 0
            for row in all_cells
        ),
        "positive_stressed_economics": all(
            float(row.get("net_total_usd", 0.0)) > 0.0 for row in stressed
        ),
        "acceptable_mll": all(
            float(row.get("mll_breach_rate", math.inf))
            <= MAXIMUM_ACCEPTABLE_MLL_BREACH_RATE
            for row in stressed
        ),
        "useful_target_velocity": (
            int(by_horizon[10]["stressed"].get("pass_count", 0)) > 0
            and float(by_horizon[10]["stressed"].get("pass_rate", 0.0)) >= 0.10
        )
        or int(by_horizon[20]["stressed"].get("pass_count", 0)) > 0,
        "behavioral_uniqueness": bool(
            candidate.get("qd_cell")
            and candidate.get("realized_behavioral_fingerprint")
            and candidate.get("candidate_fingerprint")
        ),
        "compact_evidence_bundle_complete": bool(compact_bundle["complete"]),
        "concentration_cleared": bool(
            concentration["all_concentration_dimensions_cleared"]
        ),
    }
    cleared = all(gates.values())
    pending = [name for name, value in gates.items() if not value]
    core = {
        "schema": QUALIFICATION_AUDIT_SCHEMA,
        "candidate_id": QUALIFICATION_CANDIDATE_ID,
        "account_label": QUALIFICATION_ACCOUNT_LABEL,
        "integer_quantity_tier": QUALIFICATION_INTEGER_TIER,
        "risk_governor_mode": QUALIFICATION_GOVERNOR,
        "frozen_horizons_trading_days": list(QUALIFICATION_HORIZONS),
        "retuning_performed": False,
        "source_exact_result_hash": str(exact["result_hash"]),
        "horizon_results": horizon_results,
        "compact_evidence_bundle": compact_bundle,
        "concentration_audit": concentration,
        "tier_q_gate_results": gates,
        "pending_fields": pending,
        "qualification_status": (
            "TIER_Q_GATE_CLEARED_AWAITING_AUTHORITATIVE_WRITER"
            if cleared
            else "Q_PENDING"
        ),
        "current_evidence_tier": "E",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "next_action": (
            "AUTHORITATIVE_WRITER_MAY_RECORD_TIER_Q"
            if cleared
            else "COMPLETE_FROZEN_CONCENTRATION_EVIDENCE_WITHOUT_RETUNING"
        ),
    }
    return {**core, "audit_hash": stable_hash(core)}


def _inventory_from_initial(
    initial: Mapping[str, Any], initial_ids: Sequence[str]
) -> dict[str, Any]:
    """Fail closed when a composer has no continuation inventory receipt."""

    counters = dict(initial.get("counters") or {})
    if int(counters.get("source_unique_candidate_count", 0)) == len(initial_ids):
        # Tiny fixtures may legitimately contain only the initial cohort.
        core = {
            "source_campaign_id": "hydra_fast_pass_factory_0029",
            "source_bank_entry_count": int(
                counters.get("source_bank_entry_count", len(initial_ids))
            ),
            "source_unique_candidate_count": len(initial_ids),
            "source_qd_replayable_candidate_count": len(initial_ids),
            "source_qd_excluded_candidate_count": 0,
            "sealed_initial_exact_candidate_count": len(initial_ids),
            "remaining_exact_candidate_count": 0,
            "sealed_initial_candidate_ids": list(initial_ids),
            "remaining_candidate_ids": [],
            "ordered_replayable_candidate_ids_hash": stable_hash(list(initial_ids)),
            "source_bank_files": list(
                dict(initial.get("source_banks") or {}).get("files") or ()
            ),
        }
        return {**core, "source_inventory_hash": stable_hash(core)}
    raise AutonomousExactContinuationError(
        "continuation source inventory is required before declaring exhaustion"
    )


def _verify_exact_result(value: Mapping[str, Any]) -> None:
    result = dict(value)
    claimed = str(result.pop("result_hash", ""))
    schema = value.get("schema")
    # A direct worker result deliberately excludes runtime telemetry from its
    # hash.  The existing parent writer wraps the same payload as a branch
    # result and re-hashes the complete mapping, including that telemetry.
    # Both are authoritative forms; neither is normalized or rewritten here.
    if schema == EXACT_REPLAY_SCHEMA:
        result.pop("runtime_seconds", None)
    allowed_schema = schema in {
        EXACT_REPLAY_SCHEMA,
        "hydra_autonomous_economic_branch_result_v1",
    }
    if (
        not claimed
        or stable_hash(result) != claimed
        or not allowed_schema
        or value.get("status") != "COMPLETE_EXACT_CAUSAL_ACCOUNT_SIZE_RACE"
        or value.get("source_campaign_id") != "hydra_fast_pass_factory_0029"
        or (
            schema == "hydra_autonomous_economic_branch_result_v1"
            and value.get("branch_id") != "EXACT_0029_ACCOUNT_SIZE_RACE"
        )
    ):
        raise AutonomousExactContinuationError("exact 0029 result identity/hash drift")


def _unwrap_exact_result(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema") == CONTINUATION_SCHEMA:
        return dict(value["exact_result"])
    return dict(value)


def _verify_continuation_result(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema") == AUTONOMOUS_BRANCH_RESULT_SCHEMA:
        outer = dict(value)
        claimed_outer_hash = str(outer.pop("result_hash", ""))
        embedded = value.get("continuation_result")
        if (
            not claimed_outer_hash
            or stable_hash(outer) != claimed_outer_hash
            or value.get("status") != "COMPLETE"
            or value.get("campaign_id") != AUTONOMOUS_DIRECTOR_CAMPAIGN_ID
            or value.get("decision")
            != "COMPLETE_READ_ONLY_EXACT_CONTINUATION_COHORT"
            or value.get("read_only_worker") is not True
            or value.get("evidence_tier") != "E"
            or value.get("promotion_status") is not None
            or not isinstance(embedded, Mapping)
        ):
            raise AutonomousExactContinuationError(
                "live exact continuation envelope identity/hash drift"
            )
        _require_diagnostic_safety(value)
        verified = _verify_continuation_payload(embedded)
        expected_branch_id = (
            "REMAINING_EXACT_0029_OFFSET_"
            f"{int(verified['cohort_offset']):04d}"
        )
        if value.get("branch_id") != expected_branch_id:
            raise AutonomousExactContinuationError(
                "live exact continuation envelope cohort identity drift"
            )
        return verified
    return _verify_continuation_payload(value)


def _verify_continuation_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    claimed = str(result.pop("result_hash", ""))
    result.pop("runtime_seconds", None)
    if (
        not claimed
        or stable_hash(result) != claimed
        or value.get("schema") != CONTINUATION_SCHEMA
        or value.get("status")
        != "COMPLETE_READ_ONLY_EXACT_CONTINUATION_COHORT"
        or value.get("read_only_worker") is not True
        or value.get("promotion_status") is not None
    ):
        raise AutonomousExactContinuationError(
            "exact continuation result identity/hash drift"
        )
    _require_diagnostic_safety(value)
    return dict(value)


def _require_diagnostic_safety(value: Mapping[str, Any]) -> None:
    counters = dict(value.get("counters") or {})
    for key in _FORBIDDEN_COUNTERS:
        observed = int(value.get(key, counters.get(key, 0)) or 0)
        if observed != 0:
            raise AutonomousExactContinuationError(
                f"read-only exact continuation attempted forbidden counter: {key}"
            )
    if value.get("promotion_status") is not None:
        raise AutonomousExactContinuationError(
            "read-only exact continuation assigned a promotion"
        )


def _candidate_pass_sets(
    exact_results: Sequence[Mapping[str, Any]],
) -> dict[str, set[str]]:
    normal: set[str] = set()
    stressed: set[str] = set()
    for result in exact_results:
        for candidate in result.get("results", ()):
            if int(
                dict(candidate.get("session_contract") or {}).get(
                    "event_violation_count", 0
                )
            ):
                continue
            candidate_id = str(candidate["candidate_id"])
            for cell in candidate.get("frontier", ()):
                if (
                    cell.get("legally_executable") is not True
                    or cell.get("account_rule_compliant") is not True
                ):
                    continue
                if int(dict(cell.get("normal") or {}).get("pass_count", 0)) > 0:
                    normal.add(candidate_id)
                if int(dict(cell.get("stressed") or {}).get("pass_count", 0)) > 0:
                    stressed.add(candidate_id)
    return {"normal": normal, "stressed": stressed, "both": normal & stressed}


def _best_exact_point(
    exact_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    points = [
        dict(result["best_exact_frontier_point"])
        for result in exact_results
        if result.get("best_exact_frontier_point") is not None
    ]
    if not points:
        return None
    return max(
        points,
        key=lambda row: (
            float(dict(row["stressed"])["pass_rate"]),
            float(dict(row["normal"])["pass_rate"]),
            -float(dict(row["stressed"])["mll_breach_rate"]),
            -int(row["horizon_trading_days"]),
            float(dict(row["stressed"])["target_progress_median"]),
            -int(row["account_size_usd"]),
        ),
    )


def _qualification_metrics(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "episode_count",
            "pass_count",
            "pass_rate",
            "mll_breach_count",
            "mll_breach_rate",
            "consistency_compliance_rate",
            "net_total_usd",
            "net_median_usd",
            "target_progress_p25",
            "target_progress_median",
            "minimum_mll_buffer_usd",
            "median_days_to_target",
            "episode_path_hash",
        )
    }


def _compact_candidate_evidence(
    exact: Mapping[str, Any],
    candidate: Mapping[str, Any],
    by_horizon: Mapping[int, Mapping[str, Any]],
) -> dict[str, Any]:
    event_receipt = dict(candidate.get("source_event_evidence") or {})
    source_manifest = dict(exact.get("source_manifest") or {})
    rule_snapshot = dict(exact.get("official_rule_snapshot") or {})
    grid = dict(exact.get("frozen_grid") or {})
    core = {
        "schema": COMPACT_EVIDENCE_SCHEMA,
        "candidate_id": QUALIFICATION_CANDIDATE_ID,
        "candidate_fingerprint": candidate.get("candidate_fingerprint"),
        "realized_behavioral_fingerprint": candidate.get(
            "realized_behavioral_fingerprint"
        ),
        "qd_cell": candidate.get("qd_cell"),
        "source_campaign_id": exact.get("source_campaign_id"),
        "source_manifest_hash": source_manifest.get("manifest_hash"),
        "source_manifest_file_sha256": source_manifest.get("file_sha256"),
        "source_event_file_sha256": event_receipt.get("sha256"),
        "source_event_content_sha256": event_receipt.get("uncompressed_sha256"),
        "source_event_record_count": event_receipt.get("record_count"),
        "official_rule_snapshot_hash": rule_snapshot.get("parsed_rule_hash"),
        "frozen_grid_hash": grid.get("grid_hash"),
        "candidate_result_hash": candidate.get("candidate_result_hash"),
        "source_exact_result_hash": exact.get("result_hash"),
        "account_label": QUALIFICATION_ACCOUNT_LABEL,
        "integer_quantity_tier": QUALIFICATION_INTEGER_TIER,
        "risk_governor_mode": QUALIFICATION_GOVERNOR,
        "horizon_episode_path_hashes": {
            str(horizon): {
                "normal": dict(by_horizon[horizon].get("normal") or {}).get(
                    "episode_path_hash"
                ),
                "stressed": dict(by_horizon[horizon].get("stressed") or {}).get(
                    "episode_path_hash"
                ),
            }
            for horizon in QUALIFICATION_HORIZONS
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "retuning_performed": False,
    }
    required = (
        "candidate_fingerprint",
        "realized_behavioral_fingerprint",
        "qd_cell",
        "source_manifest_hash",
        "source_manifest_file_sha256",
        "source_event_file_sha256",
        "source_event_content_sha256",
        "official_rule_snapshot_hash",
        "frozen_grid_hash",
        "candidate_result_hash",
        "source_exact_result_hash",
    )
    hashes_complete = all(core.get(key) not in {None, ""} for key in required)
    path_hashes_complete = all(
        all(value not in {None, ""} for value in pair.values())
        for pair in core["horizon_episode_path_hashes"].values()
    )
    complete = bool(hashes_complete and path_hashes_complete)
    payload = {**core, "complete": complete}
    return {**payload, "bundle_hash": stable_hash(payload)}


def _verify_concentration_receipt(
    value: Mapping[str, Any] | None,
    *,
    exact_result_hash: str,
) -> dict[str, Any]:
    required_metrics = (
        "maximum_single_day_profit_share",
        "maximum_single_trade_profit_share",
        "maximum_single_event_profit_share",
    )
    if value is None:
        return {
            "status": "NOT_PROVIDED",
            "cleared": False,
            "missing_fields": list(required_metrics),
        }
    receipt = dict(value)
    claimed = str(receipt.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(receipt) != claimed
        or str(value.get("candidate_id")) != QUALIFICATION_CANDIDATE_ID
        or str(value.get("source_exact_result_hash")) != exact_result_hash
    ):
        raise AutonomousExactContinuationError(
            "candidate concentration receipt identity/hash drift"
        )
    metrics = {key: float(value.get(key, math.inf)) for key in required_metrics}
    cleared = all(
        math.isfinite(observed)
        and 0.0 <= observed <= MAXIMUM_ACCEPTABLE_CONCENTRATION_SHARE
        for observed in metrics.values()
    )
    return {
        "status": "CLEARED" if cleared else "NOT_CLEARED",
        "cleared": cleared,
        "maximum_allowed_share": MAXIMUM_ACCEPTABLE_CONCENTRATION_SHARE,
        **metrics,
        "receipt_hash": claimed,
    }


__all__ = [
    "AutonomousExactContinuationError",
    "CONTINUATION_COMPOSITE_SCHEMA",
    "CONTINUATION_PLAN_SCHEMA",
    "CONTINUATION_SCHEMA",
    "INITIAL_EXACT_COHORT_SIZE",
    "QUALIFICATION_AUDIT_SCHEMA",
    "QUALIFICATION_CANDIDATE_ID",
    "audit_hazard_19327_tier_q",
    "compose_remaining_0029_exact_results",
    "inspect_remaining_0029_source",
    "plan_remaining_0029_exact_jobs",
    "remaining_0029_exact_worker",
]
