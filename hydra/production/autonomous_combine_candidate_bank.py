"""Read-only Combine candidate bank derived from exact 0029 evidence.

This module classifies, but never promotes, candidates.  Tier-Q classification
follows the autonomous master contract: valid causal/accounting evidence,
positive stressed economics, controlled MLL, useful target velocity,
behavioural uniqueness, and a complete compact EvidenceBundle.  Concentration
is intentionally *not* a Tier-Q gate; it remains diagnostic and is required,
along with frozen final-development evidence, before a candidate can be marked
G-ready.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_exact_continuation import (
    _require_diagnostic_safety,
    _unwrap_exact_result,
    _verify_continuation_result,
    _verify_exact_result,
    compose_remaining_0029_exact_results,
)


SCHEMA = "hydra_autonomous_combine_candidate_bank_v1"
COMPACT_BUNDLE_SCHEMA = "hydra_autonomous_compact_candidate_bundle_v1"
CONCENTRATION_RECEIPT_SCHEMA = "hydra_candidate_concentration_receipt_v1"

MAXIMUM_TIER_Q_MLL_BREACH_RATE = 0.10
MAXIMUM_G_BLOCK_PASS_SHARE = 0.75
MAXIMUM_G_CONCENTRATION_SHARE = 0.25


class AutonomousCombineCandidateBankError(RuntimeError):
    """Exact evidence cannot be classified without violating an invariant."""


def build_autonomous_combine_candidate_bank(
    initial_exact_result: Mapping[str, Any],
    continuation_results: Sequence[Mapping[str, Any]] = (),
    *,
    concentration_receipts: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Aggregate exact cohorts and classify unique candidates without writes."""

    # The existing composer is the source-bank/exhaustion and disjoint-cohort
    # guard.  Its output remains a compact provenance receipt for this bank.
    composite = compose_remaining_0029_exact_results(
        initial_exact_result, continuation_results
    )
    exact_results = [_unwrap_exact_result(initial_exact_result)]
    for wrapper_value in continuation_results:
        wrapper = _verify_continuation_result(wrapper_value)
        exact_results.append(dict(wrapper["exact_result"]))
    for result in exact_results:
        _verify_exact_result(result)
        _require_diagnostic_safety(result)

    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    selected_ids: list[str] = []
    for result in exact_results:
        expected = tuple(
            str(value) for value in result["selection"]["selected_candidate_ids"]
        )
        result_rows = [dict(value) for value in result.get("results", ())]
        observed = tuple(str(value.get("candidate_id") or "") for value in result_rows)
        if (
            len(observed) != len(set(observed))
            or set(observed) != set(expected)
            or any(not value for value in observed)
        ):
            raise AutonomousCombineCandidateBankError(
                "exact candidate rows differ from their frozen cohort selection"
            )
        selected_ids.extend(expected)
        rows.extend((row, result) for row in result_rows)
    if len(selected_ids) != len(set(selected_ids)):
        raise AutonomousCombineCandidateBankError(
            "candidate ID appears in more than one exact cohort"
        )

    qd_counts = Counter(str(row.get("qd_cell") or "") for row, _ in rows)
    behavior_counts = Counter(
        str(row.get("realized_behavioral_fingerprint") or "") for row, _ in rows
    )
    receipts = dict(concentration_receipts or {})
    unknown_receipts = set(receipts).difference(selected_ids)
    if unknown_receipts:
        raise AutonomousCombineCandidateBankError(
            "concentration receipt references an unknown candidate"
        )

    classified = [
        _classify_candidate(
            candidate,
            source_result,
            qd_counts=qd_counts,
            behavior_counts=behavior_counts,
            concentration_receipt=receipts.get(str(candidate["candidate_id"])),
        )
        for candidate, source_result in rows
    ]
    classified.sort(
        key=lambda row: (
            bool(row["tier_q_contract_cleared"]),
            bool(row["g_ready"]),
            _cell_rank(row.get("best_safe_cell")),
            str(row["candidate_id"]),
        ),
        reverse=True,
    )

    normal_pass_ids = {
        str(row["candidate_id"])
        for row in classified
        if row["observed_passes"]["normal_any"]
    }
    stressed_pass_ids = {
        str(row["candidate_id"])
        for row in classified
        if row["observed_passes"]["stressed_any"]
    }
    safe_pass_ids = {
        str(row["candidate_id"])
        for row in classified
        if row["best_safe_cell"] is not None
    }
    q_ids = {
        str(row["candidate_id"])
        for row in classified
        if row["tier_q_contract_cleared"]
    }
    g_ready_ids = {
        str(row["candidate_id"]) for row in classified if row["g_ready"]
    }
    counters = dict(composite["aggregate_counters"])
    if int(counters["exact_candidate_count"]) != len(classified):
        raise AutonomousCombineCandidateBankError(
            "composite exact-candidate denominator differs from classified bank"
        )
    if any(int(counters.get(key, 0)) for key in (
        "promotion_count",
        "xfa_paths_started",
        "data_purchase_count",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
    )):
        raise AutonomousCombineCandidateBankError(
            "candidate bank source contains a forbidden side effect"
        )

    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
        "source_composite_result_hash": str(composite["result_hash"]),
        "source_bank_exhausted": bool(composite["source_bank_exhausted"]),
        "source_inventory": dict(composite["source_inventory"]),
        "candidates": classified,
        "counts": {
            "exact_candidate_count": len(classified),
            "candidate_with_any_normal_pass_count": len(normal_pass_ids),
            "candidate_with_any_stressed_pass_count": len(stressed_pass_ids),
            "candidate_with_normal_and_stressed_pass_count": len(
                normal_pass_ids & stressed_pass_ids
            ),
            "candidate_with_safe_normal_and_stressed_pass_count": len(
                safe_pass_ids
            ),
            "tier_q_contract_cleared_count": len(q_ids),
            "g_ready_count": len(g_ready_ids),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
        },
        "candidate_ids": {
            "normal_pass_any": sorted(normal_pass_ids),
            "stressed_pass_any": sorted(stressed_pass_ids),
            "safe_normal_and_stressed_pass": sorted(safe_pass_ids),
            "tier_q_contract_cleared": sorted(q_ids),
            "g_ready": sorted(g_ready_ids),
        },
        "tier_contract": {
            "tier_q_requires_concentration": False,
            "tier_q_required_fields": [
                "causal_accounting_valid",
                "positive_stressed_economics",
                "acceptable_mll",
                "useful_target_velocity",
                "behavioral_uniqueness",
                "compact_evidence_bundle_complete",
            ],
            "concentration_is_diagnostic_and_tier_g_gate": True,
            "classification_mutates_authoritative_registry": False,
        },
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "independent_confirmation_claimed": False,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": (
            "FREEZE_TIER_Q_CLASSIFICATIONS_FOR_NEXT_HONEST_EVIDENCE_TIER"
            if q_ids
            else "CONTINUE_EXACT_CLASSIFICATION_WITHOUT_LOWERING_GATES"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def _classify_candidate(
    candidate: Mapping[str, Any],
    source_result: Mapping[str, Any],
    *,
    qd_counts: Mapping[str, int],
    behavior_counts: Mapping[str, int],
    concentration_receipt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_id"])
    session_valid = int(
        dict(candidate.get("session_contract") or {}).get(
            "event_violation_count", 0
        )
    ) == 0
    cells = [
        dict(value)
        for value in candidate.get("frontier", ())
        if session_valid
        and value.get("legally_executable") is True
        and value.get("account_rule_compliant") is True
        and int(value.get("hard_compliance_failure_count", 0)) == 0
        and isinstance(value.get("normal"), Mapping)
        and isinstance(value.get("stressed"), Mapping)
    ]
    normal_pass_any = any(
        int(dict(cell["normal"]).get("pass_count", 0)) > 0 for cell in cells
    )
    stressed_pass_any = any(
        int(dict(cell["stressed"]).get("pass_count", 0)) > 0 for cell in cells
    )
    paired_pass_cells = [
        cell
        for cell in cells
        if int(dict(cell["normal"]).get("pass_count", 0)) > 0
        and int(dict(cell["stressed"]).get("pass_count", 0)) > 0
    ]
    safe_cells = [
        cell
        for cell in paired_pass_cells
        if session_valid
        and float(dict(cell["stressed"]).get("net_total_usd", -math.inf)) > 0.0
        and float(dict(cell["stressed"]).get("mll_breach_rate", math.inf))
        <= MAXIMUM_TIER_Q_MLL_BREACH_RATE
        and _cell_account_paths_complete(cell)
    ]
    best_safe = max(safe_cells, key=_cell_rank) if safe_cells else None
    best_observed = max(paired_pass_cells, key=_cell_rank) if paired_pass_cells else None

    qd = str(candidate.get("qd_cell") or "")
    behavior = str(candidate.get("realized_behavioral_fingerprint") or "")
    unique_behavior = bool(
        qd
        and behavior
        and qd_counts.get(qd, 0) == 1
        and behavior_counts.get(behavior, 0) == 1
        and candidate.get("candidate_fingerprint")
    )
    bundle = _compact_bundle(candidate, source_result, best_safe)
    q_gates = {
        "causal_accounting_valid": bool(
            best_safe is not None
            and session_valid
            and _cell_account_paths_complete(best_safe)
        ),
        "positive_stressed_economics": bool(
            best_safe is not None
            and float(dict(best_safe["stressed"])["net_total_usd"]) > 0.0
        ),
        "acceptable_mll": bool(
            best_safe is not None
            and float(dict(best_safe["stressed"])["mll_breach_rate"])
            <= MAXIMUM_TIER_Q_MLL_BREACH_RATE
        ),
        "useful_target_velocity": bool(
            best_safe is not None
            and int(dict(best_safe["normal"])["pass_count"]) > 0
            and int(dict(best_safe["stressed"])["pass_count"]) > 0
        ),
        "behavioral_uniqueness": unique_behavior,
        "compact_evidence_bundle_complete": bool(bundle["complete"]),
    }
    tier_q = all(q_gates.values())

    concentration = _concentration_diagnostic(
        candidate_id,
        best_safe,
        concentration_receipt,
        source_exact_result_hash=str(source_result["result_hash"]),
    )
    g_gates = {
        "tier_q_contract_cleared": tier_q,
        "multiple_complete_normal_and_stressed_passes": bool(
            best_safe is not None
            and int(dict(best_safe["normal"])["pass_count"]) >= 2
            and int(dict(best_safe["stressed"])["pass_count"]) >= 2
        ),
        "passes_in_multiple_temporal_contexts": bool(
            concentration["positive_pass_context_count"] >= 2
        ),
        "block_concentration_cleared": bool(
            concentration["block_concentration_cleared"]
        ),
        "day_trade_event_concentration_cleared": bool(
            concentration["day_trade_event_concentration_cleared"]
        ),
        "frozen_final_development_complete": bool(
            concentration["frozen_final_development_complete"]
        ),
        "complete_accounting_evidence": bool(
            best_safe is not None and _cell_account_paths_complete(best_safe)
        ),
    }
    g_ready = all(g_gates.values())
    if g_ready:
        status = "G_READY_CLASSIFICATION_AWAITING_AUTHORITATIVE_WRITER"
    elif tier_q:
        status = "TIER_Q_CLASSIFIED_DEVELOPMENT_ONLY"
    elif best_observed is not None:
        status = "OBSERVED_COMBINE_PASS_DEVELOPMENT_NOT_TIER_Q"
    else:
        status = "NO_PAIRED_EXACT_COMBINE_PASS"
    return {
        "candidate_id": candidate_id,
        "candidate_fingerprint": candidate.get("candidate_fingerprint"),
        "realized_behavioral_fingerprint": behavior or None,
        "qd_cell": qd or None,
        "source_exact_result_hash": str(source_result["result_hash"]),
        "observed_passes": {
            "normal_any": normal_pass_any,
            "stressed_any": stressed_pass_any,
            "normal_and_stressed_same_cell": bool(paired_pass_cells),
            "development_only": True,
        },
        "best_observed_pass_cell": _compact_cell(best_observed),
        "best_safe_cell": _compact_cell(best_safe),
        "tier_q_gate_results": q_gates,
        "tier_q_contract_cleared": tier_q,
        "concentration_diagnostic": concentration,
        "tier_g_gate_results": g_gates,
        "g_ready": g_ready,
        "compact_evidence_bundle": bundle,
        "classification_status": status,
        "computed_development_tier": "Q" if tier_q else "E",
        "authoritative_promotion_status": None,
        "independent_confirmation_claimed": False,
    }


def _cell_account_paths_complete(cell: Mapping[str, Any]) -> bool:
    return all(
        int(dict(cell.get(scenario) or {}).get("episode_count", 0)) > 0
        and bool(dict(cell.get(scenario) or {}).get("episode_path_hash"))
        for scenario in ("normal", "stressed")
    )


def _cell_rank(cell: Mapping[str, Any] | None) -> tuple[Any, ...]:
    if cell is None:
        return (False, 0.0, 0.0, -math.inf, -math.inf, -math.inf)
    normal = dict(cell["normal"])
    stressed = dict(cell["stressed"])
    return (
        True,
        float(stressed.get("pass_rate", 0.0)),
        float(normal.get("pass_rate", 0.0)),
        -float(stressed.get("mll_breach_rate", math.inf)),
        -int(cell.get("horizon_trading_days", 10**9)),
        float(stressed.get("target_progress_median", -math.inf)),
        float(stressed.get("net_total_usd", -math.inf)),
        -int(cell.get("account_size_usd", 10**9)),
    )


def _compact_cell(cell: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if cell is None:
        return None
    return {
        "candidate_id": cell.get("candidate_id"),
        "account_label": cell.get("account_label"),
        "account_size_usd": cell.get("account_size_usd"),
        "integer_quantity_tier": cell.get("integer_quantity_tier"),
        "risk_governor_mode": cell.get("risk_governor_mode"),
        "horizon_trading_days": cell.get("horizon_trading_days"),
        "full_coverage_start_count": cell.get("full_coverage_start_count"),
        "data_censored_start_count": cell.get("data_censored_start_count"),
        "normal": _compact_scenario(dict(cell["normal"])),
        "stressed": _compact_scenario(dict(cell["stressed"])),
        "cell_hash": stable_hash(cell),
    }


def _compact_scenario(value: Mapping[str, Any]) -> dict[str, Any]:
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


def _compact_bundle(
    candidate: Mapping[str, Any],
    source_result: Mapping[str, Any],
    selected_cell: Mapping[str, Any] | None,
) -> dict[str, Any]:
    event = dict(candidate.get("source_event_evidence") or {})
    manifest = dict(source_result.get("source_manifest") or {})
    rules = dict(source_result.get("official_rule_snapshot") or {})
    grid = dict(source_result.get("frozen_grid") or {})
    core = {
        "schema": COMPACT_BUNDLE_SCHEMA,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_fingerprint": candidate.get("candidate_fingerprint"),
        "realized_behavioral_fingerprint": candidate.get(
            "realized_behavioral_fingerprint"
        ),
        "qd_cell": candidate.get("qd_cell"),
        "source_exact_result_hash": source_result.get("result_hash"),
        "source_manifest_hash": manifest.get("manifest_hash"),
        "source_manifest_file_sha256": manifest.get("file_sha256"),
        "official_rule_snapshot_hash": rules.get("parsed_rule_hash"),
        "frozen_grid_hash": grid.get("grid_hash"),
        "source_event_file_sha256": event.get("sha256"),
        "source_event_content_sha256": event.get("uncompressed_sha256"),
        "source_event_record_count": event.get("record_count"),
        "candidate_result_hash": candidate.get("candidate_result_hash"),
        "selected_cell_hash": (
            stable_hash(selected_cell) if selected_cell is not None else None
        ),
        "normal_episode_path_hash": (
            dict(selected_cell["normal"]).get("episode_path_hash")
            if selected_cell is not None
            else None
        ),
        "stressed_episode_path_hash": (
            dict(selected_cell["stressed"]).get("episode_path_hash")
            if selected_cell is not None
            else None
        ),
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
    }
    required = (
        "candidate_id",
        "candidate_fingerprint",
        "realized_behavioral_fingerprint",
        "qd_cell",
        "source_exact_result_hash",
        "source_manifest_hash",
        "source_manifest_file_sha256",
        "official_rule_snapshot_hash",
        "frozen_grid_hash",
        "source_event_file_sha256",
        "source_event_content_sha256",
        "candidate_result_hash",
        "selected_cell_hash",
        "normal_episode_path_hash",
        "stressed_episode_path_hash",
    )
    complete = all(core.get(key) not in {None, ""} for key in required)
    payload = {**core, "complete": complete}
    return {**payload, "bundle_hash": stable_hash(payload)}


def _concentration_diagnostic(
    candidate_id: str,
    selected_cell: Mapping[str, Any] | None,
    receipt_value: Mapping[str, Any] | None,
    *,
    source_exact_result_hash: str,
) -> dict[str, Any]:
    stressed = dict((selected_cell or {}).get("stressed") or {})
    by_block = dict(stressed.get("by_block") or {})
    passes = {
        str(block): int(dict(value).get("pass_count", 0))
        for block, value in by_block.items()
    }
    total = sum(passes.values())
    positive_contexts = sum(value > 0 for value in passes.values())
    maximum_block_share = (
        max(passes.values(), default=0) / total if total else None
    )
    block_cleared = bool(
        total > 0
        and positive_contexts >= 2
        and maximum_block_share is not None
        and maximum_block_share <= MAXIMUM_G_BLOCK_PASS_SHARE
    )
    external = _verify_generic_concentration_receipt(
        receipt_value,
        candidate_id=candidate_id,
        source_exact_result_hash=source_exact_result_hash,
    )
    return {
        "passes_by_block": dict(sorted(passes.items())),
        "positive_pass_context_count": positive_contexts,
        "maximum_block_pass_share": maximum_block_share,
        "maximum_allowed_block_pass_share": MAXIMUM_G_BLOCK_PASS_SHARE,
        "block_concentration_cleared": block_cleared,
        "day_trade_event_concentration": external,
        "day_trade_event_concentration_cleared": bool(external["cleared"]),
        "frozen_final_development_complete": bool(
            external["frozen_final_development_complete"]
        ),
        "tier_q_gate": False,
        "tier_g_gate": True,
    }


def _verify_generic_concentration_receipt(
    value: Mapping[str, Any] | None,
    *,
    candidate_id: str,
    source_exact_result_hash: str,
) -> dict[str, Any]:
    metrics = (
        "maximum_single_day_profit_share",
        "maximum_single_trade_profit_share",
        "maximum_single_event_profit_share",
    )
    if value is None:
        return {
            "status": "NOT_AVAILABLE",
            "cleared": False,
            "frozen_final_development_complete": False,
            "missing_fields": [*metrics, "final_development_evidence_hash"],
        }
    receipt = dict(value)
    claimed = str(receipt.pop("receipt_hash", ""))
    if (
        not claimed
        or stable_hash(receipt) != claimed
        or value.get("schema") != CONCENTRATION_RECEIPT_SCHEMA
        or str(value.get("candidate_id")) != candidate_id
        or str(value.get("source_exact_result_hash")) != source_exact_result_hash
    ):
        raise AutonomousCombineCandidateBankError(
            "concentration receipt identity/hash drift"
        )
    observed = {key: float(value.get(key, math.inf)) for key in metrics}
    final_complete = bool(value.get("final_development_evidence_hash"))
    cleared = final_complete and all(
        math.isfinite(number)
        and 0.0 <= number <= MAXIMUM_G_CONCENTRATION_SHARE
        for number in observed.values()
    )
    return {
        "status": "CLEARED" if cleared else "NOT_CLEARED",
        "cleared": cleared,
        "maximum_allowed_share": MAXIMUM_G_CONCENTRATION_SHARE,
        **observed,
        "frozen_final_development_complete": final_complete,
        "final_development_evidence_hash": value.get(
            "final_development_evidence_hash"
        ),
        "receipt_hash": claimed,
    }


__all__ = [
    "AutonomousCombineCandidateBankError",
    "CONCENTRATION_RECEIPT_SCHEMA",
    "SCHEMA",
    "build_autonomous_combine_candidate_bank",
]
