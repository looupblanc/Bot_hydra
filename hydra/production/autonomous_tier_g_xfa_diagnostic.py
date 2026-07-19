"""Read-only XFA diagnostics for exact Tier-G Combine transitions.

This relay consumes the verified continuation handoff, freezes the executable
account-size XFA adapter, and evaluates Standard and Consistency as mutually
exclusive alternatives.  It never edits the Tier-G receipt, selects a payout
path, writes authoritative state, or exposes an order route.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_tier_g_xfa_handoff import (
    materialize_transition_trajectories,
    verify_tier_g_combine_xfa_handoffs,
)
from hydra.propfirm.account_size_xfa import (
    DEFAULT_RULE_SNAPSHOT,
    freeze_account_size_xfa_handoff,
    load_account_size_xfa_rules,
    run_account_size_xfa_alternatives,
)


SCHEMA = "hydra_autonomous_tier_g_xfa_diagnostic_v1"
PATH_RECORD_SCHEMA = "hydra_autonomous_tier_g_xfa_path_record_v1"
AGGREGATE_SCHEMA = "hydra_autonomous_tier_g_xfa_path_aggregate_v1"
STATUS = "COMPLETE_READ_ONLY_TIER_G_XFA_DIAGNOSTIC"
PATHS = ("STANDARD", "CONSISTENCY")
SCENARIOS = ("NORMAL", "STRESSED")
DEFAULT_XFA_HORIZON_DAYS = 120
_ACCOUNT_DEATH_TERMINALS = {
    "MLL_BREACHED",
    "HARD_RULE_FAILURE",
    "INACTIVITY_RISK",
}


class AutonomousTierGXfaDiagnosticError(RuntimeError):
    """The XFA diagnostic cannot be reconciled without mixing evidence."""


def build_autonomous_tier_g_xfa_diagnostic(
    continuation_handoff: Mapping[str, Any],
    *,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
    horizon_days: int = DEFAULT_XFA_HORIZON_DAYS,
) -> dict[str, Any]:
    """Run both frozen XFA alternatives for every ready Combine transition."""

    source = verify_tier_g_combine_xfa_handoffs(continuation_handoff)
    if int(horizon_days) < 1:
        raise AutonomousTierGXfaDiagnosticError("XFA horizon must be positive")
    candidates = {
        str(row["candidate_id"]): dict(row)
        for row in source.get("candidate_handoffs", ())
    }
    transitions = [dict(row) for row in source.get("transitions", ())]
    if not transitions:
        raise AutonomousTierGXfaDiagnosticError(
            "verified handoff contains no ready XFA transition"
        )
    transition_ids = [str(row["transition_id"]) for row in transitions]
    if len(transition_ids) != len(set(transition_ids)):
        raise AutonomousTierGXfaDiagnosticError(
            "ready Combine transition identity is duplicated"
        )
    attempt_denominator = _uniform_attempt_denominator(source)
    rule_cache: dict[str, Any] = {}
    alternative_results: list[dict[str, Any]] = []
    path_records: list[dict[str, Any]] = []

    for transition in sorted(transitions, key=lambda row: str(row["transition_id"])):
        candidate_id = str(transition["candidate_id"])
        candidate = candidates.get(candidate_id)
        if candidate is None:
            raise AutonomousTierGXfaDiagnosticError(
                f"transition candidate handoff is absent: {candidate_id}"
            )
        _verify_transition_candidate_binding(transition, candidate)
        account_label = str(transition["account_label"])
        rules = rule_cache.setdefault(
            account_label,
            load_account_size_xfa_rules(
                account_label, snapshot_path=rule_snapshot_path
            ),
        )
        source_profile = dict(candidate["xfa_profile"])
        source_book = dict(candidate["xfa_book"])
        frozen = freeze_account_size_xfa_handoff(
            candidate_id=candidate_id,
            combine_book_hash=str(transition["combine_book_hash"]),
            component_priority=tuple(source_book["component_ids"]),
            rules=rules,
            risk_multiplier=float(source_profile["risk_multiplier"]),
            maximum_simultaneous_positions=int(
                source_profile["maximum_concurrent_sleeves"]
            ),
            maximum_mini_equivalent=float(
                source_profile["maximum_mini_equivalent"]
            ),
            same_market_exclusive=True,
            profile_id=str(source_profile["xfa_profile_id"]),
        )
        trajectories = materialize_transition_trajectories(
            source, str(transition["transition_id"])
        )
        result = run_account_size_xfa_alternatives(
            trajectories,
            tuple(int(day) for day in transition["eligible_session_days"]),
            handoff=frozen,
            rules=rules,
            transition_id=str(transition["transition_id"]),
            combine_path_hash=str(transition["combine_path_hash"]),
            start_day=int(transition["xfa_start_day"]),
            horizon_days=int(horizon_days),
        )
        mapped = result.to_dict()
        _verify_engine_result(
            mapped,
            transition,
            frozen_handoff_hash=frozen.handoff_hash,
        )
        alternative_results.append(mapped)
        for path in PATHS:
            path_records.append(
                _path_record(
                    transition=transition,
                    engine_result=mapped,
                    path=path,
                    frozen_handoff_hash=frozen.handoff_hash,
                )
            )

    grouped = _aggregate_records(
        path_records, combine_attempt_denominator=attempt_denominator
    )
    path_totals = _path_totals(
        path_records, combine_attempt_denominator=attempt_denominator
    )
    counts = _counts(transitions, path_records)
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": STATUS,
        "source_continuation_handoff_hash": str(source["result_hash"]),
        "source_tier_g_graduation_hash": str(
            source["source_tier_g_graduation_hash"]
        ),
        "official_rule_snapshot_hashes": {
            label: rules.fingerprint for label, rules in sorted(rule_cache.items())
        },
        "requested_xfa_horizon_days": int(horizon_days),
        "combine_attempt_denominator_per_candidate_scenario": attempt_denominator,
        "alternative_results": alternative_results,
        "path_records": sorted(
            path_records, key=lambda row: str(row["path_record_id"])
        ),
        "candidate_scenario_path_aggregates": grouped,
        "path_totals_by_scenario": path_totals,
        "counts": counts,
        "economic_hashes": _economic_hashes(path_records, grouped, counts),
        "alternative_path_audit": {
            "unique_combine_transition_count": len(transitions),
            "expected_path_count_per_transition": 2,
            "expected_alternative_path_count": len(transitions) * 2,
            "observed_alternative_path_count": len(path_records),
            "exactly_one_standard_and_one_consistency_per_transition": True,
            "first_payout_count_never_exceeds_path_count": all(
                int(row["first_payout_count"]) <= int(row["path_count"])
                for row in grouped
            ),
            "standard_and_consistency_are_mutually_exclusive": True,
            "standard_and_consistency_ev_summed": False,
            "outcome_selected_path": None,
        },
        "evidence_role": "DIAGNOSTIC_XFA_FROM_VIEWED_TIER_G_DEVELOPMENT",
        "independent_confirmation_claimed": False,
        "promotion_status": None,
        "combine_outcomes_modified": False,
        "underlying_signals_modified": False,
        "database_writes": 0,
        "registry_writes": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": "RANK_TIER_G_BOOKS_WITH_SEPARATE_XFA_ALTERNATIVES",
    }
    output = {**core, "result_hash": stable_hash(core)}
    verify_autonomous_tier_g_xfa_diagnostic(output)
    return output


def verify_autonomous_tier_g_xfa_diagnostic(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify deterministic identities, no-sum semantics, and zero side effects."""

    row = dict(value)
    claimed = row.pop("result_hash", None)
    if row.get("schema") != SCHEMA or row.get("status") != STATUS:
        raise AutonomousTierGXfaDiagnosticError("XFA diagnostic schema/status drift")
    if claimed != stable_hash(row):
        raise AutonomousTierGXfaDiagnosticError("XFA diagnostic result hash drift")
    for field in (
        "database_writes",
        "registry_writes",
        "q4_access_count_delta",
        "data_purchase_count",
        "broker_connections",
        "orders",
    ):
        if int(row.get(field, -1)) != 0:
            raise AutonomousTierGXfaDiagnosticError(
                f"read-only XFA diagnostic side effect: {field}"
            )
    audit = dict(row.get("alternative_path_audit") or {})
    if (
        audit.get("standard_and_consistency_are_mutually_exclusive") is not True
        or audit.get("standard_and_consistency_ev_summed") is not False
        or audit.get("outcome_selected_path") is not None
        or audit.get("exactly_one_standard_and_one_consistency_per_transition")
        is not True
    ):
        raise AutonomousTierGXfaDiagnosticError("XFA alternative separation drift")

    raw_records = [dict(value) for value in row.get("path_records", ())]
    records = [dict(value) for value in raw_records]
    record_ids: set[str] = set()
    by_transition: dict[str, set[str]] = defaultdict(set)
    transition_bindings: dict[str, tuple[Any, ...]] = {}
    for record in records:
        record_hash = record.pop("path_record_hash", None)
        record_id = str(record.get("path_record_id") or "")
        if (
            record.get("schema") != PATH_RECORD_SCHEMA
            or not record_id
            or record_id in record_ids
            or record_hash != stable_hash(record)
        ):
            raise AutonomousTierGXfaDiagnosticError(
                "XFA path-record identity/hash drift or duplication"
            )
        record_ids.add(record_id)
        path = str(record["path"])
        if path not in PATHS:
            raise AutonomousTierGXfaDiagnosticError("unsupported XFA path record")
        transition_id = str(record["transition_id"])
        expected_record_id = (
            "xfa-path-"
            + stable_hash(
                {
                    "transition_id": transition_id,
                    "path": path,
                    "engine_result_hash": str(record["engine_result_hash"]),
                }
            )[:24]
        )
        if record_id != expected_record_id:
            raise AutonomousTierGXfaDiagnosticError(
                "XFA path-record deterministic identity drift"
            )
        binding = (
            str(record["candidate_id"]),
            str(record["scenario"]),
            str(record["combine_path_hash"]),
            str(record["combine_book_hash"]),
            str(record["account_label"]),
        )
        prior_binding = transition_bindings.setdefault(transition_id, binding)
        if prior_binding != binding:
            raise AutonomousTierGXfaDiagnosticError(
                "XFA alternatives disagree on their Combine transition binding"
            )
        if path in by_transition[transition_id]:
            raise AutonomousTierGXfaDiagnosticError(
                "duplicate XFA path for one Combine transition"
            )
        by_transition[transition_id].add(path)
        first = int(record["first_payout_count"])
        if first not in {0, 1} or bool(record["first_payout_day"] is not None) != bool(
            first
        ):
            raise AutonomousTierGXfaDiagnosticError(
                "first-payout uniqueness/identity drift"
            )
    if not by_transition or any(
        paths != set(PATHS) for paths in by_transition.values()
    ):
        raise AutonomousTierGXfaDiagnosticError(
            "Combine transition lacks one frozen XFA alternative"
        )

    results = [dict(value) for value in row.get("alternative_results", ())]
    if len(results) != len(by_transition):
        raise AutonomousTierGXfaDiagnosticError("engine result/transition count drift")
    result_ids: set[str] = set()
    result_by_transition: dict[str, dict[str, Any]] = {}
    for result in results:
        result_hash = result.pop("result_hash", None)
        transition_id = str(result.get("transition_id") or "")
        if (
            transition_id in result_ids
            or result_hash != stable_hash(result)
            or set(dict(result.get("alternatives") or {})) != set(PATHS)
            or result.get("standard_and_consistency_are_alternatives") is not True
            or result.get("sum_standard_and_consistency_ev_allowed") is not False
            or result.get("selected_path") is not None
        ):
            raise AutonomousTierGXfaDiagnosticError(
                "XFA engine result identity/hash/alternative drift"
            )
        result_ids.add(transition_id)
        result_by_transition[transition_id] = {
            **result,
            "result_hash": result_hash,
        }
    if result_ids != set(by_transition):
        raise AutonomousTierGXfaDiagnosticError(
            "engine and path-record transition inventories differ"
        )
    for record in raw_records:
        result = result_by_transition[str(record["transition_id"])]
        alternative = dict(dict(result["alternatives"])[str(record["path"])])
        comparisons = (
            (record["engine_result_hash"], result["result_hash"]),
            (record["engine_path_hash"], alternative["path_hash"]),
            (record["source_trajectory_hash"], result["source_trajectory_hash"]),
            (record["combine_path_hash"], result["combine_path_hash"]),
            (record["terminal"], alternative["terminal"]),
            (record["first_payout_count"], alternative["first_payout_count"]),
            (record["payout_cycles"], alternative["payout_cycles"]),
            (record["trader_net_payout_usd"], alternative["trader_net_payout"]),
            (record["minimum_mll_buffer_usd"], alternative["minimum_mll_buffer"]),
        )
        if any(str(left) != str(right) for left, right in comparisons):
            raise AutonomousTierGXfaDiagnosticError(
                "XFA path record does not reconcile to engine evidence"
            )

    raw_aggregates = [
        dict(value) for value in row.get("candidate_scenario_path_aggregates", ())
    ]
    aggregates = [dict(value) for value in raw_aggregates]
    for aggregate in aggregates:
        aggregate_hash = aggregate.pop("aggregate_hash", None)
        if (
            aggregate.get("schema") != AGGREGATE_SCHEMA
            or aggregate_hash != stable_hash(aggregate)
            or str(aggregate.get("path")) not in PATHS
        ):
            raise AutonomousTierGXfaDiagnosticError(
                "XFA aggregate identity/hash drift"
            )
    denominator = int(row.get("combine_attempt_denominator_per_candidate_scenario", 0))
    expected_aggregates = _aggregate_records(
        raw_records,
        combine_attempt_denominator=denominator,
    )
    if stable_hash(raw_aggregates) != stable_hash(expected_aggregates):
        raise AutonomousTierGXfaDiagnosticError(
            "XFA aggregate does not reconcile to unique path records"
        )
    expected_path_totals = _path_totals(
        raw_records,
        combine_attempt_denominator=denominator,
    )
    if stable_hash(row.get("path_totals_by_scenario")) != stable_hash(
        expected_path_totals
    ):
        raise AutonomousTierGXfaDiagnosticError(
            "XFA path totals do not reconcile to unique path records"
        )
    counts = dict(row.get("counts") or {})
    if (
        int(counts.get("combine_transition_count", -1)) != len(by_transition)
        or int(counts.get("alternative_path_count", -1)) != len(records)
        or int(counts.get("standard_path_count", -1)) != len(by_transition)
        or int(counts.get("consistency_path_count", -1)) != len(by_transition)
    ):
        raise AutonomousTierGXfaDiagnosticError("XFA diagnostic count drift")
    transition_candidates: dict[str, str] = {}
    for record in raw_records:
        transition_candidates.setdefault(
            str(record["transition_id"]), str(record["candidate_id"])
        )
    expected_counts = _counts(
        [
            {"candidate_id": candidate_id}
            for _transition_id, candidate_id in sorted(
                transition_candidates.items()
            )
        ],
        raw_records,
    )
    if stable_hash(counts) != stable_hash(expected_counts):
        raise AutonomousTierGXfaDiagnosticError(
            "XFA diagnostic counts do not reconcile to path records"
        )
    expected_hashes = _economic_hashes(
        raw_records,
        expected_aggregates,
        expected_counts,
    )
    if stable_hash(row.get("economic_hashes")) != stable_hash(expected_hashes):
        raise AutonomousTierGXfaDiagnosticError(
            "XFA payout/survival/EV hashes do not reconcile"
        )
    return {**row, "result_hash": claimed}


def _verify_transition_candidate_binding(
    transition: Mapping[str, Any], candidate: Mapping[str, Any]
) -> None:
    profile = dict(candidate.get("xfa_profile") or {})
    book = dict(candidate.get("xfa_book") or {})
    bindings = (
        (transition.get("candidate_id"), candidate.get("candidate_id"), "candidate"),
        (
            transition.get("combine_book_hash"),
            candidate.get("combine_book_hash"),
            "Combine book",
        ),
        (transition.get("xfa_book_hash"), book.get("xfa_book_hash"), "XFA book"),
        (
            transition.get("xfa_profile_hash"),
            profile.get("xfa_profile_hash"),
            "XFA profile",
        ),
        (transition.get("account_label"), candidate.get("account_label"), "account"),
    )
    for left, right, label in bindings:
        if str(left) != str(right):
            raise AutonomousTierGXfaDiagnosticError(
                f"transition/candidate {label} binding drift"
            )
    if (
        profile.get("standard_and_consistency_are_alternative_paths") is not True
        or float(profile.get("risk_multiplier", 0.0)) <= 0.0
        or bool(profile.get("outbound_order_capability", True))
    ):
        raise AutonomousTierGXfaDiagnosticError("frozen source XFA profile drift")


def _verify_engine_result(
    result: Mapping[str, Any],
    transition: Mapping[str, Any],
    *,
    frozen_handoff_hash: str,
) -> None:
    mapped = dict(result)
    claimed = mapped.pop("result_hash", None)
    if claimed != stable_hash(mapped):
        raise AutonomousTierGXfaDiagnosticError("XFA engine result hash drift")
    if (
        str(mapped.get("transition_id")) != str(transition["transition_id"])
        or str(mapped.get("combine_path_hash"))
        != str(transition["combine_path_hash"])
        or str(dict(mapped.get("handoff") or {}).get("handoff_hash"))
        != str(frozen_handoff_hash)
    ):
        raise AutonomousTierGXfaDiagnosticError("XFA engine transition binding drift")
    alternatives = dict(mapped.get("alternatives") or {})
    if (
        set(alternatives) != set(PATHS)
        or mapped.get("standard_and_consistency_are_alternatives") is not True
        or mapped.get("sum_standard_and_consistency_ev_allowed") is not False
        or mapped.get("selected_path") is not None
    ):
        raise AutonomousTierGXfaDiagnosticError("XFA engine alternatives were mixed")
    for path in PATHS:
        payload = dict(alternatives[path])
        claimed_path = payload.pop("path_hash", None)
        if (
            str(payload.get("path")) != f"XFA_{path}"
            or claimed_path != stable_hash(payload)
            or int(payload.get("first_payout_count", -1)) not in {0, 1}
        ):
            raise AutonomousTierGXfaDiagnosticError(
                f"XFA {path} path identity/hash drift"
            )


def _path_record(
    *,
    transition: Mapping[str, Any],
    engine_result: Mapping[str, Any],
    path: str,
    frozen_handoff_hash: str,
) -> dict[str, Any]:
    result = dict(engine_result)
    value = dict(dict(result["alternatives"])[path])
    identity = {
        "transition_id": str(transition["transition_id"]),
        "path": path,
        "engine_result_hash": str(result["result_hash"]),
    }
    path_record_id = f"xfa-path-{stable_hash(identity)[:24]}"
    terminal = str(value["terminal"])
    first = int(value["first_payout_count"])
    core = {
        "schema": PATH_RECORD_SCHEMA,
        "path_record_id": path_record_id,
        "transition_id": str(transition["transition_id"]),
        "candidate_id": str(transition["candidate_id"]),
        "scenario": str(transition["scenario"]),
        "temporal_block": str(transition["temporal_block"]),
        "combine_start_id": str(transition["combine_start_id"]),
        "combine_path_hash": str(transition["combine_path_hash"]),
        "combine_book_hash": str(transition["combine_book_hash"]),
        "account_label": str(transition["account_label"]),
        "account_size_usd": int(transition["account_size_usd"]),
        "path": path,
        "engine_path": str(value["path"]),
        "engine_result_hash": str(result["result_hash"]),
        "engine_path_hash": str(value["path_hash"]),
        "engine_handoff_hash": str(frozen_handoff_hash),
        "source_trajectory_hash": str(result["source_trajectory_hash"]),
        "terminal": terminal,
        "terminal_reason": str(value["terminal_reason"]),
        "observed_days": int(value["observed_days"]),
        "traded_days": int(value["traded_days"]),
        "accepted_event_count": int(value["accepted_event_count"]),
        "skipped_event_count": int(value["skipped_event_count"]),
        "first_payout_count": first,
        "first_payout_day": value.get("first_payout_day"),
        "payout_cycles": int(value["payout_cycles"]),
        "gross_payout_usd": float(value["gross_payout"]),
        "trader_net_payout_usd": float(value["trader_net_payout"]),
        "minimum_mll_buffer_usd": float(value["minimum_mll_buffer"]),
        "ending_balance_usd": float(value["ending_balance"]),
        "ending_mll_floor_usd": float(value["ending_mll_floor"]),
        "post_payout_survived": bool(value["post_payout_survived"]),
        "closure_before_payout": bool(
            first == 0 and terminal in _ACCOUNT_DEATH_TERMINALS
        ),
        "data_censored": terminal == "DATA_CENSORED",
        "standard_and_consistency_are_alternatives": True,
        "alternative_value_not_additive": True,
    }
    return {**core, "path_record_hash": stable_hash(core)}


def _aggregate_records(
    records: Sequence[Mapping[str, Any]],
    *,
    combine_attempt_denominator: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        key = (
            str(row["candidate_id"]),
            str(row["scenario"]),
            str(row["path"]),
        )
        grouped[key].append(row)
    return [
        _aggregate_one(
            values,
            candidate_id=key[0],
            scenario=key[1],
            path=key[2],
            combine_attempt_denominator=combine_attempt_denominator,
        )
        for key, values in sorted(grouped.items())
    ]


def _aggregate_one(
    records: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
    scenario: str,
    path: str,
    combine_attempt_denominator: int,
) -> dict[str, Any]:
    count = len(records)
    if count < 1 or combine_attempt_denominator < count:
        raise AutonomousTierGXfaDiagnosticError(
            "XFA aggregate Combine denominator is invalid"
        )
    first = sum(int(row["first_payout_count"]) for row in records)
    closure = sum(int(bool(row["closure_before_payout"])) for row in records)
    censored = sum(int(bool(row["data_censored"])) for row in records)
    cycles = sum(int(row["payout_cycles"]) for row in records)
    net = sum(float(row["trader_net_payout_usd"]) for row in records)
    gross = sum(float(row["gross_payout_usd"]) for row in records)
    paid_nets = [
        float(row["trader_net_payout_usd"])
        for row in records
        if int(row["first_payout_count"]) == 1
    ]
    payout_days = [
        int(row["first_payout_day"])
        for row in records
        if row.get("first_payout_day") is not None
    ]
    post_survival = sum(
        int(bool(row["post_payout_survived"])) for row in records
    )
    terminal = Counter(str(row["terminal"]) for row in records)
    core = {
        "schema": AGGREGATE_SCHEMA,
        "candidate_id": candidate_id,
        "scenario": scenario,
        "path": path,
        "account_label": str(records[0]["account_label"]),
        "account_size_usd": int(records[0]["account_size_usd"]),
        "combine_attempt_count": int(combine_attempt_denominator),
        "combine_pass_count": count,
        "combine_pass_rate": count / combine_attempt_denominator,
        "path_count": count,
        "first_payout_count": first,
        "first_payout_rate": first / count,
        "median_days_to_first_payout": (
            float(statistics.median(payout_days)) if payout_days else None
        ),
        "closure_before_payout_count": closure,
        "closure_before_payout_rate": closure / count,
        "data_censored_count": censored,
        "data_censored_rate": censored / count,
        "payout_cycles_total": cycles,
        "expected_payout_cycles_per_successful_combine": cycles / count,
        "gross_payout_total_usd": gross,
        "trader_net_payout_total_usd": net,
        "conditional_trader_net_payout_usd": (
            sum(paid_nets) / len(paid_nets) if paid_nets else 0.0
        ),
        "expected_trader_payout_per_successful_combine_usd": net / count,
        "expected_trader_payout_per_new_combine_attempt_usd": (
            net / combine_attempt_denominator
        ),
        "post_payout_survival_count": post_survival,
        "post_payout_survival_rate": post_survival / first if first else 0.0,
        "minimum_mll_buffer_usd": min(
            float(row["minimum_mll_buffer_usd"]) for row in records
        ),
        "accepted_event_count": sum(
            int(row["accepted_event_count"]) for row in records
        ),
        "skipped_event_count": sum(
            int(row["skipped_event_count"]) for row in records
        ),
        "terminal_distribution": dict(sorted(terminal.items())),
        "transition_ids": sorted(str(row["transition_id"]) for row in records),
        "path_record_hashes": sorted(
            str(row["path_record_hash"]) for row in records
        ),
        "alternative_value_not_additive": True,
    }
    return {**core, "aggregate_hash": stable_hash(core)}


def _path_totals(
    records: Sequence[Mapping[str, Any]],
    *,
    combine_attempt_denominator: int,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    candidates = len({str(row["candidate_id"]) for row in records})
    total_attempts = combine_attempt_denominator * candidates
    for path in PATHS:
        by_scenario: dict[str, Any] = {}
        for scenario in SCENARIOS:
            selected = [
                row
                for row in records
                if str(row["path"]) == path and str(row["scenario"]) == scenario
            ]
            net = sum(float(row["trader_net_payout_usd"]) for row in selected)
            first = sum(int(row["first_payout_count"]) for row in selected)
            by_scenario[scenario] = {
                "path_count": len(selected),
                "combine_attempt_count": total_attempts,
                "first_payout_count": first,
                "first_payout_rate": first / len(selected) if selected else 0.0,
                "trader_net_payout_total_usd": net,
                "expected_trader_payout_per_successful_combine_usd": (
                    net / len(selected) if selected else 0.0
                ),
                "expected_trader_payout_per_new_combine_attempt_usd": (
                    net / total_attempts if total_attempts else 0.0
                ),
            }
        core = {
            "path": path,
            "by_scenario": by_scenario,
            "not_added_to_other_path": True,
        }
        output[path] = {**core, "path_total_hash": stable_hash(core)}
    return output


def _counts(
    transitions: Sequence[Mapping[str, Any]],
    path_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    standard = [row for row in path_records if row["path"] == "STANDARD"]
    consistency = [row for row in path_records if row["path"] == "CONSISTENCY"]
    return {
        "candidate_count": len({str(row["candidate_id"]) for row in transitions}),
        "combine_transition_count": len(transitions),
        "alternative_path_count": len(path_records),
        "standard_path_count": len(standard),
        "consistency_path_count": len(consistency),
        "standard_first_payout_count": sum(
            int(row["first_payout_count"]) for row in standard
        ),
        "consistency_first_payout_count": sum(
            int(row["first_payout_count"]) for row in consistency
        ),
        "standard_payout_cycle_count": sum(
            int(row["payout_cycles"]) for row in standard
        ),
        "consistency_payout_cycle_count": sum(
            int(row["payout_cycles"]) for row in consistency
        ),
        "standard_post_payout_survival_count": sum(
            int(bool(row["post_payout_survived"])) for row in standard
        ),
        "consistency_post_payout_survival_count": sum(
            int(bool(row["post_payout_survived"])) for row in consistency
        ),
        "xfa_engine_result_count": len(transitions),
        "database_writes": 0,
        "registry_writes": 0,
        "broker_connections": 0,
        "orders": 0,
    }


def _uniform_attempt_denominator(source: Mapping[str, Any]) -> int:
    counts = dict(source.get("counts") or {})
    episodes = int(counts.get("exact_combine_episode_reconstruction_count", 0))
    tapes = int(counts.get("source_tape_count", 0))
    if tapes <= 0 or episodes <= 0 or episodes % tapes:
        raise AutonomousTierGXfaDiagnosticError(
            "Combine attempt denominator is not a uniform candidate/scenario grid"
        )
    denominator = episodes // tapes
    if denominator < 1:
        raise AutonomousTierGXfaDiagnosticError("Combine attempt denominator is empty")
    return denominator


def _economic_hashes(
    records: Sequence[Mapping[str, Any]],
    aggregates: Sequence[Mapping[str, Any]],
    counts: Mapping[str, Any],
) -> dict[str, str]:
    ordered = sorted(records, key=lambda row: str(row["path_record_id"]))
    payout = [
        {
            "path_record_id": str(row["path_record_id"]),
            "first_payout_count": int(row["first_payout_count"]),
            "first_payout_day": row.get("first_payout_day"),
            "payout_cycles": int(row["payout_cycles"]),
            "gross_payout_usd": float(row["gross_payout_usd"]),
            "trader_net_payout_usd": float(row["trader_net_payout_usd"]),
        }
        for row in ordered
    ]
    survival = [
        {
            "path_record_id": str(row["path_record_id"]),
            "terminal": str(row["terminal"]),
            "minimum_mll_buffer_usd": float(row["minimum_mll_buffer_usd"]),
            "closure_before_payout": bool(row["closure_before_payout"]),
            "post_payout_survived": bool(row["post_payout_survived"]),
        }
        for row in ordered
    ]
    ev = [
        {
            "candidate_id": str(row["candidate_id"]),
            "scenario": str(row["scenario"]),
            "path": str(row["path"]),
            "expected_per_successful_combine": float(
                row["expected_trader_payout_per_successful_combine_usd"]
            ),
            "expected_per_new_combine_attempt": float(
                row["expected_trader_payout_per_new_combine_attempt_usd"]
            ),
        }
        for row in sorted(
            aggregates,
            key=lambda value: (
                str(value["candidate_id"]),
                str(value["scenario"]),
                str(value["path"]),
            ),
        )
    ]
    return {
        "path_inventory_hash": stable_hash(
            [str(row["path_record_hash"]) for row in ordered]
        ),
        "payout_observation_hash": stable_hash(payout),
        "survival_observation_hash": stable_hash(survival),
        "expected_value_hash": stable_hash(ev),
        "counts_hash": stable_hash(dict(counts)),
    }


__all__ = [
    "AGGREGATE_SCHEMA",
    "AutonomousTierGXfaDiagnosticError",
    "DEFAULT_XFA_HORIZON_DAYS",
    "PATH_RECORD_SCHEMA",
    "PATHS",
    "SCHEMA",
    "STATUS",
    "build_autonomous_tier_g_xfa_diagnostic",
    "verify_autonomous_tier_g_xfa_diagnostic",
]
