"""Bounded causal repair of the only near-pass exact legal-frontier book.

This is a read-only economic worker, not a promotion path.  The source 0029
book ``fast_book_477b...`` produced two normal and two stressed diagnostic
passes out of four full 20-day starts at 50K, but its MNQ sleeve also opened
positions whose declared 30-minute lifetime crossed the mandatory 15:10 CT
flatten.  Those outcomes are therefore inadmissible.

The worker evaluates exactly two preregistered repairs:

``HORIZON_SAFE_ENTRY_CUTOFF``
    Reject an entry *before it is placed* whenever its frozen maximum holding
    horizon could extend past the session flatten.  The decision uses only the
    entry timestamp and the immutable sleeve horizon.

``DROP_OFFENDING_COMPONENT``
    Remove the complete sleeve that generated the violations.  This is a
    useful lower-complexity counterfactual and cannot cherry-pick its trades.

Both repairs reuse the immutable causal event ledgers, quality decisions,
headline starts and official account rules.  They assign no inherited status,
perform no writes, touch no Q4 data and expose no order capability.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.production import autonomous_exact_replay as exact
from hydra.production import frozen_legal_frontier_replay as source
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.fast_pass_runtime_helpers import _quality_trajectories
from hydra.research.causal_target_velocity import HazardOutcome


SCHEMA = "hydra_session_safe_fast_book_tripwire_v1"
BRANCH_ID = "SESSION_SAFE_FAST_BOOK_477B_TRIPWIRE_V1"
SOURCE_POLICY_ID = "fast_book_477b40613795d1d45557a83a:w1"
ACCOUNT_LABELS = ("50K", "100K", "150K")
REPAIR_VARIANTS = (
    "HORIZON_SAFE_ENTRY_CUTOFF",
    "DROP_OFFENDING_COMPONENT",
)
MAXIMUM_CPU_SHARDS = 2
SCALE_FACTOR = 3
EVIDENCE_ROLE = "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY"

_TRADING_TIMEZONE = ZoneInfo("America/Chicago")
_SESSION_START_LOCAL = time(17, 0)
_SESSION_FLATTEN_LOCAL = time(15, 10)


class SessionSafeFastBookError(RuntimeError):
    """A frozen source or causal/account invariant drifted."""


def session_safe_fast_book_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe read-only worker adapter (one variant per CPU shard)."""

    return run_session_safe_fast_book_tripwire(
        str(payload["root"]),
        repair_variant=str(payload["repair_variant"]),
        fast_pass_manifest_path=str(
            payload.get("fast_pass_manifest_path", exact.DEFAULT_FAST_PASS_MANIFEST)
        ),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", exact.DEFAULT_RULE_SNAPSHOT)
        ),
    )


def run_session_safe_fast_book_tripwire(
    root: str | Path,
    *,
    repair_variant: str,
    fast_pass_manifest_path: str | Path = exact.DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = exact.DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Run one of the two frozen session repairs with exact account replay."""

    if repair_variant not in REPAIR_VARIANTS:
        raise SessionSafeFastBookError("repair variant is outside the frozen lattice")

    project = Path(root).resolve()
    manifest_path = _inside(project, fast_pass_manifest_path)
    rules_path = _inside(project, rule_snapshot_path)
    bank_path = _inside(project, source.DEFAULT_BANK_PATH)
    proposals_path = _inside(project, source.DEFAULT_PROPOSALS_PATH)

    manifest = exact._load_self_hashed_manifest(manifest_path)
    if str(manifest.get("campaign_id")) != source.SOURCE_CAMPAIGN_ID:
        raise SessionSafeFastBookError("FAST-PASS source campaign drift")
    rules, rule_receipt = exact._load_rule_snapshot(rules_path)
    if tuple(sorted(rules, key=lambda value: int(value[:-1]))) != ACCOUNT_LABELS:
        raise SessionSafeFastBookError("official account-size inventory drift")
    calendar, starts, grid_receipt = exact._load_frozen_grid(project, manifest)
    bank, bank_receipt = source._load_wave_one_bank(bank_path)
    bank_by_id = {str(row["candidate_id"]): row for row in bank}
    proposals, proposal_receipt = source._load_proposals(proposals_path)
    frozen = _source_cell()
    source_book, source_receipt = source._load_source_book_result(project, frozen)
    proposal = _one(
        [row for row in proposals if str(row.get("book_id")) == frozen.book_id],
        "frozen 477b proposal",
    )
    source._verify_proposal(frozen, proposal, source_book)
    component_ids = tuple(str(value) for value in source_book["component_ids"])
    quality = {
        str(key): float(value)
        for key, value in dict(source_book["quality_multipliers"]).items()
    }
    if set(component_ids) != set(quality):
        raise SessionSafeFastBookError("source component/quality inventory drift")

    source_quality_inputs: dict[str, dict[str, tuple[Any, ...]]] = {
        scenario: {} for scenario in source.SCENARIOS
    }
    repaired_unscaled: dict[str, dict[str, tuple[Any, ...]]] = {
        scenario: {} for scenario in source.SCENARIOS
    }
    repaired_scaled: dict[str, dict[str, tuple[Any, ...]]] = {
        scenario: {} for scenario in source.SCENARIOS
    }
    eligible_days: dict[str, frozenset[int]] = {}
    censored_days: dict[str, frozenset[int]] = {}
    risk_charges_by_component: dict[str, float] = {}
    candidate_fingerprints: dict[str, str] = {}
    original_violations: dict[str, int] = {}
    repaired_violations: dict[str, int] = {}
    causal_rejections: dict[str, int] = {}
    component_receipts: dict[str, Any] = {}
    total_source_events = 0

    for component_id in component_ids:
        entry = bank_by_id.get(component_id)
        if entry is None:
            raise SessionSafeFastBookError(
                f"source component absent from immutable bank: {component_id}"
            )
        events, event_receipt = exact._read_verified_event_evidence(
            project, dict(entry["event_evidence"])
        )
        replay = reconstruct_exact_hazard_replay(
            candidate_payload=entry["candidate"],
            event_mappings=events,
            eligible_session_days=entry["eligible_session_days"],
            expected_hashes=entry["exact_hashes"],
        )
        normal, normal_quality = _quality_trajectories(
            replay.normal_trajectories, quality[component_id]
        )
        stressed, stressed_quality = _quality_trajectories(
            replay.stressed_trajectories, quality[component_id]
        )
        if normal_quality != stressed_quality:
            raise SessionSafeFastBookError("normal/stressed quality decision drift")
        if normal_quality != dict(source_book["quality_scaling"][component_id]):
            raise SessionSafeFastBookError("source quality receipt drift")
        source_quality_inputs["NORMAL"][component_id] = normal
        source_quality_inputs["STRESSED_1_5X"][component_id] = stressed
        _require_scenario_event_identity(normal, stressed)

        normal_checked, normal_violation_count = exact._apply_session_contract(normal)
        stressed_checked, stressed_violation_count = exact._apply_session_contract(
            stressed
        )
        if normal_violation_count != stressed_violation_count:
            raise SessionSafeFastBookError("session violations changed with cost scenario")
        original_violations[component_id] = int(normal_violation_count)

        included = not (
            repair_variant == "DROP_OFFENDING_COMPONENT"
            and normal_violation_count > 0
        )
        if included and repair_variant == "HORIZON_SAFE_ENTRY_CUTOFF":
            maximum_holding_minutes = int(entry["candidate"]["horizon"])
            repaired_normal = tuple(
                row
                for row in normal_checked
                if causal_horizon_safe_entry(
                    int(row.event.decision_ns), maximum_holding_minutes
                )
            )
            repaired_stressed = tuple(
                row
                for row in stressed_checked
                if causal_horizon_safe_entry(
                    int(row.event.decision_ns), maximum_holding_minutes
                )
            )
            rejected = len(normal_checked) - len(repaired_normal)
            if rejected != len(stressed_checked) - len(repaired_stressed):
                raise SessionSafeFastBookError(
                    "causal cutoff changed with cost scenario"
                )
        elif included:
            repaired_normal = normal_checked
            repaired_stressed = stressed_checked
            rejected = 0
        else:
            repaired_normal = ()
            repaired_stressed = ()
            rejected = len(normal_checked)
        _require_scenario_event_identity(repaired_normal, repaired_stressed)
        causal_rejections[component_id] = int(rejected)

        if included:
            repaired_normal, post_normal = exact._apply_session_contract(
                repaired_normal
            )
            repaired_stressed, post_stressed = exact._apply_session_contract(
                repaired_stressed
            )
            if post_normal != 0 or post_stressed != 0:
                raise SessionSafeFastBookError(
                    "preregistered repair left a session-contract violation"
                )
            repaired_violations[component_id] = 0
            repaired_unscaled["NORMAL"][component_id] = repaired_normal
            repaired_unscaled["STRESSED_1_5X"][component_id] = repaired_stressed
            scaled_normal = tuple(
                scale_causal_trajectory(
                    row, executable_quantity_multiplier=SCALE_FACTOR
                )
                for row in repaired_normal
            )
            scaled_stressed = tuple(
                scale_causal_trajectory(
                    row, executable_quantity_multiplier=SCALE_FACTOR
                )
                for row in repaired_stressed
            )
            _require_scenario_event_identity(scaled_normal, scaled_stressed)
            repaired_scaled["NORMAL"][component_id] = scaled_normal
            repaired_scaled["STRESSED_1_5X"][component_id] = scaled_stressed
            risk_charges_by_component[component_id] = float(
                exact._declared_stop_risk_charge_per_mini(
                    events, entry["candidate"]
                )
            )
        else:
            repaired_violations[component_id] = 0

        eligible_days[component_id] = frozenset(
            int(value) for value in replay.eligible_session_days
        )
        censored_days[component_id] = frozenset(
            int(row.session_day)
            for row in replay.events
            if str(getattr(row.outcome, "value", row.outcome))
            == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
        )
        candidate_fingerprints[component_id] = str(entry["candidate_fingerprint"])
        component_receipts[component_id] = {
            "included_after_repair": included,
            "candidate_fingerprint": str(entry["candidate_fingerprint"]),
            "candidate": dict(entry["candidate"]),
            "event_evidence": event_receipt,
            "quality_scaling": normal_quality,
            "original_session_violation_count": int(normal_violation_count),
            "causal_pre_entry_rejection_count": int(rejected),
            "repaired_session_violation_count": 0,
        }
        total_source_events += len(events)

    source._verify_source_trajectory_hashes(
        source_book, source_quality_inputs, frozen
    )
    active_component_ids = tuple(
        component_id
        for component_id in component_ids
        if component_id in repaired_scaled["NORMAL"]
    )
    if not active_component_ids:
        raise SessionSafeFastBookError("repair removed every component")
    if set(repaired_scaled["NORMAL"]) != set(repaired_scaled["STRESSED_1_5X"]):
        raise SessionSafeFastBookError("repaired scenario inventory drift")

    # Keep the exact preregistered start denominator.  Removing or filtering a
    # trade cannot turn a previously censored market-data window into evidence.
    coverage = source._book_coverage(
        calendar, starts, eligible_days, censored_days
    )
    source._verify_source_coverage(source_book, coverage, frozen)
    profile = source._profile_for_source(manifest, source_book)

    account_results: dict[str, Any] = {}
    evaluated_records: list[dict[str, Any]] = []
    total_exact_replays = 0
    for account_label in ACCOUNT_LABELS:
        rule = dict(rules[account_label])
        cap_breaches: dict[str, int] = {}
        for component_id in active_component_ids:
            entry = bank_by_id[component_id]
            limit = exact._market_contract_limit_mini(entry["candidate"], rule)
            cap_breaches[component_id] = sum(
                float(row.event.mini_equivalent) > limit + 1e-12
                for row in repaired_scaled["NORMAL"][component_id]
            )
        if sum(cap_breaches.values()):
            raise SessionSafeFastBookError(
                f"repair exceeds official {account_label} market cap"
            )

        risk_charges = tuple(
            (component_id, risk_charges_by_component[component_id])
            for component_id in active_component_ids
        )
        base_policy = source._uniform_legal_policy(
            frozen=frozen,
            component_ids=active_component_ids,
            risk_charges=risk_charges,
            profile=profile,
            rule=rule,
        )
        policy_id = (
            f"session_safe_477b:{repair_variant}:{account_label}:{SCALE_FACTOR}x"
        )
        policy = replace(base_policy, policy_id=policy_id)
        horizon_results: dict[str, Any] = {}
        for horizon in source.HORIZONS:
            episodes: dict[str, list[tuple[Any, str]]] = {
                scenario: [] for scenario in source.SCENARIOS
            }
            for start_day, block in coverage["starts"][horizon]:
                for scenario in source.SCENARIOS:
                    episode = source.run_causal_shared_account_episode(
                        repaired_scaled[scenario],
                        calendar,
                        policy=policy,
                        start_day=int(start_day),
                        maximum_duration_days=int(horizon),
                        config=exact._account_config(rule),
                    )
                    episodes[scenario].append((episode, str(block)))
                    record = {
                        "policy_id": policy_id,
                        "source_policy_id": SOURCE_POLICY_ID,
                        "repair_variant": repair_variant,
                        "account_label": account_label,
                        "episode_id": (
                            f"{policy_id}:{horizon}:{int(start_day)}:{scenario}"
                        ),
                        "scenario": scenario,
                        "cost_scenario": scenario,
                        "horizon": f"{horizon}_TRADING_DAYS",
                        "horizon_trading_days": int(horizon),
                        "temporal_block": str(block),
                        "start_day": int(start_day),
                        "coverage_state": "FULL_COVERAGE",
                        "episode": episode.to_dict(include_paths=True),
                    }
                    record["record_hash"] = stable_hash(record)
                    evaluated_records.append(record)
                    total_exact_replays += 1
            horizon_results[str(horizon)] = {
                "horizon_trading_days": int(horizon),
                "requested_start_count": len(starts[horizon]),
                "full_coverage_start_count": len(coverage["starts"][horizon]),
                "data_censored_start_count": int(coverage["censored"][horizon]),
                "normal": exact._summarize_exact_episodes(episodes["NORMAL"]),
                "stressed": exact._summarize_exact_episodes(
                    episodes["STRESSED_1_5X"]
                ),
            }
        account_results[account_label] = {
            "policy_id": policy_id,
            "account_size_usd": int(rule["account_size_usd"]),
            "policy": policy.to_dict(),
            "official_market_contract_cap_breach_count_by_component": (
                cap_breaches
            ),
            "hard_execution_contract_clean": True,
            "horizon_results": horizon_results,
            "promotion_status": None,
            "evidence_tier": "E_EXACT_DEVELOPMENT_TRIPWIRE",
        }

    selected = account_results["50K"]["horizon_results"]["20"]
    signal_gate = _repair_signal_gate(selected)
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": "COMPLETE_SESSION_SAFE_FAST_BOOK_TRIPWIRE",
        "repair_variant": repair_variant,
        "evidence_role": EVIDENCE_ROLE,
        "source_policy_id": SOURCE_POLICY_ID,
        "status_inherited": False,
        "promotion_status": None,
        "evidence_tier": "E",
        "repair_contract": {
            "variant": repair_variant,
            "scale_factor": SCALE_FACTOR,
            "decision_inputs": [
                "decision_ns",
                "frozen_maximum_holding_minutes",
                "official_session_timezone",
                "official_mandatory_flatten_time",
            ],
            "outcome_fields_used": False,
            "future_label_eligibility_used": False,
            "session_timezone": "America/Chicago",
            "mandatory_flatten_local": "15:10",
            "inactive_sleeves_reserve_risk": False,
            "silent_quantity_reduction_allowed": False,
        },
        "source_manifest": {
            "path": str(manifest_path),
            "file_sha256": exact._sha256(manifest_path),
            "manifest_hash": str(manifest["manifest_hash"]),
        },
        "official_rule_snapshot": rule_receipt,
        "frozen_grid": grid_receipt,
        "source_bank": bank_receipt,
        "source_proposals": proposal_receipt,
        "source_book": source_receipt,
        "source_component_ids": list(component_ids),
        "active_component_ids": list(active_component_ids),
        "candidate_fingerprints": candidate_fingerprints,
        "component_provenance": component_receipts,
        "original_session_violation_count_by_component": original_violations,
        "causal_pre_entry_rejection_count_by_component": causal_rejections,
        "repaired_session_violation_count_by_component": repaired_violations,
        "repaired_trajectory_hashes": {
            scenario: stable_hash(
                {
                    component_id: [row.to_dict() for row in trajectories]
                    for component_id, trajectories in values.items()
                }
            )
            for scenario, values in repaired_scaled.items()
        },
        "account_results": account_results,
        "repair_signal_gate": signal_gate,
        "evidence_bundle_adapter": {
            "schema": "hydra_evidence_bundle_adapter_input_v1",
            "campaign_id": BRANCH_ID,
            "evidence_role": EVIDENCE_ROLE,
            "evaluated_policy_records": evaluated_records,
            "records_hash": stable_hash(evaluated_records),
            "sealing_performed": False,
            "authoritative_writer_required_for_sealing": True,
        },
        "counters": {
            "cpu_shards_for_this_result": 1,
            "maximum_parallel_cpu_shards": MAXIMUM_CPU_SHARDS,
            "source_event_rows_reconstructed": total_source_events,
            "source_component_count": len(component_ids),
            "active_component_count": len(active_component_ids),
            "causal_pre_entry_rejection_count": sum(causal_rejections.values()),
            "repaired_session_violation_count": sum(repaired_violations.values()),
            "exact_account_replays": total_exact_replays,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "authoritative_writes": 0,
            "promotion_count": 0,
        },
        "decision": (
            "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_VALIDATION"
            if signal_gate["passed"]
            else "SESSION_SAFE_REPAIR_FALSIFIED_AT_TRIPWIRE"
        ),
        "next_action": (
            "FREEZE_REPAIR_AND_RUN_UNCHANGED_CHRONOLOGICAL_VALIDATION"
            if signal_gate["passed"]
            else "RUN_DISTINCT_CLEAN_CROSS_ASSET_DAILY_RECONSTRUCTION"
        ),
        "read_only_worker": True,
        "outbound_order_capability": False,
    }
    return {**core, "result_hash": stable_hash(core)}


def causal_horizon_safe_entry(
    decision_ns: int, maximum_holding_minutes: int
) -> bool:
    """Return whether a maximum-duration exit is no later than 15:10 CT.

    This predicate deliberately does not inspect the realised exit timestamp.
    It is therefore safe to use before an order intention is admitted.
    """

    if decision_ns <= 0 or maximum_holding_minutes <= 0:
        raise SessionSafeFastBookError("invalid causal session-cutoff input")
    decision = datetime.fromtimestamp(
        decision_ns / 1_000_000_000, tz=UTC
    ).astimezone(_TRADING_TIMEZONE)
    clock = decision.timetz().replace(tzinfo=None)
    if clock >= _SESSION_START_LOCAL:
        trading_date = decision.date() + timedelta(days=1)
    elif clock < _SESSION_FLATTEN_LOCAL:
        trading_date = decision.date()
    else:
        return False
    flatten = datetime.combine(
        trading_date, _SESSION_FLATTEN_LOCAL, tzinfo=_TRADING_TIMEZONE
    )
    return decision + timedelta(minutes=maximum_holding_minutes) <= flatten


def _repair_signal_gate(selected: Mapping[str, Any]) -> dict[str, Any]:
    normal = dict(selected["normal"])
    stressed = dict(selected["stressed"])
    pass_blocks = {
        block
        for block, summary in dict(normal["by_block"]).items()
        if int(summary["pass_count"]) > 0
    } | {
        block
        for block, summary in dict(stressed["by_block"]).items()
        if int(summary["pass_count"]) > 0
    }
    checks = {
        "four_frozen_full_coverage_20d_starts": (
            int(selected["full_coverage_start_count"]) == 4
        ),
        "at_least_two_normal_passes": int(normal["pass_count"]) >= 2,
        "at_least_two_stressed_passes": int(stressed["pass_count"]) >= 2,
        "positive_stressed_net": float(stressed["net_total_usd"]) > 0.0,
        "stressed_mll_breach_at_most_10pct": (
            float(stressed["mll_breach_rate"]) <= 0.10
        ),
        "passes_in_at_least_two_blocks": len(pass_blocks) >= 2,
        "stressed_consistency_compliance_at_least_75pct": (
            float(stressed["consistency_compliance_rate"]) >= 0.75
        ),
    }
    return {
        "gate_role": "DEVELOPMENT_TRIPWIRE_ONLY_NO_PROMOTION",
        "checks": checks,
        "passing_blocks": sorted(pass_blocks),
        "passed": all(checks.values()),
        "gate_hash": stable_hash(checks),
    }


def _source_cell() -> source.FrozenLegalCell:
    return _one(
        [row for row in source.FROZEN_CELLS if row.policy_id == SOURCE_POLICY_ID],
        "frozen 477b legal cell",
    )


def _require_scenario_event_identity(
    normal: Sequence[Any], stressed: Sequence[Any]
) -> None:
    if len(normal) != len(stressed):
        raise SessionSafeFastBookError("normal/stressed event count drift")
    for left, right in zip(normal, stressed):
        left_id = str(left.event.event_id).rsplit(":", 1)[0]
        right_id = str(right.event.event_id).rsplit(":", 1)[0]
        if (
            left_id != right_id
            or int(left.event.decision_ns) != int(right.event.decision_ns)
            or int(left.event.exit_ns) != int(right.event.exit_ns)
            or int(left.event.quantity) != int(right.event.quantity)
        ):
            raise SessionSafeFastBookError("normal/stressed causal identity drift")


def _inside(project: Path, value: str | Path) -> Path:
    root = project.resolve()
    raw = Path(value)
    path = (raw if raw.is_absolute() else root / raw).resolve()
    if path != root and root not in path.parents:
        raise SessionSafeFastBookError("source path escapes project root")
    if not path.is_file():
        raise SessionSafeFastBookError(f"required source file is absent: {path}")
    return path


def _one(values: Sequence[Any], label: str) -> Any:
    if len(values) != 1:
        raise SessionSafeFastBookError(f"{label} is absent or duplicated")
    return values[0]
