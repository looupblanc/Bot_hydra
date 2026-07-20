"""Hash-bound graduation replay over the surviving 0035 development bank.

The adapter is deliberately narrow.  It reconstructs immutable 0029 causal
ledgers, evaluates the already-frozen 5/10/20-day non-overlapping start grids,
and starts read-only XFA alternatives only for unique exact Combine passes.
It never opens the sealed May--July 2026 confirmation partition and performs
no registry, mission-database, broker, order, Q4, or data-purchase write.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production import autonomous_tier_g_controls as controls
from hydra.production.autonomous_exact_replay import (
    _account_config,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _summarize_exact_episodes,
)
from hydra.propfirm.account_size_xfa import (
    freeze_account_size_xfa_handoff,
    load_account_size_xfa_rules,
    run_account_size_xfa_alternatives,
)


SCHEMA = "hydra_autonomous_graduation_cohort_result_v1"
AUDIT_SCHEMA = "hydra_autonomous_graduation_cohort_audit_v1"
PREFLIGHT_SCHEMA = "hydra_autonomous_graduation_cohort_preflight_v1"
DEFAULT_MANIFEST = Path("config/research/autonomous_graduation_cohort_v1.json")
INITIAL_EXACT = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
    "epoch_0002_exact_0029_account_race.json"
)
CONTINUATION_FILES = tuple(
    Path(
        "reports/economic_evolution/"
        "autonomous_economic_discovery_director_0035_revision_02/branch_results/"
        f"post_source_exhaustion/exact_0029_offset_{offset:04d}.json"
    )
    for offset in (32, 64, 96, 128, 160)
)
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
BLOCKS = ("B1", "B2", "B3", "B4")


class AutonomousGraduationCohortError(RuntimeError):
    """The cohort cannot be replayed without weakening its frozen contract."""


def audit_autonomous_graduation_cohort(
    root: str | Path,
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
) -> dict[str, Any]:
    """Verify source hashes, cohort identities, and consumed-evidence exclusions."""

    project = Path(root).resolve()
    manifest = _load_cohort_manifest(project / manifest_path)
    source = dict(manifest["source_contract"])
    _require_file_hashes(project, source)

    bank_wrapper = _read_object(project / source["candidate_bank_path"])
    bank = books._verify_candidate_bank(dict(bank_wrapper["candidate_bank"]))
    if str(bank["result_hash"]) != str(source["candidate_bank_hash"]):
        raise AutonomousGraduationCohortError("candidate-bank hash drift")

    marginal_wrapper = _read_object(project / source["marginal_book_path"])
    marginal = dict(marginal_wrapper["semantic_marginal_book_composite"])
    _verify_self_hashed(marginal, label="marginal book composite")
    if str(marginal["result_hash"]) != str(source["marginal_book_hash"]):
        raise AutonomousGraduationCohortError("marginal-book hash drift")

    fresh_wrapper = _read_object(project / source["fresh_confirmation_path"])
    fresh = dict(fresh_wrapper["fresh_confirmation_result"])
    _verify_self_hashed(fresh, label="fresh confirmation")
    if str(fresh["result_hash"]) != str(source["fresh_confirmation_result_hash"]):
        raise AutonomousGraduationCohortError("fresh-confirmation hash drift")

    cohort = tuple(dict(row) for row in manifest["cohort"])
    ids = tuple(str(row["candidate_id"]) for row in cohort)
    behaviors = tuple(str(row["behavioral_fingerprint"]) for row in cohort)
    qd_cells = tuple(str(row["qd_cell"]) for row in cohort)
    if len(cohort) != 12 or len(set(ids)) != 12:
        raise AutonomousGraduationCohortError("cohort must contain 12 unique policies")
    if len(set(behaviors)) != len(behaviors) or len(set(qd_cells)) != len(qd_cells):
        raise AutonomousGraduationCohortError("cohort is not behavior/QD unique")

    bank_by_id = {str(row["candidate_id"]): dict(row) for row in bank["candidates"]}
    books_by_id = {
        str(row["policy_id"]): dict(row) for row in marginal["book_results"]
    }
    bindings: list[dict[str, Any]] = []
    for frozen in cohort:
        candidate_id = str(frozen["candidate_id"])
        if frozen["kind"] == "STANDALONE":
            actual = bank_by_id.get(candidate_id)
            if actual is None:
                raise AutonomousGraduationCohortError(
                    f"standalone absent from bank: {candidate_id}"
                )
            expected = {
                "candidate_fingerprint": frozen["candidate_fingerprint"],
                "behavioral_fingerprint": frozen["behavioral_fingerprint"],
                "qd_cell": frozen["qd_cell"],
                "selected_cell_hash": frozen["selected_cell_hash"],
                "account_label": frozen["account_label"],
                "compact_bundle_hash": frozen["compact_bundle_hash"],
            }
            observed = {
                "candidate_fingerprint": actual["candidate_fingerprint"],
                "behavioral_fingerprint": actual["realized_behavioral_fingerprint"],
                "qd_cell": actual["qd_cell"],
                "selected_cell_hash": dict(actual["best_safe_cell"])["cell_hash"],
                "account_label": dict(actual["best_safe_cell"])["account_label"],
                "compact_bundle_hash": dict(actual["compact_evidence_bundle"])[
                    "bundle_hash"
                ],
            }
            if expected != observed or actual.get("tier_q_contract_cleared") is not True:
                raise AutonomousGraduationCohortError(
                    f"standalone frozen binding drift: {candidate_id}"
                )
        elif frozen["kind"] == "MARGINAL_BOOK":
            actual = books_by_id.get(candidate_id)
            if actual is None:
                raise AutonomousGraduationCohortError(
                    f"marginal book absent: {candidate_id}"
                )
            if (
                str(actual["policy_spec_hash"])
                != str(frozen["candidate_fingerprint"])
                or list(actual["component_ids"]) != list(frozen["component_ids"])
                or str(actual["governor_profile_id"])
                != str(frozen["governor_profile_id"])
                or str(actual["result_hash"]) != str(frozen["source_result_hash"])
                or actual.get("marginally_accepted") is not True
            ):
                raise AutonomousGraduationCohortError(
                    f"marginal-book frozen binding drift: {candidate_id}"
                )
        else:
            raise AutonomousGraduationCohortError("unsupported cohort policy kind")
        bindings.append(
            {
                "candidate_id": candidate_id,
                "candidate_fingerprint": str(frozen["candidate_fingerprint"]),
                "behavioral_fingerprint": str(frozen["behavioral_fingerprint"]),
                "qd_cell": str(frozen["qd_cell"]),
                "kind": str(frozen["kind"]),
            }
        )

    excluded = {
        str(value)
        for values in dict(manifest["excluded_candidate_ids"]).values()
        for value in values
    }
    component_ids = {
        str(value)
        for row in cohort
        for value in row.get("component_ids", ())
    }
    if excluded.intersection(ids) or excluded.intersection(component_ids):
        raise AutonomousGraduationCohortError(
            "failed/consumed policy leaked into cohort membership"
        )
    failed_confirmation = {
        str(row["candidate_id"])
        for row in fresh.get("candidate_results", ())
        if row.get("evidence_tier") == "G_CONFIRMATION_FAILED"
        and row.get("tier_c_promoted") is False
        and dict(row.get("tier_c_gate") or {}).get("passed") is False
    }
    expected_failed_confirmation = set(
        manifest["excluded_candidate_ids"]["failed_fresh_confirmation"]
    )
    if failed_confirmation != expected_failed_confirmation:
        raise AutonomousGraduationCohortError(
            "failed fresh-confirmation inventory drift"
        )

    development = _read_object(project / source["tier_q_2026_final_development_path"])
    if (
        development.get("confirmation_evaluated") is not False
        or list(development.get("tier_g_candidate_ids") or ())
        or development.get("status") != "FINAL_DEVELOPMENT_CONSUMED"
    ):
        raise AutonomousGraduationCohortError(
            "2026 final-development/confirmation separation drift"
        )
    consumed = {
        str(row["candidate_id"])
        for row in development.get("candidate_results", ())
    }
    expected_consumed = set(
        manifest["excluded_candidate_ids"]["failed_2026_final_development"]
    )
    if consumed != expected_consumed:
        raise AutonomousGraduationCohortError(
            "2026 consumed final-development inventory drift"
        )

    audit_core = {
        "schema": AUDIT_SCHEMA,
        "status": "PASS_HASH_BOUND_COHORT_AUDIT",
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_candidate_bank_hash": str(bank["result_hash"]),
        "source_marginal_book_hash": str(marginal["result_hash"]),
        "cohort_bindings": bindings,
        "cohort_inventory_hash": stable_hash(bindings),
        "cohort_size": len(bindings),
        "excluded_candidate_count": len(excluded),
        "failed_fresh_confirmation_candidate_ids": sorted(failed_confirmation),
        "consumed_2026_candidate_ids": sorted(consumed),
        "confirmation_partition": str(source["confirmation_partition"]),
        "confirmation_evaluated": False,
        "confirmation_partition_reads": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**audit_core, "audit_hash": stable_hash(audit_core)}


def build_autonomous_graduation_preflight(
    root: str | Path,
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
) -> dict[str, Any]:
    """Reconstruct immutable policies and freeze full-coverage replay starts."""

    project = Path(root).resolve()
    audit = audit_autonomous_graduation_cohort(project, manifest_path=manifest_path)
    manifest = _load_cohort_manifest(project / manifest_path)
    artifacts = _load_replay_artifacts(project, manifest)
    prepared = _prepare_all(project, manifest, artifacts)

    frozen: list[dict[str, Any]] = []
    for value in prepared:
        coverage = _coverage_for_prepared(value, artifacts["starts"])
        frozen.append(
            {
                "candidate_id": value["candidate_id"],
                "kind": value["kind"],
                "candidate_fingerprint": value["candidate_fingerprint"],
                "behavioral_fingerprint": value["behavioral_fingerprint"],
                "qd_cell": value["qd_cell"],
                "account_label": value["account_label"],
                "component_ids": list(value["component_ids"]),
                "frozen_policy_hash": value["frozen_policy_hash"],
                "source_evidence_hash": value["source_evidence_hash"],
                "coverage": coverage,
                "coverage_hash": stable_hash(coverage),
            }
        )
    frozen.sort(key=lambda row: str(row["candidate_id"]))
    requested = dict(manifest["evaluation_contract"])[
        "requested_non_overlapping_start_counts"
    ]
    if any(
        int(row["coverage"][str(horizon)]["requested_start_count"])
        != int(requested[str(horizon)])
        for row in frozen
        for horizon in HORIZONS
    ):
        raise AutonomousGraduationCohortError("requested start-grid count drift")

    core = {
        "schema": PREFLIGHT_SCHEMA,
        "status": "PASS_FROZEN_FULL_COVERAGE_PREFLIGHT",
        "manifest_hash": str(manifest["manifest_hash"]),
        "audit_hash": str(audit["audit_hash"]),
        "source_exact_composite_hash": artifacts["exact_composite_hash"],
        "frozen_grid_hash": artifacts["grid_receipt"]["grid_hash"],
        "official_rule_snapshot_hash": artifacts["rule_receipt"][
            "parsed_rule_hash"
        ],
        "candidate_count": len(frozen),
        "frozen_candidates": frozen,
        "frozen_candidate_inventory_hash": stable_hash(frozen),
        "independence_contract": {
            "within_horizon_grid_non_overlapping": True,
            "cross_horizon_observations_independent": False,
            "stage_48_independent_claimed": False,
            "stage_96_independent_claimed": False,
            "full_coverage_only": True,
        },
        "confirmation_partition_reads": 0,
        "registry_writes": 0,
        "database_writes": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "preflight_hash": stable_hash(core)}


def execute_autonomous_graduation_cohort(
    root: str | Path,
    preflight: Mapping[str, Any],
    *,
    manifest_path: str | Path = DEFAULT_MANIFEST,
    run_xfa: bool = True,
) -> dict[str, Any]:
    """Replay frozen 5/10/20 grids and stream XFA diagnostics from exact passes."""

    project = Path(root).resolve()
    frozen_preflight = verify_autonomous_graduation_preflight(preflight)
    manifest = _load_cohort_manifest(project / manifest_path)
    if str(frozen_preflight["manifest_hash"]) != str(manifest["manifest_hash"]):
        raise AutonomousGraduationCohortError("preflight/manifest binding drift")
    artifacts = _load_replay_artifacts(project, manifest)
    prepared = _prepare_all(project, manifest, artifacts)
    by_id = {str(row["candidate_id"]): row for row in prepared}
    frozen_by_id = {
        str(row["candidate_id"]): dict(row)
        for row in frozen_preflight["frozen_candidates"]
    }
    if set(by_id) != set(frozen_by_id):
        raise AutonomousGraduationCohortError("preflight/runtime cohort drift")

    candidate_results: list[dict[str, Any]] = []
    xfa_results: list[dict[str, Any]] = []
    unique_combine_path_keys: set[str] = set()
    for candidate_id in sorted(by_id):
        value = by_id[candidate_id]
        frozen = frozen_by_id[candidate_id]
        runtime_coverage = _coverage_for_prepared(value, artifacts["starts"])
        if (
            stable_hash(runtime_coverage) != str(frozen["coverage_hash"])
            or str(value["frozen_policy_hash"]) != str(frozen["frozen_policy_hash"])
            or str(value["source_evidence_hash"])
            != str(frozen["source_evidence_hash"])
        ):
            raise AutonomousGraduationCohortError(
                f"runtime/preflight policy or coverage drift: {candidate_id}"
            )
        summaries: dict[str, dict[str, Any]] = {scenario: {} for scenario in SCENARIOS}
        receipts: list[dict[str, Any]] = []
        passed_runtime: list[tuple[Any, str, str, int]] = []
        for scenario in SCENARIOS:
            for horizon in HORIZONS:
                values: list[tuple[Any, str]] = []
                for raw_start in runtime_coverage[str(horizon)]["full_coverage_starts"]:
                    start_day = int(raw_start["session_day"])
                    block = str(raw_start["temporal_block"])
                    episode = run_causal_shared_account_episode(
                        value["trajectories"][scenario],
                        value["calendar"],
                        policy=value["policy"],
                        start_day=start_day,
                        maximum_duration_days=horizon,
                        config=value["config"],
                    )
                    values.append((episode, block))
                    path = episode.to_dict(include_paths=True)
                    path_hash = stable_hash(path)
                    receipts.append(
                        {
                            "episode_id": stable_hash(
                                {
                                    "candidate_id": candidate_id,
                                    "scenario": scenario,
                                    "horizon": horizon,
                                    "start_day": start_day,
                                }
                            ),
                            "scenario": scenario,
                            "horizon_trading_days": horizon,
                            "start_day": start_day,
                            "temporal_block": block,
                            "terminal": episode.terminal.value,
                            "passed": bool(episode.passed),
                            "mll_breached": bool(episode.mll_breached),
                            "consistency_ok": bool(episode.consistency_ok),
                            "net_pnl_usd": float(episode.net_pnl),
                            "target_progress": float(episode.target_progress),
                            "minimum_mll_buffer_usd": float(
                                episode.minimum_mll_buffer
                            ),
                            "episode_path_hash": path_hash,
                        }
                    )
                    if episode.passed:
                        passed_runtime.append((episode, scenario, block, horizon))
                summaries[scenario][str(horizon)] = _summarize_exact_episodes(values)

        concentration = _unique_trajectory_concentration(value)
        gates = _development_gates(
            summaries, concentration, dict(manifest["development_gate"])
        )
        qualified_horizons = [
            int(horizon) for horizon, checks in gates.items() if all(checks.values())
        ]
        bundle_core = {
            "schema": "hydra_autonomous_graduation_compact_evidence_bundle_v1",
            "candidate_id": candidate_id,
            "candidate_fingerprint": value["candidate_fingerprint"],
            "behavioral_fingerprint": value["behavioral_fingerprint"],
            "frozen_policy_hash": value["frozen_policy_hash"],
            "source_evidence_hash": value["source_evidence_hash"],
            "preflight_hash": frozen_preflight["preflight_hash"],
            "coverage_hash": frozen["coverage_hash"],
            "episode_receipt_hash": stable_hash(receipts),
            "concentration_hash": concentration["concentration_hash"],
            "gate_hash": stable_hash(gates),
            "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
            "confirmation_claimed": False,
        }
        candidate_result = {
            "candidate_id": candidate_id,
            "kind": value["kind"],
            "candidate_fingerprint": value["candidate_fingerprint"],
            "behavioral_fingerprint": value["behavioral_fingerprint"],
            "qd_cell": value["qd_cell"],
            "account_label": value["account_label"],
            "component_ids": list(value["component_ids"]),
            "frozen_policy_hash": value["frozen_policy_hash"],
            "coverage": runtime_coverage,
            "summaries": summaries,
            "episode_receipts": receipts,
            "unique_trajectory_concentration": concentration,
            "development_gate_results": gates,
            "qualified_horizons": qualified_horizons,
            "computed_evidence_tier": (
                "G_DEVELOPMENT_ONLY" if qualified_horizons else "Q_REPLAYED"
            ),
            "compact_evidence_bundle": {
                **bundle_core,
                "bundle_hash": stable_hash(bundle_core),
            },
            "independent_confirmation_claimed": False,
        }
        candidate_result["result_hash"] = stable_hash(candidate_result)
        candidate_results.append(candidate_result)

        for episode, scenario, block, horizon in passed_runtime:
            path_hash = stable_hash(episode.to_dict(include_paths=True))
            key = stable_hash(
                {
                    "candidate_id": candidate_id,
                    "scenario": scenario,
                    "start_day": int(episode.start_day),
                    "combine_path_hash": path_hash,
                }
            )
            if key in unique_combine_path_keys:
                continue
            unique_combine_path_keys.add(key)
            if run_xfa:
                xfa_results.append(
                    _run_xfa_for_pass(
                        value,
                        episode=episode,
                        scenario=scenario,
                        block=block,
                        source_horizon=horizon,
                        combine_path_hash=path_hash,
                        transition_key=key,
                        rule_snapshot_path=project
                        / manifest["source_contract"]["official_rule_snapshot_path"],
                    )
                )

    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_READ_ONLY_GRADUATION_COHORT_REPLAY",
        "manifest_hash": str(manifest["manifest_hash"]),
        "preflight_hash": str(frozen_preflight["preflight_hash"]),
        "candidate_results": candidate_results,
        "candidate_result_hashes": {
            row["candidate_id"]: row["result_hash"] for row in candidate_results
        },
        "development_g_ids": sorted(
            row["candidate_id"]
            for row in candidate_results
            if row["computed_evidence_tier"] == "G_DEVELOPMENT_ONLY"
        ),
        "xfa_diagnostics": xfa_results,
        "counts": _evidence_counts(
            candidate_results,
            xfa_results,
            unique_combine_path_count=len(unique_combine_path_keys),
        ),
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "independent_confirmation_claimed": False,
        "standard_and_consistency_are_alternatives": True,
        "standard_and_consistency_ev_summed": False,
        "next_action": (
            "FREEZE_G_SURVIVORS_BEFORE_ONE_SHOT_CONFIRMATION"
            if any(
                row["computed_evidence_tier"] == "G_DEVELOPMENT_ONLY"
                for row in candidate_results
            )
            else "KEEP_CONFIRMATION_SEALED_AND_CONTINUE_DISTINCT_DISCOVERY"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def verify_autonomous_graduation_preflight(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    claimed = row.pop("preflight_hash", None)
    if (
        row.get("schema") != PREFLIGHT_SCHEMA
        or row.get("status") != "PASS_FROZEN_FULL_COVERAGE_PREFLIGHT"
        or claimed != stable_hash(row)
    ):
        raise AutonomousGraduationCohortError("preflight schema/hash drift")
    if any(
        int(row.get(field, -1)) != 0
        for field in (
            "confirmation_partition_reads",
            "registry_writes",
            "database_writes",
            "q4_access_count_delta",
            "broker_connections",
            "orders",
        )
    ):
        raise AutonomousGraduationCohortError("preflight side-effect drift")
    return {**row, "preflight_hash": claimed}


def _load_replay_artifacts(project: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    source = dict(manifest["source_contract"])
    bank = books._verify_candidate_bank(
        dict(_read_object(project / source["candidate_bank_path"])["candidate_bank"])
    )
    initial = _read_object(project / INITIAL_EXACT)
    continuations = [
        dict(_read_object(project / path)["continuation_result"])
        for path in CONTINUATION_FILES
    ]
    exact_composite, exact_results = books._verified_exact_results(
        initial, continuations
    )
    if str(exact_composite["result_hash"]) != str(bank["source_composite_result_hash"]):
        raise AutonomousGraduationCohortError("exact-composite provenance drift")
    fast_manifest = _load_self_hashed_manifest(
        project / source["fast_pass_manifest_path"]
    )
    calendar, starts, grid_receipt = _load_frozen_grid(project, fast_manifest)
    rules, rule_receipt = _load_rule_snapshot(
        project / source["official_rule_snapshot_path"]
    )
    if (
        str(fast_manifest["manifest_hash"]) != str(source["fast_pass_manifest_hash"])
        or str(grid_receipt["grid_hash"]) != str(source["frozen_grid_hash"])
        or str(rule_receipt["parsed_rule_hash"])
        != str(source["official_rule_snapshot_hash"])
    ):
        raise AutonomousGraduationCohortError("manifest/grid/rule binding drift")
    bank_entries, bank_receipt = _load_banks(project)
    marginal = dict(
        _read_object(project / source["marginal_book_path"])[
            "semantic_marginal_book_composite"
        ]
    )
    return {
        "bank": bank,
        "exact_results": exact_results,
        "exact_composite_hash": str(exact_composite["result_hash"]),
        "calendar": calendar,
        "starts": starts,
        "grid_receipt": grid_receipt,
        "rules": rules,
        "rule_receipt": rule_receipt,
        "bank_entries": bank_entries,
        "bank_receipt": bank_receipt,
        "marginal": marginal,
        "fast_manifest_path": project / source["fast_pass_manifest_path"],
        "rule_snapshot_path": project / source["official_rule_snapshot_path"],
    }


def _prepare_all(
    project: Path, manifest: Mapping[str, Any], artifacts: Mapping[str, Any]
) -> list[dict[str, Any]]:
    bank_by_id = {
        str(row["candidate_id"]): dict(row) for row in artifacts["bank"]["candidates"]
    }
    exact_by_hash = {
        str(row["result_hash"]): dict(row) for row in artifacts["exact_results"]
    }
    rows: list[dict[str, Any]] = []
    for frozen in manifest["cohort"]:
        if frozen["kind"] != "STANDALONE":
            continue
        candidate_id = str(frozen["candidate_id"])
        value = controls._prepare_candidate(
            project=project,
            classified=bank_by_id[candidate_id],
            exact_by_hash=exact_by_hash,
            bank_entries=artifacts["bank_entries"],
            calendar=artifacts["calendar"],
            starts=artifacts["starts"],
            rules=artifacts["rules"],
        )
        rows.append(
            {
                "kind": "STANDALONE",
                "candidate_id": candidate_id,
                "candidate_fingerprint": value["candidate_fingerprint"],
                "behavioral_fingerprint": value["behavioral_fingerprint"],
                "qd_cell": value["qd_cell"],
                "account_label": value["account_label"],
                "component_ids": (candidate_id,),
                "frozen_policy_hash": value["frozen_account_policy_hash"],
                "source_evidence_hash": stable_hash(value["source_event_receipt"]),
                "calendar": value["calendar"],
                "eligible_session_days": value["eligible_session_days"],
                "censored_session_days": value["censored_session_days"],
                "trajectories": {
                    "NORMAL": {candidate_id: value["normal"]},
                    "STRESSED_1_5X": {candidate_id: value["stressed"]},
                },
                "policy": value["policy"],
                "config": value["config"],
            }
        )

    book_rows = [row for row in manifest["cohort"] if row["kind"] == "MARGINAL_BOOK"]
    if book_rows:
        tier_q_rows = tuple(
            dict(row)
            for row in artifacts["bank"]["candidates"]
            if row.get("tier_q_contract_cleared") is True
        )
        context = books._prepare_replay_context(
            project,
            tier_q_rows,
            artifacts["exact_results"],
            fast_pass_manifest_path=artifacts["fast_manifest_path"],
            rule_snapshot_path=artifacts["rule_snapshot_path"],
        )
        profile_by_id = {row.profile_id: row for row in context.governor_profiles}
        source_books = {
            str(row["policy_id"]): dict(row)
            for row in artifacts["marginal"]["book_results"]
        }
        for frozen in book_rows:
            candidate_id = str(frozen["candidate_id"])
            source_book = source_books[candidate_id]
            profile = profile_by_id[str(frozen["governor_profile_id"])]
            spec = books._policy_spec(
                account_label=str(frozen["account_label"]),
                members=tuple(frozen["component_ids"]),
                profile=profile,
                components=context.components,
                policy_role="FROZEN_GRADUATION_COHORT_BOOK",
                predecessor_policy_id=source_book.get("predecessor_policy_id"),
            )
            if (
                str(spec["policy_id"]) != candidate_id
                or str(spec["policy_spec_hash"]) != str(frozen["candidate_fingerprint"])
            ):
                raise AutonomousGraduationCohortError(
                    f"book executable specification drift: {candidate_id}"
                )
            components = tuple(str(value) for value in frozen["component_ids"])
            eligible = set(context.calendar)
            censored: set[int] = set()
            normal: dict[str, Any] = {}
            stressed: dict[str, Any] = {}
            receipts: dict[str, Any] = {}
            for component_id in components:
                component = context.components[component_id]
                eligible.intersection_update(component.eligible_session_days)
                censored.update(component.censored_session_days)
                normal[component_id] = component.normal_trajectories
                stressed[component_id] = component.stressed_trajectories
                receipts[component_id] = dict(component.source_receipt)
            rows.append(
                {
                    "kind": "MARGINAL_BOOK",
                    "candidate_id": candidate_id,
                    "candidate_fingerprint": str(frozen["candidate_fingerprint"]),
                    "behavioral_fingerprint": str(frozen["behavioral_fingerprint"]),
                    "qd_cell": str(frozen["qd_cell"]),
                    "account_label": str(frozen["account_label"]),
                    "component_ids": components,
                    "frozen_policy_hash": str(spec["policy_spec_hash"]),
                    "source_evidence_hash": stable_hash(receipts),
                    "calendar": context.calendar,
                    "eligible_session_days": frozenset(eligible),
                    "censored_session_days": frozenset(censored),
                    "trajectories": {
                        "NORMAL": normal,
                        "STRESSED_1_5X": stressed,
                    },
                    "policy": books._active_policy(spec, context),
                    "config": _account_config(context.rules[str(frozen["account_label"])]),
                }
            )
    if len(rows) != len(manifest["cohort"]):
        raise AutonomousGraduationCohortError("prepared cohort count drift")
    return rows


def _coverage_for_prepared(
    value: Mapping[str, Any], starts: Mapping[int, Sequence[tuple[int, str]]]
) -> dict[str, Any]:
    calendar = tuple(int(day) for day in value["calendar"])
    index = {day: position for position, day in enumerate(calendar)}
    eligible = {int(day) for day in value["eligible_session_days"]}
    censored_days = {int(day) for day in value["censored_session_days"]}
    output: dict[str, Any] = {}
    for horizon in HORIZONS:
        full: list[dict[str, Any]] = []
        by_block = {block: 0 for block in BLOCKS}
        for start_day, block in starts[horizon]:
            position = index[int(start_day)]
            window = calendar[position : position + horizon]
            if (
                len(window) == horizon
                and all(day in eligible for day in window)
                and all(day not in censored_days for day in window)
            ):
                full.append(
                    {"session_day": int(start_day), "temporal_block": str(block)}
                )
                by_block[str(block)] += 1
        output[str(horizon)] = {
            "requested_start_count": len(starts[horizon]),
            "full_coverage_start_count": len(full),
            "data_censored_start_count": len(starts[horizon]) - len(full),
            "full_coverage_starts": full,
            "full_coverage_by_block": by_block,
            "within_horizon_non_overlapping": True,
            "headline_denominator_excludes_censored": True,
        }
    return output


def _development_gates(
    summaries: Mapping[str, Mapping[str, Mapping[str, Any]]],
    concentration: Mapping[str, Any],
    gate: Mapping[str, Any],
) -> dict[str, dict[str, bool]]:
    output: dict[str, dict[str, bool]] = {}
    for horizon in HORIZONS:
        normal = dict(summaries["NORMAL"][str(horizon)])
        stressed = dict(summaries["STRESSED_1_5X"][str(horizon)])
        pass_blocks = {
            block
            for block, row in dict(stressed.get("by_block") or {}).items()
            if int(row.get("pass_count", 0)) > 0
        }
        output[str(horizon)] = {
            "minimum_normal_passes": int(normal["pass_count"])
            >= int(gate["minimum_normal_passes_same_horizon"]),
            "minimum_stressed_passes": int(stressed["pass_count"])
            >= int(gate["minimum_stressed_passes_same_horizon"]),
            "positive_stressed_net": float(stressed["net_total_usd"]) > 0.0,
            "stressed_mll_within_tolerance": float(stressed["mll_breach_rate"])
            <= float(gate["maximum_stressed_mll_breach_rate"]),
            "passing_consistency_compliant": _passing_consistency_from_summary(
                stressed
            ),
            "multiple_stressed_pass_blocks": len(pass_blocks)
            >= int(gate["minimum_blocks_with_stressed_passes"]),
            "unique_ledger_concentration_cleared": bool(concentration["cleared"]),
        }
    return output


def _passing_consistency_from_summary(summary: Mapping[str, Any]) -> bool:
    # Exact account replay cannot pass with a failed consistency gate.  The
    # terminal engine therefore makes every TARGET_REACHED path compliant.
    return int(summary.get("pass_count", 0)) > 0


def _unique_trajectory_concentration(value: Mapping[str, Any]) -> dict[str, Any]:
    scenarios: dict[str, Any] = {}
    worst = 0.0
    for scenario in SCENARIOS:
        rows = [
            trajectory
            for component in value["trajectories"][scenario].values()
            for trajectory in component
            if getattr(trajectory, "completed", True)
        ]
        identities = [
            (str(row.component_id), str(row.event.event_id)) for row in rows
        ]
        if len(identities) != len(set(identities)):
            raise AutonomousGraduationCohortError("duplicate unique-ledger event")
        positive = [max(float(row.event.net_pnl), 0.0) for row in rows]
        total = sum(positive)
        by_day: dict[int, float] = defaultdict(float)
        for row in rows:
            by_day[int(row.event.session_day)] += max(float(row.event.net_pnl), 0.0)
        trade_share = max(positive, default=0.0) / total if total else 1.0
        day_share = max(by_day.values(), default=0.0) / total if total else 1.0
        maximum = max(trade_share, day_share)
        worst = max(worst, maximum)
        scenarios[scenario] = {
            "record_count": len(rows),
            "unique_event_count": len(identities),
            "positive_profit_total_usd": total,
            "net_total_usd": sum(float(row.event.net_pnl) for row in rows),
            "maximum_single_trade_event_profit_share": trade_share,
            "maximum_single_day_profit_share": day_share,
            "ledger_hash": stable_hash([row.to_dict() for row in rows]),
        }
    core = {
        "denominator": "UNIQUE_COMPLETED_CAUSAL_TRAJECTORIES_NOT_ROLLING_EPISODES",
        "maximum_allowed_profit_share": 0.50,
        "scenarios": scenarios,
        "worst_case_maximum_profit_share": worst,
        "cleared": worst <= 0.50,
    }
    return {**core, "concentration_hash": stable_hash(core)}


def _run_xfa_for_pass(
    value: Mapping[str, Any],
    *,
    episode: Any,
    scenario: str,
    block: str,
    source_horizon: int,
    combine_path_hash: str,
    transition_key: str,
    rule_snapshot_path: Path,
) -> dict[str, Any]:
    days = sorted(
        int(day)
        for day in value["eligible_session_days"]
        if int(day) > int(episode.end_day)
    )
    transition_id = f"cohort-xfa-{transition_key[:24]}"
    if not days:
        return {
            "transition_id": transition_id,
            "status": "XFA_CONTINUATION_UNAVAILABLE_FAIL_CLOSED",
            "candidate_id": value["candidate_id"],
            "scenario": scenario,
            "source_horizon_trading_days": source_horizon,
            "temporal_block": block,
            "combine_path_hash": combine_path_hash,
            "failure_reason": "NO_ELIGIBLE_SESSION_DAY_AFTER_COMBINE_PASS",
            "alternative_path_count": 0,
        }
    rules = load_account_size_xfa_rules(
        str(value["account_label"]), snapshot_path=rule_snapshot_path
    )
    maximum_concurrency = min(
        len(value["component_ids"]),
        int(getattr(value["policy"], "maximum_concurrent_sleeves", 1)),
    )
    handoff = freeze_account_size_xfa_handoff(
        candidate_id=str(value["candidate_id"]),
        combine_book_hash=str(value["frozen_policy_hash"]),
        component_priority=value["component_ids"],
        rules=rules,
        risk_multiplier=1.0,
        maximum_simultaneous_positions=maximum_concurrency,
        maximum_mini_equivalent=float(
            getattr(
                value["policy"],
                "maximum_mini_equivalent",
                rules.combine_maximum_mini_equivalent,
            )
        ),
        same_market_exclusive=True,
        profile_id=f"{value['candidate_id']}:DIAGNOSTIC_XFA_V1",
    )
    continuation = {
        component_id: tuple(
            row
            for row in value["trajectories"][scenario][component_id]
            if int(row.event.session_day) >= days[0]
        )
        for component_id in value["component_ids"]
    }
    result = run_account_size_xfa_alternatives(
        continuation,
        days,
        handoff=handoff,
        rules=rules,
        transition_id=transition_id,
        combine_path_hash=combine_path_hash,
        start_day=days[0],
        horizon_days=120,
    ).to_dict()
    return {
        "transition_id": transition_id,
        "status": "COMPLETE_DIAGNOSTIC_XFA_ALTERNATIVES",
        "candidate_id": value["candidate_id"],
        "scenario": scenario,
        "source_horizon_trading_days": source_horizon,
        "temporal_block": block,
        "combine_start_day": int(episode.start_day),
        "combine_pass_day": int(episode.end_day),
        "combine_path_hash": combine_path_hash,
        "xfa_start_day": days[0],
        "standard_and_consistency_are_alternatives": True,
        "sum_expected_values": False,
        "alternative_path_count": 2,
        "engine_result": result,
        "diagnostic_hash": stable_hash(result),
    }


def _evidence_counts(
    candidate_results: Sequence[Mapping[str, Any]],
    xfa_results: Sequence[Mapping[str, Any]],
    *,
    unique_combine_path_count: int,
) -> dict[str, int]:
    """Separate overlapping horizon observations from unique economic paths."""

    episode_observations = sum(
        int(summary[scenario][str(horizon)]["episode_count"])
        for summary in (row["summaries"] for row in candidate_results)
        for scenario in SCENARIOS
        for horizon in HORIZONS
    )
    pass_observations = sum(
        int(summary[scenario][str(horizon)]["pass_count"])
        for summary in (row["summaries"] for row in candidate_results)
        for scenario in SCENARIOS
        for horizon in HORIZONS
    )
    return {
        "candidate_count": len(candidate_results),
        "combine_episode_horizon_observation_count_non_independent": (
            episode_observations
        ),
        "combine_pass_horizon_observation_count_non_independent": (
            pass_observations
        ),
        "unique_combine_path_count": int(unique_combine_path_count),
        "unique_xfa_transition_record_count": len(xfa_results),
        "ready_xfa_transition_count": sum(
            row.get("status") == "COMPLETE_DIAGNOSTIC_XFA_ALTERNATIVES"
            for row in xfa_results
        ),
        "fail_closed_xfa_transition_count": sum(
            row.get("status") == "XFA_CONTINUATION_UNAVAILABLE_FAIL_CLOSED"
            for row in xfa_results
        ),
        "alternative_xfa_path_count": sum(
            int(row.get("alternative_path_count", 0)) for row in xfa_results
        ),
        "registry_writes": 0,
        "database_writes": 0,
        "confirmation_partition_reads": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }


def _load_cohort_manifest(path: Path) -> dict[str, Any]:
    payload = _read_object(path)
    claimed = str(payload.get("manifest_hash") or "")
    core = dict(payload)
    core.pop("manifest_hash", None)
    if (
        payload.get("schema") != "hydra_autonomous_graduation_cohort_manifest_v1"
        or not claimed
        or stable_hash(core) != claimed
    ):
        raise AutonomousGraduationCohortError("cohort manifest self-hash drift")
    return payload


def _require_file_hashes(project: Path, source: Mapping[str, Any]) -> None:
    bindings = (
        ("candidate_bank_path", "candidate_bank_file_sha256"),
        ("marginal_book_path", "marginal_book_file_sha256"),
        ("fresh_confirmation_path", "fresh_confirmation_file_sha256"),
        ("tier_q_2026_contract_path", "tier_q_2026_contract_file_sha256"),
        (
            "tier_q_2026_final_development_path",
            "tier_q_2026_final_development_file_sha256",
        ),
    )
    for path_key, hash_key in bindings:
        path = project / str(source[path_key])
        if _sha256(path) != str(source[hash_key]):
            raise AutonomousGraduationCohortError(f"source file hash drift: {path_key}")


def _verify_self_hashed(value: Mapping[str, Any], *, label: str) -> None:
    row = dict(value)
    claimed = row.pop("result_hash", None)
    if claimed != stable_hash(row):
        raise AutonomousGraduationCohortError(f"{label} result hash drift")


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AutonomousGraduationCohortError(f"cannot read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise AutonomousGraduationCohortError(f"JSON object required: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "AUDIT_SCHEMA",
    "AutonomousGraduationCohortError",
    "DEFAULT_MANIFEST",
    "PREFLIGHT_SCHEMA",
    "SCHEMA",
    "audit_autonomous_graduation_cohort",
    "build_autonomous_graduation_preflight",
    "execute_autonomous_graduation_cohort",
    "verify_autonomous_graduation_preflight",
]
