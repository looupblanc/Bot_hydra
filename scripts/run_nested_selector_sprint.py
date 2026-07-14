from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import random
import re
import sqlite3
import statistics
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import hydra.account_policy.basket as basket_engine
from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    ComplementarySleeveBasketPolicy,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.account_static_parent_basket import (
    STATIC_PARENT_BASKET_POLICY_VERSION,
    StaticParentBasketPolicy,
    route_static_parent_basket_entry,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    _load_and_verify_generic_account_pair_preregistration,
)
from hydra.selection.nested_basket_selector import (
    ParetoObjective,
    SelectionDecision,
    SelectorError,
    select_pareto_champion,
)
from hydra.selection.fold_candidate_compiler import (
    EXPECTED_CANDIDATE_COUNT,
    compile_fold_candidate_bank,
    frozen_compiler_policy,
)
from hydra.selection.risk_frontier import FROZEN_RISK_TIERS
from hydra.selection.selector_crossfit import (
    OuterFold,
    RecoveredCampaignLedger,
    TemporalBlock,
    TemporalBlockPlan,
    build_purged_temporal_block_plan,
    file_sha256,
    isolate_outer_fold_ledger,
    leave_one_block_out_folds,
    load_recovered_event_ledger,
)
from hydra.selection.selector_evaluation import (
    aggregate_design_block_metrics,
    elite_robustness_policy_from_dict,
    evaluate_policy_block,
    static_parent_policy_from_dict,
)
from hydra.selection.selector_manifest import (
    BASELINES,
    HARD_REQUIREMENTS,
    PARETO_OBJECTIVES,
    SCHEMA as SELECTOR_MANIFEST_SCHEMA,
    finalize_manifest,
)
from hydra.selection.selector_reporting import (
    SELECTOR_PROCEDURE_FALSIFIED,
    SELECTOR_PROCEDURE_GREEN,
    SELECTOR_PROCEDURE_WEAK,
    SelectorProcedureDecision,
    build_manifest_runtime_compatibility_projection,
    decide_selector_procedure,
    frozen_decision_manifest_policy,
    render_selector_report,
)
from hydra.selection.time_to_combine import evaluate_time_to_combine
from hydra.utils.time import utc_now_iso


ENGINE_VERSION = "hydra_v73_nested_selector_sprint_v1"
RESULT_SCHEMA = "hydra_v73_nested_selector_result_v1"
CAMPAIGN_0023_ID = "hydra_economic_evolution_static_parent_basket_0023"
DIAGNOSTIC_CHAMPION_ID = "static_parent_basket_428d24d049c7ae3c01b95e85"
PRIMARY_HORIZON_DAYS = 40
RANDOM_BASELINE_SEEDS: tuple[int, ...] = (7301, 7302, 7303, 7304, 7305)
MAXIMUM_COMPONENT_PROFIT_SHARE = 0.65
MAXIMUM_MLL_BREACH_RATE = 0.10
BACKUP_MAXIMUM_COMPONENT_JACCARD = 0.80

_WORKER_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_WORKER_BLOCKS: Mapping[str, TemporalBlock] = {}


class NestedSelectorSprintError(RuntimeError):
    pass


def _selector_execution_policy() -> dict[str, Any]:
    """Return the exact transparent selector and tie-break policy."""

    return {
        "method": "PARETO_NONDOMINATED_THEN_LEXICOGRAPHIC",
        "opaque_learned_score": False,
        "pareto_dominance": (
            "WEAKLY_BETTER_ON_EVERY_OBJECTIVE_AND_STRICTLY_BETTER_ON_AT_LEAST_ONE"
        ),
        "lexicographic_tiebreak_order": [
            {"metric": metric, "direction": direction}
            for metric, direction in PARETO_OBJECTIVES
        ],
        "final_tiebreak": "VARIANT_ID_ASCENDING",
        "clone_key": "DESIGN_BEHAVIOR_FINGERPRINT",
        "clone_representative_rule": "SAME_LEXICOGRAPHIC_ORDER",
        "primary_rule": "FIRST_LEXICOGRAPHIC_MEMBER_OF_PARETO_FRONTIER",
        "backup_rule": (
            "FIRST_ELIGIBLE_BEHAVIORALLY_DISTINCT_LEXICOGRAPHIC_MEMBER"
        ),
        "backup_maximum_component_jaccard": BACKUP_MAXIMUM_COMPONENT_JACCARD,
        "maximum_design_mll_breach_rate": MAXIMUM_MLL_BREACH_RATE,
        "maximum_design_component_profit_share": MAXIMUM_COMPONENT_PROFIT_SHARE,
        "operational_simplicity_formula": (
            "-(10*COMPONENT_COUNT + STATIC_MICRO_RISK_UNITS)"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()
    result = run_nested_selector_sprint(
        args.output_dir,
        preregistration_path=args.preregistration,
        contract_map_path=args.contract_map,
        cache_root=args.cache_root,
    )
    print(
        json.dumps(
            {
                "campaign_id": result["campaign_id"],
                "scientific_status": result["scientific_status"],
                "result_sha256": result["result_sha256"],
                "broker_connections": 0,
                "orders": 0,
            },
            sort_keys=True,
        )
    )
    return 0


def run_nested_selector_sprint(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    phase_seconds: dict[str, float] = {}
    prereg_path = Path(preregistration_path).resolve()
    prereg = _load_and_verify_generic_account_pair_preregistration(prereg_path)
    _validate_nested_preregistration(prereg)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    governance_start = _sprint_governance_observation(root)
    _validate_sprint_governance_baseline(
        prereg["sprint_governance_baseline"], governance_start
    )
    writer.write_json("sprint_governance_start.json", governance_start)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    phase_started = time.perf_counter()
    source_paths = _verify_source_artifacts(root, prereg)
    campaign_audit = _audit_campaign_0023(source_paths)
    _persist_campaign_0023_terminal(source_paths["campaign_dir"], campaign_audit)
    _stage(state_writer, prereg, "CAMPAIGN_0023_TERMINAL_PERSISTED")
    phase_seconds["predecessor_terminal_audit"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    ledger = _load_immutable_ledger(
        source_paths["component_ledger_manifest"]
    )
    plan = build_purged_temporal_block_plan(
        ledger,
        contract_map_path=contract_map_path,
        contamination_history_by_block=_contamination_history(),
    )
    folds = leave_one_block_out_folds(plan)
    selector_manifest = _realized_selector_manifest(prereg, ledger, plan)
    writer.write_json("selector_manifest.json", selector_manifest)
    writer.write_json("temporal_block_plan.json", plan.to_dict())
    writer.write_json(
        "recovered_ledger_reference.json",
        {
            "manifest_path": str(ledger.manifest_path.relative_to(root)),
            "ledger_path": str(ledger.ledger_path.relative_to(root)),
            "ledger_sha256": ledger.ledger_sha256,
            "runtime_count": len(ledger.runtimes),
            "event_count": int(ledger.manifest["event_count"]),
            "metadata_reconciliation": ledger.manifest["provenance"][
                "runtime_metadata_reconciliation"
            ],
            "features_or_signals_recomputed_on_load": False,
        },
    )
    _stage(state_writer, prereg, "LEDGER_AND_TEMPORAL_PLAN_FROZEN")
    phase_seconds["ledger_and_temporal_plan"] = time.perf_counter() - phase_started

    proposals, parents = _load_policy_banks(source_paths)
    if len(proposals) != 512 or len(parents) != 8:
        raise NestedSelectorSprintError("frozen 0023 policy-bank cardinality drift")
    global _WORKER_RUNTIMES, _WORKER_BLOCKS
    _WORKER_RUNTIMES = ledger.runtimes
    _WORKER_BLOCKS = {block.block_id: block for block in plan.blocks}

    phase_started = time.perf_counter()
    frozen_folds: list[dict[str, Any]] = []
    for fold in folds:
        frozen = _freeze_outer_fold(
            fold,
            plan=plan,
            ledger=ledger,
            parents=parents,
            worker_count=3,
            writer=writer,
        )
        frozen_folds.append(frozen)
    selection_freeze = {
        "schema": "hydra_v73_all_outer_selections_frozen_v1",
        "selector_manifest_hash": selector_manifest["manifest_hash"],
        "outer_fold_count": len(frozen_folds),
        "every_block_held_out_exactly_once": True,
        "held_out_evaluation_started_before_freeze": False,
        "folds": [row["public_freeze"] for row in frozen_folds],
    }
    selection_freeze["freeze_hash"] = stable_hash(selection_freeze)
    writer.write_json("all_outer_selections_frozen.json", selection_freeze)
    _stage(state_writer, prereg, "ALL_OUTER_SELECTIONS_FROZEN_BEFORE_HELD_OUT")
    phase_seconds["design_only_selection"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    held_out_folds = _evaluate_all_held_out(
        frozen_folds,
        diagnostic_policy=proposals[DIAGNOSTIC_CHAMPION_ID],
        worker_count=3,
        writer=writer,
    )
    decision = decide_selector_procedure(
        [row["decision_evidence"] for row in held_out_folds]
    )
    inadmissible_folds = [
        row["fold"].fold_id
        for row in frozen_folds
        if not row["selector_admissible"]
    ]
    if inadmissible_folds:
        original = decision.to_dict()
        decision = SelectorProcedureDecision(
            status=SELECTOR_PROCEDURE_FALSIFIED,
            metrics={
                **dict(decision.metrics),
                "design_selection_failure_folds": inadmissible_folds,
                "held_out_fallback_rows_are_diagnostic_only": True,
                "unforced_held_out_decision": original,
            },
            checks={
                "green": {
                    **dict(decision.checks["green"]),
                    "admissible_champion_in_every_design_set": False,
                },
                "weak": {
                    **dict(decision.checks["weak"]),
                    "admissible_champion_in_every_design_set": False,
                },
            },
            failure_reasons=tuple(
                [
                    *decision.failure_reasons,
                    *(
                        f"design.{fold_id}.no_candidate_cleared_hard_requirements"
                        for fold_id in inadmissible_folds
                    ),
                ]
            ),
            thresholds=dict(decision.thresholds),
        )
    writer.write_json("held_out_outer_fold_evidence.json", held_out_folds)
    writer.write_json("selector_procedure_decision.json", decision.to_dict())
    _stage(state_writer, prereg, "HELD_OUT_SELECTOR_DECISION_COMPLETE")
    phase_seconds["held_out_evaluation"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    final_development = _conditional_final_development_selection(
        decision_status=decision.status,
        plan=plan,
        ledger=ledger,
        parents=parents,
        prereg=prereg,
        selector_manifest=selector_manifest,
        worker_count=3,
        writer=writer,
    )
    phase_seconds["conditional_final_development"] = (
        time.perf_counter() - phase_started
    )

    service_state = _actual_state_snapshot(root)
    budget = _budget_snapshot(
        root, baseline=prereg["sprint_governance_baseline"]
    )
    q4_status = _q4_snapshot(
        root, baseline=prereg["sprint_governance_baseline"]
    )
    forward_status = _forward_feed_snapshot(
        root, baseline=governance_start["forward_feed"]
    )
    governance_end = {
        "schema": "hydra_v73_sprint_governance_end_v1",
        "budget": budget,
        "q4": q4_status,
        "forward_feed": forward_status,
    }
    writer.write_json("sprint_governance_end.json", governance_end)
    if budget["actual_spend_delta_usd"] > 1e-12 or q4_status[
        "q4_access_during_sprint"
    ] > 0:
        writer.write_json(
            "sprint_governance_violation.json",
            {
                "schema": "hydra_v73_sprint_governance_violation_v1",
                "budget": budget,
                "q4": q4_status,
                "result_publication_allowed": False,
            },
        )
        raise NestedSelectorSprintError(
            "budget purchase or Q4 access occurred during frozen selector sprint"
        )
    next_action = _next_action(decision.status, final_development)
    report_sections = _required_report_sections(
        service_state=service_state,
        campaign_audit=campaign_audit,
        selector_manifest=selector_manifest,
        plan=plan,
        frozen_folds=frozen_folds,
        held_out_folds=held_out_folds,
        decision=decision.to_dict(),
        final_development=final_development,
        budget=budget,
        q4_status=q4_status,
        forward_status=forward_status,
        next_action=next_action,
    )
    report = render_selector_report(report_sections)
    writer.write_json("nested_selector_evidence.json", report_sections)
    writer.write_text("nested_selector_report.md", report)

    elapsed = time.perf_counter() - started
    compatibility = build_manifest_runtime_compatibility_projection(
        decision,
        result_schema=str(prereg["runtime_manifest"]["result_schema"]),
        campaign_id=str(prereg["campaign_id"]),
        class_id=str(prereg["class_id"]),
        population_manifest_hash=str(
            prereg["structural_population"]["policy_manifest_hash"]
        ),
        compatibility_policy_pair_count=int(
            prereg["structural_population"]["policy_pair_count"]
        ),
        primary_rolling_combine_episode_count=(
            int(prereg["structural_population"]["policy_pair_count"])
            * int(prereg["rolling_episode_policy"]["maximum_starts"])
        ),
    )
    result = _complete_runtime_projection(
        compatibility,
        decision=decision.to_dict(),
        held_out_folds=held_out_folds,
        selector_manifest=selector_manifest,
        campaign_audit=campaign_audit,
        final_development=final_development,
        phase_seconds=phase_seconds,
        elapsed_seconds=elapsed,
        service_state=service_state,
        budget=budget,
        q4_status=q4_status,
        forward_status=forward_status,
        next_action=next_action,
    )
    _stage(state_writer, prereg, "COMPLETE")
    # Publish the controller's terminal sentinel last; once this file appears,
    # the persistent runtime is allowed to ingest and terminalize the campaign.
    writer.write_json(str(prereg["runtime_manifest"]["result_name"]), result)
    _WORKER_RUNTIMES = {}
    _WORKER_BLOCKS = {}
    return result


def _validate_nested_preregistration(prereg: Mapping[str, Any]) -> None:
    policy = prereg.get("nested_selector_policy") or {}
    baseline = prereg.get("sprint_governance_baseline") or {}
    if (
        prereg.get("class_id") != "NESTED_STATIC_BASKET_SELECTOR_PROCEDURE_V1"
        or prereg.get("runtime_manifest", {}).get("result_schema") != RESULT_SCHEMA
        or int(prereg["structural_population"]["policy_pair_count"]) != 4
        or int(prereg["rolling_episode_policy"]["maximum_starts"]) != 24
        or policy.get("selector_manifest_schema") != SELECTOR_MANIFEST_SCHEMA
        or policy.get("risk_tiers") != [0.75, 1.0, 1.25, 1.5]
        or policy.get("frozen_horizons") != [20, 40, 60, 90, "full"]
        or policy.get("pareto_objectives")
        != [
            {"metric": metric, "direction": direction}
            for metric, direction in PARETO_OBJECTIVES
        ]
        or policy.get("selector_execution_policy")
        != _selector_execution_policy()
        or policy.get("random_selection_seeds") != list(RANDOM_BASELINE_SEEDS)
        or policy.get("decision_thresholds")
        != frozen_decision_manifest_policy()
        or policy.get("candidate_bank_policy") != frozen_compiler_policy()
        or policy.get("selector_frozen_before_heldout") is not True
        or policy.get("thresholds_changed_after_outcome") is not False
        or set(baseline)
        != {
            "budget_actual_spend_usd",
            "q4_historical_access_count",
            "q4_proof_entry_count",
        }
        or type(baseline.get("budget_actual_spend_usd")) not in {int, float}
        or type(baseline.get("q4_historical_access_count")) is not int
        or type(baseline.get("q4_proof_entry_count")) is not int
    ):
        raise NestedSelectorSprintError("invalid nested-selector preregistration")


def _verify_source_artifacts(
    root: Path, prereg: Mapping[str, Any]
) -> dict[str, Path]:
    references = prereg.get("source_artifacts") or {}
    required = {
        "preregistration",
        "proposals",
        "pair_results",
        "population",
        "exact_metadata",
        "result",
        "tripwire",
        "component_ledger_manifest",
    }
    if set(references) != required:
        raise NestedSelectorSprintError("source artifact manifest is incomplete")
    output: dict[str, Path] = {}
    for name, reference in references.items():
        path = (root / str(reference["path"])).resolve()
        if file_sha256(path) != str(reference["sha256"]):
            raise NestedSelectorSprintError(f"source artifact checksum drift: {name}")
        output[name] = path
    output["campaign_dir"] = output["result"].parent
    return output


def _load_immutable_ledger(manifest_path: Path) -> RecoveredCampaignLedger:
    """Load a pre-existing event ledger; never reconstruct trading decisions.

    Campaign 0023 persisted only metadata.  The user's sprint contract forbids
    recalculating signals, entries, or exits, so absence of an authoritative
    ledger is a hard pre-outcome blocker rather than permission to regenerate
    it from feature matrices.
    """

    if not manifest_path.is_file():
        raise NestedSelectorSprintError(
            "authoritative immutable campaign-0023 component ledger is missing; "
            "signal/entry/exit reconstruction is forbidden"
        )
    ledger = load_recovered_event_ledger(manifest_path)
    if int(ledger.manifest.get("event_count", -1)) != 3_778:
        raise NestedSelectorSprintError("immutable component-ledger event count drift")
    if len(ledger.runtimes) != 48:
        raise NestedSelectorSprintError("immutable component-ledger runtime count drift")
    provenance = dict(ledger.manifest.get("provenance") or {})
    if provenance.get("signals_recomputed_for_selector") is not False:
        raise NestedSelectorSprintError(
            "component ledger lacks a no-selector-recomputation attestation"
        )
    return ledger


def _contamination_history() -> dict[str, list[dict[str, Any]]]:
    shared = [
        {
            "type": "HISTORICAL_DEVELOPMENT_REUSE",
            "effect": "Nested held-out evidence is cross-fitted development evidence only.",
        },
        {
            "type": "CAMPAIGN_0023_FULL_PERIOD_ALREADY_EVALUATED",
            "effect": "The selector procedure, not any individual basket, is the hypothesis.",
        },
        {
            "type": "DIAGNOSTIC_CHAMPION_SELECTED_ON_ALL_BLOCKS",
            "effect": "The 18.75% champion is reference-only and excluded from decisions.",
        },
    ]
    return {
        "V73_B1": [*shared, {"type": "DEVELOPMENT_Q3_2023", "q4_2024": False}],
        "V73_B2": [*shared, {"type": "DEVELOPMENT_Q4_2023_Q1_2024", "q4_2024": False}],
        "V73_B3": [
            *shared,
            {
                "type": "Q2_2024_PREVIOUSLY_CONSUMED_DIAGNOSTIC_OVERLAP",
                "q4_2024": False,
            },
        ],
        "V73_B4": [
            *shared,
            {
                "type": "Q2_Q3_2024_CONTAMINATED_DEVELOPMENT",
                "q4_2024": False,
            },
        ],
    }


def _realized_selector_manifest(
    prereg: Mapping[str, Any],
    ledger: RecoveredCampaignLedger,
    plan: TemporalBlockPlan,
) -> dict[str, Any]:
    policy = dict(prereg["nested_selector_policy"])
    blocks: list[dict[str, Any]] = []
    for block_index, block in enumerate(plan.blocks):
        flattened_contracts = sorted(
            {
                contract
                for values in block.contracts_by_market.values()
                for contract in values
            }
        )
        regime = ",".join(
            f"{key}:{value}"
            for key, value in sorted(block.volatility_regime_counts.items())
        )
        interval_audit = _block_event_interval_audit(
            ledger,
            block=block,
            next_block=(
                plan.blocks[block_index + 1]
                if block_index + 1 < len(plan.blocks)
                else None
            ),
        )
        if interval_audit["exit_intervals_reaching_next_independent_block"]:
            raise NestedSelectorSprintError(
                f"{block.block_id} event interval reaches next independent block"
            )
        blocks.append(
            {
                "block_id": block.block_id,
                "start_date": block.start_date,
                "end_date_exclusive": (
                    date.fromisoformat(block.end_date) + timedelta(days=1)
                ).isoformat(),
                "contracts": flattened_contracts,
                "contract_and_roll_separation": {
                    "contract_identity_recorded_by_market_and_session": True,
                    "roll_transition_dates_explicit": True,
                    "unsafe_roll_windows_explicit": True,
                    "roll_boundaries_not_used_to_invent_independence": True,
                    "block_may_span_contracts": len(flattened_contracts)
                    > len(block.contracts_by_market),
                },
                "event_count": block.event_count,
                "trading_days": block.trading_day_count,
                "markets": sorted(
                    set(block.signal_markets) | set(block.execution_markets)
                ),
                "sessions": [f"SESSION_CODE_{value}" for value in block.session_codes],
                "volatility_regime": regime or "NO_EVENT_REGIME",
                "episode_starts": [
                    f"{_session_date(value)}T00:00:00Z"
                    for value in block.episode_start_days
                ],
                "episode_starts_unique_across_blocks": True,
                "inference_unit": "TEMPORAL_BLOCK",
                "within_block_starts_independent": False,
                "overlapping_episode_starts_counted_as_independent": False,
                "primary_horizon_complete_for_every_start": True,
                "contamination_history": [
                    dict(value) for value in block.contamination_history
                ],
                "provenance": {
                    "ledger_path": str(ledger.ledger_path),
                    "ledger_sha256": ledger.ledger_sha256,
                    "temporal_plan_hash": plan.plan_hash,
                    "session_days": list(block.session_days),
                    "embargo_before_days": list(block.embargo_before_days),
                    "embargo_after_days": list(block.embargo_after_days),
                    "contracts_by_market": {
                        key: list(value)
                        for key, value in block.contracts_by_market.items()
                    },
                    "roll_transition_dates_by_market": {
                        key: list(value)
                        for key, value in block.roll_transition_dates_by_market.items()
                    },
                    "unsafe_roll_session_dates_by_market": {
                        key: list(value)
                        for key, value in block.unsafe_roll_session_dates_by_market.items()
                    },
                    "volatility_regime_counts": dict(
                        block.volatility_regime_counts
                    ),
                    "starts_are_dependent_descriptive_replays": True,
                    "event_interval_isolation": interval_audit,
                },
            }
        )
    payload = {
        "schema": SELECTOR_MANIFEST_SCHEMA,
        "experiment_id": prereg["campaign_id"],
        "temporal_blocks": blocks,
        "outer_crossfit": policy["outer_crossfit"],
        "frozen_horizons": policy["frozen_horizons"],
        "risk_tiers": policy["risk_tiers"],
        "risk_selected_inside_design_set": True,
        "static_risk_only": True,
        "pareto_objectives": policy["pareto_objectives"],
        "selector_execution_policy": policy["selector_execution_policy"],
        "hard_requirements": {name: True for name in HARD_REQUIREMENTS},
        "baselines": list(BASELINES),
        "random_selection_seeds": list(RANDOM_BASELINE_SEEDS),
        "decision_thresholds": policy["decision_thresholds"],
        "candidate_bank_policy": policy["candidate_bank_policy"],
        "primary_horizon_days": PRIMARY_HORIZON_DAYS,
        "block_is_inference_unit": True,
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_or_orders_allowed": False,
        },
        "compute_workers": 3,
        "selector_frozen_before_heldout": True,
    }
    return finalize_manifest(payload)


def _block_event_interval_audit(
    ledger: RecoveredCampaignLedger,
    *,
    block: TemporalBlock,
    next_block: TemporalBlock | None,
) -> dict[str, Any]:
    """Prove trade exits cannot carry design information into the next block."""

    allowed = set(block.session_days)
    events = [
        routed.event
        for runtime in ledger.runtimes.values()
        for routed in runtime.events
        if routed.event.session_day in allowed
    ]
    nanoseconds_per_day = 86_400 * 1_000_000_000
    block_end_exclusive_ns = (max(block.session_days) + 1) * nanoseconds_per_day
    next_block_start_ns = (
        min(next_block.session_days) * nanoseconds_per_day
        if next_block is not None
        else None
    )
    exits_after_block_calendar_boundary = sum(
        event.exit_ns >= block_end_exclusive_ns for event in events
    )
    reaches_next = (
        sum(event.exit_ns >= next_block_start_ns for event in events)
        if next_block_start_ns is not None
        else 0
    )
    return {
        "event_count": len(events),
        "maximum_exit_ns": max((event.exit_ns for event in events), default=None),
        "block_end_exclusive_ns": block_end_exclusive_ns,
        "next_independent_block_start_ns": next_block_start_ns,
        "exit_intervals_after_block_calendar_boundary": (
            exits_after_block_calendar_boundary
        ),
        "embargo_may_absorb_calendar_boundary_exits": True,
        "exit_intervals_reaching_next_independent_block": reaches_next,
        "independent_block_isolation_verified": reaches_next == 0,
    }


def _load_policy_banks(
    source_paths: Mapping[str, Path],
) -> tuple[dict[str, StaticParentBasketPolicy], dict[str, Any]]:
    proposals: dict[str, StaticParentBasketPolicy] = {}
    for row in _read_jsonl(source_paths["proposals"]):
        policy = static_parent_policy_from_dict(row)
        proposals[policy.policy_id] = policy
    parent_rows: dict[str, Mapping[str, Any]] = {}
    for row in _read_jsonl(source_paths["pair_results"]):
        value = row["matched_control_policy"]
        parent_rows[str(value["policy_id"])] = value
    parents = {
        key: elite_robustness_policy_from_dict(value)
        for key, value in sorted(parent_rows.items())
    }
    return dict(sorted(proposals.items())), parents


def _freeze_outer_fold(
    fold: OuterFold,
    *,
    plan: TemporalBlockPlan,
    ledger: RecoveredCampaignLedger,
    parents: Mapping[str, Any],
    worker_count: int,
    writer: AtomicResultWriter,
) -> dict[str, Any]:
    isolated = isolate_outer_fold_ledger(ledger, plan, fold)
    component_stats = _component_design_statistics(
        ledger.runtimes,
        blocks={block.block_id: block for block in plan.blocks},
        design_block_ids=fold.design_block_ids,
    )
    compilation = compile_fold_candidate_bank(
        component_design_stats=component_stats,
        parents=tuple(parents.values()),
        design_block_ids=fold.design_block_ids,
        compiler_policy=frozen_compiler_policy(),
    )
    candidates = list(compilation.policies)
    if len(candidates) != EXPECTED_CANDIDATE_COUNT:
        raise NestedSelectorSprintError("fold-local candidate compiler quota drift")
    candidate_metrics = _evaluate_design_policy_bank(
        candidates,
        fold=fold,
        worker_count=worker_count,
    )
    candidate_records = [_selector_record(row) for row in candidate_metrics]
    candidate_selection_error: str | None = None
    try:
        candidate_decision = _select(candidate_records)
        selector_admissible = True
    except SelectorError as exc:
        # A selector that cannot produce a hard-gate-admissible champion is a
        # falsified scientific procedure, not a retryable engineering crash.
        # The deterministic row below is evaluated held-out only as a clearly
        # labelled diagnostic reference so the four-block report stays complete.
        candidate_selection_error = str(exc)
        candidate_decision = _select_baseline(candidate_records)
        selector_admissible = False

    parent_metrics = _evaluate_design_policy_bank(
        list(parents.values()),
        fold=fold,
        worker_count=worker_count,
    )
    parent_decision = _select_baseline(
        [_selector_record(row) for row in parent_metrics]
    )

    candidate_lookup = {policy.policy_id: policy for policy in candidates}
    primary_policy = candidate_lookup[str(candidate_decision.primary["policy_id"])]
    primary_risk_frontier = sorted(
        (
            row
            for row in candidate_records
            if row["policy_id"] == primary_policy.policy_id
        ),
        key=lambda row: float(row["micro_risk_units"]),
    )
    basket_size = len(primary_policy.component_ids)
    eligible_components = _eligible_component_ids(component_stats)
    equal_membership = tuple(eligible_components[:basket_size])
    if len(equal_membership) != basket_size:
        raise NestedSelectorSprintError("equal-risk baseline lacks eligible components")
    equal_policy = _synthetic_baseline_policy(
        primary_policy,
        membership=equal_membership,
        label=f"{fold.fold_id}_EQUAL_RISK",
    )
    random_policies: dict[int, StaticParentBasketPolicy] = {}
    for seed in RANDOM_BASELINE_SEEDS:
        generator = random.Random(seed)
        membership = tuple(generator.sample(eligible_components, basket_size))
        random_policies[seed] = _synthetic_baseline_policy(
            primary_policy,
            membership=membership,
            label=f"{fold.fold_id}_RANDOM_{seed}",
        )
    baseline_policies = [equal_policy, *random_policies.values()]
    baseline_metrics = _evaluate_design_policy_bank(
        baseline_policies,
        fold=fold,
        worker_count=worker_count,
    )
    metrics_by_policy: dict[str, list[dict[str, Any]]] = {}
    for row in baseline_metrics:
        metrics_by_policy.setdefault(str(row["policy_id"]), []).append(row)
    equal_decision = _select_baseline(
        [_selector_record(row) for row in metrics_by_policy[equal_policy.policy_id]]
    )
    random_decisions = {
        seed: _select_baseline(
            [
                _selector_record(row)
                for row in metrics_by_policy[policy.policy_id]
            ]
        )
        for seed, policy in random_policies.items()
    }

    policy_lookup: dict[str, Any] = {
        **candidate_lookup,
        **parents,
        equal_policy.policy_id: equal_policy,
        **{policy.policy_id: policy for policy in random_policies.values()},
    }
    backup_policy = (
        policy_lookup[str(candidate_decision.backup["policy_id"])]
        if candidate_decision.backup is not None
        else None
    )
    public = {
        "schema": "hydra_v73_outer_fold_selection_freeze_v1",
        "fold": fold.to_dict(),
        "candidate_bank": {
            "available_structure_count": len(candidates),
            "evaluated_static_risk_variant_count": len(candidate_metrics),
            "policy_ids": [row.policy_id for row in candidates],
            "construction": "FROZEN_V73_FOLD_LOCAL_20_GROUP_X_4_SIZE_COMPILER",
            "compiler_policy_hash": frozen_compiler_policy()["policy_hash"],
            "compilation_audit_hash": compilation.audit["audit_hash"],
            "design_ledger_view_hash": isolated.design.view_hash,
            "campaign_0023_proposal_memberships_used": False,
            "full_period_parent_outcomes_used": False,
            "held_out_block_used": False,
        },
        "selector_decision": candidate_decision.to_dict(),
        "primary_is_admissible_champion": selector_admissible,
        "nonadmissible_fallback_is_diagnostic_only": not selector_admissible,
        "selector_design_failure": candidate_selection_error,
        "best_parent_decision": parent_decision.to_dict(),
        "equal_risk_decision": equal_decision.to_dict(),
        "random_selection_decisions": {
            str(seed): decision.to_dict()
            for seed, decision in random_decisions.items()
        },
        "frozen_primary_policy": primary_policy.to_dict(),
        "frozen_backup_policy": (
            backup_policy.to_dict() if backup_policy is not None else None
        ),
        "frozen_risk_label": candidate_decision.primary["risk_label"],
        "selected_primary_design_risk_frontier": primary_risk_frontier,
        "no_retuning_after_heldout": True,
        "held_out_evaluation_complete": False,
    }
    public["selection_freeze_hash"] = stable_hash(public)
    base = f"rotations/{fold.fold_id}"
    writer.write_json(f"{base}/selection_manifest.json", public)
    writer.write_json(
        f"{base}/candidate_compilation_audit.json", compilation.audit
    )
    writer.write_jsonl_batch(
        f"{base}/design_candidate_metrics.jsonl", candidate_records
    )
    writer.write_jsonl_batch(
        f"{base}/design_parent_metrics.jsonl",
        [_selector_record(row) for row in parent_metrics],
    )
    writer.write_json(
        f"{base}/design_baseline_metrics.json",
        {
            "equal_risk": [_selector_record(row) for row in metrics_by_policy[equal_policy.policy_id]],
            "random_selection": {
                str(seed): [
                    _selector_record(row)
                    for row in metrics_by_policy[policy.policy_id]
                ]
                for seed, policy in random_policies.items()
            },
        },
    )
    return {
        "fold": fold,
        "selector_admissible": selector_admissible,
        "public_freeze": public,
        "primary_policy": primary_policy,
        "primary_risk": str(candidate_decision.primary["risk_label"]),
        "primary_risk_frontier": primary_risk_frontier,
        "backup_policy": backup_policy,
        "backup_risk": (
            str(candidate_decision.backup["risk_label"])
            if candidate_decision.backup is not None
            else None
        ),
        "parent_policy": policy_lookup[str(parent_decision.primary["policy_id"])],
        "parent_risk": str(parent_decision.primary["risk_label"]),
        "equal_policy": equal_policy,
        "equal_risk": str(equal_decision.primary["risk_label"]),
        "random_policies": random_policies,
        "random_risks": {
            seed: str(decision.primary["risk_label"])
            for seed, decision in random_decisions.items()
        },
    }


def _evaluate_design_policy_bank(
    policies: Sequence[Any], *, fold: OuterFold, worker_count: int
) -> list[dict[str, Any]]:
    tasks = [
        (policy, tier.label, fold.design_block_ids, fold.held_out_block_id)
        for policy in policies
        for tier in FROZEN_RISK_TIERS
    ]
    if worker_count != 3:
        raise NestedSelectorSprintError("selector compute allocation drift")
    context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=3, mp_context=context) as pool:
        rows = list(pool.map(_design_worker, tasks, chunksize=2))
    return sorted(rows, key=lambda row: str(row["variant_id"]))


def _design_worker(task: tuple[Any, str, tuple[str, ...], str]) -> dict[str, Any]:
    policy, risk_label, design_ids, heldout_id = task
    if not _WORKER_RUNTIMES or not _WORKER_BLOCKS:
        raise RuntimeError("selector worker has no immutable fork state")
    block_metrics = [
        evaluate_policy_block(
            policy,
            _WORKER_RUNTIMES,
            risk_level=risk_label,
            block_id=block_id,
            session_days=_WORKER_BLOCKS[block_id].session_days,
            start_days=_WORKER_BLOCKS[block_id].episode_start_days,
            include_time_to_combine=False,
        )
        for block_id in design_ids
    ]
    return aggregate_design_block_metrics(
        block_metrics,
        allowed_block_ids=design_ids,
        heldout_block_id=heldout_id,
    )


def _selector_record(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "variant_id": str(value["variant_id"]),
        "policy_id": str(value["policy_id"]),
        "component_ids": list(value["component_ids"]),
        "risk_label": str(value["risk_label"]),
        "micro_risk_units": int(value["micro_risk_units"]),
        "design_block_ids": list(value["design_block_ids"]),
        "heldout_block_id": value["heldout_block_id"],
        "design_behavior_fingerprint": str(
            value["design_behavior_fingerprint"]
        ),
        "normal_net_usd": float(value["normal_net_usd"]),
        "stressed_net_usd": float(value["stressed_net_usd"]),
        "mll_breach_rate": float(value["mll_breach_rate"]),
        "hard_issue_count": int(value["hard_issue_count"]),
        "maximum_component_profit_share": float(
            value["maximum_component_profit_share"]
        ),
        "stressed_combine_pass_count": int(value["stress_pass_count"]),
        "normal_combine_pass_count": int(value["normal_pass_count"]),
        "stressed_median_target_progress": float(
            value["stressed_target_progress_median"]
        ),
        "lower_quartile_target_progress": float(
            value["stressed_target_progress_p25"]
        ),
        "stressed_net_pnl": float(value["stressed_net_usd"]),
        "consistency": float(value["consistency_pass_rate"]),
        "component_concentration": float(
            value["maximum_component_profit_share"]
        ),
        "temporal_block_concentration": float(
            value["maximum_block_profit_share"]
        ),
        "operational_simplicity": float(
            -(10 * int(value["operational_complexity"]) + int(value["micro_risk_units"]))
        ),
        "normal_median_net_usd": float(value["normal_median_net_usd"]),
        "stressed_median_net_usd": float(value["stressed_median_net_usd"]),
        "positive_temporal_block_count": int(
            value["positive_temporal_block_count"]
        ),
        "episode_count": int(value["episode_count"]),
    }


def _select(records: Sequence[Mapping[str, Any]]) -> SelectionDecision:
    return select_pareto_champion(
        records,
        objectives=[
            ParetoObjective(
                field=metric,
                direction="maximize" if direction == "MAXIMIZE" else "minimize",
            )
            for metric, direction in PARETO_OBJECTIVES
        ],
        maximum_mll_breach_rate=MAXIMUM_MLL_BREACH_RATE,
        maximum_component_profit_share=MAXIMUM_COMPONENT_PROFIT_SHARE,
        backup_maximum_component_jaccard=BACKUP_MAXIMUM_COMPONENT_JACCARD,
    )


def _select_baseline(records: Sequence[Mapping[str, Any]]) -> SelectionDecision:
    """Choose baseline risk in-design even when no baseline clears champion gates."""

    try:
        return _select(records)
    except SelectorError:
        if not records:
            raise
        objectives = [
            ParetoObjective(
                field=metric,
                direction="maximize" if direction == "MAXIMIZE" else "minimize",
            )
            for metric, direction in PARETO_OBJECTIVES
        ]

        def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
            values: list[Any] = []
            for objective in objectives:
                value = float(row[objective.field])
                values.append(
                    -value if objective.direction == "maximize" else value
                )
            values.append(str(row["variant_id"]))
            return tuple(values)

        ordered = sorted((dict(row) for row in records), key=key)
        primary = ordered[0]
        return SelectionDecision(
            primary=primary,
            backup=None,
            eligible_count=0,
            hard_rejection_count=len(ordered),
            behavioral_clone_rejection_count=0,
            pareto_frontier_count=1,
            pareto_frontier_ids=(str(primary["variant_id"]),),
            hard_rejections=tuple(
                {
                    "variant_id": str(row["variant_id"]),
                    "reasons": ["BASELINE_DID_NOT_CLEAR_CHAMPION_HARD_GATES"],
                }
                for row in ordered
            ),
            clone_groups=(),
            deterministic_order=tuple(
                str(row["variant_id"]) for row in ordered
            ),
        )


def _component_design_statistics(
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    blocks: Mapping[str, TemporalBlock],
    design_block_ids: Sequence[str],
) -> dict[str, dict[str, Any]]:
    block_ids = tuple(str(value) for value in design_block_ids)
    if not block_ids or len(set(block_ids)) != len(block_ids):
        raise NestedSelectorSprintError("component design blocks are invalid")
    if any(block_id not in blocks for block_id in block_ids):
        raise NestedSelectorSprintError("component design block escaped temporal plan")
    output: dict[str, dict[str, Any]] = {}
    for component_id, runtime in sorted(runtimes.items()):
        design_blocks: dict[str, dict[str, Any]] = {}
        for block_id in block_ids:
            allowed = set(int(value) for value in blocks[block_id].session_days)
            events = [
                row for row in runtime.events if row.event.session_day in allowed
            ]
            design_blocks[block_id] = {
                "event_count": len(events),
                "normal_net_usd": sum(
                    float(row.event.net_pnl) for row in events
                ),
                "stressed_net_usd": sum(
                    float(
                        _restress_routed_trade(
                            row, cost_stress=1.5
                        ).event.net_pnl
                    )
                    for row in events
                ),
            }
        output[component_id] = {
            "component_id": component_id,
            "event_count": sum(
                int(value["event_count"]) for value in design_blocks.values()
            ),
            "normal_net_usd": sum(
                float(value["normal_net_usd"])
                for value in design_blocks.values()
            ),
            "stressed_net_usd": sum(
                float(value["stressed_net_usd"])
                for value in design_blocks.values()
            ),
            "signal_market": runtime.signal_market,
            "execution_market": runtime.execution_market,
            "role": runtime.role.value,
            "design_blocks": design_blocks,
        }
    return output


def _eligible_component_ids(
    component_stats: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    rows = [
        value
        for value in component_stats.values()
        if int(value["event_count"]) > 0
        and float(value["normal_net_usd"]) > 0.0
        and float(value["stressed_net_usd"]) > 0.0
    ]
    rows.sort(
        key=lambda row: (
            -float(row["stressed_net_usd"]),
            -float(row["normal_net_usd"]),
            -int(row["event_count"]),
            str(row["component_id"]),
        )
    )
    return [str(row["component_id"]) for row in rows]


def _synthetic_baseline_policy(
    template: StaticParentBasketPolicy,
    *,
    membership: Sequence[str],
    label: str,
) -> StaticParentBasketPolicy:
    components = tuple(str(value) for value in membership)
    identity = stable_hash({"label": label, "components": components})[:24]
    return replace(
        template,
        policy_id=f"v73_baseline_{identity}",
        component_ids=components,
        retained_added_sleeve_id=components[0],
        exact_change=(("baseline_label", label),),
        expected_effect="Deterministic nested-selector baseline only.",
        assembly_profile=f"CONSENSUS_BASELINE_{len(components)}",
        inherited_status=None,
    )


def _evaluate_all_held_out(
    frozen_folds: Sequence[Mapping[str, Any]],
    *,
    diagnostic_policy: StaticParentBasketPolicy,
    worker_count: int,
    writer: AtomicResultWriter,
) -> list[dict[str, Any]]:
    tasks: list[tuple[str, str, str, Any, str, int | None]] = []
    for frozen in frozen_folds:
        fold: OuterFold = frozen["fold"]
        tasks.extend(
            [
                (
                    "selector",
                    fold.fold_id,
                    fold.held_out_block_id,
                    frozen["primary_policy"],
                    frozen["primary_risk"],
                    None,
                ),
                (
                    "best_parent",
                    fold.fold_id,
                    fold.held_out_block_id,
                    frozen["parent_policy"],
                    frozen["parent_risk"],
                    None,
                ),
                (
                    "equal_risk",
                    fold.fold_id,
                    fold.held_out_block_id,
                    frozen["equal_policy"],
                    frozen["equal_risk"],
                    None,
                ),
            ]
        )
        for seed in RANDOM_BASELINE_SEEDS:
            tasks.append(
                (
                    "random_selection",
                    fold.fold_id,
                    fold.held_out_block_id,
                    frozen["random_policies"][seed],
                    frozen["random_risks"][seed],
                    seed,
                )
            )
    if worker_count != 3:
        raise NestedSelectorSprintError("held-out compute allocation drift")
    context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=3, mp_context=context) as pool:
        evaluated = list(pool.map(_heldout_worker, tasks, chunksize=1))
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in evaluated:
        grouped.setdefault(str(row["fold_id"]), []).append(row)

    output: list[dict[str, Any]] = []
    for frozen in frozen_folds:
        fold: OuterFold = frozen["fold"]
        rows = grouped[fold.fold_id]
        selector = next(row for row in rows if row["label"] == "selector")
        parent = next(row for row in rows if row["label"] == "best_parent")
        equal = next(row for row in rows if row["label"] == "equal_risk")
        random_rows = sorted(
            (row for row in rows if row["label"] == "random_selection"),
            key=lambda row: int(row["seed"]),
        )
        block = _WORKER_BLOCKS[fold.held_out_block_id]
        diagnostic = _evaluate_development_selected_reference(
            diagnostic_policy, block=block
        )
        decision_evidence = {
            "block_id": fold.held_out_block_id,
            "selector": _decision_policy_row(selector["metrics"]),
            "best_parent": _decision_policy_row(parent["metrics"]),
            "equal_risk": _decision_policy_row(equal["metrics"]),
            "random_selection": [
                {
                    **_decision_policy_row(row["metrics"]),
                    "seed": int(row["seed"]),
                }
                for row in random_rows
            ],
        }
        evidence = {
            "schema": "hydra_v73_outer_fold_held_out_evidence_v1",
            "fold": fold.to_dict(),
            "selection_freeze_hash": frozen["public_freeze"][
                "selection_freeze_hash"
            ],
            "selector": selector,
            "best_parent": parent,
            "equal_risk": equal,
            "random_selection": random_rows,
            "development_selected_diagnostic_reference": diagnostic,
            "diagnostic_reference_is_independent_evidence": False,
            "headline_evidence_held_out_only": True,
            "block_is_only_independent_inference_unit": True,
            "within_block_starts_are_dependent": True,
            "decision_evidence": decision_evidence,
            "retuning_after_held_out": False,
        }
        evidence["evidence_hash"] = stable_hash(evidence)
        writer.write_json(
            f"rotations/{fold.fold_id}/held_out_evidence.json", evidence
        )
        output.append(evidence)
    return output


def _heldout_worker(
    task: tuple[str, str, str, Any, str, int | None]
) -> dict[str, Any]:
    label, fold_id, block_id, policy, risk_label, seed = task
    if not _WORKER_RUNTIMES or block_id not in _WORKER_BLOCKS:
        raise RuntimeError("held-out worker has no immutable fork state")
    block = _WORKER_BLOCKS[block_id]
    metrics = evaluate_policy_block(
        policy,
        _WORKER_RUNTIMES,
        risk_level=risk_label,
        block_id=block_id,
        session_days=block.session_days,
        start_days=block.episode_start_days,
        include_time_to_combine=True,
    )
    return {
        "label": label,
        "fold_id": fold_id,
        "block_id": block_id,
        "seed": seed,
        "policy_id": policy.policy_id,
        "risk_label": risk_label,
        "metrics": metrics,
        "held_out_evaluated_once": True,
        "retuned": False,
    }


def _decision_policy_row(metrics: Mapping[str, Any]) -> dict[str, Any]:
    row = {
        "normal_net_usd": float(metrics["normal_net_usd"]),
        "stressed_net_usd": float(metrics["stressed_net_usd"]),
        "stressed_target_progress": float(metrics["stressed_target_progress"]),
        "stressed_pass_count": int(metrics["stressed_pass_count"]),
        "episode_count": int(metrics["episode_count"]),
    }
    if "mll_breach_count" in metrics:
        row.update(
            {
                "mll_breach_count": int(metrics["mll_breach_count"]),
                "consistency": float(metrics["consistency"]),
                "maximum_component_profit_share": float(
                    metrics["maximum_component_profit_share"]
                ),
            }
        )
    return row


def _evaluate_development_selected_reference(
    policy: StaticParentBasketPolicy, *, block: TemporalBlock
) -> dict[str, Any]:
    selected = [
        _WORKER_RUNTIMES[component_id] for component_id in policy.component_ids
    ]
    allowed = set(block.session_days)
    normal_events = {
        runtime.sleeve_id: tuple(
            row for row in runtime.events if row.event.session_day in allowed
        )
        for runtime in selected
    }
    stressed_events = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in normal_events.items()
    }
    basket = ComplementarySleeveBasketPolicy(
        policy_id=policy.basket_policy_id,
        component_ids=policy.component_ids,
        archetype="DEVELOPMENT_SELECTED_DIAGNOSTIC_REFERENCE_ONLY",
        maximum_simultaneous_positions=policy.maximum_simultaneous_positions,
        maximum_mini_equivalent=policy.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=policy.component_ids,
        policy_version=STATIC_PARENT_BASKET_POLICY_VERSION,
    )
    with _static_parent_router_context():
        normal = evaluate_time_to_combine(
            normal_events,
            block.session_days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            start_days=block.episode_start_days,
            block_id=block.block_id,
        )
    with _static_parent_router_context():
        stressed = evaluate_time_to_combine(
            stressed_events,
            block.session_days,
            basket=basket,  # type: ignore[arg-type]
            controller=policy,  # type: ignore[arg-type]
            start_days=block.episode_start_days,
            block_id=block.block_id,
        )
    return {
        "policy_id": policy.policy_id,
        "status": "DEVELOPMENT_SELECTED_DIAGNOSTIC_CHAMPION",
        "independent_evidence": False,
        "excluded_from_selector_decision": True,
        "normal": {key: value.to_dict() for key, value in normal.items()},
        "stress_1_5x": {
            key: value.to_dict() for key, value in stressed.items()
        },
    }


@contextmanager
def _static_parent_router_context() -> Iterator[None]:
    def route(intent: Any, state: Any, *, policy: StaticParentBasketPolicy) -> Any:
        return route_static_parent_basket_entry(intent, state, policy=policy)

    prior = basket_engine.route_entry
    basket_engine.route_entry = route  # type: ignore[assignment]
    try:
        yield
    finally:
        basket_engine.route_entry = prior


def _component_version_manifest(
    policy: StaticParentBasketPolicy,
    ledger: RecoveredCampaignLedger,
) -> dict[str, dict[str, Any]]:
    """Freeze every executable component version referenced by a finalist."""

    specs = dict(ledger.manifest.get("component_specs") or {})
    output: dict[str, dict[str, Any]] = {}
    for component_id in policy.component_ids:
        runtime = ledger.runtimes.get(component_id)
        spec = specs.get(component_id)
        if runtime is None or not isinstance(spec, Mapping):
            raise NestedSelectorSprintError(
                f"finalist component provenance missing: {component_id}"
            )
        runtime_metadata = runtime.to_dict(include_events=False)
        output[component_id] = {
            "specification_hash": runtime.specification_hash,
            "component_spec_hash": stable_hash(spec),
            "runtime_metadata_hash": stable_hash(runtime_metadata),
            "source_campaign": runtime.source_campaign,
            "signal_market": runtime.signal_market,
            "execution_market": runtime.execution_market,
            "economic_role": runtime.role.value,
            "exit_implementation": runtime.exit_implementation,
            "event_count": runtime.event_count,
            "shared_recovered_ledger_sha256": ledger.ledger_sha256,
        }
    return output


def _conditional_final_development_selection(
    *,
    decision_status: str,
    plan: TemporalBlockPlan,
    ledger: RecoveredCampaignLedger,
    parents: Mapping[str, Any],
    prereg: Mapping[str, Any],
    selector_manifest: Mapping[str, Any],
    worker_count: int,
    writer: AtomicResultWriter,
) -> dict[str, Any]:
    if decision_status != SELECTOR_PROCEDURE_GREEN:
        return {
            "status": "NOT_APPLICABLE_SELECTOR_NOT_GREEN",
            "selector_status": decision_status,
            "final_development_champion": None,
            "backup": None,
            "promoted_to_96_starts": [],
            "round_b": {
                "status": "NOT_RUN",
                "reason": "Selector procedure was not GREEN.",
            },
            "paper_shadow_ready_count": 0,
        }

    all_block_ids = tuple(block.block_id for block in plan.blocks)
    stats = _component_design_statistics(
        ledger.runtimes,
        blocks={block.block_id: block for block in plan.blocks},
        design_block_ids=all_block_ids,
    )
    compilation = compile_fold_candidate_bank(
        component_design_stats=stats,
        parents=tuple(parents.values()),
        design_block_ids=all_block_ids,
        compiler_policy=frozen_compiler_policy(),
    )
    candidates = list(compilation.policies)
    synthetic_fold = OuterFold(
        fold_id="V73_ALL_DEVELOPMENT",
        design_block_ids=all_block_ids,
        held_out_block_id="NO_HELD_OUT_ALL_DEVELOPMENT",
        source_plan_hash=plan.plan_hash,
        fold_hash=stable_hash(
            {
                "design_block_ids": [block.block_id for block in plan.blocks],
                "role": "FINAL_DEVELOPMENT_SELECTION_AFTER_GREEN_PROCEDURE",
            }
        ),
    )
    metrics = _evaluate_design_policy_bank(
        candidates, fold=synthetic_fold, worker_count=worker_count
    )
    decision = _select([_selector_record(row) for row in metrics])
    candidate_lookup = {policy.policy_id: policy for policy in candidates}
    primary = candidate_lookup[str(decision.primary["policy_id"])]
    backup = (
        candidate_lookup[str(decision.backup["policy_id"])]
        if decision.backup is not None
        else None
    )
    writer.write_json(
        "final_development_candidate_compilation_audit.json", compilation.audit
    )
    immutable_manifest = {
        "schema": "hydra_v73_final_development_champion_manifest_v1",
        "selection_role": "DEVELOPMENT_SELECTED_NOT_INDEPENDENTLY_CONFIRMED",
        "selector_status": decision_status,
        "selector_manifest_hash": selector_manifest["manifest_hash"],
        "preregistration_hash": prereg["preregistration_hash"],
        "recovered_component_ledger_sha256": ledger.ledger_sha256,
        "recovered_component_ledger_manifest_sha256": file_sha256(
            ledger.manifest_path
        ),
        "candidate_compilation_audit_hash": compilation.audit["audit_hash"],
        "account_simulator_implementation_hashes": {
            key: value
            for key, value in prereg["implementation_files"].items()
            if key
            in {
                "hydra/account_policy/basket.py",
                "hydra/economic_evolution/account_evaluation.py",
                "hydra/propfirm/censored_combine.py",
                "hydra/propfirm/topstep_150k.py",
                "hydra/selection/risk_frontier.py",
                "hydra/selection/time_to_combine.py",
            }
        },
        "selector_decision": decision.to_dict(),
        "primary": {
            "policy": primary.to_dict(),
            "component_versions": _component_version_manifest(primary, ledger),
            "risk_label": str(decision.primary["risk_label"]),
            "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
            "cost_scenarios": [1.0, 1.5],
            "decision_policy": "FROZEN_V73_GREEN_CONFIRMATION_POLICY",
        },
        "backup": (
            {
                "policy": backup.to_dict(),
                "component_versions": _component_version_manifest(backup, ledger),
                "risk_label": str(decision.backup["risk_label"]),
                "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
                "cost_scenarios": [1.0, 1.5],
                "decision_policy": "FROZEN_V73_GREEN_CONFIRMATION_POLICY",
            }
            if backup is not None and decision.backup is not None
            else None
        ),
        "validated": False,
        "independently_confirmed": False,
        "paper_shadow_ready": False,
    }
    immutable_manifest["manifest_hash"] = stable_hash(immutable_manifest)
    writer.write_json("final_development_champion_manifest.json", immutable_manifest)
    writer.write_jsonl_batch(
        "final_development_design_metrics.jsonl",
        [_selector_record(row) for row in metrics],
    )

    finalists: list[tuple[StaticParentBasketPolicy, str, str]] = [
        (primary, str(decision.primary["risk_label"]), "PRIMARY")
    ]
    if backup is not None and decision.backup is not None:
        finalists.append((backup, str(decision.backup["risk_label"]), "BACKUP"))
    round_a = _run_round_a_96(finalists, plan=plan, worker_count=worker_count)
    writer.write_json("round_a_96_start_evidence.json", round_a)
    confirmation_ready = [
        row["policy_id"]
        for row in round_a["candidates"]
        if row["status"] == "BASKET_CONFIRMATION_READY"
    ]
    result = {
        "status": "FINAL_DEVELOPMENT_SELECTION_COMPLETE",
        "selector_status": decision_status,
        "manifest": immutable_manifest,
        "final_development_champion": primary.policy_id,
        "backup": backup.policy_id if backup is not None else None,
        "promoted_to_96_starts": [row[0].policy_id for row in finalists],
        "round_a": round_a,
        "basket_confirmation_ready": confirmation_ready,
        "round_b": {
            "status": "NOT_RUN",
            "reason": (
                "No additional independent temporal blocks remain after the four "
                "maximum-defensible development blocks; adjacent-day splitting is forbidden."
            ),
        },
        "paper_shadow_ready_count": 0,
        "independent_confirmation_requested": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
    }
    writer.write_json("final_development_selection.json", result)
    return result


def _run_round_a_96(
    finalists: Sequence[tuple[StaticParentBasketPolicy, str, str]],
    *,
    plan: TemporalBlockPlan,
    worker_count: int,
) -> dict[str, Any]:
    tasks = [
        (policy, risk, role, block.block_id, tuple(block.session_days[:24]))
        for policy, risk, role in finalists
        for block in plan.blocks
    ]
    context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(max_workers=worker_count, mp_context=context) as pool:
        rows = list(pool.map(_expanded_worker, tasks, chunksize=1))
    by_policy: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_policy.setdefault(str(row["policy_id"]), []).append(row)
    candidates = [
        _round_a_candidate_summary(policy_rows)
        for _, policy_rows in sorted(by_policy.items())
    ]
    return {
        "schema": "hydra_v73_round_a_96_start_evidence_v1",
        "start_count_per_candidate": 96,
        "starts_per_block": 24,
        "block_count": 4,
        "within_block_starts_independent": False,
        "inference_unit": "TEMPORAL_BLOCK",
        "normal_and_stressed_costs": True,
        "multiple_horizons": [20, 40, 60, 90, "full"],
        "retuning": False,
        "candidates": candidates,
    }


def _expanded_worker(
    task: tuple[StaticParentBasketPolicy, str, str, str, tuple[int, ...]]
) -> dict[str, Any]:
    policy, risk, role, block_id, starts = task
    block = _WORKER_BLOCKS[block_id]
    return {
        "policy_id": policy.policy_id,
        "role": role,
        "block_id": block_id,
        "risk_label": risk,
        "metrics": evaluate_policy_block(
            policy,
            _WORKER_RUNTIMES,
            risk_level=risk,
            block_id=block_id,
            session_days=block.session_days,
            start_days=starts,
            include_time_to_combine=True,
        ),
    }


def _round_a_candidate_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = [row["metrics"] for row in rows]
    episodes = sum(int(row["episode_count"]) for row in metrics)
    normal_passes = sum(int(row["normal_pass_count"]) for row in metrics)
    stressed_passes = sum(int(row["stress_pass_count"]) for row in metrics)
    stress_mll = sum(int(row["mll_breach_count"]) for row in metrics)
    stressed_net = sum(float(row["stressed_net_usd"]) for row in metrics)
    consistency = sum(
        float(row["stressed_consistency_pass_rate"]) * int(row["episode_count"])
        for row in metrics
    ) / episodes
    pass_blocks = sum(int(row["stress_pass_count"]) > 0 for row in metrics)
    max_block_pass_share = (
        max(int(row["stress_pass_count"]) for row in metrics) / stressed_passes
        if stressed_passes
        else 0.0
    )
    max_component = max(
        float(row["maximum_component_profit_share"]) for row in metrics
    )
    normal_40 = [
        row["time_to_combine"]["normal"][str(PRIMARY_HORIZON_DAYS)]
        for row in metrics
    ]
    stressed_40 = [
        row["time_to_combine"]["stress_1_5x"][str(PRIMARY_HORIZON_DAYS)]
        for row in metrics
    ]
    normal_data_censored = sum(
        int(row["data_censored_count"]) for row in normal_40
    )
    stressed_data_censored = sum(
        int(row["data_censored_count"]) for row in stressed_40
    )
    complete_primary_follow_up = (
        normal_data_censored == 0 and stressed_data_censored == 0
    )
    normal_lower = normal_passes / episodes
    normal_upper = (normal_passes + normal_data_censored) / episodes
    stressed_lower = stressed_passes / episodes
    stressed_upper = (stressed_passes + stressed_data_censored) / episodes
    checks = {
        "complete_40_day_follow_up_for_every_start": complete_primary_follow_up,
        "normal_pass_rate_at_least_10pct": (
            normal_lower >= 0.10 if complete_primary_follow_up else None
        ),
        "stressed_pass_rate_at_least_5pct": (
            stressed_lower >= 0.05 if complete_primary_follow_up else None
        ),
        "positive_stressed_net": stressed_net > 0.0,
        "mll_breach_at_most_10pct": stress_mll / episodes <= 0.10,
        "passes_in_at_least_two_blocks": pass_blocks >= 2,
        "no_block_domination": max_block_pass_share <= 0.50,
        "no_component_domination": max_component <= MAXIMUM_COMPONENT_PROFIT_SHARE,
        "acceptable_consistency": consistency >= 0.75,
    }
    ready = complete_primary_follow_up and all(value is True for value in checks.values())
    return {
        "policy_id": rows[0]["policy_id"],
        "role": rows[0]["role"],
        "risk_label": rows[0]["risk_label"],
        "episode_count": episodes,
        "normal_pass_count": normal_passes,
        "normal_pass_rate": normal_lower,
        "normal_pass_probability_lower_bound": normal_lower,
        "normal_pass_probability_upper_bound": normal_upper,
        "stressed_pass_count": stressed_passes,
        "stressed_pass_rate": stressed_lower,
        "stressed_pass_probability_lower_bound": stressed_lower,
        "stressed_pass_probability_upper_bound": stressed_upper,
        "normal_40_day_data_censored_count": normal_data_censored,
        "stressed_40_day_data_censored_count": stressed_data_censored,
        "censored_survivors_counted_as_failures": False,
        "stressed_net_usd": stressed_net,
        "stressed_mll_breach_count": stress_mll,
        "stressed_mll_breach_rate": stress_mll / episodes,
        "stressed_consistency": consistency,
        "pass_block_count": pass_blocks,
        "maximum_block_pass_share": max_block_pass_share,
        "maximum_component_profit_share": max_component,
        "checks": checks,
        "status": (
            "BASKET_CONFIRMATION_READY"
            if ready
            else (
                "ROUND_A_CENSORING_PRECLUDES_CONFIRMATION_READY"
                if not complete_primary_follow_up
                else "ROUND_A_DEVELOPMENT_CRITERIA_NOT_MET"
            )
        ),
        "blocks": list(rows),
    }


def _mission_campaign_terminal_event(root: Path) -> dict[str, Any]:
    path = root / "mission/state/hydra_mission.db"
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            """
            SELECT id, event_type, payload, created_at
            FROM events
            WHERE json_extract(payload, '$.current.manifest_campaign_id') = ?
              AND json_extract(payload, '$.current.manifest_campaign_state') = 'COMPLETE'
            ORDER BY id DESC
            LIMIT 1
            """,
            (CAMPAIGN_0023_ID,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise NestedSelectorSprintError(
            "campaign 0023 terminal event is absent from mission DB"
        )
    payload = json.loads(str(row[2]))
    current = dict(payload.get("current") or {})
    required = {
        "manifest_campaign_id",
        "manifest_campaign_state",
        "manifest_campaign_terminal_state",
        "manifest_campaign_parameter_rescue_allowed",
        "manifest_campaign_same_class_relaunch_allowed",
    }
    if not required.issubset(current):
        raise NestedSelectorSprintError(
            "campaign 0023 mission DB terminal payload is incomplete"
        )
    return {
        "event_id": int(row[0]),
        "event_type": str(row[1]),
        "created_at": str(row[3]),
        **{key: current[key] for key in sorted(required)},
    }


def _audit_campaign_0023(source_paths: Mapping[str, Path]) -> dict[str, Any]:
    proposals = _read_jsonl(source_paths["proposals"])
    pairs = _read_jsonl(source_paths["pair_results"])
    exact = _read_jsonl(source_paths["exact_metadata"])
    result = _load_json(source_paths["result"])
    tripwire = _load_json(source_paths["tripwire"])
    terminal_receipt = source_paths["campaign_dir"] / "graveyard_append_receipt.json"
    receipt = _load_json(terminal_receipt)
    root = _project_root(source_paths["campaign_dir"])
    mission_terminal = _mission_campaign_terminal_event(root)
    normal = [row["real_evaluation"]["controlled_base"] for row in pairs]
    stressed = [
        row["real_evaluation"]["controlled_stress_1_5x"] for row in pairs
    ]
    behavior_count = len({str(row["behavioral_fingerprint"]) for row in pairs})
    diagnostic_pair = next(
        row for row in pairs if row["real_policy"]["policy_id"] == DIAGNOSTIC_CHAMPION_ID
    )
    diagnostic_normal = diagnostic_pair["real_evaluation"]["controlled_base"]
    diagnostic_stress = diagnostic_pair["real_evaluation"][
        "controlled_stress_1_5x"
    ]
    verified = {
        "static_baskets_proposed": len(proposals),
        "baskets_replayed_exactly": len(pairs),
        "behaviorally_distinct_account_policies": behavior_count,
        "normal_episode_count": sum(int(row["episode_start_count"]) for row in normal),
        "stressed_episode_count": sum(
            int(row["episode_start_count"]) for row in stressed
        ),
        "normal_positive_policy_count": sum(
            float(row["median_episode_net_pnl"]) > 0.0 for row in normal
        ),
        "stressed_positive_policy_count": sum(
            float(row["median_episode_net_pnl"]) > 0.0 for row in stressed
        ),
        "normal_policy_with_pass_count": sum(int(row["pass_count"]) > 0 for row in normal),
        "stressed_policy_with_pass_count": sum(
            int(row["pass_count"]) > 0 for row in stressed
        ),
        "maximum_normal_pass_rate": max(float(row["pass_rate"]) for row in normal),
        "maximum_stressed_pass_rate": max(
            float(row["pass_rate"]) for row in stressed
        ),
        "median_normal_pass_rate": statistics.median(
            float(row["pass_rate"]) for row in normal
        ),
        "median_stressed_pass_rate": statistics.median(
            float(row["pass_rate"]) for row in stressed
        ),
        "median_normal_target_progress": statistics.median(
            float(row["target_progress_median"]) for row in normal
        ),
        "median_stressed_target_progress": statistics.median(
            float(row["target_progress_median"]) for row in stressed
        ),
        "maximum_normal_target_progress": max(
            float(row["maximum_target_progress"]) for row in normal
        ),
        "median_normal_net_usd": statistics.median(
            float(row["median_episode_net_pnl"]) for row in normal
        ),
        "median_stressed_net_usd": statistics.median(
            float(row["median_episode_net_pnl"]) for row in stressed
        ),
        "maximum_observed_mll_breach_rate": max(
            [float(row["mll_breach_rate"]) for row in normal]
            + [float(row["mll_breach_rate"]) for row in stressed]
        ),
        "real_policy_win_count": int(tripwire["real_win_count"]),
        "matched_control_win_count": int(tripwire["matched_control_win_count"]),
        "tie_count": int(tripwire["tie_count"]),
        "family_NULL_RATIO": float(tripwire["NULL_RATIO"]),
        "family_exact_one_sided_p_value": float(
            tripwire["exact_one_sided_binomial_p_value"]
        ),
        "median_stressed_paired_delta_usd": float(
            tripwire["median_stressed_net_delta_usd"]
        ),
        "median_stressed_target_progress_delta": float(
            tripwire["median_stressed_target_progress_delta"]
        ),
        "source_scientific_status": str(result["scientific_status"]),
        "source_report_verdict": str(result["report_verdict"]),
        "source_family_green": bool(result["family_tripwire"]["family_green"]),
        "source_tripwire_verdict": str(result["family_tripwire"]["verdict"]),
        "source_thresholds_changed_after_outcome": bool(
            result["family_tripwire"]["thresholds_changed_after_outcome"]
        ),
        "source_pre_holdout_ready_count": int(result["pre_holdout_ready_count"]),
        "source_account_research_candidate_count": int(
            result["account_research_candidate_count"]
        ),
        "source_validated": bool(result["validated"]),
        "family_verdict": "STATIC_PARENT_SYNTHESIS_FALSIFIED",
        "promotion_to_96_start_count": int(result["pre_holdout_ready_count"]),
        "paper_shadow_ready_count": int(result["paper_shadow_ready_count"]),
        "exact_component_metadata_count": len(exact),
        "graveyard_receipt_campaign_id": str(receipt["campaign_id"]),
        "graveyard_receipt_mechanism_class": str(receipt["mechanism_class"]),
        "graveyard_receipt_candidate_count": int(receipt["candidate_count"]),
        "graveyard_receipt_evidence_sha256": str(receipt["evidence_sha256"]),
        "mission_db_terminal_state": str(
            mission_terminal["manifest_campaign_terminal_state"]
        ),
        "mission_db_campaign_state": str(
            mission_terminal["manifest_campaign_state"]
        ),
        "mission_db_parameter_rescue_allowed": bool(
            mission_terminal["manifest_campaign_parameter_rescue_allowed"]
        ),
        "mission_db_same_class_relaunch_allowed": bool(
            mission_terminal["manifest_campaign_same_class_relaunch_allowed"]
        ),
    }
    expected: dict[str, Any] = {
        "static_baskets_proposed": 512,
        "baskets_replayed_exactly": 128,
        "behaviorally_distinct_account_policies": 80,
        "normal_episode_count": 6144,
        "stressed_episode_count": 6144,
        "normal_positive_policy_count": 128,
        "stressed_positive_policy_count": 128,
        "normal_policy_with_pass_count": 89,
        "stressed_policy_with_pass_count": 74,
        "maximum_normal_pass_rate": 9 / 48,
        "maximum_stressed_pass_rate": 9 / 48,
        "median_normal_pass_rate": 2 / 48,
        "median_stressed_pass_rate": 1 / 48,
        "median_normal_target_progress": 0.6654465277777589,
        "median_stressed_target_progress": 0.6066582638888934,
        "maximum_normal_target_progress": 1.111525555555575,
        "median_normal_net_usd": 5989.018749999825,
        "median_stressed_net_usd": 5459.924375001916,
        "maximum_observed_mll_breach_rate": 0.0,
        "real_policy_win_count": 49,
        "matched_control_win_count": 34,
        "tie_count": 45,
        "family_NULL_RATIO": 34 / 49,
        "family_exact_one_sided_p_value": 0.06192652972967916,
        "median_stressed_paired_delta_usd": -6.468749999854481,
        "median_stressed_target_progress_delta": -0.0007187499999835598,
        "source_scientific_status": "ARTEFACT_GEOMETRY_ONLY",
        "source_report_verdict": "ARTEFACT",
        "source_family_green": False,
        "source_tripwire_verdict": "ARTEFACT_GEOMETRY_ONLY",
        "source_thresholds_changed_after_outcome": False,
        "source_pre_holdout_ready_count": 0,
        "source_account_research_candidate_count": 0,
        "source_validated": False,
        "promotion_to_96_start_count": 0,
        "paper_shadow_ready_count": 0,
        "exact_component_metadata_count": 48,
        "graveyard_receipt_campaign_id": CAMPAIGN_0023_ID,
        "graveyard_receipt_mechanism_class": (
            "FROZEN_PARENT_STATIC_ACCOUNT_SYNTHESIS_V1"
        ),
        "graveyard_receipt_candidate_count": 128,
        "graveyard_receipt_evidence_sha256": file_sha256(source_paths["result"]),
        "mission_db_terminal_state": "EXACT_CLASS_TOMBSTONED",
        "mission_db_campaign_state": "COMPLETE",
        "mission_db_parameter_rescue_allowed": False,
        "mission_db_same_class_relaunch_allowed": False,
    }
    checks: dict[str, bool] = {}
    for key, wanted in expected.items():
        observed = verified[key]
        checks[key] = (
            observed == wanted
            if type(wanted) in {bool, int, str}
            else abs(float(observed) - float(wanted)) <= 1e-9
        )
    if not all(checks.values()):
        failed = [key for key, value in checks.items() if not value]
        raise NestedSelectorSprintError(f"campaign 0023 audit mismatch: {failed}")
    diagnostic = {
        "policy_id": DIAGNOSTIC_CHAMPION_ID,
        "status": "DEVELOPMENT_SELECTED_DIAGNOSTIC_CHAMPION",
        "validated": False,
        "promotion_status_inherited": False,
        "normal_pass_count": int(diagnostic_normal["pass_count"]),
        "normal_episode_count": int(diagnostic_normal["episode_start_count"]),
        "normal_pass_rate": float(diagnostic_normal["pass_rate"]),
        "stressed_pass_count": int(diagnostic_stress["pass_count"]),
        "stressed_episode_count": int(diagnostic_stress["episode_start_count"]),
        "stressed_pass_rate": float(diagnostic_stress["pass_rate"]),
        "normal_target_progress_median": float(
            diagnostic_normal["target_progress_median"]
        ),
        "stressed_target_progress_median": float(
            diagnostic_stress["target_progress_median"]
        ),
        "mll_breach_rate": max(
            float(diagnostic_normal["mll_breach_rate"]),
            float(diagnostic_stress["mll_breach_rate"]),
        ),
        "independent_evidence": False,
    }
    if (
        diagnostic["normal_pass_count"] != 9
        or diagnostic["stressed_pass_count"] != 9
        or abs(float(diagnostic["normal_target_progress_median"]) - 0.7861702777778017)
        > 1e-9
        or abs(float(diagnostic["stressed_target_progress_median"]) - 0.7635097222222701)
        > 1e-9
        or diagnostic["mll_breach_rate"] != 0.0
    ):
        raise NestedSelectorSprintError("diagnostic champion evidence drift")
    checkpoint = _project_root(source_paths["campaign_dir"]) / (
        "mission/state/snapshots/"
        "v73_nested_selector_pre_experiment_20260714T092403Z.tar.gz"
    )
    audit = {
        "schema": "hydra_v73_campaign_0023_terminal_audit_v1",
        "campaign_id": CAMPAIGN_0023_ID,
        "terminal_scope": "BROAD_STATIC_PARENT_SYNTHESIS_FAMILY",
        "terminal_verdict": "STATIC_PARENT_SYNTHESIS_FALSIFIED",
        "family_threshold_changed_after_result": False,
        "parameter_neighbour_resurrection_allowed": False,
        "same_exact_family_relaunch_allowed": False,
        "verified_values": verified,
        "expected_value_checks": checks,
        "all_reported_values_verified": True,
        "diagnostic_champion": diagnostic,
        "result_sha256": file_sha256(source_paths["result"]),
        "tripwire_sha256": file_sha256(source_paths["tripwire"]),
        "graveyard_receipt_exists": terminal_receipt.is_file(),
        "graveyard_receipt_sha256": (
            file_sha256(terminal_receipt) if terminal_receipt.is_file() else None
        ),
        "graveyard_receipt_hash_valid": (
            receipt.get("receipt_hash")
            == stable_hash(
                {
                    key: value
                    for key, value in receipt.items()
                    if key != "receipt_hash"
                }
            )
        ),
        "mission_db_terminal_event": mission_terminal,
        "safe_checkpoint": {
            "path": str(checkpoint),
            "exists": checkpoint.is_file(),
            "sha256": file_sha256(checkpoint) if checkpoint.is_file() else None,
        },
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    audit["audit_hash"] = stable_hash(audit)
    return audit


def _persist_campaign_0023_terminal(
    campaign_dir: Path, audit: Mapping[str, Any]
) -> None:
    writer = AtomicResultWriter(campaign_dir)
    writer.write_json("campaign_0023_terminal_persistence_v73.json", audit)
    champion = dict(audit["diagnostic_champion"])
    manifest = {
        "schema": "hydra_v73_development_diagnostic_champion_manifest_v1",
        **champion,
        "source_campaign_terminal_verdict": "STATIC_PARENT_SYNTHESIS_FALSIFIED",
        "may_inherit_promotion": False,
        "paper_shadow_ready": False,
        "basket_confirmation_ready": False,
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    writer.write_json("development_selected_diagnostic_champion_manifest.json", manifest)


def _actual_state_snapshot(root: Path) -> dict[str, Any]:
    heartbeat = _load_json(root / "mission/state/heartbeat.json")
    service = _systemctl_properties("hydra-autonomous-mission.service")
    controller_lines = _command_lines(
        ["pgrep", "-af", "scripts/run_v7_falsification_mission.py"]
    )
    controller_pids = [
        int(line.split(maxsplit=1)[0])
        for line in controller_lines
        if line.split(maxsplit=1)[0].isdigit()
    ]
    lock_path = root / "mission/state/hydra_mission.lock"
    lock_output = _command_output(["fuser", str(lock_path)])
    lock_holder_pids = sorted(
        {int(value) for value in re.findall(r"\b\d+\b", lock_output)}
    )
    git_status = _command_lines(["git", "status", "--porcelain"], cwd=root)
    databases = {}
    database_open_pids: set[int] = set()
    for label, relative in {
        "mission": "mission/state/hydra_mission.db",
        "registry": "registry/hydra_registry.db",
        "graveyard": "mission/state/graveyard.db",
    }.items():
        path = root / relative
        databases[label] = {
            "path": relative,
            "exists": path.is_file(),
            "integrity_check": _sqlite_integrity(path) if path.is_file() else "MISSING",
            "open_process_pids": (
                sorted(
                    int(value)
                    for value in _command_lines(["lsof", "-t", str(path)])
                    if value.isdigit()
                )
                if path.is_file()
                else []
            ),
        }
        database_open_pids.update(databases[label]["open_process_pids"])
    governance = _governance_checksum_audit(root)
    return {
        "git_head": _command_output(["git", "rev-parse", "HEAD"], cwd=root),
        "origin_main_cached": _command_output(
            ["git", "rev-parse", "refs/remotes/origin/main"], cwd=root
        ),
        "branch": _command_output(["git", "branch", "--show-current"], cwd=root),
        "working_tree_dirty_entry_count": len(git_status),
        "service": service,
        "controller_version": heartbeat.get("controller_version"),
        "mission_id": heartbeat.get("mission_id"),
        "step": heartbeat.get("step"),
        "phase": heartbeat.get("phase"),
        "heartbeat_at_utc": heartbeat.get("heartbeat_at_utc"),
        "controller_process_count": len(controller_pids),
        "controller_process_pids": controller_pids,
        "authoritative_database_writer_process_count": len(database_open_pids),
        "authoritative_database_writer_process_pids": sorted(database_open_pids),
        "separate_mission_db_writer_count": len(
            database_open_pids - set(controller_pids)
        ),
        "process_lock": {
            "heartbeat_path": heartbeat.get("process_lock"),
            "audited_path": str(lock_path),
            "holder_pids": lock_holder_pids,
            "held": bool(lock_holder_pids),
            "held_by_service_main_pid": int(service.get("MainPID", 0) or 0)
            in lock_holder_pids,
        },
        "single_writer": {
            "heartbeat_claim": heartbeat.get("single_writer"),
            "verified": len(database_open_pids) == 1
            and database_open_pids == set(controller_pids),
        },
        "snapshot_role": "WORKER_PRE_RESULT_PUBLICATION",
        "post_controller_terminal_state_included": False,
        "databases": databases,
        "governance_checksum_audit": governance,
    }


def _sprint_governance_observation(root: Path) -> dict[str, Any]:
    budget_path = root / "reports/data_budget/databento_spend_ledger.jsonl"
    budget_ledger = _read_jsonl(budget_path)
    heartbeat_path = root / "mission/state/heartbeat.json"
    proof_path = root / "mission/state/proof_registry.json"
    heartbeat = _load_json(heartbeat_path)
    proof = _load_json(proof_path)
    q4_entries = [
        row
        for row in proof.get("entries", [])
        if "Q4_2024" in json.dumps(row, sort_keys=True)
    ]
    return {
        "schema": "hydra_v73_sprint_governance_start_v1",
        "observed_at_utc": utc_now_iso(),
        "budget": {
            "ledger_path": str(budget_path.relative_to(root)),
            "ledger_sha256": file_sha256(budget_path),
            "ledger_row_count": len(budget_ledger),
            "actual_spend_usd": (
                float(budget_ledger[-1]["cumulative_actual_spend_usd"])
                if budget_ledger
                else 0.0
            ),
        },
        "q4": {
            "heartbeat_path": str(heartbeat_path.relative_to(root)),
            "heartbeat_sha256": file_sha256(heartbeat_path),
            "proof_registry_path": str(proof_path.relative_to(root)),
            "proof_registry_sha256": file_sha256(proof_path),
            "historical_access_count": int(heartbeat.get("q4_access_count", 0)),
            "q4_proof_entry_count": len(q4_entries),
            "q4_proof_entries_hash": stable_hash(q4_entries),
        },
        "forward_feed": _forward_feed_snapshot(root),
    }


def _validate_sprint_governance_baseline(
    expected: Mapping[str, Any], observed: Mapping[str, Any]
) -> None:
    checks = {
        "budget_actual_spend_usd": abs(
            float(expected["budget_actual_spend_usd"])
            - float(observed["budget"]["actual_spend_usd"])
        )
        <= 1e-12,
        "q4_historical_access_count": int(
            expected["q4_historical_access_count"]
        )
        == int(observed["q4"]["historical_access_count"]),
        "q4_proof_entry_count": int(expected["q4_proof_entry_count"])
        == int(observed["q4"]["q4_proof_entry_count"]),
    }
    if not all(checks.values()):
        raise NestedSelectorSprintError(
            f"sprint governance baseline drift before outcomes: {checks}"
        )


def _budget_snapshot(
    root: Path, *, baseline: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    path = root / "reports/data_budget/databento_spend_ledger.jsonl"
    ledger = _read_jsonl(path)
    actual = float(ledger[-1]["cumulative_actual_spend_usd"]) if ledger else 0.0
    baseline_actual = (
        float(baseline["budget_actual_spend_usd"])
        if baseline is not None
        else actual
    )
    delta = actual - baseline_actual
    return {
        "hard_cap_usd": 125.0,
        "actual_spend_usd": actual,
        "remaining_usd": 125.0 - actual,
        "baseline_actual_spend_usd": baseline_actual,
        "actual_spend_delta_usd": delta,
        "new_purchase_during_sprint": delta > 1e-12,
        "ledger_row_count": len(ledger),
        "ledger_sha256": file_sha256(path),
        "last_purchase_timestamp_utc": ledger[-1].get("timestamp_utc") if ledger else None,
    }


def _q4_snapshot(
    root: Path, *, baseline: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    heartbeat = _load_json(root / "mission/state/heartbeat.json")
    proof = _load_json(root / "mission/state/proof_registry.json")
    q4_entries = [
        row
        for row in proof.get("entries", [])
        if "Q4_2024" in json.dumps(row, sort_keys=True)
    ]
    historical = int(heartbeat.get("q4_access_count", 0))
    baseline_access = (
        int(baseline["q4_historical_access_count"])
        if baseline is not None
        else historical
    )
    baseline_entries = (
        int(baseline["q4_proof_entry_count"])
        if baseline is not None
        else len(q4_entries)
    )
    delta = max(historical - baseline_access, len(q4_entries) - baseline_entries)
    return {
        "historical_access_count": historical,
        "baseline_historical_access_count": baseline_access,
        "historical_q4_proof_entries": len(q4_entries),
        "baseline_q4_proof_entries": baseline_entries,
        "q4_proof_entries_hash": stable_hash(q4_entries),
        "heartbeat_sha256": file_sha256(root / "mission/state/heartbeat.json"),
        "proof_registry_sha256": file_sha256(
            root / "mission/state/proof_registry.json"
        ),
        "q4_burned_before_sprint": True,
        "q4_access_during_sprint": max(0, delta),
        "q4_access_authorized": False,
        "q4_in_temporal_blocks": False,
    }


def _forward_feed_snapshot(
    root: Path, *, baseline: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    manifests = sorted((root / "mission/state/forward_feed_manifests").glob("*.json"))
    candidate_count = 0
    broker_connections = 0
    outbound_orders = 0
    fabricated_bars = 0
    freeze_boundaries: list[dict[str, Any]] = []
    for path in manifests:
        value = _load_json(path)
        candidate_count = max(candidate_count, int(value.get("candidate_count", 0)))
        broker_connections += int(value.get("broker_connections", 0))
        outbound_orders += int(value.get("outbound_orders", 0))
        fabricated_bars += int(value.get("fabricated_closed_market_bars", 0))
        freeze_boundaries.append(
            {
                "path": str(path.relative_to(root)),
                "candidate_count": int(value.get("candidate_count", 0)),
                "created_at_utc": value.get("created_at_utc"),
                "forward_only": value.get("forward_only"),
                "broker_connections": int(value.get("broker_connections", 0)),
                "outbound_orders": int(value.get("outbound_orders", 0)),
            }
        )
    append_results = sorted(
        (root / "mission/state/worker_results").glob(
            "forward_shadow_append_update_*.json"
        )
    )
    published = 0
    successful_updates = 0
    failed_updates = 0
    for path in append_results:
        value = _load_json(path)
        if value.get("ok") is True and isinstance(value.get("result"), Mapping):
            successful_updates += 1
            result = value["result"]
            published = max(
                published,
                int(result.get("candidate_heartbeats_published", 0)),
            )
            broker_connections += int(result.get("broker_connections", 0))
            outbound_orders += int(result.get("outbound_orders", 0))
            fabricated_bars += int(
                result.get("fabricated_closed_market_bars", 0)
            )
        else:
            failed_updates += 1
    baseline_published = (
        int(baseline.get("genuine_post_freeze_bar_processed_count", 0))
        if baseline is not None
        else published
    )
    return {
        "boundary_manifest_count": len(manifests),
        "waiting_candidate_count": candidate_count,
        "successful_append_only_feed_preparation_count": successful_updates,
        "failed_append_only_feed_preparation_count": failed_updates,
        "genuine_post_freeze_bar_processed_count": published,
        "baseline_genuine_post_freeze_bar_processed_count": baseline_published,
        "genuine_post_freeze_bar_processed_delta": max(
            0, published - baseline_published
        ),
        "shadow_research_active_count": published,
        "fabricated_closed_market_bars": fabricated_bars,
        "broker_connections": broker_connections,
        "orders": outbound_orders,
        "status": (
            "SHADOW_RESEARCH_ACTIVE"
            if published
            else "WAITING_FOR_GENUINELY_FRESH_POST_FREEZE_BAR"
        ),
        "boundaries": freeze_boundaries,
    }


def _governance_checksum_audit(root: Path) -> dict[str, Any]:
    path = root / "mission/state/governance_protected_manifest.json"
    manifest = _load_json(path)
    changed: list[dict[str, Any]] = []
    live_digests: list[dict[str, Any]] = []
    for row in manifest.get("digests", []):
        target = root / str(row["path"])
        actual = file_sha256(target) if target.is_file() else None
        live_digests.append(
            {
                "path": row["path"],
                "sha256": actual or "",
                "exists": target.is_file(),
            }
        )
        if actual != row.get("sha256"):
            changed.append(
                {
                    "path": row["path"],
                    "recorded_sha256": row.get("sha256"),
                    "actual_sha256": actual,
                }
            )
    recorded = manifest.get("manifest_hash")
    recorded_payload = {
        "version": manifest.get("version"),
        "governance_config": manifest.get("governance_config"),
        "baseline_commit": manifest.get("baseline_commit"),
        "digests": list(manifest.get("digests", [])),
    }
    live_payload = {**recorded_payload, "digests": live_digests}
    recorded_self_hash = stable_hash(recorded_payload)
    return {
        "recorded_manifest_hash": recorded,
        "recorded_manifest_self_hash": recorded_self_hash,
        "recorded_manifest_self_hash_valid": recorded == recorded_self_hash,
        "live_protected_files_manifest_hash": stable_hash(live_payload),
        "protected_file_drift_count": len(changed),
        "changed_files": changed,
        "kernel_status_file_exists": (
            root / "mission/state/governance_kernel_status.md"
        ).is_file(),
        "interpretation": (
            "RECORDED_PROTECTED_MANIFEST_IS_STALE; fail-closed disclosure retained."
            if changed
            else "PROTECTED_MANIFEST_MATCHES"
        ),
    }


def _required_report_sections(
    *,
    service_state: Mapping[str, Any],
    campaign_audit: Mapping[str, Any],
    selector_manifest: Mapping[str, Any],
    plan: TemporalBlockPlan,
    frozen_folds: Sequence[Mapping[str, Any]],
    held_out_folds: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
    final_development: Mapping[str, Any],
    budget: Mapping[str, Any],
    q4_status: Mapping[str, Any],
    forward_status: Mapping[str, Any],
    next_action: str,
) -> dict[str, Any]:
    held_selector = [row["selector"] for row in held_out_folds]
    return {
        "actual_service_state_and_controller_version": dict(service_state),
        "campaign_0023_terminal_persistence": dict(campaign_audit),
        "temporal_blocks_used": {
            "plan_hash": plan.plan_hash,
            "inference_unit": "TEMPORAL_BLOCK",
            "within_block_starts_independent": False,
            "blocks": [block.to_dict() for block in plan.blocks],
        },
        "contamination_audit": {
            "independent_confirmation": False,
            "outer_fold_role": "NESTED_DEVELOPMENT_CROSS_FIT",
            "old_interleaved_pseudo_blocks_rejected": True,
            "diagnostic_champion_excluded_from_decision": True,
            "q4_2024_excluded": True,
            "blocks": {
                block.block_id: [dict(value) for value in block.contamination_history]
                for block in plan.blocks
            },
        },
        "selector_manifest_and_frozen_ranking": {
            "manifest_hash": selector_manifest["manifest_hash"],
            "pareto_objectives": selector_manifest["pareto_objectives"],
            "selector_execution_policy": selector_manifest[
                "selector_execution_policy"
            ],
            "hard_requirements": selector_manifest["hard_requirements"],
            "decision_thresholds": selector_manifest["decision_thresholds"],
            "candidate_bank_policy": selector_manifest[
                "candidate_bank_policy"
            ],
            "selector_frozen_before_heldout": True,
        },
        "candidates_available_per_outer_fold": [
            {
                "fold_id": row["fold"].fold_id,
                **row["public_freeze"]["candidate_bank"],
            }
            for row in frozen_folds
        ],
        "champion_selected_in_each_design_set": [
            {
                "fold_id": row["fold"].fold_id,
                "held_out_block_id": row["fold"].held_out_block_id,
                "primary_policy_id": row["primary_policy"].policy_id,
                "primary_is_admissible_champion": row["selector_admissible"],
                "backup_policy_id": (
                    row["backup_policy"].policy_id
                    if row["backup_policy"] is not None
                    else None
                ),
                "selection_freeze_hash": row["public_freeze"][
                    "selection_freeze_hash"
                ],
            }
            for row in frozen_folds
        ],
        "selected_risk_level_per_fold": [
            {
                "fold_id": row["fold"].fold_id,
                "selector_risk": row["primary_risk"],
                "selector_design_risk_frontier": row["primary_risk_frontier"],
                "best_parent_risk": row["parent_risk"],
                "equal_risk_baseline_risk": row["equal_risk"],
                "random_baseline_risks": {
                    str(seed): risk for seed, risk in row["random_risks"].items()
                },
            }
            for row in frozen_folds
        ],
        "held_out_pass_counts": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "normal": int(row["selector"]["metrics"]["normal_pass_count"]),
                "stressed": int(row["selector"]["metrics"]["stress_pass_count"]),
                "episode_count": int(row["selector"]["metrics"]["episode_count"]),
            }
            for row in held_out_folds
        ],
        "held_out_target_progress_results": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "normal_median": float(
                    row["selector"]["metrics"]["normal_target_progress_median"]
                ),
                "stressed_median": float(
                    row["selector"]["metrics"]["stressed_target_progress_median"]
                ),
                "stressed_p25": float(
                    row["selector"]["metrics"]["stressed_target_progress_p25"]
                ),
            }
            for row in held_out_folds
        ],
        "held_out_stressed_net": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "total_usd": float(row["selector"]["metrics"]["stressed_net_usd"]),
                "median_episode_usd": float(
                    row["selector"]["metrics"]["stressed_median_net_usd"]
                ),
            }
            for row in held_out_folds
        ],
        "held_out_mll": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "stressed_breach_count": int(
                    row["selector"]["metrics"]["mll_breach_count"]
                ),
                "stressed_breach_rate": float(
                    row["selector"]["metrics"]["stressed_mll_breach_rate"]
                ),
            }
            for row in held_out_folds
        ],
        "held_out_consistency": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "stressed_consistency": float(
                    row["selector"]["metrics"]["stressed_consistency_pass_rate"]
                ),
            }
            for row in held_out_folds
        ],
        "best_parent_baseline": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "policy_id": row["best_parent"]["policy_id"],
                "risk_label": row["best_parent"]["risk_label"],
                "metrics": _decision_policy_row(row["best_parent"]["metrics"]),
            }
            for row in held_out_folds
        ],
        "equal_risk_baseline": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "policy_id": row["equal_risk"]["policy_id"],
                "risk_label": row["equal_risk"]["risk_label"],
                "metrics": _decision_policy_row(row["equal_risk"]["metrics"]),
            }
            for row in held_out_folds
        ],
        "random_selection_baseline": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "members": [
                    {
                        "seed": int(value["seed"]),
                        "policy_id": value["policy_id"],
                        "risk_label": value["risk_label"],
                        "metrics": _decision_policy_row(value["metrics"]),
                    }
                    for value in row["random_selection"]
                ],
            }
            for row in held_out_folds
        ],
        "result_by_independent_block": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "selector": row["decision_evidence"]["selector"],
                "best_parent": row["decision_evidence"]["best_parent"],
                "equal_risk": row["decision_evidence"]["equal_risk"],
                "random_selection": row["decision_evidence"]["random_selection"],
                "diagnostic_reference_status": (
                    "CONTAMINATED_DEVELOPMENT_REFERENCE_ONLY"
                ),
            }
            for row in held_out_folds
        ],
        "time_to_target_and_censoring_audit": [
            {
                "block_id": row["fold"]["held_out_block_id"],
                "normal": row["selector"]["metrics"]["time_to_combine"]["normal"],
                "stress_1_5x": row["selector"]["metrics"]["time_to_combine"][
                    "stress_1_5x"
                ],
            }
            for row in held_out_folds
        ],
        "selector_procedure_decision": dict(decision),
        "final_development_champion": dict(final_development),
        "candidates_promoted_to_96_starts": {
            "candidate_ids": list(final_development.get("promoted_to_96_starts", [])),
            "count": len(final_development.get("promoted_to_96_starts", [])),
            "paper_shadow_ready_count": 0,
        },
        "remaining_budget": dict(budget),
        "q4_status": dict(q4_status),
        "forward_feed_status": dict(forward_status),
        "current_autonomous_next_action": next_action,
    }


def _complete_runtime_projection(
    projection: Mapping[str, Any],
    *,
    decision: Mapping[str, Any],
    held_out_folds: Sequence[Mapping[str, Any]],
    selector_manifest: Mapping[str, Any],
    campaign_audit: Mapping[str, Any],
    final_development: Mapping[str, Any],
    phase_seconds: Mapping[str, float],
    elapsed_seconds: float,
    service_state: Mapping[str, Any],
    budget: Mapping[str, Any],
    q4_status: Mapping[str, Any],
    forward_status: Mapping[str, Any],
    next_action: str,
) -> dict[str, Any]:
    result = dict(projection)
    result.pop("result_sha256", None)
    selector_rows = [row["decision_evidence"]["selector"] for row in held_out_folds]
    parent_rows = [row["decision_evidence"]["best_parent"] for row in held_out_folds]
    pass_rates = [
        float(row["stressed_pass_count"]) / int(row["episode_count"])
        for row in selector_rows
    ]
    progress = [float(row["stressed_target_progress"]) for row in selector_rows]
    mll_rates = [
        int(row["mll_breach_count"]) / int(row["episode_count"])
        for row in selector_rows
    ]
    selector_wins = sum(
        float(selector["stressed_net_usd"]) > float(parent["stressed_net_usd"])
        and float(selector["stressed_target_progress"])
        > float(parent["stressed_target_progress"])
        for selector, parent in zip(selector_rows, parent_rows, strict=True)
    )
    parent_wins = sum(
        float(selector["stressed_net_usd"]) < float(parent["stressed_net_usd"])
        or float(selector["stressed_target_progress"])
        < float(parent["stressed_target_progress"])
        for selector, parent in zip(selector_rows, parent_rows, strict=True)
    )
    economics = dict(result["account_policy_economics"])
    economics.update(
        {
            "policies_passing_at_least_one_combine_episode": sum(
                int(row["stressed_pass_count"]) > 0 for row in selector_rows
            ),
            "combine_pass_probability": {
                "maximum": max(pass_rates),
                "median": statistics.median(pass_rates),
            },
            "median_target_progress_distribution": {
                "median": statistics.median(progress)
            },
            "maximum_target_progress": max(progress),
            "mll_breach_rate_distribution": {
                "median": statistics.median(mll_rates),
                "maximum": max(mll_rates),
            },
            "normal_positive_policy_count": sum(
                float(row["normal_net_usd"]) > 0.0 for row in selector_rows
            ),
            "stressed_positive_policy_count": sum(
                float(row["stressed_net_usd"]) > 0.0 for row in selector_rows
            ),
            "failure_vector_distribution": {
                reason: 1 for reason in decision["failure_reasons"]
            },
            "targeted_mutations_selected": [],
            "compatibility_fields_are_not_family_average_evidence": True,
        }
    )
    total = selector_wins + parent_wins
    result.update(
        {
            "engine_version": ENGINE_VERSION,
            "selector_manifest": dict(selector_manifest),
            "campaign_0023_terminal_audit": dict(campaign_audit),
            "held_out_outer_folds": list(held_out_folds),
            "final_development": dict(final_development),
            "account_policy_economics": economics,
            "family_tripwire": {
                "family_green": decision["status"] == SELECTOR_PROCEDURE_GREEN,
                "real_win_count": selector_wins,
                "matched_control_win_count": parent_wins,
                "tie_count": max(0, 4 - selector_wins - parent_wins),
                "NULL_RATIO": parent_wins / selector_wins if selector_wins else None,
                "verdict": decision["status"],
                "thresholds_changed_after_outcome": False,
                "compatibility_projection_only": True,
                "informative_block_count": total,
            },
            "wall_clock_accounting": {
                "phase_seconds": dict(phase_seconds),
                "research_seconds": elapsed_seconds,
                "tests_and_reporting_seconds_inside_campaign": 0.0,
                "research_percent": 100.0,
                "tests_and_reporting_percent": 0.0,
                "total_seconds_to_result_assembly": elapsed_seconds,
                "full_repository_regression_is_outside_campaign_hot_loop": True,
            },
            "actual_service_state": dict(service_state),
            "remaining_budget": dict(budget),
            "q4_status": dict(q4_status),
            "forward_feed_status": dict(forward_status),
            "next_action": next_action,
            "development_only": True,
            "independently_confirmed": False,
            "paper_shadow_ready_count": 0,
            "pre_holdout_ready_count": 0,
        }
    )
    result["result_sha256"] = stable_hash(result)
    return result


def _next_action(status: str, final_development: Mapping[str, Any]) -> str:
    if status == SELECTOR_PROCEDURE_GREEN:
        if final_development.get("basket_confirmation_ready"):
            return (
                "AUDIT_UNTOUCHED_DATA_AVAILABILITY_WITHOUT_Q4_ACCESS_OR_PURCHASE; "
                "FREEZE_SMALLEST_CONFIRMATION_PACKAGE_REQUEST"
            )
        return "PRESERVE_GREEN_FINALISTS; STOP_BEFORE_INDEPENDENT_DATA_ACCESS"
    if status == SELECTOR_PROCEDURE_WEAK:
        return (
            "TERMINATE_STATIC_BASKET_SYNTHESIS; PRESERVE_COMPONENTS; "
            "PIVOT_ONLY_TO_A_MATERIALLY_DISTINCT_MECHANISM"
        )
    return (
        "TOMBSTONE_STATIC_BASKET_SYNTHESIS_AND_SELECTOR; PRESERVE_USEFUL_COMPONENTS; "
        "DO_NOT_LAUNCH_A_NEIGHBOURING_BASKET_FAMILY"
    )


def _stage(
    writer: AtomicResultWriter, prereg: Mapping[str, Any], stage: str
) -> None:
    writer.write_json(
        "nested_selector_campaign_state.json",
        {
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "broker_connections": 0,
            "orders": 0,
        },
    )


def _systemctl_properties(service: str) -> dict[str, Any]:
    output = _command_output(
        [
            "systemctl",
            "show",
            service,
            "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStartTimestamp",
        ]
    )
    values: dict[str, Any] = {"unit": service}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _sqlite_integrity(path: Path) -> str:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()


def _command_output(
    command: Sequence[str], *, cwd: Path | None = None
) -> str:
    completed = subprocess.run(
        list(command),
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return completed.stdout.strip()


def _command_lines(
    command: Sequence[str], *, cwd: Path | None = None
) -> list[str]:
    return [line for line in _command_output(command, cwd=cwd).splitlines() if line]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise NestedSelectorSprintError(f"expected JSON object: {path}")
    return value


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise NestedSelectorSprintError("project root not found")


def _session_date(session_day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=int(session_day))).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
