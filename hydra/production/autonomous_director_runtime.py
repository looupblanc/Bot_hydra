"""Persistent two-lane runtime for the autonomous economic director.

Only the parent process writes durable state.  The two process-pool workers
receive immutable paths, perform read-only economic analysis, and return plain
mappings.  The director intentionally does not terminalize after its first
epoch: it seals the two bounded decisions, queues materially distinct successor
cards, and keeps publishing a resumable heartbeat for the existing controller.
"""

from __future__ import annotations

import gzip
import json
import math
import multiprocessing
import os
import resource
import statistics
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_director_manifest import (
    ACCOUNT_SIZES_USD,
    CAMPAIGN_ID,
    RUNTIME_VERSION,
    validate_autonomous_director_manifest,
)
from hydra.production.autonomous_exact_replay import (
    exact_0029_account_size_worker,
)
from hydra.production.autonomous_event_time_safety_frontier import (
    COMPOSITE_SCHEMA as EVENT_TIME_SAFETY_COMPOSITE_SCHEMA,
    SCHEMA as EVENT_TIME_SAFETY_SHARD_SCHEMA,
    build_autonomous_event_time_safety_frontier,
    compose_autonomous_event_time_safety_frontier_shards,
)
from hydra.production.autonomous_event_time_matched_controls import (
    SCHEMA as EVENT_TIME_MATCHED_CONTROLS_SCHEMA,
    STATUS as EVENT_TIME_MATCHED_CONTROLS_STATUS,
    event_time_matched_controls_worker,
)
from hydra.production.autonomous_combine_candidate_bank import (
    SCHEMA as COMBINE_CANDIDATE_BANK_SCHEMA,
    build_autonomous_combine_candidate_bank,
)
from hydra.production.autonomous_combine_pass_bank import (
    SCHEMA as COMBINE_PASS_BANK_SCHEMA,
    build_autonomous_combine_pass_observed_bank,
)
from hydra.production.autonomous_consistency_account_policies import (
    COMPOSITE_SCHEMA as CONSISTENCY_DIRECT_COMPOSITE_SCHEMA,
    SCHEMA as CONSISTENCY_DIRECT_SHARD_SCHEMA,
    build_autonomous_consistency_account_policies,
    compose_autonomous_consistency_account_policy_shards,
)
from hydra.production.autonomous_exact_continuation import (
    INITIAL_EXACT_COHORT_SIZE,
    audit_hazard_19327_tier_q,
    compose_remaining_0029_exact_results,
    plan_remaining_0029_exact_jobs,
    remaining_0029_exact_worker,
)
from hydra.production.autonomous_marginal_combine_books import (
    COMPOSITE_SCHEMA as MARGINAL_BOOK_COMPOSITE_SCHEMA,
    build_autonomous_marginal_combine_books,
    compose_autonomous_marginal_combine_book_shards,
)
from hydra.production.autonomous_tier_g_controls import (
    COMPOSITE_SCHEMA as TIER_G_CONTROL_COMPOSITE_SCHEMA,
    SCHEMA as TIER_G_CONTROL_SHARD_SCHEMA,
    build_autonomous_tier_g_controls,
    compose_autonomous_tier_g_control_shards,
)
from hydra.production.autonomous_tier_g_graduation import (
    SCHEMA as TIER_G_GRADUATION_SCHEMA,
    build_graduated_development_books,
    verify_tier_g_development_graduation,
)
from hydra.production.autonomous_tier_g_xfa_diagnostic import (
    SCHEMA as TIER_G_XFA_DIAGNOSTIC_SCHEMA,
    STATUS as TIER_G_XFA_DIAGNOSTIC_STATUS,
    build_autonomous_tier_g_xfa_diagnostic,
    verify_autonomous_tier_g_xfa_diagnostic,
)
from hydra.production.autonomous_tier_g_xfa_handoff import (
    SCHEMA as TIER_G_XFA_HANDOFF_SCHEMA,
    STATUS as TIER_G_XFA_HANDOFF_STATUS,
    build_tier_g_combine_xfa_handoffs,
    verify_tier_g_combine_xfa_handoffs,
)
from hydra.production.cross_index_breadth_tripwire import (
    SCHEMA as CROSS_INDEX_BREADTH_SCHEMA,
    run_cross_index_breadth_tripwire,
)
from hydra.production.fresh_confirmation_lane import (
    RESULT_SCHEMA as FRESH_CONFIRMATION_RESULT_SCHEMA,
    evaluate_fresh_confirmation,
    open_confirmation_matrices,
)
from hydra.production.frozen_breadth_continuation import (
    SCHEMA as FROZEN_BREADTH_CONTINUATION_SCHEMA,
    build_breadth_feature_bundles,
    evaluate_breadth_continuation,
    open_breadth_matrices,
)
from hydra.production.frozen_legal_frontier_replay import (
    SCHEMA as FROZEN_LEGAL_FRONTIER_SCHEMA,
    frozen_legal_frontier_worker,
)
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA
from hydra.production.session_safe_fast_book_tripwire import (
    BRANCH_ID as SESSION_SAFE_FAST_BOOK_BRANCH_ID,
    EVIDENCE_ROLE as SESSION_SAFE_FAST_BOOK_EVIDENCE_ROLE,
    REPAIR_VARIANTS as SESSION_SAFE_REPAIR_VARIANTS,
    SCHEMA as SESSION_SAFE_FAST_BOOK_SCHEMA,
    SOURCE_POLICY_ID as SESSION_SAFE_FAST_BOOK_SOURCE_POLICY_ID,
    session_safe_fast_book_worker,
)
from hydra.production.session_safe_m2k_mym_confirmation import (
    FEATURE_SCHEMA as SESSION_SAFE_CONFIRMATION_FEATURE_SCHEMA,
    SCHEMA as SESSION_SAFE_CONFIRMATION_SCHEMA,
    build_feature_bundles as build_session_safe_confirmation_features,
    evaluate_confirmation as evaluate_session_safe_confirmation,
    load_decision_card as load_session_safe_confirmation_card,
    open_feature_matrices as open_session_safe_confirmation_matrices,
)
from hydra.production.v71_event_time_account_exploration import (
    event_time_account_exploration_worker,
)
from hydra.research.curve_relative_value_tripwire import (
    EVIDENCE_ROLE as TREASURY_CURVE_EVIDENCE_ROLE,
    INPUT_SCHEMA as TREASURY_CURVE_INPUT_SCHEMA,
    OFFICIAL_COST_RECEIPT as TREASURY_CURVE_COST_RECEIPT,
    SCHEMA as TREASURY_CURVE_SCHEMA,
    WAITING_STATUS as TREASURY_CURVE_WAITING_STATUS,
    build_curve_relative_value_tripwire,
)
from hydra.research.clean_cross_asset_daily_tripwire import (
    BRANCH_ID as CLEAN_CROSS_ASSET_DAILY_BRANCH_ID,
    SCHEMA as CLEAN_CROSS_ASSET_DAILY_SCHEMA,
    run_clean_cross_asset_daily_tripwire,
)


BRANCH_RESULT_SCHEMA = "hydra_autonomous_economic_branch_result_v1"
BRANCH_STATE_SCHEMA = "hydra_autonomous_branch_state_v1"
ECONOMIC_SCORECARD_SCHEMA = "hydra_autonomous_economic_scorecard_v1"
_HORIZONS = (5, 10, 20)
_SCENARIOS = ("NORMAL", "STRESSED_1_5X")
_POST_BREADTH_RELATIVE_ROOT = Path("post_breadth_portfolio")
_SESSION_SAFE_PORTFOLIO_SCHEMA = "hydra_session_safe_fast_book_portfolio_v1"
_SESSION_SAFE_CONFIRMATION_RELATIVE_ROOT = (
    _POST_BREADTH_RELATIVE_ROOT / "session_safe_m2k_mym_confirmation"
)
_SESSION_SAFE_CONFIRMATION_DEFAULT_RECEIPT = Path(
    "reports/data_access/session_safe_m2k_mym_confirmation_acquisition_receipt.json"
)
_SESSION_SAFE_CONFIRMATION_WAITING = (
    "WAITING_FOR_SESSION_SAFE_M2K_MYM_CONFIRMATION_INPUT"
)
_TREASURY_ACQUISITION_SCHEMA = (
    "hydra_treasury_curve_tripwire_acquisition_receipt_v1"
)
_DEFAULT_SCALE_FACTORS = (
    0.50,
    0.75,
    1.00,
    1.25,
    1.50,
    2.00,
    3.00,
    4.00,
    5.00,
    6.00,
    8.00,
    10.00,
    12.00,
    15.00,
    20.00,
    30.00,
)
_THREAD_ENV = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)
_RUN_STARTED_AT_UTC: str | None = None
_RUN_WALL_STARTED: float | None = None
_CPU_STARTED_SECONDS: float | None = None
_ECONOMIC_WALL_ACCUMULATED = 0.0
_ECONOMIC_ACTIVE_SINCE: float | None = None


class AutonomousDirectorRuntimeError(RuntimeError):
    """The master runtime cannot progress without violating its contract."""


def read_autonomous_director_status(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(path)
    output = path.parents[2] / str(manifest["runtime"]["output_dir"])
    state_path = output / "production_state.json"
    kpi_path = output / "production_kpis.json"
    if not state_path.is_file():
        return {
            "campaign_id": CAMPAIGN_ID,
            "state": "NOT_STARTED",
            "stage": "TWO_LANE_EPOCH_PENDING",
            "next_action": "START_EXPLOITATION_AND_EXPLORATION_WORKERS",
        }
    state = _read_hashed(state_path, "state_hash")
    kpis = _read_hashed(kpi_path, "kpi_hash") if kpi_path.is_file() else None
    return {"state": state, "kpis": kpis}


def run_autonomous_director_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
    heartbeat_seconds: float = 15.0,
) -> dict[str, Any]:
    """Run/resume the first two-lane epoch, then remain persistently active."""

    del contract_map_path, cache_root  # Inputs remain frozen in branch artifacts.
    if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise AutonomousDirectorRuntimeError(
            "autonomous stop_after is restricted to explicit test mode"
        )
    _set_single_thread_libraries()
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    manifest = load_and_validate_production_manifest(path)
    validate_autonomous_director_manifest(manifest, manifest_path=path)
    output = root / str(manifest["runtime"]["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    live_writer = AtomicResultWriter(output, immutable=False)
    branch_writer = AtomicResultWriter(output / "branch_results")
    started = time.monotonic()

    prior = _load_prior_state(output, manifest)
    _initialize_runtime_measurement(prior, started)
    sequence = int(prior.get("checkpoint_sequence", 0)) if prior else 0
    branch_files = {
        "EXPLOITATION": output / "branch_results/0034_exploitation.json",
        "EXPLORATION": output / "branch_results/legal_feasibility.json",
    }
    branch_results: dict[str, dict[str, Any]] = {}
    for lane, result_path in branch_files.items():
        if result_path.is_file():
            result = _read_hashed(result_path, "result_hash")
            if not _artifact_manifest_compatible(result, manifest):
                raise AutonomousDirectorRuntimeError(
                    f"resumed {lane} branch result belongs to another manifest"
                )
            branch_results[lane] = result

    state = _state_payload(
        manifest,
        sequence=sequence + 1,
        state="STARTING",
        stage="TWO_LANE_EPOCH_STARTING",
        branch_results=branch_results,
        next_action="START_TWO_READ_ONLY_ECONOMIC_WORKERS",
    )
    _publish(live_writer, state, _kpis(manifest, state, branch_results, started))
    _write_mission_views(root, manifest, state, branch_results)
    if stop_after and stop_after.upper() in {"START", "STARTING"}:
        return state

    missing = set(branch_files) - set(branch_results)
    if missing:
        exploitation_path = _resolve_exploitation_path(root, manifest)
        episode_paths = _resolve_episode_paths(root, manifest)
        rule_path = root / str(manifest["official_rule_snapshot"]["path"])
        factors = _scale_factors(manifest)
        maximum = _exploration_policy_maximum(manifest)
        jobs: dict[Any, str] = {}
        # The pool is deliberately exactly two processes.  Workers never receive
        # an output or database path and therefore cannot become competing writers.
        _begin_economic_phase()
        with ProcessPoolExecutor(
            max_workers=2, mp_context=multiprocessing.get_context("spawn")
        ) as pool:
            if "EXPLOITATION" in missing:
                jobs[pool.submit(_exploitation_worker, str(exploitation_path))] = (
                    "EXPLOITATION"
                )
            if "EXPLORATION" in missing:
                jobs[
                    pool.submit(
                        _exploration_worker,
                        tuple(str(value) for value in episode_paths),
                        str(rule_path),
                        factors,
                        maximum,
                    )
                ] = "EXPLORATION"
            state = _state_payload(
                manifest,
                sequence=int(state["checkpoint_sequence"]) + 1,
                state="ROBUSTNESS_ACTIVE",
                stage="INITIAL_TWO_LANE_EPOCH_RUNNING",
                branch_results=branch_results,
                next_action="COMPLETE_BOUNDED_0034_AND_LEGAL_FEASIBILITY_DECISIONS",
            )
            state["active_economic_worker_processes"] = len(jobs)
            state = _rehash(state, "state_hash")
            _publish(live_writer, state, _kpis(manifest, state, branch_results, started))
            _write_mission_views(root, manifest, state, branch_results)

            for future in as_completed(jobs):
                lane = jobs[future]
                value = dict(future.result())
                value.update(
                    {
                        "schema": BRANCH_RESULT_SCHEMA,
                        "campaign_id": manifest["campaign_id"],
                        "manifest_hash": manifest["manifest_hash"],
                        "source_commit": manifest["source_commit"],
                        "lane_id": lane,
                        "completed_at_utc": _utc_now(),
                        "read_only_worker": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                        "data_purchase_count": 0,
                    }
                )
                value = _with_hash(value, "result_hash")
                name = (
                    "0034_exploitation.json"
                    if lane == "EXPLOITATION"
                    else "legal_feasibility.json"
                )
                branch_writer.write_json(name, value)
                branch_results[lane] = value
                _append_decision_once(root, manifest, value)
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="INITIAL_TWO_LANE_EPOCH_RUNNING",
                    branch_results=branch_results,
                    next_action="CONTINUE_OTHER_LANE_AND_QUEUE_DISTINCT_SUCCESSOR",
                )
                _publish(
                    live_writer,
                    state,
                    _kpis(manifest, state, branch_results, started),
                )
                _write_mission_views(root, manifest, state, branch_results)
        _end_economic_phase()

    if set(branch_results) != {"EXPLOITATION", "EXPLORATION"}:
        raise AutonomousDirectorRuntimeError("initial branch denominator incomplete")

    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="NEXT_DISTINCT_BRANCHES_QUEUED",
        branch_results=branch_results,
        next_action="START_NEXT_MATERIALLY_DISTINCT_ECONOMIC_EPOCH",
    )
    state["next_branch_cards"] = _next_branch_cards(branch_results)
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, branch_results, started))
    _write_mission_views(root, manifest, state, branch_results)
    if stop_after and stop_after.upper() in {
        "FIRST_EPOCH",
        "INITIAL_EPOCH",
        "BRANCH_RESULTS",
    }:
        return state

    successor_results = _run_successor_epoch(
        epoch=2,
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        initial_results=branch_results,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
    )
    exact_epoch_result = successor_results.get("2:EXPLORATION")
    if exact_epoch_result is not None:
        branch_results["EXACT_0029"] = exact_epoch_result
    state = _read_hashed(output / "production_state.json", "state_hash")
    successor_results.update(
        _run_successor_epoch(
            epoch=3,
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            initial_results=branch_results,
            prior_state=state,
            started=started,
            heartbeat_seconds=heartbeat_seconds,
        )
    )
    state = _read_hashed(output / "production_state.json", "state_hash")

    # No terminal result is written.  Continue through disjoint proposal
    # shards and materially different niche questions.  Every RUNNING state
    # below is backed by two live read-only process-pool jobs.
    dimensions = (
        ("TIMEFRAME", "MECHANISM"),
        ("DIRECTION_PROFILE", "HOLDING_HORIZON"),
        ("PAYOFF_GEOMETRY", "RISK_PROFILE"),
        ("MARKET_SESSION", "CROSS_ASSET"),
    )
    epoch = 4
    while True:
        pair_index = (epoch - 4) % len(dimensions)
        shard_index = (epoch - 4) // len(dimensions)
        state, results, source_bank_exhausted = _run_recurring_niche_epoch(
            epoch=epoch,
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            initial_results=branch_results,
            prior_state=state,
            started=started,
            heartbeat_seconds=heartbeat_seconds,
            dimensions=dimensions[pair_index],
            candidate_offset=shard_index * 768,
        )
        successor_results.update(results)
        if source_bank_exhausted:
            return _run_post_source_exhaustion_epochs(
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                initial_results=branch_results,
                prior_state=state,
                started=started,
                heartbeat_seconds=heartbeat_seconds,
            )
        epoch += 1


def _run_successor_epoch(
    *,
    epoch: int,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    initial_results: Mapping[str, Mapping[str, Any]],
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
) -> dict[str, dict[str, Any]]:
    """Execute one materially distinct two-worker economic epoch."""

    names = {
        2: {
            "EXPLOITATION": "epoch_0002_nq_baseline.json",
            "EXPLORATION": "epoch_0002_exact_0029_account_race.json",
        },
        3: {
            "EXPLOITATION": "epoch_0003_account_cost.json",
            "EXPLORATION": "epoch_0003_market_session.json",
        },
    }[epoch]
    running_cards = [
        {
            "lane_id": lane,
            "branch_id": (
                "FROZEN_NQ_STRUCTURAL_BASELINE_CONFIRMATION_PREP"
                if epoch == 2 and lane == "EXPLOITATION"
                else "EXACT_0029_ACCOUNT_SIZE_RACE"
                if epoch == 2
                else "ACCOUNT_SIZE_PAID_COST_FEASIBILITY"
                if lane == "EXPLOITATION"
                else "MARKET_SESSION_ROLE_FEASIBILITY"
            ),
            "status": "RUNNING",
        }
        for lane in ("EXPLOITATION", "EXPLORATION")
    ]
    completed: dict[str, dict[str, Any]] = {}
    for lane, name in names.items():
        path = output / "branch_results" / name
        if path.is_file():
            completed[lane] = _read_hashed(path, "result_hash")
    if len(completed) == 2:
        return {f"{epoch}:{key}": value for key, value in completed.items()}

    source_0034 = _resolve_exploitation_path(root, manifest)
    rules = root / str(manifest["official_rule_snapshot"]["path"])
    _begin_economic_phase()
    with ProcessPoolExecutor(
        max_workers=2, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        jobs: dict[Any, str] = {}
        if epoch == 2:
            if "EXPLOITATION" not in completed:
                jobs[pool.submit(_nq_baseline_confirmation_prep_worker, str(source_0034))] = (
                    "EXPLOITATION"
                )
            if "EXPLORATION" not in completed:
                jobs[
                    pool.submit(
                        exact_0029_account_size_worker,
                        {
                            "root": str(root),
                            "cohort_maximum": 32,
                            "cohort_offset": 0,
                            "integer_tiers": (1, 2, 3, 4),
                            "rule_snapshot_path": str(
                                manifest["official_rule_snapshot"]["path"]
                            ),
                        },
                    )
                ] = "EXPLORATION"
        else:
            if "EXPLOITATION" not in completed:
                jobs[
                    pool.submit(
                        _baseline_account_cost_worker, str(source_0034), str(rules)
                    )
                ] = "EXPLOITATION"
            if "EXPLORATION" not in completed:
                jobs[pool.submit(_market_session_feasibility_worker, str(root))] = (
                    "EXPLORATION"
                )
        state = _state_payload(
            manifest,
            sequence=int(prior_state["checkpoint_sequence"]) + 1,
            state="ROBUSTNESS_ACTIVE",
            stage=f"ECONOMIC_EPOCH_{epoch:04d}_RUNNING",
            branch_results=initial_results,
            next_action=(
                "RUN_FROZEN_NQ_BASELINE_AND_EXACT_0029_ACCOUNT_RACE"
                if epoch == 2
                else "RUN_ACCOUNT_COST_AND_MARKET_SESSION_FEASIBILITY"
            ),
        )
        state["economic_epoch"] = epoch
        state["next_branch_cards"] = running_cards
        state["active_economic_worker_processes"] = len(jobs)
        state = _rehash(state, "state_hash")
        _publish(live_writer, state, _kpis(manifest, state, initial_results, started))
        _write_mission_views(root, manifest, state, initial_results)

        pending = set(jobs)
        while pending:
            done, pending = wait(
                pending,
                timeout=max(float(heartbeat_seconds), 1.0),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage=f"ECONOMIC_EPOCH_{epoch:04d}_RUNNING",
                    branch_results=initial_results,
                    next_action=state["next_action"],
                )
                state["economic_epoch"] = epoch
                state["next_branch_cards"] = running_cards
                state = _rehash(state, "state_hash")
                _publish(
                    live_writer,
                    state,
                    _kpis(manifest, state, initial_results, started),
                )
                continue
            for future in done:
                lane = jobs[future]
                value = dict(future.result())
                value.update(
                    {
                        "schema": BRANCH_RESULT_SCHEMA,
                        "campaign_id": manifest["campaign_id"],
                        "manifest_hash": manifest["manifest_hash"],
                        "source_commit": manifest["source_commit"],
                        "lane_id": lane,
                        "economic_epoch": epoch,
                        "completed_at_utc": _utc_now(),
                        "read_only_worker": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                        "data_purchase_count": 0,
                    }
                )
                value = _with_hash(value, "result_hash")
                branch_writer.write_json(names[lane], value)
                completed[lane] = value
                _append_decision_once(root, manifest, value)
    _end_economic_phase()

    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=f"ECONOMIC_EPOCH_{epoch + 1:04d}_STARTING",
        branch_results=initial_results,
        next_action="START_NEXT_MATERIALLY_DISTINCT_ECONOMIC_EPOCH",
    )
    state["economic_epoch"] = epoch + 1
    state["completed_successor_branch_count"] = len(completed)
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, initial_results, started))
    _write_mission_views(root, manifest, state, initial_results)
    return {f"{epoch}:{key}": value for key, value in completed.items()}


def _run_recurring_niche_epoch(
    *,
    epoch: int,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    initial_results: Mapping[str, Mapping[str, Any]],
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    dimensions: tuple[str, str],
    candidate_offset: int,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], bool]:
    """Run two real, read-only economic niche screens on a disjoint shard."""

    cards = [
        {
            "lane_id": lane,
            "branch_id": f"{dimension}_FEASIBILITY_SHARD_{candidate_offset:06d}",
            "status": "RUNNING",
            "niche_dimension": dimension,
            "candidate_offset": candidate_offset,
            "candidate_maximum": 768,
            "development_only": True,
        }
        for lane, dimension in zip(
            ("EXPLOITATION", "EXPLORATION"), dimensions, strict=True
        )
    ]
    names = {
        card["lane_id"]: (
            f"epoch_{epoch:04d}_{card['niche_dimension'].lower()}_"
            f"{candidate_offset:06d}.json"
        )
        for card in cards
    }
    completed = {
        lane: _read_hashed(output / "branch_results" / name, "result_hash")
        for lane, name in names.items()
        if (output / "branch_results" / name).is_file()
    }
    if len(completed) == 2:
        results = {
            f"{epoch}:{lane}": value for lane, value in completed.items()
        }
        if _recurring_pair_exhausted(completed):
            state = _persist_source_bank_exhaustion(
                epoch=epoch,
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                initial_results=initial_results,
                prior_state=prior_state,
                started=started,
                dimensions=dimensions,
                candidate_offset=candidate_offset,
                completed=completed,
            )
            return state, results, True
        return dict(prior_state), results, False
    _begin_economic_phase()
    with ProcessPoolExecutor(
        max_workers=2, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        jobs = {
            pool.submit(
                _candidate_niche_feasibility,
                root,
                branch_id=card["branch_id"],
                niche=card["niche_dimension"],
                maximum_candidates=768,
                candidate_offset=candidate_offset,
            ): card["lane_id"]
            for card in cards
        }
        state = _state_payload(
            manifest,
            sequence=int(prior_state["checkpoint_sequence"]) + 1,
            state="ROBUSTNESS_ACTIVE",
            stage=f"ECONOMIC_EPOCH_{epoch:04d}_RUNNING",
            branch_results=initial_results,
            next_action="COMPLETE_TWO_DISTINCT_EXISTING_EVIDENCE_NICHE_SCREENS",
        )
        state["economic_epoch"] = epoch
        state["next_branch_cards"] = cards
        state["active_economic_worker_processes"] = 2
        state = _rehash(state, "state_hash")
        _publish(live_writer, state, _kpis(manifest, state, initial_results, started))

        completed = {}
        pending = set(jobs)
        while pending:
            done, pending = wait(
                pending,
                timeout=max(float(heartbeat_seconds), 1.0),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                state = dict(state)
                state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
                state["updated_at_utc"] = _utc_now()
                state = _rehash(state, "state_hash")
                _publish(
                    live_writer,
                    state,
                    _kpis(manifest, state, initial_results, started),
                )
                continue
            for future in done:
                lane = jobs[future]
                value = dict(future.result())
                value.update(
                    {
                        "schema": BRANCH_RESULT_SCHEMA,
                        "campaign_id": manifest["campaign_id"],
                        "manifest_hash": manifest["manifest_hash"],
                        "source_commit": manifest["source_commit"],
                        "lane_id": lane,
                        "economic_epoch": epoch,
                        "completed_at_utc": _utc_now(),
                        "read_only_worker": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                        "data_purchase_count": 0,
                    }
                )
                value = _with_hash(value, "result_hash")
                branch_writer.write_json(names[lane], value)
                completed[lane] = value
                _append_decision_once(root, manifest, value)
    _end_economic_phase()

    screened = sum(int(value.get("candidate_count", 0)) for value in completed.values())
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=f"ECONOMIC_EPOCH_{epoch:04d}_COMPLETE_NEXT_STARTING",
        branch_results=initial_results,
        next_action="START_NEXT_DISJOINT_MATERIALLY_DISTINCT_NICHE_EPOCH",
    )
    state["economic_epoch"] = epoch
    state["active_economic_worker_processes"] = 0
    state["successor_feasibility_screens_completed"] = int(
        prior_state.get("successor_feasibility_screens_completed", 0)
    ) + screened
    state["next_branch_cards"] = [
        {**card, "status": "COMPLETE"} for card in cards
    ]
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, initial_results, started))
    _write_mission_views(root, manifest, state, initial_results)
    results = {f"{epoch}:{key}": value for key, value in completed.items()}
    if _recurring_pair_exhausted(completed):
        state = _persist_source_bank_exhaustion(
            epoch=epoch,
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            initial_results=initial_results,
            prior_state=state,
            started=started,
            dimensions=dimensions,
            candidate_offset=candidate_offset,
            completed=completed,
        )
        return state, results, True
    return state, results, False


def _recurring_pair_exhausted(
    completed: Mapping[str, Mapping[str, Any]],
) -> bool:
    """Return true only for a complete two-lane shard with no candidates."""

    return set(completed) == {"EXPLOITATION", "EXPLORATION"} and all(
        value.get("status") == "COMPLETE_BOUNDED_EXISTING_EVIDENCE_FEASIBILITY"
        and int(value.get("candidate_count", 0)) == 0
        for value in completed.values()
    )


def _persist_source_bank_exhaustion(
    *,
    epoch: int,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    initial_results: Mapping[str, Mapping[str, Any]],
    prior_state: Mapping[str, Any],
    started: float,
    dimensions: tuple[str, str],
    candidate_offset: int,
    completed: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Seal one idempotent source-bank transition for successor dispatch."""

    receipt_name = "source_bank_exhausted.json"
    receipt_path = output / "branch_results" / receipt_name
    if receipt_path.is_file():
        receipt = _read_hashed(receipt_path, "result_hash")
        if (
            not _artifact_manifest_compatible(receipt, manifest)
            or receipt.get("decision") != "SOURCE_BANK_EXHAUSTED"
        ):
            raise AutonomousDirectorRuntimeError(
                "source-bank exhaustion receipt identity drift"
            )
    else:
        receipt = _with_hash(
            {
                "schema": BRANCH_RESULT_SCHEMA,
                "campaign_id": manifest["campaign_id"],
                "manifest_hash": manifest["manifest_hash"],
                "source_commit": manifest["source_commit"],
                "lane_id": "DIRECTOR",
                "branch_id": "EXISTING_EVIDENCE_SOURCE_BANK",
                "economic_epoch": int(epoch),
                "status": "SOURCE_BANK_EXHAUSTED",
                "decision": "SOURCE_BANK_EXHAUSTED",
                "candidate_offset": int(candidate_offset),
                "niche_dimensions": list(dimensions),
                "candidate_counts": {
                    lane: int(value.get("candidate_count", 0))
                    for lane, value in sorted(completed.items())
                },
                "completed_pair_result_hashes": {
                    lane: str(value.get("result_hash") or "")
                    for lane, value in sorted(completed.items())
                },
                "completed_at_utc": _utc_now(),
                "read_only_worker": False,
                "q4_access_count_delta": 0,
                "broker_connections": 0,
                "orders": 0,
                "data_purchase_count": 0,
                "evidence_tier": None,
                "promotion_status": None,
                "next_materially_distinct_action": (
                    "DISPATCH_SUCCESSOR_ECONOMIC_LANES"
                ),
            },
            "result_hash",
        )
        branch_writer.write_json(receipt_name, receipt)
    _append_decision_once(root, manifest, receipt)

    state = _state_payload(
        manifest,
        sequence=int(prior_state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="SOURCE_BANK_EXHAUSTED",
        branch_results=initial_results,
        next_action="DISPATCH_SUCCESSOR_ECONOMIC_LANES",
    )
    state["economic_epoch"] = int(epoch)
    state["active_economic_worker_processes"] = 0
    state["source_bank_exhausted"] = True
    state["source_bank_exhaustion_candidate_offset"] = int(candidate_offset)
    state["source_bank_exhaustion_receipt"] = {
        "path": f"branch_results/{receipt_name}",
        "result_hash": receipt["result_hash"],
    }
    state["successor_feasibility_screens_completed"] = int(
        prior_state.get("successor_feasibility_screens_completed", 0)
    )
    state["next_branch_cards"] = [
        {
            "lane_id": "DIRECTOR",
            "branch_id": "SUCCESSOR_ECONOMIC_LANES",
            "status": "READY",
            "source_decision": "SOURCE_BANK_EXHAUSTED",
        }
    ]
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, initial_results, started))
    _write_mission_views(root, manifest, state, initial_results)
    return state


def _run_post_source_exhaustion_epochs(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    initial_results: Mapping[str, Mapping[str, Any]],
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
) -> dict[str, Any]:
    """Consume the remaining immutable exact bank and one distinct event lane.

    Workers remain read-only.  The parent is the sole durable writer and every
    cohort is an immutable, resumable source-bank slice.  The first batch uses
    one exact worker plus the event-time worker; later batches use at most two
    exact workers.
    """

    branch_root = output / "branch_results"
    initial_path = branch_root / "epoch_0002_exact_0029_account_race.json"
    if not initial_path.is_file():
        raise AutonomousDirectorRuntimeError("sealed initial exact result missing")
    initial_exact = _read_hashed(initial_path, "result_hash")
    if not _artifact_manifest_compatible(initial_exact, manifest):
        raise AutonomousDirectorRuntimeError("initial exact result identity drift")

    relative_root = Path("post_source_exhaustion")
    completed: dict[int, dict[str, Any]] = {}
    for path in sorted((branch_root / relative_root).glob("exact_0029_offset_*.json")):
        envelope = _read_hashed(path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError("exact continuation identity drift")
        continuation = dict(envelope.get("continuation_result") or {})
        offset = int(continuation.get("cohort_offset", -1))
        if offset in completed:
            raise AutonomousDirectorRuntimeError("duplicate exact continuation offset")
        completed[offset] = continuation

    event_path = branch_root / relative_root / "v71_event_time_account_exploration.json"
    event_result: dict[str, Any] | None = None
    if event_path.is_file():
        envelope = _read_hashed(event_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError("event-time result identity drift")
        event_result = dict(envelope.get("event_time_result") or {})

    audit_path = branch_root / relative_root / "hazard_19327_tier_q_audit.json"
    if not audit_path.is_file():
        audit = audit_hazard_19327_tier_q(initial_exact)
        audit_envelope = _post_source_envelope(
            manifest,
            lane_id="EXPLOITATION",
            branch_id="HAZARD_19327_NO_RETUNE_TIER_Q_AUDIT",
            decision=str(audit["qualification_status"]),
            payload_key="qualification_audit",
            payload=audit,
            next_action=str(audit["next_action"]),
        )
        branch_writer.write_json(relative_root / audit_path.name, audit_envelope)
        _append_decision_once(root, manifest, audit_envelope)

    state = dict(prior_state)
    while True:
        plan = plan_remaining_0029_exact_jobs(
            root,
            completed_cohort_offsets=tuple(sorted(completed)),
            lane_count=2 if event_result is not None else 1,
        )
        jobs_to_run = list(plan["jobs"])
        if not jobs_to_run and event_result is not None:
            break

        future_kind: dict[Any, tuple[str, Any]] = {}
        _begin_economic_phase()
        worker_count = len(jobs_to_run) + int(event_result is None)
        with ProcessPoolExecutor(
            max_workers=max(1, min(worker_count, 2)),
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            for job in jobs_to_run:
                future = pool.submit(
                    remaining_0029_exact_worker, dict(job["worker_payload"])
                )
                future_kind[future] = ("EXACT", int(job["cohort_offset"]))
            if event_result is None:
                future = pool.submit(
                    event_time_account_exploration_worker,
                    {
                        "root": str(root),
                        "rule_snapshot_path": str(
                            manifest["official_rule_snapshot"]["path"]
                        ),
                    },
                )
                future_kind[future] = ("EVENT_TIME", None)

            runtime_results = _post_source_runtime_results(
                initial_results, initial_exact, completed, event_result
            )
            state = _state_payload(
                manifest,
                sequence=int(state["checkpoint_sequence"]) + 1,
                state="ROBUSTNESS_ACTIVE",
                stage="POST_SOURCE_EXHAUSTION_ECONOMIC_LANES_RUNNING",
                branch_results=runtime_results,
                next_action="COMPLETE_REMAINING_EXACT_BANK_AND_EVENT_TIME_REPLAY",
            )
            state["active_economic_worker_processes"] = len(future_kind)
            state["exact_0029_remaining_candidate_count"] = int(
                plan["source_inventory"]["remaining_exact_candidate_count"]
                - sum(len(row.get("candidate_ids") or ()) for row in completed.values())
            )
            state = _rehash(state, "state_hash")
            _publish(live_writer, state, _kpis(manifest, state, runtime_results, started))
            _write_mission_views(root, manifest, state, runtime_results)

            pending = set(future_kind)
            while pending:
                done, pending = wait(
                    pending,
                    timeout=max(float(heartbeat_seconds), 0.1),
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    state = dict(state)
                    state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
                    state["updated_at_utc"] = _utc_now()
                    state = _rehash(state, "state_hash")
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, runtime_results, started),
                    )
                    _write_mission_views(root, manifest, state, runtime_results)
                    continue
                for future in done:
                    kind, offset = future_kind[future]
                    worker_result = dict(future.result())
                    if kind == "EXACT":
                        assert offset is not None
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id="EXPLOITATION",
                            branch_id=f"REMAINING_EXACT_0029_OFFSET_{offset:04d}",
                            decision="COMPLETE_READ_ONLY_EXACT_CONTINUATION_COHORT",
                            payload_key="continuation_result",
                            payload=worker_result,
                            next_action="CONTINUE_DISJOINT_EXACT_SOURCE_BANK",
                        )
                        branch_writer.write_json(
                            relative_root / f"exact_0029_offset_{offset:04d}.json",
                            envelope,
                        )
                        _append_decision_once(root, manifest, envelope)
                        completed[offset] = worker_result
                    else:
                        event_result = worker_result
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id="EXPLORATION",
                            branch_id="V71_EVENT_TIME_ACCOUNT_SIZE_EXPLORATION",
                            decision=str(worker_result["decision"]),
                            payload_key="event_time_result",
                            payload=worker_result,
                            next_action="PRESERVE_TIER_E_AND_APPLY_FROZEN_MLL_GATE",
                        )
                        branch_writer.write_json(
                            relative_root / event_path.name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                runtime_results = _post_source_runtime_results(
                    initial_results, initial_exact, completed, event_result
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="POST_SOURCE_EXHAUSTION_ECONOMIC_LANES_RUNNING",
                    branch_results=runtime_results,
                    next_action="CONTINUE_STREAMING_EXACT_SOURCE_BANK",
                )
                state["active_economic_worker_processes"] = len(pending)
                state = _rehash(state, "state_hash")
                _publish(
                    live_writer,
                    state,
                    _kpis(manifest, state, runtime_results, started),
                )
                _write_mission_views(root, manifest, state, runtime_results)
        _end_economic_phase()

    runtime_results = _post_source_runtime_results(
        initial_results, initial_exact, completed, event_result
    )
    composite = runtime_results["EXACT_0029_COMPOSITE"]
    composite_path = branch_root / relative_root / "exact_0029_composite.json"
    if composite_path.is_file():
        final_envelope = _read_hashed(composite_path, "result_hash")
        if (
            not _artifact_manifest_compatible(final_envelope, manifest)
            or dict(final_envelope.get("exact_composite") or {}).get("result_hash")
            != composite.get("result_hash")
        ):
            raise AutonomousDirectorRuntimeError("exact composite identity drift")
    else:
        final_envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="EXACT_0029_SOURCE_BANK_COMPOSITE",
            decision=str(composite["status"]),
            payload_key="exact_composite",
            payload=composite,
            next_action="DISPATCH_NEXT_RESEARCH_BOARD_EPOCH",
        )
        branch_writer.write_json(
            relative_root / "exact_0029_composite.json", final_envelope
        )
        _append_decision_once(root, manifest, final_envelope)
    continuation_paths = tuple(
        branch_root
        / relative_root
        / f"exact_0029_offset_{offset:04d}.json"
        for offset in sorted(completed)
    )
    return _run_post_composite_economic_relay(
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        initial_results=initial_results,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
        initial_exact_path=initial_path,
        continuation_paths=continuation_paths,
        runtime_results=runtime_results,
    )


def _run_post_composite_economic_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    initial_results: Mapping[str, Mapping[str, Any]],
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    initial_exact_path: Path,
    continuation_paths: Sequence[Path],
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Classify Tier-Q and exact-replay one two-worker marginal book wave.

    Every worker is read-only.  The existing parent remains the sole writer,
    and immutable envelopes make both the qualification and book shards
    resumable after a controlled restart.
    """

    relative_root = Path("post_source_exhaustion/post_composite")
    branch_root = output / "branch_results"
    candidate_path = branch_root / relative_root / "combine_candidate_bank.json"
    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}

    if candidate_path.is_file():
        envelope = _read_hashed(candidate_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "Combine candidate-bank envelope identity drift"
            )
        candidate_bank = _verified_inner_result(
            envelope,
            key="candidate_bank",
            expected_schema="hydra_autonomous_combine_candidate_bank_v1",
            expected_status="COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
        )
    else:
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    _candidate_bank_from_artifacts_worker,
                    str(initial_exact_path),
                    tuple(str(value) for value in continuation_paths),
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="TIER_Q_CANDIDATE_BANK_CLASSIFICATION_RUNNING",
                    branch_results=results,
                    next_action="CLASSIFY_EXACT_COMBINE_SURVIVORS_WITHOUT_PROMOTION",
                )
                state["active_economic_worker_processes"] = 1
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                    state = dict(state)
                    state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
                    state["updated_at_utc"] = _utc_now()
                    state = _rehash(state, "state_hash")
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                candidate_bank = dict(future.result())
        finally:
            _end_economic_phase()
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="EXACT_COMBINE_CANDIDATE_BANK",
            decision=str(candidate_bank["status"]),
            payload_key="candidate_bank",
            payload=candidate_bank,
            next_action="RUN_TWO_SHARD_MARGINAL_COMBINE_BOOK_REPLAY",
        )
        branch_writer.write_json(relative_root / candidate_path.name, envelope)
        _append_decision_once(root, manifest, envelope)

    results["CANDIDATE_BANK"] = candidate_bank
    shard_results: dict[int, dict[str, Any]] = {}
    shard_paths = {
        index: branch_root / relative_root / f"marginal_books_shard_{index:02d}.json"
        for index in range(2)
    }
    for index, path in shard_paths.items():
        if not path.is_file():
            continue
        envelope = _read_hashed(path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "marginal-book shard envelope identity drift"
            )
        shard = _verified_inner_result(
            envelope,
            key="marginal_book_shard",
            expected_schema="hydra_autonomous_marginal_combine_books_v1",
            expected_status="COMPLETE_BOUNDED_EXACT_MARGINAL_COMBINE_BOOK_BATCH",
        )
        if (
            int(dict(shard.get("shard") or {}).get("shard_index", -1)) != index
            or int(dict(shard.get("shard") or {}).get("shard_count", -1)) != 2
        ):
            raise AutonomousDirectorRuntimeError(
                "marginal-book shard index/count drift"
            )
        shard_results[index] = shard

    missing = [index for index in range(2) if index not in shard_results]
    if missing:
        future_index: dict[Any, int] = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=len(missing),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                for index in missing:
                    future = pool.submit(
                        _marginal_books_from_artifacts_worker,
                        str(root),
                        str(candidate_path),
                        str(initial_exact_path),
                        tuple(str(value) for value in continuation_paths),
                        requested_book_count=256,
                        shard_index=index,
                        shard_count=2,
                    )
                    future_index[future] = index
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="MARGINAL_COMBINE_BOOK_EXACT_REPLAY_RUNNING",
                    branch_results=results,
                    next_action="EXACT_REPLAY_B1_B2_SELECTED_BOOKS_ON_B3_B4",
                )
                state["active_economic_worker_processes"] = len(future_index)
                state["tier_q_candidate_count"] = int(
                    dict(candidate_bank["counts"])["tier_q_contract_cleared_count"]
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_index)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = dict(state)
                        state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
                        state["updated_at_utc"] = _utc_now()
                        state = _rehash(state, "state_hash")
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        index = future_index[future]
                        shard = dict(future.result())
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION" if index == 0 else "EXPLORATION"
                            ),
                            branch_id=f"MARGINAL_COMBINE_BOOK_SHARD_{index:02d}",
                            decision=str(shard["status"]),
                            payload_key="marginal_book_shard",
                            payload=shard,
                            next_action="COMPOSE_DISJOINT_BOOK_SHARDS",
                        )
                        branch_writer.write_json(
                            relative_root / shard_paths[index].name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                        shard_results[index] = shard
                    state = dict(state)
                    state["active_economic_worker_processes"] = len(pending)
                    state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
                    state["updated_at_utc"] = _utc_now()
                    state = _rehash(state, "state_hash")
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    book_composite = compose_autonomous_marginal_combine_book_shards(
        [shard_results[index] for index in range(2)]
    )
    composite_path = branch_root / relative_root / "marginal_books_composite.json"
    if composite_path.is_file():
        envelope = _read_hashed(composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "marginal-book composite envelope identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="marginal_book_composite",
            expected_schema="hydra_autonomous_marginal_combine_book_shards_v1",
            expected_status="COMPLETE_RECONCILED_MARGINAL_COMBINE_BOOK_SHARDS",
        )
        if str(persisted["result_hash"]) != str(book_composite["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "marginal-book composite changed after persistence"
            )
        book_composite = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="MARGINAL_COMBINE_BOOK_COMPOSITE",
            decision=str(book_composite["status"]),
            payload_key="marginal_book_composite",
            payload=book_composite,
            next_action=str(book_composite["next_action"]),
        )
        branch_writer.write_json(relative_root / composite_path.name, envelope)
        _append_decision_once(root, manifest, envelope)

    # Preserve the first immutable book wave exactly as written.  A later
    # semantic reconciliation may legitimately produce a different summary
    # hash after a shared summarizer correction, so it must never overwrite
    # this development artifact.
    results["MARGINAL_BOOKS"] = book_composite
    counts = dict(book_composite["counts"])
    g_ready = int(counts.get("g_ready_count", 0)) + int(
        counts.get("standalone_g_ready_count", 0)
    )
    next_action = (
        "RUN_MATCHED_CONTROLS_AND_CONCENTRATION_FOR_G_READY_POLICIES"
        if g_ready
        else "DISPATCH_MATERIALLY_DISTINCT_FAILURE_GUIDED_ECONOMIC_BRANCH"
    )
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="MARGINAL_COMBINE_BOOK_WAVE_COMPLETE",
        branch_results=results,
        next_action=next_action,
    )
    state["active_economic_worker_processes"] = 0
    state["source_bank_exhausted"] = True
    state["exact_0029_source_bank_exhausted"] = True
    state["tier_q_candidate_count"] = int(
        dict(candidate_bank["counts"])["tier_q_contract_cleared_count"]
    )
    state["g_precontrol_ready_count"] = g_ready
    state["marginal_book_count"] = int(counts["primary_book_exact_replay_count"])
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return _run_post_book_graduation_relay(
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
        candidate_path=candidate_path,
        initial_exact_path=initial_exact_path,
        continuation_paths=continuation_paths,
        runtime_results=results,
        legacy_book_composite=book_composite,
    )


def _run_post_book_graduation_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    candidate_path: Path,
    initial_exact_path: Path,
    continuation_paths: Sequence[Path],
    runtime_results: Mapping[str, Mapping[str, Any]],
    legacy_book_composite: Mapping[str, Any],
) -> dict[str, Any]:
    """Reconcile book semantics, inventory honest passes, then test governors.

    The relay is deliberately read-only below the parent process.  It preserves
    the first marginal-book artifacts, writes a distinct replay after the
    account-summary semantic correction, and never turns a development pass or
    pre-control gate into an authoritative Tier-G promotion.
    """

    relative_root = Path("post_source_exhaustion/post_composite")
    branch_root = output / "branch_results"
    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    results["MARGINAL_BOOKS_LEGACY"] = dict(legacy_book_composite)

    semantic_shards: dict[int, dict[str, Any]] = {}
    semantic_paths = {
        index: branch_root
        / relative_root
        / f"marginal_books_semantic_reconciliation_shard_{index:02d}.json"
        for index in range(2)
    }
    for index, path in semantic_paths.items():
        if not path.is_file():
            continue
        shard = _read_relay_shard(
            path,
            manifest=manifest,
            key="semantic_marginal_book_shard",
            expected_schema="hydra_autonomous_marginal_combine_books_v1",
            expected_status="COMPLETE_BOUNDED_EXACT_MARGINAL_COMBINE_BOOK_BATCH",
            expected_index=index,
            expected_count=2,
            label="semantic-reconciliation book",
        )
        semantic_shards[index] = shard

    missing_semantic = [
        index for index in range(2) if index not in semantic_shards
    ]
    if missing_semantic:
        future_index: dict[Any, int] = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=len(missing_semantic),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                for index in missing_semantic:
                    future = pool.submit(
                        _marginal_books_from_artifacts_worker,
                        str(root),
                        str(candidate_path),
                        str(initial_exact_path),
                        tuple(str(value) for value in continuation_paths),
                        requested_book_count=256,
                        shard_index=index,
                        shard_count=2,
                    )
                    future_index[future] = index
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="MARGINAL_BOOK_SEMANTIC_RECONCILIATION_RUNNING",
                    branch_results=results,
                    next_action=(
                        "REPLAY_PATCHED_SUMMARIES_WITHOUT_OVERWRITING_LEGACY_EVIDENCE"
                    ),
                )
                state["active_economic_worker_processes"] = len(future_index)
                state["legacy_marginal_book_result_hash"] = str(
                    legacy_book_composite["result_hash"]
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_index)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = _heartbeat_state(state)
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        index = future_index[future]
                        shard = dict(future.result())
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION" if index == 0 else "EXPLORATION"
                            ),
                            branch_id=(
                                "MARGINAL_BOOK_SEMANTIC_RECONCILIATION_"
                                f"SHARD_{index:02d}"
                            ),
                            decision=str(shard["status"]),
                            payload_key="semantic_marginal_book_shard",
                            payload=shard,
                            next_action="COMPOSE_SEMANTIC_RECONCILIATION_SHARDS",
                        )
                        branch_writer.write_json(
                            relative_root / semantic_paths[index].name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                        semantic_shards[index] = shard
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    semantic_composite = compose_autonomous_marginal_combine_book_shards(
        [semantic_shards[index] for index in range(2)]
    )
    semantic_composite_path = (
        branch_root
        / relative_root
        / "marginal_books_semantic_reconciliation_composite.json"
    )
    if semantic_composite_path.is_file():
        envelope = _read_hashed(semantic_composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "semantic-reconciliation book composite identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="semantic_marginal_book_composite",
            expected_schema="hydra_autonomous_marginal_combine_book_shards_v1",
            expected_status="COMPLETE_RECONCILED_MARGINAL_COMBINE_BOOK_SHARDS",
        )
        if str(persisted["result_hash"]) != str(semantic_composite["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "semantic-reconciliation book composite changed after persistence"
            )
        semantic_composite = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="MARGINAL_BOOK_SEMANTIC_RECONCILIATION_COMPOSITE",
            decision=str(semantic_composite["status"]),
            payload_key="semantic_marginal_book_composite",
            payload=semantic_composite,
            next_action="BUILD_HONEST_COMBINE_PASS_OBSERVED_BANK",
        )
        branch_writer.write_json(
            relative_root / semantic_composite_path.name, envelope
        )
        _append_decision_once(root, manifest, envelope)
    results["MARGINAL_BOOKS"] = semantic_composite

    pass_bank_path = branch_root / relative_root / "combine_pass_observed_bank.json"
    if pass_bank_path.is_file():
        envelope = _read_hashed(pass_bank_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "Combine pass-observed bank envelope identity drift"
            )
        pass_bank = _verified_inner_result(
            envelope,
            key="combine_pass_observed_bank",
            expected_schema=COMBINE_PASS_BANK_SCHEMA,
            expected_status=(
                "COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK_TARGET_REACHED",
                "COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK_SHORTAGE",
            ),
        )
    else:
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    _combine_pass_bank_from_artifacts_worker,
                    str(candidate_path),
                    str(semantic_composite_path),
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="COMBINE_PASS_OBSERVED_BANK_BUILD_RUNNING",
                    branch_results=results,
                    next_action="DEDUPLICATE_EXACT_PASS_OBSERVED_DEVELOPMENT_POLICIES",
                )
                state["active_economic_worker_processes"] = 1
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                    state = _heartbeat_state(state)
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                pass_bank = dict(future.result())
        finally:
            _end_economic_phase()
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="COMBINE_PASS_OBSERVED_DEVELOPMENT_BANK",
            decision=str(pass_bank["status"]),
            payload_key="combine_pass_observed_bank",
            payload=pass_bank,
            next_action="RUN_TWO_SHARD_CONSISTENCY_DIRECT_ACCOUNT_REPLAY",
        )
        branch_writer.write_json(relative_root / pass_bank_path.name, envelope)
        _append_decision_once(root, manifest, envelope)
    results["PASS_OBSERVED_BANK"] = pass_bank

    direct_shards: dict[int, dict[str, Any]] = {}
    direct_paths = {
        index: branch_root
        / relative_root
        / f"consistency_direct_shard_{index:02d}.json"
        for index in range(2)
    }
    for index, path in direct_paths.items():
        if not path.is_file():
            continue
        shard = _read_relay_shard(
            path,
            manifest=manifest,
            key="consistency_direct_shard",
            expected_schema=CONSISTENCY_DIRECT_SHARD_SCHEMA,
            expected_status="COMPLETE_BOUNDED_CONSISTENCY_DIRECT_ACCOUNT_SHARD",
            expected_index=index,
            expected_count=2,
            label="consistency-direct",
        )
        direct_shards[index] = shard

    missing_direct = [index for index in range(2) if index not in direct_shards]
    if missing_direct:
        future_index = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=len(missing_direct),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                for index in missing_direct:
                    future = pool.submit(
                        _consistency_direct_from_artifacts_worker,
                        str(root),
                        str(candidate_path),
                        str(initial_exact_path),
                        tuple(str(value) for value in continuation_paths),
                        maximum_candidates=64,
                        shard_index=index,
                        shard_count=2,
                    )
                    future_index[future] = index
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="CONSISTENCY_DIRECT_ACCOUNT_POLICY_REPLAY_RUNNING",
                    branch_results=results,
                    next_action=(
                        "TEST_BOUNDED_ACCOUNT_CONSISTENCY_GOVERNORS_ON_TIER_Q"
                    ),
                )
                state["active_economic_worker_processes"] = len(future_index)
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_index)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = _heartbeat_state(state)
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        index = future_index[future]
                        shard = dict(future.result())
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION" if index == 0 else "EXPLORATION"
                            ),
                            branch_id=f"CONSISTENCY_DIRECT_SHARD_{index:02d}",
                            decision=str(shard["status"]),
                            payload_key="consistency_direct_shard",
                            payload=shard,
                            next_action="COMPOSE_CONSISTENCY_DIRECT_SHARDS",
                        )
                        branch_writer.write_json(
                            relative_root / direct_paths[index].name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                        direct_shards[index] = shard
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    direct_composite = compose_autonomous_consistency_account_policy_shards(
        [direct_shards[index] for index in range(2)]
    )
    direct_composite_path = (
        branch_root / relative_root / "consistency_direct_composite.json"
    )
    if direct_composite_path.is_file():
        envelope = _read_hashed(direct_composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "consistency-direct composite identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="consistency_direct_composite",
            expected_schema=CONSISTENCY_DIRECT_COMPOSITE_SCHEMA,
            expected_status="COMPLETE_RECONCILED_CONSISTENCY_DIRECT_ACCOUNT_SHARDS",
        )
        if str(persisted["result_hash"]) != str(direct_composite["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "consistency-direct composite changed after persistence"
            )
        direct_composite = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="CONSISTENCY_DIRECT_ACCOUNT_POLICY_COMPOSITE",
            decision=str(direct_composite["status"]),
            payload_key="consistency_direct_composite",
            payload=direct_composite,
            next_action=str(direct_composite["next_action"]),
        )
        branch_writer.write_json(
            relative_root / direct_composite_path.name, envelope
        )
        _append_decision_once(root, manifest, envelope)
    results["CONSISTENCY_DIRECT"] = direct_composite

    state, results, event_safety_composite = _run_event_time_safety_relay(
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
        runtime_results=results,
    )
    (
        state,
        results,
        tier_g_control_composite,
        tier_g_graduation,
    ) = _run_tier_g_control_relay(
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
        runtime_results=results,
        candidate_bank_path=candidate_path,
        initial_exact_path=initial_exact_path,
        continuation_paths=continuation_paths,
    )
    state, results, xfa_handoff, xfa_diagnostic, breadth_tripwire = (
        _run_tier_g_xfa_and_breadth_relay(
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            prior_state=state,
            started=started,
            heartbeat_seconds=heartbeat_seconds,
            runtime_results=results,
            candidate_bank_path=candidate_path,
            initial_exact_path=initial_exact_path,
            continuation_paths=continuation_paths,
        )
    )

    book_counts = dict(semantic_composite.get("counts") or {})
    direct_counts = dict(direct_composite.get("counts") or {})
    event_safety_counts = dict(event_safety_composite.get("counts") or {})
    tier_g_control_counts = dict(tier_g_control_composite.get("counts") or {})
    tier_g_graduation_counts = dict(tier_g_graduation.get("counts") or {})
    xfa_handoff_counts = dict(xfa_handoff.get("counts") or {})
    xfa_diagnostic_counts = dict(xfa_diagnostic.get("counts") or {})
    breadth_counts = dict(breadth_tripwire.get("counts") or {})
    breadth_gate = dict(breadth_tripwire.get("gate") or {})
    pass_counts = dict(pass_bank.get("counts") or {})
    book_ready = int(book_counts.get("g_ready_count", 0)) + int(
        book_counts.get("standalone_g_ready_count", 0)
    )
    direct_ready = int(direct_counts.get("g_precontrol_ready_count", 0))
    event_safety_ready = int(
        event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
    )
    g_precontrol = book_ready + direct_ready + event_safety_ready
    tier_g_control_ready = int(
        tier_g_control_counts.get("g_control_ready_count", 0)
    )
    graduated_tier_g = int(
        tier_g_graduation_counts.get("graduated_development_book_count", 0)
    )
    next_action = (
        "FREEZE_AND_RUN_ONE_UNTOUCHED_CONFIRMATION_FOR_TIER_G_AND_BREADTH_QUALIFIER"
        if graduated_tier_g
        else (
            "RUN_TRADE_CONCENTRATION_AND_MATCHED_CONTROLS_FOR_PRECONTROL_SURVIVORS"
            if g_precontrol
            else "DISPATCH_MATERIALLY_DISTINCT_FAILURE_GUIDED_ECONOMIC_BRANCH"
        )
    )
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="POST_TIER_G_XFA_AND_BREADTH_RELAYS_COMPLETE",
        branch_results=results,
        next_action=next_action,
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "source_bank_exhausted": True,
            "exact_0029_source_bank_exhausted": True,
            "tier_q_candidate_count": int(
                dict(results["CANDIDATE_BANK"]["counts"])[
                    "tier_q_contract_cleared_count"
                ]
            ),
            "combine_pass_observed_bank_count": int(
                pass_counts.get("bank_policy_count", 0)
            ),
            "combine_pass_observed_shortage": int(
                pass_counts.get("shortage_to_minimum_target", 0)
            ),
            "semantic_reconciliation_book_count": int(
                book_counts.get("primary_book_exact_replay_count", 0)
            ),
            "consistency_direct_policy_exact_replay_count": int(
                direct_counts.get("direct_policy_exact_replay_count", 0)
            ),
            "consistency_direct_identity_control_count": int(
                direct_counts.get("identity_control_exact_replay_count", 0)
            ),
            "consistency_direct_g_precontrol_ready_count": direct_ready,
            "event_time_safety_candidate_count": int(
                event_safety_counts.get("selected_candidate_count", 0)
            ),
            "event_time_safety_profile_count": int(
                event_safety_counts.get("profile_count", 0)
            ),
            "event_time_safety_exact_episode_count": int(
                event_safety_counts.get("exact_episode_count", 0)
            ),
            "event_time_safety_g_precontrol_ready_count": event_safety_ready,
            "g_precontrol_ready_count": g_precontrol,
            "tier_g_control_candidate_count": int(
                tier_g_control_counts.get("selected_candidate_count", 0)
            ),
            "tier_g_control_exact_replay_count": int(
                tier_g_control_counts.get("exact_account_replay_count", 0)
            ),
            "tier_g_control_synthetic_count": int(
                tier_g_control_counts.get("synthetic_control_count", 0)
            ),
            "tier_g_control_ready_count": tier_g_control_ready,
            "authoritative_tier_g_count": graduated_tier_g,
            "combine_to_xfa_transition_count": int(
                xfa_handoff_counts.get("ready_xfa_transition_count", 0)
            ),
            "xfa_paths_started": int(
                xfa_diagnostic_counts.get("alternative_path_count", 0)
            ),
            "xfa_alternative_path_count": int(
                xfa_diagnostic_counts.get("alternative_path_count", 0)
            ),
            "xfa_standard_path_count": int(
                xfa_diagnostic_counts.get("standard_path_count", 0)
            ),
            "xfa_consistency_path_count": int(
                xfa_diagnostic_counts.get("consistency_path_count", 0)
            ),
            "xfa_standard_first_payout_count": int(
                xfa_diagnostic_counts.get("standard_first_payout_count", 0)
            ),
            "xfa_consistency_first_payout_count": int(
                xfa_diagnostic_counts.get("consistency_first_payout_count", 0)
            ),
            "cross_index_breadth_primary_count": int(
                breadth_counts.get("primary_candidate_count", 0)
            ),
            "cross_index_breadth_control_count": int(
                breadth_counts.get("control_candidate_count", 0)
            ),
            "cross_index_breadth_exact_account_replay_count": int(
                breadth_counts.get("exact_account_replays", 0)
            ),
            "cross_index_breadth_qualifying_cell_count": int(
                breadth_gate.get("qualifying_cell_count", 0)
            ),
            "cross_index_breadth_status": str(breadth_tripwire["status"]),
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    state, results, _fresh_confirmation = _run_fresh_confirmation_relay(
        root=root,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        prior_state=state,
        started=started,
        heartbeat_seconds=heartbeat_seconds,
        runtime_results=results,
    )
    if manifest.get("post_confirmation_branch_portfolio"):
        state, results, _event_time_controls = _run_event_time_matched_controls_relay(
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            prior_state=state,
            started=started,
            heartbeat_seconds=heartbeat_seconds,
            runtime_results=results,
        )
        state, results, _breadth_continuation = _run_frozen_breadth_continuation_relay(
            root=root,
            manifest=manifest,
            output=output,
            live_writer=live_writer,
            branch_writer=branch_writer,
            prior_state=state,
            started=started,
            heartbeat_seconds=heartbeat_seconds,
            runtime_results=results,
        )
        state, results, _post_breadth_portfolio = (
            _run_post_breadth_portfolio_relay(
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                prior_state=state,
                started=started,
                heartbeat_seconds=heartbeat_seconds,
                runtime_results=results,
            )
        )
        state, results, _session_safe_portfolio = (
            _run_session_safe_fast_book_relay(
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                prior_state=state,
                started=started,
                heartbeat_seconds=heartbeat_seconds,
                runtime_results=results,
            )
        )
        state, results, _session_safe_confirmation = (
            _run_session_safe_m2k_mym_confirmation_relay(
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                prior_state=state,
                started=started,
                heartbeat_seconds=heartbeat_seconds,
                runtime_results=results,
            )
        )
        state, results, _cross_asset_daily_tripwire = (
            _run_clean_cross_asset_daily_tripwire_relay(
                root=root,
                manifest=manifest,
                output=output,
                live_writer=live_writer,
                branch_writer=branch_writer,
                prior_state=state,
                started=started,
                heartbeat_seconds=heartbeat_seconds,
                runtime_results=results,
            )
        )
    elif os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
        raise AutonomousDirectorRuntimeError(
            "post-confirmation branch portfolio is undeclared"
        )
    if os.environ.get("HYDRA_PRODUCTION_TEST_MODE") == "1":
        return state

    # This manifest is intentionally persistent and has no terminal result.
    # Returning here makes V17 relaunch the same completed checkpoint forever.
    # Keep the single runner alive after the bounded branch decision so the
    # controller remains healthy while the next frozen branch is prepared.
    while True:
        time.sleep(max(float(heartbeat_seconds), 1.0))
        state = _heartbeat_state(state, active_economic_worker_processes=0)
        _publish(live_writer, state, _kpis(manifest, state, results, started))
        _write_mission_views(root, manifest, state, results)


def _run_tier_g_xfa_and_breadth_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
    candidate_bank_path: Path,
    initial_exact_path: Path,
    continuation_paths: Sequence[Path],
) -> tuple[
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    """Persist one bounded XFA diagnostic and one distinct breadth tripwire.

    The handoff reconstruction and breadth experiment are independent and run
    concurrently when both are absent.  XFA simulation begins only after the
    parent has durably persisted the verified handoff.  Workers receive no
    writer, database, registry, broker, or order capability; all envelopes are
    written by this parent process.
    """

    relative_root = Path("post_source_exhaustion/post_composite")
    branch_root = output / "branch_results"
    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    graduation = dict(results.get("TIER_G_GRADUATION") or {})
    graduation_count = int(
        dict(graduation.get("counts") or {}).get(
            "graduated_development_book_count", 0
        )
    )
    graduation_path = (
        branch_root / relative_root / "tier_g_development_graduation.json"
    )
    handoff_path = branch_root / relative_root / "tier_g_xfa_handoff.json"
    diagnostic_path = branch_root / relative_root / "tier_g_xfa_diagnostic.json"
    breadth_path = branch_root / relative_root / "cross_index_breadth_tripwire.json"

    handoff: dict[str, Any] = {}
    breadth: dict[str, Any] = {}
    if handoff_path.is_file():
        envelope = _read_hashed(handoff_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError("Tier-G XFA handoff identity drift")
        handoff = _verified_inner_result(
            envelope,
            key="tier_g_xfa_handoff",
            expected_schema=TIER_G_XFA_HANDOFF_SCHEMA,
            expected_status=TIER_G_XFA_HANDOFF_STATUS,
        )
        verify_tier_g_combine_xfa_handoffs(handoff)
    if breadth_path.is_file():
        envelope = _read_hashed(breadth_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "cross-index breadth tripwire identity drift"
            )
        breadth = _verify_breadth_tripwire_result(
            dict(envelope.get("cross_index_breadth_tripwire") or {})
        )

    jobs_to_run: list[str] = []
    if graduation_count and not handoff:
        jobs_to_run.append("XFA_HANDOFF")
    if not breadth:
        jobs_to_run.append("BREADTH")
    if jobs_to_run:
        future_kind: dict[Any, str] = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=min(len(jobs_to_run), 2),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                if "XFA_HANDOFF" in jobs_to_run:
                    future_kind[
                        pool.submit(
                            _tier_g_xfa_handoff_from_artifacts_worker,
                            str(root),
                            str(candidate_bank_path),
                            str(initial_exact_path),
                            tuple(str(value) for value in continuation_paths),
                            str(graduation_path),
                        )
                    ] = "XFA_HANDOFF"
                if "BREADTH" in jobs_to_run:
                    future_kind[
                        pool.submit(_cross_index_breadth_tripwire_worker, str(root))
                    ] = "BREADTH"
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="TIER_G_XFA_HANDOFF_AND_BREADTH_RUNNING",
                    branch_results=results,
                    next_action=(
                        "RECONSTRUCT_IMMUTABLE_COMBINE_TRANSITIONS_AND_RUN_"
                        "DISTINCT_BREADTH_TRIPWIRE"
                    ),
                )
                state["active_economic_worker_processes"] = len(future_kind)
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_kind)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = _heartbeat_state(
                            state,
                            active_economic_worker_processes=len(pending),
                        )
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        kind = future_kind[future]
                        value = dict(future.result())
                        if kind == "XFA_HANDOFF":
                            handoff = verify_tier_g_combine_xfa_handoffs(value)
                            envelope = _post_source_envelope(
                                manifest,
                                lane_id="EXPLOITATION",
                                branch_id="TIER_G_ACCOUNT_SIZE_AWARE_XFA_HANDOFF",
                                decision=str(handoff["status"]),
                                payload_key="tier_g_xfa_handoff",
                                payload=handoff,
                                next_action=str(handoff["next_action"]),
                            )
                            branch_writer.write_json(
                                relative_root / handoff_path.name, envelope
                            )
                            _append_decision_once(root, manifest, envelope)
                            results["TIER_G_XFA_HANDOFF"] = handoff
                        else:
                            breadth = _verify_breadth_tripwire_result(value)
                            envelope = _post_source_envelope(
                                manifest,
                                lane_id="EXPLORATION",
                                branch_id="CROSS_INDEX_BREADTH_TRIPWIRE",
                                decision=str(breadth["status"]),
                                payload_key="cross_index_breadth_tripwire",
                                payload=breadth,
                                next_action=str(breadth["next_action"]),
                            )
                            branch_writer.write_json(
                                relative_root / breadth_path.name, envelope
                            )
                            _append_decision_once(root, manifest, envelope)
                            results["CROSS_INDEX_BREADTH"] = breadth
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    if handoff:
        results["TIER_G_XFA_HANDOFF"] = handoff
    results["CROSS_INDEX_BREADTH"] = breadth

    diagnostic: dict[str, Any] = {}
    if graduation_count:
        if not handoff:
            raise AutonomousDirectorRuntimeError(
                "graduated Tier-G books lack a verified XFA handoff"
            )
        if diagnostic_path.is_file():
            envelope = _read_hashed(diagnostic_path, "result_hash")
            if not _artifact_manifest_compatible(envelope, manifest):
                raise AutonomousDirectorRuntimeError(
                    "Tier-G XFA diagnostic identity drift"
                )
            diagnostic = _verified_inner_result(
                envelope,
                key="tier_g_xfa_diagnostic",
                expected_schema=TIER_G_XFA_DIAGNOSTIC_SCHEMA,
                expected_status=TIER_G_XFA_DIAGNOSTIC_STATUS,
            )
            verify_autonomous_tier_g_xfa_diagnostic(diagnostic)
        else:
            _begin_economic_phase()
            try:
                with ProcessPoolExecutor(
                    max_workers=1,
                    mp_context=multiprocessing.get_context("spawn"),
                ) as pool:
                    future = pool.submit(
                        _tier_g_xfa_diagnostic_from_artifact_worker,
                        str(handoff_path),
                    )
                    state = _state_payload(
                        manifest,
                        sequence=int(state["checkpoint_sequence"]) + 1,
                        state="ROBUSTNESS_ACTIVE",
                        stage="TIER_G_XFA_ALTERNATIVE_DIAGNOSTICS_RUNNING",
                        branch_results=results,
                        next_action=(
                            "SIMULATE_STANDARD_AND_CONSISTENCY_AS_SEPARATE_"
                            "DIAGNOSTIC_ALTERNATIVES"
                        ),
                    )
                    state["active_economic_worker_processes"] = 1
                    state = _rehash(state, "state_hash")
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                    while not future.done():
                        time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                        state = _heartbeat_state(
                            state, active_economic_worker_processes=1
                        )
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                    diagnostic = verify_autonomous_tier_g_xfa_diagnostic(
                        dict(future.result())
                    )
            finally:
                _end_economic_phase()
            envelope = _post_source_envelope(
                manifest,
                lane_id="EXPLOITATION",
                branch_id="TIER_G_SEPARATE_XFA_ALTERNATIVE_DIAGNOSTIC",
                decision=str(diagnostic["status"]),
                payload_key="tier_g_xfa_diagnostic",
                payload=diagnostic,
                next_action=str(diagnostic["next_action"]),
            )
            branch_writer.write_json(
                relative_root / diagnostic_path.name, envelope
            )
            _append_decision_once(root, manifest, envelope)
        results["TIER_G_XFA_DIAGNOSTIC"] = diagnostic

    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="TIER_G_XFA_AND_BREADTH_RELAYS_PERSISTED",
        branch_results=results,
        next_action="FREEZE_ONE_UNTOUCHED_CONFIRMATION_CONTRACT",
    )
    state["active_economic_worker_processes"] = 0
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, handoff, diagnostic, breadth


def _run_fresh_confirmation_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Consume one prewritten frozen confirmation package exactly once.

    This relay deliberately has no acquisition, feature-building, cache-writing,
    database, registry, broker, or order capability.  Its sole worker opens the
    immutable YM/ES feature bundles read-only and evaluates the already-frozen
    contract.  The parent remains the only writer and persists one envelope.
    """

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    paths, missing_declarations = _fresh_confirmation_manifest_paths(root, manifest)
    result_path = paths.get("result_path")
    confirmation: dict[str, Any] = {}

    if result_path is not None and result_path.is_file():
        envelope = _read_hashed(result_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation result identity drift"
            )
        confirmation = _verify_fresh_confirmation_result(
            _verified_inner_result(
                envelope,
                key="fresh_confirmation_result",
                expected_schema=FRESH_CONFIRMATION_RESULT_SCHEMA,
                expected_status="CONFIRMATION_CONSUMED_ONCE",
            )
        )
        results["FRESH_CONFIRMATION"] = confirmation

    required_inputs = (
        "contract_path",
        "acquisition_receipt_path",
        "feature_receipt_path",
        "result_path",
    )
    missing_inputs = list(missing_declarations)
    for key in required_inputs[:-1]:
        path = paths.get(key)
        if path is not None and not path.is_file():
            missing_inputs.append(key)
    missing_inputs = sorted(set(missing_inputs))

    if not confirmation and missing_inputs:
        state = _state_payload(
            manifest,
            sequence=int(state["checkpoint_sequence"]) + 1,
            state="ROBUSTNESS_ACTIVE",
            stage="FRESH_CONFIRMATION_ACQUISITION_REQUIRED",
            branch_results=results,
            next_action=(
                "COMPLETE_PREWRITTEN_FRESH_CONFIRMATION_INPUTS_THEN_RESUME_"
                "THE_SAME_BOUNDED_RELAY"
            ),
        )
        state.update(
            {
                "active_economic_worker_processes": 0,
                "fresh_confirmation_fail_closed": True,
                "fresh_confirmation_missing_inputs": missing_inputs,
                "fresh_confirmation_runtime_network_access": False,
                "fresh_confirmation_runtime_feature_cache_writes": 0,
            }
        )
        state = _rehash(state, "state_hash")
        _publish(live_writer, state, _kpis(manifest, state, results, started))
        _write_mission_views(root, manifest, state, results)
        return state, results, {}

    if not confirmation:
        if result_path is None or result_path.name != "fresh_confirmation_result.json":
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation result path must end in fresh_confirmation_result.json"
            )
        branch_root = (output / "branch_results").resolve()
        try:
            relative_result_path = result_path.resolve().relative_to(branch_root)
        except ValueError as exc:
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation result path must remain under branch_results"
            ) from exc

        contract = _read_json_object(paths["contract_path"])
        expected_contract_hash = str(
            dict(
                manifest.get("fresh_confirmation")
                or manifest.get("fresh_confirmation_contract")
                or {}
            ).get("contract_hash")
            or ""
        )
        if expected_contract_hash and str(contract.get("contract_hash") or "") != expected_contract_hash:
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation contract hash differs from manifest freeze"
            )

        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    _fresh_confirmation_worker,
                    str(paths["contract_path"]),
                    str(paths["acquisition_receipt_path"]),
                    str(paths["feature_receipt_path"]),
                    expected_contract_hash,
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="FRESH_CONFIRMATION_READ_ONLY_EVALUATION_RUNNING",
                    branch_results=results,
                    next_action="EVALUATE_FROZEN_CONFIRMATION_ONCE_WITHOUT_RETUNING",
                )
                state.update(
                    {
                        "active_economic_worker_processes": 1,
                        "fresh_confirmation_runtime_network_access": False,
                        "fresh_confirmation_runtime_feature_cache_writes": 0,
                    }
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                    state = _heartbeat_state(
                        state, active_economic_worker_processes=1
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                confirmation = _verify_fresh_confirmation_result(
                    dict(future.result())
                )
        finally:
            _end_economic_phase()

        tier_c_count = len(confirmation["tier_c_candidate_ids"])
        next_action = (
            "PROVE_F0_FOR_CONFIRMED_TIER_C_BOOKS"
            if tier_c_count
            else "DISPATCH_MATERIALLY_DISTINCT_EXPLORATION_AFTER_CONFIRMATION_FAILURE"
        )
        envelope = _post_source_envelope(
            manifest,
            lane_id="EXPLOITATION",
            branch_id="FROZEN_TIER_G_FRESH_CONFIRMATION",
            decision=str(confirmation["status"]),
            payload_key="fresh_confirmation_result",
            payload=confirmation,
            next_action=next_action,
        )
        envelope_payload = dict(envelope)
        envelope_payload.pop("result_hash", None)
        envelope_payload["evidence_tier"] = (
            "C" if tier_c_count else "G_CONFIRMATION_FAILED"
        )
        envelope = _with_hash(envelope_payload, "result_hash")
        branch_writer.write_json(relative_result_path, envelope)
        _append_decision_once(root, manifest, envelope)
        results["FRESH_CONFIRMATION"] = confirmation

    tier_c_count = len(confirmation.get("tier_c_candidate_ids") or ())
    next_action = (
        "PROVE_F0_FOR_CONFIRMED_TIER_C_BOOKS"
        if tier_c_count
        else "DISPATCH_MATERIALLY_DISTINCT_EXPLORATION_AFTER_CONFIRMATION_FAILURE"
    )
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=(
            "FRESH_CONFIRMATION_COMPLETE_TIER_C_GATE_PASSED"
            if tier_c_count
            else "FRESH_CONFIRMATION_COMPLETE_NO_TIER_C_PASSERS"
        ),
        branch_results=results,
        next_action=next_action,
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "independently_confirmed_tier_c_count": tier_c_count,
            "forward_tier_f_count": 0,
            "fresh_confirmation_fail_closed": False,
            "fresh_confirmation_runtime_network_access": False,
            "fresh_confirmation_runtime_feature_cache_writes": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, confirmation


def _run_event_time_matched_controls_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Run/adopt the bounded compute-only event-time matched controls."""

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    section = dict(manifest.get("post_confirmation_branch_portfolio") or {})
    raw_path = str(section.get("event_time_controls_result_path") or "").strip()
    if not raw_path:
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control result path is undeclared"
        )
    result_path = (root / raw_path).resolve()
    try:
        result_path.relative_to(root.resolve())
    except ValueError as exc:
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control result path escapes repository"
        ) from exc

    if result_path.is_file():
        result = _verify_event_time_matched_controls_result(
            _read_hashed(result_path, "result_hash")
        )
    else:
        relative = _relative_branch_artifact_path(
            output, result_path, "event-time matched-control result"
        )
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    event_time_matched_controls_worker,
                    {
                        "root": str(root),
                        "source_composite_path": str(
                            section.get("event_time_controls_source_path") or ""
                        ),
                        "rule_snapshot_path": str(
                            manifest["official_rule_snapshot"]["path"]
                        ),
                    },
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="EVENT_TIME_MATCHED_CONTROLS_RUNNING",
                    branch_results=results,
                    next_action="RUN_FROZEN_EVENT_TIME_MATCHED_CONTROLS",
                )
                state["active_economic_worker_processes"] = 1
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                    state = _heartbeat_state(
                        state, active_economic_worker_processes=1
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                result = _verify_event_time_matched_controls_result(
                    dict(future.result())
                )
        finally:
            _end_economic_phase()
        branch_writer.write_json(relative, result)

    expected_hash = str(section.get("event_time_controls_expected_hash") or "")
    if expected_hash and str(result.get("result_hash") or "") != expected_hash:
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control result differs from manifest freeze"
        )
    results["EVENT_TIME_MATCHED_CONTROLS"] = result
    envelope = _post_source_envelope(
        manifest,
        lane_id="EXPLORATION",
        branch_id="FROZEN_EVENT_TIME_MATCHED_CONTROLS",
        decision=str(result["control_verdict"]),
        payload_key="event_time_matched_controls",
        payload=result,
        next_action=str(result["next_action"]),
    )
    envelope_payload = dict(envelope)
    envelope_payload.pop("result_hash", None)
    envelope_payload["evidence_tier"] = "E_CONTROL_DIAGNOSTIC"
    envelope = _with_hash(envelope_payload, "result_hash")
    _append_decision_once(root, manifest, envelope)

    counts = _event_time_control_counts(result)
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=str(result["control_verdict"]),
        branch_results=results,
        next_action=str(result["next_action"]),
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "event_time_matched_control_count": counts["control_count"],
            "event_time_matched_control_exact_episode_count": counts[
                "exact_episode_count"
            ],
            "event_time_matched_controls_status": str(result["control_verdict"]),
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, result


def _run_frozen_breadth_continuation_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Run the singleton frozen Q3 breadth continuation exactly once.

    Network acquisition is deliberately external and manifest-bound.  The
    parent process is the only feature/evidence writer; the spawned worker
    receives immutable matrices and performs only economic replay.  A success
    is capped at Tier G because this candidate was not already graduated when
    the Q3 block was frozen.
    """

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    paths, missing_declarations = _frozen_breadth_manifest_paths(root, manifest)
    result_path = paths.get("result_path")
    result: dict[str, Any] = {}

    if result_path is not None and result_path.is_file():
        envelope = _read_hashed(result_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "frozen-breadth continuation result identity drift"
            )
        result = _verify_frozen_breadth_continuation_result(
            _verified_inner_result(
                envelope,
                key="frozen_breadth_continuation",
                expected_schema=FROZEN_BREADTH_CONTINUATION_SCHEMA,
                expected_status=str(
                    dict(envelope.get("frozen_breadth_continuation") or {}).get(
                        "status"
                    )
                ),
            )
        )
        results["FROZEN_BREADTH_CONTINUATION"] = result

    required = (
        "contract_path",
        "acquisition_receipt_path",
        "feature_receipt_path",
        "result_path",
    )
    missing_inputs = list(missing_declarations)
    for key in required[:2]:
        path = paths.get(key)
        if path is not None and not path.is_file():
            missing_inputs.append(key)
    missing_inputs = sorted(set(missing_inputs))

    if not result and missing_inputs:
        state = _state_payload(
            manifest,
            sequence=int(state["checkpoint_sequence"]) + 1,
            state="ROBUSTNESS_ACTIVE",
            stage="FROZEN_BREADTH_Q3_ACQUISITION_REQUIRED",
            branch_results=results,
            next_action="ACQUIRE_EXACT_FROZEN_Q3_BREADTH_BUNDLE_THEN_RESUME",
        )
        state.update(
            {
                "active_economic_worker_processes": 0,
                "frozen_breadth_missing_inputs": missing_inputs,
                "frozen_breadth_status": "FROZEN_AWAITING_ACQUISITION",
                "frozen_breadth_runtime_network_access": False,
                "q4_access_count_delta": 0,
            }
        )
        state = _rehash(state, "state_hash")
        _publish(live_writer, state, _kpis(manifest, state, results, started))
        _write_mission_views(root, manifest, state, results)
        return state, results, {}

    if not result:
        feature_path = paths["feature_receipt_path"]
        contract = _read_json_object(paths["contract_path"])
        acquisition = _read_json_object(paths["acquisition_receipt_path"])
        expected_contract_hash = str(
            dict(
                manifest.get("post_confirmation_branch_portfolio") or {}
            ).get("breadth_contract_hash")
            or ""
        )
        if expected_contract_hash and str(contract.get("contract_hash") or "") != expected_contract_hash:
            raise AutonomousDirectorRuntimeError(
                "frozen-breadth contract differs from manifest freeze"
            )
        if not feature_path.is_file():
            inputs = dict(acquisition.get("feature_build_inputs") or {})
            if not inputs:
                raise AutonomousDirectorRuntimeError(
                    "frozen-breadth acquisition lacks feature-build inputs"
                )
            _begin_economic_phase()
            try:
                feature_receipt = build_breadth_feature_bundles(
                    contract,
                    source_files=list(inputs.get("source_files") or ()),
                    contract_map_path=str(inputs.get("contract_map_path") or ""),
                    cache_root=str(inputs.get("cache_root") or ""),
                )
            finally:
                _end_economic_phase()
            relative_feature = _relative_branch_artifact_path(
                output, feature_path, "frozen-breadth feature receipt"
            )
            branch_writer.write_json(relative_feature, feature_receipt)

        if result_path is None:
            raise AutonomousDirectorRuntimeError(
                "frozen-breadth result path is undeclared"
            )
        relative_result = _relative_branch_artifact_path(
            output, result_path, "frozen-breadth result"
        )
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    _frozen_breadth_continuation_worker,
                    str(paths["contract_path"]),
                    str(paths["acquisition_receipt_path"]),
                    str(paths["feature_receipt_path"]),
                    expected_contract_hash,
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="FROZEN_BREADTH_Q3_ECONOMIC_REPLAY_RUNNING",
                    branch_results=results,
                    next_action="EVALUATE_SINGLETON_BREADTH_CANDIDATE_WITHOUT_RETUNING",
                )
                state.update(
                    {
                        "active_economic_worker_processes": 1,
                        "frozen_breadth_runtime_network_access": False,
                        "frozen_breadth_runtime_feature_cache_writes": 0,
                    }
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    time.sleep(max(min(float(heartbeat_seconds), 5.0), 0.1))
                    state = _heartbeat_state(
                        state, active_economic_worker_processes=1
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                result = _verify_frozen_breadth_continuation_result(
                    dict(future.result())
                )
        finally:
            _end_economic_phase()

        envelope = _post_source_envelope(
            manifest,
            lane_id="EXPLORATION",
            branch_id="FROZEN_YM_OPEN_BREADTH_Q3_CONTINUATION",
            decision=str(result["status"]),
            payload_key="frozen_breadth_continuation",
            payload=result,
            next_action=str(result["next_action"]),
        )
        envelope_payload = dict(envelope)
        envelope_payload.pop("result_hash", None)
        envelope_payload["evidence_tier"] = (
            "G" if result.get("tier_g_account_labels") else "E_FALSIFIED"
        )
        envelope = _with_hash(envelope_payload, "result_hash")
        branch_writer.write_json(relative_result, envelope)
        _append_decision_once(root, manifest, envelope)
        results["FROZEN_BREADTH_CONTINUATION"] = result

    counts = _frozen_breadth_counts(result)
    tier_g_count = len(result.get("tier_g_account_labels") or ())
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=(
            "FROZEN_BREADTH_Q3_TIER_G_GRADUATED"
            if tier_g_count
            else "FROZEN_BREADTH_Q3_FALSIFIED"
        ),
        branch_results=results,
        next_action=str(result["next_action"]),
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "frozen_breadth_normal_episode_count": counts["normal_episode_count"],
            "frozen_breadth_stressed_episode_count": counts["stressed_episode_count"],
            "frozen_breadth_exact_account_replay_count": counts[
                "exact_account_replay_count"
            ],
            "frozen_breadth_tier_g_count": tier_g_count,
            "frozen_breadth_status": str(result["status"]),
            "frozen_breadth_runtime_network_access": False,
            "frozen_breadth_runtime_feature_cache_writes": 0,
            "data_purchase_count": 0,
            "new_data_purchase_count": 0,
            "external_manifest_bound_acquisition_count": 1,
            "independently_confirmed_tier_c_count": int(
                state.get("independently_confirmed_tier_c_count", 0)
            ),
            "forward_tier_f_count": int(state.get("forward_tier_f_count", 0)),
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, result


def _run_post_breadth_portfolio_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Run/resume the bounded post-breadth legal/curve portfolio.

    Both workers are read-only and at most two are spawned.  The coordinator
    alone seals immutable branch envelopes.  A missing Treasury acquisition or
    input contract produces a transient fail-closed result in production state,
    not an immutable terminal artifact, so a later manifest-bound input can be
    consumed exactly once without rerunning the completed legal branch.
    """

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    paths = _post_breadth_manifest_paths(root, output, manifest)
    branch_specs = {
        "POST_BREADTH_LEGAL_EXACT": {
            "path": paths["legal_result_path"],
            "payload_key": "frozen_legal_frontier_exact",
            "branch_id": "FROZEN_LEGAL_FRONTIER_EXACT_REPLAY",
        },
        "POST_BREADTH_TREASURY_CURVE": {
            "path": paths["treasury_result_path"],
            "payload_key": "treasury_curve_tripwire",
            "branch_id": "TREASURY_CURVE_RELATIVE_VALUE_TRIPWIRE",
        },
    }

    completed: dict[str, dict[str, Any]] = {}
    for key, spec in branch_specs.items():
        result_path = Path(spec["path"])
        if not result_path.is_file():
            continue
        envelope = _read_hashed(result_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                f"post-breadth {key} envelope identity drift"
            )
        payload = dict(envelope.get(str(spec["payload_key"])) or {})
        if key == "POST_BREADTH_LEGAL_EXACT":
            payload = _verify_frozen_legal_frontier_result(payload)
        else:
            payload = _verify_treasury_curve_result(payload, allow_waiting=False)
        completed[key] = payload
        results[key] = payload

    treasury_contract, treasury_missing = _treasury_input_if_ready(root, paths)
    jobs: dict[str, tuple[Any, dict[str, Any]]] = {}
    if "POST_BREADTH_LEGAL_EXACT" not in completed:
        jobs["POST_BREADTH_LEGAL_EXACT"] = (
            frozen_legal_frontier_worker,
            {"root": str(root)},
        )
    if "POST_BREADTH_TREASURY_CURVE" not in completed:
        jobs["POST_BREADTH_TREASURY_CURVE"] = (
            _treasury_curve_tripwire_worker,
            {
                "root": str(root),
                "input_contract": treasury_contract,
                "missing_inputs": list(treasury_missing),
            },
        )

    if jobs:
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=min(len(jobs), 2),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                futures = {
                    pool.submit(worker, payload): key
                    for key, (worker, payload) in jobs.items()
                }
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="POST_BREADTH_PORTFOLIO_RUNNING",
                    branch_results=results,
                    next_action=(
                        "RUN_FROZEN_LEGAL_EXACT_AND_TREASURY_CURVE_READ_ONLY"
                    ),
                )
                state.update(
                    {
                        "active_economic_worker_processes": len(futures),
                        "post_breadth_worker_limit": 2,
                        "post_breadth_parent_writer_only": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                    }
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)

                pending = set(futures)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(min(float(heartbeat_seconds), 5.0), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    for future in done:
                        key = futures[future]
                        raw = dict(future.result())
                        result = (
                            _verify_frozen_legal_frontier_result(raw)
                            if key == "POST_BREADTH_LEGAL_EXACT"
                            else _verify_treasury_curve_result(
                                raw, allow_waiting=True
                            )
                        )
                        completed[key] = result
                        results[key] = result
                        # WAITING is a resumable input boundary, not a terminal
                        # economic result.  Persist it only in mutable state.
                        if not (
                            key == "POST_BREADTH_TREASURY_CURVE"
                            and result["status"] == TREASURY_CURVE_WAITING_STATUS
                        ):
                            spec = branch_specs[key]
                            next_action = _post_breadth_branch_next_action(
                                key, result
                            )
                            envelope = _post_source_envelope(
                                manifest,
                                lane_id=(
                                    "EXPLOITATION"
                                    if key == "POST_BREADTH_LEGAL_EXACT"
                                    else "EXPLORATION"
                                ),
                                branch_id=str(spec["branch_id"]),
                                decision=str(result["decision"]),
                                payload_key=str(spec["payload_key"]),
                                payload=result,
                                next_action=next_action,
                            )
                            relative = _relative_branch_artifact_path(
                                output,
                                Path(spec["path"]),
                                f"post-breadth {key} result",
                            )
                            branch_writer.write_json(relative, envelope)
                            _append_decision_once(root, manifest, envelope)
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    if "POST_BREADTH_LEGAL_EXACT" not in completed:
        raise AutonomousDirectorRuntimeError(
            "post-breadth legal exact branch did not produce a result"
        )
    if "POST_BREADTH_TREASURY_CURVE" not in completed:
        raise AutonomousDirectorRuntimeError(
            "post-breadth Treasury branch did not produce a result"
        )

    portfolio = {
        "legal_exact": completed["POST_BREADTH_LEGAL_EXACT"],
        "treasury_curve": completed["POST_BREADTH_TREASURY_CURVE"],
    }
    portfolio["portfolio_hash"] = stable_hash(portfolio)
    next_action = _post_breadth_portfolio_next_action(portfolio)
    counts = _post_breadth_counts(results)
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="POST_BREADTH_PORTFOLIO_DECIDED",
        branch_results=results,
        next_action=next_action,
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "post_breadth_worker_limit": 2,
            "post_breadth_parent_writer_only": True,
            **counts,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, portfolio


def _run_session_safe_fast_book_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Run/resume the two bounded session-safe repairs after post-breadth.

    Each child is a read-only economic worker.  Only the persistent parent
    writes branch envelopes and the deterministic portfolio receipt.  Results
    remain Tier E diagnostics even when their frozen signal gate passes; the
    only allowed advance is an unchanged chronological 50K/20-day
    confirmation.
    """

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    relative_root = _POST_BREADTH_RELATIVE_ROOT / "session_safe_fast_book"
    branch_root = output / "branch_results"
    variant_keys = {
        "HORIZON_SAFE_ENTRY_CUTOFF": (
            "POST_BREADTH_SESSION_SAFE_HORIZON_CUTOFF"
        ),
        "DROP_OFFENDING_COMPONENT": (
            "POST_BREADTH_SESSION_SAFE_DROP_COMPONENT"
        ),
    }
    variant_paths = {
        variant: branch_root
        / relative_root
        / f"{variant.lower()}.json"
        for variant in SESSION_SAFE_REPAIR_VARIANTS
    }

    completed: dict[str, dict[str, Any]] = {}
    for variant in SESSION_SAFE_REPAIR_VARIANTS:
        path = variant_paths[variant]
        if not path.is_file():
            continue
        envelope = _read_hashed(path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                f"session-safe {variant} envelope identity drift"
            )
        result = _verify_session_safe_fast_book_result(
            dict(envelope.get("session_safe_fast_book_tripwire") or {}),
            expected_variant=variant,
        )
        completed[variant] = result
        results[variant_keys[variant]] = result

    missing = [
        variant
        for variant in SESSION_SAFE_REPAIR_VARIANTS
        if variant not in completed
    ]
    if missing:
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=min(len(missing), 2),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                futures = {
                    pool.submit(
                        session_safe_fast_book_worker,
                        {
                            "root": str(root),
                            "repair_variant": variant,
                        },
                    ): variant
                    for variant in missing
                }
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="SESSION_SAFE_FAST_BOOK_REPAIR_RUNNING",
                    branch_results=results,
                    next_action=(
                        "RUN_TWO_FROZEN_SESSION_SAFE_REPAIRS_READ_ONLY"
                    ),
                )
                state.update(
                    {
                        "active_economic_worker_processes": len(futures),
                        "session_safe_worker_limit": 2,
                        "session_safe_parent_writer_only": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                    }
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)

                pending = set(futures)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(min(float(heartbeat_seconds), 5.0), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    for future in done:
                        variant = futures[future]
                        result = _verify_session_safe_fast_book_result(
                            dict(future.result()), expected_variant=variant
                        )
                        completed[variant] = result
                        key = variant_keys[variant]
                        results[key] = result
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION"
                                if variant == "HORIZON_SAFE_ENTRY_CUTOFF"
                                else "EXPLORATION"
                            ),
                            branch_id=(
                                f"{SESSION_SAFE_FAST_BOOK_BRANCH_ID}:{variant}"
                            ),
                            decision=str(result["decision"]),
                            payload_key="session_safe_fast_book_tripwire",
                            payload=result,
                            next_action=_session_safe_result_next_action(result),
                        )
                        branch_writer.write_json(
                            relative_root / variant_paths[variant].name,
                            envelope,
                        )
                        _append_decision_once(root, manifest, envelope)
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    if set(completed) != set(SESSION_SAFE_REPAIR_VARIANTS):
        raise AutonomousDirectorRuntimeError(
            "session-safe repair portfolio did not produce both frozen variants"
        )

    portfolio = _build_session_safe_fast_book_portfolio(completed)
    composite_path = branch_root / relative_root / "portfolio.json"
    if composite_path.is_file():
        envelope = _read_hashed(composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "session-safe portfolio envelope identity drift"
            )
        persisted = _verify_session_safe_fast_book_portfolio(
            dict(envelope.get("session_safe_fast_book_portfolio") or {}),
            completed,
        )
        if str(persisted["result_hash"]) != str(portfolio["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "session-safe portfolio changed after persistence"
            )
        portfolio = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id=f"{SESSION_SAFE_FAST_BOOK_BRANCH_ID}:PORTFOLIO",
            decision=str(portfolio["decision"]),
            payload_key="session_safe_fast_book_portfolio",
            payload=portfolio,
            next_action=str(portfolio["next_action"]),
        )
        branch_writer.write_json(
            relative_root / composite_path.name,
            envelope,
        )
        _append_decision_once(root, manifest, envelope)

    results["POST_BREADTH_SESSION_SAFE_PORTFOLIO"] = portfolio
    signal_count = int(
        dict(portfolio.get("counters") or {}).get("signal_gate_pass_count", 0)
    )
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=(
            "SESSION_SAFE_FAST_BOOK_SIGNAL_REQUIRES_FROZEN_CONFIRMATION"
            if signal_count
            else "SESSION_SAFE_FAST_BOOK_TRIPWIRE_FALSIFIED"
        ),
        branch_results=results,
        next_action=str(portfolio["next_action"]),
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "session_safe_worker_limit": 2,
            "session_safe_parent_writer_only": True,
            **_session_safe_counts(results),
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, portfolio


def _session_safe_result_next_action(result: Mapping[str, Any]) -> str:
    if dict(result.get("repair_signal_gate") or {}).get("passed") is True:
        return (
            "FREEZE_UNCHANGED_REPAIR_FOR_CHRONOLOGICAL_CONFIRMATION_50K_20D"
        )
    return "CLOSE_SESSION_SAFE_REPAIR_AND_ADVANCE_DISTINCT_BRANCH"


def _run_session_safe_m2k_mym_confirmation_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
    acquisition_receipt_path: str | Path = _SESSION_SAFE_CONFIRMATION_DEFAULT_RECEIPT,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Consume the frozen M2K+MYM confirmation once, or wait resumably.

    Feature materialization is a parent-side cache operation.  The sole child
    process is read-only: it opens the sealed matrices and evaluates the
    unchanged decision card.  Only the persistent parent writes the result
    envelope and decision ledger.
    """

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    branch_root = output / "branch_results"
    relative_root = _SESSION_SAFE_CONFIRMATION_RELATIVE_ROOT
    feature_path = branch_root / relative_root / "technical_feature_receipt.json"
    result_path = branch_root / relative_root / "confirmation_result.json"
    card_path = (root / "config/research/session_safe_m2k_mym_confirmation_decision_card_v1.json").resolve()
    if not card_path.is_file():
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation decision card is absent"
        )
    card = load_session_safe_confirmation_card(card_path)
    card_hash = str(card["card_hash"])
    raw_receipt_path = Path(acquisition_receipt_path)
    receipt_path = (
        raw_receipt_path
        if raw_receipt_path.is_absolute()
        else (root / raw_receipt_path)
    ).resolve()

    if result_path.is_file():
        if not receipt_path.is_file() or not feature_path.is_file():
            raise AutonomousDirectorRuntimeError(
                "persisted session-safe confirmation lost a bound input"
            )
        feature_envelope = _read_hashed(feature_path, "result_hash")
        if not _artifact_manifest_compatible(feature_envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "session-safe confirmation feature envelope identity drift"
            )
        _verify_session_safe_confirmation_feature_receipt(
            dict(feature_envelope.get("session_safe_confirmation_features") or {}),
            expected_card_hash=card_hash,
        )
        envelope = _read_hashed(result_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "session-safe confirmation envelope identity drift"
            )
        confirmation = _verify_session_safe_m2k_mym_confirmation_result(
            dict(envelope.get("session_safe_m2k_mym_confirmation") or {}),
            expected_card_hash=card_hash,
        )
        results["POST_BREADTH_SESSION_SAFE_CONFIRMATION"] = confirmation
        return _publish_session_safe_confirmation_state(
            root=root,
            manifest=manifest,
            live_writer=live_writer,
            prior_state=state,
            started=started,
            results=results,
            confirmation=confirmation,
        )

    if not receipt_path.is_file():
        waiting = {
            "status": _SESSION_SAFE_CONFIRMATION_WAITING,
            "economic_result_created": False,
            "decision_card_hash": card_hash,
            "acquisition_receipt_path": str(receipt_path),
            "evidence_tier": None,
            "promotion_status": None,
        }
        state = _state_payload(
            manifest,
            sequence=int(state["checkpoint_sequence"]) + 1,
            state="ROBUSTNESS_ACTIVE",
            stage=_SESSION_SAFE_CONFIRMATION_WAITING,
            branch_results=results,
            next_action=(
                "ACQUIRE_FROZEN_SESSION_SAFE_M2K_MYM_CONFIRMATION_INPUT_AND_RESUME"
            ),
        )
        state.update(
            {
                "active_economic_worker_processes": 0,
                "session_safe_confirmation_worker_limit": 1,
                "session_safe_confirmation_parent_writer_only": True,
                "session_safe_confirmation_status": _SESSION_SAFE_CONFIRMATION_WAITING,
                "session_safe_confirmation_economic_result_created": False,
                "session_safe_confirmation_tier_q_eligible_count": 0,
                "session_safe_confirmation_automatic_tier_g_count": 0,
                "session_safe_confirmation_automatic_tier_c_count": 0,
                "session_safe_confirmation_automatic_tier_f_count": 0,
                "q4_access_count_delta": 0,
                "broker_connections": 0,
                "orders": 0,
            }
        )
        state = _rehash(state, "state_hash")
        _publish(live_writer, state, _kpis(manifest, state, results, started))
        _write_mission_views(root, manifest, state, results)
        return state, results, waiting

    acquisition = _read_json_object(receipt_path)
    acquisition_core = dict(acquisition)
    acquisition_hash = str(acquisition_core.pop("receipt_hash", ""))
    if (
        not acquisition_hash
        or stable_hash(acquisition_core) != acquisition_hash
        or acquisition.get("decision_card_hash") != card_hash
        or acquisition.get("download_status") != "DOWNLOADED"
        or int(acquisition.get("q4_access_count_delta", -1)) != 0
        or int(acquisition.get("broker_connections", -1)) != 0
        or int(acquisition.get("orders", -1)) != 0
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation acquisition receipt drift"
        )

    if feature_path.is_file():
        feature_envelope = _read_hashed(feature_path, "result_hash")
        if not _artifact_manifest_compatible(feature_envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "session-safe confirmation feature envelope identity drift"
            )
        features = _verify_session_safe_confirmation_feature_receipt(
            dict(feature_envelope.get("session_safe_confirmation_features") or {}),
            expected_card_hash=card_hash,
        )
    else:
        feature_inputs = dict(acquisition.get("feature_build_inputs") or {})
        source_files = [dict(item) for item in feature_inputs.get("source_files") or ()]
        contract_map_path = Path(str(feature_inputs.get("contract_map_path") or ""))
        cache_root = Path(str(feature_inputs.get("cache_root") or ""))
        if (
            not source_files
            or not contract_map_path.is_file()
            or any(
                not Path(str(item.get("path") or "")).is_file()
                for item in source_files
            )
            or not str(cache_root)
        ):
            raise AutonomousDirectorRuntimeError(
                "session-safe confirmation receipt references absent feature inputs"
            )
        features = _verify_session_safe_confirmation_feature_receipt(
            build_session_safe_confirmation_features(
                card,
                source_files=source_files,
                contract_map_path=contract_map_path,
                cache_root=cache_root,
            ),
            expected_card_hash=card_hash,
        )
        feature_envelope = _session_safe_confirmation_feature_envelope(
            manifest, features
        )
        branch_writer.write_json(
            relative_root / feature_path.name,
            feature_envelope,
        )

    _begin_economic_phase()
    try:
        with ProcessPoolExecutor(
            max_workers=1,
            mp_context=multiprocessing.get_context("spawn"),
        ) as pool:
            future = pool.submit(
                _session_safe_m2k_mym_confirmation_worker,
                {
                    "card_path": str(card_path),
                    "acquisition_receipt_path": str(receipt_path),
                    "feature_receipt": features,
                    "expected_card_hash": card_hash,
                },
            )
            state = _state_payload(
                manifest,
                sequence=int(state["checkpoint_sequence"]) + 1,
                state="ROBUSTNESS_ACTIVE",
                stage="SESSION_SAFE_M2K_MYM_CONFIRMATION_RUNNING",
                branch_results=results,
                next_action="RUN_FROZEN_SESSION_SAFE_M2K_MYM_CONFIRMATION_ONCE",
            )
            state.update(
                {
                    "active_economic_worker_processes": 1,
                    "session_safe_confirmation_worker_limit": 1,
                    "session_safe_confirmation_parent_writer_only": True,
                    "session_safe_confirmation_status": "RUNNING",
                    "q4_access_count_delta": 0,
                    "broker_connections": 0,
                    "orders": 0,
                }
            )
            state = _rehash(state, "state_hash")
            _publish(live_writer, state, _kpis(manifest, state, results, started))
            _write_mission_views(root, manifest, state, results)
            while not future.done():
                wait(
                    {future},
                    timeout=max(min(float(heartbeat_seconds), 5.0), 0.1),
                    return_when=FIRST_COMPLETED,
                )
                state = _heartbeat_state(
                    state, active_economic_worker_processes=int(not future.done())
                )
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
            confirmation = _verify_session_safe_m2k_mym_confirmation_result(
                dict(future.result()), expected_card_hash=card_hash
            )
    finally:
        _end_economic_phase()

    next_action = _session_safe_confirmation_next_action(confirmation)
    envelope = _post_source_envelope(
        manifest,
        lane_id="EXPLOITATION",
        branch_id="SESSION_SAFE_M2K_MYM_CONFIRMATION_2022_V1",
        decision=str(confirmation["resulting_evidence_status"]),
        payload_key="session_safe_m2k_mym_confirmation",
        payload=confirmation,
        next_action=next_action,
    )
    branch_writer.write_json(relative_root / result_path.name, envelope)
    _append_decision_once(root, manifest, envelope)
    results["POST_BREADTH_SESSION_SAFE_CONFIRMATION"] = confirmation
    return _publish_session_safe_confirmation_state(
        root=root,
        manifest=manifest,
        live_writer=live_writer,
        prior_state=state,
        started=started,
        results=results,
        confirmation=confirmation,
    )


def _publish_session_safe_confirmation_state(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    live_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    results: Mapping[str, Mapping[str, Any]],
    confirmation: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    verified = _verify_session_safe_m2k_mym_confirmation_result(confirmation)
    runtime_results = {key: dict(value) for key, value in results.items()}
    runtime_results["POST_BREADTH_SESSION_SAFE_CONFIRMATION"] = verified
    passed = dict(verified["confirmation_gate"])["passed"] is True
    state = _state_payload(
        manifest,
        sequence=int(prior_state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=(
            "SESSION_SAFE_M2K_MYM_CONFIRMATION_TIER_Q_ELIGIBLE"
            if passed
            else "SESSION_SAFE_M2K_MYM_CONFIRMATION_BRANCH_CLOSED"
        ),
        branch_results=runtime_results,
        next_action=_session_safe_confirmation_next_action(verified),
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "session_safe_confirmation_worker_limit": 1,
            "session_safe_confirmation_parent_writer_only": True,
            **_session_safe_confirmation_counts(runtime_results),
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, runtime_results, started))
    _write_mission_views(root, manifest, state, runtime_results)
    return state, runtime_results, verified


def _session_safe_confirmation_next_action(result: Mapping[str, Any]) -> str:
    if dict(result.get("confirmation_gate") or {}).get("passed") is True:
        return (
            "PRESERVE_SESSION_SAFE_BOOK_AS_TIER_Q_ELIGIBLE_AND_RUN_FROZEN_TIER_G_DEVELOPMENT"
        )
    return "CLOSE_SESSION_SAFE_REPAIR_AND_DISPATCH_DISTINCT_CAUSAL_BRANCH"


def _run_clean_cross_asset_daily_tripwire_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Persist the frozen CL-to-YM tripwire once, then dispatch a new branch."""

    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    section = dict(manifest.get("post_breadth_branch_portfolio") or {})
    card_raw = str(
        section.get("cross_asset_daily_decision_card_path")
        or "config/research/clean_cross_asset_daily_tripwire_decision_card_v1.json"
    )
    result_raw = str(section.get("cross_asset_daily_result_path") or "").strip()
    card_path = (root / card_raw).resolve()
    result_path = (
        (root / result_raw).resolve()
        if result_raw
        else (
            output
            / "branch_results"
            / _POST_BREADTH_RELATIVE_ROOT
            / "clean_cross_asset_daily_tripwire.json"
        ).resolve()
    )
    _assert_within(root.resolve(), card_path)
    relative_result = _relative_branch_artifact_path(
        output, result_path, "clean cross-asset daily result"
    )
    if not card_path.is_file():
        raise AutonomousDirectorRuntimeError(
            "clean cross-asset daily decision card is absent"
        )

    result: dict[str, Any]
    if result_path.is_file():
        envelope = _read_hashed(result_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "clean cross-asset daily envelope identity drift"
            )
        result = _verify_clean_cross_asset_daily_tripwire_result(
            _verified_inner_result(
                envelope,
                key="clean_cross_asset_daily_tripwire",
                expected_schema=CLEAN_CROSS_ASSET_DAILY_SCHEMA,
                expected_status=str(
                    dict(envelope.get("clean_cross_asset_daily_tripwire") or {}).get(
                        "status"
                    )
                ),
            )
        )
    else:
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                future = pool.submit(
                    _clean_cross_asset_daily_tripwire_worker,
                    str(root),
                    str(card_path),
                )
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="CLEAN_CROSS_ASSET_DAILY_TRIPWIRE_RUNNING",
                    branch_results=results,
                    next_action="RUN_FROZEN_CL_TO_YM_EXACT_ACCOUNT_TRIPWIRE_ONCE",
                )
                state.update(
                    {
                        "active_economic_worker_processes": 1,
                        "cross_asset_daily_worker_limit": 1,
                        "cross_asset_daily_parent_writer_only": True,
                        "q4_access_count_delta": 0,
                        "broker_connections": 0,
                        "orders": 0,
                    }
                )
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                while not future.done():
                    wait(
                        {future},
                        timeout=max(min(float(heartbeat_seconds), 5.0), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    state = _heartbeat_state(
                        state, active_economic_worker_processes=int(not future.done())
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
                result = _verify_clean_cross_asset_daily_tripwire_result(
                    dict(future.result())
                )
        finally:
            _end_economic_phase()

        expected_card_hash = str(
            section.get("cross_asset_daily_decision_card_hash") or ""
        )
        actual_card_hash = str(
            dict(result.get("source_bindings") or {}).get("decision_card_hash") or ""
        )
        if expected_card_hash and actual_card_hash != expected_card_hash:
            raise AutonomousDirectorRuntimeError(
                "clean cross-asset daily decision card differs from manifest freeze"
            )
        envelope = _post_source_envelope(
            manifest,
            lane_id="EXPLORATION",
            branch_id=CLEAN_CROSS_ASSET_DAILY_BRANCH_ID,
            decision=str(result["status"]),
            payload_key="clean_cross_asset_daily_tripwire",
            payload=result,
            next_action=str(result["next_action"]),
        )
        branch_writer.write_json(relative_result, envelope)
        _append_decision_once(root, manifest, envelope)

    results["POST_BREADTH_CLEAN_CROSS_ASSET_DAILY"] = result
    best = dict(result["best_safe_primary_cell"])
    normal = dict(best["normal"])
    stressed = dict(best["stressed"])
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage=str(result["status"]),
        branch_results=results,
        next_action=str(result["next_action"]),
    )
    state.update(
        {
            "active_economic_worker_processes": 0,
            "cross_asset_daily_status": str(result["status"]),
            "cross_asset_daily_event_count": int(result["event_count"]),
            "cross_asset_daily_account_cell_count": int(result["account_cell_count"]),
            "cross_asset_daily_best_safe_account_label": str(best["account_label"]),
            "cross_asset_daily_best_safe_micro_quantity": int(best["micro_quantity"]),
            "cross_asset_daily_best_safe_normal_pass_count": int(normal["pass_count"]),
            "cross_asset_daily_best_safe_stressed_pass_count": int(stressed["pass_count"]),
            "cross_asset_daily_best_safe_stressed_mll_breach_count": int(
                stressed["mll_breach_count"]
            ),
            "cross_asset_daily_promotion_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        }
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, result


def _session_safe_confirmation_feature_envelope(
    manifest: Mapping[str, Any], features: Mapping[str, Any]
) -> dict[str, Any]:
    value = {
        "schema": BRANCH_RESULT_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "lane_id": "DIRECTOR",
        "branch_id": "SESSION_SAFE_M2K_MYM_CONFIRMATION_FEATURES_2022_V1",
        "status": "COMPLETE",
        "decision": "CAUSAL_CONFIRMATION_FEATURES_READY",
        "session_safe_confirmation_features": dict(features),
        "completed_at_utc": _utc_now(),
        "implementation_validation_only": True,
        "economic_result_created": False,
        "parent_writer_only": True,
        "promotion_status": None,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return _with_hash(value, "result_hash")


def _build_session_safe_fast_book_portfolio(
    completed: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    exact_replays = 0
    source_replay_rows = 0
    normal_episode_count = 0
    stressed_episode_count = 0
    normal_pass_count = 0
    stressed_pass_count = 0
    signal_variants: list[str] = []
    for variant in SESSION_SAFE_REPAIR_VARIANTS:
        result = _verify_session_safe_fast_book_result(
            completed.get(variant) or {}, expected_variant=variant
        )
        selected = dict(
            dict(result["account_results"])["50K"]["horizon_results"]["20"]
        )
        normal = dict(selected["normal"])
        stressed = dict(selected["stressed"])
        passed = dict(result["repair_signal_gate"])["passed"] is True
        if passed:
            signal_variants.append(variant)
        counts = dict(result["counters"])
        exact_replays += int(counts["exact_account_replays"])
        source_replay_rows += int(counts["source_event_rows_reconstructed"])
        normal_episode_count += int(normal["episode_count"])
        stressed_episode_count += int(stressed["episode_count"])
        normal_pass_count += int(normal["pass_count"])
        stressed_pass_count += int(stressed["pass_count"])
        variants[variant] = {
            "result_hash": str(result["result_hash"]),
            "evidence_tier": "E",
            "promotion_status": None,
            "signal_gate_passed": passed,
            "frozen_account_label": "50K",
            "frozen_horizon_trading_days": 20,
            "full_coverage_start_count": int(
                selected["full_coverage_start_count"]
            ),
            "normal_pass_count": int(normal["pass_count"]),
            "stressed_pass_count": int(stressed["pass_count"]),
            "normal_target_progress_median": float(
                normal["target_progress_median"]
            ),
            "stressed_target_progress_median": float(
                stressed["target_progress_median"]
            ),
            "stressed_mll_breach_count": int(stressed["mll_breach_count"]),
        }

    signal_gate_pass_count = len(signal_variants)
    next_action = (
        "FREEZE_SESSION_SAFE_REPAIRS_AND_RUN_UNCHANGED_CHRONOLOGICAL_CONFIRMATION_50K_20D"
        if signal_gate_pass_count
        else "CLOSE_SESSION_SAFE_REPAIRS_AND_DISPATCH_DISTINCT_CAUSAL_BRANCH"
    )
    core: dict[str, Any] = {
        "schema": _SESSION_SAFE_PORTFOLIO_SCHEMA,
        "status": "COMPLETE_SESSION_SAFE_FAST_BOOK_PORTFOLIO",
        "branch_id": SESSION_SAFE_FAST_BOOK_BRANCH_ID,
        "source_policy_id": SESSION_SAFE_FAST_BOOK_SOURCE_POLICY_ID,
        "evidence_role": SESSION_SAFE_FAST_BOOK_EVIDENCE_ROLE,
        "evidence_tier": "E",
        "promotion_status": None,
        "status_inherited": False,
        "repair_variants": variants,
        "signal_gate_passing_variants": signal_variants,
        "confirmation_contract": {
            "required_only_when_signal_gate_passes": True,
            "account_label": "50K",
            "horizon_trading_days": 20,
            "chronological": True,
            "frozen_before_outcomes": True,
            "mutation_allowed": False,
            "automatic_promotion_allowed": False,
        },
        "counters": {
            "repair_variant_count": len(variants),
            "signal_gate_pass_count": signal_gate_pass_count,
            "source_event_replay_operations": source_replay_rows,
            "exact_account_replays": exact_replays,
            "frozen_50k_20d_normal_episode_count": normal_episode_count,
            "frozen_50k_20d_stressed_episode_count": stressed_episode_count,
            "frozen_50k_20d_normal_pass_count": normal_pass_count,
            "frozen_50k_20d_stressed_pass_count": stressed_pass_count,
            "worker_authoritative_writes": 0,
            "parent_branch_artifact_count": 3,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "promotion_count": 0,
        },
        "decision": (
            "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_CHRONOLOGICAL_CONFIRMATION_50K_20D"
            if signal_gate_pass_count
            else "SESSION_SAFE_REPAIR_PORTFOLIO_FALSIFIED"
        ),
        "next_action": next_action,
        "read_only_workers": True,
        "parent_writer_only": True,
    }
    return {**core, "result_hash": stable_hash(core)}


def _post_breadth_branch_next_action(
    key: str, result: Mapping[str, Any]
) -> str:
    if key == "POST_BREADTH_LEGAL_EXACT":
        passes = int(
            dict(result.get("counters") or {}).get(
                "admissible_exact_passes_all_horizons_and_scenarios", 0
            )
        )
        return (
            "FREEZE_LEGAL_EXACT_SURVIVORS_FOR_CHRONOLOGICAL_VALIDATION"
            if passes
            else "CLOSE_SUMMARY_SCALE_SIGNAL_AND_ADVANCE_DISTINCT_BRANCH"
        )
    if result.get("status") == TREASURY_CURVE_WAITING_STATUS:
        return "ACQUIRE_AND_BIND_TREASURY_CURVE_INPUT_THEN_RESUME"
    if result.get("decision") == (
        "CURVE_RELATIVE_VALUE_TRIPWIRE_GREEN_DEVELOPMENT_ONLY"
    ):
        return "FREEZE_CURVE_TRIPWIRE_SURVIVORS_FOR_UNTOUCHED_CONFIRMATION"
    return "CLOSE_OR_REDIRECT_TREASURY_CURVE_TRIPWIRE"


def _post_breadth_portfolio_next_action(
    portfolio: Mapping[str, Mapping[str, Any]],
) -> str:
    legal = dict(portfolio["legal_exact"])
    treasury = dict(portfolio["treasury_curve"])
    legal_passes = int(
        dict(legal.get("counters") or {}).get(
            "admissible_exact_passes_all_horizons_and_scenarios", 0
        )
    )
    if treasury.get("status") == TREASURY_CURVE_WAITING_STATUS:
        return (
            "FREEZE_LEGAL_EXACT_SURVIVORS_AND_AWAIT_MANIFEST_BOUND_TREASURY_INPUT"
            if legal_passes
            else "AWAIT_MANIFEST_BOUND_TREASURY_INPUT_AND_ADVANCE_DISTINCT_BRANCH"
        )
    if legal_passes or treasury.get("decision") == (
        "CURVE_RELATIVE_VALUE_TRIPWIRE_GREEN_DEVELOPMENT_ONLY"
    ):
        return "ADVANCE_POST_BREADTH_WINNERS_TO_FROZEN_CHRONOLOGICAL_VALIDATION"
    return "SELECT_NEXT_MATERIALLY_DISTINCT_BRANCH_AFTER_POST_BREADTH_DECISION"


def _run_event_time_safety_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    """Run/resume the bounded event-time MLL safety frontier in two shards."""

    relative_root = Path("post_source_exhaustion/post_composite")
    branch_root = output / "branch_results"
    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    shards: dict[int, dict[str, Any]] = {}
    shard_paths = {
        index: branch_root
        / relative_root
        / f"event_time_safety_shard_{index:02d}.json"
        for index in range(2)
    }
    for index, path in shard_paths.items():
        if not path.is_file():
            continue
        shards[index] = _read_relay_shard(
            path,
            manifest=manifest,
            key="event_time_safety_shard",
            expected_schema=EVENT_TIME_SAFETY_SHARD_SCHEMA,
            expected_status="COMPLETE_BOUNDED_EVENT_TIME_SAFETY_SHARD",
            expected_index=index,
            expected_count=2,
            label="event-time safety",
        )

    missing = [index for index in range(2) if index not in shards]
    if missing:
        future_index: dict[Any, int] = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=len(missing),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                for index in missing:
                    future = pool.submit(
                        _event_time_safety_from_root_worker,
                        str(root),
                        shard_index=index,
                        shard_count=2,
                    )
                    future_index[future] = index
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="EVENT_TIME_SAFETY_FRONTIER_REPLAY_RUNNING",
                    branch_results=results,
                    next_action=(
                        "TEST_BOUNDED_MICRO_CONTRACT_MLL_SAFETY_FRONTIER"
                    ),
                )
                state["active_economic_worker_processes"] = len(future_index)
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_index)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = _heartbeat_state(state)
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        index = future_index[future]
                        shard = dict(future.result())
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION" if index == 0 else "EXPLORATION"
                            ),
                            branch_id=f"EVENT_TIME_SAFETY_SHARD_{index:02d}",
                            decision=str(shard["status"]),
                            payload_key="event_time_safety_shard",
                            payload=shard,
                            next_action="COMPOSE_EVENT_TIME_SAFETY_SHARDS",
                        )
                        branch_writer.write_json(
                            relative_root / shard_paths[index].name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                        shards[index] = shard
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    composite = compose_autonomous_event_time_safety_frontier_shards(
        [shards[index] for index in range(2)]
    )
    composite_path = branch_root / relative_root / "event_time_safety_composite.json"
    if composite_path.is_file():
        envelope = _read_hashed(composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "event-time safety composite identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="event_time_safety_composite",
            expected_schema=EVENT_TIME_SAFETY_COMPOSITE_SCHEMA,
            expected_status="COMPLETE_RECONCILED_EVENT_TIME_SAFETY_SHARDS",
        )
        if str(persisted["result_hash"]) != str(composite["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "event-time safety composite changed after persistence"
            )
        composite = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="EVENT_TIME_SAFETY_FRONTIER_COMPOSITE",
            decision=str(composite["status"]),
            payload_key="event_time_safety_composite",
            payload=composite,
            next_action=str(composite["next_action"]),
        )
        branch_writer.write_json(relative_root / composite_path.name, envelope)
        _append_decision_once(root, manifest, envelope)

    results["EVENT_TIME_SAFETY"] = composite
    counts = dict(composite.get("counts") or {})
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="EVENT_TIME_SAFETY_FRONTIER_RECONCILED",
        branch_results=results,
        next_action=str(composite["next_action"]),
    )
    state["active_economic_worker_processes"] = 0
    state["event_time_safety_candidate_count"] = int(
        counts.get("selected_candidate_count", 0)
    )
    state["event_time_safety_profile_count"] = int(
        counts.get("profile_count", 0)
    )
    state["event_time_safety_exact_episode_count"] = int(
        counts.get("exact_episode_count", 0)
    )
    state["event_time_safety_g_precontrol_ready_count"] = int(
        counts.get("heldout_safety_precontrol_ready_count", 0)
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, composite


def _run_tier_g_control_relay(
    *,
    root: Path,
    manifest: Mapping[str, Any],
    output: Path,
    live_writer: AtomicResultWriter,
    branch_writer: AtomicResultWriter,
    prior_state: Mapping[str, Any],
    started: float,
    heartbeat_seconds: float,
    runtime_results: Mapping[str, Mapping[str, Any]],
    candidate_bank_path: Path,
    initial_exact_path: Path,
    continuation_paths: Sequence[Path],
) -> tuple[
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    """Run/resume the two read-only unique-ledger Tier-G control shards."""

    relative_root = Path("post_source_exhaustion/post_composite")
    branch_root = output / "branch_results"
    state = dict(prior_state)
    results = {key: dict(value) for key, value in runtime_results.items()}
    shards: dict[int, dict[str, Any]] = {}
    shard_paths = {
        index: branch_root / relative_root / f"tier_g_controls_shard_{index:02d}.json"
        for index in range(2)
    }
    for index, path in shard_paths.items():
        if not path.is_file():
            continue
        shards[index] = _read_relay_shard(
            path,
            manifest=manifest,
            key="tier_g_controls_shard",
            expected_schema=TIER_G_CONTROL_SHARD_SCHEMA,
            expected_status="COMPLETE_READ_ONLY_TIER_G_CONTROL_SHARD",
            expected_index=index,
            expected_count=2,
            label="Tier-G controls",
        )

    missing = [index for index in range(2) if index not in shards]
    if missing:
        future_index: dict[Any, int] = {}
        _begin_economic_phase()
        try:
            with ProcessPoolExecutor(
                max_workers=len(missing),
                mp_context=multiprocessing.get_context("spawn"),
            ) as pool:
                for index in missing:
                    future = pool.submit(
                        _tier_g_controls_from_artifacts_worker,
                        str(root),
                        str(candidate_bank_path),
                        str(initial_exact_path),
                        tuple(str(value) for value in continuation_paths),
                        shard_index=index,
                        shard_count=2,
                    )
                    future_index[future] = index
                state = _state_payload(
                    manifest,
                    sequence=int(state["checkpoint_sequence"]) + 1,
                    state="ROBUSTNESS_ACTIVE",
                    stage="TIER_G_UNIQUE_LEDGER_CONTROLS_RUNNING",
                    branch_results=results,
                    next_action="REPLAY_FIVE_TIER_Q_FINALISTS_WITH_EXACT_CONTROLS",
                )
                state["active_economic_worker_processes"] = len(future_index)
                state = _rehash(state, "state_hash")
                _publish(live_writer, state, _kpis(manifest, state, results, started))
                _write_mission_views(root, manifest, state, results)
                pending = set(future_index)
                while pending:
                    done, pending = wait(
                        pending,
                        timeout=max(float(heartbeat_seconds), 0.1),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        state = _heartbeat_state(state)
                        _publish(
                            live_writer,
                            state,
                            _kpis(manifest, state, results, started),
                        )
                        _write_mission_views(root, manifest, state, results)
                        continue
                    for future in done:
                        index = future_index[future]
                        shard = dict(future.result())
                        envelope = _post_source_envelope(
                            manifest,
                            lane_id=(
                                "EXPLOITATION" if index == 0 else "EXPLORATION"
                            ),
                            branch_id=f"TIER_G_CONTROLS_SHARD_{index:02d}",
                            decision=str(shard["status"]),
                            payload_key="tier_g_controls_shard",
                            payload=shard,
                            next_action="COMPOSE_TIER_G_CONTROL_SHARDS",
                        )
                        branch_writer.write_json(
                            relative_root / shard_paths[index].name, envelope
                        )
                        _append_decision_once(root, manifest, envelope)
                        shards[index] = shard
                    state = _heartbeat_state(
                        state,
                        active_economic_worker_processes=len(pending),
                    )
                    _publish(
                        live_writer,
                        state,
                        _kpis(manifest, state, results, started),
                    )
                    _write_mission_views(root, manifest, state, results)
        finally:
            _end_economic_phase()

    composite = compose_autonomous_tier_g_control_shards(
        [shards[index] for index in range(2)]
    )
    composite_path = branch_root / relative_root / "tier_g_controls_composite.json"
    if composite_path.is_file():
        envelope = _read_hashed(composite_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "Tier-G controls composite identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="tier_g_controls_composite",
            expected_schema=TIER_G_CONTROL_COMPOSITE_SCHEMA,
            expected_status="COMPLETE_RECONCILED_TIER_G_CONTROL_SHARDS",
        )
        if str(persisted["result_hash"]) != str(composite["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "Tier-G controls composite changed after persistence"
            )
        composite = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="TIER_G_UNIQUE_LEDGER_CONTROLS_COMPOSITE",
            decision=str(composite["status"]),
            payload_key="tier_g_controls_composite",
            payload=composite,
            next_action=str(composite["next_action"]),
        )
        branch_writer.write_json(relative_root / composite_path.name, envelope)
        _append_decision_once(root, manifest, envelope)

    results["TIER_G_CONTROLS"] = composite
    graduation = build_graduated_development_books(composite)
    verify_tier_g_development_graduation(graduation)
    graduation_path = (
        branch_root / relative_root / "tier_g_development_graduation.json"
    )
    if graduation_path.is_file():
        envelope = _read_hashed(graduation_path, "result_hash")
        if not _artifact_manifest_compatible(envelope, manifest):
            raise AutonomousDirectorRuntimeError(
                "Tier-G development graduation identity drift"
            )
        persisted = _verified_inner_result(
            envelope,
            key="tier_g_development_graduation",
            expected_schema=TIER_G_GRADUATION_SCHEMA,
            expected_status="COMPLETE_READ_ONLY_TIER_G_DEVELOPMENT_GRADUATION",
        )
        verify_tier_g_development_graduation(persisted)
        if str(persisted["result_hash"]) != str(graduation["result_hash"]):
            raise AutonomousDirectorRuntimeError(
                "Tier-G development graduation changed after persistence"
            )
        graduation = persisted
    else:
        envelope = _post_source_envelope(
            manifest,
            lane_id="DIRECTOR",
            branch_id="TIER_G_DEVELOPMENT_GRADUATION",
            decision=str(graduation["status"]),
            payload_key="tier_g_development_graduation",
            payload=graduation,
            next_action=str(graduation["next_action"]),
        )
        branch_writer.write_json(relative_root / graduation_path.name, envelope)
        _append_decision_once(root, manifest, envelope)
    results["TIER_G_GRADUATION"] = graduation
    counts = dict(composite.get("counts") or {})
    graduation_counts = dict(graduation.get("counts") or {})
    state = _state_payload(
        manifest,
        sequence=int(state["checkpoint_sequence"]) + 1,
        state="ROBUSTNESS_ACTIVE",
        stage="TIER_G_DEVELOPMENT_GRADUATION_PERSISTED",
        branch_results=results,
        next_action=str(graduation["next_action"]),
    )
    state["active_economic_worker_processes"] = 0
    state["tier_g_control_candidate_count"] = int(
        counts.get("selected_candidate_count", 0)
    )
    state["tier_g_control_exact_replay_count"] = int(
        counts.get("exact_account_replay_count", 0)
    )
    state["tier_g_control_synthetic_count"] = int(
        counts.get("synthetic_control_count", 0)
    )
    state["tier_g_control_ready_count"] = int(
        counts.get("g_control_ready_count", 0)
    )
    state["authoritative_tier_g_count"] = int(
        graduation_counts.get("graduated_development_book_count", 0)
    )
    state = _rehash(state, "state_hash")
    _publish(live_writer, state, _kpis(manifest, state, results, started))
    _write_mission_views(root, manifest, state, results)
    return state, results, composite, graduation


def _heartbeat_state(
    value: Mapping[str, Any], *, active_economic_worker_processes: int | None = None
) -> dict[str, Any]:
    state = dict(value)
    state["checkpoint_sequence"] = int(state["checkpoint_sequence"]) + 1
    state["updated_at_utc"] = _utc_now()
    if active_economic_worker_processes is not None:
        state["active_economic_worker_processes"] = int(
            active_economic_worker_processes
        )
    return _rehash(state, "state_hash")


def _read_relay_shard(
    path: Path,
    *,
    manifest: Mapping[str, Any],
    key: str,
    expected_schema: str,
    expected_status: str,
    expected_index: int,
    expected_count: int,
    label: str,
) -> dict[str, Any]:
    """Load one immutable shard during resume and verify its full identity."""

    envelope = _read_hashed(path, "result_hash")
    if not _artifact_manifest_compatible(envelope, manifest):
        raise AutonomousDirectorRuntimeError(f"{label} shard envelope identity drift")
    shard = _verified_inner_result(
        envelope,
        key=key,
        expected_schema=expected_schema,
        expected_status=expected_status,
    )
    shard_contract = dict(shard.get("shard") or {})
    if (
        int(shard_contract.get("shard_index", -1)) != int(expected_index)
        or int(shard_contract.get("shard_count", -1)) != int(expected_count)
    ):
        raise AutonomousDirectorRuntimeError(f"{label} shard index/count drift")
    return shard


def _verified_inner_result(
    envelope: Mapping[str, Any],
    *,
    key: str,
    expected_schema: str,
    expected_status: str | Sequence[str],
) -> dict[str, Any]:
    value = dict(envelope.get(key) or {})
    claimed = str(value.get("result_hash") or "")
    payload = dict(value)
    payload.pop("result_hash", None)
    statuses = (
        {str(expected_status)}
        if isinstance(expected_status, str)
        else {str(item) for item in expected_status}
    )
    if (
        not claimed
        or stable_hash(payload) != claimed
        or value.get("schema") != expected_schema
        or str(value.get("status")) not in statuses
        or value.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            f"embedded economic result identity/hash drift: {key}"
        )
    return value


def _post_source_envelope(
    manifest: Mapping[str, Any],
    *,
    lane_id: str,
    branch_id: str,
    decision: str,
    payload_key: str,
    payload: Mapping[str, Any],
    next_action: str,
) -> dict[str, Any]:
    value = {
        "schema": BRANCH_RESULT_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "lane_id": lane_id,
        "branch_id": branch_id,
        "status": "COMPLETE",
        "decision": decision,
        payload_key: dict(payload),
        "completed_at_utc": _utc_now(),
        "read_only_worker": True,
        "evidence_tier": payload.get("evidence_tier", "E"),
        "promotion_status": None,
        "next_materially_distinct_action": next_action,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "data_purchase_count": 0,
    }
    return _with_hash(value, "result_hash")


def _post_source_runtime_results(
    initial_results: Mapping[str, Mapping[str, Any]],
    initial_exact: Mapping[str, Any],
    completed: Mapping[int, Mapping[str, Any]],
    event_result: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    results = {key: dict(value) for key, value in initial_results.items()}
    results["EXACT_0029"] = dict(initial_exact)
    if completed:
        results["EXACT_0029_COMPOSITE"] = compose_remaining_0029_exact_results(
            initial_exact,
            [completed[key] for key in sorted(completed)],
        )
    if event_result is not None:
        results["EVENT_TIME"] = dict(event_result)
    return results


def _candidate_bank_from_artifacts_worker(
    initial_exact_path: str,
    continuation_paths: Sequence[str],
) -> dict[str, Any]:
    """Classify the sealed exact bank in a read-only worker process."""

    initial = _read_hashed(Path(initial_exact_path), "result_hash")
    continuations = [
        _read_hashed(Path(value), "result_hash")
        for value in sorted(str(path) for path in continuation_paths)
    ]
    result = build_autonomous_combine_candidate_bank(initial, continuations)
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only candidate-bank worker attempted a status side effect"
        )
    return result


def _marginal_books_from_artifacts_worker(
    root_path: str,
    candidate_bank_envelope_path: str,
    initial_exact_path: str,
    continuation_paths: Sequence[str],
    *,
    requested_book_count: int,
    shard_index: int = 0,
    shard_count: int = 1,
) -> dict[str, Any]:
    """Replay one deterministic marginal-book shard without durable writes."""

    envelope = _read_hashed(Path(candidate_bank_envelope_path), "result_hash")
    bank = dict(envelope.get("candidate_bank") or {})
    initial = _read_hashed(Path(initial_exact_path), "result_hash")
    continuations = [
        _read_hashed(Path(value), "result_hash")
        for value in sorted(str(path) for path in continuation_paths)
    ]
    result = build_autonomous_marginal_combine_books(
        root_path,
        bank,
        initial,
        continuations,
        requested_book_count=int(requested_book_count),
        maximum_components=6,
        beam_width=64,
        shard_index=int(shard_index),
        shard_count=int(shard_count),
    )
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or int(counts.get("registry_writes", 0)) != 0
        or int(counts.get("database_writes", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only marginal-book worker attempted a status side effect"
        )
    return result


def _combine_pass_bank_from_artifacts_worker(
    candidate_bank_envelope_path: str,
    semantic_book_composite_envelope_path: str,
) -> dict[str, Any]:
    """Deduplicate exact pass-observed policies without durable side effects."""

    candidate_envelope = _read_hashed(
        Path(candidate_bank_envelope_path), "result_hash"
    )
    book_envelope = _read_hashed(
        Path(semantic_book_composite_envelope_path), "result_hash"
    )
    # The semantic-reconciliation relay persists the immutable marginal-book
    # composite under a distinct envelope key so the legacy evidence remains
    # untouched.  The bank builder accepts either a direct composite or the
    # original ``marginal_book_composite`` envelope; unwrap the reconciled
    # payload explicitly instead of misclassifying a valid checkpoint as a
    # missing source.
    if "semantic_marginal_book_composite" in book_envelope:
        book_source = _verified_inner_result(
            book_envelope,
            key="semantic_marginal_book_composite",
            expected_schema=MARGINAL_BOOK_COMPOSITE_SCHEMA,
            expected_status="COMPLETE_RECONCILED_MARGINAL_COMBINE_BOOK_SHARDS",
        )
    else:
        book_source = book_envelope
    result = build_autonomous_combine_pass_observed_bank(
        candidate_envelope,
        book_source,
    )
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("tier_g_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or int(result.get("database_writes", 0)) != 0
        or int(result.get("registry_writes", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only pass-observed bank worker attempted a status side effect"
        )
    return result


def _consistency_direct_from_artifacts_worker(
    root_path: str,
    candidate_bank_envelope_path: str,
    initial_exact_path: str,
    continuation_paths: Sequence[str],
    *,
    maximum_candidates: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    """Replay one deterministic consistency-direct shard without writes."""

    candidate_envelope = _read_hashed(
        Path(candidate_bank_envelope_path), "result_hash"
    )
    bank = dict(candidate_envelope.get("candidate_bank") or {})
    initial = _read_hashed(Path(initial_exact_path), "result_hash")
    continuations = [
        _read_hashed(Path(value), "result_hash")
        for value in sorted(str(path) for path in continuation_paths)
    ]
    result = build_autonomous_consistency_account_policies(
        root_path,
        bank,
        initial,
        continuations,
        maximum_candidates=int(maximum_candidates),
        shard_index=int(shard_index),
        shard_count=int(shard_count),
    )
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or int(counts.get("registry_writes", 0)) != 0
        or int(counts.get("database_writes", 0)) != 0
        or int(counts.get("broker_connections", 0)) != 0
        or int(counts.get("orders", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only consistency-direct worker attempted a status side effect"
        )
    return result


def _event_time_safety_from_root_worker(
    root_path: str,
    *,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    """Evaluate one event-time safety shard without durable side effects."""

    result = build_autonomous_event_time_safety_frontier(
        root_path,
        shard_index=int(shard_index),
        shard_count=int(shard_count),
    )
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or int(counts.get("registry_writes", 0)) != 0
        or int(counts.get("database_writes", 0)) != 0
        or int(counts.get("q4_access_count_delta", 0)) != 0
        or int(counts.get("data_purchase_count", 0)) != 0
        or int(counts.get("broker_connections", 0)) != 0
        or int(counts.get("orders", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only event-time safety worker attempted a status side effect"
        )
    return result


def _tier_g_controls_from_artifacts_worker(
    root_path: str,
    candidate_bank_envelope_path: str,
    initial_exact_path: str,
    continuation_paths: Sequence[str],
    *,
    shard_index: int,
    shard_count: int,
) -> dict[str, Any]:
    """Reconstruct one unique-ledger Tier-G control shard read-only."""

    candidate_envelope = _read_hashed(
        Path(candidate_bank_envelope_path), "result_hash"
    )
    bank = _verified_inner_result(
        candidate_envelope,
        key="candidate_bank",
        expected_schema=COMBINE_CANDIDATE_BANK_SCHEMA,
        expected_status="COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
    )
    initial = _read_hashed(Path(initial_exact_path), "result_hash")
    continuations = [
        _read_hashed(Path(value), "result_hash")
        for value in sorted(str(path) for path in continuation_paths)
    ]
    result = build_autonomous_tier_g_controls(
        root_path,
        bank,
        initial,
        continuations,
        shard_index=int(shard_index),
        shard_count=int(shard_count),
    )
    counts = dict(result.get("counts") or {})
    if (
        int(counts.get("authoritative_promotion_count", 0)) != 0
        or int(counts.get("xfa_paths_started", 0)) != 0
        or int(counts.get("registry_writes", 0)) != 0
        or int(counts.get("database_writes", 0)) != 0
        or int(counts.get("q4_access_count_delta", 0)) != 0
        or int(counts.get("data_purchase_count", 0)) != 0
        or int(counts.get("broker_connections", 0)) != 0
        or int(counts.get("orders", 0)) != 0
        or result.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only Tier-G control worker attempted a status side effect"
        )
    return result


def _tier_g_xfa_handoff_from_artifacts_worker(
    root_path: str,
    candidate_bank_envelope_path: str,
    initial_exact_path: str,
    continuation_paths: Sequence[str],
    tier_g_graduation_envelope_path: str,
) -> dict[str, Any]:
    """Reconstruct exact post-Combine handoffs in a read-only worker."""

    candidate_envelope = _read_hashed(
        Path(candidate_bank_envelope_path), "result_hash"
    )
    candidate_bank = _verified_inner_result(
        candidate_envelope,
        key="candidate_bank",
        expected_schema=COMBINE_CANDIDATE_BANK_SCHEMA,
        expected_status="COMPLETE_READ_ONLY_DEVELOPMENT_CLASSIFICATION",
    )
    initial = _read_hashed(Path(initial_exact_path), "result_hash")
    continuations = [
        _read_hashed(Path(value), "result_hash")
        for value in sorted(str(path) for path in continuation_paths)
    ]
    graduation_envelope = _read_hashed(
        Path(tier_g_graduation_envelope_path), "result_hash"
    )
    graduation = _verified_inner_result(
        graduation_envelope,
        key="tier_g_development_graduation",
        expected_schema=TIER_G_GRADUATION_SCHEMA,
        expected_status="COMPLETE_READ_ONLY_TIER_G_DEVELOPMENT_GRADUATION",
    )
    verify_tier_g_development_graduation(graduation)
    result = build_tier_g_combine_xfa_handoffs(
        root_path,
        candidate_bank,
        initial,
        continuations,
        graduation,
    )
    verified = verify_tier_g_combine_xfa_handoffs(result)
    counts = dict(verified.get("counts") or {})
    if (
        int(counts.get("xfa_simulations_started", -1)) != 0
        or int(counts.get("database_writes", -1)) != 0
        or int(counts.get("registry_writes", -1)) != 0
        or int(counts.get("broker_connections", -1)) != 0
        or int(counts.get("orders", -1)) != 0
        or verified.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "read-only Tier-G XFA handoff worker attempted a status side effect"
        )
    return verified


def _tier_g_xfa_diagnostic_from_artifact_worker(
    tier_g_xfa_handoff_envelope_path: str,
) -> dict[str, Any]:
    """Run account-size-aware XFA alternatives without durable side effects."""

    envelope = _read_hashed(
        Path(tier_g_xfa_handoff_envelope_path), "result_hash"
    )
    handoff = _verified_inner_result(
        envelope,
        key="tier_g_xfa_handoff",
        expected_schema=TIER_G_XFA_HANDOFF_SCHEMA,
        expected_status=TIER_G_XFA_HANDOFF_STATUS,
    )
    verify_tier_g_combine_xfa_handoffs(handoff)
    result = verify_autonomous_tier_g_xfa_diagnostic(
        build_autonomous_tier_g_xfa_diagnostic(handoff)
    )
    counts = dict(result.get("counts") or {})
    for field in (
        "database_writes",
        "registry_writes",
        "broker_connections",
        "orders",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousDirectorRuntimeError(
                "read-only Tier-G XFA diagnostic worker attempted a side effect"
            )
    if result.get("promotion_status") is not None:
        raise AutonomousDirectorRuntimeError(
            "read-only Tier-G XFA diagnostic worker attempted a promotion"
        )
    return result


def _cross_index_breadth_tripwire_worker(root_path: str) -> dict[str, Any]:
    """Run the frozen cross-index experiment with no writer or status grant."""

    return _verify_breadth_tripwire_result(
        run_cross_index_breadth_tripwire(root_path)
    )


def _fresh_confirmation_worker(
    contract_path: str,
    acquisition_receipt_path: str,
    feature_receipt_path: str,
    expected_contract_hash: str,
) -> dict[str, Any]:
    """Evaluate immutable confirmation inputs with read-only feature matrices."""

    contract = _read_json_object(Path(contract_path))
    acquisition = _read_json_object(Path(acquisition_receipt_path))
    feature_receipt = _read_json_object(Path(feature_receipt_path))
    if expected_contract_hash and str(contract.get("contract_hash") or "") != str(
        expected_contract_hash
    ):
        raise AutonomousDirectorRuntimeError(
            "fresh-confirmation worker contract hash drift"
        )
    matrices = open_confirmation_matrices(feature_receipt)
    result = evaluate_fresh_confirmation(
        contract,
        matrices=matrices,
        acquisition_receipt=acquisition,
        existing_result=None,
    )
    return _verify_fresh_confirmation_result(result)


def _session_safe_m2k_mym_confirmation_worker(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate the frozen confirmation with no durable or status writes."""

    card = load_session_safe_confirmation_card(str(payload["card_path"]))
    expected = str(payload.get("expected_card_hash") or "")
    if expected and str(card.get("card_hash") or "") != expected:
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation worker card hash drift"
        )
    acquisition = _read_json_object(Path(str(payload["acquisition_receipt_path"])))
    features = _verify_session_safe_confirmation_feature_receipt(
        dict(payload.get("feature_receipt") or {}),
        expected_card_hash=expected,
    )
    result = evaluate_session_safe_confirmation(
        card,
        matrices=open_session_safe_confirmation_matrices(features),
        acquisition_receipt=acquisition,
        existing_result=None,
    )
    return _verify_session_safe_m2k_mym_confirmation_result(
        result, expected_card_hash=expected
    )


def _frozen_breadth_continuation_worker(
    contract_path: str,
    acquisition_receipt_path: str,
    feature_receipt_path: str,
    expected_contract_hash: str,
) -> dict[str, Any]:
    """Replay the immutable singleton breadth policy without side effects."""

    contract = _read_json_object(Path(contract_path))
    acquisition = _read_json_object(Path(acquisition_receipt_path))
    feature_receipt = _read_json_object(Path(feature_receipt_path))
    if expected_contract_hash and str(contract.get("contract_hash") or "") != str(
        expected_contract_hash
    ):
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth worker contract hash drift"
        )
    result = evaluate_breadth_continuation(
        contract,
        matrices=open_breadth_matrices(feature_receipt),
        acquisition_receipt=acquisition,
        existing_result=None,
    )
    return _verify_frozen_breadth_continuation_result(result)


def _treasury_curve_tripwire_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Run the Treasury tripwire read-only or return its input-boundary status."""

    result = build_curve_relative_value_tripwire(
        str(payload["root"]),
        input_contract=(
            dict(payload["input_contract"])
            if payload.get("input_contract") is not None
            else None
        ),
    )
    if result.get("status") == TREASURY_CURVE_WAITING_STATUS:
        row = dict(result)
        row.pop("result_hash", None)
        row["missing_manifest_bound_inputs"] = sorted(
            str(value) for value in payload.get("missing_inputs") or ()
        )
        row["result_hash"] = stable_hash(row)
        result = row
    return _verify_treasury_curve_result(result, allow_waiting=True)


def _clean_cross_asset_daily_tripwire_worker(
    root_path: str, decision_card_path: str
) -> dict[str, Any]:
    """Run the frozen CL-to-YM exact tripwire without durable side effects."""

    return _verify_clean_cross_asset_daily_tripwire_result(
        run_clean_cross_asset_daily_tripwire(
            root_path, decision_card_path=decision_card_path
        )
    )


def _verify_event_time_matched_controls_result(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Enforce the no-promotion, no-side-effect matched-control contract."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    counts = dict(row.get("counts") or {})
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != EVENT_TIME_MATCHED_CONTROLS_SCHEMA
        or row.get("status") != EVENT_TIME_MATCHED_CONTROLS_STATUS
        or row.get("evidence_tier") != "E"
        or row.get("promotion_status") is not None
        or row.get("control_verdict")
        not in {
            "EVENT_TIME_CONTROL_DISTINCT_DEVELOPMENT_ONLY",
            "EVENT_TIME_MATCHED_CONTROLS_NOT_DISTINCT",
        }
        or int(counts.get("arm_count", -1)) != 4
        or int(counts.get("control_count", -1)) != 3
        or int(counts.get("authoritative_promotion_count", -1)) != 0
        or int(counts.get("data_purchase_count", -1)) != 0
        or int(counts.get("q4_access_count_delta", -1)) != 0
        or int(counts.get("broker_connections", -1)) != 0
        or int(counts.get("orders", -1)) != 0
        or int(counts.get("registry_writes", -1)) != 0
        or int(counts.get("database_writes", -1)) != 0
    ):
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control result identity or safety drift"
        )
    controls = tuple(dict(row.get("control_contract") or {}).get("control_ids") or ())
    if controls != (
        "DIRECTION_FLIP",
        "SESSION_EXPOSURE_MATCHED_DIRECTION_PERMUTATION",
        "SESSION_MATCHED_TIMING_GRID",
    ):
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control inventory drift"
        )
    return {**row, "result_hash": claimed}


def _event_time_control_counts(value: Mapping[str, Any]) -> dict[str, int]:
    counts = dict(value.get("counts") or {})
    normal = 0
    stressed = 0
    for arm in dict(value.get("arms") or {}).values():
        for horizons in dict(dict(arm).get("evaluation") or {}).values():
            for scenarios in dict(horizons).values():
                normal += int(
                    dict(scenarios)
                    .get("BASE", {})
                    .get("episode_count", 0)
                )
                stressed += int(
                    dict(scenarios)
                    .get("STRESS_1_5X", {})
                    .get("episode_count", 0)
                )
    exact = int(counts.get("exact_episode_count", 0))
    if exact != normal + stressed:
        raise AutonomousDirectorRuntimeError(
            "event-time matched-control episode denominator does not reconcile"
        )
    return {
        "control_count": int(counts.get("control_count", 0)),
        "exact_episode_count": exact,
        "normal_episode_count": normal,
        "stressed_episode_count": stressed,
    }


def _verify_frozen_legal_frontier_result(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify exact legal replay identity and its no-promotion worker boundary."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    counts = dict(row.get("counters") or {})
    results = [dict(item) for item in row.get("results") or ()]
    diagnostic = int(
        counts.get("diagnostic_exact_passes_all_horizons_and_scenarios", -1)
    )
    admissible = int(
        counts.get("admissible_exact_passes_all_horizons_and_scenarios", -1)
    )
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != FROZEN_LEGAL_FRONTIER_SCHEMA
        or row.get("status") != "COMPLETE_EXACT_FROZEN_LEGAL_FRONTIER_REPLAY"
        or row.get("evidence_tier") != "E"
        or row.get("promotion_status") is not None
        or row.get("read_only_worker") is not True
        or len(results) != 3
        or int(counts.get("frozen_cell_count", -1)) != len(results)
        or int(counts.get("exact_account_replays", 0)) <= 0
        or diagnostic < 0
        or admissible < 0
        or admissible > diagnostic
        or int(counts.get("data_purchase_count", -1)) != 0
        or int(counts.get("q4_access_count_delta", -1)) != 0
        or int(counts.get("broker_connections", -1)) != 0
        or int(counts.get("orders", -1)) != 0
        or int(counts.get("authoritative_writes", -1)) != 0
        or dict(row.get("evidence_bundle_adapter") or {}).get(
            "sealing_performed"
        )
        is not False
        or dict(row.get("evidence_bundle_adapter") or {}).get(
            "authoritative_writer_required_for_sealing"
        )
        is not True
    ):
        raise AutonomousDirectorRuntimeError(
            "frozen legal-frontier result identity, denominator, or safety drift"
        )
    expected_decision = (
        "EXACT_PASS_OBSERVED_REQUIRES_FROZEN_CHRONOLOGICAL_VALIDATION"
        if admissible
        else "SUMMARY_LEGAL_FRONTIER_PASS_SIGNAL_NOT_CONFIRMED_EXACTLY"
    )
    if row.get("decision") != expected_decision:
        raise AutonomousDirectorRuntimeError(
            "frozen legal-frontier decision differs from exact pass evidence"
        )
    exact_ids = [str(item.get("exact_policy_id") or "") for item in results]
    if any(not value for value in exact_ids) or len(set(exact_ids)) != len(exact_ids):
        raise AutonomousDirectorRuntimeError(
            "frozen legal-frontier exact policy inventory drift"
        )
    return {**row, "result_hash": claimed}


def _verify_treasury_curve_result(
    value: Mapping[str, Any], *, allow_waiting: bool
) -> dict[str, Any]:
    """Verify the Treasury result without converting development into status."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    status = str(row.get("status") or "")
    if not claimed or stable_hash(row) != claimed or row.get("schema") != TREASURY_CURVE_SCHEMA:
        raise AutonomousDirectorRuntimeError("Treasury curve result hash/schema drift")
    for field in (
        "authoritative_writes",
        "data_purchase_count",
        "q4_access_count_delta",
        "broker_connections",
        "orders",
    ):
        if int(row.get(field, -1)) != 0:
            raise AutonomousDirectorRuntimeError(
                f"Treasury curve worker safety counter drift: {field}"
            )
    if row.get("promotion_status") is not None:
        raise AutonomousDirectorRuntimeError(
            "Treasury curve tripwire attempted a promotion"
        )
    rules = [dict(item) for item in row.get("rule_specs") or ()]
    if len(rules) != 16 or len({str(item.get("rule_id")) for item in rules}) != 16:
        raise AutonomousDirectorRuntimeError("Treasury frozen rule inventory drift")

    if status == TREASURY_CURVE_WAITING_STATUS:
        if (
            not allow_waiting
            or row.get("decision") != TREASURY_CURVE_WAITING_STATUS
            or row.get("economic_result_created") is not False
            or row.get("evidence_role") is not None
        ):
            raise AutonomousDirectorRuntimeError(
                "Treasury waiting result crossed an economic boundary"
            )
        return {**row, "result_hash": claimed}

    candidates = [dict(item) for item in row.get("candidate_results") or ()]
    summary = dict(row.get("economic_summary") or {})
    allowed_decisions = {
        "CURVE_RELATIVE_VALUE_TRIPWIRE_GREEN_DEVELOPMENT_ONLY",
        "CURVE_RELATIVE_VALUE_TRIPWIRE_WEAK",
        "CURVE_RELATIVE_VALUE_TRIPWIRE_FALSIFIED",
    }
    if (
        status != "COMPLETE_DEVELOPMENT_TRIPWIRE"
        or row.get("evidence_role") != TREASURY_CURVE_EVIDENCE_ROLE
        or row.get("decision") not in allowed_decisions
        or len(candidates) != 16
        or int(summary.get("rule_count", -1)) != len(candidates)
    ):
        raise AutonomousDirectorRuntimeError(
            "Treasury completed tripwire identity/evidence-role drift"
        )
    candidate_ids = [str(item.get("rule_id") or "") for item in candidates]
    if any(not value for value in candidate_ids) or len(set(candidate_ids)) != 16:
        raise AutonomousDirectorRuntimeError(
            "Treasury completed candidate inventory drift"
        )
    for candidate in candidates:
        labels = sorted(
            str(account.get("account_label") or "")
            for account in candidate.get("account_matrix") or ()
        )
        if labels != ["100K", "150K", "50K"]:
            raise AutonomousDirectorRuntimeError(
                "Treasury account-size matrix denominator drift"
            )
    return {**row, "result_hash": claimed}


def _verify_clean_cross_asset_daily_tripwire_result(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep the viewed cross-asset tripwire at its frozen Tier-E ceiling."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    gate = dict(row.get("frozen_gate") or {})
    passed = gate.get("passed") is True
    expected_status = (
        "CROSS_ASSET_DAILY_TRIPWIRE_PASSED_DEVELOPMENT_ONLY"
        if passed
        else "CROSS_ASSET_DAILY_DIRECTION_TRANSFER_FALSIFIED"
    )
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != CLEAN_CROSS_ASSET_DAILY_SCHEMA
        or row.get("branch_id") != CLEAN_CROSS_ASSET_DAILY_BRANCH_ID
        or row.get("status") != expected_status
        or row.get("evidence_role") != "VIEWED_DEVELOPMENT_TRIPWIRE_ONLY"
        or row.get("evidence_tier") != "E_DIAGNOSTIC_DEVELOPMENT"
        or row.get("promotion_status") is not None
        or row.get("independent_confirmation_claimed") is not False
        or int(row.get("event_count", -1)) != 28
        or int(row.get("account_cell_count", -1)) != 180
        or set(dict(row.get("start_counts") or {})) != {"5", "10", "20"}
        or not dict(gate.get("checks") or {})
        or passed is not all(
            bool(item) for item in dict(gate.get("checks") or {}).values()
        )
        or int(row.get("q4_access_count_delta", -1)) != 0
        or not math.isclose(
            float(row.get("incremental_data_spend_usd", -1.0)),
            0.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or int(row.get("broker_connections", -1)) != 0
        or int(row.get("orders", -1)) != 0
    ):
        raise AutonomousDirectorRuntimeError(
            "clean cross-asset daily tripwire identity, ceiling, or safety drift"
        )
    return {**row, "result_hash": claimed}


def _verify_session_safe_confirmation_feature_receipt(
    value: Mapping[str, Any], *, expected_card_hash: str = ""
) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    bundles = {
        str(key): dict(item)
        for key, item in dict(row.get("bundles") or {}).items()
    }
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != SESSION_SAFE_CONFIRMATION_FEATURE_SCHEMA
        or row.get("status") != "CAUSAL_FEATURE_BUNDLES_READY"
        or (expected_card_hash and row.get("decision_card_hash") != expected_card_hash)
        or row.get("future_outcomes_in_decision_bundle") is not False
        or set(bundles) != {"RTY", "YM", "ES"}
        or any(
            not str(item.get("path") or "")
            or not str(item.get("bundle_hash") or "")
            or int(item.get("row_count", 0)) <= 0
            for item in bundles.values()
        )
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation feature receipt drift"
        )
    return {**row, "result_hash": claimed}


def _verify_session_safe_m2k_mym_confirmation_result(
    value: Mapping[str, Any], *, expected_card_hash: str = ""
) -> dict[str, Any]:
    """Enforce the one-shot Tier-Q ceiling and exact episode denominators."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    gate = dict(row.get("confirmation_gate") or {})
    gate_checks = dict(gate.get("checks") or {})
    horizons = {
        str(key): dict(item)
        for key, item in dict(row.get("horizon_results") or {}).items()
    }
    adapter = dict(row.get("evidence_bundle_adapter") or {})
    components = dict(row.get("component_results") or {})
    passed = gate.get("passed") is True
    expected_status = (
        "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
        if passed
        else "FRESH_REPLICATION_FAILED_BRANCH_CLOSED"
    )
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != SESSION_SAFE_CONFIRMATION_SCHEMA
        or row.get("status") != "CONFIRMATION_CONSUMED_ONCE"
        or (expected_card_hash and row.get("decision_card_hash") != expected_card_hash)
        or row.get("account_label") != "50K"
        or row.get("source_evidence_tier") != "E"
        or row.get("resulting_evidence_status") != expected_status
        or set(horizons) != {"5", "10", "20"}
        or len(components) != 2
        or any(
            dict(item).get("recalibrated") is not False
            or dict(item).get("batch_stream_equal") is not True
            for item in components.values()
        )
        or not gate_checks
        or any(not isinstance(item, bool) for item in gate_checks.values())
        or passed is not all(gate_checks.values())
        or gate.get("evidence_ceiling")
        != "FRESH_REPLICATION_SUCCESS_TIER_Q_ELIGIBLE_NOT_TIER_C"
        or adapter.get("sealing_performed") is not False
        or row.get("retuning_performed") is not False
        or row.get("recalibration_performed") is not False
        or int(row.get("q4_access_count_delta", -1)) != 0
        or int(row.get("broker_connections", -1)) != 0
        or int(row.get("orders", -1)) != 0
        or row.get("promotion_status") is not None
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation identity, evidence ceiling, or safety drift"
        )

    expected_records = 0
    for horizon, result in horizons.items():
        normal = dict(result.get("normal") or {})
        stressed = dict(result.get("stressed") or {})
        start_count = int(result.get("full_coverage_start_count", -1))
        if (
            int(result.get("horizon_trading_days", -1)) != int(horizon)
            or start_count <= 0
            or int(normal.get("episode_count", -1)) != start_count
            or int(stressed.get("episode_count", -1)) != start_count
            or not 0 <= int(normal.get("pass_count", -1)) <= start_count
            or not 0 <= int(stressed.get("pass_count", -1)) <= start_count
            or not 0 <= int(normal.get("mll_breach_count", -1)) <= start_count
            or not 0 <= int(stressed.get("mll_breach_count", -1)) <= start_count
        ):
            raise AutonomousDirectorRuntimeError(
                f"session-safe confirmation {horizon}-day denominator drift"
            )
        expected_records += 2 * start_count
    records = [dict(item) for item in adapter.get("evaluated_policy_records") or ()]
    if (
        len(records) != expected_records
        or adapter.get("records_hash") != stable_hash(records)
        or any(
            item.get("record_hash")
            != stable_hash(
                {key: content for key, content in item.items() if key != "record_hash"}
            )
            for item in records
        )
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe confirmation EvidenceBundle denominator drift"
        )
    return {**row, "result_hash": claimed}


def _verify_session_safe_fast_book_result(
    value: Mapping[str, Any], *, expected_variant: str
) -> dict[str, Any]:
    """Verify one frozen session repair without granting inherited status."""

    if expected_variant not in SESSION_SAFE_REPAIR_VARIANTS:
        raise AutonomousDirectorRuntimeError(
            "session-safe verifier received an unfrozen repair variant"
        )
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    counts = dict(row.get("counters") or {})
    repair = dict(row.get("repair_contract") or {})
    gate = dict(row.get("repair_signal_gate") or {})
    checks = dict(gate.get("checks") or {})
    adapter = dict(row.get("evidence_bundle_adapter") or {})
    accounts = {
        str(key): dict(item)
        for key, item in dict(row.get("account_results") or {}).items()
    }
    expected_coverage = {"5": 36, "10": 13, "20": 4}
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != SESSION_SAFE_FAST_BOOK_SCHEMA
        or row.get("branch_id") != SESSION_SAFE_FAST_BOOK_BRANCH_ID
        or row.get("status") != "COMPLETE_SESSION_SAFE_FAST_BOOK_TRIPWIRE"
        or row.get("repair_variant") != expected_variant
        or row.get("source_policy_id") != SESSION_SAFE_FAST_BOOK_SOURCE_POLICY_ID
        or row.get("evidence_role") != SESSION_SAFE_FAST_BOOK_EVIDENCE_ROLE
        or row.get("evidence_tier") != "E"
        or row.get("promotion_status") is not None
        or row.get("status_inherited") is not False
        or row.get("read_only_worker") is not True
        or row.get("outbound_order_capability") is not False
        or repair.get("variant") != expected_variant
        or int(repair.get("scale_factor", -1)) != 3
        or repair.get("outcome_fields_used") is not False
        or repair.get("future_label_eligibility_used") is not False
        or repair.get("mandatory_flatten_local") != "15:10"
        or set(accounts) != {"50K", "100K", "150K"}
        or int(counts.get("cpu_shards_for_this_result", -1)) != 1
        or int(counts.get("maximum_parallel_cpu_shards", -1)) != 2
        or int(counts.get("source_component_count", -1)) != 3
        or int(counts.get("source_event_rows_reconstructed", 0)) <= 0
        or int(counts.get("repaired_session_violation_count", -1)) != 0
        or int(counts.get("data_purchase_count", -1)) != 0
        or int(counts.get("q4_access_count_delta", -1)) != 0
        or int(counts.get("broker_connections", -1)) != 0
        or int(counts.get("orders", -1)) != 0
        or int(counts.get("authoritative_writes", -1)) != 0
        or int(counts.get("promotion_count", -1)) != 0
        or adapter.get("sealing_performed") is not False
        or adapter.get("authoritative_writer_required_for_sealing") is not True
    ):
        raise AutonomousDirectorRuntimeError(
            f"session-safe {expected_variant} identity, tier, or safety drift"
        )
    if (
        sum(
            int(value)
            for value in dict(
                row.get("original_session_violation_count_by_component") or {}
            ).values()
        )
        != 8
        or sum(
            int(value)
            for value in dict(
                row.get("repaired_session_violation_count_by_component") or {}
            ).values()
        )
        != 0
    ):
        raise AutonomousDirectorRuntimeError(
            f"session-safe {expected_variant} session denominator drift"
        )

    exact_episode_count = 0
    for account_label, account in accounts.items():
        horizons = {
            str(key): dict(item)
            for key, item in dict(account.get("horizon_results") or {}).items()
        }
        if (
            account.get("hard_execution_contract_clean") is not True
            or account.get("promotion_status") is not None
            or account.get("evidence_tier") != "E_EXACT_DEVELOPMENT_TRIPWIRE"
            or set(horizons) != set(expected_coverage)
            or any(
                int(value) != 0
                for value in dict(
                    account.get(
                        "official_market_contract_cap_breach_count_by_component"
                    )
                    or {}
                ).values()
            )
        ):
            raise AutonomousDirectorRuntimeError(
                f"session-safe {expected_variant} {account_label} account drift"
            )
        for horizon, expected_count in expected_coverage.items():
            result = horizons[horizon]
            normal = dict(result.get("normal") or {})
            stressed = dict(result.get("stressed") or {})
            if (
                int(result.get("horizon_trading_days", -1)) != int(horizon)
                or int(result.get("full_coverage_start_count", -1))
                != expected_count
                or int(normal.get("episode_count", -1)) != expected_count
                or int(stressed.get("episode_count", -1)) != expected_count
                or int(normal.get("pass_count", -1)) < 0
                or int(stressed.get("pass_count", -1)) < 0
                or int(normal.get("pass_count", -1)) > expected_count
                or int(stressed.get("pass_count", -1)) > expected_count
                or int(normal.get("mll_breach_count", -1)) < 0
                or int(stressed.get("mll_breach_count", -1)) < 0
            ):
                raise AutonomousDirectorRuntimeError(
                    f"session-safe {expected_variant} exact episode denominator drift"
                )
            exact_episode_count += 2 * expected_count
    records = [dict(item) for item in adapter.get("evaluated_policy_records") or ()]
    if (
        exact_episode_count != int(counts.get("exact_account_replays", -1))
        or len(records) != exact_episode_count
        or adapter.get("records_hash") != stable_hash(records)
    ):
        raise AutonomousDirectorRuntimeError(
            f"session-safe {expected_variant} EvidenceBundle denominator drift"
        )
    if (
        gate.get("gate_role") != "DEVELOPMENT_TRIPWIRE_ONLY_NO_PROMOTION"
        or not checks
        or any(not isinstance(value, bool) for value in checks.values())
        or gate.get("passed") is not all(checks.values())
        or gate.get("gate_hash") != stable_hash(checks)
    ):
        raise AutonomousDirectorRuntimeError(
            f"session-safe {expected_variant} signal-gate drift"
        )
    expected_decision = (
        "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_VALIDATION"
        if gate["passed"]
        else "SESSION_SAFE_REPAIR_FALSIFIED_AT_TRIPWIRE"
    )
    if row.get("decision") != expected_decision:
        raise AutonomousDirectorRuntimeError(
            f"session-safe {expected_variant} decision drift"
        )
    return {**row, "result_hash": claimed}


def _verify_session_safe_fast_book_portfolio(
    value: Mapping[str, Any],
    completed: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    variants = {
        str(key): dict(item)
        for key, item in dict(row.get("repair_variants") or {}).items()
    }
    counts = dict(row.get("counters") or {})
    confirmation = dict(row.get("confirmation_contract") or {})
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != _SESSION_SAFE_PORTFOLIO_SCHEMA
        or row.get("status") != "COMPLETE_SESSION_SAFE_FAST_BOOK_PORTFOLIO"
        or row.get("branch_id") != SESSION_SAFE_FAST_BOOK_BRANCH_ID
        or row.get("source_policy_id") != SESSION_SAFE_FAST_BOOK_SOURCE_POLICY_ID
        or row.get("evidence_role") != SESSION_SAFE_FAST_BOOK_EVIDENCE_ROLE
        or row.get("evidence_tier") != "E"
        or row.get("promotion_status") is not None
        or row.get("status_inherited") is not False
        or row.get("read_only_workers") is not True
        or row.get("parent_writer_only") is not True
        or set(variants) != set(SESSION_SAFE_REPAIR_VARIANTS)
        or int(counts.get("repair_variant_count", -1)) != 2
        or int(counts.get("worker_authoritative_writes", -1)) != 0
        or int(counts.get("parent_branch_artifact_count", -1)) != 3
        or int(counts.get("data_purchase_count", -1)) != 0
        or int(counts.get("q4_access_count_delta", -1)) != 0
        or int(counts.get("broker_connections", -1)) != 0
        or int(counts.get("orders", -1)) != 0
        or int(counts.get("promotion_count", -1)) != 0
        or confirmation.get("account_label") != "50K"
        or int(confirmation.get("horizon_trading_days", -1)) != 20
        or confirmation.get("chronological") is not True
        or confirmation.get("frozen_before_outcomes") is not True
        or confirmation.get("mutation_allowed") is not False
        or confirmation.get("automatic_promotion_allowed") is not False
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe portfolio identity, tier, or safety drift"
        )
    signal_variants: list[str] = []
    exact_replays = 0
    for variant in SESSION_SAFE_REPAIR_VARIANTS:
        result = _verify_session_safe_fast_book_result(
            completed.get(variant) or {}, expected_variant=variant
        )
        summary = variants[variant]
        passed = dict(result["repair_signal_gate"])["passed"] is True
        if (
            summary.get("result_hash") != result["result_hash"]
            or summary.get("evidence_tier") != "E"
            or summary.get("promotion_status") is not None
            or summary.get("signal_gate_passed") is not passed
            or summary.get("frozen_account_label") != "50K"
            or int(summary.get("frozen_horizon_trading_days", -1)) != 20
        ):
            raise AutonomousDirectorRuntimeError(
                f"session-safe portfolio {variant} summary drift"
            )
        exact_replays += int(result["counters"]["exact_account_replays"])
        if passed:
            signal_variants.append(variant)
    if (
        list(row.get("signal_gate_passing_variants") or ()) != signal_variants
        or int(counts.get("signal_gate_pass_count", -1)) != len(signal_variants)
        or int(counts.get("exact_account_replays", -1)) != exact_replays
    ):
        raise AutonomousDirectorRuntimeError(
            "session-safe portfolio gate or replay denominator drift"
        )
    expected_decision = (
        "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_CHRONOLOGICAL_CONFIRMATION_50K_20D"
        if signal_variants
        else "SESSION_SAFE_REPAIR_PORTFOLIO_FALSIFIED"
    )
    expected_next = (
        "FREEZE_SESSION_SAFE_REPAIRS_AND_RUN_UNCHANGED_CHRONOLOGICAL_CONFIRMATION_50K_20D"
        if signal_variants
        else "CLOSE_SESSION_SAFE_REPAIRS_AND_DISPATCH_DISTINCT_CAUSAL_BRANCH"
    )
    if row.get("decision") != expected_decision or row.get("next_action") != expected_next:
        raise AutonomousDirectorRuntimeError(
            "session-safe portfolio decision or confirmation boundary drift"
        )
    return {**row, "result_hash": claimed}


def _session_safe_counts(
    branch_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Expose isolated Tier-E repair counters without inflating core totals."""

    key_by_variant = {
        "HORIZON_SAFE_ENTRY_CUTOFF": (
            "POST_BREADTH_SESSION_SAFE_HORIZON_CUTOFF"
        ),
        "DROP_OFFENDING_COMPONENT": (
            "POST_BREADTH_SESSION_SAFE_DROP_COMPONENT"
        ),
    }
    values: dict[str, dict[str, Any]] = {}
    for variant, key in key_by_variant.items():
        raw = branch_results.get(key) or {}
        if raw:
            values[variant] = _verify_session_safe_fast_book_result(
                raw, expected_variant=variant
            )
    counts: dict[str, Any] = {
        "session_safe_tier_e_result_count": len(values),
        "session_safe_signal_gate_pass_count": 0,
        "session_safe_exact_account_replay_count": 0,
        "session_safe_50k_20d_normal_episode_count": 0,
        "session_safe_50k_20d_stressed_episode_count": 0,
        "session_safe_50k_20d_normal_pass_count": 0,
        "session_safe_50k_20d_stressed_pass_count": 0,
        "session_safe_horizon_cutoff_exact_account_replay_count": 0,
        "session_safe_drop_component_exact_account_replay_count": 0,
        "session_safe_status": "NOT_RUN",
    }
    for variant, result in values.items():
        replay_count = int(result["counters"]["exact_account_replays"])
        selected = dict(
            result["account_results"]["50K"]["horizon_results"]["20"]
        )
        normal = dict(selected["normal"])
        stressed = dict(selected["stressed"])
        counts["session_safe_exact_account_replay_count"] += replay_count
        counts["session_safe_50k_20d_normal_episode_count"] += int(
            normal["episode_count"]
        )
        counts["session_safe_50k_20d_stressed_episode_count"] += int(
            stressed["episode_count"]
        )
        counts["session_safe_50k_20d_normal_pass_count"] += int(
            normal["pass_count"]
        )
        counts["session_safe_50k_20d_stressed_pass_count"] += int(
            stressed["pass_count"]
        )
        counts["session_safe_signal_gate_pass_count"] += int(
            dict(result["repair_signal_gate"])["passed"] is True
        )
        field = (
            "session_safe_horizon_cutoff_exact_account_replay_count"
            if variant == "HORIZON_SAFE_ENTRY_CUTOFF"
            else "session_safe_drop_component_exact_account_replay_count"
        )
        counts[field] = replay_count
    if len(values) == 2:
        portfolio_raw = branch_results.get("POST_BREADTH_SESSION_SAFE_PORTFOLIO")
        if portfolio_raw:
            portfolio = _verify_session_safe_fast_book_portfolio(
                portfolio_raw, values
            )
            counts["session_safe_status"] = str(portfolio["status"])
        else:
            counts["session_safe_status"] = "REPAIRS_COMPLETE_PORTFOLIO_PENDING"
    elif values:
        counts["session_safe_status"] = "PARTIAL_RESUME"
    return counts


def _session_safe_confirmation_counts(
    branch_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Expose fresh-replication counters without changing historical totals."""

    raw = branch_results.get("POST_BREADTH_SESSION_SAFE_CONFIRMATION") or {}
    counts: dict[str, Any] = {
        "session_safe_confirmation_status": "NOT_RUN",
        "session_safe_confirmation_economic_result_created": False,
        "session_safe_confirmation_feature_bundle_count": 0,
        "session_safe_confirmation_50k_20d_normal_episode_count": 0,
        "session_safe_confirmation_50k_20d_stressed_episode_count": 0,
        "session_safe_confirmation_50k_20d_normal_pass_count": 0,
        "session_safe_confirmation_50k_20d_stressed_pass_count": 0,
        "session_safe_confirmation_exact_episode_count": 0,
        "session_safe_confirmation_tier_q_eligible_count": 0,
        "session_safe_confirmation_automatic_tier_g_count": 0,
        "session_safe_confirmation_automatic_tier_c_count": 0,
        "session_safe_confirmation_automatic_tier_f_count": 0,
    }
    if not raw:
        return counts
    result = _verify_session_safe_m2k_mym_confirmation_result(raw)
    horizons = dict(result["horizon_results"])
    selected = dict(horizons["20"])
    normal = dict(selected["normal"])
    stressed = dict(selected["stressed"])
    counts.update(
        {
            "session_safe_confirmation_status": str(
                result["resulting_evidence_status"]
            ),
            "session_safe_confirmation_economic_result_created": True,
            "session_safe_confirmation_feature_bundle_count": 3,
            "session_safe_confirmation_50k_20d_normal_episode_count": int(
                normal["episode_count"]
            ),
            "session_safe_confirmation_50k_20d_stressed_episode_count": int(
                stressed["episode_count"]
            ),
            "session_safe_confirmation_50k_20d_normal_pass_count": int(
                normal["pass_count"]
            ),
            "session_safe_confirmation_50k_20d_stressed_pass_count": int(
                stressed["pass_count"]
            ),
            "session_safe_confirmation_exact_episode_count": sum(
                int(dict(cell)[scenario]["episode_count"])
                for cell in horizons.values()
                for scenario in ("normal", "stressed")
            ),
            "session_safe_confirmation_tier_q_eligible_count": int(
                dict(result["confirmation_gate"])["passed"] is True
            ),
        }
    )
    return counts


def _post_breadth_counts(
    branch_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Return new isolated counters; never add them to historical totals."""

    legal_raw = branch_results.get("POST_BREADTH_LEGAL_EXACT") or {}
    treasury_raw = branch_results.get("POST_BREADTH_TREASURY_CURVE") or {}
    legal = _verify_frozen_legal_frontier_result(legal_raw) if legal_raw else {}
    treasury = (
        _verify_treasury_curve_result(treasury_raw, allow_waiting=True)
        if treasury_raw
        else {}
    )
    legal_counts = dict(legal.get("counters") or {})
    treasury_summary = dict(treasury.get("economic_summary") or {})
    treasury_complete = treasury.get("status") == "COMPLETE_DEVELOPMENT_TRIPWIRE"
    return {
        "post_breadth_legal_frozen_cell_count": int(
            legal_counts.get("frozen_cell_count", 0)
        ),
        "post_breadth_legal_source_event_count": int(
            legal_counts.get("source_event_rows_reconstructed", 0)
        ),
        "post_breadth_legal_exact_account_replay_count": int(
            legal_counts.get("exact_account_replays", 0)
        ),
        "post_breadth_legal_diagnostic_pass_count": int(
            legal_counts.get(
                "diagnostic_exact_passes_all_horizons_and_scenarios", 0
            )
        ),
        "post_breadth_legal_admissible_pass_count": int(
            legal_counts.get(
                "admissible_exact_passes_all_horizons_and_scenarios", 0
            )
        ),
        "post_breadth_legal_status": str(legal.get("status") or "NOT_RUN"),
        "post_breadth_treasury_rule_count": (
            int(treasury_summary.get("rule_count", 0)) if treasury_complete else 0
        ),
        "post_breadth_treasury_final_stressed_episode_count": (
            int(
                treasury_summary.get(
                    "final_development_stressed_episode_count", 0
                )
            )
            if treasury_complete
            else 0
        ),
        "post_breadth_treasury_final_stressed_pass_count": (
            int(
                treasury_summary.get(
                    "final_development_stressed_pass_count", 0
                )
            )
            if treasury_complete
            else 0
        ),
        "post_breadth_treasury_status": str(
            treasury.get("status") or "NOT_RUN"
        ),
        "post_breadth_completed_economic_branch_count": int(bool(legal))
        + int(treasury_complete),
    }


def _verify_frozen_breadth_continuation_result(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Reject result inflation, retuning, Q4 access, or a Tier-C claim."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    statuses = {
        "BREADTH_CONTINUATION_TIER_G_GRADUATED",
        "BREADTH_CONTINUATION_FALSIFIED_TOMBSTONE_EXACT_SPEC",
    }
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != FROZEN_BREADTH_CONTINUATION_SCHEMA
        or str(row.get("status")) not in statuses
        or row.get("retuning_performed") is not False
        or row.get("calibration_reused_without_recalibration") is not True
        or row.get("tier_c_promoted") is not False
        or row.get("evidence_ceiling") != "TIER_G_DEVELOPMENT"
        or int(row.get("q4_access_count_delta", -1)) != 0
        or int(row.get("broker_connections", -1)) != 0
        or int(row.get("orders", -1)) != 0
    ):
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth result identity or evidence ceiling drift"
        )
    account_results = [dict(item) for item in row.get("account_results") or ()]
    labels = [str(item.get("account_label") or "") for item in account_results]
    if sorted(labels) != ["100K", "150K", "50K"] or len(set(labels)) != 3:
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth account-size denominator drift"
        )
    actual = sorted(
        str(item["account_label"])
        for item in account_results
        if bool(item.get("tier_g_graduated"))
    )
    declared = [str(item) for item in row.get("tier_g_account_labels") or ()]
    if declared != sorted(set(declared)) or declared != actual:
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth Tier-G labels differ from exact gates"
        )
    if bool(declared) != (row.get("promotion_status") == "TIER_G"):
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth promotion label drift"
        )
    if not declared and not isinstance(row.get("tombstone"), Mapping):
        raise AutonomousDirectorRuntimeError(
            "failed frozen-breadth branch lacks its exact tombstone"
        )
    return {**row, "result_hash": claimed}


def _frozen_breadth_counts(value: Mapping[str, Any]) -> dict[str, int]:
    normal = 0
    stressed = 0
    exact = 0
    for account in value.get("account_results") or ():
        for cell in dict(account).get("cells") or ():
            row = dict(cell)
            normal += int(dict(row.get("normal") or {}).get("episode_count", 0))
            stressed += int(dict(row.get("stressed") or {}).get("episode_count", 0))
            exact += int(row.get("exact_account_replays", 0))
    if exact != normal + stressed:
        raise AutonomousDirectorRuntimeError(
            "frozen-breadth exact replay count does not reconcile"
        )
    return {
        "normal_episode_count": normal,
        "stressed_episode_count": stressed,
        "exact_account_replay_count": exact,
    }


def _verify_fresh_confirmation_result(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify exact Tier-C passers without ever creating Tier-F evidence."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if (
        not claimed
        or stable_hash(row) != claimed
        or row.get("schema") != FRESH_CONFIRMATION_RESULT_SCHEMA
        or row.get("status") != "CONFIRMATION_CONSUMED_ONCE"
        or row.get("retuning_performed") is not False
        or row.get("recalibration_performed") is not False
        or row.get("independent_confirmation_claimed_only_for_gate_passers")
        is not True
    ):
        raise AutonomousDirectorRuntimeError(
            "fresh-confirmation result identity or causal contract drift"
        )
    for field in ("q4_access_count_delta", "broker_connections", "orders"):
        if int(row.get(field, -1)) != 0:
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation worker attempted a prohibited side effect"
            )
    results = [dict(item) for item in row.get("candidate_results") or ()]
    identifiers = [str(item.get("candidate_id") or "") for item in results]
    if not identifiers or len(set(identifiers)) != len(identifiers):
        raise AutonomousDirectorRuntimeError(
            "fresh-confirmation candidate denominator is empty or duplicated"
        )
    actual_passers = sorted(
        str(item["candidate_id"])
        for item in results
        if bool(item.get("tier_c_promoted"))
    )
    declared_passers = [str(value) for value in row.get("tier_c_candidate_ids") or ()]
    if declared_passers != sorted(set(declared_passers)) or declared_passers != actual_passers:
        raise AutonomousDirectorRuntimeError(
            "fresh-confirmation Tier-C count differs from exact gate passers"
        )
    for item in results:
        gate = dict(item.get("tier_c_gate") or {})
        promoted = bool(item.get("tier_c_promoted"))
        if (
            promoted != bool(gate.get("passed"))
            or (promoted and str(item.get("evidence_tier")) != "C")
            or (not promoted and str(item.get("evidence_tier")) == "C")
        ):
            raise AutonomousDirectorRuntimeError(
                "fresh-confirmation candidate evidence tier drift"
            )
    forbidden = {
        str(item.get("evidence_tier") or "")
        for item in results
        if str(item.get("evidence_tier") or "").startswith("F")
    }
    if forbidden or int(row.get("tier_f_count", 0)) != 0:
        raise AutonomousDirectorRuntimeError(
            "fresh-confirmation result attempted Tier-F inflation"
        )
    return {**row, "result_hash": claimed}


def _verify_breadth_tripwire_result(value: Mapping[str, Any]) -> dict[str, Any]:
    """Verify the tripwire's evidence hash and strict zero-side-effect contract."""

    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    economic_payload = {
        key: item
        for key, item in row.items()
        if key not in {"runtime_seconds", "completed_at_utc"}
    }
    allowed_statuses = {
        "CROSS_INDEX_BREADTH_TRIPWIRE_GREEN_DEVELOPMENT_ONLY",
        "CROSS_INDEX_BREADTH_TRIPWIRE_WEAK_DEVELOPMENT_ONLY",
        "CROSS_INDEX_BREADTH_TRIPWIRE_FALSIFIED",
        "NON_DECISIONAL_SUBSET_SMOKE_COMPLETE",
    }
    if (
        row.get("schema") != CROSS_INDEX_BREADTH_SCHEMA
        or str(row.get("status")) not in allowed_statuses
        or not claimed
        or stable_hash(economic_payload) != claimed
        or row.get("promotion_status") is not None
        or str(row.get("evidence_tier")) != "E_DIAGNOSTIC_DEVELOPMENT"
    ):
        raise AutonomousDirectorRuntimeError(
            "cross-index breadth result identity or evidence-tier drift"
        )
    counts = dict(row.get("counts") or {})
    gate = dict(row.get("gate") or {})
    for field in (
        "authoritative_promotion_count",
        "xfa_paths_started",
        "broker_connections",
        "orders",
        "q4_access_count_delta",
        "data_purchase_count",
        "database_writes",
        "registry_writes",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousDirectorRuntimeError(
                "cross-index breadth worker attempted a side effect"
            )
    if (
        int(gate.get("authoritative_promotion_count", -1)) != 0
        or int(gate.get("xfa_paths_started", -1)) != 0
        or gate.get("independent_confirmation_claimed") is not False
    ):
        raise AutonomousDirectorRuntimeError(
            "cross-index breadth gate attempted evidence inflation"
        )
    return {**row, "result_hash": claimed}


def _exploitation_worker(result_path: str) -> dict[str, Any]:
    """Read the immutable 0034 result and apply the one frozen keep/kill rule."""

    result = _load_verified_0034_result(Path(result_path))
    long_sample = (
        (result.get("economic_summary") or {}).get("long_sample")
        or result.get("long_sample")
        or {}
    )
    roles = long_sample.get("role_results") or result.get("role_results") or {}
    audited: dict[str, dict[str, Any]] = {}
    veto_incremental_pass = True
    for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
        row = dict(roles.get(role) or {})
        paired = _finite(row.get("paired_stressed_uplift_usd"), default=-math.inf)
        overlay = _finite(row.get("stressed_net_usd"), default=-math.inf)
        baseline = _finite(row.get("baseline_stressed_net_usd"), default=math.inf)
        role_pass = paired > 0.0 and overlay > baseline
        veto_incremental_pass = veto_incremental_pass and role_pass
        audited[role] = {
            "paired_stressed_uplift_usd": paired,
            "overlay_stressed_net_usd": overlay,
            "baseline_stressed_net_usd": baseline,
            "overlay_exceeds_baseline": overlay > baseline,
            "incremental_gate_passed": role_pass,
        }
    decision = (
        "BASELINE_AND_VETO_RETAINED_AS_UNCONFIRMED_REFERENCES"
        if veto_incremental_pass
        else "BASELINE_REFERENCE_RETAINED_VETO_INCREMENTAL_FAILED"
    )
    return {
        "branch_id": "0034_NQ_SELECTIVE_EXECUTION_CONFIRMATION",
        "status": "COMPLETE_BOUNDED_INTERNAL_ROBUSTNESS_DECISION",
        "decision": decision,
        "roles": audited,
        "veto_incremental_gate_passed": veto_incremental_pass,
        "baseline_retained_as_reference": True,
        "baseline_independently_confirmed": False,
        "fresh_confirmation_attempts_consumed": 0,
        "fresh_confirmation_attempts_remaining": 1,
        "retuning_performed": False,
        "promotion_status": None,
        "evidence_tier": "E",
        "next_materially_distinct_action": (
            "PREPARE_BASELINE_FOR_ONE_FRESH_CONFIRMATION_ATTEMPT"
            if not veto_incremental_pass
            else "PREPARE_BASELINE_AND_VETO_FOR_ONE_FRESH_CONFIRMATION_ATTEMPT"
        ),
    }


def _load_verified_0034_result(path: Path) -> dict[str, Any]:
    result = json.loads(path.read_text(encoding="utf-8"))
    claimed = str(result.get("result_hash") or "")
    payload = dict(result)
    payload.pop("result_hash", None)
    if (
        not claimed
        or stable_hash(payload) != claimed
        or result.get("status") != "COMPLETE"
        or result.get("campaign_id")
        not in {None, "hydra_selective_order_flow_veto_expansion_0034"}
        or result.get("campaign_mode")
        not in {None, "SELECTIVE_ORDER_FLOW_VETO_EXPANSION"}
        or result.get("decision")
        not in {None, "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK"}
        or result.get("independently_confirmed") not in {None, False}
    ):
        raise AutonomousDirectorRuntimeError("0034 terminal identity/result hash drift")
    receipt = result.get("evidence_bundle")
    # Production evidence must carry its sealed bundle.  Tiny unit fixtures may
    # bind a minimal immutable receipt through ``bundle_manifest_path``.
    if not isinstance(receipt, Mapping):
        raise AutonomousDirectorRuntimeError("0034 terminal EvidenceBundle absent")
    manifest_path = Path(
        str(receipt.get("manifest_path") or receipt.get("bundle_manifest_path") or "")
    )
    if not manifest_path.is_absolute():
        manifest_path = (path.parent / manifest_path).resolve()
    expected = str(
        receipt.get("manifest_sha256")
        or receipt.get("bundle_manifest_sha256")
        or ""
    )
    if not manifest_path.is_file() or expected != _file_sha256(manifest_path):
        raise AutonomousDirectorRuntimeError("0034 EvidenceBundle checksum drift")
    return result


def _nq_baseline_confirmation_prep_worker(result_path: str) -> dict[str, Any]:
    """Reconcile the frozen baseline only; never consume fresh confirmation."""

    result = _load_verified_0034_result(Path(result_path))
    long_sample = dict((result.get("economic_summary") or {}).get("long_sample") or {})
    roles = dict(long_sample.get("role_results") or {})
    evidence: dict[str, dict[str, float]] = {}
    for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
        row = dict(roles.get(role) or {})
        evidence[role] = {
            "baseline_normal_net_usd": _finite(row.get("baseline_normal_net_usd")),
            "baseline_stressed_net_usd": _finite(
                row.get("baseline_stressed_net_usd")
            ),
            "veto_normal_net_usd": _finite(row.get("normal_net_usd")),
            "veto_stressed_net_usd": _finite(row.get("stressed_net_usd")),
        }
    return {
        "branch_id": "FROZEN_NQ_STRUCTURAL_BASELINE_CONFIRMATION_PREP",
        "status": "COMPLETE_DEVELOPMENT_ONLY_CONFIRMATION_PREPARATION",
        "decision": "BASELINE_REFERENCE_FROZEN_FRESH_CONFIRMATION_DATA_UNAVAILABLE",
        "baseline_development_evidence": evidence,
        "veto_retained": False,
        "retuning_performed": False,
        "fresh_confirmation_evaluated": False,
        "fresh_confirmation_data_available": False,
        "independently_confirmed": False,
        "promotion_status": None,
        "evidence_tier": "E",
        "next_materially_distinct_action": "IDENTIFY_GENUINELY_FRESH_CONFIRMATION_WITHIN_AUTHORITY",
    }


def _baseline_account_cost_worker(
    result_path: str, rule_snapshot_path: str
) -> dict[str, Any]:
    """Compare account targets/prices for the frozen baseline without retuning."""

    result = _load_verified_0034_result(Path(result_path))
    snapshot = json.loads(Path(rule_snapshot_path).read_text(encoding="utf-8"))
    rules = _load_account_rules(Path(rule_snapshot_path))
    combine_rows = snapshot.get("combine") or snapshot.get("account_rules") or {}
    roles = dict(
        ((result.get("economic_summary") or {}).get("long_sample") or {}).get(
            "role_results"
        )
        or {}
    )
    validation_net = _finite(
        (roles.get("VALIDATION") or {}).get("baseline_stressed_net_usd")
    )
    final_net = _finite(
        (roles.get("FINAL_DEVELOPMENT") or {}).get("baseline_stressed_net_usd")
    )
    matrix: list[dict[str, Any]] = []
    for size in ACCOUNT_SIZES_USD:
        rule = rules[size]
        label = f"{size // 1_000}K"
        raw = dict(combine_rows.get(label) or combine_rows.get(str(size)) or {})
        monthly = _finite(
            raw.get("standard_monthly_price_usd")
            or raw.get("monthly_price_usd")
        )
        activation = _finite(
            raw.get("standard_activation_fee_usd")
            or raw.get("activation_fee_usd")
        )
        matrix.append(
            {
                "account_size_usd": size,
                "profit_target_usd": rule["profit_target_usd"],
                "validation_stressed_target_progress": validation_net
                / rule["profit_target_usd"],
                "final_development_stressed_target_progress": final_net
                / rule["profit_target_usd"],
                "standard_monthly_price_usd": monthly,
                "activation_fee_usd": activation,
                "development_pass_observed": False,
                "fresh_confirmation_evaluated": False,
            }
        )
    best = max(
        matrix,
        key=lambda row: (
            row["final_development_stressed_target_progress"],
            row["validation_stressed_target_progress"],
            -row["standard_monthly_price_usd"],
        ),
    )
    return {
        "branch_id": "ACCOUNT_SIZE_PAID_COST_FEASIBILITY",
        "status": "COMPLETE_DEVELOPMENT_ONLY_ACCOUNT_COST_DIAGNOSTIC",
        "decision": "NO_ACCOUNT_SIZE_HAS_CONFIRMED_PASS_EVIDENCE",
        "matrix": matrix,
        "development_reference_account_size_usd": best["account_size_usd"],
        "fresh_confirmation_evaluated": False,
        "retuning_performed": False,
        "promotion_status": None,
        "evidence_tier": "E",
        "next_materially_distinct_action": "AWAIT_FRESH_BASELINE_CONFIRMATION_WHILE_EXPLORATION_CONTINUES",
    }


def _cross_asset_feasibility_worker(root_path: str) -> dict[str, Any]:
    return _candidate_niche_feasibility(
        Path(root_path),
        branch_id="CROSS_ASSET_ROLE_FEASIBILITY",
        niche="CROSS_ASSET",
        maximum_candidates=512,
    )


def _market_session_feasibility_worker(root_path: str) -> dict[str, Any]:
    return _candidate_niche_feasibility(
        Path(root_path),
        branch_id="MARKET_SESSION_ROLE_FEASIBILITY",
        niche="MARKET_SESSION",
        maximum_candidates=768,
    )


def _candidate_niche_feasibility(
    root: Path,
    *,
    branch_id: str,
    niche: str,
    maximum_candidates: int,
    candidate_offset: int = 0,
) -> dict[str, Any]:
    """Aggregate immutable exact 0029 sleeve outcomes by a frozen niche."""

    candidates: list[tuple[str, dict[str, Any], Path]] = []
    eligible_seen = 0
    for wave in ("wave_01", "wave_02"):
        base = (
            root
            / "data/cache/economic_production/hydra_fast_pass_factory_0029"
            / wave
        )
        proposals = base / "structural_proposals.jsonl"
        if not proposals.is_file():
            continue
        with proposals.open("r", encoding="utf-8") as handle:
            for line in handle:
                if len(candidates) >= maximum_candidates:
                    break
                value = json.loads(line)
                spec = dict(value.get("candidate") or {})
                candidate_id = str(value.get("candidate_id") or "")
                if not candidate_id:
                    continue
                cross = bool(spec.get("cross_asset_reference_market"))
                if niche == "CROSS_ASSET" and not cross:
                    continue
                if eligible_seen < candidate_offset:
                    eligible_seen += 1
                    continue
                eligible_seen += 1
                candidates.append((candidate_id, spec, base))
        if len(candidates) >= maximum_candidates:
            break

    grouped: dict[str, list[tuple[float, float]]] = {}
    rows_read = 0
    for candidate_id, spec, base in candidates:
        episode_path = base / "stage2_episode_evidence" / f"{candidate_id}.jsonl.gz"
        if not episode_path.is_file():
            continue
        stressed: list[dict[str, Any]] = []
        for row in _iter_gzip_jsonl(episode_path):
            rows_read += 1
            if row.get("scenario") == "STRESSED_1_5X":
                stressed.append(dict(row.get("episode") or row))
        if not stressed:
            continue
        if niche == "CROSS_ASSET":
            key = str(spec.get("cross_asset_reference_market") or "NONE")
        elif niche == "TIMEFRAME":
            key = str(spec.get("timeframe") or "UNKNOWN")
        elif niche == "MECHANISM":
            key = str(spec.get("mechanism") or "UNKNOWN")
        elif niche == "DIRECTION_PROFILE":
            key = str(spec.get("direction_rule") or "UNKNOWN")
        elif niche == "HOLDING_HORIZON":
            key = str(spec.get("horizon") or "UNKNOWN")
        elif niche == "PAYOFF_GEOMETRY":
            key = f"{spec.get('favorable_r', 'NA')}R:{spec.get('adverse_r', 'NA')}R"
        elif niche == "RISK_PROFILE":
            key = str(spec.get("risk_level") or "UNKNOWN")
        elif niche == "MARKET_SESSION":
            key = ":".join(
                (
                    str(spec.get("market") or "UNKNOWN"),
                    str(spec.get("session_code") or "UNKNOWN"),
                    str(spec.get("mechanism") or "UNKNOWN"),
                )
            )
        else:
            raise AutonomousDirectorRuntimeError(
                f"unsupported recurring niche dimension: {niche}"
            )
        grouped.setdefault(key, []).append(
            (
                statistics.median(_finite(row.get("target_progress")) for row in stressed),
                statistics.median(_finite(row.get("net_pnl")) for row in stressed),
            )
        )
    niches = [
        {
            "niche": key,
            "candidate_count": len(values),
            "median_stressed_target_progress": statistics.median(
                value[0] for value in values
            ),
            "median_stressed_net_usd": statistics.median(
                value[1] for value in values
            ),
            "positive_stressed_candidate_count": sum(value[1] > 0.0 for value in values),
        }
        for key, values in sorted(grouped.items())
    ]
    positive = [row for row in niches if row["median_stressed_net_usd"] > 0.0]
    return {
        "branch_id": branch_id,
        "status": "COMPLETE_BOUNDED_EXISTING_EVIDENCE_FEASIBILITY",
        "decision": (
            "POSITIVE_NICHE_REQUIRES_EXACT_ACCOUNT_REPLAY"
            if positive
            else "NO_POSITIVE_MEDIAN_STRESSED_NICHE"
        ),
        "niche_dimension": niche,
        "candidate_count": len(candidates),
        "candidate_offset": candidate_offset,
        "source_episode_rows_reused": rows_read,
        "niche_results": niches,
        "positive_niche_count": len(positive),
        "new_exact_account_replays": 0,
        "new_combine_episodes": 0,
        "development_only": True,
        "promotion_status": None,
        "evidence_tier": "E",
        "next_materially_distinct_action": (
            "EXACTLY_REPLAY_BEST_POSITIVE_NICHE"
            if positive
            else "ADVANCE_TO_EVENT_TIME_REPRESENTATION"
        ),
    }


def _exploration_worker(
    episode_paths: Sequence[str],
    rule_snapshot_path: str,
    scale_factors: Sequence[float] = _DEFAULT_SCALE_FACTORS,
    policy_maximum: int = 256,
) -> dict[str, Any]:
    """Calculate a bounded legal feasibility envelope from immutable episodes."""

    rules = _load_account_rules(Path(rule_snapshot_path))
    summaries: dict[str, dict[str, Any]] = {}
    rows_read = 0
    source_hashes: dict[str, str] = {}
    source_paths: list[Path] = []
    for raw_path in sorted(set(str(value) for value in episode_paths)):
        path = Path(raw_path)
        if not path.is_file():
            continue
        source_paths.append(path)
        source_hashes[str(path)] = _file_sha256(path)
        for row in _iter_gzip_jsonl(path):
            rows_read += 1
            policy_id = str(row.get("policy_id") or (row.get("episode") or {}).get("policy_id") or "")
            scenario = str(row.get("scenario") or "")
            horizon = _horizon(row)
            if (
                not policy_id
                or scenario not in _SCENARIOS
                or horizon not in _HORIZONS
                or row.get("coverage_state") != "FULL_COVERAGE"
            ):
                continue
            episode = dict(row.get("episode") or row)
            target = _finite(episode.get("target_progress"))
            net = _finite(episode.get("net_pnl"))
            policy = summaries.setdefault(
                policy_id,
                {
                    "policy_id": policy_id,
                    "stressed_target_progress": [],
                    "stressed_net": [],
                },
            )
            if scenario == "STRESSED_1_5X":
                policy["stressed_target_progress"].append(target)
                policy["stressed_net"].append(net)

    ranked = sorted(
        (row for row in summaries.values() if row["stressed_target_progress"]),
        key=lambda row: (
            -statistics.median(row["stressed_target_progress"]),
            -statistics.median(row["stressed_net"]),
            row["policy_id"],
        ),
    )[: max(1, min(int(policy_maximum), 256))]

    # Keep the economic ordering identical while bounding memory.  The first
    # pass retains only ranking scalars; a second sequential pass materialises
    # full episodes solely for the frozen shortlist.
    selected = {str(row["policy_id"]): row for row in ranked}
    for policy in ranked:
        policy["rows"] = []
    selected_episode_rows_reloaded = 0
    for path in source_paths:
        for row in _iter_gzip_jsonl(path):
            policy_id = str(
                row.get("policy_id")
                or (row.get("episode") or {}).get("policy_id")
                or ""
            )
            policy = selected.get(policy_id)
            if policy is None:
                continue
            scenario = str(row.get("scenario") or "")
            horizon = _horizon(row)
            if (
                scenario not in _SCENARIOS
                or horizon not in _HORIZONS
                or row.get("coverage_state") != "FULL_COVERAGE"
            ):
                continue
            policy["rows"].append(
                {
                    "scenario": scenario,
                    "horizon": horizon,
                    "episode": dict(row.get("episode") or row),
                }
            )
            selected_episode_rows_reloaded += 1

    frontiers: list[dict[str, Any]] = []
    upper_bounds: list[dict[str, Any]] = []
    normal_episodes = 0
    stressed_episodes = 0
    for policy in ranked:
        rows = list(policy["rows"])
        normal_episodes += sum(row["scenario"] == "NORMAL" for row in rows)
        stressed_episodes += sum(row["scenario"] == "STRESSED_1_5X" for row in rows)
        for account_size in ACCOUNT_SIZES_USD:
            rule = rules[account_size]
            for horizon in _HORIZONS:
                subsets = {
                    scenario: [
                        row["episode"]
                        for row in rows
                        if row["horizon"] == horizon and row["scenario"] == scenario
                    ]
                    for scenario in _SCENARIOS
                }
                if not all(subsets.values()):
                    continue
                candidates: list[dict[str, Any]] = []
                for factor in scale_factors:
                    if float(factor) <= 0.0:
                        continue
                    by_scenario = {
                        scenario: _uniform_frontier_point(
                            policy_id=str(policy["policy_id"]),
                            episodes=subsets[scenario],
                            factor=float(factor),
                            rule=rule,
                            horizon=horizon,
                            scenario=scenario,
                        )
                        for scenario in _SCENARIOS
                    }
                    if all(row["legally_executable"] for row in by_scenario.values()):
                        candidates.append(
                            {
                                "scale_factor": float(factor),
                                "by_scenario": by_scenario,
                            }
                        )
                if candidates:
                    selected_factor = max(
                        candidates,
                        key=lambda candidate: (
                            candidate["by_scenario"]["STRESSED_1_5X"]["pass_rate"],
                            candidate["by_scenario"]["NORMAL"]["pass_rate"],
                            candidate["by_scenario"]["STRESSED_1_5X"][
                                "median_target_progress"
                            ],
                            candidate["by_scenario"]["NORMAL"][
                                "median_target_progress"
                            ],
                            -candidate["scale_factor"],
                        ),
                    )
                    frontiers.extend(selected_factor["by_scenario"].values())
                for scenario in _SCENARIOS:
                    upper_bounds.append(
                        _nondeployable_upper_bound(
                            str(policy["policy_id"]),
                            subsets[scenario],
                            rule,
                            horizon,
                            scenario,
                        )
                    )

    deployable_passes = sum(int(row["passes"]) for row in frontiers)
    upper_bound_passes = sum(int(row["passes"]) for row in upper_bounds)
    bottleneck = (
        "SUMMARY_FEASIBILITY_SHORTLIST_REQUIRES_EXACT_REPLAY"
        if deployable_passes
        else "SUMMARY_FEASIBILITY_NO_THRESHOLD_HIT"
    )
    best = max(
        frontiers,
        key=lambda row: (
            row["pass_rate"],
            row["median_target_progress"],
            row["median_scaled_net_usd"],
        ),
        default=None,
    )
    positive_stressed = {
        row["policy_id"]
        for row in frontiers
        if row["scenario"] == "STRESSED_1_5X"
        and row["median_scaled_net_usd"] > 0.0
    }
    return {
        "branch_id": "DIRECT_LEGAL_ACCOUNT_FEASIBILITY",
        "status": "COMPLETE_BOUNDED_SUMMARY_FEASIBILITY_SCREEN",
        "decision": bottleneck,
        "accounting_scope": "AGGREGATED_SUMMARY_TRANSFORMATION_NOT_EXACT_REPLAY",
        "exact_account_replay_required_before_any_PASS_claim": True,
        "episode_files_scanned": len(source_hashes),
        "episode_rows_read": rows_read,
        "selected_episode_rows_reloaded": selected_episode_rows_reloaded,
        "eligible_policy_count": len(summaries),
        "selected_policy_count": len(ranked),
        "selected_policy_ids": [str(row["policy_id"]) for row in ranked],
        "source_file_hashes": source_hashes,
        "scale_factors": [float(value) for value in scale_factors],
        "uniform_legal_frontier": frontiers,
        "causal_quality_tier_frontier": {
            "status": "DEFERRED_DECISION_TIME_QUALITY_LEDGER_REQUIRED",
            "promotable": False,
            "reason": "aggregated account episodes do not encode a frozen decision-time quality tier",
        },
        "non_deployable_upper_bound": {
            "promotable": False,
            "uses_full_trajectory_information": True,
            "frontier": upper_bounds,
        },
        "deployable_pass_count": deployable_passes,
        "actual_exact_pass_count": 0,
        "non_deployable_upper_bound_pass_count": upper_bound_passes,
        "upper_bound_excluded_from_bottleneck_verdict": True,
        "positive_stressed_policy_count": len(positive_stressed),
        "normal_episode_count": normal_episodes,
        "stressed_episode_count": stressed_episodes,
        "best_deployable_frontier_point": best,
        "promotion_status": None,
        "evidence_tier": "H",
        "next_materially_distinct_action": (
            "FREEZE_AND_EXACTLY_REPLAY_DEPLOYABLE_LEGAL_FRONTIER"
            if deployable_passes
            else "QUEUE_EVENT_TIME_CROSS_ASSET_FEASIBILITY"
        ),
    }


def _uniform_frontier_point(
    *,
    policy_id: str,
    episodes: Sequence[Mapping[str, Any]],
    factor: float,
    rule: Mapping[str, Any],
    horizon: int,
    scenario: str,
) -> dict[str, Any]:
    evaluations = [_scaled_episode(row, factor, rule) for row in episodes]
    executable = [row for row in evaluations if row["contract_limit_ok"]]
    if not executable:
        return {
            "policy_id": policy_id,
            "account_size_usd": int(rule["account_size_usd"]),
            "horizon_trading_days": horizon,
            "scenario": scenario,
            "scale_factor": factor,
            "legally_executable": False,
            "episodes": 0,
            "passes": 0,
            "pass_rate": 0.0,
            "median_target_progress": 0.0,
            "median_scaled_net_usd": 0.0,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer_usd": 0.0,
        }
    return {
        "policy_id": policy_id,
        "account_size_usd": int(rule["account_size_usd"]),
        "horizon_trading_days": horizon,
        "scenario": scenario,
        "scale_factor": factor,
        "legally_executable": len(executable) == len(evaluations),
        "episodes": len(executable),
        "passes": sum(bool(row["passed"]) for row in executable),
        "pass_rate": sum(bool(row["passed"]) for row in executable) / len(executable),
        "median_target_progress": statistics.median(
            row["target_progress"] for row in executable
        ),
        "median_scaled_net_usd": statistics.median(
            row["scaled_net_usd"] for row in executable
        ),
        "mll_breach_rate": sum(bool(row["mll_breached"]) for row in executable)
        / len(executable),
        "minimum_mll_buffer_usd": min(
            row["minimum_mll_buffer_usd"] for row in executable
        ),
        "consistency_compliance_rate": sum(
            bool(row["consistency_ok"]) for row in executable
        )
        / len(executable),
    }


def _scaled_episode(
    episode: Mapping[str, Any], factor: float, rule: Mapping[str, Any]
) -> dict[str, Any]:
    maximum_mini = max(_finite(episode.get("maximum_mini_equivalent")), 0.0)
    contract_limit_ok = maximum_mini * factor <= float(rule["maximum_mini_contracts"]) + 1e-9
    base_mll = max(_finite(episode.get("base_mll_usd"), default=4_500.0), 1.0)
    base_buffer = _finite(episode.get("minimum_mll_buffer"), default=base_mll)
    observed_drawdown = max(base_mll - base_buffer, 0.0)
    buffer = float(rule["maximum_loss_limit_usd"]) - factor * observed_drawdown
    mll_breached = bool(episode.get("mll_breached")) or buffer <= 0.0
    scaled_net = _finite(episode.get("net_pnl")) * factor
    daily = [
        _finite(row.get("day_pnl")) * factor
        for row in (episode.get("daily_path") or ())
        if isinstance(row, Mapping)
    ]
    positive = [value for value in daily if value > 0.0]
    consistency = max(positive, default=0.0) / max(scaled_net, 1e-12) if scaled_net > 0 else math.inf
    consistency_ok = consistency <= float(rule["consistency_target"]) + 1e-12
    target = float(rule["profit_target_usd"])
    minimum_days = int(rule.get("minimum_trading_days", 2))
    passed = (
        contract_limit_ok
        and not mll_breached
        and len(daily) >= minimum_days
        and scaled_net >= target
        and consistency_ok
    )
    return {
        "contract_limit_ok": contract_limit_ok,
        "mll_breached": mll_breached,
        "minimum_mll_buffer_usd": buffer,
        "scaled_net_usd": scaled_net,
        "target_progress": scaled_net / target,
        "consistency_ok": consistency_ok,
        "passed": passed,
    }


def _nondeployable_upper_bound(
    policy_id: str,
    episodes: Sequence[Mapping[str, Any]],
    rule: Mapping[str, Any],
    horizon: int,
    scenario: str,
) -> dict[str, Any]:
    passes = 0
    progress: list[float] = []
    for episode in episodes:
        maximum_mini = max(_finite(episode.get("maximum_mini_equivalent")), 1e-12)
        factor = float(rule["maximum_mini_contracts"]) / maximum_mini
        positive = [
            max(_finite(row.get("day_pnl")), 0.0) * factor
            for row in (episode.get("daily_path") or ())
            if isinstance(row, Mapping)
        ]
        total = sum(positive)
        consistency = max(positive, default=0.0) / max(total, 1e-12)
        passed = (
            len(positive) >= int(rule.get("minimum_trading_days", 2))
            and total >= float(rule["profit_target_usd"])
            and consistency <= float(rule["consistency_target"]) + 1e-12
        )
        passes += int(passed)
        progress.append(total / float(rule["profit_target_usd"]))
    return {
        "policy_id": policy_id,
        "account_size_usd": int(rule["account_size_usd"]),
        "horizon_trading_days": horizon,
        "scenario": scenario,
        "episodes": len(episodes),
        "passes": passes,
        "pass_rate": passes / max(len(episodes), 1),
        "median_target_progress": statistics.median(progress) if progress else 0.0,
        "promotable": False,
        "uses_future_trajectory_information": True,
    }


def _load_account_rules(path: Path) -> dict[int, dict[str, Any]]:
    snapshot = json.loads(path.read_text(encoding="utf-8"))
    candidates: Any = None
    for key in ("account_rules", "combine_accounts", "accounts"):
        if key in snapshot:
            candidates = snapshot[key]
            break
    if candidates is None and isinstance(snapshot.get("combine"), Mapping):
        combine = snapshot["combine"]
        candidates = (
            combine.get("account_sizes")
            or combine.get("accounts")
            or combine
        )
    rows: list[Mapping[str, Any]] = []
    if isinstance(candidates, Mapping):
        for key, value in candidates.items():
            if isinstance(value, Mapping):
                row = dict(value)
                row.setdefault("account_size_usd", _digits(key))
                rows.append(row)
    elif isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes)):
        rows = [value for value in candidates if isinstance(value, Mapping)]
    output: dict[int, dict[str, Any]] = {}
    for raw in rows:
        size = int(raw.get("account_size_usd") or raw.get("size_usd") or 0)
        if size not in ACCOUNT_SIZES_USD:
            continue
        output[size] = {
            "account_size_usd": size,
            "profit_target_usd": _required_positive(
                raw, "profit_target_usd", "profit_target"
            ),
            "maximum_loss_limit_usd": _required_positive(
                raw, "maximum_loss_limit_usd", "mll_usd", "maximum_loss_limit"
            ),
            "maximum_mini_contracts": _required_positive(
                raw, "maximum_mini_contracts", "max_mini_contracts"
            ),
            "consistency_target": _required_positive(
                raw,
                "consistency_target",
                "consistency_target_fraction",
                "consistency_fraction",
                "consistency_limit",
            ),
            "minimum_trading_days": int(
                raw.get("minimum_trading_days") or raw.get("minimum_pass_days") or 2
            ),
        }
    if set(output) != set(ACCOUNT_SIZES_USD):
        raise AutonomousDirectorRuntimeError(
            "official rule snapshot lacks complete 50K/100K/150K account rules"
        )
    return output


def _required_positive(value: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        number = _finite(value.get(key))
        if number > 0.0:
            return number
    raise AutonomousDirectorRuntimeError(f"official rule value absent: {'/'.join(keys)}")


def _resolve_exploitation_path(root: Path, manifest: Mapping[str, Any]) -> Path:
    lane = list(manifest["branch_portfolio"]["lanes"])[0]
    relative = str(
        lane.get("source_result_path")
        or "reports/economic_evolution/selective_order_flow_veto_expansion_0034/economic_production_result.json"
    )
    path = (root / relative).resolve()
    _assert_within(root, path)
    if not path.is_file():
        raise AutonomousDirectorRuntimeError("immutable 0034 source result missing")
    return path


def _resolve_episode_paths(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[Path, ...]:
    lane = list(manifest["branch_portfolio"]["lanes"])[1]
    globs = tuple(
        str(value)
        for value in lane.get("episode_source_globs")
        or (
            "data/cache/economic_production/hydra_fast_pass_factory_0029/wave_01/books_episode_evidence/*.jsonl.gz",
            "data/cache/economic_production/hydra_fast_pass_factory_0029/wave_01/sleeves_episode_evidence/*.jsonl.gz",
            "data/cache/economic_production/hydra_fast_pass_factory_0029/wave_02/books_episode_evidence/*.jsonl.gz",
            "data/cache/economic_production/hydra_fast_pass_factory_0029/wave_02/sleeves_episode_evidence/*.jsonl.gz",
        )
    )
    paths: list[Path] = []
    for pattern in globs:
        if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
            raise AutonomousDirectorRuntimeError("unsafe 0029 episode source glob")
        paths.extend(root.glob(pattern))
    resolved = tuple(sorted({value.resolve() for value in paths}))
    if not resolved or any(root not in value.parents for value in resolved):
        raise AutonomousDirectorRuntimeError("no safe immutable 0029 gzip episodes found")
    return resolved


def _scale_factors(manifest: Mapping[str, Any]) -> tuple[float, ...]:
    lane = list(manifest["branch_portfolio"]["lanes"])[1]
    raw = lane.get("uniform_scale_factors") or _DEFAULT_SCALE_FACTORS
    values = tuple(sorted({float(value) for value in raw if 0.0 < float(value) <= 100.0}))
    if not values or len(values) > 32:
        raise AutonomousDirectorRuntimeError("bounded legal scale frontier drift")
    return values


def _exploration_policy_maximum(manifest: Mapping[str, Any]) -> int:
    lane = list(manifest["branch_portfolio"]["lanes"])[1]
    return max(1, min(int(lane.get("policy_maximum", 256)), 256))


def _exact_result_metrics(
    branch_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Extract only counters supported by exact chronological account paths."""

    composite = branch_results.get("EXACT_0029_COMPOSITE") or {}
    event = branch_results.get("EVENT_TIME") or {}
    marginal_books = branch_results.get("MARGINAL_BOOKS") or {}
    consistency_direct = branch_results.get("CONSISTENCY_DIRECT") or {}
    event_time_safety = branch_results.get("EVENT_TIME_SAFETY") or {}
    tier_g_controls = branch_results.get("TIER_G_CONTROLS") or {}
    if composite:
        counters = dict(composite.get("aggregate_counters") or {})
        exact_ids = set(
            str(value)
            for values in dict(composite.get("candidate_pass_sets") or {}).values()
            for value in values
        )
        exact_ids.update(
            str(value)
            for value in (
                composite.get("source_inventory") or {}
            ).get("sealed_initial_candidate_ids", ())
        )
        completed = int(composite.get("completed_candidate_count", 0))
        if len(exact_ids) > completed:
            raise AutonomousDirectorRuntimeError("exact candidate union drift")
        event_counters = dict(event.get("counters") or {})
        event_ids = {
            str(value)
            for value in (
                event.get("source_population") or {}
            ).get("selected_candidate_ids", ())
        }
        selected = completed + len(event_ids)
        normal_episodes = int(counters.get("exact_normal_account_replays", 0)) + int(
            event_counters.get("exact_normal_account_replays", 0)
        )
        stressed_episodes = int(
            counters.get("exact_stressed_account_replays", 0)
        ) + int(event_counters.get("exact_stressed_account_replays", 0))
        total_episodes = int(counters.get("exact_account_replays", 0)) + int(
            event_counters.get("exact_chronological_account_replays", 0)
        )
        if normal_episodes != stressed_episodes or total_episodes != (
            normal_episodes + stressed_episodes
        ):
            raise AutonomousDirectorRuntimeError("exact episode denominator drift")
        normal_pass_ids = set(
            str(value)
            for value in (
                composite.get("candidate_pass_sets") or {}
            ).get("normal", ())
        )
        stressed_pass_ids = set(
            str(value)
            for value in (
                composite.get("candidate_pass_sets") or {}
            ).get("stressed", ())
        )
        for candidate in event.get("candidate_results") or ():
            cells = [
                cell
                for account in candidate.get("account_size_matrix") or ()
                for cell in account.get("frontier") or ()
            ]
            candidate_id = str(candidate.get("candidate_id") or "")
            if any(int((cell.get("normal") or {}).get("pass_count", 0)) > 0 for cell in cells):
                normal_pass_ids.add(candidate_id)
            if any(int((cell.get("stressed") or {}).get("pass_count", 0)) > 0 for cell in cells):
                stressed_pass_ids.add(candidate_id)
        book_normal_rates: list[float] = []
        book_stressed_rates: list[float] = []
        positive_stressed_policy_ids: set[str] = set(stressed_pass_ids)
        for book in marginal_books.get("book_results") or ():
            policy_id = str(book.get("policy_id") or "")
            summaries = dict(book.get("summaries") or {})
            normal_rows = list(dict(summaries.get("NORMAL") or {}).values())
            stressed_rows = list(
                dict(summaries.get("STRESSED_1_5X") or {}).values()
            )
            if any(int(row.get("pass_count", 0)) > 0 for row in normal_rows):
                normal_pass_ids.add(policy_id)
            if any(int(row.get("pass_count", 0)) > 0 for row in stressed_rows):
                stressed_pass_ids.add(policy_id)
            if any(float(row.get("net_total", 0.0)) > 0.0 for row in stressed_rows):
                positive_stressed_policy_ids.add(policy_id)
            book_normal_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in normal_rows
            )
            book_stressed_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in stressed_rows
            )
        direct_normal_rates: list[float] = []
        direct_stressed_rates: list[float] = []
        for policy in consistency_direct.get("selected_policy_results") or ():
            policy_id = str(policy.get("policy_id") or "")
            summaries = dict(policy.get("summaries") or {})
            normal_rows = list(dict(summaries.get("NORMAL") or {}).values())
            stressed_rows = list(
                dict(summaries.get("STRESSED_1_5X") or {}).values()
            )
            if any(int(row.get("pass_count", 0)) > 0 for row in normal_rows):
                normal_pass_ids.add(policy_id)
            if any(int(row.get("pass_count", 0)) > 0 for row in stressed_rows):
                stressed_pass_ids.add(policy_id)
            if any(float(row.get("net_total", 0.0)) > 0.0 for row in stressed_rows):
                positive_stressed_policy_ids.add(policy_id)
            direct_normal_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in normal_rows
            )
            direct_stressed_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in stressed_rows
            )
        safety_normal_rates: list[float] = []
        safety_stressed_rates: list[float] = []
        for candidate in event_time_safety.get("candidate_results") or ():
            selected_result = dict(candidate.get("selected_result") or {})
            policy_id = str(selected_result.get("policy_id") or "")
            heldout = dict(
                dict(selected_result.get("roles") or {}).get(
                    "HELD_OUT_DEVELOPMENT"
                )
                or {}
            )
            normal_rows = [
                dict(dict(heldout.get(str(horizon)) or {}).get("BASE") or {})
                for horizon in _HORIZONS
            ]
            stressed_rows = [
                dict(
                    dict(heldout.get(str(horizon)) or {}).get("STRESS_1_5X")
                    or {}
                )
                for horizon in _HORIZONS
            ]
            if any(int(row.get("pass_count", 0)) > 0 for row in normal_rows):
                normal_pass_ids.add(policy_id)
            if any(int(row.get("pass_count", 0)) > 0 for row in stressed_rows):
                stressed_pass_ids.add(policy_id)
            if any(
                float(row.get("net_total_usd", 0.0)) > 0.0
                for row in stressed_rows
            ):
                positive_stressed_policy_ids.add(policy_id)
            safety_normal_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in normal_rows
            )
            safety_stressed_rates.extend(
                float(row.get("pass_rate", 0.0)) for row in stressed_rows
            )
        book_counts = dict(marginal_books.get("counts") or {})
        book_episode_count = int(book_counts.get("completed_episode_count", 0))
        if book_episode_count % 2:
            raise AutonomousDirectorRuntimeError(
                "marginal-book normal/stressed episode denominator drift"
            )
        normal_episodes += book_episode_count // 2
        stressed_episodes += book_episode_count // 2
        total_episodes += book_episode_count
        selected += int(book_counts.get("primary_book_exact_replay_count", 0))
        direct_counts = dict(consistency_direct.get("counts") or {})
        direct_episode_count = int(direct_counts.get("completed_episode_count", 0))
        if direct_episode_count % 2:
            raise AutonomousDirectorRuntimeError(
                "consistency-direct normal/stressed episode denominator drift"
            )
        normal_episodes += direct_episode_count // 2
        stressed_episodes += direct_episode_count // 2
        total_episodes += direct_episode_count
        selected += int(direct_counts.get("direct_policy_exact_replay_count", 0))
        safety_counts = dict(event_time_safety.get("counts") or {})
        safety_episode_count = int(safety_counts.get("exact_episode_count", 0))
        if safety_episode_count % 2:
            raise AutonomousDirectorRuntimeError(
                "event-time safety normal/stressed episode denominator drift"
            )
        normal_episodes += safety_episode_count // 2
        stressed_episodes += safety_episode_count // 2
        total_episodes += safety_episode_count
        safety_candidate_count = int(
            safety_counts.get("selected_candidate_count", 0)
        )
        safety_profile_count = int(safety_counts.get("profile_count", 0))
        selected += safety_candidate_count * safety_profile_count
        tier_g_counts = dict(tier_g_controls.get("counts") or {})
        tier_g_episode_count = int(
            tier_g_counts.get("exact_account_replay_count", 0)
        )
        if tier_g_episode_count % 2:
            raise AutonomousDirectorRuntimeError(
                "Tier-G controls normal/stressed episode denominator drift"
            )
        normal_episodes += tier_g_episode_count // 2
        stressed_episodes += tier_g_episode_count // 2
        total_episodes += tier_g_episode_count
        tier_g_candidate_count = int(
            tier_g_counts.get("selected_candidate_count", 0)
        )
        selected += tier_g_candidate_count
        positive_stressed_policy_ids.update(stressed_pass_ids)
        control_replay_operations = int(
            book_counts.get("supporting_policy_exact_replay_count", 0)
        ) + int(
            direct_counts.get("identity_control_exact_replay_count", 0)
        ) + safety_candidate_count + int(
            tier_g_counts.get("synthetic_control_count", 0)
        )
        best = composite.get("best_exact_frontier_point")
        normal_best = max(
            float(((best or {}).get("normal") or {}).get("pass_rate", 0.0)),
            max(book_normal_rates, default=0.0),
            max(direct_normal_rates, default=0.0),
            max(safety_normal_rates, default=0.0),
        )
        stressed_best = float(
            ((best or {}).get("stressed") or {}).get("pass_rate", 0.0)
        )
        stressed_best = max(
            stressed_best,
            max(book_stressed_rates, default=0.0),
            max(direct_stressed_rates, default=0.0),
            max(safety_stressed_rates, default=0.0),
        )
        all_normal_rates = (
            book_normal_rates + direct_normal_rates + safety_normal_rates
        )
        all_stressed_rates = (
            book_stressed_rates + direct_stressed_rates + safety_stressed_rates
        )
        return {
            "selected_candidates": selected,
            "exact_account_replays": selected,
            "exact_account_episode_replays": total_episodes,
            "normal_account_replays": normal_episodes,
            "stressed_account_replays": stressed_episodes,
            "normal_pass_candidate_count": len(normal_pass_ids),
            "stressed_pass_candidate_count": len(stressed_pass_ids),
            "positive_stressed_candidate_count": len(positive_stressed_policy_ids),
            "control_policy_replay_operations": control_replay_operations,
            "best_normal_pass_rate": normal_best,
            "best_stressed_pass_rate": stressed_best,
            "median_normal_pass_rate": (
                statistics.median(all_normal_rates) if all_normal_rates else 0.0
            ),
            "median_stressed_pass_rate": (
                statistics.median(all_stressed_rates)
                if all_stressed_rates
                else 0.0
            ),
            "best_exact_frontier_point": best,
        }

    exact = branch_results.get("EXACT_0029") or {}
    counters = dict(exact.get("counters") or {})
    cells = [
        cell
        for candidate in exact.get("results") or ()
        if int(
            ((candidate.get("session_contract") or {}).get("event_violation_count", 0))
        )
        == 0
        for cell in candidate.get("frontier") or ()
        if cell.get("legally_executable") is True
        and cell.get("account_rule_compliant") is True
    ]
    normal_pass_ids = {
        str(cell.get("candidate_id"))
        for cell in cells
        if int((cell.get("normal") or {}).get("pass_count", 0)) > 0
    }
    stressed_pass_ids = {
        str(cell.get("candidate_id"))
        for cell in cells
        if int((cell.get("stressed") or {}).get("pass_count", 0)) > 0
    }
    positive_stressed_ids = {
        str(cell.get("candidate_id"))
        for cell in cells
        if float((cell.get("stressed") or {}).get("net_total_usd", 0.0)) > 0.0
    }
    normal_rates = [
        float((cell.get("normal") or {}).get("pass_rate", 0.0)) for cell in cells
    ]
    stressed_rates = [
        float((cell.get("stressed") or {}).get("pass_rate", 0.0)) for cell in cells
    ]
    return {
        "selected_candidates": int(counters.get("qd_selected_candidate_count", 0)),
        # The production-kernel KPI counts policies replayed exactly; the
        # immutable exact branch separately counts every chronological account
        # episode.  Keeping the two denominators distinct preserves the generic
        # invariant exact-policy-replays <= unique-policies-screened.
        "exact_account_replays": int(counters.get("qd_selected_candidate_count", 0)),
        "exact_account_episode_replays": int(
            counters.get("exact_account_replays", 0)
        ),
        "normal_account_replays": int(
            counters.get("exact_normal_account_replays", 0)
        ),
        "stressed_account_replays": int(
            counters.get("exact_stressed_account_replays", 0)
        ),
        "normal_pass_candidate_count": len(normal_pass_ids),
        "stressed_pass_candidate_count": len(stressed_pass_ids),
        "positive_stressed_candidate_count": len(positive_stressed_ids),
        "control_policy_replay_operations": 0,
        "best_normal_pass_rate": max(normal_rates, default=0.0),
        "best_stressed_pass_rate": max(stressed_rates, default=0.0),
        "median_normal_pass_rate": statistics.median(normal_rates)
        if normal_rates
        else 0.0,
        "median_stressed_pass_rate": statistics.median(stressed_rates)
        if stressed_rates
        else 0.0,
        "best_exact_frontier_point": exact.get("best_exact_frontier_point"),
    }


def _relay_evidence_counts(
    branch_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Return persisted relay counts without upgrading diagnostic evidence."""

    graduation_counts = dict(
        (branch_results.get("TIER_G_GRADUATION") or {}).get("counts") or {}
    )
    handoff_counts = dict(
        (branch_results.get("TIER_G_XFA_HANDOFF") or {}).get("counts") or {}
    )
    diagnostic_counts = dict(
        (branch_results.get("TIER_G_XFA_DIAGNOSTIC") or {}).get("counts") or {}
    )
    breadth = branch_results.get("CROSS_INDEX_BREADTH") or {}
    breadth_counts = dict(breadth.get("counts") or {})
    breadth_gate = dict(breadth.get("gate") or {})
    fresh = branch_results.get("FRESH_CONFIRMATION") or {}
    fresh_verified = (
        _verify_fresh_confirmation_result(fresh) if fresh else {}
    )
    continuation = branch_results.get("FROZEN_BREADTH_CONTINUATION") or {}
    continuation_verified = (
        _verify_frozen_breadth_continuation_result(continuation)
        if continuation
        else {}
    )
    tier_c_ids = tuple(fresh_verified.get("tier_c_candidate_ids") or ())
    handoff_transitions = int(handoff_counts.get("ready_xfa_transition_count", 0))
    diagnostic_transitions = int(
        diagnostic_counts.get("combine_transition_count", 0)
    )
    if (
        handoff_transitions
        and diagnostic_transitions
        and handoff_transitions != diagnostic_transitions
    ):
        raise AutonomousDirectorRuntimeError(
            "XFA handoff and diagnostic transition denominators differ"
        )
    standard_paths = int(diagnostic_counts.get("standard_path_count", 0))
    consistency_paths = int(diagnostic_counts.get("consistency_path_count", 0))
    alternative_paths = int(diagnostic_counts.get("alternative_path_count", 0))
    if alternative_paths and alternative_paths != standard_paths + consistency_paths:
        raise AutonomousDirectorRuntimeError(
            "XFA alternative-path denominator does not reconcile"
        )
    return {
        "tier_g_count": int(
            graduation_counts.get("graduated_development_book_count", 0)
        )
        + len(continuation_verified.get("tier_g_account_labels") or ()),
        "combine_to_xfa_transition_count": (
            diagnostic_transitions or handoff_transitions
        ),
        "xfa_alternative_path_count": alternative_paths,
        "xfa_standard_path_count": standard_paths,
        "xfa_consistency_path_count": consistency_paths,
        "xfa_standard_first_payout_count": int(
            diagnostic_counts.get("standard_first_payout_count", 0)
        ),
        "xfa_consistency_first_payout_count": int(
            diagnostic_counts.get("consistency_first_payout_count", 0)
        ),
        "xfa_standard_payout_cycle_count": int(
            diagnostic_counts.get("standard_payout_cycle_count", 0)
        ),
        "xfa_consistency_payout_cycle_count": int(
            diagnostic_counts.get("consistency_payout_cycle_count", 0)
        ),
        "xfa_standard_post_payout_survival_count": int(
            diagnostic_counts.get("standard_post_payout_survival_count", 0)
        ),
        "xfa_consistency_post_payout_survival_count": int(
            diagnostic_counts.get("consistency_post_payout_survival_count", 0)
        ),
        "breadth_primary_count": int(
            breadth_counts.get("primary_candidate_count", 0)
        ),
        "breadth_control_count": int(
            breadth_counts.get("control_candidate_count", 0)
        ),
        "breadth_exact_account_replay_count": int(
            breadth_counts.get("exact_account_replays", 0)
        ),
        "breadth_qualifying_cell_count": int(
            breadth_gate.get("qualifying_cell_count", 0)
        ),
        "breadth_status": str(breadth.get("status") or "NOT_RUN"),
        # XFA and breadth artifacts remain diagnostics.  Only exact passers in
        # the one-use frozen confirmation artifact can enter Tier C.
        "tier_c_count": len(tier_c_ids),
        "tier_c_candidate_ids": list(tier_c_ids),
        "fresh_confirmation_status": str(
            fresh_verified.get("status") or "NOT_RUN"
        ),
        # F0 and append-only forward are separate later gates.
        "tier_f_count": 0,
    }


def _state_payload(
    manifest: Mapping[str, Any],
    *,
    sequence: int,
    state: str,
    stage: str,
    branch_results: Mapping[str, Mapping[str, Any]],
    next_action: str,
) -> dict[str, Any]:
    exploration = branch_results.get("EXPLORATION") or {}
    exact_metrics = _exact_result_metrics(branch_results)
    pass_counts = dict(
        (branch_results.get("PASS_OBSERVED_BANK") or {}).get("counts") or {}
    )
    direct_counts = dict(
        (branch_results.get("CONSISTENCY_DIRECT") or {}).get("counts") or {}
    )
    event_safety_counts = dict(
        (branch_results.get("EVENT_TIME_SAFETY") or {}).get("counts") or {}
    )
    relay_counts = _relay_evidence_counts(branch_results)
    post_breadth_counts = _post_breadth_counts(branch_results)
    session_safe_counts = _session_safe_counts(branch_results)
    session_safe_confirmation_counts = _session_safe_confirmation_counts(
        branch_results
    )
    event_control = branch_results.get("EVENT_TIME_MATCHED_CONTROLS") or {}
    event_control_counts = (
        _event_time_control_counts(
            _verify_event_time_matched_controls_result(event_control)
        )
        if event_control
        else {
            "control_count": 0,
            "exact_episode_count": 0,
            "normal_episode_count": 0,
            "stressed_episode_count": 0,
        }
    )
    breadth_continuation = branch_results.get("FROZEN_BREADTH_CONTINUATION") or {}
    breadth_continuation_counts = (
        _frozen_breadth_counts(
            _verify_frozen_breadth_continuation_result(breadth_continuation)
        )
        if breadth_continuation
        else {
            "normal_episode_count": 0,
            "stressed_episode_count": 0,
            "exact_account_replay_count": 0,
        }
    )
    event_control_policy_replay_count = (
        1 + event_control_counts["control_count"] if event_control else 0
    )
    breadth_account_policy_replay_count = len(
        breadth_continuation.get("account_results") or ()
    )
    post_confirmation_policy_replay_count = (
        event_control_policy_replay_count + breadth_account_policy_replay_count
    )
    base_selected = max(
        int(exploration.get("selected_policy_count", 0)),
        int(exact_metrics["exact_account_replays"]),
    )
    selected = base_selected + post_confirmation_policy_replay_count
    proposed = max(
        int(exploration.get("eligible_policy_count", base_selected)), base_selected
    ) + post_confirmation_policy_replay_count
    payload = {
        "schema": PRODUCTION_STATE_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state,
        "stage": stage,
        "checkpoint_sequence": int(sequence),
        "started_at_utc": _RUN_STARTED_AT_UTC or _utc_now(),
        "updated_at_utc": _utc_now(),
        "runner_pid": os.getpid(),
        "worker_count": 2,
        "evidence_writer_count": 1,
        "active_economic_worker_processes": (
            2 if stage.endswith("_RUNNING") else 0
        ),
        "policies_proposed": max(proposed, selected),
        "unique_policies_screened": selected,
        "exact_account_replays": (
            exact_metrics["exact_account_replays"]
            + post_confirmation_policy_replay_count
        ),
        "control_policy_replay_operations": int(
            exact_metrics.get("control_policy_replay_operations", 0)
        )
        + event_control_counts["control_count"],
        "combine_episodes_completed": (
            exact_metrics["normal_account_replays"]
            + exact_metrics["stressed_account_replays"]
            + event_control_counts["exact_episode_count"]
            + breadth_continuation_counts["normal_episode_count"]
            + breadth_continuation_counts["stressed_episode_count"]
        ),
        "normal_episodes_completed": (
            exact_metrics["normal_account_replays"]
            + event_control_counts["normal_episode_count"]
            + breadth_continuation_counts["normal_episode_count"]
        ),
        "stressed_episodes_completed": (
            exact_metrics["stressed_account_replays"]
            + event_control_counts["stressed_episode_count"]
            + breadth_continuation_counts["stressed_episode_count"]
        ),
        "feasibility_screens_completed": selected,
        "source_episode_rows_reused": int(exploration.get("episode_rows_read", 0)),
        "completed_branch_count": len(branch_results),
        "branch_status": {
            lane: "COMPLETE" if lane in branch_results else "RUNNING"
            for lane in ("EXPLOITATION", "EXPLORATION")
        },
        "next_action": next_action,
        "last_completed_policy_id": str(
            (exact_metrics["best_exact_frontier_point"] or {}).get("candidate_id")
            or (exploration.get("best_deployable_frontier_point") or {}).get(
                "policy_id"
            )
            or ""
        ),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "q4_access_delta": 0,
        # Network acquisition is deliberately external to the controller and
        # reconciled by its manifest-bound receipt.  These two safety counters
        # describe purchases made by the production worker itself and must
        # therefore remain zero.
        "data_purchase_count": 0,
        "new_data_purchase_count": 0,
        "external_manifest_bound_acquisition_count": (
            1 if breadth_continuation else 0
        ),
        "proof_windows_consumed": 0,
        "combine_pass_observed_bank_count": int(
            pass_counts.get("bank_policy_count", 0)
        ),
        "combine_pass_observed_shortage": int(
            pass_counts.get("shortage_to_minimum_target", 0)
        ),
        "consistency_direct_policy_exact_replay_count": int(
            direct_counts.get("direct_policy_exact_replay_count", 0)
        ),
        "consistency_direct_identity_control_count": int(
            direct_counts.get("identity_control_exact_replay_count", 0)
        ),
        "consistency_direct_g_precontrol_ready_count": int(
            direct_counts.get("g_precontrol_ready_count", 0)
        ),
        "event_time_safety_candidate_count": int(
            event_safety_counts.get("selected_candidate_count", 0)
        ),
        "event_time_safety_profile_count": int(
            event_safety_counts.get("profile_count", 0)
        ),
        "event_time_safety_exact_episode_count": int(
            event_safety_counts.get("exact_episode_count", 0)
        ),
        "event_time_safety_g_precontrol_ready_count": int(
            event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
        ),
        "authoritative_tier_g_count": relay_counts["tier_g_count"],
        "independently_confirmed_tier_c_count": relay_counts["tier_c_count"],
        "forward_tier_f_count": relay_counts["tier_f_count"],
        "fresh_confirmation_status": relay_counts[
            "fresh_confirmation_status"
        ],
        "combine_to_xfa_transition_count": relay_counts[
            "combine_to_xfa_transition_count"
        ],
        "xfa_paths_started": relay_counts["xfa_alternative_path_count"],
        "xfa_alternative_path_count": relay_counts["xfa_alternative_path_count"],
        "xfa_standard_path_count": relay_counts["xfa_standard_path_count"],
        "xfa_consistency_path_count": relay_counts["xfa_consistency_path_count"],
        "xfa_standard_first_payout_count": relay_counts[
            "xfa_standard_first_payout_count"
        ],
        "xfa_consistency_first_payout_count": relay_counts[
            "xfa_consistency_first_payout_count"
        ],
        "cross_index_breadth_primary_count": relay_counts[
            "breadth_primary_count"
        ],
        "cross_index_breadth_control_count": relay_counts[
            "breadth_control_count"
        ],
        "cross_index_breadth_exact_account_replay_count": relay_counts[
            "breadth_exact_account_replay_count"
        ],
        "cross_index_breadth_qualifying_cell_count": relay_counts[
            "breadth_qualifying_cell_count"
        ],
        "cross_index_breadth_status": relay_counts["breadth_status"],
        "event_time_matched_control_count": event_control_counts["control_count"],
        "event_time_matched_control_exact_episode_count": event_control_counts[
            "exact_episode_count"
        ],
        "event_time_matched_controls_status": str(
            event_control.get("control_verdict") or "NOT_RUN"
        ),
        "frozen_breadth_exact_account_replay_count": breadth_continuation_counts[
            "exact_account_replay_count"
        ],
        "frozen_breadth_account_policy_replay_count": (
            breadth_account_policy_replay_count
        ),
        "frozen_breadth_normal_episode_count": breadth_continuation_counts[
            "normal_episode_count"
        ],
        "frozen_breadth_stressed_episode_count": breadth_continuation_counts[
            "stressed_episode_count"
        ],
        "frozen_breadth_tier_g_count": len(
            breadth_continuation.get("tier_g_account_labels") or ()
        ),
        "frozen_breadth_status": str(
            breadth_continuation.get("status") or "NOT_RUN"
        ),
        # These counters are deliberately separate.  They must never be added
        # retrospectively to the long-running proposal/replay/episode totals.
        **post_breadth_counts,
        **session_safe_counts,
        **session_safe_confirmation_counts,
    }
    return _rehash(payload, "state_hash")


def _kpis(
    manifest: Mapping[str, Any],
    state: Mapping[str, Any],
    branch_results: Mapping[str, Mapping[str, Any]],
    started: float,
) -> dict[str, Any]:
    exploration = branch_results.get("EXPLORATION") or {}
    exact_metrics = _exact_result_metrics(branch_results)
    candidate_counts = dict(
        (branch_results.get("CANDIDATE_BANK") or {}).get("counts") or {}
    )
    book_counts = dict(
        (branch_results.get("MARGINAL_BOOKS") or {}).get("counts") or {}
    )
    pass_counts = dict(
        (branch_results.get("PASS_OBSERVED_BANK") or {}).get("counts") or {}
    )
    direct_counts = dict(
        (branch_results.get("CONSISTENCY_DIRECT") or {}).get("counts") or {}
    )
    event_safety_counts = dict(
        (branch_results.get("EVENT_TIME_SAFETY") or {}).get("counts") or {}
    )
    relay_counts = _relay_evidence_counts(branch_results)
    post_breadth_counts = _post_breadth_counts(branch_results)
    session_safe_counts = _session_safe_counts(branch_results)
    session_safe_confirmation_counts = _session_safe_confirmation_counts(
        branch_results
    )
    frontier = list(exploration.get("uniform_legal_frontier") or ())
    stressed_points = [row for row in frontier if row.get("scenario") == "STRESSED_1_5X"]
    normal_points = [row for row in frontier if row.get("scenario") == "NORMAL"]
    elapsed_seconds = max(time.monotonic() - started, 1e-9)
    elapsed_hours = elapsed_seconds / 3600.0
    proposed = int(state["policies_proposed"])
    screened = int(state["unique_policies_screened"])
    exact = int(state["exact_account_replays"])
    episodes = int(state["combine_episodes_completed"])
    payload = {
        "schema": PRODUCTION_KPI_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state["state"],
        "checkpoint_sequence": state["checkpoint_sequence"],
        "policies_proposed": proposed,
        "unique_policies_screened": screened,
        "exact_account_replays": exact,
        "control_policy_replay_operations": int(
            state.get("control_policy_replay_operations", 0)
        ),
        "combine_episodes_completed": episodes,
        "normal_episodes_completed": int(state["normal_episodes_completed"]),
        "stressed_episodes_completed": int(state["stressed_episodes_completed"]),
        "positive_stressed_net_candidates": max(
            int(exploration.get("positive_stressed_policy_count", 0)),
            exact_metrics["positive_stressed_candidate_count"],
        ),
        "candidates_with_normal_pass": exact_metrics[
            "normal_pass_candidate_count"
        ],
        "candidates_with_stressed_pass": exact_metrics[
            "stressed_pass_candidate_count"
        ],
        "summary_threshold_hit_candidates_normal": len(
            {row.get("policy_id") for row in normal_points if int(row.get("passes", 0)) > 0}
        ),
        "summary_threshold_hit_candidates_stressed": len(
            {row.get("policy_id") for row in stressed_points if int(row.get("passes", 0)) > 0}
        ),
        "near_pass_count": sum(
            float(row.get("median_target_progress", 0.0)) >= 0.60
            for row in stressed_points
        ),
        "candidates_promoted_96": 0,
        "confirmation_ready_candidates": relay_counts["tier_c_count"],
        "independently_confirmed_tier_c_count": relay_counts["tier_c_count"],
        "forward_tier_f_count": relay_counts["tier_f_count"],
        "fresh_confirmation_status": relay_counts[
            "fresh_confirmation_status"
        ],
        "tier_q_candidate_count": int(
            candidate_counts.get("tier_q_contract_cleared_count", 0)
        ),
        "g_precontrol_ready_count": int(book_counts.get("g_ready_count", 0))
        + int(book_counts.get("standalone_g_ready_count", 0))
        + int(direct_counts.get("g_precontrol_ready_count", 0))
        + int(
            event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
        ),
        "combine_pass_observed_bank_count": int(
            pass_counts.get("bank_policy_count", 0)
        ),
        "combine_pass_observed_shortage": int(
            pass_counts.get("shortage_to_minimum_target", 0)
        ),
        "marginal_book_exact_replay_count": int(
            book_counts.get("primary_book_exact_replay_count", 0)
        ),
        "marginally_accepted_book_count": int(
            book_counts.get("marginally_accepted_count", 0)
        ),
        "consistency_direct_policy_exact_replay_count": int(
            direct_counts.get("direct_policy_exact_replay_count", 0)
        ),
        "consistency_direct_identity_control_count": int(
            direct_counts.get("identity_control_exact_replay_count", 0)
        ),
        "consistency_direct_g_precontrol_ready_count": int(
            direct_counts.get("g_precontrol_ready_count", 0)
        ),
        "event_time_safety_candidate_count": int(
            event_safety_counts.get("selected_candidate_count", 0)
        ),
        "event_time_safety_profile_count": int(
            event_safety_counts.get("profile_count", 0)
        ),
        "event_time_safety_exact_episode_count": int(
            event_safety_counts.get("exact_episode_count", 0)
        ),
        "event_time_safety_g_precontrol_ready_count": int(
            event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
        ),
        "authoritative_tier_g_count": relay_counts["tier_g_count"],
        "combine_to_xfa_transition_count": relay_counts[
            "combine_to_xfa_transition_count"
        ],
        "xfa_paths_started": relay_counts["xfa_alternative_path_count"],
        "xfa_alternative_path_count": relay_counts["xfa_alternative_path_count"],
        "xfa_standard_path_count": relay_counts["xfa_standard_path_count"],
        "xfa_consistency_path_count": relay_counts["xfa_consistency_path_count"],
        "xfa_standard_first_payout_count": relay_counts[
            "xfa_standard_first_payout_count"
        ],
        "xfa_consistency_first_payout_count": relay_counts[
            "xfa_consistency_first_payout_count"
        ],
        "xfa_standard_payout_cycle_count": relay_counts[
            "xfa_standard_payout_cycle_count"
        ],
        "xfa_consistency_payout_cycle_count": relay_counts[
            "xfa_consistency_payout_cycle_count"
        ],
        "xfa_standard_post_payout_survival_count": relay_counts[
            "xfa_standard_post_payout_survival_count"
        ],
        "xfa_consistency_post_payout_survival_count": relay_counts[
            "xfa_consistency_post_payout_survival_count"
        ],
        "cross_index_breadth_primary_count": relay_counts[
            "breadth_primary_count"
        ],
        "cross_index_breadth_control_count": relay_counts[
            "breadth_control_count"
        ],
        "cross_index_breadth_exact_account_replay_count": relay_counts[
            "breadth_exact_account_replay_count"
        ],
        "cross_index_breadth_qualifying_cell_count": relay_counts[
            "breadth_qualifying_cell_count"
        ],
        "cross_index_breadth_status": relay_counts["breadth_status"],
        "event_time_matched_control_count": int(
            state.get("event_time_matched_control_count", 0)
        ),
        "event_time_matched_control_exact_episode_count": int(
            state.get("event_time_matched_control_exact_episode_count", 0)
        ),
        "event_time_matched_controls_status": str(
            state.get("event_time_matched_controls_status", "NOT_RUN")
        ),
        "frozen_breadth_exact_account_replay_count": int(
            state.get("frozen_breadth_exact_account_replay_count", 0)
        ),
        "frozen_breadth_account_policy_replay_count": int(
            state.get("frozen_breadth_account_policy_replay_count", 0)
        ),
        "frozen_breadth_status": str(
            state.get("frozen_breadth_status", "NOT_RUN")
        ),
        **post_breadth_counts,
        **session_safe_counts,
        **session_safe_confirmation_counts,
        "best_normal_pass_rate": exact_metrics["best_normal_pass_rate"],
        "best_stressed_pass_rate": exact_metrics["best_stressed_pass_rate"],
        "best_normal_summary_threshold_rate": max(
            (float(row.get("pass_rate", 0.0)) for row in normal_points), default=0.0
        ),
        "best_stressed_summary_threshold_rate": max(
            (float(row.get("pass_rate", 0.0)) for row in stressed_points), default=0.0
        ),
        "median_normal_pass_rate": exact_metrics["median_normal_pass_rate"],
        "median_stressed_pass_rate": exact_metrics[
            "median_stressed_pass_rate"
        ],
        "median_normal_summary_threshold_rate": statistics.median(
            [float(row.get("pass_rate", 0.0)) for row in normal_points]
        )
        if normal_points
        else 0.0,
        "median_stressed_summary_threshold_rate": statistics.median(
            [float(row.get("pass_rate", 0.0)) for row in stressed_points]
        )
        if stressed_points
        else 0.0,
        "duplicate_rejection_rate": 0.0,
        "cache_hit_rate": 1.0 if branch_results else 0.0,
        "economic_research_wall_clock_fraction": _economic_wall_fraction(
            elapsed_seconds
        ),
        "cpu_utilization_fraction": _measured_cpu_fraction(elapsed_seconds),
        "rates_per_hour": {
            "policies_proposed": proposed / elapsed_hours,
            "unique_policies_screened": screened / elapsed_hours,
            "exact_account_replays": exact / elapsed_hours,
            "combine_episodes": episodes / elapsed_hours,
        },
        "workers": {"compute": 2, "evidence_writer": 1},
        "admin_overhead_alert": False,
        "matched_controls_status": "NON_DEPLOYABLE_UPPER_BOUND_SEPARATE_NON_PROMOTABLE",
        "null_status": "LEGAL_FEASIBILITY_DIAGNOSTIC_ONLY",
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": int(state.get("data_purchase_count", 0)),
    }
    return _rehash(payload, "kpi_hash")


def _write_mission_views(
    root: Path,
    manifest: Mapping[str, Any],
    state: Mapping[str, Any],
    branch_results: Mapping[str, Mapping[str, Any]],
) -> None:
    writer = AtomicResultWriter(root / "mission/state", immutable=False)
    branch_state = {
        "schema": BRANCH_STATE_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "updated_at_utc": _utc_now(),
        "lanes": {
            lane: {
                "status": "COMPLETE" if lane in branch_results else "RUNNING",
                "decision": (branch_results.get(lane) or {}).get("decision"),
                "result_hash": (branch_results.get(lane) or {}).get("result_hash"),
            }
            for lane in ("EXPLOITATION", "EXPLORATION")
        },
        "next_branch_cards": state.get("next_branch_cards") or [],
        "economic_relays": {
            key: {
                "status": (branch_results.get(key) or {}).get("status"),
                "result_hash": (branch_results.get(key) or {}).get("result_hash"),
                "evidence_role": (branch_results.get(key) or {}).get(
                    "evidence_role"
                ),
            }
            for key in (
                "TIER_G_GRADUATION",
                "TIER_G_XFA_HANDOFF",
                "TIER_G_XFA_DIAGNOSTIC",
                "CROSS_INDEX_BREADTH",
                "FRESH_CONFIRMATION",
                "EVENT_TIME_MATCHED_CONTROLS",
                "FROZEN_BREADTH_CONTINUATION",
                "POST_BREADTH_LEGAL_EXACT",
                "POST_BREADTH_TREASURY_CURVE",
                "POST_BREADTH_SESSION_SAFE_HORIZON_CUTOFF",
                "POST_BREADTH_SESSION_SAFE_DROP_COMPONENT",
                "POST_BREADTH_SESSION_SAFE_PORTFOLIO",
            )
            if key in branch_results
        },
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "data_purchase_count": int(state.get("data_purchase_count", 0)),
    }
    writer.write_json(
        "AUTONOMOUS_BRANCH_STATE.json", _rehash(branch_state, "state_hash")
    )
    exploration = branch_results.get("EXPLORATION") or {}
    exact_metrics = _exact_result_metrics(branch_results)
    exact_best = exact_metrics["best_exact_frontier_point"]
    candidate_bank = branch_results.get("CANDIDATE_BANK") or {}
    marginal_books = branch_results.get("MARGINAL_BOOKS") or {}
    pass_bank = branch_results.get("PASS_OBSERVED_BANK") or {}
    consistency_direct = branch_results.get("CONSISTENCY_DIRECT") or {}
    event_time_safety = branch_results.get("EVENT_TIME_SAFETY") or {}
    candidate_counts = dict(candidate_bank.get("counts") or {})
    book_counts = dict(marginal_books.get("counts") or {})
    pass_counts = dict(pass_bank.get("counts") or {})
    direct_counts = dict(consistency_direct.get("counts") or {})
    event_safety_counts = dict(event_time_safety.get("counts") or {})
    tier_g_graduation = branch_results.get("TIER_G_GRADUATION") or {}
    fresh_confirmation = branch_results.get("FRESH_CONFIRMATION") or {}
    relay_counts = _relay_evidence_counts(branch_results)
    post_breadth_counts = _post_breadth_counts(branch_results)
    session_safe_counts = _session_safe_counts(branch_results)
    tier_q_count = int(candidate_counts.get("tier_q_contract_cleared_count", 0))
    g_precontrol_count = int(book_counts.get("g_ready_count", 0)) + int(
        book_counts.get("standalone_g_ready_count", 0)
    ) + int(direct_counts.get("g_precontrol_ready_count", 0)) + int(
        event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
    )
    strongest_q = next(
        (
            row
            for row in candidate_bank.get("candidates") or ()
            if row.get("tier_q_contract_cleared") is True
        ),
        None,
    )
    graduated_books = list(tier_g_graduation.get("graduated_development_books") or ())
    strongest_g = dict(graduated_books[0]) if graduated_books else None
    tier_c_ids = set(str(value) for value in relay_counts["tier_c_candidate_ids"])
    strongest_c = next(
        (
            dict(row)
            for row in fresh_confirmation.get("candidate_results") or ()
            if str(row.get("candidate_id") or "") in tier_c_ids
        ),
        None,
    )
    scorecard = {
        "schema": ECONOMIC_SCORECARD_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "updated_at_utc": _utc_now(),
        "strongest_surviving_candidate": (
            strongest_c or strongest_g or strongest_q or exact_best
        ),
        "strongest_diagnostic_shortlist_point": exploration.get(
            "best_deployable_frontier_point"
        ),
        "evidence_tier": (
            "C_INDEPENDENTLY_CONFIRMED"
            if strongest_c
            else "G_DEVELOPMENT_ONLY"
            if strongest_g
            else "Q"
            if strongest_q
            else "E"
            if exact_best
            else None
        ),
        "candidate_bank_counts": {
            "H": 0,
            "E": 47 + exact_metrics["selected_candidates"],
            "Q": tier_q_count,
            "G": relay_counts["tier_g_count"],
            "C": relay_counts["tier_c_count"],
            "F": relay_counts["tier_f_count"],
        },
        "g_precontrol_ready_count": g_precontrol_count,
        "combine_pass_observed_bank_count": int(
            pass_counts.get("bank_policy_count", 0)
        ),
        "combine_pass_observed_shortage": int(
            pass_counts.get("shortage_to_minimum_target", 0)
        ),
        "marginal_book_policy_count": int(
            book_counts.get("primary_book_exact_replay_count", 0)
        ),
        "consistency_direct_policy_count": int(
            direct_counts.get("direct_policy_exact_replay_count", 0)
        ),
        "consistency_direct_g_precontrol_ready_count": int(
            direct_counts.get("g_precontrol_ready_count", 0)
        ),
        "event_time_safety_candidate_count": int(
            event_safety_counts.get("selected_candidate_count", 0)
        ),
        "event_time_safety_profile_count": int(
            event_safety_counts.get("profile_count", 0)
        ),
        "event_time_safety_g_precontrol_ready_count": int(
            event_safety_counts.get("heldout_safety_precontrol_ready_count", 0)
        ),
        "combine_to_xfa_transition_count": relay_counts[
            "combine_to_xfa_transition_count"
        ],
        "xfa_alternative_path_count": relay_counts["xfa_alternative_path_count"],
        "xfa_standard_path_count": relay_counts["xfa_standard_path_count"],
        "xfa_consistency_path_count": relay_counts["xfa_consistency_path_count"],
        "xfa_standard_first_payout_count": relay_counts[
            "xfa_standard_first_payout_count"
        ],
        "xfa_consistency_first_payout_count": relay_counts[
            "xfa_consistency_first_payout_count"
        ],
        "xfa_standard_payout_cycle_count": relay_counts[
            "xfa_standard_payout_cycle_count"
        ],
        "xfa_consistency_payout_cycle_count": relay_counts[
            "xfa_consistency_payout_cycle_count"
        ],
        "xfa_standard_post_payout_survival_count": relay_counts[
            "xfa_standard_post_payout_survival_count"
        ],
        "xfa_consistency_post_payout_survival_count": relay_counts[
            "xfa_consistency_post_payout_survival_count"
        ],
        "cross_index_breadth_status": relay_counts["breadth_status"],
        "cross_index_breadth_primary_count": relay_counts[
            "breadth_primary_count"
        ],
        "cross_index_breadth_control_count": relay_counts[
            "breadth_control_count"
        ],
        "cross_index_breadth_exact_account_replay_count": relay_counts[
            "breadth_exact_account_replay_count"
        ],
        "cross_index_breadth_qualifying_cell_count": relay_counts[
            "breadth_qualifying_cell_count"
        ],
        "fresh_confirmation_status": relay_counts[
            "fresh_confirmation_status"
        ],
        **post_breadth_counts,
        **session_safe_counts,
        "tier_c_candidate_ids": relay_counts["tier_c_candidate_ids"],
        "branch_decisions": {
            lane: (branch_results.get(lane) or {}).get("decision")
            for lane in ("EXPLOITATION", "EXPLORATION")
        },
        "promotion_status": (
            "TIER_C_INDEPENDENTLY_CONFIRMED" if strongest_c else None
        ),
    }
    writer.write_json(
        "ECONOMIC_SCORECARD.json", _rehash(scorecard, "scorecard_hash")
    )
    current_path = root / "mission/state/CURRENT_STATE.json"
    if current_path.is_file():
        current = json.loads(current_path.read_text(encoding="utf-8"))
        if not isinstance(current, dict):
            raise AutonomousDirectorRuntimeError("CURRENT_STATE must remain an object")
    else:
        current = {}
    current.update(
        {
            "schema": "hydra_current_state_v1",
            "updated_at_utc": _utc_now(),
            "active_campaign_id": manifest["campaign_id"],
            "production_state": state["state"],
            "production_stage": state["stage"],
            "checkpoint_sequence": state["checkpoint_sequence"],
            "two_lane_director_active": True,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "authoritative_tier_g_count": relay_counts["tier_g_count"],
            "independently_confirmed_tier_c_count": relay_counts["tier_c_count"],
            "forward_tier_f_count": relay_counts["tier_f_count"],
            "combine_to_xfa_transition_count": relay_counts[
                "combine_to_xfa_transition_count"
            ],
            "xfa_alternative_path_count": relay_counts[
                "xfa_alternative_path_count"
            ],
            "cross_index_breadth_status": relay_counts["breadth_status"],
            "fresh_confirmation_status": relay_counts[
                "fresh_confirmation_status"
            ],
            "event_time_matched_controls_status": str(
                state.get("event_time_matched_controls_status", "NOT_RUN")
            ),
            "frozen_breadth_status": str(
                state.get("frozen_breadth_status", "NOT_RUN")
            ),
            **post_breadth_counts,
            **session_safe_counts,
            "data_purchase_count": int(state.get("data_purchase_count", 0)),
            "tier_c_candidate_ids": relay_counts["tier_c_candidate_ids"],
        }
    )
    writer.write_json("CURRENT_STATE.json", _rehash(current, "state_hash"))


def _append_decision_once(
    root: Path, manifest: Mapping[str, Any], result: Mapping[str, Any]
) -> None:
    path = root / "mission/state/decision_ledger.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    decision_id = stable_hash(
        {
            "manifest_hash": manifest["manifest_hash"],
            "lane_id": result["lane_id"],
            "result_hash": result["result_hash"],
        }
    )
    if path.is_file():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if decision_id in line:
                    return
    row = {
        "schema": "hydra_autonomous_decision_ledger_event_v1",
        "decision_id": decision_id,
        "recorded_at_utc": _utc_now(),
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "lane_id": result["lane_id"],
        "branch_id": result["branch_id"],
        "decision": result["decision"],
        "evidence_tier": result.get("evidence_tier"),
        "promotion_status": result.get("promotion_status"),
        "next_materially_distinct_action": result.get(
            "next_materially_distinct_action"
        ),
        "result_hash": result["result_hash"],
    }
    row["event_hash"] = stable_hash(row)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _next_branch_cards(
    branch_results: Mapping[str, Mapping[str, Any]]
) -> list[dict[str, Any]]:
    exploitation = branch_results["EXPLOITATION"]
    exploration = branch_results["EXPLORATION"]
    return [
        {
            "lane_id": "EXPLOITATION",
            "branch_id": "FROZEN_NQ_STRUCTURAL_BASELINE_FRESH_CONFIRMATION",
            "status": "QUEUED",
            "hypothesis": "The causal NQ structural baseline survives one genuinely fresh confirmation without the failed veto.",
            "materially_distinct_from_prior_attempt": True,
            "fresh_confirmation_attempt_maximum": 1,
            "retuning_allowed": False,
            "source_decision": exploitation["decision"],
        },
        {
            "lane_id": "EXPLORATION",
            "branch_id": "EVENT_TIME_CROSS_ASSET_FEASIBILITY",
            "status": "QUEUED",
            "hypothesis": "An event-time cross-asset representation can add target velocity absent from bar-time ledgers.",
            "materially_distinct_from_prior_attempt": True,
            "data_purchase_required": False,
            "source_decision": exploration["decision"],
        },
    ]


def _publish(
    writer: AtomicResultWriter,
    state: Mapping[str, Any],
    kpis: Mapping[str, Any],
) -> None:
    writer.write_json("production_state.json", dict(state))
    writer.write_json("production_kpis.json", dict(kpis))


def _load_prior_state(
    output: Path, manifest: Mapping[str, Any]
) -> dict[str, Any] | None:
    path = output / "production_state.json"
    if not path.is_file():
        return None
    value = _read_hashed(path, "state_hash")
    if (
        value.get("campaign_id") != manifest["campaign_id"]
        or not _artifact_manifest_compatible(value, manifest)
    ):
        raise AutonomousDirectorRuntimeError("resumable state identity drift")
    return value


def _artifact_manifest_compatible(
    value: Mapping[str, Any], manifest: Mapping[str, Any]
) -> bool:
    allowed = {
        str(manifest.get("manifest_hash") or ""),
        *(
            str(item)
            for item in manifest.get("compatible_artifact_manifest_hashes") or ()
        ),
    }
    return str(value.get("manifest_hash") or "") in allowed


def _fresh_confirmation_manifest_paths(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, Path], tuple[str, ...]]:
    """Resolve the sealed confirmation paths without requiring them to exist."""

    section = dict(
        manifest.get("fresh_confirmation")
        or manifest.get("fresh_confirmation_contract")
        or {}
    )
    required = (
        "contract_path",
        "acquisition_receipt_path",
        "feature_receipt_path",
        "result_path",
    )
    paths: dict[str, Path] = {}
    missing: list[str] = []
    project = root.resolve()
    for key in required:
        raw = str(section.get(key) or "").strip()
        if not raw:
            missing.append(key)
            continue
        candidate = Path(raw)
        resolved = (
            candidate.resolve()
            if candidate.is_absolute()
            else (project / candidate).resolve()
        )
        try:
            resolved.relative_to(project)
        except ValueError as exc:
            raise AutonomousDirectorRuntimeError(
                f"fresh-confirmation path escapes repository: {key}"
            ) from exc
        paths[key] = resolved
    return paths, tuple(sorted(missing))


def _frozen_breadth_manifest_paths(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[dict[str, Path], tuple[str, ...]]:
    """Resolve the single post-confirmation breadth branch declarations."""

    section = dict(manifest.get("post_confirmation_branch_portfolio") or {})
    required = (
        "breadth_contract_path",
        "breadth_acquisition_receipt_path",
        "breadth_feature_receipt_path",
        "breadth_result_path",
    )
    output_keys = {
        "breadth_contract_path": "contract_path",
        "breadth_acquisition_receipt_path": "acquisition_receipt_path",
        "breadth_feature_receipt_path": "feature_receipt_path",
        "breadth_result_path": "result_path",
    }
    paths: dict[str, Path] = {}
    missing: list[str] = []
    project = root.resolve()
    for manifest_key in required:
        raw = str(section.get(manifest_key) or "").strip()
        output_key = output_keys[manifest_key]
        if not raw:
            missing.append(output_key)
            continue
        candidate = Path(raw)
        resolved = (
            candidate.resolve()
            if candidate.is_absolute()
            else (project / candidate).resolve()
        )
        try:
            resolved.relative_to(project)
        except ValueError as exc:
            raise AutonomousDirectorRuntimeError(
                f"frozen-breadth path escapes repository: {manifest_key}"
            ) from exc
        paths[output_key] = resolved
    return paths, tuple(sorted(missing))


def _post_breadth_manifest_paths(
    root: Path, output: Path, manifest: Mapping[str, Any]
) -> dict[str, Path]:
    """Resolve optional declarations with manifest-output-bound defaults."""

    section = dict(manifest.get("post_breadth_branch_portfolio") or {})
    defaults = {
        "legal_result_path": output
        / "branch_results"
        / _POST_BREADTH_RELATIVE_ROOT
        / "frozen_legal_frontier_exact.json",
        "treasury_result_path": output
        / "branch_results"
        / _POST_BREADTH_RELATIVE_ROOT
        / "treasury_curve_tripwire.json",
        "treasury_acquisition_receipt_path": root
        / "reports/data_access/treasury_curve_tripwire_acquisition_receipt.json",
        "treasury_input_contract_path": root
        / "reports/data_access/treasury_curve_tripwire_input_contract.json",
    }
    resolved: dict[str, Path] = {}
    project = root.resolve()
    branch_root = (output / "branch_results").resolve()
    for key, default in defaults.items():
        raw = str(section.get(key) or "").strip()
        candidate = Path(raw) if raw else default
        path = (
            candidate.resolve()
            if candidate.is_absolute()
            else (project / candidate).resolve()
        )
        try:
            path.relative_to(project)
        except ValueError as exc:
            raise AutonomousDirectorRuntimeError(
                f"post-breadth manifest path escapes repository: {key}"
            ) from exc
        if key.endswith("result_path"):
            try:
                path.relative_to(branch_root)
            except ValueError as exc:
                raise AutonomousDirectorRuntimeError(
                    f"post-breadth result must remain under branch_results: {key}"
                ) from exc
        resolved[key] = path
    return resolved


def _treasury_input_if_ready(
    root: Path, paths: Mapping[str, Path],
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    """Verify the acquisition/input binding or return the exact missing set."""

    receipt_path = paths["treasury_acquisition_receipt_path"]
    contract_path = paths["treasury_input_contract_path"]
    missing = tuple(
        key
        for key, path in (
            ("treasury_acquisition_receipt", receipt_path),
            ("treasury_input_contract", contract_path),
        )
        if not path.is_file()
    )
    if missing:
        return None, missing

    receipt = _read_json_object(receipt_path)
    receipt_core = dict(receipt)
    claimed = str(receipt_core.pop("receipt_hash", ""))
    request = dict(receipt.get("request") or {})
    expected_request = {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": [
            "ZT.c.0",
            "ZF.c.0",
            "ZN.c.0",
            "TN.c.0",
            "ZB.c.0",
            "UB.c.0",
        ],
        "stype_in": "continuous",
        "start": "2023-01-03",
        "end": "2024-10-01",
    }
    expected_cost = float(TREASURY_CURVE_COST_RECEIPT["estimated_cost_usd"])
    if (
        not claimed
        or stable_hash(receipt_core) != claimed
        or receipt.get("schema") != _TREASURY_ACQUISITION_SCHEMA
        or request != expected_request
        or not math.isclose(
            float(receipt.get("official_live_cost_usd", -1.0)),
            expected_cost,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        or int(receipt.get("official_record_count", -1))
        != int(TREASURY_CURVE_COST_RECEIPT["estimated_records"])
        or int(receipt.get("official_billable_bytes", -1))
        != int(TREASURY_CURVE_COST_RECEIPT["estimated_bytes"])
        or receipt.get("raw_immutable") is not True
        or receipt.get("runtime_or_manifest_modified") is not False
        or int(receipt.get("q4_access_count_delta", -1)) != 0
        or int(receipt.get("protected_data_access_count_delta", -1)) != 0
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
    ):
        raise AutonomousDirectorRuntimeError(
            "Treasury acquisition receipt identity or safety drift"
        )
    files = [dict(item) for item in receipt.get("files") or ()]
    if len(files) != 1 or files[0].get("kind") != "RAW_DBN_OHLCV":
        raise AutonomousDirectorRuntimeError(
            "Treasury acquisition raw-file inventory drift"
        )
    raw_path = Path(str(files[0].get("path") or "")).resolve()
    _assert_within(root.resolve(), raw_path)
    if (
        not raw_path.is_file()
        or _file_sha256(raw_path) != str(files[0].get("sha256") or "")
    ):
        raise AutonomousDirectorRuntimeError(
            "Treasury acquisition raw artifact drift"
        )

    contract = _read_json_object(contract_path)
    if (
        contract.get("schema") != TREASURY_CURVE_INPUT_SCHEMA
        or contract.get("q4_excluded") is not True
        or contract.get("source_acquisition_receipt_hash") != claimed
    ):
        raise AutonomousDirectorRuntimeError(
            "Treasury input contract is not bound to the acquisition receipt"
        )
    return contract, ()


def _relative_branch_artifact_path(
    output: Path, artifact: Path, label: str
) -> Path:
    branch_root = (output / "branch_results").resolve()
    try:
        return artifact.resolve().relative_to(branch_root)
    except ValueError as exc:
        raise AutonomousDirectorRuntimeError(
            f"{label} must remain under branch_results"
        ) from exc


def _read_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AutonomousDirectorRuntimeError(f"JSON object expected: {path}")
    return value


def _read_hashed(path: Path, hash_field: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    claimed = str(value.get(hash_field) or "")
    payload = dict(value)
    payload.pop(hash_field, None)
    if not claimed or stable_hash(payload) != claimed:
        raise AutonomousDirectorRuntimeError(f"snapshot hash drift: {path}")
    return value


def _with_hash(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    payload = dict(value)
    payload.pop(field, None)
    payload[field] = stable_hash(payload)
    return payload


def _rehash(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    return _with_hash(value, field)


def _iter_gzip_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            value = json.loads(line)
            if isinstance(value, dict):
                yield value


def _horizon(row: Mapping[str, Any]) -> int | None:
    raw = row.get("horizon_trading_days") or row.get("requested_duration_trading_days")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    text = str(row.get("horizon") or "")
    for value in _HORIZONS:
        if text.startswith(str(value)):
            return value
    return None


def _read_only_resumption_audit(root: str) -> dict[str, Any]:
    path = Path(root)
    return {
        "repository_exists": path.is_dir(),
        "production_cache_exists": (path / "data/cache/economic_production").is_dir(),
    }


def _set_single_thread_libraries() -> None:
    for name in _THREAD_ENV:
        os.environ[name] = "1"


def _initialize_runtime_measurement(
    prior: Mapping[str, Any] | None, started: float
) -> None:
    global _CPU_STARTED_SECONDS, _RUN_STARTED_AT_UTC, _RUN_WALL_STARTED
    global _ECONOMIC_WALL_ACCUMULATED, _ECONOMIC_ACTIVE_SINCE
    _RUN_WALL_STARTED = started
    _RUN_STARTED_AT_UTC = (
        str(prior.get("started_at_utc"))
        if prior and prior.get("started_at_utc")
        else _utc_now()
    )
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    _CPU_STARTED_SECONDS = (
        usage.ru_utime
        + usage.ru_stime
        + children.ru_utime
        + children.ru_stime
    )
    _ECONOMIC_WALL_ACCUMULATED = 0.0
    _ECONOMIC_ACTIVE_SINCE = None


def _begin_economic_phase() -> None:
    global _ECONOMIC_ACTIVE_SINCE
    if _ECONOMIC_ACTIVE_SINCE is None:
        _ECONOMIC_ACTIVE_SINCE = time.monotonic()


def _end_economic_phase() -> None:
    global _ECONOMIC_ACTIVE_SINCE, _ECONOMIC_WALL_ACCUMULATED
    if _ECONOMIC_ACTIVE_SINCE is not None:
        _ECONOMIC_WALL_ACCUMULATED += max(
            time.monotonic() - _ECONOMIC_ACTIVE_SINCE, 0.0
        )
        _ECONOMIC_ACTIVE_SINCE = None


def _economic_wall_fraction(elapsed_seconds: float) -> float:
    active = _ECONOMIC_WALL_ACCUMULATED
    if _ECONOMIC_ACTIVE_SINCE is not None:
        active += max(time.monotonic() - _ECONOMIC_ACTIVE_SINCE, 0.0)
    return min(max(active / max(elapsed_seconds, 1e-9), 0.0), 1.0)


def _measured_cpu_fraction(elapsed_seconds: float) -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    current = (
        usage.ru_utime
        + usage.ru_stime
        + children.ru_utime
        + children.ru_stime
    )
    baseline = _CPU_STARTED_SECONDS if _CPU_STARTED_SECONDS is not None else current
    consumed = max(current - baseline, 0.0)
    return min(max(consumed / max(elapsed_seconds * 3.0, 1e-9), 0.0), 1.0)


def _file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_within(root: Path, target: Path) -> None:
    if target == root or root not in target.parents:
        raise AutonomousDirectorRuntimeError("runtime input escapes project root")


def _finite(value: Any, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _digits(value: Any) -> int:
    text = "".join(character for character in str(value) if character.isdigit())
    return int(text) * (1_000 if text and int(text) < 1_000 else 1) if text else 0


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "AutonomousDirectorRuntimeError",
    "read_autonomous_director_status",
    "run_autonomous_director_manifest",
]
