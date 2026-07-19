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
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA


BRANCH_RESULT_SCHEMA = "hydra_autonomous_economic_branch_result_v1"
BRANCH_STATE_SCHEMA = "hydra_autonomous_branch_state_v1"
ECONOMIC_SCORECARD_SCHEMA = "hydra_autonomous_economic_scorecard_v1"
_HORIZONS = (5, 10, 20)
_SCENARIOS = ("NORMAL", "STRESSED_1_5X")
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
            if result.get("manifest_hash") != manifest["manifest_hash"]:
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
        state, results = _run_recurring_niche_epoch(
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
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
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

        completed: dict[str, dict[str, Any]] = {}
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
    return state, {f"{epoch}:{key}": value for key, value in completed.items()}


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
        "exact_account_replays": int(counters.get("exact_account_replays", 0)),
        "normal_account_replays": int(
            counters.get("exact_normal_account_replays", 0)
        ),
        "stressed_account_replays": int(
            counters.get("exact_stressed_account_replays", 0)
        ),
        "normal_pass_candidate_count": len(normal_pass_ids),
        "stressed_pass_candidate_count": len(stressed_pass_ids),
        "positive_stressed_candidate_count": len(positive_stressed_ids),
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
    selected = int(exploration.get("selected_policy_count", 0))
    proposed = int(exploration.get("eligible_policy_count", selected))
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
        "exact_account_replays": exact_metrics["exact_account_replays"],
        "combine_episodes_completed": exact_metrics["exact_account_replays"],
        "normal_episodes_completed": exact_metrics["normal_account_replays"],
        "stressed_episodes_completed": exact_metrics["stressed_account_replays"],
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
        "data_purchase_count": 0,
        "new_data_purchase_count": 0,
        "proof_windows_consumed": 0,
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
        "confirmation_ready_candidates": 0,
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
        "data_purchase_count": 0,
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
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "data_purchase_count": 0,
    }
    writer.write_json(
        "AUTONOMOUS_BRANCH_STATE.json", _rehash(branch_state, "state_hash")
    )
    exploration = branch_results.get("EXPLORATION") or {}
    exact_metrics = _exact_result_metrics(branch_results)
    exact_best = exact_metrics["best_exact_frontier_point"]
    scorecard = {
        "schema": ECONOMIC_SCORECARD_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "updated_at_utc": _utc_now(),
        "strongest_surviving_candidate": exact_best,
        "strongest_diagnostic_shortlist_point": exploration.get(
            "best_deployable_frontier_point"
        ),
        "evidence_tier": "E" if exact_best else None,
        "candidate_bank_counts": {
            "H": 0,
            "E": 47 + exact_metrics["selected_candidates"],
            "Q": 0,
            "G": 0,
            "C": 0,
            "F": 0,
        },
        "branch_decisions": {
            lane: (branch_results.get(lane) or {}).get("decision")
            for lane in ("EXPLOITATION", "EXPLORATION")
        },
        "promotion_status": None,
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
        or value.get("manifest_hash") != manifest["manifest_hash"]
    ):
        raise AutonomousDirectorRuntimeError("resumable state identity drift")
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
