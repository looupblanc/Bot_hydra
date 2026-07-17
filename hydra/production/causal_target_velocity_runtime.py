"""Manifest-driven runtime for causal target-velocity campaign 0028.

This module is deliberately a campaign runner, not another controller.  The
existing V17 manifest runtime owns the process and the multiplicity
reservation.  This worker owns only immutable campaign payload parts plus the
two mutable, hash-chained production snapshots consumed by that controller.

The hot path uses the shared causal decision/next-tradable-open implementation
from :mod:`hydra.research.causal_target_velocity`.  Outcome labels are opened
only after the controller-created multiplicity reservation has been verified
by the bounded risk preflight.
"""

from __future__ import annotations

import hashlib
import gzip
import itertools
import json
import math
import multiprocessing
import os
import sqlite3
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    finalize_causal_target_velocity_evidence_bundle,
    reconstruct_exact_hazard_replay,
)
from hydra.evidence import verify_evidence_bundle
from hydra.features.feature_matrix import FeatureMatrix
from hydra.production.causal_risk_preflight import run_causal_risk_preflight
from hydra.production.causal_target_velocity_manifest import (
    CAUSAL_TARGET_VELOCITY_ENGINE,
    load_and_validate_causal_target_velocity_manifest,
)
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA
from hydra.production.halving import build_final_result_payload
from hydra.account_policy.active_pool_replay import AccountPolicyEpisode
from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.propfirm.combine_episode import CombineEpisodeResult
from hydra.research.causal_target_velocity import (
    HazardCandidate,
    HazardOutcome,
    calibrate_candidate,
    deduplicate_for_event_screen,
    direction_flipped_intents,
    discover_intents_batch,
    exact_sleeve_replay,
    frozen_eligible_session_calendar,
    generate_structural_proposals,
    matched_random_intents,
    observe_outcomes,
    realized_behavioral_fingerprint,
    screen_result,
    with_availability_safe_cross_asset_feature,
)


CAUSAL_TARGET_VELOCITY_RUNTIME_VERSION = "causal_target_velocity_runtime_v1"
_MICRO_MARKETS = {
    "CL": "MCL",
    "ES": "MES",
    "GC": "MGC",
    "NQ": "MNQ",
    "RTY": "M2K",
    "YM": "MYM",
}
_CROSS_ASSET_REFERENCE_MARKETS = {
    "CL": "ES",
    "ES": "NQ",
    "NQ": "ES",
    "RTY": "ES",
    "YM": "ES",
}
_STAGE1_BATCH_SIZE = 16
_STAGE2_BATCH_SIZE = 8
_STAGE3_BATCH_SIZE = 4
_WORKER_MATRICES: dict[str, FeatureMatrix] = {}
_WORKER_PERIOD: tuple[int, int, int] | None = None
_WORKER_BLOCKS: tuple[dict[str, Any], ...] = ()
_CLEAN_BASELINE_CACHE: dict[str, Any] | None = None


class CausalTargetVelocityRuntimeError(RuntimeError):
    """Campaign 0028 failed closed before a valid checkpoint/result."""


def read_causal_target_velocity_status(
    manifest_path: str | Path,
) -> dict[str, Any]:
    manifest = load_and_validate_causal_target_velocity_manifest(manifest_path)
    root = _project_root(Path(manifest_path).resolve())
    output = root / str(manifest["runtime"]["output_dir"])
    state = _read_json(output / "production_state.json")
    kpis = _read_json(output / "production_kpis.json")
    _verify_snapshot(state, "state_hash", manifest)
    _verify_snapshot(kpis, "kpi_hash", manifest)
    return {"state": state, "kpis": kpis}


def run_causal_target_velocity_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path,
    cache_root: str | Path,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run or resume the single frozen 0028 production manifest."""

    return _CausalTargetVelocityRun(
        manifest_path=Path(manifest_path),
        contract_map_path=Path(contract_map_path),
        cache_root=Path(cache_root),
        stop_after=stop_after,
    ).execute()


class _CausalTargetVelocityRun:
    def __init__(
        self,
        *,
        manifest_path: Path,
        contract_map_path: Path,
        cache_root: Path,
        stop_after: str | None,
    ) -> None:
        self.manifest_path = manifest_path.resolve()
        self.root = _project_root(self.manifest_path)
        self.manifest = load_and_validate_causal_target_velocity_manifest(
            self.manifest_path
        )
        if self.manifest["runtime"]["engine"] != CAUSAL_TARGET_VELOCITY_ENGINE:
            raise CausalTargetVelocityRuntimeError("wrong manifest engine")
        self.contract_map_path = contract_map_path.resolve()
        self.cache_root = cache_root.resolve()
        self.stop_after = stop_after
        if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
            raise CausalTargetVelocityRuntimeError(
                "stop_after requires HYDRA_PRODUCTION_TEST_MODE=1"
            )
        self.campaign_id = str(self.manifest["campaign_id"])
        self.output_dir = (
            self.root / str(self.manifest["runtime"]["output_dir"])
        ).resolve()
        self.payload_dir = (
            self.root / "data/cache/economic_production" / self.campaign_id
        ).resolve()
        self.live_writer = AtomicResultWriter(self.output_dir, immutable=False)
        self.output_writer = AtomicResultWriter(self.output_dir)
        self.payload_writer = AtomicResultWriter(self.payload_dir)
        self.started_wall = time.perf_counter()
        self.cpu_ticks_started = _system_cpu_ticks()
        self.hot_seconds = 0.0
        self.cold_seconds = 0.0
        self.reporting_seconds = 0.0
        self.population_summary: dict[str, Any] = {}
        self.stage1_rows: list[dict[str, Any]] = []
        self.stage2_rows: list[dict[str, Any]] = []
        self.stage3_rows: list[dict[str, Any]] = []
        self.evidence_only_candidate_ids: set[str] = set()
        self.state = self._initial_state()
        self.prior_runner_wall_seconds = float(
            self.state.get("campaign_runner_wall_seconds", 0.0)
        )
        # Resume accounting is cumulative.  A restarted worker must not make
        # economic allocation appear to go backwards or silently discard the
        # time already spent by the persistent campaign.
        self.hot_seconds = float(self.state.get("economic_compute_seconds", 0.0))
        self.cold_seconds = float(self.state.get("integrity_seconds", 0.0))
        self.reporting_seconds = float(self.state.get("reporting_seconds", 0.0))

    def execute(self) -> dict[str, Any]:
        result_path = self.output_dir / str(self.manifest["runtime"]["result_name"])
        if result_path.is_file():
            # The controller performs the deep EvidenceBundle verification.
            return _read_json(result_path)
        try:
            self._verify_deployment()
            self._publish(
                state="STARTING",
                stage="RISK_FRONTIER_PREFLIGHT",
                next_action="RUN_BOUNDED_3_4_5_6_MICRO_FRONTIER",
            )
            preflight_started = time.perf_counter()
            preflight = run_causal_risk_preflight(
                self.manifest_path,
                repository_root=self.root,
                output_dir=_risk_preflight_output_dir(
                    self.root, self.output_dir, self.manifest
                ),
            )
            self.hot_seconds += time.perf_counter() - preflight_started
            self._publish(
                state="STARTING",
                stage="RISK_FRONTIER_PREFLIGHT_COMPLETE",
                risk_frontier_status=str(preflight["status"]),
                risk_frontier_result_hash=str(preflight["result_hash"]),
                risk_frontier_raw_identity_status=str(
                    (preflight.get("former_book_raw_unscaled_identity") or {}).get(
                        "status", "SEPARATE_RAW_IDENTITY_RECEIPT"
                    )
                ),
                next_action="FREEZE_CAUSAL_STRUCTURAL_POPULATION",
            )
            matrices = _discover_feature_matrices(
                self.cache_root,
                tuple(str(value) for value in self.manifest["search_space"]["markets"]),
            )
            period = _resolve_period(self.manifest, matrices)
            proposals, screened = self._stage0_population()
            if len(proposals) < 20_000 or len(screened) < 4_096:
                raise CausalTargetVelocityRuntimeError(
                    "Stage 0 minimum population contract was not met"
                )
            self._publish(
                state="POPULATION_FROZEN",
                stage="STAGE_1_EVENT_SCREEN_ACTIVE",
                policies_proposed=len(proposals),
                unique_policies_screened=len(self._load_stage1_rows()),
                next_action="SCREEN_TARGET_BEFORE_ADVERSE_EVENTS",
            )
            self.stage1_rows = self._stage1_event_screen(
                screened[:4_096], matrices=matrices, period=period
            )
            selected_stage1 = _select_stage1(self.stage1_rows, maximum=1_024)
            self.payload_writer.write_json(
                "stage1_halving.json",
                _selection_receipt(
                    campaign_id=self.campaign_id,
                    stage="STAGE_1_EVENT_SCREEN",
                    input_count=len(self.stage1_rows),
                    selected=selected_stage1,
                    selection="TRANSPARENT_PARETO_HAZARD_DENSITY_V1",
                ),
            )
            self._publish(
                state="FIRST_HALVING_COMPLETE",
                stage="STAGE_1",
                policies_proposed=len(proposals),
                unique_policies_screened=len(self.stage1_rows),
                stage1_survivor_count=len(selected_stage1),
                next_action="EXACT_CAUSAL_SLEEVE_REPLAY",
            )
            if self.stop_after in {"FAST_SCREEN", "FIRST_HALVING"}:
                return dict(self.state)

            selected_candidates = tuple(
                HazardCandidate(**dict(row["candidate"])) for row in selected_stage1
            )
            evidence_only_ids = {
                str(row["candidate_id"])
                for row in selected_stage1
                if row.get("selection_role")
                == "EVIDENCE_ONLY_DIAGNOSTIC_EXACT_REPLAY"
            }
            self.evidence_only_candidate_ids = set(evidence_only_ids)
            self.stage2_rows = self._stage2_exact_replay(
                selected_candidates, matrices=matrices, period=period
            )
            selected_stage2 = [
                row
                for row in _select_stage2(self.stage2_rows, maximum=256)
                if str(row["candidate_id"]) not in evidence_only_ids
            ]
            self.payload_writer.write_json(
                "stage2_halving.json",
                _selection_receipt(
                    campaign_id=self.campaign_id,
                    stage="STAGE_2_EXACT_SLEEVE_REPLAY",
                    input_count=len(self.stage2_rows),
                    selected=selected_stage2,
                    selection="TRANSPARENT_PARETO_EXACT_CAUSAL_SLEEVE_V1",
                ),
            )
            self._publish(
                state="ROBUSTNESS_ACTIVE",
                stage="STAGE_3_ROLLING_COMBINE_ACTIVE",
                exact_account_replays=_completed_count(
                    self.stage2_rows, "STAGE_2_COMPLETE"
                ),
                stage2_survivor_count=len(selected_stage2),
                next_action="RUN_FULL_COVERAGE_BLOCK_AWARE_COMBINE",
            )
            if not selected_stage2:
                return self._finalize_campaign(
                    preflight=preflight,
                    useful=(),
                    stage4=(),
                    promoted=(),
                    reason="NO_EXACT_CAUSAL_SLEEVE_SURVIVED_STAGE_2",
                )
            stage3_candidates = tuple(
                HazardCandidate(**dict(row["candidate"])) for row in selected_stage2
            )
            self.stage3_rows = self._stage3_rolling_combine(
                stage3_candidates, matrices=matrices, period=period
            )
            useful = _select_stage3_useful(self.stage3_rows, maximum=64)
            self.payload_writer.write_json(
                "stage3_halving.json",
                _selection_receipt(
                    campaign_id=self.campaign_id,
                    stage="STAGE_3_ROLLING_COMBINE",
                    input_count=len(self.stage3_rows),
                    selected=useful,
                    selection="FROZEN_CLEAN_SLEEVE_ACCOUNT_ASSEMBLY_GATE_V1",
                ),
            )
            self._publish(
                state="ROBUSTNESS_ACTIVE",
                stage="STAGE_3_COMPLETE",
                exact_account_replays=_completed_count(
                    self.stage2_rows, "STAGE_2_COMPLETE"
                ),
                combine_episodes_completed=(
                    2 * _completed_count(self.stage2_rows, "STAGE_2_COMPLETE")
                    + _stage3_episode_count(self.stage3_rows)
                ),
                clean_useful_sleeve_count=len(useful),
                next_action=(
                    "ASSEMBLE_CAUSAL_ACTIVE_RISK_POOL_BOOKS"
                    if len(useful) >= 4
                    else "SEAL_EXACT_MECHANISM_FALSIFICATION"
                ),
            )
            # Account assembly deliberately remains a separate bounded stage.
            if len(useful) >= 4:
                stage4 = self._stage4_account_assembly(
                    useful, matrices=matrices, period=period
                )
                promoted = _select_stage4_for_expansion(stage4, maximum=16)
                self.payload_writer.write_json(
                    "stage4_halving.json",
                    _selection_receipt(
                        campaign_id=self.campaign_id,
                        stage="STAGE_4_ACCOUNT_ASSEMBLY",
                        input_count=len(stage4),
                        selected=promoted,
                        selection="FROZEN_CAUSAL_ACTIVE_POOL_PROMOTION_GATE_V1",
                    ),
                )
                self._publish(
                    state="FINALIZING",
                    stage="STAGE_4_COMPLETE",
                    combine_episodes_completed=(
                        2 * _completed_count(self.stage2_rows, "STAGE_2_COMPLETE")
                        + _stage3_episode_count(self.stage3_rows)
                        + _stage4_episode_count(stage4)
                    ),
                    causal_books_assembled=len(stage4),
                    candidates_promoted_96=0,
                    full_coverage_96_start_limitation=True,
                    next_action="SEAL_COMPLETE_CAUSAL_EVIDENCE_BUNDLE",
                )
                return self._finalize_campaign(
                    preflight=preflight,
                    useful=useful,
                    stage4=stage4,
                    promoted=promoted,
                    reason=(
                        "CLEAN_BOOKS_MET_FROZEN_48_TO_96_GATE"
                        if promoted
                        else (
                            "FULL_COVERAGE_START_LIMIT_PREVENTED_96_START_EXPANSION"
                            if any(
                                int(
                                    (row.get("coverage") or {}).get(
                                        "candidate_full_horizon_start_count", 0
                                    )
                                )
                                < 48
                                for row in stage4
                                if row.get("status") == "STAGE_4_COMPLETE"
                            )
                            else "NO_BOOK_MET_FROZEN_48_TO_96_GATE"
                        )
                    ),
                )
            return self._finalize_campaign(
                preflight=preflight,
                useful=useful,
                stage4=(),
                promoted=(),
                reason="FEWER_THAN_FOUR_USEFUL_CAUSAL_SLEEVES",
            )
        except BaseException as exc:
            self._publish(
                state="FAILED_CLOSED",
                stage=str(self.state.get("stage") or "UNKNOWN"),
                next_action="MANUAL_INTEGRITY_REVIEW_REQUIRED",
                failure_type=type(exc).__name__,
                failure_message=str(exc)[:1_000],
            )
            raise

    def _verify_deployment(self) -> None:
        completed = subprocess.run(
            ["git", "merge-base", "--is-ancestor", str(self.manifest["source_commit"]), "HEAD"],
            cwd=self.root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if completed.returncode != 0:
            raise CausalTargetVelocityRuntimeError(
                "manifest source commit is not an ancestor of live HEAD"
            )
        if not self.contract_map_path.is_file():
            raise CausalTargetVelocityRuntimeError("frozen contract map is missing")
        if not self.cache_root.is_dir():
            raise CausalTargetVelocityRuntimeError("frozen feature cache is missing")
        search = self.manifest["search_space"]
        markets = tuple(str(value) for value in search["markets"])
        if set(markets) != set(_CROSS_ASSET_REFERENCE_MARKETS):
            raise CausalTargetVelocityRuntimeError(
                "0028 complete-coverage market bank drift"
            )
        declared_cross_map = {
            str(key): str(value)
            for key, value in dict(
                search.get("cross_asset_reference_map") or {}
            ).items()
        }
        if declared_cross_map != _CROSS_ASSET_REFERENCE_MARKETS:
            raise CausalTargetVelocityRuntimeError(
                "0028 cross-asset reference map is not frozen in the manifest"
            )
        exclusions = dict(search.get("market_exclusions") or {})
        if "GC" not in exclusions or "INCOMPLETE" not in str(exclusions["GC"]):
            raise CausalTargetVelocityRuntimeError(
                "GC incomplete-coverage exclusion is not explicit"
            )

    def _stage0_population(
        self,
    ) -> tuple[tuple[HazardCandidate, ...], tuple[HazardCandidate, ...]]:
        started = time.perf_counter()
        proposal_path = self.payload_dir / "structural_proposals.jsonl"
        screen_path = self.payload_dir / "stage1_candidate_population.jsonl"
        if proposal_path.is_file() and screen_path.is_file():
            proposals = tuple(
                HazardCandidate(**row["candidate"])
                for row in _read_jsonl(proposal_path)
            )
            screened = tuple(
                HazardCandidate(**row["candidate"])
                for row in _read_jsonl(screen_path)
            )
            self.population_summary = _read_json(
                self.payload_dir / "structural_population_summary.json"
            )
        else:
            market_pairs = {
                market: _MICRO_MARKETS[market]
                for market in self.manifest["search_space"]["markets"]
            }
            source_proposals = generate_structural_proposals(
                market_pairs,
                minimum_count=int(self.manifest["search_space"]["proposal_count"])
                + 256,
            )
            proposals, cemetery = self._audit_cemetery(source_proposals)
            target = int(self.manifest["search_space"]["proposal_count"])
            proposals = proposals[:target]
            if len(proposals) != target:
                raise CausalTargetVelocityRuntimeError(
                    "cemetery exclusion left fewer than 20,000 proposals"
                )
            deduplicated = deduplicate_for_event_screen(
                proposals,
                minimum_unique=int(
                    self.manifest["search_space"]["unique_event_screen_minimum"]
                ),
            )
            screened = _balanced_candidate_sample(deduplicated, 4_096)
            proposal_rows = [_candidate_record(row) for row in proposals]
            screen_rows = [_candidate_record(row) for row in screened]
            self.payload_writer.write_jsonl_batch(
                "structural_proposals.jsonl", proposal_rows
            )
            self.payload_writer.write_jsonl_batch(
                "stage1_candidate_population.jsonl", screen_rows
            )
            self.population_summary = {
                "schema": "hydra_causal_target_velocity_population_v1",
                "campaign_id": self.campaign_id,
                "proposal_count": len(proposals),
                "behaviorally_unique_count": len(deduplicated),
                "stage1_screen_count": len(screened),
                "duplicate_rejection_count": len(proposals) - len(deduplicated),
                "duplicate_rejection_rate": (
                    (len(proposals) - len(deduplicated)) / len(proposals)
                ),
                "pre_reservation_quarantine_count": int(
                    cemetery["pre_reservation_quarantine_count"]
                ),
                "pre_reservation_quarantine_population_collision_count": len(
                    cemetery["pre_reservation_quarantine_population_collisions"]
                ),
                "screen_truncation_is_duplicate_rejection": False,
                "mechanism_counts": _counts(
                    row.mechanism for row in proposals
                ),
                "market_counts": _counts(row.market for row in proposals),
                "population_hash": stable_hash(proposal_rows),
                "stage1_population_hash": stable_hash(screen_rows),
            }
            self.payload_writer.write_json(
                "structural_population_summary.json", self.population_summary
            )
            self.payload_writer.write_json("cemetery_audit.json", cemetery)
        self.hot_seconds += time.perf_counter() - started
        self._publish(
            state="POPULATION_FROZEN",
            stage="STAGE_0",
            policies_proposed=len(proposals),
            unique_policies_screened=len(self._load_stage1_rows()),
            next_action="FAST_CAUSAL_EVENT_SCREEN",
        )
        return proposals, screened

    def _audit_cemetery(
        self, proposals: Sequence[HazardCandidate]
    ) -> tuple[tuple[HazardCandidate, ...], dict[str, Any]]:
        path = self.root / "mission/state/graveyard.db"
        if not path.is_file():
            raise CausalTargetVelocityRuntimeError("authoritative cemetery is missing")
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            rows = connection.execute(
                "SELECT signature_hash, mechanism_class, regime, death_cause "
                "FROM class_tombstones ORDER BY signature_hash"
            ).fetchall()
        finally:
            connection.close()
        signatures = {str(row[0]) for row in rows}
        dead_classes = {str(row[1]) for row in rows}
        if str(self.manifest["class_id"]) in dead_classes:
            raise CausalTargetVelocityRuntimeError(
                "the exact 0028 mechanism class is already tombstoned"
            )
        dead_recipe_collisions = sorted(
            {row.mechanism for row in proposals} & dead_classes
        )
        if dead_recipe_collisions:
            raise CausalTargetVelocityRuntimeError(
                "Stage 0 attempted exact tombstoned mechanism recipes: "
                + ",".join(dead_recipe_collisions)
            )
        collisions = sorted(
            row.structural_fingerprint
            for row in proposals
            if row.structural_fingerprint in signatures
        )
        retained = tuple(
            row
            for row in proposals
            if row.structural_fingerprint not in signatures
        )
        quarantine_path = (
            self.root
            / "reports/economic_evolution/causal_target_velocity_0028_pre_reservation_exposure_quarantine.json"
        )
        quarantine = _read_json(quarantine_path)
        quarantine_payload = dict(quarantine)
        claimed_quarantine_hash = str(quarantine_payload.pop("receipt_hash", ""))
        if (
            quarantine.get("schema")
            != "hydra_causal_target_velocity_pre_reservation_exposure_quarantine_v1"
            or quarantine.get("campaign_id") != self.campaign_id
            or claimed_quarantine_hash != stable_hash(quarantine_payload)
            or int(quarantine.get("exposed_candidate_count", -1)) != 10
        ):
            raise CausalTargetVelocityRuntimeError(
                "pre-reservation exposure quarantine integrity failure"
            )
        quarantine_fingerprints = {
            str(row["structural_fingerprint"])
            for row in quarantine["exposed_candidates"]
        }
        if len(quarantine_fingerprints) != 10:
            raise CausalTargetVelocityRuntimeError(
                "pre-reservation quarantine fingerprint cardinality drift"
            )
        quarantine_collisions = sorted(
            row.structural_fingerprint
            for row in retained
            if row.structural_fingerprint in quarantine_fingerprints
        )
        retained = tuple(
            row
            for row in retained
            if row.structural_fingerprint not in quarantine_fingerprints
        )
        receipt = {
            "schema": "hydra_causal_target_velocity_cemetery_audit_v1",
            "campaign_id": self.campaign_id,
            "cemetery_path": str(path.relative_to(self.root)),
            "cemetery_sha256": _sha256(path),
            "class_tombstone_count": len(rows),
            "exact_class_collision": False,
            "exact_recipe_class_collisions": dead_recipe_collisions,
            "candidate_fingerprint_collision_count": len(collisions),
            "candidate_fingerprint_collisions": collisions,
            "pre_reservation_quarantine_path": str(
                quarantine_path.relative_to(self.root)
            ),
            "pre_reservation_quarantine_sha256": _sha256(quarantine_path),
            "pre_reservation_quarantine_receipt_hash": claimed_quarantine_hash,
            "pre_reservation_quarantine_count": len(quarantine_fingerprints),
            "pre_reservation_quarantine_population_collisions": quarantine_collisions,
            "source_proposal_count": len(proposals),
            "retained_after_cemetery": len(retained),
            "cemetery_resurrection_allowed": False,
        }
        receipt["receipt_hash"] = stable_hash(receipt)
        return retained, receipt

    def _stage1_event_screen(
        self,
        candidates: Sequence[HazardCandidate],
        *,
        matrices: Mapping[str, Path],
        period: tuple[int, int, int],
    ) -> list[dict[str, Any]]:
        existing = self._load_stage1_rows()
        for row in existing:
            _verify_stage1_event_evidence_receipt(self.payload_dir, row)
        completed_ids = {str(row["candidate_id"]) for row in existing}
        pending = [row for row in candidates if row.candidate_id not in completed_ids]
        if pending:
            payloads = [
                [row.payload for row in pending[index : index + _STAGE1_BATCH_SIZE]]
                for index in range(0, len(pending), _STAGE1_BATCH_SIZE)
            ]
            batch_start = _next_batch_index(
                self.payload_dir / "stage1_event_screen_batches"
            )
            context = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=3,
                mp_context=context,
                initializer=_initialize_worker,
                initargs=(
                    {key: str(value) for key, value in matrices.items()},
                    period,
                    tuple(dict(row) for row in self.manifest["temporal_blocks"]["blocks"]),
                ),
            ) as pool:
                last_tick = time.perf_counter()
                for offset, rows in enumerate(
                    pool.map(_screen_candidate_batch_worker, payloads, chunksize=1)
                ):
                    public_rows: list[dict[str, Any]] = []
                    for row in rows:
                        materialized = dict(row)
                        event_rows = list(materialized.pop("_event_evidence", ()))
                        if materialized.get("status") == "STAGE_1_COMPLETE":
                            candidate_id = str(materialized["candidate_id"])
                            uncompressed = _canonical_jsonl_bytes(event_rows)
                            compressed = gzip.compress(
                                uncompressed, compresslevel=6, mtime=0
                            )
                            relative = (
                                f"stage1_event_evidence/{candidate_id}.jsonl.gz"
                            )
                            receipt = self.payload_writer.write_bytes(
                                relative, compressed
                            )
                            materialized["event_evidence"] = {
                                "schema": (
                                    "hydra_causal_target_velocity_stage1_"
                                    "event_evidence_receipt_v1"
                                ),
                                "relative_path": relative,
                                "encoding": "canonical-jsonl+gzip-mtime-zero",
                                "record_count": len(event_rows),
                                "sha256": receipt.sha256,
                                "uncompressed_sha256": hashlib.sha256(
                                    uncompressed
                                ).hexdigest(),
                                "event_fingerprint_hash": stable_hash(
                                    [
                                        str(value["event_fingerprint"])
                                        for value in event_rows
                                    ]
                                ),
                            }
                        public_rows.append(materialized)
                    self.payload_writer.write_jsonl_batch(
                        f"stage1_event_screen_batches/batch_{batch_start + offset:04d}.jsonl",
                        public_rows,
                    )
                    existing.extend(public_rows)
                    now = time.perf_counter()
                    self.hot_seconds += max(now - last_tick, 0.0)
                    last_tick = now
                    self.stage1_rows = existing
                    self._publish(
                        state="POPULATION_FROZEN",
                        stage="STAGE_1_EVENT_SCREEN_ACTIVE",
                        policies_proposed=int(
                            self.manifest["search_space"]["proposal_count"]
                        ),
                        unique_policies_screened=len(existing),
                        next_action="CONTINUE_CAUSAL_EVENT_SCREEN",
                        last_completed_policy_id=str(public_rows[-1]["candidate_id"]),
                    )
        rows = self._load_stage1_rows()
        for row in rows:
            _verify_stage1_event_evidence_receipt(self.payload_dir, row)
        expected = {row.candidate_id for row in candidates}
        observed = {str(row["candidate_id"]) for row in rows}
        if observed != expected or len(rows) != len(expected):
            raise CausalTargetVelocityRuntimeError(
                "Stage 1 candidate/result identity reconciliation failed"
            )
        return sorted(rows, key=lambda row: str(row["candidate_id"]))

    def _stage2_exact_replay(
        self,
        candidates: Sequence[HazardCandidate],
        *,
        matrices: Mapping[str, Path],
        period: tuple[int, int, int],
    ) -> list[dict[str, Any]]:
        existing = _load_batches(self.payload_dir / "stage2_exact_replay_batches")
        completed = {str(row["candidate_id"]) for row in existing}
        pending = [row for row in candidates if row.candidate_id not in completed]
        self.stage2_rows = list(existing)
        self._publish(
            state="EXACT_REPLAY_ACTIVE",
            stage="STAGE_2_EXACT_SLEEVE_REPLAY",
            exact_account_replays=_completed_count(existing, "STAGE_2_COMPLETE"),
            next_action="REPLAY_EXACT_CAUSAL_SLEEVES",
        )
        if pending:
            payloads = [
                [row.payload for row in pending[index : index + _STAGE2_BATCH_SIZE]]
                for index in range(0, len(pending), _STAGE2_BATCH_SIZE)
            ]
            batch_start = _next_batch_index(
                self.payload_dir / "stage2_exact_replay_batches"
            )
            context = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=3,
                mp_context=context,
                initializer=_initialize_worker,
                initargs=(
                    {key: str(value) for key, value in matrices.items()},
                    period,
                    tuple(dict(row) for row in self.manifest["temporal_blocks"]["blocks"]),
                ),
            ) as pool:
                last_tick = time.perf_counter()
                for offset, rows in enumerate(
                    pool.map(_exact_candidate_batch_worker, payloads, chunksize=1)
                ):
                    public_rows: list[dict[str, Any]] = []
                    for row in rows:
                        materialized = dict(row)
                        event_rows = list(materialized.pop("_event_evidence", ()))
                        episode_rows = list(materialized.pop("_episode_evidence", ()))
                        candidate_id = str(materialized["candidate_id"])
                        materialized["economic_advancement_authorized"] = (
                            candidate_id not in self.evidence_only_candidate_ids
                        )
                        if event_rows:
                            self.payload_writer.write_jsonl_batch(
                                f"stage2_event_evidence/{candidate_id}.jsonl",
                                event_rows,
                            )
                        if episode_rows:
                            relative = f"stage2_episode_evidence/{candidate_id}.jsonl"
                            self.payload_writer.write_jsonl_batch(relative, episode_rows)
                            episode_path = self.payload_dir / relative
                            materialized["episode_evidence"] = {
                                "relative_path": relative,
                                "record_count": len(episode_rows),
                                "sha256": _sha256(episode_path),
                            }
                        public_rows.append(materialized)
                    self.payload_writer.write_jsonl_batch(
                        f"stage2_exact_replay_batches/batch_{batch_start + offset:04d}.jsonl",
                        public_rows,
                    )
                    existing.extend(public_rows)
                    now = time.perf_counter()
                    self.hot_seconds += max(now - last_tick, 0.0)
                    last_tick = now
                    self.stage2_rows = list(existing)
                    self._publish(
                        state="EXACT_REPLAY_ACTIVE",
                        stage="STAGE_2_EXACT_SLEEVE_REPLAY",
                        exact_account_replays=_completed_count(
                            existing, "STAGE_2_COMPLETE"
                        ),
                        combine_episodes_completed=2
                        * _completed_count(existing, "STAGE_2_COMPLETE"),
                        next_action="CONTINUE_EXACT_CAUSAL_SLEEVE_REPLAY",
                        last_completed_policy_id=str(public_rows[-1]["candidate_id"]),
                    )
        expected = {row.candidate_id for row in candidates}
        rows = _load_batches(self.payload_dir / "stage2_exact_replay_batches")
        for row in rows:
            if str(row["candidate_id"]) in self.evidence_only_candidate_ids:
                row["economic_advancement_authorized"] = False
        if {str(row["candidate_id"]) for row in rows} != expected:
            raise CausalTargetVelocityRuntimeError("Stage 2 identity reconciliation failed")
        return sorted(rows, key=lambda row: str(row["candidate_id"]))

    def _stage3_rolling_combine(
        self,
        candidates: Sequence[HazardCandidate],
        *,
        matrices: Mapping[str, Path],
        period: tuple[int, int, int],
    ) -> list[dict[str, Any]]:
        existing = _load_batches(self.payload_dir / "stage3_rolling_combine_batches")
        completed = {str(row["candidate_id"]) for row in existing}
        pending = [row for row in candidates if row.candidate_id not in completed]
        if pending:
            payloads = [
                [row.payload for row in pending[index : index + _STAGE3_BATCH_SIZE]]
                for index in range(0, len(pending), _STAGE3_BATCH_SIZE)
            ]
            batch_start = _next_batch_index(
                self.payload_dir / "stage3_rolling_combine_batches"
            )
            context = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=3,
                mp_context=context,
                initializer=_initialize_worker,
                initargs=(
                    {key: str(value) for key, value in matrices.items()},
                    period,
                    tuple(dict(row) for row in self.manifest["temporal_blocks"]["blocks"]),
                ),
            ) as pool:
                last_tick = time.perf_counter()
                for offset, rows in enumerate(
                    pool.map(_rolling_candidate_batch_worker, payloads, chunksize=1)
                ):
                    public_rows: list[dict[str, Any]] = []
                    for row in rows:
                        materialized = dict(row)
                        event_rows = list(materialized.pop("_event_evidence", ()))
                        episode_rows = list(materialized.pop("_episode_evidence", ()))
                        candidate_id = str(materialized["candidate_id"])
                        if event_rows:
                            self.payload_writer.write_jsonl_batch(
                                f"stage3_event_evidence/{candidate_id}.jsonl",
                                event_rows,
                            )
                        if episode_rows:
                            relative = (
                                f"stage3_episode_evidence/{candidate_id}.jsonl"
                            )
                            self.payload_writer.write_jsonl_batch(relative, episode_rows)
                            episode_path = self.payload_dir / relative
                            materialized["episode_evidence"] = {
                                "relative_path": relative,
                                "record_count": len(episode_rows),
                                "sha256": _sha256(episode_path),
                            }
                        public_rows.append(materialized)
                    self.payload_writer.write_jsonl_batch(
                        f"stage3_rolling_combine_batches/batch_{batch_start + offset:04d}.jsonl",
                        public_rows,
                    )
                    existing.extend(public_rows)
                    now = time.perf_counter()
                    self.hot_seconds += max(now - last_tick, 0.0)
                    last_tick = now
                    self.stage3_rows = list(existing)
                    self._publish(
                        state="ROBUSTNESS_ACTIVE",
                        stage="STAGE_3_ROLLING_COMBINE_ACTIVE",
                        exact_account_replays=_completed_count(
                            self.stage2_rows, "STAGE_2_COMPLETE"
                        ),
                        combine_episodes_completed=(
                            2
                            * _completed_count(
                                self.stage2_rows, "STAGE_2_COMPLETE"
                            )
                            + _stage3_episode_count(existing)
                        ),
                        next_action="CONTINUE_FULL_COVERAGE_ROLLING_COMBINE",
                        last_completed_policy_id=str(public_rows[-1]["candidate_id"]),
                    )
        rows = _load_batches(self.payload_dir / "stage3_rolling_combine_batches")
        expected = {row.candidate_id for row in candidates}
        if {str(row["candidate_id"]) for row in rows} != expected:
            raise CausalTargetVelocityRuntimeError("Stage 3 identity reconciliation failed")
        return sorted(rows, key=lambda row: str(row["candidate_id"]))

    def _stage4_account_assembly(
        self,
        useful: Sequence[Mapping[str, Any]],
        *,
        matrices: Mapping[str, Path],
        period: tuple[int, int, int],
    ) -> list[dict[str, Any]]:
        books = _generate_stage4_books(useful, maximum=16)
        base_episode_count = (
            2 * _completed_count(self.stage2_rows, "STAGE_2_COMPLETE")
            + _stage3_episode_count(self.stage3_rows)
        )
        path = self.payload_dir / "stage4_account_assembly_batches"
        existing = _load_book_batches(path)
        completed = {str(row["book_id"]) for row in existing}
        pending = [row for row in books if str(row["book_id"]) not in completed]
        if pending:
            payloads = [pending[index : index + 2] for index in range(0, len(pending), 2)]
            batch_start = _next_batch_index(path)
            context = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(
                max_workers=3,
                mp_context=context,
                initializer=_initialize_worker,
                initargs=(
                    {key: str(value) for key, value in matrices.items()},
                    period,
                    tuple(dict(row) for row in self.manifest["temporal_blocks"]["blocks"]),
                ),
            ) as pool:
                last_tick = time.perf_counter()
                for offset, rows in enumerate(
                    pool.map(_account_book_batch_worker, payloads, chunksize=1)
                ):
                    public_rows: list[dict[str, Any]] = []
                    for row in rows:
                        materialized = dict(row)
                        episode_rows = list(materialized.pop("_episode_evidence", ()))
                        book_id = str(materialized["book_id"])
                        if episode_rows:
                            relative = f"stage4_episode_evidence/{book_id}.jsonl"
                            self.payload_writer.write_jsonl_batch(relative, episode_rows)
                            materialized["episode_evidence"] = {
                                "relative_path": relative,
                                "record_count": len(episode_rows),
                                "sha256": _sha256(self.payload_dir / relative),
                            }
                        public_rows.append(materialized)
                    self.payload_writer.write_jsonl_batch(
                        f"stage4_account_assembly_batches/batch_{batch_start + offset:04d}.jsonl",
                        public_rows,
                    )
                    existing.extend(public_rows)
                    now = time.perf_counter()
                    self.hot_seconds += max(now - last_tick, 0.0)
                    last_tick = now
                    self._publish(
                        state="ROBUSTNESS_ACTIVE",
                        stage="STAGE_4_ACCOUNT_ASSEMBLY_ACTIVE",
                        causal_books_assembled=len(existing),
                        combine_episodes_completed=(
                            base_episode_count + _stage4_episode_count(existing)
                        ),
                        next_action="CONTINUE_CAUSAL_ACTIVE_POOL_ASSEMBLY",
                        last_completed_policy_id=str(public_rows[-1]["book_id"]),
                    )
        rows = _load_book_batches(path)
        expected = {str(row["book_id"]) for row in books}
        if {str(row["book_id"]) for row in rows} != expected:
            raise CausalTargetVelocityRuntimeError("Stage 4 book identity reconciliation failed")
        return sorted(rows, key=lambda row: str(row["book_id"]))

    def _finalize_campaign(
        self,
        *,
        preflight: Mapping[str, Any],
        useful: Sequence[Mapping[str, Any]],
        stage4: Sequence[Mapping[str, Any]],
        promoted: Sequence[Mapping[str, Any]],
        reason: str,
    ) -> dict[str, Any]:
        """Seal one complete bundle and the sole V17 terminal result."""

        stage2_evidence_rows: list[Mapping[str, Any]] = [
            row
            for row in self.stage2_rows
            if row.get("status") == "STAGE_2_COMPLETE"
            and (row.get("episode_evidence") or {}).get("record_count", 0)
        ]
        stage3_evidence_rows: list[Mapping[str, Any]] = [
            row
            for row in self.stage3_rows
            if row.get("status") == "STAGE_3_COMPLETE"
            and (row.get("episode_evidence") or {}).get("record_count", 0)
        ]
        stage4_evidence_rows: list[Mapping[str, Any]] = [
            row
            for row in stage4
            if row.get("status") == "STAGE_4_COMPLETE"
            and (row.get("episode_evidence") or {}).get("record_count", 0)
        ]
        evidence_rows: list[Mapping[str, Any]] = [
            *stage2_evidence_rows,
            *stage3_evidence_rows,
            *stage4_evidence_rows,
        ]
        evidence_stage = "stage2_to_stage4_complete_union"
        if not evidence_rows:
            raise CausalTargetVelocityRuntimeError(
                "terminal EvidenceBundle has no exact account episodes"
            )
        stage1_inventory = (
            _seal_stage1_event_evidence_inventory(
                payload_dir=self.payload_dir,
                writer=self.payload_writer,
                campaign_id=self.campaign_id,
                rows=self.stage1_rows,
            )
            if self.stage1_rows
            else {
                "relative_path": None,
                "sha256": None,
                "inventory_hash": stable_hash([]),
                "candidate_ledger_count": 0,
                "event_record_count": 0,
                "censored_future_coverage_count": 0,
            }
        )

        evaluated_records = _load_episode_evidence_records(
            self.payload_dir, evidence_rows
        )
        policies: dict[str, ActiveRiskPoolPolicy] = {}
        component_ids: set[str] = set()
        for row in (*stage2_evidence_rows, *stage3_evidence_rows):
            candidate_id = str(row["candidate_id"])
            policy = _standalone_hazard_policy(candidate_id)
            policies[policy.policy_id] = policy
            component_ids.add(candidate_id)
        for row in stage4_evidence_rows:
            policy = ActiveRiskPoolPolicy.from_mapping(row["policy"])
            policies[policy.policy_id] = policy
            component_ids.update(policy.component_ids)

        stage3_by_id = {
            str(row["candidate_id"]): row
            for row in self.stage3_rows
            if row.get("status") == "STAGE_3_COMPLETE"
        }
        stage2_by_id = {
            str(row["candidate_id"]): row
            for row in self.stage2_rows
            if row.get("status") == "STAGE_2_COMPLETE"
        }
        exact_replays: dict[str, Any] = {}
        for component_id in sorted(component_ids):
            source = stage2_by_id.get(component_id) or stage3_by_id.get(component_id)
            if source is None:
                raise CausalTargetVelocityRuntimeError(
                    f"terminal component lacks exact replay row: {component_id}"
                )
            event_stage = "stage2" if component_id in stage2_by_id else "stage3"
            event_path = (
                self.payload_dir
                / f"{event_stage}_event_evidence/{component_id}.jsonl"
            )
            events = _read_jsonl(event_path)
            expected_hashes = {
                name: str(source[name])
                for name in (
                    "decision_hash",
                    "normal_event_hash",
                    "stressed_event_hash",
                    "normal_trajectory_hash",
                    "stressed_trajectory_hash",
                    "fill_policy_hash",
                )
            }
            exact_replays[component_id] = reconstruct_exact_hazard_replay(
                candidate_payload=source["candidate"],
                event_mappings=events,
                eligible_session_days=source["eligible_session_days"],
                expected_hashes=expected_hashes,
            )

        access_ledger = self.root / "reports/data_access/data_access_ledger.jsonl"
        data_fingerprints = {
            "contract_map": _sha256(self.contract_map_path),
            **{
                f"feature_matrix_manifest:{path.parent.name}": _sha256(path)
                for path in sorted(self.cache_root.glob("*/manifest.json"))
            },
        }
        if not data_fingerprints or not access_ledger.is_file():
            raise CausalTargetVelocityRuntimeError(
                "terminal data/provenance fingerprints are incomplete"
            )
        receipt = finalize_causal_target_velocity_evidence_bundle(
            base_dir=self.root / "data/cache/evidence_bundles",
            lightweight_manifest_path=self.output_dir / "evidence_bundle_receipt.json",
            campaign_manifest=self.manifest,
            exact_replays=exact_replays,
            policies=policies,
            evaluated_policy_records=list(evaluated_records),
            data_fingerprints=data_fingerprints,
            provenance={
                "access_ledger_sha256": _sha256(access_ledger),
                "recorded_at_utc": _utc_now(),
                "market_data_role": "PRE_Q4_CACHED_DEVELOPMENT_ONLY",
                "immutable_checksums": {
                    "risk_frontier_preflight": str(preflight["result_hash"]),
                    "manifest": str(self.manifest["manifest_hash"]),
                    "stage1_event_evidence_inventory": str(
                        stage1_inventory["inventory_hash"]
                    ),
                },
            },
            compact_context={
                "terminal_reason": reason,
                "evidence_stage": evidence_stage,
                "risk_frontier_status": str(preflight["status"]),
                "clean_useful_sleeve_count": len(useful),
                "causal_book_count": len(stage4),
                "promoted_96_count": len(promoted),
                "xfa_deferred": True,
                "forward_deferred": True,
                "stage1_event_evidence_inventory": dict(stage1_inventory),
            },
        )
        verified = verify_evidence_bundle(receipt.bundle_path, deep=False)
        if verified.get("status") != "COMPLETE":
            raise CausalTargetVelocityRuntimeError(
                "terminal EvidenceBundle verification failed"
            )
        disposition = _terminal_disposition(
            self.stage2_rows,
            self.stage3_rows,
            useful=useful,
            promoted=promoted,
            reason=reason,
        )
        next_action = str(disposition["action"])
        stage_decisions = _terminal_stage_decisions(
            self.stage1_rows,
            self.stage2_rows,
            self.stage3_rows,
            stage4,
            useful=useful,
            promoted=promoted,
        )
        self._publish(
            state="FINALIZING",
            stage="EVIDENCE_BUNDLE_SEALED",
            evidence_bundle_manifest_sha256=receipt.manifest_sha256,
            clean_useful_sleeve_count=len(useful),
            causal_books_assembled=len(stage4),
            candidates_promoted_96=0,
            confirmation_ready_candidates=0,
            next_action=next_action,
        )
        receipt_payload = receipt.to_dict()
        terminal_kpis = self._kpis()
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=terminal_kpis,
            economic_results=_terminal_economic_summary(
                self.stage1_rows,
                self.stage2_rows,
                self.stage3_rows,
                stage4,
                useful=useful,
                promoted=promoted,
                reason=reason,
                kpis=terminal_kpis,
                stage1_inventory=stage1_inventory,
            ),
            successive_halving={
                "stage_decisions": stage_decisions,
                "stage1_input": len(self.stage1_rows),
                "stage2_exact_replays": _completed_count(
                    self.stage2_rows, "STAGE_2_COMPLETE"
                ),
                "stage3_rolling_policies": _completed_count(
                    self.stage3_rows, "STAGE_3_COMPLETE"
                ),
                "stage4_books": _completed_count(stage4, "STAGE_4_COMPLETE"),
                "stage5_promoted_96": 0,
                "full_coverage_shortfall_not_manufactured": True,
            },
            matched_controls=_terminal_control_summary(self.stage3_rows),
            failure_vectors=_terminal_failure_vectors(
                self.stage1_rows, self.stage2_rows, self.stage3_rows, stage4
            ),
            evidence_receipt=receipt_payload,
            autonomous_next_action={
                "action": next_action,
                "candidate_ids": [
                    str(row.get("book_id") or row.get("candidate_id"))
                    for row in (
                        promoted
                        or (useful if "COVERAGE_LIMITED" in next_action else ())
                    )
                ],
                "manifest_required": bool(disposition["manifest_required"]),
                "new_data_purchase_authorized": False,
                "q4_access_authorized": False,
            },
            scientific_status=str(disposition["scientific_status"]),
        )
        self.output_writer.write_json(
            str(self.manifest["runtime"]["result_name"]), result
        )
        self._publish(
            state="COMPLETE",
            stage="CAMPAIGN_COMPLETE",
            next_action=str(result["autonomous_next_action"]["action"]),
        )
        return result

    def _terminalize_without_clean_sleeves(
        self,
        *,
        preflight: Mapping[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        return self._finalize_campaign(
            preflight=preflight,
            useful=(),
            stage4=(),
            promoted=(),
            reason=reason,
        )

    def _initial_state(self) -> dict[str, Any]:
        path = self.output_dir / "production_state.json"
        if path.is_file():
            value = _read_json(path)
            _verify_snapshot(value, "state_hash", self.manifest)
            return value
        evidence_base = (
            self.root / str(self.manifest["evidence_bundle"]["destination"])
        )
        return {
            "schema": PRODUCTION_STATE_SCHEMA,
            "campaign_id": self.campaign_id,
            "manifest_hash": self.manifest["manifest_hash"],
            "source_commit": self.manifest["source_commit"],
            "state": "STARTING",
            "stage": "STARTING",
            "checkpoint_sequence": 0,
            "started_at_utc": _utc_now(),
            "updated_at_utc": _utc_now(),
            "runner_pid": os.getpid(),
            "worker_count": 3,
            "evidence_writer_count": 1,
            "policies_proposed": 0,
            "unique_policies_screened": 0,
            "exact_account_replays": 0,
            "combine_episodes_completed": 0,
            "next_action": "RUN_BOUNDED_RISK_FRONTIER",
            "evidence_staging_path": str(
                evidence_base / f".{self.campaign_id}.evidence-v1.staging"
            ),
            "evidence_final_path": str(
                evidence_base / f"{self.campaign_id}.evidence-v1"
            ),
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        }

    def _publish(self, **updates: Any) -> None:
        prior = dict(self.state)
        self.state.update(updates)
        for field in (
            "policies_proposed",
            "unique_policies_screened",
            "exact_account_replays",
            "combine_episodes_completed",
        ):
            if int(self.state.get(field, 0)) < int(prior.get(field, 0)):
                raise CausalTargetVelocityRuntimeError(
                    f"production counter moved backwards: {field}"
                )
        self.state.update(
            schema=PRODUCTION_STATE_SCHEMA,
            campaign_id=self.campaign_id,
            manifest_hash=self.manifest["manifest_hash"],
            source_commit=self.manifest["source_commit"],
            checkpoint_sequence=int(self.state.get("checkpoint_sequence", 0)) + 1,
            updated_at_utc=_utc_now(),
            runner_pid=os.getpid(),
            worker_count=3,
            evidence_writer_count=1,
            broker_connections=0,
            orders=0,
            q4_access_count_delta=0,
            data_purchase_count=0,
            economic_compute_seconds=float(self.hot_seconds),
            integrity_seconds=float(self.cold_seconds),
            reporting_seconds=float(self.reporting_seconds),
            campaign_runner_wall_seconds=(
                self.prior_runner_wall_seconds
                + max(time.perf_counter() - self.started_wall, 0.0)
            ),
        )
        self.state.pop("state_hash", None)
        self.state["state_hash"] = stable_hash(self.state)
        self.live_writer.write_json("production_state.json", self.state)
        self.live_writer.write_json("production_kpis.json", self._kpis())

    def _kpis(self) -> dict[str, Any]:
        elapsed_hours = max(
            (datetime.now(UTC) - _parse_datetime(self.state["started_at_utc"])).total_seconds()
            / 3_600.0,
            1e-9,
        )
        episodes = int(self.state.get("combine_episodes_completed", 0))
        exact = int(self.state.get("exact_account_replays", 0))
        economic = max(self.hot_seconds, 0.0)
        elapsed = max(
            self.prior_runner_wall_seconds
            + time.perf_counter()
            - self.started_wall,
            1e-9,
        )
        allocation = min(max(economic / elapsed, 0.0), 1.0)
        stage3 = [
            row
            for row in self.stage3_rows
            if row.get("status") == "STAGE_3_COMPLETE"
        ]
        normal_rates = [float(row["normal"]["pass_rate"]) for row in stage3]
        stressed_rates = [float(row["stressed"]["pass_rate"]) for row in stage3]
        value: dict[str, Any] = {
            "schema": PRODUCTION_KPI_SCHEMA,
            "campaign_id": self.campaign_id,
            "manifest_hash": self.manifest["manifest_hash"],
            "source_commit": self.manifest["source_commit"],
            "checkpoint_sequence": int(self.state["checkpoint_sequence"]),
            "updated_at_utc": _utc_now(),
            "state": self.state["state"],
            "policies_proposed": int(self.state.get("policies_proposed", 0)),
            "unique_policies_screened": int(
                self.state.get("unique_policies_screened", 0)
            ),
            "exact_account_replays": exact,
            "combine_episodes_completed": episodes,
            "normal_episodes_completed": episodes // 2,
            "stressed_episodes_completed": episodes // 2,
            "positive_stressed_net_candidates": sum(
                float(row["stressed"]["net_total"]) > 0.0 for row in stage3
            ),
            "candidates_with_normal_pass": sum(value > 0.0 for value in normal_rates),
            "candidates_with_stressed_pass": sum(
                value > 0.0 for value in stressed_rates
            ),
            "best_normal_pass_rate": max(normal_rates, default=0.0),
            "best_stressed_pass_rate": max(stressed_rates, default=0.0),
            "median_normal_pass_rate": (
                float(np.median(normal_rates)) if normal_rates else 0.0
            ),
            "median_stressed_pass_rate": (
                float(np.median(stressed_rates)) if stressed_rates else 0.0
            ),
            "near_pass_count": sum(
                int(row["normal"]["pass_count"]) == 0
                and float(row["normal"]["target_progress_median"]) >= 0.60
                for row in stage3
            ),
            "candidates_promoted_96": int(
                self.state.get("candidates_promoted_96", 0)
            ),
            "confirmation_ready_candidates": int(
                self.state.get("confirmation_ready_candidates", 0)
            ),
            "duplicate_rejection_rate": float(
                self.population_summary.get("duplicate_rejection_rate", 0.0)
            ),
            "cache_hit_rate": 1.0 if self.state.get("policies_proposed", 0) else 0.0,
            "cache_hit_measurement": "IMMUTABLE_FEATURE_MATRIX_INPUT_NO_FEATURE_RECOMPUTATION",
            "economic_research_wall_clock_fraction": allocation,
            "cpu_utilization_fraction": _system_cpu_fraction(
                self.cpu_ticks_started
            ),
            "cpu_utilization_measurement": "PROC_STAT_BUSY_DELTA_THIS_RUNNER_INVOCATION",
            "rates_per_hour": {
                "policies_proposed": int(self.state.get("policies_proposed", 0))
                / elapsed_hours,
                "unique_policies_screened": int(
                    self.state.get("unique_policies_screened", 0)
                )
                / elapsed_hours,
                "exact_account_replays": exact / elapsed_hours,
                "combine_episodes": episodes / elapsed_hours,
            },
            "workers": {"compute": 3, "evidence_writer": 1},
            "matched_controls_status": str(
                self.state.get("matched_controls_status", "STAGE1_MATCHED_RANDOM_ACTIVE")
            ),
            "null_status": str(
                self.state.get("null_status", "SESSION_MATCHED_RANDOM_NULL_ACTIVE")
            ),
            "admin_overhead_alert": bool(elapsed > 60.0 and allocation < 0.80),
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        }
        value["kpi_hash"] = stable_hash(value)
        return value

    def _load_stage1_rows(self) -> list[dict[str, Any]]:
        return _load_batches(self.payload_dir / "stage1_event_screen_batches")


def _risk_preflight_output_dir(
    root: Path,
    campaign_output_dir: Path,
    manifest: Mapping[str, Any],
) -> Path:
    """Reuse the byte-identical sealed preflight for the KPI-only revision."""

    repair = manifest.get("technical_repair")
    if not isinstance(repair, Mapping):
        return campaign_output_dir / "preflight"
    raw = str(repair.get("preserved_preflight_path") or "")
    preserved = (root / raw).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        repair.get("classification")
        != "TECHNICAL_STAGE3_KPI_INVALID_ROW_AGGREGATION_DEFECT"
        or preserved.name != "risk_frontier_preflight_result.json"
        or allowed not in preserved.parents
        or not preserved.is_file()
    ):
        raise CausalTargetVelocityRuntimeError(
            "technical revision preserved preflight is invalid"
        )
    return preserved.parent


def _initialize_worker(
    matrix_paths: Mapping[str, str],
    period: tuple[int, int, int],
    blocks: tuple[dict[str, Any], ...],
) -> None:
    global _WORKER_MATRICES, _WORKER_PERIOD, _WORKER_BLOCKS
    _WORKER_MATRICES = {
        market: FeatureMatrix.open(Path(path).parent, mmap=True)
        for market, path in matrix_paths.items()
    }
    _WORKER_PERIOD = tuple(int(value) for value in period)
    _WORKER_BLOCKS = tuple(dict(row) for row in blocks)


def _screen_candidate_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [_candidate_failure_boundary(_screen_candidate_worker, value, "STAGE_1") for value in payloads]


def _screen_candidate_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate, calibrated, matrix, intents, events = _evaluate_candidate(payload)
    block_map = _block_map(matrix, _WORKER_BLOCKS)
    eligible_days = _candidate_evaluation_days(candidate, matrix)
    screen = screen_result(
        calibrated,
        events,
        eligible_session_days=eligible_days,
        block_by_session_day=block_map,
    )
    seed = int(candidate.structural_fingerprint[:16], 16) % (2**32)
    assert _WORKER_PERIOD is not None
    _, evaluation_start, evaluation_end = _WORKER_PERIOD
    random_intents = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
        seed=seed,
    )
    random_events = observe_outcomes(calibrated, matrix, random_intents)
    random_screen = screen_result(
        calibrated,
        random_events,
        eligible_session_days=eligible_days,
        block_by_session_day=block_map,
    )
    return {
        "schema": "hydra_causal_target_velocity_stage1_row_v1",
        "candidate_id": candidate.candidate_id,
        "candidate": candidate.payload,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "behavioral_fingerprint": candidate.behavioral_fingerprint,
        "realized_behavioral_fingerprint": realized_behavioral_fingerprint(events),
        "calibration": asdict(calibrated),
        "screen": screen.to_dict(),
        "matched_random": random_screen.to_dict(),
        "matched_deltas": {
            "favorable_before_adverse_rate": (
                screen.favorable_before_adverse_rate
                - random_screen.favorable_before_adverse_rate
            ),
            "stressed_net_pnl": screen.stressed_net_pnl - random_screen.stressed_net_pnl,
            "cost_adjusted_target_velocity": (
                screen.cost_adjusted_target_velocity
                - random_screen.cost_adjusted_target_velocity
            ),
        },
        "event_contract": {
            "emitted": len(events),
            "completed": screen.completed_event_count,
            "censored_future_coverage": screen.censored_event_count,
            "future_coverage_suppressed_signals": 0,
        },
        "hard_causality_defect_count": 0,
        "_event_evidence": [
            _stage1_event_evidence_record(row.to_dict()) for row in events
        ],
        "row_hash": stable_hash(
            {
                "candidate": candidate.payload,
                "screen": screen.to_dict(),
                "matched_random": random_screen.to_dict(),
            }
        ),
    }


def _exact_candidate_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [_candidate_failure_boundary(_exact_candidate_worker, value, "STAGE_2") for value in payloads]


def _exact_candidate_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate, calibrated, matrix, _intents, events = _evaluate_candidate(payload)
    days = _candidate_evaluation_days(candidate, matrix)
    replay = exact_sleeve_replay(
        calibrated, events, eligible_session_days=days
    )
    if not days:
        raise CausalTargetVelocityRuntimeError("exact replay has no evaluation days")
    duration = len(days)
    policy = _standalone_hazard_policy(candidate.candidate_id)
    normal_episode = run_causal_shared_account_episode(
        {candidate.candidate_id: replay.normal_trajectories},
        days,
        policy=policy,
        start_day=days[0],
        maximum_duration_days=duration,
    )
    stressed_episode = run_causal_shared_account_episode(
        {candidate.candidate_id: replay.stressed_trajectories},
        days,
        policy=policy,
        start_day=days[0],
        maximum_duration_days=duration,
    )
    return {
        "schema": "hydra_causal_target_velocity_stage2_row_v1",
        "candidate_id": candidate.candidate_id,
        "candidate": candidate.payload,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "realized_behavioral_fingerprint": realized_behavioral_fingerprint(events),
        "calibration_fingerprint": calibrated.fingerprint,
        "completed_event_count": len(replay.normal_events),
        "censored_event_count": len(events) - len(replay.normal_events),
        "decision_hash": replay.decision_hash,
        "normal_event_hash": replay.normal_event_hash,
        "stressed_event_hash": replay.stressed_event_hash,
        "normal_trajectory_hash": replay.normal_trajectory_hash,
        "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        "fill_policy_hash": replay.fill_policy_hash,
        "eligible_session_days": list(days),
        "eligible_session_calendar_hash": stable_hash(list(days)),
        "normal": _compact_episode(normal_episode),
        "stressed": _compact_episode(stressed_episode),
        "exact_replay_scope": (
            "COMPLETED_EVENT_ECONOMIC_DIAGNOSTIC_DATA_CENSORED"
            if len(events) != len(replay.normal_events)
            else "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON"
        ),
        "coverage_status": (
            "DATA_CENSORED"
            if len(events) != len(replay.normal_events)
            else "FULL_COVERAGE"
        ),
        "full_coverage_start_count": int(len(events) == len(replay.normal_events)),
        "data_censored_start_count": int(len(events) != len(replay.normal_events)),
        "_event_evidence": [row.to_dict() for row in events],
        "_episode_evidence": [
            {
                "policy_id": policy.policy_id,
                "episode_id": f"{policy.policy_id}:{days[0]}",
                "scenario": scenario,
                "horizon": "FULL_CHRONOLOGICAL_HORIZON",
                "temporal_block": "MULTI_BLOCK_DIAGNOSTIC",
                "episode": episode.to_dict(include_paths=True),
            }
            for scenario, episode in (
                ("NORMAL", normal_episode),
                ("STRESSED_1_5X", stressed_episode),
            )
        ],
    }


def _rolling_candidate_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [_candidate_failure_boundary(_rolling_candidate_worker, value, "STAGE_3") for value in payloads]


def _account_book_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for payload in payloads:
        try:
            output.append({**_account_book_worker(payload), "status": "STAGE_4_COMPLETE"})
        except (KeyError, ValueError, FloatingPointError) as exc:
            output.append(
                {
                    "schema": "hydra_causal_target_velocity_book_failure_v1",
                    "book_id": str(payload["book_id"]),
                    "policy": dict(payload["policy"]),
                    "candidate_ids": list(payload["candidate_ids"]),
                    "status": "BOOK_INVALID_RECORDED",
                    "failure_type": type(exc).__name__,
                    "failure_reason": str(exc)[:500],
                }
            )
    return output


def _account_book_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    policy = ActiveRiskPoolPolicy.from_mapping(payload["policy"])
    candidates = tuple(
        HazardCandidate(**dict(value)) for value in payload["candidates"]
    )
    normal: dict[str, Sequence[Any]] = {}
    stressed: dict[str, Sequence[Any]] = {}
    all_events: list[Any] = []
    days_set: set[int] = set()
    block_map: dict[int, str] = {}
    for candidate in candidates:
        resolved, calibrated, matrix, _intents, events = _evaluate_candidate(
            candidate.payload
        )
        days = _candidate_evaluation_days(resolved, matrix)
        replay = exact_sleeve_replay(
            calibrated, events, eligible_session_days=days
        )
        normal[resolved.candidate_id] = replay.normal_trajectories
        stressed[resolved.candidate_id] = replay.stressed_trajectories
        days_set.update(days)
        block_map.update(_block_map(matrix, _WORKER_BLOCKS))
        all_events.extend(events)
    days = tuple(sorted(days_set))
    proposed = _block_aware_full_coverage_starts(
        days,
        block_map=block_map,
        horizon=90,
        maximum=48,
        preferred_starts=_clean_baseline_store()["starts"],
    )
    starts, censored_starts = _partition_starts_by_event_coverage(
        proposed, days=days, horizon=90, events=all_events
    )
    normal_episodes = tuple(
        run_causal_shared_account_episode(
            normal,
            days,
            policy=policy,
            start_day=start,
            maximum_duration_days=90,
        )
        for start in starts
    )
    stressed_episodes = tuple(
        run_causal_shared_account_episode(
            stressed,
            days,
            policy=policy,
            start_day=start,
            maximum_duration_days=90,
        )
        for start in starts
    )
    contribution = {
        "normal": _sum_component_contribution(normal_episodes),
        "stressed": _sum_component_contribution(stressed_episodes),
    }
    return {
        "schema": "hydra_causal_target_velocity_stage4_book_v1",
        "book_id": policy.policy_id,
        "policy": policy.to_dict(),
        "candidate_ids": list(policy.component_ids),
        "behavioral_cluster": str(payload["behavioral_cluster"]),
        "normal": _aggregate_episodes(normal_episodes),
        "stressed": _aggregate_episodes(stressed_episodes),
        "component_contribution": contribution,
        "maximum_stressed_component_profit_share": _maximum_positive_share(
            contribution["stressed"]
        ),
        "by_block": _block_local_event_economics(all_events, block_map),
        "coverage": {
            "headline_role": "CHRONOLOGICAL_90D_START_ORIGIN_BLOCK_NONOVERLAPPING",
            "window_may_cross_start_origin_block": True,
            "requested_start_count": 48,
            "candidate_full_horizon_start_count": len(proposed),
            "full_coverage_start_count": len(starts),
            "data_censored_start_count": len(censored_starts),
            "starts": list(starts),
            "data_censored_starts": list(censored_starts),
            "shortfall_reported_not_manufactured": max(0, 48 - len(starts)),
        },
        "matched_controls_complete": False,
        "matched_controls_status": (
            "DEFERRED_FULL_48_START_COVERAGE_NOT_AVAILABLE"
            if len(starts) < 48
            else "REQUIRED_BEFORE_PROMOTION"
        ),
        "_episode_evidence": [
            {
                "policy_id": policy.policy_id,
                "episode_id": f"{policy.policy_id}:{episode.start_day}",
                "scenario": scenario,
                "horizon": "90_TRADING_DAYS",
                "temporal_block": block_map.get(episode.start_day, "OUTSIDE_BLOCK"),
                "episode": episode.to_dict(include_paths=True),
            }
            for scenario, rows in (
                ("NORMAL", normal_episodes),
                ("STRESSED_1_5X", stressed_episodes),
            )
            for episode in rows
        ],
    }


def _rolling_candidate_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate, calibrated, matrix, intents, events = _evaluate_candidate(payload)
    days = _candidate_evaluation_days(candidate, matrix)
    replay = exact_sleeve_replay(
        calibrated, events, eligible_session_days=days
    )
    block_map = _block_map(matrix, _WORKER_BLOCKS)
    proposed_starts = _block_aware_full_coverage_starts(
        days,
        block_map=block_map,
        horizon=90,
        maximum=48,
        preferred_starts=_clean_baseline_store()["starts"],
    )
    starts, censored_starts = _partition_starts_by_event_coverage(
        proposed_starts,
        days=days,
        horizon=90,
        events=events,
    )
    policy = _standalone_hazard_policy(candidate.candidate_id)
    normal_episodes = _run_causal_episodes(
        replay.normal_trajectories,
        days=days,
        starts=starts,
        policy=policy,
    )
    stressed_episodes = _run_causal_episodes(
        replay.stressed_trajectories,
        days=days,
        starts=starts,
        policy=policy,
    )
    seed = int(candidate.structural_fingerprint[-16:], 16) % (2**32)
    assert _WORKER_PERIOD is not None
    _, evaluation_start, evaluation_end = _WORKER_PERIOD
    random_intents = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
        seed=seed,
    )
    random_events = observe_outcomes(calibrated, matrix, random_intents)
    random_replay = exact_sleeve_replay(
        calibrated,
        random_events,
        eligible_session_days=days,
    )
    session_null_intents = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
        seed=seed ^ 0x5E5510A7,
    )
    session_null_events = observe_outcomes(
        calibrated, matrix, session_null_intents
    )
    session_null_replay = exact_sleeve_replay(
        calibrated,
        session_null_events,
        eligible_session_days=days,
    )
    flipped_events = observe_outcomes(
        calibrated, matrix, direction_flipped_intents(intents)
    )
    flipped_replay = exact_sleeve_replay(
        calibrated,
        flipped_events,
        eligible_session_days=days,
    )
    control_audits = {
        "RANDOM_EVENT_TIMING_MATCHED": _matched_control_audit(
            candidate, events, random_events
        ),
        "SESSION_MATCHED_NULL": _matched_control_audit(
            candidate, events, session_null_events
        ),
        "DIRECTION_FLIPPED": _matched_control_audit(
            candidate, events, flipped_events
        ),
    }
    if not all(row["all_required_dimensions_matched"] for row in control_audits.values()):
        raise ValueError("serious matched-control dimension reconciliation failed")
    controls = {
        "RANDOM_EVENT_TIMING_MATCHED": _paired_control_summary(
            random_replay, days=days, starts=starts, policy=policy
        ),
        "SESSION_MATCHED_NULL": _paired_control_summary(
            session_null_replay, days=days, starts=starts, policy=policy
        ),
        "DIRECTION_FLIPPED": _paired_control_summary(
            flipped_replay, days=days, starts=starts, policy=policy
        ),
    }
    return {
        "schema": "hydra_causal_target_velocity_stage3_row_v1",
        "candidate_id": candidate.candidate_id,
        "candidate": candidate.payload,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "behavioral_cluster": realized_behavioral_fingerprint(events)[:16],
        "decision_hash": replay.decision_hash,
        "normal_event_hash": replay.normal_event_hash,
        "stressed_event_hash": replay.stressed_event_hash,
        "normal_trajectory_hash": replay.normal_trajectory_hash,
        "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        "fill_policy_hash": replay.fill_policy_hash,
        "normal": _aggregate_episodes(normal_episodes),
        "stressed": _aggregate_episodes(stressed_episodes),
        "episodes": {
            "normal": [row.to_dict() for row in normal_episodes],
            "stressed": [row.to_dict() for row in stressed_episodes],
        },
        "coverage": {
            "headline_role": "CHRONOLOGICAL_90D_START_ORIGIN_BLOCK_NONOVERLAPPING",
            "window_may_cross_start_origin_block": True,
            "full_coverage_start_count": len(starts),
            "candidate_full_horizon_start_count": len(proposed_starts),
            "requested_diagnostic_start_count": 48,
            "shortfall_reported_not_manufactured": max(0, 48 - len(starts)),
            "data_censored_start_count": len(censored_starts),
            "data_censored_starts": list(censored_starts),
            "starts": list(starts),
        },
        "by_block": _block_local_event_economics(events, block_map),
        "matched_controls": controls,
        "matched_control_audits": control_audits,
        "clean_low_velocity_baseline": _clean_low_velocity_baseline_comparison(
            candidate,
            normal_episodes=normal_episodes,
            stressed_episodes=stressed_episodes,
            starts=starts,
        ),
        "completed_event_count": len(replay.normal_events),
        "censored_event_count": len(events) - len(replay.normal_events),
        "eligible_session_days": list(days),
        "eligible_session_calendar_hash": stable_hash(list(days)),
        "event_evidence_hash": stable_hash([row.to_dict() for row in events]),
        "_event_evidence": [row.to_dict() for row in events],
        "_episode_evidence": [
            {
                "policy_id": policy.policy_id,
                "episode_id": f"{policy.policy_id}:{episode.start_day}",
                "scenario": scenario,
                "horizon": "90_TRADING_DAYS",
                "temporal_block": block_map.get(episode.start_day, "OUTSIDE_BLOCK"),
                "episode": episode.to_dict(include_paths=True),
            }
            for scenario, rows in (
                ("NORMAL", normal_episodes),
                ("STRESSED_1_5X", stressed_episodes),
            )
            for episode in rows
        ],
    }


def _evaluate_candidate(
    payload: Mapping[str, Any],
) -> tuple[Any, Any, FeatureMatrix, Any, Any]:
    if _WORKER_PERIOD is None:
        raise CausalTargetVelocityRuntimeError("worker period is not initialized")
    calibration_end, evaluation_start, evaluation_end = _WORKER_PERIOD
    candidate = HazardCandidate(**dict(payload))
    matrix = _WORKER_MATRICES[candidate.market]
    if candidate.mechanism == "CROSS_ASSET_STATE":
        reference_market = str(candidate.cross_asset_reference_market or "")
        if _CROSS_ASSET_REFERENCE_MARKETS.get(candidate.market) != reference_market:
            raise ValueError("cross-asset reference map escaped the frozen contract")
        reference = _WORKER_MATRICES.get(reference_market)
        if reference is None:
            raise ValueError("cross-asset reference matrix is unavailable")
        matrix = with_availability_safe_cross_asset_feature(matrix, reference)
    calibrated = calibrate_candidate(
        candidate,
        matrix,
        calibration_end_exclusive_ns=calibration_end,
    )
    intents = discover_intents_batch(
        calibrated,
        matrix,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
    )
    events = observe_outcomes(calibrated, matrix, intents)
    return candidate, calibrated, matrix, intents, events


def _candidate_failure_boundary(
    worker: Any,
    payload: Mapping[str, Any],
    stage: str,
) -> dict[str, Any]:
    """Convert expected candidate invalidity into evidence, not campaign death."""

    candidate = HazardCandidate(**dict(payload))
    try:
        row = worker(payload)
    except (KeyError, ValueError, FloatingPointError) as exc:
        return {
            "schema": "hydra_causal_target_velocity_candidate_failure_v1",
            "stage": stage,
            "candidate_id": candidate.candidate_id,
            "candidate": candidate.payload,
            "candidate_fingerprint": candidate.structural_fingerprint,
            "status": "CANDIDATE_INVALID_RECORDED",
            "failure_type": type(exc).__name__,
            "failure_reason": str(exc)[:500],
            "hard_causality_defect_count": int(
                "availability" in str(exc).lower()
                or "future" in str(exc).lower()
            ),
        }
    return {**row, "status": f"{stage}_COMPLETE"}


def _standalone_hazard_policy(component_id: str) -> ActiveRiskPoolPolicy:
    """Frozen one-sleeve governor used for exact causal MLL accounting."""

    return ActiveRiskPoolPolicy(
        policy_id=f"hazard-standalone:{component_id}",
        component_priority=(component_id,),
        nominal_risk_charge_per_mini=((component_id, 2_250.0),),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=4_500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4_500.0,
        daily_consistency_profit_guard=9_000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _run_causal_episodes(
    trajectories: Sequence[Any],
    *,
    days: Sequence[int],
    starts: Sequence[int],
    policy: ActiveRiskPoolPolicy,
    maximum_duration_days: int = 90,
) -> tuple[AccountPolicyEpisode, ...]:
    component_id = policy.component_ids[0]
    return tuple(
        run_causal_shared_account_episode(
            {component_id: trajectories},
            days,
            policy=policy,
            start_day=int(start),
            maximum_duration_days=maximum_duration_days,
        )
        for start in starts
    )


def _paired_control_summary(
    replay: Any,
    *,
    days: Sequence[int],
    starts: Sequence[int],
    policy: ActiveRiskPoolPolicy,
) -> dict[str, Any]:
    normal = _run_causal_episodes(
        replay.normal_trajectories,
        days=days,
        starts=starts,
        policy=policy,
    )
    stressed = _run_causal_episodes(
        replay.stressed_trajectories,
        days=days,
        starts=starts,
        policy=policy,
    )
    return {"normal": _aggregate_episodes(normal), "stressed": _aggregate_episodes(stressed)}


def _matched_control_audit(
    candidate: HazardCandidate,
    observed: Sequence[Any],
    control: Sequence[Any],
) -> dict[str, Any]:
    """Prove all preregistered matching dimensions for a serious control."""

    def cell_counts(rows: Sequence[Any]) -> dict[str, int]:
        return _counts(
            f"{row.session_day}:{row.session_code}" for row in rows
        )

    def average_exposure(rows: Sequence[Any]) -> float:
        return float(np.mean([row.quantity for row in rows])) if rows else 0.0

    observed_fill_hashes = sorted({str(row.fill_policy_hash) for row in observed})
    control_fill_hashes = sorted({str(row.fill_policy_hash) for row in control})
    checks = {
        "market": {row.market for row in observed} == {row.market for row in control},
        "session": cell_counts(observed) == cell_counts(control),
        "timeframe": all(row.timeframe == candidate.timeframe for row in (*observed, *control)),
        "opportunity_count": len(observed) == len(control),
        # The realized barrier time is an outcome and cannot be matched without
        # leaking the label.  The maximum holding-time contract is matched
        # exactly before outcomes instead.
        "active_duration": all(
            row.maximum_horizon == candidate.horizon for row in (*observed, *control)
        ),
        "average_exposure": math.isclose(
            average_exposure(observed), average_exposure(control), abs_tol=1e-12
        ),
        "cost_level": observed_fill_hashes == control_fill_hashes,
    }
    return {
        "schema": "hydra_causal_target_velocity_control_match_audit_v1",
        "required_dimensions": sorted(checks),
        "dimension_match": checks,
        "all_required_dimensions_matched": all(checks.values()),
        "active_duration_match_basis": "FROZEN_MAXIMUM_HOLDING_CONTRACT_NOT_OUTCOME_DURATION",
        "observed_opportunity_count": len(observed),
        "control_opportunity_count": len(control),
        "observed_average_quantity": average_exposure(observed),
        "control_average_quantity": average_exposure(control),
        "direction_relation": (
            "EXACTLY_FLIPPED"
            if len(observed) == len(control)
            and all(
                int(left.direction) == -int(right.direction)
                and int(left.session_day) == int(right.session_day)
                and int(left.session_code) == int(right.session_code)
                for left, right in zip(observed, control, strict=True)
            )
            else "PRESERVED_CELL_DISTRIBUTION_OR_RANDOM_TIMING"
        ),
    }


def _clean_baseline_store() -> dict[str, Any]:
    """Load the sealed clean 0027 standalone evidence once per worker."""

    global _CLEAN_BASELINE_CACHE
    if _CLEAN_BASELINE_CACHE is not None:
        return _CLEAN_BASELINE_CACHE
    root = Path(__file__).resolve().parents[2]
    baseline_path = (
        root
        / "reports/economic_evolution/causal_target_velocity_0028_baseline_freeze.json"
    )
    phase_path = (
        root
        / "reports/economic_evolution/causal_salvage_sprint_0027_revision_02/phase_a_results.json"
    )
    operating_path = root / "reports/operating/hydra_operating_package_v1/OPERATING_PACKAGE_V1.json"
    baseline = _read_json(baseline_path)
    phase = _read_json(phase_path)
    expected_phase_sha = str(
        baseline["source_artifacts"]["phase_a_results"]["file_sha256"]
    )
    if _sha256(phase_path) != expected_phase_sha:
        raise CausalTargetVelocityRuntimeError("clean 0027 phase-A baseline hash drift")
    positive = {str(value) for value in baseline["positive_component_ids"]}
    inventory = {
        str(row["component_id"]): dict(row["sleeve_specification"])
        for row in _read_json(operating_path)["canonical_sleeve_inventory"]["members"]
        if str(row["component_id"]) in positive
    }
    if set(inventory) != positive:
        raise CausalTargetVelocityRuntimeError("clean baseline specification inventory drift")
    # Recompile once from the immutable 0027 bindings.  The sealed episode
    # summaries do not cover all 0028 non-overlapping starts; reusing them as a
    # one-start scalar would not be a decision-grade control.
    from hydra.production.causal_salvage_runtime import (
        _compile_causal_sleeves,
        _load_frozen_packages,
        _open_frozen_matrices,
    )

    salvage_manifest = _read_json(root / "config/v7/causal_salvage_sprint_0027.json")
    reconstructed, _receipt = _load_frozen_packages(root, salvage_manifest)
    specs = dict(reconstructed[0].sleeve_specs)
    bindings = dict(reconstructed[0].frozen_signal_bindings)
    matrices = _open_frozen_matrices(root, bindings)
    replays, failures = _compile_causal_sleeves(
        specs,
        bindings,
        matrices,
        start_inclusive=str(salvage_manifest["economic_contract"]["development_start"]),
        end_exclusive=str(
            salvage_manifest["economic_contract"]["development_end_exclusive"]
        ),
    )
    if failures or not positive.issubset(replays):
        raise CausalTargetVelocityRuntimeError(
            "clean 0027 causal baseline replay reconstruction failed"
        )
    starts = tuple(int(value) for value in phase["starts"]["epoch_days"])
    _CLEAN_BASELINE_CACHE = {
        "specs": inventory,
        "replays": {key: replays[key] for key in sorted(positive)},
        "starts": starts,
        "baseline_sha256": _sha256(baseline_path),
        "phase_a_sha256": expected_phase_sha,
        "evidence_role": "SEALED_CLEAN_CAUSAL_0027_STANDALONE_REPLAY",
    }
    return _CLEAN_BASELINE_CACHE


def _clean_low_velocity_baseline_comparison(
    candidate: HazardCandidate,
    *,
    normal_episodes: Sequence[AccountPolicyEpisode],
    stressed_episodes: Sequence[AccountPolicyEpisode],
    starts: Sequence[int],
) -> dict[str, Any]:
    """Choose the nearest clean sleeve and reuse its identical-start replay."""

    store = _clean_baseline_store()
    candidate_accepted = sum(row.accepted_events for row in normal_episodes)

    def spec_score(item: tuple[str, Mapping[str, Any]]) -> tuple[Any, ...]:
        sleeve_id, spec = item
        timeframe_distance = abs(
            int(str(spec["timeframe"]).removesuffix("m"))
            - int(candidate.timeframe.removesuffix("m"))
        )
        replay = store["replays"][sleeve_id]
        baseline_accepted = sum(
            int(start) <= row.event.session_day
            for start in starts
            for row in replay.normal_trajectories
            if row.event.session_day < int(start) + 130
        )
        return (
            int(str(spec["market"]) != candidate.market),
            int(int(spec["session_code"]) != candidate.session_code),
            timeframe_distance,
            abs(baseline_accepted - candidate_accepted),
            sleeve_id,
        )

    sleeve_id, spec = min(store["specs"].items(), key=spec_score)
    replay = store["replays"][sleeve_id]
    baseline_policy = _standalone_hazard_policy(sleeve_id)
    baseline_days = tuple(int(value) for value in replay.eligible_session_days)
    eligible_starts = tuple(int(value) for value in starts if int(value) in set(baseline_days))
    missing = sorted(set(int(value) for value in starts) - set(eligible_starts))
    matched_normal = _run_causal_episodes(
        replay.normal_trajectories,
        days=baseline_days,
        starts=eligible_starts,
        policy=baseline_policy,
    )
    matched_stressed = _run_causal_episodes(
        replay.stressed_trajectories,
        days=baseline_days,
        starts=eligible_starts,
        policy=baseline_policy,
    )
    normal_summary = _aggregate_episodes(matched_normal)
    stressed_summary = _aggregate_episodes(matched_stressed)
    candidate_normal = _aggregate_episodes(normal_episodes)
    candidate_stressed = _aggregate_episodes(stressed_episodes)
    baseline_accepted = sum(row.accepted_events for row in matched_normal)
    candidate_duration = (
        float(np.mean([row.eligible_days for row in normal_episodes]))
        if normal_episodes
        else 0.0
    )
    baseline_duration = (
        float(np.mean([row.eligible_days for row in matched_normal]))
        if matched_normal
        else 0.0
    )
    candidate_exposure = (
        float(np.mean([row.maximum_mini_equivalent for row in normal_episodes]))
        if normal_episodes
        else 0.0
    )
    baseline_exposure = (
        float(np.mean([row.maximum_mini_equivalent for row in matched_normal]))
        if matched_normal
        else 0.0
    )
    dimensions = {
        "market": str(spec["market"]) == candidate.market,
        "session": int(spec["session_code"]) == candidate.session_code,
        "timeframe": str(spec["timeframe"]) == candidate.timeframe,
        "opportunity_count": baseline_accepted == candidate_accepted,
        "active_duration": math.isclose(candidate_duration, baseline_duration, abs_tol=1e-12),
        "average_exposure": math.isclose(candidate_exposure, baseline_exposure, abs_tol=1e-12),
        "cost_level": True,
    }
    exact_cell = all(
        dimensions[field] for field in ("market", "session", "timeframe")
    )
    return {
        "schema": "hydra_clean_low_velocity_paired_baseline_v1",
        "source_role": store["evidence_role"],
        "comparison_role": (
            "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
        ),
        "source_baseline_sha256": store["baseline_sha256"],
        "source_phase_a_sha256": store["phase_a_sha256"],
        "baseline_sleeve_id": sleeve_id,
        "baseline_specification": {
            "market": spec["market"],
            "session_code": spec["session_code"],
            "timeframe": spec["timeframe"],
        },
        "identical_start_count": len(matched_normal),
        "required_identical_start_count": len(starts),
        "all_identical_starts_replayed": len(matched_normal) == len(starts),
        "missing_identical_starts": missing,
        "clean_baseline_exact_cell": exact_cell,
        "normal": normal_summary,
        "stressed": stressed_summary,
        "paired_deltas": {
            "normal_net_total": candidate_normal["net_total"] - normal_summary["net_total"],
            "stressed_net_total": candidate_stressed["net_total"] - stressed_summary["net_total"],
            "normal_target_progress_median": candidate_normal["target_progress_median"] - normal_summary["target_progress_median"],
            "stressed_target_progress_median": candidate_stressed["target_progress_median"] - stressed_summary["target_progress_median"],
        },
        "match_audit": {
            "dimension_match": dimensions,
            "all_seven_dimensions_exact": all(dimensions.values()),
            "opportunity_count_delta": candidate_accepted - baseline_accepted,
            "active_duration_mean_delta": candidate_duration - baseline_duration,
            "maximum_exposure_proxy_mean_delta": candidate_exposure - baseline_exposure,
            "exposure_limitation": "SEALED_0027_EPISODES_PERSIST_MAXIMUM_NOT_TIME_AVERAGE_EXPOSURE",
            "comparison_is_matched_null": False,
            "nearest_baseline_limitations_explicit": True,
        },
        "promotion_status_inherited": False,
    }


def _aggregate_episodes(
    rows: Sequence[CombineEpisodeResult | AccountPolicyEpisode],
) -> dict[str, Any]:
    count = len(rows)
    if not count:
        return {
            "episode_count": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "net_total": 0.0,
            "net_median": 0.0,
            "target_progress_p25": 0.0,
            "target_progress_median": 0.0,
            "minimum_mll_buffer": 4_500.0,
            "consistency_rate": 0.0,
            "median_days_to_target": None,
        }
    progress = [float(row.target_progress) for row in rows]
    passing_days = [float(row.days_to_target) for row in rows if row.days_to_target]
    return {
        "episode_count": count,
        "pass_count": sum(row.passed for row in rows),
        "pass_rate": sum(row.passed for row in rows) / count,
        "mll_breach_count": sum(row.mll_breached for row in rows),
        "mll_breach_rate": sum(row.mll_breached for row in rows) / count,
        "net_total": float(sum(row.net_pnl for row in rows)),
        "net_median": float(np.median([row.net_pnl for row in rows])),
        "target_progress_p25": float(np.percentile(progress, 25)),
        "target_progress_median": float(np.median(progress)),
        "minimum_mll_buffer": float(min(row.minimum_mll_buffer for row in rows)),
        "consistency_rate": sum(row.consistency_ok for row in rows) / count,
        "median_days_to_target": (
            float(np.median(passing_days)) if passing_days else None
        ),
    }


def _compact_episode(row: CombineEpisodeResult | AccountPolicyEpisode) -> dict[str, Any]:
    return {
        "terminal": row.terminal.value,
        "net_pnl": float(row.net_pnl),
        "target_progress": float(row.target_progress),
        "minimum_mll_buffer": float(row.minimum_mll_buffer),
        "mll_breached": bool(row.mll_breached),
        "consistency_ok": bool(row.consistency_ok),
        "days_to_target": row.days_to_target,
        "eligible_days": int(row.eligible_days),
        "event_count": int(
            getattr(
                row,
                "event_count",
                int(getattr(row, "accepted_events", 0))
                + int(getattr(row, "skipped_events", 0)),
            )
        ),
        "account_replay": (
            "CAUSAL_PER_BAR_SHARED_ACCOUNT"
            if isinstance(row, AccountPolicyEpisode)
            else "LEGACY_EVENT_SUMMARY"
        ),
    }


def _select_stage1(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("status") == "STAGE_1_COMPLETE"
        and int(row["hard_causality_defect_count"]) == 0
        and int(row["screen"]["completed_event_count"]) > 0
        and float(row["screen"]["normal_net_pnl"]) > 0.0
        and float(row["screen"]["stressed_net_pnl"]) > 0.0
        and float(row["screen"]["independent_events_per_20_sessions"]) > 0.0
    ]
    ordered = sorted(
        eligible,
        key=lambda row: (
            -int(float(row["matched_deltas"]["stressed_net_pnl"]) > 0.0),
            -int(
                float(row["matched_deltas"]["favorable_before_adverse_rate"])
                > 0.0
            ),
            -float(row["screen"]["cost_adjusted_target_velocity"]),
            -float(row["screen"]["favorable_before_adverse_rate"]),
            -float(row["screen"]["independent_events_per_20_sessions"]),
            float(row["screen"]["day_concentration"]),
            str(row["candidate_id"]),
        ),
    )
    selected = _one_per_behavior_then_fill(ordered, maximum)
    if not selected:
        diagnostics = sorted(
            (
                row
                for row in rows
                if row.get("status") == "STAGE_1_COMPLETE"
                and int(row.get("hard_causality_defect_count", 0)) == 0
                and int((row.get("screen") or {}).get("completed_event_count", 0)) > 0
            ),
            key=lambda row: (
                -float(row["screen"]["cost_adjusted_target_velocity"]),
                -float(row["screen"]["stressed_net_pnl"]),
                str(row["candidate_id"]),
            ),
        )
        if diagnostics:
            diagnostic = dict(diagnostics[0])
            diagnostic["selection_role"] = "EVIDENCE_ONLY_DIAGNOSTIC_EXACT_REPLAY"
            diagnostic["economic_advancement_authorized"] = False
            selected = [diagnostic]
    return selected


def _select_stage2(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("status") == "STAGE_2_COMPLETE"
        and bool(row.get("economic_advancement_authorized", True))
        and float(row["normal"]["net_pnl"]) > 0.0
        and float(row["stressed"]["net_pnl"]) > 0.0
        and not bool(row["normal"]["mll_breached"])
        and not bool(row["stressed"]["mll_breached"])
    ]
    ordered = sorted(
        eligible,
        key=lambda row: (
            -float(row["stressed"]["target_progress"]),
            -float(row["stressed"]["net_pnl"]),
            -float(row["stressed"]["minimum_mll_buffer"]),
            str(row["candidate_id"]),
        ),
    )
    selected = _one_per_behavior_then_fill(ordered, maximum)
    return selected


def _select_stage3_useful(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[dict[str, Any]]:
    eligible: list[Mapping[str, Any]] = []
    for row in rows:
        if row.get("status") != "STAGE_3_COMPLETE":
            continue
        normal = row["normal"]
        stressed = row["stressed"]
        if int(row.get("coverage", {}).get("full_coverage_start_count", 0)) < 1:
            continue
        clean_baseline = row.get("clean_low_velocity_baseline") or {}
        if not bool(clean_baseline.get("all_identical_starts_replayed")):
            continue
        baseline_audit = clean_baseline.get("match_audit") or {}
        if (
            clean_baseline.get("source_role")
            != "SEALED_CLEAN_CAUSAL_0027_STANDALONE_REPLAY"
            or clean_baseline.get("comparison_role")
            != "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
            or baseline_audit.get("comparison_is_matched_null") is not False
            or baseline_audit.get("nearest_baseline_limitations_explicit") is not True
        ):
            continue
        if float(
            (clean_baseline.get("paired_deltas") or {}).get(
                "stressed_target_progress_median", -math.inf
            )
        ) <= 0.0:
            continue
        matched_audits = row.get("matched_control_audits") or {}
        if set(matched_audits) != {
            "RANDOM_EVENT_TIMING_MATCHED",
            "SESSION_MATCHED_NULL",
            "DIRECTION_FLIPPED",
        }:
            continue
        if not all(
            bool(value.get("all_required_dimensions_matched"))
            for value in matched_audits.values()
        ):
            continue
        positive_blocks = sum(
            float(value["stressed_net_total"]) > 0.0
            for value in row["by_block"].values()
        )
        baseline_velocity = (
            float(clean_baseline["stressed"]["target_progress_median"]) / 90.0
        )
        observed_velocity = float(stressed["target_progress_median"]) / 90.0
        if (
            float(normal["net_total"]) > 0.0
            and float(stressed["net_total"]) > 0.0
            and float(stressed["mll_breach_rate"]) <= 0.10
            and observed_velocity > baseline_velocity
            and positive_blocks >= 2
        ):
            eligible.append(row)
    ordered = sorted(
        eligible,
        key=lambda row: (
            -int(row["stressed"]["pass_count"]),
            -int(row["normal"]["pass_count"]),
            -float(row["stressed"]["target_progress_median"]),
            -float(row["stressed"]["minimum_mll_buffer"]),
            str(row["candidate_id"]),
        ),
    )
    return _one_per_behavior_then_fill(ordered, maximum)


def _generate_stage4_books(
    useful: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[dict[str, Any]]:
    representatives = sorted(
        useful,
        key=lambda row: (
            -int(row["stressed"]["pass_count"]),
            -float(row["stressed"]["target_progress_median"]),
            str(row["candidate_id"]),
        ),
    )[:12]
    candidates = {
        str(row["candidate_id"]): dict(row["candidate"])
        for row in representatives
    }
    combinations: list[tuple[str, ...]] = []
    ids = tuple(candidates)
    for size in (2, 3, 4):
        combinations.extend(itertools.combinations(ids, size))
    combinations.sort(
        key=lambda members: (
            -len({str(candidates[value]["market"]) for value in members}),
            -len({str(candidates[value]["mechanism"]) for value in members}),
            members,
        )
    )
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for members in combinations:
        policy = ActiveRiskPoolPolicy(
            policy_id="hazard_book_"
            + stable_hash(
                {
                    "members": members,
                    "governor": "CAUSAL_ACTIVE_POOL_IDENTITY_V1",
                }
            )[:24],
            component_priority=tuple(members),
            nominal_risk_charge_per_mini=tuple(
                (value, 2_250.0) for value in members
            ),
            maximum_concurrent_sleeves=min(3, len(members)),
            aggregate_open_risk_ceiling=4_500.0,
            maximum_mll_buffer_fraction=1.0,
            protected_mll_buffer=0.0,
            maximum_mini_equivalent=15.0,
            concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
            same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
            daily_loss_guard=4_500.0,
            daily_consistency_profit_guard=9_000.0,
            target_protection_distance=0.0,
            target_protection_mode=TargetProtectionMode.NONE,
            static_risk_tier=1.0,
        )
        fingerprint = policy.structural_fingerprint
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        output.append(
            {
                "book_id": policy.policy_id,
                "policy": policy.to_dict(),
                "candidate_ids": list(members),
                "candidates": [candidates[value] for value in members],
                "behavioral_cluster": stable_hash(
                    sorted(
                        str(
                            next(
                                row["behavioral_cluster"]
                                for row in representatives
                                if row["candidate_id"] == value
                            )
                        )
                        for value in members
                    )
                )[:16],
            }
        )
        if len(output) >= maximum:
            break
    return output


def _select_stage4_for_expansion(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[dict[str, Any]]:
    eligible = []
    for row in rows:
        if row.get("status") != "STAGE_4_COMPLETE":
            continue
        normal = row["normal"]
        stressed = row["stressed"]
        positive_blocks = sum(
            float(value["stressed_net_total"]) > 0.0
            or float(value["stressed_target_progress"]) >= 0.60
            for value in row["by_block"].values()
        )
        if (
            int(row["coverage"]["full_coverage_start_count"]) >= 48
            and bool(row.get("matched_controls_complete"))
            and int(normal["pass_count"]) >= 3
            and int(stressed["pass_count"]) >= 2
            and float(stressed["net_total"]) > 0.0
            and float(stressed["mll_breach_rate"]) <= 0.10
            and positive_blocks >= 2
            and float(row["maximum_stressed_component_profit_share"]) <= 0.75
        ):
            eligible.append(row)
    ordered = sorted(
        eligible,
        key=lambda row: (
            -int(row["stressed"]["pass_count"]),
            -int(row["normal"]["pass_count"]),
            -float(row["stressed"]["target_progress_median"]),
            -float(row["stressed"]["minimum_mll_buffer"]),
            str(row["book_id"]),
        ),
    )
    return _one_per_behavior_then_fill(ordered, maximum)


def _sum_component_contribution(
    rows: Sequence[AccountPolicyEpisode],
) -> dict[str, float]:
    output: dict[str, float] = {}
    for row in rows:
        for component_id, value in row.component_contribution.items():
            output[component_id] = output.get(component_id, 0.0) + float(value)
    return dict(sorted(output.items()))


def _maximum_positive_share(values: Mapping[str, float]) -> float:
    positive = [max(0.0, float(value)) for value in values.values()]
    total = sum(positive)
    return max(positive, default=0.0) / total if total > 0.0 else 0.0


def _stage4_episode_count(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(
        int(row["normal"]["episode_count"])
        + int(row["stressed"]["episode_count"])
        for row in rows
        if row.get("status") == "STAGE_4_COMPLETE"
    )


def _terminal_disposition(
    stage2: Sequence[Mapping[str, Any]],
    stage3: Sequence[Mapping[str, Any]],
    *,
    useful: Sequence[Mapping[str, Any]],
    promoted: Sequence[Mapping[str, Any]],
    reason: str,
) -> dict[str, Any]:
    if promoted:
        return {
            "action": "QUEUE_FAILURE_GUIDED_TARGETED_MUTATION_MANIFEST",
            "manifest_required": True,
            "scientific_status": "CAUSAL_TARGET_VELOCITY_DEVELOPMENT_SURVIVORS",
        }
    if useful:
        return {
            "action": "PRESERVE_CLEAN_CAUSAL_SLEEVES_COVERAGE_LIMITED",
            "manifest_required": False,
            "scientific_status": (
                "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_COVERAGE_LIMITED"
            ),
        }
    completed3 = [row for row in stage3 if row.get("status") == "STAGE_3_COMPLETE"]
    required_controls = {
        "RANDOM_EVENT_TIMING_MATCHED",
        "SESSION_MATCHED_NULL",
        "DIRECTION_FLIPPED",
    }
    decision_grade = bool(completed3) and all(
        int((row.get("coverage") or {}).get("full_coverage_start_count", 0)) >= 1
        and set(row.get("matched_control_audits") or {}) == required_controls
        and all(
            bool(value.get("all_required_dimensions_matched"))
            for value in (row.get("matched_control_audits") or {}).values()
        )
        for row in completed3
    )
    exact_stage2_falsification = reason in {
        "NO_EXACT_CAUSAL_SLEEVE_SURVIVED_STAGE_2",
        "SYNTHETIC_NO_SURVIVOR",
    } and any(row.get("status") == "STAGE_2_COMPLETE" for row in stage2)
    if decision_grade or exact_stage2_falsification:
        return {
            "action": "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST",
            "manifest_required": True,
            "scientific_status": "CAUSAL_TARGET_VELOCITY_FALSIFIED",
        }
    return {
        "action": "HOLD_CAUSAL_CAMPAIGN_INTEGRITY_OR_COVERAGE_BLOCK",
        "manifest_required": False,
        "scientific_status": "CAUSAL_TARGET_VELOCITY_INCONCLUSIVE_EVIDENCE_GAP",
    }


def _terminal_stage_decisions(
    stage1: Sequence[Mapping[str, Any]],
    stage2: Sequence[Mapping[str, Any]],
    stage3: Sequence[Mapping[str, Any]],
    stage4: Sequence[Mapping[str, Any]],
    *,
    useful: Sequence[Mapping[str, Any]],
    promoted: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    completed2 = [row for row in stage2 if row.get("status") == "STAGE_2_COMPLETE"]
    completed3 = [row for row in stage3 if row.get("status") == "STAGE_3_COMPLETE"]
    completed4 = [row for row in stage4 if row.get("status") == "STAGE_4_COMPLETE"]

    def decision(
        stage: str,
        input_count: int,
        selected: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return {
            "stage": stage,
            "input_count": int(input_count),
            "output_count": len(selected),
            "selected_policy_ids": [
                str(row.get("book_id") or row.get("candidate_id"))
                for row in selected
            ],
            "thresholds_changed_after_outcomes": False,
            "opaque_score_used": False,
        }

    return [
        decision("STAGE_1_EVENT_SCREEN", len(stage1), completed2),
        decision("STAGE_2_EXACT_SLEEVE_REPLAY", len(completed2), completed3),
        decision("STAGE_3_ROLLING_COMBINE", len(completed3), useful),
        decision("STAGE_4_ACCOUNT_ASSEMBLY", len(completed4), promoted),
    ]


def _terminal_economic_summary(
    stage1: Sequence[Mapping[str, Any]],
    stage2: Sequence[Mapping[str, Any]],
    stage3: Sequence[Mapping[str, Any]],
    stage4: Sequence[Mapping[str, Any]],
    *,
    useful: Sequence[Mapping[str, Any]],
    promoted: Sequence[Mapping[str, Any]],
    reason: str,
    kpis: Mapping[str, Any] | None = None,
    stage1_inventory: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    successful3 = [row for row in stage3 if row.get("status") == "STAGE_3_COMPLETE"]
    successful4 = [row for row in stage4 if row.get("status") == "STAGE_4_COMPLETE"]
    successful2 = [row for row in stage2 if row.get("status") == "STAGE_2_COMPLETE"]
    population = successful4 or successful3 or successful2

    def scenario_value(
        row: Mapping[str, Any], scenario: str, aggregate: str, scalar: str
    ) -> float:
        values = row[scenario]
        return float(values.get(aggregate, values.get(scalar, 0.0)))

    def episode_count(row: Mapping[str, Any], scenario: str) -> int:
        values = row[scenario]
        if "episode_count" in values:
            return int(values["episode_count"])
        coverage = int(
            (row.get("coverage") or {}).get("full_coverage_start_count", 0)
        )
        return coverage if coverage > 0 else 1

    def pass_count(row: Mapping[str, Any], scenario: str) -> int:
        values = row[scenario]
        if "pass_count" in values:
            return int(values["pass_count"])
        return int(values.get("terminal") == "TARGET_REACHED")

    normal_episode_count = sum(episode_count(row, "normal") for row in population)
    stressed_episode_count = sum(
        episode_count(row, "stressed") for row in population
    )
    normal_rates = [
        pass_count(row, "normal") / max(episode_count(row, "normal"), 1)
        for row in population
    ]
    stressed_rates = [
        pass_count(row, "stressed") / max(episode_count(row, "stressed"), 1)
        for row in population
    ]
    stressed_progress = [
        max(scenario_value(row, "stressed", "target_progress_median", "target_progress"), 0.0)
        for row in population
    ]
    stressed_mll_rates = [
        scenario_value(row, "stressed", "mll_breach_rate", "mll_breached")
        for row in population
    ]
    positive_stressed = sum(
        scenario_value(row, "stressed", "net_total", "net_pnl") > 0.0
        for row in population
    )
    terminal_kpis = dict(kpis or {})
    rates = dict(terminal_kpis.get("rates_per_hour") or {})
    for field in (
        "policies_proposed",
        "unique_policies_screened",
        "exact_account_replays",
        "combine_episodes",
    ):
        rates.setdefault(field, 0.0)
    production_kpis = {
        "rates_per_hour": rates,
        "economic_research_wall_clock_fraction": float(
            terminal_kpis.get("economic_research_wall_clock_fraction", 0.0)
        ),
        "cpu_utilization_fraction": float(
            terminal_kpis.get("cpu_utilization_fraction", 0.0)
        ),
        "workers": dict(
            terminal_kpis.get("workers")
            or {"compute": 3, "evidence_writer": 1}
        ),
        "duplicate_rejection_rate": float(
            terminal_kpis.get("duplicate_rejection_rate", 0.0)
        ),
        "cache_hit_rate": float(terminal_kpis.get("cache_hit_rate", 1.0)),
    }
    frontier = {
        "candidate_count": len(population),
        "positive_stressed_net_count": positive_stressed,
        "normal_pass_fraction_best": max(normal_rates) if normal_rates else None,
        "normal_pass_fraction_median": (
            float(np.median(normal_rates)) if normal_rates else None
        ),
        "stressed_pass_fraction_best": (
            max(stressed_rates) if stressed_rates else None
        ),
        "stressed_pass_fraction_median": (
            float(np.median(stressed_rates)) if stressed_rates else None
        ),
        "stressed_target_progress_median_best": (
            max(stressed_progress) if stressed_progress else None
        ),
        "stressed_target_progress_median_population": (
            float(np.median(stressed_progress)) if stressed_progress else None
        ),
        "stressed_mll_breach_rate_minimum": (
            min(stressed_mll_rates) if stressed_mll_rates else None
        ),
        "stressed_mll_breach_rate_maximum": (
            max(stressed_mll_rates) if stressed_mll_rates else None
        ),
    }
    promoted_ids = [
        str(row.get("book_id") or row.get("candidate_id")) for row in promoted
    ]
    return {
        "schema": "hydra_causal_target_velocity_campaign_summary_v1",
        "campaign_id": "hydra_causal_target_velocity_0028",
        "terminal_reason": reason,
        "proposals_generated": 20_000,
        "unique_event_screens": _completed_count(stage1, "STAGE_1_COMPLETE"),
        "exact_sleeve_replays": _completed_count(stage2, "STAGE_2_COMPLETE"),
        "rolling_combine_policies": len(successful3),
        "causal_active_pool_books": len(successful4),
        "normal_combine_passes": sum(pass_count(row, "normal") for row in population),
        "stressed_combine_passes": sum(pass_count(row, "stressed") for row in population),
        "best_normal_pass_rate": max(normal_rates, default=0.0),
        "best_stressed_pass_rate": max(stressed_rates, default=0.0),
        "median_normal_pass_rate": float(np.median(normal_rates)) if normal_rates else 0.0,
        "median_stressed_pass_rate": float(np.median(stressed_rates)) if stressed_rates else 0.0,
        "best_stressed_target_progress_median": max(
            stressed_progress, default=0.0
        ),
        "minimum_stressed_mll_buffer": min(
            (float(row["stressed"]["minimum_mll_buffer"]) for row in population),
            default=4_500.0,
        ),
        "full_coverage_start_count_total": sum(
            int((row.get("coverage") or {}).get("full_coverage_start_count", 0))
            for row in population
        ),
        "data_censored_start_count_total": sum(
            int((row.get("coverage") or {}).get("data_censored_start_count", 0))
            for row in population
        ),
        "clean_useful_sleeve_ids": [str(row["candidate_id"]) for row in useful],
        "promoted_96_policy_ids": [
            str(row.get("book_id") or row.get("candidate_id")) for row in promoted
        ],
        "candidate_count": len(population),
        "normal_pass_candidate_count": sum(value > 0.0 for value in normal_rates),
        "stressed_pass_candidate_count": sum(value > 0.0 for value in stressed_rates),
        "positive_stressed_net_count": positive_stressed,
        "confirmation_ready_candidate_ids": [],
        "stage5_96_start_candidate_ids": promoted_ids,
        "development_finalist_ids": [],
        "production_counters": {
            "serious_exact_account_replays": len(successful2),
            "predeclared_control_policy_replays": 3 * len(successful3),
            "combine_episodes_completed": normal_episode_count
            + stressed_episode_count,
            "normal_episodes_completed": normal_episode_count,
            "stressed_episodes_completed": stressed_episode_count,
        },
        "production_kpis": production_kpis,
        "economic_frontier": frontier,
        "target_progress_frontier": {
            "stressed_target_progress_median_best": frontier[
                "stressed_target_progress_median_best"
            ],
            "stressed_target_progress_median_population": frontier[
                "stressed_target_progress_median_population"
            ],
        },
        "mll_frontier": {
            "stressed_mll_breach_rate_minimum": frontier[
                "stressed_mll_breach_rate_minimum"
            ],
            "stressed_mll_breach_rate_maximum": frontier[
                "stressed_mll_breach_rate_maximum"
            ],
        },
        "stage1_event_evidence_inventory": dict(stage1_inventory or {}),
        "xfa_paths_started": 0,
        "development_only": True,
        "independently_confirmed": False,
    }


def _terminal_control_summary(
    stage3: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [row for row in stage3 if row.get("status") == "STAGE_3_COMPLETE"]
    required = {
        "RANDOM_EVENT_TIMING_MATCHED",
        "SESSION_MATCHED_NULL",
        "DIRECTION_FLIPPED",
    }
    return {
        "serious_candidate_count": len(rows),
        "strict_matched_control_names": sorted(required),
        "strict_suite_complete_count": sum(
            set(row.get("matched_control_audits") or {}) == required
            and all(
                bool(value.get("all_required_dimensions_matched"))
                for value in row["matched_control_audits"].values()
            )
            for row in rows
        ),
        "clean_low_velocity_same_start_replay_count": sum(
            bool(
                (row.get("clean_low_velocity_baseline") or {}).get(
                    "all_identical_starts_replayed"
                )
            )
            for row in rows
        ),
        "clean_low_velocity_full_7d_match_count": sum(
            bool(
                (
                    (row.get("clean_low_velocity_baseline") or {}).get(
                        "match_audit"
                    )
                    or {}
                ).get("all_seven_dimensions_exact")
            )
            for row in rows
        ),
        "clean_baseline_exact_cell_count": sum(
            bool(
                (row.get("clean_low_velocity_baseline") or {}).get(
                    "clean_baseline_exact_cell"
                )
            )
            for row in rows
        ),
        "clean_low_velocity_role": (
            "SAME_START_ACTUAL_ECONOMIC_BASELINE_NEAREST_DEFENSIBLE_CELL_"
            "WITH_EXPLICIT_LIMITATIONS_NOT_A_MATCHED_NULL"
        ),
        "controls_selected_after_outcomes": False,
    }


def _terminal_failure_vectors(
    *stages: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    failures = [
        row
        for values in stages
        for row in values
        if str(row.get("status", "")).endswith("INVALID_RECORDED")
    ]
    counts = _counts(row.get("failure_reason", "UNKNOWN") for row in failures)
    return {
        "candidate_invalid_count": len(failures),
        "failure_reason_counts": counts,
        "hard_causality_defect_count": sum(
            int(row.get("hard_causality_defect_count", 0)) for row in failures
        ),
        "broad_threshold_retuning_performed": False,
        "xfa_deferred_until_clean_combine_survivors": True,
    }


def _one_per_behavior_then_fill(
    rows: Sequence[Mapping[str, Any]], maximum: int
) -> list[dict[str, Any]]:
    retained: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = str(
            row.get("behavioral_cluster")
            or row.get("realized_behavioral_fingerprint")
            or row.get("behavioral_fingerprint")
            or stable_hash(
                {
                    "market": row["candidate"]["market"],
                    "mechanism": row["candidate"]["mechanism"],
                    "timeframe": row["candidate"]["timeframe"],
                    "session_code": row["candidate"]["session_code"],
                }
            )
        )
        if key in seen:
            continue
        retained.append(dict(row))
        seen.add(key)
        if len(retained) >= maximum:
            return retained
    return retained


def _selection_receipt(
    *,
    campaign_id: str,
    stage: str,
    input_count: int,
    selected: Sequence[Mapping[str, Any]],
    selection: str,
) -> dict[str, Any]:
    value = {
        "schema": "hydra_causal_target_velocity_halving_v1",
        "campaign_id": campaign_id,
        "stage": stage,
        "input_count": int(input_count),
        "output_count": len(selected),
        "selected_candidate_ids": [
            str(row.get("candidate_id") or row.get("book_id")) for row in selected
        ],
        "selection": selection,
        "opaque_learned_score": False,
        "thresholds_changed_after_outcomes": False,
    }
    value["selection_hash"] = stable_hash(value)
    return value


def _balanced_candidate_sample(
    rows: Sequence[HazardCandidate], count: int
) -> tuple[HazardCandidate, ...]:
    groups: dict[tuple[str, str], list[HazardCandidate]] = {}
    for row in rows:
        groups.setdefault((row.market, row.mechanism), []).append(row)
    ordered_groups = [
        sorted(values, key=lambda value: value.structural_fingerprint)
        for _, values in sorted(groups.items())
    ]
    retained: list[HazardCandidate] = []
    position = 0
    while len(retained) < count:
        progressed = False
        for values in ordered_groups:
            if position < len(values):
                retained.append(values[position])
                progressed = True
                if len(retained) == count:
                    break
        if not progressed:
            break
        position += 1
    if len(retained) != count:
        raise CausalTargetVelocityRuntimeError(
            f"balanced Stage 1 population contains {len(retained)} != {count}"
        )
    return tuple(retained)


def _candidate_record(row: HazardCandidate) -> dict[str, Any]:
    return {
        "schema": "hydra_causal_target_velocity_candidate_v1",
        "candidate_id": row.candidate_id,
        "candidate": row.payload,
        "structural_fingerprint": row.structural_fingerprint,
        "behavioral_fingerprint": row.behavioral_fingerprint,
        "executable_specification": row.executable_specification(),
    }


def _discover_feature_matrices(
    cache_root: Path, markets: Sequence[str]
) -> dict[str, Path]:
    expected = set(markets)
    found: dict[str, Path] = {}
    required = {
        "availability_ns",
        "bar_close",
        "bar_high",
        "bar_low",
        "bar_open",
        "contract_code",
        "decision_ns",
        "feature__past_return_60",
        "feature__past_volatility",
        "segment_code",
        "session_code",
        "session_day",
        "timestamp_ns",
    }
    for path in sorted(cache_root.glob("*/manifest.json")):
        value = _read_json(path)
        market = str((value.get("provenance") or {}).get("market") or "")
        if market not in expected:
            continue
        arrays = set((value.get("arrays") or {}).keys())
        if not required.issubset(arrays):
            raise CausalTargetVelocityRuntimeError(
                f"causal feature matrix is incomplete: {market}"
            )
        if any(name.startswith("feature__forward") for name in arrays):
            raise CausalTargetVelocityRuntimeError(
                f"future outcome field escaped into decision feature namespace: {market}"
            )
        if market in found:
            raise CausalTargetVelocityRuntimeError(
                f"ambiguous cached feature matrix: {market}"
            )
        found[market] = path
    missing = sorted(expected - set(found))
    if missing:
        raise CausalTargetVelocityRuntimeError(
            "cache-only feature matrices missing: " + ",".join(missing)
        )
    return found


def _resolve_period(
    manifest: Mapping[str, Any], matrices: Mapping[str, Path]
) -> tuple[int, int, int]:
    blocks = list(manifest["temporal_blocks"]["blocks"])
    try:
        starts = [_date_value(str(row["start"])) for row in blocks]
        ends = [_date_value(str(row["end"])) for row in blocks]
    except (KeyError, ValueError) as exc:
        raise CausalTargetVelocityRuntimeError(
            "temporal blocks require frozen start/end dates"
        ) from exc
    evaluation_start = min(starts)
    evaluation_end_exclusive = max(ends) + timedelta(days=1)
    if evaluation_end_exclusive > date(2024, 10, 1):
        raise CausalTargetVelocityRuntimeError("0028 evaluation attempted to enter Q4")
    calibration_end = evaluation_start
    for path in matrices.values():
        matrix = FeatureMatrix.open(path.parent, mmap=True)
        timestamp = matrix.array("timestamp_ns")
        if not np.any(timestamp < _date_ns(calibration_end)):
            raise CausalTargetVelocityRuntimeError(
                "feature matrix lacks pre-evaluation calibration history"
            )
    return (
        _date_ns(calibration_end),
        _date_ns(evaluation_start),
        _date_ns(evaluation_end_exclusive),
    )


def _evaluation_days(matrix: FeatureMatrix) -> tuple[int, ...]:
    if _WORKER_PERIOD is None:
        raise CausalTargetVelocityRuntimeError("worker period is not initialized")
    _, start, end = _WORKER_PERIOD
    timestamp = matrix.array("timestamp_ns")
    days = matrix.array("session_day")
    mask = (timestamp >= start) & (timestamp < end)
    return tuple(sorted({int(value) for value in days[mask]}))


def _candidate_evaluation_days(
    candidate: HazardCandidate, matrix: FeatureMatrix
) -> tuple[int, ...]:
    """Return the full eligible calendar for this frozen session role.

    Opportunity-density denominators must not shrink to emitted-event days and
    ANY_RTH must not accidentally absorb overnight rows.
    """

    if _WORKER_PERIOD is None:
        raise CausalTargetVelocityRuntimeError("worker period is not initialized")
    _, start, end = _WORKER_PERIOD
    return frozen_eligible_session_calendar(
        candidate,
        matrix,
        evaluation_start_ns=start,
        evaluation_end_exclusive_ns=end,
    )


def _block_map(
    matrix: FeatureMatrix, blocks: Sequence[Mapping[str, Any]]
) -> dict[int, str]:
    output: dict[int, str] = {}
    for raw_day in np.unique(matrix.array("session_day")):
        epoch_day = int(raw_day)
        value = date(1970, 1, 1) + timedelta(days=epoch_day)
        for block in blocks:
            if _date_value(str(block["start"])) <= value <= _date_value(
                str(block["end"])
            ):
                output[epoch_day] = str(block["block_id"])
                break
    return output


def _block_aware_full_coverage_starts(
    days: Sequence[int],
    *,
    block_map: Mapping[int, str],
    horizon: int,
    maximum: int,
    preferred_starts: Sequence[int] | None = None,
) -> tuple[int, ...]:
    ordered = tuple(sorted({int(value) for value in days}))
    usable = max(0, len(ordered) - horizon + 1)
    usable_days = set(ordered[:usable])
    index_by_day = {day: index for index, day in enumerate(ordered)}
    # Equal-length intervals admit a maximum-cardinality non-overlapping set by
    # greedily choosing the earliest eligible start.  Unlike the old one/block
    # shortcut this does not discard a later defensible start in the same block.
    preferred = set(int(value) for value in (preferred_starts or ()))

    def greedy(candidate_order: Sequence[int]) -> tuple[int, ...]:
        starts: list[int] = []
        next_index = 0
        for day in candidate_order:
            index = index_by_day[day]
            if index < next_index or day not in usable_days or day not in block_map:
                continue
            starts.append(day)
            next_index = index + horizon
            if len(starts) >= maximum:
                break
        return tuple(starts)

    unrestricted = greedy(ordered)
    if not preferred:
        return unrestricted
    matched = greedy([day for day in ordered if day in preferred])
    return matched if len(matched) == len(unrestricted) else unrestricted


def _partition_starts_by_event_coverage(
    starts: Sequence[int],
    *,
    days: Sequence[int],
    horizon: int,
    events: Sequence[Any],
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Separate headline starts from windows containing censored emissions."""

    ordered = tuple(int(value) for value in days)
    index_by_day = {day: index for index, day in enumerate(ordered)}
    censored_days = {
        int(row.session_day)
        for row in events
        if row.outcome == HazardOutcome.CENSORED_FUTURE_COVERAGE
    }
    full: list[int] = []
    censored: list[int] = []
    for start in starts:
        start_index = index_by_day[int(start)]
        window = set(ordered[start_index : start_index + int(horizon)])
        (censored if window & censored_days else full).append(int(start))
    return tuple(full), tuple(censored)


def _block_local_event_economics(
    events: Sequence[Any], block_map: Mapping[int, str]
) -> dict[str, Any]:
    """Report block-local economics without pretending 90d paths are blocks."""

    output: dict[str, Any] = {}
    for block in sorted(set(block_map.values())):
        emitted = [row for row in events if block_map.get(row.session_day) == block]
        completed = [
            row
            for row in emitted
            if row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
        ]
        normal_net = float(sum(float(row.normal_net_pnl or 0.0) for row in completed))
        stressed_net = float(
            sum(float(row.stressed_net_pnl or 0.0) for row in completed)
        )
        output[block] = {
            "evidence_role": "EVENT_DECISION_ORIGIN_BLOCK_DIAGNOSTIC_NOT_90D_EPISODE",
            "emitted_event_count": len(emitted),
            "completed_event_count": len(completed),
            "data_censored_event_count": len(emitted) - len(completed),
            "normal_net_total": normal_net,
            "stressed_net_total": stressed_net,
            "normal_target_progress": normal_net / 9_000.0,
            "stressed_target_progress": stressed_net / 9_000.0,
            "favorable_first_count": sum(
                row.outcome == HazardOutcome.FAVORABLE_FIRST for row in completed
            ),
            "adverse_first_count": sum(
                row.outcome == HazardOutcome.ADVERSE_FIRST for row in completed
            ),
        }
    return output


def _episodes_by_block(
    normal: Sequence[CombineEpisodeResult], stressed: Sequence[CombineEpisodeResult]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for block in sorted({row.start_regime for row in (*normal, *stressed)}):
        nrows = [row for row in normal if row.start_regime == block]
        srows = [row for row in stressed if row.start_regime == block]
        output[block] = {
            "normal_pass_count": sum(row.passed for row in nrows),
            "stressed_pass_count": sum(row.passed for row in srows),
            "normal_net_total": float(sum(row.net_pnl for row in nrows)),
            "stressed_net_total": float(sum(row.net_pnl for row in srows)),
            "normal_target_progress_median": (
                float(np.median([row.target_progress for row in nrows]))
                if nrows
                else 0.0
            ),
            "stressed_target_progress_median": (
                float(np.median([row.target_progress for row in srows]))
                if srows
                else 0.0
            ),
        }
    return output


def _stage3_episode_count(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(
        int(row["normal"]["episode_count"])
        + int(row["stressed"]["episode_count"])
        for row in rows
        if row.get("status") == "STAGE_3_COMPLETE"
    )


def _completed_count(rows: Sequence[Mapping[str, Any]], status: str) -> int:
    return sum(str(row.get("status")) == status for row in rows)


def _stage1_event_evidence_record(value: Mapping[str, Any]) -> dict[str, Any]:
    """Compact one complete Stage-1 event contract without per-bar account paths."""

    record = dict(value)
    for field in ("normal_marks", "stressed_marks"):
        marks = list(record.pop(field, ()))
        record[f"{field}_count"] = len(marks)
        record[f"{field}_hash"] = stable_hash(marks)
    record["evidence_scope"] = (
        "COMPLETE_CAUSAL_EVENT_CONTRACT_STAGE1_OUTCOME_"
        "WITHOUT_PER_BAR_ACCOUNT_TRAJECTORY"
    )
    return record


def _canonical_jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
        for row in rows
    ).encode("utf-8")


def _verify_stage1_event_evidence_receipt(
    payload_dir: Path, row: Mapping[str, Any]
) -> None:
    """Deep-check the immutable event ledger before trusting a resumed screen."""

    if row.get("status") != "STAGE_1_COMPLETE":
        return
    receipt = dict(row.get("event_evidence") or {})
    if receipt.get("schema") != (
        "hydra_causal_target_velocity_stage1_event_evidence_receipt_v1"
    ) or receipt.get("encoding") != "canonical-jsonl+gzip-mtime-zero":
        raise CausalTargetVelocityRuntimeError(
            "Stage 1 event evidence receipt is missing or invalid"
        )
    path = payload_dir / str(receipt.get("relative_path", ""))
    try:
        compressed = path.read_bytes()
        uncompressed = gzip.decompress(compressed)
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise CausalTargetVelocityRuntimeError(
            f"Stage 1 event evidence is unreadable: {path}"
        ) from exc
    if hashlib.sha256(compressed).hexdigest() != str(receipt.get("sha256")):
        raise CausalTargetVelocityRuntimeError(
            f"Stage 1 compressed event evidence checksum drift: {path}"
        )
    if hashlib.sha256(uncompressed).hexdigest() != str(
        receipt.get("uncompressed_sha256")
    ):
        raise CausalTargetVelocityRuntimeError(
            f"Stage 1 uncompressed event evidence checksum drift: {path}"
        )
    try:
        values = [
            json.loads(line)
            for line in uncompressed.decode("utf-8").splitlines()
            if line.strip()
        ]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CausalTargetVelocityRuntimeError(
            f"Stage 1 event evidence JSONL is invalid: {path}"
        ) from exc
    if (
        len(values) != int(receipt.get("record_count", -1))
        or len(values) != int((row.get("event_contract") or {}).get("emitted", -1))
        or any(str(value.get("candidate_id")) != str(row["candidate_id"]) for value in values)
        or stable_hash([str(value["event_fingerprint"]) for value in values])
        != str(receipt.get("event_fingerprint_hash"))
    ):
        raise CausalTargetVelocityRuntimeError(
            f"Stage 1 event evidence identity/count reconciliation failed: {path}"
        )


def _seal_stage1_event_evidence_inventory(
    *,
    payload_dir: Path,
    writer: AtomicResultWriter,
    campaign_id: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Bind every Stage-1 event ledger into one immutable terminal inventory."""

    entries: list[dict[str, Any]] = []
    total_censored = 0
    for row in sorted(rows, key=lambda value: str(value["candidate_id"])):
        if row.get("status") != "STAGE_1_COMPLETE":
            continue
        _verify_stage1_event_evidence_receipt(payload_dir, row)
        receipt = dict(row["event_evidence"])
        event_contract = dict(row.get("event_contract") or {})
        total_censored += int(event_contract.get("censored_future_coverage", 0))
        entries.append(
            {
                "candidate_id": str(row["candidate_id"]),
                "candidate_fingerprint": str(row["candidate_fingerprint"]),
                "relative_path": str(receipt["relative_path"]),
                "record_count": int(receipt["record_count"]),
                "sha256": str(receipt["sha256"]),
                "uncompressed_sha256": str(receipt["uncompressed_sha256"]),
                "event_fingerprint_hash": str(
                    receipt["event_fingerprint_hash"]
                ),
                "censored_future_coverage_count": int(
                    event_contract.get("censored_future_coverage", 0)
                ),
            }
        )
    inventory: dict[str, Any] = {
        "schema": "hydra_causal_target_velocity_stage1_event_inventory_v1",
        "campaign_id": campaign_id,
        "candidate_ledger_count": len(entries),
        "event_record_count": sum(value["record_count"] for value in entries),
        "censored_future_coverage_count": total_censored,
        "entries": entries,
        "orphan_ledger_allowed": False,
    }
    inventory["inventory_hash"] = stable_hash(inventory)
    receipt = writer.write_json("stage1_event_evidence_inventory.json", inventory)
    return {
        "relative_path": receipt.relative_path,
        "sha256": receipt.sha256,
        "inventory_hash": inventory["inventory_hash"],
        "candidate_ledger_count": inventory["candidate_ledger_count"],
        "event_record_count": inventory["event_record_count"],
        "censored_future_coverage_count": inventory[
            "censored_future_coverage_count"
        ],
    }


def _load_batches(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_dir():
        return rows
    for batch in sorted(path.glob("batch_*.jsonl")):
        rows.extend(_read_jsonl(batch))
    identifiers = [str(row["candidate_id"]) for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise CausalTargetVelocityRuntimeError(
            f"duplicate candidate across resume batches: {path}"
        )
    return rows


def _load_book_batches(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if path.is_dir():
        for batch in sorted(path.glob("batch_*.jsonl")):
            rows.extend(_read_jsonl(batch))
    identifiers = [str(row["book_id"]) for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise CausalTargetVelocityRuntimeError(
            f"duplicate book across resume batches: {path}"
        )
    return rows


def _load_episode_evidence_records(
    payload_dir: Path, rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        receipt = dict(row["episode_evidence"])
        path = payload_dir / str(receipt["relative_path"])
        if _sha256(path) != str(receipt["sha256"]):
            raise CausalTargetVelocityRuntimeError(
                f"episode evidence checksum drift: {path}"
            )
        values = _read_jsonl(path)
        if len(values) != int(receipt["record_count"]):
            raise CausalTargetVelocityRuntimeError(
                f"episode evidence row-count drift: {path}"
            )
        records.extend(values)
    return records


def _next_batch_index(path: Path) -> int:
    values = [int(row.stem.rsplit("_", 1)[-1]) for row in path.glob("batch_*.jsonl")]
    return max(values, default=-1) + 1


def _counts(values: Iterable[Any]) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        key = str(value)
        output[key] = output.get(key, 0) + 1
    return dict(sorted(output.items()))


def _verify_snapshot(
    value: Mapping[str, Any], hash_field: str, manifest: Mapping[str, Any]
) -> None:
    payload = dict(value)
    claimed = str(payload.pop(hash_field, ""))
    if (
        claimed != stable_hash(payload)
        or value.get("campaign_id") != manifest.get("campaign_id")
        or value.get("manifest_hash") != manifest.get("manifest_hash")
        or value.get("source_commit") != manifest.get("source_commit")
    ):
        raise CausalTargetVelocityRuntimeError("live snapshot integrity failure")


def _project_root(path: Path) -> Path:
    for candidate in (path.parent, *path.parents):
        if (candidate / "MISSION_CONTRACT.md").is_file():
            return candidate
    raise CausalTargetVelocityRuntimeError("project root not found")


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CausalTargetVelocityRuntimeError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CausalTargetVelocityRuntimeError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise CausalTargetVelocityRuntimeError(f"invalid JSONL: {path}") from exc
    for line in lines:
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise CausalTargetVelocityRuntimeError(f"JSONL object required: {path}")
        output.append(value)
    return output


def _date_value(value: str) -> date:
    return date.fromisoformat(value[:10])


def _date_ns(value: date) -> int:
    moment = datetime(value.year, value.month, value.day, tzinfo=UTC)
    return int(moment.timestamp() * 1_000_000_000)


def _parse_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise CausalTargetVelocityRuntimeError("timestamp lacks timezone")
    return parsed


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _system_cpu_ticks() -> tuple[int, int]:
    """Return Linux host busy/total ticks for measured utilisation deltas."""

    try:
        fields = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()
        if not fields or fields[0] != "cpu":
            return (0, 0)
        values = [int(value) for value in fields[1:]]
        total = sum(values)
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return (total - idle, total)
    except (OSError, ValueError, IndexError):
        return (0, 0)


def _system_cpu_fraction(start: tuple[int, int]) -> float:
    busy, total = _system_cpu_ticks()
    busy_delta = busy - int(start[0])
    total_delta = total - int(start[1])
    if total_delta <= 0:
        return 0.0
    return min(max(busy_delta / total_delta, 0.0), 1.0)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CAUSAL_TARGET_VELOCITY_RUNTIME_VERSION",
    "CausalTargetVelocityRuntimeError",
    "read_causal_target_velocity_status",
    "run_causal_target_velocity_manifest",
]
