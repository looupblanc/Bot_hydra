from __future__ import annotations

import hashlib
import json
import os
import resource
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.compute.result_writer import AtomicResultWriter, AtomicWriteReceipt
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy
from hydra.evidence import (
    guard_campaign_completion,
    recover_finalized_evidence_bundle,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.production.component_evidence import materialize_component_evidence
from hydra.production.episode_evidence import replay_evidence_rows
from hydra.production.evidence_adapter import AsyncEvidenceBundleSink
from hydra.production.halving import (
    aggregate_policy_evidence,
    build_compact_outputs,
    build_final_result_payload,
    build_leave_one_block_out_plan,
    complete_leave_one_block_out,
    development_decision,
    pareto_select,
    select_stage3_survivors,
    select_stage4_survivors,
    select_stage5_survivors,
)
from hydra.production.manifest import (
    PRODUCTION_RESULT_SCHEMA,
    load_and_validate_production_manifest,
    verify_runtime_inputs,
)
from hydra.production.policy_factory import (
    ComponentCandidate,
    ProductionPolicy,
    build_predeclared_control_bank,
    fast_economic_screen,
    generate_policy_population,
    load_component_candidates,
)
from hydra.production.replay import (
    PRODUCTION_REPLAY_VERSION,
    evaluate_policy_batch_parallel,
    rank_exact_replays,
    transform_policy_events,
)
from hydra.research.economic_evolution_pilot import _bind_selected, _build_exact_runtimes


PRODUCTION_STATE_SCHEMA = "hydra_economic_production_state_v1"
PRODUCTION_KPI_SCHEMA = "hydra_economic_production_kpis_v1"


class ProductionRuntimeError(RuntimeError):
    pass


@dataclass
class _Clock:
    started_wall: float
    started_cpu: float
    children_cpu: float
    system_busy_ticks: int
    system_total_ticks: int
    hot_seconds: float = 0.0
    cold_seconds: float = 0.0
    engineering_seconds: float = 0.0

    @classmethod
    def start(cls) -> "_Clock":
        busy, total = _system_cpu_ticks()
        return cls(
            time.perf_counter(), time.process_time(), _children_cpu(), busy, total
        )

    @property
    def elapsed(self) -> float:
        return max(time.perf_counter() - self.started_wall, 1e-9)


def load_and_verify_production_result(
    path: str | Path,
    manifest: Mapping[str, Any],
    *,
    deep_evidence: bool = True,
) -> dict[str, Any]:
    target = Path(path)
    try:
        result = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionRuntimeError(f"invalid production result: {target}") from exc
    if not isinstance(result, dict):
        raise ProductionRuntimeError("production result must be an object")
    claimed = str(result.get("result_hash") or "")
    payload = dict(result)
    payload.pop("result_hash", None)
    if not claimed or stable_hash(payload) != claimed:
        raise ProductionRuntimeError("production result hash drift")
    if (
        result.get("schema") != PRODUCTION_RESULT_SCHEMA
        or result.get("campaign_id") != manifest.get("campaign_id")
        or result.get("manifest_hash") != manifest.get("manifest_hash")
        or result.get("source_commit") != manifest.get("source_commit")
        or result.get("status") != "COMPLETE"
    ):
        raise ProductionRuntimeError("production result identity/status drift")
    receipt = result.get("evidence_bundle")
    if not isinstance(receipt, Mapping) or not str(receipt.get("bundle_path") or ""):
        raise ProductionRuntimeError("production result lacks sealed EvidenceBundle receipt")
    verified = guard_campaign_completion(
        "COMPLETE",
        str(receipt["bundle_path"]),
        campaign_id=str(manifest["campaign_id"]),
        deep=deep_evidence,
    )
    if str(receipt.get("manifest_sha256") or "") != _sha256(
        Path(str(receipt["manifest_path"]))
    ):
        raise ProductionRuntimeError("EvidenceBundle receipt manifest checksum drift")
    if verified is None or verified.get("status") != "COMPLETE":
        raise ProductionRuntimeError("EvidenceBundle is not complete")
    return result


def read_live_status(manifest_path: str | Path) -> dict[str, Any]:
    manifest = load_and_validate_production_manifest(manifest_path)
    if manifest.get("campaign_mode") == "FAST_PASS_FACTORY":
        from hydra.production.fast_pass_runtime import read_fast_pass_status

        return read_fast_pass_status(manifest_path)
    root = Path(manifest_path).resolve().parents[2]
    output = root / str(manifest["runtime"]["output_dir"])
    state = _load_json(output / "production_state.json")
    kpis = _load_json(output / "production_kpis.json")
    _verify_snapshot_hash(state, "state_hash")
    _verify_snapshot_hash(kpis, "kpi_hash")
    return {"state": state, "kpis": kpis}


def run_production_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path,
    cache_root: str | Path,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run/resume the evidence-first production funnel; never terminalize early."""

    manifest = load_and_validate_production_manifest(manifest_path)
    if manifest.get("campaign_mode") == "FAST_PASS_FACTORY":
        from hydra.production.fast_pass_runtime import run_fast_pass_manifest

        return run_fast_pass_manifest(
            manifest_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
            stop_after=stop_after,
        )
    if manifest.get("campaign_mode") == "ACTIVE_RISK_POOL":
        from hydra.production.active_risk_runtime import run_active_risk_manifest

        return run_active_risk_manifest(
            manifest_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
            stop_after=stop_after,
        )
    if manifest.get("campaign_mode") == "PORTFOLIO_FIRST":
        from hydra.production.portfolio_runtime import run_portfolio_first_manifest

        return run_portfolio_first_manifest(
            manifest_path,
            contract_map_path=contract_map_path,
            cache_root=cache_root,
            stop_after=stop_after,
        )

    return _ProductionRun(
        manifest_path=Path(manifest_path),
        contract_map_path=Path(contract_map_path),
        cache_root=Path(cache_root),
        stop_after=stop_after,
    ).execute()


class _ProductionRun:
    def __init__(
        self,
        *,
        manifest_path: Path,
        contract_map_path: Path,
        cache_root: Path,
        stop_after: str | None,
    ) -> None:
        self.manifest_path = manifest_path.resolve()
        self.root = self.manifest_path.parents[2]
        self.contract_map_path = contract_map_path.resolve()
        self.cache_root = cache_root.resolve()
        self.stop_after = stop_after
        if stop_after is not None and os.environ.get("HYDRA_PRODUCTION_TEST_MODE") != "1":
            raise ProductionRuntimeError("stop_after is restricted to explicit test mode")
        self.manifest = load_and_validate_production_manifest(self.manifest_path)
        self.campaign_id = str(self.manifest["campaign_id"])
        self.output_dir = (self.root / str(self.manifest["runtime"]["output_dir"])).resolve()
        self.payload_dir = self.root / "data/cache/economic_production" / self.campaign_id
        self.output_writer = AtomicResultWriter(self.output_dir)
        self.live_writer = AtomicResultWriter(self.output_dir, immutable=False)
        self.payload_writer = AtomicResultWriter(self.payload_dir)
        self.clock = _Clock.start()
        self.state = self._initial_state()
        self.summaries: dict[str, dict[str, Any]] = {}
        self.population_summary: dict[str, Any] = {}
        self.cache_hit_rate = 0.0
        self.feature_cache_fingerprints: dict[str, str] = {}
        self._durable_episode_count: int | None = None
        self._allocation_last_wall = self.clock.started_wall
        self._allocation_last_hot = 0.0
        self._allocation_last_cold = 0.0
        self._allocation_last_engineering = 0.0
        self._allocation_last_busy = self.clock.system_busy_ticks
        self._allocation_last_total = self.clock.system_total_ticks

    def execute(self) -> dict[str, Any]:
        result_path = self.output_dir / str(self.manifest["runtime"]["result_name"])
        if result_path.is_file():
            result = load_and_verify_production_result(result_path, self.manifest)
            self._reconcile_completed_result_snapshots(result)
            return result
        try:
            self._verify_source_commit()
            recovered = self._recover_sealed_bundle_result()
            if recovered is not None:
                return recovered
            cemetery = self._audit_cemetery()
            components = load_component_candidates(self.manifest, self.root)
            population = self._load_or_generate_population(components)
            component_map = {row.sleeve.sleeve_id: row for row in components}
            screen_rows, selected = self._load_or_screen(population, component_map)
            if len(population) != 20_000 or len(screen_rows) < 4_096 or len(selected) < 512:
                raise ProductionRuntimeError(
                    "production funnel minimums not met (20000/4096/512)"
                )
            self._publish(
                state="FAST_SCREEN_COMPLETE",
                stage="STAGE_1",
                policies_proposed=len(population),
                unique_policies_screened=len(screen_rows),
                next_action="OPEN_CACHE_ONLY_COMPONENTS",
                cemetery_audit=cemetery,
            )
            if self.stop_after == "FAST_SCREEN":
                return dict(self.state)

            matrices = self._open_cached_matrices()
            sleeves_needed = {
                row.sleeve.sleeve_id for row in components
            }
            runtimes, failures = self._compile_components(
                components, sleeves_needed, matrices
            )
            if failures:
                self.payload_writer.write_json("component_compile_failures.json", failures)
                raise ProductionRuntimeError(
                    "declared 0024 eligible component bank failed exact compilation: "
                    f"{len(failures)} component(s)"
                )
            executable = tuple(
                policy
                for policy in selected
                if set(policy.sleeve_ids).issubset(runtimes)
                and _routing_is_executable(policy, runtimes)
            )
            if len(executable) < 512:
                raise ProductionRuntimeError(
                    f"fewer than 512 executable exact policies: {len(executable)}"
                )
            base_policies = executable[:512]
            control_policies = build_predeclared_control_bank(
                tuple(sorted(runtimes)), self.manifest
            )
            evidence_policies = (*base_policies, *control_policies)
            if len({row.policy_id for row in evidence_policies}) != len(evidence_policies):
                raise ProductionRuntimeError("candidate/control policy identity collision")
            starts = _block_aware_starts(
                runtimes,
                self.manifest,
                maximum=48,
            )
            if len(starts) != 48:
                raise ProductionRuntimeError(
                    f"frozen 48-start policy produced {len(starts)}"
                )
            calendars = _block_calendars(runtimes, self.manifest, starts)
            sink = self._open_evidence(
                evidence_policies, component_map, runtimes, starts
            )
            try:
                self._persist_static_evidence(
                    sink, evidence_policies, component_map, runtimes, matrices
                )
                self._publish(
                    state="EXACT_REPLAY_ACTIVE",
                    stage="STAGE_2",
                    policies_proposed=len(population),
                    unique_policies_screened=len(screen_rows),
                    next_action="REPLAY_512_BASE_POLICIES",
                )
                self._run_exact_batches(
                    sink, base_policies, runtimes, starts, calendars
                )
                if self.stop_after == "FIRST_HALVING" and len(self.summaries) >= 512:
                    return dict(self.state)
                ranked = rank_exact_replays(list(self.summaries.values()), limit=256)
                if len(self.summaries) < 512:
                    raise ProductionRuntimeError(
                        f"exact replay minimum not met: {len(self.summaries)}/512"
                    )
                self.payload_writer.write_json(
                    "stage2_halving.json",
                    {
                        "schema": "hydra_production_halving_v1",
                        "campaign_id": self.campaign_id,
                        "input_count": len(self.summaries),
                        "output_count": len(ranked),
                        "policy_ids": list(ranked),
                        "selection": "TRANSPARENT_PARETO_STRATIFIED_V1",
                        "matched_controls_status": "PENDING_STAGE_4_NOT_EXECUTED",
                        "null_status": "PENDING_STAGE_4_NOT_EXECUTED",
                    },
                )
                ranked_policies = [
                    next(row for row in base_policies if row.policy_id == policy_id)
                    for policy_id in ranked
                ]
                self._publish(
                    state="ROBUSTNESS_ACTIVE",
                    stage="STAGE_3",
                    policies_proposed=len(population),
                    unique_policies_screened=len(screen_rows),
                    exact_account_replays=len(self.summaries),
                    combine_episodes_completed=self._durable_episode_total(),
                    next_action="REPLAY_FROZEN_20_40_90_HORIZONS",
                )
                self._run_additional_horizons(
                    sink, ranked_policies, runtimes, starts, calendars
                )
                full_horizon = max(len(value) for value in calendars.values())
                full_rows = self._run_auxiliary_evidence_batches(
                    sink,
                    ranked_policies,
                    runtimes,
                    starts,
                    calendars,
                    horizon=full_horizon,
                    horizon_label="FULL_AVAILABLE_CHRONOLOGICAL_HORIZON",
                    namespace="full_available_episode_rows",
                )
                base_rows = _load_policy_episode_rows(
                    self.payload_dir / "exact_episode_rows",
                    base_policies,
                    horizon="60_TRADING_DAYS",
                )
                ranked_ids = {row.policy_id for row in ranked_policies}
                base_horizon_rows: dict[str, list[dict[str, Any]]] = {
                    "20_TRADING_DAYS": _load_policy_episode_rows(
                        self.payload_dir / "horizon_episode_rows" / "20",
                        ranked_policies,
                        horizon="20_TRADING_DAYS",
                    ),
                    "40_TRADING_DAYS": _load_policy_episode_rows(
                        self.payload_dir / "horizon_episode_rows" / "40",
                        ranked_policies,
                        horizon="40_TRADING_DAYS",
                    ),
                    "60_TRADING_DAYS": [
                        row
                        for row in base_rows
                        if str(row["policy_id"]) in ranked_ids
                    ],
                    "90_TRADING_DAYS": _load_policy_episode_rows(
                        self.payload_dir / "horizon_episode_rows" / "90",
                        ranked_policies,
                        horizon="90_TRADING_DAYS",
                    ),
                    "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON": full_rows,
                }
                block_ids = tuple(
                    str(row["block_id"])
                    for row in self.manifest["temporal_blocks"]["blocks"]
                )
                stage3_metrics = [
                    aggregate_policy_evidence(
                        policy.to_dict(),
                        base_rows,
                        block_ids=block_ids,
                        horizon="60_TRADING_DAYS",
                    )
                    for policy in ranked_policies
                ]
                stage3_decision = select_stage3_survivors(stage3_metrics, limit=64)
                stage3_survivor_ids = tuple(stage3_decision["selected_policy_ids"])
                stage3_policies = tuple(
                    row for row in ranked_policies if row.policy_id in stage3_survivor_ids
                )
                self.payload_writer.write_json(
                    "stage3_halving.json",
                    {
                        **stage3_decision,
                        "campaign_id": self.campaign_id,
                        "frozen_horizons_completed": [20, 40, 60, 90],
                        "full_available_chronological_horizon_completed": True,
                        "matched_controls_status": "PENDING_STAGE_4_NOT_EXECUTED",
                    },
                )
                total_episodes = self._durable_episode_total()
                self._publish(
                    state="ROBUSTNESS_ACTIVE",
                    stage="MATCHED_CONTROL_REPLAY",
                    exact_account_replays=len(self.summaries),
                    combine_episodes_completed=total_episodes,
                    next_action="REPLAY_PREDECLARED_CONTROL_BANK",
                    stage3_survivor_count=len(stage3_policies),
                    predeclared_control_policy_count=len(control_policies),
                )
                control_rows = self._run_auxiliary_evidence_batches(
                    sink,
                    control_policies,
                    runtimes,
                    starts,
                    calendars,
                    horizon=60,
                    horizon_label="60_TRADING_DAYS",
                    namespace="control_60_episode_rows",
                )
                stage_decisions: list[Mapping[str, Any]] = [stage3_decision]
                crossfit_result: Mapping[str, Any] | None = None
                stage4_metrics: list[Mapping[str, Any]] = []
                stage5_metrics: list[Mapping[str, Any]] = []
                stage6_metrics: list[Mapping[str, Any]] = []
                development_decisions: list[Mapping[str, Any]] = []

                if stage3_policies:
                    lobo_plan = build_leave_one_block_out_plan(
                        [row.to_dict() for row in stage3_policies],
                        base_rows,
                        predeclared_baseline_policies=[
                            row.to_dict() for row in control_policies
                        ],
                        baseline_design_episode_rows=control_rows,
                        block_ids=block_ids,
                        random_seeds=self.manifest["matched_controls"]["random_seeds"],
                        horizon="60_TRADING_DAYS",
                    )
                    # This immutable write is the freeze barrier: held-out rows
                    # are not passed to the completion routine until it succeeds.
                    self.payload_writer.write_json("stage4_lobo_plan.json", lobo_plan)
                    crossfit_result = complete_leave_one_block_out(
                        lobo_plan,
                        base_rows,
                        control_rows,
                    )
                    self.payload_writer.write_json(
                        "stage4_lobo_result.json", crossfit_result
                    )
                    selector_status = str(
                        crossfit_result["selector_decision"]["status"]
                    )
                    if selector_status == "SELECTOR_PROCEDURE_GREEN":
                        stage4_metrics = [
                            aggregate_policy_evidence(
                                policy.to_dict(),
                                base_rows,
                                block_ids=block_ids,
                                horizon="60_TRADING_DAYS",
                            )
                            for policy in stage3_policies
                        ]
                        stage4_decision = select_stage4_survivors(
                            stage4_metrics, limit=16
                        )
                    else:
                        stage4_decision = _stage4_selector_gate_decision(
                            [row.policy_id for row in stage3_policies],
                            selector_status=selector_status,
                        )
                    stage_decisions.append(stage4_decision)
                    self.payload_writer.write_json(
                        "stage4_halving.json", stage4_decision
                    )
                    stage4_ids = set(stage4_decision["selected_policy_ids"])
                    stage4_policies = tuple(
                        row for row in stage3_policies if row.policy_id in stage4_ids
                    )
                else:
                    stage4_policies = ()

                if stage4_policies:
                    starts96 = _block_aware_starts(
                        runtimes,
                        self.manifest,
                        maximum=96,
                        required_starts=starts,
                    )
                    new96 = tuple(row for row in starts96 if row not in set(starts))
                    calendars96 = _block_calendars(runtimes, self.manifest, new96)
                    expanded96_by_horizon = self._run_expanded_horizon_set(
                        sink,
                        stage4_policies,
                        runtimes,
                        new96,
                        calendars96,
                        namespace="expanded_96",
                    )
                    stage4_policy_ids = {policy.policy_id for policy in stage4_policies}
                    horizon_rows96 = {
                        label: [
                            row
                            for row in prior
                            if str(row["policy_id"]) in stage4_policy_ids
                        ]
                        + expanded96_by_horizon[label]
                        for label, prior in base_horizon_rows.items()
                    }
                    combined96 = horizon_rows96["60_TRADING_DAYS"]
                    stage5_metrics = [
                        aggregate_policy_evidence(
                            policy.to_dict(),
                            combined96,
                            block_ids=block_ids,
                            horizon="60_TRADING_DAYS",
                        )
                        for policy in stage4_policies
                    ]
                    stage5_decision = select_stage5_survivors(
                        stage5_metrics,
                        criteria=self.manifest["promotion_policy"],
                        limit=4,
                    )
                    stage_decisions.append(stage5_decision)
                    self.payload_writer.write_json(
                        "stage5_halving.json", stage5_decision
                    )
                    stage5_ids = set(stage5_decision["selected_policy_ids"])
                    stage5_policies = tuple(
                        row for row in stage4_policies if row.policy_id in stage5_ids
                    )
                else:
                    starts96 = starts
                    combined96 = []
                    horizon_rows96 = {}
                    stage5_policies = ()

                if stage5_policies:
                    starts192 = _block_aware_starts(
                        runtimes,
                        self.manifest,
                        maximum=192,
                        required_starts=starts96,
                    )
                    new192 = tuple(row for row in starts192 if row not in set(starts96))
                    calendars192 = _block_calendars(runtimes, self.manifest, new192)
                    expanded192_by_horizon = self._run_expanded_horizon_set(
                        sink,
                        stage5_policies,
                        runtimes,
                        new192,
                        calendars192,
                        namespace="expanded_192",
                    )
                    stage5_policy_ids = {row.policy_id for row in stage5_policies}
                    horizon_rows192 = {
                        label: [
                            row
                            for row in prior
                            if str(row["policy_id"]) in stage5_policy_ids
                        ]
                        + expanded192_by_horizon[label]
                        for label, prior in horizon_rows96.items()
                    }
                    stage6_source_rows = horizon_rows192["60_TRADING_DAYS"]
                    stage6_metrics = [
                        aggregate_policy_evidence(
                            policy.to_dict(),
                            stage6_source_rows,
                            block_ids=block_ids,
                            horizon="60_TRADING_DAYS",
                        )
                        for policy in stage5_policies
                    ]
                    stage6_decision = pareto_select(
                        stage6_metrics,
                        limit=4,
                        stage="STAGE_6_DEVELOPMENT_FINALISTS_192_STARTS",
                        require_complete_component_attribution=True,
                        minimum_block_count=4,
                    )
                    stage_decisions.append(stage6_decision)
                    self.payload_writer.write_json(
                        "stage6_halving.json", stage6_decision
                    )
                    selected_stage6_metrics = _selected_stage6_metrics(
                        stage6_metrics, stage6_decision
                    )
                    development_decisions = [
                        development_decision(
                            metric,
                            criteria=self.manifest["promotion_policy"],
                            minimum_starts=192,
                        )
                        for metric in selected_stage6_metrics
                    ]
                else:
                    horizon_rows192 = {}

                final_metrics = list(
                    stage6_metrics
                    or stage5_metrics
                    or stage4_metrics
                    or stage3_metrics
                )
                if horizon_rows192:
                    audit_rows = horizon_rows192
                elif horizon_rows96:
                    audit_rows = horizon_rows96
                else:
                    stage3_ids = {row.policy_id for row in stage3_policies}
                    selected_for_audit = stage3_ids or ranked_ids
                    audit_rows = {
                        label: [
                            row
                            for row in rows
                            if str(row["policy_id"]) in selected_for_audit
                        ]
                        for label, rows in base_horizon_rows.items()
                    }
                return self._finalize_production_campaign(
                    sink,
                    metrics=final_metrics,
                    stage_decisions=stage_decisions,
                    crossfit_result=crossfit_result,
                    development_decisions=development_decisions,
                    horizon_audit=_build_horizon_audit(audit_rows),
                )
            finally:
                sink.close()
        except BaseException as exc:
            self._publish(
                state="FAILED_CLOSED",
                stage=str(self.state.get("stage") or "STARTING"),
                next_action="REQUIRE_SPECIFIC_RUNTIME_REPAIR",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise

    def _finalize_production_campaign(
        self,
        sink: AsyncEvidenceBundleSink,
        *,
        metrics: Sequence[Mapping[str, Any]],
        stage_decisions: Sequence[Mapping[str, Any]],
        crossfit_result: Mapping[str, Any] | None,
        development_decisions: Sequence[Mapping[str, Any]],
        horizon_audit: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        compact = build_compact_outputs(
            campaign_id=self.campaign_id,
            metrics=metrics,
            stage_decisions=stage_decisions,
            crossfit_result=crossfit_result,
            development_decisions=development_decisions,
        )
        if horizon_audit is not None:
            compact["campaign_summary"]["horizon_audit"] = dict(horizon_audit)
        total_episodes = self._durable_episode_total()
        # The durable caches are the counter source of truth.  Updating memory
        # before taking the KPI snapshot prevents a crash-recovered run from
        # carrying a stale delta-derived counter into its terminal result.
        self.state["combine_episodes_completed"] = total_episodes
        kpi_snapshot = self._kpis()
        evidence_policy_count = len(
            self.evidence_identity.get("policy_fingerprints", {})
        )
        compact["campaign_summary"].update(
            {
                "production_counters": {
                    "serious_exact_account_replays": len(self.summaries),
                    "predeclared_control_policy_replays": max(
                        evidence_policy_count - len(self.summaries), 0
                    ),
                    "combine_episodes_completed": total_episodes,
                    "normal_episodes_completed": total_episodes // 2,
                    "stressed_episodes_completed": total_episodes // 2,
                },
                "production_kpis": {
                    "rates_per_hour": dict(kpi_snapshot["rates_per_hour"]),
                    "economic_research_wall_clock_fraction": kpi_snapshot[
                        "economic_research_wall_clock_fraction"
                    ],
                    "cpu_utilization_fraction": kpi_snapshot[
                        "cpu_utilization_fraction"
                    ],
                    "workers": dict(kpi_snapshot["workers"]),
                    "duplicate_rejection_rate": kpi_snapshot[
                        "duplicate_rejection_rate"
                    ],
                    "cache_hit_rate": kpi_snapshot["cache_hit_rate"],
                },
                "economic_frontier": _economic_frontier(metrics),
                "matched_controls_status": _matched_controls_status(
                    crossfit_result
                ),
                "null_status": "NOT_EXECUTED_DISTINCT_ROUTING_NULL_PENDING",
                "crossfit_contamination_audit": {
                    "candidate_bank_prefiltered_on_all_development_blocks": True,
                    "independent_selector_claim_allowed": False,
                    "development_robustness_crossfit_only": True,
                },
            }
        )
        if crossfit_result is not None:
            crossfit_result = {
                **dict(crossfit_result),
                "contamination_audit": dict(
                    compact["campaign_summary"]["crossfit_contamination_audit"]
                ),
                "independent_confirmation": False,
            }
            compact["pareto_archive"]["crossfit"] = dict(crossfit_result)
        for name, payload in compact.items():
            sink.write_compact_output(name, payload)
        sink.checkpoint(
            {
                "stage": "FINALIZING_EVIDENCE_BUNDLE",
                "stage_decision_count": len(stage_decisions),
                "development_decision_count": len(development_decisions),
            }
        )
        sink.flush()
        confirmation_ready = sorted(
            str(row["policy_id"])
            for row in development_decisions
            if row.get("status") == "BASKET_CONFIRMATION_READY"
        )
        promoted_96, surviving_96 = _expanded_candidate_counts(stage_decisions)
        selector_status = (
            "DEVELOPMENT_ROBUSTNESS_"
            + str(crossfit_result["selector_decision"]["status"])
            + "_NOT_INDEPENDENT"
            if crossfit_result
            else "NOT_RUN_NO_STAGE3_SURVIVOR"
        )
        self._publish(
            state="FINALIZING",
            stage="EVIDENCE_BUNDLE_ATOMIC_FINALIZE",
            next_action="SEAL_AND_DEEP_VERIFY_EVIDENCE_BUNDLE",
            candidates_promoted_96=promoted_96,
            candidates_surviving_96=surviving_96,
            candidates_promoted_192=surviving_96,
            confirmation_ready_candidates=len(confirmation_ready),
            confirmation_ready_candidate_ids=confirmation_ready,
            combine_episodes_completed=total_episodes,
            matched_controls_status=_matched_controls_status(crossfit_result),
            null_status="NOT_EXECUTED_DISTINCT_ROUTING_NULL_PENDING",
            selector_status=selector_status,
        )
        receipt_path = self.root / str(
            self.manifest["evidence_bundle"]["lightweight_manifest_path"]
        )
        receipt = sink.finalize(lightweight_manifest_path=receipt_path)
        verified = sink.guard_completion(receipt.bundle_path)
        if verified.get("status") != "COMPLETE":
            raise ProductionRuntimeError("sealed production EvidenceBundle is incomplete")
        terminal_updates = {
            "state": "COMPLETE",
            "stage": "CAMPAIGN_COMPLETE",
            "next_action": str(
                compact["next_campaign_recommendations"]["recommendation"]["action"]
            ),
            "evidence_bundle_path": receipt.bundle_path,
            "evidence_bundle_manifest_sha256": receipt.manifest_sha256,
        }
        # Prepare terminal KPIs in memory, but keep the durable state FINALIZING
        # until the immutable result exists and verifies.
        self.state.update(terminal_updates)
        kpis = self._kpis()
        scientific_status = (
            "BASKET_CONFIRMATION_READY_DEVELOPMENT_ONLY"
            if confirmation_ready
            else "DEVELOPMENT_WAVE_COMPLETE"
        )
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=kpis,
            economic_results=compact["campaign_summary"],
            successive_halving={
                "stage_decisions": [dict(row) for row in stage_decisions]
            },
            matched_controls=_matched_controls_payload(crossfit_result),
            failure_vectors=compact["failure_vectors"],
            evidence_receipt=receipt.to_dict(),
            autonomous_next_action=compact["next_campaign_recommendations"][
                "recommendation"
            ],
            scientific_status=scientific_status,
        )
        result_name = str(self.manifest["runtime"]["result_name"])
        self.output_writer.write_json(result_name, result)
        checked = load_and_verify_production_result(
            self.output_dir / result_name, self.manifest
        )
        self._publish(**terminal_updates)
        return checked

    def _reconcile_completed_result_snapshots(
        self, result: Mapping[str, Any]
    ) -> None:
        """Make live state/KPIs agree with an already verified terminal result.

        The immutable result and sealed EvidenceBundle are authoritative.  A
        crash after writing the result but before publishing COMPLETE must not
        leave the persistent controller observing FINALIZING forever.
        """

        receipt = result.get("evidence_bundle")
        economic = result.get("economic_results")
        result_kpis = result.get("kpis")
        next_action = result.get("autonomous_next_action")
        if not all(
            isinstance(value, Mapping)
            for value in (receipt, economic, result_kpis, next_action)
        ):
            raise ProductionRuntimeError(
                "verified production result lacks terminal snapshot inputs"
            )
        assert isinstance(receipt, Mapping)
        assert isinstance(economic, Mapping)
        assert isinstance(result_kpis, Mapping)
        assert isinstance(next_action, Mapping)
        counters = economic.get("production_counters")
        if not isinstance(counters, Mapping):
            raise ProductionRuntimeError(
                "verified production result lacks durable production counters"
            )
        confirmation_ids = sorted(
            str(value)
            for value in (economic.get("confirmation_ready_candidate_ids") or ())
        )
        successive = result.get("successive_halving")
        if not isinstance(successive, Mapping):
            raise ProductionRuntimeError("terminal successive-halving payload is malformed")
        stage_decisions = successive.get("stage_decisions", ())
        if not isinstance(stage_decisions, Sequence) or isinstance(
            stage_decisions, (str, bytes)
        ):
            raise ProductionRuntimeError("terminal stage decisions are malformed")
        promoted_96, surviving_96 = _expanded_candidate_counts(stage_decisions)
        matched_status = str(
            economic.get("matched_controls_status")
            or result_kpis.get("matched_controls_status")
            or "UNKNOWN"
        )
        evidence_path = str(receipt["bundle_path"])
        evidence_sha = str(receipt["manifest_sha256"])
        expected_episodes = int(counters["combine_episodes_completed"])

        prior_kpi_path = self.output_dir / "production_kpis.json"
        prior_kpis: Mapping[str, Any] | None = None
        if prior_kpi_path.is_file():
            prior_kpis = _load_json(prior_kpi_path)
            _verify_snapshot_hash(prior_kpis, "kpi_hash")
        already_current = (
            self.state.get("state") == "COMPLETE"
            and self.state.get("evidence_bundle_path") == evidence_path
            and self.state.get("evidence_bundle_manifest_sha256") == evidence_sha
            and int(self.state.get("combine_episodes_completed", -1))
            == expected_episodes
            and prior_kpis is not None
            and prior_kpis.get("state") == "COMPLETE"
            and prior_kpis.get("campaign_id") == self.campaign_id
            and prior_kpis.get("manifest_hash") == self.manifest["manifest_hash"]
            and prior_kpis.get("source_commit") == self.manifest["source_commit"]
            and int(prior_kpis.get("combine_episodes_completed", -1))
            == expected_episodes
        )
        if already_current:
            return

        # Seed _kpis() from the immutable result so reconciliation also works
        # when the pre-terminal KPI snapshot was never created.  _publish then
        # aligns checkpoint sequence and hashes both live snapshots atomically.
        seed_kpis = dict(result_kpis)
        seed_kpis.update(
            {
                "schema": PRODUCTION_KPI_SCHEMA,
                "campaign_id": self.campaign_id,
                "manifest_hash": self.manifest["manifest_hash"],
                "source_commit": self.manifest["source_commit"],
                "state": "COMPLETE",
                "exact_account_replays": int(
                    counters.get("serious_exact_account_replays", 0)
                ),
                "combine_episodes_completed": expected_episodes,
                "normal_episodes_completed": int(
                    counters.get("normal_episodes_completed", expected_episodes // 2)
                ),
                "stressed_episodes_completed": int(
                    counters.get("stressed_episodes_completed", expected_episodes // 2)
                ),
                "candidates_promoted_96": promoted_96,
                "candidates_surviving_96": surviving_96,
                "candidates_promoted_192": surviving_96,
                "confirmation_ready_candidates": len(confirmation_ids),
                "matched_controls_status": matched_status,
                "null_status": str(
                    economic.get("null_status")
                    or result_kpis.get("null_status")
                    or "UNKNOWN"
                ),
            }
        )
        seed_kpis.pop("kpi_hash", None)
        seed_kpis["kpi_hash"] = stable_hash(seed_kpis)
        self.live_writer.write_json("production_kpis.json", seed_kpis)
        self._publish(
            state="COMPLETE",
            stage="CAMPAIGN_COMPLETE_RECONCILED_FROM_RESULT",
            next_action=str(next_action.get("action") or "CAMPAIGN_COMPLETE"),
            exact_account_replays=int(
                counters.get("serious_exact_account_replays", 0)
            ),
            combine_episodes_completed=expected_episodes,
            candidates_promoted_96=promoted_96,
            candidates_surviving_96=surviving_96,
            candidates_promoted_192=surviving_96,
            confirmation_ready_candidates=len(confirmation_ids),
            confirmation_ready_candidate_ids=confirmation_ids,
            matched_controls_status=matched_status,
            null_status=str(
                economic.get("null_status")
                or result_kpis.get("null_status")
                or "UNKNOWN"
            ),
            evidence_bundle_path=evidence_path,
            evidence_bundle_manifest_sha256=evidence_sha,
        )

    def _recover_sealed_bundle_result(self) -> dict[str, Any] | None:
        base_dir = self.root / str(self.manifest["evidence_bundle"]["destination"])
        final = base_dir / f"{self.campaign_id}.evidence-v1"
        if not final.is_dir():
            return None
        receipt_path = self.root / str(
            self.manifest["evidence_bundle"]["lightweight_manifest_path"]
        )
        identity = _load_json(final / "identity.json")
        if (
            identity.get("campaign_id") != self.campaign_id
            or identity.get("source_commit") != self.manifest["source_commit"]
            or identity.get("configuration_sha256") != _sha256(self.manifest_path)
        ):
            raise ProductionRuntimeError(
                "sealed EvidenceBundle identity differs from the frozen production manifest"
            )
        receipt = recover_finalized_evidence_bundle(
            base_dir,
            self.campaign_id,
            lightweight_manifest_path=receipt_path,
            expected_identity=identity,
        )
        outputs = {
            name: _load_json(final / "outputs" / f"{name}.json")
            for name in (
                "campaign_summary",
                "failure_vectors",
                "pareto_archive",
                "next_campaign_recommendations",
            )
        }
        prior_kpi_path = self.output_dir / "production_kpis.json"
        kpis = _load_json(prior_kpi_path) if prior_kpi_path.is_file() else self._kpis()
        if "kpi_hash" in kpis:
            _verify_snapshot_hash(kpis, "kpi_hash")
        pareto = outputs["pareto_archive"]
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=kpis,
            economic_results=outputs["campaign_summary"],
            successive_halving={
                "stage_decisions": list(pareto.get("stage_decisions") or ())
            },
            matched_controls=dict(
                pareto.get("crossfit")
                or {
                    "status": str(
                        outputs["campaign_summary"].get("matched_controls_status")
                        or "NOT_RUN"
                    )
                }
            ),
            failure_vectors=outputs["failure_vectors"],
            evidence_receipt=receipt.to_dict(),
            autonomous_next_action=outputs["next_campaign_recommendations"][
                "recommendation"
            ],
            scientific_status=(
                "BASKET_CONFIRMATION_READY_DEVELOPMENT_ONLY"
                if outputs["campaign_summary"].get("confirmation_ready_candidate_ids")
                else "DEVELOPMENT_WAVE_COMPLETE"
            ),
        )
        result_name = str(self.manifest["runtime"]["result_name"])
        self.output_writer.write_json(result_name, result)
        checked = load_and_verify_production_result(
            self.output_dir / result_name, self.manifest
        )
        self._reconcile_completed_result_snapshots(checked)
        return checked

    def _initial_state(self) -> dict[str, Any]:
        prior_path = self.output_dir / "production_state.json"
        if prior_path.is_file():
            prior = _load_json(prior_path)
            _verify_snapshot_hash(prior, "state_hash")
            if (
                prior.get("campaign_id") != self.campaign_id
                or prior.get("manifest_hash") != self.manifest.get("manifest_hash")
            ):
                raise ProductionRuntimeError("live state belongs to another manifest")
            return prior
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
            "next_action": "GENERATE_STRUCTURAL_POLICIES",
            "evidence_staging_path": str(
                self.root
                / str(self.manifest["evidence_bundle"]["destination"])
                / f".{self.campaign_id}.evidence-v1.staging"
            ),
            "evidence_final_path": str(
                self.root
                / str(self.manifest["evidence_bundle"]["destination"])
                / f"{self.campaign_id}.evidence-v1"
            ),
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        }

    def _publish(self, **updates: Any) -> None:
        self.state.update(updates)
        self._record_allocation_sample()
        self.state.update(
            {
                "schema": PRODUCTION_STATE_SCHEMA,
                "campaign_id": self.campaign_id,
                "manifest_hash": self.manifest["manifest_hash"],
                "source_commit": self.manifest["source_commit"],
                "checkpoint_sequence": int(self.state.get("checkpoint_sequence", 0)) + 1,
                "updated_at_utc": _utc_now(),
                "runner_pid": os.getpid(),
                "worker_count": 3,
                "evidence_writer_count": 1,
                "broker_connections": 0,
                "orders": 0,
                "q4_access_count_delta": 0,
                "data_purchase_count": 0,
            }
        )
        self.state.pop("state_hash", None)
        self.state["state_hash"] = stable_hash(self.state)
        self.live_writer.write_json("production_state.json", self.state)
        kpis = self._kpis()
        self.live_writer.write_json("production_kpis.json", kpis)

    def _record_allocation_sample(self) -> None:
        """Persist a bounded rolling two-hour hot/cold/admin allocation sample."""

        now_wall = time.perf_counter()
        clock = getattr(self, "clock", None)
        current_hot = float(getattr(clock, "hot_seconds", 0.0))
        current_cold = float(getattr(clock, "cold_seconds", 0.0))
        current_engineering = float(getattr(clock, "engineering_seconds", 0.0))
        wall_seconds = max(
            now_wall - float(getattr(self, "_allocation_last_wall", now_wall)),
            0.0,
        )
        hot_seconds = max(
            current_hot - float(getattr(self, "_allocation_last_hot", 0.0)), 0.0
        )
        cold_seconds = max(
            current_cold - float(getattr(self, "_allocation_last_cold", 0.0)), 0.0
        )
        engineering_seconds = max(
            current_engineering
            - float(getattr(self, "_allocation_last_engineering", 0.0)),
            0.0,
        )
        # Time not explicitly bracketed by an economic or safety operation is
        # administrative/runtime overhead.  This keeps the 80/10/10 audit tied
        # to wall clock rather than only to instrumented code paths.
        engineering_seconds += max(
            wall_seconds - hot_seconds - cold_seconds - engineering_seconds,
            0.0,
        )
        busy, total = _system_cpu_ticks()
        sample = {
            "ended_at_utc": _utc_now(),
            "wall_seconds": wall_seconds,
            "hot_economic_seconds": hot_seconds,
            "cold_safety_seconds": cold_seconds,
            "engineering_reporting_seconds": engineering_seconds,
            "host_cpu_busy_ticks": max(
                busy - int(getattr(self, "_allocation_last_busy", busy)), 0
            ),
            "host_cpu_total_ticks": max(
                total - int(getattr(self, "_allocation_last_total", total)), 0
            ),
        }
        cutoff = datetime.now(UTC).timestamp() - 7_200.0
        prior = self.state.get("rolling_two_hour_allocation_samples") or ()
        retained: list[dict[str, Any]] = []
        if isinstance(prior, Sequence) and not isinstance(prior, (str, bytes)):
            for raw in prior:
                if not isinstance(raw, Mapping):
                    continue
                try:
                    ended = datetime.fromisoformat(
                        str(raw["ended_at_utc"]).replace("Z", "+00:00")
                    ).timestamp()
                except (KeyError, TypeError, ValueError):
                    continue
                if ended >= cutoff:
                    retained.append(dict(raw))
        retained.append(sample)
        self.state["rolling_two_hour_allocation_samples"] = retained[-2_000:]
        self._allocation_last_wall = now_wall
        self._allocation_last_hot = current_hot
        self._allocation_last_cold = current_cold
        self._allocation_last_engineering = current_engineering
        self._allocation_last_busy = busy
        self._allocation_last_total = total

    def _rolling_allocation_metrics(self) -> dict[str, float]:
        samples = self.state.get("rolling_two_hour_allocation_samples") or ()
        rows = [dict(row) for row in samples if isinstance(row, Mapping)]
        wall = sum(max(float(row.get("wall_seconds", 0.0)), 0.0) for row in rows)
        hot = sum(max(float(row.get("hot_economic_seconds", 0.0)), 0.0) for row in rows)
        cold = sum(max(float(row.get("cold_safety_seconds", 0.0)), 0.0) for row in rows)
        engineering = sum(
            max(float(row.get("engineering_reporting_seconds", 0.0)), 0.0)
            for row in rows
        )
        busy = sum(max(int(row.get("host_cpu_busy_ticks", 0)), 0) for row in rows)
        total = sum(max(int(row.get("host_cpu_total_ticks", 0)), 0) for row in rows)
        denominator = max(wall, 1e-9)
        return {
            "window_seconds": wall,
            "hot_fraction": min(max(hot / denominator, 0.0), 1.0),
            "cold_fraction": min(max(cold / denominator, 0.0), 1.0),
            "engineering_fraction": min(
                max(engineering / denominator, 0.0), 1.0
            ),
            "host_cpu_fraction": min(max(busy / max(total, 1), 0.0), 1.0),
        }

    def _campaign_elapsed_hours(self) -> float:
        try:
            started = datetime.fromisoformat(
                str(self.state["started_at_utc"]).replace("Z", "+00:00")
            )
            seconds = (datetime.now(UTC) - started).total_seconds()
        except (KeyError, TypeError, ValueError):
            clock = getattr(self, "clock", None)
            seconds = float(getattr(clock, "elapsed", 1.0))
        return max(seconds / 3_600.0, 1e-9)

    def _kpis(self) -> dict[str, Any]:
        rows = list(self.summaries.values())
        allocation = self._rolling_allocation_metrics()
        elapsed_hours = self._campaign_elapsed_hours()
        episodes = int(self.state.get("combine_episodes_completed", 0))
        state_exact = int(self.state.get("exact_account_replays", len(rows)))
        exact = len(rows) if rows else state_exact
        rates_per_hour = {
            "policies_proposed": int(self.state.get("policies_proposed", 0))
            / elapsed_hours,
            "unique_policies_screened": int(
                self.state.get("unique_policies_screened", 0)
            )
            / elapsed_hours,
            "exact_account_replays": exact / elapsed_hours,
            "combine_episodes": episodes / elapsed_hours,
        }
        prior_path = self.output_dir / "production_kpis.json"
        if not rows and prior_path.is_file():
            prior = _load_json(prior_path)
            _verify_snapshot_hash(prior, "kpi_hash")
            prior.update(
                {
                    "checkpoint_sequence": int(
                        self.state.get("checkpoint_sequence", 0)
                    ),
                    "updated_at_utc": _utc_now(),
                    "state": self.state.get("state"),
                    "policies_proposed": int(
                        self.state.get("policies_proposed", 0)
                    ),
                    "unique_policies_screened": int(
                        self.state.get("unique_policies_screened", 0)
                    ),
                    "exact_account_replays": exact,
                    "combine_episodes_completed": episodes,
                    "normal_episodes_completed": episodes // 2,
                    "stressed_episodes_completed": episodes // 2,
                    "rates_per_hour": rates_per_hour,
                    "rates_per_hour_scope": "CAMPAIGN_CUMULATIVE",
                    "economic_research_wall_clock_fraction": allocation[
                        "hot_fraction"
                    ],
                    "cold_safety_wall_clock_fraction": allocation[
                        "cold_fraction"
                    ],
                    "engineering_reporting_wall_clock_fraction": allocation[
                        "engineering_fraction"
                    ],
                    "allocation_window": "ROLLING_TWO_HOURS",
                    "allocation_window_seconds": allocation["window_seconds"],
                    "cpu_utilization_fraction": allocation["host_cpu_fraction"],
                    "cpu_utilization_scope": "HOST_WIDE_ROLLING_TWO_HOURS",
                    "workers": {"compute": 3, "evidence_writer": 1},
                    "cache_hit_rate": (
                        float(self.cache_hit_rate)
                        if self.cache_hit_rate > 0.0
                        else float(prior.get("cache_hit_rate", 0.0))
                    ),
                    "duplicate_rejection_rate": float(
                        self.population_summary.get(
                            "duplicate_rejection_rate",
                            prior.get("duplicate_rejection_rate", 0.0),
                        )
                    ),
                    "candidates_promoted_96": int(
                        self.state.get(
                            "candidates_promoted_96",
                            prior.get("candidates_promoted_96", 0),
                        )
                    ),
                    "candidates_surviving_96": int(
                        self.state.get(
                            "candidates_surviving_96",
                            prior.get("candidates_surviving_96", 0),
                        )
                    ),
                    "candidates_promoted_192": int(
                        self.state.get(
                            "candidates_promoted_192",
                            prior.get("candidates_promoted_192", 0),
                        )
                    ),
                    "confirmation_ready_candidates": int(
                        self.state.get(
                            "confirmation_ready_candidates",
                            prior.get("confirmation_ready_candidates", 0),
                        )
                    ),
                    "matched_controls_status": str(
                        self.state.get(
                            "matched_controls_status",
                            prior.get("matched_controls_status", "PENDING"),
                        )
                    ),
                    "null_status": str(
                        self.state.get(
                            "null_status", prior.get("null_status", "PENDING")
                        )
                    ),
                    "admin_overhead_alert": bool(
                        allocation["cold_fraction"]
                        + allocation["engineering_fraction"]
                        > 0.20
                        and self.state.get("state") != "FAILED_CLOSED"
                    ),
                }
            )
            prior.pop("kpi_hash", None)
            prior["kpi_hash"] = stable_hash(prior)
            return prior
        normal_rates = [float(row["normal"]["pass_rate"]) for row in rows]
        stress_rates = [float(row["stressed_1_5x"]["pass_rate"]) for row in rows]
        value: dict[str, Any] = {
            "schema": PRODUCTION_KPI_SCHEMA,
            "campaign_id": self.campaign_id,
            "manifest_hash": self.manifest["manifest_hash"],
            "source_commit": self.manifest["source_commit"],
            "checkpoint_sequence": int(self.state.get("checkpoint_sequence", 0)),
            "updated_at_utc": _utc_now(),
            "state": self.state.get("state"),
            "policies_proposed": int(self.state.get("policies_proposed", 0)),
            "unique_policies_screened": int(self.state.get("unique_policies_screened", 0)),
            "exact_account_replays": exact,
            "combine_episodes_completed": episodes,
            "normal_episodes_completed": episodes // 2,
            "stressed_episodes_completed": episodes // 2,
            "positive_stressed_net_candidates": sum(
                float(row["stressed_1_5x"]["median_episode_net_pnl"]) > 0.0 for row in rows
            ),
            "candidates_with_normal_pass": sum(rate > 0.0 for rate in normal_rates),
            "candidates_with_stressed_pass": sum(rate > 0.0 for rate in stress_rates),
            "best_normal_pass_rate": max(normal_rates, default=0.0),
            "best_stressed_pass_rate": max(stress_rates, default=0.0),
            "median_normal_pass_rate": float(np.median(normal_rates)) if normal_rates else 0.0,
            "median_stressed_pass_rate": float(np.median(stress_rates)) if stress_rates else 0.0,
            "near_pass_count": sum(
                int(row["normal"]["pass_count"]) == 0
                and float(row["normal"]["target_progress_median"]) >= 0.70
                for row in rows
            ),
            "candidates_promoted_96": int(
                self.state.get("candidates_promoted_96", 0)
            ),
            "candidates_surviving_96": int(
                self.state.get("candidates_surviving_96", 0)
            ),
            "candidates_promoted_192": int(
                self.state.get("candidates_promoted_192", 0)
            ),
            "confirmation_ready_candidates": int(
                self.state.get("confirmation_ready_candidates", 0)
            ),
            "duplicate_rejection_rate": float(
                self.population_summary.get("duplicate_rejection_rate", 0.0)
            ),
            "mechanism_counts": dict(
                self.population_summary.get("mechanism_counts") or {}
            ),
            "target_velocity_policy_count": int(
                (self.population_summary.get("mechanism_counts") or {}).get(
                    "TARGET_VELOCITY_MLL_PROTECTION", 0
                )
            ),
            "market_exclusions": dict(
                self.manifest["component_bank"].get("market_exclusions") or {}
            ),
            "cache_hit_rate": float(self.cache_hit_rate),
            "economic_research_wall_clock_fraction": allocation["hot_fraction"],
            "cold_safety_wall_clock_fraction": allocation["cold_fraction"],
            "engineering_reporting_wall_clock_fraction": allocation[
                "engineering_fraction"
            ],
            "allocation_window": "ROLLING_TWO_HOURS",
            "allocation_window_seconds": allocation["window_seconds"],
            "cpu_utilization_fraction": allocation["host_cpu_fraction"],
            "cpu_utilization_scope": "HOST_WIDE_ROLLING_TWO_HOURS",
            "rates_per_hour": rates_per_hour,
            "rates_per_hour_scope": "CAMPAIGN_CUMULATIVE",
            "workers": {"compute": 3, "evidence_writer": 1},
            "matched_controls_status": str(
                self.state.get(
                    "matched_controls_status", "PENDING_STAGE_4_NOT_EXECUTED"
                )
            ),
            "null_status": str(
                self.state.get("null_status", "PENDING_STAGE_4_NOT_EXECUTED")
            ),
            "admin_overhead_alert": bool(
                allocation["cold_fraction"]
                + allocation["engineering_fraction"]
                > 0.20
                and self.state.get("state") != "FAILED_CLOSED"
            ),
            "broker_connections": 0,
            "orders": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
        }
        value["kpi_hash"] = stable_hash(value)
        return value

    def _verify_source_commit(self) -> None:
        source = str(self.manifest["source_commit"])
        completed = subprocess.run(
            ["git", "merge-base", "--is-ancestor", source, "HEAD"],
            cwd=self.root,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise ProductionRuntimeError(
                f"manifest source commit {source} is not an ancestor of live HEAD"
            )
        for relative, claimed in self.manifest["implementation_files"].items():
            if _sha256(self.root / str(relative)) != str(claimed):
                raise ProductionRuntimeError(
                    f"live production implementation checksum drift: {relative}"
                )

    def _audit_cemetery(self) -> dict[str, Any]:
        path = self.root / "mission/state/graveyard.db"
        if not path.is_file():
            raise ProductionRuntimeError("authoritative graveyard DB is missing")
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            rows = connection.execute(
                "SELECT mechanism_class, regime, death_cause, signature_hash "
                "FROM class_tombstones ORDER BY mechanism_class, regime, death_cause"
            ).fetchall()
        finally:
            connection.close()
        generated = set(str(row) for row in self.manifest["policy_classes"])
        collisions = sorted(
            str(row[0]) for row in rows if str(row[0]) in generated
        )
        if collisions:
            raise ProductionRuntimeError(f"generated mechanism is tombstoned: {collisions}")
        return {
            "path": str(path),
            "sha256": _sha256(path),
            "tombstone_count": len(rows),
            "generated_class_collisions": collisions,
            "checked": True,
            "audit_scope": "CLASS_NAME_ONLY_NO_STRUCTURAL_FINGERPRINT_MATCH",
            "structural_fingerprint_enforcement": False,
        }

    def _load_or_generate_population(
        self, components: Sequence[ComponentCandidate]
    ) -> tuple[ProductionPolicy, ...]:
        path = self.payload_dir / "structural_policies.jsonl"
        started = time.perf_counter()
        if path.is_file():
            policies = tuple(
                ProductionPolicy.from_dict(json.loads(line))
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
            summary = _load_json(self.payload_dir / "structural_population_summary.json")
        else:
            population = generate_policy_population(
                components,
                self.manifest,
                count=20_000,
            )
            policies = population.policies
            summary = population.summary()
            self.payload_writer.write_jsonl_batch(
                "structural_policies.jsonl", [row.to_dict() for row in policies]
            )
            self.payload_writer.write_json("structural_population_summary.json", summary)
        self.population_summary = summary
        self.clock.hot_seconds += time.perf_counter() - started
        self._publish(
            state="POPULATION_FROZEN",
            stage="STAGE_0",
            policies_proposed=len(policies),
            next_action="FAST_ECONOMIC_SCREEN",
        )
        return policies

    def _load_or_screen(
        self,
        policies: Sequence[ProductionPolicy],
        components: Mapping[str, ComponentCandidate],
    ) -> tuple[list[dict[str, Any]], tuple[ProductionPolicy, ...]]:
        path = self.payload_dir / "fast_screen_results.jsonl"
        started = time.perf_counter()
        if path.is_file():
            rows = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            by_id = {row.policy_id: row for row in policies}
            selected = tuple(
                by_id[str(row["policy_id"])]
                for row in rows
                if row.get("selected_for_exact_replay")
            )
        else:
            rows, selected = fast_economic_screen(
                policies,
                components,
                survivor_limit=1_024,
            )
            self.payload_writer.write_jsonl_batch("fast_screen_results.jsonl", rows)
            self.payload_writer.write_json(
                "fast_screen_summary.json",
                {
                    "screened_count": len(rows),
                    "eligible_count": sum(bool(row["fast_screen_survivor"]) for row in rows),
                    "selected_count": len(selected),
                    "outcome_claim": False,
                },
            )
        self.clock.hot_seconds += time.perf_counter() - started
        return rows, selected

    def _open_cached_matrices(self) -> dict[str, FeatureMatrix]:
        started = time.perf_counter()
        expected = str(self.manifest["data"]["feature_source_fingerprint"])
        found: dict[str, FeatureMatrix] = {}
        transformations: set[str] = set()
        dags: set[str] = set()
        cache_manifests: dict[str, str] = {}
        for manifest_path in sorted(self.cache_root.glob("*/manifest.json")):
            value = _load_json(manifest_path)
            provenance = value.get("provenance") or {}
            market = str(provenance.get("market") or "")
            if provenance.get("data_fingerprint") != expected or market not in self.manifest["markets"]:
                continue
            key = value.get("key") or {}
            if (
                key.get("start_inclusive") != self.manifest["data"]["period"][0]
                or key.get("end_exclusive") != self.manifest["data"]["period"][1]
                or key.get("roll_map_hash") != self.manifest["data"]["contract_map_sha256"]
            ):
                raise ProductionRuntimeError(f"canonical feature key drift for {market}")
            if not set(str(row) for row in self.manifest["timeframes"]).issubset(
                {str(row) for row in key.get("timeframes") or ()}
            ):
                raise ProductionRuntimeError(
                    f"canonical feature timeframes incomplete for {market}"
                )
            transformation = str(key.get("transformation_version") or "")
            dag = str(key.get("feature_dag_hash") or "")
            if not transformation or len(dag) != 64:
                raise ProductionRuntimeError(
                    f"canonical feature transformation provenance missing for {market}"
                )
            transformations.add(transformation)
            dags.add(dag)
            cache_manifests[market] = _sha256(manifest_path)
            required_arrays = {
                "decision_ns",
                "timestamp_ns",
                "session_day",
                "session_code",
                "entry_price",
                *(f"forward_move__{value}" for value in (5, 15, 30, 60)),
            }
            if not required_arrays.issubset(set((value.get("arrays") or {}).keys())):
                raise ProductionRuntimeError(f"canonical feature horizons incomplete for {market}")
            if market in found:
                raise ProductionRuntimeError(f"ambiguous canonical feature cache for {market}")
            found[market] = FeatureMatrix.open(manifest_path.parent, mmap=True)
        missing = sorted(set(self.manifest["markets"]) - set(found))
        if missing:
            raise ProductionRuntimeError(
                "cache-only preflight failed before any feature builder call: " + ",".join(missing)
            )
        if len(transformations) != 1 or len(dags) != 1:
            raise ProductionRuntimeError(
                "canonical transformation_version/feature_dag_hash drift across markets"
            )
        self.feature_cache_fingerprints = {
            "feature_transformation_version": stable_hash(
                {"value": next(iter(transformations))}
            ),
            "feature_dag_hash": next(iter(dags)),
            **{
                f"feature_manifest:{market}": digest
                for market, digest in sorted(cache_manifests.items())
            },
        }
        verify_runtime_inputs(
            self.manifest,
            contract_map_path=self.contract_map_path,
            feature_source_fingerprint=expected,
        )
        self.cache_hit_rate = 1.0
        self.clock.cold_seconds += time.perf_counter() - started
        return found

    def _compile_components(
        self,
        components: Sequence[ComponentCandidate],
        sleeve_ids: set[str],
        matrices: Mapping[str, FeatureMatrix],
    ) -> tuple[dict[str, ExactSleeveRuntime], list[dict[str, str]]]:
        started = time.perf_counter()
        selected = tuple(
            row.sleeve for row in components if row.sleeve.sleeve_id in sleeve_ids
        )
        materialization = self.manifest["component_materialization"]
        calibration = materialization["calibration_period"]
        exact = materialization["exact_replay_period"]
        policy = CheapScreenPolicy(
            calibration_start=str(calibration[0]),
            calibration_end_exclusive=str(calibration[1]),
            screen_start="2023-07-01",
            screen_end_exclusive="2024-01-01",
            minimum_opportunities=20,
            stress_cost_multiplier=1.5,
            maximum_best_positive_event_share=0.40,
            maximum_approximate_drawdown=4_500.0,
            require_nonnegative_half=False,
            micro_batch_size=64,
        )
        bound = _bind_selected(selected, matrices, policy=policy)
        runtimes, failures = _build_exact_runtimes(
            bound,
            matrices,
            start_inclusive=str(exact[0]),
            end_exclusive=str(exact[1]),
            worker_count=3,
        )
        self.clock.hot_seconds += time.perf_counter() - started
        self.payload_writer.write_jsonl_batch(
            "component_runtime_summaries.jsonl",
            [runtime.to_dict(include_events=False) for runtime in runtimes.values()],
        )
        self._publish(
            state="FAST_SCREEN_COMPLETE",
            stage="COMPONENT_MATERIALIZATION",
            next_action="PERSIST_COMPONENT_EVIDENCE",
            component_runtime_count=len(runtimes),
            component_compile_failure_count=len(failures),
        )
        return runtimes, failures

    def _open_evidence(
        self,
        policies: Sequence[ProductionPolicy],
        components: Mapping[str, ComponentCandidate],
        runtimes: Mapping[str, ExactSleeveRuntime],
        starts: Sequence[int],
    ) -> AsyncEvidenceBundleSink:
        component_ids = sorted({value for policy in policies for value in policy.sleeve_ids})
        policy_ids = [policy.policy_id for policy in policies]
        required = [
            {
                "policy_id": policy_id,
                "episode_id": f"{policy_id}:{start}",
                "horizon": "60_TRADING_DAYS",
            }
            for policy_id in policy_ids
            for start in starts
        ]
        data_fingerprints = {
            "canonical_feature_source": str(self.manifest["data"]["feature_source_fingerprint"]),
            "contract_map": str(self.manifest["data"]["contract_map_sha256"]),
            **self.feature_cache_fingerprints,
        }
        for name, source in self.manifest["component_bank"]["sources"].items():
            data_fingerprints[f"source:{name}"] = str(source["file_sha256"])
        access_ledger = self.root / "reports/data_access/data_access_ledger.jsonl"
        graveyard = self.root / "mission/state/graveyard.db"
        data_fingerprints["data_access_ledger"] = _sha256(access_ledger)
        data_fingerprints["graveyard"] = _sha256(graveyard)
        identity = {
            "campaign_id": self.campaign_id,
            "grammar_id": str(self.manifest["class_id"]),
            "policy_fingerprints": {
                policy.policy_id: policy.structural_fingerprint for policy in policies
            },
            "component_fingerprints": {
                component_id: components[component_id].sleeve.structural_fingerprint
                for component_id in component_ids
            },
            "source_commit": str(self.manifest["source_commit"]),
            "data_fingerprints": data_fingerprints,
            "configuration_sha256": _sha256(self.manifest_path),
            "seeds": [
                int(self.manifest["generator"]["seed"]),
                *[int(row) for row in self.manifest["matched_controls"]["random_seeds"]],
            ],
            "created_at_utc": str(self.manifest["created_at_utc"]),
            "expected_coverage": {
                "policy_ids": sorted(policy_ids),
                "component_ids": component_ids,
                "required_episode_keys": required,
                "allowed_horizons": [
                    f"{int(value)}_TRADING_DAYS"
                    for value in self.manifest["successive_halving"]["frozen_horizons"]
                ] + ["FULL_AVAILABLE_CHRONOLOGICAL_HORIZON"],
                "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
                "allow_additional_episode_keys": True,
            },
        }
        self.evidence_identity = identity
        base_dir = self.root / str(self.manifest["evidence_bundle"]["destination"])
        staging = base_dir / f".{self.campaign_id}.evidence-v1.staging"
        return AsyncEvidenceBundleSink(
            base_dir=base_dir,
            identity=identity,
            writer_id=f"production-kernel:{self.campaign_id}",
            resume=staging.is_dir(),
        )

    def _persist_static_evidence(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ProductionPolicy],
        components: Mapping[str, ComponentCandidate],
        runtimes: Mapping[str, ExactSleeveRuntime],
        matrices: Mapping[str, FeatureMatrix],
    ) -> None:
        started = time.perf_counter()
        memberships = [
            {
                "campaign_id": self.campaign_id,
                "policy_id": policy.policy_id,
                "component_id": component_id,
                "risk_allocation": float(policy.risk_micro_units),
                "component_role": (
                    components[component_id].sleeve.role.value
                    + "|"
                    + ",".join(components[component_id].source_roles)
                ),
            }
            for policy in policies
            for component_id in policy.sleeve_ids
        ]
        _append_chunks(
            sink,
            "account_policy_membership",
            memberships,
            prefix="static-membership",
            size=5_000,
        )
        used = {value for policy in policies for value in policy.sleeve_ids}
        ledgers = materialize_component_evidence(
            self.campaign_id,
            {key: value for key, value in runtimes.items() if key in used},
            components,
            matrices,
        )
        for dataset, rows in ledgers.items():
            _append_chunks(
                sink,
                dataset,
                rows,
                prefix=f"fresh-component-{dataset}",
                size=5_000,
            )
        identity = self.evidence_identity
        checksums = {"configuration": str(identity["configuration_sha256"])}
        checksums.update(
            {
                f"data:{key}": str(value)
                for key, value in identity["data_fingerprints"].items()
            }
        )
        sink.append_records(
            "provenance",
            [
                {
                    "campaign_id": self.campaign_id,
                    "validator_version": "hydra_evidence_bundle_validator_v1",
                    "replay_version": PRODUCTION_REPLAY_VERSION,
                    "market_data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
                    "access_ledger_sha256": str(
                        identity["data_fingerprints"]["data_access_ledger"]
                    ),
                    "reconstruction_flag": False,
                    "immutable_checksums": checksums,
                    "recorded_at_utc": str(self.manifest["created_at_utc"]),
                }
            ],
            batch_id="fresh-development-provenance-v1",
        )
        sink.checkpoint(
            {
                "stage": "COMPONENT_LEDGER_COMPLETE",
                "component_count": len(used),
                "component_trade_count": len(ledgers["component_trades"]),
            }
        )
        sink.flush()
        self.clock.cold_seconds += time.perf_counter() - started
        self._publish(
            state="COMPONENT_LEDGER_COMPLETE",
            stage="EVIDENCE_MATERIALIZATION",
            next_action="START_EXACT_ACCOUNT_REPLAY",
            component_trade_count=len(ledgers["component_trades"]),
        )

    def _run_exact_batches(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ProductionPolicy],
        runtimes: Mapping[str, ExactSleeveRuntime],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
    ) -> None:
        summary_dir = self.payload_dir / "exact_summaries"
        summary_dir.mkdir(parents=True, exist_ok=True)
        for path in sorted(summary_dir.glob("*.json")):
            row = _load_json(path)
            episode_path = self.payload_dir / "exact_episode_rows" / path.name
            if (
                row.get("schema") == "hydra_production_policy_replay_v1"
                and episode_path.is_file()
            ):
                _load_episode_cache(episode_path, expected_policy_id=path.stem)
                self.summaries[str(row["policy"]["policy_id"])] = row
        remaining = [row for row in policies if row.policy_id not in self.summaries]
        started = time.perf_counter()
        for batch in evaluate_policy_batch_parallel(
            remaining,
            runtimes,
            starts=starts,
            horizon=60,
            worker_count=3,
            batch_size=8,
            eligible_days_by_start=calendars,
        ):
            pending_summaries: list[
                tuple[str, dict[str, Any], list[dict[str, Any]]]
            ] = []
            for replay in batch:
                policy_id = str(replay["policy"]["policy_id"])
                if replay.get("schema") != "hydra_production_policy_replay_v1":
                    self.payload_writer.write_json(
                        f"exact_failures/{policy_id}.json", replay
                    )
                    continue
                episodes, paths = replay_evidence_rows(replay, self.manifest)
                sink.append_records(
                    "episodes",
                    episodes,
                    batch_id=f"exact-60:{policy_id}:episodes",
                )
                sink.append_records(
                    "account_daily_paths",
                    paths,
                    batch_id=f"exact-60:{policy_id}:daily-paths",
                )
                summary = {
                    key: value
                    for key, value in replay.items()
                    if key not in {"normal_episodes", "stressed_episodes"}
                }
                pending_summaries.append((policy_id, summary, episodes))
            sink.flush()
            summary_writer = AtomicResultWriter(summary_dir)
            for policy_id, summary, episodes in pending_summaries:
                self._write_durable_episode_cache(
                    f"exact_episode_rows/{policy_id}.json",
                    policy_id=policy_id,
                    horizon="60_TRADING_DAYS",
                    episodes=episodes,
                )
                summary_writer.write_json(f"{policy_id}.json", summary)
                self.summaries[policy_id] = summary
            sink.checkpoint(
                {
                    "stage": "EXACT_REPLAY_ACTIVE",
                    "exact_account_replays": len(self.summaries),
                    "combine_episodes_completed": self._durable_episode_total(),
                }
            )
            sink.flush()
            self.clock.hot_seconds += time.perf_counter() - started
            started = time.perf_counter()
            exact = len(self.summaries)
            self._publish(
                state=(
                    "FIRST_HALVING_COMPLETE" if exact >= 512 else "EXACT_REPLAY_ACTIVE"
                ),
                stage="STAGE_2",
                exact_account_replays=exact,
                combine_episodes_completed=self._durable_episode_total(),
                last_completed_policy_id=(
                    str(batch[-1]["policy"]["policy_id"]) if batch else None
                ),
                next_action=(
                    "RANK_STAGE2_PARETO" if exact >= 512 else "CONTINUE_EXACT_REPLAY"
                ),
            )
            if self.stop_after == "FIRST_HALVING" and exact >= 512:
                return

    def _run_additional_horizons(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ProductionPolicy],
        runtimes: Mapping[str, ExactSleeveRuntime],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
    ) -> int:
        cache = AtomicResultWriter(self.payload_dir / "horizon_summaries")
        completed_episode_count = 0
        for horizon in (20, 40, 90):
            horizon_dir = self.payload_dir / "horizon_summaries" / str(horizon)
            episode_dir = self.payload_dir / "horizon_episode_rows" / str(horizon)
            completed = {
                path.stem
                for path in horizon_dir.glob("*.json")
                if path.is_file() and (episode_dir / path.name).is_file()
            }
            completed_episode_count += len(completed) * len(starts) * 2
            remaining = [row for row in policies if row.policy_id not in completed]
            batch_started = time.perf_counter()
            for batch in evaluate_policy_batch_parallel(
                remaining,
                runtimes,
                starts=starts,
                horizon=horizon,
                worker_count=3,
                batch_size=8,
                eligible_days_by_start=calendars,
            ):
                pending_summaries: list[
                    tuple[str, dict[str, Any], list[dict[str, Any]]]
                ] = []
                for replay in batch:
                    policy_id = str(replay["policy"]["policy_id"])
                    if replay.get("schema") != "hydra_production_policy_replay_v1":
                        self.payload_writer.write_json(
                            f"horizon_failures/{horizon}/{policy_id}.json", replay
                        )
                        continue
                    episodes, paths = replay_evidence_rows(replay, self.manifest)
                    sink.append_records(
                        "episodes",
                        episodes,
                        batch_id=f"exact-{horizon}:{policy_id}:episodes",
                    )
                    sink.append_records(
                        "account_daily_paths",
                        paths,
                        batch_id=f"exact-{horizon}:{policy_id}:daily-paths",
                    )
                    summary = {
                        key: value
                        for key, value in replay.items()
                        if key not in {"normal_episodes", "stressed_episodes"}
                    }
                    pending_summaries.append((policy_id, summary, episodes))
                sink.flush()
                for policy_id, summary, episodes in pending_summaries:
                    self._write_durable_episode_cache(
                        f"horizon_episode_rows/{horizon}/{policy_id}.json",
                        policy_id=policy_id,
                        horizon=f"{horizon}_TRADING_DAYS",
                        episodes=episodes,
                    )
                    cache.write_json(f"{horizon}/{policy_id}.json", summary)
                    completed_episode_count += len(starts) * 2
                sink.checkpoint(
                    {
                        "stage": "STAGE_3",
                        "horizon": horizon,
                        "combine_episodes_completed": self._durable_episode_total(),
                    }
                )
                sink.flush()
                self.clock.hot_seconds += time.perf_counter() - batch_started
                batch_started = time.perf_counter()
                self._publish(
                    state="ROBUSTNESS_ACTIVE",
                    stage="STAGE_3",
                    exact_account_replays=len(self.summaries),
                    combine_episodes_completed=self._durable_episode_total(),
                    next_action=f"CONTINUE_HORIZON_{horizon}",
                )
        return completed_episode_count

    def _run_auxiliary_evidence_batches(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ProductionPolicy],
        runtimes: Mapping[str, ExactSleeveRuntime],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        *,
        horizon: int,
        horizon_label: str,
        namespace: str,
    ) -> list[dict[str, Any]]:
        """Replay, persist, and cache one frozen auxiliary evaluation exactly once."""

        cache_dir = self.payload_dir / namespace
        cached: dict[str, list[dict[str, Any]]] = {}
        for policy in policies:
            path = cache_dir / f"{policy.policy_id}.json"
            if path.is_file():
                cached[policy.policy_id] = _load_episode_cache(
                    path,
                    expected_policy_id=policy.policy_id,
                    expected_horizon=horizon_label,
                )
        remaining = [row for row in policies if row.policy_id not in cached]
        started = time.perf_counter()
        completed = len(cached)
        for batch in evaluate_policy_batch_parallel(
            remaining,
            runtimes,
            starts=starts,
            horizon=horizon,
            worker_count=3,
            batch_size=8,
            eligible_days_by_start=calendars,
        ):
            pending: list[tuple[str, list[dict[str, Any]]]] = []
            for replay in batch:
                policy_id = str(replay["policy"]["policy_id"])
                if replay.get("schema") != "hydra_production_policy_replay_v1":
                    self.payload_writer.write_json(
                        f"{namespace}_failures/{policy_id}.json", replay
                    )
                    continue
                if horizon_label == "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON":
                    for key in ("normal_episodes", "stressed_episodes"):
                        for raw in replay[key]:
                            raw["horizon_label"] = horizon_label
                episodes, paths = replay_evidence_rows(replay, self.manifest)
                sink.append_records(
                    "episodes",
                    episodes,
                    batch_id=f"{namespace}:{policy_id}:episodes",
                )
                sink.append_records(
                    "account_daily_paths",
                    paths,
                    batch_id=f"{namespace}:{policy_id}:daily-paths",
                )
                pending.append((policy_id, episodes))
            sink.flush()
            for policy_id, episodes in pending:
                self._write_durable_episode_cache(
                    f"{namespace}/{policy_id}.json",
                    policy_id=policy_id,
                    horizon=horizon_label,
                    episodes=episodes,
                )
                cached[policy_id] = episodes
            completed += len(pending)
            sink.checkpoint(
                {
                    "stage": namespace.upper(),
                    "completed_policy_count": completed,
                    "required_policy_count": len(policies),
                    "combine_episodes_completed": self._durable_episode_total(),
                }
            )
            sink.flush()
            self.clock.hot_seconds += time.perf_counter() - started
            started = time.perf_counter()
            self._publish(
                state="ROBUSTNESS_ACTIVE",
                stage=namespace.upper(),
                combine_episodes_completed=self._durable_episode_total(),
                next_action=f"CONTINUE_{namespace.upper()}",
            )
        if len(cached) != len(policies):
            raise ProductionRuntimeError(
                f"{namespace} incomplete: {len(cached)}/{len(policies)}"
            )
        expected_rows = len(starts) * 2
        for policy_id, rows in cached.items():
            if len(rows) != expected_rows:
                raise ProductionRuntimeError(
                    f"{namespace} episode coverage drift for {policy_id}: "
                    f"{len(rows)}/{expected_rows}"
                )
        return [
            row
            for policy in policies
            for row in cached[policy.policy_id]
        ]

    def _durable_episode_total(self) -> int:
        """Return the verified durable total without rescanning unchanged caches."""

        if self._durable_episode_count is None:
            self._durable_episode_count = _durable_episode_cache_count(
                self.payload_dir
            )
        return self._durable_episode_count

    def _write_durable_episode_cache(
        self,
        relative_path: str,
        *,
        policy_id: str,
        horizon: str,
        episodes: Sequence[Mapping[str, Any]],
    ) -> AtomicWriteReceipt:
        """Atomically cache rows and advance the total only for a new file.

        The absolute total is initialized before the write.  If the process
        dies after the atomic rename but before the in-memory increment, the
        next process recovers the correct value with its one initial scan.
        """

        prior_total = self._durable_episode_total()
        receipt = _write_episode_cache(
            self.payload_writer,
            relative_path,
            policy_id=policy_id,
            horizon=horizon,
            episodes=episodes,
        )
        if not receipt.idempotent_existing:
            self._durable_episode_count = prior_total + len(episodes)
        return receipt

    def _run_expanded_horizon_set(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ProductionPolicy],
        runtimes: Mapping[str, ExactSleeveRuntime],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        *,
        namespace: str,
    ) -> dict[str, list[dict[str, Any]]]:
        output: dict[str, list[dict[str, Any]]] = {}
        for horizon in (20, 40, 60, 90):
            label = f"{horizon}_TRADING_DAYS"
            output[label] = self._run_auxiliary_evidence_batches(
                sink,
                policies,
                runtimes,
                starts,
                calendars,
                horizon=horizon,
                horizon_label=label,
                namespace=f"{namespace}_{horizon}_episode_rows",
            )
        full_label = "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON"
        output[full_label] = self._run_auxiliary_evidence_batches(
            sink,
            policies,
            runtimes,
            starts,
            calendars,
            horizon=max(len(value) for value in calendars.values()),
            horizon_label=full_label,
            namespace=f"{namespace}_full_episode_rows",
        )
        return output


def _verify_snapshot_hash(value: Mapping[str, Any], field: str) -> None:
    payload = dict(value)
    claimed = str(payload.pop(field, ""))
    if not claimed or stable_hash(payload) != claimed:
        raise ProductionRuntimeError(f"live snapshot hash drift: {field}")


def _append_chunks(
    sink: AsyncEvidenceBundleSink,
    dataset: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    prefix: str,
    size: int,
) -> None:
    for index in range(0, len(rows), size):
        sink.append_records(
            dataset,
            rows[index : index + size],
            batch_id=f"{prefix}:{index // size:06d}",
        )


def _routing_is_executable(
    policy: ProductionPolicy,
    runtimes: Mapping[str, ExactSleeveRuntime],
) -> bool:
    try:
        transformed, _ = transform_policy_events(
            policy, {key: runtimes[key] for key in policy.sleeve_ids}
        )
    except (ValueError, KeyError):
        return False
    return all(transformed.get(key) for key in policy.sleeve_ids)


def _block_aware_starts(
    runtimes: Mapping[str, ExactSleeveRuntime],
    manifest: Mapping[str, Any],
    *,
    maximum: int,
    required_starts: Sequence[int] = (),
) -> tuple[int, ...]:
    values = list(runtimes.values())
    if not values:
        return ()
    common = set(values[0].eligible_session_days)
    for runtime in values[1:]:
        common.intersection_update(runtime.eligible_session_days)
    blocks = list(manifest["temporal_blocks"]["blocks"])
    required = tuple(int(value) for value in required_starts)
    if len(set(required)) != len(required) or len(required) > maximum:
        raise ProductionRuntimeError("nested block-aware start requirement is invalid")
    quota, remainder = divmod(maximum, len(blocks))
    output: list[int] = []
    for index, block in enumerate(blocks):
        count = quota + int(index < remainder)
        lower = _epoch_day(str(block["start"]))
        upper = _epoch_day(str(block["end"]))
        eligible = sorted(day for day in common if lower <= day <= upper)
        if len(eligible) < count:
            raise ProductionRuntimeError(
                f"temporal block {block['block_id']} lacks {count} starts"
            )
        frozen = sorted(day for day in required if lower <= day <= upper)
        if any(day not in set(eligible) for day in frozen) or len(frozen) > count:
            raise ProductionRuntimeError(
                f"nested starts do not fit temporal block {block['block_id']}"
            )
        available = [day for day in eligible if day not in set(frozen)]
        needed = count - len(frozen)
        if needed:
            positions = np.linspace(0, len(available) - 1, num=needed, dtype=int)
            added = [available[int(position)] for position in positions]
        else:
            added = []
        chosen = sorted([*frozen, *added])
        if len(set(chosen)) != count:
            raise ProductionRuntimeError("block-aware start selection duplicated starts")
        output.extend(chosen)
    if not set(required).issubset(output):
        raise ProductionRuntimeError("nested block-aware start set was not preserved")
    return tuple(output)


def _epoch_day(value: str) -> int:
    return (date.fromisoformat(value) - date(1970, 1, 1)).days


def _block_calendars(
    runtimes: Mapping[str, ExactSleeveRuntime],
    manifest: Mapping[str, Any],
    starts: Sequence[int],
) -> dict[int, tuple[int, ...]]:
    values = list(runtimes.values())
    common = set(values[0].eligible_session_days)
    for runtime in values[1:]:
        common.intersection_update(runtime.eligible_session_days)
    output: dict[int, tuple[int, ...]] = {}
    for start in starts:
        matched = None
        for block in manifest["temporal_blocks"]["blocks"]:
            lower = _epoch_day(str(block["start"]))
            upper = _epoch_day(str(block["end"]))
            if lower <= int(start) <= upper:
                matched = tuple(sorted(day for day in common if lower <= day <= upper))
                break
        if matched is None:
            raise ProductionRuntimeError(f"episode start {start} has no frozen temporal block")
        output[int(start)] = matched
    return output


def _durable_episode_cache_count(payload_dir: Path) -> int:
    """Count each durably cached normal/stressed episode exactly once.

    Only episode-row cache namespaces are included.  Recomputing this absolute
    value at checkpoints makes retries idempotent even when a process dies
    between the cache rename and the live-state publication.
    """

    roots: list[Path] = []
    exact = payload_dir / "exact_episode_rows"
    horizons = payload_dir / "horizon_episode_rows"
    if exact.is_dir():
        roots.append(exact)
    if horizons.is_dir():
        roots.append(horizons)
    if payload_dir.is_dir():
        roots.extend(
            path
            for path in payload_dir.iterdir()
            if path.is_dir()
            and path.name.endswith("_episode_rows")
            and path not in {exact, horizons}
        )
    paths = sorted(
        {
            path
            for root in roots
            for path in root.rglob("*.json")
            if path.is_file()
        }
    )
    return sum(len(_load_episode_cache(path)) for path in paths)


def _selected_stage6_metrics(
    metrics: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    selected = {str(value) for value in decision.get("selected_policy_ids", ())}
    return [row for row in metrics if str(row.get("policy_id") or "") in selected]


def _matched_controls_status(
    crossfit_result: Mapping[str, Any] | None,
) -> str:
    if crossfit_result is None:
        return "BASELINE_REPLAY_EXECUTED_COMPARISON_NOT_RUN_NO_SURVIVOR"
    if (
        crossfit_result.get("evaluation_status")
        == "NOT_RUN_SELECTOR_PLAN_INCOMPLETE"
    ):
        return (
            "BASELINE_REPLAY_EXECUTED_COMPARISON_NOT_RUN_"
            "NO_ELIGIBLE_CHAMPION"
        )
    return "EXECUTED"


def _matched_controls_payload(
    crossfit_result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if crossfit_result is None:
        return {"status": _matched_controls_status(None)}
    return {
        **dict(crossfit_result),
        "status": _matched_controls_status(crossfit_result),
    }


def _stage4_selector_gate_decision(
    policy_ids: Sequence[str],
    *,
    selector_status: str,
) -> dict[str, Any]:
    """Prevent non-GREEN selector procedures from reaching expanded replay."""

    ordered = sorted({str(value) for value in policy_ids})
    decision = {
        "schema": "hydra_production_selector_gate_decision_v1",
        "stage": "STAGE_4_ROBUSTNESS_CROSSFIT",
        "input_count": len(ordered),
        "eligible_count": 0,
        "output_limit": 16,
        "output_count": 0,
        "selected_policy_ids": [],
        "selector_status": str(selector_status),
        "required_selector_status": "SELECTOR_PROCEDURE_GREEN",
        "excluded": [
            {
                "policy_id": policy_id,
                "reasons": [f"SELECTOR_PROCEDURE_NOT_GREEN:{selector_status}"],
            }
            for policy_id in ordered
        ],
        "development_only": True,
    }
    decision["decision_hash"] = stable_hash(decision)
    return decision


def _expanded_candidate_counts(
    stage_decisions: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    promoted_96 = 0
    surviving_96 = 0
    for decision in stage_decisions:
        if decision.get("stage") == "STAGE_4_ROBUSTNESS_CROSSFIT":
            promoted_96 = int(decision.get("output_count", 0))
        elif decision.get("stage") == "STAGE_5_EXPANDED_96_STARTS":
            surviving_96 = int(decision.get("output_count", 0))
    return promoted_96, surviving_96


def _write_episode_cache(
    writer: AtomicResultWriter,
    relative_path: str,
    *,
    policy_id: str,
    horizon: str,
    episodes: Sequence[Mapping[str, Any]],
) -> AtomicWriteReceipt:
    payload: dict[str, Any] = {
        "schema": "hydra_production_episode_row_cache_v1",
        "policy_id": policy_id,
        "horizon": horizon,
        "episodes": [dict(row) for row in episodes],
    }
    payload["cache_hash"] = stable_hash(payload)
    return writer.write_json(relative_path, payload)


def _load_episode_cache(
    path: Path,
    *,
    expected_policy_id: str | None = None,
    expected_horizon: str | None = None,
) -> list[dict[str, Any]]:
    payload = _load_json(path)
    claimed = str(payload.pop("cache_hash", ""))
    if not claimed or stable_hash(payload) != claimed:
        raise ProductionRuntimeError(f"episode-row cache hash drift: {path}")
    if payload.get("schema") != "hydra_production_episode_row_cache_v1":
        raise ProductionRuntimeError(f"episode-row cache schema drift: {path}")
    if expected_policy_id is not None and payload.get("policy_id") != expected_policy_id:
        raise ProductionRuntimeError(f"episode-row cache policy drift: {path}")
    if expected_horizon is not None and payload.get("horizon") != expected_horizon:
        raise ProductionRuntimeError(f"episode-row cache horizon drift: {path}")
    rows = payload.get("episodes")
    if not isinstance(rows, list) or not rows:
        raise ProductionRuntimeError(f"episode-row cache is empty: {path}")
    return [dict(row) for row in rows]


def _load_policy_episode_rows(
    directory: Path,
    policies: Sequence[ProductionPolicy],
    *,
    horizon: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for policy in policies:
        output.extend(
            _load_episode_cache(
                directory / f"{policy.policy_id}.json",
                expected_policy_id=policy.policy_id,
                expected_horizon=horizon,
            )
        )
    return output


def _build_horizon_audit(
    rows_by_horizon: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    order = {
        "20_TRADING_DAYS": 20,
        "40_TRADING_DAYS": 40,
        "60_TRADING_DAYS": 60,
        "90_TRADING_DAYS": 90,
        "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON": 10_000,
    }
    by_horizon: dict[str, Any] = {}
    curves = {"NORMAL": [], "STRESSED_1_5X": []}
    for label in sorted(rows_by_horizon, key=lambda value: order.get(value, 99_999)):
        rows = list(rows_by_horizon[label])
        horizon_value: dict[str, Any] = {}
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            selected = [row for row in rows if row["cost_scenario"] == scenario]
            if not selected:
                continue
            progress = np.asarray(
                [float(row["target_progress"]) for row in selected], dtype=float
            )
            days = np.asarray(
                [
                    float(row["days_to_target"])
                    for row in selected
                    if row.get("days_to_target") is not None
                ],
                dtype=float,
            )
            net = np.asarray([float(row["net_pnl"]) for row in selected], dtype=float)
            pass_count = sum(bool(row["target_reached"]) for row in selected)
            mll_count = sum(bool(row["mll_breached"]) for row in selected)
            censored = [row for row in selected if bool(row["censored_state"])]
            metrics = {
                "episode_count": len(selected),
                "pass_count": pass_count,
                "pass_probability_observed_fraction": pass_count / len(selected),
                "mll_breach_count": mll_count,
                "mll_breach_probability": mll_count / len(selected),
                "target_progress": {
                    "p25": float(np.percentile(progress, 25)),
                    "median": float(np.median(progress)),
                    "p75": float(np.percentile(progress, 75)),
                    "maximum": float(np.max(progress)),
                },
                "time_to_target_trading_days": {
                    "observed_count": int(days.size),
                    "minimum": float(np.min(days)) if days.size else None,
                    "p25": float(np.percentile(days, 25)) if days.size else None,
                    "median": float(np.median(days)) if days.size else None,
                    "p75": float(np.percentile(days, 75)) if days.size else None,
                    "maximum": float(np.max(days)) if days.size else None,
                },
                "expected_trading_days_to_pass_conditional_on_observed_pass": (
                    float(np.mean(days)) if days.size else None
                ),
                "censored_episode_count": len(censored),
                "data_censored_count": sum(
                    row["terminal_state"] == "DATA_CENSORED" for row in selected
                ),
                "operational_horizon_not_reached_count": sum(
                    row["terminal_state"] == "OPERATIONAL_HORIZON_NOT_REACHED"
                    for row in selected
                ),
                "net_after_costs": {
                    "total": float(np.sum(net)),
                    "median": float(np.median(net)),
                    "costs_total": float(
                        sum(float(row["costs"]) for row in selected)
                    ),
                },
                "research_horizon_survival_is_not_failure": True,
            }
            horizon_value[scenario] = metrics
            curves[scenario].append(
                {
                    "horizon": label,
                    "target_progress_p25": metrics["target_progress"]["p25"],
                    "target_progress_median": metrics["target_progress"]["median"],
                    "target_progress_p75": metrics["target_progress"]["p75"],
                    "pass_probability_observed_fraction": metrics[
                        "pass_probability_observed_fraction"
                    ],
                }
            )
        by_horizon[label] = horizon_value
    return {
        "schema": "hydra_production_time_to_combine_horizon_audit_v1",
        "by_horizon": by_horizon,
        "target_progress_curve_proxy": curves,
        "censoring_policy": "SURVIVING_PROFITABLE_EPISODES_NOT_CONVERTED_TO_FAILURE",
    }


def _economic_frontier(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not metrics:
        return {"candidate_count": 0}
    normal_pass = [
        float(row["normal"]["observed_pass_fraction"]) for row in metrics
    ]
    stressed_pass = [
        float(row["stressed_1_5x"]["observed_pass_fraction"]) for row in metrics
    ]
    stressed_progress = [
        float(row["stressed_1_5x"]["target_progress_median"]) for row in metrics
    ]
    stressed_mll = [
        float(row["stressed_1_5x"]["mll_breach_rate"]) for row in metrics
    ]
    stressed_net = [
        float(row["stressed_1_5x"]["observed_net_total"]) for row in metrics
    ]
    return {
        "candidate_count": len(metrics),
        "normal_pass_fraction_best": max(normal_pass),
        "normal_pass_fraction_median": float(np.median(normal_pass)),
        "stressed_pass_fraction_best": max(stressed_pass),
        "stressed_pass_fraction_median": float(np.median(stressed_pass)),
        "stressed_target_progress_median_best": max(stressed_progress),
        "stressed_target_progress_median_population": float(
            np.median(stressed_progress)
        ),
        "stressed_mll_breach_rate_minimum": min(stressed_mll),
        "stressed_mll_breach_rate_maximum": max(stressed_mll),
        "positive_stressed_net_count": sum(value > 0.0 for value in stressed_net),
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionRuntimeError(f"invalid production artifact: {path}") from exc
    if not isinstance(value, dict):
        raise ProductionRuntimeError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _children_cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return float(usage.ru_utime + usage.ru_stime)


def _system_cpu_ticks() -> tuple[int, int]:
    fields = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()[1:]
    values = [int(value) for value in fields]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return total - idle, total


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "PRODUCTION_KPI_SCHEMA",
    "PRODUCTION_STATE_SCHEMA",
    "ProductionRuntimeError",
    "load_and_verify_production_result",
    "read_live_status",
    "run_production_manifest",
]
