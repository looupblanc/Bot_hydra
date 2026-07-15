"""Manifest-driven active-risk-pool and target-velocity production runtime.

This lane changes only causal account admission and bounded contract sizing over
the eighteen immutable sleeve ledgers sealed by campaign 0025.  It deliberately
does not recompute features, signals, entries, or exits.  All outcome-bearing
work is cached before publication and the single asynchronous EvidenceBundle
writer remains the only authoritative evidence writer.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import random
import statistics
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ACTIVE_RISK_POOL_POLICY_VERSION,
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
    policy_from_mapping,
)
from hydra.account_policy.active_pool_replay import run_shared_account_episode
from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS, recover_finalized_evidence_bundle
from hydra.production.component_evidence import materialize_component_evidence
from hydra.production.active_risk_manifest import (
    ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE,
    ACTIVE_RISK_RANDOM_PRIORITY_SEEDS,
)
from hydra.production.episode_evidence import _convert_episode
from hydra.production.evidence_adapter import AsyncEvidenceBundleSink
from hydra.production.halving import build_final_result_payload
from hydra.production.manifest import PRODUCTION_RESULT_SCHEMA
from hydra.production.policy_factory import ComponentCandidate, load_component_candidates
from hydra.production.portfolio_books import BookPair, SleeveRecord
from hydra.production.portfolio_manifest import PORTFOLIO_RUNTIME_VERSION
from hydra.production.portfolio_runtime import (
    PortfolioBasketPolicy,
    PortfolioFirstRun,
    _assert_unique_portfolio_event_ids,
    _block_for_day,
    _combine_inputs,
    _portfolio_provenance_checksums,
    _remaining_chronological_calendars,
    _restress,
)
from hydra.production.replay import _episode_row
from hydra.production.runtime import (
    ProductionRuntimeError,
    _block_aware_starts,
    _block_calendars,
    _load_json,
    _sha256,
    _verify_snapshot_hash,
    load_and_verify_production_result,
)
from hydra.propfirm.combine_episode import CombineTerminal, run_combine_episode
from hydra.propfirm.combine_to_xfa import (
    FrozenRiskProfile,
    LIFECYCLE_VERSION,
    RULE_SNAPSHOT_VERSION,
    UNREALIZED_AGGREGATION_SEMANTICS,
    _run_xfa_path,
    _scale_events,
    _zero_observation_xfa_path,
    official_rule_snapshot_2026_07_15,
)


ACTIVE_RISK_RUNTIME_VERSION = "hydra_active_risk_pool_runtime_v1"
ACTIVE_RISK_POLICY_SCHEMA = "hydra_active_risk_pool_population_v1"
ACTIVE_RISK_XFA_OVERLAY_VERSION = "hydra_active_risk_pool_xfa_overlay_v1"
ACTIVE_RISK_XFA_EVIDENCE_SCHEMA = "hydra_active_risk_pool_xfa_evidence_v1"
ACTIVE_RISK_XFA_OVERLAY_SEMANTICS = (
    "FROZEN_STATIC_XFA_PROFILE_NO_COMBINE_GOVERNOR_CONTROLS_V1"
)
_HORIZONS: tuple[int | str, ...] = (20, 40, 60, 90, "FULL")
_CANONICAL_HORIZON = "90_TRADING_DAYS"
_XFA_HORIZON = 120
_WORK: dict[str, Any] = {}


class ActiveRiskRuntimeError(ProductionRuntimeError):
    """The frozen active-risk campaign cannot be replayed safely."""


def run_active_risk_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path,
    cache_root: str | Path,
    stop_after: str | None = None,
) -> dict[str, Any]:
    return ActiveRiskPoolRun(
        manifest_path=Path(manifest_path),
        contract_map_path=Path(contract_map_path),
        cache_root=Path(cache_root),
        stop_after=stop_after,
    ).execute()


class ActiveRiskPoolRun(PortfolioFirstRun):
    """Resumable 20k -> 4096 -> 1024 -> 256 -> 32 -> 8 funnel."""

    def execute(self) -> dict[str, Any]:
        result_path = self.output_dir / str(self.manifest["runtime"]["result_name"])
        if result_path.is_file():
            result = load_and_verify_production_result(result_path, self.manifest)
            self._reconcile_completed_result_snapshots(result)
            return result
        sink: AsyncEvidenceBundleSink | None = None
        try:
            self._verify_source_commit()
            cemetery = self._audit_cemetery()
            recovered = self._recover_sealed_bundle_result()
            if recovered is not None:
                return recovered

            sleeves = self._sleeve_records()
            population = self._load_or_generate_governors(sleeves)
            self._publish(
                state="POPULATION_FROZEN",
                stage="ACTIVE_POOL_STAGE_0",
                policies_proposed=len(population),
                sleeve_bank_size=len(sleeves),
                next_action="COMPILE_IMMUTABLE_SLEEVE_LEDGERS_ONCE",
                cemetery_audit=cemetery,
            )

            components = load_component_candidates(self.manifest, self.root)
            component_map = {row.sleeve.sleeve_id: row for row in components}
            required_ids = {row.sleeve_id for row in sleeves}
            if not required_ids.issubset(component_map):
                raise ActiveRiskRuntimeError("frozen active-risk sleeve bank is incomplete")
            matrices = self._open_cached_matrices()
            runtimes, failures = self._compile_components(
                components, required_ids, matrices
            )
            if failures or set(runtimes) != required_ids:
                self.payload_writer.write_json("component_failures.json", failures)
                raise ActiveRiskRuntimeError("active-risk sleeve compilation failed")
            self._reconcile_sleeves(
                sleeves, runtimes, components=component_map, matrices=matrices
            )
            timelines = {key: tuple(value.events) for key, value in runtimes.items()}
            _assert_unique_portfolio_event_ids(timelines)

            starts48 = _block_aware_starts(runtimes, self.manifest, maximum=48)
            starts96 = _block_aware_starts(
                runtimes, self.manifest, maximum=96, required_starts=starts48
            )
            starts192 = _block_aware_starts(
                runtimes, self.manifest, maximum=192, required_starts=starts96
            )
            calendars = _block_calendars(runtimes, self.manifest, starts192)
            full_calendars = _remaining_chronological_calendars(runtimes, starts192)
            stage12_starts = tuple(
                next(
                    start
                    for start in starts48
                    if _block_for_day(start, self.manifest) == str(block["block_id"])
                )
                for block in self.manifest["temporal_blocks"]["blocks"]
            )

            identity_audit = self._run_identity_audit(
                sleeves=sleeves,
                timelines=timelines,
                starts=stage12_starts,
                calendars=calendars,
            )
            self._publish(
                state="COMPONENT_LEDGER_COMPLETE",
                stage="ACTIVE_POOL_IDENTITY_AUDIT_COMPLETE",
                identity_audit_status="PASS",
                next_action="VECTORIZED_FAST_LEDGER_SCREEN",
            )

            screen_bank = tuple(
                sorted(population, key=lambda row: row.structural_fingerprint)[:4096]
            )
            stage1 = self._run_stage(
                "stage1",
                screen_bank,
                timelines,
                stage12_starts,
                calendars,
                full_calendars,
                horizons=(40,),
                include_stress=False,
                include_evidence=False,
                start_state="POPULATION_FROZEN",
                prior_normal=0,
                prior_stressed=0,
                sink=None,
                lifecycle=False,
            )
            stage1_policies, stage1_decision = _select_policies(
                screen_bank, stage1, limit=1024, require_stress=False,
                stage="ACTIVE_POOL_STAGE_1_TO_2",
            )
            self._write_halving_decision("stage1", stage1_decision)
            if len(stage1) < 4096 or not stage1_policies:
                raise ActiveRiskRuntimeError("fast screen failed the 4096/1024 contract")
            self.active_metrics = {row["policy_id"]: row for row in stage1}
            self._publish(
                state="FAST_SCREEN_COMPLETE",
                stage="ACTIVE_POOL_STAGE_1",
                unique_policies_screened=len(stage1),
                next_action="EXACT_NORMAL_AND_STRESSED_ACCOUNT_REPLAY",
            )
            if self.stop_after == "FAST_SCREEN":
                return dict(self.state)

            stage2 = self._run_stage(
                "stage2",
                stage1_policies,
                timelines,
                stage12_starts,
                calendars,
                full_calendars,
                horizons=(90,),
                include_stress=True,
                include_evidence=True,
                start_state="EXACT_REPLAY_ACTIVE",
                prior_normal=0,
                prior_stressed=0,
                sink=None,
                lifecycle=False,
            )
            stage2_policies, stage2_decision = _select_policies(
                stage1_policies, stage2, limit=256, require_stress=True,
                stage="ACTIVE_POOL_STAGE_2_TO_3",
            )
            self._write_halving_decision("stage2", stage2_decision)
            if len(stage2) != 1024 or not stage2_policies:
                raise ActiveRiskRuntimeError("exact replay failed the 1024/256 contract")
            self.active_metrics = {row["policy_id"]: row for row in stage2}
            self._publish(
                state="FIRST_HALVING_COMPLETE",
                stage="ACTIVE_POOL_STAGE_2",
                exact_account_replays=len(stage2),
                replay_computations_completed=_metric_episode_total(stage2),
                next_action="OPEN_SINGLE_WRITER_AND_RUN_48_START_HORIZONS",
            )

            sink = self._open_active_evidence(
                stage1_policies, sleeves, stage12_starts
            )
            self._persist_static_evidence(
                sink, stage1_policies, sleeves, component_map, runtimes, matrices
            )
            advancing_ids = {row.policy_id for row in stage2_policies}
            self._persist_cached_stage_evidence(
                sink,
                stage="stage2",
                included_ids={row.policy_id for row in stage1_policies} - advancing_ids,
            )
            stage2_eliminated = [
                row for row in stage2 if str(row["policy_id"]) not in advancing_ids
            ]
            prior_normal = _scenario_episode_total(stage2_eliminated, "normal")
            prior_stressed = _scenario_episode_total(stage2_eliminated, "stressed")
            self._publish(
                state="ROBUSTNESS_ACTIVE",
                stage="ACTIVE_POOL_STAGE_3_48_STARTS",
                combine_episodes_completed=prior_normal + prior_stressed,
                normal_episodes_completed=prior_normal,
                stressed_episodes_completed=prior_stressed,
                next_action="MULTI_HORIZON_REPLAY_AND_AUTOMATIC_XFA",
            )

            stage3 = self._run_stage(
                "stage3",
                stage2_policies,
                timelines,
                starts48,
                calendars,
                full_calendars,
                horizons=_HORIZONS,
                include_stress=True,
                include_evidence=True,
                start_state="ROBUSTNESS_ACTIVE",
                prior_normal=prior_normal,
                prior_stressed=prior_stressed,
                sink=sink,
                lifecycle=True,
            )
            controls = self._run_matched_controls(
                stage2_policies,
                stage3,
                timelines,
                starts48,
                calendars,
                full_calendars,
            )
            stage3_policies, stage3_decision = _select_for_96(
                stage2_policies, stage3, controls, limit=32
            )
            self._write_halving_decision("stage3", stage3_decision)
            self.active_metrics = {row["policy_id"]: row for row in stage3}
            self._publish(
                state="EXPANDED_EPISODES_ACTIVE",
                stage="ACTIVE_POOL_STAGE_4_96_STARTS",
                candidates_promoted_96=len(stage3_policies),
                matched_controls_status="EXECUTED",
                next_action="REPLAY_FROZEN_GOVERNORS_TO_96_STARTS",
            )

            extra96 = tuple(row for row in starts96 if row not in set(starts48))
            stage4_increment = (
                self._run_stage(
                    "stage4",
                    stage3_policies,
                    timelines,
                    extra96,
                    calendars,
                    full_calendars,
                    horizons=_HORIZONS,
                    include_stress=True,
                    include_evidence=True,
                    start_state="EXPANDED_EPISODES_ACTIVE",
                    prior_normal=int(self.state.get("normal_episodes_completed", 0)),
                    prior_stressed=int(self.state.get("stressed_episodes_completed", 0)),
                    sink=sink,
                    lifecycle=True,
                )
                if stage3_policies
                else []
            )
            stage4 = _merge_metrics(
                _metrics_for(stage3, {row.policy_id for row in stage3_policies}),
                stage4_increment,
            )
            stage4_policies, stage4_decision = _select_confirmation_candidates(
                stage3_policies, stage4, limit=8
            )
            self._write_halving_decision("stage4", stage4_decision)
            self.active_metrics = {row["policy_id"]: row for row in stage4}
            self._publish(
                state="EXPANDED_EPISODES_ACTIVE",
                stage="ACTIVE_POOL_STAGE_5_192_STARTS",
                candidates_surviving_96=len(stage4_policies),
                candidates_promoted_192=len(stage4_policies),
                next_action="REPLAY_FINALISTS_TO_192_STARTS_NO_MUTATION",
            )

            extra192 = tuple(row for row in starts192 if row not in set(starts96))
            stage5_increment = (
                self._run_stage(
                    "stage5",
                    stage4_policies,
                    timelines,
                    extra192,
                    calendars,
                    full_calendars,
                    horizons=_HORIZONS,
                    include_stress=True,
                    include_evidence=True,
                    start_state="EXPANDED_EPISODES_ACTIVE",
                    prior_normal=int(self.state.get("normal_episodes_completed", 0)),
                    prior_stressed=int(self.state.get("stressed_episodes_completed", 0)),
                    sink=sink,
                    lifecycle=True,
                )
                if stage4_policies
                else []
            )
            stage5 = _merge_metrics(
                _metrics_for(stage4, {row.policy_id for row in stage4_policies}),
                stage5_increment,
            )
            final_policies, stage5_decision = _select_confirmation_candidates(
                stage4_policies, stage5, limit=8
            )
            self._write_halving_decision("stage5", stage5_decision)
            return self._finalize_active_campaign(
                sink,
                population=population,
                sleeves=sleeves,
                stage1=stage1,
                stage2=stage2,
                stage3=stage3,
                stage4=stage4,
                stage5=stage5,
                stage4_increment=stage4_increment,
                stage5_increment=stage5_increment,
                controls=controls,
                identity_audit=identity_audit,
                finalists=final_policies,
            )
        except BaseException as exc:
            self._publish(
                state="FAILED_CLOSED",
                stage=str(self.state.get("stage") or "STARTING"),
                next_action="REQUIRE_SPECIFIC_ACTIVE_RISK_RUNTIME_REPAIR",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise
        finally:
            if sink is not None:
                sink.close()

    def _recover_sealed_bundle_result(self) -> dict[str, Any] | None:
        """Recover the active-risk result without portfolio forward anchoring.

        ``PortfolioFirstRun`` owns a post-seal forward-package transaction that
        is intentionally absent from this campaign.  Calling its recovery hook
        would therefore turn the valid seal-to-result crash window into a
        permanent missing-freeze failure.  Rebuild the active result directly
        from the four compact outputs sealed in the EvidenceBundle instead.
        """

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
            raise ActiveRiskRuntimeError(
                "sealed EvidenceBundle identity differs from the frozen active-risk manifest"
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
        summary = outputs["campaign_summary"]
        if (
            summary.get("schema") != "hydra_active_risk_campaign_summary_v1"
            or summary.get("campaign_id") != self.campaign_id
        ):
            raise ActiveRiskRuntimeError("sealed active-risk campaign summary drift")
        receipt_payload = receipt.to_dict()
        _assert_active_authoritative_episode_counters(summary, receipt_payload)

        prior_kpi_path = self.output_dir / "production_kpis.json"
        kpis = _load_json(prior_kpi_path) if prior_kpi_path.is_file() else self._kpis()
        if "kpi_hash" in kpis:
            _verify_snapshot_hash(kpis, "kpi_hash")
        pareto = outputs["pareto_archive"]
        stage_decisions = pareto.get("stage_decisions")
        if not isinstance(stage_decisions, list):
            raise ActiveRiskRuntimeError(
                "sealed active-risk Pareto archive lacks successive-halving decisions"
            )
        controls = summary.get("matched_controls")
        if not isinstance(controls, Mapping):
            raise ActiveRiskRuntimeError("sealed active-risk matched controls are malformed")
        scientific_status = _sealed_active_scientific_status(summary)
        recommendation = outputs["next_campaign_recommendations"].get(
            "recommendation"
        )
        if not isinstance(recommendation, Mapping):
            raise ActiveRiskRuntimeError(
                "sealed active-risk autonomous recommendation is malformed"
            )
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=kpis,
            economic_results=summary,
            successive_halving={"stage_decisions": list(stage_decisions)},
            matched_controls=dict(controls),
            failure_vectors=outputs["failure_vectors"],
            evidence_receipt=receipt_payload,
            autonomous_next_action=recommendation,
            scientific_status=scientific_status,
        )
        result_name = str(self.manifest["runtime"]["result_name"])
        self.output_writer.write_json(result_name, result)
        checked = load_and_verify_production_result(
            self.output_dir / result_name, self.manifest
        )
        self._reconcile_completed_result_snapshots(checked)
        return checked

    def _reconcile_completed_result_snapshots(
        self, result: Mapping[str, Any]
    ) -> None:
        """Map active-pool promotion counts onto the stable snapshot contract."""

        economic = result.get("economic_results")
        if not isinstance(economic, Mapping):
            raise ActiveRiskRuntimeError("active-risk result lacks economic results")
        promoted = int(economic.get("policies_promoted_to_96", 0))
        surviving = int(economic.get("policies_surviving_96", 0))
        shadow = dict(result)
        shadow["successive_halving"] = {
            "stage_decisions": [
                {
                    "stage": "STAGE_4_ROBUSTNESS_CROSSFIT",
                    "output_count": promoted,
                },
                {
                    "stage": "STAGE_5_EXPANDED_96_STARTS",
                    "output_count": surviving,
                },
            ]
        }
        super()._reconcile_completed_result_snapshots(shadow)

    def _load_or_generate_governors(
        self, sleeves: Sequence[SleeveRecord]
    ) -> tuple[ActiveRiskPoolPolicy, ...]:
        path = self.payload_dir / "active_risk_governors.jsonl"
        started = time.perf_counter()
        if path.is_file():
            policies = tuple(
                policy_from_mapping(json.loads(line))
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
            summary = _load_json(self.payload_dir / "active_risk_population_summary.json")
        else:
            seed = int(self.manifest["governor_generator"].get("seed", 250026))
            policies, attempts = _generate_governors(sleeves, seed=seed, count=20_000)
            summary = {
                "schema": ACTIVE_RISK_POLICY_SCHEMA,
                "proposal_count": len(policies),
                "generation_attempt_count": attempts,
                "duplicate_rejection_count": attempts - len(policies),
                "duplicate_rejection_rate": (attempts - len(policies)) / attempts,
                "underlying_signal_mutation_count": 0,
                "inactive_sleeve_risk_reservation_count": 0,
                "seed": seed,
            }
            self.payload_writer.write_jsonl_batch(
                "active_risk_governors.jsonl", [row.to_dict() for row in policies]
            )
            self.payload_writer.write_json("active_risk_population_summary.json", summary)
        if len(policies) != 20_000:
            raise ActiveRiskRuntimeError("active-risk generator did not freeze 20,000 policies")
        fingerprints = [row.structural_fingerprint for row in policies]
        if len(set(fingerprints)) != len(fingerprints):
            raise ActiveRiskRuntimeError("active-risk structural duplicate escaped")
        self.population_summary = dict(summary)
        self.clock.hot_seconds += time.perf_counter() - started
        return policies

    def _run_identity_audit(
        self,
        *,
        sleeves: Sequence[SleeveRecord],
        timelines: Mapping[str, Sequence[RoutedTrade]],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
    ) -> dict[str, Any]:
        """Re-execute causal identity checks before any new policy outcome."""

        started = time.perf_counter()
        receipt_path = (
            self.root
            / "reports/economic_evolution/portfolio_first_combine_to_payout_0025"
            / "static_capital_partition_terminal_receipt.json"
        )
        receipt = _load_json(receipt_path)
        claimed = str(receipt.get("receipt_hash") or "")
        checked = dict(receipt)
        checked.pop("receipt_hash", None)
        prior = receipt.get("identity_and_dilution_audit") or {}
        if (
            stable_hash(checked) != claimed
            or receipt.get("terminal_status") != "STATIC_CAPITAL_PARTITION_TOO_SLOW"
            or int(prior.get("trade_pnl_cost_mll_target_progress_failures", -1)) != 0
            or int(prior.get("inactive_sleeve_invariance_failures", -1)) != 0
            or int(prior.get("nonoverlapping_pair_conservation_failures", -1)) != 0
        ):
            raise ActiveRiskRuntimeError("sealed 0025 identity audit is invalid")

        identity_policy = _identity_policy(sleeves)
        single_failures: list[dict[str, Any]] = []
        inactive_failures: list[dict[str, Any]] = []
        cases = 0
        rules = official_rule_snapshot_2026_07_15().combine_config()
        for sleeve in sleeves:
            selected = {sleeve.sleeve_id: tuple(timelines[sleeve.sleeve_id])}
            single_basket = PortfolioBasketPolicy(
                policy_id=f"identity:{sleeve.sleeve_id}",
                component_ids=(sleeve.sleeve_id,),
                archetype="ACTIVE_POOL_SINGLE_SLEEVE_IDENTITY",
                maximum_simultaneous_positions=1,
                maximum_mini_equivalent=15,
                component_priority=(sleeve.sleeve_id,),
            )
            for start in starts:
                days = calendars[int(start)]
                horizon = min(90, len(days[days.index(start) :]))
                legacy = run_combine_episode(
                    [row.event for row in selected[sleeve.sleeve_id]],
                    days,
                    start_day=int(start),
                    maximum_duration_days=horizon,
                    config=rules,
                )
                book = run_shared_account_episode(
                    selected,
                    days,
                    basket=single_basket,
                    start_day=int(start),
                    maximum_duration_days=horizon,
                    config=rules,
                )
                pooled = run_shared_account_episode(
                    selected,
                    days,
                    basket=identity_policy,
                    active_pool_policy=identity_policy,
                    start_day=int(start),
                    maximum_duration_days=horizon,
                    config=rules,
                )
                cases += 1
                equality = {
                    "terminal": legacy.terminal is book.terminal,
                    "event_count": legacy.event_count == book.accepted_events,
                    "net_pnl": math.isclose(legacy.net_pnl, book.net_pnl, abs_tol=1e-8),
                    "target_progress": math.isclose(
                        legacy.target_progress, book.target_progress, abs_tol=1e-12
                    ),
                    "minimum_mll_buffer": math.isclose(
                        legacy.minimum_mll_buffer,
                        book.minimum_mll_buffer,
                        abs_tol=1e-8,
                    ),
                    "consistency": legacy.consistency_ok is book.consistency_ok,
                }
                if not all(equality.values()):
                    single_failures.append(
                        {"sleeve_id": sleeve.sleeve_id, "start": start, "checks": equality}
                    )
                invariance = {
                    "terminal": pooled.terminal is book.terminal,
                    "accepted_events": pooled.accepted_events == book.accepted_events,
                    "skipped_events": pooled.skipped_events == 0,
                    "net_pnl": math.isclose(pooled.net_pnl, book.net_pnl, abs_tol=1e-8),
                    "costs": math.isclose(pooled.total_cost, book.total_cost, abs_tol=1e-8),
                    "target_progress": math.isclose(
                        pooled.target_progress, book.target_progress, abs_tol=1e-12
                    ),
                    "minimum_mll_buffer": math.isclose(
                        pooled.minimum_mll_buffer,
                        book.minimum_mll_buffer,
                        abs_tol=1e-8,
                    ),
                }
                if not all(invariance.values()):
                    inactive_failures.append(
                        {"sleeve_id": sleeve.sleeve_id, "start": start, "checks": invariance}
                    )
        audit = {
            "schema": "hydra_active_risk_identity_audit_v1",
            "campaign_id": self.campaign_id,
            "single_sleeve_identity_cases": cases,
            "single_sleeve_identity_failures": single_failures,
            "inactive_sleeve_invariance_cases": cases,
            "inactive_sleeve_invariance_failures": inactive_failures,
            "nonoverlapping_sleeve_conservation_cases": int(
                prior.get("nonoverlapping_pair_conservation_passes", 0)
            ),
            "nonoverlapping_sleeve_conservation_failures": int(
                prior.get("nonoverlapping_pair_conservation_failures", 0)
            ),
            "sealed_predecessor_receipt_path": str(receipt_path.relative_to(self.root)),
            "sealed_predecessor_receipt_sha256": _sha256(receipt_path),
            "inactive_sleeves_reserve_risk": False,
            "actual_stop_risk_available": False,
            "routing_risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
            "future_outcome_fields_used_for_routing": False,
            "passed": not single_failures and not inactive_failures,
        }
        audit["audit_hash"] = stable_hash(audit)
        self.payload_writer.write_json("active_risk_identity_audit.json", audit)
        self.clock.cold_seconds += time.perf_counter() - started
        if not audit["passed"]:
            raise ActiveRiskRuntimeError("active-risk identity audit failed closed")
        return audit

    def _run_stage(
        self,
        stage: str,
        policies: Sequence[ActiveRiskPoolPolicy],
        timelines: Mapping[str, Sequence[RoutedTrade]],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        full_calendars: Mapping[int, Sequence[int]],
        *,
        horizons: Sequence[int | str],
        include_stress: bool,
        include_evidence: bool,
        start_state: str,
        prior_normal: int,
        prior_stressed: int,
        sink: AsyncEvidenceBundleSink | None,
        lifecycle: bool,
    ) -> list[dict[str, Any]]:
        namespace = self.payload_dir / f"{stage}_active_batches"
        batch_size = 16 if stage == "stage1" else 4 if stage == "stage2" else 1
        batches = [
            tuple(policies[index : index + batch_size])
            for index in range(0, len(policies), batch_size)
        ]
        output: list[dict[str, Any]] = []
        missing: list[tuple[int, tuple[ActiveRiskPoolPolicy, ...]]] = []
        for index, batch in enumerate(batches):
            path = namespace / f"batch_{index:06d}.json"
            if path.is_file():
                cached = _load_json(path)
                rows = cached.get("rows")
                if not isinstance(rows, list) or stable_hash(rows) != cached.get("rows_hash"):
                    raise ActiveRiskRuntimeError(f"{stage} cache hash drift")
                if include_evidence and any("evidence_raw" not in row for row in rows):
                    raise ActiveRiskRuntimeError(f"{stage} cache lacks exact evidence")
                output.extend(_compact_metric(row) for row in rows)
                if sink is not None and include_evidence:
                    self._append_stage_rows(sink, stage, index, rows)
                    if stage in {"stage3", "stage4", "stage5"}:
                        # Exact multi-horizon rows are intentionally large.  A
                        # completed batch is the durable backpressure boundary:
                        # do not retain several copied daily paths while the
                        # single EvidenceBundle writer catches up.
                        sink.flush()
                del cached, rows
            else:
                missing.append((index, batch))
        if missing:
            hot_started = time.perf_counter()
            context = multiprocessing.get_context("spawn")
            worker_args = (
                timelines,
                starts,
                calendars,
                full_calendars,
                self.campaign_id,
                self.manifest["temporal_blocks"]["blocks"],
            )
            with ProcessPoolExecutor(
                max_workers=3,
                mp_context=context,
                initializer=_set_worker_state,
                initargs=worker_args,
            ) as pool:
                pending_batches = iter(missing)
                futures: dict[Any, int] = {}

                def submit_next_batch() -> bool:
                    try:
                        index, batch = next(pending_batches)
                    except StopIteration:
                        return False
                    future = pool.submit(
                        _active_batch_worker,
                        [row.to_dict() for row in batch],
                        tuple(horizons),
                        include_stress,
                        include_evidence,
                        lifecycle,
                    )
                    futures[future] = index
                    return True

                for _ in range(min(3, len(missing))):
                    submit_next_batch()

                completed = 0
                total_missing = len(missing)
                while futures:
                    # Snapshot only the bounded in-flight set.  Popping the
                    # yielded Future is essential: Future retains its result,
                    # and a Stage-3 result can be hundreds of MB.
                    future = next(as_completed(tuple(futures)))
                    index = futures.pop(future)
                    rows = future.result()
                    payload = {
                        "schema": "hydra_active_risk_stage_batch_v1",
                        "stage": stage,
                        "rows": rows,
                        "rows_hash": stable_hash(rows),
                    }
                    self.payload_writer.write_json(
                        f"{stage}_active_batches/batch_{index:06d}.json", payload
                    )
                    if sink is not None and include_evidence:
                        self._append_stage_rows(sink, stage, index, rows)
                        if stage in {"stage3", "stage4", "stage5"}:
                            sink.flush()
                    output.extend(_compact_metric(row) for row in rows)
                    completed += 1
                    if completed % 4 == 0 or completed == total_missing:
                        normal = prior_normal + _scenario_episode_total(output, "normal")
                        stressed = prior_stressed + _scenario_episode_total(output, "stressed")
                        # Stage 1 is approximate and Stage 2 is superseded for
                        # advancing policies by the complete 48-start Stage-3
                        # record.  Keep authoritative counters monotone by
                        # publishing Stage-2 work as computations only; the
                        # non-advancing exact rows are admitted once selection
                        # freezes their final evidence boundary.
                        if not include_stress or stage == "stage2":
                            normal = prior_normal
                            stressed = prior_stressed
                        self.clock.hot_seconds += max(time.perf_counter() - hot_started, 0.0)
                        self.active_metrics = {
                            row["policy_id"]: row for row in output[-1024:]
                        }
                        self._publish(
                            state=start_state,
                            stage=f"ACTIVE_POOL_{stage.upper()}",
                            unique_policies_screened=(
                                len(output)
                                if stage == "stage1"
                                else int(self.state.get("unique_policies_screened", 0))
                            ),
                            exact_account_replays=(
                                len(output)
                                if stage == "stage2"
                                else int(self.state.get("exact_account_replays", 0))
                            ),
                            combine_episodes_completed=normal + stressed,
                            normal_episodes_completed=normal,
                            stressed_episodes_completed=stressed,
                            replay_computations_completed=(
                                _metric_episode_total(output)
                                if stage in {"stage1", "stage2"}
                                else normal + stressed
                            ),
                            last_completed_policy_id=(
                                str(output[-1]["policy_id"]) if output else None
                            ),
                            next_action=f"CONTINUE_{stage.upper()}_ECONOMIC_REPLAY",
                        )
                        hot_started = time.perf_counter()
                    del payload, rows, future
                    submit_next_batch()
            self.clock.hot_seconds += max(time.perf_counter() - hot_started, 0.0)
        ordered = sorted(output, key=lambda row: str(row["policy_id"]))
        if len(ordered) != len(policies):
            raise ActiveRiskRuntimeError(f"{stage} policy count drift")
        return ordered

    def _append_stage_rows(
        self,
        sink: AsyncEvidenceBundleSink,
        stage: str,
        batch_index: int,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        episodes: list[dict[str, Any]] = []
        daily: list[dict[str, Any]] = []
        for row in rows:
            row_episodes, row_daily = _evidence_rows(row, self.manifest)
            episodes.extend(row_episodes)
            daily.extend(row_daily)
        sink.append_records(
            "episodes", episodes,
            batch_id=f"active:{stage}:{batch_index:06d}:episodes",
        )
        sink.append_records(
            "account_daily_paths", daily,
            batch_id=f"active:{stage}:{batch_index:06d}:daily",
        )

    def _write_halving_decision(self, stage: str, decision: Mapping[str, Any]) -> None:
        self.output_writer.write_json(f"successive_halving/{stage}.json", decision)

    def _open_active_evidence(
        self,
        policies: Sequence[ActiveRiskPoolPolicy],
        sleeves: Sequence[SleeveRecord],
        starts: Sequence[int],
    ) -> AsyncEvidenceBundleSink:
        identity = {
            "campaign_id": self.campaign_id,
            "grammar_id": str(self.manifest["class_id"]),
            "policy_fingerprints": {
                row.policy_id: row.structural_fingerprint for row in policies
            },
            "component_fingerprints": {
                row.sleeve_id: row.immutable_fingerprint for row in sleeves
            },
            "source_commit": str(self.manifest["source_commit"]),
            "data_fingerprints": {
                "canonical_feature_source": str(
                    self.manifest["data"]["feature_source_fingerprint"]
                ),
                "contract_map": str(self.manifest["data"]["contract_map_sha256"]),
                "data_access_ledger": _sha256(
                    self.root / "reports/data_access/data_access_ledger.jsonl"
                ),
                **self.feature_cache_fingerprints,
            },
            "configuration_sha256": _sha256(self.manifest_path),
            "seeds": [int(self.manifest["governor_generator"].get("seed", 250026))],
            "created_at_utc": str(self.manifest["created_at_utc"]),
            "expected_coverage": {
                "policy_ids": [row.policy_id for row in policies],
                "component_ids": [row.sleeve_id for row in sleeves],
                "required_episode_keys": [
                    {
                        "policy_id": policy.policy_id,
                        "episode_id": f"{policy.policy_id}:{start}",
                        "horizon": _CANONICAL_HORIZON,
                    }
                    for policy in policies
                    for start in starts
                ],
                "allowed_horizons": [
                    "20_TRADING_DAYS", "40_TRADING_DAYS", "60_TRADING_DAYS",
                    "90_TRADING_DAYS", "FULL_CHRONOLOGICAL_HORIZON",
                ],
                "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
                "allow_additional_episode_keys": True,
            },
        }
        self.evidence_identity = identity
        base = self.root / str(self.manifest["evidence_bundle"]["destination"])
        return AsyncEvidenceBundleSink(
            base_dir=base,
            identity=identity,
            writer_id=f"active-risk-kernel:{self.campaign_id}",
            resume=(base / f".{self.campaign_id}.evidence-v1.staging").is_dir(),
        )

    def _persist_static_evidence(
        self,
        sink: AsyncEvidenceBundleSink,
        policies: Sequence[ActiveRiskPoolPolicy],
        sleeves: Sequence[SleeveRecord],
        components: Mapping[str, ComponentCandidate],
        runtimes: Mapping[str, Any],
        matrices: Mapping[str, Any],
    ) -> None:
        used = {row.sleeve_id for row in sleeves}
        ledgers = materialize_component_evidence(
            self.campaign_id,
            {key: runtimes[key] for key in used},
            {key: components[key] for key in used},
            matrices,
        )
        for dataset, rows in ledgers.items():
            for index in range(0, len(rows), 5000):
                sink.append_records(
                    dataset,
                    rows[index : index + 5000],
                    batch_id=f"active-static:{dataset}:{index // 5000:06d}",
                )
        roles = {row.sleeve_id: row.economic_role for row in sleeves}
        membership = [
            {
                "campaign_id": self.campaign_id,
                "policy_id": policy.policy_id,
                "component_id": component_id,
                "risk_allocation": float(policy.static_risk_tier),
                "component_role": roles[component_id],
                "active_risk_policy": policy.to_dict(),
                "inactive_sleeve_reserves_risk": False,
                "underlying_signal_mutated": False,
            }
            for policy in policies
            for component_id in policy.component_ids
        ]
        for index in range(0, len(membership), 5000):
            sink.append_records(
                "account_policy_membership",
                membership[index : index + 5000],
                batch_id=f"active-membership:{index // 5000:06d}",
            )
        checksums = _portfolio_provenance_checksums(
            self.evidence_identity, ledgers, sleeves, campaign_id=self.campaign_id
        )
        sink.append_records(
            "provenance",
            [{
                "campaign_id": self.campaign_id,
                "validator_version": "hydra_evidence_bundle_validator_v1",
                "replay_version": ACTIVE_RISK_RUNTIME_VERSION,
                "market_data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
                "access_ledger_sha256": self.evidence_identity["data_fingerprints"][
                    "data_access_ledger"
                ],
                "reconstruction_flag": False,
                "immutable_checksums": checksums,
                "recorded_at_utc": str(self.manifest["created_at_utc"]),
            }],
            batch_id="active-provenance:000000",
        )
        sink.checkpoint({"stage": "ACTIVE_RISK_STATIC_EVIDENCE_COMPLETE"})
        sink.flush()

    def _persist_cached_stage_evidence(
        self,
        sink: AsyncEvidenceBundleSink,
        *,
        stage: str,
        included_ids: set[str],
    ) -> None:
        observed: set[str] = set()
        for path in sorted((self.payload_dir / f"{stage}_active_batches").glob("batch_*.json")):
            cached = _load_json(path)
            rows = cached.get("rows")
            if not isinstance(rows, list) or stable_hash(rows) != cached.get("rows_hash"):
                raise ActiveRiskRuntimeError(f"{stage} evidence cache drift")
            selected = [row for row in rows if str(row["policy_id"]) in included_ids]
            observed.update(str(row["policy_id"]) for row in selected)
            if selected:
                index = int(path.stem.removeprefix("batch_"))
                self._append_stage_rows(sink, f"{stage}-eliminated", index, selected)
        if observed != included_ids:
            raise ActiveRiskRuntimeError(f"{stage} eliminated evidence coverage drift")

    def _run_matched_controls(
        self,
        policies: Sequence[ActiveRiskPoolPolicy],
        policy_metrics: Sequence[Mapping[str, Any]],
        timelines: Mapping[str, Sequence[RoutedTrade]],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        full_calendars: Mapping[int, Sequence[int]],
    ) -> dict[str, Any]:
        path = self.payload_dir / "active_risk_matched_controls.json"
        if path.is_file():
            payload = _load_json(path)
            claimed = str(payload.get("controls_hash") or "")
            checked = dict(payload)
            checked.pop("controls_hash", None)
            if stable_hash(checked) != claimed:
                raise ActiveRiskRuntimeError("matched-control cache hash drift")
            return payload

        sleeves = self._sleeve_records()
        identity = _identity_policy(sleeves)
        equal = replace(
            identity,
            policy_id="control:equal-risk-active-sleeve-pooling",
            maximum_concurrent_sleeves=3,
            aggregate_open_risk_ceiling=2250.0,
        )
        always = replace(
            identity,
            policy_id="control:always-on-pooled-governor",
            maximum_concurrent_sleeves=min(6, len(sleeves)),
            aggregate_open_risk_ceiling=4500.0,
        )
        specs: list[dict[str, Any]] = [
            {"control_id": equal.policy_id, "kind": "ACTIVE", "policy": equal.to_dict()},
            {"control_id": always.policy_id, "kind": "ACTIVE", "policy": always.to_dict()},
        ]
        for sleeve in sleeves:
            standalone = _standalone_policy(sleeve)
            specs.append(
                {
                    "control_id": standalone.policy_id,
                    "kind": "ACTIVE",
                    "policy": standalone.to_dict(),
                }
            )
        static_pair = self._static_partition_baseline_pair()
        specs.append(
            {
                "control_id": "control:static-capital-partition-0025",
                "kind": "STATIC_PAIR",
                "pair": static_pair.to_dict(),
            }
        )
        started = time.perf_counter()
        rows: list[dict[str, Any]] = []
        random_screen_specs: list[dict[str, Any]] = []
        for policy in policies:
            for seed in ACTIVE_RISK_RANDOM_PRIORITY_SEEDS:
                control = _random_priority_control(policy, seed=seed)
                random_screen_specs.append(
                    {
                        "control_id": control.policy_id,
                        "kind": "ACTIVE",
                        "policy": control.to_dict(),
                        "matched_policy_id": policy.policy_id,
                        "seed": seed,
                    }
                )
        metrics_by_policy = {
            str(row["policy_id"]): row for row in policy_metrics
        }
        if set(metrics_by_policy) != {row.policy_id for row in policies}:
            raise ActiveRiskRuntimeError(
                "random-priority matching lacks serious-policy metrics"
            )
        random_screen_rows: list[dict[str, Any]] = []
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=3,
            mp_context=context,
            initializer=_set_worker_state,
            initargs=(
                timelines,
                starts,
                calendars,
                full_calendars,
                self.campaign_id,
                self.manifest["temporal_blocks"]["blocks"],
            ),
        ) as pool:
            screen_futures = {
                pool.submit(_random_exposure_worker, spec): spec["control_id"]
                for spec in random_screen_specs
            }
            for completed, future in enumerate(
                as_completed(screen_futures), start=1
            ):
                random_screen_rows.append(future.result())
                if completed % 128 == 0 or completed == len(screen_futures):
                    self.clock.hot_seconds += max(
                        time.perf_counter() - started, 0.0
                    )
                    self._publish(
                        state="ROBUSTNESS_ACTIVE",
                        stage="ACTIVE_POOL_RANDOM_EXPOSURE_MATCHING",
                        matched_controls_status="EXPOSURE_MATCHING_ACTIVE",
                        random_priority_exposure_screens_completed=completed,
                        next_action="SELECT_RANDOM_CONTROL_BY_EXPOSURE_ONLY",
                    )
                    started = time.perf_counter()

            selected_random_specs, random_matches = _match_random_priority_controls(
                policies=policies,
                metrics_by_policy=metrics_by_policy,
                screen_specs=random_screen_specs,
                screen_rows=random_screen_rows,
            )
            specs.extend(selected_random_specs)
            self.payload_writer.write_json(
                "active_risk_random_priority_exposure_screen.json",
                {
                    "schema": "hydra_active_risk_random_exposure_screen_v1",
                    "campaign_id": self.campaign_id,
                    "fixed_seeds": list(ACTIVE_RISK_RANDOM_PRIORITY_SEEDS),
                    "relative_tolerance": (
                        ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE
                    ),
                    "selection_fields": [
                        "TIME_WEIGHTED_MINI_NANOSECONDS_PER_OBSERVED_DAY",
                        "ACCEPTED_EVENT_RATE",
                    ],
                    "economic_outcomes_used_for_selection": False,
                    "matches": random_matches,
                    "screen_hash": stable_hash(random_matches),
                },
            )
            futures = {
                pool.submit(_control_worker, spec, _HORIZONS): spec["control_id"]
                for spec in specs
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                rows.append(future.result())
                if completed % 4 == 0 or completed == len(futures):
                    self.clock.hot_seconds += max(time.perf_counter() - started, 0.0)
                    self._publish(
                        state="ROBUSTNESS_ACTIVE",
                        stage="ACTIVE_POOL_MATCHED_CONTROLS",
                        matched_controls_status="ACTIVE",
                        matched_control_replays_completed=completed,
                        control_replay_computations_completed=(
                            len(random_screen_rows) * len(starts)
                            + _metric_episode_total(rows)
                        ),
                        next_action="COMPLETE_EXPOSURE_MATCHED_CONTROLS",
                    )
                    started = time.perf_counter()
        by_id = {str(row["policy_id"]): row for row in rows}
        standalone_rows = [
            row for key, row in by_id.items() if key.startswith("control:standalone:")
        ]
        best_standalone = max(
            standalone_rows,
            key=lambda row: _selection_key(row, require_stress=True),
        )
        random_controls = {
            str(spec["matched_policy_id"]): by_id[str(spec["control_id"])]
            for spec in specs
            if "matched_policy_id" in spec
        }
        random_match_by_policy = {
            str(row["matched_policy_id"]): row for row in random_matches
        }
        for policy_id, row in random_controls.items():
            row["exposure_matching"] = random_match_by_policy[policy_id]
        all_random_matched = all(
            bool(row["matched"]) for row in random_matches
        )
        payload = {
            "schema": "hydra_active_risk_matched_controls_v1",
            "campaign_id": self.campaign_id,
            "static_partition": by_id["control:static-capital-partition-0025"],
            "standalone_controls": standalone_rows,
            "best_standalone": best_standalone,
            "equal_risk_active_pool": by_id[equal.policy_id],
            "always_on_pooled_governor": by_id[always.policy_id],
            "random_priority_by_policy": random_controls,
            "random_priority_exposure_match_by_policy": random_match_by_policy,
            "random_priority_fixed_seeds": list(
                ACTIVE_RISK_RANDOM_PRIORITY_SEEDS
            ),
            "random_priority_exposure_relative_tolerance": (
                ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE
            ),
            "identical_starts": True,
            "identical_temporal_blocks": True,
            "identical_horizons": True,
            "identical_costs": True,
            "random_priority_exposure_matched": all_random_matched,
            "random_priority_exposure_match_rate": (
                sum(bool(row["matched"]) for row in random_matches)
                / len(random_matches)
                if random_matches
                else 0.0
            ),
            "random_priority_outcomes_used_for_matching": False,
            "random_priority_exposure_screen_replays": len(random_screen_rows),
            "random_priority_full_control_replays": len(random_controls),
            "matched_controls_status": (
                "EXECUTED_EXPOSURE_MATCHED"
                if all_random_matched
                else "PARTIAL_RANDOM_PRIORITY_UNMATCHED"
            ),
            "development_only": True,
        }
        payload["controls_hash"] = stable_hash(payload)
        self.payload_writer.write_json("active_risk_matched_controls.json", payload)
        self.output_writer.write_json("matched_controls.json", payload)
        return payload

    def _static_partition_baseline_pair(self) -> BookPair:
        source = (
            self.root
            / "data/cache/economic_production"
            / "hydra_portfolio_first_combine_to_payout_0025"
        )
        terminal_dir = (
            self.root
            / "reports/economic_evolution"
            / "portfolio_first_combine_to_payout_0025"
        )
        receipt = _load_json(
            terminal_dir / "static_capital_partition_terminal_receipt.json"
        )
        receipt_payload = dict(receipt)
        receipt_hash = str(receipt_payload.pop("receipt_hash", ""))
        result = _load_json(terminal_dir / "economic_production_result.json")
        result_payload = dict(result)
        result_hash = str(result_payload.pop("result_hash", ""))
        metric_rows = (result.get("economic_results") or {}).get(
            "lifecycle_matrix"
        )
        if (
            stable_hash(receipt_payload) != receipt_hash
            or receipt.get("terminal_status")
            != "STATIC_CAPITAL_PARTITION_TOO_SLOW"
            or stable_hash(result_payload) != result_hash
            or result.get("campaign_id")
            != "hydra_portfolio_first_combine_to_payout_0025"
            or not isinstance(metric_rows, list)
            or len(metric_rows) != 256
            or result.get("evidence_bundle", {}).get("manifest_sha256")
            != receipt.get("evidence_bundle", {}).get("manifest_sha256")
        ):
            raise ActiveRiskRuntimeError("sealed 0025 static baseline result drift")
        selected = max(
            metric_rows,
            key=lambda row: (
                float((row.get("stressed") or {}).get("target_progress_median", -math.inf)),
                float((row.get("stressed") or {}).get("net_total", -math.inf)),
                str(row.get("pair_id") or ""),
            ),
        )
        pair_id = str(selected["pair_id"])
        for line in (source / "portfolio_book_pairs.jsonl").read_text(
            encoding="utf-8"
        ).splitlines():
            pair = BookPair.from_mapping(json.loads(line))
            if pair.pair_id == pair_id:
                self.output_writer.write_json(
                    "static_partition_baseline_freeze.json",
                    {
                        "source_campaign_id": "hydra_portfolio_first_combine_to_payout_0025",
                        "pair": pair.to_dict(),
                        "selection_rule": "MAX_STRESSED_TARGET_PROGRESS_THEN_NET_FROM_SEALED_STAGE3",
                        "selected_before_active_pool_outcomes": True,
                    },
                )
                return pair
        raise ActiveRiskRuntimeError("selected 0025 static baseline pair is missing")

    def _kpis(self) -> dict[str, Any]:
        saved = self.summaries
        self.summaries = {}
        try:
            value = dict(super(PortfolioFirstRun, self)._kpis())
        finally:
            self.summaries = saved
        rows = list(getattr(self, "active_metrics", {}).values())
        normal_rates = [float(row["normal"]["pass_rate"]) for row in rows]
        stressed_rates = [
            float(row["stressed"]["pass_rate"])
            for row in rows
            if isinstance(row.get("stressed"), Mapping)
        ]
        value.update(
            {
                "normal_episodes_completed": int(
                    self.state.get("normal_episodes_completed", 0)
                ),
                "stressed_episodes_completed": int(
                    self.state.get("stressed_episodes_completed", 0)
                ),
                "positive_stressed_net_candidates": sum(
                    isinstance(row.get("stressed"), Mapping)
                    and float(row["stressed"]["net_total"]) > 0.0
                    for row in rows
                ),
                "candidates_with_normal_pass": sum(rate > 0.0 for rate in normal_rates),
                "candidates_with_stressed_pass": sum(rate > 0.0 for rate in stressed_rates),
                "best_normal_pass_rate": max(normal_rates, default=0.0),
                "best_stressed_pass_rate": max(stressed_rates, default=0.0),
                "median_normal_pass_rate": (
                    statistics.median(normal_rates) if normal_rates else 0.0
                ),
                "median_stressed_pass_rate": (
                    statistics.median(stressed_rates) if stressed_rates else 0.0
                ),
                "near_pass_count": sum(
                    int(row["normal"]["pass_count"]) == 0
                    and float(row["normal"]["target_progress_median"]) >= 0.70
                    for row in rows
                ),
            }
        )
        value.pop("kpi_hash", None)
        value["kpi_hash"] = stable_hash(value)
        return value

    def _finalize_active_campaign(
        self,
        sink: AsyncEvidenceBundleSink,
        *,
        population: Sequence[ActiveRiskPoolPolicy],
        sleeves: Sequence[SleeveRecord],
        stage1: Sequence[Mapping[str, Any]],
        stage2: Sequence[Mapping[str, Any]],
        stage3: Sequence[Mapping[str, Any]],
        stage4: Sequence[Mapping[str, Any]],
        stage5: Sequence[Mapping[str, Any]],
        stage4_increment: Sequence[Mapping[str, Any]],
        stage5_increment: Sequence[Mapping[str, Any]],
        controls: Mapping[str, Any],
        identity_audit: Mapping[str, Any],
        finalists: Sequence[ActiveRiskPoolPolicy],
    ) -> dict[str, Any]:
        final_metrics = list(stage5 or stage4 or stage3)
        stage3_rows = list(stage3)
        lifecycle_work_rows = [
            *stage3_rows,
            *stage4_increment,
            *stage5_increment,
        ]
        normal_rates = [float(row["normal"]["pass_rate"]) for row in stage3_rows]
        stress_rates = [float(row["stressed"]["pass_rate"]) for row in stage3_rows]
        positive_stressed_count = sum(
            float(row["stressed"]["net_total"]) > 0.0 for row in stage3_rows
        )
        normal_pass_candidate_count = sum(value > 0.0 for value in normal_rates)
        stressed_pass_candidate_count = sum(value > 0.0 for value in stress_rates)
        confirmation_ids = [
            row["policy_id"]
            for row in final_metrics
            if _confirmation_gate(row)
        ]
        any_velocity = any(
            float(row["stressed"]["target_progress_median"])
            > float(controls["static_partition"]["stressed"]["target_progress_median"])
            and float(row["stressed"]["net_total"]) > 0.0
            for row in stage3_rows
        )
        scientific_status = (
            "ACTIVE_POOL_CONFIRMATION_CANDIDATES_DEVELOPMENT_ONLY"
            if confirmation_ids
            else "ACTIVE_RISK_POOL_TARGET_VELOCITY_IMPROVED_WEAK"
            if any_velocity
            else "POSITIVE_BUT_INSUFFICIENT_TARGET_VELOCITY"
        )
        terminal_kpis = self._kpis()
        stage_decisions = [
            _load_json(path)
            for path in sorted(
                (self.output_dir / "successive_halving").glob("*.json")
            )
        ]
        summary = {
            "schema": "hydra_active_risk_campaign_summary_v1",
            "campaign_id": self.campaign_id,
            "source_static_classification": "STATIC_CAPITAL_PARTITION_TOO_SLOW",
            "sleeve_bank_size": len(sleeves),
            "governor_proposals_generated": len(population),
            "unique_policies_screened": len(stage1),
            "exact_account_replays": len(stage2),
            "stage3_policy_count": len(stage3),
            "normal_combine_passes": sum(int(row["normal"]["pass_count"]) for row in stage3_rows),
            "stressed_combine_passes": sum(int(row["stressed"]["pass_count"]) for row in stage3_rows),
            "all_evaluated_normal_combine_passes": sum(
                int(row["normal"]["pass_count"]) for row in lifecycle_work_rows
            ),
            "all_evaluated_stressed_combine_passes": sum(
                int(row["stressed"]["pass_count"]) for row in lifecycle_work_rows
            ),
            "best_normal_pass_rate": max(normal_rates, default=0.0),
            "best_stressed_pass_rate": max(stress_rates, default=0.0),
            "median_normal_pass_rate": statistics.median(normal_rates) if normal_rates else 0.0,
            "median_stressed_pass_rate": statistics.median(stress_rates) if stress_rates else 0.0,
            "normal_pass_candidate_count": normal_pass_candidate_count,
            "stressed_pass_candidate_count": stressed_pass_candidate_count,
            "positive_stressed_net_count": positive_stressed_count,
            "normal_target_progress_median": statistics.median(
                [float(row["normal"]["target_progress_median"]) for row in stage3_rows]
            ) if stage3_rows else 0.0,
            "stressed_target_progress_median": statistics.median(
                [float(row["stressed"]["target_progress_median"]) for row in stage3_rows]
            ) if stage3_rows else 0.0,
            "stressed_mll_breach_rate_maximum": max(
                [float(row["stressed"]["mll_breach_rate"]) for row in stage3_rows],
                default=0.0,
            ),
            "risk_utilisation": _aggregate_utilisation(stage3_rows),
            "suppression": _aggregate_suppression(stage3_rows),
            "horizon_frontier": _aggregate_horizons(stage3_rows),
            "matched_controls": dict(controls),
            "matched_controls_status": str(
                controls.get("matched_controls_status") or "UNKNOWN"
            ),
            "identity_audit": dict(identity_audit),
            "policies_promoted_to_96": int(self.state.get("candidates_promoted_96", 0)),
            "policies_surviving_96": int(
                self.state.get("candidates_surviving_96", 0)
            ),
            "active_pool_confirmation_candidate_ids": confirmation_ids,
            "confirmation_ready_candidate_ids": confirmation_ids,
            "development_finalist_ids": [row.policy_id for row in finalists],
            "xfa_paths_started": sum(
                int(row.get("xfa_paths_started", 0))
                for row in lifecycle_work_rows
            ),
            "first_payouts": sum(
                int(row.get("first_payouts", 0)) for row in lifecycle_work_rows
            ),
            "payout_cycles": sum(
                int(row.get("payout_cycles", 0)) for row in lifecycle_work_rows
            ),
            "trader_net_payout": sum(
                float(row.get("trader_net_payout", 0.0))
                for row in lifecycle_work_rows
            ),
            "scientific_status": scientific_status,
            "development_only": True,
            "independently_confirmed": False,
            "paper_shadow_ready_ids": [],
            "broker_connections": 0,
            "orders": 0,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "production_counters": {
                "serious_exact_account_replays": len(stage2),
                "predeclared_control_policy_replays": (
                    3
                    + len(controls.get("standalone_controls") or ())
                    + len(controls.get("random_priority_by_policy") or {})
                ),
                "random_priority_exposure_screen_replays": int(
                    controls.get("random_priority_exposure_screen_replays", 0)
                ),
                "replay_computations_completed": int(self.state.get("replay_computations_completed", 0)),
                "combine_episodes_completed": int(self.state.get("combine_episodes_completed", 0)),
                "normal_episodes_completed": int(self.state.get("normal_episodes_completed", 0)),
                "stressed_episodes_completed": int(self.state.get("stressed_episodes_completed", 0)),
            },
            "production_kpis": {
                "rates_per_hour": dict(terminal_kpis["rates_per_hour"]),
                "economic_research_wall_clock_fraction": terminal_kpis[
                    "economic_research_wall_clock_fraction"
                ],
                "cpu_utilization_fraction": terminal_kpis[
                    "cpu_utilization_fraction"
                ],
                "workers": dict(terminal_kpis["workers"]),
                "duplicate_rejection_rate": terminal_kpis[
                    "duplicate_rejection_rate"
                ],
                "cache_hit_rate": terminal_kpis["cache_hit_rate"],
            },
            "economic_frontier": {
                "candidate_count": len(stage3_rows),
                "normal_pass_fraction_best": max(normal_rates, default=0.0),
                "normal_pass_fraction_median": statistics.median(normal_rates)
                if normal_rates else 0.0,
                "stressed_pass_fraction_best": max(stress_rates, default=0.0),
                "stressed_pass_fraction_median": statistics.median(stress_rates)
                if stress_rates else 0.0,
                "stressed_target_progress_median_best": max(
                    (
                        max(float(row["stressed"]["target_progress_median"]), 0.0)
                        for row in stage3_rows
                    ),
                    default=0.0,
                ),
                "stressed_target_progress_median_population": max(
                    statistics.median(
                        [
                            float(row["stressed"]["target_progress_median"])
                            for row in stage3_rows
                        ]
                    ) if stage3_rows else 0.0,
                    0.0,
                ),
                "stressed_mll_breach_rate_minimum": min(
                    (
                        float(row["stressed"]["mll_breach_rate"])
                        for row in stage3_rows
                    ),
                    default=0.0,
                ),
                "stressed_mll_breach_rate_maximum": max(
                    (
                        float(row["stressed"]["mll_breach_rate"])
                        for row in stage3_rows
                    ),
                    default=0.0,
                ),
                "positive_stressed_net_count": positive_stressed_count,
            },
        }
        failure_vectors = {
            "schema": "hydra_production_failure_vectors_v1",
            "campaign_id": self.campaign_id,
            "by_policy": {
                str(row["policy_id"]): _failure_vectors(row) for row in final_metrics
            },
        }
        pareto = {
            "schema": "hydra_active_risk_pareto_archive_v1",
            "campaign_id": self.campaign_id,
            "frontier": final_metrics,
            "stage_decisions": stage_decisions,
            "opaque_score_used": False,
        }
        next_action = {
            "action": (
                "CONTINUE_FROZEN_ACTIVE_POOL_FINALISTS"
                if confirmation_ids
                else "QUEUE_TARGET_BEFORE_ADVERSE_EXCURSION_HIGH_VELOCITY_SLEEVES"
            ),
            "candidate_ids": confirmation_ids,
            "manifest_required": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
        for name, payload in (
            ("campaign_summary", summary),
            ("failure_vectors", failure_vectors),
            ("pareto_archive", pareto),
            ("next_campaign_recommendations", {
                "schema": "hydra_production_next_campaign_recommendations_v1",
                "campaign_id": self.campaign_id,
                "recommendation": next_action,
            }),
        ):
            sink.write_compact_output(name, payload)
        sink.checkpoint({"stage": "ACTIVE_RISK_FINALIZING"})
        sink.flush()
        self._publish(
            state="FINALIZING",
            stage="EVIDENCE_BUNDLE_ATOMIC_FINALIZE",
            confirmation_ready_candidates=len(confirmation_ids),
            next_action="SEAL_ACTIVE_RISK_EVIDENCE",
        )
        receipt = sink.finalize(
            lightweight_manifest_path=self.root
            / str(self.manifest["evidence_bundle"]["lightweight_manifest_path"])
        )
        sink.guard_completion(receipt.bundle_path)
        _assert_active_authoritative_episode_counters(summary, receipt.to_dict())
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=terminal_kpis,
            economic_results=summary,
            successive_halving={"stage_decisions": stage_decisions},
            matched_controls=dict(controls),
            failure_vectors=failure_vectors,
            evidence_receipt=receipt.to_dict(),
            autonomous_next_action=next_action,
            scientific_status=scientific_status,
        )
        if result["schema"] != PRODUCTION_RESULT_SCHEMA:
            raise ActiveRiskRuntimeError("active-risk result schema drift")
        self.output_writer.write_json(str(self.manifest["runtime"]["result_name"]), result)
        self.state.update(
            {
                "state": "COMPLETE",
                "stage": "ACTIVE_RISK_CAMPAIGN_COMPLETE",
                "next_action": next_action["action"],
                "evidence_bundle_path": receipt.bundle_path,
                "evidence_bundle_manifest_sha256": receipt.manifest_sha256,
                "confirmation_ready_candidates": len(confirmation_ids),
            }
        )
        self._reconcile_completed_result_snapshots(result)
        return result


def _generate_governors(
    sleeves: Sequence[SleeveRecord], *, seed: int, count: int
) -> tuple[tuple[ActiveRiskPoolPolicy, ...], int]:
    rng = random.Random(seed)
    identifiers = tuple(sorted(row.sleeve_id for row in sleeves))
    charges = {component_id: 2250.0 for component_id in identifiers}
    ceilings = (900.0, 1350.0, 1800.0, 2250.0, 3000.0, 3600.0, 4500.0)
    buffer_fractions = (0.25, 0.50, 0.75, 1.00)
    protected_buffers = (0.0, 500.0, 1000.0, 1500.0)
    maximum_sizes = (3.0, 6.0, 10.0, 15.0)
    daily_losses = (1500.0, 2250.0, 3000.0, 4500.0)
    daily_profits = (2000.0, 3000.0, 4500.0, 9000.0)
    target_rows = (
        (0.0, TargetProtectionMode.NONE),
        (1000.0, TargetProtectionMode.SCALE_75),
        (1000.0, TargetProtectionMode.SCALE_50),
        (500.0, TargetProtectionMode.LOCK_NEW_ENTRIES),
    )
    risks = (1.0, 2.0, 3.0, 4.0)
    policies: list[ActiveRiskPoolPolicy] = []
    observed: set[str] = set()
    attempts = 0
    while len(policies) < count:
        attempts += 1
        if attempts > count * 20:
            raise ActiveRiskRuntimeError("bounded governor space failed to yield 20,000 uniques")
        priority = list(identifiers)
        rng.shuffle(priority)
        target_distance, target_mode = rng.choice(target_rows)
        temporary = ActiveRiskPoolPolicy(
            policy_id=f"active-risk-proposal-{attempts:08d}",
            component_priority=tuple(priority),
            nominal_risk_charge_per_mini=tuple(
                (component_id, charges[component_id]) for component_id in priority
            ),
            maximum_concurrent_sleeves=rng.choice((1, 2, 3, 4, 5, 6)),
            aggregate_open_risk_ceiling=rng.choice(ceilings),
            maximum_mll_buffer_fraction=rng.choice(buffer_fractions),
            protected_mll_buffer=rng.choice(protected_buffers),
            maximum_mini_equivalent=rng.choice(maximum_sizes),
            concurrency_scaling=rng.choice(tuple(ConcurrencyScaling)),
            same_instrument_conflict_rule=rng.choice(
                tuple(SameInstrumentConflictRule)
            ),
            daily_loss_guard=rng.choice(daily_losses),
            daily_consistency_profit_guard=rng.choice(daily_profits),
            target_protection_distance=target_distance,
            target_protection_mode=target_mode,
            static_risk_tier=rng.choice(risks),
        )
        fingerprint = temporary.structural_fingerprint
        if fingerprint in observed:
            continue
        observed.add(fingerprint)
        policies.append(
            replace(
                temporary,
                policy_id=f"active_pool_{fingerprint[:24]}",
            )
        )
    return tuple(sorted(policies, key=lambda row: row.policy_id)), attempts


def _identity_policy(sleeves: Sequence[SleeveRecord]) -> ActiveRiskPoolPolicy:
    identifiers = tuple(sorted(row.sleeve_id for row in sleeves))
    return ActiveRiskPoolPolicy(
        policy_id="active-pool-identity-no-inactive-reservation",
        component_priority=identifiers,
        nominal_risk_charge_per_mini=tuple(
            (component_id, 2250.0) for component_id in identifiers
        ),
        maximum_concurrent_sleeves=len(identifiers),
        aggregate_open_risk_ceiling=4500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4500.0,
        daily_consistency_profit_guard=9000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _standalone_policy(sleeve: SleeveRecord) -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id=f"control:standalone:{sleeve.sleeve_id}",
        component_priority=(sleeve.sleeve_id,),
        nominal_risk_charge_per_mini=((sleeve.sleeve_id, 2250.0),),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=4500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4500.0,
        daily_consistency_profit_guard=9000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _random_priority_control(
    policy: ActiveRiskPoolPolicy, *, seed: int
) -> ActiveRiskPoolPolicy:
    derived_seed = int(
        hashlib.sha256(
            f"{policy.policy_id}:{seed}".encode("utf-8")
        ).hexdigest()[:16],
        16,
    )
    rng = random.Random(derived_seed)
    priority = list(policy.component_priority)
    rng.shuffle(priority)
    if priority == list(policy.component_priority) and len(priority) > 1:
        priority = priority[1:] + priority[:1]
    charges = policy.nominal_risk_charge_map
    return replace(
        policy,
        policy_id=f"control:random-priority:{policy.policy_id}:seed-{seed}",
        component_priority=tuple(priority),
        nominal_risk_charge_per_mini=tuple(
            (component_id, charges[component_id]) for component_id in priority
        ),
    )


def _match_random_priority_controls(
    *,
    policies: Sequence[ActiveRiskPoolPolicy],
    metrics_by_policy: Mapping[str, Mapping[str, Any]],
    screen_specs: Sequence[Mapping[str, Any]],
    screen_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Choose controls lexicographically by exposure distance, never outcomes."""

    fields = (
        "time_weighted_mini_nanoseconds_per_observed_day",
        "accepted_event_rate",
    )
    specs_by_id = {str(row["control_id"]): row for row in screen_specs}
    rows_by_policy: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in screen_rows:
        rows_by_policy[str(row["matched_policy_id"])].append(row)
    selected_specs: list[dict[str, Any]] = []
    matches: list[dict[str, Any]] = []
    for policy in sorted(policies, key=lambda row: row.policy_id):
        target = metrics_by_policy[policy.policy_id].get("exposure_signature")
        candidates = rows_by_policy.get(policy.policy_id, [])
        if not isinstance(target, Mapping) or len(candidates) != len(
            ACTIVE_RISK_RANDOM_PRIORITY_SEEDS
        ):
            raise ActiveRiskRuntimeError(
                "random-priority exposure-screen coverage drift"
            )
        ranked: list[tuple[tuple[float, float, int, str], Mapping[str, Any], dict[str, Any]]] = []
        for row in candidates:
            control = row.get("exposure_signature")
            if not isinstance(control, Mapping):
                raise ActiveRiskRuntimeError(
                    "random-priority exposure signature is missing"
                )
            deltas: dict[str, Any] = {}
            finite_for_rank: list[float] = []
            matched = True
            for field in fields:
                expected = float(target[field])
                observed = float(control[field])
                absolute = abs(observed - expected)
                if expected == 0.0:
                    relative: float | None = 0.0 if observed == 0.0 else None
                    rank_value = 0.0 if observed == 0.0 else math.inf
                else:
                    relative = absolute / abs(expected)
                    rank_value = relative
                matched = matched and rank_value <= (
                    ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE + 1e-12
                )
                finite_for_rank.append(rank_value)
                deltas[field] = {
                    "candidate": expected,
                    "control": observed,
                    "absolute_delta": absolute,
                    "relative_delta": relative,
                }
            key = (
                max(finite_for_rank),
                sum(finite_for_rank),
                int(row["seed"]),
                str(row["control_id"]),
            )
            ranked.append(
                (
                    key,
                    row,
                    {
                        "matched": matched,
                        "deltas": deltas,
                    },
                )
            )
        _key, chosen, detail = min(ranked, key=lambda value: value[0])
        control_id = str(chosen["control_id"])
        selected_specs.append(dict(specs_by_id[control_id]))
        matches.append(
            {
                "matched_policy_id": policy.policy_id,
                "control_id": control_id,
                "selected_seed": int(chosen["seed"]),
                "candidate_signature": dict(target),
                "control_signature": dict(chosen["exposure_signature"]),
                "relative_tolerance": (
                    ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE
                ),
                "matched": bool(detail["matched"]),
                "deltas": detail["deltas"],
                "selection_key_fields": list(fields),
                "economic_outcomes_used_for_selection": False,
            }
        )
    return selected_specs, matches


def _set_worker_state(
    timelines: Mapping[str, Sequence[RoutedTrade]],
    starts: Sequence[int],
    calendars: Mapping[int, Sequence[int]],
    full_calendars: Mapping[int, Sequence[int]],
    campaign_id: str,
    blocks: Sequence[Mapping[str, Any]],
) -> None:
    global _WORK
    normal = {key: tuple(value) for key, value in timelines.items()}
    _WORK = {
        "normal": normal,
        "stressed": {
            key: tuple(_restress(row) for row in values)
            for key, values in normal.items()
        },
        "starts": tuple(int(value) for value in starts),
        "calendars": {int(key): tuple(value) for key, value in calendars.items()},
        "full_calendars": {
            int(key): tuple(value) for key, value in full_calendars.items()
        },
        "campaign_id": campaign_id,
        "blocks": tuple(dict(row) for row in blocks),
        "rules": official_rule_snapshot_2026_07_15(),
    }


def _active_batch_worker(
    values: Sequence[Mapping[str, Any]],
    horizons: Sequence[int | str],
    include_stress: bool,
    include_evidence: bool,
    lifecycle: bool,
) -> list[dict[str, Any]]:
    return [
        _evaluate_active_policy(
            policy_from_mapping(value),
            horizons=horizons,
            include_stress=include_stress,
            include_evidence=include_evidence,
            lifecycle=lifecycle,
        )
        for value in values
    ]


def _evaluate_active_policy(
    policy: ActiveRiskPoolPolicy,
    *,
    horizons: Sequence[int | str],
    include_stress: bool,
    include_evidence: bool,
    lifecycle: bool,
) -> dict[str, Any]:
    state = _WORK
    scenarios = (("NORMAL", "normal"),)
    if include_stress:
        scenarios = (*scenarios, ("STRESSED_1_5X", "stressed"))
    summaries: dict[str, dict[str, Any]] = {"normal": {}, "stressed": {}}
    raw_rows: list[dict[str, Any]] = []
    lifecycle_rows: list[dict[str, Any]] = []
    episodes_by_horizon: dict[str, dict[str, list[Any]]] = {
        "normal": {},
        "stressed": {},
    }
    for scenario, scenario_key in scenarios:
        events = {
            component_id: state[scenario_key][component_id]
            for component_id in policy.component_ids
        }
        for horizon in horizons:
            label = _horizon_label(horizon)
            episodes: list[Any] = []
            classified: list[dict[str, Any]] = []
            for start in state["starts"]:
                if horizon == "FULL":
                    days = state["full_calendars"][start]
                    duration = len(days[days.index(start) :])
                else:
                    days = state["calendars"][start]
                    duration = int(horizon)
                episode = run_shared_account_episode(
                    events,
                    days,
                    basket=policy,
                    active_pool_policy=policy,
                    start_day=start,
                    maximum_duration_days=max(duration, 1),
                    config=state["rules"].combine_config(),
                )
                if include_evidence:
                    raw = _episode_row(
                        SimpleNamespace(
                            source_campaign=state["campaign_id"],
                            policy_id=policy.policy_id,
                        ),
                        episode,
                        scenario=scenario,
                        horizon=max(duration, 1),
                        events=events,
                    )
                    raw["horizon_label"] = label
                else:
                    raw = {
                        "terminal_classification": _terminal_classification(
                            episode,
                            duration=max(duration, 1),
                            full_horizon=horizon == "FULL",
                        )
                    }
                if horizon == "FULL" and episode.terminal is CombineTerminal.TIMEOUT:
                    raw["terminal_classification"] = "DATA_CENSORED"
                    raw["censored"] = True
                episodes.append(episode)
                classified.append(raw)
                if include_evidence:
                    raw_rows.append(raw)
                if lifecycle and horizon == "FULL" and episode.passed:
                    lifecycle_rows.append(
                        _run_xfa_for_pass(
                            policy,
                            events,
                            episode,
                            scenario=scenario,
                            eligible_days=state["full_calendars"][start],
                        )
                    )
            summaries[scenario_key][label] = _episode_summary(
                episodes, classified, state["blocks"]
            )
            episodes_by_horizon[scenario_key][label] = episodes
    canonical_label = (
        _CANONICAL_HORIZON
        if _CANONICAL_HORIZON in summaries["normal"]
        else next(iter(summaries["normal"]))
    )
    normal = summaries["normal"][canonical_label]
    stressed = summaries["stressed"].get(canonical_label)
    canonical_episodes = {
        scenario: episodes_by_horizon[scenario].get(canonical_label, [])
        for scenario in ("normal", "stressed")
    }
    utilization = _utilisation_summary(canonical_episodes["normal"])
    exposure_signature = _exposure_signature(canonical_episodes["normal"])
    suppression = _suppression_summary(canonical_episodes["normal"])
    lifecycle_summary = _lifecycle_summary(lifecycle_rows)
    behavior = stable_hash(
        {
            "normal": _behavior_rows(canonical_episodes["normal"]),
            "stressed": _behavior_rows(canonical_episodes["stressed"]),
        }
    )
    payload: dict[str, Any] = {
        "schema": "hydra_active_risk_policy_metric_v1",
        "policy_id": policy.policy_id,
        "structural_fingerprint": policy.structural_fingerprint,
        "actual_account_behavior_fingerprint": behavior,
        "normal": normal,
        "stressed": stressed,
        "horizons": summaries,
        "risk_utilisation": utilization,
        "exposure_signature": exposure_signature,
        "suppression": suppression,
        **lifecycle_summary,
        "development_only": True,
    }
    if include_evidence:
        payload["evidence_raw"] = raw_rows
    if lifecycle:
        payload["lifecycle_rows"] = lifecycle_rows
    return payload


def _run_xfa_for_pass(
    policy: ActiveRiskPoolPolicy,
    events: Mapping[str, Sequence[RoutedTrade]],
    episode: Any,
    *,
    scenario: str,
    eligible_days: Sequence[int],
) -> dict[str, Any]:
    rules = _WORK["rules"]
    start = next((day for day in eligible_days if int(day) > int(episode.end_day)), None)
    profile = FrozenRiskProfile(
        profile_id=f"{policy.policy_id}:XFA_PROFILE",
        risk_multiplier=float(policy.static_risk_tier),
        maximum_simultaneous_positions=policy.maximum_concurrent_sleeves,
        maximum_mini_equivalent=int(policy.maximum_mini_equivalent),
    )
    if start is None:
        standard = _zero_observation_xfa_path(
            path="STANDARD", horizon=_XFA_HORIZON, rules=rules
        )
        consistency = _zero_observation_xfa_path(
            path="CONSISTENCY", horizon=_XFA_HORIZON, rules=rules
        )
    else:
        scaled = _scale_events(events, float(policy.static_risk_tier))
        standard = _run_xfa_path(
            scaled,
            eligible_days,
            basket=policy,
            profile=profile,
            rules=rules,
            start_day=int(start),
            horizon=_XFA_HORIZON,
            path="STANDARD",
        )
        consistency = _run_xfa_path(
            scaled,
            eligible_days,
            basket=policy,
            profile=profile,
            rules=rules,
            start_day=int(start),
            horizon=_XFA_HORIZON,
            path="CONSISTENCY",
        )
    payload = {
        "schema": ACTIVE_RISK_XFA_EVIDENCE_SCHEMA,
        "lifecycle_version": LIFECYCLE_VERSION,
        "overlay_version": ACTIVE_RISK_XFA_OVERLAY_VERSION,
        "policy_id": policy.policy_id,
        "scenario": scenario,
        "combine_start_day": int(episode.start_day),
        "combine_end_day": int(episode.end_day),
        "combine_status": "TARGET_REACHED",
        "combine_horizon": "FULL_CHRONOLOGICAL_HORIZON",
        "xfa_start_day": None if start is None else int(start),
        "xfa_horizon_days": _XFA_HORIZON,
        "xfa_profile": profile.to_dict(),
        "xfa_profile_projection": {
            "risk_multiplier": float(profile.risk_multiplier),
            "maximum_simultaneous_positions": int(
                profile.maximum_simultaneous_positions
            ),
            "maximum_mini_equivalent": int(profile.maximum_mini_equivalent),
            "clip_to_official_xfa_scaling_plan": bool(
                profile.clip_to_xfa_scaling_plan
            ),
            "same_market_exclusive": bool(profile.same_market_exclusive),
        },
        "rule_snapshot": rules.to_dict(),
        "standard": standard.to_dict(),
        "consistency": consistency.to_dict(),
        "combine_profit_transferred_to_xfa": False,
        "xfa_profile_frozen_before_replay": True,
        "xfa_profile_selected_from_outcomes": False,
        "xfa_overlay_semantics": ACTIVE_RISK_XFA_OVERLAY_SEMANTICS,
        "combine_governor_controls_applied_in_xfa": False,
        "payout_path_oracle_used": False,
        "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
        "development_only": True,
    }
    payload["source_lifecycle_sha256"] = stable_hash(payload)
    return payload


def _control_worker(
    spec: Mapping[str, Any], horizons: Sequence[int | str]
) -> dict[str, Any]:
    if spec["kind"] == "ACTIVE":
        row = _evaluate_active_policy(
            policy_from_mapping(spec["policy"]),
            horizons=horizons,
            include_stress=True,
            include_evidence=False,
            lifecycle=False,
        )
    elif spec["kind"] == "STATIC_PAIR":
        row = _evaluate_static_pair(
            BookPair.from_mapping(spec["pair"]), horizons=horizons
        )
    else:
        raise ActiveRiskRuntimeError("unknown matched-control kind")
    row["policy_id"] = str(spec["control_id"])
    row["matched_policy_id"] = spec.get("matched_policy_id")
    return row


def _random_exposure_worker(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Replay a frozen random priority using only the matching horizon."""

    row = _evaluate_active_policy(
        policy_from_mapping(spec["policy"]),
        horizons=(90,),
        include_stress=False,
        include_evidence=False,
        lifecycle=False,
    )
    return {
        "control_id": str(spec["control_id"]),
        "matched_policy_id": str(spec["matched_policy_id"]),
        "seed": int(spec["seed"]),
        "exposure_signature": dict(row["exposure_signature"]),
    }


def _evaluate_static_pair(
    pair: BookPair, *, horizons: Sequence[int | str]
) -> dict[str, Any]:
    state = _WORK
    summaries: dict[str, dict[str, Any]] = {"normal": {}, "stressed": {}}
    for scenario_key, stressed in (("normal", False), ("stressed", True)):
        events, basket = _combine_inputs(pair, state[scenario_key], stressed=False)
        for horizon in horizons:
            label = _horizon_label(horizon)
            episodes: list[Any] = []
            classified: list[dict[str, Any]] = []
            for start in state["starts"]:
                days = (
                    state["full_calendars"][start]
                    if horizon == "FULL"
                    else state["calendars"][start]
                )
                duration = (
                    len(days[days.index(start) :])
                    if horizon == "FULL"
                    else int(horizon)
                )
                episode = run_shared_account_episode(
                    events,
                    days,
                    basket=basket,
                    start_day=start,
                    maximum_duration_days=max(duration, 1),
                    config=state["rules"].combine_config(),
                )
                raw = {
                    "terminal_classification": _terminal_classification(
                        episode,
                        duration=max(duration, 1),
                        full_horizon=horizon == "FULL",
                    )
                }
                episodes.append(episode)
                classified.append(raw)
            summaries[scenario_key][label] = _episode_summary(
                episodes, classified, state["blocks"]
            )
    return {
        "schema": "hydra_active_risk_control_metric_v1",
        "policy_id": pair.pair_id,
        "normal": summaries["normal"][_CANONICAL_HORIZON],
        "stressed": summaries["stressed"][_CANONICAL_HORIZON],
        "horizons": summaries,
        "risk_utilisation": {},
        "suppression": {},
        "actual_account_behavior_fingerprint": stable_hash(summaries),
        **_lifecycle_summary(()),
        "development_only": True,
    }


def _horizon_label(value: int | str) -> str:
    return (
        "FULL_CHRONOLOGICAL_HORIZON"
        if value == "FULL"
        else f"{int(value)}_TRADING_DAYS"
    )


def _terminal_classification(
    episode: Any, *, duration: int, full_horizon: bool
) -> str:
    if episode.terminal is CombineTerminal.PASSED:
        return "TARGET_REACHED"
    if episode.terminal is CombineTerminal.MLL_BREACH:
        return "MLL_BREACHED"
    if episode.terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return "HARD_RULE_FAILURE"
    if full_horizon or int(episode.eligible_days) < int(duration):
        return "DATA_CENSORED"
    return "OPERATIONAL_HORIZON_NOT_REACHED"


def _episode_summary(
    episodes: Sequence[Any],
    classified: Sequence[Mapping[str, Any]],
    blocks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if len(episodes) != len(classified):
        raise ActiveRiskRuntimeError("episode classification length drift")
    if not episodes:
        return _empty_episode_summary()
    manifest = {"temporal_blocks": {"blocks": list(blocks)}}
    net = [float(row.net_pnl) for row in episodes]
    progress = [float(row.target_progress) for row in episodes]
    durations = [int(row.eligible_days) for row in episodes]
    active_durations = [int(row.traded_days) for row in episodes]
    calendar_durations = [int(row.end_day) - int(row.start_day) + 1 for row in episodes]
    target_days = [
        int(row.days_to_target) for row in episodes if row.days_to_target is not None
    ]
    terminal = [str(row["terminal_classification"]) for row in classified]
    contribution: dict[str, float] = defaultdict(float)
    by_block_net: dict[str, float] = defaultdict(float)
    by_block_progress: dict[str, list[float]] = defaultdict(list)
    pass_blocks: set[str] = set()
    for episode, raw in zip(episodes, classified, strict=True):
        block = _block_for_day(int(episode.start_day), manifest)
        by_block_net[block] += float(episode.net_pnl)
        by_block_progress[block].append(float(episode.target_progress))
        if raw["terminal_classification"] == "TARGET_REACHED":
            pass_blocks.add(block)
        for component_id, value in episode.component_contribution.items():
            contribution[str(component_id)] += float(value)
    pass_count = terminal.count("TARGET_REACHED")
    breaches = terminal.count("MLL_BREACHED")
    censored = sum(
        value in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}
        for value in terminal
    )
    positive_component_total = sum(max(value, 0.0) for value in contribution.values())
    positive_block_total = sum(max(value, 0.0) for value in by_block_net.values())
    projected_active = [
        duration / value
        for duration, value in zip(active_durations, progress, strict=True)
        if value > 0.0
    ]
    projected_calendar = [
        duration / value
        for duration, value in zip(calendar_durations, progress, strict=True)
        if value > 0.0
    ]
    return {
        "episode_count": len(episodes),
        "pass_count": pass_count,
        "pass_rate": pass_count / len(episodes),
        "mll_breach_count": breaches,
        "mll_breach_rate": breaches / len(episodes),
        "censored_episode_count": censored,
        "censoring_rate": censored / len(episodes),
        "terminal_distribution": dict(sorted(Counter(terminal).items())),
        "net_total": sum(net),
        "net_median": statistics.median(net),
        "net_values": net,
        "target_progress_median": statistics.median(progress),
        "target_progress_p25": _quantile(progress, 0.25),
        "target_progress_values": progress,
        "maximum_target_progress": max(
            float(row.maximum_target_progress) for row in episodes
        ),
        "minimum_mll_buffer": min(float(row.minimum_mll_buffer) for row in episodes),
        "consistency_rate": sum(bool(row.consistency_ok) for row in episodes)
        / len(episodes),
        "consistency_ok_count": sum(bool(row.consistency_ok) for row in episodes),
        "duration_trading_days_values": durations,
        "duration_trading_days_median": statistics.median(durations),
        "active_trading_days_values": active_durations,
        "active_trading_days_median": statistics.median(active_durations),
        "calendar_days_values": calendar_durations,
        "calendar_days_median": statistics.median(calendar_durations),
        "days_to_target_values": target_days,
        "median_days_to_target": (
            statistics.median(target_days) if target_days else None
        ),
        "projected_active_days_to_target_median": (
            statistics.median(projected_active) if projected_active else None
        ),
        "projected_calendar_days_to_target_median": (
            statistics.median(projected_calendar) if projected_calendar else None
        ),
        "monthly_subscription_duration_proxy_median": (
            statistics.median(projected_calendar) / 30.0
            if projected_calendar
            else None
        ),
        "pass_block_count": len(pass_blocks),
        "pass_block_ids": sorted(pass_blocks),
        "by_block_net": dict(sorted(by_block_net.items())),
        "by_block_target_progress_median": {
            key: statistics.median(values)
            for key, values in sorted(by_block_progress.items())
        },
        "component_contribution": dict(sorted(contribution.items())),
        "maximum_block_profit_share": (
            max((max(value, 0.0) for value in by_block_net.values()), default=0.0)
            / positive_block_total
            if positive_block_total > 0.0
            else 0.0
        ),
        "maximum_sleeve_profit_share": (
            max((max(value, 0.0) for value in contribution.values()), default=0.0)
            / positive_component_total
            if positive_component_total > 0.0
            else 0.0
        ),
    }


def _empty_episode_summary() -> dict[str, Any]:
    return {
        "episode_count": 0,
        "pass_count": 0,
        "pass_rate": 0.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "censored_episode_count": 0,
        "censoring_rate": 0.0,
        "terminal_distribution": {},
        "net_total": 0.0,
        "net_median": 0.0,
        "net_values": [],
        "target_progress_median": 0.0,
        "target_progress_p25": 0.0,
        "target_progress_values": [],
        "maximum_target_progress": 0.0,
        "minimum_mll_buffer": 4500.0,
        "consistency_rate": 0.0,
        "consistency_ok_count": 0,
        "duration_trading_days_values": [],
        "duration_trading_days_median": 0.0,
        "active_trading_days_values": [],
        "active_trading_days_median": 0.0,
        "calendar_days_values": [],
        "calendar_days_median": 0.0,
        "days_to_target_values": [],
        "median_days_to_target": None,
        "projected_active_days_to_target_median": None,
        "projected_calendar_days_to_target_median": None,
        "monthly_subscription_duration_proxy_median": None,
        "pass_block_count": 0,
        "pass_block_ids": [],
        "by_block_net": {},
        "by_block_target_progress_median": {},
        "component_contribution": {},
        "maximum_block_profit_share": 0.0,
        "maximum_sleeve_profit_share": 0.0,
    }


def _utilisation_summary(episodes: Sequence[Any]) -> dict[str, Any]:
    values: list[float] = []
    groups: dict[str, list[float]] = defaultdict(list)
    for episode in episodes:
        for decision in episode.risk_allocation_path:
            for side in ("risk_before", "risk_after"):
                audit = decision.get(side)
                if not isinstance(audit, Mapping):
                    continue
                value = audit.get("utilisation")
                if value is None:
                    continue
                count = int(audit.get("active_sleeve_count", 0))
                label = "zero" if count == 0 else "one" if count == 1 else "two" if count == 2 else "three_or_more"
                values.append(float(value))
                groups[label].append(float(value))
    return {
        "risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
        "actual_stop_risk_available": False,
        "observation_count": len(values),
        "mean": statistics.fmean(values) if values else 0.0,
        "median": statistics.median(values) if values else 0.0,
        "p25": _quantile(values, 0.25) if values else 0.0,
        "p75": _quantile(values, 0.75) if values else 0.0,
        "by_active_sleeve_count": {
            label: {
                "observation_count": len(groups.get(label, ())),
                "mean": statistics.fmean(groups[label]) if groups.get(label) else 0.0,
                "median": statistics.median(groups[label]) if groups.get(label) else 0.0,
            }
            for label in ("zero", "one", "two", "three_or_more")
        },
    }


def _exposure_signature(episodes: Sequence[Any]) -> dict[str, Any]:
    """Outcome-agnostic exposure signature used to match random priorities."""

    accepted = 0
    emitted = 0
    mini_nanoseconds = 0.0
    observed_days = 0
    for episode in episodes:
        observed_days += int(episode.eligible_days)
        for decision in episode.risk_allocation_path:
            emitted += 1
            if not bool(decision.get("accepted")):
                continue
            accepted += 1
            opened = int(decision.get("decision_ns", 0))
            closed = int(decision.get("exit_ns", opened))
            duration = max(closed - opened, 0)
            mini_nanoseconds += float(decision.get("mini_equivalent", 0.0)) * duration
    return {
        "schema": "hydra_active_risk_exposure_signature_v1",
        "time_weighted_mini_nanoseconds_per_observed_day": (
            mini_nanoseconds / observed_days if observed_days else 0.0
        ),
        "accepted_event_rate": accepted / emitted if emitted else 0.0,
        "accepted_event_count": accepted,
        "emitted_event_count": emitted,
        "observed_episode_days": observed_days,
        "outcome_fields_used": False,
    }


def _suppression_summary(episodes: Sequence[Any]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    foregone = 0.0
    emitted = 0
    for episode in episodes:
        for decision in episode.risk_allocation_path:
            emitted += 1
            status = str(decision.get("decision_status") or "UNKNOWN")
            counts[status] += 1
            foregone += float(decision.get("foregone_realized_pnl_ex_post") or 0.0)
    accepted = counts["ACCEPTED"] + counts["SIZE_REDUCED"]
    return {
        "signals_emitted": emitted,
        "signals_accepted": accepted,
        "signals_rejected": emitted - accepted,
        "decision_status_counts": dict(sorted(counts.items())),
        "size_reduced": counts["SIZE_REDUCED"],
        "conflict_rejected": counts["CONFLICT_REJECTED"],
        "contract_limit_rejected": counts["CONTRACT_LIMIT_REJECTED"],
        "mll_risk_rejected": counts["MLL_RISK_REJECTED"],
        "foregone_realized_pnl_ex_post": foregone,
        "foregone_expected_pnl": None,
        "foregone_expected_pnl_status": "UNAVAILABLE_NO_FROZEN_PRE_OUTCOME_ESTIMATE",
        "foregone_realized_pnl_used_for_routing": False,
    }


def _behavior_rows(episodes: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {
            "start": int(row.start_day),
            "terminal": row.terminal.value,
            "accepted": int(row.accepted_events),
            "skipped": int(row.skipped_events),
            "quantity_path": [
                [
                    str(decision.get("event_id") or ""),
                    int(decision.get("quantity", 0)),
                    str(decision.get("decision_status") or ""),
                ]
                for decision in row.risk_allocation_path
            ],
        }
        for row in episodes
    ]


def _lifecycle_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    paths = [
        row[path]
        for row in rows
        for path in ("standard", "consistency")
        if isinstance(row.get(path), Mapping)
    ]
    return {
        "xfa_paths_started": len(rows),
        "xfa_standard_paths": len(rows),
        "xfa_consistency_paths": len(rows),
        "first_payouts": sum(bool(row.get("payout_eligible")) for row in paths),
        "payout_cycles": sum(int(row.get("payout_cycles", 0)) for row in paths),
        "trader_net_payout": sum(float(row.get("trader_net_payout", 0.0)) for row in paths),
        "post_payout_survival_count": sum(
            bool(row.get("post_payout_survived")) for row in paths
        ),
        "post_payout_survival_rate": (
            sum(bool(row.get("post_payout_survived")) for row in paths) / len(paths)
            if paths else 0.0
        ),
    }


_XFA_PATH_HASH_FIELDS = (
    "path",
    "terminal",
    "terminal_reason",
    "start_day",
    "end_day",
    "requested_horizon_days",
    "observed_days",
    "traded_days",
    "event_count",
    "accepted_event_count",
    "skipped_event_count",
    "payout_eligible",
    "payout_cycles",
    "gross_payout",
    "trader_net_payout",
    "first_payout_day",
    "post_payout_survived",
    "post_payout_censored",
    "post_payout_observed_days",
    "ending_balance",
    "ending_mll_floor",
    "minimum_mll_buffer",
    "qualifying_winning_days",
    "maximum_consistency_ratio",
    "maximum_mini_equivalent",
    "total_cost",
    "skipped_reasons",
    "component_contribution",
    "daily_ledger",
    "calendar_inactivity_auditable",
    "payout_request_policy",
    "payout_path_selected_from_outcomes",
)


def _validate_frozen_mapping_fingerprint(
    value: Any, *, label: str, required_version: tuple[str, str] | None = None
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ActiveRiskRuntimeError(f"active XFA {label} is absent")
    payload = dict(value)
    claimed = str(payload.pop("fingerprint", ""))
    if stable_hash(payload) != claimed:
        raise ActiveRiskRuntimeError(f"active XFA {label} fingerprint drift")
    if required_version is not None:
        field, expected = required_version
        if str(payload.get(field)) != expected:
            raise ActiveRiskRuntimeError(f"active XFA {label} version drift")
    return dict(value)


def _validate_xfa_path_evidence(
    value: Any, *, expected_path: str, expected_start_day: int | None
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ActiveRiskRuntimeError(f"{expected_path} XFA path is absent")
    missing = set(_XFA_PATH_HASH_FIELDS) - set(value)
    if missing:
        raise ActiveRiskRuntimeError(
            f"{expected_path} XFA path is incomplete: "
            + ", ".join(sorted(missing))
        )
    if str(value["path"]) != expected_path:
        raise ActiveRiskRuntimeError(f"{expected_path} XFA path identity drift")
    if value["start_day"] != expected_start_day:
        raise ActiveRiskRuntimeError(f"{expected_path} XFA start-day drift")
    ledger = value["daily_ledger"]
    if not isinstance(ledger, list) or len(ledger) != int(value["observed_days"]):
        raise ActiveRiskRuntimeError(
            f"{expected_path} XFA daily-ledger cardinality drift"
        )
    payout_cycles = sum(bool(row.get("payout_requested")) for row in ledger)
    if payout_cycles != int(value["payout_cycles"]):
        raise ActiveRiskRuntimeError(f"{expected_path} XFA payout cardinality drift")
    source = {field: value[field] for field in _XFA_PATH_HASH_FIELDS}
    if stable_hash(source) != str(value.get("path_hash") or ""):
        raise ActiveRiskRuntimeError(f"{expected_path} XFA path hash drift")
    return dict(value)


def _active_pool_lifecycle_evidence(
    raw: Mapping[str, Any],
    *,
    expected_policy_id: str,
    expected_scenario: str,
    expected_start_day: int,
) -> dict[str, Any]:
    """Validate and seal one FULL-pass XFA overlay inside its Combine row."""

    required = {
        "schema",
        "lifecycle_version",
        "overlay_version",
        "policy_id",
        "scenario",
        "combine_start_day",
        "combine_end_day",
        "combine_status",
        "combine_horizon",
        "xfa_start_day",
        "xfa_horizon_days",
        "xfa_profile",
        "xfa_profile_projection",
        "rule_snapshot",
        "standard",
        "consistency",
        "source_lifecycle_sha256",
    }
    missing = required - set(raw)
    if missing:
        raise ActiveRiskRuntimeError(
            "active XFA lifecycle evidence is incomplete: "
            + ", ".join(sorted(missing))
        )
    source = dict(raw)
    claimed_source_hash = str(source.pop("source_lifecycle_sha256", ""))
    if stable_hash(source) != claimed_source_hash:
        raise ActiveRiskRuntimeError("active XFA source lifecycle hash drift")
    if str(raw["schema"]) != ACTIVE_RISK_XFA_EVIDENCE_SCHEMA:
        raise ActiveRiskRuntimeError("active XFA evidence schema drift")
    if str(raw["lifecycle_version"]) != LIFECYCLE_VERSION:
        raise ActiveRiskRuntimeError("active XFA lifecycle version drift")
    if str(raw["overlay_version"]) != ACTIVE_RISK_XFA_OVERLAY_VERSION:
        raise ActiveRiskRuntimeError("active XFA overlay version drift")
    if str(raw["policy_id"]) != expected_policy_id:
        raise ActiveRiskRuntimeError("active XFA policy identity drift")
    if str(raw["scenario"]) != expected_scenario:
        raise ActiveRiskRuntimeError("active XFA cost-scenario drift")
    if int(raw["combine_start_day"]) != expected_start_day:
        raise ActiveRiskRuntimeError("active XFA Combine start-day drift")
    if str(raw["combine_status"]) != "TARGET_REACHED":
        raise ActiveRiskRuntimeError("active XFA path did not originate from a pass")
    if str(raw["combine_horizon"]) != "FULL_CHRONOLOGICAL_HORIZON":
        raise ActiveRiskRuntimeError("active XFA path is not a FULL-horizon path")
    if int(raw["xfa_horizon_days"]) != _XFA_HORIZON:
        raise ActiveRiskRuntimeError("active XFA horizon drift")
    if bool(raw.get("combine_profit_transferred_to_xfa", True)):
        raise ActiveRiskRuntimeError("Combine profit was transferred to XFA")
    if not bool(raw.get("xfa_profile_frozen_before_replay", False)):
        raise ActiveRiskRuntimeError("active XFA profile was not frozen")
    if bool(raw.get("xfa_profile_selected_from_outcomes", True)):
        raise ActiveRiskRuntimeError("active XFA profile used an outcome oracle")
    if str(raw.get("xfa_overlay_semantics")) != ACTIVE_RISK_XFA_OVERLAY_SEMANTICS:
        raise ActiveRiskRuntimeError("active XFA overlay semantics drift")
    if bool(raw.get("combine_governor_controls_applied_in_xfa", True)):
        raise ActiveRiskRuntimeError("Combine governor controls leaked into XFA")
    if bool(raw.get("payout_path_oracle_used", True)):
        raise ActiveRiskRuntimeError("active XFA payout path used an outcome oracle")
    if raw.get("unrealized_aggregation_semantics") != UNREALIZED_AGGREGATION_SEMANTICS:
        raise ActiveRiskRuntimeError("active XFA unrealized-PnL semantics drift")
    profile = _validate_frozen_mapping_fingerprint(
        raw["xfa_profile"],
        label="profile",
        required_version=("profile_version", LIFECYCLE_VERSION),
    )
    projection = raw["xfa_profile_projection"]
    if not isinstance(projection, Mapping) or dict(projection) != {
        "risk_multiplier": float(profile["risk_multiplier"]),
        "maximum_simultaneous_positions": int(
            profile["maximum_simultaneous_positions"]
        ),
        "maximum_mini_equivalent": int(profile["maximum_mini_equivalent"]),
        "clip_to_official_xfa_scaling_plan": bool(
            profile["clip_to_xfa_scaling_plan"]
        ),
        "same_market_exclusive": bool(profile["same_market_exclusive"]),
    }:
        raise ActiveRiskRuntimeError("active XFA profile projection drift")
    if not bool(profile["clip_to_xfa_scaling_plan"]):
        raise ActiveRiskRuntimeError("active XFA scaling-plan guard is disabled")
    if not bool(profile["same_market_exclusive"]):
        raise ActiveRiskRuntimeError("active XFA same-market guard is disabled")
    rules = _validate_frozen_mapping_fingerprint(
        raw["rule_snapshot"],
        label="rule snapshot",
        required_version=("rule_version", RULE_SNAPSHOT_VERSION),
    )
    xfa_start = (
        None if raw["xfa_start_day"] is None else int(raw["xfa_start_day"])
    )
    standard = _validate_xfa_path_evidence(
        raw["standard"], expected_path="XFA_STANDARD", expected_start_day=xfa_start
    )
    consistency = _validate_xfa_path_evidence(
        raw["consistency"],
        expected_path="XFA_CONSISTENCY",
        expected_start_day=xfa_start,
    )
    payload = {
        **source,
        "xfa_profile": profile,
        "rule_snapshot": rules,
        "standard": standard,
        "consistency": consistency,
        "source_lifecycle_sha256": claimed_source_hash,
    }
    payload["sealed_lifecycle_sha256"] = stable_hash(payload)
    return payload


def _evidence_rows(
    metric: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    episodes: list[dict[str, Any]] = []
    daily: list[dict[str, Any]] = []
    failure = _failure_vectors(metric)[0]
    raw_rows = list(metric.get("evidence_raw") or ())
    lifecycle_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for raw in metric.get("lifecycle_rows") or ():
        key = (str(raw["scenario"]), int(raw["combine_start_day"]))
        if key in lifecycle_by_key:
            raise ActiveRiskRuntimeError("duplicate active XFA lifecycle path")
        lifecycle_by_key[key] = raw
    full_pass_keys = {
        (str(raw["scenario"]), int(raw["start_day"]))
        for raw in raw_rows
        if str(raw.get("horizon_label")) == "FULL_CHRONOLOGICAL_HORIZON"
        and str(raw.get("terminal_classification")) == "TARGET_REACHED"
    }
    if set(lifecycle_by_key) != full_pass_keys:
        raise ActiveRiskRuntimeError(
            "FULL-pass/XFA authoritative evidence cardinality drift"
        )
    for raw in raw_rows:
        episode, paths = _convert_episode(
            raw, manifest, str(raw["scenario"]), failure
        )
        if (
            str(raw.get("horizon_label")) == "FULL_CHRONOLOGICAL_HORIZON"
            and str(raw.get("terminal_classification")) == "TARGET_REACHED"
        ):
            key = (str(raw["scenario"]), int(raw["start_day"]))
            lifecycle = lifecycle_by_key.pop(key, None)
            if lifecycle is None:
                raise ActiveRiskRuntimeError(
                    "FULL-pass episode lacks authoritative XFA evidence"
                )
            episode["active_risk_pool_lifecycle"] = _active_pool_lifecycle_evidence(
                lifecycle,
                expected_policy_id=str(raw["policy_id"]),
                expected_scenario=str(raw["scenario"]),
                expected_start_day=int(raw["start_day"]),
            )
        episodes.append(episode)
        daily.extend(paths)
    if lifecycle_by_key:
        raise ActiveRiskRuntimeError("orphan active XFA lifecycle evidence")
    return episodes, daily


def _compact_metric(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key not in {"evidence_raw", "lifecycle_rows"}
    }


def _select_policies(
    policies: Sequence[ActiveRiskPoolPolicy],
    metrics: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    require_stress: bool,
    stage: str,
) -> tuple[tuple[ActiveRiskPoolPolicy, ...], dict[str, Any]]:
    by_policy = {row.policy_id: row for row in policies}
    eligible: list[Mapping[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    behavior_owner: dict[str, str] = {}
    for row in sorted(metrics, key=lambda value: str(value["policy_id"])):
        reasons: list[str] = []
        if float(row["normal"]["net_total"]) <= 0.0:
            reasons.append("NONPOSITIVE_NORMAL_NET")
        stressed = row.get("stressed")
        if require_stress and (
            not isinstance(stressed, Mapping)
            or float(stressed["net_total"]) <= 0.0
        ):
            reasons.append("NONPOSITIVE_STRESSED_NET")
        if float(row["normal"]["mll_breach_rate"]) > 0.15:
            reasons.append("MLL_TOLERANCE_EXCEEDED")
        behavior = str(row["actual_account_behavior_fingerprint"])
        if behavior in behavior_owner:
            reasons.append(f"BEHAVIORAL_CLONE_OF:{behavior_owner[behavior]}")
        if reasons:
            excluded.append({"policy_id": row["policy_id"], "reasons": reasons})
        else:
            behavior_owner[behavior] = str(row["policy_id"])
            eligible.append(row)
    ranked = _pareto_order(eligible, require_stress=require_stress)
    selected_rows = ranked[:limit]
    selected_ids = {str(row["policy_id"]) for row in selected_rows}
    selected = tuple(
        by_policy[policy_id] for policy_id in sorted(selected_ids)
    )
    decision = {
        "schema": "hydra_active_risk_pareto_selection_v1",
        "stage": stage,
        "input_count": len(metrics),
        "eligible_count": len(eligible),
        "output_limit": limit,
        "output_count": len(selected),
        "selected_policy_ids": sorted(selected_ids),
        "excluded": excluded,
        "ranking": [
            {
                "policy_id": row["policy_id"],
                "rank": index,
                "metrics": _selection_metrics(row, require_stress=require_stress),
            }
            for index, row in enumerate(ranked, start=1)
        ],
        "opaque_score_used": False,
        "development_only": True,
    }
    decision["decision_hash"] = stable_hash(decision)
    return selected, decision


def _pareto_order(
    rows: Sequence[Mapping[str, Any]], *, require_stress: bool
) -> list[Mapping[str, Any]]:
    if not rows:
        return []
    values = np.asarray(
        [_objective_vector(row, require_stress=require_stress) for row in rows],
        dtype=float,
    )
    remaining = list(range(len(rows)))
    ordered: list[int] = []
    while remaining:
        matrix = values[remaining]
        dominated = np.zeros(len(remaining), dtype=bool)
        for lower in range(0, len(remaining), 64):
            upper = min(lower + 64, len(remaining))
            targets = matrix[lower:upper]
            greater_equal = matrix[:, None, :] >= targets[None, :, :]
            greater = matrix[:, None, :] > targets[None, :, :]
            dominates = np.all(greater_equal, axis=2) & np.any(greater, axis=2)
            dominated[lower:upper] = np.any(dominates, axis=0)
        front_positions = [index for index, value in enumerate(dominated) if not value]
        if not front_positions:
            raise ActiveRiskRuntimeError("Pareto decomposition produced no front")
        front = [remaining[index] for index in front_positions]
        front.sort(
            key=lambda index: _selection_key(
                rows[index], require_stress=require_stress
            ),
            reverse=True,
        )
        ordered.extend(front)
        selected = set(front)
        remaining = [index for index in remaining if index not in selected]
    return [rows[index] for index in ordered]


def _objective_vector(
    row: Mapping[str, Any], *, require_stress: bool
) -> tuple[float, ...]:
    normal = row["normal"]
    stressed = row.get("stressed") if require_stress else normal
    assert isinstance(stressed, Mapping)
    utilization = row.get("risk_utilisation") or {}
    suppression = row.get("suppression") or {}
    return (
        float(stressed["pass_rate"]),
        float(normal["pass_rate"]),
        float(stressed["target_progress_median"]),
        float(stressed["target_progress_p25"]),
        float(stressed["net_median"]),
        -float(stressed["mll_breach_rate"]),
        float(stressed["consistency_rate"]),
        float(utilization.get("mean", 0.0)),
        -float(suppression.get("signals_rejected", 0.0)),
    )


def _selection_key(
    row: Mapping[str, Any], *, require_stress: bool
) -> tuple[Any, ...]:
    normal = row["normal"]
    stressed = row.get("stressed") if require_stress else normal
    assert isinstance(stressed, Mapping)
    return (
        int(stressed["pass_count"]),
        int(normal["pass_count"]),
        float(stressed["target_progress_p25"]),
        float(stressed["target_progress_median"]),
        float(stressed["net_median"]),
        -float(stressed["mll_breach_rate"]),
        float(stressed["consistency_rate"]),
        -float(stressed["maximum_block_profit_share"]),
        str(row["policy_id"]),
    )


def _selection_metrics(
    row: Mapping[str, Any], *, require_stress: bool
) -> dict[str, Any]:
    normal = row["normal"]
    stressed = row.get("stressed") if require_stress else normal
    assert isinstance(stressed, Mapping)
    return {
        "normal_pass_count": normal["pass_count"],
        "stressed_pass_count": stressed["pass_count"],
        "stressed_target_progress_p25": stressed["target_progress_p25"],
        "stressed_target_progress_median": stressed["target_progress_median"],
        "stressed_net_median": stressed["net_median"],
        "stressed_mll_breach_rate": stressed["mll_breach_rate"],
        "consistency_rate": stressed["consistency_rate"],
    }


def _select_for_96(
    policies: Sequence[ActiveRiskPoolPolicy],
    metrics: Sequence[Mapping[str, Any]],
    controls: Mapping[str, Any],
    *,
    limit: int,
) -> tuple[tuple[ActiveRiskPoolPolicy, ...], dict[str, Any]]:
    static = controls["static_partition"]["stressed"]
    eligible: list[Mapping[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in metrics:
        normal = row["normal"]
        stressed = row["stressed"]
        random_match = (
            controls.get("random_priority_exposure_match_by_policy") or {}
        ).get(str(row["policy_id"]))
        block_improvements = sum(
            float(value)
            > float(static.get("by_block_target_progress_median", {}).get(block, math.inf))
            for block, value in stressed.get("by_block_target_progress_median", {}).items()
        )
        reasons: list[str] = []
        if int(normal["pass_count"]) < 3:
            reasons.append("FEWER_THAN_THREE_NORMAL_PASSES")
        if int(stressed["pass_count"]) < 2:
            reasons.append("FEWER_THAN_TWO_STRESSED_PASSES")
        if float(stressed["net_total"]) <= 0.0:
            reasons.append("NONPOSITIVE_STRESSED_NET")
        if float(stressed["target_progress_median"]) <= float(static["target_progress_median"]):
            reasons.append("NO_STATIC_PARTITION_TARGET_PROGRESS_IMPROVEMENT")
        if float(stressed["mll_breach_rate"]) > 0.10:
            reasons.append("MLL_TOLERANCE_EXCEEDED")
        if float(stressed["consistency_rate"]) < 0.50:
            reasons.append("CONSISTENCY_UNACCEPTABLE")
        if int(stressed["pass_block_count"]) < 2 and block_improvements < 2:
            reasons.append("INSUFFICIENT_INDEPENDENT_BLOCK_EVIDENCE")
        if not isinstance(random_match, Mapping) or not bool(
            random_match.get("matched")
        ):
            reasons.append("RANDOM_PRIORITY_CONTROL_UNMATCHED")
        if reasons:
            excluded.append({"policy_id": row["policy_id"], "reasons": reasons})
        else:
            eligible.append(row)
    ranked = _pareto_order(eligible, require_stress=True)[:limit]
    ids = {str(row["policy_id"]) for row in ranked}
    by_id = {row.policy_id: row for row in policies}
    selected = tuple(by_id[value] for value in sorted(ids))
    decision = {
        "schema": "hydra_active_risk_promotion_decision_v1",
        "stage": "ACTIVE_POOL_STAGE_3_TO_96",
        "input_count": len(metrics),
        "eligible_count": len(eligible),
        "output_limit": limit,
        "output_count": len(selected),
        "selected_policy_ids": sorted(ids),
        "excluded": excluded,
        "gates_frozen_before_outcomes": True,
        "development_only": True,
    }
    decision["decision_hash"] = stable_hash(decision)
    return selected, decision


def _select_confirmation_candidates(
    policies: Sequence[ActiveRiskPoolPolicy],
    metrics: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> tuple[tuple[ActiveRiskPoolPolicy, ...], dict[str, Any]]:
    eligible = [row for row in metrics if _confirmation_gate(row)]
    ranked = _pareto_order(eligible, require_stress=True)[:limit]
    ids = {str(row["policy_id"]) for row in ranked}
    by_id = {row.policy_id: row for row in policies}
    selected = tuple(by_id[value] for value in sorted(ids))
    excluded = [
        {"policy_id": row["policy_id"], "reasons": _confirmation_reasons(row)}
        for row in metrics
        if str(row["policy_id"]) not in ids
    ]
    decision = {
        "schema": "hydra_active_risk_confirmation_selection_v1",
        "stage": "ACTIVE_POOL_EXPANDED_CONFIRMATION_GATE",
        "input_count": len(metrics),
        "eligible_count": len(eligible),
        "output_limit": limit,
        "output_count": len(selected),
        "selected_policy_ids": sorted(ids),
        "excluded": excluded,
        "development_only": True,
        "paper_shadow_ready": False,
    }
    decision["decision_hash"] = stable_hash(decision)
    return selected, decision


def _confirmation_gate(row: Mapping[str, Any]) -> bool:
    return not _confirmation_reasons(row)


def _confirmation_reasons(row: Mapping[str, Any]) -> list[str]:
    normal = row["normal"]
    stressed = row["stressed"]
    reasons: list[str] = []
    if float(normal["pass_rate"]) < 0.10:
        reasons.append("NORMAL_PASS_RATE_BELOW_10_PERCENT")
    if float(stressed["pass_rate"]) < 0.05:
        reasons.append("STRESSED_PASS_RATE_BELOW_5_PERCENT")
    if float(stressed["net_total"]) <= 0.0:
        reasons.append("NONPOSITIVE_STRESSED_ECONOMICS")
    if float(stressed["mll_breach_rate"]) > 0.10:
        reasons.append("MLL_TOLERANCE_EXCEEDED")
    if int(stressed["pass_block_count"]) < 2:
        reasons.append("PASS_EVIDENCE_NOT_BLOCK_DIVERSE")
    if float(stressed["maximum_block_profit_share"]) > 0.65:
        reasons.append("BLOCK_DOMINATION")
    if float(stressed["maximum_sleeve_profit_share"]) > 0.65:
        reasons.append("SLEEVE_DOMINATION")
    return reasons


def _metrics_for(
    rows: Sequence[Mapping[str, Any]], policy_ids: set[str]
) -> list[Mapping[str, Any]]:
    return [row for row in rows if str(row["policy_id"]) in policy_ids]


def _merge_metrics(
    left_rows: Sequence[Mapping[str, Any]],
    right_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    right = {str(row["policy_id"]): row for row in right_rows}
    output: list[dict[str, Any]] = []
    for left in left_rows:
        policy_id = str(left["policy_id"])
        if policy_id not in right:
            output.append(dict(left))
            continue
        other = right[policy_id]
        horizons: dict[str, dict[str, Any]] = {"normal": {}, "stressed": {}}
        for scenario in ("normal", "stressed"):
            labels = set((left.get("horizons") or {}).get(scenario, {})) | set(
                (other.get("horizons") or {}).get(scenario, {})
            )
            for label in labels:
                one = (left.get("horizons") or {}).get(scenario, {}).get(label)
                two = (other.get("horizons") or {}).get(scenario, {}).get(label)
                horizons[scenario][label] = (
                    _merge_episode_summary(one, two)
                    if isinstance(one, Mapping) and isinstance(two, Mapping)
                    else dict(one or two or {})
                )
        canonical = (
            _CANONICAL_HORIZON
            if _CANONICAL_HORIZON in horizons["normal"]
            else next(iter(horizons["normal"]))
        )
        lifecycle_keys = (
            "xfa_paths_started", "xfa_standard_paths", "xfa_consistency_paths",
            "first_payouts", "payout_cycles", "trader_net_payout",
            "post_payout_survival_count",
        )
        merged = {
            **dict(left),
            "normal": horizons["normal"][canonical],
            "stressed": horizons["stressed"].get(canonical),
            "horizons": horizons,
            "risk_utilisation": _merge_utilisation(
                left.get("risk_utilisation") or {}, other.get("risk_utilisation") or {}
            ),
            "suppression": _merge_suppression(
                left.get("suppression") or {}, other.get("suppression") or {}
            ),
            "actual_account_behavior_fingerprint": stable_hash(
                [
                    left.get("actual_account_behavior_fingerprint"),
                    other.get("actual_account_behavior_fingerprint"),
                ]
            ),
        }
        for key in lifecycle_keys:
            merged[key] = float(left.get(key, 0)) + float(other.get(key, 0))
            if key != "trader_net_payout":
                merged[key] = int(merged[key])
        total_paths = int(merged["xfa_standard_paths"]) + int(
            merged["xfa_consistency_paths"]
        )
        merged["post_payout_survival_rate"] = (
            int(merged["post_payout_survival_count"]) / total_paths
            if total_paths else 0.0
        )
        output.append(merged)
    return sorted(output, key=lambda row: str(row["policy_id"]))


def _merge_episode_summary(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, Any]:
    net = [*left.get("net_values", ()), *right.get("net_values", ())]
    progress = [
        *left.get("target_progress_values", ()),
        *right.get("target_progress_values", ()),
    ]
    durations = [
        *left.get("duration_trading_days_values", ()),
        *right.get("duration_trading_days_values", ()),
    ]
    active_durations = [
        *left.get("active_trading_days_values", ()),
        *right.get("active_trading_days_values", ()),
    ]
    calendar = [
        *left.get("calendar_days_values", ()),
        *right.get("calendar_days_values", ()),
    ]
    target_days = [
        *left.get("days_to_target_values", ()),
        *right.get("days_to_target_values", ()),
    ]
    count = int(left["episode_count"]) + int(right["episode_count"])
    passes = int(left["pass_count"]) + int(right["pass_count"])
    breaches = int(left["mll_breach_count"]) + int(right["mll_breach_count"])
    censored = int(left["censored_episode_count"]) + int(right["censored_episode_count"])
    consistency = int(left["consistency_ok_count"]) + int(right["consistency_ok_count"])
    terminals = Counter(left.get("terminal_distribution") or {})
    terminals.update(right.get("terminal_distribution") or {})
    by_block_net = defaultdict(float, left.get("by_block_net") or {})
    for key, value in (right.get("by_block_net") or {}).items():
        by_block_net[key] += float(value)
    by_block_progress: dict[str, list[float]] = defaultdict(list)
    # Per-block medians cannot be reconstructed exactly from two compact
    # medians.  Preserve a count-weighted two-point merge and identify it as
    # expanded-start aggregation; selection never uses it before Stage 3.
    for source in (left, right):
        for key, value in (source.get("by_block_target_progress_median") or {}).items():
            by_block_progress[key].append(float(value))
    contribution = defaultdict(float, left.get("component_contribution") or {})
    for key, value in (right.get("component_contribution") or {}).items():
        contribution[key] += float(value)
    positive_blocks = sum(max(value, 0.0) for value in by_block_net.values())
    positive_components = sum(max(value, 0.0) for value in contribution.values())
    projected_active = [
        duration / value
        for duration, value in zip(active_durations, progress, strict=True)
        if float(value) > 0.0
    ]
    projected_calendar = [
        duration / value
        for duration, value in zip(calendar, progress, strict=True)
        if float(value) > 0.0
    ]
    return {
        "episode_count": count,
        "pass_count": passes,
        "pass_rate": passes / max(count, 1),
        "mll_breach_count": breaches,
        "mll_breach_rate": breaches / max(count, 1),
        "censored_episode_count": censored,
        "censoring_rate": censored / max(count, 1),
        "terminal_distribution": dict(sorted(terminals.items())),
        "net_total": sum(float(value) for value in net),
        "net_median": statistics.median(net) if net else 0.0,
        "net_values": net,
        "target_progress_median": statistics.median(progress) if progress else 0.0,
        "target_progress_p25": _quantile(progress, 0.25) if progress else 0.0,
        "target_progress_values": progress,
        "maximum_target_progress": max(
            float(left.get("maximum_target_progress", 0.0)),
            float(right.get("maximum_target_progress", 0.0)),
        ),
        "minimum_mll_buffer": min(
            float(left.get("minimum_mll_buffer", 4500.0)),
            float(right.get("minimum_mll_buffer", 4500.0)),
        ),
        "consistency_rate": consistency / max(count, 1),
        "consistency_ok_count": consistency,
        "duration_trading_days_values": durations,
        "duration_trading_days_median": statistics.median(durations) if durations else 0.0,
        "active_trading_days_values": active_durations,
        "active_trading_days_median": statistics.median(active_durations) if active_durations else 0.0,
        "calendar_days_values": calendar,
        "calendar_days_median": statistics.median(calendar) if calendar else 0.0,
        "days_to_target_values": target_days,
        "median_days_to_target": statistics.median(target_days) if target_days else None,
        "projected_active_days_to_target_median": statistics.median(projected_active) if projected_active else None,
        "projected_calendar_days_to_target_median": statistics.median(projected_calendar) if projected_calendar else None,
        "monthly_subscription_duration_proxy_median": statistics.median(projected_calendar) / 30.0 if projected_calendar else None,
        "pass_block_count": len(set(left.get("pass_block_ids", ())) | set(right.get("pass_block_ids", ()))),
        "pass_block_ids": sorted(set(left.get("pass_block_ids", ())) | set(right.get("pass_block_ids", ()))),
        "by_block_net": dict(sorted(by_block_net.items())),
        "by_block_target_progress_median": {
            key: statistics.median(values)
            for key, values in sorted(by_block_progress.items())
        },
        "component_contribution": dict(sorted(contribution.items())),
        "maximum_block_profit_share": (
            max((max(value, 0.0) for value in by_block_net.values()), default=0.0)
            / positive_blocks if positive_blocks else 0.0
        ),
        "maximum_sleeve_profit_share": (
            max((max(value, 0.0) for value in contribution.values()), default=0.0)
            / positive_components if positive_components else 0.0
        ),
    }


def _merge_utilisation(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    total = int(left.get("observation_count", 0)) + int(right.get("observation_count", 0))
    mean = (
        float(left.get("mean", 0.0)) * int(left.get("observation_count", 0))
        + float(right.get("mean", 0.0)) * int(right.get("observation_count", 0))
    ) / max(total, 1)
    return {
        "risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
        "actual_stop_risk_available": False,
        "observation_count": total,
        "mean": mean,
        "median": statistics.median(
            [float(left.get("median", 0.0)), float(right.get("median", 0.0))]
        ),
        "p25": min(float(left.get("p25", 0.0)), float(right.get("p25", 0.0))),
        "p75": max(float(left.get("p75", 0.0)), float(right.get("p75", 0.0))),
        "by_active_sleeve_count": {},
    }


def _merge_suppression(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    counts = Counter(left.get("decision_status_counts") or {})
    counts.update(right.get("decision_status_counts") or {})
    emitted = int(left.get("signals_emitted", 0)) + int(right.get("signals_emitted", 0))
    accepted = int(left.get("signals_accepted", 0)) + int(right.get("signals_accepted", 0))
    return {
        "signals_emitted": emitted,
        "signals_accepted": accepted,
        "signals_rejected": emitted - accepted,
        "decision_status_counts": dict(sorted(counts.items())),
        "size_reduced": counts["SIZE_REDUCED"],
        "conflict_rejected": counts["CONFLICT_REJECTED"],
        "contract_limit_rejected": counts["CONTRACT_LIMIT_REJECTED"],
        "mll_risk_rejected": counts["MLL_RISK_REJECTED"],
        "foregone_realized_pnl_ex_post": float(left.get("foregone_realized_pnl_ex_post", 0.0)) + float(right.get("foregone_realized_pnl_ex_post", 0.0)),
        "foregone_expected_pnl": None,
        "foregone_expected_pnl_status": "UNAVAILABLE_NO_FROZEN_PRE_OUTCOME_ESTIMATE",
        "foregone_realized_pnl_used_for_routing": False,
    }


def _metric_episode_total(rows: Sequence[Mapping[str, Any]]) -> int:
    return _scenario_episode_total(rows, "normal") + _scenario_episode_total(
        rows, "stressed"
    )


def _scenario_episode_total(
    rows: Sequence[Mapping[str, Any]], scenario: str
) -> int:
    return sum(
        int(summary.get("episode_count", 0))
        for row in rows
        for summary in (row.get(scenario),)
        if isinstance(summary, Mapping)
    )


def _aggregate_utilisation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    observations = sum(
        int((row.get("risk_utilisation") or {}).get("observation_count", 0))
        for row in rows
    )
    weighted = sum(
        float((row.get("risk_utilisation") or {}).get("mean", 0.0))
        * int((row.get("risk_utilisation") or {}).get("observation_count", 0))
        for row in rows
    )
    return {
        "risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
        "actual_stop_risk_available": False,
        "observation_count": observations,
        "mean": weighted / observations if observations else 0.0,
        "policy_median_of_medians": statistics.median(
            [
                float((row.get("risk_utilisation") or {}).get("median", 0.0))
                for row in rows
            ]
        ) if rows else 0.0,
    }


def _aggregate_suppression(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(
            (row.get("suppression") or {}).get("decision_status_counts") or {}
        )
    emitted = sum(
        int((row.get("suppression") or {}).get("signals_emitted", 0))
        for row in rows
    )
    accepted = sum(
        int((row.get("suppression") or {}).get("signals_accepted", 0))
        for row in rows
    )
    return {
        "signals_emitted": emitted,
        "signals_accepted": accepted,
        "signals_rejected": emitted - accepted,
        "decision_status_counts": dict(sorted(counts.items())),
        "foregone_realized_pnl_ex_post": sum(
            float((row.get("suppression") or {}).get("foregone_realized_pnl_ex_post", 0.0))
            for row in rows
        ),
        "counterfactual_role": "POSTHOC_DIAGNOSTIC_NOT_ROUTING_INPUT",
    }


def _aggregate_horizons(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for label in (
        "20_TRADING_DAYS", "40_TRADING_DAYS", "60_TRADING_DAYS",
        "90_TRADING_DAYS", "FULL_CHRONOLOGICAL_HORIZON",
    ):
        output[label] = {}
        for scenario in ("normal", "stressed"):
            summaries = [
                row["horizons"][scenario][label]
                for row in rows
                if label in (row.get("horizons") or {}).get(scenario, {})
            ]
            output[label][scenario] = {
                "policy_count": len(summaries),
                "pass_count": sum(int(row["pass_count"]) for row in summaries),
                "episode_count": sum(int(row["episode_count"]) for row in summaries),
                "pass_rate": (
                    sum(int(row["pass_count"]) for row in summaries)
                    / max(sum(int(row["episode_count"]) for row in summaries), 1)
                ),
                "mll_breach_rate": (
                    sum(int(row["mll_breach_count"]) for row in summaries)
                    / max(sum(int(row["episode_count"]) for row in summaries), 1)
                ),
                "target_progress_policy_median": statistics.median(
                    [float(row["target_progress_median"]) for row in summaries]
                ) if summaries else 0.0,
                "censored_episode_count": sum(
                    int(row["censored_episode_count"]) for row in summaries
                ),
                "projected_active_days_to_target_policy_median": _optional_median(
                    [row.get("projected_active_days_to_target_median") for row in summaries]
                ),
                "projected_calendar_days_to_target_policy_median": _optional_median(
                    [row.get("projected_calendar_days_to_target_median") for row in summaries]
                ),
            }
    return output


def _assert_active_authoritative_episode_counters(
    economic_results: Mapping[str, Any], evidence_receipt: Mapping[str, Any]
) -> None:
    """Keep persisted multi-horizon rows distinct from canonical headlines."""

    counters = economic_results.get("production_counters")
    dataset_counts = evidence_receipt.get("dataset_row_counts")
    if not isinstance(counters, Mapping) or not isinstance(dataset_counts, Mapping):
        raise ActiveRiskRuntimeError(
            "active-risk result lacks authoritative episode counters"
        )
    total = int(counters.get("combine_episodes_completed", -1))
    normal_total = int(counters.get("normal_episodes_completed", -1))
    stressed_total = int(counters.get("stressed_episodes_completed", -1))
    persisted = int(dataset_counts.get("episodes", -1))
    if total < 0 or total != normal_total + stressed_total or total != persisted:
        raise ActiveRiskRuntimeError(
            "active-risk counters diverge from persisted multi-horizon episodes"
        )

    frontier = economic_results.get("horizon_frontier")
    canonical = (
        frontier.get(_CANONICAL_HORIZON)
        if isinstance(frontier, Mapping)
        else None
    )
    if not isinstance(canonical, Mapping):
        raise ActiveRiskRuntimeError(
            "active-risk result lacks the frozen canonical horizon"
        )
    for scenario, total_for_scenario, headline_field in (
        ("normal", normal_total, "normal_combine_passes"),
        ("stressed", stressed_total, "stressed_combine_passes"),
    ):
        row = canonical.get(scenario)
        if not isinstance(row, Mapping):
            raise ActiveRiskRuntimeError(
                f"active-risk canonical {scenario} horizon is malformed"
            )
        episodes = int(row.get("episode_count", -1))
        passes = int(row.get("pass_count", -1))
        if (
            episodes < 0
            or episodes > total_for_scenario
            or passes < 0
            or passes > episodes
            or passes != int(economic_results.get(headline_field, -1))
        ):
            raise ActiveRiskRuntimeError(
                f"active-risk canonical {scenario} counters drift"
            )


def _sealed_active_scientific_status(summary: Mapping[str, Any]) -> str:
    allowed = {
        "ACTIVE_POOL_CONFIRMATION_CANDIDATES_DEVELOPMENT_ONLY",
        "ACTIVE_RISK_POOL_TARGET_VELOCITY_IMPROVED_WEAK",
        "POSITIVE_BUT_INSUFFICIENT_TARGET_VELOCITY",
    }
    status = str(summary.get("scientific_status") or "")
    if status not in allowed:
        raise ActiveRiskRuntimeError(
            "sealed active-risk scientific status is absent or unsupported"
        )
    has_confirmation = bool(summary.get("confirmation_ready_candidate_ids"))
    if has_confirmation != (
        status == "ACTIVE_POOL_CONFIRMATION_CANDIDATES_DEVELOPMENT_ONLY"
    ):
        raise ActiveRiskRuntimeError(
            "sealed active-risk scientific status contradicts candidate evidence"
        )
    return status


def _failure_vectors(row: Mapping[str, Any]) -> list[str]:
    stress = row.get("stressed") or row.get("normal") or {}
    output: list[str] = []
    if float(stress.get("mll_breach_rate", 0.0)) > 0.10:
        output.append("MLL_BREACH")
    if float(stress.get("net_total", 0.0)) <= 0.0:
        output.append("COST_FRAGILITY")
    if float(stress.get("target_progress_median", 0.0)) < 0.35:
        output.append("TARGET_TOO_SLOW")
    if float(stress.get("maximum_sleeve_profit_share", 0.0)) > 0.65:
        output.append("OVER_CONCENTRATION")
    suppression = row.get("suppression") or {}
    if int(suppression.get("signals_rejected", 0)) > int(
        suppression.get("signals_accepted", 0)
    ):
        output.append("INSUFFICIENT_OPPORTUNITIES")
    return output or ["NO_INCREMENTAL_VALUE"]


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires values")
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _optional_median(values: Sequence[Any]) -> float | None:
    finite = [float(value) for value in values if value is not None]
    return statistics.median(finite) if finite else None


__all__ = [
    "ACTIVE_RISK_RUNTIME_VERSION",
    "ActiveRiskPoolRun",
    "ActiveRiskRuntimeError",
    "run_active_risk_manifest",
]
