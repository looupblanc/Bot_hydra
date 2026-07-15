"""Manifest-driven portfolio-first Combine-to-payout production runtime.

The hot loop changes only account-book membership and frozen sleeve risk units.
All source sleeve specifications are compiled once from the existing canonical
cache, reconciled to their immutable ledger declarations, then inherited by
forked replay workers.  One asynchronous EvidenceBundle writer remains the
only authoritative evidence writer.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import statistics
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Mapping, Sequence

from hydra.account_policy.basket import RoutedTrade, run_shared_account_episode
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import RECORD_SPECS, REQUIRED_DATASETS
from hydra.production.component_evidence import materialize_component_evidence
from hydra.production.episode_evidence import _convert_episode
from hydra.production.evidence_adapter import AsyncEvidenceBundleSink
from hydra.production.halving import build_final_result_payload
from hydra.production.manifest import PRODUCTION_RESULT_SCHEMA
from hydra.production.policy_factory import ComponentCandidate, load_component_candidates
from hydra.production.portfolio_books import (
    BookPair,
    PortfolioBookGeneratorSpec,
    SleeveRecord,
    generate_portfolio_book_pairs,
)
from hydra.production.portfolio_manifest import PORTFOLIO_RUNTIME_VERSION
from hydra.production.replay import _episode_row
from hydra.production.runtime import (
    ProductionRuntimeError,
    _ProductionRun,
    _block_aware_starts,
    _block_calendars,
    _load_json,
    _sha256,
    load_and_verify_production_result,
)
from hydra.propfirm.combine_to_xfa import (
    FrozenRiskProfile,
    UNREALIZED_AGGREGATION_SEMANTICS,
    _enforce_combine_market_caps,
    _scale_events,
    official_rule_snapshot_2026_07_15,
)
from hydra.propfirm.portfolio_combine_to_xfa import (
    PORTFOLIO_ACCOUNT_POLICY_VERSION,
    PortfolioBookRole,
    PortfolioBasketPolicy,
    freeze_portfolio_book,
    run_portfolio_combine_to_xfa_episode,
)
from hydra.shadow.package_factory import PACKAGE_SCHEMA as SHADOW_PACKAGE_SCHEMA, write_shadow_package
from hydra.shadow.portfolio_package import (
    build_portfolio_shadow_package,
    reconstruct_portfolio_shadow_package,
)
from hydra.promotion.portfolio_status import (
    BookEvidence,
    PortfolioStatus,
    decide_book_statuses,
)


_WORK_STATE: dict[str, Any] = {}
_HORIZON = 90
_XFA_HORIZON = 120


class PortfolioRuntimeError(ProductionRuntimeError):
    pass


def run_portfolio_first_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path,
    cache_root: str | Path,
    stop_after: str | None = None,
) -> dict[str, Any]:
    return PortfolioFirstRun(
        manifest_path=Path(manifest_path),
        contract_map_path=Path(contract_map_path),
        cache_root=Path(cache_root),
        stop_after=stop_after,
    ).execute()


class PortfolioFirstRun(_ProductionRun):
    def execute(self) -> dict[str, Any]:
        result_path = self.output_dir / str(self.manifest["runtime"]["result_name"])
        if result_path.is_file():
            result = load_and_verify_production_result(result_path, self.manifest)
            # The result can legitimately predate the external forward-package
            # anchor by a few filesystem writes.  Replaying this reconciliation
            # before publishing terminal snapshots makes a second restart heal
            # a crash that happened after the sealed result was written but
            # before PENDING receipts were anchored.
            result = self._reconcile_forward_package_anchors(result)
            self._reconcile_completed_result_snapshots(result)
            return result
        sink: AsyncEvidenceBundleSink | None = None
        try:
            self._verify_source_commit()
            cemetery_audit = self._audit_cemetery()
            self.payload_writer.write_json(
                "portfolio_cemetery_audit.json", cemetery_audit
            )
            recovered = self._recover_sealed_bundle_result()
            if recovered is not None:
                return recovered
            sleeves = self._sleeve_records()
            pairs = self._load_or_generate_pairs(sleeves)
            self._publish(
                state="POPULATION_FROZEN",
                stage="PORTFOLIO_STAGE_0",
                policies_proposed=len(pairs),
                portfolio_book_pairs_generated=len(pairs),
                sleeve_bank_size=len(sleeves),
                behavioral_cluster_count=len(sleeves),
                next_action="COMPILE_IMMUTABLE_SLEEVES_ONCE",
            )

            components = load_component_candidates(self.manifest, self.root)
            component_map = {row.sleeve.sleeve_id: row for row in components}
            required_ids = {row.sleeve_id for row in sleeves}
            if not required_ids.issubset(component_map):
                raise PortfolioRuntimeError("frozen sleeve bank is absent from component source")
            matrices = self._open_cached_matrices()
            runtimes, failures = self._compile_components(
                components, required_ids, matrices
            )
            if failures or set(runtimes) != required_ids:
                self.payload_writer.write_json("portfolio_component_failures.json", failures)
                raise PortfolioRuntimeError("portfolio sleeve bank failed exact compilation")
            self._reconcile_sleeves(
                sleeves,
                runtimes,
                components=component_map,
                matrices=matrices,
            )

            starts48 = _block_aware_starts(runtimes, self.manifest, maximum=48)
            starts96 = _block_aware_starts(
                runtimes, self.manifest, maximum=96, required_starts=starts48
            )
            starts192 = _block_aware_starts(
                runtimes, self.manifest, maximum=192, required_starts=starts96
            )
            calendars = _block_calendars(runtimes, self.manifest, starts192)
            # Combine independence is bounded by each frozen temporal block.
            # Once a pass occurs, XFA is a fresh account and follows the full
            # remaining cached chronology.  Keeping these calendars separate
            # avoids structurally censoring every 120-day payout lifecycle at
            # a block boundary while preserving block-aware Combine starts.
            self.xfa_calendars = _remaining_chronological_calendars(
                runtimes, starts192
            )
            timelines = {key: tuple(value.events) for key, value in runtimes.items()}
            stage12_starts = tuple(
                next(start for start in starts48 if _block_for_day(start, self.manifest) == block["block_id"])
                for block in self.manifest["temporal_blocks"]["blocks"]
            )

            stage1 = self._run_combine_stage(
                "stage1",
                pairs,
                timelines,
                stage12_starts,
                calendars,
                stressed=False,
                prior_episode_count=0,
                prior_normal_episode_count=0,
                prior_stressed_episode_count=0,
            )
            stage1_pairs = _select_pairs(
                pairs,
                stage1,
                limit=2_000,
                require_stress=False,
                required_sleeve_ids={row.sleeve_id for row in sleeves},
            )
            self._write_halving("stage1", pairs, stage1_pairs, stage1)
            self._publish(
                state="EXACT_REPLAY_ACTIVE",
                stage="PORTFOLIO_STAGE_2",
                unique_policies_screened=len(stage1),
                fast_screen_episode_observations=_episode_total(stage1),
                combine_episodes_completed=0,
                normal_episodes_completed=0,
                stressed_episodes_completed=0,
                stage1_survivor_count=len(stage1_pairs),
                next_action="EXACT_NORMAL_AND_STRESSED_ACCOUNT_REPLAY",
            )
            if not stage1_pairs:
                raise PortfolioRuntimeError("Stage-1 produced no auditable portfolio policy")

            stage2 = self._run_combine_stage(
                "stage2",
                stage1_pairs,
                timelines,
                stage12_starts,
                calendars,
                stressed=True,
                prior_episode_count=0,
                prior_normal_episode_count=0,
                prior_stressed_episode_count=0,
            )
            stage2_pairs = _select_pairs(
                stage1_pairs, stage2, limit=256, require_stress=True
            )
            self._write_halving("stage2", stage1_pairs, stage2_pairs, stage2)
            if not stage2_pairs:
                raise PortfolioRuntimeError("Stage-2 produced no auditable exact policy")
            self.portfolio_metrics = {row["pair_id"]: row for row in stage2}
            # Stage-3 replaces the four Stage-2 starts for advancing books with
            # its complete 48-start lifecycle record.  Only eliminated Stage-2
            # books are persisted at this boundary, so authoritative episode
            # counters must exclude the deferred rows.  CPU work remains
            # visible separately as replay computations.
            stage2_persisted = _metrics_excluding_pairs(stage2, stage2_pairs)
            prior_episodes = _episode_total(stage2_persisted)
            prior_normal_episodes = _scenario_episode_total(
                stage2_persisted, "normal"
            )
            prior_stressed_episodes = _scenario_episode_total(
                stage2_persisted, "stressed"
            )
            if prior_episodes != prior_normal_episodes + prior_stressed_episodes:
                raise PortfolioRuntimeError(
                    "Stage-2 authoritative episode counters do not reconcile"
                )
            self._publish(
                state="ROBUSTNESS_ACTIVE",
                stage="PORTFOLIO_STAGE_3_48_STARTS",
                exact_account_replays=len(stage2),
                replay_computations_completed=_episode_total(stage2),
                combine_episodes_completed=prior_episodes,
                normal_episodes_completed=prior_normal_episodes,
                stressed_episodes_completed=prior_stressed_episodes,
                stage2_survivor_count=len(stage2_pairs),
                next_action="STREAM_COMBINE_PASSES_TO_BOTH_XFA_PATHS",
            )

            sink = self._open_portfolio_evidence(
                stage1_pairs, sleeves, stage12_starts
            )
            self._persist_static_evidence(
                sink, stage1_pairs, sleeves, component_map, runtimes, matrices
            )
            # Every Stage-2 exact replay is authoritative economic evidence.
            # Persist base four-block evidence for books eliminated at Stage 2.
            # Advancing books are deliberately excluded here because Stage 3
            # evaluates and persists the same starts as part of its frozen
            # 48-start lifecycle run; this keeps episode keys unique while
            # preserving complete coverage of the exact-replay universe.
            self._persist_stage2_evidence(
                sink,
                exact_pair_ids={row.pair_id for row in stage1_pairs},
                excluded_pair_ids={row.pair_id for row in stage2_pairs},
            )
            stage3 = self._run_lifecycle_stage(
                "stage3",
                stage2_pairs,
                timelines,
                starts48,
                calendars,
                sink=sink,
                prior_episode_count=prior_episodes,
                prior_lifecycle_metrics=(),
                prior_normal_episode_count=prior_normal_episodes,
                prior_stressed_episode_count=prior_stressed_episodes,
            )
            self.portfolio_metrics = {row["pair_id"]: row for row in stage3}
            stage3_pairs = _select_lifecycle_pairs(
                stage2_pairs, stage3, limit=32, minimum_starts=48
            )
            self._write_halving("stage3", stage2_pairs, stage3_pairs, stage3)
            self._publish(
                state="EXPANDED_EPISODES_ACTIVE",
                stage="PORTFOLIO_STAGE_4_96_STARTS",
                candidates_promoted_96=len(stage3_pairs),
                stage3_survivor_count=len(stage3_pairs),
                next_action="REPLAY_FROZEN_BOOKS_TO_96_STARTS_NO_RETUNING",
            )

            stage4 = self._run_lifecycle_stage(
                "stage4",
                stage3_pairs,
                timelines,
                tuple(row for row in starts96 if row not in set(starts48)),
                calendars,
                sink=sink,
                prior_episode_count=int(self.state["combine_episodes_completed"]),
                prior_lifecycle_metrics=stage3,
                prior_normal_episode_count=int(self.state.get("normal_episodes_completed", 0)),
                prior_stressed_episode_count=int(self.state.get("stressed_episodes_completed", 0)),
            ) if stage3_pairs else []
            stage3_selected = _metrics_for_pairs(stage3, stage3_pairs)
            combined96 = _combine_stage_metrics(stage3_selected, stage4)
            self.portfolio_metrics = {row["pair_id"]: row for row in combined96}
            stage4_pairs = _select_lifecycle_pairs(
                stage3_pairs, combined96, limit=8, minimum_starts=96
            )
            self._write_halving("stage4", stage3_pairs, stage4_pairs, combined96)
            self._publish(
                state="EXPANDED_EPISODES_ACTIVE",
                stage="PORTFOLIO_STAGE_5_192_STARTS",
                candidates_surviving_96=len(stage4_pairs),
                next_action="REPLAY_FINALISTS_TO_192_STARTS_NO_MUTATION",
            )

            stage5 = self._run_lifecycle_stage(
                "stage5",
                stage4_pairs,
                timelines,
                tuple(row for row in starts192 if row not in set(starts96)),
                calendars,
                sink=sink,
                prior_episode_count=int(self.state["combine_episodes_completed"]),
                # KPI counters cover every completed lifecycle path, including
                # books eliminated after Stage 3.  The per-book metric merge
                # below intentionally remains restricted to advancing books.
                prior_lifecycle_metrics=(*stage3, *stage4),
                prior_normal_episode_count=int(self.state.get("normal_episodes_completed", 0)),
                prior_stressed_episode_count=int(self.state.get("stressed_episodes_completed", 0)),
            ) if stage4_pairs else []
            stage4_selected = _metrics_for_pairs(combined96, stage4_pairs)
            combined192 = _combine_stage_metrics(stage4_selected, stage5)
            self.portfolio_metrics = {row["pair_id"]: row for row in combined192}
            finalists = _select_lifecycle_pairs(
                stage4_pairs, combined192, limit=6, minimum_starts=192
            )
            self._write_halving("stage5", stage4_pairs, finalists, combined192)
            return self._finalize_portfolio(
                sink,
                population=pairs,
                stage1=stage1,
                stage2=stage2,
                stage3=stage3,
                stage4=combined96,
                stage5=combined192,
                finalists=finalists,
                sleeve_specs={
                    sleeve_id: component_map[sleeve_id].sleeve
                    for sleeve_id in required_ids
                },
            )
        except BaseException as exc:
            self._publish(
                state="FAILED_CLOSED",
                stage=str(self.state.get("stage") or "STARTING"),
                next_action="REQUIRE_SPECIFIC_PORTFOLIO_RUNTIME_REPAIR",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise
        finally:
            if sink is not None:
                sink.close()

    def _recover_sealed_bundle_result(self) -> dict[str, Any] | None:
        """Recover a sealed portfolio bundle and finish forward anchoring.

        Atomic EvidenceBundle finalization necessarily precedes the external
        package anchor.  A crash in that narrow window must be resumable
        without leaving a PENDING receipt attached to a claimed forward
        status.
        """

        result = super()._recover_sealed_bundle_result()
        if result is None:
            return None
        return self._reconcile_forward_package_anchors(result)

    def _reconcile_forward_package_anchors(
        self, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Idempotently bind every frozen package to the sealed bundle."""

        self.forward_package_receipts = self._load_forward_package_receipts()
        _assert_authoritative_episode_counters(
            result["economic_results"], result["evidence_bundle"]
        )
        anchors = self._anchor_forward_packages(result["evidence_bundle"])
        economic = dict(result["economic_results"])
        if (
            economic.get("forward_shadow_anchor_receipts") != anchors
            or result.get("scientific_status")
            != "PORTFOLIO_FIRST_DEVELOPMENT_COMPLETE"
        ):
            economic["forward_shadow_anchor_receipts"] = anchors
            updated = dict(result)
            updated["economic_results"] = economic
            updated["scientific_status"] = "PORTFOLIO_FIRST_DEVELOPMENT_COMPLETE"
            updated.pop("result_hash", None)
            updated["result_hash"] = stable_hash(updated)
            name = str(self.manifest["runtime"]["result_name"])
            self.output_writer.write_json(name, updated)
            result = load_and_verify_production_result(
                self.output_dir / name, self.manifest
            )
            self._reconcile_completed_result_snapshots(result)
        return result

    def _load_forward_package_receipts(self) -> list[dict[str, Any]]:
        freeze_path = (
            self.output_dir / "forward_shadow/portfolio_forward_freeze.json"
        )
        if not freeze_path.is_file():
            raise PortfolioRuntimeError(
                "sealed portfolio bundle lacks the post-selection freeze"
            )
        freeze = _load_json(freeze_path)
        claimed_freeze = str(freeze.get("freeze_hash") or "")
        freeze_payload = dict(freeze)
        freeze_payload.pop("freeze_hash", None)
        if stable_hash(freeze_payload) != claimed_freeze:
            raise PortfolioRuntimeError("recovered forward freeze hash drift")
        receipts: list[dict[str, Any]] = []
        for selected in freeze.get("selected_books") or ():
            candidate_id = str(selected["pair_id"])
            directory = self.output_dir / "forward_shadow" / candidate_id
            receipt = _load_json(directory / "forward_shadow_receipt.json")
            claimed = str(receipt.get("receipt_hash") or "")
            checked = dict(receipt)
            checked.pop("receipt_hash", None)
            if (
                stable_hash(checked) != claimed
                or receipt.get("campaign_id") != self.campaign_id
                or receipt.get("candidate_id") != candidate_id
                or receipt.get("book_pair_structural_fingerprint")
                != selected["structural_fingerprint"]
                or receipt.get("development_finalist_role")
                != selected["development_finalist_role"]
            ):
                raise PortfolioRuntimeError("recovered forward receipt drift")
            package_path = directory / "shadow_package.json"
            if _sha256(package_path) != receipt["package_file_sha256"]:
                raise PortfolioRuntimeError("recovered forward package file drift")
            reconstructed = reconstruct_portfolio_shadow_package(
                _load_json(package_path)
            )
            if reconstructed.package.package_hash != receipt["package_hash"]:
                raise PortfolioRuntimeError("recovered forward package hash drift")
            receipts.append(receipt)
        return receipts

    def _kpis(self) -> dict[str, Any]:
        """Build portfolio KPIs without feeding portfolio rows to the legacy schema.

        The production-kernel KPI builder expects ``stressed_1_5x`` replay rows.
        Portfolio metrics intentionally use ``stressed`` and include lifecycle
        fields, so passing them through the legacy builder would either fail or
        silently halve the normal-only Stage-1 counter.  Reuse its allocation and
        throughput accounting, then replace all scenario/economic fields from the
        portfolio state and compact metrics.
        """

        saved = self.summaries
        self.summaries = {}
        try:
            value = dict(super()._kpis())
        finally:
            self.summaries = saved
        rows = list(getattr(self, "portfolio_metrics", {}).values())
        normal_rates = [float(row["normal"]["pass_rate"]) for row in rows]
        stressed_rates = [float(row["stressed"]["pass_rate"]) for row in rows]
        value.update(
            {
                "replay_computations_completed": int(
                    self.state.get("replay_computations_completed", 0)
                ),
                "normal_episodes_completed": int(
                    self.state.get("normal_episodes_completed", 0)
                ),
                "stressed_episodes_completed": int(
                    self.state.get("stressed_episodes_completed", 0)
                ),
                "positive_stressed_net_candidates": sum(
                    float(row["stressed"]["net_total"]) > 0.0 for row in rows
                ),
                "candidates_with_normal_pass": sum(rate > 0.0 for rate in normal_rates),
                "candidates_with_stressed_pass": sum(
                    rate > 0.0 for rate in stressed_rates
                ),
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
        value["rates_per_hour"] = {
            **dict(value.get("rates_per_hour") or {}),
            "replay_computations": int(
                self.state.get("replay_computations_completed", 0)
            )
            / self._campaign_elapsed_hours(),
        }
        value.pop("kpi_hash", None)
        value["kpi_hash"] = stable_hash(value)
        return value

    def _publish_hot_progress(
        self, hot_started_at: float, **updates: Any
    ) -> float:
        """Accrue hot wall time once, excluding checkpoint administration."""

        before_publish = time.perf_counter()
        self.clock.hot_seconds += max(before_publish - hot_started_at, 0.0)
        self._publish(**updates)
        return time.perf_counter()

    def _sleeve_records(self) -> tuple[SleeveRecord, ...]:
        rows = tuple(
            SleeveRecord.from_mapping(row["record"])
            for row in self.manifest["sleeve_bank"]["members"]
        )
        if len(rows) != len({row.behavioral_fingerprint for row in rows}):
            raise PortfolioRuntimeError("frozen sleeve bank contains a behavioral clone")
        return tuple(sorted(rows, key=lambda row: row.sleeve_id))

    def _load_or_generate_pairs(
        self, sleeves: Sequence[SleeveRecord]
    ) -> tuple[BookPair, ...]:
        path = self.payload_dir / "portfolio_book_pairs.jsonl"
        started = time.perf_counter()
        if path.is_file():
            rows = tuple(
                BookPair.from_mapping(json.loads(line))
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
            summary = _load_json(self.payload_dir / "portfolio_population_summary.json")
        else:
            spec = PortfolioBookGeneratorSpec.from_manifest(
                self.manifest["portfolio_books"]
            )
            result = generate_portfolio_book_pairs(sleeves, spec)
            rows = result.pairs
            summary = result.to_dict(include_pairs=False)
            self.payload_writer.write_jsonl_batch(
                "portfolio_book_pairs.jsonl", [row.to_dict() for row in rows]
            )
            self.payload_writer.write_json("portfolio_population_summary.json", summary)
        if len(rows) != 20_000:
            raise PortfolioRuntimeError("portfolio generator did not freeze 20,000 pairs")
        by_id = {row.sleeve_id: row for row in sleeves}
        for row in rows:
            row.verify_immutable_sources(by_id)
            if row.conflict_policy != "PRIORITY":
                raise PortfolioRuntimeError("unimplemented conflict semantics escaped manifest")
        self.population_summary = dict(summary)
        self.clock.hot_seconds += time.perf_counter() - started
        return rows

    def _reconcile_sleeves(
        self,
        sleeves: Sequence[SleeveRecord],
        runtimes: Mapping[str, ExactSleeveRuntime],
        *,
        components: Mapping[str, ComponentCandidate],
        matrices: Mapping[str, Any],
    ) -> None:
        declarations = {
            str(row["sleeve_id"]): row
            for row in self.manifest["sleeve_bank"]["members"]
        }
        source_campaign_id = str(
            self.manifest["sleeve_bank"].get("source_evidence_campaign_id") or ""
        )
        if source_campaign_id != "hydra_economic_production_0024":
            raise PortfolioRuntimeError(
                "portfolio source-evidence campaign identity is not frozen to 0024"
            )
        source_ledgers = materialize_component_evidence(
            source_campaign_id,
            {row.sleeve_id: runtimes[row.sleeve_id] for row in sleeves},
            {row.sleeve_id: components[row.sleeve_id] for row in sleeves},
            matrices,
        )
        failures: list[dict[str, Any]] = []
        for sleeve in sleeves:
            runtime = runtimes[sleeve.sleeve_id]
            row = declarations[sleeve.sleeve_id]
            signal_sha, signal_count = _canonical_component_ledger_hash(
                "component_signals",
                source_ledgers["component_signals"],
                sleeve.sleeve_id,
                source_campaign_id,
            )
            trade_sha, trade_count = _canonical_component_ledger_hash(
                "component_trades",
                source_ledgers["component_trades"],
                sleeve.sleeve_id,
                source_campaign_id,
            )
            checks = {
                "specification_hash": runtime.specification_hash
                == sleeve.immutable_fingerprint,
                "event_count": len(runtime.events) == int(row["event_count"]),
                "normal_net": math.isclose(
                    float(runtime.net_pnl), float(row["normal_net_pnl"]), abs_tol=1e-8
                ),
                "stressed_net": math.isclose(
                    float(runtime.cost_stress_1_5x_net),
                    float(row["stressed_net_pnl"]),
                    abs_tol=1e-8,
                ),
                "signal_ledger_sha256": signal_sha
                == sleeve.signal_ledger_sha256,
                "trade_ledger_sha256": trade_sha == sleeve.trade_ledger_sha256,
                "signal_row_count": signal_count == int(row["event_count"]),
                "trade_row_count": trade_count == int(row["event_count"]),
            }
            if not all(checks.values()):
                failures.append({"sleeve_id": sleeve.sleeve_id, "checks": checks})
        self.payload_writer.write_json(
            "sleeve_bank_reconciliation.json",
            {
                "schema": "hydra_portfolio_sleeve_reconciliation_v1",
                "sleeve_count": len(sleeves),
                "failures": failures,
                "source_evidence_campaign_id": source_campaign_id,
                "source_signal_and_trade_ledgers_recomputed": True,
                "source_recompile_role": "INTRATRADE_PATH_RECONCILIATION_ONLY",
                "source_signals_changed": False,
                "source_outcomes_used_to_mutate_sleeves": False,
            },
        )
        if failures:
            raise PortfolioRuntimeError("compiled sleeve bank failed immutable reconciliation")

    def _run_combine_stage(
        self,
        stage: str,
        pairs: Sequence[BookPair],
        timelines: Mapping[str, Sequence[RoutedTrade]],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        *,
        stressed: bool,
        prior_episode_count: int,
        prior_normal_episode_count: int,
        prior_stressed_episode_count: int,
    ) -> list[dict[str, Any]]:
        namespace = self.payload_dir / f"{stage}_batches"
        batch_size = 40 if stage == "stage1" else 5
        batches = [tuple(pairs[i : i + batch_size]) for i in range(0, len(pairs), batch_size)]
        output: list[dict[str, Any]] = []
        missing: list[tuple[int, tuple[BookPair, ...]]] = []
        for index, batch in enumerate(batches):
            path = namespace / f"batch_{index:06d}.json"
            if path.is_file():
                cached = _load_json(path)
                if stable_hash(cached["rows"]) != cached["rows_hash"]:
                    raise PortfolioRuntimeError(f"{stage} cache hash drift")
                if stage == "stage2" and any(
                    "evidence_raw" not in row for row in cached["rows"]
                ):
                    raise PortfolioRuntimeError(
                        "stage2 cache lacks required exact-replay evidence"
                    )
                output.extend(
                    _compact_combine_metric(row) for row in cached["rows"]
                )
            else:
                missing.append((index, batch))
        if missing:
            started = time.perf_counter()
            hot_started_at = started
            worker_args = (
                timelines,
                starts,
                calendars,
                self.campaign_id,
                self.manifest["temporal_blocks"]["blocks"],
                getattr(self, "xfa_calendars", calendars),
            )
            context = multiprocessing.get_context("spawn")
            try:
                with ProcessPoolExecutor(
                    max_workers=3,
                    mp_context=context,
                    initializer=_set_worker_state,
                    initargs=worker_args,
                ) as pool:
                    futures = {
                        pool.submit(
                            _combine_batch_worker,
                            [row.to_dict() for row in batch],
                            stressed,
                            stage == "stage2",
                        ): (index, batch)
                        for index, batch in missing
                    }
                    completed = len(output)
                    episode_count = _episode_total(output)
                    for future in as_completed(futures):
                        index, _batch = futures[future]
                        rows = future.result()
                        payload = {"stage": stage, "rows": rows, "rows_hash": stable_hash(rows)}
                        self.payload_writer.write_json(
                            f"{stage}_batches/batch_{index:06d}.json", payload
                        )
                        output.extend(_compact_combine_metric(row) for row in rows)
                        completed += len(rows)
                        episode_count += _episode_total(rows)
                        if index % 5 == 0 or completed == len(pairs):
                            official = stage != "stage1"
                            hot_started_at = self._publish_hot_progress(
                                hot_started_at,
                                # V17 intentionally exposes a small, frozen set
                                # of resumable production states.  Stage 1 is
                                # still part of the already-frozen population;
                                # its finer-grained phase belongs in ``stage``.
                                state="POPULATION_FROZEN" if stage == "stage1" else "EXACT_REPLAY_ACTIVE",
                                stage=f"PORTFOLIO_{stage.upper()}",
                                unique_policies_screened=completed if stage == "stage1" else int(self.state.get("unique_policies_screened", 0)),
                                exact_account_replays=completed if stage == "stage2" else int(self.state.get("exact_account_replays", 0)),
                                fast_screen_episode_observations=(
                                    episode_count
                                    if not official
                                    else int(
                                        self.state.get(
                                            "fast_screen_episode_observations", 0
                                        )
                                    )
                                ),
                                combine_episodes_completed=(
                                    prior_episode_count
                                ),
                                replay_computations_completed=(
                                    prior_episode_count + episode_count
                                    if official
                                    else int(
                                        self.state.get(
                                            "replay_computations_completed", 0
                                        )
                                    )
                                ),
                                normal_episodes_completed=(
                                    prior_normal_episode_count
                                ),
                                stressed_episodes_completed=(
                                    prior_stressed_episode_count
                                ),
                                next_action="PROCESS_NEXT_PORTFOLIO_BATCH",
                            )
            finally:
                _clear_worker_state()
            self.clock.hot_seconds += max(
                time.perf_counter() - hot_started_at, 0.0
            )
        by_id = {row["pair_id"]: row for row in output}
        if set(by_id) != {row.pair_id for row in pairs}:
            raise PortfolioRuntimeError(f"{stage} result coverage drift")
        return [by_id[row.pair_id] for row in pairs]

    def _run_lifecycle_stage(
        self,
        stage: str,
        pairs: Sequence[BookPair],
        timelines: Mapping[str, Sequence[RoutedTrade]],
        starts: Sequence[int],
        calendars: Mapping[int, Sequence[int]],
        *,
        sink: AsyncEvidenceBundleSink,
        prior_episode_count: int,
        prior_lifecycle_metrics: Sequence[Mapping[str, Any]],
        prior_normal_episode_count: int,
        prior_stressed_episode_count: int,
    ) -> list[dict[str, Any]]:
        if not pairs or not starts:
            return []
        prior_replay_computations = int(
            self.state.get("replay_computations_completed", 0)
        )
        output: list[dict[str, Any]] = []
        missing: list[BookPair] = []
        for pair in pairs:
            path = self.payload_dir / f"{stage}_lifecycle" / f"{pair.pair_id}.json"
            if path.is_file():
                cached = _load_json(path)
                if stable_hash(cached["payload_without_hash"]) != cached["cache_hash"]:
                    raise PortfolioRuntimeError(f"{stage} lifecycle cache hash drift")
                output.append(_compact_lifecycle_metric(cached["payload_without_hash"]))
            else:
                missing.append(pair)
        if missing:
            started = time.perf_counter()
            hot_started_at = started
            worker_args = (
                timelines,
                starts,
                calendars,
                self.campaign_id,
                self.manifest["temporal_blocks"]["blocks"],
                getattr(self, "xfa_calendars", calendars),
            )
            context = multiprocessing.get_context("spawn")
            try:
                with ProcessPoolExecutor(
                    max_workers=3,
                    mp_context=context,
                    initializer=_set_worker_state,
                    initargs=worker_args,
                ) as pool:
                    futures = {
                        pool.submit(_lifecycle_pair_worker, pair.to_dict()): pair
                        for pair in missing
                    }
                    for future in as_completed(futures):
                        pair = futures[future]
                        payload = future.result()
                        self.payload_writer.write_json(
                            f"{stage}_lifecycle/{pair.pair_id}.json",
                            {
                                "payload_without_hash": payload,
                                "cache_hash": stable_hash(payload),
                            },
                        )
                        output.append(_compact_lifecycle_metric(payload))
                        episodes, paths = _evidence_rows(payload, self.manifest)
                        sink.append_records(
                            "episodes",
                            episodes,
                            batch_id=f"{stage}:{pair.pair_id}:episodes",
                        )
                        sink.append_records(
                            "account_daily_paths",
                            paths,
                            batch_id=f"{stage}:{pair.pair_id}:daily",
                        )
                        sink.checkpoint(
                            {
                                "stage": stage,
                                "last_pair_id": pair.pair_id,
                                "combine_episode_count": int(self.state.get("combine_episodes_completed", 0)) + int(payload["combine_episode_count"]),
                                "xfa_paths_started": int(self.state.get("xfa_paths_started", 0)) + int(payload["xfa_paths_started"]),
                            }
                        )
                        sink.flush()
                        completed = len(output)
                        aggregate = _sum_lifecycle(output)
                        prior_lifecycle = _sum_lifecycle(prior_lifecycle_metrics)
                        self.portfolio_metrics = {
                            str(row["pair_id"]): row for row in output
                        }
                        hot_started_at = self._publish_hot_progress(
                            hot_started_at,
                            state="ROBUSTNESS_ACTIVE" if stage == "stage3" else "EXPANDED_EPISODES_ACTIVE",
                            stage=f"PORTFOLIO_{stage.upper()}_LIFECYCLE",
                            combine_episodes_completed=prior_episode_count + int(aggregate["combine_episode_count"]),
                            replay_computations_completed=(
                                prior_replay_computations
                                + int(aggregate["combine_episode_count"])
                            ),
                            normal_episodes_completed=(
                                prior_normal_episode_count
                                + _scenario_episode_total(output, "normal")
                            ),
                            stressed_episodes_completed=(
                                prior_stressed_episode_count
                                + _scenario_episode_total(output, "stressed")
                            ),
                            combine_passes=int(prior_lifecycle["normal_combine_passes"] + prior_lifecycle["stressed_combine_passes"] + aggregate["normal_combine_passes"] + aggregate["stressed_combine_passes"]),
                            xfa_paths_started=int(prior_lifecycle["xfa_paths_started"] + aggregate["xfa_paths_started"]),
                            xfa_standard_paths=int(prior_lifecycle["xfa_standard_paths"] + aggregate["xfa_standard_paths"]),
                            xfa_consistency_paths=int(prior_lifecycle["xfa_consistency_paths"] + aggregate["xfa_consistency_paths"]),
                            first_payouts=int(prior_lifecycle["first_payouts"] + aggregate["first_payouts"]),
                            payout_cycles=int(prior_lifecycle["payout_cycles"] + aggregate["payout_cycles"]),
                            last_completed_book_pair=pair.pair_id,
                            lifecycle_book_pairs_completed=completed,
                            next_action="PROCESS_NEXT_FROZEN_PORTFOLIO_BOOK",
                        )
            finally:
                _clear_worker_state()
            self.clock.hot_seconds += max(
                time.perf_counter() - hot_started_at, 0.0
            )
        by_id = {row["pair_id"]: row for row in output}
        ordered = [by_id[row.pair_id] for row in pairs]
        # Deterministic batch IDs make this idempotent and close the narrow
        # cache-write/evidence-write crash window on resume.
        for pair in pairs:
            cached = _load_json(
                self.payload_dir / f"{stage}_lifecycle" / f"{pair.pair_id}.json"
            )["payload_without_hash"]
            payload = cached
            episodes, paths = _evidence_rows(payload, self.manifest)
            sink.append_records(
                "episodes",
                episodes,
                batch_id=f"{stage}:{payload['pair_id']}:episodes",
            )
            sink.append_records(
                "account_daily_paths",
                paths,
                batch_id=f"{stage}:{payload['pair_id']}:daily",
            )
        sink.flush()
        aggregate = _sum_lifecycle(ordered)
        prior_lifecycle = _sum_lifecycle(prior_lifecycle_metrics)
        self.portfolio_metrics = {str(row["pair_id"]): row for row in ordered}
        self._publish(
            state=(
                "ROBUSTNESS_ACTIVE"
                if stage == "stage3"
                else "EXPANDED_EPISODES_ACTIVE"
            ),
            stage=f"PORTFOLIO_{stage.upper()}_LIFECYCLE_COMPLETE",
            combine_episodes_completed=prior_episode_count
            + int(aggregate["combine_episode_count"]),
            replay_computations_completed=(
                prior_replay_computations
                + int(aggregate["combine_episode_count"])
            ),
            normal_episodes_completed=prior_normal_episode_count
            + _scenario_episode_total(ordered, "normal"),
            stressed_episodes_completed=prior_stressed_episode_count
            + _scenario_episode_total(ordered, "stressed"),
            combine_passes=int(
                prior_lifecycle["normal_combine_passes"]
                + prior_lifecycle["stressed_combine_passes"]
                + aggregate["normal_combine_passes"]
                + aggregate["stressed_combine_passes"]
            ),
            xfa_paths_started=int(
                prior_lifecycle["xfa_paths_started"]
                + aggregate["xfa_paths_started"]
            ),
            xfa_standard_paths=int(
                prior_lifecycle["xfa_standard_paths"]
                + aggregate["xfa_standard_paths"]
            ),
            xfa_consistency_paths=int(
                prior_lifecycle["xfa_consistency_paths"]
                + aggregate["xfa_consistency_paths"]
            ),
            first_payouts=int(
                prior_lifecycle["first_payouts"] + aggregate["first_payouts"]
            ),
            payout_cycles=int(
                prior_lifecycle["payout_cycles"] + aggregate["payout_cycles"]
            ),
            lifecycle_book_pairs_completed=len(ordered),
            next_action="APPLY_FROZEN_SUCCESSIVE_HALVING_GATE",
        )
        return ordered

    def _open_portfolio_evidence(
        self,
        pairs: Sequence[BookPair],
        sleeves: Sequence[SleeveRecord],
        starts: Sequence[int],
    ) -> AsyncEvidenceBundleSink:
        policy_ids = [row.pair_id for row in pairs]
        component_ids = [row.sleeve_id for row in sleeves]
        identity = {
            "campaign_id": self.campaign_id,
            "grammar_id": str(self.manifest["class_id"]),
            "policy_fingerprints": {
                row.pair_id: row.structural_fingerprint for row in pairs
            },
            "component_fingerprints": {
                row.sleeve_id: row.immutable_fingerprint for row in sleeves
            },
            "source_commit": str(self.manifest["source_commit"]),
            "data_fingerprints": {
                "canonical_feature_source": str(self.manifest["data"]["feature_source_fingerprint"]),
                "contract_map": str(self.manifest["data"]["contract_map_sha256"]),
                "source_runtime_summary": str(self.manifest["sleeve_bank"]["source_runtime_summary"]["file_sha256"]),
                "data_access_ledger": _sha256(self.root / "reports/data_access/data_access_ledger.jsonl"),
                **self.feature_cache_fingerprints,
            },
            "configuration_sha256": _sha256(self.manifest_path),
            "seeds": [int(self.manifest["portfolio_books"]["seed"])],
            "created_at_utc": str(self.manifest["created_at_utc"]),
            "expected_coverage": {
                "policy_ids": policy_ids,
                "component_ids": component_ids,
                "required_episode_keys": [
                    {
                        "policy_id": policy_id,
                        "episode_id": f"{policy_id}:{start}",
                        "horizon": "90_TRADING_DAYS",
                    }
                    for policy_id in policy_ids
                    for start in starts
                ],
                "allowed_horizons": ["90_TRADING_DAYS"],
                "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
                "allow_additional_episode_keys": True,
            },
        }
        self.evidence_identity = identity
        base = self.root / str(self.manifest["evidence_bundle"]["destination"])
        return AsyncEvidenceBundleSink(
            base_dir=base,
            identity=identity,
            writer_id=f"portfolio-kernel:{self.campaign_id}",
            resume=(base / f".{self.campaign_id}.evidence-v1.staging").is_dir(),
        )

    def _persist_static_evidence(
        self,
        sink: AsyncEvidenceBundleSink,
        pairs: Sequence[BookPair],
        sleeves: Sequence[SleeveRecord],
        components: Mapping[str, ComponentCandidate],
        runtimes: Mapping[str, ExactSleeveRuntime],
        matrices: Mapping[str, Any],
    ) -> None:
        marker = self.payload_dir / "portfolio_static_evidence.json"
        # The report marker is advisory.  Deterministic EvidenceBundle batch
        # IDs are the authoritative idempotency boundary, so a stale marker
        # can never suppress evidence after a staging-directory recovery.
        used = {row.sleeve_id for row in sleeves}
        ledgers = materialize_component_evidence(
            self.campaign_id,
            {key: runtimes[key] for key in used},
            {key: components[key] for key in used},
            matrices,
        )
        for dataset, rows in ledgers.items():
            for index in range(0, len(rows), 5_000):
                sink.append_records(
                    dataset,
                    rows[index : index + 5_000],
                    batch_id=f"portfolio-static:{dataset}:{index // 5000:06d}",
                )
        membership = _portfolio_membership_rows(self.campaign_id, pairs, sleeves)
        for index in range(0, len(membership), 5_000):
            sink.append_records(
                "account_policy_membership",
                membership[index : index + 5_000],
                batch_id=f"portfolio-membership:{index // 5000:06d}",
            )
        access_sha = self.evidence_identity["data_fingerprints"]["data_access_ledger"]
        provenance_checksums = _portfolio_provenance_checksums(
            self.evidence_identity,
            ledgers,
            sleeves,
            campaign_id=self.campaign_id,
        )
        sink.append_records(
            "provenance",
            [
                {
                    "campaign_id": self.campaign_id,
                    "validator_version": "hydra_evidence_bundle_validator_v1",
                    "replay_version": PORTFOLIO_RUNTIME_VERSION,
                    "market_data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
                    "access_ledger_sha256": access_sha,
                    "reconstruction_flag": False,
                    "immutable_checksums": provenance_checksums,
                    "recorded_at_utc": str(self.manifest["created_at_utc"]),
                }
            ],
            batch_id="portfolio-provenance:000000",
        )
        sink.checkpoint({"stage": "PORTFOLIO_STATIC_EVIDENCE_COMPLETE"})
        sink.flush()
        self.payload_writer.write_json(
            "portfolio_static_evidence.json",
            {"status": "COMPLETE", "dataset_counts": {key: len(value) for key, value in ledgers.items()}},
        )

    def _persist_stage2_evidence(
        self,
        sink: AsyncEvidenceBundleSink,
        *,
        exact_pair_ids: set[str],
        excluded_pair_ids: set[str],
    ) -> None:
        """Stream exact Stage-2 evidence for books not replayed in Stage 3.

        Full raw paths live in deterministic batch caches while coordinator
        ranking metrics remain compact.  Stage-3 survivors are excluded from
        this base append because their 48-start lifecycle payload includes the
        same four block-aware starts; persisting both would duplicate episode
        keys in the immutable bundle.
        """

        namespace = self.payload_dir / "stage2_batches"
        paths = sorted(namespace.glob("batch_*.json"))
        if not paths:
            raise PortfolioRuntimeError("missing Stage-2 evidence batches")
        persisted_ids: set[str] = set()
        observed_ids: set[str] = set()
        for path in paths:
            cached = _load_json(path)
            rows = cached.get("rows")
            if not isinstance(rows, list) or stable_hash(rows) != cached.get("rows_hash"):
                raise PortfolioRuntimeError("stage2 cache hash drift during evidence append")
            episodes: list[dict[str, Any]] = []
            daily_paths: list[dict[str, Any]] = []
            for row in rows:
                pair_id = str(row.get("pair_id") or "")
                if not pair_id or pair_id in observed_ids:
                    raise PortfolioRuntimeError(
                        "duplicate or empty Stage-2 policy evidence"
                    )
                observed_ids.add(pair_id)
                if pair_id in excluded_pair_ids:
                    continue
                if "evidence_raw" not in row:
                    raise PortfolioRuntimeError(
                        "stage2 cache lacks required exact-replay evidence"
                    )
                row_episodes, row_paths = _combine_evidence_rows(row, self.manifest)
                episodes.extend(row_episodes)
                daily_paths.extend(row_paths)
                persisted_ids.add(pair_id)
            suffix = path.stem.removeprefix("batch_")
            if episodes:
                sink.append_records(
                    "episodes",
                    episodes,
                    batch_id=f"stage2-base:{suffix}:episodes",
                )
                sink.append_records(
                    "account_daily_paths",
                    daily_paths,
                    batch_id=f"stage2-base:{suffix}:daily",
                )
        if observed_ids != exact_pair_ids:
            raise PortfolioRuntimeError("Stage-2 exact policy universe drift")
        if not excluded_pair_ids.issubset(exact_pair_ids):
            raise PortfolioRuntimeError("Stage-3 deferral references unknown exact policy")
        expected = exact_pair_ids - excluded_pair_ids
        if persisted_ids != expected:
            raise PortfolioRuntimeError("Stage-2 EvidenceBundle coverage drift")
        sink.checkpoint(
            {
                "stage": "PORTFOLIO_STAGE_2_EVIDENCE_COMPLETE",
                "base_policy_count": len(persisted_ids),
                "stage3_deferred_policy_count": len(excluded_pair_ids),
            }
        )
        sink.flush()

    def _write_halving(
        self,
        stage: str,
        inputs: Sequence[BookPair],
        outputs: Sequence[BookPair],
        metrics: Sequence[Mapping[str, Any]],
    ) -> None:
        eligible_ids = {row.pair_id for row in outputs}
        diagnostic_fallback_ids: list[str] = []
        economic_eligible_count = len(outputs)
        if stage in {"stage1", "stage2"}:
            require_stress = stage == "stage2"
            economic_ids = {
                str(row["pair_id"])
                for row in metrics
                if _pair_metric_is_economically_eligible(
                    row, require_stress=require_stress
                )
            }
            economic_eligible_count = len(economic_ids)
            diagnostic_fallback_ids = sorted(eligible_ids - economic_ids)
        decision = {
            "schema": "hydra_portfolio_halving_v1",
            "campaign_id": self.campaign_id,
            "stage": _halving_stage_name(stage),
            "portfolio_stage": stage,
            "input_count": len(inputs),
            "eligible_count": economic_eligible_count,
            "output_count": len(outputs),
            "selected_policy_ids": [row.pair_id for row in outputs],
            "selected_pair_ids": [row.pair_id for row in outputs],
            "selection": "TRANSPARENT_PARETO_LEXICOGRAPHIC_V1",
            "opaque_score_used": False,
            "thresholds_changed_after_outcomes": False,
            "diagnostic_fallback_selected_ids": diagnostic_fallback_ids,
            "scientific_null_causes_failed_closed": False,
            "actual_account_behavior_unique_count": len(
                {
                    _metric_account_behavior_fingerprint(row)
                    for row in metrics
                }
            ),
            "actual_account_behavior_duplicate_rejections": max(
                0,
                len(metrics)
                - len(
                    {
                        _metric_account_behavior_fingerprint(row)
                        for row in metrics
                    }
                ),
            ),
            "behavioral_deduplication_basis": (
                "OBSERVED_COMBINE_AND_XFA_LIFECYCLE_TRAJECTORY_V1"
                if stage in {"stage3", "stage4", "stage5"}
                else "OBSERVED_COMBINE_TRAJECTORY_NORMAL_AND_STRESSED_V1"
            ),
            "metrics_hash": stable_hash(list(metrics)),
        }
        decision["decision_hash"] = stable_hash(decision)
        self.payload_writer.write_json(f"{stage}_halving.json", decision)

    def _halving_decisions(self) -> list[dict[str, Any]]:
        decisions: list[dict[str, Any]] = []
        for stage in ("stage1", "stage2", "stage3", "stage4", "stage5"):
            path = self.payload_dir / f"{stage}_halving.json"
            if not path.is_file():
                raise PortfolioRuntimeError(f"missing durable halving decision: {stage}")
            decision = _load_json(path)
            claimed = str(decision.get("decision_hash") or "")
            payload = dict(decision)
            payload.pop("decision_hash", None)
            if stable_hash(payload) != claimed:
                raise PortfolioRuntimeError(f"halving decision hash drift: {stage}")
            if decision.get("stage") != _halving_stage_name(stage):
                raise PortfolioRuntimeError(f"halving stage identity drift: {stage}")
            decisions.append(decision)
        return decisions

    def _finalize_portfolio(
        self,
        sink: AsyncEvidenceBundleSink,
        *,
        population: Sequence[BookPair],
        stage1: Sequence[Mapping[str, Any]],
        stage2: Sequence[Mapping[str, Any]],
        stage3: Sequence[Mapping[str, Any]],
        stage4: Sequence[Mapping[str, Any]],
        stage5: Sequence[Mapping[str, Any]],
        finalists: Sequence[BookPair],
        sleeve_specs: Mapping[str, Any],
    ) -> dict[str, Any]:
        final_metrics = list(stage5 or stage4 or stage3)
        stage_decisions = self._halving_decisions()
        forward_ids = self._export_forward_packages(
            finalists,
            sleeve_specs=sleeve_specs,
        )
        statuses = _status_matrix(final_metrics, forward_ids=forward_ids)
        finalist_roles = _development_finalist_roles(finalists)
        graduated = [row["pair_id"] for row in statuses if "COMBINE_BOOK_GRADUATED" in row["statuses"]]
        payout = [row["pair_id"] for row in statuses if "PAYOUT_PATH_CANDIDATE" in row["statuses"]]
        rates_n = [float(row["normal"]["pass_rate"]) for row in final_metrics]
        rates_s = [float(row["stressed"]["pass_rate"]) for row in final_metrics]
        progress = [float(row["stressed"]["target_progress_median"]) for row in final_metrics]
        mll = [float(row["stressed"]["mll_breach_rate"]) for row in final_metrics]
        positive = sum(float(row["stressed"]["net_total"]) > 0.0 for row in final_metrics)
        stressed_standard_paths = [
            row["path_metrics"]["STRESSED_1_5X"]["STANDARD"]
            for row in final_metrics
        ]
        payout_day_medians = [
            float(row["median_trading_days_to_first_payout"])
            for row in stressed_standard_paths
            if row.get("median_trading_days_to_first_payout") is not None
        ]
        counters = {
            "serious_exact_account_replays": len(stage2),
            "predeclared_control_policy_replays": 0,
            "replay_computations_completed": int(
                self.state.get("replay_computations_completed", 0)
            ),
            "combine_episodes_completed": int(self.state.get("combine_episodes_completed", 0)),
            "normal_episodes_completed": int(
                self.state.get("normal_episodes_completed", 0)
            ),
            "stressed_episodes_completed": int(
                self.state.get("stressed_episodes_completed", 0)
            ),
        }
        kpis = self._kpis()
        frontier = {
            "candidate_count": len(final_metrics),
            "normal_pass_fraction_best": max(rates_n, default=0.0),
            "normal_pass_fraction_median": statistics.median(rates_n) if rates_n else 0.0,
            "stressed_pass_fraction_best": max(rates_s, default=0.0),
            "stressed_pass_fraction_median": statistics.median(rates_s) if rates_s else 0.0,
            "stressed_target_progress_median_best": max(progress, default=0.0),
            "stressed_target_progress_median_population": statistics.median(progress) if progress else 0.0,
            "stressed_mll_breach_rate_minimum": min(mll, default=0.0),
            "stressed_mll_breach_rate_maximum": max(mll, default=0.0),
            "positive_stressed_net_count": positive,
            "xfa_paths_started": sum(int(row["xfa_paths_started"]) for row in final_metrics),
            "first_payouts": sum(int(row["first_payouts"]) for row in final_metrics),
            "payout_cycles": sum(int(row["payout_cycles"]) for row in final_metrics),
            "expected_trader_net_payout_per_attempt_best": max((float(row["expected_trader_net_payout_per_attempt"]) for row in final_metrics), default=0.0),
            "stressed_xfa_entry_probability_best": max(
                (
                    float(row["xfa_entry_probability"])
                    for row in stressed_standard_paths
                ),
                default=0.0,
            ),
            "stressed_first_payout_probability_conditional_best": max(
                (
                    float(
                        row[
                            "first_payout_probability_conditional_on_combine_pass"
                        ]
                    )
                    for row in stressed_standard_paths
                ),
                default=0.0,
            ),
            "stressed_first_payout_probability_unconditional_best": max(
                (
                    float(row["first_payout_probability_unconditional"])
                    for row in stressed_standard_paths
                ),
                default=0.0,
            ),
            "stressed_expected_payout_cycles_per_attempt_best": max(
                (
                    float(row["expected_payout_cycles_per_combine_attempt"])
                    for row in stressed_standard_paths
                ),
                default=0.0,
            ),
            "stressed_median_trading_days_to_first_payout_minimum": (
                min(payout_day_medians) if payout_day_medians else None
            ),
        }
        summary = {
            "schema": "hydra_portfolio_campaign_summary_v1",
            "campaign_id": self.campaign_id,
            "sleeve_bank_size": len(self.manifest["sleeve_bank"]["members"]),
            "book_pairs_generated": len(population),
            "candidate_count": len(final_metrics),
            "positive_stressed_net_count": positive,
            "normal_pass_candidate_count": sum(value > 0.0 for value in rates_n),
            "stressed_pass_candidate_count": sum(value > 0.0 for value in rates_s),
            "confirmation_ready_candidate_ids": [],
            "combine_book_graduated_ids": graduated,
            "payout_path_candidate_ids": payout,
            "stage5_96_start_candidate_ids": list(
                stage_decisions[2]["selected_policy_ids"]
            ),
            "development_finalist_ids": [row.pair_id for row in finalists],
            "development_finalist_candidate_ids": [row.pair_id for row in finalists],
            "development_finalist_roles": finalist_roles,
            "development_primary_ids": [
                row["pair_id"]
                for row in finalist_roles
                if row["role"] == "PRIMARY_DEVELOPMENT_BOOK"
            ],
            "development_backup_ids": [
                row["pair_id"]
                for row in finalist_roles
                if row["role"] == "BEHAVIORALLY_DISTINCT_BACKUP"
            ],
            "distinct_backup_requirement_satisfied": any(
                row["role"] == "BEHAVIORALLY_DISTINCT_BACKUP"
                for row in finalist_roles
            ),
            "production_counters": counters,
            "production_kpis": {
                "rates_per_hour": dict(kpis["rates_per_hour"]),
                "economic_research_wall_clock_fraction": kpis["economic_research_wall_clock_fraction"],
                "cpu_utilization_fraction": kpis["cpu_utilization_fraction"],
                "workers": dict(kpis["workers"]),
                "duplicate_rejection_rate": kpis["duplicate_rejection_rate"],
                "cache_hit_rate": kpis["cache_hit_rate"],
            },
            "economic_frontier": frontier,
            "standard_payout_paths": sum(
                int(row["path_metrics"][scenario]["STANDARD"]["path_count"])
                for row in final_metrics
                for scenario in ("NORMAL", "STRESSED_1_5X")
            ),
            "consistency_payout_paths": sum(
                int(row["path_metrics"][scenario]["CONSISTENCY"]["path_count"])
                for row in final_metrics
                for scenario in ("NORMAL", "STRESSED_1_5X")
            ),
            "standard_first_payouts": sum(
                int(row["path_metrics"][scenario]["STANDARD"]["first_payouts"])
                for row in final_metrics
                for scenario in ("NORMAL", "STRESSED_1_5X")
            ),
            "consistency_first_payouts": sum(
                int(row["path_metrics"][scenario]["CONSISTENCY"]["first_payouts"])
                for row in final_metrics
                for scenario in ("NORMAL", "STRESSED_1_5X")
            ),
            "lifecycle_matrix": statuses,
            "development_only": True,
            "independently_confirmed": False,
            "paper_shadow_ready_ids": [],
            "matched_controls_status": "NOT_APPLICABLE_PORTFOLIO_MASS_PRODUCTION",
            "broker_connections": 0,
            "orders": 0,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "unrealized_path_claim": "CONSERVATIVE_BOUND_NOT_TIMESTAMP_EXACT",
        }
        recommendation = {
            "action": "CONTINUE_PORTFOLIO_GRADUATES_TO_FORWARD_OBSERVATION" if graduated else "REFILL_SLEEVE_BANK_AND_CONTINUE_PORTFOLIO_FACTORY",
            "candidate_ids": graduated or [row.pair_id for row in finalists],
            # V17 may execute the full frozen campaign unattended, but a later
            # economic campaign still requires its own immutable manifest.
            # Never advertise an unimplemented implicit mutation/retry path.
            "manifest_required": True,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
        compact = {
            "campaign_summary": summary,
            "failure_vectors": {
                "schema": "hydra_production_failure_vectors_v1",
                "campaign_id": self.campaign_id,
                "by_policy": {row["pair_id"]: row.get("failure_vectors", []) for row in final_metrics},
            },
            "pareto_archive": {
                "schema": "hydra_portfolio_pareto_archive_v1",
                "campaign_id": self.campaign_id,
                "lifecycle_frontier": final_metrics,
                "stage_decisions": stage_decisions,
                "opaque_score_used": False,
            },
            "next_campaign_recommendations": {
                "schema": "hydra_production_next_campaign_recommendations_v1",
                "campaign_id": self.campaign_id,
                "recommendation": recommendation,
            },
        }
        for name, payload in compact.items():
            sink.write_compact_output(name, payload)
        sink.checkpoint({"stage": "PORTFOLIO_FINALIZING"})
        sink.flush()
        self._publish(
            state="FINALIZING",
            stage="EVIDENCE_BUNDLE_ATOMIC_FINALIZE",
            next_action="SEAL_PORTFOLIO_EVIDENCE",
            candidates_promoted_96=int(stage_decisions[2]["output_count"]),
            candidates_surviving_96=int(stage_decisions[3]["output_count"]),
            confirmation_ready_candidates=0,
        )
        receipt = sink.finalize(
            lightweight_manifest_path=self.root / str(self.manifest["evidence_bundle"]["lightweight_manifest_path"])
        )
        sink.guard_completion(receipt.bundle_path)
        _assert_authoritative_episode_counters(summary, receipt.to_dict())
        forward_anchor_receipts = self._anchor_forward_packages(receipt.to_dict())
        summary["forward_shadow_anchor_receipts"] = forward_anchor_receipts
        terminal = {
            "state": "COMPLETE",
            "stage": "PORTFOLIO_CAMPAIGN_COMPLETE",
            "next_action": recommendation["action"],
            "evidence_bundle_path": receipt.bundle_path,
            "evidence_bundle_manifest_sha256": receipt.manifest_sha256,
        }
        self.state.update(terminal)
        result = build_final_result_payload(
            manifest=self.manifest,
            kpis=self._kpis(),
            economic_results=summary,
            successive_halving={"stage_decisions": stage_decisions},
            matched_controls={"status": "NOT_APPLICABLE_PORTFOLIO_MASS_PRODUCTION"},
            failure_vectors=compact["failure_vectors"],
            evidence_receipt=receipt.to_dict(),
            autonomous_next_action=recommendation,
            scientific_status="PORTFOLIO_FIRST_DEVELOPMENT_COMPLETE",
        )
        if result["schema"] != PRODUCTION_RESULT_SCHEMA:
            raise PortfolioRuntimeError("portfolio result schema drift")
        name = str(self.manifest["runtime"]["result_name"])
        self.output_writer.write_json(name, result)
        checked = load_and_verify_production_result(self.output_dir / name, self.manifest)
        self._publish(**terminal)
        return checked

    def _export_forward_packages(
        self,
        finalists: Sequence[BookPair],
        *,
        sleeve_specs: Mapping[str, Any],
    ) -> set[str]:
        """Freeze reconstructible no-order packages after final selection.

        The package points at the staging identity file because the final
        EvidenceBundle does not exist until the packages and compact campaign
        outputs have been persisted.  ``identity.json`` is immutable across
        the atomic staging-to-final rename; a post-finalize anchor receipt then
        binds each package to the sealed bundle manifest/content hashes.
        """

        freeze = self._forward_selection_freeze(finalists)
        staging_identity = (
            self.root
            / str(self.manifest["evidence_bundle"]["destination"])
            / f".{self.campaign_id}.evidence-v1.staging"
            / "identity.json"
        )
        if not staging_identity.is_file():
            raise PortfolioRuntimeError(
                "forward package requires the authoritative staging identity"
            )
        identity_sha256 = _sha256(staging_identity)
        configuration_sha256 = str(self.evidence_identity["configuration_sha256"])
        output: set[str] = set()
        receipts: list[dict[str, Any]] = []
        roles = {
            row["pair_id"]: row["role"]
            for row in _development_finalist_roles(finalists)
        }
        for pair in finalists:
            required_ids = set(pair.combine_sleeve_ids) | set(pair.xfa_sleeve_ids)
            package = build_portfolio_shadow_package(
                pair,
                {
                    sleeve_id: sleeve_specs[sleeve_id]
                    for sleeve_id in sorted(required_ids)
                },
                source_commit=str(self.manifest["source_commit"]),
                selection_completed_at_utc=str(
                    freeze["selection_completed_at_utc"]
                ),
                freeze_timestamp_utc=str(freeze["freeze_timestamp_utc"]),
                evidence_bundle_identity_sha256=identity_sha256,
                evidence_bundle_configuration_sha256=configuration_sha256,
            )
            directory = self.output_dir / "forward_shadow" / pair.pair_id
            machine, dossier = write_shadow_package(package, directory)
            loaded = _load_json(machine)
            reconstructed = reconstruct_portfolio_shadow_package(loaded)
            if (
                loaded.get("schema") != SHADOW_PACKAGE_SCHEMA
                or loaded.get("package_hash") != package.package_hash
                or reconstructed.book_pair != pair
                or _sha256(machine) == ""
            ):
                raise PortfolioRuntimeError("forward package verification drift")
            receipt = {
                "schema": "hydra_portfolio_forward_shadow_receipt_v1",
                "campaign_id": self.campaign_id,
                "candidate_id": pair.pair_id,
                "package_path": str(machine),
                "package_file_sha256": _sha256(machine),
                "package_hash": package.package_hash,
                "package_schema": SHADOW_PACKAGE_SCHEMA,
                "dossier_path": str(dossier),
                "dossier_sha256": _sha256(dossier),
                "book_pair_structural_fingerprint": pair.structural_fingerprint,
                "development_finalist_role": roles[pair.pair_id],
                "source_commit": str(self.manifest["source_commit"]),
                "selection_completed_at_utc": str(
                    freeze["selection_completed_at_utc"]
                ),
                "freeze_timestamp_utc": str(freeze["freeze_timestamp_utc"]),
                "evidence_bundle_identity_sha256": identity_sha256,
                "evidence_bundle_configuration_sha256": configuration_sha256,
                "evidence_bundle_anchor_status": "PENDING_ATOMIC_FINALIZE",
                "broker_connectivity": False,
                "outbound_order_capability": False,
                "q4_access_authorized": False,
                "new_data_purchase_authorized": False,
                "paper_shadow_ready": False,
                "development_only": True,
            }
            receipt["receipt_hash"] = stable_hash(receipt)
            self.output_writer.write_json(
                f"forward_shadow/{pair.pair_id}/forward_shadow_receipt.json",
                receipt,
            )
            checked = _load_json(
                directory / "forward_shadow_receipt.json"
            )
            claimed = str(checked.pop("receipt_hash", ""))
            if stable_hash(checked) != claimed:
                raise PortfolioRuntimeError("forward package receipt hash drift")
            receipts.append(receipt)
            output.add(pair.pair_id)
        self.forward_package_receipts = receipts
        return output

    def _forward_selection_freeze(
        self, finalists: Sequence[BookPair]
    ) -> dict[str, Any]:
        relative = "forward_shadow/portfolio_forward_freeze.json"
        path = self.output_dir / relative
        roles = {
            row["pair_id"]: row["role"]
            for row in _development_finalist_roles(finalists)
        }
        selected = [
            {
                "pair_id": row.pair_id,
                "structural_fingerprint": row.structural_fingerprint,
                "development_finalist_role": roles[row.pair_id],
            }
            for row in finalists
        ]
        if path.is_file():
            payload = _load_json(path)
            claimed = str(payload.get("freeze_hash") or "")
            checked = dict(payload)
            checked.pop("freeze_hash", None)
            if (
                stable_hash(checked) != claimed
                or payload.get("schema") != "hydra_portfolio_forward_freeze_v1"
                or payload.get("campaign_id") != self.campaign_id
                or payload.get("selected_books") != selected
            ):
                raise PortfolioRuntimeError("forward selection freeze drift")
            return payload
        selection_completed = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        freeze_timestamp = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = {
            "schema": "hydra_portfolio_forward_freeze_v1",
            "campaign_id": self.campaign_id,
            "selection_completed_at_utc": selection_completed,
            "freeze_timestamp_utc": freeze_timestamp,
            "selected_books": selected,
            "fresh_bar_operator": "BAR_CLOSE_STRICTLY_GREATER_THAN_FREEZE",
            "broker_connections": 0,
            "orders": 0,
            "q4_access_authorized": False,
            "new_data_purchase_authorized": False,
        }
        payload["freeze_hash"] = stable_hash(payload)
        self.output_writer.write_json(relative, payload)
        return _load_json(path)

    def _anchor_forward_packages(
        self, evidence_receipt: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        anchors: list[dict[str, Any]] = []
        for receipt in getattr(self, "forward_package_receipts", []):
            anchor = {
                "schema": "hydra_portfolio_forward_shadow_anchor_v1",
                "campaign_id": self.campaign_id,
                "candidate_id": receipt["candidate_id"],
                "package_file_sha256": receipt["package_file_sha256"],
                "package_hash": receipt["package_hash"],
                "evidence_bundle_path": str(evidence_receipt["bundle_path"]),
                "evidence_bundle_manifest_sha256": str(
                    evidence_receipt["manifest_sha256"]
                ),
                "evidence_bundle_content_sha256": str(
                    evidence_receipt["bundle_content_sha256"]
                ),
                "evidence_bundle_identity_sha256": receipt[
                    "evidence_bundle_identity_sha256"
                ],
                "freeze_timestamp_utc": receipt["freeze_timestamp_utc"],
                "broker_connections": 0,
                "orders": 0,
                "paper_shadow_ready": False,
                "development_only": True,
            }
            anchor["anchor_hash"] = stable_hash(anchor)
            self.output_writer.write_json(
                "forward_shadow/"
                f"{receipt['candidate_id']}/evidence_bundle_anchor.json",
                anchor,
            )
            finalized_receipt = dict(receipt)
            finalized_receipt.pop("receipt_hash", None)
            finalized_receipt.update(
                {
                    "evidence_bundle_anchor_status": "SEALED_AND_VERIFIED",
                    "evidence_bundle_path": anchor["evidence_bundle_path"],
                    "evidence_bundle_manifest_sha256": anchor[
                        "evidence_bundle_manifest_sha256"
                    ],
                    "evidence_bundle_content_sha256": anchor[
                        "evidence_bundle_content_sha256"
                    ],
                    "evidence_bundle_anchor_hash": anchor["anchor_hash"],
                }
            )
            finalized_receipt["receipt_hash"] = stable_hash(finalized_receipt)
            self.output_writer.write_json(
                "forward_shadow/"
                f"{receipt['candidate_id']}/forward_shadow_receipt.json",
                finalized_receipt,
            )
            anchors.append(anchor)
        return anchors


def _set_worker_state(
    timelines: Mapping[str, Sequence[RoutedTrade]],
    starts: Sequence[int],
    calendars: Mapping[int, Sequence[int]],
    campaign_id: str,
    manifest_blocks: Sequence[Mapping[str, Any]],
    xfa_calendars: Mapping[int, Sequence[int]],
) -> None:
    global _WORK_STATE
    _WORK_STATE = {
        "timelines": {key: tuple(value) for key, value in timelines.items()},
        "starts": tuple(int(value) for value in starts),
        "calendars": {int(key): tuple(value) for key, value in calendars.items()},
        "xfa_calendars": {
            int(key): tuple(value) for key, value in xfa_calendars.items()
        },
        "campaign_id": campaign_id,
        "manifest_blocks": tuple(dict(row) for row in manifest_blocks),
        "rules": official_rule_snapshot_2026_07_15(),
    }


def _clear_worker_state() -> None:
    global _WORK_STATE
    _WORK_STATE = {}


def _combine_batch_worker(
    pair_values: Sequence[Mapping[str, Any]],
    stressed: bool,
    include_evidence: bool = False,
) -> list[dict[str, Any]]:
    return [
        _combine_pair(
            BookPair.from_mapping(value),
            include_stress=stressed,
            include_evidence=include_evidence,
        )
        for value in pair_values
    ]


def _combine_pair(
    pair: BookPair,
    *,
    include_stress: bool,
    include_evidence: bool = False,
) -> dict[str, Any]:
    state = _WORK_STATE
    normal_events, basket = _combine_inputs(pair, state["timelines"], stressed=False)
    normal = [
        run_shared_account_episode(
            normal_events,
            state["calendars"][start],
            basket=basket,
            start_day=start,
            maximum_duration_days=_HORIZON,
            config=state["rules"].combine_config(),
        )
        for start in state["starts"]
    ]
    stressed_rows: list[Any] = []
    if include_stress:
        stress_events, stress_basket = _combine_inputs(
            pair, state["timelines"], stressed=True
        )
        stressed_rows = [
            run_shared_account_episode(
                stress_events,
                state["calendars"][start],
                basket=stress_basket,
                start_day=start,
                maximum_duration_days=_HORIZON,
                config=state["rules"].combine_config(),
            )
            for start in state["starts"]
        ]
    normal_summary = _episode_summary(normal)
    stressed_summary = _episode_summary(stressed_rows) if stressed_rows else None
    payload = {
        "schema": "hydra_portfolio_stage_metric_v1",
        "pair_id": pair.pair_id,
        "structural_fingerprint": pair.structural_fingerprint,
        "normal": normal_summary,
        "stressed": stressed_summary,
        "combine_episode_count": len(normal) + len(stressed_rows),
        "actual_account_behavior_fingerprint": _actual_account_behavior_fingerprint(
            normal_summary,
            stressed_summary,
        ),
        "development_only": True,
    }
    if include_evidence:
        evidence_raw: dict[str, list[dict[str, Any]]] = {
            "NORMAL": [
                _episode_row(
                    SimpleNamespace(
                        source_campaign=state["campaign_id"],
                        policy_id=pair.pair_id,
                    ),
                    episode,
                    scenario="NORMAL",
                    horizon=_HORIZON,
                    events=normal_events,
                )
                for episode in normal
            ],
            "STRESSED_1_5X": [],
        }
        if include_stress:
            evidence_raw["STRESSED_1_5X"] = [
                _episode_row(
                    SimpleNamespace(
                        source_campaign=state["campaign_id"],
                        policy_id=pair.pair_id,
                    ),
                    episode,
                    scenario="STRESSED_1_5X",
                    horizon=_HORIZON,
                    events=stress_events,
                )
                for episode in stressed_rows
            ]
        payload["evidence_raw"] = evidence_raw
    return payload


def _combine_inputs(
    pair: BookPair,
    timelines: Mapping[str, Sequence[RoutedTrade]],
    *,
    stressed: bool,
) -> tuple[dict[str, tuple[RoutedTrade, ...]], PortfolioBasketPolicy]:
    events: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id, units in zip(
        pair.combine_sleeve_ids, pair.combine_allocation_units, strict=True
    ):
        values = tuple(timelines[component_id])
        if stressed:
            values = tuple(_restress(row) for row in values)
        events[component_id] = _scale_events(
            {component_id: values}, float(units) * float(pair.combine_risk_tier)
        )[component_id]
    _assert_unique_portfolio_event_ids(events)
    basket = PortfolioBasketPolicy(
        policy_id=pair.pair_id,
        component_ids=pair.combine_sleeve_ids,
        archetype="PORTFOLIO_FIRST_COMBINE_BOOK",
        maximum_simultaneous_positions=min(len(pair.combine_sleeve_ids), 6),
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=pair.combine_sleeve_ids,
        policy_version=PORTFOLIO_ACCOUNT_POLICY_VERSION,
    )
    return _enforce_combine_market_caps(events, official_rule_snapshot_2026_07_15()), basket


def _assert_unique_portfolio_event_ids(
    events: Mapping[str, Sequence[RoutedTrade]],
) -> None:
    owners: dict[str, str] = {}
    for component_id, rows in events.items():
        for row in rows:
            owner = owners.get(row.event.event_id)
            if owner is not None:
                raise PortfolioRuntimeError(
                    "event_id collision in portfolio replay: "
                    f"{row.event.event_id} ({owner}, {component_id})"
                )
            owners[row.event.event_id] = component_id


def _lifecycle_pair_worker(pair_value: Mapping[str, Any]) -> dict[str, Any]:
    pair = BookPair.from_mapping(pair_value)
    state = _WORK_STATE
    lifecycle_rows: list[dict[str, Any]] = []
    evidence_raw: dict[str, list[dict[str, Any]]] = {"NORMAL": [], "STRESSED_1_5X": []}
    scenario_episodes: dict[str, list[Any]] = {"NORMAL": [], "STRESSED_1_5X": []}
    for scenario, stress in (("NORMAL", False), ("STRESSED_1_5X", True)):
        raw_timelines = {
            key: tuple(_restress(row) for row in values) if stress else tuple(values)
            for key, values in state["timelines"].items()
            if key in set(pair.combine_sleeve_ids) | set(pair.xfa_sleeve_ids)
        }
        combine_basket = PortfolioBasketPolicy(
            policy_id=pair.pair_id,
            component_ids=pair.combine_sleeve_ids,
            archetype="PORTFOLIO_FIRST_COMBINE_BOOK",
            maximum_simultaneous_positions=min(len(pair.combine_sleeve_ids), 6),
            maximum_mini_equivalent=15,
            conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
            component_priority=pair.combine_sleeve_ids,
            policy_version=PORTFOLIO_ACCOUNT_POLICY_VERSION,
        )
        xfa_basket = PortfolioBasketPolicy(
            policy_id=f"{pair.pair_id}:XFA",
            component_ids=pair.xfa_sleeve_ids,
            archetype="PORTFOLIO_FIRST_XFA_BOOK",
            maximum_simultaneous_positions=min(len(pair.xfa_sleeve_ids), 6),
            maximum_mini_equivalent=15,
            conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
            component_priority=pair.xfa_sleeve_ids,
            policy_version=PORTFOLIO_ACCOUNT_POLICY_VERSION,
        )
        combine_book = freeze_portfolio_book(
            book_id=f"{pair.pair_id}:COMBINE",
            role=PortfolioBookRole.COMBINE_BOOK,
            basket=combine_basket,
            risk_profile=FrozenRiskProfile(
                profile_id=f"{pair.pair_id}:COMBINE_PROFILE",
                risk_multiplier=float(pair.combine_risk_tier),
                maximum_simultaneous_positions=6,
                maximum_mini_equivalent=15,
            ),
            sleeve_timelines=raw_timelines,
            sleeve_risk_multipliers=dict(
                zip(pair.combine_sleeve_ids, pair.combine_allocation_units, strict=True)
            ),
        )
        xfa_book = freeze_portfolio_book(
            book_id=f"{pair.pair_id}:XFA",
            role=PortfolioBookRole.XFA_BOOK,
            basket=xfa_basket,
            risk_profile=FrozenRiskProfile(
                profile_id=f"{pair.pair_id}:XFA_PROFILE",
                risk_multiplier=float(pair.xfa_risk_tier),
                maximum_simultaneous_positions=6,
                maximum_mini_equivalent=15,
            ),
            sleeve_timelines=raw_timelines,
            sleeve_risk_multipliers=dict(
                zip(pair.xfa_sleeve_ids, pair.xfa_allocation_units, strict=True)
            ),
        )
        scaled_combine, _basket = _combine_inputs(pair, raw_timelines, stressed=False)
        for start in state["starts"]:
            result = run_portfolio_combine_to_xfa_episode(
                raw_timelines,
                state["calendars"][start],
                combine_book=combine_book,
                xfa_book=xfa_book,
                start_day=start,
                combine_horizon_days=_HORIZON,
                xfa_horizon_days=_XFA_HORIZON,
                rule_snapshot=state["rules"],
                xfa_eligible_session_days=state["xfa_calendars"][start],
            )
            lifecycle_rows.append({**result.to_dict(), "cost_scenario": scenario})
            scenario_episodes[scenario].append(result.combine_episode)
            evidence_raw[scenario].append(
                _episode_row(
                    SimpleNamespace(
                        source_campaign=state["campaign_id"], policy_id=pair.pair_id
                    ),
                    result.combine_episode,
                    scenario=scenario,
                    horizon=_HORIZON,
                    events=scaled_combine,
                )
            )
    summary = _lifecycle_summary(lifecycle_rows, scenario_episodes)
    combine_behavior = _actual_account_behavior_fingerprint(
        summary["normal"], summary["stressed"]
    )
    return {
        "schema": "hydra_portfolio_lifecycle_metric_v1",
        "pair_id": pair.pair_id,
        "structural_fingerprint": pair.structural_fingerprint,
        **summary,
        "combine_account_behavior_fingerprint": combine_behavior,
        "actual_account_behavior_fingerprint": _lifecycle_account_behavior_fingerprint(
            combine_behavior, lifecycle_rows
        ),
        "evidence_raw": evidence_raw,
        "lifecycle_rows": lifecycle_rows,
        "development_only": True,
    }


def _restress(row: RoutedTrade) -> RoutedTrade:
    event = row.event
    cost = max(0.0, float(event.gross_pnl - event.net_pnl))
    extra = 0.5 * cost
    return replace(
        row,
        event=replace(
            event,
            event_id=f"{event.event_id}:portfolio_cost_stress_1_5x",
            net_pnl=float(event.net_pnl - extra),
            worst_unrealized_pnl=float(event.worst_unrealized_pnl - extra),
            best_unrealized_pnl=float(event.best_unrealized_pnl - extra),
        ),
    )


def _episode_summary(episodes: Sequence[Any]) -> dict[str, Any]:
    if not episodes:
        return {
            "episode_count": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "evaluable_pass_rate": 0.0,
            "observed_pass_fraction": 0.0,
            "evaluable_episode_count": 0,
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
            "mll_breach_rate": 0.0,
            "mll_breach_count": 0,
            "minimum_mll_buffer": 4_500.0,
            "consistency_rate": 0.0,
            "consistency_ok_count": 0,
            "pass_block_count": 0,
            "pass_block_ids": [],
            "by_block_net": {},
            "component_contribution": {},
            "maximum_block_profit_share": 0.0,
            "maximum_sleeve_profit_share": 0.0,
        }
    net = [float(row.net_pnl) for row in episodes]
    progress = [float(row.target_progress) for row in episodes]
    contribution: dict[str, float] = {}
    for row in episodes:
        for key, value in row.component_contribution.items():
            contribution[key] = contribution.get(key, 0.0) + float(value)
    blocks = tuple(_WORK_STATE.get("manifest_blocks") or ())
    manifest = {"temporal_blocks": {"blocks": blocks}}
    by_block: dict[str, float] = {}
    pass_blocks: set[str] = set()
    for row in episodes:
        block = _block_for_day(int(row.start_day), manifest) if blocks else "UNKNOWN"
        by_block[block] = by_block.get(block, 0.0) + float(row.net_pnl)
        if row.passed:
            pass_blocks.add(block)
    positive_total = sum(max(value, 0.0) for value in contribution.values())
    positive_block_total = sum(max(value, 0.0) for value in by_block.values())
    pass_count = sum(bool(row.passed) for row in episodes)
    censored_count = sum(
        str(row.terminal.value) == "TIMEOUT"
        and int(row.eligible_days) < _HORIZON
        for row in episodes
    )
    evaluable_count = len(episodes) - censored_count
    terminal_distribution = {
        terminal: sum(str(row.terminal.value) == terminal for row in episodes)
        for terminal in sorted({str(row.terminal.value) for row in episodes})
    }
    return {
        "episode_count": len(episodes),
        "pass_count": pass_count,
        # The promotion/headline rate remains the conservative fraction of all
        # frozen starts.  Early data censoring is reported separately; it is
        # never allowed to inflate the >=10% graduation gate.
        "pass_rate": pass_count / len(episodes),
        "evaluable_pass_rate": (
            pass_count / evaluable_count if evaluable_count else 0.0
        ),
        "observed_pass_fraction": pass_count / len(episodes),
        "evaluable_episode_count": evaluable_count,
        "censored_episode_count": censored_count,
        "censoring_rate": censored_count / len(episodes),
        "terminal_distribution": terminal_distribution,
        "net_total": sum(net),
        "net_median": statistics.median(net),
        "net_values": net,
        "target_progress_median": statistics.median(progress),
        "target_progress_p25": _quantile(progress, 0.25),
        "target_progress_values": progress,
        "maximum_target_progress": max(float(row.maximum_target_progress) for row in episodes),
        "mll_breach_rate": sum(bool(row.mll_breached) for row in episodes) / len(episodes),
        "mll_breach_count": sum(bool(row.mll_breached) for row in episodes),
        "minimum_mll_buffer": min(float(row.minimum_mll_buffer) for row in episodes),
        "consistency_rate": sum(bool(row.consistency_ok) for row in episodes) / len(episodes),
        "consistency_ok_count": sum(bool(row.consistency_ok) for row in episodes),
        "pass_block_count": len(pass_blocks),
        "pass_block_ids": sorted(pass_blocks),
        "by_block_net": dict(sorted(by_block.items())),
        "component_contribution": dict(sorted(contribution.items())),
        "maximum_block_profit_share": (
            max((max(value, 0.0) for value in by_block.values()), default=0.0)
            / positive_block_total
            if positive_block_total > 0.0
            else 0.0
        ),
        "maximum_sleeve_profit_share": (
            max((max(value, 0.0) for value in contribution.values()), default=0.0)
            / positive_total
            if positive_total > 0.0
            else 0.0
        ),
    }


def _actual_account_behavior_fingerprint(
    normal: Mapping[str, Any], stressed: Mapping[str, Any] | None
) -> str:
    """Hash observed account trajectories, not just policy specifications."""

    def scenario(value: Mapping[str, Any] | None) -> Any:
        if value is None:
            return None
        episode_count = int(value.get("episode_count", 1))
        return {
            "net_values": [
                round(float(row), 8)
                for row in value.get(
                    "net_values", (float(value.get("net_total", 0.0)),)
                )
            ],
            "target_progress_values": [
                round(float(row), 12)
                for row in value.get(
                    "target_progress_values",
                    (float(value.get("target_progress_median", 0.0)),),
                )
            ],
            "pass_count": int(value["pass_count"]),
            "mll_breach_count": int(
                value.get(
                    "mll_breach_count",
                    round(float(value.get("mll_breach_rate", 0.0)) * episode_count),
                )
            ),
            "consistency_ok_count": int(
                value.get(
                    "consistency_ok_count",
                    round(float(value.get("consistency_rate", 0.0)) * episode_count),
                )
            ),
            "minimum_mll_buffer": round(
                float(value.get("minimum_mll_buffer", 4_500.0)), 8
            ),
            "maximum_sleeve_profit_share": round(
                float(value.get("maximum_sleeve_profit_share", 0.0)), 12
            ),
        }

    return stable_hash(
        {
            "schema": "hydra_observed_portfolio_account_behavior_v1",
            "normal": scenario(normal),
            "stressed": scenario(stressed),
        }
    )


def _lifecycle_account_behavior_fingerprint(
    combine_behavior_fingerprint: str,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    """Hash observed Combine plus XFA/payout account trajectories.

    Stage 1/2 use the Combine-only fingerprint.  Once lifecycle evaluation
    begins, finalist deduplication must also distinguish actual XFA balances,
    payout paths, and post-payout survival without treating book parameters
    alone as observed behavioral evidence.
    """

    def path(value: Any) -> Any:
        if not isinstance(value, Mapping):
            return None
        ledger = list(value.get("daily_ledger") or ())
        return {
            "path": str(value.get("path") or ""),
            "terminal": str(value.get("terminal") or ""),
            "observed_days": int(value.get("observed_days", 0)),
            "payout_eligible": bool(value.get("payout_eligible")),
            "payout_cycles": int(value.get("payout_cycles", 0)),
            "first_payout_day": value.get("first_payout_day"),
            "trader_net_payout": round(
                float(value.get("trader_net_payout", 0.0)), 8
            ),
            "post_payout_observed_days": int(
                value.get("post_payout_observed_days", 0)
            ),
            "post_payout_survived": bool(value.get("post_payout_survived")),
            "post_payout_censored": bool(value.get("post_payout_censored")),
            "daily_ledger_hash": stable_hash(ledger),
        }

    observed = sorted(
        (
            {
                "cost_scenario": str(row.get("cost_scenario") or ""),
                "start_day": int(row["start_day"]),
                "combine_status": str(row.get("combine_status") or ""),
                "xfa_started": bool(row.get("xfa_started")),
                "xfa_start_day": row.get("xfa_start_day"),
                "xfa_standard": path(row.get("xfa_standard")),
                "xfa_consistency": path(row.get("xfa_consistency")),
            }
            for row in rows
        ),
        key=lambda row: (row["cost_scenario"], row["start_day"]),
    )
    return stable_hash(
        {
            "schema": "hydra_observed_portfolio_lifecycle_behavior_v1",
            "combine_behavior_fingerprint": combine_behavior_fingerprint,
            "observed_lifecycle_paths": observed,
        }
    )


def _metric_account_behavior_fingerprint(row: Mapping[str, Any]) -> str:
    declared = str(row.get("actual_account_behavior_fingerprint") or "")
    if declared:
        return declared
    stressed = row.get("stressed")
    return _actual_account_behavior_fingerprint(
        row["normal"],
        stressed if isinstance(stressed, Mapping) else None,
    )


def _lifecycle_summary(
    rows: Sequence[Mapping[str, Any]],
    scenarios: Mapping[str, Sequence[Any]],
) -> dict[str, Any]:
    normal = _episode_summary(scenarios["NORMAL"])
    stressed = _episode_summary(scenarios["STRESSED_1_5X"])
    xfa_started = sum(bool(row["xfa_started"]) for row in rows)
    unique_xfa_start_days = sorted(
        {int(row["start_day"]) for row in rows if bool(row["xfa_started"])}
    )
    paths = [
        path
        for row in rows
        for path in (row.get("xfa_standard"), row.get("xfa_consistency"))
        if isinstance(path, Mapping)
    ]
    standard = [row for row in paths if row["path"] == "XFA_STANDARD"]
    consistency = [row for row in paths if row["path"] == "XFA_CONSISTENCY"]
    payout = [row for row in paths if bool(row["payout_eligible"])]
    payout_net = sum(float(row["trader_net_payout"]) for row in paths)
    path_metrics: dict[str, dict[str, dict[str, Any]]] = {}
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        scenario_rows = [row for row in rows if row["cost_scenario"] == scenario]
        scenario_summary = normal if scenario == "NORMAL" else stressed
        combine_attempts = int(scenario_summary["episode_count"])
        combine_passes = int(scenario_summary["pass_count"])
        path_metrics[scenario] = {}
        for path_name, key in (
            ("XFA_STANDARD", "STANDARD"),
            ("XFA_CONSISTENCY", "CONSISTENCY"),
        ):
            selected = [
                path
                for row in scenario_rows
                for path in (row.get("xfa_standard"), row.get("xfa_consistency"))
                if isinstance(path, Mapping) and path["path"] == path_name
            ]
            eligible = [path for path in selected if bool(path["payout_eligible"])]
            survived = [path for path in eligible if bool(path["post_payout_survived"])]
            censored = [path for path in eligible if bool(path.get("post_payout_censored"))]
            first_payout_days = sorted(
                int(path["first_payout_day"])
                for path in eligible
                if path.get("first_payout_day") is not None
            )
            payout_cycles = sum(int(path["payout_cycles"]) for path in selected)
            path_metrics[scenario][key] = {
                "combine_attempt_count": combine_attempts,
                "combine_pass_count": combine_passes,
                "path_count": len(selected),
                "first_payouts": len(eligible),
                "first_payout_day_values": first_payout_days,
                "median_trading_days_to_first_payout": (
                    statistics.median(first_payout_days)
                    if first_payout_days
                    else None
                ),
                "xfa_entry_probability": (
                    len(selected) / combine_attempts if combine_attempts else 0.0
                ),
                "first_payout_probability_conditional_on_combine_pass": (
                    len(eligible) / combine_passes if combine_passes else 0.0
                ),
                "first_payout_probability_unconditional": (
                    len(eligible) / combine_attempts if combine_attempts else 0.0
                ),
                "payout_cycles": payout_cycles,
                "expected_payout_cycles_per_combine_attempt": (
                    payout_cycles / combine_attempts if combine_attempts else 0.0
                ),
                "trader_net_payout": sum(float(path["trader_net_payout"]) for path in selected),
                "post_payout_survived_count": len(survived),
                "post_payout_survival_rate": len(survived) / len(eligible) if eligible else 0.0,
                "post_payout_censored_count": len(censored),
                "post_payout_censoring_rate": len(censored) / len(eligible) if eligible else 0.0,
            }
    # The ranking path is frozen before outcomes: stressed XFA Standard.
    # Consistency remains a separately reported alternative, never summed into
    # the primary expected-payout objective.
    ranking_path = path_metrics["STRESSED_1_5X"]["STANDARD"]
    attempts = max(int(stressed["episode_count"]), 1)
    return {
        "normal": normal,
        "stressed": stressed,
        "combine_episode_count": len(rows),
        "normal_combine_passes": int(normal["pass_count"]),
        "stressed_combine_passes": int(stressed["pass_count"]),
        "xfa_paths_started": xfa_started,
        "unique_xfa_start_days": unique_xfa_start_days,
        "unique_xfa_start_count": len(unique_xfa_start_days),
        "xfa_standard_paths": len(standard),
        "xfa_consistency_paths": len(consistency),
        "first_payouts": len(payout),
        "payout_cycles": sum(int(row["payout_cycles"]) for row in paths),
        "trader_net_payout": payout_net,
        "ranking_trader_net_payout": float(ranking_path["trader_net_payout"]),
        "expected_trader_net_payout_per_attempt": float(ranking_path["trader_net_payout"]) / attempts,
        "post_payout_survival_rate": float(ranking_path["post_payout_survival_rate"]),
        "path_metrics": path_metrics,
        "ranking_path": "STRESSED_1_5X:XFA_STANDARD",
        "failure_vectors": _failure_vectors(stressed),
    }


def _evidence_rows(
    payload: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    episodes, paths = _combine_evidence_rows(payload, manifest)
    lifecycle_by_key: dict[tuple[str, int], Mapping[str, Any]] = {}
    for raw in payload["lifecycle_rows"]:
        key = (str(raw["cost_scenario"]), int(raw["start_day"]))
        if key in lifecycle_by_key:
            raise PortfolioRuntimeError("duplicate raw portfolio lifecycle path")
        lifecycle_by_key[key] = raw
    if len(lifecycle_by_key) != len(episodes):
        raise PortfolioRuntimeError("portfolio lifecycle/evidence cardinality drift")
    for episode in episodes:
        key = (
            str(episode["cost_scenario"]),
            _episode_start_to_epoch_day(str(episode["episode_start"])),
        )
        raw_lifecycle = lifecycle_by_key.pop(key, None)
        if raw_lifecycle is None:
            raise PortfolioRuntimeError("episode lacks raw XFA lifecycle evidence")
        episode["portfolio_lifecycle"] = _portfolio_lifecycle_evidence(
            raw_lifecycle
        )
    if lifecycle_by_key:
        raise PortfolioRuntimeError("orphan raw portfolio lifecycle evidence")
    return episodes, paths


def _combine_evidence_rows(
    payload: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    episodes: list[dict[str, Any]] = []
    paths: list[dict[str, Any]] = []
    stressed = payload.get("stressed")
    failure_source = stressed if isinstance(stressed, Mapping) else payload["normal"]
    failure = str(_failure_vectors(failure_source)[0])
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        for raw in payload["evidence_raw"][scenario]:
            episode, daily = _convert_episode(
                raw,
                manifest,
                scenario,
                failure,
            )
            episodes.append(episode)
            paths.extend(daily)
    return episodes, paths


def _portfolio_lifecycle_evidence(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Embed the raw, chronological XFA/payout record in its Combine episode."""

    required = {
        "lifecycle_version",
        "start_day",
        "combine_status",
        "combine_book",
        "xfa_book",
        "xfa_started",
        "xfa_start_day",
        "xfa_standard",
        "xfa_consistency",
        "rule_snapshot",
        "union_timeline_hash",
        "evidence_hash",
    }
    missing = required - set(raw)
    if missing:
        raise PortfolioRuntimeError(
            "raw lifecycle evidence is incomplete: " + ", ".join(sorted(missing))
        )
    source_payload = dict(raw)
    claimed_source_hash = str(source_payload.pop("evidence_hash", ""))
    source_payload.pop("cost_scenario", None)
    if stable_hash(source_payload) != claimed_source_hash:
        raise PortfolioRuntimeError("portfolio source lifecycle evidence hash drift")
    started = bool(raw["xfa_started"])
    for path_name in ("xfa_standard", "xfa_consistency"):
        path = raw[path_name]
        if started and not isinstance(path, Mapping):
            raise PortfolioRuntimeError("started XFA path lacks raw lifecycle ledger")
        if not started and path is not None:
            raise PortfolioRuntimeError("failed Combine has impossible XFA path")
        if isinstance(path, Mapping):
            ledger = path.get("daily_ledger")
            if not isinstance(ledger, list) or len(ledger) != int(
                path.get("observed_days", -1)
            ):
                raise PortfolioRuntimeError("XFA daily lifecycle ledger is incomplete")
            if int(path.get("payout_cycles", -1)) != sum(
                bool(day.get("payout_requested")) for day in ledger
            ):
                raise PortfolioRuntimeError("XFA payout ledger does not reconcile")
    if (
        raw.get("unrealized_aggregation_semantics")
        != UNREALIZED_AGGREGATION_SEMANTICS
    ):
        raise PortfolioRuntimeError(
            "portfolio lifecycle omitted conservative unrealized-path semantics"
        )
    payload = {
        "schema": "hydra_portfolio_lifecycle_evidence_v1",
        "lifecycle_version": str(raw["lifecycle_version"]),
        "start_day": int(raw["start_day"]),
        "combine_status": str(raw["combine_status"]),
        "combine_book": dict(raw["combine_book"]),
        "xfa_book": dict(raw["xfa_book"]),
        "xfa_started": started,
        "xfa_start_day": (
            None if raw["xfa_start_day"] is None else int(raw["xfa_start_day"])
        ),
        "xfa_standard": (
            None if raw["xfa_standard"] is None else dict(raw["xfa_standard"])
        ),
        "xfa_consistency": (
            None
            if raw["xfa_consistency"] is None
            else dict(raw["xfa_consistency"])
        ),
        "rule_snapshot": dict(raw["rule_snapshot"]),
        "union_timeline_hash": str(raw["union_timeline_hash"]),
        "source_lifecycle_evidence_hash": claimed_source_hash,
        "combine_profit_transferred_to_xfa": bool(
            raw.get("combine_profit_transferred_to_xfa", False)
        ),
        "books_frozen_before_replay": bool(
            raw.get("books_frozen_before_replay", False)
        ),
        "xfa_book_selected_from_outcomes": bool(
            raw.get("xfa_book_selected_from_outcomes", True)
        ),
        "unrealized_aggregation_semantics": str(
            raw.get("unrealized_aggregation_semantics") or ""
        ),
        "development_only": True,
    }
    payload["sealed_lifecycle_sha256"] = stable_hash(payload)
    return payload


def _episode_start_to_epoch_day(value: str) -> int:
    from datetime import date, datetime

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    return (parsed - date(1970, 1, 1)).days


def _pair_metric_is_economically_eligible(
    row: Mapping[str, Any], *, require_stress: bool
) -> bool:
    normal = row["normal"]
    stress = row.get("stressed")
    return bool(
        float(normal["net_total"]) > 0.0
        and float(normal["mll_breach_rate"]) <= 0.15
        and (
            not require_stress
            or (
                isinstance(stress, Mapping)
                and float(stress["net_total"]) > 0.0
                and float(stress["mll_breach_rate"]) <= 0.15
            )
        )
    )


def _select_pairs(
    pairs: Sequence[BookPair],
    metrics: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    require_stress: bool,
    required_sleeve_ids: set[str] | None = None,
) -> tuple[BookPair, ...]:
    by_id = {row.pair_id: row for row in pairs}
    ranked: list[tuple[Mapping[str, Any], Mapping[str, Any], bool]] = []
    for row in metrics:
        normal = row["normal"]
        stress = row.get("stressed")
        chosen = stress if require_stress and isinstance(stress, Mapping) else normal
        ranked.append(
            (
                row,
                chosen,
                _pair_metric_is_economically_eligible(
                    row, require_stress=require_stress
                ),
            )
        )
    ranked.sort(
        key=lambda item: (
            -int(item[1]["pass_count"]),
            -float(item[1]["target_progress_p25"]),
            -float(item[1]["target_progress_median"]),
            -float(item[1]["net_total"]),
            float(item[1]["mll_breach_rate"]),
            item[0]["pair_id"],
        )
    )
    ranked_unique: list[tuple[Mapping[str, Any], Mapping[str, Any], bool]] = []
    seen_behaviors: set[str] = set()
    for item in ranked:
        behavior = _metric_account_behavior_fingerprint(item[0])
        if behavior in seen_behaviors:
            continue
        seen_behaviors.add(behavior)
        ranked_unique.append(item)
    eligible = [item for item in ranked_unique if item[2]]
    # A scientific null is an outcome, not a runtime-integrity failure.  If no
    # book meets the economic screen, continue a bounded diagnostic tranche so
    # the campaign can seal complete evidence and terminate honestly.
    pool = eligible if eligible else ranked_unique
    selected: list[BookPair] = []
    selected_ids: set[str] = set()

    required = set(required_sleeve_ids or ())
    missing = set(required)
    if missing:
        for metric, _chosen, _economic in ranked_unique:
            pair = by_id[str(metric["pair_id"])]
            components = set(pair.combine_sleeve_ids) | set(pair.xfa_sleeve_ids)
            if components & missing:
                selected.append(pair)
                selected_ids.add(pair.pair_id)
                missing -= components
                if not missing:
                    break
        if missing:
            raise PortfolioRuntimeError(
                "portfolio Stage-1 cannot cover frozen sleeves: "
                + ",".join(sorted(missing))
            )

    for metric, _chosen, _economic in pool:
        pair = by_id[str(metric["pair_id"])]
        if pair.pair_id in selected_ids:
            continue
        selected.append(pair)
        selected_ids.add(pair.pair_id)
        if len(selected) >= limit:
            break
    return tuple(selected[:limit])


def _select_lifecycle_pairs(
    pairs: Sequence[BookPair],
    metrics: Sequence[Mapping[str, Any]],
    *,
    limit: int,
    minimum_starts: int,
) -> tuple[BookPair, ...]:
    by_id = {row.pair_id: row for row in pairs}
    eligible = [
        row
        for row in metrics
        if int(row["normal"]["episode_count"]) >= minimum_starts
        and int(row["normal"]["pass_count"]) >= 3
        and int(row["normal"]["pass_block_count"]) >= 2
        and float(row["normal"]["pass_rate"]) >= 0.10
        and float(row["stressed"]["net_total"]) > 0.0
        and float(row["stressed"]["mll_breach_rate"]) <= 0.10
        and float(row["stressed"]["consistency_rate"]) >= 0.50
        and float(row["stressed"]["maximum_block_profit_share"]) <= 0.50
        and float(row["stressed"]["maximum_sleeve_profit_share"]) <= 0.50
        and float(row["normal"]["target_progress_median"]) > 0.0
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["expected_trader_net_payout_per_attempt"]),
            -float(row["stressed"]["pass_rate"]),
            -float(row["normal"]["pass_rate"]),
            -float(row["stressed"]["target_progress_p25"]),
            float(row["stressed"]["mll_breach_rate"]),
            float(row["normal"]["maximum_sleeve_profit_share"]),
            row["pair_id"],
        )
    )
    selected: list[BookPair] = []
    seen_behaviors: set[str] = set()
    for row in eligible:
        behavior = _metric_account_behavior_fingerprint(row)
        if behavior in seen_behaviors:
            continue
        seen_behaviors.add(behavior)
        selected.append(by_id[str(row["pair_id"])])
        if len(selected) >= limit:
            break
    return tuple(selected)


def _combine_stage_metrics(
    prior: Sequence[Mapping[str, Any]], newer: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    by_id = {str(row["pair_id"]): dict(row) for row in prior}
    for row in newer:
        pair_id = str(row["pair_id"])
        if pair_id not in by_id:
            by_id[pair_id] = dict(row)
            continue
        left = by_id[pair_id]
        combined = dict(left)
        for scenario in ("normal", "stressed"):
            combined[scenario] = _merge_summary(left[scenario], row[scenario])
        for key in (
            "combine_episode_count",
            "normal_combine_passes",
            "stressed_combine_passes",
            "xfa_paths_started",
            "xfa_standard_paths",
            "xfa_consistency_paths",
            "first_payouts",
            "payout_cycles",
            "trader_net_payout",
            "ranking_trader_net_payout",
        ):
            combined[key] = float(left[key]) + float(row[key])
            if key not in {"trader_net_payout", "ranking_trader_net_payout"}:
                combined[key] = int(combined[key])
        combined["path_metrics"] = _merge_path_metrics(
            left["path_metrics"], row["path_metrics"]
        )
        ranking = combined["path_metrics"]["STRESSED_1_5X"]["STANDARD"]
        combined["ranking_trader_net_payout"] = float(
            ranking["trader_net_payout"]
        )
        attempts = max(int(combined["stressed"]["episode_count"]), 1)
        combined["expected_trader_net_payout_per_attempt"] = float(combined["ranking_trader_net_payout"]) / attempts
        combined["post_payout_survival_rate"] = float(
            ranking["post_payout_survival_rate"]
        )
        combined["lifecycle_rows"] = list(left.get("lifecycle_rows", ())) + list(row.get("lifecycle_rows", ()))
        combined["unique_xfa_start_days"] = sorted(
            {
                int(value)
                for value in (
                    *left.get("unique_xfa_start_days", ()),
                    *row.get("unique_xfa_start_days", ()),
                )
            }
        )
        combined["unique_xfa_start_count"] = len(
            combined["unique_xfa_start_days"]
        )
        combined["evidence_raw"] = {
            scenario: list(left.get("evidence_raw", {}).get(scenario, ())) + list(row.get("evidence_raw", {}).get(scenario, ()))
            for scenario in ("NORMAL", "STRESSED_1_5X")
        }
        combine_behavior = _actual_account_behavior_fingerprint(
            combined["normal"], combined["stressed"]
        )
        combined["combine_account_behavior_fingerprint"] = combine_behavior
        combined["actual_account_behavior_fingerprint"] = (
            _lifecycle_account_behavior_fingerprint(
                combine_behavior, combined["lifecycle_rows"]
            )
        )
        by_id[pair_id] = combined
    return list(by_id.values())


def _merge_path_metrics(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, dict[str, dict[str, Any]]]:
    """Add disjoint lifecycle batches without summing alternative objectives."""

    output: dict[str, dict[str, dict[str, float | int]]] = {}
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        output[scenario] = {}
        for path in ("STANDARD", "CONSISTENCY"):
            first = left[scenario][path]
            second = right[scenario][path]
            payouts = int(first["first_payouts"]) + int(second["first_payouts"])
            attempts = int(first.get("combine_attempt_count", 0)) + int(
                second.get("combine_attempt_count", 0)
            )
            combine_passes = int(first.get("combine_pass_count", 0)) + int(
                second.get("combine_pass_count", 0)
            )
            path_count = int(first["path_count"]) + int(second["path_count"])
            payout_cycles = int(first["payout_cycles"]) + int(
                second["payout_cycles"]
            )
            first_payout_days = sorted(
                int(value)
                for value in (
                    *first.get("first_payout_day_values", ()),
                    *second.get("first_payout_day_values", ()),
                )
            )
            survived = int(first.get("post_payout_survived_count", 0)) + int(
                second.get("post_payout_survived_count", 0)
            )
            censored = int(first.get("post_payout_censored_count", 0)) + int(
                second.get("post_payout_censored_count", 0)
            )
            output[scenario][path] = {
                "combine_attempt_count": attempts,
                "combine_pass_count": combine_passes,
                "path_count": path_count,
                "first_payouts": payouts,
                "first_payout_day_values": first_payout_days,
                "median_trading_days_to_first_payout": (
                    statistics.median(first_payout_days)
                    if first_payout_days
                    else None
                ),
                "xfa_entry_probability": (
                    path_count / attempts if attempts else 0.0
                ),
                "first_payout_probability_conditional_on_combine_pass": (
                    payouts / combine_passes if combine_passes else 0.0
                ),
                "first_payout_probability_unconditional": (
                    payouts / attempts if attempts else 0.0
                ),
                "payout_cycles": payout_cycles,
                "expected_payout_cycles_per_combine_attempt": (
                    payout_cycles / attempts if attempts else 0.0
                ),
                "trader_net_payout": float(first["trader_net_payout"])
                + float(second["trader_net_payout"]),
                "post_payout_survived_count": survived,
                "post_payout_survival_rate": survived / payouts if payouts else 0.0,
                "post_payout_censored_count": censored,
                "post_payout_censoring_rate": censored / payouts if payouts else 0.0,
            }
    return output


def _merge_summary(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    count = int(left["episode_count"]) + int(right["episode_count"])
    passes = int(left["pass_count"]) + int(right["pass_count"])
    censored = int(left.get("censored_episode_count", 0)) + int(
        right.get("censored_episode_count", 0)
    )
    evaluable = int(
        left.get("evaluable_episode_count", int(left["episode_count"]))
    ) + int(right.get("evaluable_episode_count", int(right["episode_count"])))
    net = float(left["net_total"]) + float(right["net_total"])
    net_values = list(left.get("net_values", ())) + list(
        right.get("net_values", ())
    )
    progress_values = list(left.get("target_progress_values", ())) + list(
        right.get("target_progress_values", ())
    )
    breaches = int(
        left.get(
            "mll_breach_count",
            round(float(left["mll_breach_rate"]) * int(left["episode_count"])),
        )
    ) + int(
        right.get(
            "mll_breach_count",
            round(float(right["mll_breach_rate"]) * int(right["episode_count"])),
        )
    )
    consistency = int(
        left.get(
            "consistency_ok_count",
            round(float(left["consistency_rate"]) * int(left["episode_count"])),
        )
    ) + int(
        right.get(
            "consistency_ok_count",
            round(float(right["consistency_rate"]) * int(right["episode_count"])),
        )
    )
    by_block = {
        key: float(left.get("by_block_net", {}).get(key, 0.0))
        + float(right.get("by_block_net", {}).get(key, 0.0))
        for key in set(left.get("by_block_net", {})) | set(right.get("by_block_net", {}))
    }
    positive_block_total = sum(max(value, 0.0) for value in by_block.values())
    contribution = {
        key: float(left.get("component_contribution", {}).get(key, 0.0))
        + float(right.get("component_contribution", {}).get(key, 0.0))
        for key in set(left.get("component_contribution", {}))
        | set(right.get("component_contribution", {}))
    }
    positive_component_total = sum(
        max(value, 0.0) for value in contribution.values()
    )
    pass_block_ids = sorted(
        set(left.get("pass_block_ids", ())) | set(right.get("pass_block_ids", ()))
    )
    terminal_distribution = {
        key: int(left.get("terminal_distribution", {}).get(key, 0))
        + int(right.get("terminal_distribution", {}).get(key, 0))
        for key in set(left.get("terminal_distribution", {}))
        | set(right.get("terminal_distribution", {}))
    }
    return {
        **dict(left),
        "episode_count": count,
        "pass_count": passes,
        "pass_rate": passes / max(count, 1),
        "evaluable_pass_rate": passes / max(evaluable, 1),
        "observed_pass_fraction": passes / max(count, 1),
        "evaluable_episode_count": evaluable,
        "censored_episode_count": censored,
        "censoring_rate": censored / max(count, 1),
        "terminal_distribution": dict(sorted(terminal_distribution.items())),
        "net_total": net,
        "net_median": (
            statistics.median(net_values)
            if net_values
            else (
                float(left["net_median"]) * int(left["episode_count"])
                + float(right["net_median"]) * int(right["episode_count"])
            )
            / max(count, 1)
        ),
        "net_values": net_values,
        "target_progress_median": (
            statistics.median(progress_values)
            if progress_values
            else (
                float(left["target_progress_median"])
                * int(left["episode_count"])
                + float(right["target_progress_median"])
                * int(right["episode_count"])
            )
            / max(count, 1)
        ),
        "target_progress_p25": (
            _quantile(progress_values, 0.25)
            if progress_values
            else min(
                float(left["target_progress_p25"]),
                float(right["target_progress_p25"]),
            )
        ),
        "target_progress_values": progress_values,
        "maximum_target_progress": max(float(left["maximum_target_progress"]), float(right["maximum_target_progress"])),
        "mll_breach_rate": breaches / max(count, 1),
        "mll_breach_count": breaches,
        "minimum_mll_buffer": min(float(left["minimum_mll_buffer"]), float(right["minimum_mll_buffer"])),
        "consistency_rate": consistency / max(count, 1),
        "consistency_ok_count": consistency,
        "pass_block_count": len(pass_block_ids),
        "pass_block_ids": pass_block_ids,
        "by_block_net": dict(sorted(by_block.items())),
        "component_contribution": dict(sorted(contribution.items())),
        "maximum_block_profit_share": (
            max((max(value, 0.0) for value in by_block.values()), default=0.0)
            / positive_block_total
            if positive_block_total > 0.0
            else 0.0
        ),
        "maximum_sleeve_profit_share": (
            max(
                (max(value, 0.0) for value in contribution.values()), default=0.0
            )
            / positive_component_total
            if positive_component_total > 0.0
            else max(
                float(left["maximum_sleeve_profit_share"]),
                float(right["maximum_sleeve_profit_share"]),
            )
        ),
    }


def _status_matrix(
    metrics: Sequence[Mapping[str, Any]], *, forward_ids: set[str]
) -> list[dict[str, Any]]:
    output = []
    for row in metrics:
        evidence = BookEvidence(
            book_pair_id=str(row["pair_id"]),
            combine_starts=int(row["normal"]["episode_count"]),
            combine_evaluable_starts=int(row["normal"]["evaluable_episode_count"]),
            normal_combine_passes=int(row["normal"]["pass_count"]),
            stressed_combine_passes=int(row["stressed"]["pass_count"]),
            pass_block_ids=tuple(str(value) for value in row["normal"].get("pass_block_ids", ())),
            stressed_net_pnl=float(row["stressed"]["net_total"]),
            stressed_economically_defensible=float(row["stressed"]["net_total"]) > 0.0,
            mll_breach_rate=float(row["stressed"]["mll_breach_rate"]),
            consistency_acceptable=float(row["stressed"]["consistency_rate"]) >= 0.50,
            maximum_block_profit_share=float(row["stressed"]["maximum_block_profit_share"]),
            maximum_sleeve_profit_share=float(row["stressed"]["maximum_sleeve_profit_share"]),
            xfa_paths_started=int(row["xfa_paths_started"]),
            unique_xfa_start_days=tuple(
                int(value) for value in row.get("unique_xfa_start_days", ())
            ),
            payout_eligible_paths=int(row["first_payouts"]),
            payout_cycles=int(row["payout_cycles"]),
            expected_trader_net_payout_per_attempt=float(row["expected_trader_net_payout_per_attempt"]),
            post_payout_survival_rate=float(row["post_payout_survival_rate"]),
            complete_evidence_bundle=True,
            immutable_books_complete=True,
            forward_no_order_package_complete=str(row["pair_id"]) in forward_ids,
        )
        output.append(
            {
                "pair_id": row["pair_id"],
                "statuses": [value.value for value in decide_book_statuses(evidence)],
                "normal": row["normal"],
                "stressed": row["stressed"],
                "xfa_paths_started": row["xfa_paths_started"],
                "unique_xfa_start_days": row.get("unique_xfa_start_days", []),
                "unique_xfa_start_count": row.get("unique_xfa_start_count", 0),
                "first_payouts": row["first_payouts"],
                "payout_cycles": row["payout_cycles"],
                "expected_trader_net_payout_per_attempt": row["expected_trader_net_payout_per_attempt"],
                "post_payout_survival_rate": row["post_payout_survival_rate"],
                "path_metrics": row["path_metrics"],
                "ranking_path": row["ranking_path"],
            }
        )
    return output


def _failure_vectors(stressed: Mapping[str, Any]) -> list[str]:
    output = []
    if float(stressed["mll_breach_rate"]) > 0.10:
        output.append("MLL_BREACH")
    if float(stressed["net_total"]) <= 0.0:
        output.append("COST_FRAGILITY")
    if float(stressed["target_progress_median"]) < 0.35:
        output.append("TARGET_TOO_SLOW")
    if float(stressed["maximum_sleeve_profit_share"]) > 0.50:
        output.append("OVER_CONCENTRATION")
    return output or ["NO_INCREMENTAL_VALUE"]


def _sum_lifecycle(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    keys = (
        "combine_episode_count",
        "normal_combine_passes",
        "stressed_combine_passes",
        "xfa_paths_started",
        "xfa_standard_paths",
        "xfa_consistency_paths",
        "first_payouts",
        "payout_cycles",
        "trader_net_payout",
    )
    return {key: sum(float(row[key]) for row in rows) for key in keys}


def _compact_lifecycle_metric(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if key not in {"evidence_raw", "lifecycle_rows"}
    }


def _compact_combine_metric(value: Mapping[str, Any]) -> dict[str, Any]:
    """Drop heavy daily evidence from in-memory ranking metrics only.

    The complete payload remains in the deterministic Stage-2 batch cache and
    is streamed to the single EvidenceBundle writer after selection.  Keeping
    ranking rows compact avoids retaining thousands of daily account paths in
    the coordinator process.
    """

    return {key: item for key, item in value.items() if key != "evidence_raw"}


def _scenario_episode_total(
    rows: Sequence[Mapping[str, Any]], scenario: str
) -> int:
    return sum(
        int(metrics["episode_count"])
        for row in rows
        for metrics in (row.get(scenario),)
        if isinstance(metrics, Mapping)
    )


def _portfolio_membership_rows(
    campaign_id: str,
    pairs: Sequence[BookPair],
    sleeves: Sequence[SleeveRecord],
) -> list[dict[str, Any]]:
    """Materialize a lossless, reconstructible Combine/XFA membership ledger."""

    by_id = {row.sleeve_id: row for row in sleeves}
    output: list[dict[str, Any]] = []
    for pair in pairs:
        combine = dict(
            zip(
                pair.combine_sleeve_ids,
                pair.combine_allocation_units,
                strict=True,
            )
        )
        xfa = dict(
            zip(pair.xfa_sleeve_ids, pair.xfa_allocation_units, strict=True)
        )
        exact_book = {
            "portfolio_membership_schema": "hydra_portfolio_membership_v1",
            "combine_book_sleeve_ids": list(pair.combine_sleeve_ids),
            "combine_book_allocation_units": {
                key: int(value) for key, value in combine.items()
            },
            "combine_risk_tier": float(pair.combine_risk_tier),
            "xfa_book_sleeve_ids": list(pair.xfa_sleeve_ids),
            "xfa_book_allocation_units": {
                key: int(value) for key, value in xfa.items()
            },
            "xfa_risk_tier": float(pair.xfa_risk_tier),
            "conflict_policy": pair.conflict_policy,
            "maximum_mini_equivalent": 15,
            "combine_maximum_simultaneous_positions": min(
                len(pair.combine_sleeve_ids), 6
            ),
            "xfa_maximum_simultaneous_positions": min(
                len(pair.xfa_sleeve_ids), 6
            ),
            "book_pair_structural_fingerprint": pair.structural_fingerprint,
        }
        for component_id in sorted(set(combine) | set(xfa)):
            combine_units = combine.get(component_id)
            xfa_units = xfa.get(component_id)
            roles = []
            if combine_units is not None:
                roles.append(f"COMBINE:{combine_units}")
            if xfa_units is not None:
                roles.append(f"XFA:{xfa_units}")
            output.append(
                {
                    "campaign_id": campaign_id,
                    "policy_id": pair.pair_id,
                    "component_id": component_id,
                    # Backward-compatible scalar only; exact mode-specific
                    # allocations below are the authoritative declaration.
                    "risk_allocation": float(
                        max(combine_units or 0, xfa_units or 0)
                    ),
                    "component_role": by_id[component_id].economic_role
                    + "|"
                    + "|".join(roles),
                    "combine_member": combine_units is not None,
                    "combine_allocation_units": (
                        None if combine_units is None else int(combine_units)
                    ),
                    "combine_effective_risk_multiplier": (
                        0.0
                        if combine_units is None
                        else float(combine_units) * float(pair.combine_risk_tier)
                    ),
                    "xfa_member": xfa_units is not None,
                    "xfa_allocation_units": (
                        None if xfa_units is None else int(xfa_units)
                    ),
                    "xfa_effective_risk_multiplier": (
                        0.0
                        if xfa_units is None
                        else float(xfa_units) * float(pair.xfa_risk_tier)
                    ),
                    **exact_book,
                }
            )
    return output


def _portfolio_provenance_checksums(
    identity: Mapping[str, Any],
    ledgers: Mapping[str, Sequence[Mapping[str, Any]]],
    sleeves: Sequence[SleeveRecord],
    *,
    campaign_id: str,
) -> dict[str, str]:
    """Separate inherited source-ledger hashes from current bundle hashes."""

    checksums = {
        "configuration": str(identity["configuration_sha256"]),
        **{
            f"data:{name}": str(digest)
            for name, digest in identity["data_fingerprints"].items()
        },
    }
    for sleeve in sleeves:
        component_id = sleeve.sleeve_id
        checksums[f"source:component_signals:{component_id}"] = (
            sleeve.signal_ledger_sha256
        )
        checksums[f"source:component_trades:{component_id}"] = (
            sleeve.trade_ledger_sha256
        )
        for dataset in (
            "component_signals",
            "component_entries",
            "component_exits",
            "component_trades",
        ):
            digest, count = _canonical_component_ledger_hash(
                dataset,
                ledgers[dataset],
                component_id,
                campaign_id,
            )
            if count <= 0:
                raise PortfolioRuntimeError(
                    f"bundle ledger lacks component evidence: {dataset}:{component_id}"
                )
            checksums[f"bundle:{dataset}:{component_id}"] = digest
    return dict(sorted(checksums.items()))


def _canonical_component_ledger_hash(
    dataset: str,
    rows: Sequence[Mapping[str, Any]],
    component_id: str,
    campaign_id: str,
) -> tuple[str, int]:
    """Reproduce the EvidenceBundle per-component canonical row hash."""

    spec = RECORD_SPECS[dataset]
    selected = [
        spec.validate(row, campaign_id=campaign_id)
        for row in rows
        if str(row.get("component_id") or "") == component_id
    ]
    selected.sort(
        key=lambda row: tuple(str(row[field]) for field in spec.sort_fields)
    )
    digest = hashlib.sha256()
    for row in selected:
        digest.update(
            (
                json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
        )
    return digest.hexdigest(), len(selected)


def _metrics_for_pairs(
    rows: Sequence[Mapping[str, Any]], pairs: Sequence[BookPair]
) -> list[dict[str, Any]]:
    by_id = {str(row["pair_id"]): dict(row) for row in rows}
    return [by_id[pair.pair_id] for pair in pairs]


def _metrics_excluding_pairs(
    rows: Sequence[Mapping[str, Any]], pairs: Sequence[BookPair]
) -> list[dict[str, Any]]:
    excluded = {pair.pair_id for pair in pairs}
    available = {str(row["pair_id"]) for row in rows}
    if not excluded.issubset(available):
        raise PortfolioRuntimeError("deferred portfolio metric identity drift")
    return [dict(row) for row in rows if str(row["pair_id"]) not in excluded]


def _assert_authoritative_episode_counters(
    economic_results: Mapping[str, Any], evidence_receipt: Mapping[str, Any]
) -> None:
    counters = economic_results.get("production_counters")
    dataset_counts = evidence_receipt.get("dataset_row_counts")
    if not isinstance(counters, Mapping) or not isinstance(dataset_counts, Mapping):
        raise PortfolioRuntimeError("portfolio result lacks authoritative counters")
    combine = int(counters.get("combine_episodes_completed", -1))
    normal = int(counters.get("normal_episodes_completed", -1))
    stressed = int(counters.get("stressed_episodes_completed", -1))
    persisted = int(dataset_counts.get("episodes", -1))
    if combine < 0 or combine != normal + stressed or combine != persisted:
        raise PortfolioRuntimeError(
            "portfolio counters diverge from persisted EvidenceBundle episodes"
        )


def _development_finalist_roles(
    finalists: Sequence[BookPair],
) -> list[dict[str, str]]:
    """Label ranked primaries and one observed-behavior-distinct backup."""

    count = len(finalists)
    primary_count = min(5, count - 1) if count >= 4 else count
    return [
        {
            "pair_id": row.pair_id,
            "role": (
                "PRIMARY_DEVELOPMENT_BOOK"
                if index < primary_count
                else "BEHAVIORALLY_DISTINCT_BACKUP"
            ),
        }
        for index, row in enumerate(finalists)
    ]


def _halving_stage_name(stage: str) -> str:
    names = {
        "stage1": "STAGE_1_FAST_SCREEN",
        "stage2": "STAGE_2_EXACT_ACCOUNT_REPLAY",
        "stage3": "STAGE_3_48_START_COMBINE_TO_XFA_LIFECYCLE",
        "stage4": "STAGE_4_EXPANDED_96_STARTS",
        "stage5": "STAGE_5_DEVELOPMENT_FINALISTS_192_STARTS",
    }
    try:
        return names[stage]
    except KeyError as exc:
        raise PortfolioRuntimeError(f"unknown portfolio halving stage: {stage}") from exc


def _episode_total(rows: Sequence[Mapping[str, Any]]) -> int:
    return sum(int(row.get("combine_episode_count", 0)) for row in rows)


def _remaining_chronological_calendars(
    runtimes: Mapping[str, ExactSleeveRuntime], starts: Sequence[int]
) -> dict[int, tuple[int, ...]]:
    values = list(runtimes.values())
    if not values:
        raise PortfolioRuntimeError("XFA chronology requires compiled sleeves")
    common = set(int(day) for day in values[0].eligible_session_days)
    for runtime in values[1:]:
        common.intersection_update(int(day) for day in runtime.eligible_session_days)
    ordered = tuple(sorted(common))
    output = {
        int(start): tuple(day for day in ordered if day >= int(start))
        for start in starts
    }
    if any(not days or int(start) not in days for start, days in output.items()):
        raise PortfolioRuntimeError("XFA chronology does not contain every frozen start")
    return output


def _block_for_day(day: int, manifest: Mapping[str, Any]) -> str:
    from datetime import date, timedelta

    value = (date(1970, 1, 1) + timedelta(days=int(day))).isoformat()
    for block in manifest["temporal_blocks"]["blocks"]:
        if str(block["start"]) <= value <= str(block["end"]):
            return str(block["block_id"])
    return "OUTSIDE_FROZEN_BLOCK"


def _quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


__all__ = ["PortfolioFirstRun", "PortfolioRuntimeError", "run_portfolio_first_manifest"]
