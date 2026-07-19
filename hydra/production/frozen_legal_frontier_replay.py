"""Exact read-only replay of the frozen legal-feasibility shortlist.

The autonomous director's legal-feasibility branch deliberately operated on
already-aggregated campaign-0029 episode summaries.  Its apparent passes were
therefore only a shortlist, never exact account evidence.  This module closes
that narrow evidence gap for the three preregistered cells by rebuilding the
underlying causal component trajectories and running the authoritative shared
account simulator under the official 50K snapshot.

The runner is intentionally isolated from the persistent controller.  It
performs no writes, assigns no promotion, consumes no protected data and has no
order capability.  Its return value is deterministic JSON and contains the
episode records expected by the existing EvidenceBundle adapter.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _market_contract_limit_mini,
    _read_verified_event_evidence,
    _require_scenario_identity,
    _summarize_exact_episodes,
)
from hydra.production.causal_risk_preflight import scale_causal_trajectory
from hydra.production.fast_pass_runtime_helpers import _quality_trajectories
from hydra.research.causal_target_velocity import HazardOutcome


SCHEMA = "hydra_frozen_legal_frontier_exact_replay_v1"
SOURCE_CAMPAIGN_ID = "hydra_fast_pass_factory_0029"
SOURCE_WAVE = 1
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
DEFAULT_BANK_PATH = Path(
    "data/cache/economic_production/hydra_fast_pass_factory_0029/"
    "wave_01/causal_executable_bank.json"
)
DEFAULT_PROPOSALS_PATH = Path(
    "data/cache/economic_production/hydra_fast_pass_factory_0029/"
    "wave_01/marginal_book_proposals.jsonl"
)
DEFAULT_LEGAL_FEASIBILITY_PATH = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/legal_feasibility.json"
)


class FrozenLegalFrontierReplayError(RuntimeError):
    """A frozen source, causal reconstruction or exact account invariant drifted."""


@dataclass(frozen=True, slots=True)
class FrozenLegalCell:
    policy_id: str
    book_id: str
    selection_horizon_trading_days: int
    uniform_quantity_scale: int
    sprint_batch_path: str
    source_result_hash: str
    source_episode_sha256: str
    proposal_structural_fingerprint: str

    @property
    def exact_policy_id(self) -> str:
        return (
            f"exact_legal:{self.policy_id}:50K:"
            f"{self.uniform_quantity_scale}x"
        )


FROZEN_CELLS: tuple[FrozenLegalCell, ...] = (
    FrozenLegalCell(
        policy_id="fast_book_477b40613795d1d45557a83a:w1",
        book_id="fast_book_477b40613795d1d45557a83a",
        selection_horizon_trading_days=20,
        uniform_quantity_scale=3,
        sprint_batch_path=(
            "data/cache/economic_production/hydra_fast_pass_factory_0029/"
            "wave_01/books_sprint_batches/batch_0071.jsonl"
        ),
        source_result_hash=(
            "fbc636e255f59e33f403dba8fe51cf9d410d44bc833f757299d400318fb441d5"
        ),
        source_episode_sha256=(
            "ea77ffc287a89ccf2ca0cc02aed01a0c596e3d21ca4052a44151ee97ccc1f246"
        ),
        proposal_structural_fingerprint=(
            "477b40613795d1d45557a83a0a02295e6bfa9ea2586b6c942447e088b034fec1"
        ),
    ),
    FrozenLegalCell(
        policy_id="fast_book_7bbfd61968d4122c8bb75f64:w1",
        book_id="fast_book_7bbfd61968d4122c8bb75f64",
        selection_horizon_trading_days=10,
        uniform_quantity_scale=6,
        sprint_batch_path=(
            "data/cache/economic_production/hydra_fast_pass_factory_0029/"
            "wave_01/books_sprint_batches/batch_0234.jsonl"
        ),
        source_result_hash=(
            "fc9477ab10982b3eb4c6a62362a393babc4c59349f049cc669c13494202ba4c3"
        ),
        source_episode_sha256=(
            "e1a323489d6a886a79b37825faacb330f77a88dee15b4bdde7eed733eb7dcd89"
        ),
        proposal_structural_fingerprint=(
            "7bbfd61968d4122c8bb75f64a1f2eaeecfb0bf004d9ab82cbb4d75bb9ed68af3"
        ),
    ),
    FrozenLegalCell(
        policy_id="fast_book_bfc9dcbdfb4e4d134b46f0e6:w1",
        book_id="fast_book_bfc9dcbdfb4e4d134b46f0e6",
        selection_horizon_trading_days=10,
        uniform_quantity_scale=8,
        sprint_batch_path=(
            "data/cache/economic_production/hydra_fast_pass_factory_0029/"
            "wave_01/books_sprint_batches/batch_0241.jsonl"
        ),
        source_result_hash=(
            "fa2207a9f36eb29b5206db88f45918e7748b5f614bd5d64db6648d6c23945625"
        ),
        source_episode_sha256=(
            "fc24833981646a6bdcca98ef4da39222548c8ccca3bb879cd557573599427aa1"
        ),
        proposal_structural_fingerprint=(
            "bfc9dcbdfb4e4d134b46f0e68dc4551e5bb98436767946250c8aada5169a0b5d"
        ),
    ),
)


def run_frozen_legal_frontier_exact_replay(
    root: str | Path,
    *,
    fast_pass_manifest_path: str | Path = DEFAULT_FAST_PASS_MANIFEST,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
    legal_feasibility_path: str | Path = DEFAULT_LEGAL_FEASIBILITY_PATH,
) -> dict[str, Any]:
    """Rebuild and exactly replay the three frozen cells without writing.

    The uniform scale is applied to the immutable quality-quantized causal
    trajectories, never to PnL summaries.  A conservative static stop-risk
    charge is derived from the source event ledger for each component.  The
    account router either admits the requested whole quantity unchanged or
    rejects it; silent clipping would make the frozen cell a different policy.
    """

    project = Path(root).resolve()
    manifest_path = _inside(project, fast_pass_manifest_path)
    rules_path = _inside(project, rule_snapshot_path)
    legal_path = _inside(project, legal_feasibility_path)
    bank_path = _inside(project, DEFAULT_BANK_PATH)
    proposals_path = _inside(project, DEFAULT_PROPOSALS_PATH)

    manifest = _load_self_hashed_manifest(manifest_path)
    if str(manifest.get("campaign_id")) != SOURCE_CAMPAIGN_ID:
        raise FrozenLegalFrontierReplayError("FAST-PASS campaign identity drift")
    rules, rule_receipt = _load_rule_snapshot(rules_path)
    rule = dict(rules["50K"])
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    bank_entries, bank_receipt = _load_wave_one_bank(bank_path)
    bank_by_id = {str(row["candidate_id"]): row for row in bank_entries}
    proposals, proposals_receipt = _load_proposals(proposals_path)
    legal, legal_receipt = _load_legal_feasibility(legal_path)
    bank_root = _directory_inside(
        project,
        "data/cache/economic_production/hydra_fast_pass_factory_0029",
    )

    results: list[dict[str, Any]] = []
    evaluated_records: list[dict[str, Any]] = []
    total_source_events = 0
    total_exact_replays = 0

    for frozen in FROZEN_CELLS:
        source, source_receipt = _load_source_book_result(project, frozen)
        proposal = _one(
            [row for row in proposals if str(row.get("book_id")) == frozen.book_id],
            f"proposal for {frozen.book_id}",
        )
        _verify_proposal(frozen, proposal, source)
        summary_anchor = _legal_frontier_anchor(legal, frozen)
        legal_episode_path = str(
            _inside(
                project,
                str(source["episode_evidence"]["relative_path"]),
                base=bank_root,
            )
        )
        claimed_legal_sha = str(
            dict(legal.get("source_file_hashes") or {}).get(legal_episode_path, "")
        )
        if claimed_legal_sha != frozen.source_episode_sha256:
            raise FrozenLegalFrontierReplayError(
                f"legal-feasibility source SHA drift: {frozen.policy_id}"
            )

        component_ids = tuple(str(value) for value in source["component_ids"])
        quality = {
            str(key): float(value)
            for key, value in dict(source["quality_multipliers"]).items()
        }
        if set(component_ids) != set(quality):
            raise FrozenLegalFrontierReplayError("book quality inventory drift")
        scenario_inputs: dict[str, dict[str, tuple[Any, ...]]] = {
            scenario: {} for scenario in SCENARIOS
        }
        source_quality_inputs: dict[str, dict[str, tuple[Any, ...]]] = {
            scenario: {} for scenario in SCENARIOS
        }
        component_receipts: dict[str, Any] = {}
        candidate_fingerprints: dict[str, str] = {}
        eligible_days_by_component: dict[str, frozenset[int]] = {}
        censored_days_by_component: dict[str, frozenset[int]] = {}
        risk_charges: list[tuple[str, float]] = []
        maximum_component_mini: dict[str, float] = {}
        session_violations_by_component: dict[str, int] = {}
        market_cap_breaches_by_component: dict[str, int] = {}

        for component_id in component_ids:
            entry = bank_by_id.get(component_id)
            if entry is None:
                raise FrozenLegalFrontierReplayError(
                    f"frozen book component absent from wave-1 bank: {component_id}"
                )
            events, event_receipt = _read_verified_event_evidence(
                project, dict(entry["event_evidence"])
            )
            replay = reconstruct_exact_hazard_replay(
                candidate_payload=entry["candidate"],
                event_mappings=events,
                eligible_session_days=entry["eligible_session_days"],
                expected_hashes=entry["exact_hashes"],
            )
            if str(replay.candidate.candidate_id) != component_id:
                raise FrozenLegalFrontierReplayError(
                    f"component reconstruction identity drift: {component_id}"
                )
            normal, normal_quality = _quality_trajectories(
                replay.normal_trajectories, quality[component_id]
            )
            stressed, stressed_quality = _quality_trajectories(
                replay.stressed_trajectories, quality[component_id]
            )
            if normal_quality != stressed_quality:
                raise FrozenLegalFrontierReplayError(
                    f"normal/stressed quality decision drift: {component_id}"
                )
            if normal_quality != dict(source["quality_scaling"][component_id]):
                raise FrozenLegalFrontierReplayError(
                    f"frozen source quality receipt drift: {component_id}"
                )
            source_quality_inputs["NORMAL"][component_id] = normal
            source_quality_inputs["STRESSED_1_5X"][component_id] = stressed
            normal_checked, normal_violations = _apply_session_contract(normal)
            stressed_checked, stressed_violations = _apply_session_contract(stressed)
            if normal_violations != stressed_violations:
                raise FrozenLegalFrontierReplayError(
                    f"normal/stressed session-contract drift: {component_id}"
                )
            session_violations_by_component[component_id] = normal_violations
            _require_scenario_identity(normal_checked, stressed_checked)
            factor = frozen.uniform_quantity_scale
            scaled_normal = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=factor)
                for row in normal_checked
            )
            scaled_stressed = tuple(
                scale_causal_trajectory(row, executable_quantity_multiplier=factor)
                for row in stressed_checked
            )
            _require_scenario_identity(scaled_normal, scaled_stressed)
            component_limit = _market_contract_limit_mini(entry["candidate"], rule)
            observed_maximum = max(
                (float(row.event.mini_equivalent) for row in scaled_normal),
                default=0.0,
            )
            market_cap_breaches_by_component[component_id] = sum(
                float(row.event.mini_equivalent) > component_limit + 1e-12
                for row in scaled_normal
            )
            scenario_inputs["NORMAL"][component_id] = scaled_normal
            scenario_inputs["STRESSED_1_5X"][component_id] = scaled_stressed
            risk_charges.append(
                (
                    component_id,
                    float(
                        _declared_stop_risk_charge_per_mini(
                            events, entry["candidate"]
                        )
                    ),
                )
            )
            eligible_days_by_component[component_id] = frozenset(
                int(value) for value in replay.eligible_session_days
            )
            censored_days_by_component[component_id] = frozenset(
                int(row.session_day)
                for row in replay.events
                if str(getattr(row.outcome, "value", row.outcome))
                == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
            )
            candidate_fingerprints[component_id] = str(
                entry["candidate_fingerprint"]
            )
            maximum_component_mini[component_id] = observed_maximum
            component_receipts[component_id] = {
                "candidate_fingerprint": str(entry["candidate_fingerprint"]),
                "candidate": dict(entry["candidate"]),
                "event_evidence": event_receipt,
                "exact_hashes": dict(entry["exact_hashes"]),
                "quality_scaling": normal_quality,
                "declared_stop_risk_charge_per_mini_usd": risk_charges[-1][1],
                "official_market_contract_limit_mini_equivalent": component_limit,
                "maximum_scaled_mini_equivalent": observed_maximum,
                "session_contract_violation_count": normal_violations,
            }
            total_source_events += len(events)

        _verify_source_trajectory_hashes(source, source_quality_inputs, frozen)
        coverage = _book_coverage(
            calendar,
            starts,
            eligible_days_by_component,
            censored_days_by_component,
        )
        _verify_source_coverage(source, coverage, frozen)
        profile = _profile_for_source(manifest, source)
        policy = _uniform_legal_policy(
            frozen=frozen,
            component_ids=component_ids,
            risk_charges=tuple(risk_charges),
            profile=profile,
            rule=rule,
        )

        horizon_results: dict[str, Any] = {}
        cell_records: list[dict[str, Any]] = []
        for horizon in HORIZONS:
            episodes: dict[str, list[tuple[Any, str]]] = {
                scenario: [] for scenario in SCENARIOS
            }
            for start_day, block in coverage["starts"][horizon]:
                for scenario in SCENARIOS:
                    episode = run_causal_shared_account_episode(
                        scenario_inputs[scenario],
                        calendar,
                        policy=policy,
                        start_day=int(start_day),
                        maximum_duration_days=int(horizon),
                        config=_account_config(rule),
                    )
                    episodes[scenario].append((episode, block))
                    record = {
                        "policy_id": frozen.exact_policy_id,
                        "source_policy_id": frozen.policy_id,
                        "episode_id": (
                            f"{frozen.exact_policy_id}:{horizon}:{int(start_day)}"
                        ),
                        "scenario": scenario,
                        "cost_scenario": scenario,
                        "horizon": f"{horizon}_TRADING_DAYS",
                        "horizon_trading_days": int(horizon),
                        "requested_duration_trading_days": int(horizon),
                        "temporal_block": str(block),
                        "start_day": int(start_day),
                        "coverage_state": "FULL_COVERAGE",
                        "episode": episode.to_dict(include_paths=True),
                    }
                    record["record_hash"] = stable_hash(record)
                    cell_records.append(record)
                    total_exact_replays += 1
            summaries = {
                scenario: _summarize_exact_episodes(episodes[scenario])
                for scenario in SCENARIOS
            }
            horizon_results[str(horizon)] = {
                "horizon_trading_days": int(horizon),
                "requested_start_count": len(starts[horizon]),
                "full_coverage_start_count": len(coverage["starts"][horizon]),
                "data_censored_start_count": int(coverage["censored"][horizon]),
                "normal": summaries["NORMAL"],
                "stressed": summaries["STRESSED_1_5X"],
            }

        selected = horizon_results[str(frozen.selection_horizon_trading_days)]
        comparison = _anchor_comparison(summary_anchor, selected)
        hard_execution_contract_clean = bool(
            sum(session_violations_by_component.values()) == 0
            and sum(market_cap_breaches_by_component.values()) == 0
        )
        exact_scale_contract_satisfied = hard_execution_contract_clean and all(
            int(summary["size_reduced_count"]) == 0
            and int(summary["risk_or_contract_rejection_count"]) == 0
            for value in horizon_results.values()
            for summary in (value["normal"], value["stressed"])
        )
        cell_core = {
            "source_policy_id": frozen.policy_id,
            "exact_policy_id": frozen.exact_policy_id,
            "account_label": "50K",
            "account_size_usd": int(rule["account_size_usd"]),
            "selection_horizon_trading_days": (
                frozen.selection_horizon_trading_days
            ),
            "uniform_quantity_scale": frozen.uniform_quantity_scale,
            "component_ids": list(component_ids),
            "candidate_fingerprints": candidate_fingerprints,
            "source_quality_multipliers": quality,
            "source_governor_profile_id": str(source["governor_profile_id"]),
            "exact_uniform_policy": policy.to_dict(),
            "risk_contract": {
                "name": "STATIC_MAX_DECLARED_STOP_RISK_PRIORITY_ADMISSION_V1",
                "whole_quantity_only": True,
                "silent_size_reduction_allowed": False,
                "aggregate_open_risk_ceiling_usd": float(
                    rule["maximum_loss_limit_usd"]
                ),
                "maximum_mll_buffer_fraction": 1.0,
                "inactive_sleeves_reserve_risk": False,
                "future_outcome_fields_used": False,
            },
            "component_provenance": component_receipts,
            "maximum_scaled_mini_equivalent_by_component": (
                maximum_component_mini
            ),
            "session_contract_violation_count_by_component": (
                session_violations_by_component
            ),
            "official_market_contract_cap_breach_count_by_component": (
                market_cap_breaches_by_component
            ),
            "scaled_scenario_trajectory_hashes": {
                scenario: stable_hash(
                    {
                        component_id: [row.to_dict() for row in trajectories]
                        for component_id, trajectories in values.items()
                    }
                )
                for scenario, values in scenario_inputs.items()
            },
            "coverage": {
                "full_coverage_start_counts": {
                    str(horizon): len(coverage["starts"][horizon])
                    for horizon in HORIZONS
                },
                "data_censored_start_counts": {
                    str(horizon): int(coverage["censored"][horizon])
                    for horizon in HORIZONS
                },
            },
            "source_artifacts": source_receipt,
            "legal_summary_anchor": summary_anchor,
            "horizon_results": horizon_results,
            "selection_horizon_summary_delta": comparison,
            "exact_scale_contract_satisfied": exact_scale_contract_satisfied,
            "hard_execution_contract_clean": hard_execution_contract_clean,
            "source_summary_pass_claim_promotable": False,
            "exact_development_pass_observed": bool(
                int(selected["normal"]["pass_count"]) > 0
                or int(selected["stressed"]["pass_count"]) > 0
            ),
            "admissible_exact_development_pass_observed": bool(
                hard_execution_contract_clean
                and (
                    int(selected["normal"]["pass_count"]) > 0
                    or int(selected["stressed"]["pass_count"]) > 0
                )
            ),
            "promotion_status": None,
            "evidence_tier": "E_EXACT_DEVELOPMENT_DIAGNOSTIC",
        }
        cell_core["episode_records_hash"] = stable_hash(cell_records)
        cell_core["cell_result_hash"] = stable_hash(cell_core)
        results.append(cell_core)
        evaluated_records.extend(cell_records)

    evidence_bundle_adapter = {
        "schema": "hydra_evidence_bundle_adapter_input_v1",
        "campaign_id": "hydra_frozen_legal_frontier_exact_replay",
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "policies": {
            row["exact_policy_id"]: row["exact_uniform_policy"] for row in results
        },
        "policy_fingerprints": {
            row["exact_policy_id"]: stable_hash(row["exact_uniform_policy"])
            for row in results
        },
        "component_fingerprints": dict(
            sorted(
                {
                    key: value
                    for row in results
                    for key, value in row["candidate_fingerprints"].items()
                }.items()
            )
        ),
        "evaluated_policy_records": evaluated_records,
        "component_reconstruction_source": {
            "campaign_id": SOURCE_CAMPAIGN_ID,
            "bank_file_sha256": bank_receipt["file_sha256"],
            "source_ledgers_referenced_immutably": True,
            "signals_or_trades_recomputed_from_market_data": False,
        },
        "adapter_compatibility": (
            "CAUSAL_TARGET_VELOCITY_EVIDENCE_EPISODE_RECORD_V1"
        ),
        "sealing_performed": False,
        "authoritative_writer_required_for_sealing": True,
    }
    evidence_bundle_adapter["adapter_payload_hash"] = stable_hash(
        evidence_bundle_adapter
    )

    diagnostic_exact_passes = sum(
        int(value[scenario]["pass_count"])
        for row in results
        for value in row["horizon_results"].values()
        for scenario in ("normal", "stressed")
    )
    admissible_exact_passes = sum(
        int(value[scenario]["pass_count"])
        for row in results
        if row["hard_execution_contract_clean"]
        for value in row["horizon_results"].values()
        for scenario in ("normal", "stressed")
    )
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_EXACT_FROZEN_LEGAL_FRONTIER_REPLAY",
        "source_campaign_id": SOURCE_CAMPAIGN_ID,
        "source_commit": str(legal.get("source_commit") or ""),
        "runner_repository_head": _git_head(project),
        "runner_source_sha256": _sha256(Path(__file__)),
        "source_manifest": {
            "path": str(manifest_path),
            "file_sha256": _sha256(manifest_path),
            "manifest_hash": str(manifest["manifest_hash"]),
        },
        "official_rule_snapshot": rule_receipt,
        "frozen_grid": grid_receipt,
        "source_bank": bank_receipt,
        "source_proposals": proposals_receipt,
        "source_legal_feasibility": legal_receipt,
        "frozen_cell_contract_hash": stable_hash(
            [
                {
                    "policy_id": row.policy_id,
                    "account_label": "50K",
                    "selection_horizon_trading_days": (
                        row.selection_horizon_trading_days
                    ),
                    "uniform_quantity_scale": row.uniform_quantity_scale,
                }
                for row in FROZEN_CELLS
            ]
        ),
        "results": results,
        "evidence_bundle_adapter": evidence_bundle_adapter,
        "counters": {
            "frozen_cell_count": len(results),
            "unique_component_count": len(
                {
                    value
                    for row in results
                    for value in row["component_ids"]
                }
            ),
            "source_event_rows_reconstructed": total_source_events,
            "exact_account_replays": total_exact_replays,
            "diagnostic_exact_passes_all_horizons_and_scenarios": (
                diagnostic_exact_passes
            ),
            "admissible_exact_passes_all_horizons_and_scenarios": (
                admissible_exact_passes
            ),
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "authoritative_writes": 0,
        },
        "decision": (
            "EXACT_PASS_OBSERVED_REQUIRES_FROZEN_CHRONOLOGICAL_VALIDATION"
            if admissible_exact_passes
            else "SUMMARY_LEGAL_FRONTIER_PASS_SIGNAL_NOT_CONFIRMED_EXACTLY"
        ),
        "evidence_tier": "E",
        "promotion_status": None,
        "read_only_worker": True,
        "interpretation_boundary": (
            "Viewed development evidence only. Summary-scaled passes carry no "
            "status; only the exact causal replay is economically admissible."
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def frozen_legal_frontier_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe read-only worker adapter."""

    return run_frozen_legal_frontier_exact_replay(
        str(payload["root"]),
        fast_pass_manifest_path=str(
            payload.get("fast_pass_manifest_path", DEFAULT_FAST_PASS_MANIFEST)
        ),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
        ),
        legal_feasibility_path=str(
            payload.get("legal_feasibility_path", DEFAULT_LEGAL_FEASIBILITY_PATH)
        ),
    )


def _load_wave_one_bank(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = _read_json(path)
    rows = [dict(row) for row in payload.get("entries") or ()]
    if (
        payload.get("schema") != "hydra_fast_pass_qd_bank_v1"
        or payload.get("campaign_id") != SOURCE_CAMPAIGN_ID
        or int(payload.get("wave", -1)) != SOURCE_WAVE
        or stable_hash(rows) != str(payload.get("bank_hash") or "")
    ):
        raise FrozenLegalFrontierReplayError("wave-1 causal bank identity drift")
    ids = [str(row.get("candidate_id") or "") for row in rows]
    if any(not value for value in ids) or len(ids) != len(set(ids)):
        raise FrozenLegalFrontierReplayError("wave-1 causal bank IDs are invalid")
    return rows, {
        "path": str(path),
        "file_sha256": _sha256(path),
        "bank_hash": str(payload["bank_hash"]),
        "entry_count": len(rows),
    }


def _load_proposals(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = _read_jsonl(path)
    if any(
        row.get("schema") != "hydra_fast_pass_marginal_book_proposal_v1"
        or int(row.get("wave", -1)) != SOURCE_WAVE
        for row in rows
    ):
        raise FrozenLegalFrontierReplayError("marginal proposal schema drift")
    return rows, {
        "path": str(path),
        "file_sha256": _sha256(path),
        "record_count": len(rows),
        "records_hash": stable_hash(rows),
    }


def _load_legal_feasibility(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path)
    if (
        payload.get("status") != "COMPLETE_BOUNDED_SUMMARY_FEASIBILITY_SCREEN"
        or payload.get("accounting_scope")
        != "AGGREGATED_SUMMARY_TRANSFORMATION_NOT_EXACT_REPLAY"
        or payload.get("exact_account_replay_required_before_any_PASS_claim")
        is not True
    ):
        raise FrozenLegalFrontierReplayError("legal-feasibility boundary drift")
    return payload, {
        "path": str(path),
        "file_sha256": _sha256(path),
        "result_hash": str(payload.get("result_hash") or ""),
        "accounting_scope": str(payload["accounting_scope"]),
        "exact_account_replay_required_before_any_PASS_claim": True,
    }


def _load_source_book_result(
    project: Path, frozen: FrozenLegalCell
) -> tuple[dict[str, Any], dict[str, Any]]:
    path = _inside(project, frozen.sprint_batch_path)
    source = _one(
        [row for row in _read_jsonl(path) if row.get("policy_id") == frozen.policy_id],
        f"source sprint result for {frozen.policy_id}",
    )
    claimed = str(source.get("result_hash") or "")
    hash_payload = {
        key: value
        for key, value in source.items()
        if key not in {"result_hash", "episode_evidence"}
    }
    if (
        source.get("schema") != "hydra_fast_pass_sprint_result_v1"
        or claimed != frozen.source_result_hash
        or stable_hash(hash_payload) != claimed
    ):
        raise FrozenLegalFrontierReplayError(
            f"source sprint result hash drift: {frozen.policy_id}"
        )
    episode_receipt = dict(source.get("episode_evidence") or {})
    episode_path = _inside(
        project,
        str(episode_receipt.get("relative_path") or ""),
        base=_directory_inside(
            project,
            "data/cache/economic_production/hydra_fast_pass_factory_0029",
        ),
    )
    compressed = episode_path.read_bytes()
    if (
        hashlib.sha256(compressed).hexdigest() != frozen.source_episode_sha256
        or str(episode_receipt.get("sha256")) != frozen.source_episode_sha256
    ):
        raise FrozenLegalFrontierReplayError(
            f"source book episode SHA drift: {frozen.policy_id}"
        )
    raw = gzip.decompress(compressed)
    rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]
    if (
        len(rows) != int(episode_receipt.get("record_count", -1))
        or hashlib.sha256(raw).hexdigest()
        != str(episode_receipt.get("uncompressed_sha256"))
    ):
        raise FrozenLegalFrontierReplayError(
            f"source book episode ledger drift: {frozen.policy_id}"
        )
    return source, {
        "sprint_batch_path": str(path),
        "sprint_batch_file_sha256": _sha256(path),
        "source_result_hash": claimed,
        "episode_evidence": {
            "path": str(episode_path),
            "sha256": frozen.source_episode_sha256,
            "uncompressed_sha256": hashlib.sha256(raw).hexdigest(),
            "record_count": len(rows),
            "records_hash": stable_hash(rows),
        },
    }


def _verify_proposal(
    frozen: FrozenLegalCell,
    proposal: Mapping[str, Any],
    source: Mapping[str, Any],
) -> None:
    if (
        str(proposal.get("structural_fingerprint"))
        != frozen.proposal_structural_fingerprint
        or tuple(str(value) for value in proposal.get("sleeve_priority") or ())
        != tuple(str(value) for value in source.get("component_ids") or ())
        or str(proposal.get("governor_profile_id"))
        != str(source.get("governor_profile_id"))
    ):
        raise FrozenLegalFrontierReplayError(
            f"book proposal/source identity drift: {frozen.policy_id}"
        )


def _legal_frontier_anchor(
    legal: Mapping[str, Any], frozen: FrozenLegalCell
) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in legal.get("uniform_legal_frontier") or ()
        if str(row.get("policy_id")) == frozen.policy_id
        and int(row.get("account_size_usd", -1)) == 50_000
        and int(row.get("horizon_trading_days", -1))
        == frozen.selection_horizon_trading_days
        and float(row.get("scale_factor", -1.0))
        == float(frozen.uniform_quantity_scale)
    ]
    by_scenario = {str(row.get("scenario")): row for row in rows}
    if len(rows) != 2 or set(by_scenario) != set(SCENARIOS):
        raise FrozenLegalFrontierReplayError(
            f"frozen legal-frontier cell is absent or duplicated: {frozen.policy_id}"
        )
    if any(row.get("legally_executable") is not True for row in rows):
        raise FrozenLegalFrontierReplayError("frozen frontier anchor is not executable")
    return {
        "accounting_scope": "AGGREGATED_SUMMARY_TRANSFORMATION_NOT_EXACT_REPLAY",
        "promotable": False,
        "normal": by_scenario["NORMAL"],
        "stressed": by_scenario["STRESSED_1_5X"],
        "anchor_hash": stable_hash(by_scenario),
    }


def _verify_source_trajectory_hashes(
    source: Mapping[str, Any],
    source_inputs: Mapping[str, Mapping[str, Sequence[Any]]],
    frozen: FrozenLegalCell,
) -> None:
    source_hashes = dict(source.get("scenario_trajectory_hashes") or {})
    if set(source_hashes) != set(SCENARIOS):
        raise FrozenLegalFrontierReplayError(
            f"source aggregate trajectory hash inventory drift: {frozen.policy_id}"
        )
    for scenario in SCENARIOS:
        if not source_inputs[scenario]:
            raise FrozenLegalFrontierReplayError("empty scaled trajectory inventory")
        observed = stable_hash(
            {
                component_id: [row.to_dict() for row in trajectories]
                for component_id, trajectories in source_inputs[scenario].items()
            }
        )
        if observed != str(source_hashes[scenario]):
            raise FrozenLegalFrontierReplayError(
                f"source aggregate trajectory hash drift: "
                f"{frozen.policy_id}:{scenario}"
            )


def _book_coverage(
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    eligible: Mapping[str, frozenset[int]],
    censored: Mapping[str, frozenset[int]],
) -> dict[str, Any]:
    calendar_index = {int(day): index for index, day in enumerate(calendar)}
    missing_days = set().union(
        *(set(calendar) - set(values) for values in eligible.values())
    )
    censored_days = missing_days | set().union(*(set(value) for value in censored.values()))
    accepted: dict[int, tuple[tuple[int, str], ...]] = {}
    rejected: dict[int, int] = {}
    for horizon in HORIZONS:
        values: list[tuple[int, str]] = []
        count = 0
        for start_day, block in starts[horizon]:
            index = calendar_index.get(int(start_day))
            if index is None:
                raise FrozenLegalFrontierReplayError("frozen start absent from calendar")
            window = tuple(calendar[index : index + horizon])
            if len(window) != horizon or any(day in censored_days for day in window):
                count += 1
                continue
            values.append((int(start_day), str(block)))
        accepted[horizon] = tuple(values)
        rejected[horizon] = count
    return {"starts": accepted, "censored": rejected}


def _verify_source_coverage(
    source: Mapping[str, Any], coverage: Mapping[str, Any], frozen: FrozenLegalCell
) -> None:
    for scenario in SCENARIOS:
        for horizon in HORIZONS:
            summary = dict(source["summaries"][scenario][str(horizon)])
            if (
                int(summary["episode_count"])
                != len(coverage["starts"][horizon])
                or int(summary["data_censored_count"])
                != int(coverage["censored"][horizon])
            ):
                raise FrozenLegalFrontierReplayError(
                    f"source/full-coverage reconciliation drift: "
                    f"{frozen.policy_id}:{scenario}:{horizon}"
                )


def _profile_for_source(
    manifest: Mapping[str, Any], source: Mapping[str, Any]
) -> dict[str, Any]:
    profile_id = str(source["governor_profile_id"])
    return _one(
        [
            dict(row)
            for row in manifest["risk_governor"]["frozen_profiles"]
            if str(row.get("profile_id")) == profile_id
        ],
        f"governor profile {profile_id}",
    )


def _uniform_legal_policy(
    *,
    frozen: FrozenLegalCell,
    component_ids: Sequence[str],
    risk_charges: tuple[tuple[str, float], ...],
    profile: Mapping[str, Any],
    rule: Mapping[str, Any],
) -> ActiveRiskPoolPolicy:
    if tuple(value for value, _charge in risk_charges) != tuple(component_ids):
        raise FrozenLegalFrontierReplayError("risk-charge priority drift")
    conflict = str(profile["same_instrument_conflict_policy"])
    if conflict == "priority":
        conflict_rule = SameInstrumentConflictRule.PRIORITY
    elif conflict == "reject_both":
        conflict_rule = SameInstrumentConflictRule.REJECT_BOTH
    else:
        raise FrozenLegalFrontierReplayError("unsupported frozen conflict policy")
    mll = float(rule["maximum_loss_limit_usd"])
    target = float(rule["profit_target_usd"])
    return ActiveRiskPoolPolicy(
        policy_id=frozen.exact_policy_id,
        component_priority=tuple(component_ids),
        nominal_risk_charge_per_mini=risk_charges,
        maximum_concurrent_sleeves=min(
            int(profile["maximum_concurrent_sleeves"]), len(component_ids)
        ),
        aggregate_open_risk_ceiling=mll,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
        concurrency_scaling=ConcurrencyScaling.PRIORITY,
        same_instrument_conflict_rule=conflict_rule,
        daily_loss_guard=mll,
        daily_consistency_profit_guard=target,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _anchor_comparison(
    anchor: Mapping[str, Any], selected: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for scenario, exact_key in (("NORMAL", "normal"), ("STRESSED_1_5X", "stressed")):
        screened = dict(anchor[exact_key])
        exact = dict(selected[exact_key])
        output[scenario] = {
            "screened_pass_count": int(screened["passes"]),
            "exact_pass_count": int(exact["pass_count"]),
            "pass_count_delta_exact_minus_screen": (
                int(exact["pass_count"]) - int(screened["passes"])
            ),
            "screened_pass_rate": float(screened["pass_rate"]),
            "exact_pass_rate": float(exact["pass_rate"]),
            "pass_rate_delta_exact_minus_screen": (
                float(exact["pass_rate"]) - float(screened["pass_rate"])
            ),
            "screened_mll_breach_rate": float(screened["mll_breach_rate"]),
            "exact_mll_breach_rate": float(exact["mll_breach_rate"]),
            "screened_median_target_progress": float(
                screened["median_target_progress"]
            ),
            "exact_median_target_progress": float(
                exact["target_progress_median"]
            ),
        }
    output["comparison_hash"] = stable_hash(output)
    return output


def _inside(project: Path, value: str | Path, *, base: Path | None = None) -> Path:
    root = project.resolve()
    raw = Path(value)
    anchor = (base or root).resolve()
    path = (raw if raw.is_absolute() else anchor / raw).resolve()
    if path != root and root not in path.parents:
        raise FrozenLegalFrontierReplayError("source path escapes project root")
    if not path.is_file():
        raise FrozenLegalFrontierReplayError(f"required source file is absent: {path}")
    return path


def _directory_inside(project: Path, value: str | Path) -> Path:
    root = project.resolve()
    raw = Path(value)
    path = (raw if raw.is_absolute() else root / raw).resolve()
    if path != root and root not in path.parents:
        raise FrozenLegalFrontierReplayError("source directory escapes project root")
    if not path.is_dir():
        raise FrozenLegalFrontierReplayError(
            f"required source directory is absent: {path}"
        )
    return path


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise FrozenLegalFrontierReplayError(f"JSON root is not an object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    if any(not isinstance(row, dict) for row in rows):
        raise FrozenLegalFrontierReplayError(f"JSONL row is not an object: {path}")
    return rows


def _one(values: Sequence[dict[str, Any]], label: str) -> dict[str, Any]:
    if len(values) != 1:
        raise FrozenLegalFrontierReplayError(
            f"{label} must resolve exactly once, observed {len(values)}"
        )
    return dict(values[0])


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_head(project: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=project,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise FrozenLegalFrontierReplayError("cannot resolve repository HEAD") from exc


__all__ = [
    "FROZEN_CELLS",
    "FrozenLegalFrontierReplayError",
    "SCHEMA",
    "frozen_legal_frontier_worker",
    "run_frozen_legal_frontier_exact_replay",
]
