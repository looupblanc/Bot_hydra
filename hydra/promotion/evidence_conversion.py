from __future__ import annotations

import hashlib
import json
import math
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.compute.result_writer import AtomicResultWriter
from hydra.features.feature_matrix import FeatureMatrix
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.portfolio.account_contribution import (
    compare_account_contribution,
    matched_random_inclusion_controls,
    replay_shared_account,
)
from hydra.portfolio.strategy_role import StrategyPool
from hydra.promotion.behavioral_clustering import cluster_candidates
from hydra.promotion.evidence_debt import build_evidence_debt_record
from hydra.propfirm.intraday_mll import conservative_intraday_mll_audit
from hydra.propfirm.topstep_150k import Topstep150KConfig, simulate_combine
from hydra.propfirm.xfa_consistency import simulate_xfa_consistency
from hydra.propfirm.xfa_standard import simulate_xfa_standard
from hydra.research.qd_economic_tournament import (
    MARKET_PAIRS,
    _benjamini_hochberg,
    _round_turn_cost_all,
)
from hydra.research.turbo_exact_replay import (
    _array_compare,
    _day,
    _non_overlapping,
    spec_from_dict,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.strategies.turbo_dsl import ComparisonOperator, StrategyRole, StrategySpec


VERSION = "hydra_evidence_conversion_foundry_v3"
ALLOWED_DECISIONS = (
    "PROMOTION_FAILED",
    "SHADOW_RESEARCH_ONLY",
    "PRE_HOLDOUT_READY",
)
DEVELOPMENT_START = "2023-04-01"
DEVELOPMENT_END = "2024-10-01"
BOOTSTRAP_DRAWS = 2_048
MATCHED_NULL_DRAWS = 1_024


class EvidenceConversionError(RuntimeError):
    pass


def run_evidence_conversion_cohort(
    output_dir: str | Path,
    *,
    source_result_paths: Sequence[str | Path],
    source_result_sha256s: Mapping[str, str],
    source_exact_result_paths: Sequence[str | Path],
    source_exact_result_sha256s: Mapping[str, str],
    candidate_bank_manifest_path: str | Path,
    candidate_bank_manifest_sha256: str,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    contract_map_path: str | Path,
    contract_map_sha256: str,
    code_commit: str,
    cohort_id: str = "evidence_conversion_v3_cohort_0000",
    allocation: Mapping[str, float] | None = None,
    max_representatives: int = 40,
    max_complete_validation: int = 20,
    previously_decided_candidate_ids: Sequence[str] = (),
    q4_access_allowed: bool = False,
    random_seed: int = 20260712,
) -> dict[str, Any]:
    started = time.perf_counter()
    if q4_access_allowed:
        raise EvidenceConversionError("V3 initial conversion must keep Q4 sealed.")
    if not 1 <= max_complete_validation <= max_representatives <= 40:
        raise EvidenceConversionError("Invalid bounded V3 cohort sizes.")
    task = Path(engineering_task_path)
    bank_path = Path(candidate_bank_manifest_path)
    map_path = Path(contract_map_path)
    _verify(task, engineering_task_sha256, "engineering task")
    _verify(bank_path, candidate_bank_manifest_sha256, "candidate bank manifest")
    _verify(map_path, contract_map_sha256, "explicit-contract map")
    _verify_many(source_result_paths, source_result_sha256s, "promotion result")
    _verify_many(source_exact_result_paths, source_exact_result_sha256s, "exact replay")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise EvidenceConversionError("Conversion worker commit differs from frozen specification.")

    promotion_candidates = _load_promotion_candidates(source_result_paths)
    exact_by_id = _load_exact_rows(source_exact_result_paths)
    bank_manifest = json.loads(bank_path.read_text(encoding="utf-8"))
    manifest_semantic = dict(bank_manifest)
    manifest_hash = str(manifest_semantic.pop("manifest_hash", ""))
    if manifest_hash != _stable_hash(manifest_semantic):
        raise EvidenceConversionError("Candidate-bank semantic manifest hash is invalid.")
    if (
        str(bank_manifest.get("cohort_id") or "") != cohort_id
        or str(bank_manifest.get("code_commit") or "") != code_commit
        or bool(bank_manifest.get("q4_access_allowed"))
        or bool(bank_manifest.get("live_or_broker_allowed"))
    ):
        raise EvidenceConversionError(
            "Candidate-bank manifest scope differs from the frozen no-Q4/no-live cohort."
        )
    bank_rows = _candidate_bank_rows(bank_manifest)
    if int(bank_manifest.get("candidate_count") or 0) != len(bank_rows):
        raise EvidenceConversionError("Candidate-bank manifest count is inconsistent.")
    bank_count = len(bank_rows) or len(promotion_candidates)
    if not promotion_candidates:
        raise EvidenceConversionError("No immutable promotion candidates were supplied.")

    # The immutable candidate-bank manifest is authoritative.  Promotion files
    # contribute richer candidate-level evidence, but may not silently re-add a
    # killed/ineligible candidate or omit an eligible inventory row.
    bank_by_id = {
        str(row.get("candidate_id") or ""): dict(row)
        for row in bank_rows
        if str(row.get("candidate_id") or "")
    }
    promotion_by_id = {
        str(row["candidate_id"]): dict(row) for row in promotion_candidates
    }
    inventory_candidates: list[dict[str, Any]] = []
    for candidate_id in sorted(bank_by_id or promotion_by_id):
        merged = dict(bank_by_id.get(candidate_id) or {})
        merged.update(promotion_by_id.get(candidate_id) or {})
        merged["candidate_id"] = candidate_id
        inventory_candidates.append(merged)
    detailed_candidates = [
        row
        for row in inventory_candidates
        if isinstance(row.get("specification"), dict)
        and bool(row.get("specification"))
        and str(row["candidate_id"]) in exact_by_id
    ]
    if not detailed_candidates:
        raise EvidenceConversionError(
            "The frozen eligible bank has no candidate with immutable specification and exact ledger."
        )

    # The feature matrices are built before clustering so detailed candidates
    # can be fingerprinted with exact post-decision timestamps, not merely by
    # their daily PnL summaries.
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=_canonical_cache_root(),
        contract_map_path=map_path,
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    _enrich_behavioral_ledgers(detailed_candidates, exact_by_id, matrices)
    _apply_global_multiple_testing(inventory_candidates)
    clusters, membership = cluster_candidates(inventory_candidates, exact_by_id)
    cluster_sizes = {row["cluster_id"]: int(row["member_count"]) for row in clusters}
    debt = [
        build_evidence_debt_record(
            candidate,
            exact_by_id.get(str(candidate["candidate_id"])),
            cluster_id=membership[str(candidate["candidate_id"])],
            cluster_size=cluster_sizes[membership[str(candidate["candidate_id"])]],
        ).to_dict()
        for candidate in inventory_candidates
    ]
    debt.sort(
        key=lambda row: (-float(row["evidence_conversion_priority"]), row["candidate_id"])
    )
    candidate_lookup = {str(row["candidate_id"]): row for row in detailed_candidates}
    debt_lookup = {str(row["candidate_id"]): row for row in debt}
    previously_decided = {str(value) for value in previously_decided_candidate_ids}
    all_representatives = _select_representatives(
        clusters,
        candidate_lookup,
        debt_lookup,
        limit=None,
    )
    unseen_representatives_all = [
        candidate_id
        for candidate_id in all_representatives
        if candidate_id not in previously_decided
    ]
    representatives = _select_role_diverse(
        unseen_representatives_all,
        candidate_lookup,
        debt_lookup,
        limit=max_representatives,
    )
    validation_ids = _select_role_diverse(
        representatives,
        candidate_lookup,
        debt_lookup,
        limit=max_complete_validation,
    )

    evaluations = []
    for index, candidate_id in enumerate(validation_ids):
        evaluations.append(
            _complete_candidate_validation(
                candidate_lookup[candidate_id],
                exact_by_id.get(candidate_id) or {},
                matrices,
                seed=random_seed + index * 1009,
            )
        )
    interactions = _portfolio_interactions(evaluations)
    for row in evaluations:
        row["account_interaction"] = interactions.get(row["candidate_id"], {})
        _finalize_decision(row)

    pre_holdout_all = [
        row["candidate_id"]
        for row in evaluations
        if row["decision"] == "PRE_HOLDOUT_READY"
    ]
    pre_holdout = _select_pre_holdout_cohort(
        pre_holdout_all, candidate_lookup, membership, limit=8
    )
    pre_holdout_set = set(pre_holdout)
    for row in evaluations:
        if row["decision"] == "PRE_HOLDOUT_READY" and row["candidate_id"] not in pre_holdout_set:
            row["decision"] = "SHADOW_RESEARCH_ONLY"
            row["decision_reasons"].append("pre_holdout_cohort_capacity_or_diversity_limit")
    decisions = Counter(str(row["decision"]) for row in evaluations)
    status_counts = {status: int(decisions.get(status, 0)) for status in ALLOWED_DECISIONS}
    full_economic = sum(bool(row["stage_completion"]["full_economic_replay"]) for row in evaluations)
    full_risk = sum(bool(row["stage_completion"]["full_risk_replay"]) for row in evaluations)
    full_promotion = sum(
        bool(row["stage_completion"]["full_promotion_validation"])
        for row in evaluations
    )
    role_distribution = dict(
        Counter(str(candidate_lookup[candidate_id].get("role") or "UNKNOWN") for candidate_id in representatives)
    )
    allocation = dict(
        allocation
        or {
            "promotion": 0.70,
            "shadow_feed_engineering": 0.15,
            "targeted_mutation": 0.10,
            "discovery": 0.05,
        }
    )
    if not math.isclose(sum(float(value) for value in allocation.values()), 1.0, abs_tol=1e-9):
        raise EvidenceConversionError("Compute allocation must sum to one.")

    source_hashes = {
        **{str(Path(path)): _expected_hash(Path(path), source_result_sha256s) for path in source_result_paths},
        **{str(Path(path)): _expected_hash(Path(path), source_exact_result_sha256s) for path in source_exact_result_paths},
        str(bank_path): candidate_bank_manifest_sha256,
        str(task): engineering_task_sha256,
        str(map_path): contract_map_sha256,
    }
    pre_holdout_manifest = None
    if 3 <= len(pre_holdout) <= 8:
        frozen = {
            "schema": "hydra_pre_holdout_cohort_v1",
            "cohort_id": cohort_id,
            "candidate_ids": pre_holdout,
            "candidate_roles": {
                candidate_id: candidate_lookup[candidate_id].get("role")
                for candidate_id in pre_holdout
            },
            "specifications": {
                candidate_id: candidate_lookup[candidate_id].get("specification")
                for candidate_id in pre_holdout
            },
            "source_commit": code_commit,
            "source_hashes": source_hashes,
            "cost_policy": "frozen_candidate_exact_cost_and_1_5x_stress",
            "null_policy": "global_bh_plus_matched_opportunity_null_preregistered_v1",
            "decision_policy": "one_shot_q4_no_mutation_no_retest_v1",
            "q4_access_allowed": False,
            "q4_access_count_before": 0,
            "status": "FROZEN_PRE_HOLDOUT_Q4_UNOPENED",
        }
        frozen["manifest_hash"] = _stable_hash(frozen)
        pre_holdout_manifest = frozen

    payload: dict[str, Any] = {
        "schema": VERSION,
        "cohort_id": cohort_id,
        "scientific_conclusion": (
            "PRE_HOLDOUT_COHORT_FROZEN_Q4_UNOPENED"
            if pre_holdout_manifest
            else "EVIDENCE_CONVERSION_COMPLETED_MORE_DEBT_CLOSURE_REQUIRED"
        ),
        "candidates_before_clustering": len(inventory_candidates),
        "candidate_inventory_count": bank_count,
        "detailed_promotion_candidates": len(detailed_candidates),
        "behavioral_clusters": len(clusters),
        "representative_count": len(representatives),
        "total_distinct_representative_count": len(all_representatives),
        "representative_candidate_ids": representatives,
        "role_distribution": role_distribution,
        "evidence_debt_inventory_count": len(debt),
        "evidence_debt_queue_count": max(
            len(unseen_representatives_all) - len(validation_ids), 0
        ),
        "complete_validation_candidate_ids": validation_ids,
        "previously_decided_candidate_ids": sorted(previously_decided),
        "promotion_decisions_count": len(evaluations),
        "full_economic_replay_count": full_economic,
        "full_risk_replay_count": full_risk,
        "full_promotion_validation_count": full_promotion,
        "status_counts": status_counts,
        "pre_holdout_candidate_ids": pre_holdout,
        "pre_holdout_ready_count": len(pre_holdout),
        "pre_holdout_manifest": pre_holdout_manifest,
        "q4_access_count": 0,
        "q4_result": None,
        "paper_shadow_ready": 0,
        "forward_observations": 0,
        "compute_allocation": allocation,
        "discovery_throttled": True,
        "discovery_allocation": float(allocation.get("discovery", 0.0)),
        "feature_store": {
            "cache_hits": feature_build.cache_hits,
            "cache_misses": feature_build.cache_misses,
            "rows": feature_build.rows,
            "risk_path_ohlc": True,
        },
        "throughput": {
            "wall_seconds": time.perf_counter() - started,
            "full_economic_replays_per_hour": full_economic / max(time.perf_counter() - started, 1e-9) * 3600.0,
            "full_risk_replays_per_hour": full_risk / max(time.perf_counter() - started, 1e-9) * 3600.0,
            "full_promotion_validations_per_hour": full_promotion / max(time.perf_counter() - started, 1e-9) * 3600.0,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": DEVELOPMENT_END,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "outbound_order_capability": False,
        },
        "integrity": {
            "source_hashes_verified": True,
            "code_commit_verified": True,
            "no_status_inheritance": True,
            "q4_excluded": True,
            "no_broker_or_order_capability": True,
            "allowed_decision_vocabulary": list(ALLOWED_DECISIONS),
        },
        "next_recommended_action": (
            "VERIFY_Q4_ONE_SHOT_PROTOCOL_FOR_FROZEN_COHORT"
            if pre_holdout_manifest
            else "CLOSE_REMAINING_RISK_PORTFOLIO_AND_SHADOW_PACKAGE_DEBT"
        ),
    }
    payload = _strict_json_value(payload)
    payload["candidate_count"] = len(evaluations)
    payload["candidates"] = [
        {
            "candidate_id": row["candidate_id"],
            "status": row["decision"],
            "role": row["role"],
            "primary_market": row["primary_market"],
            "execution_market": row["execution_market"],
            "timeframe": row["timeframe"],
            "topstep": row["source_topstep"],
            "net_pnl": row["economic"]["net_pnl"],
            "full_economic_replay": row["stage_completion"]["full_economic_replay"],
            "full_risk_replay": row["stage_completion"]["full_risk_replay"],
            "full_promotion_validation": row["stage_completion"]["full_promotion_validation"],
            "paper_shadow_ready": False,
        }
        for row in evaluations
    ]
    payload["result_hash"] = _stable_hash(payload)
    writer = AtomicResultWriter(output_dir)
    debt_receipt = writer.write_json("evidence_debt_queue.json", debt)
    cluster_receipt = writer.write_json("behavioral_clusters.json", clusters)
    representative_receipt = writer.write_json(
        "promotion_representatives.json",
        [
            {
                "candidate_id": candidate_id,
                "cluster_id": membership[candidate_id],
                "role": candidate_lookup[candidate_id].get("role"),
                "specification": candidate_lookup[candidate_id].get("specification"),
                "evidence_conversion_priority": debt_lookup[candidate_id]["evidence_conversion_priority"],
            }
            for candidate_id in representatives
        ],
    )
    validation_receipt = writer.write_json("complete_validation.json", evaluations)
    pre_holdout_receipt = (
        writer.write_json("pre_holdout_cohort_manifest.json", pre_holdout_manifest)
        if pre_holdout_manifest
        else None
    )
    result_receipt = writer.write_json("evidence_conversion_result.json", payload)
    report = _report(payload, evaluations)
    report_receipt = writer.write_text("evidence_conversion_report.md", report)
    root = Path(output_dir).resolve()
    artifacts = {
        "evidence_debt_queue_path": str(root / debt_receipt.relative_path),
        "behavioral_clusters_path": str(root / cluster_receipt.relative_path),
        "representatives_path": str(root / representative_receipt.relative_path),
        "complete_validation_path": str(root / validation_receipt.relative_path),
        "pre_holdout_manifest_path": (
            str(root / pre_holdout_receipt.relative_path)
            if pre_holdout_receipt
            else None
        ),
        "result_path": str(root / result_receipt.relative_path),
        "report_path": str(root / report_receipt.relative_path),
    }
    artifact_sha256s = {
        "evidence_debt_queue_path": debt_receipt.sha256,
        "behavioral_clusters_path": cluster_receipt.sha256,
        "representatives_path": representative_receipt.sha256,
        "complete_validation_path": validation_receipt.sha256,
        "pre_holdout_manifest_path": (
            pre_holdout_receipt.sha256 if pre_holdout_receipt else None
        ),
        "result_path": result_receipt.sha256,
        "report_path": report_receipt.sha256,
    }
    return {
        **payload,
        "artifacts": artifacts,
        "artifact_sha256s": artifact_sha256s,
        "report_path": str(root / report_receipt.relative_path),
    }


def _load_promotion_candidates(paths: Sequence[str | Path]) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    for path in sorted(Path(value) for value in paths):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if int(((payload.get("governance") or {}).get("q4_access_count_delta") or 0)) != 0:
            raise EvidenceConversionError(f"Promotion source crossed Q4 boundary: {path}")
        for row in payload.get("candidates") or []:
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                continue
            previous = candidates.get(candidate_id)
            if previous is not None and previous != row:
                raise EvidenceConversionError(f"Candidate changed across immutable promotion sources: {candidate_id}")
            candidates[candidate_id] = dict(row)
    return [candidates[key] for key in sorted(candidates)]


def _load_exact_rows(paths: Sequence[str | Path]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(Path(value) for value in paths):
        for raw in path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            row = json.loads(raw)
            candidate_id = str(row.get("candidate_id") or "")
            if candidate_id:
                previous = rows.get(candidate_id)
                if previous is not None and previous != row:
                    raise EvidenceConversionError(
                        f"Exact candidate changed across immutable ledgers: {candidate_id}"
                    )
                rows[candidate_id] = row
    return rows


def _enrich_behavioral_ledgers(
    candidates: Sequence[Mapping[str, Any]],
    exact_by_id: dict[str, dict[str, Any]],
    matrices: Mapping[str, FeatureMatrix],
) -> None:
    """Attach full development event timestamps for behavioral comparison.

    Source exact ledgers intentionally remain immutable on disk.  This function
    enriches only the in-memory copy and derives every timestamp from the frozen
    specification and past-only feature matrix.
    """

    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        spec = spec_from_dict(dict(candidate["specification"]))
        matrix = matrices.get(spec.market)
        if matrix is None:
            raise EvidenceConversionError(
                f"No frozen feature matrix for {candidate_id}/{spec.market}."
            )
        positions = _period_positions(spec, matrix)
        forward = matrix.array(f"forward_move__{spec.holding_events}")[positions]
        net = (
            forward * spec.side * spec.point_value * spec.quantity
            - spec.round_turn_cost * spec.quantity
        )
        exact = dict(exact_by_id.get(candidate_id) or {})
        exact.update(
            {
                "event_timestamp_ns": matrix.array("decision_ns")[positions]
                .astype(np.int64)
                .tolist(),
                "event_session_days": matrix.array("session_day")[positions]
                .astype(np.int64)
                .tolist(),
                "event_net_pnl": net.astype(float).tolist(),
                "behavioral_replay_period": [DEVELOPMENT_START, DEVELOPMENT_END],
                "behavioral_replay_complete": True,
            }
        )
        exact_by_id[candidate_id] = exact


def _candidate_bank_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("candidates", "candidate_bank", "promotion_eligible_candidates"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            return [{"candidate_id": candidate_id, **dict(row)} for candidate_id, row in value.items()]
    return []


def _apply_global_multiple_testing(candidates: list[dict[str, Any]]) -> None:
    raw = [float((row.get("candidate_null") or {}).get("raw_probability") or 1.0) for row in candidates]
    adjusted = _benjamini_hochberg(raw)
    for row, value in zip(candidates, adjusted, strict=True):
        row.setdefault("candidate_null", {})["global_adjusted_probability"] = float(value)
        row["candidate_null"]["global_family_size"] = len(candidates)


def _select_representatives(
    clusters: Sequence[Mapping[str, Any]],
    candidates: Mapping[str, Mapping[str, Any]],
    debt: Mapping[str, Mapping[str, Any]],
    *,
    limit: int | None,
) -> list[str]:
    primaries = []
    for cluster in clusters:
        members = [value for value in cluster.get("member_ids") or [] if value in candidates]
        if not members:
            continue
        members.sort(key=lambda value: (-float(debt[value]["evidence_conversion_priority"]), value))
        primaries.append(members[0])
    primaries.sort(key=lambda value: (-float(debt[value]["evidence_conversion_priority"]), value))
    if limit is None:
        return primaries
    return _select_role_diverse(primaries, candidates, debt, limit=limit)


def _select_role_diverse(
    candidate_ids: Sequence[str],
    candidates: Mapping[str, Mapping[str, Any]],
    debt: Mapping[str, Mapping[str, Any]],
    *,
    limit: int,
) -> list[str]:
    role_order = ("COMBINE_PASSER", "XFA_PAYOUT", "DEFENSIVE", "PORTFOLIO_ONLY")
    caps = {
        "COMBINE_PASSER": max(1, math.ceil(limit * 0.30)),
        "XFA_PAYOUT": max(1, math.ceil(limit * 0.45)),
        "DEFENSIVE": max(1, math.ceil(limit * 0.20)),
        "PORTFOLIO_ONLY": max(1, math.floor(limit * 0.05)),
    }
    pools: dict[str, list[str]] = defaultdict(list)
    for candidate_id in candidate_ids:
        pools[str(candidates[candidate_id].get("role") or "UNKNOWN")].append(candidate_id)
    for values in pools.values():
        values.sort(key=lambda value: (-float(debt[value]["evidence_conversion_priority"]), value))
    selected: list[str] = []
    for role in role_order:
        selected.extend(pools.get(role, [])[: caps[role]])
    remaining = sorted(
        (candidate_id for candidate_id in candidate_ids if candidate_id not in selected),
        key=lambda value: (-float(debt[value]["evidence_conversion_priority"]), value),
    )
    selected.extend(remaining[: max(0, limit - len(selected))])
    return selected[:limit]


def _complete_candidate_validation(
    candidate: Mapping[str, Any],
    source_exact: Mapping[str, Any],
    matrices: Mapping[str, FeatureMatrix],
    *,
    seed: int,
) -> dict[str, Any]:
    specification = dict(candidate.get("specification") or {})
    spec = spec_from_dict(specification)
    matrix = matrices[spec.market]
    positions = _period_positions(spec, matrix)
    economic = _economic_metrics(spec, matrix, positions, seed=seed)
    risk = _risk_metrics(candidate, spec, matrix, positions, economic)
    null = dict(candidate.get("candidate_null") or {})
    matched_null = _matched_opportunity_null(
        spec, matrix, positions, seed=seed + 17
    )
    simple_baselines = _simple_baseline_suite(spec, matrix, positions)
    robustness = {
        "elevated_cost_positive": float(economic["cost_stress_1_5x_net"]) > 0.0,
        "delayed_signal_positive": float(candidate.get("one_bar_delay_net_pnl") or 0.0) > 0.0,
        "best_trade_removal": economic["best_trade_removal"],
        "best_day_removal": economic["best_day_removal"],
        "best_month_removal": economic["best_month_removal"],
        "best_period_removal": economic["best_period_removal"],
        "block_bootstrap": economic["block_bootstrap"],
        "simple_baseline_suite": simple_baselines,
        "parameter_neighborhood_passed": bool((candidate.get("parameter_neighborhood") or {}).get("passed")),
        "contract_transfer_passed": bool((candidate.get("contract_transfer") or {}).get("passed")),
    }
    # Stage-completion fields are throughput/evidence vocabulary.  They state
    # that a replay was executed completely; pass/fail lives in the separate
    # decision gates below and must never be hidden inside throughput counts.
    full_economic = bool(economic["complete"])
    full_risk = bool(risk["complete"])
    return {
        "candidate_id": candidate["candidate_id"],
        "immutable_specification_hash": hashlib.sha256(
            json.dumps(specification, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "role": candidate.get("role"),
        "primary_market": candidate.get("primary_market"),
        "execution_market": candidate.get("execution_market"),
        "timeframe": candidate.get("timeframe"),
        "economic": economic,
        "risk": risk,
        "candidate_null": {
            **null,
            "matched_opportunity_probability": matched_null["probability"],
            "matched_opportunity_null": matched_null,
            "simple_baseline_suite": simple_baselines,
            "source_session_block_sign_flip_complete": bool(
                null.get("raw_probability") is not None
                and "session_block_sign_flip" in str(null.get("method") or "")
            ),
            "global_adjusted_probability": float(null.get("global_adjusted_probability") or 1.0),
        },
        "robustness": robustness,
        "role_evidence": candidate.get("role_specific_evidence") or {},
        "source_topstep": risk.get("topstep") or {},
        "prior_source_topstep": candidate.get("topstep") or {},
        "source_exact_hash": _stable_hash(source_exact),
        "stage_completion": {
            "full_economic_replay": full_economic,
            "full_risk_replay": full_risk,
            "full_promotion_validation": False,
        },
        "decision": "SHADOW_RESEARCH_ONLY",
        "decision_reasons": [],
        "q4_access_count": 0,
        "paper_shadow_ready": False,
    }


def _period_positions(spec: StrategySpec, matrix: FeatureMatrix) -> np.ndarray:
    feature = matrix.array(f"feature__{spec.feature}")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    day = matrix.array("session_day")
    session = matrix.array("session_code")
    mask = (
        (day >= _day(DEVELOPMENT_START))
        & (day < _day(DEVELOPMENT_END))
        & np.isfinite(feature)
        & np.isfinite(forward)
        & _array_compare(feature, spec.operator, spec.threshold)
    )
    mask &= session == spec.session_code if spec.session_code >= 0 else session >= 0
    if spec.context_feature is not None:
        context = matrix.array(f"feature__{spec.context_feature}")
        mask &= np.isfinite(context) & _array_compare(
            context,
            spec.context_operator or ComparisonOperator.GREATER_THAN,
            float(spec.context_threshold or 0.0),
        )
    return _non_overlapping(np.flatnonzero(mask), spec, matrix)


def _economic_metrics(
    spec: StrategySpec,
    matrix: FeatureMatrix,
    positions: np.ndarray,
    *,
    seed: int,
) -> dict[str, Any]:
    move = matrix.array(f"forward_move__{spec.holding_events}")[positions]
    gross = move * spec.side * spec.point_value * spec.quantity
    cost = spec.round_turn_cost * spec.quantity
    net = gross - cost
    days = matrix.array("session_day")[positions].astype(np.int64)
    folds = {
        "2023_development": ("2023-04-01", "2024-01-01"),
        "2024_q1": ("2024-01-01", "2024-04-01"),
        "2024_q2": ("2024-04-01", "2024-07-01"),
        "2024_q3": ("2024-07-01", "2024-10-01"),
    }
    fold_results = {}
    for name, (start, end) in folds.items():
        selected = (days >= _day(start)) & (days < _day(end))
        values = net[selected]
        fold_results[name] = {
            "events": int(len(values)),
            "net_pnl": float(values.sum()),
            "mean_net_pnl": float(values.mean()) if len(values) else 0.0,
        }
    active_folds = [value for value in fold_results.values() if value["events"] > 0]
    supportive_folds = sum(float(value["net_pnl"]) > 0.0 for value in active_folds)
    positive_fold_pnl = sum(max(float(value["net_pnl"]), 0.0) for value in active_folds)
    worst_fold_pnl = min(
        (float(value["net_pnl"]) for value in active_folds), default=0.0
    )
    catastrophic_weak_period = bool(
        worst_fold_pnl
        < -max(4_500.0, 0.75 * max(positive_fold_pnl, 1.0))
    )
    temporal_transfer_passed = bool(
        len(active_folds) >= 3
        and supportive_folds >= max(2, len(active_folds) - 1)
        and not catastrophic_weak_period
    )
    daily = _aggregate(days, net)
    month_keys = np.asarray(
        [int(np.datetime64(int(day), "D").astype("datetime64[M]").astype(int)) for day in days],
        dtype=np.int64,
    )
    monthly = _aggregate(month_keys, net)
    best_trade = float(net.max(initial=0.0))
    best_day_key, best_day = _maximum_item(daily)
    best_month_key, best_month = _maximum_item(monthly)
    best_fold_name, best_fold_pnl = max(
        (
            (name, float(value["net_pnl"]))
            for name, value in fold_results.items()
        ),
        key=lambda value: (value[1], value[0]),
        default=(None, 0.0),
    )
    bootstrap = _block_bootstrap(daily, seed=seed)
    equity = np.cumsum(net)
    peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:] if len(equity) else np.asarray([])
    drawdown = float(np.max(peak - equity, initial=0.0)) if len(equity) else 0.0
    return {
        "period": [DEVELOPMENT_START, DEVELOPMENT_END],
        "events": int(len(net)),
        "gross_pnl": float(gross.sum()),
        "net_pnl": float(net.sum()),
        "cost_stress_1_5x_net": float((gross - 1.5 * cost).sum()),
        "maximum_drawdown": drawdown,
        "supportive_folds": supportive_folds,
        "active_folds": len(active_folds),
        "temporal_transfer_passed": temporal_transfer_passed,
        "catastrophic_weak_period": catastrophic_weak_period,
        "fold_results": fold_results,
        "best_trade_removal": {"removed_pnl": best_trade, "net_pnl": float(net.sum() - best_trade)},
        "best_day_removal": {"day": best_day_key, "removed_pnl": best_day, "net_pnl": float(net.sum() - best_day)},
        "best_month_removal": {"month": best_month_key, "removed_pnl": best_month, "net_pnl": float(net.sum() - best_month)},
        "best_period_removal": {
            "period": best_fold_name,
            "removed_pnl": best_fold_pnl,
            "net_pnl": float(net.sum() - best_fold_pnl),
        },
        "block_bootstrap": bootstrap,
        "event_net_pnl": net.astype(float).tolist(),
        "event_session_days": days.astype(int).tolist(),
        "daily_pnl": {str(key): float(value) for key, value in daily.items()},
        "finite": bool(np.isfinite(net).all()),
        "complete": bool(
            len(positions) == len(net) == len(days)
            and np.isfinite(net).all()
            and all("events" in value and "net_pnl" in value for value in fold_results.values())
        ),
    }


def _risk_metrics(
    candidate: Mapping[str, Any],
    spec: StrategySpec,
    matrix: FeatureMatrix,
    positions: np.ndarray,
    economic: Mapping[str, Any],
) -> dict[str, Any]:
    required = {"bar_high", "bar_low", "bar_close", "timestamp_ns", "segment_code", "session_day"}
    if not required <= set(matrix.arrays):
        return {"complete": False, "reason": "risk_path_ohlc_missing", "mll_breached": None}
    execution_market = str(candidate.get("execution_market") or MARKET_PAIRS[spec.market])
    quantity = int((candidate.get("topstep") or {}).get("selected_micro_contracts") or 1)
    quantity = max(1, min(quantity, 15))
    micro_point = instrument_spec(execution_market).point_value
    micro_cost = _round_turn_cost_all(execution_market)
    moves = matrix.array(f"forward_move__{spec.holding_events}")[positions]
    pnl = moves * spec.side * micro_point * quantity - micro_cost * quantity
    timestamps = pd.to_datetime(matrix.array("timestamp_ns"), unit="ns", utc=True)
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "high": matrix.array("bar_high"),
            "low": matrix.array("bar_low"),
            "close": matrix.array("bar_close"),
            "symbol": execution_market,
        }
    )
    segments = matrix.array("segment_code")
    session_days = matrix.array("session_day")
    entry_price = matrix.array("entry_price")
    bar_high = matrix.array("bar_high")
    bar_low = matrix.array("bar_low")
    trades = []
    account_trade_ledger: list[dict[str, Any]] = []
    event_mae: list[float] = []
    event_days: list[int] = []
    daily_state: dict[int, dict[str, float | int]] = {}
    session_flatten = True
    for index, position in enumerate(positions):
        entry_i = int(position) + 1
        exit_i = int(position) + spec.holding_events + 1
        if exit_i >= matrix.row_count:
            session_flatten = False
            continue
        same_segment = int(segments[entry_i]) == int(segments[exit_i])
        same_session = int(session_days[entry_i]) == int(session_days[exit_i])
        session_flatten &= same_segment and same_session
        price = float(entry_price[int(position)])
        if spec.side > 0:
            adverse_price = float(np.nanmin(bar_low[entry_i : exit_i + 1]))
        else:
            adverse_price = float(np.nanmax(bar_high[entry_i : exit_i + 1]))
        adverse = (
            (adverse_price - price)
            * spec.side
            * micro_point
            * quantity
        )
        day = int(session_days[entry_i])
        state = daily_state.setdefault(
            day,
            {"pnl": 0.0, "worst_intraday_pnl": 0.0, "trades": 0},
        )
        state["worst_intraday_pnl"] = min(
            float(state["worst_intraday_pnl"]),
            float(state["pnl"]) + min(adverse, 0.0),
        )
        state["pnl"] = float(state["pnl"]) + float(pnl[index])
        state["trades"] = int(state["trades"]) + 1
        event_mae.append(float(min(adverse, 0.0)))
        event_days.append(day)
        availability_ns = int(matrix.array("availability_ns")[int(position)])
        source_close_ns = int(matrix.array("timestamp_ns")[int(position)])
        trades.append(
            {
                "entry_i": entry_i,
                "exit_i": exit_i,
                "entry_price": price,
                "side": spec.side,
                "symbol": execution_market,
                "point_value": micro_point,
                "risk_scale": quantity,
                "pnl": float(pnl[index]),
            }
        )
        account_trade_ledger.append(
            {
                "trade_id": f"{candidate['candidate_id']}:{index:06d}",
                "event_session_id": str(np.datetime64(day, "D")),
                "entry_timestamp": timestamps[entry_i].isoformat(),
                "exit_timestamp": timestamps[exit_i].isoformat(),
                "availability_timestamp": pd.to_datetime(
                    availability_ns, unit="ns", utc=True
                ).isoformat(),
                "source_bar_close": pd.to_datetime(
                    source_close_ns, unit="ns", utc=True
                ).isoformat(),
                "net_pnl": float(pnl[index]),
                "mae_dollars": float(min(adverse, 0.0)),
                "cost": float(micro_cost * quantity),
                "contracts": int(quantity),
                "underlying": spec.market,
                "symbol": execution_market,
                "side": int(spec.side),
            }
        )
    config = Topstep150KConfig()
    audit = conservative_intraday_mll_audit(
        trades,
        frame,
        starting_balance=config.combine_starting_balance,
        starting_floor=config.combine_starting_mll,
        mll_distance=config.combine_max_loss_limit,
        floor_lock=config.combine_starting_balance,
    )
    daily = pd.DataFrame(
        [
            {
                "date": str(np.datetime64(int(day), "D")),
                "pnl": float(values["pnl"]),
                "raw_pnl": float(values["pnl"]),
                "worst_intraday_pnl": float(values["worst_intraday_pnl"]),
                "trades": int(values["trades"]),
                "skipped_trades": 0,
                "hit_daily_stop": False,
                "hit_daily_profit_lock": False,
            }
            for day, values in sorted(daily_state.items())
        ]
    )
    topstep = _fixed_quantity_topstep(daily, quantity=quantity)
    complete = bool(
        len(trades) == len(positions)
        and bool(trades)
        and len(event_mae) == len(trades)
        and topstep.get("complete")
    )
    return {
        "complete": complete,
        "method": "conservative_1m_ohlc_intraday_unrealized_mll_v1",
        "execution_market": execution_market,
        "micro_contracts": quantity,
        "events": len(trades),
        "mll_breached": audit.breached,
        "minimum_mll_buffer": audit.min_buffer,
        "breach_timestamp": audit.breach_timestamp,
        "same_bar_ambiguities": audit.ambiguous_same_bar_count,
        "forced_liquidation_slippage": audit.forced_liquidation_slippage,
        "session_flatten_proved": session_flatten,
        "contract_limit_proved": quantity <= 15,
        "notes": audit.notes,
        "daily_pnl": {
            str(day): float(values["pnl"])
            for day, values in sorted(daily_state.items())
        },
        "daily_worst_intraday_pnl": {
            str(day): float(values["worst_intraday_pnl"])
            for day, values in sorted(daily_state.items())
        },
        "event_net_pnl": pnl.astype(float).tolist(),
        "event_session_days": event_days,
        "event_mae_dollars": event_mae,
        "account_trade_ledger": account_trade_ledger,
        "topstep": topstep,
    }


def _fixed_quantity_topstep(
    daily: pd.DataFrame, *, quantity: int
) -> dict[str, Any]:
    if daily.empty:
        return {"complete": False, "reason": "no_daily_path"}
    config = Topstep150KConfig()
    combine = simulate_combine(daily, config)
    standard = simulate_xfa_standard(
        daily, mll_distance=config.combine_max_loss_limit
    )
    consistency = simulate_xfa_consistency(
        daily, mll_distance=config.combine_max_loss_limit
    )
    return {
        "complete": True,
        "rule_version": "topstep_150k_2026-07-10_no_dll_baseline",
        "sizing_policy": "frozen_selected_micro_contracts_no_reselection",
        "selected_micro_contracts": int(quantity),
        "combine": combine,
        "xfa_standard": standard,
        "xfa_consistency": consistency,
        "path_candidate": bool(
            combine.get("passed")
            and not combine.get("mll_breached")
            and combine.get("consistency_ok")
        ),
    }


def _matched_opportunity_null(
    spec: StrategySpec,
    matrix: FeatureMatrix,
    positions: np.ndarray,
    *,
    seed: int,
) -> dict[str, Any]:
    count = len(positions)
    if count < 10:
        return {
            "probability": 1.0,
            "draws": 0,
            "complete": False,
            "reason": "fewer_than_10_events",
            "matching": "session_and_temporal_fold_event_count",
        }
    days = matrix.array("session_day")
    sessions = matrix.array("session_code")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    eligible = np.flatnonzero(
        (days >= _day(DEVELOPMENT_START))
        & (days < _day(DEVELOPMENT_END))
        & np.isfinite(forward)
        & (sessions == spec.session_code if spec.session_code >= 0 else sessions >= 0)
    )
    eligible = _non_overlapping(eligible, spec, matrix)
    if len(eligible) < count:
        return {
            "probability": 1.0,
            "draws": 0,
            "complete": False,
            "reason": "insufficient_matched_opportunities",
            "matching": "session_and_temporal_fold_event_count",
        }
    observed = float(
        (
            forward[positions] * spec.side * spec.point_value * spec.quantity
            - spec.round_turn_cost * spec.quantity
        ).sum()
    )
    rng = np.random.default_rng(seed)
    null = np.empty(MATCHED_NULL_DRAWS, dtype=float)
    boundaries = (
        (_day("2023-04-01"), _day("2024-01-01")),
        (_day("2024-01-01"), _day("2024-04-01")),
        (_day("2024-04-01"), _day("2024-07-01")),
        (_day("2024-07-01"), _day("2024-10-01")),
    )
    strata: list[tuple[np.ndarray, int]] = []
    for start, end in boundaries:
        observed_count = int(
            np.count_nonzero((days[positions] >= start) & (days[positions] < end))
        )
        pool = eligible[(days[eligible] >= start) & (days[eligible] < end)]
        if observed_count > len(pool):
            return {
                "probability": 1.0,
                "draws": 0,
                "complete": False,
                "reason": "insufficient_temporal_stratum_opportunities",
                "matching": "session_and_temporal_fold_event_count",
            }
        if observed_count:
            strata.append((pool, observed_count))
    for draw in range(MATCHED_NULL_DRAWS):
        sampled = np.concatenate(
            [rng.choice(pool, size=size, replace=False) for pool, size in strata]
        )
        null[draw] = float(
            (
                forward[sampled] * spec.side * spec.point_value * spec.quantity
                - spec.round_turn_cost * spec.quantity
            ).sum()
        )
    return {
        "probability": float(
            (1 + np.count_nonzero(null >= observed)) / (MATCHED_NULL_DRAWS + 1)
        ),
        "draws": MATCHED_NULL_DRAWS,
        "complete": True,
        "observed_net_pnl": observed,
        "matching": "session_and_temporal_fold_event_count",
        "strata_counts": [int(size) for _pool, size in strata],
        "seed": int(seed),
    }


def _simple_baseline_suite(
    spec: StrategySpec,
    matrix: FeatureMatrix,
    positions: np.ndarray,
) -> dict[str, Any]:
    """Evaluate past-only simple explanations at the candidate horizon.

    Baselines are diagnostic competing mechanisms.  Their event means are
    scaled to the candidate opportunity count so frequency alone cannot make a
    simple rule appear superior.
    """

    days = matrix.array("session_day")
    sessions = matrix.array("session_code")
    forward = matrix.array(f"forward_move__{spec.holding_events}")
    past_return = matrix.array("feature__past_return_60")
    past_volatility = matrix.array("feature__past_volatility")
    eligible_mask = (
        (days >= _day(DEVELOPMENT_START))
        & (days < _day(DEVELOPMENT_END))
        & np.isfinite(forward)
        & np.isfinite(past_return)
        & np.isfinite(past_volatility)
        & (
            sessions == spec.session_code
            if spec.session_code >= 0
            else sessions >= 0
        )
    )
    raw_eligible = np.flatnonzero(eligible_mask)
    baseline_cap = 100_000
    if len(raw_eligible) > baseline_cap:
        sample_indices = np.linspace(
            0, len(raw_eligible) - 1, baseline_cap, dtype=np.int64
        )
        raw_eligible = raw_eligible[sample_indices]
    eligible = _non_overlapping(raw_eligible, spec, matrix)
    cost = spec.round_turn_cost * spec.quantity
    scale = spec.point_value * spec.quantity

    def metrics(name: str, values: np.ndarray) -> dict[str, Any]:
        mean = float(values.mean()) if len(values) else 0.0
        return {
            "name": name,
            "events": int(len(values)),
            "mean_net_pnl": mean,
            "candidate_count_scaled_net_pnl": mean * len(positions),
        }

    session_values = forward[eligible] * spec.side * scale - cost
    momentum_values = (
        forward[eligible] * np.where(past_return[eligible] >= 0.0, 1.0, -1.0) * scale
        - cost
    )
    reversion_values = (
        forward[eligible] * np.where(past_return[eligible] >= 0.0, -1.0, 1.0) * scale
        - cost
    )
    volatility_cut = float(np.nanmedian(past_volatility[eligible])) if len(eligible) else 0.0
    volatility_positions = eligible[past_volatility[eligible] >= volatility_cut]
    volatility_values = forward[volatility_positions] * spec.side * scale - cost
    rows = [
        metrics("session_only_fixed_side", session_values),
        metrics("past_return_momentum", momentum_values),
        metrics("past_return_mean_reversion", reversion_values),
        metrics("past_volatility_only_fixed_side", volatility_values),
    ]
    candidate_move = forward[positions] * spec.side * scale - cost
    candidate_mean = float(candidate_move.mean()) if len(candidate_move) else 0.0
    best = max((float(row["mean_net_pnl"]) for row in rows), default=0.0)
    return {
        "complete": bool(len(positions) >= 10 and all(row["events"] >= 10 for row in rows)),
        "past_only": True,
        "opportunity_sampling": "deterministic_evenly_spaced_cap_100000",
        "candidate_events": int(len(positions)),
        "candidate_mean_net_pnl": candidate_mean,
        "volatility_threshold": volatility_cut,
        "baselines": rows,
        "best_simple_baseline_mean_net_pnl": best,
        "candidate_outperforms_best_simple_baseline": candidate_mean > best,
    }


def _portfolio_interactions(evaluations: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    daily = {
        str(row["candidate_id"]): {
            int(key): float(value)
            for key, value in (row.get("economic") or {}).get("daily_pnl", {}).items()
        }
        for row in evaluations
    }
    ledgers = {
        str(row["candidate_id"]): _micro_first_account_ledger(
            list((row.get("risk") or {}).get("account_trade_ledger") or [])
        )
        for row in evaluations
    }
    alpha_ids = sorted(
        str(row["candidate_id"])
        for row in evaluations
        if str(row.get("role") or "") in {"COMBINE_PASSER", "XFA_PAYOUT"}
        and ledgers.get(str(row["candidate_id"]))
    )
    output: dict[str, dict[str, Any]] = {}
    for row in evaluations:
        candidate_id = str(row["candidate_id"])
        role = str(row.get("role") or "")
        if role in {"DEFENSIVE", "PORTFOLIO_ONLY"}:
            peer_ids = [value for value in alpha_ids if value != candidate_id][:4]
        else:
            peer_ids = [
                str(value["candidate_id"])
                for value in evaluations
                if str(value["candidate_id"]) != candidate_id
                and ledgers.get(str(value["candidate_id"]))
            ][:4]
        peers = [
            value
            for value in evaluations
            if str(value["candidate_id"]) in set(peer_ids)
        ]
        correlations = []
        shared_loss_days = 0
        for peer in peers:
            peer_id = str(peer["candidate_id"])
            common = sorted(set(daily[candidate_id]) & set(daily[peer_id]))
            if len(common) >= 3:
                left = np.asarray([daily[candidate_id][day] for day in common])
                right = np.asarray([daily[peer_id][day] for day in common])
                if np.std(left) > 1e-12 and np.std(right) > 1e-12:
                    correlations.append(float(np.corrcoef(left, right)[0, 1]))
                shared_loss_days += sum((left < 0.0) & (right < 0.0))
        maximum = max(correlations, default=0.0)
        result: dict[str, Any] = {
            "peer_count": len(peers),
            "peer_candidate_ids": peer_ids,
            "maximum_daily_pnl_correlation": maximum,
            "shared_loss_days": int(shared_loss_days),
            "behaviorally_distinct": maximum < 0.95,
            "complete": False,
            "method": (
                "conservative_trade_ledger_shared_account_v1_"
                "micro_first_one_contract_per_strategy"
            ),
        }
        if not ledgers.get(candidate_id) or len(peer_ids) < 2:
            result["reason"] = "candidate_ledger_or_two_reference_peers_missing"
            output[candidate_id] = result
            continue
        base = {peer_id: ledgers[peer_id] for peer_id in peer_ids}
        try:
            shared = replay_shared_account(
                {**base, candidate_id: ledgers[candidate_id]}
            )
            result["shared_account_replay"] = shared.to_dict()
            result["complete"] = True
            result["account_hard_risk_violation"] = bool(
                shared.mll_breached or shared.contract_limit_breached
            )
            if role in {"DEFENSIVE", "PORTFOLIO_ONLY"}:
                contribution = compare_account_contribution(
                    base,
                    candidate_id,
                    ledgers[candidate_id],
                    target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
                )
                seed = int(
                    hashlib.sha256(candidate_id.encode()).hexdigest()[:8], 16
                )
                control = matched_random_inclusion_controls(
                    base,
                    candidate_id,
                    ledgers[candidate_id],
                    target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
                    # 63 preregistered assignments give 0.015625 minimum
                    # attainable p-value and adequate resolution at the 0.10
                    # research threshold without letting account controls
                    # monopolize the promotion queue.
                    control_count=63,
                    seed=seed,
                )
                result["account_contribution"] = contribution.to_dict()
                result["matched_account_utility_control"] = control.to_dict()
                result["matched_account_utility_passed"] = bool(
                    contribution.pool_utility_delta > 0.0
                    and control.one_sided_p_value <= 0.10
                    and not contribution.hard_risk_violation
                )
        except Exception as exc:
            result["complete"] = False
            result["hard_account_integrity_failure"] = type(exc).__name__
            result["reason"] = str(exc)[:500]
        output[candidate_id] = result
    return output


def _micro_first_account_ledger(
    ledger: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Normalize standalone frozen sizing to one micro for basket research.

    Standalone Combine/XFA sizing remains frozen and is evaluated separately.
    Shared-account evidence must respect one common 15-contract ceiling, so the
    interaction replay uses an explicit one-micro-per-strategy scheduling role
    rather than naively summing several standalone maxima.
    """

    output: list[dict[str, Any]] = []
    for source in ledger:
        row = dict(source)
        contracts = max(int(row.get("contracts") or 1), 1)
        for key in ("net_pnl", "mae_dollars", "cost"):
            row[key] = float(row.get(key) or 0.0) / contracts
        row["contracts"] = 1
        row["account_sizing_policy"] = "micro_first_one_contract_v1"
        output.append(row)
    return output


def _finalize_decision(row: dict[str, Any]) -> None:
    economic_complete = bool(row["stage_completion"]["full_economic_replay"])
    risk_complete = bool(row["stage_completion"]["full_risk_replay"])
    economic = row["economic"]
    risk = row["risk"]
    robustness = row["robustness"]
    null = row["candidate_null"]
    interaction = row.get("account_interaction") or {}
    role = str(row.get("role") or "")
    topstep = row.get("source_topstep") or {}
    reasons = []
    hard_failures = []
    alpha_role = role in {"COMBINE_PASSER", "XFA_PAYOUT"}
    economic_pass = bool(
        economic_complete
        and int(economic.get("events") or 0) >= 15
        and (
            not alpha_role
            or (
                float(economic.get("net_pnl") or 0.0) > 0.0
                and float(economic.get("cost_stress_1_5x_net") or 0.0) > 0.0
                and bool(economic.get("temporal_transfer_passed"))
            )
        )
    )
    risk_pass = bool(
        risk_complete
        and not risk.get("mll_breached")
        and risk.get("session_flatten_proved")
        and risk.get("contract_limit_proved")
    )
    if not economic_complete:
        hard_failures.append("deterministic_full_economic_replay_incomplete")
    if not risk_complete:
        hard_failures.append("deterministic_full_risk_replay_incomplete")
    if alpha_role and economic_complete and (
        float(economic.get("net_pnl") or 0.0) <= 0.0
        or float(economic.get("cost_stress_1_5x_net") or 0.0) <= 0.0
    ):
        hard_failures.append("negative_or_cost_destroyed_pooled_economics")
    if alpha_role and bool(economic.get("catastrophic_weak_period")):
        hard_failures.append("catastrophic_temporal_contradiction")
    if bool(risk.get("mll_breached")):
        hard_failures.append("catastrophic_mll_breach")
    if risk_complete and not bool(risk.get("session_flatten_proved")):
        hard_failures.append("session_flatten_violation")
    if risk_complete and not bool(risk.get("contract_limit_proved")):
        hard_failures.append("contract_limit_violation")
    if interaction.get("hard_account_integrity_failure"):
        hard_failures.append("shared_account_integrity_failure")
    if not economic_pass:
        reasons.append("role_specific_economic_or_temporal_gate_not_supported")
    if not risk_pass:
        reasons.append("risk_path_gate_not_supported")
    if float(null.get("global_adjusted_probability") or 1.0) > 0.10:
        reasons.append("global_multiple_testing_uncertainty")
    if float(null.get("matched_opportunity_probability") or 1.0) > 0.10:
        reasons.append("matched_opportunity_null_failed")
    if not robustness.get("parameter_neighborhood_passed"):
        reasons.append("parameter_neighborhood_failed")
    if not robustness.get("contract_transfer_passed"):
        reasons.append("contract_transfer_failed")
    if not robustness.get("delayed_signal_positive"):
        reasons.append("delay_stress_failed")
    if not robustness.get("best_trade_removal", {}).get("net_pnl", 0.0) > 0.0:
        reasons.append("best_trade_concentration")
    if not robustness.get("best_day_removal", {}).get("net_pnl", 0.0) > 0.0:
        reasons.append("best_day_concentration")
    if not robustness.get("best_month_removal", {}).get("net_pnl", 0.0) > 0.0:
        reasons.append("best_month_concentration")
    if not robustness.get("best_period_removal", {}).get("net_pnl", 0.0) > 0.0:
        reasons.append("best_period_concentration")
    if not robustness.get("block_bootstrap", {}).get("passed"):
        reasons.append("block_bootstrap_uncertainty")
    if not robustness.get("simple_baseline_suite", {}).get(
        "candidate_outperforms_best_simple_baseline"
    ):
        reasons.append("simpler_baseline_explanation_not_rejected")
    if not interaction.get("complete") or not interaction.get("behaviorally_distinct"):
        reasons.append("account_interaction_incomplete_or_redundant")
    if interaction.get("account_hard_risk_violation"):
        reasons.append("micro_first_shared_account_risk_or_contract_violation")
    role_pass = True
    if role == "COMBINE_PASSER":
        role_pass = bool(topstep.get("complete") and topstep.get("path_candidate"))
        if not role_pass:
            reasons.append("combine_role_path_failed")
    elif role == "XFA_PAYOUT":
        standard = topstep.get("xfa_standard") or {}
        consistency = topstep.get("xfa_consistency") or {}
        cycles = max(
            int(standard.get("payout_cycles_survived") or 0),
            int(consistency.get("payout_cycles_survived") or 0),
        )
        role_pass = bool(
            topstep.get("complete")
            and cycles >= 1
            and (standard.get("survived") or consistency.get("survived"))
        )
        if not role_pass:
            reasons.append("xfa_role_utility_failed")
    else:
        role_pass = bool(interaction.get("matched_account_utility_passed"))
        if not role_pass:
            reasons.append("defensive_matched_account_control_not_supported")
    matched_null = null.get("matched_opportunity_null") or {}
    robustness_complete = bool(
        int((robustness.get("block_bootstrap") or {}).get("draws") or 0) > 0
        and bool(robustness.get("simple_baseline_suite", {}).get("complete"))
        and bool(null.get("source_session_block_sign_flip_complete"))
        and isinstance(robustness.get("parameter_neighborhood_passed"), bool)
        and isinstance(robustness.get("contract_transfer_passed"), bool)
    )
    promotion_complete = bool(
        economic_complete
        and risk_complete
        and bool(matched_null.get("complete"))
        and robustness_complete
        and bool(topstep.get("complete"))
        and bool(interaction.get("complete"))
        and (
            role not in {"DEFENSIVE", "PORTFOLIO_ONLY"}
            or bool(interaction.get("matched_account_utility_control"))
        )
    )
    concentration_pass = bool(
        robustness.get("best_trade_removal", {}).get("net_pnl", 0.0) > 0.0
        and robustness.get("best_day_removal", {}).get("net_pnl", 0.0) > 0.0
        and robustness.get("best_month_removal", {}).get("net_pnl", 0.0) > 0.0
        and robustness.get("best_period_removal", {}).get("net_pnl", 0.0) > 0.0
    )
    promotion_pass = bool(
        promotion_complete
        and economic_pass
        and risk_pass
        and role_pass
        and float(null.get("global_adjusted_probability") or 1.0) <= 0.10
        and float(null.get("matched_opportunity_probability") or 1.0) <= 0.10
        and robustness.get("parameter_neighborhood_passed")
        and robustness.get("contract_transfer_passed")
        and robustness.get("delayed_signal_positive")
        and robustness.get("block_bootstrap", {}).get("passed")
        and robustness.get("simple_baseline_suite", {}).get(
            "candidate_outperforms_best_simple_baseline"
        )
        and concentration_pass
        and interaction.get("complete")
        and interaction.get("behaviorally_distinct")
        and not interaction.get("account_hard_risk_violation")
    )
    row["stage_completion"]["full_promotion_validation"] = promotion_complete
    row["promotion_gate_passed"] = promotion_pass
    if hard_failures:
        decision = "PROMOTION_FAILED"
    elif promotion_pass:
        decision = "PRE_HOLDOUT_READY"
    else:
        decision = "SHADOW_RESEARCH_ONLY"
    row["decision"] = decision
    row["hard_failure_reasons"] = hard_failures
    row["decision_reasons"] = hard_failures + reasons


def _select_pre_holdout_cohort(
    candidate_ids: Sequence[str],
    candidates: Mapping[str, Mapping[str, Any]],
    membership: Mapping[str, str],
    *,
    limit: int,
) -> list[str]:
    selected = []
    clusters = set()
    markets = Counter()
    for candidate_id in candidate_ids:
        cluster = membership[candidate_id]
        market = str(candidates[candidate_id].get("primary_market") or "")
        if cluster in clusters or markets[market] >= 2:
            continue
        selected.append(candidate_id)
        clusters.add(cluster)
        markets[market] += 1
        if len(selected) == limit:
            break
    return selected


def _aggregate(keys: np.ndarray, values: np.ndarray) -> dict[int, float]:
    output: dict[int, float] = {}
    for key, value in zip(keys, values, strict=True):
        output[int(key)] = output.get(int(key), 0.0) + float(value)
    return output


def _maximum_item(values: Mapping[int, float]) -> tuple[str | None, float]:
    if not values:
        return None, 0.0
    key, value = max(values.items(), key=lambda row: (row[1], -row[0]))
    return str(key), float(value)


def _block_bootstrap(daily: Mapping[int, float], *, seed: int) -> dict[str, Any]:
    values = np.asarray([daily[key] for key in sorted(daily)], dtype=float)
    if len(values) < 5:
        return {"draws": 0, "lower_05_net_pnl": None, "probability_positive": 0.0, "passed": False}
    rng = np.random.default_rng(seed)
    block_length = max(2, min(10, int(round(math.sqrt(len(values))))))
    blocks_needed = int(math.ceil(len(values) / block_length))
    starts = rng.integers(0, len(values), size=(BOOTSTRAP_DRAWS, blocks_needed))
    offsets = np.arange(block_length, dtype=np.int64)
    sampled_indices = (starts[:, :, None] + offsets[None, None, :]) % len(values)
    sampled = values[sampled_indices.reshape(BOOTSTRAP_DRAWS, -1)[:, : len(values)]]
    totals = sampled.sum(axis=1)
    lower = float(np.quantile(totals, 0.05))
    probability = float(np.mean(totals > 0.0))
    return {
        "draws": BOOTSTRAP_DRAWS,
        "method": "circular_moving_session_block_bootstrap",
        "block_length": block_length,
        "lower_05_net_pnl": lower,
        "probability_positive": probability,
        "passed": probability >= 0.90,
    }


def _canonical_cache_root() -> Path:
    return Path("/root/hydra-bot/data/cache/turbo_foundry_v2")


def _verify_many(
    paths: Sequence[str | Path], expected: Mapping[str, str], label: str
) -> None:
    for value in paths:
        path = Path(value)
        _verify(path, _expected_hash(path, expected), label)


def _expected_hash(path: Path, expected: Mapping[str, str]) -> str:
    value = expected.get(str(path)) or expected.get(path.name)
    if not value:
        raise EvidenceConversionError(f"Frozen hash missing for {path}")
    return str(value)


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise EvidenceConversionError(f"Frozen {label} missing or changed: {path}")


def _report(payload: Mapping[str, Any], evaluations: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# HYDRA Evidence Conversion Foundry V3",
        "",
        f"- Cohort: `{payload['cohort_id']}`",
        f"- Candidates before clustering: **{payload['candidates_before_clustering']}**",
        f"- Behavioral clusters: **{payload['behavioral_clusters']}**",
        f"- Promotion representatives: **{payload['representative_count']}**",
        f"- Complete decisions: **{payload['promotion_decisions_count']}**",
        f"- FULL_ECONOMIC_REPLAY: **{payload['full_economic_replay_count']}**",
        f"- FULL_RISK_REPLAY: **{payload['full_risk_replay_count']}**",
        f"- FULL_PROMOTION_VALIDATION: **{payload['full_promotion_validation_count']}**",
        f"- PRE_HOLDOUT_READY: **{payload['pre_holdout_ready_count']}**",
        "- Q4 access: **0**",
        "- Paper-ready: **0**",
        "",
        "## Decisions",
        "",
    ]
    for row in evaluations:
        lines.append(
            f"- `{row['candidate_id']}` — **{row['decision']}** — "
            + ", ".join(row.get("decision_reasons") or ["all_pre-holdout_gates_passed"])
        )
    lines.extend(
        [
            "",
            "This report uses development data ending before 2024-10-01. It does not",
            "open Q4, inherit a prior status, authorize orders, or claim funded readiness.",
            "",
        ]
    )
    return "\n".join(lines)
