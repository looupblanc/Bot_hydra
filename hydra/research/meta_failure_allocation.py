from __future__ import annotations

import hashlib
import json
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from hydra.mission.calibration_retest_execution import _stable_hash, _strict_json_value
from hydra.research.equity_open_gap_reversal import _write_immutable


VERSION = "meta_failure_allocation_v1"
LANES = (
    "structural_discovery",
    "targeted_mutation",
    "multitimeframe_cross_asset",
    "distribution_hazard",
    "defensive_portfolio",
    "novel_methods",
)


class MetaFailureAllocationError(RuntimeError):
    pass


def run_meta_failure_allocation(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    snapshot: dict[str, Any],
    snapshot_hash: str,
    code_commit: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    task = Path(engineering_task_path)
    if (
        not task.is_file()
        or hashlib.sha256(task.read_bytes()).hexdigest()
        != engineering_task_sha256
    ):
        raise MetaFailureAllocationError("Engineering task missing or changed.")
    if _stable_hash(snapshot) != snapshot_hash:
        raise MetaFailureAllocationError("Frozen mission snapshot hash changed.")
    if len(code_commit) == 40:
        current = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if current != code_commit:
            raise MetaFailureAllocationError(
                "Worker commit differs from queued specification."
            )
    experiments = list(snapshot.get("experiments") or [])
    engine_rows: list[dict[str, Any]] = []
    failure_reasons: Counter[str] = Counter()
    ecology_totals: Counter[str] = Counter()
    family_totals: Counter[str] = Counter()
    for experiment in experiments:
        prototypes = int(experiment.get("structural_prototypes") or 0)
        promising = int(experiment.get("promising_candidates") or 0)
        shadow = int(experiment.get("shadow_candidates") or 0)
        topstep = int(experiment.get("topstep_path_candidates") or 0)
        killed = int(experiment.get("killed_candidates") or 0)
        posterior_shadow_mean = (shadow + 1.0) / (prototypes + 2.0)
        posterior_promotion_mean = (promising + 1.0) / (prototypes + 2.0)
        conclusion = str(experiment.get("scientific_conclusion") or "UNKNOWN")
        failure_reasons[_failure_class(conclusion)] += 1
        ecology = str(experiment.get("market_ecology") or "mixed")
        family = str(experiment.get("mechanism_family") or experiment.get("engine") or "mixed")
        ecology_totals[ecology] += prototypes
        family_totals[family] += prototypes
        engine_rows.append(
            {
                "experiment_id": experiment.get("experiment_id"),
                "engine": experiment.get("engine") or experiment.get("experiment_type"),
                "structural_prototypes": prototypes,
                "promising_candidates": promising,
                "shadow_candidates": shadow,
                "topstep_path_candidates": topstep,
                "killed_candidates": killed,
                "posterior_promotion_probability_mean": posterior_promotion_mean,
                "posterior_shadow_probability_mean": posterior_shadow_mean,
                "expected_validation_cost": float(
                    (experiment.get("performance") or {}).get("total_seconds") or 0.0
                ),
                "failure_class": _failure_class(conclusion),
                "unexplored_false_negative_risk": 1.0 / (prototypes + 2.0) ** 0.5,
            }
        )
    allocation = recommend_allocation(snapshot, engine_rows, failure_reasons)
    if sum(allocation.values()) != 100:
        raise MetaFailureAllocationError("Recommended allocation does not total 100%.")
    if max(allocation.values()) > 25 or allocation["novel_methods"] < 5:
        raise MetaFailureAllocationError("Allocation concentration policy failed.")
    exploration_share = (
        allocation["structural_discovery"] + allocation["novel_methods"]
    )
    if exploration_share < 15:
        raise MetaFailureAllocationError("Exploration share fell below 15%.")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": "META_ALLOCATION_UPDATED_WITH_SHRUNK_CONVERSION_RATES",
        "interpretation_boundary": (
            "This model allocates compute only. It cannot validate, promote, kill or "
            "mutate any strategy."
        ),
        "code_commit": code_commit,
        "snapshot_hash": snapshot_hash,
        "candidate_count": 0,
        "candidates": [],
        "engine_posteriors": engine_rows,
        "failure_reason_counts": dict(failure_reasons),
        "ecology_prototype_counts": dict(ecology_totals),
        "family_prototype_counts": dict(family_totals),
        "recommended_compute_allocation_pct": allocation,
        "constraints": {
            "allocation_total_pct": sum(allocation.values()),
            "maximum_lane_share_pct": max(allocation.values()),
            "minimum_exploration_share_pct": exploration_share,
            "maximum_family_share_pct": 25,
            "maximum_ecology_share_pct": 40,
            "unexplored_lane_suppressed_to_zero": False,
        },
        "false_negative_risk": {
            "method": "beta_1_1_shrinkage_and_nonzero_exploration_floor",
            "registry_is_strategy_evidence": False,
            "unexplored_regions_remain_enabled": True,
            "minimum_novel_method_share_pct": allocation["novel_methods"],
        },
        "governance": {
            "q4_access_count_delta": 0,
            "market_data_rows_read": 0,
            "shared_ledger_writes": 0,
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "performance": {"total_seconds": time.perf_counter() - started},
        "next_recommended_action": "APPLY_ALLOCATION_TO_NEXT_FACTORY_EPOCH",
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "meta_failure_allocation_result.json"
    report_path = destination / "meta_failure_allocation_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
        },
        "report_path": str(report_path),
    }


def recommend_allocation(
    snapshot: dict[str, Any],
    engine_rows: list[dict[str, Any]],
    failure_reasons: Counter[str],
) -> dict[str, int]:
    allocation = {
        "structural_discovery": 25,
        "targeted_mutation": 20,
        "multitimeframe_cross_asset": 20,
        "distribution_hazard": 15,
        "defensive_portfolio": 10,
        "novel_methods": 10,
    }
    total_prototypes = max(int(snapshot.get("strategy_prototypes_generated") or 0), 1)
    shadow_count = int(snapshot.get("shadow_active_candidates") or 0)
    conversion = shadow_count / total_prototypes
    if conversion < 0.001:
        allocation["structural_discovery"] -= 5
        allocation["distribution_hazard"] += 3
        allocation["novel_methods"] += 2
    if int(snapshot.get("executable_baskets") or 0) >= 3:
        allocation["defensive_portfolio"] -= 2
        allocation["targeted_mutation"] += 2
    if failure_reasons.get("TEMPORAL_OR_CONCENTRATION", 0) >= 2:
        allocation["targeted_mutation"] += 2
        allocation["structural_discovery"] -= 2
    # Preserve exact total and caps after deterministic adjustments.
    difference = 100 - sum(allocation.values())
    allocation["novel_methods"] += difference
    return {lane: int(allocation[lane]) for lane in LANES}


def _failure_class(conclusion: str) -> str:
    text = conclusion.upper()
    if "SHADOW" in text and "FOUND" in text:
        return "SHADOW_CONVERSION"
    if "CONCENTR" in text or "INSUFFICIENT" in text or "TEMPORAL" in text:
        return "TEMPORAL_OR_CONCENTRATION"
    if "NO_PRIMARY" in text or "NO_PROMOTION" in text or "FALSIFIED" in text:
        return "ECONOMIC_OR_NULL_FAILURE"
    if "CALIBR" in text or "VALIDATOR" in text:
        return "VALIDATOR_RESEARCH"
    return "OTHER"


def _render_report(payload: dict[str, Any]) -> str:
    allocation = payload["recommended_compute_allocation_pct"]
    lines = [
        "# Meta Failure-Allocation Audit",
        "",
        f"- Conclusion: `{payload['scientific_conclusion']}`",
        f"- Snapshot: `{payload['snapshot_hash']}`",
        "- Strategy evidence: `false`",
        "- Q4 access delta: `0`",
        "- Market-data rows: `0`",
        "",
        "## Recommended next-epoch allocation",
        "",
    ]
    lines.extend(f"- {lane}: `{share}%`" for lane, share in allocation.items())
    lines.append("")
    return "\n".join(lines)
