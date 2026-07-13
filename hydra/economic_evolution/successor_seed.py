from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive


class SuccessorSeedError(RuntimeError):
    pass


def build_successor_seed_archive(
    run_dir: str | Path, *, project_root: str | Path
) -> dict[str, Any]:
    """Freeze one development-only campaign as input to its successor.

    The archive carries specifications and failure classes, never a validated
    status.  It may guide another development campaign but cannot consume or
    replace independent evidence.
    """

    root = Path(run_dir).resolve()
    project = Path(project_root).resolve()
    result = _load_json(root / "economic_evolution_campaign_result.json")
    prereg = _load_json(root / "preregistration_copy.json")
    bank = _load_json(root / "component_bank.json")
    bank_rows = [dict(row) for row in bank.get("components") or ()]
    bank_by_id = {str(row["sleeve_id"]): row for row in bank_rows}
    if not bank_by_id:
        raise SuccessorSeedError("component bank is empty")

    specifications: dict[str, dict[str, Any]] = {}
    predecessor_path = project / str(prereg["seed_archive"]["path"])
    predecessor = load_and_verify_seed_archive(predecessor_path)
    for row in predecessor.get("sleeves") or ():
        spec = dict(row["specification"])
        specifications[str(spec["sleeve_id"])] = spec
    for row in _read_jsonl(root / "structural_sleeves.jsonl"):
        specifications[str(row["sleeve_id"])] = row
    missing = sorted(set(bank_by_id) - set(specifications))
    if missing:
        raise SuccessorSeedError(
            f"component-bank sleeve specifications missing: {missing}"
        )

    policies: list[dict[str, Any]] = []
    failure_counts: Counter[str] = Counter()
    for row in _read_jsonl(root / "rolling_combine_elite_results.jsonl"):
        if row.get("development_only") is not True or row.get("validated") is not False:
            raise SuccessorSeedError("rolling seed must remain development-only")
        failure = str(row["failure_vector"]["dominant"])
        failure_counts[failure] += 1
        evaluation = dict(row["evaluation"])
        policies.append(
            {
                "policy": row["policy"],
                "source_status": str(row["status"]),
                "next_campaign_status_inherited": False,
                "failure_vector": row["failure_vector"],
                "episode_start_days": evaluation["episode_start_days"],
                "controlled_base": _account_summary(evaluation["controlled_base"]),
                "controlled_stress_1_5x": _account_summary(
                    evaluation["controlled_stress_1_5x"]
                ),
                "xfa_summary": _xfa_summary(evaluation.get("xfa")),
                "validated": False,
            }
        )
    if not policies:
        raise SuccessorSeedError("rolling policy seed is empty")

    mutations: list[dict[str, Any]] = []
    mutation_path = root / "failure_directed_policy_comparisons.jsonl"
    for row in _read_jsonl(mutation_path):
        mutations.append(
            {
                "parent_policy_id": row["parent_policy_id"],
                "dominant_failure": row["dominant_failure"],
                "mutation_kind": row["mutation_kind"],
                "exact_change": row["exact_change"],
                "expected_effect": row["expected_effect"],
                "child_policy": row.get("child_policy"),
                "evaluated": bool(row.get("evaluated")),
                "improved": bool(row.get("improved")),
                "utility_delta": row.get("utility_delta"),
                "identical_episode_starts": bool(
                    row.get("identical_episode_starts")
                ),
                "validated": False,
            }
        )

    source_campaign = str(result.get("campaign_id") or "")
    if source_campaign != str(prereg.get("campaign_id") or ""):
        raise SuccessorSeedError("campaign identity drift")
    payload: dict[str, Any] = {
        "schema": "hydra_economic_evolution_seed_archive_v1",
        "source_campaign": source_campaign,
        "source_result_sha256": _sha256(
            root / "economic_evolution_campaign_result.json"
        ),
        "source_preregistration_hash": str(prereg["preregistration_hash"]),
        "development_only": True,
        "proof_window_consumed": False,
        "component_count": len(bank_by_id),
        "micro_edge_useful_count": sum(
            row.get("incremental_status") == "MICRO_EDGE_USEFUL"
            for row in bank_rows
        ),
        "policy_count": len(policies),
        "account_research_candidate_count": sum(
            row["source_status"] == "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
            for row in policies
        ),
        "combine_path_count": sum(
            row["source_status"] == "COMBINE_PATH_CANDIDATE"
            for row in policies
        ),
        "improved_mutation_count": sum(row["improved"] for row in mutations),
        "dominant_failure_counts": dict(sorted(failure_counts.items())),
        "sleeves": [
            {
                "specification": specifications[sleeve_id],
                "development_evidence": bank_by_id[sleeve_id],
            }
            for sleeve_id in sorted(bank_by_id)
        ],
        "policies": sorted(
            policies, key=lambda row: str(row["policy"]["policy_id"])
        ),
        "mutations": sorted(
            mutations,
            key=lambda row: (
                str(row["parent_policy_id"]),
                str((row.get("child_policy") or {}).get("policy_id") or ""),
            ),
        ),
        "governance": {
            "new_data_purchase": False,
            "q4_access": False,
            "broker_connections": 0,
            "orders": 0,
            "status_inheritance": False,
        },
        "CONTRE": (
            "The successor archive is selected on development outcomes and may "
            "only guide failure-directed research; it is not confirmation evidence."
        ),
    }
    payload["archive_hash"] = stable_hash(payload)
    return payload


def _account_summary(value: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "policy_id",
        "policy_kind",
        "episode_start_count",
        "effective_block_count",
        "pass_count",
        "pass_rate",
        "mll_breach_count",
        "mll_breach_rate",
        "target_progress_median",
        "target_progress_p25",
        "target_progress_p75",
        "maximum_target_progress",
        "median_episode_net_pnl",
        "consistency_pass_rate",
        "projected_days_to_target",
        "minimum_mll_buffer",
        "accepted_event_count",
        "conflict_rate",
    )
    return {key: value.get(key) for key in keys}


def _xfa_summary(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not value:
        return None
    rolling = dict(value.get("rolling_xfa") or {})
    return {
        key: rolling.get(key)
        for key in (
            "payout_probability",
            "post_payout_survival_rate",
            "survival_rate",
            "expected_payout_cycles_before_ruin",
            "median_first_payout_day",
            "median_trader_net_payout",
        )
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SuccessorSeedError(f"invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise SuccessorSeedError(f"expected object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise SuccessorSeedError(f"invalid JSONL artifact: {path}") from exc


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = ["SuccessorSeedError", "build_successor_seed_archive"]
