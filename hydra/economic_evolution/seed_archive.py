from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash


class SeedArchiveError(RuntimeError):
    pass


def build_seed_archive(run_dir: str | Path) -> dict[str, Any]:
    root = Path(run_dir)
    component_bank = _load_json(root / "component_bank.json")
    bank_rows = list(component_bank.get("components") or [])
    bank_by_id = {str(row["sleeve_id"]): dict(row) for row in bank_rows}
    sleeves: dict[str, dict[str, Any]] = {}
    for row in _read_jsonl(root / "structural_sleeves.jsonl"):
        sleeve_id = str(row["sleeve_id"])
        if sleeve_id in bank_by_id:
            sleeves[sleeve_id] = row
    missing = sorted(set(bank_by_id) - set(sleeves))
    if missing:
        raise SeedArchiveError(f"component-bank sleeve specifications missing: {missing}")

    policies = []
    for row in _read_jsonl(root / "rolling_combine_elite_results.jsonl"):
        evaluation = dict(row["evaluation"])
        policies.append(
            {
                "policy": row["policy"],
                # The raw pilot fallback mislabeled all non-pass rows.  The
                # authoritative reconciliation preserves their upstream gate.
                "reconciled_status": "ACCOUNT_POLICY_DIAGNOSTIC_ONLY",
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

    mutations = []
    for row in _read_jsonl(root / "directed_mutation_results.jsonl"):
        mutations.append(
            {
                "parent_policy_id": row["parent_policy_id"],
                "dominant_failure": row["dominant_failure"],
                "decision": row["decision"],
                "exact_change": row["exact_change"],
                "expected_effect": row["expected_effect"],
                "child_policy": row.get("child_policy"),
                "evaluated": bool(row.get("evaluated")),
                "improved": bool(row.get("improved")),
                "utility_delta": row.get("utility_delta"),
                "identical_episode_starts": row.get("identical_episode_starts"),
                "validated": False,
            }
        )

    payload: dict[str, Any] = {
        "schema": "hydra_economic_evolution_seed_archive_v1",
        "source_campaign": "hydra_economic_evolution_pilot_0001",
        "development_only": True,
        "proof_window_consumed": False,
        "component_count": len(sleeves),
        "micro_edge_useful_count": sum(
            row.get("incremental_status") == "MICRO_EDGE_USEFUL"
            for row in bank_rows
        ),
        "policy_count": len(policies),
        "combine_path_count": 0,
        "improved_mutation_count": sum(row["improved"] for row in mutations),
        "sleeves": [
            {
                "specification": sleeves[sleeve_id],
                "pilot_evidence": bank_by_id[sleeve_id],
            }
            for sleeve_id in sorted(sleeves)
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
            "The archive contains development-selected components and may only "
            "seed further research; it is not independent evidence."
        ),
    }
    payload["archive_hash"] = stable_hash(payload)
    return payload


def load_and_verify_seed_archive(path: str | Path) -> dict[str, Any]:
    payload = _load_json(Path(path))
    expected = str(payload.pop("archive_hash", ""))
    if not expected or stable_hash(payload) != expected:
        raise SeedArchiveError("seed archive hash drift")
    payload["archive_hash"] = expected
    if payload.get("development_only") is not True:
        raise SeedArchiveError("seed archive cannot be validation evidence")
    governance = dict(payload.get("governance") or {})
    if (
        governance.get("new_data_purchase") is not False
        or governance.get("q4_access") is not False
        or int(governance.get("broker_connections") or 0) != 0
        or int(governance.get("orders") or 0) != 0
    ):
        raise SeedArchiveError("seed archive governance drift")
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
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SeedArchiveError(f"expected object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


__all__ = [
    "SeedArchiveError",
    "build_seed_archive",
    "load_and_verify_seed_archive",
]
