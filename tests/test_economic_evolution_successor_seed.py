from __future__ import annotations

import json
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.economic_evolution.successor_seed import build_successor_seed_archive


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _account(policy_id: str) -> dict[str, object]:
    return {
        "policy_id": policy_id,
        "policy_kind": "ADAPTIVE_ACCOUNT_CONTROLLER",
        "episode_start_count": 24,
        "effective_block_count": 4,
        "pass_count": 0,
        "pass_rate": 0.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "target_progress_median": 0.4,
        "target_progress_p25": 0.1,
        "target_progress_p75": 0.7,
        "maximum_target_progress": 0.9,
        "median_episode_net_pnl": 3600.0,
        "consistency_pass_rate": 0.75,
        "projected_days_to_target": 120.0,
        "minimum_mll_buffer": 2500.0,
        "accepted_event_count": 200,
        "conflict_rate": 0.05,
    }


def test_successor_seed_preserves_specs_and_never_inherits_status(tmp_path: Path) -> None:
    project = tmp_path / "project"
    run = project / "reports/economic_evolution/run"
    run.mkdir(parents=True)
    predecessor = {
        "schema": "hydra_economic_evolution_seed_archive_v1",
        "source_campaign": "previous",
        "development_only": True,
        "proof_window_consumed": False,
        "sleeves": [{"specification": {"sleeve_id": "old"}}],
        "policies": [],
        "mutations": [],
        "governance": {
            "new_data_purchase": False,
            "q4_access": False,
            "broker_connections": 0,
            "orders": 0,
            "status_inheritance": False,
        },
    }
    predecessor["archive_hash"] = stable_hash(predecessor)
    _write_json(project / "reports/seed.json", predecessor)
    prereg = {
        "campaign_id": "campaign_0002",
        "preregistration_hash": "frozen",
        "seed_archive": {"path": "reports/seed.json"},
    }
    _write_json(run / "preregistration_copy.json", prereg)
    _write_json(
        run / "economic_evolution_campaign_result.json",
        {"campaign_id": "campaign_0002"},
    )
    _write_json(
        run / "component_bank.json",
        {
            "components": [
                {"sleeve_id": "old", "incremental_status": "MICRO_EDGE_USEFUL"},
                {"sleeve_id": "new", "incremental_status": "COMPONENT_RESEARCH_ONLY"},
            ]
        },
    )
    _write_jsonl(run / "structural_sleeves.jsonl", [{"sleeve_id": "new"}])
    policy = {
        "policy_id": "policy",
        "sleeve_ids": ["old", "new"],
        "allocation_units": [1, 1],
    }
    evaluation = {
        "episode_start_days": [1, 2, 3, 4],
        "controlled_base": _account("policy"),
        "controlled_stress_1_5x": _account("policy"),
        "xfa": None,
    }
    _write_jsonl(
        run / "rolling_combine_elite_results.jsonl",
        [
            {
                "development_only": True,
                "validated": False,
                "status": "ACCOUNT_POLICY_RESEARCH_CANDIDATE",
                "policy": policy,
                "evaluation": evaluation,
                "failure_vector": {"dominant": "LONG_RECOVERY_TIME", "scores": []},
            }
        ],
    )
    _write_jsonl(
        run / "failure_directed_policy_comparisons.jsonl",
        [
            {
                "parent_policy_id": "parent",
                "dominant_failure": "LONG_RECOVERY_TIME",
                "mutation_kind": "BOUNDED_CONCURRENCY",
                "exact_change": {"maximum": 2},
                "expected_effect": "faster",
                "child_policy": policy,
                "evaluated": True,
                "improved": True,
                "utility_delta": 0.1,
                "identical_episode_starts": True,
            }
        ],
    )

    first = build_successor_seed_archive(run, project_root=project)
    second = build_successor_seed_archive(run, project_root=project)

    assert first == second
    assert first["component_count"] == 2
    assert first["policy_count"] == 1
    assert first["account_research_candidate_count"] == 1
    assert first["dominant_failure_counts"] == {"LONG_RECOVERY_TIME": 1}
    assert first["policies"][0]["next_campaign_status_inherited"] is False
    assert first["governance"]["status_inheritance"] is False
    destination = project / "seed.json"
    _write_json(destination, first)
    assert load_and_verify_seed_archive(destination)["archive_hash"] == first["archive_hash"]
