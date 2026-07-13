from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.seed_archive import (
    SeedArchiveError,
    build_seed_archive,
    load_and_verify_seed_archive,
)


def test_seed_archive_is_compact_reconciled_and_hash_checked(
    tmp_path: Path,
) -> None:
    run = tmp_path / "run"
    run.mkdir()
    _write(
        run / "component_bank.json",
        {
            "components": [
                {
                    "sleeve_id": "s1",
                    "incremental_status": "MICRO_EDGE_USEFUL",
                }
            ]
        },
    )
    _write_jsonl(
        run / "structural_sleeves.jsonl",
        [{"sleeve_id": "s1", "market": "ES"}],
    )
    summary = {
        "policy_id": "p1",
        "policy_kind": "STATIC_ACCOUNT_BASKET",
        "episode_start_count": 24,
        "effective_block_count": 4,
        "pass_count": 0,
        "pass_rate": 0.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "target_progress_median": 0.3,
        "target_progress_p25": 0.2,
        "target_progress_p75": 0.4,
        "maximum_target_progress": 0.5,
        "median_episode_net_pnl": 2700.0,
        "consistency_pass_rate": 0.75,
        "projected_days_to_target": 280.0,
        "minimum_mll_buffer": 3000.0,
        "accepted_event_count": 200,
        "conflict_rate": 0.0,
    }
    _write_jsonl(
        run / "rolling_combine_elite_results.jsonl",
        [
            {
                "policy": {"policy_id": "p1"},
                "status": "ACCOUNT_POLICY_RESEARCH_CANDIDATE",
                "failure_vector": {"dominant": "LONG_RECOVERY_TIME"},
                "evaluation": {
                    "episode_start_days": [1, 2, 3, 4],
                    "controlled_base": summary,
                    "controlled_stress_1_5x": summary,
                    "xfa": None,
                },
            }
        ],
    )
    _write_jsonl(
        run / "directed_mutation_results.jsonl",
        [
            {
                "parent_policy_id": "p1",
                "dominant_failure": "LONG_RECOVERY_TIME",
                "decision": "REPLAY_ON_IDENTICAL_STARTS",
                "exact_change": {"allocation_units": 2},
                "expected_effect": "faster",
                "child_policy": {"policy_id": "c1"},
                "evaluated": True,
                "improved": True,
                "utility_delta": 10.0,
                "identical_episode_starts": True,
            }
        ],
    )

    archive = build_seed_archive(run)
    path = tmp_path / "seed.json"
    _write(path, archive)
    loaded = load_and_verify_seed_archive(path)

    assert loaded["component_count"] == 1
    assert loaded["micro_edge_useful_count"] == 1
    assert loaded["policies"][0]["reconciled_status"] == (
        "ACCOUNT_POLICY_DIAGNOSTIC_ONLY"
    )
    assert loaded["improved_mutation_count"] == 1
    assert loaded["governance"]["orders"] == 0

    drifted = dict(archive)
    drifted["component_count"] = 2
    _write(path, drifted)
    with pytest.raises(SeedArchiveError, match="hash drift"):
        load_and_verify_seed_archive(path)


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
