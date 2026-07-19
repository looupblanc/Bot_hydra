from __future__ import annotations

import gzip
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.mission import economic_evolution_manifest_runtime as manifest_runtime_module
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)
from hydra.production import autonomous_director_runtime as director_runtime
from hydra.production import runtime as production_runtime


def _hashed(value: dict[str, Any], field: str) -> dict[str, Any]:
    value = dict(value)
    value.pop(field, None)
    value[field] = stable_hash(value)
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _episode(
    policy_id: str,
    *,
    scenario: str,
    horizon: int,
    net: float,
    minimum_buffer: float = 4_480.0,
) -> dict[str, Any]:
    return {
        "policy_id": policy_id,
        "scenario": scenario,
        "horizon_trading_days": horizon,
        "coverage_state": "FULL_COVERAGE",
        "episode": {
            "policy_id": policy_id,
            "net_pnl": net,
            "target_progress": net / 9_000.0,
            "minimum_mll_buffer": minimum_buffer,
            "mll_breached": False,
            "maximum_mini_equivalent": 1.0,
            "daily_path": [
                {"day_pnl": net / 2.0},
                {"day_pnl": net / 2.0},
            ],
        },
    }


def _write_gzip(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _rule_snapshot(path: Path) -> None:
    rules = {
        "50K": {
            "account_size_usd": 50_000,
            "profit_target_usd": 3_000,
            "maximum_loss_limit_usd": 2_000,
            "maximum_mini_contracts": 5,
            "consistency_target": 0.50,
            "minimum_trading_days": 2,
        },
        "100K": {
            "account_size_usd": 100_000,
            "profit_target_usd": 6_000,
            "maximum_loss_limit_usd": 3_000,
            "maximum_mini_contracts": 10,
            "consistency_target": 0.50,
            "minimum_trading_days": 2,
        },
        "150K": {
            "account_size_usd": 150_000,
            "profit_target_usd": 9_000,
            "maximum_loss_limit_usd": 4_500,
            "maximum_mini_contracts": 15,
            "consistency_target": 0.50,
            "minimum_trading_days": 2,
        },
    }
    _write_json(
        path,
        {
            "parsed_rule_hash": "a" * 64,
            "account_sizes_usd": [50_000, 100_000, 150_000],
            "account_rules": rules,
        },
    )


def _0034_result(path: Path, *, final_uplift: float = -5.0) -> None:
    bundle_manifest = path.parent / "evidence_bundle/manifest.json"
    bundle_sha = hashlib.sha256(b"{}\n").hexdigest()
    bundle_manifest.parent.mkdir(parents=True, exist_ok=True)
    bundle_manifest.write_text("{}\n", encoding="utf-8")
    value = {
        "status": "COMPLETE",
        "campaign_id": "hydra_selective_order_flow_veto_expansion_0034",
        "campaign_mode": "SELECTIVE_ORDER_FLOW_VETO_EXPANSION",
        "decision": "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK",
        "independently_confirmed": False,
        "evidence_bundle": {
            "manifest_path": str(bundle_manifest),
            "manifest_sha256": bundle_sha,
        },
        "economic_summary": {
            "long_sample": {
                "role_results": {
                    "VALIDATION": {
                        "paired_stressed_uplift_usd": 20.0,
                        "stressed_net_usd": 10.0,
                        "baseline_stressed_net_usd": 0.0,
                    },
                    "FINAL_DEVELOPMENT": {
                        "paired_stressed_uplift_usd": final_uplift,
                        "stressed_net_usd": 15.0,
                        "baseline_stressed_net_usd": 20.0,
                    },
                }
            }
        },
    }
    _write_json(path, _hashed(value, "result_hash"))


def test_bounded_0034_decision_kills_incremental_veto_without_promotion(
    tmp_path: Path,
) -> None:
    source = tmp_path / "0034.json"
    _0034_result(source)

    result = director_runtime._exploitation_worker(str(source))

    assert result["veto_incremental_gate_passed"] is False
    assert result["baseline_retained_as_reference"] is True
    assert result["baseline_independently_confirmed"] is False
    assert result["promotion_status"] is None
    assert result["fresh_confirmation_attempts_remaining"] == 1


def test_legal_frontier_scans_gzip_and_keeps_upper_bound_non_promotable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "episodes/policies.jsonl.gz"
    rows: list[dict[str, Any]] = []
    for horizon in (5, 10, 20):
        rows.extend(
            [
                _episode("policy_fast", scenario="NORMAL", horizon=horizon, net=2_000.0),
                _episode(
                    "policy_fast",
                    scenario="STRESSED_1_5X",
                    horizon=horizon,
                    net=1_800.0,
                ),
                _episode("policy_slow", scenario="NORMAL", horizon=horizon, net=100.0),
                _episode(
                    "policy_slow",
                    scenario="STRESSED_1_5X",
                    horizon=horizon,
                    net=-100.0,
                ),
            ]
        )
    _write_gzip(source, rows)
    rules = tmp_path / "rules.json"
    _rule_snapshot(rules)

    result = director_runtime._exploration_worker(
        (str(source),), str(rules), (0.5, 1.0, 2.0), 256
    )

    assert result["selected_policy_ids"][0] == "policy_fast"
    assert result["selected_policy_count"] == 2
    assert result["episode_rows_read"] == len(rows)
    assert result["selected_episode_rows_reloaded"] == len(rows)
    assert any(
        row["account_size_usd"] == 50_000
        and row["scenario"] == "STRESSED_1_5X"
        and row["passes"] > 0
        for row in result["uniform_legal_frontier"]
    )
    assert result["non_deployable_upper_bound"]["promotable"] is False
    assert result["causal_quality_tier_frontier"]["promotable"] is False
    assert result["promotion_status"] is None


def test_exact_result_metrics_count_only_chronological_exact_paths() -> None:
    exact = {
        "counters": {
            "qd_selected_candidate_count": 2,
            "exact_account_replays": 40,
            "exact_normal_account_replays": 20,
            "exact_stressed_account_replays": 20,
        },
        "best_exact_frontier_point": {"candidate_id": "candidate-a"},
        "results": [
            {
                "frontier": [
                    {
                        "candidate_id": "candidate-a",
                        "legally_executable": True,
                        "account_rule_compliant": True,
                        "normal": {"pass_count": 2, "pass_rate": 0.10},
                        "stressed": {
                            "pass_count": 1,
                            "pass_rate": 0.05,
                            "net_total_usd": 250.0,
                        },
                    }
                ]
            },
            {
                "frontier": [
                    {
                        "candidate_id": "candidate-b",
                        "legally_executable": False,
                        "normal": None,
                        "stressed": None,
                    }
                ]
            },
        ],
    }

    metrics = director_runtime._exact_result_metrics({"EXACT_0029": exact})

    assert metrics["exact_account_replays"] == 2
    assert metrics["exact_account_episode_replays"] == 40
    assert metrics["normal_pass_candidate_count"] == 1
    assert metrics["stressed_pass_candidate_count"] == 1
    assert metrics["positive_stressed_candidate_count"] == 1
    assert metrics["best_stressed_pass_rate"] == pytest.approx(0.05)


def test_artifact_compatibility_and_recurring_epoch_resume_are_non_mutating(
    tmp_path: Path,
) -> None:
    old_hash = "a" * 64
    manifest = {
        "manifest_hash": "b" * 64,
        "compatible_artifact_manifest_hashes": [old_hash],
    }
    assert director_runtime._artifact_manifest_compatible(
        {"manifest_hash": old_hash}, manifest
    )

    output = tmp_path / "output"
    branch = output / "branch_results"
    branch.mkdir(parents=True)
    for name in (
        "epoch_0004_timeframe_000000.json",
        "epoch_0004_mechanism_000000.json",
    ):
        _write_json(
            branch / name,
            director_runtime._with_hash({"candidate_count": 1}, "result_hash"),
        )
    prior = {"checkpoint_sequence": 7, "state": "ROBUSTNESS_ACTIVE"}
    state, results, exhausted = director_runtime._run_recurring_niche_epoch(
        epoch=4,
        root=tmp_path,
        manifest=manifest,
        output=output,
        live_writer=object(),
        branch_writer=object(),
        initial_results={},
        prior_state=prior,
        started=0.0,
        heartbeat_seconds=1.0,
        dimensions=("TIMEFRAME", "MECHANISM"),
        candidate_offset=0,
    )
    assert state == prior
    assert set(results) == {"4:EXPLOITATION", "4:EXPLORATION"}
    assert exhausted is False


def test_recurring_empty_pair_seals_one_exhaustion_transition_and_stops_sharding(
    tmp_path: Path,
) -> None:
    manifest = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "b" * 64,
        "source_commit": "c" * 40,
    }
    output = tmp_path / "output"
    branch = output / "branch_results"
    branch.mkdir(parents=True)
    empty_results: dict[str, dict[str, Any]] = {}
    for lane, dimension in (
        ("EXPLOITATION", "TIMEFRAME"),
        ("EXPLORATION", "MECHANISM"),
    ):
        value = director_runtime._with_hash(
            {
                "status": "COMPLETE_BOUNDED_EXISTING_EVIDENCE_FEASIBILITY",
                "decision": "NO_POSITIVE_MEDIAN_STRESSED_NICHE",
                "candidate_count": 0,
                "candidate_offset": 100_608,
                "niche_dimension": dimension,
            },
            "result_hash",
        )
        empty_results[lane] = value
        _write_json(
            branch / f"epoch_0528_{dimension.lower()}_100608.json",
            value,
        )

    prior = {
        "checkpoint_sequence": 41,
        "state": "ROBUSTNESS_ACTIVE",
        "successor_feasibility_screens_completed": 100_000,
    }
    live_writer = director_runtime.AtomicResultWriter(output, immutable=False)
    branch_writer = director_runtime.AtomicResultWriter(branch)
    call = dict(
        epoch=528,
        root=tmp_path,
        manifest=manifest,
        output=output,
        live_writer=live_writer,
        branch_writer=branch_writer,
        initial_results={},
        prior_state=prior,
        started=0.0,
        heartbeat_seconds=1.0,
        dimensions=("TIMEFRAME", "MECHANISM"),
        candidate_offset=100_608,
    )

    state, results, exhausted = director_runtime._run_recurring_niche_epoch(**call)

    assert exhausted is True
    assert set(results) == {"528:EXPLOITATION", "528:EXPLORATION"}
    assert state["stage"] == "SOURCE_BANK_EXHAUSTED"
    assert state["next_action"] == "DISPATCH_SUCCESSOR_ECONOMIC_LANES"
    assert state["source_bank_exhausted"] is True
    receipt_path = branch / "source_bank_exhausted.json"
    first_receipt = receipt_path.read_bytes()
    assert director_runtime._read_hashed(receipt_path, "result_hash")[
        "decision"
    ] == "SOURCE_BANK_EXHAUSTED"

    call["prior_state"] = state
    resumed, _, resumed_exhausted = director_runtime._run_recurring_niche_epoch(
        **call
    )

    assert resumed_exhausted is True
    assert resumed["stage"] == "SOURCE_BANK_EXHAUSTED"
    assert receipt_path.read_bytes() == first_receipt
    ledger = (tmp_path / "mission/state/decision_ledger.jsonl").read_text(
        encoding="utf-8"
    )
    assert ledger.count('"decision":"SOURCE_BANK_EXHAUSTED"') == 1
    assert not list(branch.glob("epoch_0529_*.json"))


def test_source_bank_exhaustion_requires_both_completed_lanes_empty() -> None:
    complete_empty = {
        "status": "COMPLETE_BOUNDED_EXISTING_EVIDENCE_FEASIBILITY",
        "candidate_count": 0,
    }
    complete_nonempty = {**complete_empty, "candidate_count": 1}

    assert director_runtime._recurring_pair_exhausted(
        {"EXPLOITATION": complete_empty, "EXPLORATION": complete_empty}
    )
    assert not director_runtime._recurring_pair_exhausted(
        {"EXPLOITATION": complete_empty, "EXPLORATION": complete_nonempty}
    )
    assert not director_runtime._recurring_pair_exhausted(
        {"EXPLOITATION": complete_empty}
    )


def test_post_composite_metrics_add_unique_book_evidence_denominators() -> None:
    composite = {
        "aggregate_counters": {
            "exact_normal_account_replays": 10,
            "exact_stressed_account_replays": 10,
            "exact_account_replays": 20,
        },
        "completed_candidate_count": 2,
        "source_inventory": {
            "sealed_initial_candidate_ids": ["candidate-a", "candidate-b"]
        },
        "candidate_pass_sets": {
            "normal": ["candidate-a"],
            "stressed": ["candidate-a"],
        },
        "best_exact_frontier_point": {
            "candidate_id": "candidate-a",
            "normal": {"pass_rate": 0.10},
            "stressed": {"pass_rate": 0.05},
        },
    }
    book = {
        "policy_id": "book-a",
        "summaries": {
            "NORMAL": {"5": {"pass_count": 1, "pass_rate": 0.25}},
            "STRESSED_1_5X": {
                "5": {"pass_count": 1, "pass_rate": 0.20, "net_total": 100.0}
            },
        },
    }
    metrics = director_runtime._exact_result_metrics(
        {
            "EXACT_0029_COMPOSITE": composite,
            "MARGINAL_BOOKS": {
                "book_results": [book],
                "counts": {
                    "completed_episode_count": 12,
                    "primary_book_exact_replay_count": 1,
                    "supporting_policy_exact_replay_count": 3,
                },
            },
        }
    )

    assert metrics["exact_account_replays"] == 3
    assert metrics["control_policy_replay_operations"] == 3
    assert metrics["exact_account_episode_replays"] == 32
    assert metrics["normal_account_replays"] == 16
    assert metrics["stressed_account_replays"] == 16
    assert metrics["normal_pass_candidate_count"] == 2
    assert metrics["stressed_pass_candidate_count"] == 2
    assert metrics["best_normal_pass_rate"] == 0.25
    assert metrics["best_stressed_pass_rate"] == 0.20


def test_post_book_metrics_include_consistency_direct_exact_work() -> None:
    composite = {
        "aggregate_counters": {
            "exact_normal_account_replays": 10,
            "exact_stressed_account_replays": 10,
            "exact_account_replays": 20,
        },
        "completed_candidate_count": 2,
        "source_inventory": {
            "sealed_initial_candidate_ids": ["candidate-a", "candidate-b"]
        },
        "candidate_pass_sets": {
            "normal": ["candidate-a"],
            "stressed": ["candidate-a"],
        },
        "best_exact_frontier_point": {
            "candidate_id": "candidate-a",
            "normal": {"pass_rate": 0.10},
            "stressed": {"pass_rate": 0.05},
        },
    }
    direct_policy = {
        "policy_id": "direct-a",
        "summaries": {
            "NORMAL": {"5": {"pass_count": 2, "pass_rate": 0.20}},
            "STRESSED_1_5X": {
                "5": {"pass_count": 1, "pass_rate": 0.10, "net_total": 250.0}
            },
        },
    }

    metrics = director_runtime._exact_result_metrics(
        {
            "EXACT_0029_COMPOSITE": composite,
            "CONSISTENCY_DIRECT": {
                "selected_policy_results": [direct_policy],
                "counts": {
                    "completed_episode_count": 24,
                    "direct_policy_exact_replay_count": 2,
                    "identity_control_exact_replay_count": 1,
                },
            },
        }
    )

    assert metrics["exact_account_replays"] == 4
    assert metrics["control_policy_replay_operations"] == 1
    assert metrics["exact_account_episode_replays"] == 44
    assert metrics["normal_account_replays"] == 22
    assert metrics["stressed_account_replays"] == 22
    assert metrics["normal_pass_candidate_count"] == 2
    assert metrics["stressed_pass_candidate_count"] == 2
    assert metrics["best_normal_pass_rate"] == 0.20
    assert metrics["best_stressed_pass_rate"] == 0.10


def test_post_consistency_metrics_include_event_time_safety_frontier() -> None:
    composite = {
        "aggregate_counters": {
            "exact_normal_account_replays": 10,
            "exact_stressed_account_replays": 10,
            "exact_account_replays": 20,
        },
        "completed_candidate_count": 2,
        "source_inventory": {
            "sealed_initial_candidate_ids": ["candidate-a", "candidate-b"]
        },
        "candidate_pass_sets": {
            "normal": ["candidate-a"],
            "stressed": ["candidate-a"],
        },
        "best_exact_frontier_point": {
            "candidate_id": "candidate-a",
            "normal": {"pass_rate": 0.10},
            "stressed": {"pass_rate": 0.05},
        },
    }
    heldout = {
        str(horizon): {
            "BASE": {
                "pass_count": 1 if horizon == 5 else 0,
                "pass_rate": 0.25 if horizon == 5 else 0.0,
                "net_total_usd": 100.0,
            },
            "STRESS_1_5X": {
                "pass_count": 1 if horizon == 5 else 0,
                "pass_rate": 0.20 if horizon == 5 else 0.0,
                "net_total_usd": 80.0,
            },
        }
        for horizon in (5, 10, 20)
    }
    event_safety = {
        "candidate_results": [
            {
                "selected_result": {
                    "policy_id": "event-safe-a",
                    "roles": {"HELD_OUT_DEVELOPMENT": heldout},
                }
            }
        ],
        "counts": {
            "selected_candidate_count": 1,
            "profile_count": 8,
            "exact_episode_count": 20,
        },
    }

    results = {
        "EXACT_0029_COMPOSITE": composite,
        "EVENT_TIME_SAFETY": event_safety,
    }
    metrics = director_runtime._exact_result_metrics(results)

    assert metrics["exact_account_replays"] == 10
    assert metrics["exact_account_episode_replays"] == 40
    assert metrics["normal_account_replays"] == 20
    assert metrics["stressed_account_replays"] == 20
    assert metrics["control_policy_replay_operations"] == 1
    assert metrics["normal_pass_candidate_count"] == 2
    assert metrics["stressed_pass_candidate_count"] == 2
    assert metrics["best_normal_pass_rate"] == 0.25
    assert metrics["best_stressed_pass_rate"] == 0.20

    manifest = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
    }
    state = director_runtime._state_payload(
        manifest,
        sequence=1,
        state="ROBUSTNESS_ACTIVE",
        stage="EVENT_TIME_SAFETY_FRONTIER_RECONCILED",
        branch_results=results,
        next_action="TERMINALIZE_EVENT_TIME_SAFETY_REPAIR",
    )
    kpis = director_runtime._kpis(
        manifest,
        state,
        results,
        director_runtime.time.monotonic(),
    )
    assert state["event_time_safety_candidate_count"] == 1
    assert state["event_time_safety_profile_count"] == 8
    assert state["event_time_safety_exact_episode_count"] == 20
    assert kpis["event_time_safety_candidate_count"] == 1
    assert kpis["event_time_safety_profile_count"] == 8
    assert kpis["event_time_safety_exact_episode_count"] == 20


def test_event_time_safety_runtime_worker_rejects_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    safe = {
        "status": "COMPLETE_BOUNDED_EVENT_TIME_SAFETY_SHARD",
        "counts": {
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "promotion_status": None,
    }
    monkeypatch.setattr(
        director_runtime,
        "build_autonomous_event_time_safety_frontier",
        lambda *_args, **_kwargs: safe,
    )

    assert director_runtime._event_time_safety_from_root_worker(
        ".", shard_index=0, shard_count=2
    ) == safe

    unsafe = deepcopy(safe)
    unsafe["counts"] = {**safe["counts"], "orders": 1}
    monkeypatch.setattr(
        director_runtime,
        "build_autonomous_event_time_safety_frontier",
        lambda *_args, **_kwargs: unsafe,
    )
    with pytest.raises(
        director_runtime.AutonomousDirectorRuntimeError,
        match="event-time safety worker attempted a status side effect",
    ):
        director_runtime._event_time_safety_from_root_worker(
            ".", shard_index=0, shard_count=2
        )


def test_embedded_post_composite_result_requires_its_own_hash() -> None:
    inner = director_runtime._with_hash(
        {
            "schema": "test-schema",
            "status": "COMPLETE",
            "promotion_status": None,
        },
        "result_hash",
    )
    envelope = {"payload": inner}

    assert director_runtime._verified_inner_result(
        envelope,
        key="payload",
        expected_schema="test-schema",
        expected_status="COMPLETE",
    ) == inner

    changed = dict(inner)
    changed["status"] = "FORGED"
    with pytest.raises(
        director_runtime.AutonomousDirectorRuntimeError,
        match="embedded economic result identity/hash drift",
    ):
        director_runtime._verified_inner_result(
            {"payload": changed},
            key="payload",
            expected_schema="test-schema",
            expected_status="COMPLETE",
        )


def test_relay_shard_resume_validates_manifest_hash_and_partition(
    tmp_path: Path,
) -> None:
    manifest = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
    }
    inner = director_runtime._with_hash(
        {
            "schema": "test-shard-v1",
            "status": "COMPLETE",
            "shard": {"shard_index": 0, "shard_count": 2},
            "promotion_status": None,
        },
        "result_hash",
    )
    envelope = director_runtime._post_source_envelope(
        manifest,
        lane_id="EXPLOITATION",
        branch_id="TEST_RELAY_SHARD_00",
        decision="COMPLETE",
        payload_key="relay_shard",
        payload=inner,
        next_action="COMPOSE",
    )
    path = tmp_path / "relay_shard_00.json"
    _write_json(path, envelope)
    original = path.read_bytes()

    first = director_runtime._read_relay_shard(
        path,
        manifest=manifest,
        key="relay_shard",
        expected_schema="test-shard-v1",
        expected_status="COMPLETE",
        expected_index=0,
        expected_count=2,
        label="test relay",
    )
    resumed = director_runtime._read_relay_shard(
        path,
        manifest=manifest,
        key="relay_shard",
        expected_schema="test-shard-v1",
        expected_status="COMPLETE",
        expected_index=0,
        expected_count=2,
        label="test relay",
    )

    assert first == resumed == inner
    assert path.read_bytes() == original
    with pytest.raises(
        director_runtime.AutonomousDirectorRuntimeError,
        match="index/count drift",
    ):
        director_runtime._read_relay_shard(
            path,
            manifest=manifest,
            key="relay_shard",
            expected_schema="test-shard-v1",
            expected_status="COMPLETE",
            expected_index=1,
            expected_count=2,
            label="test relay",
        )


def test_embedded_result_accepts_only_declared_status_set() -> None:
    inner = director_runtime._with_hash(
        {
            "schema": "bank-v1",
            "status": "SHORTAGE",
            "promotion_status": None,
        },
        "result_hash",
    )

    assert director_runtime._verified_inner_result(
        {"bank": inner},
        key="bank",
        expected_schema="bank-v1",
        expected_status=("TARGET_REACHED", "SHORTAGE"),
    ) == inner
    with pytest.raises(
        director_runtime.AutonomousDirectorRuntimeError,
        match="embedded economic result identity/hash drift",
    ):
        director_runtime._verified_inner_result(
            {"bank": inner},
            key="bank",
            expected_schema="bank-v1",
            expected_status=("TARGET_REACHED",),
        )


def test_dispatch_runs_two_worker_epoch_and_publishes_resumable_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path
    manifest_path = root / "config/v7/director.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}\n", encoding="utf-8")
    source_0034 = root / "reports/source_0034/result.json"
    _0034_result(source_0034)
    episode_file = root / "data/episodes/test.jsonl.gz"
    rows = [
        _episode("policy_fast", scenario=scenario, horizon=horizon, net=1_800.0)
        for horizon in (5, 10, 20)
        for scenario in ("NORMAL", "STRESSED_1_5X")
    ]
    _write_gzip(episode_file, rows)
    rule_path = root / "config/rules.json"
    _rule_snapshot(rule_path)
    (root / "mission/state").mkdir(parents=True, exist_ok=True)
    (root / "mission/state/decision_ledger.jsonl").write_text("", encoding="utf-8")
    _write_json(root / "mission/state/CURRENT_STATE.json", {})

    manifest: dict[str, Any] = {
        "campaign_id": "hydra_autonomous_economic_discovery_director_0035",
        "campaign_mode": "AUTONOMOUS_ECONOMIC_DISCOVERY_DIRECTOR",
        "manifest_hash": "c" * 64,
        "source_commit": "d" * 40,
        "runtime": {
            "output_dir": "reports/economic_evolution/director",
            "worker_count": 2,
            "asynchronous_evidence_writer_count": 1,
        },
        "official_rule_snapshot": {"path": "config/rules.json"},
        "branch_portfolio": {
            "lanes": [
                {
                    "source_result_path": source_0034.relative_to(root).as_posix(),
                },
                {
                    "episode_source_globs": ["data/episodes/*.jsonl.gz"],
                    "uniform_scale_factors": [0.5, 1.0, 2.0],
                    "policy_maximum": 256,
                },
            ]
        },
    }
    monkeypatch.setenv("HYDRA_PRODUCTION_TEST_MODE", "1")
    monkeypatch.setattr(
        production_runtime,
        "load_and_validate_production_manifest",
        lambda _: manifest,
    )
    monkeypatch.setattr(
        director_runtime,
        "load_and_validate_production_manifest",
        lambda _: manifest,
    )
    monkeypatch.setattr(
        director_runtime,
        "validate_autonomous_director_manifest",
        lambda *_args, **_kwargs: None,
    )

    state = production_runtime.run_production_manifest(
        manifest_path,
        contract_map_path=root / "contract.json",
        cache_root=root / "cache",
        stop_after="FIRST_EPOCH",
    )

    assert state["state"] == "ROBUSTNESS_ACTIVE"
    assert state["stage"] == "NEXT_DISTINCT_BRANCHES_QUEUED"
    assert state["worker_count"] == 2
    assert len(state["next_branch_cards"]) == 2
    assert all(row["status"] == "QUEUED" for row in state["next_branch_cards"])
    output = root / manifest["runtime"]["output_dir"]
    persisted_state = director_runtime._read_hashed(
        output / "production_state.json", "state_hash"
    )
    persisted_kpis = director_runtime._read_hashed(
        output / "production_kpis.json", "kpi_hash"
    )
    assert persisted_state["worker_count"] == 2
    assert persisted_kpis["workers"] == {"compute": 2, "evidence_writer": 1}
    assert persisted_kpis["normal_episodes_completed"] == persisted_kpis[
        "stressed_episodes_completed"
    ]
    assert (root / "mission/state/AUTONOMOUS_BRANCH_STATE.json").is_file()
    assert (root / "mission/state/ECONOMIC_SCORECARD.json").is_file()
    assert len(
        (root / "mission/state/decision_ledger.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ) == 2

    status = production_runtime.read_live_status(manifest_path)
    assert status["state"]["worker_count"] == 2


def _production_queue_fixture(
    root: Path, *, terminal_result: bool
) -> tuple[EconomicEvolutionManifestRuntime, dict[str, Any], dict[str, Any], bytes]:
    source_commit = "1" * 40
    manifest: dict[str, Any] = {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": "terminal_worm_campaign",
        "source_commit": source_commit,
        "implementation_files": {"live.py": "0" * 64},
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "output_dir": "reports/economic_evolution/terminal_worm_campaign",
            "result_name": "economic_production_result.json",
            "worker_count": 2,
            "asynchronous_evidence_writer_count": 1,
        },
    }
    manifest["manifest_hash"] = stable_hash(manifest)
    manifest_path = root / "config/v7/terminal_manifest.json"
    _write_json(manifest_path, manifest)
    manifest_bytes = manifest_path.read_bytes()
    if terminal_result:
        result = {
            "schema": "hydra_economic_production_result_v1",
            "campaign_id": manifest["campaign_id"],
            "manifest_hash": manifest["manifest_hash"],
            "source_commit": source_commit,
            "status": "COMPLETE",
        }
        _write_json(
            root
            / "reports/economic_evolution/terminal_worm_campaign/economic_production_result.json",
            _hashed(result, "result_hash"),
        )
    entry = {
        "engine": "production_kernel_v1",
        "campaign_id": manifest["campaign_id"],
        "manifest_path": manifest_path.relative_to(root).as_posix(),
        "manifest_file_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "manifest_semantic_hash": manifest["manifest_hash"],
        "worm_tag": "worm/test-terminal",
        "worm_commit": source_commit,
    }
    return (
        EconomicEvolutionManifestRuntime(root, root / "mission/state"),
        entry,
        manifest,
        manifest_bytes,
    )


def _mock_worm_git(
    monkeypatch: pytest.MonkeyPatch, manifest_bytes: bytes, commit: str
) -> None:
    def check_output(args: list[str], **kwargs: Any) -> Any:
        if args[1] == "show":
            return manifest_bytes
        return commit if kwargs.get("text") else commit.encode()

    class Completed:
        returncode = 0

    monkeypatch.setattr(manifest_runtime_module.subprocess, "check_output", check_output)
    monkeypatch.setattr(
        manifest_runtime_module.subprocess,
        "run",
        lambda *_args, **_kwargs: Completed(),
    )


def test_completed_worm_manifest_skips_live_implementation_checksum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, entry, manifest, blob = _production_queue_fixture(
        tmp_path, terminal_result=True
    )
    _mock_worm_git(monkeypatch, blob, str(manifest["source_commit"]))
    monkeypatch.setattr(
        manifest_runtime_module,
        "load_and_validate_production_manifest",
        lambda _path: (_ for _ in ()).throw(AssertionError("live validator called")),
    )

    loaded = runtime._verify_entry(entry)

    assert loaded["manifest_hash"] == manifest["manifest_hash"]


def test_active_worm_manifest_still_runs_live_implementation_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, entry, manifest, blob = _production_queue_fixture(
        tmp_path, terminal_result=False
    )
    _mock_worm_git(monkeypatch, blob, str(manifest["source_commit"]))
    calls: list[Path] = []

    def validate(path: Path) -> dict[str, Any]:
        calls.append(Path(path))
        return manifest

    monkeypatch.setattr(
        manifest_runtime_module, "load_and_validate_production_manifest", validate
    )

    loaded = runtime._verify_entry(entry)

    assert calls == [tmp_path / entry["manifest_path"]]
    assert loaded["manifest_hash"] == manifest["manifest_hash"]
