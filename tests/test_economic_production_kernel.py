from __future__ import annotations

import json
import hashlib
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.account_policy.schema import BasketPolicy
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import EvidenceBundleWriter, verify_evidence_bundle
from hydra.production.episode_evidence import replay_evidence_rows
from hydra.production.policy_factory import (
    ProductionPolicy,
    build_predeclared_control_bank,
    generate_policy_population,
    load_component_candidates,
)
from hydra.production.halving import build_leave_one_block_out_plan
from hydra.production.replay import (
    _episode_row,
    _evaluate_isolated_blocks,
    _past_only_market_rotation,
    _past_only_opportunity_density_gate,
    _restress,
)
import hydra.production.runtime as production_runtime
from hydra.production.runtime import (
    _ProductionRun,
    _block_aware_starts,
    _build_horizon_audit,
    _durable_episode_cache_count,
    _matched_controls_payload,
    _selected_stage6_metrics,
    _write_episode_cache,
)
from hydra.propfirm.combine_episode import TradePathEvent


ROOT = Path(__file__).resolve().parents[1]


def test_result_loader_uses_one_completion_guard_and_honors_depth_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_manifest = tmp_path / "evidence_bundle_manifest.json"
    bundle_manifest.write_text('{"status":"COMPLETE"}\n', encoding="utf-8")
    manifest = {
        "campaign_id": "campaign",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
    }
    payload = {
        "schema": production_runtime.PRODUCTION_RESULT_SCHEMA,
        "campaign_id": manifest["campaign_id"],
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "status": "COMPLETE",
        "evidence_bundle": {
            "bundle_path": str(tmp_path / "bundle"),
            "manifest_path": str(bundle_manifest),
            "manifest_sha256": hashlib.sha256(
                bundle_manifest.read_bytes()
            ).hexdigest(),
        },
    }
    payload["result_hash"] = stable_hash(payload)
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")
    modes: list[bool] = []

    def completion_guard(
        requested_status,
        bundle_path,
        *,
        campaign_id=None,
        deep: bool = True,
    ):
        assert requested_status == "COMPLETE"
        assert bundle_path == str(tmp_path / "bundle")
        assert campaign_id == manifest["campaign_id"]
        modes.append(deep)
        return {"status": "COMPLETE"}

    monkeypatch.setattr(
        production_runtime,
        "guard_campaign_completion",
        completion_guard,
    )

    assert production_runtime.load_and_verify_production_result(
        result_path,
        manifest,
        deep_evidence=False,
    ) == payload
    assert production_runtime.load_and_verify_production_result(
        result_path,
        manifest,
    ) == payload
    assert modes == [False, True]


def _trade(
    component: str,
    market: str,
    day: int,
    *,
    decision: int,
    net: float = 100.0,
    exit_offset: int = 10,
) -> RoutedTrade:
    event = TradePathEvent(
        event_id=f"{component}:{day}:{decision}",
        decision_ns=decision,
        exit_ns=decision + exit_offset,
        session_day=day,
        net_pnl=net,
        gross_pnl=net + 10.0,
        worst_unrealized_pnl=-100.0,
        best_unrealized_pnl=max(net, 0.0) + 50.0,
        quantity=1,
        mini_equivalent=0.1,
        regime="NORMAL",
    )
    return RoutedTrade(component, market, 1, event)


def test_generation_is_seeded_deterministic_and_covers_all_mechanisms() -> None:
    manifest = json.loads(
        (ROOT / "config/v7/economic_evolution_production_0024.json").read_text()
    )
    components = load_component_candidates(manifest, ROOT)
    first = generate_policy_population(components, manifest, count=1_000)
    second = generate_policy_population(components, manifest, count=1_000)
    changed = json.loads(json.dumps(manifest))
    changed["generator"]["seed"] += 1
    third = generate_policy_population(components, changed, count=1_000)

    assert [row.structural_fingerprint for row in first.policies] == [
        row.structural_fingerprint for row in second.policies
    ]
    assert [row.structural_fingerprint for row in first.policies] != [
        row.structural_fingerprint for row in third.policies
    ]
    assert set(first.mechanism_counts) == set(manifest["policy_classes"])
    assert first.mechanism_counts["TARGET_VELOCITY_MLL_PROTECTION"] > 0


def test_control_bank_is_fully_predeclared_before_outcomes() -> None:
    manifest = json.loads(
        (ROOT / "config/v7/economic_evolution_production_0024.json").read_text()
    )
    component_ids = tuple(f"component_{index:02d}" for index in range(6))
    first = build_predeclared_control_bank(component_ids, manifest)
    second = build_predeclared_control_bank(tuple(reversed(component_ids)), manifest)

    expected = 4 * (len(component_ids) + 3 * (1 + 5))
    assert len(first) == expected
    assert [row.to_dict() for row in first] == [row.to_dict() for row in second]
    assert len({row.policy_id for row in first}) == expected
    assert len({row.behavioral_fingerprint for row in first}) == expected
    assert all(row.mechanism == "FIXED_STATIC_RISK_FRONTIER" for row in first)
    assert {
        row.baseline_role for row in first
    } == {"BEST_PARENT_CANDIDATE", "EQUAL_RISK", "RANDOM_SELECTION"}


def test_predeclared_control_bank_resolves_lobo_without_new_policy_ids() -> None:
    manifest = json.loads(
        (ROOT / "config/v7/economic_evolution_production_0024.json").read_text()
    )
    components = tuple(f"component_{index:02d}" for index in range(6))
    controls = build_predeclared_control_bank(components, manifest)
    candidate = ProductionPolicy(
        policy_id="candidate",
        mechanism="FIXED_STATIC_RISK_FRONTIER",
        sleeve_ids=components[:2],
        component_priority=components[:2],
        risk_level=1.0,
        risk_micro_units=4,
        maximum_simultaneous_positions=2,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        route_parameters=(),
        parent_policy_ids=(),
        structural_fingerprint="a" * 64,
        behavioral_fingerprint="b" * 64,
        source_campaign="campaign",
    )

    def rows(policy: ProductionPolicy) -> list[dict[str, object]]:
        contribution = {
            component: 100.0 / len(policy.sleeve_ids)
            for component in policy.sleeve_ids
        }
        return [
            {
                "policy_id": policy.policy_id,
                "episode_id": f"{policy.policy_id}:{block}",
                "horizon": "60_TRADING_DAYS",
                "temporal_block": block,
                "cost_scenario": scenario,
                "terminal_state": "TARGET_REACHED",
                "target_reached": True,
                "mll_breached": False,
                "censored_state": False,
                "net_pnl": 100.0,
                "target_progress": 1.0,
                "minimum_mll_buffer": 2_000.0,
                "consistency_ok": True,
                "duration_trading_days": 60,
                "days_to_target": 20.0,
                "component_contribution": contribution,
            }
            for block in ("B1", "B2", "B3", "B4")
            for scenario in ("NORMAL", "STRESSED_1_5X")
        ]

    candidate_rows = rows(candidate)
    control_rows = [row for policy in controls for row in rows(policy)]
    plan = build_leave_one_block_out_plan(
        [candidate.to_dict()],
        candidate_rows,
        predeclared_baseline_policies=[row.to_dict() for row in controls],
        baseline_design_episode_rows=control_rows,
        block_ids=("B1", "B2", "B3", "B4"),
        random_seeds=manifest["matched_controls"]["random_seeds"],
    )
    frozen_ids = {row.policy_id for row in controls}

    assert all(
        fold["baselines"]["design_selected_best_parent"]["policy_id"] in frozen_ids
        and fold["baselines"]["deterministic_equal_risk"]["policy_id"] in frozen_ids
        and all(
            row["policy_id"] in frozen_ids
            for row in fold["baselines"]["fixed_seed_random_selection"]
        )
        for fold in plan["folds"]
    )


def test_expanded_start_sets_are_nested_across_48_96_192() -> None:
    blocks = []
    all_days: list[int] = []
    first_day = 19_000
    for index in range(4):
        lower = first_day + index * 100
        days = tuple(range(lower, lower + 60))
        all_days.extend(days)
        blocks.append(
            {
                "block_id": f"B{index + 1}",
                "start": (date(1970, 1, 1) + timedelta(days=lower)).isoformat(),
                "end": (date(1970, 1, 1) + timedelta(days=lower + 59)).isoformat(),
            }
        )
    runtimes = {
        "component": SimpleNamespace(eligible_session_days=tuple(all_days))
    }
    manifest = {"temporal_blocks": {"blocks": blocks}}
    starts48 = _block_aware_starts(runtimes, manifest, maximum=48)
    starts96 = _block_aware_starts(
        runtimes, manifest, maximum=96, required_starts=starts48
    )
    starts192 = _block_aware_starts(
        runtimes, manifest, maximum=192, required_starts=starts96
    )

    assert len(starts48) == 48
    assert len(starts96) == 96
    assert len(starts192) == 192
    assert set(starts48).issubset(starts96)
    assert set(starts96).issubset(starts192)


def test_horizon_audit_reports_censoring_and_handles_no_survivor() -> None:
    base = {
        "policy_id": "policy",
        "cost_scenario": "NORMAL",
        "target_progress": 0.8,
        "days_to_target": None,
        "net_pnl": 100.0,
        "costs": 10.0,
        "target_reached": False,
        "mll_breached": False,
        "censored_state": True,
        "terminal_state": "DATA_CENSORED",
    }
    rows = [base, {**base, "cost_scenario": "STRESSED_1_5X", "net_pnl": 90.0}]
    audit = _build_horizon_audit({"20_TRADING_DAYS": rows})

    assert audit["by_horizon"]["20_TRADING_DAYS"]["NORMAL"][
        "pass_probability_observed_fraction"
    ] == 0.0
    assert audit["by_horizon"]["20_TRADING_DAYS"]["NORMAL"][
        "censored_episode_count"
    ] == 1
    empty = _build_horizon_audit(
        {
            "20_TRADING_DAYS": [],
            "FULL_AVAILABLE_CHRONOLOGICAL_HORIZON": [],
        }
    )
    assert empty["by_horizon"]["20_TRADING_DAYS"] == {}


def test_rolling_two_hour_allocation_uses_wall_clock_and_host_ticks() -> None:
    run = object.__new__(_ProductionRun)
    run.state = {
        "rolling_two_hour_allocation_samples": [
            {
                "wall_seconds": 60.0,
                "hot_economic_seconds": 48.0,
                "cold_safety_seconds": 6.0,
                "engineering_reporting_seconds": 6.0,
                "host_cpu_busy_ticks": 300,
                "host_cpu_total_ticks": 400,
            },
            {
                "wall_seconds": 40.0,
                "hot_economic_seconds": 32.0,
                "cold_safety_seconds": 4.0,
                "engineering_reporting_seconds": 4.0,
                "host_cpu_busy_ticks": 150,
                "host_cpu_total_ticks": 200,
            },
        ]
    }

    metrics = run._rolling_allocation_metrics()

    assert metrics == {
        "window_seconds": 100.0,
        "hot_fraction": 0.8,
        "cold_fraction": 0.1,
        "engineering_fraction": 0.1,
        "host_cpu_fraction": 0.75,
    }


def test_sealed_bundle_recovery_writes_result_before_complete_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign_id = "recovery_test"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    config_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    final = tmp_path / "cache" / f"{campaign_id}.evidence-v1"
    (final / "outputs").mkdir(parents=True)
    (final / "identity.json").write_text(
        json.dumps(
            {
                "campaign_id": campaign_id,
                "source_commit": "d" * 40,
                "configuration_sha256": config_sha,
            }
        ),
        encoding="utf-8",
    )
    outputs = {
        "campaign_summary": {"confirmation_ready_candidate_ids": []},
        "failure_vectors": {},
        "pareto_archive": {"stage_decisions": [], "crossfit": {}},
        "next_campaign_recommendations": {
            "recommendation": {"action": "QUEUE_NEXT_MANIFEST"}
        },
    }
    for name, value in outputs.items():
        (final / "outputs" / f"{name}.json").write_text(
            json.dumps(value), encoding="utf-8"
        )

    events: list[str] = []
    receipt = SimpleNamespace(
        bundle_path=str(final),
        manifest_sha256="a" * 64,
        to_dict=lambda: {
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
            "bundle_path": str(final),
            "manifest_path": str(final / "evidence_bundle_manifest.json"),
            "manifest_sha256": "a" * 64,
            "bundle_content_sha256": "b" * 64,
            "dataset_row_counts": {"episodes": 2},
        },
    )
    monkeypatch.setattr(
        production_runtime,
        "recover_finalized_evidence_bundle",
        lambda *args, **kwargs: (events.append("recover") or receipt),
    )
    monkeypatch.setattr(
        production_runtime,
        "build_final_result_payload",
        lambda **kwargs: {"status": "COMPLETE", "result_hash": "x"},
    )
    monkeypatch.setattr(
        production_runtime,
        "load_and_verify_production_result",
        lambda *args, **kwargs: (events.append("verify") or {"status": "COMPLETE"}),
    )

    run = object.__new__(_ProductionRun)
    run.root = tmp_path
    run.campaign_id = campaign_id
    run.manifest_path = manifest_path
    run.manifest = {
        "campaign_id": campaign_id,
        "source_commit": "d" * 40,
        "manifest_hash": "e" * 64,
        "runtime": {"result_name": "result.json"},
        "evidence_bundle": {
            "destination": "cache",
            "lightweight_manifest_path": "reports/receipt.json",
        },
    }
    run.output_dir = tmp_path / "output"
    run.output_dir.mkdir()
    run.output_writer = SimpleNamespace(
        write_json=lambda *args, **kwargs: events.append("result_write")
    )
    run._kpis = lambda: {"state": "FINALIZING"}
    run._reconcile_completed_result_snapshots = (
        lambda result: events.append("reconcile:COMPLETE")
    )

    result = run._recover_sealed_bundle_result()

    assert result["status"] == "COMPLETE"
    assert events.index("result_write") < events.index("verify")
    assert events.index("verify") < events.index("reconcile:COMPLETE")


def test_existing_result_reconciles_stale_live_snapshots_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    (output / "result.json").write_text("{}\n", encoding="utf-8")
    result = {
        "status": "COMPLETE",
        "evidence_bundle": {
            "bundle_path": str(tmp_path / "sealed"),
            "manifest_sha256": "a" * 64,
        },
        "economic_results": {
            "production_counters": {
                "serious_exact_account_replays": 512,
                "combine_episodes_completed": 123_456,
                "normal_episodes_completed": 61_728,
                "stressed_episodes_completed": 61_728,
            },
            "confirmation_ready_candidate_ids": ["finalist"],
            "matched_controls_status": "EXECUTED",
            "null_status": "NOT_EXECUTED_DISTINCT_ROUTING_NULL_PENDING",
        },
        "kpis": {
            "schema": production_runtime.PRODUCTION_KPI_SCHEMA,
            "campaign_id": "campaign",
            "manifest_hash": "b" * 64,
            "source_commit": "c" * 40,
            "state": "FINALIZING",
            "policies_proposed": 20_000,
            "unique_policies_screened": 4_096,
            "combine_episodes_completed": 1,
        },
        "successive_halving": {
            "stage_decisions": [
                {
                    "stage": "STAGE_4_ROBUSTNESS_CROSSFIT",
                    "output_count": 8,
                },
                {
                    "stage": "STAGE_5_EXPANDED_96_STARTS",
                    "output_count": 3,
                },
            ]
        },
        "autonomous_next_action": {"action": "QUEUE_NEXT_MANIFEST"},
    }
    monkeypatch.setattr(
        production_runtime,
        "load_and_verify_production_result",
        lambda *args, **kwargs: result,
    )
    run = object.__new__(_ProductionRun)
    run.output_dir = output
    run.live_writer = production_runtime.AtomicResultWriter(output, immutable=False)
    run.campaign_id = "campaign"
    run.manifest = {
        "campaign_id": "campaign",
        "manifest_hash": "b" * 64,
        "source_commit": "c" * 40,
        "runtime": {"result_name": "result.json"},
    }
    run.state = {
        "campaign_id": "campaign",
        "manifest_hash": "b" * 64,
        "source_commit": "c" * 40,
        "state": "FINALIZING",
        "checkpoint_sequence": 7,
        "combine_episodes_completed": 1,
    }
    run.summaries = {}
    run.population_summary = {}
    run.cache_hit_rate = 0.0

    checked = run.execute()
    first_state = json.loads((output / "production_state.json").read_text())
    first_kpis = json.loads((output / "production_kpis.json").read_text())
    checked_again = run.execute()
    second_state = json.loads((output / "production_state.json").read_text())

    assert checked is result and checked_again is result
    assert first_state["state"] == "COMPLETE"
    assert first_state["combine_episodes_completed"] == 123_456
    assert first_state["confirmation_ready_candidate_ids"] == ["finalist"]
    assert first_kpis["state"] == "COMPLETE"
    assert first_kpis["combine_episodes_completed"] == 123_456
    assert first_kpis["candidates_promoted_96"] == 8
    assert first_kpis["candidates_surviving_96"] == 3
    assert second_state["checkpoint_sequence"] == first_state["checkpoint_sequence"]


def test_durable_episode_counter_is_absolute_across_cache_namespaces(
    tmp_path: Path,
) -> None:
    writer = production_runtime.AtomicResultWriter(tmp_path)
    rows = [{"episode": index} for index in range(4)]
    _write_episode_cache(
        writer,
        "exact_episode_rows/policy_a.json",
        policy_id="policy_a",
        horizon="60_TRADING_DAYS",
        episodes=rows,
    )
    _write_episode_cache(
        writer,
        "horizon_episode_rows/20/policy_a.json",
        policy_id="policy_a",
        horizon="20_TRADING_DAYS",
        episodes=rows[:2],
    )
    _write_episode_cache(
        writer,
        "control_60_episode_rows/control.json",
        policy_id="control",
        horizon="60_TRADING_DAYS",
        episodes=rows,
    )
    writer.write_json("unrelated_summary.json", {"episode_count": 999_999})

    first = _durable_episode_cache_count(tmp_path)
    second = _durable_episode_cache_count(tmp_path)

    assert first == 10
    assert second == first


def test_run_episode_counter_scans_once_then_advances_atomically_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [{"episode": index} for index in range(4)]
    writer = production_runtime.AtomicResultWriter(tmp_path)
    _write_episode_cache(
        writer,
        "exact_episode_rows/existing.json",
        policy_id="existing",
        horizon="60_TRADING_DAYS",
        episodes=rows,
    )

    original_scan = production_runtime._durable_episode_cache_count
    scan_calls = 0

    def counted_scan(payload_dir: Path) -> int:
        nonlocal scan_calls
        scan_calls += 1
        return original_scan(payload_dir)

    monkeypatch.setattr(
        production_runtime, "_durable_episode_cache_count", counted_scan
    )

    run = object.__new__(_ProductionRun)
    run.payload_dir = tmp_path
    run.payload_writer = writer
    run._durable_episode_count = None

    assert run._durable_episode_total() == 4
    assert run._durable_episode_total() == 4
    assert scan_calls == 1

    created = run._write_durable_episode_cache(
        "control_60_episode_rows/new.json",
        policy_id="new",
        horizon="60_TRADING_DAYS",
        episodes=rows[:2],
    )
    duplicate = run._write_durable_episode_cache(
        "control_60_episode_rows/new.json",
        policy_id="new",
        horizon="60_TRADING_DAYS",
        episodes=rows[:2],
    )
    assert created.idempotent_existing is False
    assert duplicate.idempotent_existing is True
    assert run._durable_episode_total() == 6
    assert scan_calls == 1

    # Model a crash after the atomic rename but before the process-local
    # increment.  A fresh run must recover the fsynced cache with one scan.
    _write_episode_cache(
        writer,
        "horizon_episode_rows/20/crash_window.json",
        policy_id="crash_window",
        horizon="20_TRADING_DAYS",
        episodes=rows[:3],
    )
    recovered = object.__new__(_ProductionRun)
    recovered.payload_dir = tmp_path
    recovered.payload_writer = writer
    recovered._durable_episode_count = None

    assert recovered._durable_episode_total() == 9
    assert recovered._durable_episode_total() == 9
    assert scan_calls == 2


def test_no_survivor_control_status_and_stage6_decisions_are_truthful() -> None:
    status = "BASELINE_REPLAY_EXECUTED_COMPARISON_NOT_RUN_NO_SURVIVOR"
    assert _matched_controls_payload(None) == {"status": status}
    metrics = [
        {"policy_id": "selected"},
        {"policy_id": "not_selected_but_threshold_passing"},
    ]
    selected = _selected_stage6_metrics(
        metrics,
        {"selected_policy_ids": ["selected"]},
    )
    assert [row["policy_id"] for row in selected] == ["selected"]


def test_opportunity_density_treats_same_timestamp_atomically() -> None:
    left = _trade("left", "MES", 1, decision=1_000)
    right = _trade("right", "MCL", 1, decision=1_000)
    inputs = {"right": (right,), "left": (left,)}
    result = _past_only_opportunity_density_gate(
        inputs, lookback_minutes=30, minimum_sources=2
    )
    reversed_result = _past_only_opportunity_density_gate(
        dict(reversed(tuple(inputs.items()))),
        lookback_minutes=30,
        minimum_sources=2,
    )

    assert {key: [row.event.event_id for row in value] for key, value in result.items()} == {
        "right": [right.event.event_id],
        "left": [left.event.event_id],
    }
    assert {
        key: [row.event.event_id for row in value]
        for key, value in reversed_result.items()
    } == {
        "left": [left.event.event_id],
        "right": [right.event.event_id],
    }


def test_market_rotation_keeps_distinct_equal_pnl_event_identity() -> None:
    history_z1 = _trade("z1", "Z", 1, decision=100, net=1.0)
    history_z2 = _trade("z2", "Z", 1, decision=200, net=1.0)
    history_a = _trade("a1", "A", 1, decision=300, net=1.0)
    current_z = _trade("znow", "Z", 2, decision=10_000, net=0.0)
    current_a = _trade("anow", "A", 2, decision=10_000, net=0.0)
    result = _past_only_market_rotation(
        {
            "z1": (history_z1,),
            "z2": (history_z2,),
            "a1": (history_a,),
            "znow": (current_z,),
            "anow": (current_a,),
        },
        lookback_sessions=10,
        active_market_count=1,
    )

    assert result["znow"] == (current_z,)
    assert result["anow"] == ()


def test_block_isolated_replay_censors_without_reading_next_block() -> None:
    b1 = _trade("component", "MES", 1, decision=1_000, net=100.0)
    b2 = _trade("component", "MES", 10, decision=10_000, net=9_000.0)
    events = {"component": (b1, b2)}
    basket = BasketPolicy(
        policy_id="policy",
        component_ids=("component",),
        archetype="PRODUCTION_TEST",
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        component_priority=("component",),
        policy_version="hydra_account_policy_v7_2_production_replay_v1",
    )
    summary = _evaluate_isolated_blocks(
        events,
        basket=basket,
        starts=(1,),
        eligible_days_by_start={1: (1,)},
        horizon=60,
    )

    assert summary.episodes[0].net_pnl == 100.0
    assert summary.episodes[0].eligible_days == 1
    assert summary.pass_count == 0


def test_production_mll_breach_realizes_conservative_open_loss() -> None:
    left = _trade("left", "MES", 1, decision=1_000, net=1_000.0, exit_offset=10_000)
    right = _trade("right", "MCL", 1, decision=1_010, net=1_000.0, exit_offset=10_000)
    left = RoutedTrade(
        left.component_id,
        left.market,
        left.side,
        TradePathEvent(**{**left.event.to_dict(), "worst_unrealized_pnl": -2_500.0}),
    )
    right = RoutedTrade(
        right.component_id,
        right.market,
        right.side,
        TradePathEvent(**{**right.event.to_dict(), "worst_unrealized_pnl": -2_500.0}),
    )
    basket = BasketPolicy(
        policy_id="policy",
        component_ids=("left", "right"),
        archetype="PRODUCTION_TEST",
        maximum_simultaneous_positions=2,
        maximum_mini_equivalent=15,
        component_priority=("left", "right"),
        policy_version="hydra_account_policy_v7_2_production_replay_v1",
    )
    summary = _evaluate_isolated_blocks(
        {"left": (left,), "right": (right,)},
        basket=basket,
        starts=(1,),
        eligible_days_by_start={1: (1,)},
        horizon=60,
    )
    episode = summary.episodes[0]

    assert episode.mll_breached is True
    assert episode.net_pnl < 0.0
    assert sum(episode.component_contribution.values()) == episode.net_pnl
    assert episode.daily_path[-1]["balance"] == 150_000.0 + episode.net_pnl

    stressed_events = {
        component_id: tuple(_restress(row, 1.5) for row in rows)
        for component_id, rows in {"left": (left,), "right": (right,)}.items()
    }
    stressed_episode = _evaluate_isolated_blocks(
        stressed_events,
        basket=basket,
        starts=(1,),
        eligible_days_by_start={1: (1,)},
        horizon=60,
    ).episodes[0]
    assert stressed_episode.mll_breached is True
    assert stressed_episode.net_pnl < 0.0
    assert sum(stressed_episode.component_contribution.values()) == pytest.approx(
        stressed_episode.net_pnl
    )


def test_daily_evidence_reconciles_frozen_episode_without_synthetic_timeout_failure() -> None:
    trade = _trade("component", "MES", 19_542, decision=19_542_000, net=100.0)
    events = {"component": (trade,)}
    basket = BasketPolicy(
        policy_id="policy",
        component_ids=("component",),
        archetype="PRODUCTION_TEST",
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        component_priority=("component",),
        policy_version="hydra_account_policy_v7_2_production_replay_v1",
    )
    summary = _evaluate_isolated_blocks(
        events,
        basket=basket,
        starts=(19_542,),
        eligible_days_by_start={19_542: (19_542,)},
        horizon=60,
    )
    policy = ProductionPolicy(
        policy_id="policy",
        mechanism="FIXED_STATIC_RISK_FRONTIER",
        sleeve_ids=("component",),
        component_priority=("component",),
        risk_level=1.0,
        risk_micro_units=4,
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        route_parameters=(),
        parent_policy_ids=(),
        structural_fingerprint=stable_hash({"policy": 1}),
        behavioral_fingerprint=stable_hash({"behavior": 1}),
        source_campaign="campaign",
    )
    raw = _episode_row(
        policy,
        summary.episodes[0],
        scenario="NORMAL",
        horizon=60,
        events=events,
    )
    replay = {
        "normal": summary.to_dict(),
        "stressed_1_5x": summary.to_dict(),
        "normal_episodes": [raw],
        "stressed_episodes": [raw],
    }
    manifest = {
        "temporal_blocks": {
            "blocks": [
                {"block_id": "B1", "start": "2023-07-03", "end": "2023-10-05"}
            ]
        }
    }
    episodes, paths = replay_evidence_rows(replay, manifest)

    assert episodes[0]["terminal_state"] == "DATA_CENSORED"
    assert episodes[0]["censored_state"] is True
    normal_paths = [row for row in paths if row["cost_scenario"] == "NORMAL"]
    assert sum(row["daily_pnl"] for row in normal_paths) == episodes[0]["net_pnl"]
    assert sum(row["costs"] for row in normal_paths) == episodes[0]["costs"]
    assert sum(
        sum(row["component_attribution"].values()) for row in normal_paths
    ) == episodes[0]["net_pnl"]
    assert paths[-1]["risk_allocation"][0]["allow"] is True


def test_production_rows_seal_and_pass_deep_evidence_reconciliation(
    tmp_path: Path,
) -> None:
    """Exercise the production row adapter against the authoritative deep validator."""

    trade = _trade("component", "MES", 19_542, decision=1_688_400_000_000_000_000)
    events = {"component": (trade,)}
    basket = BasketPolicy(
        policy_id="policy",
        component_ids=("component",),
        archetype="PRODUCTION_TEST",
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        component_priority=("component",),
        policy_version="hydra_account_policy_v7_2_production_replay_v1",
    )
    summary = _evaluate_isolated_blocks(
        events,
        basket=basket,
        starts=(19_542,),
        eligible_days_by_start={19_542: (19_542,)},
        horizon=60,
    )
    policy = ProductionPolicy(
        policy_id="policy",
        mechanism="FIXED_STATIC_RISK_FRONTIER",
        sleeve_ids=("component",),
        component_priority=("component",),
        risk_level=1.0,
        risk_micro_units=4,
        maximum_simultaneous_positions=1,
        maximum_mini_equivalent=15,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        route_parameters=(),
        parent_policy_ids=(),
        structural_fingerprint="a" * 64,
        behavioral_fingerprint="b" * 64,
        source_campaign="campaign",
    )
    raw = _episode_row(
        policy,
        summary.episodes[0],
        scenario="NORMAL",
        horizon=60,
        events=events,
    )
    replay = {
        "normal": summary.to_dict(),
        "stressed_1_5x": summary.to_dict(),
        "normal_episodes": [raw],
        "stressed_episodes": [raw],
    }
    manifest = {
        "temporal_blocks": {
            "blocks": [
                {"block_id": "B1", "start": "2023-07-03", "end": "2023-10-05"}
            ]
        }
    }
    episodes, paths = replay_evidence_rows(replay, manifest)
    episode_id = str(episodes[0]["episode_id"])
    horizon = str(episodes[0]["horizon"])
    sha = "c" * 64
    identity = {
        "campaign_id": "production_adapter_deep_test",
        "grammar_id": "production_kernel_manifest_v1",
        "policy_fingerprints": {"policy": "a" * 64},
        "component_fingerprints": {"component": "b" * 64},
        "source_commit": "d" * 40,
        "data_fingerprints": {"test_cache": sha},
        "configuration_sha256": sha,
        "seeds": [1],
        "created_at_utc": "2026-07-14T00:00:00Z",
        "expected_coverage": {
            "policy_ids": ["policy"],
            "component_ids": ["component"],
            "required_episode_keys": [
                {"policy_id": "policy", "episode_id": episode_id, "horizon": horizon}
            ],
            "allowed_horizons": [horizon],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }
    campaign = identity["campaign_id"]
    entry_time = "2023-07-05T12:00:00Z"
    exit_time = "2023-07-05T12:00:00.000000010Z"
    records = {
        "component_signals": [
            {
                "campaign_id": campaign,
                "component_id": "component",
                "signal_id": "signal",
                "event_time": entry_time,
                "market": "MES",
                "contract": "MESU3",
                "timeframe": "5m",
                "signal": 1,
                "sizing": 1.0,
                "stop": None,
                "target": None,
                "veto": False,
                "component_role": "TARGET_VELOCITY",
            }
        ],
        "component_entries": [
            {
                "campaign_id": campaign,
                "component_id": "component",
                "trade_id": "trade",
                "entry_time": entry_time,
                "market": "MES",
                "contract": "MESU3",
                "side": "LONG",
                "quantity": 1.0,
                "entry_price": 5_000.0,
                "sizing": 1.0,
                "stop_price": None,
                "target_price": None,
            }
        ],
        "component_exits": [
            {
                "campaign_id": campaign,
                "component_id": "component",
                "trade_id": "trade",
                "exit_time": exit_time,
                "exit_price": 5_020.0,
                "exit_reason": "FROZEN_COMPONENT_EXIT",
            }
        ],
        "component_trades": [
            {
                "campaign_id": campaign,
                "component_id": "component",
                "trade_id": "trade",
                "entry_time": entry_time,
                "exit_time": exit_time,
                "market": "MES",
                "contract": "MESU3",
                "side": "LONG",
                "quantity": 1.0,
                "entry_price": 5_000.0,
                "exit_price": 5_020.0,
                "gross_pnl": 110.0,
                "costs": 10.0,
                "net_pnl": 100.0,
            }
        ],
        "account_policy_membership": [
            {
                "campaign_id": campaign,
                "policy_id": "policy",
                "component_id": "component",
                "risk_allocation": 4.0,
                "component_role": "TARGET_VELOCITY",
            }
        ],
        "account_daily_paths": [
            {**row, "campaign_id": campaign} for row in paths
        ],
        "episodes": [{**row, "campaign_id": campaign} for row in episodes],
        "provenance": [
            {
                "campaign_id": campaign,
                "validator_version": "hydra_evidence_bundle_validator_v1",
                "replay_version": "hydra_production_account_replay_v1",
                "market_data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
                "access_ledger_sha256": sha,
                "reconstruction_flag": False,
                "immutable_checksums": {
                    "configuration": sha,
                    "data:test_cache": sha,
                },
                "recorded_at_utc": "2026-07-14T00:00:00Z",
            }
        ],
    }
    writer = EvidenceBundleWriter.create(tmp_path / "cache", identity)
    for dataset, rows in records.items():
        writer.append_records(dataset, rows, batch_id=f"production:{dataset}")
    writer.write_compact_output("campaign_summary", {"policy_count": 1})
    writer.write_compact_output("failure_vectors", [])
    writer.write_compact_output("pareto_archive", [{"policy_id": "policy"}])
    writer.write_compact_output(
        "next_campaign_recommendations", {"action": "CONTINUE_SUCCESSIVE_HALVING"}
    )
    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=tmp_path / "reports/receipt.json",
    )

    verified = verify_evidence_bundle(receipt.bundle_path, deep=True)
    assert verified["dataset_row_counts"]["episodes"] == 2
