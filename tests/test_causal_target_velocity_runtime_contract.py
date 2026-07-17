from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.production import causal_target_velocity_runtime as runtime
from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)


def _stage3_row() -> dict[str, object]:
    return {
        "status": "STAGE_3_COMPLETE",
        "candidate_id": "hazard_candidate_a",
        "behavioral_cluster": "cluster_a",
        "normal": {
            "episode_count": 2,
            "pass_count": 2,
            "pass_rate": 1.0,
            "net_total": 12_000.0,
            "target_progress_median": 0.75,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 3_500.0,
        },
        "stressed": {
            "episode_count": 2,
            "pass_count": 1,
            "pass_rate": 1.0,
            "net_total": 10_000.0,
            "target_progress_median": 0.70,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 3_250.0,
        },
        "by_block": {
            "B1": {"stressed_net_total": 2_000.0},
            "B2": {"stressed_net_total": 1_500.0},
        },
        "coverage": {
            "headline_role": (
                "CHRONOLOGICAL_90D_START_ORIGIN_BLOCK_NONOVERLAPPING"
            ),
            "full_coverage_start_count": 2,
            "candidate_full_horizon_start_count": 2,
            "data_censored_start_count": 0,
            "starts": [19_000, 19_090],
            "data_censored_starts": [],
            "shortfall_reported_not_manufactured": 46,
        },
        "matched_control_audits": {
            name: {"all_required_dimensions_matched": True}
            for name in (
                "RANDOM_EVENT_TIMING_MATCHED",
                "SESSION_MATCHED_NULL",
                "DIRECTION_FLIPPED",
            )
        },
        "clean_low_velocity_baseline": {
            "all_identical_starts_replayed": True,
            "source_role": "SEALED_CLEAN_CAUSAL_0027_STANDALONE_REPLAY",
            "normal": {"target_progress_median": 0.0656},
            "stressed": {"target_progress_median": 0.0600},
            "paired_deltas": {"stressed_target_progress_median": 0.64},
            "comparison_role": (
                "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
            ),
            "match_audit": {
                "all_seven_dimensions_exact": True,
                "comparison_is_matched_null": False,
                "nearest_baseline_limitations_explicit": True,
            },
        },
    }


def _stage4_row() -> dict[str, object]:
    return {
        "status": "STAGE_4_COMPLETE",
        "book_id": "hazard_book_a",
        "behavioral_cluster": "book_cluster_a",
        "normal": {
            "pass_count": 3,
            "net_total": 25_000.0,
            "target_progress_median": 0.80,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 3_500.0,
        },
        "stressed": {
            "pass_count": 2,
            "net_total": 20_000.0,
            "target_progress_median": 0.70,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 3_200.0,
        },
        "by_block": {
            "B1": {"stressed_net_total": 2_000.0},
            "B2": {"stressed_net_total": 1_000.0},
        },
        "coverage": {"full_coverage_start_count": 48},
        "matched_controls_complete": True,
        "maximum_stressed_component_profit_share": 0.60,
    }


def _stage2_row() -> dict[str, object]:
    return {
        "status": "STAGE_2_COMPLETE",
        "candidate_id": "hazard_candidate_a",
        "behavioral_cluster": "cluster_a",
        "normal": {
            "terminal": "OPERATIONAL_HORIZON_NOT_REACHED",
            "net_pnl": 10_000.0,
            "target_progress": 0.80,
            "minimum_mll_buffer": 3_500.0,
            "mll_breached": False,
        },
        "stressed": {
            "terminal": "OPERATIONAL_HORIZON_NOT_REACHED",
            "net_pnl": 8_000.0,
            "target_progress": 0.70,
            "minimum_mll_buffer": 3_250.0,
            "mll_breached": False,
        },
    }


def _stage1_row(*, stressed_net: float) -> dict[str, object]:
    return {
        "status": "STAGE_1_COMPLETE",
        "candidate_id": "hazard_candidate_a",
        "behavioral_cluster": "cluster_a",
        "hard_causality_defect_count": 0,
        "screen": {
            "completed_event_count": 20,
            "normal_net_pnl": stressed_net + 100.0,
            "stressed_net_pnl": stressed_net,
            "independent_events_per_20_sessions": 4.0,
            "cost_adjusted_target_velocity": 0.10,
            "favorable_before_adverse_rate": 0.60,
            "day_concentration": 0.20,
        },
        "matched_deltas": {
            "stressed_net_pnl": 50.0,
            "favorable_before_adverse_rate": 0.10,
        },
    }


def test_runtime_has_complete_terminal_and_resume_symbols() -> None:
    assert hasattr(runtime._CausalTargetVelocityRun, "_finalize_campaign")
    assert callable(runtime._load_book_batches)
    assert callable(runtime._system_cpu_ticks)
    assert callable(runtime._terminal_economic_summary)
    assert callable(runtime._terminal_control_summary)
    assert callable(runtime._terminal_failure_vectors)


def test_feature_matrix_discovery_reports_missing_market_symbol(tmp_path: Path) -> None:
    required_arrays = {
        name: {}
        for name in (
            "availability_ns",
            "bar_close",
            "bar_high",
            "bar_low",
            "bar_open",
            "contract_code",
            "decision_ns",
            "feature__past_return_60",
            "feature__past_volatility",
            "segment_code",
            "session_code",
            "session_day",
            "timestamp_ns",
        )
    }
    path = tmp_path / "cl" / "manifest.json"
    path.parent.mkdir()
    path.write_text(
        json.dumps(
            {
                "provenance": {"market": "CL"},
                "arrays": required_arrays,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        runtime.CausalTargetVelocityRuntimeError,
        match="cache-only feature matrices missing: ES",
    ):
        runtime._discover_feature_matrices(tmp_path, ("CL", "ES"))


def test_stage3_selection_requires_explicit_complete_control_inventory() -> None:
    row = _stage3_row()
    row["matched_control_audits"] = {}

    assert runtime._select_stage3_useful([row], maximum=64) == []


def test_stage3_selection_accepts_complete_control_and_coverage_contract() -> None:
    row = _stage3_row()

    assert runtime._select_stage3_useful([row], maximum=64) == [row]


def test_stage3_clean_baseline_is_same_start_economic_not_strict_7d_null() -> None:
    row = _stage3_row()
    row["clean_low_velocity_baseline"] = {
        **dict(row["clean_low_velocity_baseline"]),
        "comparison_role": (
            "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
        ),
        "match_audit": {
            "all_seven_dimensions_exact": False,
            "comparison_is_matched_null": False,
            "nearest_baseline_limitations_explicit": True,
        },
    }

    assert runtime._select_stage3_useful([row], maximum=64) == [row]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("comparison_is_matched_null", True),
        ("nearest_baseline_limitations_explicit", False),
    ),
)
def test_stage3_clean_economic_baseline_requires_explicit_limitations(
    field: str, value: object
) -> None:
    row = _stage3_row()
    row["clean_low_velocity_baseline"] = {
        **dict(row["clean_low_velocity_baseline"]),
        "comparison_role": (
            "SAME_START_NEAREST_CELL_ECONOMIC_BASELINE_NOT_MATCHED_NULL"
        ),
        "match_audit": {
            "all_seven_dimensions_exact": False,
            "comparison_is_matched_null": False,
            "nearest_baseline_limitations_explicit": True,
            field: value,
        },
    }

    assert runtime._select_stage3_useful([row], maximum=64) == []


def test_stage3_clean_economic_baseline_requires_frozen_comparison_role() -> None:
    row = _stage3_row()
    row["clean_low_velocity_baseline"] = {
        **dict(row["clean_low_velocity_baseline"]),
        "comparison_role": "STRICT_MATCHED_NULL",
    }

    assert runtime._select_stage3_useful([row], maximum=64) == []


def test_evidence_only_exact_replay_cannot_advance_from_stage2() -> None:
    row = {
        **_stage2_row(),
        "selection_role": "EVIDENCE_ONLY_DIAGNOSTIC_EXACT_REPLAY",
        "economic_advancement_authorized": False,
    }

    assert runtime._select_stage2([row], maximum=256) == []


def test_stage1_inventory_retains_one_nonpromotional_exact_evidence_fallback() -> None:
    row = _stage1_row(stressed_net=-10.0)

    selected = runtime._select_stage1([row], maximum=1_024)

    assert len(selected) == 1
    assert selected[0]["candidate_id"] == "hazard_candidate_a"
    assert selected[0]["selection_role"] == (
        "EVIDENCE_ONLY_DIAGNOSTIC_EXACT_REPLAY"
    )
    assert selected[0]["economic_advancement_authorized"] is False
    assert row.get("selection_role") is None


def test_stage1_inventory_prefers_economically_eligible_rows_without_fallback() -> None:
    row = _stage1_row(stressed_net=100.0)

    selected = runtime._select_stage1([row], maximum=1_024)

    assert selected == [row]
    assert selected[0].get("selection_role") is None


def test_evidence_only_role_propagates_to_resumed_exact_replay(
    tmp_path: Path,
) -> None:
    candidate_id = "hazard_candidate_a"
    batches = tmp_path / "stage2_exact_replay_batches"
    batches.mkdir()
    exact_row = _stage2_row()
    (batches / "batch_0000.jsonl").write_text(
        json.dumps(exact_row) + "\n",
        encoding="utf-8",
    )
    run = object.__new__(runtime._CausalTargetVelocityRun)
    run.payload_dir = tmp_path
    run.stage2_rows = []
    run.evidence_only_candidate_ids = {candidate_id}
    run._publish = lambda **_updates: None

    rows = run._stage2_exact_replay(
        [SimpleNamespace(candidate_id=candidate_id)],
        matrices={},
        period=(0, 1, 2),
    )

    assert rows[0]["economic_advancement_authorized"] is False
    assert runtime._select_stage2(rows, maximum=256) == []


def test_stage3_selection_fails_closed_without_full_coverage_start() -> None:
    row = _stage3_row()
    row["coverage"] = {
        **dict(row["coverage"]),
        "full_coverage_start_count": 0,
        "data_censored_start_count": 2,
        "starts": [],
        "data_censored_starts": [19_000, 19_090],
    }

    assert runtime._select_stage3_useful([row], maximum=64) == []


@pytest.mark.parametrize(
    ("field", "value"),
    (("full_coverage_start_count", 47), ("matched_controls_complete", False)),
)
def test_stage4_promotion_requires_48_full_starts_and_controls(
    field: str, value: object
) -> None:
    row = _stage4_row()
    if field == "full_coverage_start_count":
        row["coverage"] = {**dict(row["coverage"]), field: value}
    else:
        row[field] = value

    assert runtime._select_stage4_for_expansion([row], maximum=16) == []


def test_full_coverage_start_builder_reports_real_nonoverlapping_capacity() -> None:
    days = tuple(range(360))
    block_map = {
        day: f"B{min(day // 90 + 1, 4)}"
        for day in days
    }

    starts = runtime._block_aware_full_coverage_starts(
        days,
        block_map=block_map,
        horizon=90,
        maximum=48,
    )

    assert starts == (0, 90, 180, 270)
    assert len(starts) == 4


def test_direction_flipped_control_matches_declared_dimensions_without_matching_side(
) -> None:
    candidate = SimpleNamespace(timeframe="1m", horizon=15)
    common = {
        "session_day": 19_000,
        "session_code": 1,
        "market": "CL",
        "timeframe": "1m",
        "maximum_horizon": 15,
        "quantity": 2,
        "fill_policy_hash": "a" * 64,
    }
    observed = [SimpleNamespace(**common, direction=1)]
    flipped = [SimpleNamespace(**common, direction=-1)]

    audit = runtime._matched_control_audit(candidate, observed, flipped)

    assert audit["all_required_dimensions_matched"] is True
    assert audit["dimension_match"] == {
        "market": True,
        "session": True,
        "timeframe": True,
        "opportunity_count": True,
        "active_duration": True,
        "average_exposure": True,
        "cost_level": True,
    }


def test_resume_batch_loading_is_idempotent_and_duplicate_identity_fails_closed(
    tmp_path: Path,
) -> None:
    batches = tmp_path / "batches"
    batches.mkdir()
    first = batches / "batch_0000.jsonl"
    first.write_text(
        json.dumps({"candidate_id": "candidate_a", "status": "STAGE_1_COMPLETE"})
        + "\n",
        encoding="utf-8",
    )

    assert runtime._load_batches(batches) == [
        {"candidate_id": "candidate_a", "status": "STAGE_1_COMPLETE"}
    ]
    assert runtime._load_batches(batches) == runtime._load_batches(batches)
    assert runtime._next_batch_index(batches) == 1

    (batches / "batch_0001.jsonl").write_text(
        json.dumps({"candidate_id": "candidate_a", "status": "STAGE_1_COMPLETE"})
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(
        runtime.CausalTargetVelocityRuntimeError,
        match="duplicate candidate across resume batches",
    ):
        runtime._load_batches(batches)


def test_terminal_seals_bundle_writes_result_then_publishes_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = object.__new__(runtime._CausalTargetVelocityRun)
    run.root = tmp_path
    run.output_dir = tmp_path / "reports/economic_evolution/0028"
    run.payload_dir = tmp_path / "payload"
    run.contract_map_path = tmp_path / "contract_map.json"
    run.cache_root = tmp_path / "feature_cache"
    run.output_dir.mkdir(parents=True)
    run.payload_dir.mkdir()
    run.contract_map_path.write_text("{}\n", encoding="utf-8")
    matrix_manifest = run.cache_root / "cl" / "manifest.json"
    matrix_manifest.parent.mkdir(parents=True)
    matrix_manifest.write_text("{}\n", encoding="utf-8")
    access_ledger = tmp_path / "reports/data_access/data_access_ledger.jsonl"
    access_ledger.parent.mkdir(parents=True)
    access_ledger.write_text("{}\n", encoding="utf-8")
    run.manifest = {
        "campaign_id": "hydra_causal_target_velocity_0028",
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "runtime": {"result_name": "economic_production_result.json"},
        "evidence_bundle": {"destination": "data/cache/evidence_bundles"},
    }
    run.campaign_id = str(run.manifest["campaign_id"])
    run.state = {"next_action": "PENDING"}
    run.stage1_rows = []
    run.stage3_rows = []

    candidate_id = "hazard_candidate_a"
    event_path = run.payload_dir / f"stage2_event_evidence/{candidate_id}.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text("{}\n", encoding="utf-8")
    episode_path = run.payload_dir / f"stage2_episode_evidence/{candidate_id}.jsonl"
    episode_path.parent.mkdir(parents=True)
    episode_path.write_text(
        json.dumps({"policy_id": candidate_id, "episode_id": "episode_a"}) + "\n",
        encoding="utf-8",
    )
    run.stage2_rows = [
        {
            "status": "STAGE_2_COMPLETE",
            "candidate_id": candidate_id,
            "candidate": {"market": "CL"},
            "eligible_session_days": [19_000],
            "decision_hash": "1" * 64,
            "normal_event_hash": "2" * 64,
            "stressed_event_hash": "3" * 64,
            "normal_trajectory_hash": "4" * 64,
            "stressed_trajectory_hash": "5" * 64,
            "fill_policy_hash": "6" * 64,
            "episode_evidence": {
                "relative_path": (
                    f"stage2_episode_evidence/{candidate_id}.jsonl"
                ),
                "record_count": 1,
                "sha256": runtime._sha256(episode_path),
            },
        }
    ]

    chronology: list[str] = []
    written: dict[str, object] = {}

    class _Writer:
        def write_json(self, name: str, value: object) -> None:
            chronology.append("result_write")
            written[name] = value

    def _publish(**updates: object) -> None:
        run.state.update(updates)
        if updates.get("state") == "COMPLETE":
            chronology.append("state_complete")

    receipt_payload = {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "campaign_id": run.campaign_id,
        "bundle_path": str(tmp_path / "data/cache/evidence_bundles/bundle"),
        "manifest_path": str(tmp_path / "bundle_manifest.json"),
        "manifest_sha256": "7" * 64,
        "bundle_content_sha256": "8" * 64,
        "dataset_row_counts": {"episode_outcomes": 1},
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
    }
    fake_receipt = SimpleNamespace(
        bundle_path=receipt_payload["bundle_path"],
        manifest_sha256=receipt_payload["manifest_sha256"],
        to_dict=lambda: dict(receipt_payload),
    )
    run.output_writer = _Writer()
    run._publish = _publish
    run._kpis = lambda: {"schema": "test_kpis"}

    monkeypatch.setattr(
        runtime, "reconstruct_exact_hazard_replay", lambda **_: object()
    )
    monkeypatch.setattr(
        runtime,
        "finalize_causal_target_velocity_evidence_bundle",
        lambda **_: fake_receipt,
    )
    monkeypatch.setattr(
        runtime, "verify_evidence_bundle", lambda *_args, **_kwargs: {"status": "COMPLETE"}
    )
    monkeypatch.setattr(runtime, "_terminal_economic_summary", lambda *_a, **_k: {})
    monkeypatch.setattr(runtime, "_terminal_control_summary", lambda *_a, **_k: {})
    monkeypatch.setattr(runtime, "_terminal_failure_vectors", lambda *_a, **_k: {})

    result = run._finalize_campaign(
        preflight={"result_hash": "9" * 64, "status": "RISK_SCALE_ONLY_FALSIFIED"},
        useful=(),
        stage4=(),
        promoted=(),
        reason="SYNTHETIC_NO_SURVIVOR",
    )

    assert result["status"] == "COMPLETE"
    payload = dict(result)
    claimed = payload.pop("result_hash")
    assert claimed == stable_hash(payload)
    assert written["economic_production_result.json"] == result
    assert chronology == ["result_write", "state_complete"]
    assert result["evidence_bundle"]["evidence_status"] == (
        "FRESH_DEVELOPMENT_EVIDENCE"
    )
    decisions = result["successive_halving"]["stage_decisions"]
    assert decisions
    assert decisions[-1]["output_count"] == 0
    assert decisions[-1]["selected_policy_ids"] == []
    assert result["autonomous_next_action"]["action"] == (
        "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST"
    )

    chronology.clear()
    coverage_limited = run._finalize_campaign(
        preflight={"result_hash": "9" * 64, "status": "RISK_SCALE_ONLY_FALSIFIED"},
        useful=[{"candidate_id": candidate_id}],
        stage4=(),
        promoted=(),
        reason="FULL_COVERAGE_START_LIMIT_PREVENTED_96_START_EXPANSION",
    )
    coverage_action = coverage_limited["autonomous_next_action"]
    assert "COVERAGE" in coverage_action["action"]
    assert coverage_action["action"] != (
        "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST"
    )
    assert coverage_action["candidate_ids"] == [candidate_id]
    assert coverage_action["manifest_required"] is False
    assert chronology == ["result_write", "state_complete"]


def test_terminal_economic_summary_satisfies_v17_canonical_views() -> None:
    stage2 = [{**_stage2_row(), "status": "STAGE_2_COMPLETE"}]
    stage3 = [_stage3_row()]
    summary = runtime._terminal_economic_summary(
        [{"status": "STAGE_1_COMPLETE"}],
        stage2,
        stage3,
        (),
        useful=(),
        promoted=(),
        reason="NO_BOOK_MET_FROZEN_GATE",
    )
    kpis = {
        "rates_per_hour": {
            "policies_proposed": 1.0,
            "unique_policies_screened": 1.0,
            "exact_account_replays": 1.0,
            "combine_episodes": 4.0,
        },
        "economic_research_wall_clock_fraction": 0.90,
        "cpu_utilization_fraction": 0.75,
        "workers": {"compute": 3, "evidence_writer": 1},
        "duplicate_rejection_rate": 0.20,
        "cache_hit_rate": 1.0,
    }

    counters, production_kpis, frontier = (
        EconomicEvolutionManifestRuntime._production_terminal_views(summary, kpis)
    )

    assert counters["serious_exact_account_replays"] == 1
    assert counters["normal_episodes_completed"] == 2
    assert counters["stressed_episodes_completed"] == 2
    assert production_kpis["workers"] == {"compute": 3, "evidence_writer": 1}
    assert frontier["candidate_count"] == 1
    assert summary["normal_pass_candidate_count"] == 1
    assert summary["stressed_pass_candidate_count"] == 1
    assert summary["positive_stressed_net_count"] == 1
    assert summary["development_only"] is True
    assert summary["independently_confirmed"] is False
    assert summary["confirmation_ready_candidate_ids"] == []
    assert summary["stage5_96_start_candidate_ids"] == []
    assert summary["development_finalist_ids"] == []


def test_stage2_only_evidence_terminal_still_has_nonempty_v17_frontier() -> None:
    summary = runtime._terminal_economic_summary(
        [{"status": "STAGE_1_COMPLETE"}],
        [{**_stage2_row(), "status": "STAGE_2_COMPLETE"}],
        (),
        (),
        useful=(),
        promoted=(),
        reason="NO_EXACT_CAUSAL_SLEEVE_SURVIVED_STAGE_2",
    )
    kpis = {
        "rates_per_hour": {
            "policies_proposed": 1.0,
            "unique_policies_screened": 1.0,
            "exact_account_replays": 1.0,
            "combine_episodes": 2.0,
        },
        "economic_research_wall_clock_fraction": 0.90,
        "cpu_utilization_fraction": 0.75,
        "workers": {"compute": 3, "evidence_writer": 1},
        "duplicate_rejection_rate": 0.20,
        "cache_hit_rate": 1.0,
    }

    counters, _, frontier = (
        EconomicEvolutionManifestRuntime._production_terminal_views(summary, kpis)
    )

    assert counters["serious_exact_account_replays"] == 1
    assert counters["combine_episodes_completed"] == 2
    assert frontier["candidate_count"] == 1
    assert frontier["stressed_target_progress_median_best"] == pytest.approx(0.70)


def test_synthetic_stage0_to_terminal_orchestration_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = object.__new__(runtime._CausalTargetVelocityRun)
    run.output_dir = tmp_path / "output"
    run.output_dir.mkdir()
    run.manifest_path = tmp_path / "manifest.json"
    run.root = tmp_path
    run.cache_root = tmp_path / "cache"
    run.manifest = {
        "runtime": {"result_name": "economic_production_result.json"},
        "search_space": {"markets": ["CL", "ES", "NQ", "RTY", "YM"]},
    }
    run.campaign_id = "hydra_causal_target_velocity_0028"
    run.state = {}
    run.stop_after = None
    run.hot_seconds = 0.0
    run.stage1_rows = []
    run.stage2_rows = []
    run.stage3_rows = []
    run.payload_writer = SimpleNamespace(write_json=lambda *_args, **_kwargs: None)
    run._verify_deployment = lambda: None
    run._publish = lambda **updates: run.state.update(updates)
    run._load_stage1_rows = lambda: []
    run._stage0_population = lambda: (
        (object(),) * 20_000,
        (object(),) * 4_096,
    )
    evidence_only = {
        "candidate_id": "screened_a",
        "candidate": {},
        "selection_role": "EVIDENCE_ONLY_DIAGNOSTIC_EXACT_REPLAY",
    }
    exact_row = {"candidate_id": "screened_a", "status": "STAGE_2_COMPLETE"}
    run._stage1_event_screen = lambda *_args, **_kwargs: [evidence_only]
    run._stage2_exact_replay = lambda *_args, **_kwargs: [exact_row]
    terminal_calls: list[dict[str, object]] = []

    def _finalize_campaign(**kwargs: object) -> dict[str, object]:
        terminal_calls.append(kwargs)
        return {"status": "COMPLETE", "reason": kwargs["reason"]}

    run._finalize_campaign = _finalize_campaign

    preflight = {
        "status": "RISK_SCALE_ONLY_FALSIFIED",
        "result_hash": "a" * 64,
    }
    monkeypatch.setattr(runtime, "run_causal_risk_preflight", lambda *_a, **_k: preflight)
    monkeypatch.setattr(runtime, "_discover_feature_matrices", lambda *_a, **_k: {})
    monkeypatch.setattr(runtime, "_resolve_period", lambda *_a, **_k: (0, 1, 2))
    monkeypatch.setattr(runtime, "_select_stage1", lambda *_a, **_k: [evidence_only])
    monkeypatch.setattr(runtime, "_select_stage2", lambda *_a, **_k: [exact_row])
    monkeypatch.setattr(
        runtime,
        "HazardCandidate",
        lambda **_kwargs: SimpleNamespace(candidate_id="screened_a"),
    )

    result = run.execute()

    assert result == {
        "status": "COMPLETE",
        "reason": "NO_EXACT_CAUSAL_SLEEVE_SURVIVED_STAGE_2",
    }
    assert terminal_calls == [
        {
            "preflight": preflight,
            "useful": (),
            "stage4": (),
            "promoted": (),
            "reason": "NO_EXACT_CAUSAL_SLEEVE_SURVIVED_STAGE_2",
        }
    ]
