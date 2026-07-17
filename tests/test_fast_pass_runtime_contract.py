from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)
from hydra.production import fast_pass_runtime as runtime
from hydra.production.fast_pass_runtime import _FastPassRun


def _summary(*, passes: int, net: float, progress: float = 0.1) -> dict[str, object]:
    return {
        "episode_count": 10,
        "pass_count": passes,
        "pass_rate": passes / 10.0,
        "net_total": net,
        "target_progress_p25": progress,
        "target_progress_median": progress,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 4_000.0,
        "consistency_rate": 1.0,
        "blocks_with_passes": ["B3", "B4"] if passes else [],
        "block_pass_counts": ({"B3": 1, "B4": 1} if passes else {}),
        "component_contribution": {"sleeve": net},
        "best_day_concentration_max": 0.4,
        "single_trade_domination": False,
    }


def _role_summaries(*, passes: int, net: float) -> dict[str, object]:
    return {
        scenario: {
            str(horizon): _summary(passes=passes, net=net)
            for horizon in (5, 10, 20)
        }
        for scenario in ("NORMAL", "STRESSED_1_5X")
    }


def _run() -> _FastPassRun:
    run = object.__new__(_FastPassRun)
    run.campaign_id = "hydra_fast_pass_factory_0029"
    run.manifest = {
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "bank_architecture": {
            "fast_5d_capacity": 50,
            "balanced_10d_capacity": 50,
            "robust_20d_capacity": 50,
            "graduated_maximum_target": 20,
        },
        "promotion_gates": {
            "fast_5d_bank": {"full_coverage_normal_passes_minimum": 1},
            "graduated_book": {
                "normal_5d_pass_rate_minimum": 0.05,
                "stressed_5d_pass_rate_minimum": 0.02,
                "normal_10d_pass_rate_minimum": 0.10,
                "stressed_10d_pass_rate_minimum": 0.05,
                "mll_breach_rate_maximum": 0.10,
                "independent_blocks_with_passes_minimum": 2,
            },
            "strong_sprint_book": {
                "normal_5d_pass_rate_minimum": 0.10,
                "stressed_5d_pass_rate_minimum": 0.05,
            },
        },
    }
    run.state = {"state": "FINALIZING"}
    return run


def test_tier_gate_uses_held_out_development_not_design_results() -> None:
    run = _run()
    row = {
        "policy_id": "book",
        "policy_role": "MARGINAL_BOOK_CANDIDATE",
        "component_ids": ["sleeve"],
        "policy": {"policy_id": "book"},
        "marginally_accepted": True,
        "summaries_by_role": {
            "DESIGN": _role_summaries(passes=3, net=10_000.0),
            "HELD_OUT_DEVELOPMENT": _role_summaries(passes=0, net=100.0),
        },
    }

    decision = run._tier_decisions([], [row])

    assert decision["fast_5d_bank_ids"] == []
    assert decision["graduated_book_ids"] == []


def test_tier_gate_admits_only_frozen_marginal_book_with_block_diverse_passes() -> None:
    run = _run()
    row = {
        "policy_id": "book",
        "policy_role": "MARGINAL_BOOK_CANDIDATE",
        "component_ids": ["sleeve"],
        "policy": {"policy_id": "book"},
        "marginally_accepted": True,
        "summaries_by_role": {
            "DESIGN": _role_summaries(passes=0, net=-1.0),
            "HELD_OUT_DEVELOPMENT": _role_summaries(passes=2, net=10_000.0),
        },
    }

    decision = run._tier_decisions([], [row])

    assert decision["fast_5d_bank_ids"] == ["book"]
    assert decision["graduated_book_ids"] == ["book"]


def test_fast_pass_kpi_topology_matches_persistent_controller_contract() -> None:
    run = _run()
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    run.state = {
        "checkpoint_sequence": 1,
        "started_at_utc": now,
        "state": "STARTING",
        "policies_proposed": 100,
        "unique_policies_screened": 50,
    }
    run._exact_count_cache = 10
    run._episode_count_cache = 20
    run._book_count_cache = 5
    run.hot_seconds = 1.0
    run.prior_wall_seconds = 0.0
    run.started_wall = time.perf_counter() - 1.0
    run.cpu_ticks_started = (0, 0)

    kpis = run._kpis()

    required_counters = {
        "policies_proposed",
        "unique_policies_screened",
        "exact_account_replays",
        "combine_episodes_completed",
        "normal_episodes_completed",
        "stressed_episodes_completed",
        "positive_stressed_net_candidates",
        "candidates_with_normal_pass",
        "candidates_with_stressed_pass",
        "near_pass_count",
        "candidates_promoted_96",
        "confirmation_ready_candidates",
    }
    assert required_counters.issubset(kpis)
    assert kpis["normal_episodes_completed"] == 10
    assert kpis["stressed_episodes_completed"] == 10
    assert {
        "policies_proposed",
        "unique_policies_screened",
        "exact_account_replays",
        "combine_episodes",
    }.issubset(kpis["rates_per_hour"])
    assert kpis["matched_controls_status"]
    assert kpis["null_status"]


def test_fast_pass_economic_summary_satisfies_v17_canonical_views() -> None:
    run = _run()
    run.manifest["budget"] = {"remaining_usd": 37.15}
    run.state = {"policies_proposed": 20_000, "unique_policies_screened": 4_096}
    run._exact_count_cache = 512
    run._episode_count_cache = 192
    run._book_count_cache = 16
    run._load_bank_wave = lambda _wave: [
        {
            "candidate": {
                "market": "CL",
                "timeframe": "1m",
                "session_code": 0,
                "mechanism": "VOLATILITY_EXPANSION",
            }
        }
    ]
    kpis = {
        "rates_per_hour": {
            "policies_proposed": 1.0,
            "unique_policies_screened": 1.0,
            "exact_account_replays": 1.0,
            "combine_episodes": 1.0,
        },
        "economic_research_wall_clock_fraction": 0.90,
        "cpu_utilization_fraction": 0.75,
        "workers": {"compute": 3, "evidence_writer": 1},
        "duplicate_rejection_rate": 0.20,
        "cache_hit_rate": 1.0,
    }
    run._kpis = lambda: dict(kpis)
    row = {
        "policy_id": "book",
        "policy_role": "MARGINAL_BOOK_CANDIDATE",
        "summaries": _role_summaries(passes=2, net=10_000.0),
    }
    tiers = {
        "fast_5d_bank_ids": ["book"],
        "balanced_10d_bank_ids": ["book"],
        "robust_20d_bank_ids": ["book"],
        "graduated_book_ids": ["book"],
        "strong_sprint_book_ids": ["book"],
    }

    summary = run._economic_summary(
        latest_tiers=tiers,
        sprint_rows=[row],
        starts={5: [(1, "B1")], 10: [(1, "B1")], 20: [(1, "B1")]},
        diversity={"audit_hash": "a" * 64},
        microstructure={"triggered": False},
    )
    counters, production_kpis, frontier = (
        EconomicEvolutionManifestRuntime._production_terminal_views(summary, kpis)
    )

    assert counters == {
        "serious_exact_account_replays": 512,
        "predeclared_control_policy_replays": 0,
        "combine_episodes_completed": 192,
        "normal_episodes_completed": 96,
        "stressed_episodes_completed": 96,
    }
    assert production_kpis["workers"] == {"compute": 3, "evidence_writer": 1}
    assert frontier["candidate_count"] == 1
    assert frontier["normal_pass_fraction_best"] == pytest.approx(0.20)
    assert frontier["stressed_pass_fraction_best"] == pytest.approx(0.20)


def test_existing_terminal_result_uses_canonical_verifier_and_reconciles_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = object.__new__(_FastPassRun)
    run.output_dir = tmp_path
    run.campaign_id = "hydra_fast_pass_factory_0029"
    run.manifest = {
        "campaign_id": run.campaign_id,
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "runtime": {"result_name": "economic_production_result.json"},
    }
    terminal = {
        "schema": "hydra_economic_production_result_v1",
        "campaign_id": run.campaign_id,
        "manifest_hash": run.manifest["manifest_hash"],
        "source_commit": run.manifest["source_commit"],
        "status": "COMPLETE",
        "evidence_bundle": {
            "bundle_path": str(tmp_path / "bundle"),
            "manifest_sha256": "c" * 64,
        },
    }
    (tmp_path / "economic_production_result.json").write_text(
        json.dumps(terminal), encoding="utf-8"
    )
    verified = {
        **terminal,
        "result_hash": "verified",
        "economic_results": {
            "production_counters": {
                "serious_exact_account_replays": 512,
                "predeclared_control_policy_replays": 8,
                "combine_episodes_completed": 192,
                "normal_episodes_completed": 96,
                "stressed_episodes_completed": 96,
            },
            "confirmation_ready_candidate_ids": [],
        },
        "successive_halving": {"stage_decisions": []},
    }
    calls: list[str] = []
    published: list[dict[str, object]] = []
    monkeypatch.setattr(
        runtime,
        "load_and_verify_production_result",
        lambda *_args, **_kwargs: calls.append("verified") or verified,
        raising=False,
    )
    monkeypatch.setattr(
        runtime, "verify_evidence_bundle", lambda *_args, **_kwargs: {"status": "COMPLETE"}
    )
    run._publish = lambda **updates: published.append(dict(updates))

    result = run.execute()

    assert result is verified
    assert calls == ["verified"]
    assert published[-1]["state"] == "COMPLETE"
    assert published[-1]["exact_account_replays"] == 512
    assert published[-1]["combine_episodes_completed"] == 192
    assert published[-1]["evidence_bundle_path"] == terminal["evidence_bundle"][
        "bundle_path"
    ]
    assert published[-1]["evidence_bundle_manifest_sha256"] == (
        terminal["evidence_bundle"]["manifest_sha256"]
    )


def test_sealed_bundle_recovery_precedes_any_economic_reentry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = object.__new__(_FastPassRun)
    run.output_dir = tmp_path / "output"
    run.output_dir.mkdir()
    run.campaign_id = "hydra_fast_pass_factory_0029"
    run.manifest = {
        "campaign_id": run.campaign_id,
        "manifest_hash": "a" * 64,
        "source_commit": "b" * 40,
        "runtime": {"result_name": "economic_production_result.json"},
        "markets": ["CL"],
    }
    run.cache_root = tmp_path / "cache"
    run.state = {"stage": "EVIDENCE_BUNDLE_SEALED"}
    run._verify_deployment = lambda: None
    run._publish = lambda **_updates: None
    recovered = {"status": "COMPLETE", "recovered": True}
    run._recover_sealed_bundle_result = lambda: recovered
    monkeypatch.setattr(
        runtime,
        "_discover_feature_matrices",
        lambda *_args, **_kwargs: pytest.fail(
            "economic/cache work restarted before sealed-bundle recovery"
        ),
    )

    assert run.execute() is recovered


def test_fast_pass_exposes_a_real_sealed_bundle_recovery_projection() -> None:
    assert callable(getattr(_FastPassRun, "_recover_sealed_bundle_result", None))
