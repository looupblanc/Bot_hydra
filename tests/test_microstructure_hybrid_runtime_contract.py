from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.mission.economic_evolution_manifest_runtime import (
    EconomicEvolutionManifestRuntime,
)
from hydra.production.microstructure_hybrid_manifest import CAMPAIGN_ID
from hydra.production.microstructure_hybrid_runtime import (
    KPI_SCHEMA,
    OVERLAY_DECISIONS,
    RESULT_SCHEMA,
    STATE_SCHEMA,
    HybridRuntimeError,
    _build_terminal_result,
    _conditional_cost_report,
    _pilot_config,
    _pilot_mapping,
    _set_single_thread_libraries,
    _source_bindings,
    _write_state,
)


def _manifest() -> dict[str, object]:
    return {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": CAMPAIGN_ID,
        "campaign_mode": "HYBRID_STRUCTURAL_ALPHA_ORDER_FLOW",
        "manifest_hash": "a" * 64,
        "source_commit": "c" * 40,
        "runtime": {
            "output_dir": (
                "reports/economic_evolution/"
                "hybrid_structural_alpha_order_flow_0033"
            ),
            "result_name": "economic_production_result.json",
        },
        "compute_contract": {"cpu_worker_count": 2},
        "evidence_bundle": {
            "destination": "data/cache/evidence_bundles",
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
        },
        "paired_action_frontier": {
            "action_ids": ["A0", "A1", "A2", "A3", "A4", "A5"],
            "actions": [
                {"action_id": f"A{index}", "action_type": f"TYPE_{index}"}
                for index in range(6)
            ],
            "risk_levels": [0.5, 1.0, 1.5],
            "maximum_policy_count": 20,
        },
        "chronological_roles": {
            "discovery_sessions": 3,
            "validation_sessions": 1,
            "final_development_sessions": 1,
        },
        "immutable_source_store_0031": {
            "source_hashes": {
                "feature_matrices": {
                    "path": (
                        "reports/economic_evolution/0031/pilot/datasets/"
                        "feature_matrices/part-000000.parquet"
                    ),
                    "sha256": "1" * 64,
                }
            }
        },
        "terminal_source_0032": {
            "source_hashes": {
                "authoritative_result": {
                    "path": "reports/economic_evolution/0032/result.json",
                    "sha256": "2" * 64,
                }
            }
        },
        "clean_structural_anchors_0028": {
            "maximum_anchor_count": 24,
            "frozen_anchor_count": 2,
            "markets": ["NQ", "YM"],
            "coverage_session_dates": [
                "2024-07-08",
                "2024-07-09",
                "2024-07-10",
                "2024-07-11",
                "2024-07-12",
            ],
            "anchor_ids": ["hazard_a", "hazard_b"],
            "source_hashes": {
                "candidate_population": {
                    "path": "reports/economic_evolution/0028/population.json",
                    "sha256": "3" * 64,
                },
                "source_result": {
                    "path": "reports/economic_evolution/0028/result.json",
                    "sha256": "4" * 64,
                },
            },
            "event_ledgers": {
                "hazard_a": {
                    "path": "reports/economic_evolution/0028/events/hazard_a.jsonl"
                },
                "hazard_b": {
                    "path": "reports/economic_evolution/0028/events/hazard_b.jsonl"
                },
            },
        },
        "conditional_extension": {
            "current_remaining_budget_usd": 28.498462508622012,
            "maximum_incremental_spend_usd": 3.25,
            "minimum_budget_reserve_usd": 25.0,
        },
    }


def _pilot(status: str = "HYBRID_OVERLAY_WEAK") -> dict[str, object]:
    paths = [
        {
            "episode_id": "p0-normal",
            "policy_id": "hybrid_0",
            "scenario": "NORMAL",
            "target_reached": True,
            "mll_breached": False,
            "target_progress_pct": 100.0,
        },
        {
            "episode_id": "p0-stress",
            "policy_id": "hybrid_0",
            "scenario": "STRESSED_1_5X",
            "target_reached": False,
            "mll_breached": False,
            "target_progress_pct": 12.5,
        },
        {
            "episode_id": "p1-normal",
            "policy_id": "hybrid_1",
            "scenario": "NORMAL",
            "target_reached": False,
            "mll_breached": False,
            "target_progress_pct": -2.0,
        },
        {
            "episode_id": "p1-stress",
            "policy_id": "hybrid_1",
            "scenario": "STRESSED_1_5X",
            "target_reached": False,
            "mll_breached": True,
            "target_progress_pct": -3.0,
        },
    ]
    return {
        "pilot_status": status,
        "candidate_results": [
            {"candidate_id": "episode_0"},
            {"candidate_id": "episode_1"},
            {"candidate_id": "episode_2"},
        ],
        "policy_results": [
            {
                "policy_id": "hybrid_0",
                "stressed_net_usd": 25.0,
                "survives_gate": status == "HYBRID_OVERLAY_GREEN",
                "account_paths": paths[:2],
            },
            {
                "policy_id": "hybrid_1",
                "stressed_net_usd": -10.0,
                "account_paths": paths[2:],
            },
        ],
        "evidence_identity": {"campaign_id": CAMPAIGN_ID},
        "evidence_datasets": {},
        "compact_outputs": {},
        "production_kpis": {
            "control_replay_count": 6,
            "matched_controls_status": "PAIRED_COMPLETE",
            "paired_uplift": {"stressed_net_usd": -2.5},
        },
        "runtime_metrics": {
            "elapsed_seconds": 10.0,
            "cpu_utilization_fraction": 0.75,
            "economic_wall_clock_fraction": 0.9,
        },
        "gate_checks": {"positive_stressed_economics": False},
    }


def _receipt() -> dict[str, object]:
    return {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
        "bundle_path": "/tmp/0033.evidence-v1",
        "manifest_path": "/tmp/0033.evidence-v1/manifest.json",
        "manifest_sha256": "d" * 64,
        "bundle_content_sha256": "e" * 64,
        "dataset_row_counts": {"episodes": 4},
    }


def test_state_and_kpis_publish_v17_two_worker_contract(tmp_path: Path) -> None:
    output = tmp_path / "reports/economic_evolution/0033"
    state = _write_state(
        output,
        _manifest(),
        state="EXACT_REPLAY_ACTIVE",
        stage="PAIRED_ACTION_AND_RISK_OVERLAY",
        next_action="RUN_HYBRID_PILOT",
        pilot=_pilot(),
    )
    kpis = json.loads((output / "production_kpis.json").read_text())

    assert state["schema"] == STATE_SCHEMA
    assert state["worker_count"] == 2
    assert state["evidence_writer_count"] == 1
    assert state["broker_connections"] == state["orders"] == 0
    assert state["q4_access_count_delta"] == state["data_purchase_count"] == 0
    assert state["state_hash"] == stable_hash(
        {key: value for key, value in state.items() if key != "state_hash"}
    )
    assert kpis["schema"] == KPI_SCHEMA
    assert kpis["workers"] == {"compute": 2, "evidence_writer": 1}
    assert kpis["exact_account_replays"] == 2
    assert (
        kpis["exact_account_replays"]
        <= kpis["unique_policies_screened"]
        <= kpis["policies_proposed"]
    )
    assert kpis["normal_episodes_completed"] == 2
    assert kpis["stressed_episodes_completed"] == 2
    assert kpis["combine_episodes_completed"] == 4
    assert kpis["kpi_hash"] == stable_hash(
        {key: value for key, value in kpis.items() if key != "kpi_hash"}
    )


def test_terminal_result_is_standard_hashed_and_fail_closed(tmp_path: Path) -> None:
    manifest = _manifest()
    pilot = _pilot()
    cost = _conditional_cost_report(
        tmp_path, manifest, pilot, "HYBRID_OVERLAY_WEAK"
    )
    result = _build_terminal_result(
        manifest=manifest,
        pilot=pilot,
        evidence_receipt=_receipt(),
        decision="HYBRID_OVERLAY_WEAK",
        conditional_cost_report=cost,
    )

    assert result["schema"] == RESULT_SCHEMA
    assert result["scientific_status"] == "HYBRID_OVERLAY_WEAK"
    assert result["new_data_purchase_count"] == 0
    assert result["actual_additional_spend_usd"] == 0.0
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == result["orders"] == 0
    assert result["matched_controls"]["paired_uplift"] == {
        "stressed_net_usd": -2.5
    }
    assert result["autonomous_next_action"]["new_data_purchase_authorized"] is False
    assert result["autonomous_next_action"]["q4_access_authorized"] is False
    assert result["result_hash"] == stable_hash(
        {key: value for key, value in result.items() if key != "result_hash"}
    )
    counters, production_kpis, frontier = (
        EconomicEvolutionManifestRuntime._production_terminal_views(
            result["economic_results"], result["kpis"]
        )
    )
    assert counters["combine_episodes_completed"] == 4
    assert production_kpis["workers"] == {"compute": 2, "evidence_writer": 1}
    assert frontier["stressed_target_progress_median_best"] == pytest.approx(0.125)


def test_green_conditional_cost_report_is_metadata_only(tmp_path: Path) -> None:
    pilot = {
        **_pilot("HYBRID_OVERLAY_GREEN"),
        "conditional_data_cost_report": {"NQ": {"tbbo": {"usd": 2.5}}},
    }
    report = _conditional_cost_report(
        tmp_path, _manifest(), pilot, "HYBRID_OVERLAY_GREEN"
    )

    assert report["status"] == "OFFICIAL_COST_REPORT_AVAILABLE_NO_PURCHASE"
    assert report["actual_additional_spend_usd"] == 0.0
    assert report["purchase_performed"] is False
    assert report["automatic_purchase_allowed"] is False
    assert report["q4_accessed"] is False


def test_pilot_result_contract_requires_exact_overlay_status_and_cap() -> None:
    assert tuple(OVERLAY_DECISIONS) == (
        "HYBRID_OVERLAY_GREEN",
        "HYBRID_OVERLAY_WEAK",
        "HYBRID_OVERLAY_FALSIFIED",
    )
    assert _pilot_mapping(_pilot())["pilot_status"] == "HYBRID_OVERLAY_WEAK"

    invalid = {**_pilot(), "pilot_status": "HYBRID_PILOT_WEAK"}
    with pytest.raises(HybridRuntimeError, match="unsupported scientific decision"):
        _pilot_mapping(invalid)
    missing = dict(_pilot())
    missing.pop("runtime_metrics")
    with pytest.raises(HybridRuntimeError, match="contract is incomplete"):
        _pilot_mapping(missing)
    oversized = {**_pilot(), "policy_results": [{}] * 21}
    with pytest.raises(HybridRuntimeError, match="20-policy cap"):
        _pilot_mapping(oversized)


def test_config_and_all_source_bindings_are_frozen(tmp_path: Path) -> None:
    manifest = _manifest()
    feature = (
        tmp_path
        / "reports/economic_evolution/0031/pilot/datasets/"
        "feature_matrices/part-000000.parquet"
    )
    population = tmp_path / "reports/economic_evolution/0028/population.json"
    clean = tmp_path / "reports/economic_evolution/0028/result.json"
    event_a = tmp_path / "reports/economic_evolution/0028/events/hazard_a.jsonl"
    event_b = tmp_path / "reports/economic_evolution/0028/events/hazard_b.jsonl"
    for path in (feature, population, clean, event_a, event_b):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n")

    @dataclass(frozen=True)
    class Config:
        campaign_id: str
        manifest_hash: str
        source_commit: str
        source_store_hash: str
        cpu_worker_count: int
        action_ids: tuple[str, ...]
        risk_levels: tuple[float, ...]
        maximum_policies: int
        chronological_role_counts: tuple[int, int, int]

    config = _pilot_config(manifest, Config)
    bindings = _source_bindings(tmp_path, manifest)

    assert config.cpu_worker_count == 2
    assert config.maximum_policies == 20
    assert config.risk_levels == (0.5, 1.0, 1.5)
    assert bindings == {
        "source_store_dir": feature.parents[2],
        "anchor_population_path": population,
        "anchor_event_root": event_a.parent,
        "clean_result_path": clean,
    }


def test_blas_thread_contract_is_one() -> None:
    _set_single_thread_libraries()
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        assert os.environ[name] == "1"
