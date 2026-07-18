from __future__ import annotations

import json
import os
from pathlib import Path

from hydra.economic_evolution.schema import stable_hash
from hydra.production.microstructure_sparse_manifest import CAMPAIGN_ID
from hydra.production.microstructure_sparse_pilot import SparsePilotConfig
from hydra.production.microstructure_sparse_runtime import (
    KPI_SCHEMA,
    RESULT_SCHEMA,
    STATE_SCHEMA,
    _build_terminal_result,
    _conditional_cost_report,
    _pilot_config,
    _set_single_thread_libraries,
    _source_store_dir,
    _write_state,
)


def _manifest() -> dict[str, object]:
    return {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": CAMPAIGN_ID,
        "campaign_mode": "MICROSTRUCTURE_SPARSE_ALPHA_DISTILLATION",
        "manifest_hash": "a" * 64,
        "source_commit": "c" * 40,
        "runtime": {
            "output_dir": (
                "reports/economic_evolution/"
                "microstructure_sparse_alpha_distillation_0032"
            ),
            "result_name": "economic_production_result.json",
        },
        "evidence_bundle": {
            "destination": "data/cache/evidence_bundles",
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
        },
        "source_store": {
            "source_hashes": {
                "feature_matrices": {
                    "path": (
                        "reports/economic_evolution/0031/pilot/datasets/"
                        "feature_matrices/part-000000.parquet"
                    ),
                    "sha256": "f" * 64,
                }
            }
        },
        "compute_contract": {"cpu_worker_count": 2},
        "sparse_policy_frontier": {
            "edge_to_cost_ratios": [1.25, 1.5, 2.0, 3.0],
            "trade_budgets_per_session": [2, 4, 8, 12],
            "max_strategies": 30,
        },
        "holding_exit_frontier": {
            "horizons_seconds": [30, 120, 300, 900],
            "exit_types": [
                "FIXED_TARGET_STOP",
                "ORDER_FLOW_DECAY",
                "OPPOSITE_STATE_TRANSITION",
                "TIME_STOP",
                "VWAP_LIQUIDITY_LEVEL",
                "EVENT_STATE_RESET",
            ],
        },
        "execution_model": {"stressed_cost_multiplier": 1.5},
        "conditional_extension": {
            "current_remaining_budget_usd": 28.498462508622012,
            "maximum_incremental_spend_usd": 3.25,
            "minimum_budget_reserve_usd": 25.0,
        },
    }


def _pilot() -> dict[str, object]:
    return {
        "pilot_status": "SPARSE_PILOT_WEAK",
        "runtime_kpis": {
            "cpu_utilization_fraction": 0.82,
            "economic_wall_clock_fraction": 0.88,
        },
        "decision_report": {
            "production_kpis": {
                "exact_replay_count": 12,
                "control_replay_count": 36,
                "normal_episode_count": 180,
                "stressed_episode_count": 180,
                "positive_stressed_count": 0,
                "normal_pass_candidate_count": 0,
                "stressed_pass_candidate_count": 0,
                "normal_p5_pass_rate_best": 0.0,
                "stressed_p5_pass_rate_best": 0.0,
                "normal_p5_pass_rate_median": 0.0,
                "stressed_p5_pass_rate_median": 0.0,
                "mll_breach_rate_minimum": 0.0,
                "mll_breach_rate_maximum": 0.1,
            },
            "green_checks": {"positive_final_net": False},
        },
    }


def _receipt() -> dict[str, object]:
    return {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
        "reconstruction_flag": False,
        "bundle_path": "/tmp/0032.evidence-v1",
        "manifest_path": "/tmp/0032.evidence-v1/manifest.json",
        "manifest_sha256": "d" * 64,
        "bundle_content_sha256": "e" * 64,
        "dataset_row_counts": {"episodes": 360},
    }


def test_state_and_kpis_publish_two_worker_three_core_contract(tmp_path: Path) -> None:
    manifest = _manifest()
    output = tmp_path / "reports/economic_evolution/0032"
    state = _write_state(
        output,
        manifest,
        state="STARTING",
        stage="FORENSIC_PNL_BRIDGE",
        next_action="RUN_SPARSE_PILOT",
        pilot=_pilot(),
    )
    kpis = json.loads((output / "production_kpis.json").read_text())

    assert state["schema"] == STATE_SCHEMA
    assert state["worker_count"] == 2
    assert state["evidence_writer_count"] == 1
    assert kpis["schema"] == KPI_SCHEMA
    assert kpis["workers"] == {"compute": 2, "evidence_writer": 1}
    assert kpis["exact_account_replays"] == 12
    assert kpis["combine_episodes_completed"] == 360


def test_terminal_result_is_hashed_and_never_authorizes_purchase(tmp_path: Path) -> None:
    manifest = _manifest()
    cost = _conditional_cost_report(
        tmp_path,
        manifest,
        _pilot(),
        "SPARSE_PILOT_WEAK",
    )
    result = _build_terminal_result(
        manifest=manifest,
        pilot=_pilot(),
        evidence_receipt=_receipt(),
        decision="SPARSE_PILOT_WEAK",
        conditional_cost_report=cost,
    )

    assert result["schema"] == RESULT_SCHEMA
    assert result["scientific_status"] == "SPARSE_PILOT_WEAK"
    assert result["new_data_purchase_count"] == 0
    assert result["actual_additional_spend_usd"] == 0.0
    assert result["autonomous_next_action"]["new_data_purchase_authorized"] is False
    assert result["result_hash"] == stable_hash(
        {key: value for key, value in result.items() if key != "result_hash"}
    )


def test_green_cost_matrix_is_metadata_only_and_does_not_purchase(tmp_path: Path) -> None:
    manifest = _manifest()
    pilot = {
        **_pilot(),
        "conditional_data_cost_matrix": {
            "NQ": {"tbbo": {"10_sessions_usd": 1.25}}
        },
    }
    report = _conditional_cost_report(
        tmp_path,
        manifest,
        pilot,
        "SPARSE_PILOT_GREEN",
    )

    assert report["status"] == "OFFICIAL_COST_MATRIX_AVAILABLE_NO_PURCHASE"
    assert report["actual_additional_spend_usd"] == 0.0
    assert report["purchase_performed"] is False
    assert report["automatic_purchase_allowed"] is False


def test_config_and_source_binding_are_frozen(tmp_path: Path) -> None:
    manifest = _manifest()
    source = (
        tmp_path
        / "reports/economic_evolution/0031/pilot/datasets/"
        "feature_matrices/part-000000.parquet"
    )
    source.parent.mkdir(parents=True)
    source.write_bytes(b"parquet-test")
    manifest["source_store"]["source_hashes"]["feature_matrices"]["path"] = str(
        source.relative_to(tmp_path)
    )
    config = _pilot_config(manifest, SparsePilotConfig)

    assert config.cpu_worker_count == 2
    assert config.maximum_strategies == 30
    assert config.edge_to_cost_ratios == (1.25, 1.5, 2.0, 3.0)
    assert _source_store_dir(tmp_path, manifest) == source.parents[2]


def test_blas_thread_contract_is_one() -> None:
    _set_single_thread_libraries()
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        assert os.environ[name] == "1"
