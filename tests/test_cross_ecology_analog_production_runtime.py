from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS, verify_evidence_bundle
from hydra.production import cross_ecology_analog_manifest as contract
from hydra.production import cross_ecology_analog_runtime as runtime


ROOT = Path(__file__).resolve().parents[1]
SOURCE_COMMIT = "a" * 40
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
POLICY_ID = "policy_0036"
COMPONENT_ID = "component_0036"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(root: Path, *, source_mode: str = "GENERATE_READ_ONLY_ONCE") -> dict[str, Any]:
    source_result = (
        root
        / "reports/economic_evolution/cross_ecology_session_path_analog_router_0036"
        / "scientific_result.json"
    )
    source: dict[str, Any] = {
        "decision_card_path": "config/research/cross_ecology_session_path_analog_router_v1.json",
        "decision_card_file_sha256": _sha(
            root / "config/research/cross_ecology_session_path_analog_router_v1.json"
        ),
        "decision_card_hash": json.loads(
            (root / "config/research/cross_ecology_session_path_analog_router_v1.json").read_text()
        )["card_hash"],
        "frozen_input_contract_hash": json.loads(
            (root / "config/research/cross_ecology_session_path_analog_router_v1.json").read_text()
        )["frozen_input_contract_hash"],
        "module_path": "hydra/research/cross_ecology_session_path_analog_router.py",
        "module_file_sha256": _sha(
            root / "hydra/research/cross_ecology_session_path_analog_router.py"
        ),
        "runner_path": "scripts/run_cross_ecology_session_path_analog_router.py",
        "runner_file_sha256": _sha(
            root / "scripts/run_cross_ecology_session_path_analog_router.py"
        ),
        "source_mode": source_mode,
        "result_path": str(source_result.relative_to(root)),
        "result_file_sha256": None,
        "result_hash": None,
        "root_authorization": contract.ROOT_AUTHORIZATION,
        "maximum_economic_replays": 1,
    }
    value: dict[str, Any] = {
        "schema": contract.MANIFEST_SCHEMA,
        "campaign_mode": contract.CAMPAIGN_MODE,
        "campaign_id": contract.CAMPAIGN_ID,
        "campaign_ordinal": 36,
        "class_id": contract.CLASS_ID,
        "policy_classes": [contract.CLASS_ID],
        "created_at_utc": "2026-07-19T12:00:00Z",
        "source_commit": SOURCE_COMMIT,
        "development_only": True,
        "economic_hypothesis": "bounded causal cross-ecology session-path analog router",
        "implementation_files": {
            relative: _sha(root / relative)
            for relative in contract._REQUIRED_IMPLEMENTATION_FILES
        },
        "research_source": source,
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "result_schema": "hydra_economic_production_result_v1",
            "result_name": "economic_production_result.json",
            "output_dir": "reports/economic_evolution/cross_ecology_session_path_analog_router_0036",
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "worker_count": 1,
            "asynchronous_evidence_writer_count": 1,
            "runtime_version": contract.RUNTIME_VERSION,
        },
        "multiplicity": {
            "prior_global_N_trials": 862_254,
            "reserved_delta_trials": 6,
            "expected_global_N_trials_after_reservation": 862_260,
            "prospective_comparisons": 6,
            "campaign_specific_inflation": 1.0,
            "reservation_receipt_path": (
                "reports/economic_evolution/"
                "hydra_cross_ecology_session_path_analog_router_0036_"
                "multiplicity_reservation.json"
            ),
            "reservation_receipt_sha256": HASH_A,
        },
        "evidence_bundle": {
            "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
            "required_datasets": list(REQUIRED_DATASETS),
            "destination": "data/cache/evidence_bundles",
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
            "embedded_material_requires_replay": False,
            "summary_only_completion_allowed": False,
        },
        "governance": {
            "tier_ceiling": "E",
            "tier_q_allowed": False,
            "promotion_allowed": False,
            "independent_confirmation_claimed": False,
            "q4_access_allowed": False,
            "protected_holdout_access_allowed": False,
            "new_data_purchase_allowed": False,
            "network_access_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "mission_database_write_allowed": False,
            "registry_write_allowed": False,
            "cemetery_write_allowed": False,
            "controller_version_change_required": False,
            "status_inheritance_allowed": False,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
    }
    value["manifest_hash"] = stable_hash(value)
    return value


def _copy_contract_project(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "project"
    for relative in contract._REQUIRED_IMPLEMENTATION_FILES:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    manifest = _manifest(root)
    path = root / "config/v7/cross_ecology_session_path_analog_router_0036.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return root, path


def _identity() -> dict[str, Any]:
    return {
        "campaign_id": contract.CAMPAIGN_ID,
        "grammar_id": contract.CLASS_ID,
        "policy_fingerprints": {POLICY_ID: HASH_A},
        "component_fingerprints": {COMPONENT_ID: HASH_B},
        "source_commit": SOURCE_COMMIT,
        "data_fingerprints": {"cached": HASH_C},
        "configuration_sha256": HASH_A,
        "seeds": [25],
        "created_at_utc": "2026-07-19T12:00:00Z",
        "expected_coverage": {
            "policy_ids": [POLICY_ID],
            "component_ids": [COMPONENT_ID],
            "required_episode_keys": [
                {"policy_id": POLICY_ID, "episode_id": "episode_0036", "horizon": "20D"}
            ],
            "allowed_horizons": ["20D"],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": False,
        },
    }


def _datasets() -> dict[str, list[dict[str, Any]]]:
    signal = {
        "campaign_id": contract.CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "signal_id": "trade_0036",
        "event_time": "2024-04-02T14:35:00Z",
        "market": "MNQ",
        "contract": "MNQM4",
        "timeframe": "1m",
        "signal": 1,
        "sizing": 1.0,
        "stop": 17990.0,
        "target": 18020.0,
        "veto": False,
        "component_role": "CROSS_ECOLOGY_ANALOG_ROUTER",
    }
    entry = {
        "campaign_id": contract.CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_0036",
        "entry_time": "2024-04-02T14:36:00Z",
        "market": "MNQ",
        "contract": "MNQM4",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 18000.0,
        "sizing": 1.0,
        "stop_price": 17990.0,
        "target_price": 18020.0,
    }
    exit_row = {
        "campaign_id": contract.CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_0036",
        "exit_time": "2024-04-02T15:00:00Z",
        "exit_price": 18005.0,
        "exit_reason": "TIME_EXIT",
    }
    trade = {
        "campaign_id": contract.CAMPAIGN_ID,
        "component_id": COMPONENT_ID,
        "trade_id": "trade_0036",
        "entry_time": "2024-04-02T14:36:00Z",
        "exit_time": "2024-04-02T15:00:00Z",
        "market": "MNQ",
        "contract": "MNQM4",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 18000.0,
        "exit_price": 18005.0,
        "gross_pnl": 10.0,
        "costs": 3.0,
        "net_pnl": 7.0,
    }
    paths: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    for scenario, costs in (("NORMAL", 3.0), ("STRESSED_1_5X", 4.5)):
        net = 10.0 - costs
        paths.append(
            {
                "campaign_id": contract.CAMPAIGN_ID,
                "policy_id": POLICY_ID,
                "episode_id": "episode_0036",
                "trading_day": "2024-04-02",
                "cost_scenario": scenario,
                "horizon": "20D",
                "realized_pnl": net,
                "unrealized_pnl": 0.0,
                "daily_pnl": net,
                "equity": 50_000.0 + net,
                "mll": 48_000.0,
                "mll_buffer": 2_000.0 + net,
                "minimum_mll_buffer": 2_000.0,
                "consistency": 1.0,
                "target_progress": net / 3_000.0,
                "costs": costs,
                "conflicts": [],
                "consistency_ok": True,
                "exposure": {"maximum_micro_contracts": 1.0},
                "component_attribution": {COMPONENT_ID: net},
            }
        )
        episodes.append(
            {
                "campaign_id": contract.CAMPAIGN_ID,
                "policy_id": POLICY_ID,
                "episode_id": "episode_0036",
                "episode_start": "2024-04-02T00:00:00Z",
                "horizon": "20D",
                "temporal_block": "FINAL_DEVELOPMENT",
                "duration_trading_days": 1,
                "target_reached": False,
                "mll_breached": False,
                "censored_state": True,
                "cost_scenario": scenario,
                "costs": costs,
                "net_pnl": net,
                "target_progress": net / 3_000.0,
                "minimum_mll_buffer": 2_000.0,
                "consistency_ok": True,
                "days_to_target": None,
                "failure_vector": {"INSUFFICIENT_TARGET_VELOCITY": 1.0},
                "terminal_state": "OPERATIONAL_HORIZON_NOT_REACHED",
            }
        )
    return {
        "component_signals": [signal],
        "component_entries": [entry],
        "component_exits": [exit_row],
        "component_trades": [trade],
        "account_policy_membership": [
            {
                "campaign_id": contract.CAMPAIGN_ID,
                "policy_id": POLICY_ID,
                "component_id": COMPONENT_ID,
                "risk_allocation": 1.0,
                "component_role": "CROSS_ECOLOGY_ANALOG_ROUTER",
            }
        ],
        "account_daily_paths": paths,
        "episodes": episodes,
        "provenance": [
            {
                "campaign_id": contract.CAMPAIGN_ID,
                "validator_version": "cross_ecology_session_path_analog_router_v1",
                "replay_version": contract.SCIENTIFIC_RESULT_SCHEMA,
                "market_data_role": "DISCOVERY_VALIDATION_FINAL_DEVELOPMENT_PRE_Q4",
                "access_ledger_sha256": HASH_A,
                "reconstruction_flag": False,
                "immutable_checksums": {
                    "configuration": HASH_A,
                    "data:cached": HASH_C,
                },
                "recorded_at_utc": "2026-07-19T12:01:00Z",
            }
        ],
    }


def _canonical() -> dict[str, Any]:
    datasets = _datasets()
    core = {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "identity": _identity(),
        "datasets": datasets,
        "dataset_hashes": {key: stable_hash(value) for key, value in datasets.items()},
        "adapter_requires_economic_replay": False,
    }
    return {**core, "canonical_material_hash": stable_hash(core)}


def _scientific(manifest: dict[str, Any]) -> dict[str, Any]:
    canonical = _canonical()
    production_manifest = {
        "schema": manifest["schema"],
        "campaign_id": contract.CAMPAIGN_ID,
        "campaign_ordinal": 36,
        "path": contract.DEFAULT_MANIFEST_PATH,
        "production_manifest_hash": manifest["manifest_hash"],
        "manifest_file_sha256": HASH_C,
        "source_commit": manifest["source_commit"],
        "decision_card_hash": manifest["research_source"]["decision_card_hash"],
        "implementation_files": dict(sorted(manifest["implementation_files"].items())),
        "multiplicity_reservation": {
            "path": manifest["multiplicity"]["reservation_receipt_path"],
            "sha256": manifest["multiplicity"]["reservation_receipt_sha256"],
            "reserved_delta_trials": manifest["multiplicity"]["reserved_delta_trials"],
        },
        "verified_against_committed_blobs": True,
        "source_commit_is_live_head_ancestor": True,
    }
    core = {
        "schema": contract.SCIENTIFIC_RESULT_SCHEMA,
        "campaign_id": contract.CAMPAIGN_ID,
        "source_commit": manifest["source_commit"],
        "production_manifest": production_manifest,
        "branch_id": contract.CLASS_ID,
        "status": "SESSION_PATH_ANALOG_FALSIFIED",
        "evidence_role": contract.EVIDENCE_ROLE,
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_audit": {
            "decision_card_hash": manifest["research_source"]["decision_card_hash"],
            "decision_card_file_sha256": manifest["research_source"][
                "decision_card_file_sha256"
            ],
        },
        "candidate_decisions": [],
        "branch_gate": {"passed_candidate_ids": [], "candidate_gates": []},
        "canonical_evidence_material": canonical,
        "canonical_evidence_material_hash": canonical["canonical_material_hash"],
        "governance": {
            "incremental_data_spend_usd": 0.0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "cemetery_writes": 0,
            "tier_q_allowed": False,
            "promotion_allowed": False,
        },
    }
    return {**core, "result_hash": stable_hash(core)}


def _metrics() -> dict[str, Any]:
    return {
        "proposal_count": 1,
        "candidate_count": 1,
        "canonical_policy_count": 1,
        "control_policy_count": 0,
        "normal_episode_count": 1,
        "stressed_episode_count": 1,
        "combine_episode_count": 2,
        "normal_pass_candidate_count": 0,
        "stressed_pass_candidate_count": 0,
        "positive_stressed_count": 1,
        "best_normal_pass_rate": 0.0,
        "median_normal_pass_rate": 0.0,
        "best_stressed_pass_rate": 0.0,
        "median_stressed_pass_rate": 0.0,
        "best_stressed_target_progress": 0.1,
        "median_stressed_target_progress": 0.1,
        "minimum_stressed_mll_breach_rate": 0.0,
        "maximum_stressed_mll_breach_rate": 0.0,
        "near_pass_count": 0,
        "tier_e_passed_candidate_ids": [],
        "headline_by_candidate": [{"candidate_id": "c1"}],
    }


def test_specialized_manifest_and_generic_dispatch_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, path = _copy_contract_project(tmp_path)
    from hydra.production.manifest import load_and_validate_production_manifest

    monkeypatch.setattr(contract, "_committed_implementation", lambda *_args: None)
    loaded = load_and_validate_production_manifest(path)
    assert loaded["campaign_id"] == contract.CAMPAIGN_ID
    assert loaded["campaign_ordinal"] == 36


def test_generic_runtime_dispatches_run_and_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hydra.production.runtime as generic

    manifest = {"campaign_mode": contract.CAMPAIGN_MODE}
    monkeypatch.setattr(generic, "load_and_validate_production_manifest", lambda _path: manifest)
    monkeypatch.setattr(
        runtime,
        "run_cross_ecology_analog_manifest",
        lambda *_args, **_kwargs: {"route": "run"},
    )
    monkeypatch.setattr(
        runtime,
        "read_cross_ecology_analog_status",
        lambda *_args, **_kwargs: {"route": "status"},
    )
    assert generic.run_production_manifest(
        "manifest.json", contract_map_path="map", cache_root="cache"
    ) == {"route": "run"}
    assert generic.read_live_status("manifest.json") == {"route": "status"}


def test_preexisting_source_is_hash_bound_and_never_calls_router(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    source_path = root / manifest["research_source"]["result_path"]
    source_path.parent.mkdir(parents=True, exist_ok=True)
    scientific = _scientific(manifest)
    source_path.write_text(json.dumps(scientific, indent=2, sort_keys=True) + "\n")
    manifest["research_source"].update(
        source_mode="PREEXISTING_HASH_BOUND",
        result_file_sha256=_sha(source_path),
        result_hash=scientific["result_hash"],
    )
    called = False

    def forbidden(*_args: object, **_kwargs: object) -> None:
        nonlocal called
        called = True
        raise AssertionError("router was called")

    monkeypatch.setattr(
        "hydra.research.cross_ecology_session_path_analog_router.run_economic_tripwire",
        forbidden,
    )
    observed, executed = runtime._obtain_scientific_result(root, source_path.parent, manifest)
    assert observed == source_path
    assert executed is False
    assert called is False


def test_generate_read_only_once_reuses_lease_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    output = root / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    scientific = _scientific(manifest)
    calls = 0

    def fake(*_args: object, **_kwargs: object) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return scientific

    monkeypatch.setattr(
        "hydra.research.cross_ecology_session_path_analog_router.run_economic_tripwire",
        fake,
    )
    first, executed = runtime._obtain_scientific_result(root, output, manifest)
    second, resumed = runtime._obtain_scientific_result(root, output, manifest)
    assert first == second
    assert executed is resumed is True
    assert calls == 1


def test_scientific_governance_and_canonical_hash_fail_closed(tmp_path: Path) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    scientific = _scientific(manifest)
    broken = json.loads(json.dumps(scientific))
    broken["governance"]["orders"] = 1
    core = dict(broken)
    core.pop("result_hash")
    broken["result_hash"] = stable_hash(core)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="governance"):
        runtime._validate_scientific_payload(broken, manifest)
    broken = json.loads(json.dumps(scientific))
    broken["canonical_evidence_material"]["datasets"]["component_trades"][0][
        "net_pnl"
    ] = 999.0
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="canonical"):
        runtime._canonical_material(broken, manifest)


def test_embedded_material_seals_deep_and_recovers_without_replay(tmp_path: Path) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    manifest["evidence_bundle"]["destination"] = "data/cache/evidence_bundles"
    output = root / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    scientific = _scientific(manifest)
    canonical = _canonical()
    receipt = runtime._seal_evidence(
        root, output, manifest, scientific, canonical, _metrics()
    )
    verified = verify_evidence_bundle(receipt["bundle_path"], deep=True)
    assert verified["status"] == "COMPLETE"
    recovered = runtime._seal_evidence(
        root, output, manifest, scientific, canonical, _metrics()
    )
    assert recovered["bundle_content_sha256"] == receipt["bundle_content_sha256"]


def test_terminal_restart_returns_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    output = root / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    result_path = output / "economic_production_result.json"
    result_path.write_text("{}\n")
    sentinel = {"status": "COMPLETE", "campaign_id": contract.CAMPAIGN_ID}
    monkeypatch.setattr(
        "hydra.production.manifest.load_and_validate_production_manifest",
        lambda _path: manifest,
    )
    monkeypatch.setattr(runtime, "validate_cross_ecology_analog_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_load_terminal_result", lambda *_a, **_k: sentinel)
    monkeypatch.setattr(
        runtime,
        "_atomic_json",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("unexpected write")),
    )
    assert runtime.run_cross_ecology_analog_manifest(path) == sentinel


def test_atomic_production_result_is_literal_last_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    output = root / manifest["runtime"]["output_dir"]
    source_path = root / manifest["research_source"]["result_path"]
    scientific = _scientific(manifest)
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps(scientific, indent=2, sort_keys=True) + "\n")
    writes: list[str] = []
    monkeypatch.setattr(
        "hydra.production.manifest.load_and_validate_production_manifest",
        lambda _path: manifest,
    )
    monkeypatch.setattr(runtime, "validate_cross_ecology_analog_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_obtain_scientific_result", lambda *_a, **_k: (source_path, True))
    monkeypatch.setattr(runtime, "_load_scientific_result", lambda *_a, **_k: scientific)
    monkeypatch.setattr(runtime, "_canonical_material", lambda *_a, **_k: _canonical())
    monkeypatch.setattr(runtime, "_economic_metrics", lambda *_a, **_k: _metrics())
    monkeypatch.setattr(
        runtime,
        "_seal_evidence",
        lambda *_a, **_k: {
            "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
            "schema_version": 1,
            "campaign_id": contract.CAMPAIGN_ID,
            "bundle_path": "/tmp/bundle",
            "manifest_path": "/tmp/bundle/manifest",
            "manifest_sha256": HASH_A,
            "bundle_content_sha256": HASH_B,
            "dataset_row_counts": {key: 1 for key in REQUIRED_DATASETS},
            "evidence_status": "FRESH_DEVELOPMENT_EVIDENCE",
            "reconstruction_flag": False,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_terminal_result",
        lambda **_kwargs: {
            "status": "COMPLETE",
            "autonomous_next_action": {"action": "NEXT"},
        },
    )

    def record(destination: Path, _value: object) -> None:
        writes.append(destination.name)

    monkeypatch.setattr(runtime, "_atomic_json", record)
    result = runtime.run_cross_ecology_analog_manifest(path)
    assert result["status"] == "COMPLETE"
    assert writes[-3:] == [
        "production_state.json",
        "production_kpis.json",
        "economic_production_result.json",
    ]
    assert writes[-1] == "economic_production_result.json"
