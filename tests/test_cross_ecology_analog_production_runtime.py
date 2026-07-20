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
            "network_requests": 0,
            "broker_connections": 0,
            "orders": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "cemetery_writes": 0,
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
    safety = {field: 0 for field in runtime.SAFETY_COUNTER_FIELDS}
    core = {
        "contract": "HYDRA_EVIDENCE_BUNDLE_V1",
        "schema_version": 1,
        "source_audit": dict(safety),
        "governance": dict(safety),
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
            "network_requests": 0,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "cemetery_writes": 0,
        },
        "candidate_decisions": [],
        "branch_gate": {"passed_candidate_ids": [], "candidate_gates": []},
        "canonical_evidence_material": canonical,
        "canonical_evidence_material_hash": canonical["canonical_material_hash"],
        "governance": {
            "incremental_data_spend_usd": 0.0,
            "data_purchase_count": 0,
            "network_requests": 0,
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


def _nonmaterialized_scientific(
    manifest: dict[str, Any], *, source_manifest_file_sha256: str = HASH_C
) -> dict[str, Any]:
    candidate_id = "cross_ecology_analog_v1:CROSS_ECOLOGY_PRICE_PATH:0835"
    control_names = (
        "PRIMARY",
        "OWN_PATH_ONLY",
        "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM",
        "ANALOG_LABEL_PERMUTATION",
        "DIRECTION_FLIP",
    )
    fragment_core = {
        "schema": "hydra_compact_complete_evidence_bundle_v1",
        "campaign_id": contract.CAMPAIGN_ID,
        "candidate_id": candidate_id,
        "source_commit": manifest["source_commit"],
        "complete": False,
        "materialization_status": "NON_MATERIALIZED_FRAGMENT_EXCLUDED",
        "canonical_evidence_material": None,
        "tier_ceiling": "E",
        "tier_q_allowed": False,
        "promotion_allowed": False,
        "row_counts": {
            "canonical_rows": 0,
            "controls": 5,
            "routed_event_rows": 0,
            "routed_trade_rows": 0,
            "scenarios": 2,
        },
        "materialization_exclusion_reasons": ["PRIMARY:NO_MATERIALIZED_TRADE"],
        "routed_event_ledgers": {
            name: [] for name in control_names
        },
        "routed_trade_ledgers": {
            name: {"NORMAL": [], "STRESSED_1_5X": []}
            for name in control_names
        },
        "ledger_hashes": {
            "events": {name: stable_hash([]) for name in control_names},
            "trades": {
                name: {
                    "NORMAL": stable_hash([]),
                    "STRESSED_1_5X": stable_hash([]),
                }
                for name in control_names
            },
        },
        "account_episode_material": {
            "evaluations": {
                "PRIMARY": {
                    "FINAL_DEVELOPMENT": {
                        "20": {
                            "NORMAL": {
                                "episodes": 0,
                                "episode_ledger": [],
                                "episode_ledger_hash": stable_hash([]),
                                "passes": 0,
                                "mll_breaches": 0,
                                "net_total_usd": 0.0,
                                "pass_rate": 0.0,
                                "mll_breach_rate": 0.0,
                                "target_progress_median": 0.0,
                                "target_progress_p25": 0.0,
                            },
                            "full_coverage_start_count": 0,
                        }
                    }
                }
            }
        },
    }
    fragment_core["ledger_hashes"]["account_material"] = stable_hash(
        fragment_core["account_episode_material"]
    )
    fragment = {
        **fragment_core,
        "evidence_bundle_hash": stable_hash(fragment_core),
    }
    production_manifest = {
        "schema": manifest["schema"],
        "campaign_id": contract.CAMPAIGN_ID,
        "campaign_ordinal": 36,
        "path": contract.DEFAULT_MANIFEST_PATH,
        "production_manifest_hash": manifest["manifest_hash"],
        "manifest_file_sha256": source_manifest_file_sha256,
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
    compact_account_cell = {
        "evaluations": {
            "PRIMARY": {
                "FINAL_DEVELOPMENT": {
                    "20": {
                        "NORMAL": {
                            "episodes": 0,
                            "episode_ledger_hash": stable_hash([]),
                            "passes": 0,
                            "mll_breaches": 0,
                            "net_total_usd": 0.0,
                            "pass_rate": 0.0,
                            "mll_breach_rate": 0.0,
                            "target_progress_median": 0.0,
                            "target_progress_p25": 0.0,
                        },
                        "full_coverage_start_count": 0,
                    }
                }
            }
        }
    }
    decision = {
        "candidate_id": candidate_id,
        "materialized_economic_event_count": 0,
        "routed_event_count": 0,
        "future_outcome_censored_signal_count": 0,
        "routed_signals_by_role": {
            "DISCOVERY": 0,
            "VALIDATION": 0,
            "FINAL_DEVELOPMENT": 0,
        },
        "routed_events_by_role": {
            "DISCOVERY": 0,
            "VALIDATION": 0,
            "FINAL_DEVELOPMENT": 0,
        },
        "control_event_counts": {
            name: 0
            for name in (
                "PRIMARY",
                "OWN_PATH_ONLY",
                "SESSION_MARKET_EXPOSURE_MATCHED_RANDOM",
                "ANALOG_LABEL_PERMUTATION",
                "DIRECTION_FLIP",
            )
        },
        "routed_events_by_market": {},
        "control_opportunity_identity_equal": True,
        "event_ledger_hash": stable_hash([]),
        "discovery_stressed_net_per_one_micro_usd": 0.0,
        "discovery_target_first_rate": 0.0,
        "account_frontier": [compact_account_cell],
        "discovery_selected_account_cell": compact_account_cell,
        "stressed_context_economics_per_one_micro": {
            "future_outcome_censored_signal_count": 0,
            "net_by_market_usd": {},
            "net_by_temporal_subblock_usd": {
                "VALIDATION_H1": 0.0,
                "VALIDATION_H2": 0.0,
                "FINAL_DEVELOPMENT_H1": 0.0,
                "FINAL_DEVELOPMENT_H2": 0.0,
            },
            "positive_market_or_temporal_context_count": 0,
            "positive_markets": [],
            "positive_temporal_subblocks": [],
        },
    }
    # The production contract freezes six prospective rules.  Reusing the
    # structure with distinct IDs keeps the fixture compact while proving the
    # exact candidate inventory and zero-material invariants.
    decisions: list[dict[str, Any]] = []
    bundles: dict[str, dict[str, Any]] = {}
    for index in range(6):
        candidate = f"{candidate_id}:{index}"
        row = {**decision, "candidate_id": candidate}
        bundle_core = {**fragment_core, "candidate_id": candidate}
        bundle = {**bundle_core, "evidence_bundle_hash": stable_hash(bundle_core)}
        row["evidence_bundle_hash"] = bundle["evidence_bundle_hash"]
        decisions.append(row)
        bundles[candidate] = bundle
    bundle_hashes = {
        key: value["evidence_bundle_hash"] for key, value in bundles.items()
    }
    core = {
        "schema": contract.SCIENTIFIC_RESULT_SCHEMA,
        "campaign_id": contract.CAMPAIGN_ID,
        "source_commit": manifest["source_commit"],
        "production_manifest": production_manifest,
        "branch_id": contract.CLASS_ID,
        "status": "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
        "evidence_role": contract.EVIDENCE_ROLE,
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_audit": {
            "decision_card_hash": manifest["research_source"]["decision_card_hash"],
            "decision_card_file_sha256": manifest["research_source"][
                "decision_card_file_sha256"
            ],
            **{field: 0 for field in runtime.SAFETY_COUNTER_FIELDS},
        },
        "candidate_decisions": decisions,
        "branch_gate": {
            "passed_candidate_ids": [],
            "status": "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
            "threshold_relaxation_allowed": False,
            "tier_ceiling": "E",
        },
        "power_preflight": {
            "passed": False,
            "underpowered_status": "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
        },
        "canonical_evidence_material": None,
        "canonical_evidence_status": contract.NON_ECONOMIC_CANONICAL_STATUS,
        "evidence_bundles": bundles,
        "evidence_bundle_hashes": bundle_hashes,
        "governance": {
            "incremental_data_spend_usd": 0.0,
            "tier_q_allowed": False,
            "promotion_allowed": False,
            **{field: 0 for field in runtime.SAFETY_COUNTER_FIELDS},
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


def _adoption_manifest(
    base: dict[str, Any],
    *,
    scientific_file_sha256: str,
    scientific_result_hash: str,
    lease_file_sha256: str,
    lease_attempt_hash: str,
    source_manifest_file_sha256: str = HASH_C,
    source_manifest_worm_commit: str = "c" * 40,
    source_manifest_worm_tag: str = "worm/cross-ecology-0036-test",
) -> dict[str, Any]:
    old_manifest_hash = base["manifest_hash"]
    old_source_commit = base["source_commit"]
    value = json.loads(json.dumps(base))
    value["source_commit"] = "b" * 40
    value["research_source"].update(
        source_mode="PREEXISTING_HASH_BOUND",
        result_file_sha256=scientific_file_sha256,
        result_hash=scientific_result_hash,
    )
    output = Path(value["runtime"]["output_dir"])
    value["completed_scientific_result_adoption"] = {
        "schema": contract.COMPLETED_RESULT_ADOPTION_SCHEMA,
        "classification": contract.COMPLETED_RESULT_ADOPTION_CLASSIFICATION,
        "terminal_mode": contract.NON_ECONOMIC_TERMINAL_MODE,
        "economic_outcomes_changed": False,
        "scientific_policy_changed": False,
        "economic_replay_allowed": False,
        "source_manifest_hash": old_manifest_hash,
        "source_manifest_file_sha256": source_manifest_file_sha256,
        "source_manifest_path": contract.DEFAULT_MANIFEST_PATH,
        "source_manifest_worm_commit": source_manifest_worm_commit,
        "source_manifest_worm_tag": source_manifest_worm_tag,
        "source_commit": old_source_commit,
        "scientific_status": "SESSION_PATH_ANALOG_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
        "canonical_evidence_status": contract.NON_ECONOMIC_CANONICAL_STATUS,
        "scientific_result_path": value["research_source"]["result_path"],
        "scientific_result_hash": scientific_result_hash,
        "scientific_result_file_sha256": scientific_file_sha256,
        "replay_lease_path": (output / "scientific_replay_attempt.json").as_posix(),
        "replay_lease_attempt_hash": lease_attempt_hash,
        "replay_lease_file_sha256": lease_file_sha256,
        "multiplicity_preregistration_hash": old_manifest_hash,
        "audit_receipt_path": (output / "non_economic_audit_receipt.json").as_posix(),
    }
    value["compatible_artifact_manifest_hashes"] = [old_manifest_hash]
    value["resumable_snapshot_compatibility"] = {
        "schema": "hydra_resumable_snapshot_compatibility_v1",
        "classification": "HASH_BOUND_TECHNICAL_REVISION_RESUME_ONLY",
        "economic_outcomes_changed": False,
        "scientific_policy_changed": False,
        "bindings": [
            {
                "path": (output / "production_state.json").as_posix(),
                "schema": runtime.STATE_SCHEMA,
                "hash_field": "state_hash",
                "manifest_hash": old_manifest_hash,
                "source_commit": old_source_commit,
                "snapshot_hash": HASH_A,
            },
            {
                "path": (output / "production_kpis.json").as_posix(),
                "schema": runtime.KPI_SCHEMA,
                "hash_field": "kpi_hash",
                "manifest_hash": old_manifest_hash,
                "source_commit": old_source_commit,
                "snapshot_hash": HASH_B,
            },
        ],
    }
    value.pop("manifest_hash")
    value["manifest_hash"] = stable_hash(value)
    return value


def test_specialized_manifest_and_generic_dispatch_validate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, path = _copy_contract_project(tmp_path)
    from hydra.production.manifest import load_and_validate_production_manifest

    monkeypatch.setattr(contract, "_committed_implementation", lambda *_args: None)
    loaded = load_and_validate_production_manifest(path)
    assert loaded["campaign_id"] == contract.CAMPAIGN_ID
    assert loaded["campaign_ordinal"] == 36


def test_preexisting_manifest_validation_does_not_open_economic_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    manifest = _manifest(root, source_mode="PREEXISTING_HASH_BOUND")
    manifest["research_source"].update(
        result_file_sha256=HASH_A,
        result_hash=HASH_B,
    )
    manifest.pop("manifest_hash")
    manifest["manifest_hash"] = stable_hash(manifest)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    outcome = root / manifest["research_source"]["result_path"]
    assert not outcome.exists()
    monkeypatch.setattr(contract, "_committed_implementation", lambda *_args: None)

    # Structural discovery is valid even though the economic source does not
    # exist yet.  Its content is a post-reservation runtime concern.
    contract.validate_cross_ecology_analog_manifest(manifest, manifest_path=path)
    assert not outcome.exists()


def test_card_and_manifest_require_all_exact_zero_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    card_path = root / "config/research/cross_ecology_session_path_analog_router_v1.json"
    card = json.loads(card_path.read_text())
    card["governance"]["network_requests"] = 1
    card.pop("card_hash")
    card["card_hash"] = stable_hash(card)
    card_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    manifest = _manifest(root)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    monkeypatch.setattr(contract, "_committed_implementation", lambda *_args: None)
    with pytest.raises(contract.CrossEcologyAnalogManifestError, match="governance counters"):
        contract.validate_cross_ecology_analog_manifest(manifest, manifest_path=path)

    # Restore a valid copied card, then prove the production declaration itself
    # is equally strict and does not int-coerce booleans/floats.
    shutil.copy2(
        ROOT / "config/research/cross_ecology_session_path_analog_router_v1.json",
        card_path,
    )
    manifest = _manifest(root)
    manifest["governance"]["registry_writes"] = 0.0
    manifest.pop("manifest_hash")
    manifest["manifest_hash"] = stable_hash(manifest)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    with pytest.raises(contract.CrossEcologyAnalogManifestError, match="counters"):
        contract.validate_cross_ecology_analog_manifest(manifest, manifest_path=path)


def test_reservation_failure_precedes_all_scientific_source_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root, source_mode="PREEXISTING_HASH_BOUND")
    manifest["research_source"].update(
        result_file_sha256=HASH_A,
        result_hash=HASH_B,
    )
    accessed = False

    def no_reservation(*_args: object) -> None:
        raise runtime.CrossEcologyAnalogRuntimeError("reservation missing")

    def forbidden_source(*_args: object, **_kwargs: object) -> None:
        nonlocal accessed
        accessed = True
        raise AssertionError("scientific outcome accessed before reservation")

    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", no_reservation)
    monkeypatch.setattr(runtime, "_load_scientific_result", forbidden_source)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="reservation"):
        runtime._obtain_scientific_result(
            root,
            root / manifest["runtime"]["output_dir"],
            manifest,
        )
    assert accessed is False


def test_status_proves_reservation_before_durable_existence_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    reserved = False
    original_is_file = Path.is_file

    def reserve(*_args: object) -> None:
        nonlocal reserved
        reserved = True

    output = root / manifest["runtime"]["output_dir"]
    guarded = {
        (output / "economic_production_result.json").resolve(),
        (output / "production_state.json").resolve(),
    }

    def guarded_is_file(candidate: Path) -> bool:
        if candidate.resolve() in guarded and not reserved:
            raise AssertionError("durable outcome existence checked before reservation")
        return original_is_file(candidate)

    monkeypatch.setattr(
        "hydra.production.manifest.load_and_validate_production_manifest",
        lambda _path: manifest,
    )
    monkeypatch.setattr(runtime, "validate_cross_ecology_analog_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", reserve)
    monkeypatch.setattr(Path, "is_file", guarded_is_file)
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "1")
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="NETWORK"):
        runtime.read_cross_ecology_analog_status(path)
    assert reserved is False
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "0")
    observed = runtime.read_cross_ecology_analog_status(path)
    assert reserved is True
    assert observed["state"] == "NOT_STARTED"


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
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a: None)
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
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a: None)
    first, executed = runtime._obtain_scientific_result(root, output, manifest)
    second, resumed = runtime._obtain_scientific_result(root, output, manifest)
    assert first == second
    assert executed is True
    assert resumed is False
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
    broken["source_audit"]["network_requests"] = 1
    core = dict(broken)
    core.pop("result_hash")
    broken["result_hash"] = stable_hash(core)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="source_audit"):
        runtime._validate_scientific_payload(broken, manifest)
    broken = json.loads(json.dumps(scientific))
    broken["canonical_evidence_material"]["datasets"]["component_trades"][0][
        "net_pnl"
    ] = 999.0
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="canonical"):
        runtime._canonical_material(broken, manifest)
    broken_material = _canonical()
    broken_material["source_audit"]["network_requests"] = 1
    broken_material.pop("canonical_material_hash")
    broken_material["canonical_material_hash"] = stable_hash(broken_material)
    broken = _scientific(manifest)
    broken["canonical_evidence_material"] = broken_material
    broken["canonical_evidence_material_hash"] = broken_material[
        "canonical_material_hash"
    ]
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="source_audit"):
        runtime._canonical_material(broken, manifest)


def test_replay_lease_requires_full_identity_and_safety() -> None:
    manifest = {"manifest_hash": HASH_A, "source_commit": SOURCE_COMMIT}
    lease = {
        "schema": runtime.REPLAY_LEASE_SCHEMA,
        "campaign_id": contract.CAMPAIGN_ID,
        "manifest_hash": HASH_A,
        "source_commit": SOURCE_COMMIT,
        "generation": 0,
        "maximum_generations": 1,
        "status": "RUNNING",
        "authorization": contract.ROOT_AUTHORIZATION,
        "runner_pid": 123,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    runtime._validate_replay_lease(lease, manifest)
    for field, invalid in (
        ("campaign_id", "other"),
        ("manifest_hash", HASH_B),
        ("source_commit", "b" * 40),
        ("maximum_generations", 2),
        ("network_requests", 1),
        ("status", "RETRY"),
    ):
        broken = dict(lease)
        broken[field] = invalid
        with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="lease"):
            runtime._validate_replay_lease(broken, manifest)


def test_terminal_generated_source_requires_complete_hash_bound_lease(
    tmp_path: Path,
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    output = root / manifest["runtime"]["output_dir"]
    output.mkdir(parents=True)
    source_path = root / manifest["research_source"]["result_path"]
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps({"result_hash": HASH_A}) + "\n")
    source_sha = _sha(source_path)
    scientific = {
        "source_mode": "GENERATE_READ_ONLY_ONCE",
        "economic_replay_executed_by_adapter": True,
        "scientific_replay_previously_completed": True,
        "file_sha256": source_sha,
        "result_hash": HASH_A,
    }
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="lacks"):
        runtime._validate_terminal_scientific_replay(
            output, manifest, scientific, source_path
        )

    lease = {
        "schema": runtime.REPLAY_LEASE_SCHEMA,
        "campaign_id": contract.CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "generation": 0,
        "maximum_generations": 1,
        "status": "RUNNING",
        "authorization": contract.ROOT_AUTHORIZATION,
        "runner_pid": 123,
        **{field: 0 for field in runtime.SAFETY_COUNTER_FIELDS},
    }

    def write_lease(value: dict[str, Any]) -> None:
        payload = dict(value)
        payload["attempt_hash"] = stable_hash(payload)
        (output / "scientific_replay_attempt.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n"
        )

    write_lease(lease)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="not COMPLETE"):
        runtime._validate_terminal_scientific_replay(
            output, manifest, scientific, source_path
        )

    complete = {
        **lease,
        "status": "COMPLETE",
        "result_hash": HASH_A,
        "result_file_sha256": HASH_B,
    }
    write_lease(complete)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="hash drift"):
        runtime._validate_terminal_scientific_replay(
            output, manifest, scientific, source_path
        )

    complete["result_file_sha256"] = source_sha
    write_lease(complete)
    runtime._validate_terminal_scientific_replay(
        output, manifest, scientific, source_path
    )

    wrong_mode = dict(scientific)
    wrong_mode["source_mode"] = "PREEXISTING_HASH_BOUND"
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="source-mode"):
        runtime._validate_terminal_scientific_replay(
            output, manifest, wrong_mode, source_path
        )
    wrong_flag = dict(scientific)
    wrong_flag["scientific_replay_previously_completed"] = False
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="source-mode"):
        runtime._validate_terminal_scientific_replay(
            output, manifest, wrong_flag, source_path
        )


def test_runtime_network_guard_and_terminal_safety(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "1")
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="NETWORK"):
        runtime._assert_closed_governance_environment()
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "0")

    metrics = _metrics()
    manifest = {"manifest_hash": HASH_A, "source_commit": SOURCE_COMMIT}
    kpis = runtime._kpis(
        manifest,
        state="COMPLETE",
        sequence=2,
        metrics=metrics,
        elapsed=1.0,
        cpu_seconds=1.0,
        replay_executed=True,
    )
    action = runtime._next_action("SESSION_PATH_ANALOG_FALSIFIED", metrics)
    state = {
        "next_action": action["action"],
        "stage": "TIER_E_BRANCH_DECISION_SEALED",
        "checkpoint_sequence": 2,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
    }
    result = {
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "network_requests": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
        "kpis": kpis,
        "autonomous_next_action": action,
        "development_only": True,
        "independently_confirmed": False,
        "status_inheritance": False,
        "scientific_status": "SESSION_PATH_ANALOG_FALSIFIED",
    }
    runtime._validate_terminal_safety(result, state, kpis)
    report = {
        "metrics": metrics,
        "scientific_status": "SESSION_PATH_ANALOG_FALSIFIED",
        "autonomous_next_action": action,
    }
    scientific = {
        "status": "SESSION_PATH_ANALOG_FALSIFIED",
        "branch_gate": {"passed_candidate_ids": []},
    }
    runtime._validate_terminal_semantics(result, state, kpis, report, scientific)
    broken = json.loads(json.dumps(result))
    broken["autonomous_next_action"]["network_access_authorized"] = True
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="next-action"):
        runtime._validate_terminal_safety(broken, state, kpis)
    broken = json.loads(json.dumps(result))
    broken["kpis"]["orders"] = 1
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="orders"):
        runtime._validate_terminal_safety(broken, state, kpis)
    broken_state = dict(state)
    broken_state["stage"] = "OTHER"
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="semantic"):
        runtime._validate_terminal_semantics(
            result, broken_state, kpis, report, scientific
        )
    broken_report = json.loads(json.dumps(report))
    broken_report["autonomous_next_action"]["action"] = "MUTATED"
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="semantic"):
        runtime._validate_terminal_semantics(
            result, state, kpis, broken_report, scientific
        )


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


def _rehash_nonmaterialized_scientific(value: dict[str, Any]) -> None:
    value.pop("result_hash", None)
    value["result_hash"] = stable_hash(value)


def test_nonmaterialized_contract_rejects_rehashed_hidden_economic_rows(
    tmp_path: Path,
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root)
    scientific = _nonmaterialized_scientific(manifest)
    runtime._validate_scientific_payload(scientific, manifest)
    assert runtime._canonical_material(scientific, manifest) is None
    assert "TIER_E" not in runtime._next_action(scientific["status"], {
        "tier_e_passed_candidate_ids": []
    })["action"]

    hidden_event = json.loads(json.dumps(scientific))
    candidate_id = hidden_event["candidate_decisions"][0]["candidate_id"]
    bundle = hidden_event["evidence_bundles"][candidate_id]
    bundle["routed_event_ledgers"]["PRIMARY"] = [{"hidden": True}]
    bundle["ledger_hashes"]["events"]["PRIMARY"] = stable_hash([{"hidden": True}])
    bundle.pop("evidence_bundle_hash")
    bundle["evidence_bundle_hash"] = stable_hash(bundle)
    hidden_event["evidence_bundle_hashes"][candidate_id] = bundle["evidence_bundle_hash"]
    hidden_event["candidate_decisions"][0]["evidence_bundle_hash"] = bundle[
        "evidence_bundle_hash"
    ]
    _rehash_nonmaterialized_scientific(hidden_event)
    runtime._validate_scientific_payload(hidden_event, manifest)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="routed event"):
        runtime._canonical_material(hidden_event, manifest)

    hidden_episode = json.loads(json.dumps(scientific))
    hidden_episode["candidate_decisions"][0]["account_frontier"][0]["evaluations"][
        "PRIMARY"
    ]["FINAL_DEVELOPMENT"]["20"]["NORMAL"]["episodes"] = 1
    _rehash_nonmaterialized_scientific(hidden_episode)
    runtime._validate_scientific_payload(hidden_episode, manifest)
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="account evidence"):
        runtime._canonical_material(hidden_episode, manifest)


def test_preexisting_terminal_can_never_claim_adapter_replay(
    tmp_path: Path,
) -> None:
    root, _ = _copy_contract_project(tmp_path)
    manifest = _manifest(root, source_mode="PREEXISTING_HASH_BOUND")
    source_path = root / manifest["research_source"]["result_path"]
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(json.dumps({"result_hash": HASH_A}) + "\n")
    scientific = {
        "source_mode": "PREEXISTING_HASH_BOUND",
        "economic_replay_executed_by_adapter": True,
        "scientific_replay_previously_completed": False,
        "file_sha256": _sha(source_path),
        "result_hash": HASH_A,
    }
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="source-mode"):
        runtime._validate_terminal_scientific_replay(
            source_path.parent, manifest, scientific, source_path
        )


def test_completed_nonmaterialized_result_is_adopted_without_replay_or_fake_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess
    import hydra.production.runtime as generic

    root, manifest_path = _copy_contract_project(tmp_path)
    base = _manifest(root)
    old_blob = (json.dumps(base, indent=2, sort_keys=True) + "\n").encode()
    old_blob_sha = hashlib.sha256(old_blob).hexdigest()
    output = root / base["runtime"]["output_dir"]
    output.mkdir(parents=True, exist_ok=True)
    scientific_path = root / base["research_source"]["result_path"]
    scientific = _nonmaterialized_scientific(
        base, source_manifest_file_sha256=old_blob_sha
    )
    scientific_path.write_text(json.dumps(scientific, indent=2, sort_keys=True) + "\n")
    lease_core = {
        "schema": runtime.REPLAY_LEASE_SCHEMA,
        "campaign_id": contract.CAMPAIGN_ID,
        "manifest_hash": base["manifest_hash"],
        "source_commit": base["source_commit"],
        "generation": 0,
        "maximum_generations": 1,
        "status": "COMPLETE",
        "authorization": contract.ROOT_AUTHORIZATION,
        "runner_pid": 123,
        **{field: 0 for field in runtime.SAFETY_COUNTER_FIELDS},
        "result_hash": scientific["result_hash"],
        "result_file_sha256": _sha(scientific_path),
    }
    lease = {**lease_core, "attempt_hash": stable_hash(lease_core)}
    lease_path = output / "scientific_replay_attempt.json"
    lease_path.write_text(json.dumps(lease, indent=2, sort_keys=True) + "\n")
    adoption = _adoption_manifest(
        base,
        scientific_file_sha256=_sha(scientific_path),
        scientific_result_hash=scientific["result_hash"],
        lease_file_sha256=_sha(lease_path),
        lease_attempt_hash=lease["attempt_hash"],
        source_manifest_file_sha256=old_blob_sha,
    )
    manifest_path.write_text(json.dumps(adoption, indent=2, sort_keys=True) + "\n")
    scientific_before = scientific_path.read_bytes()
    lease_before = lease_path.read_bytes()
    calls = 0
    published_states: list[str] = []
    original_publish = runtime._publish

    def forbidden(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise AssertionError("economic runner was called during completed-result adoption")

    def recording_publish(*args: object, **kwargs: Any) -> None:
        published_states.append(str(kwargs["state"]))
        original_publish(*args, **kwargs)

    monkeypatch.setattr(
        "hydra.production.manifest.load_and_validate_production_manifest",
        lambda _path: adoption,
    )
    monkeypatch.setattr(runtime, "validate_cross_ecology_analog_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_publish", recording_publish)
    monkeypatch.setattr(
        "hydra.research.cross_ecology_session_path_analog_router.run_economic_tripwire",
        forbidden,
    )
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "0")
    result = runtime.run_cross_ecology_analog_manifest(manifest_path)
    assert calls == 0
    assert "NON_ECONOMIC_AUDIT_ACTIVE" in published_states
    assert "EXACT_REPLAY_ACTIVE" not in published_states
    assert scientific_path.read_bytes() == scientific_before
    assert lease_path.read_bytes() == lease_before
    assert result["status"] == "COMPLETE"
    assert result["evidence_bundle"] is None
    assert result["evidence_tier_awarded"] is None
    assert result["economic_results"]["economic_evidence_materialized"] is False
    assert not (root / base["evidence_bundle"]["destination"] / f"{contract.CAMPAIGN_ID}.evidence-v1").exists()

    worm_commit = adoption["completed_scientific_result_adoption"][
        "source_manifest_worm_commit"
    ]

    def fake_git(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess:
        if args[1] == "rev-parse":
            return subprocess.CompletedProcess(args, 0, stdout=worm_commit + "\n", stderr="")
        return subprocess.CompletedProcess(args, 0, stdout=old_blob, stderr=b"")

    monkeypatch.setattr(generic.subprocess, "run", fake_git)
    generic.verify_non_economic_terminal_publication(result, adoption, root=root)
    tampered = json.loads(json.dumps(result))
    tampered["kpis"]["combine_episodes_completed"] = 1
    with pytest.raises(generic.ProductionRuntimeError, match="invented economic evidence"):
        generic.verify_non_economic_terminal_publication(tampered, adoption, root=root)

    escaped_receipt = json.loads(
        Path(result["non_economic_audit"]["path"]).read_text()
    )
    escaped_receipt["scientific_result_path"] = "/outside/not-authorized.json"
    escaped_receipt.pop("receipt_hash")
    escaped_receipt["receipt_hash"] = stable_hash(escaped_receipt)
    receipt_path = Path(result["non_economic_audit"]["path"])
    receipt_path.write_text(json.dumps(escaped_receipt, indent=2, sort_keys=True) + "\n")
    escaped_result = json.loads(json.dumps(result))
    escaped_result["non_economic_audit"]["receipt_hash"] = escaped_receipt[
        "receipt_hash"
    ]
    escaped_result["non_economic_audit"]["file_sha256"] = _sha(receipt_path)
    with pytest.raises(generic.ProductionRuntimeError, match="path drift"):
        generic.verify_non_economic_terminal_publication(
            escaped_result, adoption, root=root
        )


def test_status_validates_checkpoint_pair_and_adoption_labels_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, path = _copy_contract_project(tmp_path)
    base = _manifest(root)
    adoption = _adoption_manifest(
        base,
        scientific_file_sha256=HASH_A,
        scientific_result_hash=HASH_B,
        lease_file_sha256=HASH_C,
        lease_attempt_hash=HASH_A,
    )
    monkeypatch.setattr(
        "hydra.production.manifest.load_and_validate_production_manifest",
        lambda _path: adoption,
    )
    monkeypatch.setattr(runtime, "validate_cross_ecology_analog_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_verify_multiplicity_reservation", lambda *_a, **_k: None)
    monkeypatch.setenv("HYDRA_NETWORK_ACCESS_ALLOWED", "0")
    status = runtime.read_cross_ecology_analog_status(path)
    assert status["next_action"] == "ADOPT_COMPLETED_HASH_BOUND_RESULT_WITHOUT_REPLAY"
    monkeypatch.setenv("HYDRA_PRODUCTION_TEST_MODE", "1")
    started = runtime.run_cross_ecology_analog_manifest(path, stop_after="START")
    assert started["next_action"] == "ADOPT_COMPLETED_HASH_BOUND_RESULT_WITHOUT_REPLAY"

    # A monitor must never accept or silently overwrite just one half of a
    # checkpoint pair.
    output = root / adoption["runtime"]["output_dir"]
    (output / "production_state.json").unlink()
    with pytest.raises(runtime.CrossEcologyAnalogRuntimeError, match="pair is incomplete"):
        runtime.read_cross_ecology_analog_status(path)
