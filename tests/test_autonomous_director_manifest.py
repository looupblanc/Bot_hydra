from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.autonomous_director_manifest import (
    ACCOUNT_SIZES_USD,
    CAMPAIGN_ID,
    CAMPAIGN_MODE,
    CLASS_ID,
    EVIDENCE_TIERS,
    EXPLOITATION_BRANCH,
    EXPLORATION_BRANCH,
    MANIFEST_SCHEMA,
    RUNTIME_VERSION,
    AutonomousDirectorManifestError,
    validate_autonomous_director_manifest,
)
from hydra.production.manifest import (
    ProductionManifestError,
    load_and_validate_production_manifest,
)


def _write(path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rehash(manifest: dict[str, Any]) -> None:
    manifest.pop("manifest_hash", None)
    manifest["manifest_hash"] = stable_hash(manifest)


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    implementation = {
        relative: _write(tmp_path / relative, f"# fixture {relative}\n")
        for relative in (
            "hydra/production/autonomous_director_manifest.py",
            "hydra/production/autonomous_director_runtime.py",
            "hydra/production/autonomous_exact_replay.py",
            "hydra/production/manifest.py",
            "hydra/production/runtime.py",
            "scripts/run_economic_production_manifest.py",
        )
    }

    objective_sha = _write(tmp_path / "MISSION_OBJECTIVE.md", "economic mission\n")
    _write(tmp_path / "mission/state/CURRENT_STATE.json", "{}\n")
    _write(tmp_path / "mission/state/decision_ledger.jsonl", "")
    _write(tmp_path / "mission/state/ECONOMIC_SCORECARD.json", "{}\n")
    _write(tmp_path / "mission/state/AUTONOMOUS_BRANCH_STATE.json", "{}\n")
    _write(tmp_path / "reports/data_budget/databento_spend_ledger.jsonl", "")

    parsed_rule_fields = {
        "combine": {},
        "combine_common": {},
        "xfa": {},
        "product_restrictions": {},
    }
    parsed_rule_hash = stable_hash(parsed_rule_fields)
    rule_file = tmp_path / "config/rulesets/topstep_master_2026-07-19.json"
    rule_file_sha = _write(
        rule_file,
        json.dumps(
            {
                "schema": "topstep_official_rule_snapshot_v1",
                "account_sizes_usd": list(ACCOUNT_SIZES_USD),
                "parsed_rule_hash": parsed_rule_hash,
                **parsed_rule_fields,
            },
            sort_keys=True,
        )
        + "\n",
    )

    manifest: dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "campaign_mode": CAMPAIGN_MODE,
        "campaign_id": CAMPAIGN_ID,
        "class_id": CLASS_ID,
        "policy_classes": [CLASS_ID],
        "development_only": True,
        "created_at_utc": "2026-07-19T02:00:00Z",
        "source_commit": "b" * 40,
        "economic_hypothesis": (
            "A two-lane autonomous portfolio can maximise economic information "
            "gain while advancing the strongest honest Combine candidate."
        ),
        "implementation_files": implementation,
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "result_schema": "hydra_economic_production_result_v1",
            "result_name": "economic_production_result.json",
            "output_dir": "reports/economic_evolution/autonomous_director_0035",
            "autonomous_director_runtime_version": RUNTIME_VERSION,
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "orchestrator_count": 1,
            "worker_count": 2,
            "asynchronous_evidence_writer_count": 1,
        },
        "evidence_bundle": {
            "required": True,
            "atomic_single_writer_finalization": True,
            "destination": "data/cache/evidence_bundles",
            "required_datasets": [
                "component_signals",
                "component_entries",
                "component_exits",
                "component_trades",
                "account_policy_membership",
                "account_daily_paths",
                "episodes",
                "provenance",
            ],
            "exact_account_replay_required": True,
            "sentinel_economic_records_allowed": False,
        },
        "compute_contract": {
            "host_logical_cpu_count": 4,
            "economic_process_slot_count": 3,
            "reserved_logical_cpu_count": 1,
            "orchestrator_count": 1,
            "cpu_worker_count": 2,
            "cpu_worker_maximum": 2,
            "authoritative_writer_count": 1,
            "cpu_workers_read_only": True,
            "single_writer_atomic_commits": True,
            "oversubscription_allowed": False,
            "economic_wall_clock_minimum": 0.85,
            "target_cpu_utilization_min": 0.80,
            "target_cpu_utilization_max": 0.95,
            "worker_roles": {
                "worker_a": "EXPLOITATION",
                "worker_b": "EXPLORATION",
            },
            "thread_limits": {
                "OMP_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "NUMEXPR_NUM_THREADS": "1",
            },
        },
        "governance": {
            "live_trading_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "q4_access_allowed": False,
            "protected_holdout_access_allowed": False,
            "new_mission_allowed": False,
            "new_service_allowed": False,
            "new_database_allowed": False,
            "new_registry_writer_allowed": False,
            "controller_version_change_required": False,
            "status_inheritance_allowed": False,
            "falsified_result_resurrection_allowed": False,
            "causality_weakening_allowed": False,
            "accounting_weakening_allowed": False,
            "silent_purchase_above_authority_allowed": False,
            "data_purchase_policy": "AUTHORITATIVE_LEDGER_BOUNDED_ONLY",
            "q4_access_count_delta": 0,
            "broker_connection_count": 0,
            "order_count": 0,
        },
        "official_rule_snapshot": {
            "path": rule_file.relative_to(tmp_path).as_posix(),
            "file_sha256": rule_file_sha,
            "parsed_rule_hash": parsed_rule_hash,
            "provenance": "OFFICIAL_TOPSTEP_SOURCES",
            "stale": False,
            "retrieved_at_utc": "2026-07-19T01:04:10Z",
            "account_sizes_usd": list(ACCOUNT_SIZES_USD),
            "official_source_urls": [
                "https://help.topstep.com/en/articles/8284197-trading-combine-parameters",
                "https://help.topstep.com/en/articles/8284233-topstep-payout-policy",
            ],
        },
        "mission_state_contract": {
            "files": {
                "objective": "MISSION_OBJECTIVE.md",
                "current_state": "mission/state/CURRENT_STATE.json",
                "decision_ledger": "mission/state/decision_ledger.jsonl",
                "economic_scorecard": "mission/state/ECONOMIC_SCORECARD.json",
                "branch_state": "mission/state/AUTONOMOUS_BRANCH_STATE.json",
            },
            "objective_sha256": objective_sha,
            "decision_ledger_append_only": True,
            "create_new_reporting_framework": False,
        },
        "branch_portfolio": {
            "immutable_initial_allocation": True,
            "one_lane_block_must_not_idle_other": True,
            "lanes": [
                {
                    "lane_id": "EXPLOITATION",
                    "initial_branch_id": EXPLOITATION_BRANCH,
                    "source_campaign": "0034",
                    "internal_robustness_decision_maximum": 1,
                    "fresh_confirmation_attempt_maximum": 1,
                    "threshold_refinement_allowed": False,
                    "permanent_loyalty": False,
                },
                {
                    "lane_id": "EXPLORATION",
                    "initial_branch_id": EXPLORATION_BRANCH,
                    "source_population": "CLEAN_CAUSAL_0029_LEDGER_BANK",
                    "materially_distinct_from_exploitation": True,
                    "account_sizes_usd": list(ACCOUNT_SIZES_USD),
                    "diagnostics": [
                        "UNIFORM_LEGAL_SCALE_FRONTIER",
                        "CAUSAL_QUALITY_TIER_FRONTIER",
                        "NON_DEPLOYABLE_LEGAL_UPPER_BOUND",
                    ],
                    "non_deployable_upper_bound_promotable": False,
                },
            ],
        },
        "economic_epoch_policy": {
            "minimum_minutes": 45,
            "maximum_minutes": 120,
            "branch_materially_distinct_attempt_maximum": 2,
            "economic_worker_idle_timeout_minutes": 10,
            "unchanged_epoch_repeat_allowed": False,
            "gate_is_user_handoff": False,
            "continue_after_gate": True,
            "required_frozen_fields": [
                "hypothesis",
                "compute_budget",
                "data_budget",
                "promotion_gate",
                "falsification_gate",
                "next_branch_rule",
            ],
        },
        "research_board": {
            "decision_card_fields": [
                "hypothesis",
                "strongest_argument_against",
                "smallest_decisive_falsification_experiment",
                "expected_runtime_minutes",
                "expected_data_cost_usd",
                "expected_information_gain",
                "expected_economic_upside",
                "next_materially_distinct_alternative",
            ],
            "persist_private_reasoning": False,
            "materially_distinct_alternative_required": True,
        },
        "evidence_tiers": {
            "ordered_tiers": list(EVIDENCE_TIERS),
            "status_inheritance_allowed": False,
            "collapse_to_validated_allowed": False,
            "independent_confirmation_required_for_tier_c": True,
            "f0_required_for_tier_f": True,
        },
        "economic_objective": {
            "headline_horizons_trading_days": [5, 10, 20],
            "account_sizes_usd": list(ACCOUNT_SIZES_USD),
            "required_metrics": [
                "P_PASS_5D_NORMAL",
                "P_PASS_5D_STRESSED",
                "P_PASS_10D_NORMAL",
                "P_PASS_10D_STRESSED",
                "P_PASS_20D_NORMAL",
                "P_PASS_20D_STRESSED",
                "P_PASS_BEFORE_BREACH",
                "EXPECTED_TRADING_DAYS_TO_PASS",
                "EXPECTED_COMBINE_COST_TO_XFA",
                "MLL_BREACH_RATE",
                "MINIMUM_MLL_BUFFER",
                "CONSISTENCY_COMPLIANCE",
                "STRESSED_NET",
                "LOWER_QUARTILE_TARGET_PROGRESS",
                "MEDIAN_TARGET_PROGRESS",
                "OPPORTUNITY_DENSITY",
                "DEPLOYABILITY",
            ],
            "pareto_frontier": True,
            "largest_account_default": False,
            "exact_mll_required": True,
            "exact_consistency_required": True,
            "causal_executable_fills_required": True,
            "combine_and_funded_products_separate": True,
            "xfa_before_credible_combine_allowed": False,
        },
        "data_policy": {
            "existing_data_first": True,
            "q4_access_allowed": False,
            "protected_holdout_access_allowed": False,
            "official_cost_estimate_before_purchase": True,
            "freeze_roles_before_purchase": True,
            "silent_purchase_allowed": False,
            "budget_ledger_path": (
                "reports/data_budget/databento_spend_ledger.jsonl"
            ),
        },
        "multiplicity": {
            "campaign_run_limit": 1,
            "controller_reservation_required": True,
            "single_existing_controller": True,
            "single_authoritative_writer": True,
        },
    }
    _rehash(manifest)
    manifest_path = tmp_path / "config/v7/autonomous_director_0035.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return manifest_path, manifest


def _mutate(
    manifest_path: Path,
    manifest: dict[str, Any],
    path: tuple[str | int, ...],
    value: Any,
) -> None:
    target: Any = manifest
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    _rehash(manifest)
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def test_autonomous_director_manifest_and_dispatch_accept_frozen_contract(
    tmp_path: Path,
) -> None:
    path, manifest = _fixture(tmp_path)

    validate_autonomous_director_manifest(manifest, manifest_path=path)
    assert load_and_validate_production_manifest(path) == manifest


@pytest.mark.parametrize(
    ("field_path", "bad_value", "message"),
    [
        (("runtime", "worker_count"), 3, "runtime"),
        (("evidence_bundle", "destination"), "reports/evidence", "EvidenceBundle"),
        (("compute_contract", "thread_limits", "OMP_NUM_THREADS"), "2", "thread"),
        (("governance", "orders_allowed"), True, "unsafe"),
        (
            ("branch_portfolio", "lanes", 0, "fresh_confirmation_attempt_maximum"),
            2,
            "0034",
        ),
        (
            ("branch_portfolio", "lanes", 1, "materially_distinct_from_exploitation"),
            False,
            "legal-feasibility",
        ),
        (("economic_epoch_policy", "minimum_minutes"), 44, "epoch"),
        (("economic_epoch_policy", "branch_materially_distinct_attempt_maximum"), 3, "epoch"),
        (("evidence_tiers", "ordered_tiers"), ["H", "E", "C"], "tier"),
    ],
)
def test_autonomous_director_manifest_fails_closed_on_contract_drift(
    tmp_path: Path,
    field_path: tuple[str | int, ...],
    bad_value: Any,
    message: str,
) -> None:
    path, manifest = _fixture(tmp_path)
    _mutate(path, manifest, field_path, bad_value)

    with pytest.raises(AutonomousDirectorManifestError, match=message):
        validate_autonomous_director_manifest(manifest, manifest_path=path)


def test_autonomous_director_manifest_rejects_rule_snapshot_tamper(
    tmp_path: Path,
) -> None:
    path, manifest = _fixture(tmp_path)
    rule_path = tmp_path / manifest["official_rule_snapshot"]["path"]
    rule_path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(AutonomousDirectorManifestError, match="rule-snapshot"):
        validate_autonomous_director_manifest(manifest, manifest_path=path)


def test_generic_loader_rejects_semantic_hash_drift(tmp_path: Path) -> None:
    path, manifest = _fixture(tmp_path)
    manifest["branch_portfolio"]["lanes"][0]["permanent_loyalty"] = True
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ProductionManifestError, match="manifest hash drift"):
        load_and_validate_production_manifest(path)
