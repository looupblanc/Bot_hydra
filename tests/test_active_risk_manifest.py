from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from hydra.evidence import REQUIRED_DATASETS
from hydra.production.active_risk_manifest import (
    ACTIVE_RISK_CAMPAIGN_MODE,
    ACTIVE_RISK_CLASS_ID,
    ACTIVE_RISK_DECISION_FIELDS,
    ACTIVE_RISK_HORIZONS,
    ACTIVE_RISK_IDENTITY_INVARIANTS,
    ACTIVE_RISK_MATCHED_CONTROLS,
    ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE,
    ACTIVE_RISK_RANDOM_EXPOSURE_SIGNATURE_FIELDS,
    ACTIVE_RISK_RANDOM_PRIORITY_SEEDS,
    ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES,
    ACTIVE_RISK_RISK_FRONTIER,
    ACTIVE_RISK_RUNTIME_VERSION,
    ActiveRiskManifestError,
    validate_active_risk_manifest,
)
from hydra.production.portfolio_books import SleeveRecord
from hydra.propfirm.combine_to_xfa import UNREALIZED_AGGREGATION_SEMANTICS


def _sha(value: str | Path) -> str:
    if isinstance(value, Path):
        return hashlib.sha256(value.read_bytes()).hexdigest()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _valid_manifest(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    implementation_files: dict[str, str] = {}
    for relative in sorted(ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"# active-risk fixture: {relative}\n", encoding="utf-8")
        implementation_files[relative] = _sha(target)

    members = []
    for index in range(18):
        record = SleeveRecord(
            sleeve_id=f"sleeve-{index:02d}",
            immutable_fingerprint=_sha(f"immutable:{index}"),
            behavioral_fingerprint=_sha(f"behavior:{index}"),
            signal_ledger_sha256=_sha(f"signals:{index}"),
            trade_ledger_sha256=_sha(f"trades:{index}"),
            market=("CL", "ES", "GC", "NQ")[index % 4],
            contract=("MCL", "MES", "MGC", "MNQ")[index % 4],
            timeframe=("5m", "15m", "30m")[index % 3],
            session=("OPEN", "MID", "CLOSE")[index % 3],
            economic_role="TARGET_VELOCITY",
            source_campaign="hydra_portfolio_first_combine_to_payout_0025",
            family_id=f"family-{index:02d}",
        )
        members.append(
            {
                "sleeve_id": record.sleeve_id,
                "immutable_fingerprint": record.immutable_fingerprint,
                "behavioral_fingerprint": record.behavioral_fingerprint,
                "signal_ledger_sha256": record.signal_ledger_sha256,
                "trade_ledger_sha256": record.trade_ledger_sha256,
                "market": record.market,
                "contract": record.contract,
                "timeframe": record.timeframe,
                "session": record.session,
                "complete_trade_ledger": True,
                "signal_mutation_allowed": False,
                "entry_exit_recalculation_allowed": False,
                "preserved_after_static_family_failure": True,
                "record": record.to_dict(),
            }
        )

    path = tmp_path / "config/v7/active_risk_fixture.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n", encoding="utf-8")
    manifest: dict[str, Any] = {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "campaign_mode": ACTIVE_RISK_CAMPAIGN_MODE,
        "class_id": ACTIVE_RISK_CLASS_ID,
        "policy_classes": [ACTIVE_RISK_CLASS_ID],
        "created_at_utc": "2026-07-15T04:00:00Z",
        "source_commit": "a" * 40,
        "development_only": True,
        "economic_hypothesis": (
            "Causal shared risk admission improves target velocity without "
            "changing immutable sleeves."
        ),
        "source_static_book_classification": "STATIC_CAPITAL_PARTITION_TOO_SLOW",
        "source_static_book_campaign_id": (
            "hydra_portfolio_first_combine_to_payout_0025"
        ),
        "source_sleeves_killed": False,
        "implementation_files": implementation_files,
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "active_risk_runtime_version": ACTIVE_RISK_RUNTIME_VERSION,
            "output_dir": "reports/economic_evolution/active_risk_fixture_0026",
            "result_name": "economic_production_result.json",
            "result_schema": "hydra_economic_production_result_v1",
            "controller_source_change_required": False,
            "resume_from_checkpoint": True,
            "worker_count": 3,
            "asynchronous_evidence_writer_count": 1,
        },
        "governance": {
            "q4_access_allowed": False,
            "new_data_purchase_allowed": False,
            "broker_connection_allowed": False,
            "orders_allowed": False,
            "proof_window_consumption_allowed": False,
            "status_inheritance_allowed": False,
            "source_signal_mutation_allowed": False,
            "source_entry_exit_recalculation_allowed": False,
            "new_market_grammar_allowed": False,
            "new_sleeve_discovery_campaign_allowed": False,
        },
        "compute_allocation": {
            "active_pool_target_velocity_fraction": 0.70,
            "targeted_high_velocity_replenishment_fraction": 0.20,
            "xfa_lifecycle_fraction": 0.05,
            "safety_controls_reporting_fraction": 0.05,
        },
        "sleeve_bank": {
            "members": members,
            "member_count": 18,
            "underlying_sleeves_immutable": True,
            "inactive_sleeves_reserve_risk": False,
        },
        "component_bank": {"sources": {}},
        "data": {
            "role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
            "feature_source_fingerprint": "b" * 64,
            "contract_map_sha256": "c" * 64,
            "cached_features_only": True,
            "feature_recalculation_allowed": False,
            "q4_access_allowed": False,
            "new_purchase_allowed": False,
        },
        "multiplicity": {
            "prior_global_N_trials": 100,
            "prospective_comparisons": 20_000,
            "campaign_specific_inflation": 1.5,
            "reserved_delta_trials": 30_000,
            "expected_global_N_trials_after_reservation": 30_100,
            "reservation_required_before_outcome_access": True,
            "proof_window_consumed": False,
        },
        "budget": {
            "actual_spend_usd": 87.85,
            "hard_cap_usd": 125.0,
            "remaining_usd": 37.15,
            "new_data_purchase_count": 0,
        },
        "episode_starts": {
            "serious_policy_starts": 48,
            "block_aware": True,
            "overlapping_starts_independent": False,
            "retuning_after_start_outcomes": False,
        },
        "costs": {
            "normal_multiplier": 1.0,
            "stressed_multiplier": 1.5,
            "source_component_costs_frozen": True,
            "retune_after_outcomes": False,
        },
        "account_parameters": {
            "starting_balance": 150_000.0,
            "profit_target": 9_000.0,
            "maximum_loss_limit": 4_500.0,
            "maximum_mini_equivalent": 15,
            "dynamic_loss_streak_ratchet": False,
            "unrealized_aggregation_semantics": UNREALIZED_AGGREGATION_SEMANTICS,
            "timestamp_exact_combined_unrealized_claimed": False,
        },
        "compute": {
            "worker_count": 3,
            "asynchronous_evidence_writer_count": 1,
            "compute_workers_read_only": True,
            "process_start_method": "spawn",
            "batched_evidence_commits": True,
            "immutable_episode_cache": True,
            "full_repository_regression_per_wave": False,
        },
        "markets": ["CL", "ES", "GC", "NQ"],
        "contracts": {
            market: {"mini": market, "micro": f"M{market}", "micro_per_mini": 10}
            for market in ("CL", "ES", "GC", "NQ")
        },
        "timeframes": ["5m", "15m", "30m"],
        "session_rules": {
            "source": "FROZEN_COMPONENT_SESSION_CODE",
            "same_session_enforcement": True,
            "overnight_fabrication_allowed": False,
        },
        "identity_audit": {
            "required_invariants": list(ACTIVE_RISK_IDENTITY_INVARIANTS),
            "required_before_economic_outcomes": True,
            "repair_shared_engine_before_evaluation_on_failure": True,
            "single_deterministic_audit": True,
            "future_outcomes_used_for_routing": False,
            "actual_stop_risk_available": False,
            "routing_risk_measure": "DECLARED_NOMINAL_RISK_UTILISATION",
            "ex_post_mae_routing_allowed": False,
            "conflict_decision_fields": list(ACTIVE_RISK_DECISION_FIELDS),
            "foregone_pnl_persisted": True,
        },
        "governor_generator": {
            "proposal_count": 20_000,
            "unique_vectorized_screen_minimum": 4_096,
            "exact_replay_maximum": 1_024,
            "risk_frontier": list(ACTIVE_RISK_RISK_FRONTIER),
            "inactive_sleeves_reserve_risk": False,
            "sole_active_sleeve_preserves_nominal_risk": True,
            "concurrent_sleeves_share_current_available_risk": True,
            "bounded_discrete_policy_set": True,
            "structural_deduplication": True,
            "behavioral_deduplication": True,
            "continuous_optimization_allowed": False,
            "global_contract_multiplier_allowed": False,
            "loss_streak_ratchet_allowed": False,
            "underlying_signal_changes_allowed": False,
            "bounded_dimensions": [
                "MAXIMUM_CONCURRENT_SLEEVES",
                "AGGREGATE_OPEN_RISK_CEILING",
                "PER_SLEEVE_NOMINAL_RISK_PRESERVATION",
                "PROPORTIONAL_SCALING_DURING_CONCURRENCY",
                "DETERMINISTIC_SLEEVE_PRIORITY",
                "SAME_INSTRUMENT_CONFLICT_RULE",
                "DAILY_CONSISTENCY_GUARD",
                "TARGET_PROTECTION_MODE",
                "STATIC_RISK_TIER",
            ],
        },
        "matched_controls": {
            "controls": list(ACTIVE_RISK_MATCHED_CONTROLS),
            "identical_sleeve_ledgers": True,
            "identical_episode_starts": True,
            "identical_temporal_blocks": True,
            "identical_costs": True,
            "identical_horizons": True,
            "identical_topstep_configuration": True,
            "random_priority_exposure_matched": True,
            "random_priority_seeds": list(ACTIVE_RISK_RANDOM_PRIORITY_SEEDS),
            "random_priority_exposure_relative_tolerance": (
                ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE
            ),
            "random_priority_exposure_signature_fields": list(
                ACTIVE_RISK_RANDOM_EXPOSURE_SIGNATURE_FIELDS
            ),
            "random_priority_match_selection_uses_economic_outcomes": False,
            "unmatched_random_control_blocks_promotion": True,
            "executed_for_every_serious_policy": True,
        },
        "successive_halving": {
            "stage1_proposals": 20_000,
            "stage1_unique_screen_minimum": 4_096,
            "stage2_exact_replay_maximum": 1_024,
            "stage3_48_start_maximum": 256,
            "stage3_survivor_maximum": 32,
            "stage4_96_start_maximum": 32,
            "stage4_survivor_maximum": 8,
            "stage5_192_start_maximum": 8,
            "frozen_horizons": list(ACTIVE_RISK_HORIZONS),
            "stress_cost_multiplier": 1.5,
            "retuning_after_outcomes": False,
            "automatic_xfa_on_combine_pass": True,
            "xfa_profile_projection": {
                "profile_version": "hydra_combine_to_xfa_v1",
                "policy": "STATIC_PROJECTION_OF_ACTIVE_GOVERNOR_V1",
                "risk_multiplier_source": "STATIC_RISK_TIER",
                "maximum_simultaneous_positions_source": (
                    "MAXIMUM_CONCURRENT_SLEEVES"
                ),
                "maximum_mini_equivalent_source": (
                    "GOVERNOR_MAXIMUM_MINI_EQUIVALENT"
                ),
                "clip_to_official_scaling_plan": True,
                "same_market_exclusive": True,
                "active_pool_combine_only_controls_applied": False,
                "selected_after_combine_outcome": False,
            },
        },
        "temporal_blocks": {
            "overlapping_starts_independent": False,
            "blocks": [{"block_id": f"B{index}"} for index in range(1, 5)],
        },
        "evidence_bundle": {
            "destination": "data/cache/evidence_bundles",
            "lightweight_manifest_path": (
                "reports/economic_evolution/active_risk_fixture_0026/"
                "evidence_bundle_receipt.json"
            ),
            "required_for_campaign_complete": True,
            "atomic_finalize": True,
            "summary_only_complete_allowed": False,
            "large_files_git_tracked": False,
            "reconstruction_flag": False,
            "required_datasets": list(REQUIRED_DATASETS),
        },
    }
    return path, manifest


def test_active_risk_manifest_freezes_campaign_identity_and_closure() -> None:
    assert ACTIVE_RISK_CAMPAIGN_MODE == "ACTIVE_RISK_POOL"
    assert ACTIVE_RISK_CLASS_ID == "ACTIVE_RISK_POOL_TARGET_VELOCITY_V1"
    assert "hydra/account_policy/active_risk_pool.py" in (
        ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES
    )
    assert "hydra/account_policy/active_pool_replay.py" in (
        ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES
    )
    assert "hydra/production/active_risk_runtime.py" in (
        ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES
    )
    assert ACTIVE_RISK_HORIZONS == (20, 40, 60, 90, "FULL")


def test_active_risk_manifest_accepts_frozen_contract(tmp_path: Path) -> None:
    path, manifest = _valid_manifest(tmp_path)

    validate_active_risk_manifest(manifest, manifest_path=path)


def test_active_risk_manifest_detects_active_pool_replay_checksum_drift(
    tmp_path: Path,
) -> None:
    path, manifest = _valid_manifest(tmp_path)
    replay = tmp_path / "hydra/account_policy/active_pool_replay.py"
    replay.write_text("# mutated replay semantics\n", encoding="utf-8")

    with pytest.raises(
        ActiveRiskManifestError,
        match=(
            "active-risk implementation checksum drift: "
            "hydra/account_policy/active_pool_replay.py"
        ),
    ):
        validate_active_risk_manifest(manifest, manifest_path=path)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("sleeve_count", "exactly 18"),
        ("implementation_closure", "implementation closure drift"),
        ("risk_frontier", "governor generator drift"),
        ("unsafe_q4", "unsafe active-risk authority"),
        ("actual_stop_claim", "identity/audit contract drift"),
        ("control_removed", "matched-control contract drift"),
        ("horizon_removed", "horizon/lifecycle policy drift"),
        ("xfa_profile_drift", "frozen XFA profile drift"),
    ],
)
def test_active_risk_manifest_fails_closed_on_economic_contract_drift(
    tmp_path: Path, mutation: str, match: str
) -> None:
    path, manifest = _valid_manifest(tmp_path)
    if mutation == "sleeve_count":
        manifest["sleeve_bank"]["members"].pop()
        manifest["sleeve_bank"]["member_count"] = 17
    elif mutation == "implementation_closure":
        manifest["implementation_files"].pop("hydra/account_policy/active_risk_pool.py")
    elif mutation == "risk_frontier":
        manifest["governor_generator"]["risk_frontier"] = [0.75, 1.0, 1.3]
    elif mutation == "unsafe_q4":
        manifest["governance"]["q4_access_allowed"] = True
    elif mutation == "actual_stop_claim":
        manifest["identity_audit"]["actual_stop_risk_available"] = True
    elif mutation == "control_removed":
        manifest["matched_controls"]["controls"].pop()
    elif mutation == "horizon_removed":
        manifest["successive_halving"]["frozen_horizons"].pop()
    elif mutation == "xfa_profile_drift":
        manifest["successive_halving"]["xfa_profile_projection"][
            "selected_after_combine_outcome"
        ] = True
    else:  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(mutation)

    with pytest.raises(ActiveRiskManifestError, match=match):
        validate_active_risk_manifest(manifest, manifest_path=path)


def test_active_risk_manifest_rejects_signal_mutation(tmp_path: Path) -> None:
    path, manifest = _valid_manifest(tmp_path)
    manifest["sleeve_bank"]["members"][0]["signal_mutation_allowed"] = True

    with pytest.raises(ActiveRiskManifestError, match="immutability"):
        validate_active_risk_manifest(manifest, manifest_path=path)
