from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any

import pytest

from hydra.evidence import REQUIRED_DATASETS
from hydra.production.portfolio_books import (
    PortfolioBookGeneratorSpec,
    SleeveRecord,
    stable_hash,
)
from hydra.production.portfolio_manifest import (
    PORTFOLIO_CAMPAIGN_MODE,
    PORTFOLIO_CLASS_ID,
    PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES,
    PORTFOLIO_RUNTIME_VERSION,
    PortfolioManifestError,
    SLEEVE_ROLES,
    validate_portfolio_manifest,
)
from hydra.promotion.portfolio_status import (
    FROZEN_PORTFOLIO_PROMOTION_POLICY,
    PortfolioStatus,
)
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15


def _sha(value: str | Path) -> str:
    if isinstance(value, Path):
        return hashlib.sha256(value.read_bytes()).hexdigest()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _rehash_portfolio_books(manifest: dict[str, Any]) -> None:
    books = manifest["portfolio_books"]
    books.pop("manifest_hash", None)
    books["manifest_hash"] = stable_hash(books)


def _set_path(value: dict[str, Any], path: tuple[str, ...], replacement: Any) -> None:
    target = value
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = replacement


def _valid_manifest(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    implementation_files: dict[str, str] = {}
    for relative in sorted(PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES):
        implementation = tmp_path / relative
        implementation.parent.mkdir(parents=True, exist_ok=True)
        implementation.write_text(f"# fixture: {relative}\n", encoding="utf-8")
        implementation_files[relative] = _sha(implementation)
    source = tmp_path / "reports/portfolio_source.json"
    source.parent.mkdir(parents=True)
    source.write_text('{"sleeves": 12}\n', encoding="utf-8")
    terminal_receipt = tmp_path / "reports/portfolio_terminal_receipt.json"
    terminal_receipt.write_text('{"status":"TERMINAL"}\n', encoding="utf-8")
    seed_archive = tmp_path / "reports/portfolio_seed_archive.json"
    seed_archive.write_text('{"status":"FROZEN"}\n', encoding="utf-8")

    members = []
    for index in range(12):
        role = (
            "TARGET_VELOCITY",
            "SESSION_DIVERSIFIER",
            "MLL_PROTECTOR",
            "PAYOUT_SURVIVAL",
        )[index % 4]
        record = SleeveRecord(
            sleeve_id=f"sleeve-{index:02d}",
            immutable_fingerprint=_sha(f"immutable:{index}"),
            behavioral_fingerprint=_sha(f"behavior:{index}"),
            signal_ledger_sha256=_sha(f"signals:{index}"),
            trade_ledger_sha256=_sha(f"trades:{index}"),
            market=("ES", "NQ", "CL", "GC")[index % 4],
            contract=("MES", "MNQ", "MCL", "MGC")[index % 4],
            timeframe=("5m", "15m", "30m")[index % 3],
            session=("OPEN", "MID", "CLOSE")[index % 3],
            economic_role=role,
            source_campaign=f"campaign-{18 + index % 5:04d}",
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
                "source_campaign": record.source_campaign,
                "family_id": record.family_id,
                "behavioral_cluster": f"cluster-{index:02d}",
                "status": "SLEEVE_ECONOMICALLY_ELIGIBLE",
                "role": role,
                "family_status_inherited": False,
                "complete_trade_ledger": True,
                "event_count": 100 + index,
                "normal_net_pnl": 1_000.0 + index,
                "stressed_net_pnl": 900.0 + index,
                "record": record.to_dict(),
            }
        )

    portfolio_books = PortfolioBookGeneratorSpec(
        seed=25_000_501,
        conflict_policies=("PRIORITY",),
    ).to_manifest()
    portfolio_books.pop("manifest_hash")
    portfolio_books["manifest_hash"] = stable_hash(portfolio_books)

    snapshot = official_rule_snapshot_2026_07_15()
    manifest: dict[str, Any] = {
        "schema": "hydra_economic_production_manifest_v1",
        "campaign_id": "hydra_portfolio_fixture_0025",
        "campaign_mode": PORTFOLIO_CAMPAIGN_MODE,
        "class_id": PORTFOLIO_CLASS_ID,
        "policy_classes": [PORTFOLIO_CLASS_ID],
        "created_at_utc": "2026-07-15T00:00:00Z",
        "source_commit": "a" * 40,
        "development_only": True,
        "economic_hypothesis": "Complementary immutable sleeves improve lifecycle paths.",
        "implementation_files": implementation_files,
        "runtime": {
            "engine": "production_kernel_v1",
            "runner": "scripts/run_economic_production_manifest.py",
            "portfolio_runtime_version": PORTFOLIO_RUNTIME_VERSION,
            "output_dir": "reports/economic_evolution/portfolio_fixture_0025",
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
        },
        "compute_allocation": {
            "portfolio_lifecycle_fraction": 0.80,
            "sleeve_replenishment_fraction": 0.10,
            "forward_shadow_fraction": 0.05,
            "safety_controls_reporting_fraction": 0.05,
        },
        "sleeve_bank": {
            "members": members,
            "member_count": len(members),
            "behavioral_cluster_count": len(members),
            "source_evidence_campaign_id": "hydra_economic_production_0024",
            "source_bundle_status": "STAGING_PRESERVED_NONCONFIRMATORY",
            "source_bundle_complete": False,
            "component_ledgers_complete": True,
            "runtime_requires_deterministic_recompile_and_hash_reconciliation": True,
            "new_campaign_rematerializes_and_seals_own_ledgers": True,
            "source_runtime_summary": {
                "path": "reports/portfolio_source.json",
                "file_sha256": _sha(source),
            },
            "selector_terminal_receipt": {
                "path": "reports/portfolio_terminal_receipt.json",
                "file_sha256": _sha(terminal_receipt),
            },
            "source_seed_archive": {
                "path": "reports/portfolio_seed_archive.json",
                "file_sha256": _sha(seed_archive),
            },
            "source_ledger_reconciliation": {
                "campaign_id": "hydra_economic_production_0024",
                "component_signal_rows_checked": 1200,
                "component_trade_rows_checked": 1200,
                "all_member_specification_hashes_match": True,
                "all_member_signal_hashes_recomputed": True,
                "all_member_trade_hashes_recomputed": True,
                "outcomes_used_to_mutate_sleeves": False,
            },
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
            "unrealized_aggregation_semantics": (
                "CONSERVATIVE_SUM_OF_OPEN_TRADE_EXTREMA_BOUND_V1"
            ),
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
        "book_generator": {
            "pair_count": 20_000,
            "combine_sleeve_minimum": 2,
            "combine_sleeve_maximum": 6,
            "xfa_sleeve_minimum": 1,
            "xfa_sleeve_maximum": 6,
            "risk_frontier": [0.75, 1.0, 1.15, 1.3],
            "conflict_policies": ["PRIORITY"],
            "seed": 25_000_501,
            "structural_deduplication": True,
            "behavioral_deduplication": True,
            "stage1_full_sleeve_coverage_required": True,
            "scientific_null_diagnostic_fallback_allowed": True,
            "behavioral_novelty_minimum": 0.20,
            "pre_replay_behavioral_basis": (
                "SEMANTIC_SLEEVE_COMPOSITION_PREDICTION_ONLY"
            ),
            "stage1_actual_account_behavior_dedup_required": True,
            "cross_campaign_account_path_novelty_claimed": False,
            "maximum_attempt_multiplier": 100,
            "excluded_structural_fingerprints": [],
            "reference_book_behavioral_fingerprints": [],
        },
        "portfolio_books": portfolio_books,
        "matched_controls": {
            "status": "NOT_APPLICABLE_PORTFOLIO_MASS_PRODUCTION",
            "reason": (
                "PRIMARY_PRODUCT_IS_ACCOUNT_BOOK_POPULATION_NOT_FAMILY_EFFECT_TEST"
            ),
            "controls_claimed_executed": False,
            "raw_pnl_is_primary_rank": False,
        },
        "successive_halving": {
            "stage1_pairs": 20_000,
            "stage1_survivor_maximum": 2_000,
            "stage2_exact_maximum": 2_000,
            "stage2_survivor_maximum": 256,
            "stage3_48_start_maximum": 256,
            "stage3_survivor_maximum": 32,
            "stage4_96_start_maximum": 32,
            "stage4_survivor_maximum": 8,
            "stage5_192_start_maximum": 8,
            "stage5_primary_minimum": 3,
            "stage5_primary_maximum": 5,
            "stage5_distinct_backup_minimum": 1,
            "stress_cost_multiplier": 1.5,
        },
        "lifecycle": {
            "rule_snapshot_version": snapshot.rule_version,
            "rule_snapshot_fingerprint": snapshot.fingerprint,
            "combine_profit_transferred_to_xfa": False,
            "standard_and_consistency_both_evaluated": True,
            "books_frozen_before_outcomes": True,
            "combine_calendar_scope": "FROZEN_START_TEMPORAL_BLOCK_ONLY",
            "xfa_calendar_scope": (
                "FULL_REMAINING_CACHED_CHRONOLOGY_AFTER_COMBINE_PASS"
            ),
            "cross_block_xfa_paths_claimed_independent": False,
            "successful_combine_without_remaining_xfa_day": "XFA_DATA_CENSORED",
        },
        "promotion_policy": {
            "policy": FROZEN_PORTFOLIO_PROMOTION_POLICY.to_dict(),
            "policy_fingerprint": FROZEN_PORTFOLIO_PROMOTION_POLICY.fingerprint,
            "family_failure_erases_candidate_evidence": False,
            "paper_shadow_ready_from_development_allowed": False,
            "no_order_forward_observation_allowed": True,
            "status_ladder": [value.value for value in PortfolioStatus],
        },
        "temporal_blocks": {
            "overlapping_starts_independent": False,
            "blocks": [
                {"block_id": f"B{index}"} for index in range(1, 5)
            ],
        },
        "evidence_bundle": {
            "destination": "data/cache/evidence_bundles",
            "lightweight_manifest_path": "reports/economic_evolution/portfolio_fixture_0025/evidence_bundle_receipt.json",
            "required_for_campaign_complete": True,
            "atomic_finalize": True,
            "summary_only_complete_allowed": False,
            "large_files_git_tracked": False,
            "reconstruction_flag": False,
            "required_datasets": list(REQUIRED_DATASETS),
        },
    }
    path = tmp_path / "config/v7/portfolio_fixture.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n", encoding="utf-8")
    return path, manifest


def test_portfolio_manifest_constants_freeze_product_semantics() -> None:
    assert PORTFOLIO_CAMPAIGN_MODE == "PORTFOLIO_FIRST"
    assert PORTFOLIO_CLASS_ID == "PORTFOLIO_FIRST_COMBINE_TO_PAYOUT_V1"
    assert PORTFOLIO_RUNTIME_VERSION.endswith("_v1")
    assert "TARGET_VELOCITY" in SLEEVE_ROLES
    assert "PAYOUT_SURVIVAL" in SLEEVE_ROLES


def test_portfolio_manifest_accepts_exactly_reconciled_representations(
    tmp_path: Path,
) -> None:
    path, manifest = _valid_manifest(tmp_path)

    validate_portfolio_manifest(manifest, manifest_path=path)


def test_portfolio_manifest_rejects_unexecuted_control_claims(tmp_path: Path) -> None:
    path, manifest = _valid_manifest(tmp_path)
    manifest["matched_controls"]["always_on_sleeves"] = True

    with pytest.raises(PortfolioManifestError, match="unexecuted matched controls"):
        validate_portfolio_manifest(manifest, manifest_path=path)


@pytest.mark.parametrize(
    "missing",
    (
        "campaign_id",
        "source_commit",
        "data",
        "component_bank",
        "multiplicity",
        "episode_starts",
        "account_parameters",
    ),
)
def test_portfolio_manifest_rejects_missing_shared_production_envelope(
    tmp_path: Path, missing: str
) -> None:
    path, manifest = _valid_manifest(tmp_path)
    manifest.pop(missing)

    with pytest.raises(PortfolioManifestError):
        validate_portfolio_manifest(manifest, manifest_path=path)


@pytest.mark.parametrize(
    ("path", "replacement", "label"),
    [
        (("unique_pair_target",), 20_001, "pair_count"),
        (("combine_book", "sleeve_maximum"), 5, "combine_sleeve_maximum"),
        (("xfa_book", "sleeve_minimum"), 2, "xfa_sleeve_minimum"),
        (("risk_frontier",), [0.75, 1.0, 1.15], "risk_frontier"),
        (
            ("conflict_policies",),
            ["PRIORITY", "NET_TO_FLAT"],
            "conflict_policies",
        ),
        (("seed",), 25_000_502, "seed"),
        (("deduplication", "structural"), False, "structural_deduplication"),
        (("behavioral_novelty", "minimum_fraction"), 0.25, "novelty"),
    ],
)
def test_portfolio_manifest_rejects_generator_representation_drift(
    tmp_path: Path,
    path: tuple[str, ...],
    replacement: Any,
    label: str,
) -> None:
    manifest_path, manifest = _valid_manifest(tmp_path)
    _set_path(manifest["portfolio_books"], path, replacement)
    _rehash_portfolio_books(manifest)

    with pytest.raises(PortfolioManifestError, match=label):
        validate_portfolio_manifest(manifest, manifest_path=manifest_path)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("sleeve_id", "different-sleeve"),
        ("immutable_fingerprint", "f" * 64),
        ("behavioral_fingerprint", "e" * 64),
        ("signal_ledger_sha256", "d" * 64),
        ("trade_ledger_sha256", "c" * 64),
        ("market", "RTY"),
        ("economic_role", "RARE_EVENT_ALPHA"),
    ],
)
def test_portfolio_manifest_rejects_direct_nested_sleeve_drift(
    tmp_path: Path,
    field: str,
    replacement: Any,
) -> None:
    path, manifest = _valid_manifest(tmp_path)
    manifest["sleeve_bank"]["members"][0]["record"][field] = replacement

    with pytest.raises(
        PortfolioManifestError,
        match="sleeve direct/nested immutable field drift",
    ):
        validate_portfolio_manifest(manifest, manifest_path=path)
