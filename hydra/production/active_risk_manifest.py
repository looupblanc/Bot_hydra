"""Frozen manifest contract for the shared active-risk-pool campaign.

This campaign is an account-governor experiment over the eighteen immutable
portfolio sleeves.  It is deliberately not a new sleeve grammar: source
signals and trade ledgers remain frozen, while only causal account admission,
concurrency, and risk-pool decisions may vary.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from hydra.evidence import REQUIRED_DATASETS
from hydra.production.portfolio_books import PortfolioBookError, SleeveRecord
from hydra.production.portfolio_manifest import (
    PortfolioManifestError,
    _validate_shared_production_envelope,
)
from hydra.propfirm.combine_to_xfa import LIFECYCLE_VERSION


ACTIVE_RISK_CAMPAIGN_MODE = "ACTIVE_RISK_POOL"
ACTIVE_RISK_CLASS_ID = "ACTIVE_RISK_POOL_TARGET_VELOCITY_V1"
ACTIVE_RISK_RUNTIME_VERSION = "hydra_active_risk_pool_runtime_v1"
# Executable micro-contract multipliers.  Fractional tiers collapse onto the
# same integer micro quantities for these sleeves, so the frozen frontier uses
# distinct 1x..4x levels.  The 4x nominal ceiling is the executable discrete
# representation of the sealed static campaign's approximately 3.9x observed
# effective maximum.
ACTIVE_RISK_RISK_FRONTIER = (1.0, 2.0, 3.0, 4.0)
ACTIVE_RISK_HORIZONS = (20, 40, 60, 90, "FULL")
ACTIVE_RISK_MATCHED_CONTROLS = (
    "STATIC_CAPITAL_PARTITION",
    "COMPONENT_SLEEVE_STANDALONE",
    "EQUAL_RISK_ACTIVE_SLEEVE_POOLING",
    "RANDOM_PRIORITY_ACTIVE_POOLING_EXPOSURE_MATCHED",
    "ALWAYS_ON_POOLED_GOVERNOR",
)
ACTIVE_RISK_IDENTITY_INVARIANTS = (
    "SINGLE_SLEEVE_IDENTITY",
    "INACTIVE_SLEEVE_INVARIANCE",
    "NON_OVERLAPPING_SLEEVE_CONSERVATION",
    "CONFLICT_ACCOUNTING",
    "RISK_UTILISATION_AUDIT",
    "STATIC_DILUTION_AUDIT",
)
ACTIVE_RISK_DECISION_FIELDS = (
    "emitted",
    "accepted",
    "rejected",
    "size_reduced",
    "conflict_rejected",
    "contract_limit_rejected",
    "mll_risk_rejected",
)
ACTIVE_RISK_RANDOM_PRIORITY_SEEDS = tuple(range(25_002_600, 25_002_632))
ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE = 0.05
ACTIVE_RISK_RANDOM_EXPOSURE_SIGNATURE_FIELDS = (
    "TIME_WEIGHTED_MINI_NANOSECONDS_PER_OBSERVED_DAY",
    "ACCEPTED_EVENT_RATE",
)

# Exact executable closure: every path must be declared and checksum-pinned.
# Keeping this literal prevents an unrelated implementation file from silently
# entering (or leaving) the frozen economic hypothesis through a broad glob.
ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
        "hydra/account_policy/active_pool_replay.py",
        "hydra/account_policy/active_risk_pool.py",
        "hydra/account_policy/basket.py",
        "hydra/account_policy/router.py",
        "hydra/account_policy/schema.py",
        "hydra/compute/result_writer.py",
        "hydra/economic_evolution/account_evaluation.py",
        "hydra/economic_evolution/schema.py",
        "hydra/economic_evolution/screen.py",
        "hydra/evidence/__init__.py",
        "hydra/evidence/bundle.py",
        "hydra/evidence/schema.py",
        "hydra/features/feature_matrix.py",
        "hydra/markets/instruments.py",
        "hydra/production/__init__.py",
        "hydra/production/active_risk_manifest.py",
        "hydra/production/active_risk_runtime.py",
        "hydra/production/component_evidence.py",
        "hydra/production/episode_evidence.py",
        "hydra/production/evidence_adapter.py",
        "hydra/production/halving.py",
        "hydra/production/manifest.py",
        "hydra/production/mll_accounting.py",
        "hydra/production/policy_factory.py",
        "hydra/production/portfolio_books.py",
        "hydra/production/portfolio_manifest.py",
        "hydra/production/portfolio_runtime.py",
        "hydra/production/replay.py",
        "hydra/production/runtime.py",
        "hydra/promotion/portfolio_status.py",
        "hydra/propfirm/combine_episode.py",
        "hydra/propfirm/combine_to_xfa.py",
        "hydra/propfirm/mll_variants.py",
        "hydra/propfirm/portfolio_combine_to_xfa.py",
        "hydra/propfirm/rolling_combine.py",
        "hydra/propfirm/topstep_150k.py",
        "hydra/shadow/package_factory.py",
        "hydra/shadow/portfolio_package.py",
        "scripts/run_economic_production_manifest.py",
    }
)


class ActiveRiskManifestError(RuntimeError):
    """The active-risk campaign declaration is incomplete or has drifted."""


def validate_active_risk_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    """Validate the frozen active-risk-pool economic hypothesis."""

    path = Path(manifest_path).resolve()
    root = path.parents[2]
    try:
        _validate_shared_production_envelope(manifest, root=root)
    except PortfolioManifestError as exc:
        raise ActiveRiskManifestError(str(exc)) from exc

    if (
        manifest.get("campaign_mode") != ACTIVE_RISK_CAMPAIGN_MODE
        or manifest.get("class_id") != ACTIVE_RISK_CLASS_ID
        or tuple(manifest.get("policy_classes") or ()) != (ACTIVE_RISK_CLASS_ID,)
        or manifest.get("development_only") is not True
    ):
        raise ActiveRiskManifestError("active-risk campaign identity drift")
    if not str(manifest.get("economic_hypothesis") or "").strip():
        raise ActiveRiskManifestError("active-risk economic hypothesis is required")
    if (
        manifest.get("source_static_book_classification")
        != "STATIC_CAPITAL_PARTITION_TOO_SLOW"
        or not str(manifest.get("source_static_book_campaign_id") or "").strip()
        or manifest.get("source_sleeves_killed") is not False
    ):
        raise ActiveRiskManifestError("source static-book classification drift")

    _validate_implementation_closure(manifest, root=root)
    _validate_runtime(manifest)
    _validate_governance(manifest)
    _validate_compute_allocation(manifest)
    _validate_sleeve_bank(manifest)
    _validate_identity_audit(manifest)
    _validate_governor(manifest)
    _validate_controls(manifest)
    _validate_halving(manifest)
    _validate_evidence(manifest)


def _validate_implementation_closure(
    manifest: Mapping[str, Any], *, root: Path
) -> None:
    implementation = _mapping(manifest, "implementation_files")
    declared = {str(value) for value in implementation}
    missing = ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES - declared
    unexpected = declared - ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES
    if missing or unexpected:
        raise ActiveRiskManifestError(
            "active-risk implementation closure drift; missing: "
            + ", ".join(sorted(missing))
            + "; unexpected: "
            + ", ".join(sorted(unexpected))
        )
    for relative, claimed in implementation.items():
        target = (root / str(relative)).resolve()
        if root not in target.parents or _sha256(target) != str(claimed):
            raise ActiveRiskManifestError(
                f"active-risk implementation checksum drift: {relative}"
            )


def _validate_runtime(manifest: Mapping[str, Any]) -> None:
    runtime = _mapping(manifest, "runtime")
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("active_risk_runtime_version")
        != ACTIVE_RISK_RUNTIME_VERSION
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or int(runtime.get("worker_count", 0)) != 3
        or int(runtime.get("asynchronous_evidence_writer_count", 0)) != 1
    ):
        raise ActiveRiskManifestError("stable active-risk runtime declaration drift")


def _validate_governance(manifest: Mapping[str, Any]) -> None:
    governance = _mapping(manifest, "governance")
    forbidden = (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "proof_window_consumption_allowed",
        "status_inheritance_allowed",
        "source_signal_mutation_allowed",
        "source_entry_exit_recalculation_allowed",
        "new_market_grammar_allowed",
        "new_sleeve_discovery_campaign_allowed",
    )
    for key in forbidden:
        if governance.get(key) is not False:
            raise ActiveRiskManifestError(f"unsafe active-risk authority: {key}")


def _validate_compute_allocation(manifest: Mapping[str, Any]) -> None:
    allocation = _mapping(manifest, "compute_allocation")
    values = (
        float(allocation.get("active_pool_target_velocity_fraction", -1.0)),
        float(allocation.get("targeted_high_velocity_replenishment_fraction", -1.0)),
        float(allocation.get("xfa_lifecycle_fraction", -1.0)),
        float(allocation.get("safety_controls_reporting_fraction", -1.0)),
    )
    if values != (0.70, 0.20, 0.05, 0.05) or abs(sum(values) - 1.0) > 1e-12:
        raise ActiveRiskManifestError(
            "active-risk compute allocation must be 70/20/5/5"
        )


def _validate_sleeve_bank(manifest: Mapping[str, Any]) -> None:
    bank = _mapping(manifest, "sleeve_bank")
    members = list(bank.get("members") or ())
    if len(members) != 18 or int(bank.get("member_count", -1)) != 18:
        raise ActiveRiskManifestError("active-risk bank must preserve exactly 18 sleeves")
    if bank.get("underlying_sleeves_immutable") is not True:
        raise ActiveRiskManifestError("active-risk sleeves must remain immutable")
    if bank.get("inactive_sleeves_reserve_risk") is not False:
        raise ActiveRiskManifestError("inactive sleeves cannot reserve account risk")
    if any(not isinstance(row, Mapping) for row in members):
        raise ActiveRiskManifestError("active-risk sleeve members must be objects")

    ids: list[str] = []
    immutable: list[str] = []
    behavioral: list[str] = []
    signals: list[str] = []
    trades: list[str] = []
    for member in members:
        try:
            record = SleeveRecord.from_mapping(_mapping(member, "record"))
        except (KeyError, TypeError, ValueError, PortfolioBookError) as exc:
            raise ActiveRiskManifestError(f"invalid active-risk sleeve: {exc}") from exc
        direct = {
            "sleeve_id": record.sleeve_id,
            "immutable_fingerprint": record.immutable_fingerprint,
            "behavioral_fingerprint": record.behavioral_fingerprint,
            "signal_ledger_sha256": record.signal_ledger_sha256,
            "trade_ledger_sha256": record.trade_ledger_sha256,
            "market": record.market,
            "contract": record.contract,
            "timeframe": record.timeframe,
            "session": record.session,
        }
        if any(member.get(key) != expected for key, expected in direct.items()):
            raise ActiveRiskManifestError(
                "active-risk sleeve direct/nested immutable field drift"
            )
        if (
            member.get("complete_trade_ledger") is not True
            or member.get("signal_mutation_allowed") is not False
            or member.get("entry_exit_recalculation_allowed") is not False
            or member.get("preserved_after_static_family_failure") is not True
        ):
            raise ActiveRiskManifestError("active-risk sleeve evidence/immutability drift")
        ids.append(record.sleeve_id)
        immutable.append(record.immutable_fingerprint)
        behavioral.append(record.behavioral_fingerprint)
        signals.append(record.signal_ledger_sha256)
        trades.append(record.trade_ledger_sha256)
    for label, values in (
        ("IDs", ids),
        ("immutable fingerprints", immutable),
        ("behavioral fingerprints", behavioral),
        ("signal ledgers", signals),
        ("trade ledgers", trades),
    ):
        if len(set(values)) != 18 or any(not value for value in values):
            raise ActiveRiskManifestError(f"active-risk sleeve {label} are not unique")


def _validate_identity_audit(manifest: Mapping[str, Any]) -> None:
    audit = _mapping(manifest, "identity_audit")
    if (
        tuple(audit.get("required_invariants") or ())
        != ACTIVE_RISK_IDENTITY_INVARIANTS
        or audit.get("required_before_economic_outcomes") is not True
        or audit.get("repair_shared_engine_before_evaluation_on_failure") is not True
        or audit.get("single_deterministic_audit") is not True
        or audit.get("future_outcomes_used_for_routing") is not False
        or audit.get("actual_stop_risk_available") is not False
        or audit.get("routing_risk_measure")
        != "DECLARED_NOMINAL_RISK_UTILISATION"
        or audit.get("ex_post_mae_routing_allowed") is not False
        or tuple(audit.get("conflict_decision_fields") or ())
        != ACTIVE_RISK_DECISION_FIELDS
        or audit.get("foregone_pnl_persisted") is not True
    ):
        raise ActiveRiskManifestError("active-risk identity/audit contract drift")


def _validate_governor(manifest: Mapping[str, Any]) -> None:
    generator = _mapping(manifest, "governor_generator")
    if (
        int(generator.get("proposal_count", 0)) != 20_000
        or int(generator.get("unique_vectorized_screen_minimum", 0)) < 4_096
        or int(generator.get("exact_replay_maximum", 0)) != 1_024
        or tuple(float(value) for value in generator.get("risk_frontier") or ())
        != ACTIVE_RISK_RISK_FRONTIER
        or generator.get("inactive_sleeves_reserve_risk") is not False
        or generator.get("sole_active_sleeve_preserves_nominal_risk") is not True
        or generator.get("concurrent_sleeves_share_current_available_risk") is not True
        or generator.get("bounded_discrete_policy_set") is not True
        or generator.get("structural_deduplication") is not True
        or generator.get("behavioral_deduplication") is not True
        or generator.get("continuous_optimization_allowed") is not False
        or generator.get("global_contract_multiplier_allowed") is not False
        or generator.get("loss_streak_ratchet_allowed") is not False
        or generator.get("underlying_signal_changes_allowed") is not False
    ):
        raise ActiveRiskManifestError("active-risk governor generator drift")
    dimensions = set(str(value) for value in generator.get("bounded_dimensions") or ())
    required = {
        "MAXIMUM_CONCURRENT_SLEEVES",
        "AGGREGATE_OPEN_RISK_CEILING",
        "PER_SLEEVE_NOMINAL_RISK_PRESERVATION",
        "PROPORTIONAL_SCALING_DURING_CONCURRENCY",
        "DETERMINISTIC_SLEEVE_PRIORITY",
        "SAME_INSTRUMENT_CONFLICT_RULE",
        "DAILY_CONSISTENCY_GUARD",
        "TARGET_PROTECTION_MODE",
        "STATIC_RISK_TIER",
    }
    if dimensions != required:
        raise ActiveRiskManifestError("active-risk bounded governor dimensions drift")


def _validate_controls(manifest: Mapping[str, Any]) -> None:
    controls = _mapping(manifest, "matched_controls")
    if (
        tuple(controls.get("controls") or ()) != ACTIVE_RISK_MATCHED_CONTROLS
        or controls.get("identical_sleeve_ledgers") is not True
        or controls.get("identical_episode_starts") is not True
        or controls.get("identical_temporal_blocks") is not True
        or controls.get("identical_costs") is not True
        or controls.get("identical_horizons") is not True
        or controls.get("identical_topstep_configuration") is not True
        or controls.get("random_priority_exposure_matched") is not True
        or tuple(int(value) for value in controls.get("random_priority_seeds") or ())
        != ACTIVE_RISK_RANDOM_PRIORITY_SEEDS
        or float(controls.get("random_priority_exposure_relative_tolerance", -1.0))
        != ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE
        or tuple(controls.get("random_priority_exposure_signature_fields") or ())
        != ACTIVE_RISK_RANDOM_EXPOSURE_SIGNATURE_FIELDS
        or controls.get("random_priority_match_selection_uses_economic_outcomes")
        is not False
        or controls.get("unmatched_random_control_blocks_promotion") is not True
        or controls.get("executed_for_every_serious_policy") is not True
    ):
        raise ActiveRiskManifestError("active-risk matched-control contract drift")


def _validate_halving(manifest: Mapping[str, Any]) -> None:
    halving = _mapping(manifest, "successive_halving")
    expected = {
        "stage1_proposals": 20_000,
        "stage1_unique_screen_minimum": 4_096,
        "stage2_exact_replay_maximum": 1_024,
        "stage3_48_start_maximum": 256,
        "stage3_survivor_maximum": 32,
        "stage4_96_start_maximum": 32,
        "stage4_survivor_maximum": 8,
        "stage5_192_start_maximum": 8,
    }
    if any(int(halving.get(key, -1)) != value for key, value in expected.items()):
        raise ActiveRiskManifestError("active-risk successive-halving policy drift")
    if (
        tuple(halving.get("frozen_horizons") or ()) != ACTIVE_RISK_HORIZONS
        or float(halving.get("stress_cost_multiplier", 0.0)) != 1.5
        or halving.get("retuning_after_outcomes") is not False
        or halving.get("automatic_xfa_on_combine_pass") is not True
    ):
        raise ActiveRiskManifestError("active-risk horizon/lifecycle policy drift")
    xfa = _mapping(halving, "xfa_profile_projection")
    if (
        xfa.get("profile_version") != LIFECYCLE_VERSION
        or xfa.get("policy")
        != "STATIC_PROJECTION_OF_ACTIVE_GOVERNOR_V1"
        or xfa.get("risk_multiplier_source") != "STATIC_RISK_TIER"
        or xfa.get("maximum_simultaneous_positions_source")
        != "MAXIMUM_CONCURRENT_SLEEVES"
        or xfa.get("maximum_mini_equivalent_source")
        != "GOVERNOR_MAXIMUM_MINI_EQUIVALENT"
        or xfa.get("clip_to_official_scaling_plan") is not True
        or xfa.get("same_market_exclusive") is not True
        or xfa.get("active_pool_combine_only_controls_applied") is not False
        or xfa.get("selected_after_combine_outcome") is not False
    ):
        raise ActiveRiskManifestError("active-risk frozen XFA profile drift")


def _validate_evidence(manifest: Mapping[str, Any]) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    if (
        evidence.get("required_for_campaign_complete") is not True
        or evidence.get("atomic_finalize") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or set(evidence.get("required_datasets") or ()) != set(REQUIRED_DATASETS)
        or evidence.get("large_files_git_tracked") is not False
        or evidence.get("reconstruction_flag") is not False
        or str(evidence.get("destination") or "") != "data/cache/evidence_bundles"
    ):
        raise ActiveRiskManifestError("active-risk EvidenceBundle contract drift")
    receipt = str(evidence.get("lightweight_manifest_path") or "")
    if (
        not receipt.startswith("reports/economic_evolution/")
        or not receipt.endswith("/evidence_bundle_receipt.json")
    ):
        raise ActiveRiskManifestError("active-risk EvidenceBundle receipt path drift")


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    output = value.get(key)
    if not isinstance(output, Mapping):
        raise ActiveRiskManifestError(f"active-risk manifest requires object: {key}")
    return output


def _sha256(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise ActiveRiskManifestError(
            f"cannot hash active-risk implementation: {path}"
        ) from exc


__all__ = [
    "ACTIVE_RISK_CAMPAIGN_MODE",
    "ACTIVE_RISK_CLASS_ID",
    "ACTIVE_RISK_DECISION_FIELDS",
    "ACTIVE_RISK_HORIZONS",
    "ACTIVE_RISK_IDENTITY_INVARIANTS",
    "ACTIVE_RISK_MATCHED_CONTROLS",
    "ACTIVE_RISK_RANDOM_EXPOSURE_RELATIVE_TOLERANCE",
    "ACTIVE_RISK_RANDOM_EXPOSURE_SIGNATURE_FIELDS",
    "ACTIVE_RISK_RANDOM_PRIORITY_SEEDS",
    "ACTIVE_RISK_REQUIRED_IMPLEMENTATION_FILES",
    "ACTIVE_RISK_RISK_FRONTIER",
    "ACTIVE_RISK_RUNTIME_VERSION",
    "ActiveRiskManifestError",
    "validate_active_risk_manifest",
]
