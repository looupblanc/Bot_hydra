"""Strict manifest contract for the portfolio-first production lane."""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.evidence import REQUIRED_DATASETS
from hydra.production.portfolio_books import (
    PortfolioBookError,
    PortfolioBookGeneratorSpec,
    SleeveRecord,
)
from hydra.promotion.portfolio_status import (
    FROZEN_PORTFOLIO_PROMOTION_POLICY,
    PortfolioStatus,
)
from hydra.propfirm.combine_to_xfa import (
    UNREALIZED_AGGREGATION_SEMANTICS,
    official_rule_snapshot_2026_07_15,
)


PORTFOLIO_CAMPAIGN_MODE = "PORTFOLIO_FIRST"
PORTFOLIO_CLASS_ID = "PORTFOLIO_FIRST_COMBINE_TO_PAYOUT_V1"
PORTFOLIO_RUNTIME_VERSION = "hydra_portfolio_first_runtime_v1"
PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES = frozenset(
    {
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
        "hydra/research/economic_evolution_campaign.py",
        "hydra/research/economic_evolution_pilot.py",
        "hydra/research/qd_economic_tournament.py",
        "hydra/research/turbo_feature_builder.py",
        "hydra/shadow/package_factory.py",
        "hydra/shadow/portfolio_package.py",
        "scripts/run_economic_production_manifest.py",
    }
)
SLEEVE_ROLES = frozenset(
    {
        "TARGET_VELOCITY",
        "SESSION_DIVERSIFIER",
        "MARKET_DIVERSIFIER",
        "MLL_PROTECTOR",
        "CONSISTENCY_SMOOTHER",
        "XFA_WINNING_DAY_GENERATOR",
        "PAYOUT_SURVIVAL",
        "RARE_EVENT_ALPHA",
    }
)


class PortfolioManifestError(RuntimeError):
    pass


def validate_portfolio_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    _validate_shared_production_envelope(manifest, root=root)
    if (
        manifest.get("campaign_mode") != PORTFOLIO_CAMPAIGN_MODE
        or manifest.get("class_id") != PORTFOLIO_CLASS_ID
        or tuple(manifest.get("policy_classes") or ()) != (PORTFOLIO_CLASS_ID,)
        or manifest.get("development_only") is not True
    ):
        raise PortfolioManifestError("portfolio campaign identity drift")
    if not str(manifest.get("economic_hypothesis") or "").strip():
        raise PortfolioManifestError("portfolio economic hypothesis is required")

    implementation = _mapping(manifest, "implementation_files")
    declared_implementation = set(str(value) for value in implementation)
    missing_implementation = (
        PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES - declared_implementation
    )
    unexpected_implementation = (
        declared_implementation - PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES
    )
    if not implementation or missing_implementation or unexpected_implementation:
        raise PortfolioManifestError(
            "portfolio implementation closure is incomplete: "
            + ", ".join(sorted(missing_implementation))
            + "; unexpected: "
            + ", ".join(sorted(unexpected_implementation))
        )
    for relative, claimed in implementation.items():
        target = (root / str(relative)).resolve()
        if root not in target.parents or _sha256(target) != str(claimed):
            raise PortfolioManifestError(
                f"portfolio implementation checksum drift: {relative}"
            )

    runtime = _mapping(manifest, "runtime")
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("portfolio_runtime_version") != PORTFOLIO_RUNTIME_VERSION
        or runtime.get("controller_source_change_required") is not False
        or runtime.get("resume_from_checkpoint") is not True
        or int(runtime.get("worker_count", 0)) != 3
        or int(runtime.get("asynchronous_evidence_writer_count", 0)) != 1
    ):
        raise PortfolioManifestError("stable portfolio runtime declaration drift")

    governance = _mapping(manifest, "governance")
    for key in (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "proof_window_consumption_allowed",
        "status_inheritance_allowed",
    ):
        if governance.get(key) is not False:
            raise PortfolioManifestError(f"unsafe portfolio authority: {key}")

    allocation = _mapping(manifest, "compute_allocation")
    values = (
        float(allocation.get("portfolio_lifecycle_fraction", -1.0)),
        float(allocation.get("sleeve_replenishment_fraction", -1.0)),
        float(allocation.get("forward_shadow_fraction", -1.0)),
        float(allocation.get("safety_controls_reporting_fraction", -1.0)),
    )
    if values != (0.80, 0.10, 0.05, 0.05) or abs(sum(values) - 1.0) > 1e-12:
        raise PortfolioManifestError("portfolio compute allocation must be 80/10/5/5")

    bank = _mapping(manifest, "sleeve_bank")
    members = list(bank.get("members") or ())
    if not 12 <= len(members) <= 20:
        raise PortfolioManifestError("portfolio sleeve bank must contain 12..20 sleeves")
    if any(not isinstance(row, Mapping) for row in members):
        raise PortfolioManifestError("portfolio sleeve members must be objects")
    ids = [str(row.get("sleeve_id") or "") for row in members]
    fingerprints = [str(row.get("immutable_fingerprint") or "") for row in members]
    clusters = [str(row.get("behavioral_cluster") or "") for row in members]
    if (
        len(set(ids)) != len(ids)
        or len(set(fingerprints)) != len(fingerprints)
        or len(set(clusters)) != len(clusters)
        or any(len(value) != 64 for value in fingerprints)
        or any(str(row.get("status")) != "SLEEVE_ECONOMICALLY_ELIGIBLE" for row in members)
        or any(str(row.get("role")) not in SLEEVE_ROLES for row in members)
        or any(row.get("family_status_inherited") is not False for row in members)
        or any(row.get("complete_trade_ledger") is not True for row in members)
    ):
        raise PortfolioManifestError("sleeve-bank identity/evidence drift")
    records = [_validated_sleeve_record(row) for row in members]
    if (
        len({row.sleeve_id for row in records}) != len(records)
        or len({row.immutable_fingerprint for row in records}) != len(records)
        or len({row.behavioral_fingerprint for row in records}) != len(records)
    ):
        raise PortfolioManifestError("nested sleeve records are not unique")
    source = _mapping(bank, "source_runtime_summary")
    if (
        bank.get("source_evidence_campaign_id")
        != "hydra_economic_production_0024"
        or bank.get("source_bundle_status")
        != "STAGING_PRESERVED_NONCONFIRMATORY"
        or bank.get("source_bundle_complete") is not False
        or bank.get("component_ledgers_complete") is not True
        or bank.get(
            "runtime_requires_deterministic_recompile_and_hash_reconciliation"
        )
        is not True
        or bank.get("new_campaign_rematerializes_and_seals_own_ledgers") is not True
        or int(bank.get("member_count", -1)) != len(members)
        or int(bank.get("behavioral_cluster_count", -1)) != len(members)
    ):
        raise PortfolioManifestError("portfolio source EvidenceBundle identity drift")
    source_path = (root / str(source.get("path") or "")).resolve()
    if root not in source_path.parents or _sha256(source_path) != str(
        source.get("file_sha256") or ""
    ):
        raise PortfolioManifestError("sleeve-bank source summary checksum drift")
    for label in ("selector_terminal_receipt", "source_seed_archive"):
        reference = _mapping(bank, label)
        reference_path = (root / str(reference.get("path") or "")).resolve()
        if (
            root not in reference_path.parents
            or _sha256(reference_path) != str(reference.get("file_sha256") or "")
        ):
            raise PortfolioManifestError(
                f"sleeve-bank {label} checksum drift"
            )
    reconciliation = _mapping(bank, "source_ledger_reconciliation")
    if (
        reconciliation.get("campaign_id") != "hydra_economic_production_0024"
        or int(reconciliation.get("component_signal_rows_checked", 0)) < 1
        or int(reconciliation.get("component_trade_rows_checked", 0)) < 1
        or reconciliation.get("all_member_specification_hashes_match") is not True
        or reconciliation.get("all_member_signal_hashes_recomputed") is not True
        or reconciliation.get("all_member_trade_hashes_recomputed") is not True
        or reconciliation.get("outcomes_used_to_mutate_sleeves") is not False
    ):
        raise PortfolioManifestError("sleeve source-ledger reconciliation drift")

    generator = _mapping(manifest, "book_generator")
    portfolio_books = _mapping(manifest, "portfolio_books")
    try:
        _validate_generator_representation_consistency(generator, portfolio_books)
        PortfolioBookGeneratorSpec.from_manifest(portfolio_books)
    except PortfolioManifestError:
        raise
    except (KeyError, TypeError, ValueError, PortfolioBookError) as exc:
        raise PortfolioManifestError(
            f"invalid portfolio_books generator contract: {exc}"
        ) from exc
    if (
        int(generator.get("pair_count", 0)) != 20_000
        or int(generator.get("combine_sleeve_minimum", 0)) != 2
        or int(generator.get("combine_sleeve_maximum", 0)) != 6
        or int(generator.get("xfa_sleeve_minimum", 0)) != 1
        or int(generator.get("xfa_sleeve_maximum", 0)) != 6
        or float(generator.get("behavioral_novelty_minimum", 0.0)) < 0.20
        or generator.get("structural_deduplication") is not True
        or generator.get("behavioral_deduplication") is not True
        or generator.get("stage1_full_sleeve_coverage_required") is not True
        or generator.get("scientific_null_diagnostic_fallback_allowed") is not True
        or generator.get("pre_replay_behavioral_basis")
        != "SEMANTIC_SLEEVE_COMPOSITION_PREDICTION_ONLY"
        or generator.get("stage1_actual_account_behavior_dedup_required") is not True
        or generator.get("cross_campaign_account_path_novelty_claimed") is not False
        or tuple(generator.get("conflict_policies") or ())
        != ("PRIORITY",)
    ):
        raise PortfolioManifestError("portfolio book generator drift")
    risk = tuple(float(value) for value in generator.get("risk_frontier") or ())
    if risk != (0.75, 1.0, 1.15, 1.3):
        raise PortfolioManifestError("portfolio static risk frontier drift")

    controls = _mapping(manifest, "matched_controls")
    if controls != {
        "status": "NOT_APPLICABLE_PORTFOLIO_MASS_PRODUCTION",
        "reason": "PRIMARY_PRODUCT_IS_ACCOUNT_BOOK_POPULATION_NOT_FAMILY_EFFECT_TEST",
        "controls_claimed_executed": False,
        "raw_pnl_is_primary_rank": False,
    }:
        raise PortfolioManifestError(
            "portfolio manifest must not claim unexecuted matched controls"
        )

    halving = _mapping(manifest, "successive_halving")
    expected = {
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
    }
    if any(int(halving.get(key, -1)) != value for key, value in expected.items()):
        raise PortfolioManifestError("portfolio successive-halving policy drift")
    if float(halving.get("stress_cost_multiplier", 0.0)) != 1.5:
        raise PortfolioManifestError("portfolio stress-cost policy drift")

    lifecycle = _mapping(manifest, "lifecycle")
    snapshot = official_rule_snapshot_2026_07_15()
    if (
        lifecycle.get("rule_snapshot_version") != snapshot.rule_version
        or lifecycle.get("rule_snapshot_fingerprint") != snapshot.fingerprint
        or lifecycle.get("combine_profit_transferred_to_xfa") is not False
        or lifecycle.get("standard_and_consistency_both_evaluated") is not True
        or lifecycle.get("books_frozen_before_outcomes") is not True
        or lifecycle.get("combine_calendar_scope")
        != "FROZEN_START_TEMPORAL_BLOCK_ONLY"
        or lifecycle.get("xfa_calendar_scope")
        != "FULL_REMAINING_CACHED_CHRONOLOGY_AFTER_COMBINE_PASS"
        or lifecycle.get("cross_block_xfa_paths_claimed_independent") is not False
        or lifecycle.get("successful_combine_without_remaining_xfa_day")
        != "XFA_DATA_CENSORED"
    ):
        raise PortfolioManifestError("Combine-to-XFA lifecycle declaration drift")

    promotion = _mapping(manifest, "promotion_policy")
    frozen_policy = FROZEN_PORTFOLIO_PROMOTION_POLICY.to_dict()
    if (
        promotion.get("policy") != frozen_policy
        or promotion.get("policy_fingerprint")
        != FROZEN_PORTFOLIO_PROMOTION_POLICY.fingerprint
        or promotion.get("family_failure_erases_candidate_evidence") is not False
        or promotion.get("paper_shadow_ready_from_development_allowed") is not False
        or promotion.get("no_order_forward_observation_allowed") is not True
        or tuple(promotion.get("status_ladder") or ())
        != tuple(value.value for value in PortfolioStatus)
    ):
        raise PortfolioManifestError("portfolio promotion policy drift")

    temporal = _mapping(manifest, "temporal_blocks")
    blocks = list(temporal.get("blocks") or ())
    if len(blocks) < 4 or len({str(row.get("block_id")) for row in blocks}) != len(blocks):
        raise PortfolioManifestError("portfolio replay needs four independent blocks")
    if temporal.get("overlapping_starts_independent") is not False:
        raise PortfolioManifestError("overlapping starts cannot become independent")

    evidence = _mapping(manifest, "evidence_bundle")
    if (
        evidence.get("required_for_campaign_complete") is not True
        or evidence.get("atomic_finalize") is not True
        or evidence.get("summary_only_complete_allowed") is not False
        or set(evidence.get("required_datasets") or ()) != set(REQUIRED_DATASETS)
        or str(evidence.get("destination") or "")
        != "data/cache/evidence_bundles"
        or evidence.get("large_files_git_tracked") is not False
        or evidence.get("reconstruction_flag") is not False
    ):
        raise PortfolioManifestError("portfolio EvidenceBundle contract drift")
    lightweight = str(evidence.get("lightweight_manifest_path") or "")
    if (
        not lightweight.startswith("reports/economic_evolution/")
        or not lightweight.endswith("/evidence_bundle_receipt.json")
    ):
        raise PortfolioManifestError("portfolio EvidenceBundle receipt path drift")


def _validate_shared_production_envelope(
    manifest: Mapping[str, Any], *, root: Path
) -> None:
    """Validate fields consumed by V17 and the shared production runtime.

    PORTFOLIO_FIRST is a campaign mode, not a bypass around the production
    envelope.  Keeping these checks here makes malformed manifests fail before
    multiplicity reservation or a worker launch.
    """

    if manifest.get("schema") != "hydra_economic_production_manifest_v1":
        raise PortfolioManifestError("portfolio production schema drift")
    campaign_id = str(manifest.get("campaign_id") or "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", campaign_id):
        raise PortfolioManifestError("unsafe or empty portfolio campaign identity")
    source_commit = str(manifest.get("source_commit") or "")
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise PortfolioManifestError("portfolio source_commit must be a full Git SHA")
    created = str(manifest.get("created_at_utc") or "")
    try:
        parsed = datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioManifestError("invalid portfolio freeze timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioManifestError("portfolio freeze timestamp must be UTC-aware")

    runtime = _mapping(manifest, "runtime")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    allowed_output = (root / "reports/economic_evolution").resolve()
    result_name = str(runtime.get("result_name") or "")
    if (
        runtime.get("result_schema") != "hydra_economic_production_result_v1"
        or (output != allowed_output and allowed_output not in output.parents)
        or not result_name
        or Path(result_name).name != result_name
    ):
        raise PortfolioManifestError("portfolio runtime output envelope drift")

    data = _mapping(manifest, "data")
    if (
        data.get("role") != "DEVELOPMENT_ONLY_Q4_EXCLUDED"
        or not _is_sha256(str(data.get("feature_source_fingerprint") or ""))
        or not _is_sha256(str(data.get("contract_map_sha256") or ""))
        or data.get("cached_features_only") is not True
        or data.get("feature_recalculation_allowed") is not False
        or data.get("q4_access_allowed") is not False
        or data.get("new_purchase_allowed") is not False
    ):
        raise PortfolioManifestError("portfolio development-data envelope drift")

    component_bank = _mapping(manifest, "component_bank")
    if not isinstance(component_bank.get("sources"), Mapping):
        raise PortfolioManifestError("portfolio component-bank sources are required")

    multiplicity = _mapping(manifest, "multiplicity")
    try:
        prior = int(multiplicity["prior_global_N_trials"])
        prospective = int(multiplicity["prospective_comparisons"])
        delta = int(multiplicity["reserved_delta_trials"])
        expected = int(multiplicity["expected_global_N_trials_after_reservation"])
        inflation = float(multiplicity["campaign_specific_inflation"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioManifestError("invalid portfolio multiplicity envelope") from exc
    if (
        min(prior, prospective, delta, expected) < 0
        or prospective != 20_000
        or not math.isclose(inflation, 1.5)
        or delta != int(prospective * inflation)
        or expected != prior + delta
        or multiplicity.get("reservation_required_before_outcome_access") is not True
        or multiplicity.get("proof_window_consumed") is not False
    ):
        raise PortfolioManifestError("portfolio multiplicity reservation drift")

    budget = _mapping(manifest, "budget")
    try:
        actual = float(budget["actual_spend_usd"])
        hard_cap = float(budget["hard_cap_usd"])
        remaining = float(budget["remaining_usd"])
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioManifestError("invalid portfolio budget envelope") from exc
    if (
        not all(math.isfinite(value) and value >= 0.0 for value in (actual, hard_cap, remaining))
        or not math.isclose(actual + remaining, hard_cap, abs_tol=1e-8)
        or int(budget.get("new_data_purchase_count", -1)) != 0
    ):
        raise PortfolioManifestError("portfolio protected-budget drift")

    starts = _mapping(manifest, "episode_starts")
    if (
        int(starts.get("serious_policy_starts", 0)) != 48
        or starts.get("block_aware") is not True
        or starts.get("overlapping_starts_independent") is not False
        or starts.get("retuning_after_start_outcomes") is not False
    ):
        raise PortfolioManifestError("portfolio episode-start envelope drift")
    costs = _mapping(manifest, "costs")
    if (
        float(costs.get("normal_multiplier", 0.0)) != 1.0
        or float(costs.get("stressed_multiplier", 0.0)) != 1.5
        or costs.get("source_component_costs_frozen") is not True
        or costs.get("retune_after_outcomes") is not False
    ):
        raise PortfolioManifestError("portfolio cost envelope drift")
    account = _mapping(manifest, "account_parameters")
    if (
        float(account.get("starting_balance", 0.0)) != 150_000.0
        or float(account.get("profit_target", 0.0)) != 9_000.0
        or float(account.get("maximum_loss_limit", 0.0)) != 4_500.0
        or int(account.get("maximum_mini_equivalent", 0)) != 15
        or account.get("dynamic_loss_streak_ratchet") is not False
        or account.get("unrealized_aggregation_semantics")
        != UNREALIZED_AGGREGATION_SEMANTICS
        or account.get("timestamp_exact_combined_unrealized_claimed") is not False
    ):
        raise PortfolioManifestError("portfolio account envelope drift")

    compute = _mapping(manifest, "compute")
    if (
        int(compute.get("worker_count", 0)) != 3
        or int(compute.get("asynchronous_evidence_writer_count", 0)) != 1
        or compute.get("compute_workers_read_only") is not True
        or compute.get("process_start_method") != "spawn"
        or compute.get("batched_evidence_commits") is not True
        or compute.get("immutable_episode_cache") is not True
        or compute.get("full_repository_regression_per_wave") is not False
    ):
        raise PortfolioManifestError("portfolio compute envelope drift")

    markets = tuple(str(value) for value in manifest.get("markets") or ())
    contracts = _mapping(manifest, "contracts")
    if not markets or set(contracts) != set(markets):
        raise PortfolioManifestError("portfolio market/contract envelope drift")
    for market in markets:
        declaration = contracts.get(market)
        if (
            not isinstance(declaration, Mapping)
            or not str(declaration.get("mini") or "")
            or not str(declaration.get("micro") or "")
            or float(declaration.get("micro_per_mini", 0.0)) <= 0.0
        ):
            raise PortfolioManifestError("portfolio contract equivalence drift")
    if not set(str(value) for value in manifest.get("timeframes") or ()):
        raise PortfolioManifestError("portfolio timeframe envelope is empty")
    sessions = _mapping(manifest, "session_rules")
    if (
        sessions.get("source") != "FROZEN_COMPONENT_SESSION_CODE"
        or sessions.get("same_session_enforcement") is not True
        or sessions.get("overnight_fabrication_allowed") is not False
    ):
        raise PortfolioManifestError("portfolio session envelope drift")


def _validated_sleeve_record(member: Mapping[str, Any]) -> SleeveRecord:
    record_value = _mapping(member, "record")
    try:
        record = SleeveRecord.from_mapping(record_value)
    except (KeyError, TypeError, ValueError, PortfolioBookError) as exc:
        raise PortfolioManifestError(f"invalid nested sleeve record: {exc}") from exc

    direct_fields = (
        "sleeve_id",
        "immutable_fingerprint",
        "behavioral_fingerprint",
        "signal_ledger_sha256",
        "trade_ledger_sha256",
        "market",
        "contract",
        "timeframe",
        "session",
        "source_campaign",
        "family_id",
        "evidence_complete",
        "development_only",
        "inherited_status",
        "signal_mutation_allowed",
    )
    for field in direct_fields:
        if field in member and member[field] != getattr(record, field):
            raise PortfolioManifestError(
                f"sleeve direct/nested immutable field drift: {field}"
            )
    aliases = (
        ("role", "economic_role"),
        ("family_status_inherited", "inherited_status"),
    )
    for direct, nested in aliases:
        if direct in member and member[direct] != getattr(record, nested):
            raise PortfolioManifestError(
                f"sleeve direct/nested immutable field drift: {direct}"
            )
    return record


def _validate_generator_representation_consistency(
    generator: Mapping[str, Any], portfolio_books: Mapping[str, Any]
) -> None:
    combine = _mapping(portfolio_books, "combine_book")
    xfa = _mapping(portfolio_books, "xfa_book")
    deduplication = _mapping(portfolio_books, "deduplication")
    novelty = _mapping(portfolio_books, "behavioral_novelty")

    comparisons = (
        (
            "pair_count",
            int(generator.get("pair_count", -1)),
            int(portfolio_books.get("unique_pair_target", -2)),
        ),
        (
            "combine_sleeve_minimum",
            int(generator.get("combine_sleeve_minimum", -1)),
            int(combine.get("sleeve_minimum", -2)),
        ),
        (
            "combine_sleeve_maximum",
            int(generator.get("combine_sleeve_maximum", -1)),
            int(combine.get("sleeve_maximum", -2)),
        ),
        (
            "xfa_sleeve_minimum",
            int(generator.get("xfa_sleeve_minimum", -1)),
            int(xfa.get("sleeve_minimum", -2)),
        ),
        (
            "xfa_sleeve_maximum",
            int(generator.get("xfa_sleeve_maximum", -1)),
            int(xfa.get("sleeve_maximum", -2)),
        ),
        (
            "risk_frontier",
            tuple(float(value) for value in generator.get("risk_frontier") or ()),
            tuple(
                float(value) for value in portfolio_books.get("risk_frontier") or ()
            ),
        ),
        (
            "conflict_policies",
            tuple(str(value) for value in generator.get("conflict_policies") or ()),
            tuple(
                str(value) for value in portfolio_books.get("conflict_policies") or ()
            ),
        ),
        (
            "structural_deduplication",
            generator.get("structural_deduplication"),
            deduplication.get("structural"),
        ),
        (
            "behavioral_deduplication",
            generator.get("behavioral_deduplication"),
            deduplication.get("behavioral"),
        ),
        (
            "behavioral_novelty_minimum",
            float(generator.get("behavioral_novelty_minimum", -1.0)),
            float(novelty.get("minimum_fraction", -2.0)),
        ),
    )
    for label, declared, executable in comparisons:
        if declared != executable:
            raise PortfolioManifestError(
                f"book_generator/portfolio_books representation drift: {label}"
            )

    conditional = (
        ("seed", "seed", int),
        ("combine_risk_tier", "combine_risk_tier", float),
        ("xfa_risk_tier", "xfa_risk_tier", float),
        ("maximum_attempt_multiplier", "maximum_attempt_multiplier", int),
    )
    for direct, nested, cast in conditional:
        if direct in generator:
            if nested not in portfolio_books or cast(generator[direct]) != cast(
                portfolio_books[nested]
            ):
                raise PortfolioManifestError(
                    f"book_generator/portfolio_books representation drift: {direct}"
                )

    conditional_nested = (
        (
            "excluded_structural_fingerprints",
            deduplication,
            "excluded_structural_fingerprints",
        ),
        (
            "reference_book_behavioral_fingerprints",
            novelty,
            "reference_book_fingerprints",
        ),
    )
    for direct, nested_object, nested in conditional_nested:
        if direct in generator and tuple(generator[direct] or ()) != tuple(
            nested_object.get(nested) or ()
        ):
            raise PortfolioManifestError(
                f"book_generator/portfolio_books representation drift: {direct}"
            )


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    output = value.get(key)
    if not isinstance(output, Mapping):
        raise PortfolioManifestError(f"portfolio manifest requires object: {key}")
    return output


def _sha256(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError as exc:
        raise PortfolioManifestError(f"cannot hash portfolio source: {path}") from exc


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


__all__ = [
    "PORTFOLIO_CAMPAIGN_MODE",
    "PORTFOLIO_CLASS_ID",
    "PORTFOLIO_REQUIRED_IMPLEMENTATION_FILES",
    "PORTFOLIO_RUNTIME_VERSION",
    "PortfolioManifestError",
    "SLEEVE_ROLES",
    "validate_portfolio_manifest",
]
