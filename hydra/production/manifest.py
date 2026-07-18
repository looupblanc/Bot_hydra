from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import REQUIRED_DATASETS


PRODUCTION_MANIFEST_SCHEMA = "hydra_economic_production_manifest_v1"
PRODUCTION_RESULT_SCHEMA = "hydra_economic_production_result_v1"
PRODUCTION_ENGINE = "production_kernel_v1"
POLICY_CLASSES = frozenset(
    {
        "REGIME_GATED_SLEEVES",
        "SESSION_SPECIALIZED_ROUTING",
        "OPPORTUNITY_DENSITY",
        "FIXED_STATIC_RISK_FRONTIER",
        "MARKET_ROLE_ROTATION",
        "TARGET_VELOCITY_MLL_PROTECTION",
        "NEW_MICRO_EDGE_ASSEMBLY",
    }
)
FORBIDDEN_REUSE_CLASSES = frozenset(
    {
        "LOSS_STREAK_BUFFER_RATCHET",
        "STATIC_PARENT_SYNTHESIS",
        "GEOMETRY_ONLY",
    }
)


class ProductionManifestError(RuntimeError):
    pass


def load_and_validate_production_manifest(path: str | Path) -> dict[str, Any]:
    """Load a frozen production manifest and verify its development sources.

    This validator intentionally does not reserve multiplicity or mutate any
    mission state.  The persistent controller remains the only registry/DB
    writer and performs reservation before invoking the runner.
    """

    resolved = Path(path).resolve()
    manifest = _load_json(resolved)
    claimed = str(manifest.get("manifest_hash") or "")
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    if not claimed or stable_hash(payload) != claimed:
        raise ProductionManifestError("production manifest hash drift")
    if manifest.get("schema") != PRODUCTION_MANIFEST_SCHEMA:
        raise ProductionManifestError("unsupported production manifest schema")
    if manifest.get("campaign_mode") == "FAST_PASS_FACTORY":
        from hydra.production.fast_pass_manifest import (
            FastPassManifestError,
            validate_fast_pass_manifest,
        )

        try:
            validate_fast_pass_manifest(manifest, manifest_path=resolved)
        except FastPassManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        return manifest
    if manifest.get("campaign_mode") == "MICROSTRUCTURE_ORDER_FLOW_FOUNDRY":
        from hydra.production.microstructure_foundry_manifest import (
            FoundryManifestError,
            validate_microstructure_foundry_manifest,
        )

        try:
            validate_microstructure_foundry_manifest(manifest, manifest_path=resolved)
        except FoundryManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        return manifest
    if manifest.get("campaign_mode") == "MICROSTRUCTURE_SPARSE_ALPHA_DISTILLATION":
        from hydra.production.microstructure_sparse_manifest import (
            SparseManifestError,
            validate_microstructure_sparse_manifest,
        )

        try:
            validate_microstructure_sparse_manifest(manifest, manifest_path=resolved)
        except SparseManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        return manifest
    if manifest.get("campaign_mode") == "HYBRID_STRUCTURAL_ALPHA_ORDER_FLOW":
        from hydra.production.microstructure_hybrid_manifest import (
            HybridManifestError,
            validate_microstructure_hybrid_manifest,
        )

        try:
            validate_microstructure_hybrid_manifest(
                manifest, manifest_path=resolved
            )
        except HybridManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        return manifest
    if manifest.get("campaign_mode") == "SELECTIVE_ORDER_FLOW_VETO_EXPANSION":
        from hydra.production.selective_veto_manifest import (
            SelectiveVetoManifestError,
            validate_selective_veto_manifest,
        )

        try:
            validate_selective_veto_manifest(manifest, manifest_path=resolved)
        except SelectiveVetoManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        return manifest
    if manifest.get("campaign_mode") == "ACTIVE_RISK_POOL":
        from hydra.production.active_risk_manifest import (
            ActiveRiskManifestError,
            validate_active_risk_manifest,
        )

        try:
            validate_active_risk_manifest(manifest, manifest_path=resolved)
        except ActiveRiskManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        _validate_component_bank(manifest, resolved.parents[2])
        return manifest
    if manifest.get("campaign_mode") == "PORTFOLIO_FIRST":
        from hydra.production.portfolio_manifest import (
            PortfolioManifestError,
            validate_portfolio_manifest,
        )

        try:
            validate_portfolio_manifest(manifest, manifest_path=resolved)
        except PortfolioManifestError as exc:
            raise ProductionManifestError(str(exc)) from exc
        # Portfolio-first reuses the exact 0024 immutable component source.
        # The specialized sleeve contract does not replace verification of the
        # authoritative component-bank files and their campaign provenance.
        _validate_component_bank(manifest, resolved.parents[2])
        return manifest
    campaign_id = str(manifest.get("campaign_id") or "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", campaign_id):
        raise ProductionManifestError("unsafe or empty production campaign identity")
    if manifest.get("development_only") is not True:
        raise ProductionManifestError("production evidence must remain development-only")
    if not str(manifest.get("economic_hypothesis") or "").strip():
        raise ProductionManifestError("economic hypothesis is required")
    implementation = _mapping(manifest, "implementation_files")
    if not implementation:
        raise ProductionManifestError("implementation file checksums are required")
    for relative, claimed_sha in implementation.items():
        target = (resolved.parents[2] / str(relative)).resolve()
        if resolved.parents[2] not in target.parents or _sha256(target) != str(claimed_sha):
            raise ProductionManifestError(f"production implementation checksum drift: {relative}")

    runtime = _mapping(manifest, "runtime")
    if (
        runtime.get("engine") != PRODUCTION_ENGINE
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("result_schema") != PRODUCTION_RESULT_SCHEMA
        or runtime.get("controller_source_change_required") is not False
        or int(runtime.get("worker_count", 0)) != 3
        or int(runtime.get("asynchronous_evidence_writer_count", 0)) != 1
        or runtime.get("resume_from_checkpoint") is not True
    ):
        raise ProductionManifestError("invalid stable production runtime declaration")

    governance = _mapping(manifest, "governance")
    forbidden_true = (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "proof_window_consumption_allowed",
        "status_inheritance_allowed",
    )
    if any(governance.get(key) is not False for key in forbidden_true):
        raise ProductionManifestError("unsafe production governance declaration")

    allocation = _mapping(manifest, "compute_allocation")
    hot = float(allocation.get("hot_economic_loop_minimum", -1.0))
    cold = float(allocation.get("cold_safety_loop_maximum", 2.0))
    engineering = float(allocation.get("engineering_reporting_maximum", 2.0))
    if hot < 0.80 or cold > 0.10 or engineering > 0.10:
        raise ProductionManifestError("production compute allocation violates 80/10/10")

    classes = tuple(str(value) for value in manifest.get("policy_classes") or ())
    if set(classes) != POLICY_CLASSES or len(classes) != len(POLICY_CLASSES):
        raise ProductionManifestError("production policy classes are incomplete")
    if any(any(token in value for token in FORBIDDEN_REUSE_CLASSES) for value in classes):
        raise ProductionManifestError("tombstoned or ratchet class reused")

    _validate_campaign_declaration(manifest)

    _validate_successive_halving(_mapping(manifest, "successive_halving"))
    _validate_component_bank(manifest, resolved.parents[2])
    _validate_evidence_contract(_mapping(manifest, "evidence_bundle"))

    risk = _mapping(manifest, "static_risk_frontier")
    levels = tuple(float(value) for value in risk.get("normalized_levels") or ())
    units = tuple(int(value) for value in risk.get("micro_units") or ())
    if levels != (0.75, 1.0, 1.25, 1.5) or units != (3, 4, 5, 6):
        raise ProductionManifestError("static risk frontier drift")
    if risk.get("continuous_optimization") is not False:
        raise ProductionManifestError("continuous sizing is forbidden")

    temporal = _mapping(manifest, "temporal_blocks")
    blocks = list(temporal.get("blocks") or [])
    if len(blocks) < 4 or len({str(row.get("block_id")) for row in blocks}) != len(blocks):
        raise ProductionManifestError("at least four distinct temporal blocks are required")
    if temporal.get("overlapping_starts_independent") is not False:
        raise ProductionManifestError("overlapping starts cannot be independent")

    return manifest


def verify_runtime_inputs(
    manifest: Mapping[str, Any],
    *,
    contract_map_path: str | Path,
    feature_source_fingerprint: str,
) -> None:
    data = _mapping(manifest, "data")
    contract_path = Path(contract_map_path).resolve()
    if _sha256(contract_path) != str(data.get("contract_map_sha256") or ""):
        raise ProductionManifestError("contract-map checksum drift")
    if feature_source_fingerprint != str(data.get("feature_source_fingerprint") or ""):
        raise ProductionManifestError("feature source fingerprint drift")
    if data.get("role") != "DEVELOPMENT_ONLY_Q4_EXCLUDED":
        raise ProductionManifestError("production data role drift")


def _validate_successive_halving(value: Mapping[str, Any]) -> None:
    expected = {
        "stage0_proposals": 20_000,
        "stage1_fast_screen_minimum": 4_096,
        "stage1_survivor_maximum": 1_024,
        "stage2_exact_replay_minimum": 512,
        "stage2_survivor_maximum": 256,
        "stage3_rolling_combine_maximum": 256,
        "stage3_survivor_maximum": 64,
        "stage4_crossfit_maximum": 64,
        "stage4_survivor_maximum": 16,
        "stage5_96_start_maximum": 16,
        "stage5_survivor_maximum": 4,
        "stage6_finalist_maximum": 4,
    }
    for key, minimum in expected.items():
        actual = int(value.get(key, -1))
        if key.endswith("minimum") or key == "stage0_proposals":
            if actual < minimum:
                raise ProductionManifestError(f"halving target too small: {key}")
        elif actual > minimum or actual < 1:
            raise ProductionManifestError(f"halving cap drift: {key}")
    if int(value.get("rolling_start_count", -1)) != 48:
        raise ProductionManifestError("rolling Combine start count drift")
    if tuple(int(row) for row in value.get("frozen_horizons") or ()) != (
        20,
        40,
        60,
        90,
    ):
        raise ProductionManifestError("frozen horizon policy drift")
    if float(value.get("stress_cost_multiplier", 0.0)) != 1.5:
        raise ProductionManifestError("stress cost multiplier drift")


def _validate_component_bank(manifest: Mapping[str, Any], root: Path) -> None:
    bank = _mapping(manifest, "component_bank")
    sources = _mapping(bank, "sources")
    for name, reference in sources.items():
        if not isinstance(reference, Mapping):
            raise ProductionManifestError(f"invalid component source: {name}")
        relative = str(reference.get("path") or "")
        path = (root / relative).resolve()
        if root != path and root not in path.parents:
            raise ProductionManifestError("component source escapes project root")
        if _sha256(path) != str(reference.get("file_sha256") or ""):
            raise ProductionManifestError(f"component source checksum drift: {name}")

    elite_source = _load_json(root / str(sources["canonical_0018_elites"]["path"]))
    policy_by_id = {
        str(row["policy_id"]): row for row in elite_source.get("policies") or []
    }
    passing = tuple(str(value) for value in bank.get("primary_passing_policy_ids") or ())
    near = tuple(str(value) for value in bank.get("primary_near_elite_policy_ids") or ())
    if len(passing) != 4 or set(passing) != set(elite_source.get("passing_policy_ids") or ()):
        raise ProductionManifestError("0024 must retain the four exact 0018 passers")
    if len(near) != 4 or not set(near).issubset(
        set(elite_source.get("near_pass_policy_ids") or ())
    ):
        raise ProductionManifestError("0024 near-elite bank drift")
    selected = passing + near
    if len(set(selected)) != 8 or any(value not in policy_by_id for value in selected):
        raise ProductionManifestError("primary policy identity drift")
    behaviors = {str(policy_by_id[value].get("behavioral_fingerprint") or "") for value in selected}
    evidence_hashes = {str(policy_by_id[value].get("episode_evidence_hash") or "") for value in selected}
    if len(behaviors) != 8 or "" in behaviors or len(evidence_hashes) != 8 or "" in evidence_hashes:
        raise ProductionManifestError("primary policies lack distinct complete episode evidence")

    fallback = _mapping(bank, "diagnostic_fallback_0020")
    if (
        fallback.get("status") != "DIAGNOSTIC_FALLBACK_ONLY"
        or fallback.get("status_inheritance") is not False
        or not str(fallback.get("policy_id") or "").startswith("elite_robustness_child_")
    ):
        raise ProductionManifestError("0020 diagnostic fallback drift")
    fallback_components = tuple(str(row) for row in fallback.get("component_ids") or ())
    if not fallback_components or not set(fallback_components).issubset(
        {row for policy in policy_by_id.values() for row in policy.get("component_ids") or ()}
        | {
            str(row.get("specification", {}).get("sleeve_id") or "")
            for row in _load_json(root / str(sources["seed_archive"]["path"])).get("sleeves") or ()
        }
    ):
        raise ProductionManifestError("0020 fallback component bank is not executable")
    baseline = _mapping(bank, "campaign_0023_baseline")
    if (
        baseline.get("role") != "BASELINE_ONLY"
        or baseline.get("family_verdict") != "STATIC_PARENT_SYNTHESIS_FALSIFIED"
        or baseline.get("promotion_status") is not None
    ):
        raise ProductionManifestError("0023 falsified family escaped baseline-only role")
    recovery = _load_json(root / str(sources["campaign_0023_recovery_failure"]["path"]))
    if (
        recovery.get("evidence_status") != "SUMMARY_ONLY_NONCONFIRMATORY"
        or recovery.get("nested_selector_0023_allowed") is not False
        or recovery.get("retry_allowed") is not False
        or recovery.get("historical_summaries_preserved") is not True
    ):
        raise ProductionManifestError("campaign 0023 recovery failure provenance drift")


def _validate_evidence_contract(value: Mapping[str, Any]) -> None:
    datasets = set(str(row) for row in value.get("required_datasets") or ())
    if datasets != set(REQUIRED_DATASETS):
        raise ProductionManifestError("EvidenceBundle dataset declaration is incomplete")
    if (
        value.get("required_for_campaign_complete") is not True
        or value.get("atomic_finalize") is not True
        or value.get("summary_only_complete_allowed") is not False
        or value.get("large_files_git_tracked") is not False
        or str(value.get("destination") or "") != "data/cache/evidence_bundles"
        or not str(value.get("lightweight_manifest_path") or "").startswith(
            "reports/economic_evolution/"
        )
        or not str(value.get("lightweight_manifest_path") or "").endswith(
            "/evidence_bundle_receipt.json"
        )
    ):
        raise ProductionManifestError("EvidenceBundle completion contract drift")


def _validate_campaign_declaration(manifest: Mapping[str, Any]) -> None:
    generator = _mapping(manifest, "generator")
    if (
        generator.get("engine") != "deterministic_multi_mechanism_policy_factory_v1"
        or int(generator.get("proposal_count", 0)) < 20_000
        or generator.get("structural_deduplication") is not True
        or generator.get("behavioral_deduplication") is not True
        or generator.get("cemetery_check") is not True
        or generator.get("cemetery_check_scope")
        != "CLASS_NAME_ONLY_NO_STRUCTURAL_FINGERPRINT_MATCH"
        or generator.get("neighbour_resurrection_0023") is not False
    ):
        raise ProductionManifestError("production generator declaration drift")
    markets = tuple(str(row) for row in manifest.get("markets") or ())
    if set(markets) != {"CL", "ES", "NQ", "RTY", "YM"}:
        raise ProductionManifestError("production market declaration drift")
    if _mapping(_mapping(manifest, "component_bank"), "market_exclusions").get("GC") != (
        "NO_ELIGIBLE_COMPONENT_IN_FROZEN_0024_BANK"
    ):
        raise ProductionManifestError("scientific GC exclusion is not explicit")
    contracts = _mapping(manifest, "contracts")
    if set(contracts) != set(markets) or any(
        not isinstance(contracts[market], Mapping)
        or not str(contracts[market].get("mini") or "")
        or not str(contracts[market].get("micro") or "")
        or float(contracts[market].get("micro_per_mini", 0.0)) <= 0.0
        for market in markets
    ):
        raise ProductionManifestError("mini/micro contract equivalence is incomplete")
    timeframes = set(str(row) for row in manifest.get("timeframes") or ())
    if not {"5m", "15m", "30m", "60m"}.issubset(timeframes):
        raise ProductionManifestError("production timeframe declaration drift")
    sessions = _mapping(manifest, "session_rules")
    if (
        sessions.get("source") != "FROZEN_COMPONENT_SESSION_CODE"
        or sessions.get("same_session_enforcement") is not True
        or sessions.get("overnight_fabrication_allowed") is not False
    ):
        raise ProductionManifestError("production session rules drift")
    starts = _mapping(manifest, "episode_starts")
    if (
        int(starts.get("serious_policy_starts", 0)) != 48
        or starts.get("block_aware") is not True
        or starts.get("overlapping_starts_independent") is not False
        or starts.get("retuning_after_start_outcomes") is not False
    ):
        raise ProductionManifestError("episode start policy drift")
    costs = _mapping(manifest, "costs")
    if (
        float(costs.get("normal_multiplier", 0.0)) != 1.0
        or float(costs.get("stressed_multiplier", 0.0)) != 1.5
        or costs.get("source_component_costs_frozen") is not True
    ):
        raise ProductionManifestError("production cost scenarios drift")
    account = _mapping(manifest, "account_parameters")
    if (
        float(account.get("starting_balance", 0.0)) != 150_000.0
        or float(account.get("profit_target", 0.0)) != 9_000.0
        or float(account.get("maximum_loss_limit", 0.0)) != 4_500.0
        or int(account.get("maximum_mini_equivalent", 0)) != 15
        or account.get("dynamic_loss_streak_ratchet") is not False
    ):
        raise ProductionManifestError("production account parameters drift")
    controls = _mapping(manifest, "matched_controls")
    if (
        controls.get("best_parent") is not True
        or controls.get("equal_risk") is not True
        or int(controls.get("random_seed_count", 0)) < 1
        or controls.get("campaign_0023_static_baseline_only") is not True
    ):
        raise ProductionManifestError("matched-control declaration drift")
    null_policy = _mapping(manifest, "null_policy")
    if (
        null_policy.get("definition") != "MATCHED_STRUCTURE_FIXED_SEED_ROUTING_NULL"
        or null_policy.get("reuse_identical_definition") is not True
        or null_policy.get("family_average_p_value_primary") is not False
    ):
        raise ProductionManifestError("null policy drift")
    screen = _mapping(manifest, "screening_policy")
    if (
        screen.get("stage1_is_approximate") is not True
        or screen.get("stage1_outcome_claim_allowed") is not False
        or screen.get("pareto_ranking") is not True
        or screen.get("opaque_score") is not False
    ):
        raise ProductionManifestError("screening policy drift")
    mutation = _mapping(manifest, "mutation_policy")
    if (
        mutation.get("failure_guided_only") is not True
        or mutation.get("blind_whole_policy_mutation") is not False
        or mutation.get("identical_start_parent_child_delta") is not True
        or mutation.get("campaign_0023_neighbour_variants") is not False
    ):
        raise ProductionManifestError("mutation policy drift")
    kill = _mapping(manifest, "kill_policy")
    if kill.get("tombstone_exact_class") is not True or kill.get(
        "lower_threshold_after_result"
    ) is not False:
        raise ProductionManifestError("kill policy drift")
    promotion = _mapping(manifest, "promotion_policy")
    if (
        promotion.get("development_only") is not True
        or promotion.get("paper_shadow_ready_allowed") is not False
        or promotion.get("independent_confirmation_required") is not True
        or promotion.get("status_inheritance") is not False
    ):
        raise ProductionManifestError("promotion policy drift")

    materialization = _mapping(manifest, "component_materialization")
    if (
        tuple(materialization.get("calibration_period") or ()) != ("2023-01-01", "2023-07-01")
        or tuple(materialization.get("exact_replay_period") or ()) != ("2023-01-01", "2024-10-01")
        or materialization.get("cached_features_only") is not True
        or materialization.get("cache_miss_allowed") is not False
    ):
        raise ProductionManifestError("component materialization policy drift")


def freeze_production_manifest(
    path: str | Path,
    *,
    source_commit: str,
) -> dict[str, Any]:
    """Freeze the deployment commit and semantic manifest hash in-place."""

    if len(source_commit) not in {40, 64} or any(ch not in "0123456789abcdef" for ch in source_commit):
        raise ProductionManifestError("source_commit must be a full hexadecimal commit")
    target = Path(path).resolve()
    manifest = _load_json(target)
    manifest["source_commit"] = source_commit
    root = target.parents[2]
    implementation = manifest.get("implementation_files")
    if not isinstance(implementation, Mapping) or not implementation:
        raise ProductionManifestError("implementation_files must be declared before freeze")
    manifest["implementation_files"] = {
        str(relative): _sha256(root / str(relative))
        for relative in implementation
    }
    payload = dict(manifest)
    payload.pop("manifest_hash", None)
    manifest["manifest_hash"] = stable_hash(payload)
    target.write_text(
        json.dumps(manifest, indent=2, sort_keys=False, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def _mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    row = value.get(key)
    if not isinstance(row, Mapping):
        raise ProductionManifestError(f"missing manifest object: {key}")
    return row


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionManifestError(f"cannot load production source: {path}") from exc
    if not isinstance(value, dict):
        raise ProductionManifestError(f"expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ProductionManifestError(f"missing production source: {path}") from exc
    return digest.hexdigest()


__all__ = [
    "FORBIDDEN_REUSE_CLASSES",
    "POLICY_CLASSES",
    "PRODUCTION_ENGINE",
    "PRODUCTION_MANIFEST_SCHEMA",
    "PRODUCTION_RESULT_SCHEMA",
    "ProductionManifestError",
    "load_and_validate_production_manifest",
    "verify_runtime_inputs",
    "freeze_production_manifest",
]
