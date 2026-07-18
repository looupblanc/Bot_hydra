"""Fail-closed manifest contract for HYDRA campaign 0031."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


CAMPAIGN_MODE = "MICROSTRUCTURE_ORDER_FLOW_FOUNDRY"
CAMPAIGN_ID = "hydra_microstructure_order_flow_foundry_0031"
CLASS_ID = "EVENT_SOURCED_LOB_TEACHER_STUDENT_MOE_FAST_COMBINE_V1"
RUNTIME_VERSION = "hydra_microstructure_order_flow_foundry_runtime_v1"
EXPERT_FAMILIES = (
    "ABSORPTION_REVERSAL",
    "INITIATIVE_CONTINUATION",
    "LIQUIDITY_VACUUM_CONTINUATION",
    "EXHAUSTION_REVERSAL",
    "VWAP_ACCEPTANCE_REJECTION",
    "OPENING_DRIVE",
    "CROSS_ASSET_FLOW_DIVERGENCE",
    "QUEUE_REPLENISHMENT",
)
DEPLOYABILITY = (
    "L1_DEPLOYABLE",
    "L2_DEPLOYABLE",
    "MBO_TEACHER_ONLY",
    "UNDEPLOYABLE",
)
SCHEMAS = ("trades", "tbbo", "mbp-1", "mbp-10", "mbo")
SESSION_COUNTS = (5, 10, 20, 30)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")


class FoundryManifestError(RuntimeError):
    """Campaign 0031 is incomplete, unsafe, or has drifted."""


def validate_microstructure_foundry_manifest(
    manifest: Mapping[str, Any], *, manifest_path: str | Path
) -> None:
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    _identity(manifest)
    _implementation(manifest, root)
    _runtime(manifest, root)
    _baseline(manifest, root)
    _market_selection(manifest)
    _account_rules(manifest, root)
    _acquisition(manifest)
    _planes_and_engine(manifest)
    _research_contract(manifest)
    _pilot(manifest)
    _evidence_and_governance(manifest, root)


def _identity(manifest: Mapping[str, Any]) -> None:
    try:
        created = datetime.fromisoformat(
            str(manifest.get("created_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise FoundryManifestError("0031 freeze timestamp is invalid") from exc
    if (
        manifest.get("schema") != "hydra_economic_production_manifest_v1"
        or manifest.get("campaign_mode") != CAMPAIGN_MODE
        or manifest.get("campaign_id") != CAMPAIGN_ID
        or manifest.get("class_id") != CLASS_ID
        or tuple(manifest.get("policy_classes") or ()) != (CLASS_ID,)
        or manifest.get("development_only") is not True
        or created.tzinfo is None
        or not _GIT_SHA.fullmatch(str(manifest.get("source_commit") or ""))
        or not str(manifest.get("economic_hypothesis") or "").strip()
    ):
        raise FoundryManifestError("0031 identity drift")


def _implementation(manifest: Mapping[str, Any], root: Path) -> None:
    required = {
        "hydra/production/microstructure_foundry_manifest.py",
        "hydra/production/microstructure_foundry_runtime.py",
        "hydra/production/microstructure_foundry_cost.py",
        "hydra/production/microstructure_event_engine.py",
        "hydra/production/microstructure_teacher_student.py",
        "hydra/production/microstructure_foundry_pilot.py",
        "scripts/acquire_microstructure_foundry_0031.py",
    }
    files = _mapping(manifest, "implementation_files")
    if not required <= set(str(key) for key in files):
        raise FoundryManifestError("0031 implementation closure is incomplete")
    for relative, claimed in files.items():
        target = (root / str(relative)).resolve()
        if (
            root not in target.parents
            or not _SHA256.fullmatch(str(claimed or ""))
            or _sha256(target) != claimed
        ):
            raise FoundryManifestError(f"0031 implementation checksum drift: {relative}")


def _runtime(manifest: Mapping[str, Any], root: Path) -> None:
    runtime = _mapping(manifest, "runtime")
    output = (root / str(runtime.get("output_dir") or "")).resolve()
    allowed = (root / "reports/economic_evolution").resolve()
    if (
        runtime.get("engine") != "production_kernel_v1"
        or runtime.get("runner") != "scripts/run_economic_production_manifest.py"
        or runtime.get("foundry_runtime_version") != RUNTIME_VERSION
        or runtime.get("controller_source_change_required") is not False
        or int(runtime.get("authoritative_writer_count", -1)) != 1
        or int(runtime.get("worker_count", -1)) != 3
        or runtime.get("resume_from_checkpoint") is not True
        or output == allowed
        or allowed not in output.parents
    ):
        raise FoundryManifestError("0031 runtime contract drift")


def _baseline(manifest: Mapping[str, Any], root: Path) -> None:
    baseline = _mapping(manifest, "terminal_baseline_0029")
    expected = {
        "terminal_status": "FAST_PASS_FACTORY_DATA_REPRESENTATION_LIMITED",
        "proposals": 100_000,
        "causal_screens": 20_000,
        "exact_replays": 5_000,
        "account_books": 2_000,
        "account_episodes": 553_674,
        "stressed_positive_candidates": 2_799,
        "combine_passes_5d": 0,
        "combine_passes_10d": 0,
        "combine_passes_20d": 0,
        "mll_breaches": 0,
        "bank_admissions": 0,
        "graduations": 0,
        "xfa_paths": 0,
    }
    if any(baseline.get(key) != value for key, value in expected.items()):
        raise FoundryManifestError("0031 terminal 0029 baseline drift")
    if baseline.get("regenerate_evidence_allowed") is not False:
        raise FoundryManifestError("0029 regeneration is forbidden")
    sources = _mapping(baseline, "sources")
    if not sources:
        raise FoundryManifestError("0029 source receipts are absent")
    for label, raw in sources.items():
        item = _mapping(sources, label)
        target = (root / str(item.get("path") or "")).resolve()
        if root not in target.parents or _sha256(target) != item.get("sha256"):
            raise FoundryManifestError(f"0029 source checksum drift: {label}")


def _market_selection(manifest: Mapping[str, Any]) -> None:
    selection = _mapping(manifest, "market_selection")
    rows = tuple(selection.get("ranked_clean_0029_evidence") or ())
    selected = tuple(str(value) for value in selection.get("selected_markets") or ())
    contracts = tuple(str(value) for value in selection.get("explicit_contracts") or ())
    by_market = {str(row.get("market")): row for row in rows if isinstance(row, Mapping)}
    if (
        selected != ("NQ", "YM")
        or contracts != ("NQU4", "YMU4")
        or not {"NQ", "YM", "ES"} <= set(by_market)
        or selection.get("default_nq_es_displaced_by_evidence") is not True
        or float(by_market["NQ"].get("stressed_net_usd", 0.0))
        <= float(by_market["ES"].get("stressed_net_usd", 0.0))
        or float(by_market["YM"].get("opportunities_per_20_sessions", 0.0))
        <= float(by_market["ES"].get("opportunities_per_20_sessions", 0.0))
    ):
        raise FoundryManifestError("0031 two-market selection drift")


def _account_rules(manifest: Mapping[str, Any], root: Path) -> None:
    rules = _mapping(manifest, "account_rule_snapshot")
    costs = _mapping(rules, "costs_and_slippage")
    source = (root / str(costs.get("source_path") or "")).resolve()
    if (
        float(rules.get("account_size_usd", -1.0)) != 150_000.0
        or float(rules.get("profit_target_usd", -1.0)) != 9_000.0
        or float(rules.get("maximum_loss_limit_usd", -1.0)) != 4_500.0
        or float(rules.get("best_day_consistency_fraction", -1.0)) != 0.50
        or int(rules.get("maximum_mini_contracts", -1)) != 15
        or int(rules.get("maximum_micro_contracts", -1)) != 150
        or rules.get("session_close_rule") != "FROZEN_TOPSTEP_SESSION_FLATTEN"
        or rules.get("derive_risk_limits_from_snapshot") is not True
        or costs.get("causal_next_tradable_event_fill") is not True
        or float(costs.get("normal_multiplier", -1.0)) != 1.0
        or float(costs.get("stressed_multiplier", -1.0)) != 1.5
        or root not in source.parents
        or _sha256(source) != costs.get("source_sha256")
    ):
        raise FoundryManifestError("0031 account rule/cost snapshot drift")


def _acquisition(manifest: Mapping[str, Any]) -> None:
    value = _mapping(manifest, "bounded_acquisition")
    if (
        value.get("provider") != "Databento"
        or value.get("dataset") != "GLBX.MDP3"
        or tuple(value.get("schemas") or ()) != SCHEMAS
        or tuple(int(v) for v in value.get("session_counts") or ()) != SESSION_COUNTS
        or value.get("cost_api") != "Historical.metadata.get_cost"
        or float(value.get("maximum_initial_spend_usd", -1.0)) != 10.0
        or float(value.get("minimum_budget_reserve_usd", -1.0)) != 25.0
        or float(value.get("total_budget_usd", -1.0)) != 125.0
        or value.get("purchase_before_cost_matrix_allowed") is not False
        or value.get("broad_historical_purchase_allowed") is not False
        or value.get("q4_access_allowed") is not False
        or value.get("temporal_split") != {"discovery": 0.6, "validation": 0.2, "final_development": 0.2}
    ):
        raise FoundryManifestError("0031 bounded acquisition drift")


def _planes_and_engine(manifest: Mapping[str, Any]) -> None:
    planes = _mapping(manifest, "dual_plane")
    engine = _mapping(manifest, "event_state_engine")
    store = _mapping(manifest, "immutable_event_store")
    if (
        tuple(planes.get("deployability_statuses") or ()) != DEPLOYABILITY
        or planes.get("server_research_allowed") is not True
        or planes.get("personal_device_export_required") is not True
        or planes.get("databento_live_order_dependency_allowed") is not False
        or planes.get("vps_order_execution_allowed") is not False
        or engine.get("single_batch_stream_step") is not True
        or engine.get("sequence_and_snapshot_recovery_required") is not True
        or engine.get("economic_results_before_reconstruction_green_allowed") is not False
        or tuple(store.get("layers") or ())
        != ("RAW_DBN", "BOOK_SNAPSHOTS", "DERIVED_EVENTS", "FEATURE_MATRICES", "OUTCOME_LABELS")
        or store.get("outcome_labels_physically_separate") is not True
        or store.get("raw_rewrite_allowed") is not False
    ):
        raise FoundryManifestError("0031 dual-plane/event-store drift")


def _research_contract(manifest: Mapping[str, Any]) -> None:
    lattice = _mapping(manifest, "feature_lattice")
    teacher = _mapping(manifest, "teacher_student")
    experts = _mapping(manifest, "expert_families")
    execution = _mapping(manifest, "execution_replay")
    if (
        tuple(lattice.get("horizons") or ()) != ("MICRO", "SHORT", "MESO", "SESSION")
        or lattice.get("availability_rule") != "available_at<=decision_time"
        or lattice.get("normalization_by_market_session_volatility_spread_depth") is not True
        or teacher.get("mbo_labels_outcome_only") is not True
        or teacher.get("mbo_teacher_direct_deployment_allowed") is not False
        or tuple(teacher.get("student_tiers") or ()) != ("L1", "L2")
        or tuple(experts.get("required_initial_families") or ()) != EXPERT_FAMILIES
        or int(experts.get("minimum_structurally_distinct_sleeves", -1)) != 20
        or int(experts.get("maximum_structurally_distinct_sleeves", -1)) != 40
        or tuple(execution.get("paths") or ()) != ("AGGRESSIVE", "PASSIVE")
        or execution.get("touch_implies_fill") is not False
        or execution.get("sub_millisecond_latency_arbitrage_allowed") is not False
    ):
        raise FoundryManifestError("0031 feature/teacher/expert/execution drift")


def _pilot(manifest: Mapping[str, Any]) -> None:
    pilot = _mapping(manifest, "bounded_pilot")
    gate = _mapping(pilot, "green_gate")
    if (
        int(pilot.get("minimum_candidates", -1)) != 20
        or int(pilot.get("maximum_candidates", -1)) != 40
        or tuple(pilot.get("horizons_days") or ()) != (5, 10, 20)
        or tuple(pilot.get("cost_scenarios") or ()) != ("NORMAL", "STRESSED_1_5X")
        or tuple(pilot.get("controls") or ())
        != ("DIRECTION_FLIP", "SESSION_MATCHED_TIMING_NULL", "EXPOSURE_MATCHED_RANDOM")
        or tuple(pilot.get("allowed_decisions") or ())
        != ("MICROSTRUCTURE_PILOT_GREEN", "MICROSTRUCTURE_PILOT_WEAK", "MICROSTRUCTURE_PILOT_FALSIFIED")
        or gate.get("material_target_velocity_uplift_over_ohlcv") is not True
        or gate.get("positive_stressed_economics") is not True
        or int(gate.get("minimum_useful_mechanism_families", -1)) != 3
        or gate.get("minimum_one_l1_or_l2_serious_sleeve") is not True
        or gate.get("final_development_evidence_required") is not True
        or gate.get("thresholds_may_change_after_results") is not False
    ):
        raise FoundryManifestError("0031 bounded pilot/gate drift")


def _evidence_and_governance(manifest: Mapping[str, Any], root: Path) -> None:
    evidence = _mapping(manifest, "evidence_bundle")
    governance = _mapping(manifest, "governance")
    destination = (root / str(evidence.get("destination") or "")).resolve()
    if (
        evidence.get("required") is not True
        or evidence.get("atomic_single_writer_finalization") is not True
        or root not in destination.parents
        or governance.get("live_trading_allowed") is not False
        or governance.get("broker_connection_allowed") is not False
        or governance.get("orders_allowed") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("xfa_before_clean_combine_survivors") is not False
        or governance.get("mass_scale_before_pilot_green") is not False
    ):
        raise FoundryManifestError("0031 evidence/governance drift")


def _mapping(parent: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        raise FoundryManifestError(f"0031 mapping absent: {key}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
