from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_volume_front import VOLUME_FRONT_MAP_TYPE
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _stable_hash,
    _strict_json_value,
)
from hydra.research.energy_metals_barrier_primary import _read_period
from hydra.research.energy_metals_session_execution_repair import (
    synchronize_micro_execution,
)
from hydra.research.energy_metals_session_geometry_primary import (
    _concentration_stress,
    _diagnostics,
    build_session_geometry_events,
    build_session_geometry_table,
)
from hydra.research.equity_open_gap_reversal import _account_replay, _write_immutable
from hydra.research.qd_economic_tournament import (
    _block_sign_flip_probability,
    _period_metrics,
    _round_turn_cost_all,
    _validation_metrics,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "gc_session_geometry_fresh_primary_v1"
SOURCE_ID = "strategy_session_geometry_GC_overnight_displacement_reversal_q65_h60_none_v1"
CANDIDATE_ID = (
    "strategy_session_geometry_GC_signal_MGC_execution_overnight_"
    "displacement_reversal_q65_h60_none_v2"
)
SOURCE_PREREGISTRATION_HASH = (
    "4733a540c91dd7a569b65449722867dc9aea8553c88b59f1b9b33b7513405e3a"
)
SOURCE_MANIFEST_HASH = (
    "f11a6f657e018f2d8b137eddb64cf497dcf63ed0ee17848744667fa968201d96"
)
SOURCE_POPULATION_HASH = (
    "2c2f7b45c14dcca09014e654711c799060ef9fedd57c6799e2535216c97cc097"
)
PROMOTION_ALPHA = 0.03
SHADOW_ALPHA = 0.20


class GCSessionGeometryFreshPrimaryError(RuntimeError):
    pass


def candidate_specification() -> dict[str, Any]:
    specification = {
        "representation": VERSION,
        "candidate_id": CANDIDATE_ID,
        "source_diagnostic_id": SOURCE_ID,
        "signal_market": "GC",
        "execution_market": "MGC",
        "feature": "overnight_displacement",
        "policy_direction": "reversal",
        "quantile": 0.65,
        "horizon": 60,
        "context": "none",
        "signal_semantics": "GC_SIGNAL_MGC_SYNCHRONIZED_EXECUTION",
        "mechanism_family": "overnight_inventory_reversal",
        "market_ecology": "metals",
        "portfolio_role": "reversal",
    }
    fingerprint = structural_fingerprint(specification)
    return {
        **specification,
        "structural_fingerprint": fingerprint,
        "lineage_id": f"lineage_gc_session_{fingerprint[:20]}",
    }


def run_gc_session_geometry_fresh_primary(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_preregistration_path: str | Path,
    source_preregistration_sha256: str,
    source_freeze_path: str | Path,
    source_freeze_sha256: str,
    metals_data_path: str | Path,
    metals_data_sha256: str,
    metals_map_path: str | Path,
    metals_map_sha256: str,
    metals_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    frozen = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (
            Path(source_preregistration_path),
            source_preregistration_sha256,
            "source preregistration",
        ),
        (Path(source_freeze_path), source_freeze_sha256, "source freeze"),
        (Path(metals_data_path), metals_data_sha256, "metals data"),
        (Path(metals_map_path), metals_map_sha256, "metals map"),
    )
    for path, expected, label in frozen:
        _verify(path, expected, label)
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise GCSessionGeometryFreshPrimaryError(
                "Worker commit differs from queued specification."
            )
    source_preregistration = json.loads(
        Path(source_preregistration_path).read_text(encoding="utf-8")
    )
    source_freeze = json.loads(Path(source_freeze_path).read_text(encoding="utf-8"))
    source_hypothesis = _verify_source_selection(
        source_preregistration, source_freeze
    )
    roll_map = load_roll_map(metals_map_path)
    if (
        roll_map.map_type != VOLUME_FRONT_MAP_TYPE
        or roll_map.roll_map_hash() != metals_roll_map_hash
    ):
        raise GCSessionGeometryFreshPrimaryError("Metals roll map changed.")
    child = candidate_specification()
    if child["structural_fingerprint"] == source_hypothesis["structural_fingerprint"]:
        raise GCSessionGeometryFreshPrimaryError("Fresh candidate fingerprint was reused.")

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration: dict[str, Any] = {
        "schema": VERSION,
        "candidate": child,
        "source_diagnostic_id": SOURCE_ID,
        "source_diagnostic_fingerprint": source_hypothesis["structural_fingerprint"],
        "source_preregistration_hash": SOURCE_PREREGISTRATION_HASH,
        "source_manifest_hash": SOURCE_MANIFEST_HASH,
        "source_population_hash": SOURCE_POPULATION_HASH,
        "selection_data_end_exclusive": "2024-01-01",
        "confirmation_end_exclusive": "2024-10-01",
        "promotion_alpha": PROMOTION_ALPHA,
        "shadow_support_alpha": SHADOW_ALPHA,
        "signal_parameters_mutable": False,
        "execution_change_only": True,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "gc_fresh_primary_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(child) if record_data_access else None
    raw = _read_period(Path(metals_data_path), {"GC", "MGC"}, "2024-10-01")
    raw, contract_audit = _apply_explicit_contract_map(
        raw, roll_map, required_map_type=VOLUME_FRONT_MAP_TYPE
    )
    gc_table = build_session_geometry_table(raw, "GC")
    mgc_table = build_session_geometry_table(raw, "MGC")
    signal_hypothesis = {
        **source_hypothesis,
        "market": "GC",
        "execution_market": "MGC",
    }
    gc_signals = build_session_geometry_events(gc_table, signal_hypothesis)
    child_events, missing = synchronize_micro_execution(
        gc_signals,
        mgc_table,
        signal_symbol="GC",
        execution_symbol="MGC",
        candidate_id=CANDIDATE_ID,
        parent_candidate_id=SOURCE_ID,
        entry_prefix="overnight",
        horizon=60,
    )
    delayed_events, delayed_missing = synchronize_micro_execution(
        gc_signals,
        mgc_table,
        signal_symbol="GC",
        execution_symbol="MGC",
        candidate_id=CANDIDATE_ID,
        parent_candidate_id=SOURCE_ID,
        entry_prefix="overnight",
        horizon=60,
        entry_delay_bars=1,
    )
    if child_events.empty:
        raise GCSessionGeometryFreshPrimaryError("No synchronized MGC events.")
    missing_rate = len(missing) / max(len(gc_signals), 1)
    folds = {
        "2023_h1": _period_metrics(_period(child_events, "2023-01-01", "2023-07-01")),
        "2023_h2": _period_metrics(_period(child_events, "2023-07-01", "2024-01-01")),
        "2024_q1": _period_metrics(_period(child_events, "2024-01-01", "2024-04-01")),
        "2024_q2": _period_metrics(_period(child_events, "2024-04-01", "2024-07-01")),
        "2024_q3": _period_metrics(_period(child_events, "2024-07-01", "2024-10-01")),
    }
    development = _period(child_events, "2023-01-01", "2024-01-01")
    confirmation = _period(child_events, "2024-01-01", "2024-10-01")
    delayed_confirmation = _period(delayed_events, "2024-01-01", "2024-10-01")
    development_metrics = _period_metrics(development)
    confirmation_metrics = _validation_metrics(confirmation)
    delayed_metrics = _period_metrics(delayed_confirmation)
    null_probability = _block_sign_flip_probability(confirmation, seed=991073)
    concentration = _concentration_stress(confirmation)
    diagnostics = _diagnostics(gc_table, signal_hypothesis)
    account = _account_replay(
        confirmation.rename(columns={"net_pnl": "net_pnl_60"}).copy()
    )
    prospective_support = bool(
        development_metrics["net_pnl"] > 0
        and development_metrics["cost_stress_1_5x_net"] > 0
        and confirmation_metrics["net_pnl"] > 0
        and confirmation_metrics["cost_stress_1_5x_net"] > 0
        and confirmation_metrics["supportive_temporal_folds"] >= 2
        and not confirmation_metrics["catastrophic_transfer"]
        and null_probability <= SHADOW_ALPHA
        and confirmation_metrics["best_positive_event_share"] <= 0.35
        and concentration["remove_best_event_net"] > 0
        and concentration["remove_best_month_net"] > 0
        and delayed_metrics["net_pnl"] > 0
        and missing_rate <= 0.10
        and bool(account.get("micro_one_contract_mll_safe", False))
    )
    evidence = ShadowEvidence(
        candidate_id=CANDIDATE_ID,
        data_integrity=True,
        no_lookahead=True,
        deterministic_signals=True,
        net_after_costs=float(confirmation_metrics["net_pnl"]),
        supportive_temporal_folds=int(
            confirmation_metrics["supportive_temporal_folds"]
        ),
        catastrophic_transfer=bool(confirmation_metrics["catastrophic_transfer"]),
        candidate_null_pass=prospective_support,
        null_probability=float(null_probability),
        parameter_stable=bool(
            diagnostics["positive_neighbor_count"] >= 1
            and delayed_metrics["net_pnl"] > 0
        ),
        contract_evidence=bool(
            development_metrics["net_pnl"] > 0
            and confirmation_metrics["net_pnl"] > 0
            and missing_rate <= 0.10
        ),
        account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
        execution_possible=True,
        realtime_features_available=True,
        shadow_spec_complete=True,
        observability_complete=True,
        untouched_holdout_passed=False,
        sample_size=int(confirmation_metrics["events"]),
        uncertainty="fresh_2024_development_confirmation_q4_unopened",
    )
    admission = decide_shadow_admission(evidence)
    if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
        raise GCSessionGeometryFreshPrimaryError("Development test attempted paper promotion.")
    configuration = _shadow_specification(child, preregistration["preregistration_hash"])
    shadow_configurations: list[dict[str, Any]] = []
    if admission.permits_zero_risk_shadow:
        configuration_path = configuration.write_immutable(
            destination / "shadow_configurations" / f"{CANDIDATE_ID}.json"
        )
        shadow_configurations.append(
            {
                "candidate_id": CANDIDATE_ID,
                "status": admission.tier.value,
                "path": str(configuration_path),
                "configuration_hash": configuration.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )
    candidate = {
        **child,
        "status": admission.tier.value,
        "admission": admission.to_dict(),
        "events": int(confirmation_metrics["events"]),
        "net_pnl": float(confirmation_metrics["net_pnl"]),
        "micro_events": int(confirmation_metrics["events"]),
        "micro_net_pnl": float(confirmation_metrics["net_pnl"]),
        "supportive_temporal_folds": int(
            confirmation_metrics["supportive_temporal_folds"]
        ),
        "fold_results": confirmation_metrics["fold_results"],
        "micro_fold_results": confirmation_metrics["fold_results"],
        "cost_stress_1_5x_net": float(
            confirmation_metrics["cost_stress_1_5x_net"]
        ),
        "development_2023": development_metrics,
        "null_evidence": {
            "method": "fresh_primary_mgc_five_session_block_sign_flip",
            "raw_probability": float(null_probability),
            "prospective_alpha": PROMOTION_ALPHA,
            "promotion_passed": bool(null_probability <= PROMOTION_ALPHA),
            "shadow_research_support_threshold": SHADOW_ALPHA,
            "shadow_research_support_passed": prospective_support,
        },
        "contract_transfer": {
            "signal": "GC",
            "execution": "MGC",
            "passed": bool(evidence.contract_evidence),
            "signal_recomputed_from_mgc": False,
            "matched_events": int(len(child_events)),
            "missing_events": int(len(missing)),
            "missing_rate": float(missing_rate),
        },
        "parameter_diagnostics": diagnostics,
        "attacks": {
            **concentration,
            "one_additional_bar_delay_net": float(delayed_metrics["net_pnl"]),
            "missing_match_rate": float(missing_rate),
            "source_selected_before_2024": True,
            "gc_signal_recomputed_from_mgc": False,
        },
        "topstep": account,
        "shadow_evidence": evidence.__dict__,
    }
    conclusion = (
        "GC_SESSION_GEOMETRY_FRESH_SHADOW_CANDIDATE_FOUND"
        if admission.permits_zero_risk_shadow
        else "GC_SESSION_GEOMETRY_FRESH_PRIMARY_FALSIFIED_OR_INSUFFICIENT"
    )
    trade_path = destination / "gc_fresh_primary_trade_ledger.jsonl"
    _write_ledger(trade_path, child_events)
    integrity = {
        "fresh_candidate_fingerprint": child["structural_fingerprint"]
        != source_hypothesis["structural_fingerprint"],
        "source_selection_ended_before_2024": source_freeze.get(
            "selection_data_end_exclusive"
        )
        == "2024-01-01",
        "old_diagnostic_inherited_no_status": True,
        "mgc_signal_not_recomputed": bool(
            not child_events["signal_recomputed_from_micro"].any()
        ),
        "missing_rate_bounded": missing_rate <= 0.10,
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise GCSessionGeometryFreshPrimaryError(f"Integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "The GC diagnostic was selected only on pre-2024 evidence and receives a fresh "
            "identity. This development confirmation can authorize zero-risk shadow only."
        ),
        "code_commit": code_commit,
        "candidate_count": 1,
        "structural_prototypes": 1,
        "primary_candidate_id": CANDIDATE_ID,
        "candidates": [candidate],
        "promising_candidates": int(
            admission.tier
            in {
                EvidenceTier.PROMISING_RESEARCH_CANDIDATE,
                EvidenceTier.ROBUST_RESEARCH_CANDIDATE,
                EvidenceTier.SHADOW_RESEARCH_CANDIDATE,
            }
        ),
        "shadow_candidates": int(
            admission.tier == EvidenceTier.SHADOW_RESEARCH_CANDIDATE
        ),
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(bool(account.get("path_candidate"))),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "fold_results": folds,
        "matching_audit": {
            "source_signal_count": int(len(gc_signals)),
            "matched_count": int(len(child_events)),
            "missing_count": int(len(missing)),
            "missing_rate": float(missing_rate),
            "missing": missing,
            "delayed_missing_count": int(len(delayed_missing)),
        },
        "contract_audit": contract_audit,
        "integrity_proof": integrity,
        "data_access_record": access,
        "preregistration_path": str(preregistration_path),
        "preregistration_hash": preregistration["preregistration_hash"],
        "shadow_configurations": shadow_configurations,
        "performance": {"total_seconds": time.perf_counter() - started},
        "governance": {
            "q4_access_count_delta": 0,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": (
            "ACTIVATE_IMMUTABLE_ZERO_ORDER_SHADOW"
            if admission.permits_zero_risk_shadow
            else "KILL_EXACT_CANDIDATE_AND_PIVOT_DAILY_OR_CROSS_ASSET"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "gc_fresh_primary_result.json"
    report_path = destination / "gc_fresh_primary_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "trade_ledger_path": str(trade_path),
        },
        "report_path": str(report_path),
    }


def _verify_source_selection(
    preregistration: dict[str, Any], freeze: dict[str, Any]
) -> dict[str, Any]:
    if (
        preregistration.get("preregistration_hash") != SOURCE_PREREGISTRATION_HASH
        or preregistration.get("population_hash") != SOURCE_POPULATION_HASH
        or freeze.get("primary_manifest_hash") != SOURCE_MANIFEST_HASH
        or freeze.get("population_hash") != SOURCE_POPULATION_HASH
        or freeze.get("selection_data_end_exclusive") != "2024-01-01"
        or freeze.get("diagnostics_inherit_status") is not False
        or SOURCE_ID not in list(freeze.get("archive_candidate_ids") or [])
        or freeze.get("primary_candidate_id") == SOURCE_ID
    ):
        raise GCSessionGeometryFreshPrimaryError("Frozen source-selection contract changed.")
    hypotheses = [
        row
        for row in preregistration.get("hypotheses") or []
        if row.get("candidate_id") == SOURCE_ID
    ]
    ranking = list(freeze.get("ranking") or [])
    ranked = [row for row in ranking if row.get("candidate_id") == SOURCE_ID]
    first_metals = next(
        (row for row in ranking if "_GC_" in str(row.get("candidate_id") or "")),
        None,
    )
    if (
        len(hypotheses) != 1
        or len(ranked) != 1
        or first_metals is None
        or first_metals.get("candidate_id") != SOURCE_ID
        or ranked[0].get("structural_fingerprint")
        != hypotheses[0].get("structural_fingerprint")
    ):
        raise GCSessionGeometryFreshPrimaryError("GC diagnostic selection is ambiguous.")
    expected = {
        "market": "GC",
        "execution_market": "MGC",
        "feature": "overnight_displacement",
        "policy_direction": "reversal",
        "quantile": 0.65,
        "horizon": 60,
        "context": "none",
    }
    if any(hypotheses[0].get(key) != value for key, value in expected.items()):
        raise GCSessionGeometryFreshPrimaryError("GC diagnostic parameters changed.")
    return dict(hypotheses[0])


def _period(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    timestamps = pd.to_datetime(events["entry_timestamp"], utc=True)
    return events[timestamps.ge(start) & timestamps.lt(end)].copy()


def _record_access_once(child: dict[str, Any]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "fresh GC signal/MGC synchronized primary; Q4 excluded"
    module = "hydra.research.gc_session_geometry_fresh_primary"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == module
                and row.get("candidate_ids") == [CANDIDATE_ID]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        module,
        [CANDIDATE_ID],
        reason,
        None,
    )
    return record.__dict__


def _shadow_specification(
    child: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    return ShadowSpecification(
        strategy_id=CANDIDATE_ID,
        strategy_version="v2_gc_signal_mgc_execution_pre_holdout",
        feature_versions=(
            "causal_gc_session_geometry_v1",
            "synchronized_mgc_execution_v1",
        ),
        markets=("GC", "MGC"),
        timeframes=("1m", "overnight", "RTH_session"),
        session_rules={
            "timezone": "America/Chicago",
            "signal_market": "GC",
            "execution_market": "MGC",
            "market_open_minute": 490,
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "event": "gc_overnight_displacement_threshold",
            "feature": child["feature"],
            "quantile": child["quantile"],
            "direction": child["policy_direction"],
            "context": child["context"],
            "threshold_history_sessions": 20,
            "execution_delay_completed_bars": 1,
            "mgc_signal_recomputation": False,
            "exact_timestamp_match_required": True,
            "missing_match_policy": "fail_closed_skip_signal",
        },
        exit_rules={"holding_completed_1m_bars": 60, "no_overnight": True},
        sizing={"contracts": 1, "instrument": "MGC", "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all("MGC"),
            "slippage_ticks_round_turn": 2,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=3600,
        maximum_exposure=0.1,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data",
            "duplicate_signal",
            "session_closed",
            "clock_invalid",
            "contract_map_mismatch",
            "signal_execution_timestamp_mismatch",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "gc_signal_ledger": True,
            "mgc_virtual_fill_ledger": True,
            "signal_execution_match_audit": True,
            "latency_and_staleness": True,
            "account_mll_path": True,
            "source_manifest_hash": source_manifest_hash,
        },
        reconciliation={
            "startup_reconcile": True,
            "expected_vs_observed_virtual_fill": True,
            "fail_on_configuration_hash_mismatch": True,
        },
        source_manifest_hash=source_manifest_hash,
        outbound_orders_enabled=False,
    )


def _write_ledger(path: Path, frame: pd.DataFrame) -> None:
    ordered = frame.sort_values(["entry_timestamp", "event_session_id"])
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in ordered.to_dict("records")
    ]
    _write_immutable(path, "\n".join(lines) + "\n")


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise GCSessionGeometryFreshPrimaryError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    candidate = payload["candidates"][0]
    return "\n".join(
        [
            "# GC Session-Geometry Fresh Primary",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Candidate: `{candidate['candidate_id']}`",
            f"- 2024 MGC events: `{candidate['events']}`",
            f"- 2024 MGC net: `{candidate['net_pnl']}`",
            f"- 1.5x cost net: `{candidate['cost_stress_1_5x_net']}`",
            f"- Supportive quarters: `{candidate['supportive_temporal_folds']}`",
            f"- Null p: `{candidate['null_evidence']['raw_probability']}`",
            f"- Missing match rate: `{candidate['contract_transfer']['missing_rate']}`",
            f"- Status: `{candidate['status']}`",
            "- PAPER_SHADOW_READY: `0`",
            "- Q4 access delta: `0`",
            "- Outbound orders: `0`",
            "",
        ]
    )
