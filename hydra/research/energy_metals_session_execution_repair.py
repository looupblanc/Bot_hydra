from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.factory.quality_diversity import structural_fingerprint
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    _apply_explicit_contract_map,
    _stable_hash,
    _strict_json_value,
)
from hydra.research.energy_metals_barrier_primary import _read_period
from hydra.research.energy_metals_session_geometry_primary import (
    _concentration_stress,
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


VERSION = "session_geometry_micro_execution_repair_v1"
PARENT_ID = (
    "strategy_session_geometry_CL_overnight_extreme_position_"
    "continuation_q65_h60_prior_trend_agree_v1"
)
CHILD_ID = (
    "strategy_session_geometry_CL_signal_MCL_execution_overnight_extreme_"
    "position_continuation_q65_h60_prior_trend_agree_v2"
)
PRIMARY_ALPHA = 0.03
SHADOW_ALPHA = 0.20


class SessionGeometryExecutionRepairError(RuntimeError):
    pass


def child_specification() -> dict[str, Any]:
    specification = {
        "representation": VERSION,
        "candidate_id": CHILD_ID,
        "parent_candidate_id": PARENT_ID,
        "signal_market": "CL",
        "execution_market": "MCL",
        "feature": "overnight_extreme_position",
        "policy_direction": "continuation",
        "quantile": 0.65,
        "horizon": 60,
        "context": "prior_trend_agree",
        "signal_semantics": "CL_SIGNAL_MCL_SYNCHRONIZED_EXECUTION",
        "mechanism_family": "overnight_inventory_transfer",
        "market_ecology": "energy",
        "portfolio_role": "trend",
    }
    fingerprint = structural_fingerprint(specification)
    return {
        **specification,
        "structural_fingerprint": fingerprint,
        "lineage_id": f"lineage_session_execution_{fingerprint[:20]}",
    }


def synchronize_mcl_execution(
    cl_signals: pd.DataFrame,
    mcl_table: pd.DataFrame,
    *,
    entry_delay_bars: int = 0,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    suffix = "" if entry_delay_bars == 0 else "_delay1"
    indexed = mcl_table.set_index("session_id", drop=False)
    point_value = instrument_spec("MCL").point_value
    cost = _round_turn_cost_all("MCL")
    records: list[dict[str, Any]] = []
    missing: list[dict[str, str]] = []
    for signal in cl_signals.to_dict("records"):
        session_id = str(signal["trading_session_id"])
        if session_id not in indexed.index:
            missing.append({"session_id": session_id, "reason": "missing_mcl_session"})
            continue
        row = indexed.loc[session_id]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        entry_timestamp = pd.Timestamp(row[f"overnight_entry_timestamp{suffix}"])
        expected_entry = pd.Timestamp(signal["entry_timestamp"]) + pd.Timedelta(
            minutes=entry_delay_bars
        )
        if entry_timestamp != expected_entry:
            missing.append(
                {"session_id": session_id, "reason": "entry_timestamp_mismatch"}
            )
            continue
        exit_column = f"overnight_exit_60{suffix}"
        exit_time_column = f"overnight_exit_timestamp_60{suffix}"
        if pd.isna(row.get(exit_column)) or pd.isna(row.get(exit_time_column)):
            missing.append({"session_id": session_id, "reason": "missing_mcl_exit"})
            continue
        side = float(signal["side"])
        entry_price = float(row[f"overnight_entry_price{suffix}"])
        exit_price = float(row[exit_column])
        gross = side * (exit_price - entry_price) * point_value
        long_mae = float(row[f"overnight_long_mae_60{suffix}"]) * point_value
        short_mae = float(row[f"overnight_short_mae_60{suffix}"]) * point_value
        records.append(
            {
                "candidate_id": CHILD_ID,
                "parent_candidate_id": PARENT_ID,
                "event_session_id": session_id,
                "trading_session_id": session_id,
                "symbol": "MCL",
                "active_contract": str(row["active_contract"]),
                "signal_symbol": "CL",
                "side": side,
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": pd.Timestamp(row[exit_time_column]),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "gross_pnl": gross,
                "cost": cost,
                "net_pnl": gross - cost,
                "mae_dollars": (long_mae if side > 0 else short_mae) - cost / 2,
                "entry_delay_bars": entry_delay_bars,
                "signal_recomputed_from_mcl": False,
            }
        )
    return pd.DataFrame(records), missing


def run_session_geometry_micro_execution_repair(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    source_result_path: str | Path,
    source_result_sha256: str,
    source_result_hash: str,
    source_manifest_path: str | Path,
    source_manifest_sha256: str,
    source_manifest_hash: str,
    source_trade_ledger_path: str | Path,
    source_trade_ledger_sha256: str,
    energy_data_path: str | Path,
    energy_data_sha256: str,
    energy_map_path: str | Path,
    energy_map_sha256: str,
    energy_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
) -> dict[str, Any]:
    started = time.perf_counter()
    frozen = (
        (Path(engineering_task_path), engineering_task_sha256, "engineering task"),
        (Path(source_result_path), source_result_sha256, "source result"),
        (Path(source_manifest_path), source_manifest_sha256, "source manifest"),
        (Path(source_trade_ledger_path), source_trade_ledger_sha256, "source ledger"),
        (Path(energy_data_path), energy_data_sha256, "energy data"),
        (Path(energy_map_path), energy_map_sha256, "energy map"),
    )
    for path, expected, label in frozen:
        _verify(path, expected, label)
    source = json.loads(Path(source_result_path).read_text(encoding="utf-8"))
    manifest = json.loads(Path(source_manifest_path).read_text(encoding="utf-8"))
    if (
        source.get("result_hash") != source_result_hash
        or source.get("primary_candidate_id") != PARENT_ID
        or source.get("scientific_conclusion")
        != "ENERGY_METALS_SESSION_GEOMETRY_PROMISING_BUT_INSUFFICIENT"
        or manifest.get("primary_manifest_hash") != source_manifest_hash
        or manifest.get("primary_candidate_id") != PARENT_ID
    ):
        raise SessionGeometryExecutionRepairError("Parent evidence contract changed.")
    parent_candidates = [
        item for item in source.get("candidates") or [] if item.get("candidate_id") == PARENT_ID
    ]
    if (
        len(parent_candidates) != 1
        or parent_candidates[0].get("status") != "PROMISING_RESEARCH_CANDIDATE"
        or (parent_candidates[0].get("contract_transfer") or {}).get("passed") is not False
    ):
        raise SessionGeometryExecutionRepairError("Parent is not the frozen transfer failure.")
    if len(code_commit) == 40:
        current = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if current != code_commit:
            raise SessionGeometryExecutionRepairError("Worker commit differs from specification.")
    roll_map = load_roll_map(energy_map_path)
    if roll_map.roll_map_hash() != energy_roll_map_hash:
        raise SessionGeometryExecutionRepairError("Energy roll map changed.")
    child = child_specification()
    if child["structural_fingerprint"] == parent_candidates[0]["structural_fingerprint"]:
        raise SessionGeometryExecutionRepairError("Child did not receive a fresh fingerprint.")
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration = {
        "schema": VERSION,
        "child": child,
        "source_result_hash": source_result_hash,
        "source_manifest_hash": source_manifest_hash,
        "signal_parameters_mutable": False,
        "execution_change_only": True,
        "promotion_alpha": PRIMARY_ALPHA,
        "shadow_support_alpha": SHADOW_ALPHA,
        "development_confirmation_contaminated_by_parent": True,
        "q4_access_allowed": False,
        "network_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "code_commit": code_commit,
    }
    preregistration["preregistration_hash"] = _stable_hash(preregistration)
    preregistration_path = destination / "micro_execution_repair_preregistration.json"
    _write_immutable(
        preregistration_path,
        json.dumps(preregistration, indent=2, sort_keys=True) + "\n",
    )
    access = _record_access_once(child) if record_data_access else None
    raw = _read_period(Path(energy_data_path), {"CL", "MCL"}, "2024-10-01")
    raw, contract_audit = _apply_explicit_contract_map(
        raw, roll_map, required_map_type=roll_map.map_type
    )
    cl_table = build_session_geometry_table(raw, "CL")
    mcl_table = build_session_geometry_table(raw, "MCL")
    parent_hypothesis = {
        "market": "CL",
        "execution_market": "MCL",
        "feature": "overnight_extreme_position",
        "policy_direction": "continuation",
        "quantile": 0.65,
        "horizon": 60,
        "context": "prior_trend_agree",
    }
    cl_signals = build_session_geometry_events(cl_table, parent_hypothesis)
    _verify_parent_signals(cl_signals, Path(source_trade_ledger_path))
    child_events, missing = synchronize_mcl_execution(cl_signals, mcl_table)
    delayed_events, delayed_missing = synchronize_mcl_execution(
        cl_signals, mcl_table, entry_delay_bars=1
    )
    if child_events.empty:
        raise SessionGeometryExecutionRepairError("No synchronized MCL events.")
    missing_rate = len(missing) / max(len(cl_signals), 1)
    folds = {
        "2023_h1": _period_metrics(_period(child_events, "2023-01-01", "2023-07-01")),
        "2023_h2": _period_metrics(_period(child_events, "2023-07-01", "2024-01-01")),
        "2024_q1": _period_metrics(_period(child_events, "2024-01-01", "2024-04-01")),
        "2024_q2": _period_metrics(_period(child_events, "2024-04-01", "2024-07-01")),
        "2024_q3": _period_metrics(_period(child_events, "2024-07-01", "2024-10-01")),
    }
    development_2023 = _period(child_events, "2023-01-01", "2024-01-01")
    confirmation = _period(child_events, "2024-01-01", "2024-10-01")
    delayed_confirmation = _period(delayed_events, "2024-01-01", "2024-10-01")
    metrics_2023 = _period_metrics(development_2023)
    metrics_2024 = _validation_metrics(confirmation)
    delayed_metrics = _period_metrics(delayed_confirmation)
    probability = _block_sign_flip_probability(confirmation, seed=991071)
    concentration = _concentration_stress(confirmation)
    account = _account_replay(
        confirmation.rename(columns={"net_pnl": "net_pnl_60"}).copy()
    )
    explicit_support = bool(
        metrics_2023["net_pnl"] > 0
        and metrics_2023["cost_stress_1_5x_net"] > 0
        and metrics_2024["net_pnl"] > 0
        and metrics_2024["cost_stress_1_5x_net"] > 0
        and metrics_2024["supportive_temporal_folds"] >= 2
        and not metrics_2024["catastrophic_transfer"]
        and probability <= SHADOW_ALPHA
        and metrics_2024["best_positive_event_share"] <= 0.35
        and concentration["remove_best_event_net"] > 0
        and concentration["remove_best_month_net"] > 0
        and delayed_metrics["net_pnl"] > 0
        and missing_rate <= 0.10
        and bool(account.get("micro_one_contract_mll_safe", False))
    )
    evidence = ShadowEvidence(
        candidate_id=CHILD_ID,
        data_integrity=True,
        no_lookahead=True,
        deterministic_signals=True,
        net_after_costs=float(metrics_2024["net_pnl"]),
        supportive_temporal_folds=int(metrics_2024["supportive_temporal_folds"]),
        catastrophic_transfer=bool(metrics_2024["catastrophic_transfer"]),
        candidate_null_pass=explicit_support,
        null_probability=float(probability),
        parameter_stable=bool(
            (parent_candidates[0].get("parameter_diagnostics") or {}).get(
                "positive_neighbor_count", 0
            )
            >= 1
            and delayed_metrics["net_pnl"] > 0
        ),
        contract_evidence=bool(
            metrics_2023["net_pnl"] > 0
            and metrics_2024["net_pnl"] > 0
            and missing_rate <= 0.10
        ),
        account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
        execution_possible=True,
        realtime_features_available=True,
        shadow_spec_complete=True,
        observability_complete=True,
        untouched_holdout_passed=False,
        sample_size=int(metrics_2024["events"]),
        uncertainty="parent_informed_development_repair_requires_forward_shadow",
    )
    admission = decide_shadow_admission(evidence)
    if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
        raise SessionGeometryExecutionRepairError("Repair attempted paper promotion.")
    configuration = _shadow_specification(child, preregistration["preregistration_hash"])
    configuration_path = None
    shadow_configurations: list[dict[str, Any]] = []
    if admission.permits_zero_risk_shadow:
        configuration_path = configuration.write_immutable(
            destination / "shadow_configurations" / f"{CHILD_ID}.json"
        )
        shadow_configurations.append(
            {
                "candidate_id": CHILD_ID,
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
        "events": int(metrics_2024["events"]),
        "net_pnl": float(metrics_2024["net_pnl"]),
        "micro_events": int(metrics_2024["events"]),
        "micro_net_pnl": float(metrics_2024["net_pnl"]),
        "supportive_temporal_folds": int(metrics_2024["supportive_temporal_folds"]),
        "fold_results": metrics_2024["fold_results"],
        "micro_fold_results": metrics_2024["fold_results"],
        "cost_stress_1_5x_net": float(metrics_2024["cost_stress_1_5x_net"]),
        "development_2023": metrics_2023,
        "null_evidence": {
            "method": "synchronized_mcl_five_session_block_sign_flip",
            "raw_probability": float(probability),
            "prospective_alpha": PRIMARY_ALPHA,
            "promotion_passed": bool(probability <= PRIMARY_ALPHA),
            "shadow_research_support_threshold": SHADOW_ALPHA,
            "shadow_research_support_passed": explicit_support,
        },
        "contract_transfer": {
            "signal": "CL",
            "execution": "MCL",
            "passed": bool(evidence.contract_evidence),
            "signal_recomputed_from_mcl": False,
            "matched_events": int(len(child_events)),
            "missing_events": int(len(missing)),
            "missing_rate": float(missing_rate),
        },
        "parameter_diagnostics": {
            "parent_signal_neighbors_positive": int(
                (parent_candidates[0].get("parameter_diagnostics") or {}).get(
                    "positive_neighbor_count", 0
                )
            ),
            "one_bar_delay_net": float(delayed_metrics["net_pnl"]),
            "parameter_stable": evidence.parameter_stable,
        },
        "attacks": {
            **concentration,
            "one_additional_bar_delay_net": float(delayed_metrics["net_pnl"]),
            "missing_match_rate": float(missing_rate),
            "signal_sessions_preserved": True,
            "signal_sides_preserved": True,
            "mcl_signal_recomputed": False,
        },
        "topstep": account,
        "shadow_evidence": evidence.__dict__,
    }
    conclusion = (
        "SYNCHRONIZED_MCL_EXECUTION_SHADOW_CANDIDATE_FOUND"
        if admission.permits_zero_risk_shadow
        else "SYNCHRONIZED_MCL_EXECUTION_REPAIR_FALSIFIED_OR_INSUFFICIENT"
    )
    trade_path = destination / "micro_execution_repair_trade_ledger.jsonl"
    _write_ledger(trade_path, child_events)
    integrity = {
        "fresh_child_fingerprint": child["structural_fingerprint"]
        != parent_candidates[0]["structural_fingerprint"],
        "parent_signals_reproduced": True,
        "mcl_signal_not_recomputed": bool(
            not child_events["signal_recomputed_from_mcl"].any()
        ),
        "missing_rate_bounded": missing_rate <= 0.10,
        "q4_excluded": True,
        "no_network_or_paid_data": True,
        "no_outbound_order_capability": True,
    }
    if not all(integrity.values()):
        raise SessionGeometryExecutionRepairError(f"Integrity failed: {integrity}")
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "This parent-informed execution repair uses development data already exposed by "
            "the parent and can authorize only zero-risk forward shadow research."
        ),
        "code_commit": code_commit,
        "candidate_count": 1,
        "structural_prototypes": 1,
        "primary_candidate_id": CHILD_ID,
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
            "source_signal_count": int(len(cl_signals)),
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
            else "KILL_EXACT_CHILD_AND_PIVOT"
        ),
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "micro_execution_repair_result.json"
    report_path = destination / "micro_execution_repair_report.md"
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


def _verify_parent_signals(cl_signals: pd.DataFrame, ledger_path: Path) -> None:
    source_rows = [
        json.loads(line)
        for line in ledger_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    source = [row for row in source_rows if row.get("contract_role") == "primary_mini"]
    computed = cl_signals[
        pd.to_datetime(cl_signals["entry_timestamp"], utc=True).ge("2024-01-01")
        & pd.to_datetime(cl_signals["entry_timestamp"], utc=True).lt("2024-10-01")
    ]
    source_keys = {
        (
            str(row.get("event_session_id") or row.get("trading_session_id")),
            int(float(row["side"])),
            pd.Timestamp(row["entry_timestamp"]).isoformat(),
        )
        for row in source
    }
    computed_keys = {
        (
            str(row["event_session_id"]),
            int(float(row["side"])),
            pd.Timestamp(row["entry_timestamp"]).isoformat(),
        )
        for row in computed.to_dict("records")
    }
    if source_keys != computed_keys:
        raise SessionGeometryExecutionRepairError(
            "Recomputed CL signals differ from the immutable parent ledger."
        )


def _period(events: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    timestamp = pd.to_datetime(events["entry_timestamp"], utc=True)
    return events[timestamp.ge(start) & timestamp.lt(end)].copy()


def _record_access_once(child: dict[str, Any]) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "CL-signal synchronized MCL execution repair; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module")
                == "hydra.research.energy_metals_session_execution_repair"
                and row.get("candidate_ids") == [CHILD_ID]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.energy_metals_session_execution_repair",
        [CHILD_ID],
        reason,
        None,
    )
    return record.__dict__


def _shadow_specification(
    child: dict[str, Any], source_manifest_hash: str
) -> ShadowSpecification:
    return ShadowSpecification(
        strategy_id=CHILD_ID,
        strategy_version="v2_cl_signal_mcl_execution_pre_holdout",
        feature_versions=("causal_cl_session_geometry_v1", "synchronized_mcl_execution_v1"),
        markets=("CL", "MCL"),
        timeframes=("1m", "overnight", "RTH_session"),
        session_rules={
            "timezone": "America/Chicago",
            "signal_market": "CL",
            "execution_market": "MCL",
            "market_open_minute": 480,
            "mandatory_flatten_before_session_end": True,
        },
        entry_rules={
            "event": "cl_overnight_extreme_position_threshold",
            "feature": child["feature"],
            "quantile": child["quantile"],
            "direction": child["policy_direction"],
            "context": child["context"],
            "threshold_history_sessions": 20,
            "execution_delay_completed_bars": 1,
            "mcl_signal_recomputation": False,
            "exact_timestamp_match_required": True,
            "missing_match_policy": "fail_closed_skip_signal",
        },
        exit_rules={"holding_completed_1m_bars": 60, "no_overnight": True},
        sizing={"contracts": 1, "instrument": "MCL", "micro_first": True},
        costs={
            "round_turn_usd": _round_turn_cost_all("MCL"),
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
            "cl_signal_ledger": True,
            "mcl_virtual_fill_ledger": True,
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
        raise SessionGeometryExecutionRepairError(
            f"Frozen {label} missing or changed: {path}"
        )


def _render_report(payload: dict[str, Any]) -> str:
    candidate = payload["candidates"][0]
    return "\n".join(
        [
            "# Session Geometry Micro-Execution Repair",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Child: `{candidate['candidate_id']}`",
            f"- 2024 MCL events: `{candidate['events']}`",
            f"- 2024 MCL net: `{candidate['net_pnl']}`",
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
