from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.foundry.status import (
    EvidenceTier,
    ShadowAdmissionDecision,
    ShadowEvidence,
    decide_shadow_admission,
)
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _file_sha256,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.research.equity_open_gap_reversal import (
    FOLDS,
    MAP_TYPE,
    SOURCE_PREREGISTRATION_SHA256,
    _account_replay,
    _concentration,
    _fold_results,
    _round_turn_cost,
    _shadow_specification,
    _write_immutable,
)
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "ym_shared_risk_off_overlay_v1"
CANDIDATE_ID = "strategy_open_gap_continuation_YM_riskoff_v1"
PARENT_ID = "strategy_open_gap_continuation_YM_v1"
RISK_SYMBOLS = ("ES", "NQ", "RTY", "YM")
PRIMARY_THRESHOLD = 0.80
MINIMUM_PRIOR_DAYS = 40
RANDOM_SKIP_DRAWS = 4096
FEATURE_VERSION = "shared_equity_risk_state_closed_1m_past_only_v1"


class YMSharedRiskOffOverlayError(RuntimeError):
    pass


def run_ym_shared_risk_off_overlay(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    source_parent_result_path: str | Path,
    source_parent_result_sha256: str,
    source_parent_result_hash: str,
    source_parent_trade_ledger_path: str | Path,
    source_parent_trade_ledger_sha256: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771887,
) -> dict[str, Any]:
    task_path = Path(engineering_task_path)
    map_path = Path(repaired_map_path)
    parent_result_path = Path(source_parent_result_path)
    parent_ledger_path = Path(source_parent_trade_ledger_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    _verify(parent_result_path, source_parent_result_sha256, "frozen parent result")
    _verify(parent_ledger_path, source_parent_trade_ledger_sha256, "frozen parent ledger")

    parent_result = json.loads(parent_result_path.read_text(encoding="utf-8"))
    parent_candidates = [
        item for item in parent_result.get("candidates", []) if item.get("candidate_id") == PARENT_ID
    ]
    if (
        parent_result.get("result_hash") != source_parent_result_hash
        or len(parent_candidates) != 1
        or parent_candidates[0].get("status") != EvidenceTier.SHADOW_RESEARCH_CANDIDATE.value
        or int(parent_result.get("paper_shadow_ready", -1)) != 0
        or int((parent_result.get("governance") or {}).get("q4_access_count_delta", -1)) != 0
    ):
        raise YMSharedRiskOffOverlayError("Frozen parent does not authorize the defensive child.")

    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise YMSharedRiskOffOverlayError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise YMSharedRiskOffOverlayError("Worker commit differs from queued specification.")

    source_preregistration = project_path(
        "reports",
        "mission_experiments",
        "calibration_affected_atom_retest_v3_design_v1",
        "calibration_affected_atom_retest_v3_preregistration.json",
    )
    if not source_preregistration.is_file():
        source_preregistration = Path(
            "/root/hydra-bot/reports/mission_experiments/"
            "calibration_affected_atom_retest_v3_design_v1/"
            "calibration_affected_atom_retest_v3_preregistration.json"
        )
    _verify(source_preregistration, SOURCE_PREREGISTRATION_SHA256, "development manifest")
    source = json.loads(source_preregistration.read_text(encoding="utf-8"))
    _verify_development_manifest((source.get("source") or {}).get("development_data_manifest") or {})

    preregistration = _preregistration(
        engineering_task_sha256=engineering_task_sha256,
        repaired_map_sha256=repaired_map_sha256,
        repaired_roll_map_hash=repaired_roll_map_hash,
        source_parent_result_sha256=source_parent_result_sha256,
        source_parent_result_hash=source_parent_result_hash,
        source_parent_trade_ledger_sha256=source_parent_trade_ledger_sha256,
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "ym_shared_risk_off_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )

    access = _record_access_once() if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(RISK_SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    risk_state = build_shared_risk_state(raw)
    parent_rows = _load_parent_ledger(parent_ledger_path)
    parent_mini = parent_rows[parent_rows["symbol"].eq("YM")].copy()
    parent_micro = parent_rows[parent_rows["symbol"].eq("MYM")].copy()
    if len(parent_mini) != int(parent_candidates[0].get("events", -1)) or len(parent_micro) != int(
        parent_candidates[0].get("micro_events", -1)
    ):
        raise YMSharedRiskOffOverlayError("Parent result and parent trade ledger disagree.")

    mini_overlay = apply_risk_off_overlay(parent_mini, risk_state, threshold=PRIMARY_THRESHOLD)
    micro_overlay = apply_risk_off_overlay(parent_micro, risk_state, threshold=PRIMARY_THRESHOLD)
    mini_metrics = _overlay_metrics(parent_mini, mini_overlay)
    micro_metrics = _overlay_metrics(parent_micro, micro_overlay)
    null_evidence = _matched_random_skip_controls(
        parent_mini, mini_overlay, random_seed=random_seed, draws=RANDOM_SKIP_DRAWS
    )
    diagnostics = _diagnostics(parent_mini, risk_state)
    integrity = _integrity_proof(
        risk_state=risk_state,
        parent_mini=parent_mini,
        parent_micro=parent_micro,
        mini_overlay=mini_overlay,
        micro_overlay=micro_overlay,
    )
    if not all(integrity.values()):
        raise YMSharedRiskOffOverlayError(f"Defensive overlay integrity failed: {integrity}")

    primary_success = bool(mini_metrics["primary_utility_success"])
    candidate_null_pass = bool(primary_success and null_evidence["utility_probability"] <= 0.20)
    parameter_stable = bool(
        sum(
            bool(item["retained_net_positive"])
            and float(item["retention_ratio"]) >= 0.60
            and float(item["maximum_drawdown_reduction_fraction"]) >= 0.0
            for item in diagnostics["threshold_variants"].values()
        )
        == len(diagnostics["threshold_variants"])
    )
    contract_evidence = bool(
        mini_metrics["retained_net_pnl"] > 0
        and micro_metrics["retained_net_pnl"] > 0
        and micro_metrics["supportive_transfer_folds"] >= 1
    )
    retained_micro = micro_overlay[micro_overlay["retained"]].copy()
    account = _account_replay(retained_micro)
    specification = _shadow_specification_for_child(
        preregistration_hash=str(preregistration["preregistration_hash"])
    )
    specification.validate()
    evidence = ShadowEvidence(
        candidate_id=CANDIDATE_ID,
        data_integrity=True,
        no_lookahead=True,
        deterministic_signals=True,
        net_after_costs=float(mini_metrics["retained_net_pnl"]),
        supportive_temporal_folds=int(mini_metrics["supportive_transfer_folds"]),
        catastrophic_transfer=bool(mini_metrics["catastrophic_transfer"]),
        candidate_null_pass=candidate_null_pass,
        null_probability=float(null_evidence["utility_probability"]),
        parameter_stable=parameter_stable,
        contract_evidence=contract_evidence,
        account_mll_safe=bool(account.get("micro_one_contract_mll_safe", False)),
        execution_possible=True,
        realtime_features_available=True,
        shadow_spec_complete=True,
        observability_complete=True,
        untouched_holdout_passed=False,
        sample_size=int(mini_metrics["retained_events"]),
        uncertainty="development_only_parent_q4_lineage_prohibited_forward_shadow_only",
    )
    admission = decide_shadow_admission(evidence)
    # Positive residual PnL is inherited mechanically from the parent event set.
    # It cannot make this new defensive mechanism promising when its frozen
    # utility objective failed.
    if not primary_success:
        admission = ShadowAdmissionDecision(
            tier=EvidenceTier.RESEARCH_PROTOTYPE,
            fatal_reasons=(),
            missing_requirements=("preregistered_defensive_utility",),
            uncertainty=evidence.uncertainty,
            permits_zero_risk_shadow=False,
        )
    if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
        raise YMSharedRiskOffOverlayError("Defensive child attempted pre-holdout paper promotion.")

    retained_mini = mini_overlay[mini_overlay["retained"]].copy()
    concentration = _concentration(retained_mini, _fold_results(retained_mini))
    candidate = {
        "candidate_id": CANDIDATE_ID,
        "parent_candidate_id": PARENT_ID,
        "mechanism_family": "shared_equity_risk_off_activation",
        "primary_market": "YM",
        "execution_market": "MYM",
        "portfolio_role": "defensive_mll_and_loss_day_control",
        "status": admission.tier.value,
        "admission": admission.to_dict(),
        "events": int(mini_metrics["retained_events"]),
        "parent_events": int(mini_metrics["parent_events"]),
        "event_overlap_with_parent": float(mini_metrics["retention_ratio"]),
        "net_pnl": float(mini_metrics["retained_net_pnl"]),
        "parent_net_pnl": float(mini_metrics["parent_net_pnl"]),
        "micro_events": int(micro_metrics["retained_events"]),
        "micro_net_pnl": float(micro_metrics["retained_net_pnl"]),
        "supportive_temporal_folds": int(mini_metrics["supportive_transfer_folds"]),
        "fold_results": mini_metrics["retained_fold_results"],
        "micro_fold_results": micro_metrics["retained_fold_results"],
        "defensive_utility": mini_metrics,
        "micro_defensive_utility": micro_metrics,
        "null_evidence": {
            "method": "opportunity_count_matched_random_skip_4096_draws",
            "raw_probability": float(null_evidence["utility_probability"]),
            "family_adjusted_probability": float(null_evidence["utility_probability"]),
            **null_evidence,
        },
        "parameter_diagnostics": diagnostics,
        "contract_transfer": {
            "mini": "YM",
            "micro": "MYM",
            "passed": contract_evidence,
            "micro_supportive_folds": int(micro_metrics["supportive_transfer_folds"]),
        },
        "attacks": {
            "best_event_share_of_positive_pnl": concentration["best_event_share"],
            "best_fold_share_of_positive_pnl": concentration["best_fold_share"],
            "event_dominated": concentration["event_dominated"],
            "parent_event_additions": 0,
            "hidden_resizing": False,
        },
        "topstep": account,
        "shadow_evidence": evidence.__dict__,
        "q4_lineage_reuse_allowed": False,
    }

    shadow_configs: list[dict[str, Any]] = []
    if admission.permits_zero_risk_shadow:
        config_path = specification.write_immutable(
            destination / "shadow_configurations" / f"{CANDIDATE_ID}.json"
        )
        shadow_configs.append(
            {
                "candidate_id": CANDIDATE_ID,
                "status": admission.tier.value,
                "path": str(config_path),
                "configuration_hash": specification.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )

    ledger_path = destination / "ym_shared_risk_off_trade_ledger.jsonl"
    _write_overlay_ledger(ledger_path, pd.concat([mini_overlay, micro_overlay], ignore_index=True))
    risk_path = destination / "ym_shared_risk_state.jsonl"
    _write_risk_ledger(risk_path, risk_state)
    promising = int(
        admission.tier
        in {
            EvidenceTier.PROMISING_RESEARCH_CANDIDATE,
            EvidenceTier.ROBUST_RESEARCH_CANDIDATE,
            EvidenceTier.SHADOW_RESEARCH_CANDIDATE,
        }
    )
    shadow = int(admission.tier == EvidenceTier.SHADOW_RESEARCH_CANDIDATE)
    if shadow:
        conclusion = "YM_SHARED_RISK_OFF_SHADOW_RESEARCH_CANDIDATE"
        next_action = "START_FORWARD_SHADOW_AND_BUILD_DISTINCT_SHARED_ACCOUNT_BASKET"
    elif promising:
        conclusion = "YM_SHARED_RISK_OFF_PROMISING_BUT_INSUFFICIENT"
        next_action = "MAP_DEFENSIVE_FAILURE_SURFACE_WITHOUT_REUSING_Q4"
    else:
        conclusion = "YM_SHARED_RISK_OFF_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_INVENTED_OR_PORTFOLIO_LEVEL_SEARCH"

    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Fresh child-level development evidence only. Parent events and economics were replayed "
            "from the frozen ledger; no status was inherited. The parent Q4 lineage is prohibited for "
            "this child, PAPER_SHADOW_READY is impossible here, and no outbound order path exists."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "source_parent": {
            "candidate_id": PARENT_ID,
            "result_path": str(parent_result_path),
            "result_sha256": source_parent_result_sha256,
            "result_hash": source_parent_result_hash,
            "trade_ledger_path": str(parent_ledger_path),
            "trade_ledger_sha256": source_parent_trade_ledger_sha256,
        },
        "data_provenance": provenance,
        "data_access_record": access,
        "risk_state_provenance": {
            "feature_version": FEATURE_VERSION,
            "rows": int(len(risk_state)),
            "fingerprint": _frame_fingerprint(risk_state),
            "decision_time_chicago": "08:31",
            "source_timeframe": "closed_1m",
            "markets": list(RISK_SYMBOLS),
        },
        "integrity_proof": integrity,
        "candidate_count": 1,
        "candidate_tier_counts": {admission.tier.value: 1},
        "candidates": [candidate],
        "promising_candidates": promising,
        "shadow_candidates": shadow,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(bool(account.get("path_candidate", False))),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["shared_equity_risk_off_activation"],
        "market_ecologies": ["equity_indices", "defensive_portfolio"],
        "timeframe_profiles": ["shared_closed_1m_state_session_event_1m_execution"],
        "shadow_configurations": shadow_configs,
        "governance": {
            "q4_access_count_delta": 0,
            "q4_lineage_reuse_allowed": False,
            "latest_data_end_exclusive": "2024-10-01",
            "network_requests": 0,
            "incremental_databento_spend_usd": 0.0,
            "live_or_broker_execution": False,
            "outbound_order_capability": False,
        },
        "next_recommended_action": next_action,
    }
    payload = _strict_json_value(payload)
    payload["result_hash"] = _stable_hash(payload)
    result_path = destination / "ym_shared_risk_off_result.json"
    report_path = destination / "ym_shared_risk_off_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "trade_ledger_path": str(ledger_path),
            "risk_state_path": str(risk_path),
            "shadow_configuration_directory": str(destination / "shadow_configurations"),
        },
        "report_path": str(report_path),
    }


def past_only_percentile(values: pd.Series, *, minimum_history: int = MINIMUM_PRIOR_DAYS) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype(float)
    output = pd.Series(np.nan, index=numeric.index, dtype=float)
    history: list[float] = []
    for index, value in numeric.items():
        if np.isfinite(value) and len(history) >= minimum_history:
            previous = np.asarray(history, dtype=float)
            output.at[index] = float(np.mean(previous <= value))
        if np.isfinite(value):
            history.append(float(value))
    return output


def build_shared_risk_state(
    frame: pd.DataFrame, *, minimum_history: int = MINIMUM_PRIOR_DAYS
) -> pd.DataFrame:
    required = {"timestamp", "symbol", "active_contract", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise YMSharedRiskOffOverlayError(f"Risk source missing columns: {sorted(missing)}")
    data = frame[frame["symbol"].astype(str).isin(RISK_SYMBOLS)].copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    data = (
        data.sort_values(["symbol", "active_contract", "timestamp"])
        .drop_duplicates(["symbol", "active_contract", "timestamp"], keep=False)
        .reset_index(drop=True)
    )
    grouping = ["symbol", "active_contract"]
    grouped = data.groupby(grouping, sort=False, observed=True)
    data["return_1"] = grouped["close"].pct_change(fill_method=None)
    data["past_return_60"] = grouped["close"].pct_change(60, fill_method=None)
    data["past_volatility"] = grouped["return_1"].transform(
        lambda item: item.rolling(120, min_periods=120).std(ddof=0)
    )
    data["downside_state"] = np.maximum(-data["past_return_60"].astype(float), 0.0)
    chicago = data["timestamp"].dt.tz_convert("America/Chicago")
    decision_rows = data[(chicago.dt.hour == 8) & (chicago.dt.minute == 30)].copy()
    decision_rows["feature_complete"] = np.isfinite(
        decision_rows[["past_volatility", "past_return_60", "downside_state"]]
    ).all(axis=1)
    state = (
        decision_rows.groupby("timestamp", sort=True)
        .agg(
            market_count=("symbol", "nunique"),
            complete_market_count=("feature_complete", "sum"),
            mean_past_volatility=("past_volatility", "mean"),
            mean_downside_state=("downside_state", "mean"),
            cross_market_dispersion=("past_return_60", lambda item: float(np.std(item, ddof=0))),
        )
        .reset_index(names="source_bar_start")
    )
    state = state[
        state["market_count"].eq(len(RISK_SYMBOLS))
        & state["complete_market_count"].eq(len(RISK_SYMBOLS))
    ].copy()
    state["source_bar_close"] = state["source_bar_start"] + pd.Timedelta(minutes=1)
    state["availability_timestamp"] = state["source_bar_close"]
    state["decision_timestamp"] = state["availability_timestamp"]
    state["prior_decision_count"] = np.arange(len(state), dtype=int)
    components = (
        "mean_past_volatility",
        "mean_downside_state",
        "cross_market_dispersion",
    )
    percentile_columns: list[str] = []
    for component in components:
        column = f"{component}_percentile"
        state[column] = past_only_percentile(state[component], minimum_history=minimum_history)
        percentile_columns.append(column)
    state["shared_risk_score"] = state[percentile_columns].mean(axis=1, skipna=False)
    state["transformation_version"] = FEATURE_VERSION
    return state.sort_values("decision_timestamp").reset_index(drop=True)


def apply_risk_off_overlay(
    parent_events: pd.DataFrame, risk_state: pd.DataFrame, *, threshold: float
) -> pd.DataFrame:
    parent = parent_events.copy()
    parent["decision_timestamp"] = pd.to_datetime(parent["decision_timestamp"], utc=True)
    state_columns = [
        "decision_timestamp",
        "source_bar_start",
        "source_bar_close",
        "availability_timestamp",
        "prior_decision_count",
        "mean_past_volatility_percentile",
        "mean_downside_state_percentile",
        "cross_market_dispersion_percentile",
        "shared_risk_score",
        "transformation_version",
    ]
    output = parent.merge(
        risk_state[state_columns],
        on="decision_timestamp",
        how="left",
        validate="many_to_one",
    )
    output["risk_threshold"] = float(threshold)
    output["risk_state_available"] = output["shared_risk_score"].notna()
    output["retained"] = output["risk_state_available"] & output["shared_risk_score"].lt(
        float(threshold)
    )
    return output.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def _overlay_metrics(parent: pd.DataFrame, overlay: pd.DataFrame) -> dict[str, Any]:
    baseline = parent.sort_values("timestamp").reset_index(drop=True)
    retained = overlay[overlay["retained"]].sort_values("timestamp").reset_index(drop=True)
    baseline_net = float(baseline["net_pnl_60"].sum())
    retained_net = float(retained["net_pnl_60"].sum())
    baseline_dd = _maximum_cumulative_drawdown(baseline["net_pnl_60"])
    retained_dd = _maximum_cumulative_drawdown(retained["net_pnl_60"])
    reduction = (baseline_dd - retained_dd) / baseline_dd if baseline_dd > 0 else 0.0
    baseline_worst_mae = float(baseline["mae_dollars"].min()) if len(baseline) else 0.0
    retained_worst_mae = float(retained["mae_dollars"].min()) if len(retained) else 0.0
    baseline_folds = _fold_results(baseline)
    retained_folds = _fold_results(retained)
    overall_mean = retained_net / max(len(retained), 1)
    catastrophic = bool(
        overall_mean > 0
        and any(
            retained_folds[fold]["events"] > 0
            and retained_folds[fold]["mean_net_pnl"] < -2.0 * abs(overall_mean)
            for fold in ("2024_q1", "2024_q2", "2024_q3")
        )
    )
    retention = len(retained) / max(len(baseline), 1)
    economic_retention = retained_net / baseline_net if baseline_net > 0 else 0.0
    success = bool(
        retention >= 0.60
        and retained_net > 0
        and reduction >= 0.15
        and retained_worst_mae >= baseline_worst_mae
        and not catastrophic
        and (economic_retention >= 0.80 or retained_net >= baseline_net)
    )
    return {
        "parent_events": int(len(baseline)),
        "retained_events": int(len(retained)),
        "skipped_events": int(len(baseline) - len(retained)),
        "retention_ratio": float(retention),
        "parent_net_pnl": baseline_net,
        "retained_net_pnl": retained_net,
        "economic_retention_ratio": float(economic_retention),
        "parent_maximum_cumulative_drawdown": float(baseline_dd),
        "retained_maximum_cumulative_drawdown": float(retained_dd),
        "maximum_drawdown_reduction_fraction": float(reduction),
        "parent_worst_event_mae": baseline_worst_mae,
        "retained_worst_event_mae": retained_worst_mae,
        "worst_event_mae_nonworse": bool(retained_worst_mae >= baseline_worst_mae),
        "catastrophic_transfer": catastrophic,
        "supportive_transfer_folds": int(
            sum(retained_folds[fold]["net_pnl"] > 0 for fold in ("2024_q1", "2024_q2", "2024_q3"))
        ),
        "parent_fold_results": baseline_folds,
        "retained_fold_results": retained_folds,
        "retained_net_positive": bool(retained_net > 0),
        "primary_utility_success": success,
    }


def _maximum_cumulative_drawdown(values: pd.Series | np.ndarray) -> float:
    pnl = np.asarray(values, dtype=float)
    if pnl.size == 0:
        return 0.0
    equity = np.concatenate(([0.0], np.cumsum(pnl)))
    peak = np.maximum.accumulate(equity)
    return float(np.max(peak - equity))


def _matched_random_skip_controls(
    parent: pd.DataFrame,
    overlay: pd.DataFrame,
    *,
    random_seed: int,
    draws: int,
) -> dict[str, Any]:
    baseline = parent.sort_values("timestamp").reset_index(drop=True)
    retained = overlay[overlay["retained"]].sort_values("timestamp").reset_index(drop=True)
    parent_net = float(baseline["net_pnl_60"].sum())
    parent_dd = _maximum_cumulative_drawdown(baseline["net_pnl_60"])

    def utility(selected: pd.DataFrame) -> float:
        net_ratio = float(selected["net_pnl_60"].sum()) / max(abs(parent_net), 1.0)
        dd = _maximum_cumulative_drawdown(selected["net_pnl_60"])
        reduction = (parent_dd - dd) / parent_dd if parent_dd > 0 else 0.0
        return float(net_ratio + reduction)

    observed = utility(retained)
    if not len(baseline) or not len(retained):
        return {
            "draws": int(draws),
            "matched_retained_count": int(len(retained)),
            "observed_utility": observed,
            "utility_probability": 1.0,
            "null_utility_mean": 0.0,
            "null_utility_q95": 0.0,
        }
    rng = np.random.default_rng(random_seed)
    values = np.empty(draws, dtype=float)
    for index in range(draws):
        selected = np.sort(rng.choice(len(baseline), size=len(retained), replace=False))
        values[index] = utility(baseline.iloc[selected])
    probability = float((1 + np.count_nonzero(values >= observed)) / (draws + 1))
    return {
        "draws": int(draws),
        "matched_retained_count": int(len(retained)),
        "observed_utility": observed,
        "utility_probability": probability,
        "null_utility_mean": float(np.mean(values)),
        "null_utility_q95": float(np.quantile(values, 0.95)),
    }


def _diagnostics(parent: pd.DataFrame, risk_state: pd.DataFrame) -> dict[str, Any]:
    thresholds: dict[str, Any] = {}
    for threshold in (0.75, 0.85):
        overlaid = apply_risk_off_overlay(parent, risk_state, threshold=threshold)
        thresholds[f"risk_threshold_{int(threshold * 100)}"] = _overlay_metrics(parent, overlaid)
    ablations: dict[str, Any] = {}
    percentile_columns = [
        "mean_past_volatility_percentile",
        "mean_downside_state_percentile",
        "cross_market_dispersion_percentile",
    ]
    for dropped in percentile_columns:
        ablated = risk_state.copy()
        kept = [item for item in percentile_columns if item != dropped]
        ablated["shared_risk_score"] = ablated[kept].mean(axis=1, skipna=False)
        overlaid = apply_risk_off_overlay(parent, ablated, threshold=PRIMARY_THRESHOLD)
        ablations[f"drop_{dropped}"] = _overlay_metrics(parent, overlaid)
    return {
        "diagnostic_only": True,
        "threshold_variants": thresholds,
        "equal_weight_component_ablations": ablations,
    }


def _integrity_proof(
    *,
    risk_state: pd.DataFrame,
    parent_mini: pd.DataFrame,
    parent_micro: pd.DataFrame,
    mini_overlay: pd.DataFrame,
    micro_overlay: pd.DataFrame,
) -> dict[str, bool]:
    usable = risk_state[risk_state["shared_risk_score"].notna()]
    return {
        "nonempty_parent_mini": bool(len(parent_mini)),
        "nonempty_parent_micro": bool(len(parent_micro)),
        "parent_event_counts_unchanged": bool(
            len(mini_overlay) == len(parent_mini) and len(micro_overlay) == len(parent_micro)
        ),
        "overlay_only_removes_events": bool(
            mini_overlay["retained"].sum() <= len(parent_mini)
            and micro_overlay["retained"].sum() <= len(parent_micro)
        ),
        "all_parent_events_have_causal_state": bool(
            mini_overlay["risk_state_available"].all() and micro_overlay["risk_state_available"].all()
        ),
        "closed_bar_only_features": bool(
            (usable["source_bar_close"] == usable["source_bar_start"] + pd.Timedelta(minutes=1)).all()
        ),
        "availability_not_after_decision": bool(
            (usable["availability_timestamp"] <= usable["decision_timestamp"]).all()
        ),
        "past_only_expanding_percentiles": bool((usable["prior_decision_count"] >= 40).all()),
        "four_market_synchronization": bool(
            risk_state["market_count"].eq(4).all() and risk_state["complete_market_count"].eq(4).all()
        ),
        "q4_excluded": bool(
            pd.to_datetime(parent_mini["timestamp"], utc=True).max()
            < pd.Timestamp("2024-10-01", tz="UTC")
        ),
        "contracts_and_costs_preserved": bool(
            mini_overlay["active_contract"].equals(parent_mini.reset_index(drop=True)["active_contract"])
            and micro_overlay["active_contract"].equals(parent_micro.reset_index(drop=True)["active_contract"])
            and np.allclose(mini_overlay["cost"], parent_mini.reset_index(drop=True)["cost"])
            and np.allclose(micro_overlay["cost"], parent_micro.reset_index(drop=True)["cost"])
        ),
        "no_hidden_resizing": True,
        "q4_lineage_reuse_prohibited": True,
        "no_outbound_order_capability": True,
    }


def _shadow_specification_for_child(*, preregistration_hash: str):
    parent = _shadow_specification(
        {
            "candidate_id": CANDIDATE_ID,
            "primary_market": "YM",
            "execution_market": "MYM",
            "entry_direction": "gap_direction",
        },
        preregistration_hash=preregistration_hash,
    )
    return replace(
        parent,
        strategy_version="v1_forward_shadow_research_q4_lineage_prohibited",
        feature_versions=parent.feature_versions + (FEATURE_VERSION,),
        session_rules={
            **parent.session_rules,
            "risk_state_markets": list(RISK_SYMBOLS),
            "risk_state_decision_time": "08:31",
            "closed_source_bars_only": True,
        },
        entry_rules={
            **parent.entry_rules,
            "activation": "shared_risk_score_lt_0.80",
            "risk_score_components": [
                "expanding_percentile_mean_120bar_realized_volatility",
                "expanding_percentile_mean_downside_60bar_return",
                "expanding_percentile_cross_market_60bar_return_dispersion",
            ],
            "risk_score_weights": [1 / 3, 1 / 3, 1 / 3],
            "minimum_prior_risk_days": MINIMUM_PRIOR_DAYS,
            "missing_risk_state_policy": "fail_closed_skip_signal",
        },
        source_manifest_hash=preregistration_hash,
        outbound_orders_enabled=False,
    )


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    source_parent_result_sha256: str,
    source_parent_result_hash: str,
    source_parent_trade_ledger_sha256: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "ym_shared_risk_off_preregistration_v1",
        "candidate_id": CANDIDATE_ID,
        "parent_candidate_id": PARENT_ID,
        "mechanism_family": "shared_equity_risk_off_activation",
        "risk_symbols": list(RISK_SYMBOLS),
        "decision_time_chicago": "08:31",
        "source_bar_minutes": 1,
        "realized_volatility_window_bars": 120,
        "lagged_return_window_bars": 60,
        "minimum_prior_decision_days": MINIMUM_PRIOR_DAYS,
        "primary_risk_threshold": PRIMARY_THRESHOLD,
        "minimum_event_retention": 0.60,
        "minimum_drawdown_reduction": 0.15,
        "minimum_economic_retention": 0.80,
        "random_skip_draws": RANDOM_SKIP_DRAWS,
        "random_skip_utility": "retained_net_over_parent_abs_net_plus_drawdown_reduction",
        "null_probability_threshold": 0.20,
        "diagnostic_thresholds": [0.75, 0.85],
        "component_ablations": "drop_one_equal_weight_component",
        "folds": FOLDS,
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "source_parent_result_sha256": source_parent_result_sha256,
        "source_parent_result_hash": source_parent_result_hash,
        "source_parent_trade_ledger_sha256": source_parent_trade_ledger_sha256,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "data_end_exclusive": "2024-10-01",
        "q4_access_allowed": False,
        "q4_lineage_reuse_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
        "inherits_parent_status": False,
        "paper_shadow_ready_requires_distinct_untouched_holdout": True,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _load_parent_ledger(path: Path) -> pd.DataFrame:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    frame = pd.DataFrame(rows)
    required = {
        "timestamp",
        "decision_timestamp",
        "event_session_id",
        "symbol",
        "active_contract",
        "cost",
        "gross_pnl_60",
        "net_pnl_60",
        "mae_dollars",
    }
    missing = required - set(frame.columns)
    if missing:
        raise YMSharedRiskOffOverlayError(f"Parent ledger missing columns: {sorted(missing)}")
    for column in ("timestamp", "decision_timestamp", "exit_timestamp_60", "reference_timestamp"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def _record_access_once() -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "YM shared-risk-off defensive child; parent replay and Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == "hydra.research.ym_shared_risk_off_overlay"
                and row.get("candidate_ids") == [CANDIDATE_ID]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.ym_shared_risk_off_overlay",
        [CANDIDATE_ID],
        reason,
        None,
    )
    return record.__dict__


def _write_overlay_ledger(path: Path, frame: pd.DataFrame) -> None:
    rows = []
    for row in frame.sort_values(["timestamp", "symbol"]).to_dict(orient="records"):
        rows.append(json.dumps(_strict_json_value(row), sort_keys=True, default=str))
    _write_immutable(path, "\n".join(rows) + ("\n" if rows else ""))


def _write_risk_ledger(path: Path, frame: pd.DataFrame) -> None:
    rows = []
    for row in frame.to_dict(orient="records"):
        rows.append(json.dumps(_strict_json_value(row), sort_keys=True, default=str))
    _write_immutable(path, "\n".join(rows) + ("\n" if rows else ""))


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    columns = [
        "decision_timestamp",
        "mean_past_volatility_percentile",
        "mean_downside_state_percentile",
        "cross_market_dispersion_percentile",
        "shared_risk_score",
    ]
    records = _strict_json_value(frame[columns].to_dict(orient="records"))
    return _stable_hash(records)


def _render_report(payload: dict[str, Any]) -> str:
    candidate = payload["candidates"][0]
    utility = candidate["defensive_utility"]
    return "\n".join(
        [
            "# YM Shared-Risk-Off Overlay v1",
            "",
            f"- Conclusion: `{payload['scientific_conclusion']}`",
            f"- Status: `{candidate['status']}`",
            f"- Events retained: `{candidate['events']}/{candidate['parent_events']}`",
            f"- Parent / retained net: `{candidate['parent_net_pnl']:.2f}` / `{candidate['net_pnl']:.2f}`",
            f"- Drawdown reduction: `{utility['maximum_drawdown_reduction_fraction']:.2%}`",
            f"- Matched-skip probability: `{candidate['null_evidence']['utility_probability']:.6f}`",
            f"- Contract transfer: `{candidate['contract_transfer']['passed']}`",
            f"- Topstep path candidate: `{candidate['topstep'].get('path_candidate', False)}`",
            "- Q4 lineage reuse: `prohibited`",
            "- PAPER_SHADOW_READY: `0`",
            "- Outbound orders: `0`",
            "",
            "## Interpretation boundary",
            "",
            payload["interpretation_boundary"],
            "",
        ]
    )


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise YMSharedRiskOffOverlayError(f"Frozen {label} is missing or changed: {path}")
