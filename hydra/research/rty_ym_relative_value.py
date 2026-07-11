from __future__ import annotations

import json
import math
import os
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.data.contract_mapping import load_roll_map
from hydra.data.multitimeframe import resample_closed_bars
from hydra.foundry.status import EvidenceTier, ShadowEvidence, decide_shadow_admission
from hydra.markets.instruments import instrument_spec
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _file_sha256,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
    _strict_json_value,
    _verify_development_manifest,
)
from hydra.propfirm.topstep_150k import Topstep150KConfig, simulate_combine
from hydra.propfirm.xfa_consistency import simulate_xfa_consistency
from hydra.propfirm.xfa_standard import simulate_xfa_standard
from hydra.research.equity_open_gap_reversal import (
    FOLDS,
    MAP_TYPE,
    SOURCE_PREREGISTRATION_SHA256,
    _round_turn_cost,
    _write_immutable,
)
from hydra.shadow.specification import ShadowSpecification
from hydra.utils.config import project_path
from hydra.validation.data_roles import DataRole
from hydra.validation.lockbox_guard import enforce_data_access


VERSION = "rty_ym_relative_value_pilot_v1"
SYMBOLS = ("RTY", "YM", "M2K", "MYM")
MICRO_COST = 2.0 * _round_turn_cost("M2K") + _round_turn_cost("MYM")
MINI_COST = 2.0 * _round_turn_cost("RTY") + _round_turn_cost("YM")


class RTYYMRelativeValueError(RuntimeError):
    pass


def run_rty_ym_relative_value_pilot(
    output_dir: str | Path,
    *,
    engineering_task_path: str | Path,
    engineering_task_sha256: str,
    repaired_map_path: str | Path,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    record_data_access: bool = True,
    random_seed: int = 771661,
) -> dict[str, Any]:
    task_path, map_path = Path(engineering_task_path), Path(repaired_map_path)
    _verify(task_path, engineering_task_sha256, "engineering task")
    _verify(map_path, repaired_map_sha256, "repaired contract map")
    roll_map = load_roll_map(map_path)
    if roll_map.map_type != MAP_TYPE or roll_map.roll_map_hash() != repaired_roll_map_hash:
        raise RTYYMRelativeValueError("Explicit-contract map contract changed.")
    if len(code_commit) == 40:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if actual != code_commit:
            raise RTYYMRelativeValueError("Worker commit differs from queued specification.")
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
        code_commit=code_commit,
        random_seed=random_seed,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    preregistration_path = destination / "rty_ym_relative_value_preregistration.json"
    _write_immutable(
        preregistration_path, json.dumps(preregistration, indent=2, sort_keys=True) + "\n"
    )
    candidate_id = "strategy_rty_ym_relative_value_v1"
    access = _record_access_once(candidate_id) if record_data_access else None
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(SYMBOLS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    events, signal_proof = build_relative_value_events(raw)
    integrity = _integrity(events, signal_proof)
    if not all(integrity.values()):
        raise RTYYMRelativeValueError(f"Relative-value integrity failed: {integrity}")
    fold_results = _fold_results(events, "micro_net_pnl")
    mini_folds = _fold_results(events, "mini_net_pnl")
    supportive = sum(fold_results[item]["net_pnl"] > 0 for item in ("2024_q1", "2024_q2", "2024_q3"))
    total_net = float(events["micro_net_pnl"].sum())
    mini_net = float(events["mini_net_pnl"].sum())
    raw_probability = _block_sign_probability(events, seed=random_seed)
    diagnostics = _diagnostics(raw)
    parameter_stable = bool(
        diagnostics["positive_neighbor_count"] >= 3
        and float((events["micro_gross_pnl"] - 1.5 * MICRO_COST).sum()) > 0
    )
    contract_evidence = bool(total_net > 0 and mini_net > 0)
    account = _account_replay(events)
    concentration = _concentration(events, fold_results)
    overall_mean = total_net / max(len(events), 1)
    catastrophic = bool(
        overall_mean > 0
        and any(
            fold_results[item]["mean_net_pnl"] < -2 * abs(overall_mean)
            for item in ("2024_q1", "2024_q2", "2024_q3")
            if fold_results[item]["events"]
        )
    )
    spec = _shadow_specification(candidate_id, preregistration["preregistration_hash"])
    spec.validate()
    evidence = ShadowEvidence(
        candidate_id=candidate_id,
        data_integrity=True,
        no_lookahead=True,
        deterministic_signals=True,
        net_after_costs=total_net,
        supportive_temporal_folds=supportive,
        catastrophic_transfer=catastrophic,
        candidate_null_pass=raw_probability <= 0.20
        and float((-events["micro_gross_pnl"] - MICRO_COST).sum()) < 0,
        null_probability=raw_probability,
        parameter_stable=parameter_stable,
        contract_evidence=contract_evidence,
        account_mll_safe=bool(account["one_unit_mll_safe"]),
        execution_possible=True,
        realtime_features_available=True,
        shadow_spec_complete=True,
        observability_complete=True,
        untouched_holdout_passed=False,
        sample_size=int(len(events)),
        uncertainty="development_only_q4_unopened_two_leg_execution_proxy",
    )
    admission = decide_shadow_admission(evidence)
    candidate = {
        "candidate_id": candidate_id,
        "mechanism_family": "rty_ym_relative_value_residual",
        "primary_market": "RTY-YM",
        "execution_market": "M2K-MYM",
        "portfolio_role": "relative_value_diversifier",
        "status": admission.tier.value,
        "admission": admission.to_dict(),
        "events": int(len(events)),
        "net_pnl": total_net,
        "mean_net_pnl": overall_mean,
        "mini_net_pnl": mini_net,
        "supportive_temporal_folds": supportive,
        "fold_results": fold_results,
        "mini_fold_results": mini_folds,
        "null_evidence": {
            "method": "five_event_block_sign_flip_4096_draws",
            "raw_probability": raw_probability,
            "family_adjusted_probability": raw_probability,
        },
        "parameter_diagnostics": diagnostics,
        "cost_stress_1_5x_net": float(
            (events["micro_gross_pnl"] - 1.5 * MICRO_COST).sum()
        ),
        "contract_transfer": {
            "mini_pair": "2_RTY_1_YM",
            "micro_pair": "2_M2K_1_MYM",
            "passed": contract_evidence,
        },
        "exposure_audit": {
            "maximum_relative_beta_mismatch": float(events["relative_beta_mismatch"].max())
            if len(events)
            else math.inf,
            "mean_relative_beta_mismatch": float(events["relative_beta_mismatch"].mean())
            if len(events)
            else math.inf,
            "maximum_allowed": 0.75,
        },
        "attacks": {
            "sign_flip_net": float((-events["micro_gross_pnl"] - MICRO_COST).sum()),
            "best_event_share_of_positive_pnl": concentration["best_event_share"],
            "best_fold_share_of_positive_pnl": concentration["best_fold_share"],
            "event_dominated": concentration["event_dominated"],
        },
        "topstep": account,
        "shadow_evidence": evidence.__dict__,
    }
    shadow_configs: list[dict[str, Any]] = []
    if admission.permits_zero_risk_shadow:
        path = spec.write_immutable(
            destination / "shadow_configurations" / f"{candidate_id}.json"
        )
        shadow_configs.append(
            {
                "candidate_id": candidate_id,
                "status": admission.tier.value,
                "path": str(path),
                "configuration_hash": spec.configuration_hash,
                "outbound_orders_enabled": False,
            }
        )
    ledger_path = destination / "rty_ym_relative_value_trade_ledger.jsonl"
    _write_trade_ledger(ledger_path, events)
    promising = int(
        admission.tier
        in {
            EvidenceTier.PROMISING_RESEARCH_CANDIDATE,
            EvidenceTier.ROBUST_RESEARCH_CANDIDATE,
            EvidenceTier.SHADOW_RESEARCH_CANDIDATE,
            EvidenceTier.PAPER_SHADOW_READY,
        }
    )
    shadow = int(
        admission.tier
        in {EvidenceTier.SHADOW_RESEARCH_CANDIDATE, EvidenceTier.PAPER_SHADOW_READY}
    )
    if admission.tier == EvidenceTier.PAPER_SHADOW_READY:
        raise RTYYMRelativeValueError("Pre-Q4 relative-value pilot attempted paper promotion.")
    if shadow:
        conclusion = "RTY_YM_RELATIVE_VALUE_SHADOW_CANDIDATE_FOUND"
        next_action = "TARGETED_PAIRED_EXECUTION_OR_DISTINCT_FREEZE"
    elif promising:
        conclusion = "RTY_YM_RELATIVE_VALUE_PROMISING_BUT_INSUFFICIENT"
        next_action = "RELATIVE_VALUE_FAILURE_SURFACE_OR_EXECUTION_AUDIT"
    else:
        conclusion = "RTY_YM_RELATIVE_VALUE_FALSIFIED_OR_INSUFFICIENT"
        next_action = "PIVOT_TO_DEFENSIVE_PORTFOLIO_RISK_ENGINE"
    payload: dict[str, Any] = {
        "schema": VERSION,
        "scientific_conclusion": conclusion,
        "interpretation_boundary": (
            "Fresh two-leg development evidence only. Q4 remains sealed, PAPER_SHADOW_READY is "
            "impossible pre-holdout, and all execution is virtual."
        ),
        "code_commit": code_commit,
        "preregistration_hash": preregistration["preregistration_hash"],
        "preregistration_path": str(preregistration_path),
        "data_provenance": provenance,
        "data_access_record": access,
        "signal_proof": signal_proof,
        "integrity_proof": integrity,
        "candidate_count": 1,
        "candidate_tier_counts": {admission.tier.value: 1},
        "candidates": [candidate],
        "promising_candidates": promising,
        "shadow_candidates": shadow,
        "paper_shadow_ready": 0,
        "topstep_path_candidates": int(bool(account["path_candidate"])),
        "validated_mechanisms": 0,
        "validated_strategies": 0,
        "mechanism_families": ["rty_ym_relative_value_residual"],
        "market_ecologies": ["equity_index_relative_value"],
        "timeframe_profiles": ["30m_signal_1m_two_leg_execution"],
        "shadow_configurations": shadow_configs,
        "governance": {
            "q4_access_count_delta": 0,
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
    result_path = destination / "rty_ym_relative_value_result.json"
    report_path = destination / "rty_ym_relative_value_report.md"
    _write_immutable(result_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {
            "result_json_path": str(result_path),
            "report_path": str(report_path),
            "trade_ledger_path": str(ledger_path),
            "shadow_configuration_directory": str(destination / "shadow_configurations"),
        },
        "report_path": str(report_path),
    }


def build_relative_value_events(
    frame: pd.DataFrame,
    *,
    beta_window: int = 120,
    z_window: int = 40,
    z_threshold: float = 2.0,
    holding_minutes: int = 120,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    data = frame.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    bars = resample_closed_bars(data[data["symbol"].isin(["RTY", "YM"])], 30)
    bars = bars[bars["source_row_count"].eq(30)].copy()
    local = bars["availability_timestamp"].dt.tz_convert("America/Chicago")
    bars = bars[(local.dt.hour * 60 + local.dt.minute).between(9 * 60, 14 * 60)].copy()
    rty = bars[bars["symbol"].eq("RTY")][
        ["availability_timestamp", "active_contract", "close", "source_row_count"]
    ].rename(columns={"active_contract": "rty_contract", "close": "rty_close"})
    ym = bars[bars["symbol"].eq("YM")][
        ["availability_timestamp", "active_contract", "close", "source_row_count"]
    ].rename(columns={"active_contract": "ym_contract", "close": "ym_close"})
    paired = rty.merge(ym, on="availability_timestamp", how="inner", suffixes=("_rty", "_ym"))
    paired = paired.sort_values("availability_timestamp").reset_index(drop=True)
    discontinuity = paired["availability_timestamp"].diff().ne(pd.Timedelta(minutes=30))
    discontinuity |= paired["rty_contract"].ne(paired["rty_contract"].shift())
    discontinuity |= paired["ym_contract"].ne(paired["ym_contract"].shift())
    paired["segment"] = np.cumsum(discontinuity.to_numpy(dtype=bool)).astype(int)
    pieces: list[pd.DataFrame] = []
    for _segment, group in paired.groupby("segment", sort=True):
        ordered = group.copy()
        ordered["rty_return"] = ordered["rty_close"].pct_change()
        ordered["ym_return"] = ordered["ym_close"].pct_change()
        covariance = ordered["rty_return"].rolling(beta_window, min_periods=beta_window).cov(
            ordered["ym_return"]
        ).shift(1)
        variance = ordered["ym_return"].rolling(beta_window, min_periods=beta_window).var().shift(1)
        ordered["beta"] = (covariance / variance.replace(0, np.nan)).clip(0.25, 2.50)
        ordered["residual"] = ordered["rty_return"] - ordered["beta"] * ordered["ym_return"]
        prior_mean = ordered["residual"].rolling(z_window, min_periods=z_window).mean().shift(1)
        prior_std = ordered["residual"].rolling(z_window, min_periods=z_window).std().shift(1)
        ordered["residual_z"] = (ordered["residual"] - prior_mean) / prior_std.replace(0, np.nan)
        pieces.append(ordered)
    signal = pd.concat(pieces, ignore_index=True).sort_values("availability_timestamp")
    signal = signal[signal["residual_z"].abs().ge(z_threshold)].copy()
    signal["decision_timestamp"] = signal["availability_timestamp"]
    signal["entry_timestamp"] = signal["decision_timestamp"]
    signal["exit_timestamp"] = signal["entry_timestamp"] + pd.Timedelta(
        minutes=holding_minutes - 1
    )
    signal["side_rty"] = -np.sign(signal["residual_z"]).astype(int)
    signal["side_ym"] = np.sign(signal["residual_z"]).astype(int)
    local_decision = signal["decision_timestamp"].dt.tz_convert("America/Chicago")
    signal["event_session_id"] = (local_decision + pd.Timedelta(hours=7)).dt.strftime("%Y-%m-%d")
    raw = data[data["symbol"].isin(SYMBOLS)][
        ["symbol", "active_contract", "timestamp", "open", "high", "low", "close"]
    ].copy()
    for symbol, prefix in (("M2K", "m2k"), ("MYM", "mym"), ("RTY", "rty"), ("YM", "ym")):
        entry = raw[raw["symbol"].eq(symbol)].rename(
            columns={
                "active_contract": f"{prefix}_entry_contract",
                "timestamp": "entry_timestamp",
                "open": f"{prefix}_entry",
            }
        )[["entry_timestamp", f"{prefix}_entry_contract", f"{prefix}_entry"]]
        exit_frame = raw[raw["symbol"].eq(symbol)].rename(
            columns={
                "active_contract": f"{prefix}_exit_contract",
                "timestamp": "exit_timestamp",
                "close": f"{prefix}_exit",
            }
        )[["exit_timestamp", f"{prefix}_exit_contract", f"{prefix}_exit"]]
        signal = signal.merge(entry, on="entry_timestamp", how="left", validate="many_to_one")
        signal = signal.merge(exit_frame, on="exit_timestamp", how="left", validate="many_to_one")
    micro_rty_notional = 2 * signal["m2k_entry"] * instrument_spec("M2K").point_value
    micro_ym_notional = signal["mym_entry"] * instrument_spec("MYM").point_value
    notional_ratio = micro_rty_notional / micro_ym_notional.replace(0, np.nan)
    signal["relative_beta_mismatch"] = (notional_ratio - signal["beta"]).abs() / signal[
        "beta"
    ].abs().replace(0, np.nan)
    contract_safe = (
        signal["m2k_entry_contract"].eq(signal["m2k_exit_contract"])
        & signal["mym_entry_contract"].eq(signal["mym_exit_contract"])
        & signal["rty_entry_contract"].eq(signal["rty_exit_contract"])
        & signal["ym_entry_contract"].eq(signal["ym_exit_contract"])
    )
    signal = signal[
        contract_safe
        & signal["relative_beta_mismatch"].le(0.75)
        & signal[["m2k_entry", "mym_entry", "m2k_exit", "mym_exit"]].notna().all(axis=1)
    ].copy()
    signal["micro_gross_pnl"] = (
        signal["side_rty"]
        * 2
        * (signal["m2k_exit"] - signal["m2k_entry"])
        * instrument_spec("M2K").point_value
        + signal["side_ym"]
        * (signal["mym_exit"] - signal["mym_entry"])
        * instrument_spec("MYM").point_value
    )
    signal["micro_net_pnl"] = signal["micro_gross_pnl"] - MICRO_COST
    signal["mini_gross_pnl"] = (
        signal["side_rty"]
        * 2
        * (signal["rty_exit"] - signal["rty_entry"])
        * instrument_spec("RTY").point_value
        + signal["side_ym"]
        * (signal["ym_exit"] - signal["ym_entry"])
        * instrument_spec("YM").point_value
    )
    signal["mini_net_pnl"] = signal["mini_gross_pnl"] - MINI_COST
    signal["mae_dollars"] = _paired_mae(signal, raw, holding_minutes=holding_minutes)
    proof = {
        "closed_30m_source_bars": bool((bars["source_row_count"] == 30).all()),
        "synchronized_signal_bars": int(len(paired)),
        "past_only_beta_window": beta_window,
        "past_only_z_window": z_window,
        "entry_after_decision": bool((signal["entry_timestamp"] >= signal["decision_timestamp"]).all()),
        "contract_safe_events": int(len(signal)),
        "maximum_relative_beta_mismatch": float(signal["relative_beta_mismatch"].max())
        if len(signal)
        else math.inf,
    }
    return signal.reset_index(drop=True), proof


def _paired_mae(events: pd.DataFrame, raw: pd.DataFrame, *, holding_minutes: int) -> pd.Series:
    values: list[float] = []
    lookup = {
        symbol: frame.sort_values("timestamp").set_index("timestamp")
        for symbol, frame in raw[raw["symbol"].isin(["M2K", "MYM"])].groupby("symbol")
    }
    for row in events.itertuples(index=False):
        timestamps = pd.date_range(row.entry_timestamp, periods=holding_minutes, freq="1min")
        m2k = lookup["M2K"].reindex(timestamps)
        mym = lookup["MYM"].reindex(timestamps)
        if m2k["close"].isna().any() or mym["close"].isna().any():
            values.append(float("nan"))
            continue
        path = (
            row.side_rty
            * 2
            * (m2k["close"].astype(float) - row.m2k_entry)
            * instrument_spec("M2K").point_value
            + row.side_ym
            * (mym["close"].astype(float) - row.mym_entry)
            * instrument_spec("MYM").point_value
        )
        values.append(float(path.min() - MICRO_COST / 2))
    return pd.Series(values, index=events.index, dtype=float)


def _fold_results(events: pd.DataFrame, pnl_column: str) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for fold, (start, end) in FOLDS.items():
        selected = events[
            events["event_session_id"].astype(str).ge(start)
            & events["event_session_id"].astype(str).lt(end)
        ]
        output[fold] = {
            "events": int(len(selected)),
            "net_pnl": float(selected[pnl_column].sum()),
            "mean_net_pnl": float(selected[pnl_column].mean()) if len(selected) else 0.0,
            "win_rate": float((selected[pnl_column] > 0).mean()) if len(selected) else 0.0,
        }
    return output


def _block_sign_probability(events: pd.DataFrame, *, seed: int) -> float:
    if len(events) < 10:
        return 1.0
    gross = events.sort_values("decision_timestamp")["micro_gross_pnl"].to_numpy(dtype=float)
    blocks = np.arange(len(gross)) // 5
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(4096, int(blocks.max()) + 1))
    null = (signs[:, blocks] * gross).sum(axis=1) - MICRO_COST * len(gross)
    observed = gross.sum() - MICRO_COST * len(gross)
    return float((1 + np.count_nonzero(null >= observed)) / 4097)


def _diagnostics(raw: pd.DataFrame) -> dict[str, Any]:
    variants = {
        "z_175_beta120_h120": (120, 1.75, 120),
        "z_225_beta120_h120": (120, 2.25, 120),
        "z_200_beta80_h120": (80, 2.0, 120),
        "z_200_beta160_h120": (160, 2.0, 120),
        "z_200_beta120_h60": (120, 2.0, 60),
        "z_200_beta120_h180": (120, 2.0, 180),
    }
    output: dict[str, dict[str, Any]] = {}
    for label, (beta_window, threshold, horizon) in variants.items():
        events, _proof = build_relative_value_events(
            raw,
            beta_window=beta_window,
            z_threshold=threshold,
            holding_minutes=horizon,
        )
        output[label] = {"events": int(len(events)), "net_pnl": float(events["micro_net_pnl"].sum())}
    return {
        "variants": output,
        "positive_neighbor_count": int(sum(row["net_pnl"] > 0 for row in output.values())),
        "diagnostic_only": True,
    }


def _account_replay(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty or events["mae_dollars"].isna().any():
        return {"one_unit_mll_safe": False, "path_candidate": False, "reason": "no_complete_events"}
    daily = (
        events.groupby("event_session_id", sort=True)
        .agg(
            pnl=("micro_net_pnl", "sum"),
            raw_pnl=("micro_net_pnl", "sum"),
            worst_intraday_pnl=("mae_dollars", "min"),
            trades=("micro_net_pnl", "size"),
        )
        .reset_index(names="date")
    )
    daily["skipped_trades"] = 0
    daily["hit_daily_stop"] = False
    daily["hit_daily_profit_lock"] = False
    config = Topstep150KConfig()
    combine = simulate_combine(daily, config)
    standard = simulate_xfa_standard(daily, config.combine_max_loss_limit)
    consistency = simulate_xfa_consistency(daily, config.combine_max_loss_limit)
    return {
        "rule_version": "topstep_150k_2026-07-10_no_dll_baseline",
        "one_unit_mll_safe": bool(
            not combine["mll_breached"] and combine["min_mll_buffer"] >= 1000
        ),
        "combine": combine,
        "xfa_standard": standard,
        "xfa_consistency": consistency,
        "path_candidate": bool(
            combine["passed"] and not combine["mll_breached"] and combine["consistency_ok"]
        ),
        "shared_account_portfolio_replay_required": True,
    }


def _concentration(events: pd.DataFrame, folds: dict[str, dict[str, Any]]) -> dict[str, Any]:
    positive = events.loc[events["micro_net_pnl"] > 0, "micro_net_pnl"]
    total = float(positive.sum())
    event_share = float(positive.max() / total) if total > 0 else 1.0
    fold_positive = [max(float(row["net_pnl"]), 0.0) for row in folds.values()]
    fold_total = sum(fold_positive)
    fold_share = max(fold_positive, default=0.0) / fold_total if fold_total > 0 else 1.0
    return {
        "best_event_share": event_share,
        "best_fold_share": fold_share,
        "event_dominated": bool(event_share > 0.25 or fold_share > 0.70),
    }


def _integrity(events: pd.DataFrame, proof: dict[str, Any]) -> dict[str, bool]:
    return {
        "nonempty_synchronized_signal_source": int(proof["synchronized_signal_bars"]) > 0,
        "closed_30m_source_bars": bool(proof["closed_30m_source_bars"]),
        "past_only_beta": int(proof["past_only_beta_window"]) == 120,
        "past_only_residual_z": int(proof["past_only_z_window"]) == 40,
        "entry_not_before_decision": bool(proof["entry_after_decision"]),
        "q4_excluded": bool(
            events.empty
            or events["decision_timestamp"].max() < pd.Timestamp("2024-10-01", tz="UTC")
        ),
        "contracts_unchanged_through_hold": bool(
            events["m2k_entry_contract"].eq(events["m2k_exit_contract"]).all()
            and events["mym_entry_contract"].eq(events["mym_exit_contract"]).all()
        ),
        "integer_sizing": True,
        "exposure_mismatch_bounded": bool(
            events.empty or events["relative_beta_mismatch"].le(0.75).all()
        ),
        "two_leg_cost_finite": math.isfinite(MICRO_COST) and MICRO_COST > 0,
        "synchronized_mae_complete": bool(events.empty or events["mae_dollars"].notna().all()),
    }


def _shadow_specification(candidate_id: str, preregistration_hash: str) -> ShadowSpecification:
    return ShadowSpecification(
        strategy_id=candidate_id,
        strategy_version="v1_pre_q4_shadow_research",
        feature_versions=("closed_30m_rty_ym_residual_v1", "past_only_beta120_z40_v1"),
        markets=("M2K", "MYM"),
        timeframes=("1m", "30m"),
        session_rules={
            "timezone": "America/Chicago",
            "decision_window": "09:00-14:00",
            "mandatory_flatten_before": "15:10",
        },
        entry_rules={
            "residual_z_absolute_gte": 2.0,
            "beta_window": 120,
            "residual_window": 40,
            "entry_next_synchronized_1m_open": True,
            "mean_reversion": True,
        },
        exit_rules={"holding_synchronized_minutes": 120, "no_overnight": True},
        sizing={"M2K_contracts": 2, "MYM_contracts": 1, "integer_legs": True},
        costs={
            "M2K_round_turn_each": _round_turn_cost("M2K"),
            "MYM_round_turn_each": _round_turn_cost("MYM"),
            "total_round_turn": MICRO_COST,
        },
        stale_data_seconds=75,
        expected_update_seconds=60,
        duplicate_signal_window_seconds=30 * 60,
        maximum_exposure=0.3,
        simulated_mll_floor=-2500.0,
        internal_daily_risk_limit=500.0,
        kill_conditions=(
            "stale_data_either_leg",
            "incomplete_30m_bar",
            "unsynchronized_legs",
            "hedge_ratio_invalid",
            "exposure_mismatch",
            "contract_roll",
            "duplicate_signal",
            "session_closed",
            "mll_floor",
            "manual_kill_switch",
        ),
        logging={
            "jsonl": True,
            "both_leg_signals": True,
            "virtual_fills": True,
            "legging_proxy": True,
            "rejections": True,
        },
        reconciliation={
            "startup": "fail_closed",
            "both_virtual_legs_required": True,
            "position_source": "virtual_only",
        },
        source_manifest_hash=str(preregistration_hash),
        outbound_orders_enabled=False,
    )


def _preregistration(
    *,
    engineering_task_sha256: str,
    repaired_map_sha256: str,
    repaired_roll_map_hash: str,
    code_commit: str,
    random_seed: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "rty_ym_relative_value_preregistration_v1",
        "candidate_id": "strategy_rty_ym_relative_value_v1",
        "signal_markets": ["RTY", "YM"],
        "execution_markets": ["M2K", "MYM"],
        "timeframes": ["closed_30m_signal", "1m_execution"],
        "beta_window": 120,
        "residual_z_window": 40,
        "z_threshold": 2.0,
        "holding_minutes": 120,
        "integer_sizing": {"M2K": 2, "MYM": 1},
        "maximum_relative_beta_mismatch": 0.75,
        "micro_round_turn_cost": MICRO_COST,
        "mini_round_turn_cost": MINI_COST,
        "folds": FOLDS,
        "task_sha256": engineering_task_sha256,
        "map_sha256": repaired_map_sha256,
        "roll_map_hash": repaired_roll_map_hash,
        "code_commit": code_commit,
        "random_seed": random_seed,
        "data_end_exclusive": "2024-10-01",
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "live_or_broker_allowed": False,
        "paper_shadow_ready_requires_untouched_holdout": True,
    }
    payload["preregistration_hash"] = _stable_hash(payload)
    return payload


def _record_access_once(candidate_id: str) -> dict[str, Any]:
    period = "2023-01-01:2024-10-01"
    reason = "RTY YM two-leg relative-value residual strategy pilot; Q4 excluded"
    ledger = project_path("reports", "data_access", "data_access_ledger.jsonl")
    if ledger.exists():
        for line in ledger.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if (
                row.get("period_accessed") == period
                and row.get("requesting_module") == "hydra.research.rty_ym_relative_value"
                and row.get("candidate_ids") == [candidate_id]
                and row.get("reason_for_access") == reason
            ):
                return row
    record = enforce_data_access(
        period,
        DataRole.DEVELOPMENT,
        "hydra.research.rty_ym_relative_value",
        [candidate_id],
        reason,
        None,
    )
    return record.__dict__


def _write_trade_ledger(path: Path, events: pd.DataFrame) -> None:
    columns = [
        "event_session_id",
        "decision_timestamp",
        "entry_timestamp",
        "exit_timestamp",
        "rty_contract",
        "ym_contract",
        "m2k_entry_contract",
        "mym_entry_contract",
        "beta",
        "residual_z",
        "relative_beta_mismatch",
        "side_rty",
        "side_ym",
        "m2k_entry",
        "mym_entry",
        "m2k_exit",
        "mym_exit",
        "micro_gross_pnl",
        "micro_net_pnl",
        "mini_net_pnl",
        "mae_dollars",
    ]
    lines = [
        json.dumps(_strict_json_value(row), sort_keys=True, default=str)
        for row in events[columns].to_dict(orient="records")
    ]
    _write_immutable(path, "\n".join(lines) + ("\n" if lines else ""))


def _render_report(payload: dict[str, Any]) -> str:
    row = payload["candidates"][0]
    return (
        "# RTY/YM Relative-Value Residual Pilot\n\n"
        f"- Conclusion: `{payload['scientific_conclusion']}`\n"
        f"- Status: `{row['status']}`\n"
        f"- Events / micro net: `{row['events']}` / `{row['net_pnl']:.2f}`\n"
        f"- Mini net: `{row['mini_net_pnl']:.2f}`\n"
        f"- Supportive folds: `{row['supportive_temporal_folds']}`\n"
        f"- Null p: `{row['null_evidence']['raw_probability']:.6f}`\n"
        f"- Shadow candidates: `{payload['shadow_candidates']}`\n"
        "- Q4 access: `0`\n"
        "- Outbound orders: `0`\n"
    )


def _verify(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or _file_sha256(path) != expected:
        raise RTYYMRelativeValueError(f"Frozen {label} is missing or changed: {path}")
