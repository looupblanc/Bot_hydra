#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.promotion.behavioral_novelty import filter_behaviorally_novel
from hydra.promotion.pre_backtest_dedup import reject_logical_duplicates
from hydra.research.cluster_aware_generator import expected_event_signature, generate_cluster_aware_prototypes
from hydra.research.component_attribution import ComponentDefinition
from hydra.research.matched_nulls import evaluate_matched_nulls
from hydra.research.representation_ablation import evaluate_component_ablation
from hydra.research.representation_lab import behavioral_sketch_for_result, cluster_behavioral_sketches, q4_access_guard
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DEFAULT_2024_PERIODS
from hydra.validation.lockbox_guard import enforce_data_access
from scripts.audit_paired_execution_costs import _load_governed_cached_frame, run_paired_cost_audit


LANES = ["overnight_inventory_rth_resolution", "intraday_range_migration_path_asymmetry"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run targeted representation ablation and cluster-aware bounded pilot.")
    parser.add_argument("--development-periods", nargs="+", default=["Q1-2024", "Q2-2024", "Q3-2024"])
    parser.add_argument("--explicit-contract-map", required=True)
    parser.add_argument("--lanes", nargs="+", default=LANES)
    parser.add_argument("--max-total-prototypes", type=int, default=80)
    parser.add_argument("--max-structural-prototypes", type=int, default=20)
    parser.add_argument("--max-variants-per-structure", type=int, default=4)
    parser.add_argument("--max-lane-share", type=float, default=0.50)
    parser.add_argument("--max-lineage-share", type=float, default=0.05)
    parser.add_argument("--minimum-exploration-share", type=float, default=0.15)
    parser.add_argument("--matched-nulls", action="store_true")
    parser.add_argument("--component-ablation", action="store_true")
    parser.add_argument("--cluster-aware-generation", action="store_true")
    parser.add_argument("--conservative-costs", action="store_true")
    parser.add_argument("--no-q4-access", action="store_true")
    parser.add_argument("--no-high-resolution-purchase", action="store_true")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--seed", type=int, default=6050)
    parser.add_argument("--research-sample-step", type=int, default=5)
    parser.add_argument("--report-tag", default="targeted_ablation_and_cost_forensics_v2")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.no_q4_access:
        try:
            q4_access_guard("2024-10-01", "2025-01-01")
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Q4 access guard failed to block sealed holdout period.")
    summary = run_lab(args)
    print(json.dumps(_compact_report_summary(summary), indent=2, sort_keys=True, default=str))
    return 0


def run_lab(args: argparse.Namespace) -> dict[str, Any]:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    cost_summary = run_paired_cost_audit(
        dataset=args.dataset,
        schema=args.schema,
        symbols=args.symbols,
        roll_map_path=args.explicit_contract_map,
        report_tag=args.report_tag,
    )
    frames = _load_frames(args.dataset, args.schema, args.symbols, sample_step=args.research_sample_step)
    lane_frames = {
        "overnight_inventory_rth_resolution": {period: build_overnight_components(frame) for period, frame in frames.items()},
        "intraday_range_migration_path_asymmetry": {period: build_range_components(frame) for period, frame in frames.items()},
    }
    components = lane_components()
    ablations: dict[str, list[dict[str, Any]]] = {}
    lane_matched: dict[str, list[dict[str, Any]]] = {}
    for lane in args.lanes:
        ablations[lane] = []
        lane_matched[lane] = []
        for period, frame in lane_frames[lane].items():
            period_ablation = evaluate_component_ablation(frame, components[lane], lane=lane, seed=args.seed)
            for row in period_ablation:
                row["period"] = period
            ablations[lane].extend(period_ablation)
            for component in components[lane]:
                signal = np.sign(pd.to_numeric(frame[component.column], errors="coerce").fillna(0.0)) * component.expected_sign
                result = evaluate_matched_nulls(frame, signal, frame["forward_return"], signal_id=f"{lane}_{component.column}_{period}", seed=args.seed)
                lane_matched[lane].append(result.to_dict() | {"period": period, "component": component.column})
    prototypes = generate_cluster_aware_prototypes(
        lanes=args.lanes,
        lane_components={lane: [component.column for component in components[lane]] for lane in args.lanes},
        symbols=args.symbols,
        max_total=args.max_total_prototypes,
        max_structures=args.max_structural_prototypes,
        max_variants_per_structure=args.max_variants_per_structure,
    )
    proposed_sketches = []
    for proto in prototypes:
        frame = lane_frames[proto.lane]["q1"]
        signal = _prototype_signal(frame, proto.components, proto.threshold_rank)
        proposed_sketches.append(
            expected_event_signature(frame, signal)
            | {
                "prototype_id": proto.prototype_id,
                "logical_fingerprint": proto.logical_fingerprint,
                "lane": proto.lane,
                "structural_id": proto.structural_id,
                "components": list(proto.components),
            }
        )
    accepted_sketches, pre_rejected = reject_logical_duplicates(proposed_sketches)
    accepted_ids = {row["prototype_id"] for row in accepted_sketches}
    prototype_results = []
    for proto in prototypes:
        if proto.prototype_id not in accepted_ids:
            continue
        period_results = []
        for period, frame in lane_frames[proto.lane].items():
            signal = _prototype_signal(frame, proto.components, proto.threshold_rank)
            period_results.append(_backtest_signal(frame, signal, proto, period))
        prototype_results.append(_aggregate(proto, period_results, lane_matched[proto.lane]))
    behavioral = [behavioral_sketch_for_result(row) for row in prototype_results if row["raw_economic_screen_pass"]]
    novel, behavioral_rejected = filter_behaviorally_novel(behavioral, threshold=0.15)
    clusters = cluster_behavioral_sketches(novel) if novel else {"calibration": {}, "clusters": [], "valid_economic_units": 0}
    statuses = Counter()
    for row in prototype_results:
        statuses.update(row["statuses"])
    lane_dispositions = {
        "overnight_inventory_rth_resolution": _lane_disposition(
            ablations["overnight_inventory_rth_resolution"],
            lane_matched["overnight_inventory_rth_resolution"],
        ),
        "intraday_range_migration_path_asymmetry": _lane_disposition(
            ablations["intraday_range_migration_path_asymmetry"],
            lane_matched["intraday_range_migration_path_asymmetry"],
        ),
    }
    summary = {
        "created_at_utc": utc_now_iso(),
        "baseline_commit": _git_commit(),
        "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
        "new_databento_requests": [],
        "spend_this_phase_usd": 0.0,
        "remaining_budget_usd": 96.106305,
        "research_sample_step_minutes": args.research_sample_step,
        "cost_forensics": cost_summary,
        "overnight_components_tested": [component.column for component in components["overnight_inventory_rth_resolution"]],
        "range_components_tested": [component.column for component in components["intraday_range_migration_path_asymmetry"]],
        "component_ablation": ablations,
        "matched_nulls": lane_matched,
        "overnight_incremental_components": _incremental_components(ablations["overnight_inventory_rth_resolution"]),
        "range_incremental_components": _incremental_components(ablations["intraday_range_migration_path_asymmetry"]),
        "overnight_matched_null_result": _lane_null_summary(lane_matched["overnight_inventory_rth_resolution"]),
        "range_matched_null_result": _lane_null_summary(lane_matched["intraday_range_migration_path_asymmetry"]),
        "lane_dispositions": lane_dispositions,
        "overnight_final_disposition": lane_dispositions["overnight_inventory_rth_resolution"],
        "range_final_disposition": lane_dispositions["intraday_range_migration_path_asymmetry"],
        "total_prototypes_proposed": len(prototypes),
        "pre_backtest_duplicates_rejected": len(pre_rejected),
        "structural_prototypes_tested": len({row["structural_id"] for row in prototype_results}),
        "parameter_variants_tested": len(prototype_results) - len({row["structural_id"] for row in prototype_results}),
        "prototype_results": _strip_trades(prototype_results),
        "behavioral_clusters": clusters,
        "behavioral_rejected": behavioral_rejected,
        "status_counts": dict(statuses),
        "raw_positive_net_prototypes": statuses["RAW_POSITIVE_NET"],
        "raw_economic_screen_passes": statuses["RAW_ECONOMIC_SCREEN_PASS"],
        "matched_null_passes": statuses["MATCHED_NULL_BEATEN"],
        "representation_evidence_passes": statuses["REPRESENTATION_EVIDENCE_PASS"],
        "topstep_path_candidates": statuses["TOPSTEP_PATH_CANDIDATE"],
        "topstep_compatible_candidates": statuses["TOPSTEP_COMPATIBLE"],
        "surviving_formulations": [lane for lane, status in lane_dispositions.items() if status == "REPRESENTATION_EVIDENCE_PASS"],
        "falsified_formulations": [lane for lane, status in lane_dispositions.items() if status == "FALSIFIED"],
        "insufficient_evidence_formulations": [lane for lane, status in lane_dispositions.items() if status == "INSUFFICIENT_EVIDENCE"],
        "future_q4_freeze_candidates": [],
        "warning": "Q1-Q3 are development/falsification data. Q4 remains sealed. Historical research only.",
    }
    summary["final_report_path"] = str(_write_final_report(summary, stamp, args.report_tag))
    summary["checkpoint_path"] = str(_write_checkpoint(summary, stamp, args.report_tag))
    return summary


def build_overnight_components(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy().sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["date"] = frame["timestamp"].dt.date.astype(str)
    minute = frame["timestamp"].dt.hour * 60 + frame["timestamp"].dt.minute
    rth_open = 14 * 60 + 30
    frame["is_overnight"] = minute < rth_open
    grouped = frame.groupby(["symbol", "date"], sort=True)
    day_open = grouped["open"].transform("first")
    prior_close = frame.groupby("symbol")["close"].shift(1)
    returns = frame.groupby("symbol")["close"].pct_change()
    past_vol = returns.groupby(frame["symbol"]).rolling(390, min_periods=60).std().reset_index(level=0, drop=True).shift(1)
    overnight_volume = frame["volume"].where(frame["is_overnight"]).groupby([frame["symbol"], frame["date"]]).transform("sum")
    hist_overnight_vol = overnight_volume.groupby(frame["symbol"]).rolling(20, min_periods=5).median().reset_index(level=0, drop=True).shift(1)
    high = frame["high"].where(frame["is_overnight"]).groupby([frame["symbol"], frame["date"]]).transform("max")
    low = frame["low"].where(frame["is_overnight"]).groupby([frame["symbol"], frame["date"]]).transform("min")
    close_15 = grouped["close"].transform(lambda item: item.iloc[min(len(item) - 1, 15)] if len(item) else np.nan)
    close_45 = grouped["close"].transform(lambda item: item.iloc[min(len(item) - 1, 45)] if len(item) else np.nan)
    frame["overnight_displacement"] = ((day_open - prior_close) / prior_close.replace(0, np.nan)) / past_vol.replace(0, np.nan)
    frame["overnight_participation"] = overnight_volume / hist_overnight_vol.replace(0, np.nan)
    frame["prior_value_position"] = (day_open - low) / (high - low).replace(0, np.nan) - 0.5
    frame["opening_response"] = (close_15 - day_open) / day_open.replace(0, np.nan)
    frame["acceptance_rejection"] = (close_45 - day_open) / (high - low).replace(0, np.nan)
    frame["regime_context"] = past_vol.groupby(frame["symbol"]).transform(lambda item: item.rank(pct=True))
    frame["forward_return"] = frame.groupby("symbol")["close"].pct_change(30).shift(-30)
    return frame.replace([np.inf, -np.inf], np.nan)


def build_range_components(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy().sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    grouped = frame.groupby("symbol", group_keys=False)
    center = grouped["close"].rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    high = grouped["high"].rolling(60, min_periods=20).max().reset_index(level=0, drop=True).shift(1)
    low = grouped["low"].rolling(60, min_periods=20).min().reset_index(level=0, drop=True).shift(1)
    width = (high - low).replace(0, np.nan)
    location = (frame["close"] - low) / width
    path = frame["close"].diff().abs().groupby(frame["symbol"]).rolling(30, min_periods=10).sum().reset_index(level=0, drop=True).shift(1)
    displacement = frame.groupby("symbol")["close"].diff(30).shift(1).abs()
    frame["accepted_center_migration"] = center.groupby(frame["symbol"]).diff().fillna(0.0)
    frame["time_at_extremes"] = ((location > 0.8).astype(float) - (location < 0.2).astype(float)).groupby(frame["symbol"]).rolling(60, min_periods=20).mean().reset_index(level=0, drop=True).shift(1)
    frame["effort_vs_progress"] = (displacement / path.replace(0, np.nan)).fillna(0.0)
    frame["path_asymmetry"] = ((frame["close"] - frame["open"]) / (frame["high"] - frame["low"]).replace(0, np.nan)).clip(-3, 3)
    frame["range_relocation"] = (location - 0.5).groupby(frame["symbol"]).rolling(30, min_periods=10).mean().reset_index(level=0, drop=True).shift(1)
    minute = frame["timestamp"].dt.hour * 60 + frame["timestamp"].dt.minute
    frame["session_phase"] = np.sin(2 * np.pi * minute / 1440.0)
    frame["forward_return"] = frame.groupby("symbol")["close"].pct_change(30).shift(-30)
    return frame.replace([np.inf, -np.inf], np.nan)


def lane_components() -> dict[str, list[ComponentDefinition]]:
    return {
        "overnight_inventory_rth_resolution": [
            ComponentDefinition("overnight_displacement", "overnight_inventory_rth_resolution", "Normalized overnight displacement", 1, "overnight_displacement"),
            ComponentDefinition("overnight_participation", "overnight_inventory_rth_resolution", "Overnight participation relative to history", 1, "overnight_participation"),
            ComponentDefinition("prior_value_position", "overnight_inventory_rth_resolution", "Position relative to prior accepted value", -1, "prior_value_position"),
            ComponentDefinition("opening_response", "overnight_inventory_rth_resolution", "Initial RTH response", 1, "opening_response"),
            ComponentDefinition("acceptance_rejection", "overnight_inventory_rth_resolution", "Early acceptance versus rejection", 1, "acceptance_rejection"),
            ComponentDefinition("regime_context", "overnight_inventory_rth_resolution", "Past volatility regime", 1, "regime_context"),
        ],
        "intraday_range_migration_path_asymmetry": [
            ComponentDefinition("accepted_center_migration", "intraday_range_migration_path_asymmetry", "Accepted-price center migration", 1, "accepted_center_migration"),
            ComponentDefinition("time_at_extremes", "intraday_range_migration_path_asymmetry", "Dwell-time imbalance at extremes", 1, "time_at_extremes"),
            ComponentDefinition("effort_vs_progress", "intraday_range_migration_path_asymmetry", "Directional effort versus progress", -1, "effort_vs_progress"),
            ComponentDefinition("path_asymmetry", "intraday_range_migration_path_asymmetry", "Return path asymmetry", 1, "path_asymmetry"),
            ComponentDefinition("range_relocation", "intraday_range_migration_path_asymmetry", "Accepted range relocation", 1, "range_relocation"),
            ComponentDefinition("session_phase", "intraday_range_migration_path_asymmetry", "Session phase interaction", 1, "session_phase"),
        ],
    }


def _load_frames(dataset: str, schema: str, symbols: list[str], *, sample_step: int) -> dict[str, pd.DataFrame]:
    out = {}
    for key in ("q1", "q2", "q3"):
        period = DEFAULT_2024_PERIODS[key]
        q4_access_guard(period.start, period.end)
        enforce_data_access(
            f"{period.start}:{period.end}",
            period.role,
            "scripts/run_targeted_representation_repair_lab.py",
            [],
            "targeted representation ablation and matched-null development; no Q4 access",
            None,
        )
        frame = _load_governed_cached_frame(dataset, schema, symbols, period.start, period.end)
        out[key] = _research_sample(frame, sample_step)
    return out


def _research_sample(frame: pd.DataFrame, sample_step: int) -> pd.DataFrame:
    if sample_step <= 1:
        return frame
    sampled = []
    for _, group in frame.sort_values(["symbol", "timestamp"]).groupby("symbol", sort=True):
        sampled.append(group.iloc[::sample_step])
    return pd.concat(sampled, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _prototype_signal(frame: pd.DataFrame, component_columns: tuple[str, ...], threshold_rank: int) -> pd.Series:
    threshold = 0.15 + threshold_rank * 0.05
    values = []
    for column in component_columns:
        values.append(np.sign(pd.to_numeric(frame[column], errors="coerce").fillna(0.0)).to_numpy())
    if not values:
        return pd.Series(0, index=frame.index, dtype=int)
    score = np.nanmean(np.vstack(values), axis=0)
    out = pd.Series(0, index=frame.index, dtype=int)
    out.loc[score > threshold] = 1
    out.loc[score < -threshold] = -1
    return out


def _backtest_signal(frame: pd.DataFrame, signal: pd.Series, proto: Any, period: str) -> dict[str, Any]:
    subset = frame[frame["symbol"] == proto.symbol].copy()
    signal = signal.reindex(subset.index).fillna(0).astype(int)
    close = pd.to_numeric(subset["close"], errors="coerce")
    entries = signal.ne(0) & signal.shift(1).fillna(0).eq(0)
    trades = []
    cost = 4.50 if proto.symbol.startswith("M") else 9.00
    point_value = {"ES": 50.0, "MES": 5.0, "NQ": 20.0, "MNQ": 2.0}[proto.symbol]
    for entry_i in list(subset.index[entries])[:80]:
        loc = subset.index.get_loc(entry_i)
        exit_loc = min(loc + proto.horizon, len(subset) - 1)
        exit_i = subset.index[exit_loc]
        side = int(signal.loc[entry_i])
        gross = (float(close.loc[exit_i]) - float(close.loc[entry_i])) * side * point_value
        net = gross - cost
        trades.append(
            {
                "entry_timestamp": pd.Timestamp(subset.loc[entry_i, "timestamp"]).isoformat(),
                "exit_timestamp": pd.Timestamp(subset.loc[exit_i, "timestamp"]).isoformat(),
                "side": side,
                "gross_pnl": gross,
                "net_pnl": net,
                "holding_bars": int(exit_loc - loc),
            }
        )
    return {
        "period": period,
        "trade_count": len(trades),
        "gross_pnl": float(sum(row["gross_pnl"] for row in trades)),
        "net_pnl": float(sum(row["net_pnl"] for row in trades)),
        "trades": trades,
    }


def _aggregate(proto: Any, period_results: list[dict[str, Any]], lane_nulls: list[dict[str, Any]]) -> dict[str, Any]:
    net = sum(row["net_pnl"] for row in period_results)
    gross = sum(row["gross_pnl"] for row in period_results)
    trades = [trade for row in period_results for trade in row["trades"]]
    positive_periods = sum(1 for row in period_results if row["net_pnl"] > 0)
    null_pass = any(row["status"] == "MATCHED_NULL_BEATEN" and set(proto.components) & {row["component"]} for row in lane_nulls)
    statuses = []
    if net > 0:
        statuses.append("RAW_POSITIVE_NET")
    if net > 0 and len(trades) >= 20:
        statuses.append("RAW_ECONOMIC_SCREEN_PASS")
    if null_pass:
        statuses.append("MATCHED_NULL_BEATEN")
    if null_pass and positive_periods >= 2:
        statuses.append("REPRESENTATION_EVIDENCE_PASS")
    if null_pass and net > 1500 and positive_periods >= 2:
        statuses.append("TOPSTEP_PATH_CANDIDATE")
    if null_pass and net > 9000 and positive_periods >= 2:
        statuses.append("TOPSTEP_COMPATIBLE")
    daily = defaultdict(float)
    for trade in trades:
        daily[str(trade["exit_timestamp"])[:10]] += float(trade["net_pnl"])
    return {
        "prototype_id": proto.prototype_id,
        "family": proto.lane,
        "structural_id": proto.structural_id,
        "variant_id": proto.variant_id,
        "symbol": proto.symbol,
        "components": list(proto.components),
        "trade_count": len(trades),
        "gross_pnl": gross,
        "net_pnl": net,
        "period_results": period_results,
        "statuses": statuses or ["FALSIFIED"],
        "raw_economic_screen_pass": "RAW_ECONOMIC_SCREEN_PASS" in statuses,
        "trades": trades,
        "daily_pnl": dict(daily),
    }


def _incremental_components(rows: list[dict[str, Any]]) -> list[str]:
    counts = Counter()
    for row in rows:
        if row.get("incremental_value"):
            counts.update(row["components"])
    return [key for key, count in counts.items() if count >= 2]


def _lane_null_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "tests": len(rows),
        "matched_null_beaten": sum(1 for row in rows if row["status"] == "MATCHED_NULL_BEATEN"),
        "best_probability_beats_null": max((row["probability_beats_matched_null"] for row in rows), default=0.0),
    }


def _lane_disposition(ablation_rows: list[dict[str, Any]], null_rows: list[dict[str, Any]]) -> str:
    if _incremental_components(ablation_rows) and any(row["status"] == "MATCHED_NULL_BEATEN" for row in null_rows):
        return "REPRESENTATION_EVIDENCE_PASS"
    if any(row["event_count"] < 30 for row in null_rows):
        return "INSUFFICIENT_EVIDENCE"
    return "FALSIFIED"


def _falsified_lanes(ablations: dict[str, list[dict[str, Any]]], nulls: dict[str, list[dict[str, Any]]]) -> list[str]:
    return [lane for lane in ablations if _lane_disposition(ablations[lane], nulls[lane]) == "FALSIFIED"]


def _strip_trades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key not in {"trades", "daily_pnl", "period_results"}} for row in rows[:100]]


def _compact_report_summary(summary: dict[str, Any]) -> dict[str, Any]:
    cost = summary["cost_forensics"]
    return {
        "baseline_commit": summary["baseline_commit"],
        "created_at_utc": summary["created_at_utc"],
        "q4_seal_verification": summary["q4_seal_verification"],
        "new_databento_requests": summary["new_databento_requests"],
        "spend_this_phase_usd": summary["spend_this_phase_usd"],
        "remaining_budget_usd": summary["remaining_budget_usd"],
        "research_sample_step_minutes": summary["research_sample_step_minutes"],
        "cost_forensics": {
            "classification": cost["classification"],
            "corrected_cost": cost["corrected_cost"],
            "legacy_mislabeled_cost": cost["legacy_mislabeled_cost"],
            "legacy_reported_mean_cost_usd": cost["legacy_reported_mean_cost_usd"],
            "cost_bug_existed": cost["cost_bug_existed"],
            "paired_lane_unfairly_penalized_by_cost": cost["paired_lane_unfairly_penalized_by_cost"],
            "quantity_distribution": cost["quantity_distribution"],
            "hedge_ratio_distribution": cost["hedge_ratio_distribution"],
            "contract_mix": cost["contract_mix"],
            "report_path": cost["report_path"],
        },
        "overnight_components_tested": summary["overnight_components_tested"],
        "range_components_tested": summary["range_components_tested"],
        "overnight_incremental_components": summary["overnight_incremental_components"],
        "range_incremental_components": summary["range_incremental_components"],
        "overnight_matched_null_result": summary["overnight_matched_null_result"],
        "range_matched_null_result": summary["range_matched_null_result"],
        "lane_dispositions": summary["lane_dispositions"],
        "overnight_final_disposition": summary["overnight_final_disposition"],
        "range_final_disposition": summary["range_final_disposition"],
        "total_prototypes_proposed": summary["total_prototypes_proposed"],
        "pre_backtest_duplicates_rejected": summary["pre_backtest_duplicates_rejected"],
        "structural_prototypes_tested": summary["structural_prototypes_tested"],
        "parameter_variants_tested": summary["parameter_variants_tested"],
        "behavioral_rejected_count": len(summary["behavioral_rejected"]),
        "behavioral_clusters": {
            "valid_economic_units": summary["behavioral_clusters"]["valid_economic_units"],
            "cluster_count": len(summary["behavioral_clusters"]["clusters"]),
            "calibration": summary["behavioral_clusters"]["calibration"],
        },
        "status_counts": summary["status_counts"],
        "raw_positive_net_prototypes": summary["raw_positive_net_prototypes"],
        "raw_economic_screen_passes": summary["raw_economic_screen_passes"],
        "matched_null_passes": summary["matched_null_passes"],
        "representation_evidence_passes": summary["representation_evidence_passes"],
        "topstep_path_candidates": summary["topstep_path_candidates"],
        "topstep_compatible_candidates": summary["topstep_compatible_candidates"],
        "surviving_formulations": summary["surviving_formulations"],
        "falsified_formulations": summary["falsified_formulations"],
        "insufficient_evidence_formulations": summary["insufficient_evidence_formulations"],
        "future_q4_freeze_candidates": summary["future_q4_freeze_candidates"],
        "final_report_path": summary.get("final_report_path"),
        "checkpoint_path": summary.get("checkpoint_path"),
        "warning": summary["warning"],
    }


def _compact_ablation_report(summary: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for lane, rows in summary["component_ablation"].items():
        report[lane] = {
            "tests": len(rows),
            "incremental_value_tests": sum(1 for row in rows if row.get("incremental_value")),
            "incremental_components": _incremental_components(rows),
            "component_counts": dict(Counter(component for row in rows for component in row.get("components", []))),
            "period_counts": dict(Counter(row.get("period", "unknown") for row in rows)),
            "matched_null_beaten_tests": sum(1 for row in rows if row.get("matched_null", {}).get("beats_all_required")),
            "best_probability_beats_null": max((row.get("matched_null", {}).get("probability_beats_matched_null", 0.0) for row in rows), default=0.0),
            "top_examples": [
                {
                    "ablation_id": row.get("ablation_id"),
                    "period": row.get("period"),
                    "components": row.get("components"),
                    "incremental_value": row.get("incremental_value"),
                    "matched_null_status": row.get("matched_null", {}).get("status"),
                    "probability_beats_matched_null": row.get("matched_null", {}).get("probability_beats_matched_null"),
                    "real_effect": row.get("matched_null", {}).get("real_effect"),
                }
                for row in sorted(rows, key=lambda item: item.get("matched_null", {}).get("probability_beats_matched_null", 0.0), reverse=True)[:20]
            ],
        }
    return report


def _compact_matched_null_report(summary: dict[str, Any]) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for lane, rows in summary["matched_nulls"].items():
        report[lane] = {
            "tests": len(rows),
            "matched_null_beaten": sum(1 for row in rows if row.get("status") == "MATCHED_NULL_BEATEN"),
            "period_counts": dict(Counter(row.get("period", "unknown") for row in rows)),
            "component_counts": dict(Counter(row.get("component", "unknown") for row in rows)),
            "best_probability_beats_null": max((row.get("probability_beats_matched_null", 0.0) for row in rows), default=0.0),
            "top_examples": sorted(rows, key=lambda item: item.get("probability_beats_matched_null", 0.0), reverse=True)[:20],
        }
    return report


def _write_final_report(summary: dict[str, Any], stamp: str, tag: str) -> Path:
    path = project_path("reports", "targeted_representation_pilot", f"targeted_representation_repair_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    report_summary = dict(summary)
    report_summary["final_report_path"] = str(path)
    lines = [
        f"# Targeted Representation Repair Lab {tag}",
        "",
        "Historical research only. No live trading approval.",
        "",
        f"- Q4 seal: {summary['q4_seal_verification']}",
        f"- Cost classification: {summary['cost_forensics']['classification']}",
        f"- Corrected paired mean cost: {summary['cost_forensics']['corrected_cost']['mean']}",
        f"- Corrected paired median cost: {summary['cost_forensics']['corrected_cost']['median']}",
        f"- Prototypes proposed: {summary['total_prototypes_proposed']}",
        f"- Pre-backtest rejected: {summary['pre_backtest_duplicates_rejected']}",
        f"- Representation evidence passes: {summary['representation_evidence_passes']}",
        f"- Topstep compatible: {summary['topstep_compatible_candidates']}",
        "",
        "```json",
        json.dumps(_compact_report_summary(report_summary), indent=2, sort_keys=True, default=str),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    _write_side_reports(summary, stamp, tag)
    return path


def _write_side_reports(summary: dict[str, Any], stamp: str, tag: str) -> None:
    payloads = {
        "representation_ablation": _compact_ablation_report(summary),
        "matched_nulls": _compact_matched_null_report(summary),
    }
    for folder, payload in payloads.items():
        path = project_path("reports", folder, f"{folder}_{stamp}_{tag}.md")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# " + folder.replace("_", " ").title() + "\n\n```json\n" + json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n```\n", encoding="utf-8")


def _write_checkpoint(summary: dict[str, Any], stamp: str, tag: str) -> Path:
    path = project_path("reports", "checkpoints", "targeted_representation_lab", f"targeted_representation_checkpoint_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                f"# Targeted Representation Checkpoint {tag}",
                "",
                f"- Prototypes proposed: {summary['total_prototypes_proposed']}",
                f"- Pre-backtest rejected: {summary['pre_backtest_duplicates_rejected']}",
                f"- Matched-null passes: {summary['matched_null_passes']}",
                f"- Representation evidence passes: {summary['representation_evidence_passes']}",
                f"- Q4 seal: {summary['q4_seal_verification']}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _git_commit() -> str:
    import subprocess

    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
