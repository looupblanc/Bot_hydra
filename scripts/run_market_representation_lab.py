#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.backtest.two_leg_execution import ExecutionMode, build_two_leg_trade
from hydra.data.contract_mapping import load_roll_map
from hydra.data.databento_loader import load_cached_databento_range
from hydra.factory.lineage_tombstone import assert_not_tombstoned, load_default_tombstones
from hydra.representations.opening_auction import OpeningAuctionConfig, opening_auction_features
from hydra.representations.overnight_inventory import OvernightInventoryConfig, overnight_inventory_features
from hydra.representations.paired_relative_value import PairedRelativeValueConfig, build_paired_residual_frame
from hydra.representations.range_migration import RangeMigrationConfig, range_migration_features
from hydra.representations.volatility_shape import VolatilityShapeConfig, volatility_shape_features
from hydra.research.representation_lab import (
    PrototypeSpec,
    RepresentationSpec,
    behavioral_sketch_for_result,
    cluster_behavioral_sketches,
    evaluate_feature_evidence,
    generate_bounded_prototypes,
    prototype_backtest,
    q4_access_guard,
    summarize_representation_evidence,
    write_behavioral_evidence,
)
from hydra.risk.pair_risk import directional_beta_audit, integer_hedge_ratio
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DEFAULT_2024_PERIODS
from hydra.validation.lockbox_guard import enforce_data_access


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA's bounded market representation lab.")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--roll-map-path", default="")
    parser.add_argument("--total-prototypes", type=int, default=150)
    parser.add_argument("--seed", type=int, default=4050)
    parser.add_argument("--report-tag", default="bounded_market_representation_pilot_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    timestamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    periods = {
        "q1": DEFAULT_2024_PERIODS["q1"],
        "q2": DEFAULT_2024_PERIODS["q2"],
        "q3": DEFAULT_2024_PERIODS["q3"],
    }
    for period in periods.values():
        q4_access_guard(period.start, period.end)
    roll_map_path = Path(args.roll_map_path) if args.roll_map_path else _latest_explicit_roll_map("2024-10-01")
    roll_map = load_roll_map(roll_map_path)
    specs, rejected = select_representations()
    tombstones = load_default_tombstones()
    for spec in specs:
        assert_not_tombstoned(
            {
                "candidate_id": spec.name,
                "family": spec.name,
                "parameters": _prototype_markers(spec.name),
            },
            tombstones,
        )
    frames = {}
    data_access_records = []
    for key, period in periods.items():
        data_access_records.append(
            enforce_data_access(
                f"{period.start}:{period.end}",
                period.role,
                "scripts/run_market_representation_lab.py",
                [],
                "market representation development/falsification; Q4 not accessed",
                None,
            )
        )
        frames[key] = _load_governed_cached_frame(args.dataset, args.schema, args.symbols, period.start, period.end)
    feature_builders = _feature_builders(roll_map)
    features: dict[str, dict[str, pd.DataFrame]] = defaultdict(dict)
    feature_evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for spec in specs:
        builder = feature_builders[spec.name]
        for period_name, frame in frames.items():
            feat = builder(frame)
            feat["period"] = period_name
            features[spec.name][period_name] = feat
            feature_evidence[spec.name].append(
                evaluate_feature_evidence(feat, period_name=period_name, seed=args.seed)
            )
    prototypes = generate_bounded_prototypes(specs, total_cap=args.total_prototypes, seed=args.seed)
    prototype_results: list[dict[str, Any]] = []
    for proto in prototypes:
        period_results = []
        for period_name, feat in features[proto.family].items():
            if proto.family == "roll_aware_beta_neutral_nq_es_residual_divergence":
                result = paired_prototype_backtest(feat, proto, period_name=period_name)
            else:
                result = prototype_backtest(feat, proto)
                result["period"] = period_name
            period_results.append(result)
        prototype_results.append(_aggregate_period_results(proto, period_results))
    viable = [row for row in prototype_results if row["economically_viable"]]
    topstep_compatible = [row for row in prototype_results if row["topstep_compatible"]]
    sketches = [behavioral_sketch_for_result(row) for row in viable]
    evidence_paths = write_behavioral_evidence(f"{timestamp}_{args.report_tag}", sketches, viable) if sketches else {}
    clustering = cluster_behavioral_sketches(sketches) if sketches else {"calibration": {}, "clusters": [], "valid_economic_units": 0}
    family_results = {
        spec.name: summarize_representation_evidence(
            spec.name,
            feature_evidence[spec.name],
            [row for row in prototype_results if row["family"] == spec.name],
        )
        for spec in specs
    }
    pair_results = [row for row in prototype_results if row["family"] == "roll_aware_beta_neutral_nq_es_residual_divergence"]
    directional_beta = _summarize_directional_beta(pair_results)
    summary = {
        "created_at_utc": utc_now_iso(),
        "baseline_commit": _git_commit(),
        "dataset": args.dataset,
        "schema": args.schema,
        "symbols": args.symbols,
        "data_roles": {key: period.role.value for key, period in periods.items()} | {"q4": DEFAULT_2024_PERIODS["q4"].role.value},
        "q4_seal_verification": "PASSED_NO_Q4_ACCESS_BY_LAB",
        "roll_map_path": str(roll_map_path),
        "roll_map_hash": roll_map.roll_map_hash(),
        "roll_map_period": roll_map.source_metadata,
        "selected_representations": [spec.__dict__ for spec in specs],
        "rejected_representations": rejected,
        "feature_level_evidence": feature_evidence,
        "family_dispositions": family_results,
        "null_model_summary": _null_summary(feature_evidence),
        "prototypes_generated": len(prototypes),
        "prototypes_by_family": dict(Counter(proto.family for proto in prototypes)),
        "structural_prototypes": len({proto.structural_id for proto in prototypes}),
        "parameter_variants": len(prototypes) - len({proto.structural_id for proto in prototypes}),
        "economically_viable_prototypes": len(viable),
        "topstep_compatible_prototypes": len(topstep_compatible),
        "prototype_results_top": _top_results(prototype_results),
        "true_paired_candidates": sum(1 for row in pair_results if row["trade_count"] > 0),
        "directional_beta_audit": directional_beta,
        "two_leg_cost_findings": _cost_summary(pair_results),
        "legging_risk_findings": _legging_summary(pair_results),
        "transfer_results": _transfer_summary(prototype_results),
        "behavioral_evidence": evidence_paths,
        "behavioral_clusters": clustering,
        "surviving_representations": [name for name, row in family_results.items() if row["disposition"] == "SURVIVES"],
        "falsified_representations": [name for name, row in family_results.items() if row["disposition"] == "FALSIFIED"],
        "insufficient_evidence_representations": [name for name, row in family_results.items() if row["disposition"] == "INSUFFICIENT_EVIDENCE"],
        "future_q4_freeze_candidates": [row["prototype_id"] for row in prototype_results if row["eligible_for_future_q4_freeze"]],
        "future_tick_tbbo_candidates": [row["prototype_id"] for row in prototype_results if row["requires_future_tick_tbbo"]],
        "databento_spend_this_phase_usd": 0.0,
        "new_databento_requests": [],
        "data_access_records": [record.__dict__ for record in data_access_records],
        "warning": "Historical research only. This is not live trading approval. Q1-Q3 are development/falsification data.",
    }
    report_path = _write_report(summary, timestamp, args.report_tag)
    checkpoint_path = _write_checkpoint(summary, timestamp, args.report_tag)
    print(json.dumps({"report_path": str(report_path), "checkpoint_path": str(checkpoint_path), **summary}, indent=2, sort_keys=True, default=str)[:200000])
    return 0


def select_representations() -> tuple[list[RepresentationSpec], list[dict[str, Any]]]:
    selected = [
        RepresentationSpec(
            "roll_aware_beta_neutral_nq_es_residual_divergence",
            1,
            True,
            "Temporary residual dislocation between synchronized NQ and ES contracts may normalize after beta-neutral shocks.",
            "Past-only hedge-ratio residual, two-leg execution, roll exclusion.",
            "Stable covariance and moderate volatility.",
            "Directional macro trend, beta instability, or roll transition.",
            "Relative-value diversifier if true beta neutrality survives.",
            "high",
            "Mandatory core lane and cleanly falsifiable after old directional proxy was killed.",
        ),
        RepresentationSpec(
            "opening_auction_displacement_failed_continuation",
            2,
            True,
            "Opening auction effort that fails to maintain direction may expose trapped early momentum.",
            "Opening range geometry, effort/progress, failed continuation.",
            "First 60-120 RTH minutes after abnormal opening displacement.",
            "Persistent trend day.",
            "Short-horizon MLL-contained sleeve.",
            "low",
            "Low parameter count and not a renamed indicator.",
        ),
        RepresentationSpec(
            "volatility_shape_transition",
            3,
            True,
            "Realized-volatility shape can distinguish actionable expansion from chop better than simple ATR expansion.",
            "Short/medium/long vol curvature plus path asymmetry.",
            "Compression resolving into directional liquidity.",
            "Headline volatility and whipsaw expansion.",
            "Target-velocity research lane with explicit cost hurdle.",
            "low",
            "Falsifies a structural state, not a stop/target grid.",
        ),
        RepresentationSpec(
            "overnight_inventory_rth_resolution",
            4,
            True,
            "Overnight inventory imbalance may resolve during the regular-session open through acceptance or rejection.",
            "Overnight displacement and early RTH response.",
            "Large overnight move with early cash-session confirmation.",
            "Quiet overnight sessions or news shock opens.",
            "Opening-session climber if costs and concentration are controlled.",
            "medium",
            "Distinct mechanism from paired residuals and volatility shape.",
        ),
        RepresentationSpec(
            "intraday_range_migration_path_asymmetry",
            5,
            True,
            "The migration of accepted price zones through the session may encode continuation/exhaustion states.",
            "Time near rolling extremes and range-location imbalance.",
            "Structured trend or failed-trend days.",
            "Featureless rotation.",
            "Portfolio diversifier / session role.",
            "low",
            "Adds path geometry without broad indicator stacking.",
        ),
    ]
    rejected = [
        {"name": "dynamic_hedge_ratio_relative_value", "reason": "Folded into the core paired lane as hedge-ratio methods, not a separate compute lane."},
        {"name": "cross_market_lead_lag_conditioned_on_vol_regime", "reason": "Deferred until paired residuals survive; high spurious-correlation risk."},
        {"name": "session_transition_state_models", "reason": "Deferred due calendar/session complexity relative to pilot budget."},
        {"name": "failed_directional_expansion_controlled_tail", "reason": "Overlaps with opening and volatility-shape lanes; keep as future ablation."},
        {"name": "mes_mnq_micro_first_portfolio_roles", "reason": "Risk/portfolio sizing role, not a standalone representation until underlying edges survive."},
    ]
    return selected, rejected


def paired_prototype_backtest(frame: pd.DataFrame, prototype: PrototypeSpec, *, period_name: str) -> dict[str, Any]:
    data = frame.copy().sort_values("timestamp").reset_index(drop=True)
    entries = data.index[(data["signal"] != 0) & (data["signal"].shift(1).fillna(0) == 0)]
    trades = []
    beta_daily = []
    for entry_i in list(entries)[:60]:
        exit_i = min(int(entry_i) + 30, len(data) - 1)
        if exit_i <= entry_i:
            continue
        entry = data.iloc[int(entry_i)]
        exit_ = data.iloc[exit_i]
        try:
            hedge = integer_hedge_ratio(
                left_symbol="MNQ",
                right_symbol="MES",
                theoretical_ratio=abs(float(entry["hedge_ratio"])),
                left_price=float(entry["left_close"]),
                right_price=float(entry["right_close"]),
                prefer_micro=True,
            )
        except ValueError:
            continue
        trade = build_two_leg_trade(
            entry_timestamp=entry["timestamp"],
            exit_timestamp=exit_["timestamp"],
            left_symbol="MNQ",
            right_symbol="MES",
            left_quantity=hedge.left_quantity,
            right_quantity=hedge.right_quantity,
            direction=int(entry["signal"]),
            left_entry=float(entry["left_close"]),
            right_entry=float(entry["right_close"]),
            left_exit=float(exit_["left_close"]),
            right_exit=float(exit_["right_close"]),
            mode=ExecutionMode.ATOMIC_CONSERVATIVE,
            slippage_bps=0.5,
        )
        row = trade.to_dict()
        row["period"] = period_name
        row["entry_i"] = int(entry_i)
        row["exit_i"] = int(exit_i)
        row["side"] = int(entry["signal"])
        row["net_pnl"] = trade.net_pnl
        row["gross_pnl"] = trade.gross_pnl
        row["holding_bars"] = int(exit_i - entry_i)
        trades.append(row)
        beta_daily.append({"timestamp": exit_["timestamp"], "pnl": trade.net_pnl, "left_return": exit_["left_return"], "right_return": exit_["right_return"]})
    daily = _daily_pnl_from_trades(trades)
    beta_frame = pd.DataFrame(beta_daily)
    beta = directional_beta_audit(beta_frame.get("pnl", pd.Series(dtype=float)), beta_frame.get("left_return", pd.Series(dtype=float)), beta_frame.get("right_return", pd.Series(dtype=float)))
    net = float(sum(row["net_pnl"] for row in trades))
    gross = float(sum(row["gross_pnl"] for row in trades))
    max_dd = _max_drawdown(daily)
    trade_count = len(trades)
    return {
        "prototype_id": prototype.prototype_id,
        "family": prototype.family,
        "symbol": "MNQ/MES",
        "period": period_name,
        "trade_count": trade_count,
        "gross_pnl": gross,
        "net_pnl": net,
        "max_drawdown": max_dd,
        "economically_viable": bool(trade_count >= 8 and net > 0 and not beta.get("directional_dominance")),
        "topstep_compatible": bool(trade_count >= 8 and net > 750 and max_dd > -1500 and not beta.get("directional_dominance")),
        "requires_future_tick_tbbo": bool(trade_count > 0),
        "directional_beta": beta,
        "trades": trades,
        "daily_pnl": daily,
        "status": "OK" if trade_count else "NO_TRADES",
    }


def _aggregate_period_results(prototype: PrototypeSpec, period_results: list[dict[str, Any]]) -> dict[str, Any]:
    trades = [trade for row in period_results for trade in row.get("trades", [])]
    daily = {}
    for row in period_results:
        for day, pnl in (row.get("daily_pnl") or {}).items():
            daily[f"{row['period']}:{day}"] = float(pnl)
    period_net = {row["period"]: float(row.get("net_pnl", 0.0)) for row in period_results}
    period_trade_count = {row["period"]: int(row.get("trade_count", 0)) for row in period_results}
    net = float(sum(period_net.values()))
    gross = float(sum(float(row.get("gross_pnl", 0.0)) for row in period_results))
    max_dd = min(float(row.get("max_drawdown", 0.0)) for row in period_results) if period_results else 0.0
    positive_periods = sum(1 for value in period_net.values() if value > 0)
    trade_count = sum(period_trade_count.values())
    topstep = bool(positive_periods >= 2 and net > 3000 and max_dd > -2500)
    return {
        "prototype_id": prototype.prototype_id,
        "family": prototype.family,
        "structural_id": prototype.structural_id,
        "variant_id": prototype.variant_id,
        "symbol": prototype.symbol,
        "period_net_pnl": period_net,
        "period_trade_count": period_trade_count,
        "trade_count": int(trade_count),
        "gross_pnl": gross,
        "net_pnl": net,
        "max_drawdown": max_dd,
        "economically_viable": bool(positive_periods >= 2 and net > 0 and trade_count >= 20),
        "topstep_compatible": topstep,
        "eligible_for_future_q4_freeze": bool(topstep and positive_periods == 3),
        "requires_future_tick_tbbo": any(row.get("requires_future_tick_tbbo") for row in period_results),
        "trades": trades,
        "daily_pnl": daily,
        "period_results": period_results,
    }


def _feature_builders(roll_map: Any) -> dict[str, Callable[[pd.DataFrame], pd.DataFrame]]:
    return {
        "roll_aware_beta_neutral_nq_es_residual_divergence": lambda df: build_paired_residual_frame(
            df,
            roll_map,
            PairedRelativeValueConfig(hedge_ratio_method="rolling_ols", hedge_window=120, z_window=120),
        ),
        "opening_auction_displacement_failed_continuation": lambda df: opening_auction_features(df, OpeningAuctionConfig()),
        "volatility_shape_transition": lambda df: volatility_shape_features(df, VolatilityShapeConfig()),
        "overnight_inventory_rth_resolution": lambda df: overnight_inventory_features(df, OvernightInventoryConfig()),
        "intraday_range_migration_path_asymmetry": lambda df: range_migration_features(df, RangeMigrationConfig()),
    }


def _load_governed_cached_frame(dataset: str, schema: str, symbols: list[str], start: str, end: str) -> pd.DataFrame:
    cache_ranges = [
        ("2024-01-01", "2024-03-31"),
        ("2024-04-01", "2024-07-01"),
        ("2024-07-01", "2024-10-01"),
    ]
    requested_start = pd.Timestamp(start, tz="UTC")
    requested_end = pd.Timestamp(end, tz="UTC")
    for cache_start, cache_end in cache_ranges:
        if pd.Timestamp(cache_start, tz="UTC") <= requested_start and pd.Timestamp(cache_end, tz="UTC") >= requested_end:
            frame = load_cached_databento_range(dataset, schema, symbols, cache_start, cache_end)
            timestamps = pd.to_datetime(frame["timestamp"], utc=True)
            return frame[(timestamps >= requested_start) & (timestamps < requested_end)].reset_index(drop=True)
    return load_cached_databento_range(dataset, schema, symbols, start, end)


def _prototype_markers(name: str) -> dict[str, Any]:
    if "nq_es" in name:
        return {
            "left_symbol": "NQ",
            "right_symbol": "ES",
            "hedge_ratio_method": "rolling_ols",
            "two_leg_execution": "ATOMIC_CONSERVATIVE",
            "pair_validity_required": True,
            "beta_neutral": True,
        }
    return {"representation": name}


def _latest_explicit_roll_map(min_end: str) -> Path:
    candidates = []
    for path in project_path("data", "cache", "contract_maps").glob("roll_map_*.json"):
        try:
            roll_map = load_roll_map(path)
        except Exception:
            continue
        if not roll_map.map_type.startswith("EXPLICIT"):
            continue
        period_end = roll_map.source_metadata.get("period_end")
        if period_end and pd.Timestamp(period_end) >= pd.Timestamp(min_end):
            candidates.append(path)
    if not candidates:
        raise FileNotFoundError("No explicit Q1-Q3 roll map found. Run scripts/build_databento_contract_map.py through 2024-10-01 first.")
    return sorted(candidates, key=lambda path: path.stat().st_mtime)[-1]


def _daily_pnl_from_trades(trades: list[dict[str, Any]]) -> dict[str, float]:
    out = defaultdict(float)
    for trade in trades:
        out[str(trade["exit_timestamp"])[:10]] += float(trade["net_pnl"])
    return dict(out)


def _max_drawdown(daily: dict[str, float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for _, pnl in sorted(daily.items()):
        equity += float(pnl)
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return float(drawdown)


def _null_summary(feature_evidence: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    out = {}
    for name, rows in feature_evidence.items():
        ok = [row for row in rows if row.get("status") == "OK"]
        out[name] = {
            "periods": len(rows),
            "beats_null_count": sum(1 for row in ok if row.get("beats_null")),
            "effective_trial_count": len(ok),
            "selection_adjusted_warning": "Pilot uses multiple representation hypotheses; effect sizes are research evidence only.",
        }
    return out


def _top_results(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stripped = []
    for row in sorted(rows, key=lambda item: float(item.get("net_pnl", 0.0)), reverse=True)[:20]:
        stripped.append({key: value for key, value in row.items() if key not in {"trades", "daily_pnl", "period_results"}})
    return stripped


def _summarize_directional_beta(rows: list[dict[str, Any]]) -> dict[str, Any]:
    audits = [period.get("directional_beta") for row in rows for period in row.get("period_results", []) if period.get("directional_beta")]
    return {
        "audits": len(audits),
        "directional_dominance_count": sum(1 for item in audits if item.get("directional_dominance")),
        "finding": "No paired candidate may be called relative value if directional dominance is present.",
    }


def _cost_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    costs = []
    for row in rows:
        for trade in row.get("trades", []):
            costs.append(abs(float(trade.get("gross_pnl", 0.0)) - float(trade.get("net_pnl", 0.0))))
    return {"trade_count": len(costs), "mean_two_leg_cost": round(sum(costs) / max(len(costs), 1), 4), "mode": "ATOMIC_CONSERVATIVE"}


def _legging_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"mode_used_for_conclusions": "ATOMIC_CONSERVATIVE", "sequential_stress_status": "implemented_and_unit_tested_not_used_for_final_viability"}


def _transfer_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = {}
    for period in ("q1", "q2", "q3"):
        out[period] = {
            "positive_net_count": sum(1 for row in rows if row["period_net_pnl"].get(period, 0.0) > 0),
            "trade_count": sum(int(row["period_trade_count"].get(period, 0)) for row in rows),
        }
    return out


def _write_report(summary: dict[str, Any], timestamp: str, tag: str) -> Path:
    path = project_path("reports", "market_representation_lab", f"market_representation_lab_{timestamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Market Representation Lab {tag}",
        "",
        "Historical research only. This is not live trading approval.",
        "",
        f"- Q4 seal: {summary['q4_seal_verification']}",
        f"- Prototypes generated: {summary['prototypes_generated']}",
        f"- Economically viable prototypes: {summary['economically_viable_prototypes']}",
        f"- Topstep-compatible prototypes: {summary['topstep_compatible_prototypes']}",
        f"- Future Q4 freeze candidates: {len(summary['future_q4_freeze_candidates'])}",
        "",
        "```json",
        json.dumps(summary, indent=2, sort_keys=True, default=str)[:180000],
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    _mirror_report(summary, timestamp, tag)
    return path


def _mirror_report(summary: dict[str, Any], timestamp: str, tag: str) -> None:
    mirrors = {
        "representation_falsification": {"family_dispositions": summary["family_dispositions"], "null_model_summary": summary["null_model_summary"]},
        "paired_relative_value": {"directional_beta_audit": summary["directional_beta_audit"], "two_leg_cost_findings": summary["two_leg_cost_findings"]},
        "prototype_pilot": {"prototypes_by_family": summary["prototypes_by_family"], "prototype_results_top": summary["prototype_results_top"]},
    }
    for folder, payload in mirrors.items():
        path = project_path("reports", folder, f"{folder}_{timestamp}_{tag}.md")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# " + folder.replace("_", " ").title() + "\n\n```json\n" + json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n```\n", encoding="utf-8")


def _write_checkpoint(summary: dict[str, Any], timestamp: str, tag: str) -> Path:
    path = project_path("reports", "checkpoints", "market_representation_lab", f"market_representation_checkpoint_{timestamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Market Representation Checkpoint {tag}",
        "",
        f"- Prototypes: {summary['prototypes_generated']}",
        f"- Economically viable: {summary['economically_viable_prototypes']}",
        f"- Topstep compatible: {summary['topstep_compatible_prototypes']}",
        f"- Q4 seal: {summary['q4_seal_verification']}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _git_commit() -> str:
    import subprocess

    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
