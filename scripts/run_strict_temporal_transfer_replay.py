#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.backtest.costs import round_turn_cost
from hydra.data.databento_loader import load_cached_databento_range
from hydra.markets.instruments import instrument_spec
from hydra.propfirm.topstep_150k import InternalRiskOverlay, Topstep150KConfig, evaluate_topstep_150k
from hydra.research.cluster_aware_generator import expected_event_signature, generate_cluster_aware_prototypes
from hydra.research.matched_nulls import evaluate_matched_nulls
from hydra.research.representation_lab import q4_access_guard
from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso
from hydra.validation.data_roles import DEFAULT_2024_PERIODS
from hydra.validation.lockbox_guard import enforce_data_access
from hydra.validation.replay_manifest import FrozenReplayCandidate, build_replay_manifest, write_manifest
from hydra.validation.status_policy import (
    STATUS_POLICY_VERSION,
    candidate_level_null_decision,
    previous_status_semantics,
)
from hydra.validation.temporal_transfer import (
    TEMPORAL_POLICY_VERSION,
    classify_temporal_transfer,
    period_metric_summary,
)
from scripts.audit_paired_execution_costs import _load_governed_cached_frame
from scripts.run_targeted_representation_repair_lab import (
    build_overnight_components,
    build_range_components,
    lane_components,
    _aggregate,
    _backtest_signal,
    _prototype_signal,
    _research_sample,
)
from hydra.promotion.pre_backtest_dedup import reject_logical_duplicates


LANES = ["overnight_inventory_rth_resolution", "intraday_range_migration_path_asymmetry"]
SOURCE_REPORT = "reports/targeted_representation_pilot/targeted_representation_repair_20260710T083333+0000_targeted_ablation_and_cost_forensics_v2.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strict unsampled temporal transfer replay for frozen representation candidates.")
    parser.add_argument("--baseline-commit", required=True)
    parser.add_argument("--periods", nargs="+", default=["Q1-2024", "Q2-2024", "Q3-2024"])
    parser.add_argument("--candidate-source", default="latest_targeted_representation_pilot")
    parser.add_argument("--max-candidates", type=int, default=15)
    parser.add_argument("--freeze-manifest", action="store_true")
    parser.add_argument("--candidate-level-matched-nulls", action="store_true")
    parser.add_argument("--component-necessity-audit", action="store_true")
    parser.add_argument("--sequential-account-replay", action="store_true")
    parser.add_argument("--conservative-topstep", action="store_true")
    parser.add_argument("--no-parameter-mutation", action="store_true")
    parser.add_argument("--no-sizing-optimization", action="store_true")
    parser.add_argument("--no-q4-access", action="store_true")
    parser.add_argument("--no-data-purchase", action="store_true")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--symbols", nargs="+", default=["ES", "MES", "NQ", "MNQ"])
    parser.add_argument("--seed", type=int, default=7050)
    parser.add_argument("--report-tag", default="strict_unsampled_temporal_transfer_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.no_q4_access:
        try:
            q4_access_guard("2024-10-01", "2025-01-01")
        except RuntimeError:
            pass
        else:
            raise RuntimeError("Q4 access guard failed to block sealed holdout.")
    summary = run_replay(args)
    print(json.dumps(_compact_summary(summary), indent=2, sort_keys=True, default=str))
    return 0


def run_replay(args: argparse.Namespace) -> dict[str, Any]:
    stamp = utc_now_iso().replace("-", "").replace(":", "").replace("+00:00", "Z")
    raw_frames = _load_unsampled_frames(args.dataset, args.schema, args.symbols)
    sampled_frames = {name: _research_sample(frame, 5) for name, frame in raw_frames.items()}
    sampled_lane_frames = _build_lane_frames(sampled_frames)
    unsampled_lane_frames = _build_lane_frames(raw_frames)
    prior = _reconstruct_prior_candidates(sampled_lane_frames, args.symbols, args.seed)
    frozen = _select_frozen_candidates(prior, args.max_candidates)
    data_fingerprints = {name: _frame_fingerprint(frame) for name, frame in raw_frames.items()}
    manifest = build_replay_manifest(
        baseline_commit=args.baseline_commit,
        candidates=frozen,
        data_fingerprints=data_fingerprints,
        source_report=SOURCE_REPORT,
        decision_policy_version=STATUS_POLICY_VERSION,
    )
    manifest_path = project_path("reports", "temporal_transfer", f"strict_replay_manifest_{stamp}_{args.report_tag}.json")
    manifest_hash = write_manifest(manifest, manifest_path) if args.freeze_manifest else manifest["manifest_hash"]
    candidate_results = []
    ledgers: dict[str, Any] = {}
    for candidate in frozen:
        result = _replay_candidate(candidate, unsampled_lane_frames, raw_frames, effective_trial_count=len(frozen), seed=args.seed)
        candidate_results.append(result)
        ledgers[candidate.candidate_id] = {period: result["period_results"][period]["trades"] for period in result["period_results"]}
    status_counts = Counter(item["temporal_transfer"]["status"] for item in candidate_results)
    topstep_candidates = [item for item in candidate_results if item["temporal_transfer"]["status"] in {"TEMPORAL_TRANSFER_STRONG", "TEMPORAL_TRANSFER_WEAK"}]
    portfolio = _portfolio_diagnostic(topstep_candidates)
    final_report_path = _write_final_report(
        {
            "created_at_utc": utc_now_iso(),
            "baseline_commit": args.baseline_commit,
            "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
            "previous_status_semantics": previous_status_semantics(),
            "previous_13_matched_null_passes_interpretation": "13 prototype labels from shared component-level null evidence; not 13 complete candidate-level null-suite passes.",
            "status_policy_version": STATUS_POLICY_VERSION,
            "temporal_policy_version": TEMPORAL_POLICY_VERSION,
            "manifest_path": str(manifest_path),
            "manifest_hash": manifest_hash,
            "candidates_frozen": [candidate.to_dict() for candidate in frozen],
            "candidate_results": _compact_candidate_results(candidate_results),
            "status_distribution": dict(status_counts),
            "candidate_level_matched_null_passes": sum(1 for item in candidate_results if item["pooled_null_decision"]["passed"]),
            "topstep_replay_count": len(topstep_candidates),
            "topstep_compatible_after_strict_replay": sum(1 for item in topstep_candidates if item.get("topstep_sequential", {}).get("status") in {"TOPSTEP_PORTFOLIO_CANDIDATE", "TOPSTEP_PAYOUT_SURVIVED", "TOPSTEP_PAYOUT_ELIGIBLE"}),
            "portfolio_diagnostic": portfolio,
            "future_q4_freeze_recommendations": [],
            "new_databento_requests": [],
            "spend_this_phase_usd": 0.0,
            "remaining_budget_usd": 96.106305,
            "warning": "Q1-Q3 are development/falsification data. Q4 remains sealed. Historical research only.",
        },
        stamp,
        args.report_tag,
    )
    ledger_path = _write_trade_ledgers(ledgers, stamp, args.report_tag)
    checkpoint_path = _write_checkpoint(candidate_results, stamp, args.report_tag)
    return {
        "baseline_commit": args.baseline_commit,
        "q4_seal_verification": "PASSED_NO_Q4_ACCESS",
        "manifest_path": str(manifest_path),
        "manifest_hash": manifest_hash,
        "candidate_count": len(frozen),
        "representation_evidence_candidates": sum("REPRESENTATION_EVIDENCE_PASS" in candidate.previous_statuses for candidate in frozen),
        "control_candidates": sum(candidate.replay_role.endswith("_CONTROL") for candidate in frozen),
        "candidate_period_replays": len(frozen) * 3,
        "status_distribution": dict(status_counts),
        "candidate_level_matched_null_passes": sum(1 for item in candidate_results if item["pooled_null_decision"]["passed"]),
        "topstep_replay_count": len(topstep_candidates),
        "topstep_compatible_after_strict_replay": sum(1 for item in topstep_candidates if item.get("topstep_sequential", {}).get("status") in {"TOPSTEP_PORTFOLIO_CANDIDATE", "TOPSTEP_PAYOUT_SURVIVED", "TOPSTEP_PAYOUT_ELIGIBLE"}),
        "future_q4_freeze_recommendations": [],
        "new_databento_requests": [],
        "spend_this_phase_usd": 0.0,
        "remaining_budget_usd": 96.106305,
        "final_report_path": str(final_report_path),
        "trade_ledger_path": str(ledger_path),
        "checkpoint_path": str(checkpoint_path),
        "candidate_results": _compact_candidate_results(candidate_results),
    }


def _load_unsampled_frames(dataset: str, schema: str, symbols: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for key in ("q1", "q2", "q3"):
        period = DEFAULT_2024_PERIODS[key]
        q4_access_guard(period.start, period.end)
        enforce_data_access(
            f"{period.start}:{period.end}",
            period.role,
            "scripts/run_strict_temporal_transfer_replay.py",
            [],
            "strict unsampled temporal transfer replay; Q4 not accessed",
            None,
        )
        out[key] = _load_governed_cached_frame(dataset, schema, symbols, period.start, period.end)
    return out


def _build_lane_frames(frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, pd.DataFrame]]:
    return {
        "overnight_inventory_rth_resolution": {period: build_overnight_components(frame) for period, frame in frames.items()},
        "intraday_range_migration_path_asymmetry": {period: build_range_components(frame) for period, frame in frames.items()},
    }


def _reconstruct_prior_candidates(lane_frames: dict[str, dict[str, pd.DataFrame]], symbols: list[str], seed: int) -> list[dict[str, Any]]:
    components = lane_components()
    prototypes = generate_cluster_aware_prototypes(
        lanes=LANES,
        lane_components={lane: [component.column for component in components[lane]] for lane in LANES},
        symbols=symbols,
        max_total=80,
        max_structures=20,
        max_variants_per_structure=4,
    )
    lane_matched: dict[str, list[dict[str, Any]]] = {lane: [] for lane in LANES}
    for lane in LANES:
        for period, frame in lane_frames[lane].items():
            for component in components[lane]:
                signal = np.sign(pd.to_numeric(frame[component.column], errors="coerce").fillna(0.0)) * component.expected_sign
                result = evaluate_matched_nulls(frame, signal, frame["forward_return"], signal_id=f"{lane}_{component.column}_{period}", seed=seed)
                lane_matched[lane].append(result.to_dict() | {"period": period, "component": component.column})
    sketches = []
    for proto in prototypes:
        frame = lane_frames[proto.lane]["q1"]
        signal = _prototype_signal(frame, proto.components, proto.threshold_rank)
        sketches.append(
            expected_event_signature(frame, signal)
            | {
                "prototype_id": proto.prototype_id,
                "logical_fingerprint": proto.logical_fingerprint,
                "lane": proto.lane,
                "structural_id": proto.structural_id,
                "components": list(proto.components),
            }
        )
    accepted, _ = reject_logical_duplicates(sketches)
    accepted_ids = {row["prototype_id"] for row in accepted}
    out: list[dict[str, Any]] = []
    for proto in prototypes:
        if proto.prototype_id not in accepted_ids:
            continue
        period_results = []
        for period, frame in lane_frames[proto.lane].items():
            signal = _prototype_signal(frame, proto.components, proto.threshold_rank)
            period_results.append(_backtest_signal(frame, signal, proto, period))
        record = _aggregate(proto, period_results, lane_matched[proto.lane])
        record["prototype"] = proto
        out.append(record)
    return out


def _select_frozen_candidates(records: list[dict[str, Any]], max_candidates: int) -> list[FrozenReplayCandidate]:
    selected: list[dict[str, Any]] = []
    by_id: set[str] = set()

    def add(record: dict[str, Any], role: str) -> None:
        if record["prototype_id"] in by_id or len(selected) >= max_candidates:
            return
        item = dict(record)
        item["replay_role"] = role
        selected.append(item)
        by_id.add(record["prototype_id"])

    for record in sorted(records, key=lambda item: item["net_pnl"], reverse=True):
        if "REPRESENTATION_EVIDENCE_PASS" in record["statuses"]:
            add(record, "REPRESENTATION_EVIDENCE_CANDIDATE")
    for record in sorted(records, key=lambda item: item["net_pnl"], reverse=True):
        if "TOPSTEP_COMPATIBLE" in record["statuses"]:
            add(record, "TOPSTEP_COMPATIBLE_CANDIDATE")
    for record in sorted(records, key=lambda item: item["net_pnl"], reverse=True):
        if "MATCHED_NULL_BEATEN" in record["statuses"] and "REPRESENTATION_EVIDENCE_PASS" not in record["statuses"]:
            add(record, "MATCHED_NULL_POSITIVE_CONTROL")
            if sum(item["replay_role"] == "MATCHED_NULL_POSITIVE_CONTROL" for item in selected) >= 5:
                break
    for record in sorted(records, key=lambda item: item["net_pnl"]):
        if "FALSIFIED" in record["statuses"]:
            add(record, "MATCHED_NULL_NEGATIVE_CONTROL")
            if sum(item["replay_role"] == "MATCHED_NULL_NEGATIVE_CONTROL" for item in selected) >= 5:
                break
    out: list[FrozenReplayCandidate] = []
    for record in selected[:max_candidates]:
        proto = record["prototype"]
        out.append(
            FrozenReplayCandidate(
                candidate_id=proto.prototype_id,
                lane=proto.lane,
                structural_id=proto.structural_id,
                variant_id=proto.variant_id,
                symbol=proto.symbol,
                components=tuple(proto.components),
                horizon=int(proto.horizon),
                threshold_rank=int(proto.threshold_rank),
                previous_statuses=tuple(record["statuses"]),
                replay_role=record["replay_role"],
            )
        )
    return out


def _replay_candidate(
    candidate: FrozenReplayCandidate,
    lane_frames: dict[str, dict[str, pd.DataFrame]],
    raw_frames: dict[str, pd.DataFrame],
    *,
    effective_trial_count: int,
    seed: int,
) -> dict[str, Any]:
    period_results: dict[str, dict[str, Any]] = {}
    period_null_decisions: dict[str, dict[str, Any]] = {}
    pooled_frames = []
    pooled_signals = []
    for period, frame in lane_frames[candidate.lane].items():
        result = _backtest_candidate_period(candidate, frame, period)
        period_results[period] = result
        subset, signal = _candidate_signal_frame(candidate, frame)
        null = evaluate_matched_nulls(subset, signal, subset["forward_return"], signal_id=f"{candidate.candidate_id}_{period}", seed=seed, max_events=1500)
        period_null_decisions[period] = candidate_level_null_decision(null.to_dict(), candidate_id=candidate.candidate_id, effective_trial_count=effective_trial_count).to_dict()
        pooled_frames.append(subset)
        pooled_signals.append(signal)
    pooled_frame = pd.concat(pooled_frames, ignore_index=True)
    pooled_signal = pd.concat([signal.reset_index(drop=True) for signal in pooled_signals], ignore_index=True)
    pooled_null = evaluate_matched_nulls(pooled_frame, pooled_signal, pooled_frame["forward_return"], signal_id=f"{candidate.candidate_id}_pooled", seed=seed, max_events=2500)
    pooled_decision = candidate_level_null_decision(pooled_null.to_dict(), candidate_id=candidate.candidate_id, effective_trial_count=effective_trial_count).to_dict()
    temporal = classify_temporal_transfer(candidate.candidate_id, period_results, period_null_decisions, pooled_decision).to_dict()
    component_audit = _component_necessity(candidate, lane_frames[candidate.lane])
    out = {
        "candidate": candidate.to_dict(),
        "period_results": period_results,
        "period_null_decisions": period_null_decisions,
        "pooled_null_decision": pooled_decision,
        "temporal_transfer": temporal,
        "component_necessity": component_audit,
        "sequential_result": _sequential_result(period_results),
    }
    if temporal["status"] in {"TEMPORAL_TRANSFER_STRONG", "TEMPORAL_TRANSFER_WEAK"}:
        out.update(_topstep_replay(candidate, lane_frames[candidate.lane], period_results))
    return out


def _candidate_signal_frame(candidate: FrozenReplayCandidate, frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    signal_all = _prototype_signal(frame, candidate.components, candidate.threshold_rank)
    subset = frame[frame["symbol"] == candidate.symbol].copy()
    signal = signal_all.reindex(subset.index).fillna(0).astype(int)
    subset = subset.reset_index(drop=True)
    signal = signal.reset_index(drop=True)
    return subset, signal


def _backtest_candidate_period(candidate: FrozenReplayCandidate, frame: pd.DataFrame, period: str) -> dict[str, Any]:
    subset, signal = _candidate_signal_frame(candidate, frame)
    close = pd.to_numeric(subset["close"], errors="coerce")
    entries = signal.ne(0) & signal.shift(1).fillna(0).eq(0)
    entry_locs = list(entries[entries].index)
    trades = []
    cost = round_turn_cost(candidate.symbol)
    spec = instrument_spec(candidate.symbol)
    for loc in entry_locs[: candidate.max_trades_per_period]:
        exit_loc = min(int(loc) + candidate.horizon, len(subset) - 1)
        if exit_loc <= loc:
            continue
        side = int(signal.iloc[loc])
        entry_price = float(close.iloc[loc])
        exit_price = float(close.iloc[exit_loc])
        path = subset.iloc[loc : exit_loc + 1]
        adverse_price = float(path["low"].min()) if side > 0 else float(path["high"].max())
        favorable_price = float(path["high"].max()) if side > 0 else float(path["low"].min())
        gross = (exit_price - entry_price) * side * spec.point_value
        net = gross - cost
        mae = (adverse_price - entry_price) * side * spec.point_value
        mfe = (favorable_price - entry_price) * side * spec.point_value
        trades.append(
            {
                "candidate_id": candidate.candidate_id,
                "period": period,
                "entry_i": int(loc),
                "exit_i": int(exit_loc),
                "entry_timestamp": pd.Timestamp(subset.loc[loc, "timestamp"]).isoformat(),
                "exit_timestamp": pd.Timestamp(subset.loc[exit_loc, "timestamp"]).isoformat(),
                "symbol": candidate.symbol,
                "contract": str(subset.loc[loc].get("contract", candidate.symbol)),
                "side": side,
                "quantity": 1,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "gross_pnl": float(gross),
                "net_pnl": float(net),
                "pnl": float(net),
                "commission": float(cost),
                "slippage": 0.0,
                "mae": float(mae),
                "mfe": float(mfe),
                "point_value": float(spec.point_value),
                "risk_scale": 1.0,
                "holding_bars": int(exit_loc - loc),
                "session": int(pd.Timestamp(subset.loc[loc, "timestamp"]).hour),
                "regime": _regime_label(subset, loc),
                "entry_reason": "+".join(candidate.components),
                "exit_reason": "fixed_horizon",
                "roll_window": _is_roll_window(pd.Timestamp(subset.loc[loc, "timestamp"])),
            }
        )
    gross = sum(float(trade["gross_pnl"]) for trade in trades)
    costs = sum(float(trade["commission"]) for trade in trades)
    net = sum(float(trade["net_pnl"]) for trade in trades)
    metrics = period_metric_summary(trades, gross_pnl=gross, costs=costs, net_pnl=net)
    metrics.update(
        {
            "period": period,
            "candidate_id": candidate.candidate_id,
            "opportunities": int(len(entry_locs)),
            "executed_trades": len(trades),
            "max_adverse_excursion": float(min((trade["mae"] for trade in trades), default=0.0)),
            "max_favorable_excursion": float(max((trade["mfe"] for trade in trades), default=0.0)),
            "session_contribution": _sum_by(trades, "session"),
            "regime_contribution": _sum_by(trades, "regime"),
            "contract_contribution": _sum_by(trades, "contract"),
            "roll_window_contribution": _sum_by(trades, "roll_window"),
            "trades": trades,
        }
    )
    return metrics


def _sequential_result(period_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    trades = []
    for period in ("q1", "q2", "q3"):
        trades.extend(period_results[period]["trades"])
    gross = sum(float(trade["gross_pnl"]) for trade in trades)
    costs = sum(float(trade["commission"]) for trade in trades)
    net = sum(float(trade["net_pnl"]) for trade in trades)
    return period_metric_summary(trades, gross_pnl=gross, costs=costs, net_pnl=net) | {"periods": "q1_to_q3"}


def _component_necessity(candidate: FrozenReplayCandidate, lane_frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    full_net = sum(_backtest_candidate_period(candidate, frame, period)["net_pnl"] for period, frame in lane_frames.items())
    removals = []
    for component in candidate.components:
        remaining = tuple(item for item in candidate.components if item != component)
        if not remaining:
            removals.append({"component": component, "removed_net_pnl": 0.0, "classification": "ESSENTIAL_ONLY_COMPONENT"})
            continue
        child = FrozenReplayCandidate(
            candidate_id=candidate.candidate_id + "_remove_" + component,
            lane=candidate.lane,
            structural_id=candidate.structural_id,
            variant_id=candidate.variant_id,
            symbol=candidate.symbol,
            components=remaining,
            horizon=candidate.horizon,
            threshold_rank=candidate.threshold_rank,
            previous_statuses=candidate.previous_statuses,
            replay_role="COMPONENT_REMOVAL_AUDIT",
        )
        removed_net = sum(_backtest_candidate_period(child, frame, period)["net_pnl"] for period, frame in lane_frames.items())
        delta = full_net - removed_net
        if removed_net > full_net + abs(full_net) * 0.10:
            classification = "HARMFUL_COMPONENT"
        elif abs(delta) <= max(abs(full_net) * 0.05, 250.0):
            classification = "REDUNDANT_COMPONENT"
        else:
            classification = "ESSENTIAL_COMPONENT"
        removals.append({"component": component, "removed_net_pnl": float(removed_net), "full_minus_removed": float(delta), "classification": classification})
    essential = [row["component"] for row in removals if row["classification"].startswith("ESSENTIAL")]
    return {
        "full_pooled_net_pnl": float(full_net),
        "removals": removals,
        "essential_components": essential,
        "redundant_components": [row["component"] for row in removals if row["classification"] == "REDUNDANT_COMPONENT"],
        "harmful_components": [row["component"] for row in removals if row["classification"] == "HARMFUL_COMPONENT"],
        "minimal_stable_formulation": essential or list(candidate.components),
    }


def _topstep_replay(candidate: FrozenReplayCandidate, lane_frames: dict[str, pd.DataFrame], period_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    cfg = Topstep150KConfig()
    overlay = InternalRiskOverlay(daily_stop=1000.0, daily_profit_lock=1500.0)
    period_eval: dict[str, Any] = {}
    seq_trades = []
    seq_frames = []
    offset = 0
    for period, frame in lane_frames.items():
        subset = frame[frame["symbol"] == candidate.symbol].copy().reset_index(drop=True)
        trades = [dict(trade) for trade in period_results[period]["trades"]]
        period_eval[period] = evaluate_topstep_150k(trades, subset, cfg, overlay).to_record()
        for trade in trades:
            shifted = dict(trade)
            shifted["entry_i"] = int(shifted["entry_i"]) + offset
            shifted["exit_i"] = int(shifted["exit_i"]) + offset
            seq_trades.append(shifted)
        seq_frames.append(subset)
        offset += len(subset)
    seq_frame = pd.concat(seq_frames, ignore_index=True)
    sequential = evaluate_topstep_150k(seq_trades, seq_frame, cfg, overlay).to_record()
    return {"topstep_periods": period_eval, "topstep_sequential": sequential}


def _portfolio_diagnostic(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if len(candidates) < 2:
        return {"status": "NOT_APPLICABLE", "reason": "fewer_than_two_temporal_transfer_candidates"}
    daily_vectors: dict[str, pd.Series] = {}
    for candidate in candidates:
        daily: dict[str, float] = {}
        for period in ("q1", "q2", "q3"):
            for day, pnl in candidate["period_results"][period]["daily_pnl"].items():
                daily[f"{period}:{day}"] = float(pnl)
        daily_vectors[candidate["candidate"]["candidate_id"]] = pd.Series(daily)
    ids = list(daily_vectors)
    correlations = {}
    for i, left in enumerate(ids):
        for right in ids[i + 1 :]:
            joined = pd.concat([daily_vectors[left], daily_vectors[right]], axis=1).fillna(0.0)
            correlations[f"{left}__{right}"] = float(joined.iloc[:, 0].corr(joined.iloc[:, 1])) if len(joined) > 1 else 0.0
    return {"status": "DIAGNOSTIC_ONLY", "candidate_count": len(candidates), "daily_pnl_correlation": correlations}


def _compact_candidate_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact = []
    for item in results:
        compact.append(
            {
                "candidate": item["candidate"],
                "period_results": {
                    period: {key: value for key, value in result.items() if key not in {"trades", "daily_pnl"}}
                    for period, result in item["period_results"].items()
                },
                "sequential_result": {key: value for key, value in item["sequential_result"].items() if key != "daily_pnl"},
                "period_null_decisions": item["period_null_decisions"],
                "pooled_null_decision": item["pooled_null_decision"],
                "temporal_transfer": item["temporal_transfer"],
                "component_necessity": item["component_necessity"],
                "topstep_sequential": item.get("topstep_sequential"),
            }
        )
    return compact


def _write_final_report(summary: dict[str, Any], stamp: str, tag: str) -> Path:
    path = project_path("reports", "temporal_transfer", f"strict_temporal_transfer_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Strict Unsampled Temporal Transfer Replay {tag}",
        "",
        "Historical research only. Q4 remains sealed. No live trading approval.",
        "",
        f"- Baseline commit: {summary['baseline_commit']}",
        f"- Q4 seal: {summary['q4_seal_verification']}",
        f"- Frozen candidates: {len(summary['candidates_frozen'])}",
        f"- Candidate-level null passes: {summary['candidate_level_matched_null_passes']}",
        f"- Topstep replay count: {summary['topstep_replay_count']}",
        f"- Future Q4 freeze recommendations: {len(summary['future_q4_freeze_recommendations'])}",
        "",
        "```json",
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        "```",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _write_trade_ledgers(ledgers: dict[str, Any], stamp: str, tag: str) -> Path:
    path = project_path("reports", "candidate_null_validation", f"strict_temporal_trade_ledgers_{stamp}_{tag}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledgers, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def _write_checkpoint(results: list[dict[str, Any]], stamp: str, tag: str) -> Path:
    path = project_path("reports", "checkpoints", "temporal_transfer", f"strict_temporal_transfer_checkpoint_{stamp}_{tag}.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter(item["temporal_transfer"]["status"] for item in results)
    path.write_text(
        "\n".join(
            [
                f"# Strict Temporal Transfer Checkpoint {tag}",
                "",
                f"- candidates: {len(results)}",
                f"- status_distribution: {dict(counts)}",
                "- q4_seal: PASSED_NO_Q4_ACCESS",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _compact_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in summary.items() if key != "candidate_results"} | {
        "candidate_result_count": len(summary.get("candidate_results", []))
    }


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    payload = {
        "rows": int(len(frame)),
        "min_ts": str(pd.to_datetime(frame["timestamp"], utc=True).min()),
        "max_ts": str(pd.to_datetime(frame["timestamp"], utc=True).max()),
        "symbols": sorted(frame["symbol"].astype(str).unique().tolist()),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _regime_label(frame: pd.DataFrame, loc: int) -> str:
    close = pd.to_numeric(frame["close"], errors="coerce")
    vol = close.pct_change().abs().rolling(60, min_periods=20).mean()
    value = float(vol.iloc[loc]) if loc < len(vol) and pd.notna(vol.iloc[loc]) else 0.0
    q75 = float(vol.quantile(0.75)) if len(vol.dropna()) else 0.0
    return "high_vol" if value >= q75 and q75 > 0 else "normal_vol"


def _is_roll_window(ts: pd.Timestamp) -> str:
    timestamp = pd.Timestamp(ts).tz_convert("UTC") if pd.Timestamp(ts).tzinfo else pd.Timestamp(ts, tz="UTC")
    roll_dates = [pd.Timestamp("2024-03-15", tz="UTC"), pd.Timestamp("2024-06-21", tz="UTC"), pd.Timestamp("2024-09-20", tz="UTC")]
    return "roll_window" if any(abs((timestamp - roll).days) <= 3 for roll in roll_dates) else "normal"


def _sum_by(trades: list[dict[str, Any]], key: str) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for trade in trades:
        out[str(trade.get(key, "unknown"))] += float(trade["net_pnl"])
    return dict(out)


if __name__ == "__main__":
    raise SystemExit(main())
