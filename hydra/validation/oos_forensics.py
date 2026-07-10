from __future__ import annotations

import json
import random
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from hydra.backtest.metrics import max_drawdown, profit_factor


OOS_THRESHOLD = 0.35

TRUE_EDGE_DECAY = "TRUE_EDGE_DECAY"
INSUFFICIENT_OOS_TRADES = "INSUFFICIENT_OOS_TRADES"
SPLIT_CONCENTRATION = "SPLIT_CONCENTRATION"
COST_SENSITIVITY = "COST_SENSITIVITY"
ROLL_ARTIFACT_SUSPECTED = "ROLL_ARTIFACT_SUSPECTED"
TIMEZONE_OR_SESSION_SUSPECTED = "TIMEZONE_OR_SESSION_SUSPECTED"
THRESHOLD_TOO_STRICT = "THRESHOLD_TOO_STRICT"
METRIC_DIRECTION_BUG = "METRIC_DIRECTION_BUG"
MISSING_DATA = "MISSING_DATA"
STATUS_INHERITANCE_BUG = "STATUS_INHERITANCE_BUG"
PIPELINE_IMPLEMENTATION_BUG = "PIPELINE_IMPLEMENTATION_BUG"
UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True)
class OOSForensicResult:
    candidate_id: str
    family: str
    symbol: str
    classification: str
    oos_score: float | None
    threshold: float
    distance_to_threshold: float | None
    exact_failed_condition: str
    insufficient_trades: bool
    sign_reversal: bool
    one_month_dominates: bool
    roll_window_involved: bool
    session_timezone_suspected: bool
    missing_feature_coverage: bool
    split_scores: dict[str, float]
    split_trade_counts: dict[str, int]
    split_net_pnl: dict[str, float]
    split_profit_factor: dict[str, float]
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_json(value: Any, fallback: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return fallback
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return fallback


def gate_history(row: dict[str, Any]) -> list[dict[str, Any]]:
    parsed = parse_json(row.get("gate_history_json") or row.get("gate_history"), [])
    return parsed if isinstance(parsed, list) else []


def split_scores(row: dict[str, Any]) -> dict[str, float]:
    parsed = parse_json(row.get("topstep_split_scores_json") or row.get("split_scores"), {})
    if not isinstance(parsed, dict):
        return {}
    out = {}
    for key, value in parsed.items():
        try:
            out[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def failed_gate_names(row: dict[str, Any]) -> list[str]:
    return [str(g.get("name")) for g in gate_history(row) if not bool(g.get("passed"))]


def passed_gate_names(row: dict[str, Any]) -> list[str]:
    return [str(g.get("name")) for g in gate_history(row) if bool(g.get("passed"))]


def q1_core_robust(row: dict[str, Any]) -> bool:
    passed = set(passed_gate_names(row))
    required = {
        "DATA_INTEGRITY",
        "DUPLICATE_FINGERPRINT",
        "NO_LOOKAHEAD",
        "ECONOMIC_PROFILE",
        "WALK_FORWARD",
        "MONTE_CARLO",
        "PARAMETER_SENSITIVITY",
        "TOPSTEP_COMBINE",
        "FUNDED_XFA",
        "PAYOUT_SURVIVAL",
        "CORRELATION",
        "PORTFOLIO_INTERACTION",
        "EXECUTION_READINESS",
    }
    return required.issubset(passed)


def classify_oos_failure(row: dict[str, Any], recomputed: dict[str, Any] | None = None) -> OOSForensicResult:
    scores = dict(split_scores(row))
    scores_from_recompute = bool(recomputed and recomputed.get("split_scores"))
    if scores_from_recompute:
        scores.update({str(k): float(v) for k, v in recomputed["split_scores"].items()})
    mar = scores.get("mar")
    gates = gate_history(row)
    oos_gate = next((g for g in gates if g.get("name") == "OOS"), None)
    failed_condition = "mar_score_below_threshold"
    notes: list[str] = []
    split_trade_counts = dict((recomputed or {}).get("split_trade_counts") or {})
    split_net_pnl = dict((recomputed or {}).get("split_net_pnl") or {})
    split_pf = dict((recomputed or {}).get("split_profit_factor") or {})
    if not scores or mar is None:
        classification = MISSING_DATA
        distance = None
        failed_condition = "missing_oos_split_score"
        notes.append("No March OOS score persisted for this candidate.")
    else:
        distance = round(float(mar) - OOS_THRESHOLD, 6)
        oos_passed = bool(oos_gate.get("passed")) if oos_gate else None
        if mar >= OOS_THRESHOLD and oos_passed is False and not scores_from_recompute:
            classification = METRIC_DIRECTION_BUG
            failed_condition = "mar_score_passes_threshold_but_gate_failed"
        elif row.get("promotion_stage") == "OOS_PASSED" and mar < OOS_THRESHOLD:
            classification = STATUS_INHERITANCE_BUG
            failed_condition = "promotion_stage_oos_passed_but_mar_score_failed"
        elif split_trade_counts.get("mar", 99_999) < 8:
            classification = INSUFFICIENT_OOS_TRADES
            failed_condition = "fewer_than_8_oos_trades"
        elif abs(distance) <= 0.03:
            classification = THRESHOLD_TOO_STRICT
            failed_condition = "within_three_points_of_oos_threshold"
        elif _sign_reversal(split_net_pnl):
            classification = TRUE_EDGE_DECAY
            failed_condition = "positive_train_negative_oos"
        elif _one_month_dominates(scores):
            classification = SPLIT_CONCENTRATION
            failed_condition = "month_to_month_concentration"
        elif _roll_suspected(row, recomputed):
            classification = ROLL_ARTIFACT_SUSPECTED
            failed_condition = "roll_window_trade_or_gap_suspected"
        else:
            classification = TRUE_EDGE_DECAY
    return OOSForensicResult(
        candidate_id=str(row.get("candidate_id") or ""),
        family=str(row.get("family") or ""),
        symbol=str(row.get("symbol") or ""),
        classification=classification,
        oos_score=mar,
        threshold=OOS_THRESHOLD,
        distance_to_threshold=distance if scores and mar is not None else None,
        exact_failed_condition=failed_condition,
        insufficient_trades=classification == INSUFFICIENT_OOS_TRADES,
        sign_reversal=_sign_reversal(split_net_pnl),
        one_month_dominates=_one_month_dominates(scores),
        roll_window_involved=_roll_suspected(row, recomputed),
        session_timezone_suspected=_session_suspected(recomputed),
        missing_feature_coverage=bool((recomputed or {}).get("missing_feature_coverage", False)),
        split_scores=scores,
        split_trade_counts={str(k): int(v) for k, v in split_trade_counts.items()},
        split_net_pnl={str(k): round(float(v), 2) for k, v in split_net_pnl.items()},
        split_profit_factor={str(k): round(float(v), 6) for k, v in split_pf.items()},
        notes=notes,
    )


def stratified_oos_candidates(rows: list[dict[str, Any]], *, random_seed: int = 4050) -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(random_seed)
    oos_failed = [row for row in rows if "OOS" in failed_gate_names(row)]
    q1_core = [row for row in oos_failed if q1_core_robust(row)]
    topstep = [row for row in oos_failed if row.get("validation_status") == "TOPSTEP_VIABLE"]
    closest = sorted(oos_failed, key=lambda r: abs(float(split_scores(r).get("mar", -999.0)) - OOS_THRESHOLD))[:500]
    random_sample = rng.sample(oos_failed, min(500, len(oos_failed))) if oos_failed else []
    by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in oos_failed:
        by_policy[str(row.get("mutation_type") or "unknown")].append(row)
    policy_sample = []
    for members in by_policy.values():
        policy_sample.extend(sorted(members, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:50])
    nq_es = [row for row in oos_failed if str(row.get("family")) == "topstep_nq_es_divergence_controlled"]
    non_nq_es = [row for row in oos_failed if str(row.get("family")) != "topstep_nq_es_divergence_controlled"]
    return {
        "q1_core_robust": q1_core,
        "best_500_topstep_viable": sorted(topstep, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:500],
        "closest_to_oos_threshold": closest,
        "random_oos_failures": random_sample,
        "parents_children_by_policy": policy_sample,
        "top_nq_es_divergence_lineages": sorted(nq_es, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:500],
        "non_nq_es_families": sorted(non_nq_es, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:500],
    }


def split_trade_statistics(trades: list[dict[str, Any]], daily: pd.DataFrame) -> dict[str, Any]:
    split_windows = {
        "jan": ("2024-01-01", "2024-02-01"),
        "feb": ("2024-02-01", "2024-03-01"),
        "mar": ("2024-03-01", "2024-04-01"),
    }
    trade_counts: dict[str, int] = {}
    net: dict[str, float] = {}
    gross: dict[str, float] = {}
    pf: dict[str, float] = {}
    expectancy: dict[str, float] = {}
    dd: dict[str, float] = {}
    costs: dict[str, float] = {}
    for name, (start, end) in split_windows.items():
        subset = _trades_in_window(trades, start, end)
        pnls = [float(t.get("net_pnl") or t.get("pnl") or 0.0) for t in subset]
        trade_counts[name] = len(subset)
        net[name] = float(sum(pnls))
        gross[name] = float(sum(float(t.get("gross_pnl") or t.get("pnl") or 0.0) for t in subset))
        costs[name] = float(sum(float(t.get("commissions") or 0.0) + abs(float(t.get("slippage") or 0.0)) for t in subset))
        pf[name] = profit_factor(pnls)
        expectancy[name] = float(sum(pnls) / len(pnls)) if pnls else 0.0
        curve = pd.Series(pd.Series(pnls, dtype=float).cumsum(), dtype=float)
        dd[name] = max_drawdown(curve) if len(curve) else 0.0
    return {
        "split_trade_counts": trade_counts,
        "split_net_pnl": net,
        "split_gross_pnl": gross,
        "split_expectancy": expectancy,
        "split_profit_factor": pf,
        "split_drawdown": dd,
        "split_costs": costs,
        "split_scores": _score_splits_from_net(net, trade_counts),
        "train_dates": {"start": "2024-01-01", "end": "2024-03-01"},
        "oos_split_dates": {"start": "2024-03-01", "end": "2024-04-01"},
        "purge_embargo_dates": {"purge_start": "2024-02-29", "embargo_end": "2024-03-01"},
    }


def summarize_oos_distribution(results: list[OOSForensicResult]) -> dict[str, int]:
    return dict(Counter(item.classification for item in results))


def roll_audit_summary(df: pd.DataFrame, symbols: list[str]) -> dict[str, Any]:
    if df.empty:
        return {"status": "missing_data", "roll_artifact_suspected": True}
    out: dict[str, Any] = {
        "status": "partial_continuous_ohlcv_only",
        "contract_definitions_available": False,
        "explicit_roll_mapping_available": False,
        "roll_artifact_suspected": False,
        "notes": [
            "Cached OHLCV uses continuous symbols; explicit raw contract mapping and definitions were not present in the registry.",
            "Q4 was not loaded or inspected.",
        ],
        "symbol_windows": {},
    }
    ts = pd.to_datetime(df["timestamp"], utc=True)
    for symbol in symbols:
        frame = df[df["symbol"] == symbol].copy()
        if frame.empty:
            out["symbol_windows"][symbol] = {"missing": True}
            out["roll_artifact_suspected"] = True
            continue
        frame_ts = pd.to_datetime(frame["timestamp"], utc=True)
        windows = {}
        for roll_date in ["2024-03-15", "2024-06-21"]:
            center = pd.Timestamp(roll_date, tz="UTC")
            window = frame[(frame_ts >= center - pd.Timedelta(days=3)) & (frame_ts < center + pd.Timedelta(days=3))]
            if window.empty:
                windows[roll_date] = {"bars": 0, "gap_suspected": True}
                out["roll_artifact_suspected"] = True
                continue
            pct_gap = float(window["close"].pct_change().abs().max() or 0.0)
            windows[roll_date] = {"bars": int(len(window)), "max_abs_close_pct_change": round(pct_gap, 6), "gap_suspected": pct_gap > 0.03}
            if pct_gap > 0.03:
                out["roll_artifact_suspected"] = True
        out["symbol_windows"][symbol] = windows
    out["timestamp_range"] = {"start": str(ts.min()), "end": str(ts.max())}
    return out


def _trades_in_window(trades: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")
    out = []
    for trade in trades:
        raw = trade.get("exit_timestamp") or trade.get("entry_timestamp")
        if not raw:
            continue
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        if start_ts <= ts < end_ts:
            out.append(trade)
    return out


def _score_splits_from_net(net: dict[str, float], trade_counts: dict[str, int]) -> dict[str, float]:
    scores = {}
    for key, value in net.items():
        activity = min(1.0, trade_counts.get(key, 0) / 20.0)
        pnl_score = max(0.0, min(1.0, (float(value) + 1000.0) / 7000.0))
        scores[key] = round(0.75 * pnl_score + 0.25 * activity, 6)
    return scores


def _sign_reversal(split_net_pnl: dict[str, float]) -> bool:
    if not split_net_pnl:
        return False
    train = float(split_net_pnl.get("jan", 0.0)) + float(split_net_pnl.get("feb", 0.0))
    oos = float(split_net_pnl.get("mar", 0.0))
    return train > 0 and oos < 0


def _one_month_dominates(scores: dict[str, float]) -> bool:
    if len(scores) < 3:
        return False
    values = [float(scores.get(k, 0.0)) for k in ("jan", "feb", "mar")]
    return max(values) - sorted(values)[1] > 0.35


def _roll_suspected(row: dict[str, Any], recomputed: dict[str, Any] | None) -> bool:
    if recomputed and recomputed.get("roll_window_trade_count", 0):
        return True
    reason = str(row.get("rejection_reason") or "").lower()
    return "roll" in reason


def _session_suspected(recomputed: dict[str, Any] | None) -> bool:
    return bool(recomputed and recomputed.get("session_timezone_suspected", False))
