from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from hydra.backtest.costs import round_turn_cost
from hydra.promotion.cluster_calibration import calibrate_clustering_controls, cluster_sketches
from hydra.research.null_models import block_shuffle, delayed_null
from hydra.research.representation_falsification import classify_representation
from hydra.utils.config import project_path


@dataclass(frozen=True)
class RepresentationSpec:
    name: str
    rank: int
    selected: bool
    economic_hypothesis: str
    mechanism: str
    expected_regime: str
    expected_failure_regime: str
    topstep_role: str
    roll_sensitivity: str
    reason: str


@dataclass(frozen=True)
class PrototypeSpec:
    prototype_id: str
    family: str
    structural_id: str
    variant_id: str
    symbol: str
    parameters: dict[str, Any]


def q4_access_guard(start: str, end: str) -> None:
    if pd.Timestamp(start, tz="UTC") < pd.Timestamp("2025-01-01", tz="UTC") and pd.Timestamp(end, tz="UTC") > pd.Timestamp("2024-10-01", tz="UTC"):
        raise RuntimeError("Q4 is sealed and cannot be loaded or summarized by the representation lab.")


def evaluate_feature_evidence(
    feature_frame: pd.DataFrame,
    *,
    feature_col: str = "feature",
    forward_col: str = "forward_return",
    period_name: str,
    seed: int = 0,
) -> dict[str, Any]:
    data = feature_frame[[feature_col, forward_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(data) < 200:
        return {"period": period_name, "rows": int(len(data)), "status": "INSUFFICIENT_EVIDENCE", "effect_size": 0.0}
    feature = data[feature_col].astype(float)
    forward = data[forward_col].astype(float)
    signed_effect = float(np.sign(feature) @ forward / max(len(data), 1))
    corr = float(feature.corr(forward) or 0.0)
    upper = forward[feature >= feature.quantile(0.75)]
    lower = forward[feature <= feature.quantile(0.25)]
    monotonicity = float(upper.mean() - lower.mean()) if len(upper) and len(lower) else 0.0
    shuffled = block_shuffle(forward, block_size=120, seed=seed)
    delayed = delayed_null(forward, delay=60)
    null_effect = max(
        abs(float(np.sign(feature) @ shuffled.fillna(0.0) / max(len(data), 1))),
        abs(float(np.sign(feature) @ delayed.fillna(0.0) / max(len(data), 1))),
    )
    return {
        "period": period_name,
        "rows": int(len(data)),
        "status": "OK",
        "effect_size": signed_effect,
        "feature_forward_correlation": corr,
        "magnitude_monotonicity": monotonicity,
        "null_effect_abs": null_effect,
        "beats_null": bool(abs(signed_effect) > null_effect),
        "direction": "positive" if signed_effect > 0 else "negative" if signed_effect < 0 else "flat",
    }


def generate_bounded_prototypes(
    selected_specs: list[RepresentationSpec],
    *,
    total_cap: int = 150,
    max_family_share: float = 0.30,
    seed: int = 4050,
) -> list[PrototypeSpec]:
    rng = np.random.default_rng(seed)
    family_cap = max(1, int(total_cap * max_family_share))
    out: list[PrototypeSpec] = []
    for spec in selected_specs:
        if not spec.selected:
            continue
        target = family_cap if spec.rank == 1 else min(family_cap, max(15, total_cap // max(len(selected_specs), 1)))
        for structural_idx in range(max(1, target // 5)):
            structural_id = f"{spec.name}_struct_{structural_idx:02d}"
            for variant_idx in range(5):
                if len([item for item in out if item.family == spec.name]) >= target or len(out) >= total_cap:
                    break
                symbol = "NQ" if "nq_es" in spec.name or "opening" in spec.name else rng.choice(["ES", "MES", "NQ", "MNQ"]).item()
                out.append(
                    PrototypeSpec(
                        prototype_id=f"{spec.name}_{structural_idx:02d}_{variant_idx:02d}",
                        family=spec.name,
                        structural_id=structural_id,
                        variant_id=f"variant_{variant_idx:02d}",
                        symbol=str(symbol),
                        parameters={"threshold_rank": int(variant_idx), "structural_seed": int(structural_idx), "seed": int(seed)},
                    )
                )
    return out


def prototype_backtest(feature_frame: pd.DataFrame, prototype: PrototypeSpec, *, horizon: int = 30) -> dict[str, Any]:
    frame = feature_frame[feature_frame["symbol"] == prototype.symbol].copy() if "symbol" in feature_frame.columns else feature_frame.copy()
    if frame.empty or "signal" not in frame.columns:
        return {"prototype_id": prototype.prototype_id, "trade_count": 0, "net_pnl": 0.0, "status": "NO_SIGNAL"}
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    close = pd.to_numeric(frame["close"], errors="coerce") if "close" in frame.columns else pd.Series(dtype=float)
    signal = pd.to_numeric(frame["signal"], errors="coerce").fillna(0).astype(int)
    entries = signal.ne(0) & signal.shift(1).fillna(0).eq(0)
    entry_indices = list(frame.index[entries])[:80]
    trades = []
    cost = round_turn_cost(prototype.symbol)
    point_value = _point_value(prototype.symbol)
    for entry_i in entry_indices:
        exit_i = min(entry_i + horizon, len(frame) - 1)
        if exit_i <= entry_i:
            continue
        side = int(signal.iloc[entry_i])
        entry = float(close.iloc[entry_i])
        exit_ = float(close.iloc[exit_i])
        gross = (exit_ - entry) * side * point_value
        net = gross - cost
        trades.append(
            {
                "entry_timestamp": pd.Timestamp(frame["timestamp"].iloc[entry_i]).isoformat(),
                "exit_timestamp": pd.Timestamp(frame["timestamp"].iloc[exit_i]).isoformat(),
                "side": side,
                "gross_pnl": gross,
                "net_pnl": net,
                "holding_bars": int(exit_i - entry_i),
            }
        )
    daily = _daily_pnl(trades)
    net_pnl = float(sum(t["net_pnl"] for t in trades))
    gross_pnl = float(sum(t["gross_pnl"] for t in trades))
    max_dd = _max_drawdown(daily)
    trade_count = len(trades)
    economically_viable = bool(trade_count >= 10 and net_pnl > 0 and max_dd > -4500)
    return {
        "prototype_id": prototype.prototype_id,
        "family": prototype.family,
        "symbol": prototype.symbol,
        "trade_count": int(trade_count),
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "max_drawdown": float(max_dd),
        "topstep_compatible": bool(economically_viable and net_pnl >= 3000 and max_dd > -2500),
        "economically_viable": economically_viable,
        "trades": trades,
        "daily_pnl": daily,
        "status": "OK" if trade_count else "NO_TRADES",
    }


def behavioral_sketch_for_result(result: dict[str, Any]) -> dict[str, Any]:
    trades = result.get("trades") or []
    entry_sig = _hash_list([trade["entry_timestamp"] for trade in trades])
    direction_sig = _hash_list([trade["side"] for trade in trades])
    pnl_items = result.get("daily_pnl") or {}
    pnl_hash = _hash_list([f"{key}:{value:.2f}" for key, value in sorted(pnl_items.items())])
    tail_values = sorted((float(trade["net_pnl"]) for trade in trades), reverse=True)[:5]
    return {
        "candidate_id": result["prototype_id"],
        "daily_pnl_hash": pnl_hash,
        "trade_timestamp_signature": entry_sig,
        "direction_signature": direction_sig,
        "tail_event_signature": _hash_list([f"{value:.2f}" for value in tail_values]),
        "holding_time_histogram": _hist([int(trade["holding_bars"]) for trade in trades], 30),
        "session_histogram": {"development": len(trades)},
        "symbol_exposure": {result.get("symbol", ""): len(trades)},
        "net_pnl": float(result.get("net_pnl", 0.0)),
    }


def write_behavioral_evidence(tag: str, sketches: list[dict[str, Any]], ledgers: list[dict[str, Any]]) -> dict[str, str]:
    folder = project_path("data", "cache", "behavioral_evidence", tag)
    folder.mkdir(parents=True, exist_ok=True)
    sketch_path = folder / "prototype_behavioral_sketches.jsonl.gz"
    ledger_path = folder / "prototype_trade_ledgers.jsonl.gz"
    with gzip.open(sketch_path, "wt", encoding="utf-8") as handle:
        for row in sketches:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    with gzip.open(ledger_path, "wt", encoding="utf-8") as handle:
        for row in ledgers:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    return {"sketch_path": str(sketch_path), "trade_ledger_path": str(ledger_path)}


def cluster_behavioral_sketches(sketches: list[dict[str, Any]]) -> dict[str, Any]:
    calibration = calibrate_clustering_controls(sketches)
    clusters = cluster_sketches(sketches)
    return {
        "calibration": calibration,
        "clusters": clusters,
        "valid_economic_units": len(clusters) if calibration["precision_known_clones"] >= 0.9 and calibration["recall_known_clones"] >= 0.9 else 0,
    }


def summarize_representation_evidence(name: str, period_evidence: list[dict[str, Any]], prototype_results: list[dict[str, Any]]) -> dict[str, Any]:
    directions = {row.get("direction") for row in period_evidence if row.get("status") == "OK"}
    trade_count = sum(int(row.get("trade_count", 0)) for row in prototype_results)
    positive_after_costs = any(row.get("net_pnl", 0.0) > 0 for row in prototype_results)
    costs_erase = any(row.get("gross_pnl", 0.0) > 0 and row.get("net_pnl", 0.0) <= 0 for row in prototype_results)
    evidence = {
        "representation": name,
        "stable_direction": len(directions - {"flat"}) == 1,
        "positive_after_costs": positive_after_costs,
        "costs_erase_effect": costs_erase and not positive_after_costs,
        "null_beats_effect": any(row.get("status") == "OK" and not row.get("beats_null") for row in period_evidence),
        "roll_artifact": False,
        "periods_with_signal": sum(1 for row in period_evidence if row.get("status") == "OK"),
        "trade_count": trade_count,
    }
    evidence["disposition"] = classify_representation(evidence)
    return evidence


def _point_value(symbol: str) -> float:
    values = {"ES": 50.0, "MES": 5.0, "NQ": 20.0, "MNQ": 2.0}
    return values.get(symbol, 1.0)


def _daily_pnl(trades: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for trade in trades:
        day = str(trade["exit_timestamp"])[:10]
        out[day] = out.get(day, 0.0) + float(trade["net_pnl"])
    return out


def _max_drawdown(daily: dict[str, float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for _, pnl in sorted(daily.items()):
        equity += float(pnl)
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return max_dd


def _hash_list(items: list[Any]) -> str:
    raw = json.dumps(items, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _hist(values: list[int], bucket: int) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        start = (int(value) // bucket) * bucket
        key = f"{start}-{start + bucket - 1}"
        out[key] = out.get(key, 0) + 1
    return out
