from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hydra.promotion.equivalence_clusters import economic_strategy_unit
from hydra.promotion.failure_attribution import attribute_candidate_failure
from hydra.promotion.repairability import recommended_mutation_class, repairability_score
from hydra.utils.config import project_path


@dataclass(frozen=True)
class CandidateDossier:
    candidate_id: str
    data: dict[str, Any]


def build_candidate_dossier(row: dict[str, Any]) -> CandidateDossier:
    attribution = attribute_candidate_failure(row)
    repair_score = repairability_score(row, attribution)
    params = _safe_json(row.get("parameters_json"))
    risk = _safe_json(row.get("risk_json"))
    split_scores = _safe_json(row.get("topstep_split_scores_json"))
    dossier = {
        "identity": {
            "candidate_id": row.get("candidate_id"),
            "logical_fingerprint": row.get("strategy_fingerprint"),
            "parameter_fingerprint": row.get("parameter_zone"),
            "lineage_cluster": economic_strategy_unit(row),
            "family": row.get("family"),
            "lane": row.get("research_lane"),
            "parent_candidate_id": row.get("parent_candidate_id"),
            "mutation_type": row.get("mutation_type"),
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
        },
        "strategy_logic": {
            "market_representation": row.get("family"),
            "entry_logic": f"{row.get('family')}_regime_path_entry",
            "parameters": params,
            "exit_policy": risk.get("exit_policy"),
            "sizing_logic": risk.get("sizing_mode"),
            "internal_daily_stop": risk.get("internal_daily_stop"),
            "daily_profit_lock": risk.get("daily_profit_lock"),
            "max_position": risk.get("max_position"),
            "order_type": "market_or_stop_proxy_backtest",
            "execution_assumptions": "ohlcv_1m_with_conservative_intrabar_required_for_promotion",
        },
        "economics": {
            "trade_count": row.get("trade_count"),
            "net_pnl": row.get("net_profit"),
            "max_drawdown": row.get("max_drawdown"),
            "profit_factor": row.get("profit_factor"),
            "sharpe": row.get("sharpe"),
            "win_rate": row.get("win_rate"),
            "worst_day_loss": row.get("worst_day_loss"),
            "max_consecutive_losing_days": row.get("max_consecutive_losing_days"),
            "best_day_pct_of_total_profit": row.get("combine_best_day_pct_of_total_profit"),
        },
        "temporal_evidence": {
            "split_scores": split_scores,
            "created_at": row.get("created_at"),
        },
        "prop_firm_path": {
            "status": row.get("validation_status"),
            "topstep_passed": bool(row.get("topstep_passed")),
            "topstep_score": row.get("topstep_score"),
            "combine_profit_target_hit": bool(row.get("combine_profit_target_hit")),
            "combine_mll_breached": bool(row.get("combine_mll_breached")),
            "combine_min_mll_buffer": row.get("combine_min_mll_buffer"),
            "combine_consistency_ok": bool(row.get("combine_consistency_ok")),
            "funded_sim_survived": bool(row.get("funded_sim_survived")),
            "payout_eligible": bool(row.get("payout_eligible")),
            "payout_cycles_survived": row.get("payout_cycles_survived"),
            "trader_net_payout": row.get("trader_net_payout"),
        },
        "robustness": {
            "promotion_score": row.get("promotion_score"),
            "economic_score": row.get("economic_score"),
            "execution_readiness_score": row.get("execution_readiness_score"),
            "gate_history": _safe_json_list(row.get("gate_history_json")),
        },
        "failure_attribution": {
            **attribution,
            "repairability_score": repair_score,
            "recommended_mutation_class": recommended_mutation_class(row, attribution),
            "risk_of_destroying_edge": "high" if repair_score < 0.35 else "moderate",
        },
    }
    return CandidateDossier(str(row["candidate_id"]), dossier)


def write_dossiers(dossiers: list[CandidateDossier], folder: str = "reports/gate_aware_remediation/dossiers") -> list[str]:
    target = project_path(folder)
    target.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for dossier in dossiers:
        path = target / f"{dossier.candidate_id}.json"
        path.write_text(json.dumps(dossier.data, indent=2, sort_keys=True), encoding="utf-8")
        paths.append(str(path))
    return paths


def _safe_json(value: str | None) -> dict[str, Any]:
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}


def _safe_json_list(value: str | None) -> list[dict[str, Any]]:
    try:
        out = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    return out if isinstance(out, list) else []

