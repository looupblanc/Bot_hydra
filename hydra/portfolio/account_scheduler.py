from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScheduledPortfolioResult:
    candidate_ids: list[str]
    executable: bool
    shared_mll_respected: bool
    target_reached_count: int
    estimated_net_profit: float
    estimated_trader_net_payout: float
    notes: list[str]


def schedule_one_account_portfolio(rows: list[dict[str, Any]], max_candidates: int = 10) -> ScheduledPortfolioResult:
    selected = []
    used_clusters = set()
    total_profit = 0.0
    payout = 0.0
    target_hits = 0
    notes = ["approximation_from_registry_metrics_not_trade_level_schedule"]
    for row in sorted(rows, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True):
        cluster = row.get("equivalence_cluster") or row.get("parameter_zone") or row.get("strategy_fingerprint")
        if cluster in used_clusters:
            continue
        if bool(row.get("combine_mll_breached")):
            continue
        selected.append(str(row["candidate_id"]))
        used_clusters.add(cluster)
        total_profit += float(row.get("net_profit") or 0.0)
        payout += float(row.get("trader_net_payout") or 0.0)
        target_hits += int(bool(row.get("combine_profit_target_hit")))
        if len(selected) >= max_candidates:
            break
    return ScheduledPortfolioResult(
        candidate_ids=selected,
        executable=bool(selected),
        shared_mll_respected=bool(selected),
        target_reached_count=target_hits,
        estimated_net_profit=total_profit,
        estimated_trader_net_payout=payout,
        notes=notes,
    )

