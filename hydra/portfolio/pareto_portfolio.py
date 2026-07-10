from __future__ import annotations

from typing import Any

from hydra.portfolio.account_scheduler import schedule_one_account_portfolio
from hydra.promotion.pareto import pareto_frontier


def build_pareto_portfolios(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frontier = pareto_frontier(rows, limit=50)
    portfolio = schedule_one_account_portfolio(frontier, max_candidates=10)
    return [
        {
            "policy": "balanced_pareto_one_account",
            "candidate_ids": portfolio.candidate_ids,
            "executable": portfolio.executable,
            "shared_mll_respected": portfolio.shared_mll_respected,
            "estimated_net_profit": portfolio.estimated_net_profit,
            "estimated_trader_net_payout": portfolio.estimated_trader_net_payout,
            "notes": portfolio.notes,
        }
    ]

