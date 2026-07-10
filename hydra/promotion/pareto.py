from __future__ import annotations

from typing import Any


PARETO_DIMENSIONS = [
    "net_profit",
    "topstep_score",
    "promotion_score",
    "economic_score",
    "combine_min_mll_buffer",
    "trader_net_payout",
]


def pareto_frontier(rows: list[dict[str, Any]], dimensions: list[str] | None = None, limit: int = 100) -> list[dict[str, Any]]:
    dims = dimensions or PARETO_DIMENSIONS
    frontier: list[dict[str, Any]] = []
    for row in rows:
        dominated = False
        for other in rows:
            if other is row:
                continue
            if all(float(other.get(dim) or 0.0) >= float(row.get(dim) or 0.0) for dim in dims) and any(
                float(other.get(dim) or 0.0) > float(row.get(dim) or 0.0) for dim in dims
            ):
                dominated = True
                break
        if not dominated:
            frontier.append(row)
    return sorted(frontier, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:limit]

