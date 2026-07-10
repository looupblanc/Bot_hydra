from __future__ import annotations

from typing import Any

from hydra.portfolio.pareto_portfolio import build_pareto_portfolios


def build_remediation_portfolio_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible = [
        row
        for row in rows
        if row.get("validation_status") in {"TOPSTEP_VIABLE", "TOPSTEP_NEAR_MISS", "ECONOMICALLY_VIABLE"}
        and not bool(row.get("combine_mll_breached"))
    ]
    return build_pareto_portfolios(eligible)

