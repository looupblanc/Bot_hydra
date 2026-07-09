from __future__ import annotations


def risk_adjusted_score(row) -> float:
    return float(row["robustness_score"]) * 1000 + float(row["mll_buffer"]) * 0.1 + float(row["net_profit"]) * 0.01 - float(row["max_drawdown"]) * 0.5
