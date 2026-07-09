from __future__ import annotations

from hydra.risk.sizing import SizingPlan, choose_topstep_sizing


SIZING_MODES = [
    "MES-only",
    "MNQ-only",
    "micro-first",
    "mixed MES/MNQ",
    "mini only if MLL-safe",
    "fixed contracts",
    "dynamic contracts based on MLL buffer",
    "dynamic contracts based on volatility",
    "risk-per-trade",
    "progressive scaling only after MLL buffer improves",
    "de-risking after losing streak",
]


def candidate_sizing_plan(symbol: str, mode: str, risk_per_trade: float, stop_distance_ticks: int, mll_buffer: float = 4500.0) -> SizingPlan:
    if mode == "dynamic contracts based on volatility":
        risk_per_trade *= 0.75
    if mode == "progressive scaling only after MLL buffer improves" and mll_buffer < 5500:
        risk_per_trade *= 0.50
    if mode == "mixed MES/MNQ" and not symbol.startswith("M"):
        mode = "micro-first"
    if mode == "risk-per-trade":
        mode = "dynamic contracts based on MLL buffer"
    return choose_topstep_sizing(symbol, mode, risk_per_trade, stop_distance_ticks, mll_buffer)
