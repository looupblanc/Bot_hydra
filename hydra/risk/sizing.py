from __future__ import annotations

from dataclasses import dataclass

from hydra.markets.instruments import instrument_spec


@dataclass(frozen=True)
class SizingPlan:
    mode: str
    max_position: int
    risk_scale: float
    risk_per_trade: float
    stop_distance_ticks: int


def choose_topstep_sizing(symbol: str, mode: str, risk_per_trade: float, stop_distance_ticks: int, mll_buffer: float = 4500.0) -> SizingPlan:
    spec = instrument_spec(symbol)
    per_contract_risk = max(stop_distance_ticks * spec.tick_value, 1.0)
    contracts = max(1, int(risk_per_trade // per_contract_risk))
    if mode in {"MES-only", "MNQ-only"} and not spec.is_micro:
        contracts = 0
    if mode == "micro-first" and not spec.is_micro:
        contracts = min(contracts, 1 if mll_buffer >= 3500 else 0)
    if mode == "mini only if MLL-safe" and spec.is_micro:
        contracts = max(1, contracts)
    if mode == "dynamic contracts based on MLL buffer":
        contracts = max(1, min(contracts, int(max(mll_buffer - 1000, 0) // per_contract_risk)))
    if mode == "de-risking after losing streak":
        contracts = max(1, contracts // 2)
    contracts = max(1, min(contracts, 6 if spec.is_micro else 2))
    return SizingPlan(mode=mode, max_position=contracts, risk_scale=float(contracts), risk_per_trade=risk_per_trade, stop_distance_ticks=stop_distance_ticks)
