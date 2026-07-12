from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Mapping


class CostStress(StrEnum):
    BASE = "BASE"
    STRESS_1_5X = "STRESS_1_5X"
    STRESS_2X = "STRESS_2X"


@dataclass(frozen=True, slots=True)
class ProductCost:
    symbol: str
    commission_round_turn_usd: float
    tick_value_usd: float


@dataclass(frozen=True, slots=True)
class V7CostModel:
    products: Mapping[str, ProductCost]
    base_slippage_ticks_per_side_by_horizon: Mapping[str, float]
    stress_multipliers: Mapping[CostStress, float]
    source: str
    source_checked_utc: str

    def round_turn_cost(
        self,
        symbol: str,
        horizon: str,
        *,
        stress: CostStress = CostStress.BASE,
        contracts: float = 1.0,
    ) -> float:
        product = self.products[symbol]
        slippage_ticks = self.base_slippage_ticks_per_side_by_horizon[horizon]
        multiplier = self.stress_multipliers[stress]
        per_contract = product.commission_round_turn_usd + (
            2.0 * slippage_ticks * multiplier * product.tick_value_usd
        )
        return float(per_contract * contracts)

    def net_after_costs(
        self,
        gross_pnl: float,
        *,
        symbol: str,
        horizon: str,
        round_turns: int,
        contracts_per_round_turn: float = 1.0,
        stress: CostStress = CostStress.BASE,
    ) -> float:
        return float(
            gross_pnl
            - round_turns
            * self.round_turn_cost(
                symbol,
                horizon,
                stress=stress,
                contracts=contracts_per_round_turn,
            )
        )

    def is_sim_exploit(
        self,
        gross_pnl: float,
        *,
        symbol: str,
        horizon: str,
        round_turns: int,
        contracts_per_round_turn: float = 1.0,
    ) -> bool:
        return self.net_after_costs(
            gross_pnl,
            symbol=symbol,
            horizon=horizon,
            round_turns=round_turns,
            contracts_per_round_turn=contracts_per_round_turn,
            stress=CostStress.STRESS_2X,
        ) <= 0.0


def default_preregistration_path() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "config"
        / "v7"
        / "phase0_g0_preregistration.json"
    )


def load_cost_model(path: str | Path | None = None) -> V7CostModel:
    payload = json.loads(
        (Path(path) if path else default_preregistration_path()).read_text(
            encoding="utf-8"
        )
    )["cost_model_preregistration"]
    products = {
        symbol: ProductCost(
            symbol=symbol,
            commission_round_turn_usd=float(commission),
            tick_value_usd=float(payload["tick_value_usd"][symbol]),
        )
        for symbol, commission in payload["round_turn_commission_usd"].items()
    }
    return V7CostModel(
        products=products,
        base_slippage_ticks_per_side_by_horizon={
            str(key): float(value)
            for key, value in payload[
                "base_slippage_ticks_per_side_by_horizon"
            ].items()
        },
        stress_multipliers={
            CostStress(key): float(value)
            for key, value in payload["stress_profiles"].items()
        },
        source=str(payload["commission_source"]),
        source_checked_utc=str(payload["commission_source_checked_utc"]),
    )


def render_cost_model_markdown(model: V7CostModel) -> str:
    lines = [
        "# HYDRA V7 — Cost model",
        "",
        f"Source: {model.source} (checked {model.source_checked_utc}).",
        "",
        "Formula: commission RT + 2 × ticks/side × stress × tick value, per contract.",
        "",
        "| Product | Horizon | Base | Stress 1.5× | Stress 2× |",
        "|---|---:|---:|---:|---:|",
    ]
    for symbol in sorted(model.products):
        for horizon in model.base_slippage_ticks_per_side_by_horizon:
            base = model.round_turn_cost(symbol, horizon)
            stress_1_5 = model.round_turn_cost(
                symbol, horizon, stress=CostStress.STRESS_1_5X
            )
            stress_2 = model.round_turn_cost(
                symbol, horizon, stress=CostStress.STRESS_2X
            )
            lines.append(
                f"| {symbol} | {horizon} | ${base:.2f} | "
                f"${stress_1_5:.2f} | ${stress_2:.2f} |"
            )
    lines.extend(
        [
            "",
            "`SIM_EXPLOIT`: net edge <= 0 after the Stress 2× slippage profile.",
            "",
            "## CONTRE",
            "",
            "This bar-level model uses a preregistered slippage schedule rather than observed bid/ask spreads; finalist execution ambiguity still requires targeted MBP-1/TBBO evidence.",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "CostStress",
    "ProductCost",
    "V7CostModel",
    "load_cost_model",
    "render_cost_model_markdown",
]
