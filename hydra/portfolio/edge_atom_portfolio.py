from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from hydra.atoms.schema import AssembledStrategySpec
from hydra.portfolio.shared_risk_scheduler import SharedRiskSchedule, schedule_shared_account


@dataclass(frozen=True)
class EdgeAtomPortfolioBasket:
    basket_id: str
    strategy_ids: tuple[str, ...]
    account_schedule: SharedRiskSchedule
    portfolio_only_candidates: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["account_schedule"] = self.account_schedule.to_dict()
        return out


def build_edge_atom_baskets(strategies: list[AssembledStrategySpec]) -> list[EdgeAtomPortfolioBasket]:
    if not strategies:
        return []
    schedule = schedule_shared_account(strategies)
    return [
        EdgeAtomPortfolioBasket(
            basket_id="edge_atom_account_basket_001",
            strategy_ids=tuple(spec.strategy_id for spec in strategies),
            account_schedule=schedule,
            portfolio_only_candidates=(),
        )
    ]

