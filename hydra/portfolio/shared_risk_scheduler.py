from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from hydra.atoms.schema import AssembledStrategySpec


@dataclass(frozen=True)
class SharedRiskSchedule:
    strategy_ids: tuple[str, ...]
    max_simultaneous_strategies: int
    shared_mll_distance: float
    max_mini_equivalent_contracts: int
    feasible: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def schedule_shared_account(
    strategies: list[AssembledStrategySpec],
    *,
    mll_distance: float = 4500.0,
    max_mini_equivalent_contracts: int = 15,
) -> SharedRiskSchedule:
    if not strategies:
        return SharedRiskSchedule((), 0, mll_distance, max_mini_equivalent_contracts, False, "no_strategy_level_candidates")
    feasible = len(strategies) <= max_mini_equivalent_contracts
    return SharedRiskSchedule(
        strategy_ids=tuple(spec.strategy_id for spec in strategies),
        max_simultaneous_strategies=min(len(strategies), max_mini_equivalent_contracts),
        shared_mll_distance=float(mll_distance),
        max_mini_equivalent_contracts=int(max_mini_equivalent_contracts),
        feasible=bool(feasible),
        reason="shared_account_schedule_feasible" if feasible else "contract_limit_conflict",
    )

