from __future__ import annotations

from collections import defaultdict
from typing import Iterable


def net_mini_equivalent_exposure(positions: Iterable[dict]) -> dict[str, float]:
    exposure: dict[str, float] = defaultdict(float)
    for pos in positions:
        symbol = str(pos.get("symbol", ""))
        contracts = float(pos.get("contracts", 0.0))
        side = float(pos.get("side", 1.0))
        root = symbol[1:] if symbol.startswith("M") else symbol
        mini_equiv = contracts / 10.0 if symbol.startswith("M") else contracts
        exposure[root] += side * mini_equiv
    return dict(exposure)

