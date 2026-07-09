from __future__ import annotations


def round_turn_cost(symbol: str) -> float:
    return 4.50 if symbol.startswith("M") else 9.00
