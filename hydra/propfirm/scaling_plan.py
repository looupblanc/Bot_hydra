from __future__ import annotations


def xfa_150k_max_mini_equivalent(balance: float) -> int:
    if balance < 1500:
        return 3
    if balance < 2000:
        return 4
    if balance < 3000:
        return 5
    if balance < 4500:
        return 10
    return 15


def mini_equivalent(symbol: str, contracts: float) -> float:
    return float(contracts) / 10.0 if symbol.upper().startswith("M") else float(contracts)


def position_limit_ok(symbol: str, contracts: float, max_mini_equivalent: float) -> bool:
    return mini_equivalent(symbol, contracts) <= max_mini_equivalent

