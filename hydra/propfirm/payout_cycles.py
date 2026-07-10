from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PayoutDecision:
    eligible: bool
    gross_payout: float
    trader_net: float
    reason: str


def payout_request(balance: float, cap: float, split: float = 0.90, pct: float = 0.50, minimum: float = 125.0) -> PayoutDecision:
    gross = min(max(balance, 0.0) * pct, cap)
    if gross < minimum:
        return PayoutDecision(False, 0.0, 0.0, "below_minimum_payout")
    return PayoutDecision(True, gross, gross * split, "eligible")

