from __future__ import annotations

from typing import Any

import pandas as pd

from hydra.propfirm.payout_cycles import payout_request


def simulate_xfa_consistency(daily: pd.DataFrame, mll_distance: float = 4500.0) -> dict[str, Any]:
    balance = 0.0
    floor = -abs(mll_distance)
    traded_days = 0
    total_profit = 0.0
    best_day = 0.0
    cycles = 0
    gross = 0.0
    net = 0.0
    survived = True
    first_eligible_day = None
    for idx, row in enumerate(daily.itertuples(index=False), start=1):
        intraday_low = balance + float(getattr(row, "worst_intraday_pnl", min(0.0, getattr(row, "pnl", 0.0))))
        if intraday_low <= floor:
            survived = False
            break
        pnl = float(row.pnl)
        balance += pnl
        total_profit += pnl
        if balance <= floor:
            survived = False
            break
        floor = min(0.0, max(floor, balance - abs(mll_distance)))
        if int(getattr(row, "trades", 0)) > 0:
            traded_days += 1
        best_day = max(best_day, pnl)
        consistency_ok = total_profit > 0 and best_day / total_profit <= 0.40
        if traded_days >= 3 and consistency_ok and balance > 0:
            decision = payout_request(balance, cap=6000)
            if decision.eligible:
                if first_eligible_day is None:
                    first_eligible_day = idx
                gross += decision.gross_payout
                net += decision.trader_net
                balance -= decision.gross_payout
                floor = 0.0
                traded_days = 0
                total_profit = balance
                best_day = 0.0
                cycles += 1
    return {
        "path": "XFA_CONSISTENCY",
        "survived": survived,
        "payout_eligible": first_eligible_day is not None,
        "payout_days_to_eligibility": first_eligible_day,
        "payout_cycles_survived": cycles,
        "gross_payout_available": gross,
        "trader_net_payout": net,
        "winning_days_150_count": int((daily["pnl"] >= 150).sum()) if len(daily) else 0,
    }
