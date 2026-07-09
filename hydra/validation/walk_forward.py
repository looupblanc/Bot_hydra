from __future__ import annotations


def walk_forward_score(trades: list[dict], folds: int = 3) -> float:
    if len(trades) < folds:
        return 0.0
    chunks = [trades[i::folds] for i in range(folds)]
    profitable = sum(1 for chunk in chunks if sum(t["pnl"] for t in chunk) > 0)
    return profitable / folds
