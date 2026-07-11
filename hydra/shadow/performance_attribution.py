from __future__ import annotations

from collections import defaultdict
from typing import Any


def attribute_virtual_pnl(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_strategy: dict[str, float] = defaultdict(float)
    for row in records:
        by_strategy[str(row.get("strategy_id") or "unknown")] += float(row.get("net_pnl", 0.0))
    return {
        "net_pnl": sum(by_strategy.values()),
        "by_strategy": dict(sorted(by_strategy.items())),
        "record_count": len(records),
    }
