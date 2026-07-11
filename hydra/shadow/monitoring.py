from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def append_shadow_event(path: str | Path, event: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True, default=str) + "\n")


def shadow_summary(events: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "events": len(events),
        "virtual_fills": sum(item.get("status") == "VIRTUAL_FILLED" for item in events),
        "rejections": sum(item.get("status") == "REJECTED" for item in events),
        "outbound_orders": 0,
    }
