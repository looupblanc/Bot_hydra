from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class EvidenceNode:
    node_id: str
    scope: str
    status: str
    parent_ids: tuple[str, ...]
    provenance_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def edge_scope_violation(source_scope: str, target_scope: str) -> bool:
    order = {
        "COMPONENT": 0,
        "EDGE_ATOM": 1,
        "REPRESENTATION": 2,
        "STRATEGY_CANDIDATE": 3,
        "PORTFOLIO": 4,
        "ACCOUNT_PATH": 5,
    }
    return order.get(target_scope, 99) > order.get(source_scope, -1)

