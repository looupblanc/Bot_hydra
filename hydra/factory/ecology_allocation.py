from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EcologyAllocationPolicy:
    maximum_elites: int = 20
    preferred_weights: dict[str, float] = field(
        default_factory=lambda: {
            "equity_indices": 0.75,
            "energy": 0.25,
            "metals": 0.0,
        }
    )
    minimum_shares_when_sufficient: dict[str, float] = field(
        default_factory=lambda: {"energy": 0.15}
    )


def feasible_ecology_quotas(
    candidates: list[dict[str, Any]], policy: EcologyAllocationPolicy
) -> dict[str, int]:
    """Allocate every feasible slot without requiring a missing ecology.

    Quotas are targets for the first selection pass. A later selector may
    redistribute any unfilled target, but this function never allocates a slot
    to an ecology without a valid survivor.
    """
    available = Counter(str(item["market_ecology"]) for item in candidates)
    active = sorted(ecology for ecology, count in available.items() if count > 0)
    target_total = min(policy.maximum_elites, sum(available.values()))
    if target_total <= 0:
        return {}
    if len(active) == 1:
        return {active[0]: target_total}

    raw_weights = {
        ecology: max(float(policy.preferred_weights.get(ecology, 0.0)), 0.0)
        for ecology in active
    }
    if sum(raw_weights.values()) <= 0:
        raw_weights = {ecology: 1.0 for ecology in active}
    weight_total = sum(raw_weights.values())
    normalized = {ecology: raw_weights[ecology] / weight_total for ecology in active}
    exact = {ecology: normalized[ecology] * target_total for ecology in active}
    quotas = {
        ecology: min(int(math.floor(exact[ecology])), available[ecology])
        for ecology in active
    }

    for ecology, minimum_share in policy.minimum_shares_when_sufficient.items():
        if ecology not in available:
            continue
        minimum = min(int(math.ceil(target_total * minimum_share)), available[ecology])
        quotas[ecology] = max(quotas.get(ecology, 0), minimum)

    while sum(quotas.values()) > target_total:
        removable = [
            ecology
            for ecology in active
            if quotas[ecology]
            > min(
                int(math.ceil(target_total * policy.minimum_shares_when_sufficient.get(ecology, 0.0))),
                available[ecology],
            )
        ]
        if not removable:
            break
        ecology = min(removable, key=lambda item: (exact[item] - quotas[item], item))
        quotas[ecology] -= 1

    while sum(quotas.values()) < target_total:
        choices = [ecology for ecology in active if quotas[ecology] < available[ecology]]
        if not choices:
            break
        ecology = max(
            choices,
            key=lambda item: (exact[item] - quotas[item], available[item] - quotas[item], item),
        )
        quotas[ecology] += 1
    return {ecology: quota for ecology, quota in quotas.items() if quota > 0}
