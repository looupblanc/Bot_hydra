from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from hydra.factory.ecology_allocation import (
    EcologyAllocationPolicy,
    feasible_ecology_quotas,
)


PROHIBITED_SELECTION_KEY_TOKENS = (
    "2024",
    "validation",
    "holdout",
    "q4",
    "future",
    "test_result",
)


class SelectorLeakageError(RuntimeError):
    pass


@dataclass(frozen=True)
class SelectorV2Policy:
    maximum_elites: int = 20
    maximum_controls: int = 2
    soft_family_share: float = 0.25
    soft_market_share: float = 0.40
    preferred_ecology_weights: dict[str, float] = field(
        default_factory=lambda: {
            "equity_indices": 0.75,
            "energy": 0.25,
            "metals": 0.0,
        }
    )
    minimum_ecology_shares: dict[str, float] = field(
        default_factory=lambda: {"energy": 0.15}
    )
    selector_version: str = "quality_diversity_selector_v2"

    def allocation_policy(self) -> EcologyAllocationPolicy:
        return EcologyAllocationPolicy(
            maximum_elites=self.maximum_elites,
            preferred_weights=dict(self.preferred_ecology_weights),
            minimum_shares_when_sufficient=dict(self.minimum_ecology_shares),
        )


@dataclass(frozen=True)
class SelectorV2Result:
    elites: tuple[dict[str, Any], ...]
    negative_controls: tuple[dict[str, Any], ...]
    audit: dict[str, Any]


def select_quality_diversity_elites_v2(
    survivors: list[dict[str, Any]],
    *,
    failed_candidates: list[dict[str, Any]] | None = None,
    policy: SelectorV2Policy | None = None,
) -> SelectorV2Result:
    resolved = policy or SelectorV2Policy()
    _assert_no_future_evidence(survivors)
    _assert_no_future_evidence(failed_candidates or [])
    eligible = _deduplicate_survivors(survivors)
    ranked = sorted(eligible, key=_ranking_key)
    quotas = feasible_ecology_quotas(ranked, resolved.allocation_policy())
    target = min(resolved.maximum_elites, len(ranked))
    family_limit = max(1, int(resolved.maximum_elites * resolved.soft_family_share))
    market_limit = max(1, int(resolved.maximum_elites * resolved.soft_market_share))

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    selected_lineages: set[str] = set()
    family_counts: Counter[str] = Counter()
    market_counts: Counter[str] = Counter()
    ecology_counts: Counter[str] = Counter()
    relaxations: list[dict[str, Any]] = []

    def add(candidate: dict[str, Any], *, relaxed: bool, pass_name: str) -> bool:
        candidate_id = str(candidate["candidate_id"])
        lineage_id = str(candidate["lineage_id"])
        if candidate_id in selected_ids or lineage_id in selected_lineages:
            return False
        family = str(candidate["mechanism_family"])
        market = str(candidate["market"])
        if not relaxed and (
            family_counts[family] >= family_limit or market_counts[market] >= market_limit
        ):
            return False
        if relaxed:
            exceeded = []
            if family_counts[family] >= family_limit:
                exceeded.append("family_soft_cap")
            if market_counts[market] >= market_limit:
                exceeded.append("market_soft_cap")
            if exceeded:
                relaxations.append(
                    {
                        "candidate_id": candidate_id,
                        "pass": pass_name,
                        "relaxed_constraints": exceeded,
                    }
                )
        selected.append(candidate)
        selected_ids.add(candidate_id)
        selected_lineages.add(lineage_id)
        family_counts[family] += 1
        market_counts[market] += 1
        ecology_counts[str(candidate["market_ecology"])] += 1
        return True

    # Pass 1: fill preregistered ecology targets while respecting soft caps.
    for ecology in sorted(quotas):
        for candidate in ranked:
            if len(selected) >= target or ecology_counts[ecology] >= quotas[ecology]:
                break
            if str(candidate["market_ecology"]) == ecology:
                add(candidate, relaxed=False, pass_name="ecology_target")

    # Pass 2: redistribute unused quotas across every active ecology under the
    # same soft diversity constraints.
    for candidate in ranked:
        if len(selected) >= target:
            break
        add(candidate, relaxed=False, pass_name="quota_redistribution")

    # Pass 3: soft constraints may not make selection infeasible. Fill every
    # remaining feasible unique lineage and record the exact relaxation.
    for candidate in ranked:
        if len(selected) >= target:
            break
        add(candidate, relaxed=True, pass_name="maximum_feasible_fill")

    controls = _select_negative_controls(
        failed_candidates or [],
        selected_ids=selected_ids,
        maximum=resolved.maximum_controls,
        active_elite_ecologies=set(ecology_counts),
    )
    audit = {
        "selector_version": resolved.selector_version,
        "policy": asdict(resolved),
        "input_survivors": len(survivors),
        "unique_eligible_survivors": len(eligible),
        "target_elites": target,
        "selected_elites": len(selected),
        "maximum_feasible_achieved": len(selected) == target,
        "initial_ecology_quotas": quotas,
        "ecology_counts": dict(ecology_counts),
        "family_counts": dict(family_counts),
        "market_counts": dict(market_counts),
        "unique_lineages": len(selected_lineages),
        "soft_cap_relaxations": relaxations,
        "negative_control_count": len(controls),
        "negative_controls_count_as_elites": False,
        "uses_2024_results": False,
    }
    return SelectorV2Result(tuple(selected), tuple(controls), audit)


def _deduplicate_survivors(survivors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_fingerprint: dict[str, dict[str, Any]] = {}
    for candidate in survivors:
        if not bool(candidate.get("stage1_pass", True)):
            continue
        fingerprint = str(candidate.get("structural_fingerprint") or "")
        lineage = str(candidate.get("lineage_id") or "")
        if not fingerprint or not lineage:
            raise ValueError("Every survivor requires a structural fingerprint and lineage.")
        current = by_fingerprint.get(fingerprint)
        if current is None or _ranking_key(candidate) < _ranking_key(current):
            by_fingerprint[fingerprint] = candidate
    return list(by_fingerprint.values())


def _ranking_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
    discovery = candidate.get("discovery") or {}
    cost_net = float(discovery.get("cost_stress_1_5x_net") or 0.0)
    net = float(discovery.get("net_pnl") or 0.0)
    drawdown = float(discovery.get("maximum_drawdown") or float("inf"))
    concentration = float(discovery.get("best_positive_event_share") or 1.0)
    events = int(discovery.get("events") or 0)
    return (
        -int(cost_net > 0),
        -cost_net,
        -net,
        drawdown,
        concentration,
        -events,
        str(candidate.get("structural_fingerprint") or ""),
        str(candidate.get("candidate_id") or ""),
    )


def _select_negative_controls(
    failed_candidates: list[dict[str, Any]],
    *,
    selected_ids: set[str],
    maximum: int,
    active_elite_ecologies: set[str],
) -> list[dict[str, Any]]:
    controls = [
        candidate
        for candidate in failed_candidates
        if not bool(candidate.get("stage1_pass", False))
        and str(candidate.get("candidate_id") or "") not in selected_ids
    ]
    controls.sort(
        key=lambda item: (
            0
            if str(item.get("market_ecology")) == "metals"
            and "metals" not in active_elite_ecologies
            else 1,
            _ranking_key(item),
        )
    )
    selected: list[dict[str, Any]] = []
    fingerprints: set[str] = set()
    for candidate in controls:
        fingerprint = str(candidate.get("structural_fingerprint") or "")
        if not fingerprint or fingerprint in fingerprints:
            continue
        selected.append(candidate)
        fingerprints.add(fingerprint)
        if len(selected) >= maximum:
            break
    return selected


def _assert_no_future_evidence(candidates: list[dict[str, Any]]) -> None:
    def walk(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                lowered = str(key).lower()
                if any(token in lowered for token in PROHIBITED_SELECTION_KEY_TOKENS):
                    raise SelectorLeakageError(
                        f"Selector input contains prohibited field: {'.'.join((*path, str(key)))}"
                    )
                walk(item, (*path, str(key)))
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, (*path, str(index)))

    walk(candidates, ("candidates",))
