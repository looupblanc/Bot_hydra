from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyRollingSummary,
    RoutedTrade,
    evaluate_account_policy,
)
from hydra.account_policy.schema import (
    BasketPolicy,
    ComponentDescriptor,
    ComponentRole,
    ControllerPolicy,
    SCHEMA_VERSION,
    stable_hash,
)
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.rolling_combine import EpisodeStartPolicy
from hydra.research.qd_economic_tournament import MARKET_PAIRS
from hydra.research.rolling_combine_replay import ExactTradePath, build_exact_trade_path
from hydra.research.turbo_exact_replay import spec_from_dict, spec_to_dict
from hydra.strategies.turbo_dsl import StrategyRole, StrategySpec


VERSION = "hydra_account_policy_evolution_v6"
_WORKER_BANK_CACHE: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True, slots=True)
class ComponentRuntime:
    descriptor: ComponentDescriptor
    specification: dict[str, Any]
    events: tuple[RoutedTrade, ...]
    eligible_session_days: tuple[int, ...]
    source_kind: str

    def to_dict(self, *, include_events: bool = True) -> dict[str, Any]:
        row = {
            "descriptor": self.descriptor.to_dict(),
            "specification": self.specification,
            "eligible_session_days": list(self.eligible_session_days),
            "source_kind": self.source_kind,
        }
        if include_events:
            row["events"] = [event.to_dict() for event in self.events]
        return row


def load_v5_component_bank(
    report_root: str | Path,
    *,
    matrix_paths: Mapping[str, str],
    maximum_components: int = 36,
) -> tuple[list[ComponentRuntime], dict[str, Any]]:
    root = Path(report_root)
    latest: dict[str, dict[str, Any]] = {}
    source_by_id: dict[str, str] = {}
    for path in sorted(root.glob("combine_first_evolution_v5_epoch_*/combine_v5_*results.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id or not row.get("specification"):
                continue
            latest[candidate_id] = row
            source_by_id[candidate_id] = path.parent.name
    eligible = [row for row in latest.values() if _eligible_v5_component(row)]
    eligible.sort(key=_v5_component_rank)
    selected_rows = _round_robin_rows(eligible, maximum_components)
    matrices: dict[str, FeatureMatrix] = {}
    provisional: list[tuple[dict[str, Any], StrategySpec, ExactTradePath]] = []
    for row in selected_rows:
        spec = spec_from_dict(dict(row["specification"]))
        matrix = matrices.get(spec.market)
        if matrix is None:
            matrix = FeatureMatrix.open(matrix_paths[spec.market], mmap=True)
            matrices[spec.market] = matrix
        path = build_exact_trade_path(spec, matrix)
        if path.net_pnl <= 0.0 or path.cost_stress_1_5x_net <= 0.0 or path.event_count < 8:
            continue
        provisional.append((row, spec, path))
    clusters = _behavioral_clusters(provisional)
    runtimes: list[ComponentRuntime] = []
    cluster_members: dict[str, list[str]] = defaultdict(list)
    for row, spec, path in provisional:
        candidate_id = spec.candidate_id
        cluster = clusters[candidate_id]
        cluster_members[cluster].append(candidate_id)
        rolling = dict(row.get("rolling_combine") or {})
        descriptor = ComponentDescriptor(
            component_id=candidate_id,
            specification_hash=stable_hash(spec_to_dict(spec)),
            market=spec.market,
            execution_market=MARKET_PAIRS[spec.market],
            family=spec.family,
            timeframe=spec.timeframe,
            role=_component_role(spec, row),
            behavioral_cluster=cluster,
            source_experiment=source_by_id[candidate_id],
            source_result_hash=stable_hash(
                {
                    "candidate_id": candidate_id,
                    "exact_trade_path": row.get("exact_trade_path"),
                    "rolling_combine": rolling,
                }
            ),
            net_pnl_after_costs=path.net_pnl,
            cost_stress_net_pnl=path.cost_stress_1_5x_net,
            event_count=path.event_count,
            rolling_pass_rate=float(rolling.get("pass_rate") or 0.0),
            rolling_mll_breach_rate=float(
                rolling.get("mll_breach_rate") or 0.0
            ),
            median_target_progress=float(
                rolling.get("median_target_progress_when_not_passed") or 0.0
            ),
            expected_xfa_cycles=float(
                (row.get("rolling_xfa") or {}).get(
                    "expected_payout_cycles_before_ruin"
                )
                or 0.0
            ),
        )
        runtimes.append(
            ComponentRuntime(
                descriptor=descriptor,
                specification=spec_to_dict(spec),
                events=tuple(
                    RoutedTrade(
                        component_id=candidate_id,
                        market=spec.market,
                        side=spec.side,
                        event=event,
                    )
                    for event in path.events
                ),
                eligible_session_days=path.eligible_session_days,
                source_kind="V5_ELITE_COMPONENT",
            )
        )
    primaries = select_cluster_primaries(runtimes)
    audit = {
        "source_rows": len(latest),
        "eligible_before_exact_rebuild": len(eligible),
        "exact_rebuilt": len(runtimes),
        "behavioral_clusters": len(cluster_members),
        "cluster_members": dict(sorted(cluster_members.items())),
        "primary_components": len(primaries),
        "primary_component_ids": [row.descriptor.component_id for row in primaries],
        "backups_retained": max(0, len(runtimes) - len(primaries)),
        "q4_evidence_inherited": False,
        "status_inherited": False,
    }
    return runtimes, audit


def select_cluster_primaries(
    components: Sequence[ComponentRuntime],
) -> list[ComponentRuntime]:
    groups: dict[str, list[ComponentRuntime]] = defaultdict(list)
    for component in components:
        groups[component.descriptor.behavioral_cluster].append(component)
    output: list[ComponentRuntime] = []
    for cluster, values in sorted(groups.items()):
        values.sort(
            key=lambda row: (
                -row.descriptor.rolling_pass_rate,
                row.descriptor.rolling_mll_breach_rate,
                -row.descriptor.median_target_progress,
                -row.descriptor.cost_stress_net_pnl,
                row.descriptor.component_id,
            )
        )
        output.append(values[0])
    return output


def generate_basket_population(
    components: Sequence[ComponentRuntime],
    *,
    count: int,
    generation_index: int,
    excluded_fingerprints: Iterable[str] = (),
) -> list[BasketPolicy]:
    primaries = select_cluster_primaries(components)
    if len(primaries) < 2:
        return []
    by_id = {row.descriptor.component_id: row for row in primaries}
    ranked = sorted(
        by_id,
        key=lambda candidate_id: (
            -by_id[candidate_id].descriptor.rolling_pass_rate,
            by_id[candidate_id].descriptor.rolling_mll_breach_rate,
            -by_id[candidate_id].descriptor.median_target_progress,
            -by_id[candidate_id].descriptor.net_pnl_after_costs,
            candidate_id,
        ),
    )
    rng = np.random.default_rng(60_000 + generation_index)
    output: list[BasketPolicy] = []
    excluded = {str(value) for value in excluded_fingerprints}
    seen: set[tuple[str, ...]] = set()
    archetypes = (
        "MAXIMUM_TARGET_VELOCITY",
        "MAXIMUM_MLL_SURVIVAL",
        "BALANCED_PASS_PROBABILITY",
        "MULTI_SESSION",
        "MULTI_MARKET",
        "ALPHA_PLUS_DEFENSIVE",
        "COMBINE_PLUS_CONSISTENCY",
    )
    attempts = 0
    while len(output) < count and attempts < count * 40:
        size = 2 + attempts % min(4, len(ranked) - 1)
        size = min(size, 5, len(ranked))
        if attempts < len(ranked) - 1:
            chosen = tuple(sorted((ranked[0], ranked[attempts + 1])))
        else:
            chosen = tuple(sorted(rng.choice(ranked, size=size, replace=False).tolist()))
        attempts += 1
        if chosen in seen or not _basket_is_distinct(chosen, by_id):
            continue
        seen.add(chosen)
        archetype = archetypes[len(output) % len(archetypes)]
        priority = tuple(sorted(chosen, key=lambda item: ranked.index(item)))
        fingerprint = stable_hash(
            {
                "component_ids": chosen,
                "archetype": archetype,
                "maximum_simultaneous_positions": min(4, len(chosen)),
                "maximum_mini_equivalent": 15,
                "conflict_policy": "FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
                "component_priority": priority,
                "policy_version": SCHEMA_VERSION,
            }
        )
        if fingerprint in excluded:
            continue
        excluded.add(fingerprint)
        output.append(
            BasketPolicy(
                policy_id="basket_v6_" + fingerprint[:18],
                component_ids=chosen,
                archetype=archetype,
                maximum_simultaneous_positions=min(4, len(chosen)),
                maximum_mini_equivalent=15,
                component_priority=priority,
            )
        )
    return output


def write_component_bank_manifest(
    path: str | Path,
    components: Sequence[ComponentRuntime],
) -> dict[str, Any]:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    eligible_days = sorted(
        {
            day
            for component in components
            for day in component.eligible_session_days
        }
    )
    payload = {
        "schema": "hydra_v6_component_bank_v1",
        "version": VERSION,
        "components": {
            component.descriptor.component_id: component.to_dict(include_events=True)
            for component in sorted(
                components, key=lambda row: row.descriptor.component_id
            )
        },
        "eligible_session_days": eligible_days,
        "outbound_order_capability": False,
        "q4_access_allowed": False,
        "status_inheritance": False,
    }
    payload["manifest_hash"] = stable_hash(payload)
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload


def run_account_policy_job(payload: Mapping[str, Any]) -> dict[str, Any]:
    bank_path = str(payload["component_bank_path"])
    bank = _WORKER_BANK_CACHE.get(bank_path)
    if bank is None:
        bank = json.loads(Path(bank_path).read_text(encoding="utf-8"))
        expected = str(bank.get("manifest_hash") or "")
        unhashed = dict(bank)
        unhashed.pop("manifest_hash", None)
        if not expected or expected != stable_hash(unhashed):
            raise ValueError("component bank manifest hash mismatch")
        _WORKER_BANK_CACHE[bank_path] = bank
    basket = BasketPolicy.from_dict(payload["basket"])
    controller = (
        ControllerPolicy.from_dict(payload["controller"])
        if payload.get("controller")
        else None
    )
    components: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id in basket.component_ids:
        value = bank["components"][component_id]
        components[component_id] = tuple(
            RoutedTrade.from_dict(row) for row in value["events"]
        )
    policy = EpisodeStartPolicy(
        maximum_starts=int(payload.get("maximum_starts") or 24),
        minimum_spacing_sessions=int(payload.get("minimum_spacing_sessions") or 5),
        minimum_observation_sessions=int(
            payload.get("minimum_observation_sessions") or 30
        ),
        maximum_duration_sessions=int(
            payload.get("maximum_duration_sessions") or 60
        ),
        regime_balanced=False,
    )
    summary = evaluate_account_policy(
        components,
        bank["eligible_session_days"],
        basket=basket,
        controller=controller,
        episode_policy=policy,
        explicit_start_days=payload.get("episode_start_days"),
    )
    independent_starts = _independent_episode_starts(
        summary.episode_start_days,
        bank["eligible_session_days"],
        int(payload.get("maximum_duration_sessions") or 60),
    )
    return {
        "policy_id": summary.policy_id,
        "basket_id": basket.policy_id,
        "basket": basket.to_dict(),
        "controller": controller.to_dict() if controller else None,
        "summary": summary.to_dict(include_episodes=False),
        "episode_metrics": [
            {
                "start_day": episode.start_day,
                "passed": episode.passed,
                "mll_breached": episode.mll_breached,
                "target_progress": episode.target_progress,
                "consistency_ok": episode.consistency_ok,
                "net_pnl": episode.net_pnl,
                "effective_block": episode.start_day in independent_starts,
            }
            for episode in summary.episodes
        ],
        "episode_start_days": list(summary.episode_start_days),
        "q4_access_count_delta": 0,
        "outbound_order_capability": False,
    }


def _independent_episode_starts(
    starts: Sequence[int], eligible_days: Sequence[int], duration: int
) -> set[int]:
    positions = {
        int(day): index
        for index, day in enumerate(sorted({int(value) for value in eligible_days}))
    }
    retained: set[int] = set()
    next_position = -1
    for start in sorted(int(value) for value in starts):
        position = positions[start]
        if position < next_position:
            continue
        retained.add(start)
        next_position = position + duration
    return retained


def summary_from_dict(value: Mapping[str, Any]) -> AccountPolicyRollingSummary:
    from hydra.account_policy.basket import AccountPolicyEpisode
    from hydra.propfirm.combine_episode import CombineTerminal

    episodes: list[AccountPolicyEpisode] = []
    for row in value.get("episodes", ()):
        item = dict(row)
        item["terminal"] = CombineTerminal(str(item["terminal"]))
        item["risk_allocation_path"] = tuple(item.get("risk_allocation_path", ()))
        item["daily_path"] = tuple(item.get("daily_path", ()))
        item.pop("passed", None)
        episodes.append(AccountPolicyEpisode(**item))
    fields = dict(value)
    fields["episode_start_days"] = tuple(fields["episode_start_days"])
    fields["episodes"] = tuple(episodes)
    return AccountPolicyRollingSummary(**fields)


def _eligible_v5_component(row: Mapping[str, Any]) -> bool:
    exact = dict(row.get("exact_trade_path") or {})
    rolling = dict(row.get("rolling_combine") or {})
    return bool(
        not row.get("hard_invalidation")
        and float(exact.get("net_pnl") or 0.0) > 0.0
        and float(exact.get("cost_stress_1_5x_net") or 0.0) > 0.0
        and int(exact.get("event_count") or 0) >= 8
        and float(rolling.get("mll_breach_rate") or 0.0) <= 0.25
    )


def _v5_component_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    exact = dict(row.get("exact_trade_path") or {})
    rolling = dict(row.get("rolling_combine") or {})
    xfa = dict(row.get("rolling_xfa") or {})
    return (
        -float(rolling.get("pass_rate") or 0.0),
        float(rolling.get("mll_breach_rate") or 0.0),
        -float(rolling.get("median_target_progress_when_not_passed") or 0.0),
        -float(xfa.get("expected_payout_cycles_before_ruin") or 0.0),
        -float(exact.get("cost_stress_1_5x_net") or 0.0),
        str(row.get("candidate_id") or ""),
    )


def _round_robin_rows(
    rows: Sequence[dict[str, Any]], maximum: int
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        spec = dict(row["specification"])
        role_value = int(spec.get("role") or int(StrategyRole.ALPHA))
        groups[(str(spec["market"]), str(spec["family"]), StrategyRole(role_value).name)].append(row)
    for values in groups.values():
        values.sort(key=_v5_component_rank)
    output: list[dict[str, Any]] = []
    depth = 0
    while len(output) < maximum:
        inserted = False
        for key in sorted(groups):
            values = groups[key]
            if depth < len(values):
                output.append(values[depth])
                inserted = True
                if len(output) >= maximum:
                    break
        if not inserted:
            break
        depth += 1
    return output


def _component_role(spec: StrategySpec, row: Mapping[str, Any]) -> ComponentRole:
    rolling = dict(row.get("rolling_combine") or {})
    xfa = dict(row.get("rolling_xfa") or {})
    if spec.role is StrategyRole.XFA_PAYOUT or float(
        xfa.get("expected_payout_cycles_before_ruin") or 0.0
    ) >= 1.0:
        return ComponentRole.XFA_PAYOUT_COMPONENT
    if spec.role in {
        StrategyRole.DEFENSIVE,
        StrategyRole.PORTFOLIO_ONLY,
        StrategyRole.HAZARD,
    }:
        return ComponentRole.DEFENSIVE_COMPONENT
    if float(rolling.get("pass_rate") or 0.0) > 0.0:
        return ComponentRole.TARGET_VELOCITY_COMPONENT
    if int((row.get("exact_trade_path") or {}).get("event_count") or 0) < 20:
        return ComponentRole.RARE_EVENT_COMPONENT
    if spec.session_code >= 0:
        return ComponentRole.SESSION_SPECIALIST
    return ComponentRole.ALPHA_COMPONENT


def _behavioral_clusters(
    values: Sequence[tuple[dict[str, Any], StrategySpec, ExactTradePath]],
) -> dict[str, str]:
    parent = {spec.candidate_id: spec.candidate_id for _, spec, _ in values}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    for index, (_, left_spec, left_path) in enumerate(values):
        left_days = _daily_pnl(left_path.events)
        left_decisions = {event.decision_ns for event in left_path.events}
        for _, right_spec, right_path in values[index + 1 :]:
            if left_spec.market != right_spec.market:
                continue
            right_days = _daily_pnl(right_path.events)
            common = sorted(set(left_days) & set(right_days))
            correlation = 0.0
            if len(common) >= 5:
                correlation = float(
                    np.corrcoef(
                        [left_days[day] for day in common],
                        [right_days[day] for day in common],
                    )[0, 1]
                )
                if not np.isfinite(correlation):
                    correlation = 0.0
            right_decisions = {event.decision_ns for event in right_path.events}
            union_count = len(left_decisions | right_decisions)
            overlap = (
                len(left_decisions & right_decisions) / union_count
                if union_count
                else 1.0
            )
            if overlap >= 0.80 or (correlation >= 0.95 and len(common) >= 10):
                union(left_spec.candidate_id, right_spec.candidate_id)
    clusters: dict[str, str] = {}
    for candidate_id in sorted(parent):
        clusters[candidate_id] = "cluster_" + hashlib.sha256(
            find(candidate_id).encode("utf-8")
        ).hexdigest()[:16]
    return clusters


def _daily_pnl(events: Sequence[TradePathEvent]) -> dict[int, float]:
    output: dict[int, float] = defaultdict(float)
    for event in events:
        output[event.session_day] += event.net_pnl
    return output


def _basket_is_distinct(
    component_ids: Sequence[str], by_id: Mapping[str, ComponentRuntime]
) -> bool:
    clusters = {by_id[item].descriptor.behavioral_cluster for item in component_ids}
    markets = {by_id[item].descriptor.market for item in component_ids}
    roles = {by_id[item].descriptor.role for item in component_ids}
    return bool(
        len(clusters) == len(component_ids)
        and (len(markets) >= 2 or len(roles) >= 2)
    )


__all__ = [
    "ComponentRuntime",
    "generate_basket_population",
    "load_v5_component_bank",
    "run_account_policy_job",
    "select_cluster_primaries",
    "summary_from_dict",
    "write_component_bank_manifest",
]
