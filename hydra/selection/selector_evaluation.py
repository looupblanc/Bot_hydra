from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import RoutedTrade
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    ComplementarySleeveBasketPolicy,
)
from hydra.economic_evolution.account_elite_robustness import (
    EliteRobustnessPolicy,
)
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    _restress_routed_trade,
)
from hydra.economic_evolution.account_static_parent_basket import (
    StaticParentBasketPolicy,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.selection.risk_frontier import (
    STATIC_RISK_FRONTIER_VERSION,
    StaticIntegerMicroPolicy,
    StaticRiskTier,
    adapt_static_risk_policy,
    resolve_static_risk_tier,
    static_risk_router_context,
)
from hydra.selection.time_to_combine import (
    CensoredAccountPolicyEpisode,
    TimeToCombineHorizonSummary,
    evaluate_time_to_combine,
    run_censored_shared_account_episode,
    summarize_time_to_combine,
)


PRIMARY_SELECTOR_HORIZON_DAYS = 40
SELECTOR_EVALUATION_VERSION = "hydra_v73_selector_immutable_ledger_evaluation_v1"


class SelectorEvaluationError(ValueError):
    """Raised when an evaluation could cross a frozen inference boundary."""


PersistedSourcePolicy = StaticParentBasketPolicy | EliteRobustnessPolicy


@dataclass(frozen=True, slots=True)
class FrozenStaticAccountPolicy:
    """Executable basket and controller frozen at one discrete risk tier."""

    source_policy_id: str
    source_policy_fingerprint: str
    basket: ComplementarySleeveBasketPolicy
    controller: StaticIntegerMicroPolicy
    risk_tier: StaticRiskTier
    evaluation_version: str = SELECTOR_EVALUATION_VERSION

    def __post_init__(self) -> None:
        if self.basket.policy_id != self.controller.basket_policy_id:
            raise SelectorEvaluationError("basket/controller identity drift")
        if self.basket.component_ids != self.controller.component_priority:
            raise SelectorEvaluationError("basket/controller membership drift")
        if self.controller.risk_label != self.risk_tier.label:
            raise SelectorEvaluationError("controller risk tier is not frozen")
        if self.evaluation_version != SELECTOR_EVALUATION_VERSION:
            raise SelectorEvaluationError("selector evaluation version drift")

    @property
    def variant_id(self) -> str:
        return f"{self.source_policy_id}::{self.risk_tier.label}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "source_policy_id": self.source_policy_id,
            "source_policy_fingerprint": self.source_policy_fingerprint,
            "component_ids": list(self.basket.component_ids),
            "risk_tier": self.risk_tier.to_dict(),
            "basket": {
                "policy_id": self.basket.policy_id,
                "component_ids": list(self.basket.component_ids),
                "archetype": self.basket.archetype,
                "maximum_simultaneous_positions": (
                    self.basket.maximum_simultaneous_positions
                ),
                "maximum_mini_equivalent": self.basket.maximum_mini_equivalent,
                "conflict_policy": self.basket.conflict_policy,
                "component_priority": list(self.basket.component_priority),
                "policy_version": self.basket.policy_version,
            },
            "controller": self.controller.to_dict(),
            "evaluation_version": self.evaluation_version,
        }


def static_parent_policy_from_dict(
    value: Mapping[str, Any],
) -> StaticParentBasketPolicy:
    """Rehydrate one 0023 basket and verify its persisted fingerprint."""

    expected = _required_fingerprint(value)
    policy = StaticParentBasketPolicy(
        policy_id=str(value["policy_id"]),
        parent_policy_id=str(value["parent_policy_id"]),
        parent_policy_fingerprint=str(value["parent_policy_fingerprint"]),
        source_parent_ids=tuple(str(row) for row in value["source_parent_ids"]),
        component_ids=tuple(str(row) for row in value["component_ids"]),
        retained_added_sleeve_id=str(value["retained_added_sleeve_id"]),
        mutation_family=str(value["mutation_family"]),
        failure_target=str(value["failure_target"]),
        exact_change=_exact_change(value.get("exact_change", {})),
        expected_effect=str(value["expected_effect"]),
        high_risk_units=int(value["high_risk_units"]),
        daily_loss_guard=float(value["daily_loss_guard"]),
        daily_profit_lock=float(value["daily_profit_lock"]),
        critical_buffer=float(value["critical_buffer"]),
        high_zone_buffer=float(value["high_zone_buffer"]),
        high_zone_remaining_target=float(value["high_zone_remaining_target"]),
        middle_zone_buffer=float(value["middle_zone_buffer"]),
        middle_zone_remaining_target=float(value["middle_zone_remaining_target"]),
        middle_risk_units=int(value["middle_risk_units"]),
        maximum_simultaneous_positions=int(
            value["maximum_simultaneous_positions"]
        ),
        maximum_mini_equivalent=int(value["maximum_mini_equivalent"]),
        assembly_profile=str(value["assembly_profile"]),
        version=int(value.get("version", 1)),
        inherited_status=value.get("inherited_status"),
    )
    _verify_policy_fingerprint(policy, expected)
    return policy


def elite_robustness_policy_from_dict(
    value: Mapping[str, Any],
) -> EliteRobustnessPolicy:
    """Rehydrate one frozen parent and verify its persisted fingerprint."""

    expected = _required_fingerprint(value)
    policy = EliteRobustnessPolicy(
        policy_id=str(value["policy_id"]),
        parent_policy_id=str(value["parent_policy_id"]),
        parent_policy_fingerprint=str(value["parent_policy_fingerprint"]),
        component_ids=tuple(str(row) for row in value["component_ids"]),
        retained_added_sleeve_id=str(value["retained_added_sleeve_id"]),
        mutation_family=str(value["mutation_family"]),
        failure_target=str(value["failure_target"]),
        exact_change=_exact_change(value.get("exact_change", {})),
        expected_effect=str(value["expected_effect"]),
        high_risk_units=int(value["high_risk_units"]),
        daily_loss_guard=float(value["daily_loss_guard"]),
        daily_profit_lock=float(value["daily_profit_lock"]),
        critical_buffer=float(value["critical_buffer"]),
        high_zone_buffer=float(value["high_zone_buffer"]),
        high_zone_remaining_target=float(value["high_zone_remaining_target"]),
        middle_zone_buffer=float(value["middle_zone_buffer"]),
        middle_zone_remaining_target=float(value["middle_zone_remaining_target"]),
        middle_risk_units=int(value["middle_risk_units"]),
        maximum_simultaneous_positions=int(
            value["maximum_simultaneous_positions"]
        ),
        maximum_mini_equivalent=int(value["maximum_mini_equivalent"]),
        version=int(value.get("version", 1)),
        inherited_status=value.get("inherited_status"),
    )
    _verify_policy_fingerprint(policy, expected)
    return policy


def persisted_policy_from_dict(value: Mapping[str, Any]) -> PersistedSourcePolicy:
    """Dispatch persisted 0023 baskets and their frozen parent baselines."""

    if "source_parent_ids" in value or "assembly_profile" in value:
        return static_parent_policy_from_dict(value)
    return elite_robustness_policy_from_dict(value)


def build_frozen_static_account_policy(
    source_policy: PersistedSourcePolicy | Mapping[str, Any],
    risk_level: StaticRiskTier | str | float,
) -> FrozenStaticAccountPolicy:
    """Compile membership and one preregistered risk tier, without mutation."""

    source = (
        persisted_policy_from_dict(source_policy)
        if isinstance(source_policy, Mapping)
        else source_policy
    )
    if not isinstance(source, (StaticParentBasketPolicy, EliteRobustnessPolicy)):
        raise SelectorEvaluationError("unsupported source account policy")
    tier = resolve_static_risk_tier(risk_level)
    controller = adapt_static_risk_policy(source, tier)
    basket = ComplementarySleeveBasketPolicy(
        policy_id=controller.basket_policy_id,
        component_ids=source.component_ids,
        archetype="V73_NESTED_SELECTOR_FROZEN_STATIC_RISK",
        maximum_simultaneous_positions=source.maximum_simultaneous_positions,
        maximum_mini_equivalent=source.maximum_mini_equivalent,
        conflict_policy="FIXED_PRIORITY_SAME_MARKET_EXCLUSIVE",
        component_priority=source.component_ids,
        policy_version=STATIC_RISK_FRONTIER_VERSION,
    )
    return FrozenStaticAccountPolicy(
        source_policy_id=source.policy_id,
        source_policy_fingerprint=source.structural_fingerprint,
        basket=basket,
        controller=controller,
        risk_tier=tier,
    )


def evaluate_policy_block(
    source_policy: PersistedSourcePolicy | Mapping[str, Any],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    risk_level: StaticRiskTier | str | float,
    block_id: str,
    session_days: Sequence[int],
    start_days: Sequence[int],
    include_time_to_combine: bool = False,
) -> dict[str, Any]:
    """Replay one frozen variant on one explicitly bounded temporal block.

    The supplied runtimes are already-compiled immutable trade ledgers.  This
    function performs no feature, signal, contract-map, entry, or exit work.
    Events are only filtered to the exact block-day set and cost-restressed.
    """

    if not str(block_id).strip():
        raise SelectorEvaluationError("block_id is required")
    days = _unique_ordered_ints(session_days, name="session_days")
    starts = _unique_ordered_ints(start_days, name="start_days")
    day_set = set(days)
    if any(start not in day_set for start in starts):
        raise SelectorEvaluationError("every start day must belong to the block")

    frozen = build_frozen_static_account_policy(source_policy, risk_level)
    missing = [row for row in frozen.basket.component_ids if row not in runtimes]
    if missing:
        raise SelectorEvaluationError(f"missing immutable runtimes: {missing}")

    component_events: dict[str, tuple[RoutedTrade, ...]] = {}
    runtime_hashes: dict[str, str] = {}
    for component_id in frozen.basket.component_ids:
        runtime = runtimes[component_id]
        if runtime.sleeve_id != component_id:
            raise SelectorEvaluationError("runtime lookup identity drift")
        if not day_set.issubset(set(runtime.eligible_session_days)):
            raise SelectorEvaluationError(
                f"runtime {component_id} does not cover every block session day"
            )
        # Filtering prevents an event on an embargo/gap day from entering the
        # simulator merely because its integer day lies between block bounds.
        component_events[component_id] = tuple(
            row for row in runtime.events if row.event.session_day in day_set
        )
        runtime_hashes[component_id] = runtime.specification_hash

    stressed_events = {
        component_id: tuple(
            _restress_routed_trade(row, cost_stress=1.5) for row in values
        )
        for component_id, values in component_events.items()
    }

    if include_time_to_combine:
        with static_risk_router_context():
            normal_horizons = evaluate_time_to_combine(
                component_events,
                days,
                basket=frozen.basket,  # type: ignore[arg-type]
                controller=frozen.controller,  # type: ignore[arg-type]
                start_days=starts,
                block_id=str(block_id),
            )
        with static_risk_router_context():
            stressed_horizons = evaluate_time_to_combine(
                stressed_events,
                days,
                basket=frozen.basket,  # type: ignore[arg-type]
                controller=frozen.controller,  # type: ignore[arg-type]
                start_days=starts,
                block_id=str(block_id),
            )
        normal = normal_horizons[str(PRIMARY_SELECTOR_HORIZON_DAYS)]
        stressed = stressed_horizons[str(PRIMARY_SELECTOR_HORIZON_DAYS)]
        time_to_combine: dict[str, Any] | None = {
            "normal": {
                key: row.to_dict() for key, row in normal_horizons.items()
            },
            "stress_1_5x": {
                key: row.to_dict() for key, row in stressed_horizons.items()
            },
        }
    else:
        normal = _evaluate_primary_horizon(
            component_events,
            days,
            starts,
            frozen=frozen,
            block_id=str(block_id),
        )
        stressed = _evaluate_primary_horizon(
            stressed_events,
            days,
            starts,
            frozen=frozen,
            block_id=str(block_id),
        )
        time_to_combine = None

    return _block_metrics(
        frozen,
        block_id=str(block_id),
        session_days=days,
        starts=starts,
        runtime_hashes=runtime_hashes,
        normal=normal,
        stressed=stressed,
        time_to_combine=time_to_combine,
    )


def aggregate_design_block_metrics(
    block_metrics: Sequence[Mapping[str, Any]],
    *,
    allowed_block_ids: Sequence[str],
    heldout_block_id: str | None = None,
) -> dict[str, Any]:
    """Pool design evidence while enforcing the outer-fold block boundary."""

    allowed = tuple(str(row) for row in allowed_block_ids)
    if not allowed or len(set(allowed)) != len(allowed):
        raise SelectorEvaluationError("allowed block IDs must be non-empty and unique")
    heldout = None if heldout_block_id is None else str(heldout_block_id)
    if heldout is not None and heldout in set(allowed):
        raise SelectorEvaluationError("held-out block cannot be allowed for design")
    rows = tuple(dict(row) for row in block_metrics)
    if not rows:
        raise SelectorEvaluationError("design aggregation needs block metrics")
    observed = tuple(str(row.get("block_id", "")) for row in rows)
    if heldout is not None and heldout in set(observed):
        raise SelectorEvaluationError("held-out evidence was passed into design")
    if len(set(observed)) != len(observed):
        raise SelectorEvaluationError("design block metrics contain duplicates")
    if set(observed) != set(allowed):
        raise SelectorEvaluationError(
            "design metrics must contain exactly the explicitly allowed blocks"
        )
    ordered = tuple(sorted(rows, key=lambda row: allowed.index(str(row["block_id"]))))

    identity_fields = (
        "variant_id",
        "policy_id",
        "source_policy_fingerprint",
        "risk_label",
        "micro_risk_units",
        "component_ids",
    )
    for field in identity_fields:
        reference = ordered[0][field]
        if any(row[field] != reference for row in ordered[1:]):
            raise SelectorEvaluationError(f"design variant {field} drift across blocks")

    normal_episodes = _pooled_episode_rows(ordered, "normal")
    stress_episodes = _pooled_episode_rows(ordered, "stress_1_5x")
    normal_net = np.asarray([float(row["net_pnl"]) for row in normal_episodes])
    stress_net = np.asarray([float(row["net_pnl"]) for row in stress_episodes])
    stress_progress = np.asarray(
        [float(row["target_progress"]) for row in stress_episodes]
    )
    normal_mll = sum(bool(row["mll_breached"]) for row in normal_episodes)
    stress_mll = sum(bool(row["mll_breached"]) for row in stress_episodes)
    normal_consistency = np.mean(
        [bool(row["consistency_ok"]) for row in normal_episodes]
    )
    stress_consistency = np.mean(
        [bool(row["consistency_ok"]) for row in stress_episodes]
    )
    normal_mll_rate = normal_mll / len(normal_episodes)
    stressed_mll_rate = stress_mll / len(stress_episodes)

    contributions = _sum_contributions(
        row["stressed_component_contributions"] for row in ordered
    )
    normal_contributions = _sum_contributions(
        row["normal_component_contributions"] for row in ordered
    )
    maximum_component_share = _maximum_positive_share(contributions)
    positive_block_nets = [
        max(0.0, float(row["stressed_net_usd"])) for row in ordered
    ]
    positive_block_total = sum(positive_block_nets)
    maximum_block_share = (
        max(positive_block_nets, default=0.0) / positive_block_total
        if positive_block_total > 0.0
        else 1.0
    )
    behavior_payload = {
        "allowed_block_ids": list(allowed),
        "risk_label": ordered[0]["risk_label"],
        "normal": [_behavior_episode(row) for row in normal_episodes],
        "stress_1_5x": [_behavior_episode(row) for row in stress_episodes],
    }
    behavior = stable_hash(behavior_payload)

    normal_pass_count = sum(
        bool(row["target_reached"]) for row in normal_episodes
    )
    stress_pass_count = sum(
        bool(row["target_reached"]) for row in stress_episodes
    )
    normal_net_total = float(np.sum(normal_net))
    stress_net_total = float(np.sum(stress_net))
    stressed_progress_median = float(np.median(stress_progress))
    stressed_progress_p25 = float(np.percentile(stress_progress, 25))
    consistency = float(min(normal_consistency, stress_consistency))
    operational_complexity = len(ordered[0]["component_ids"])
    return {
        "variant_id": ordered[0]["variant_id"],
        "policy_id": ordered[0]["policy_id"],
        "source_policy_fingerprint": ordered[0]["source_policy_fingerprint"],
        "component_ids": list(ordered[0]["component_ids"]),
        "risk_label": ordered[0]["risk_label"],
        "micro_risk_units": int(ordered[0]["micro_risk_units"]),
        "design_block_ids": list(allowed),
        "heldout_block_id": heldout,
        "episode_count": len(stress_episodes),
        "normal_pass_count": normal_pass_count,
        "stress_pass_count": stress_pass_count,
        "normal_net_usd": normal_net_total,
        "stressed_net_usd": stress_net_total,
        "normal_median_net_usd": float(np.median(normal_net)),
        "stressed_median_net_usd": float(np.median(stress_net)),
        "stressed_target_progress_median": stressed_progress_median,
        "stressed_target_progress_p25": stressed_progress_p25,
        "normal_mll_breach_rate": normal_mll_rate,
        "stressed_mll_breach_rate": stressed_mll_rate,
        "mll_breach_rate": max(normal_mll_rate, stressed_mll_rate),
        "normal_consistency_pass_rate": float(normal_consistency),
        "stressed_consistency_pass_rate": float(stress_consistency),
        "consistency_pass_rate": consistency,
        "hard_issue_count": sum(int(row["hard_issue_count"]) for row in ordered),
        "maximum_component_profit_share": maximum_component_share,
        "maximum_block_profit_share": maximum_block_share,
        "positive_temporal_block_count": sum(
            float(row["stressed_net_usd"]) > 0.0 for row in ordered
        ),
        "operational_complexity": operational_complexity,
        "normal_component_contributions": normal_contributions,
        "stressed_component_contributions": contributions,
        "design_behavior_fingerprint": behavior,
        "behavior_fingerprint": behavior,
        "primary_horizon_days": PRIMARY_SELECTOR_HORIZON_DAYS,
        "cost_scenarios": [1.0, 1.5],
        "block_metrics": list(ordered),
        "evaluation_version": SELECTOR_EVALUATION_VERSION,
        # Manifest/report vocabulary aliases.  The selector's native names
        # above remain authoritative; these values are exact aliases, not a
        # second calculation or score.
        "normal_combine_pass_count": normal_pass_count,
        "stressed_combine_pass_count": stress_pass_count,
        "normal_net_pnl": normal_net_total,
        "stressed_net_pnl": stress_net_total,
        "stressed_median_target_progress": stressed_progress_median,
        "lower_quartile_target_progress": stressed_progress_p25,
        "stressed_target_progress": stressed_progress_median,
        "stressed_pass_count": stress_pass_count,
        "mll_breach_count": stress_mll,
        "consistency": consistency,
        "component_concentration": maximum_component_share,
        "temporal_block_concentration": maximum_block_share,
        "operational_simplicity": 1.0 / operational_complexity,
    }


def _evaluate_primary_horizon(
    events: Mapping[str, Sequence[RoutedTrade]],
    days: Sequence[int],
    starts: Sequence[int],
    *,
    frozen: FrozenStaticAccountPolicy,
    block_id: str,
) -> TimeToCombineHorizonSummary:
    with static_risk_router_context():
        episodes = tuple(
            run_censored_shared_account_episode(
                events,
                days,
                basket=frozen.basket,  # type: ignore[arg-type]
                controller=frozen.controller,  # type: ignore[arg-type]
                start_day=start,
                horizon_days=PRIMARY_SELECTOR_HORIZON_DAYS,
            )
            for start in starts
        )
    return summarize_time_to_combine(
        episodes,
        horizon_label=str(PRIMARY_SELECTOR_HORIZON_DAYS),
        requested_horizon_days=PRIMARY_SELECTOR_HORIZON_DAYS,
        block_id=block_id,
    )


def _block_metrics(
    frozen: FrozenStaticAccountPolicy,
    *,
    block_id: str,
    session_days: Sequence[int],
    starts: Sequence[int],
    runtime_hashes: Mapping[str, str],
    normal: TimeToCombineHorizonSummary,
    stressed: TimeToCombineHorizonSummary,
    time_to_combine: Mapping[str, Any] | None,
) -> dict[str, Any]:
    normal_rows = [_episode_row(row) for row in normal.episodes]
    stress_rows = [_episode_row(row) for row in stressed.episodes]
    normal_contributions = _episode_contributions(normal.episodes)
    stressed_contributions = _episode_contributions(stressed.episodes)
    maximum_component_share = _maximum_positive_share(stressed_contributions)
    behavior = stable_hash(
        {
            "block_id": block_id,
            "risk_label": frozen.risk_tier.label,
            "normal": [_behavior_episode(row) for row in normal_rows],
            "stress_1_5x": [_behavior_episode(row) for row in stress_rows],
        }
    )
    normal_mll_rate = normal.mll_breach_probability
    stressed_mll_rate = stressed.mll_breach_probability
    consistency = min(
        normal.consistency_ok_probability,
        stressed.consistency_ok_probability,
    )
    operational_complexity = len(frozen.basket.component_ids)
    payload: dict[str, Any] = {
        "block_id": block_id,
        "variant_id": frozen.variant_id,
        "policy_id": frozen.source_policy_id,
        "source_policy_fingerprint": frozen.source_policy_fingerprint,
        "component_ids": list(frozen.basket.component_ids),
        "risk_label": frozen.risk_tier.label,
        "risk_multiplier": frozen.risk_tier.multiplier,
        "micro_risk_units": frozen.risk_tier.micro_risk_units,
        "session_day_count": len(session_days),
        "first_session_day": int(session_days[0]),
        "last_session_day": int(session_days[-1]),
        "episode_start_days": list(starts),
        "episode_count": len(stress_rows),
        "normal_pass_count": normal.pass_count,
        "stress_pass_count": stressed.pass_count,
        "normal_net_usd": normal.net_after_costs_total,
        "stressed_net_usd": stressed.net_after_costs_total,
        "normal_median_net_usd": normal.net_after_costs_median,
        "stressed_median_net_usd": stressed.net_after_costs_median,
        "normal_target_progress_median": normal.target_progress_median,
        "normal_target_progress_p25": normal.target_progress_p25,
        "stressed_target_progress_median": stressed.target_progress_median,
        "stressed_target_progress_p25": stressed.target_progress_p25,
        "normal_mll_breach_rate": normal_mll_rate,
        "stressed_mll_breach_rate": stressed_mll_rate,
        "mll_breach_rate": max(normal_mll_rate, stressed_mll_rate),
        "normal_consistency_pass_rate": normal.consistency_ok_probability,
        "stressed_consistency_pass_rate": stressed.consistency_ok_probability,
        "consistency_pass_rate": consistency,
        "normal_hard_rule_failure_count": normal.hard_rule_failure_count,
        "stressed_hard_rule_failure_count": stressed.hard_rule_failure_count,
        "hard_issue_count": (
            normal.hard_rule_failure_count + stressed.hard_rule_failure_count
        ),
        "maximum_component_profit_share": maximum_component_share,
        "maximum_block_profit_share": 1.0,
        "operational_complexity": operational_complexity,
        "normal_component_contributions": normal_contributions,
        "stressed_component_contributions": stressed_contributions,
        "primary_episode_records": {
            "normal": normal_rows,
            "stress_1_5x": stress_rows,
        },
        "design_behavior_fingerprint": behavior,
        "behavior_fingerprint": behavior,
        "runtime_specification_hashes": dict(sorted(runtime_hashes.items())),
        "immutable_ledger_only": True,
        "underlying_signals_recomputed": False,
        "primary_horizon_days": PRIMARY_SELECTOR_HORIZON_DAYS,
        "cost_scenarios": [1.0, 1.5],
        "evaluation_version": SELECTOR_EVALUATION_VERSION,
        # Exact aliases consumed by the frozen manifest and held-out report.
        "normal_combine_pass_count": normal.pass_count,
        "stressed_combine_pass_count": stressed.pass_count,
        "normal_net_pnl": normal.net_after_costs_total,
        "stressed_net_pnl": stressed.net_after_costs_total,
        "stressed_median_target_progress": stressed.target_progress_median,
        "lower_quartile_target_progress": stressed.target_progress_p25,
        "stressed_target_progress": stressed.target_progress_median,
        "stressed_pass_count": stressed.pass_count,
        "mll_breach_count": stressed.mll_breach_count,
        "consistency": consistency,
        "component_concentration": maximum_component_share,
        "temporal_block_concentration": 1.0,
        "operational_simplicity": 1.0 / operational_complexity,
    }
    if time_to_combine is not None:
        payload["time_to_combine"] = dict(time_to_combine)
    return payload


def _episode_row(row: CensoredAccountPolicyEpisode) -> dict[str, Any]:
    episode = row.legacy_episode
    return {
        "start_day": episode.start_day,
        "end_day": episode.end_day,
        "observation_status": row.observation_status.value,
        "terminal": episode.terminal.value,
        "target_reached": row.target_reached,
        "net_pnl": episode.net_pnl,
        "target_progress": episode.target_progress,
        "maximum_target_progress": episode.maximum_target_progress,
        "mll_breached": episode.mll_breached,
        "consistency_ok": episode.consistency_ok,
        "days_to_target": episode.days_to_target,
        "eligible_days": episode.eligible_days,
        "accepted_events": episode.accepted_events,
        "skipped_events": episode.skipped_events,
    }


def _behavior_episode(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "start_day": int(row["start_day"]),
        "status": str(row["observation_status"]),
        "net": round(float(row["net_pnl"]), 8),
        "progress": round(float(row["target_progress"]), 10),
        "mll": bool(row["mll_breached"]),
        "consistency": bool(row["consistency_ok"]),
    }


def _episode_contributions(
    rows: Sequence[CensoredAccountPolicyEpisode],
) -> dict[str, float]:
    output: dict[str, float] = {}
    for row in rows:
        for component_id, value in row.legacy_episode.component_contribution.items():
            output[component_id] = output.get(component_id, 0.0) + float(value)
    return dict(sorted(output.items()))


def _sum_contributions(
    rows: Sequence[Mapping[str, Any]] | Any,
) -> dict[str, float]:
    output: dict[str, float] = {}
    for row in rows:
        for component_id, value in row.items():
            output[str(component_id)] = output.get(str(component_id), 0.0) + float(
                value
            )
    return dict(sorted(output.items()))


def _maximum_positive_share(contributions: Mapping[str, float]) -> float:
    positive = [max(0.0, float(value)) for value in contributions.values()]
    total = sum(positive)
    return max(positive, default=0.0) / total if total > 0.0 else 1.0


def _pooled_episode_rows(
    rows: Sequence[Mapping[str, Any]], scenario: str
) -> tuple[dict[str, Any], ...]:
    output: list[dict[str, Any]] = []
    for block in rows:
        records = block.get("primary_episode_records")
        if not isinstance(records, Mapping) or scenario not in records:
            raise SelectorEvaluationError("block metrics lack primary episode records")
        for record in records[scenario]:
            row = dict(record)
            row["block_id"] = str(block["block_id"])
            output.append(row)
    if not output:
        raise SelectorEvaluationError("design aggregation has no episode evidence")
    return tuple(output)


def _unique_ordered_ints(
    values: Sequence[int], *, name: str, sort: bool = True
) -> tuple[int, ...]:
    rows = tuple(int(row) for row in values)
    if not rows or len(set(rows)) != len(rows):
        raise SelectorEvaluationError(f"{name} must be non-empty and unique")
    if sort:
        rows = tuple(sorted(rows))
    return rows


def _required_fingerprint(value: Mapping[str, Any]) -> str:
    fingerprint = str(value.get("structural_fingerprint", ""))
    if len(fingerprint) != 64:
        raise SelectorEvaluationError(
            "persisted policy requires its 64-character structural fingerprint"
        )
    return fingerprint


def _verify_policy_fingerprint(
    policy: PersistedSourcePolicy, expected: str
) -> None:
    if policy.structural_fingerprint != expected:
        raise SelectorEvaluationError("persisted policy structural fingerprint drift")


def _exact_change(value: Any) -> tuple[tuple[str, Any], ...]:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), row) for key, row in value.items()))
    try:
        return tuple((str(key), row) for key, row in value)
    except (TypeError, ValueError) as exc:
        raise SelectorEvaluationError("persisted exact_change is malformed") from exc


# Short aliases for runner call sites.
evaluate_selector_variant_block = evaluate_policy_block
aggregate_selector_design_metrics = aggregate_design_block_metrics


__all__ = [
    "FrozenStaticAccountPolicy",
    "PRIMARY_SELECTOR_HORIZON_DAYS",
    "SELECTOR_EVALUATION_VERSION",
    "SelectorEvaluationError",
    "aggregate_design_block_metrics",
    "aggregate_selector_design_metrics",
    "build_frozen_static_account_policy",
    "elite_robustness_policy_from_dict",
    "evaluate_policy_block",
    "evaluate_selector_variant_block",
    "persisted_policy_from_dict",
    "static_parent_policy_from_dict",
]
