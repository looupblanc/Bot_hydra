"""Bounded safety frontier for the three immutable V7.1 event-time sleeves.

The original event-time diagnostic found genuine development Combine passes at
one mini contract, but also 32--40% MLL breach rates.  This module asks one
strictly narrower, failure-guided question: can a preregistered micro-contract
and causal account-state governor retain passes while reducing MLL breach below
10%?

Signals, decisions, exits, gross edge and event ordering are immutable.  A
governor may only reject a still-unopened event or reduce its whole-micro
quantity from information known before that event: realized account PnL,
current MLL buffer, current-day PnL and target distance.  Profiles are selected
on B1/B2 only.  The selected profile is then evaluated once on B3/B4.  Results
are viewed development diagnostics; this module performs no writes, promotion,
XFA, registry/database access, broker connection or order action.
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.production.autonomous_exact_replay import (
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _load_rule_snapshot,
)
from hydra.production.v71_event_time_account_exploration import (
    G4_CANDIDATE_ID,
    G6_CANDIDATE_IDS,
    HORIZONS,
    _load_frozen_event_population,
    _require_scenario_identity,
)
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    CombineTerminal,
    TradePathEvent,
    run_combine_episode,
)
from hydra.propfirm.mll_variants import (
    advance_end_of_day_floor,
    advance_intraday_floor,
)
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year


SCHEMA = "hydra_autonomous_event_time_safety_frontier_v1"
COMPOSITE_SCHEMA = "hydra_autonomous_event_time_safety_frontier_shards_v1"
ACCOUNT_LABEL = "50K"
DESIGN_BLOCKS = ("B1", "B2")
HELD_OUT_DEVELOPMENT_BLOCKS = ("B3", "B4")
SCENARIOS = (CostStress.BASE, CostStress.STRESS_1_5X)
MAXIMUM_PROFILES = 8


class AutonomousEventTimeSafetyFrontierError(RuntimeError):
    """The bounded safety-frontier contract failed closed."""


@dataclass(frozen=True, slots=True)
class EventTimeSafetyProfile:
    """One preregistered causal whole-micro account governor."""

    profile_id: str
    nominal_micro_contracts: int
    low_buffer_micro_contracts: int
    low_buffer_trigger_fraction: float
    daily_loss_guard_fraction: float
    target_protection_progress: float
    target_micro_contracts: int
    source_mini_identity: bool = False

    def __post_init__(self) -> None:
        if not (
            1 <= self.low_buffer_micro_contracts <= self.nominal_micro_contracts <= 10
            and 1 <= self.target_micro_contracts <= self.nominal_micro_contracts
            and 0.0 < self.low_buffer_trigger_fraction <= 1.0
            and 0.0 < self.daily_loss_guard_fraction <= 1.0
            and 0.0 < self.target_protection_progress <= 1.0
        ):
            raise ValueError("invalid event-time safety profile")


def frozen_event_time_safety_profiles() -> tuple[EventTimeSafetyProfile, ...]:
    """Return the complete discrete frontier, frozen before held-out replay."""

    profiles = (
        EventTimeSafetyProfile("event_safe_static_04", 4, 4, 0.25, 1.00, 1.00, 4),
        EventTimeSafetyProfile("event_safe_static_06", 6, 6, 0.25, 1.00, 1.00, 6),
        EventTimeSafetyProfile("event_safe_static_08", 8, 8, 0.25, 1.00, 1.00, 8),
        EventTimeSafetyProfile("event_safe_guard_06_02", 6, 2, 0.75, 0.25, 0.70, 2),
        EventTimeSafetyProfile("event_safe_guard_08_02", 8, 2, 0.75, 0.25, 0.70, 3),
        EventTimeSafetyProfile("event_safe_guard_08_03", 8, 3, 0.85, 0.33, 0.80, 4),
        EventTimeSafetyProfile("event_safe_guard_10_01", 10, 1, 0.90, 0.20, 0.65, 2),
        EventTimeSafetyProfile("event_safe_guard_10_03", 10, 3, 0.75, 0.33, 0.75, 4),
    )
    if len(profiles) != MAXIMUM_PROFILES or len(
        {row.profile_id for row in profiles}
    ) != len(profiles):
        raise AutonomousEventTimeSafetyFrontierError(
            "frozen event-time safety frontier is invalid"
        )
    return profiles


def build_autonomous_event_time_safety_frontier(
    root: str | Path,
    *,
    shard_index: int = 0,
    shard_count: int = 1,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Evaluate one deterministic candidate shard without authoritative writes."""

    if int(shard_count) not in {1, 2} or not 0 <= int(shard_index) < int(
        shard_count
    ):
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shards require shard_count 1/2 and valid index"
        )
    project = Path(root).resolve()
    rules, rule_receipt = _load_rule_snapshot(_inside(project, rule_snapshot_path))
    rule = dict(rules[ACCOUNT_LABEL])
    config = _account_config(rule)
    cost_model = load_cost_model()
    micro_costs = {
        scenario.value: cost_model.round_turn_cost(
            "MES", "60m", stress=scenario
        )
        for scenario in SCENARIOS
    }

    population = _load_frozen_event_population(project)
    minute = population.pop("_minute")
    source_audit = population.pop("_source_audit")
    metadata = population.pop("_candidate_metadata")
    integration = population.pop("_integration")
    candidate_ids = tuple(sorted(population))
    expected_ids = tuple(sorted((G4_CANDIDATE_ID, *G6_CANDIDATE_IDS)))
    if candidate_ids != expected_ids:
        raise AutonomousEventTimeSafetyFrontierError(
            "immutable event-time candidate inventory drift"
        )
    selected_ids = tuple(
        candidate_id
        for position, candidate_id in enumerate(candidate_ids)
        if position % int(shard_count) == int(shard_index)
    )
    inventory_hash = stable_hash(list(candidate_ids))

    calendar = tuple(
        sorted(
            int(day)
            for days in _eligible_days_by_year(minute).values()
            for day in days
        )
    )
    block_contract = _chronological_block_contract(calendar)
    profiles = frozen_event_time_safety_profiles()
    profile_hash = stable_hash([asdict(row) for row in profiles])

    candidate_results: list[dict[str, Any]] = []
    exact_episode_count = 0
    for candidate_id in selected_ids:
        scenario_events = population[candidate_id]
        _require_scenario_identity(
            scenario_events[CostStress.BASE.value],
            scenario_events[CostStress.STRESS_1_5X.value],
        )
        identity = _evaluate_profile_roles(
            candidate_id=candidate_id,
            profile=_identity_profile(),
            scenario_events=scenario_events,
            block_contract=block_contract,
            config=config,
            maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
            micro_costs=micro_costs,
            roles=("DESIGN", "HELD_OUT_DEVELOPMENT"),
        )
        exact_episode_count += int(identity["exact_episode_count"])

        design_frontier: list[dict[str, Any]] = []
        for profile in profiles:
            row = _evaluate_profile_roles(
                candidate_id=candidate_id,
                profile=profile,
                scenario_events=scenario_events,
                block_contract=block_contract,
                config=config,
                maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
                micro_costs=micro_costs,
                roles=("DESIGN",),
            )
            exact_episode_count += int(row["exact_episode_count"])
            design_frontier.append(row)
        selected_design = max(design_frontier, key=_design_rank)
        selected_profile = next(
            row
            for row in profiles
            if row.profile_id == selected_design["profile"]["profile_id"]
        )
        heldout = _evaluate_profile_roles(
            candidate_id=candidate_id,
            profile=selected_profile,
            scenario_events=scenario_events,
            block_contract=block_contract,
            config=config,
            maximum_mini_equivalent=float(rule["maximum_mini_contracts"]),
            micro_costs=micro_costs,
            roles=("HELD_OUT_DEVELOPMENT",),
        )
        exact_episode_count += int(heldout["exact_episode_count"])
        selected_result = _merge_selected_roles(selected_design, heldout)
        gates = _heldout_safety_gates(selected_result)
        selected_result["heldout_safety_gate_results"] = gates
        selected_result["heldout_safety_precontrol_ready"] = all(gates.values())
        selected_result["authoritative_promotion_status"] = None
        selected_result["result_hash"] = stable_hash(
            {
                key: value
                for key, value in selected_result.items()
                if key != "result_hash"
            }
        )
        candidate_core = {
            "candidate_id": candidate_id,
            "source_candidate_metadata": dict(metadata[candidate_id]),
            "source_event_count": len(scenario_events[CostStress.BASE.value]),
            "source_signal_and_exit_logic_changed": False,
            "identity_control": identity,
            "design_frontier": design_frontier,
            "selected_profile_id": selected_profile.profile_id,
            "selection_receipt": {
                "selection_blocks": list(DESIGN_BLOCKS),
                "heldout_fields_used": False,
                "selected_profile_id": selected_profile.profile_id,
                "design_rank": list(_design_rank(selected_design)),
                "frontier_policy_ids": sorted(
                    str(row["policy_id"]) for row in design_frontier
                ),
            },
            "selected_result": selected_result,
            "evidence_tier": "E_DEVELOPMENT_DIAGNOSTIC",
            "promotion_status": None,
        }
        candidate_results.append(
            {**candidate_core, "candidate_result_hash": stable_hash(candidate_core)}
        )

    ready = sorted(
        str(row["selected_result"]["policy_id"])
        for row in candidate_results
        if row["selected_result"]["heldout_safety_precontrol_ready"] is True
    )
    core: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_EVENT_TIME_SAFETY_SHARD",
        "branch_id": "EVENT_TIME_MICRO_MLL_SAFETY_FRONTIER",
        "source_population": {
            "candidate_ids": list(candidate_ids),
            "candidate_inventory_hash": inventory_hash,
            "source_audit": source_audit,
            "integration": integration,
        },
        "official_rule_snapshot": rule_receipt,
        "account_rule": {
            "account_label": ACCOUNT_LABEL,
            "profit_target_usd": float(rule["profit_target_usd"]),
            "maximum_loss_limit_usd": float(rule["maximum_loss_limit_usd"]),
            "maximum_mini_contracts": int(rule["maximum_mini_contracts"]),
        },
        "frontier_contract": {
            "profiles": [asdict(row) for row in profiles],
            "profile_count": len(profiles),
            "profile_hash": profile_hash,
            "quantity_unit": "WHOLE_MICRO_CONTRACTS_0_1_MINI_EQUIVALENT",
            "source_instrument": "ES",
            "safety_execution_instrument": "MES",
            "micro_round_turn_cost_usd": dict(sorted(micro_costs.items())),
            "cost_model_source": cost_model.source,
            "cost_model_source_checked_utc": cost_model.source_checked_utc,
            "allowed_actions": ["ACCEPT_REDUCED_QUANTITY", "REJECT_ENTRY"],
            "signal_logic_changed": False,
            "entry_time_changed": False,
            "exit_time_changed": False,
            "future_outcome_fields_used": False,
        },
        "evaluation_contract": block_contract["receipt"],
        "shard": {
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "partition_rule": "SORTED_CANDIDATE_POSITION_MODULO_SHARD_COUNT_V1",
            "candidate_inventory_hash": inventory_hash,
            "candidate_inventory_ids": list(candidate_ids),
            "selected_candidate_ids": list(selected_ids),
        },
        "candidate_results": candidate_results,
        "counts": {
            "source_candidate_count": len(candidate_ids),
            "selected_candidate_count": len(selected_ids),
            "profile_count": len(profiles),
            "exact_episode_count": exact_episode_count,
            "heldout_safety_precontrol_ready_count": len(ready),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {"heldout_safety_precontrol_ready": ready},
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "next_action": (
            "RUN_MATCHED_CONTROLS_AND_CONCENTRATION_AUDIT"
            if ready
            else "TERMINALIZE_EVENT_TIME_SAFETY_REPAIR"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def compose_autonomous_event_time_safety_frontier_shards(
    shards: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reconcile one/two deterministic read-only shards without replay."""

    values = [_verify_shard(row) for row in shards]
    if not values:
        raise AutonomousEventTimeSafetyFrontierError(
            "at least one event-time safety shard is required"
        )
    declared_count = int(dict(values[0]["shard"])["shard_count"])
    if declared_count not in {1, 2} or len(values) != declared_count:
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shard set is incomplete"
        )
    shared = (
        "source_population",
        "official_rule_snapshot",
        "account_rule",
        "frontier_contract",
        "evaluation_contract",
    )
    for field in shared:
        expected = stable_hash(values[0][field])
        if any(stable_hash(row[field]) != expected for row in values[1:]):
            raise AutonomousEventTimeSafetyFrontierError(
                f"event-time shard shared field differs: {field}"
            )
    inventory_ids = tuple(
        str(row)
        for row in dict(values[0]["shard"])["candidate_inventory_ids"]
    )
    inventory_hash = str(
        dict(values[0]["shard"])["candidate_inventory_hash"]
    )
    indexes: set[int] = set()
    selected_sets: list[set[str]] = []
    by_candidate: dict[str, dict[str, Any]] = {}
    for value in values:
        shard = dict(value["shard"])
        index = int(shard["shard_index"])
        selected = {str(row) for row in shard["selected_candidate_ids"]}
        if (
            index in indexes
            or int(shard["shard_count"]) != declared_count
            or str(shard["candidate_inventory_hash"]) != inventory_hash
            or tuple(str(row) for row in shard["candidate_inventory_ids"])
            != inventory_ids
        ):
            raise AutonomousEventTimeSafetyFrontierError(
                "event-time safety shard identity drift"
            )
        indexes.add(index)
        selected_sets.append(selected)
        observed = {str(row["candidate_id"]) for row in value["candidate_results"]}
        if observed != selected:
            raise AutonomousEventTimeSafetyFrontierError(
                "event-time shard candidate result inventory drift"
            )
        for row in value["candidate_results"]:
            candidate_id = str(row["candidate_id"])
            if candidate_id in by_candidate:
                raise AutonomousEventTimeSafetyFrontierError(
                    "candidate appears in multiple event-time shards"
                )
            by_candidate[candidate_id] = dict(row)
    if indexes != set(range(declared_count)):
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shard indexes are incomplete"
        )
    for position, left in enumerate(selected_sets):
        if any(left & right for right in selected_sets[position + 1 :]):
            raise AutonomousEventTimeSafetyFrontierError(
                "event-time safety shards overlap"
            )
    if set().union(*selected_sets) != set(inventory_ids):
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shard union is incomplete"
        )
    ordered = [by_candidate[candidate_id] for candidate_id in inventory_ids]
    ready = sorted(
        str(row["selected_result"]["policy_id"])
        for row in ordered
        if row["selected_result"]["heldout_safety_precontrol_ready"] is True
    )
    core: dict[str, Any] = {
        "schema": COMPOSITE_SCHEMA,
        "status": "COMPLETE_RECONCILED_EVENT_TIME_SAFETY_SHARDS",
        **{field: values[0][field] for field in shared},
        "shard_receipts": [
            {
                "shard_index": int(dict(row["shard"])["shard_index"]),
                "result_hash": str(row["result_hash"]),
                "selected_candidate_ids": list(
                    dict(row["shard"])["selected_candidate_ids"]
                ),
            }
            for row in sorted(
                values, key=lambda value: int(dict(value["shard"])["shard_index"])
            )
        ],
        "candidate_results": ordered,
        "counts": {
            "source_candidate_count": len(inventory_ids),
            "selected_candidate_count": len(ordered),
            "profile_count": int(values[0]["frontier_contract"]["profile_count"]),
            "exact_episode_count": sum(
                int(dict(row["counts"])["exact_episode_count"]) for row in values
            ),
            "heldout_safety_precontrol_ready_count": len(ready),
            "authoritative_promotion_count": 0,
            "xfa_paths_started": 0,
            "registry_writes": 0,
            "database_writes": 0,
            "q4_access_count_delta": 0,
            "data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "candidate_ids": {"heldout_safety_precontrol_ready": ready},
        "evidence_role": "VIEWED_DEVELOPMENT_ONLY",
        "promotion_status": None,
        "next_action": (
            "RUN_MATCHED_CONTROLS_AND_CONCENTRATION_AUDIT"
            if ready
            else "TERMINALIZE_EVENT_TIME_SAFETY_REPAIR"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def event_time_safety_frontier_worker(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pickle-safe worker adapter."""

    return build_autonomous_event_time_safety_frontier(
        str(payload["root"]),
        shard_index=int(payload.get("shard_index", 0)),
        shard_count=int(payload.get("shard_count", 1)),
        rule_snapshot_path=str(payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)),
    )


def _identity_profile() -> EventTimeSafetyProfile:
    return EventTimeSafetyProfile(
        "event_time_one_mini_identity_control",
        10,
        10,
        0.01,
        1.0,
        1.0,
        10,
        source_mini_identity=True,
    )


def _chronological_block_contract(calendar: Sequence[int]) -> dict[str, Any]:
    days = tuple(sorted({int(day) for day in calendar}))
    if len(days) < 40:
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety frontier requires at least 40 sessions"
        )
    role_cut = (len(days) + 1) // 2
    design = days[:role_cut]
    heldout = days[role_cut:]

    def split(role_days: Sequence[int], names: tuple[str, str]) -> dict[int, str]:
        cut = (len(role_days) + 1) // 2
        return {
            int(day): names[0] if index < cut else names[1]
            for index, day in enumerate(role_days)
        }

    by_day = {
        **split(design, DESIGN_BLOCKS),
        **split(heldout, HELD_OUT_DEVELOPMENT_BLOCKS),
    }
    role_days = {"DESIGN": design, "HELD_OUT_DEVELOPMENT": heldout}
    starts: dict[str, dict[int, tuple[tuple[int, str], ...]]] = {}
    for role, values in role_days.items():
        starts[role] = {}
        for horizon in HORIZONS:
            usable = max(0, len(values) - int(horizon) + 1)
            starts[role][horizon] = tuple(
                (int(values[index]), by_day[int(values[index])])
                for index in range(usable)
            )
    receipt = {
        "design_blocks": list(DESIGN_BLOCKS),
        "held_out_development_blocks": list(HELD_OUT_DEVELOPMENT_BLOCKS),
        "selection_uses": "B1_B2_ONLY",
        "precontrol_evaluation_uses": "B3_B4_ONLY",
        "calendar_session_count": len(days),
        "design_session_count": len(design),
        "heldout_session_count": len(heldout),
        "calendar_hash": stable_hash(list(days)),
        "block_by_day_hash": stable_hash(by_day),
        "start_counts": {
            role: {str(horizon): len(values) for horizon, values in role_starts.items()}
            for role, role_starts in starts.items()
        },
        "overlap_warning": (
            "Rolling starts are dependent development diagnostics; no confirmation "
            "or authoritative promotion is claimed."
        ),
    }
    return {
        "days": role_days,
        "starts": starts,
        "block_by_day": by_day,
        "receipt": receipt,
    }


def _evaluate_profile_roles(
    *,
    candidate_id: str,
    profile: EventTimeSafetyProfile,
    scenario_events: Mapping[str, Sequence[TradePathEvent]],
    block_contract: Mapping[str, Any],
    config: Any,
    maximum_mini_equivalent: float,
    micro_costs: Mapping[str, float],
    roles: Sequence[str],
) -> dict[str, Any]:
    role_results: dict[str, Any] = {}
    exact_count = 0
    for role in roles:
        role_days = tuple(int(day) for day in block_contract["days"][role])
        horizon_results: dict[str, Any] = {}
        for horizon in HORIZONS:
            scenarios: dict[str, Any] = {}
            for scenario in SCENARIOS:
                episodes: list[tuple[CombineEpisodeResult, str, Mapping[str, Any]]] = []
                for start_day, block in block_contract["starts"][role][horizon]:
                    transformed, decisions = _govern_episode_events(
                        scenario_events[scenario.value],
                        role_days,
                        start_day=int(start_day),
                        maximum_duration_days=int(horizon),
                        profile=profile,
                        config=config,
                        micro_round_turn_cost_usd=float(
                            micro_costs[scenario.value]
                        ),
                    )
                    episode = run_combine_episode(
                        transformed,
                        role_days,
                        start_day=int(start_day),
                        maximum_duration_days=int(horizon),
                        config=config,
                        maximum_mini_equivalent=maximum_mini_equivalent,
                    )
                    episodes.append((episode, str(block), decisions))
                exact_count += len(episodes)
                scenarios[scenario.value] = _summarize_governed_episodes(episodes)
            horizon_results[str(horizon)] = scenarios
        role_results[role] = horizon_results
    policy_core = {
        "candidate_id": candidate_id,
        "profile": asdict(profile),
        "profile_hash": stable_hash(asdict(profile)),
        "roles": role_results,
        "signal_logic_changed": False,
        "entry_and_exit_timestamps_changed": False,
        "only_quantity_or_entry_rejection_changed": True,
    }
    return {
        **policy_core,
        "policy_id": "event_time_safety_" + stable_hash(policy_core)[:20],
        "exact_episode_count": exact_count,
        "result_hash": stable_hash(policy_core),
    }


def _govern_episode_events(
    events: Sequence[TradePathEvent],
    eligible_days: Sequence[int],
    *,
    start_day: int,
    maximum_duration_days: int,
    profile: EventTimeSafetyProfile,
    config: Any,
    micro_round_turn_cost_usd: float,
) -> tuple[tuple[TradePathEvent, ...], dict[str, Any]]:
    days = tuple(sorted({int(day) for day in eligible_days}))
    start_index = days.index(int(start_day))
    episode_days = days[start_index : start_index + int(maximum_duration_days)]
    end_day = int(episode_days[-1])
    ordered = sorted(
        (row for row in events if start_day <= row.session_day <= end_day),
        key=lambda row: (row.session_day, row.decision_ns, row.event_id),
    )
    balance = float(config.combine_starting_balance)
    floor = float(config.combine_starting_mll)
    current_day: int | None = None
    day_pnl = 0.0
    accepted: list[TradePathEvent] = []
    rejected = 0
    reductions = 0
    micro_counts: Counter[int] = Counter()
    terminal = False
    traded_days: set[int] = set()
    best_day = 0.0
    for event in ordered:
        if terminal:
            break
        if current_day is None or int(event.session_day) != current_day:
            if current_day is not None:
                best_day = max(best_day, day_pnl)
                floor = advance_end_of_day_floor(
                    floor,
                    closing_balance=balance,
                    distance=float(config.combine_max_loss_limit),
                    lock=float(config.combine_starting_balance),
                )
                total_profit = balance - float(config.combine_starting_balance)
                required_target = max(
                    float(config.combine_profit_target),
                    (
                        best_day
                        / float(config.consistency_best_day_max_pct_of_profit_target)
                        if best_day > 0
                        else 0.0
                    ),
                )
                consistency_ok = (
                    total_profit <= 0
                    or best_day / total_profit
                    <= float(config.consistency_best_day_max_pct_of_profit_target)
                    + 1e-12
                )
                if (
                    total_profit >= required_target
                    and consistency_ok
                    and len(traded_days) >= int(config.minimum_pass_days)
                ):
                    terminal = True
                    break
            current_day = int(event.session_day)
            day_pnl = 0.0

        maximum_loss = float(config.combine_max_loss_limit)
        if day_pnl <= -profile.daily_loss_guard_fraction * maximum_loss:
            rejected += 1
            continue
        mll_buffer = max(0.0, balance - floor)
        micro_count = int(profile.nominal_micro_contracts)
        if mll_buffer <= profile.low_buffer_trigger_fraction * maximum_loss:
            micro_count = min(micro_count, profile.low_buffer_micro_contracts)
        target_progress = max(
            0.0,
            (balance - float(config.combine_starting_balance))
            / max(float(config.combine_profit_target), 1e-12),
        )
        if target_progress >= profile.target_protection_progress:
            micro_count = min(micro_count, profile.target_micro_contracts)
        if micro_count <= 0:
            rejected += 1
            continue
        scaled = _scale_to_micro(
            event,
            micro_count,
            profile.profile_id,
            micro_round_turn_cost_usd=micro_round_turn_cost_usd,
            source_mini_identity=profile.source_mini_identity,
        )
        reductions += int(micro_count < 10)
        micro_counts[micro_count] += 1
        accepted.append(scaled)
        traded_days.add(int(event.session_day))

        floor = advance_intraday_floor(
            floor,
            live_equity_high=balance + max(float(scaled.best_unrealized_pnl), 0.0),
            distance=maximum_loss,
            lock=float(config.combine_starting_balance),
            variant=config.resolved_mll_mode,
        )
        if balance + min(float(scaled.worst_unrealized_pnl), 0.0) <= floor:
            terminal = True
            break
        balance += float(scaled.net_pnl)
        day_pnl += float(scaled.net_pnl)
        floor = advance_intraday_floor(
            floor,
            live_equity_high=balance,
            distance=maximum_loss,
            lock=float(config.combine_starting_balance),
            variant=config.resolved_mll_mode,
        )
        if balance <= floor:
            terminal = True
            break
    decision_core = {
        "source_event_count": len(ordered),
        "accepted_event_count": len(accepted),
        "rejected_event_count": rejected,
        "reduced_quantity_event_count": reductions,
        "micro_contract_distribution": {
            str(key): value for key, value in sorted(micro_counts.items())
        },
    }
    return tuple(accepted), {
        **decision_core,
        "decision_path_hash": stable_hash(decision_core),
    }


def _scale_to_micro(
    event: TradePathEvent,
    micro_contracts: int,
    profile_id: str,
    *,
    micro_round_turn_cost_usd: float,
    source_mini_identity: bool = False,
) -> TradePathEvent:
    if source_mini_identity:
        return event
    scale = float(micro_contracts) / 10.0
    source_cost = float(event.gross_pnl - event.net_pnl)
    gross = float(event.gross_pnl * scale)
    net = float(gross - micro_round_turn_cost_usd * micro_contracts)
    raw_worst = float(event.worst_unrealized_pnl + source_cost)
    raw_best = float(event.best_unrealized_pnl + source_cost)
    return replace(
        event,
        event_id=f"{event.event_id}:{profile_id}:M{micro_contracts}",
        net_pnl=net,
        gross_pnl=gross,
        worst_unrealized_pnl=float(
            raw_worst * scale - micro_round_turn_cost_usd * micro_contracts
        ),
        best_unrealized_pnl=float(
            raw_best * scale - micro_round_turn_cost_usd * micro_contracts
        ),
        quantity=int(micro_contracts),
        mini_equivalent=scale,
    )


def _summarize_governed_episodes(
    values: Sequence[tuple[CombineEpisodeResult, str, Mapping[str, Any]]],
) -> dict[str, Any]:
    episodes = [row for row, _block, _decisions in values]
    if not episodes:
        return _empty_summary()
    target_progress = [float(row.target_progress) for row in episodes]
    passing_days = [
        int(row.days_to_target) for row in episodes if row.days_to_target is not None
    ]
    terminals = Counter(row.terminal.value for row in episodes)
    by_block: dict[str, Any] = {}
    for block in sorted({block for _row, block, _decisions in values}):
        selected = [row for row, observed, _decisions in values if observed == block]
        by_block[block] = {
            "episode_count": len(selected),
            "pass_count": sum(row.passed for row in selected),
            "mll_breach_count": sum(row.mll_breached for row in selected),
            "net_total_usd": float(sum(row.net_pnl for row in selected)),
            "target_progress_median": float(
                statistics.median(row.target_progress for row in selected)
            ),
        }
    accepted = sum(
        int(decisions["accepted_event_count"]) for _row, _block, decisions in values
    )
    rejected = sum(
        int(decisions["rejected_event_count"]) for _row, _block, decisions in values
    )
    distribution: Counter[int] = Counter()
    for _row, _block, decisions in values:
        distribution.update(
            {
                int(key): int(count)
                for key, count in decisions["micro_contract_distribution"].items()
            }
        )
    summary_core = {
        "episode_count": len(episodes),
        "pass_count": sum(row.passed for row in episodes),
        "pass_rate": sum(row.passed for row in episodes) / len(episodes),
        "mll_breach_count": sum(row.mll_breached for row in episodes),
        "mll_breach_rate": sum(row.mll_breached for row in episodes) / len(episodes),
        "net_total_usd": float(sum(row.net_pnl for row in episodes)),
        "net_median_usd": float(statistics.median(row.net_pnl for row in episodes)),
        "target_progress_p25": float(np.percentile(target_progress, 25)),
        "target_progress_median": float(statistics.median(target_progress)),
        "minimum_mll_buffer_usd": float(
            min(row.minimum_mll_buffer for row in episodes)
        ),
        "consistency_compliant_pass_count": sum(
            row.passed and row.consistency_ok for row in episodes
        ),
        "all_passing_paths_consistency_compliant": all(
            row.consistency_ok for row in episodes if row.passed
        ),
        "median_days_to_target": (
            float(statistics.median(passing_days)) if passing_days else None
        ),
        "terminal_distribution": dict(sorted(terminals.items())),
        "accepted_event_count": accepted,
        "rejected_event_count": rejected,
        "acceptance_rate": accepted / max(accepted + rejected, 1),
        "micro_contract_distribution": {
            str(key): count for key, count in sorted(distribution.items())
        },
        "by_block": by_block,
        "episode_path_hash": stable_hash([row.to_dict() for row in episodes]),
        "decision_path_hash": stable_hash(
            [decisions for _row, _block, decisions in values]
        ),
    }
    return summary_core


def _empty_summary() -> dict[str, Any]:
    return {
        "episode_count": 0,
        "pass_count": 0,
        "pass_rate": 0.0,
        "mll_breach_count": 0,
        "mll_breach_rate": 0.0,
        "net_total_usd": 0.0,
        "net_median_usd": 0.0,
        "target_progress_p25": 0.0,
        "target_progress_median": 0.0,
        "minimum_mll_buffer_usd": None,
        "consistency_compliant_pass_count": 0,
        "all_passing_paths_consistency_compliant": True,
        "median_days_to_target": None,
        "terminal_distribution": {},
        "accepted_event_count": 0,
        "rejected_event_count": 0,
        "acceptance_rate": 0.0,
        "micro_contract_distribution": {},
        "by_block": {},
        "episode_path_hash": stable_hash([]),
        "decision_path_hash": stable_hash([]),
    }


def _design_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    role = row["roles"]["DESIGN"]
    normal5 = role["5"][CostStress.BASE.value]
    stressed5 = role["5"][CostStress.STRESS_1_5X.value]
    normal10 = role["10"][CostStress.BASE.value]
    stressed10 = role["10"][CostStress.STRESS_1_5X.value]
    stressed_mll = max(
        float(stressed5["mll_breach_rate"]),
        float(stressed10["mll_breach_rate"]),
    )
    gate_bits = (
        int(normal5["pass_count"]) > 0,
        int(stressed5["pass_count"]) > 0,
        int(normal10["pass_count"]) > 0,
        int(stressed10["pass_count"]) > 0,
        stressed_mll <= 0.10,
        float(stressed10["net_total_usd"]) > 0.0,
    )
    return (
        int(stressed_mll <= 0.10),
        sum(gate_bits),
        int(stressed10["pass_count"]),
        int(normal10["pass_count"]),
        int(stressed5["pass_count"]),
        float(stressed10["target_progress_p25"]),
        float(stressed10["net_total_usd"]),
        -stressed_mll,
        str(row["profile"]["profile_id"]),
    )


def _merge_selected_roles(
    design: Mapping[str, Any], heldout: Mapping[str, Any]
) -> dict[str, Any]:
    if (
        design["candidate_id"] != heldout["candidate_id"]
        or design["profile_hash"] != heldout["profile_hash"]
    ):
        raise AutonomousEventTimeSafetyFrontierError(
            "selected design/heldout policy identity drift"
        )
    core = {
        "candidate_id": str(design["candidate_id"]),
        "profile": dict(design["profile"]),
        "profile_hash": str(design["profile_hash"]),
        "roles": {
            "DESIGN": dict(design["roles"]["DESIGN"]),
            "HELD_OUT_DEVELOPMENT": dict(
                heldout["roles"]["HELD_OUT_DEVELOPMENT"]
            ),
        },
        "signal_logic_changed": False,
        "entry_and_exit_timestamps_changed": False,
        "only_quantity_or_entry_rejection_changed": True,
        "selection_outcome_role": "B1_B2_ONLY",
        "heldout_outcome_role": "B3_B4_VIEWED_DEVELOPMENT",
    }
    return {
        **core,
        "policy_id": "event_time_safety_selected_" + stable_hash(core)[:20],
        "exact_episode_count": int(design["exact_episode_count"])
        + int(heldout["exact_episode_count"]),
    }


def _heldout_safety_gates(row: Mapping[str, Any]) -> dict[str, bool]:
    design = row["roles"]["DESIGN"]
    heldout = row["roles"]["HELD_OUT_DEVELOPMENT"]
    design_normal5 = design["5"][CostStress.BASE.value]
    design_stressed5 = design["5"][CostStress.STRESS_1_5X.value]
    design_normal10 = design["10"][CostStress.BASE.value]
    design_stressed10 = design["10"][CostStress.STRESS_1_5X.value]
    design_stressed20 = design["20"][CostStress.STRESS_1_5X.value]
    normal5 = heldout["5"][CostStress.BASE.value]
    stressed5 = heldout["5"][CostStress.STRESS_1_5X.value]
    normal10 = heldout["10"][CostStress.BASE.value]
    stressed10 = heldout["10"][CostStress.STRESS_1_5X.value]
    stressed20 = heldout["20"][CostStress.STRESS_1_5X.value]
    positive_blocks = {
        block
        for horizon in (normal5, stressed5, normal10, stressed10)
        for block, summary in horizon["by_block"].items()
        if int(summary["pass_count"]) > 0
    }
    return {
        "design_normal_pass_preserved": int(design_normal5["pass_count"])
        + int(design_normal10["pass_count"])
        > 0,
        "design_stressed_pass_preserved": int(design_stressed5["pass_count"])
        + int(design_stressed10["pass_count"])
        > 0,
        "design_stressed_mll_breach_rate_le_10pct": max(
            float(design_stressed5["mll_breach_rate"]),
            float(design_stressed10["mll_breach_rate"]),
            float(design_stressed20["mll_breach_rate"]),
        )
        <= 0.10,
        "design_positive_stressed_net": float(
            design_stressed5["net_total_usd"]
        )
        + float(design_stressed10["net_total_usd"])
        > 0.0,
        "normal_pass_preserved": int(normal5["pass_count"])
        + int(normal10["pass_count"])
        > 0,
        "stressed_pass_preserved": int(stressed5["pass_count"])
        + int(stressed10["pass_count"])
        > 0,
        "stressed_mll_breach_rate_le_10pct": max(
            float(stressed5["mll_breach_rate"]),
            float(stressed10["mll_breach_rate"]),
            float(stressed20["mll_breach_rate"]),
        )
        <= 0.10,
        "positive_stressed_net": float(stressed5["net_total_usd"])
        + float(stressed10["net_total_usd"])
        > 0.0,
        "passing_paths_consistency_compliant": bool(
            normal5["all_passing_paths_consistency_compliant"]
            and stressed5["all_passing_paths_consistency_compliant"]
            and normal10["all_passing_paths_consistency_compliant"]
            and stressed10["all_passing_paths_consistency_compliant"]
        ),
        "heldout_temporal_context_present": bool(positive_blocks),
    }


def _verify_shard(value: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(value)
    if row.get("schema") != SCHEMA or row.get("status") != (
        "COMPLETE_BOUNDED_EVENT_TIME_SAFETY_SHARD"
    ):
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shard schema/status mismatch"
        )
    expected = stable_hash(
        {key: item for key, item in row.items() if key != "result_hash"}
    )
    if str(row.get("result_hash")) != expected:
        raise AutonomousEventTimeSafetyFrontierError(
            "event-time safety shard result hash drift"
        )
    counts = dict(row.get("counts") or {})
    for field in (
        "authoritative_promotion_count",
        "xfa_paths_started",
        "registry_writes",
        "database_writes",
        "q4_access_count_delta",
        "data_purchase_count",
        "broker_connections",
        "orders",
    ):
        if int(counts.get(field, -1)) != 0:
            raise AutonomousEventTimeSafetyFrontierError(
                f"read-only event-time safety invariant failed: {field}"
            )
    return row


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AutonomousEventTimeSafetyFrontierError(
            f"path escapes project root: {value}"
        ) from exc
    return resolved


__all__ = [
    "AutonomousEventTimeSafetyFrontierError",
    "COMPOSITE_SCHEMA",
    "MAXIMUM_PROFILES",
    "SCHEMA",
    "build_autonomous_event_time_safety_frontier",
    "compose_autonomous_event_time_safety_frontier_shards",
    "event_time_safety_frontier_worker",
    "frozen_event_time_safety_profiles",
]
