"""Bounded causal hierarchical source router over the complete 0029 Stage-2 bank.

This experiment deliberately differs from the event-level learned selector.  It
uses B1 only to estimate source and niche economics, freezes a small diverse
expert set, uses B2 only to select a daily trade budget/static risk tier, and
opens B3 once.  Missing future outcomes never affect source eligibility or the
router decision; an accepted missing outcome censors the account window.
"""

from __future__ import annotations

import json
import os
import statistics
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
import hydra.account_policy.causal_active_pool_replay as causal_account_replay
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import _hazard_event
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _read_verified_event_evidence,
)
from hydra.research.causal_target_velocity import (
    HazardOutcome,
    _hazard_trajectory,
    _trade_path_event,
)
from hydra.research.direct_trade_ecology_oracle import (
    ACCOUNT_RISK_SCALE,
    HORIZONS,
    SCENARIOS,
    _resize,
)


SCHEMA = "hydra_hierarchical_expert_source_router_v1"
SOURCE_ROOT = Path("data/cache/economic_production/hydra_fast_pass_factory_0029")
DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/hierarchical_expert_source_router_v1/"
    "economic_result.json"
)
TOP_K = 15
GLOBAL_PRIOR_EVENTS = 100.0
NICHE_PRIOR_EVENTS = 20.0
MAX_PER_MARKET = 3
MAX_PER_MECHANISM = 2
TRADE_BUDGETS = (2, 4, 8)
RISK_TIERS = (0.5, 1.0, 1.5)


class HierarchicalExpertRouterError(RuntimeError):
    """The bounded experiment cannot proceed without weakening its contract."""


def decision_card() -> dict[str, Any]:
    core = {
        "schema": "hydra_hierarchical_expert_router_decision_card_v1",
        "hypothesis": (
            "A small behaviorally diverse source ecology selected with B1-only "
            "hierarchical economic shrinkage can route simultaneous causal "
            "signals into faster legal account paths than one expert, equal-risk "
            "pooling, or exposure-matched random priority."
        ),
        "source_universe": "ALL_5000_STAGE2_CANDIDATES",
        "source_fit_role": "B1_ONLY",
        "profile_selection_role": "B2_ONLY",
        "final_development_role": "B3_OPENED_ONCE",
        "B4_access": False,
        "top_k": TOP_K,
        "trade_budgets_per_session": list(TRADE_BUDGETS),
        "risk_tiers": list(RISK_TIERS),
        "advancement_gate": {
            "minimum_B3_normal_passes": 1,
            "minimum_B3_stressed_passes_advisory": 1,
            "maximum_B3_normal_mll_breach_rate": 0.10,
            "positive_B3_stressed_net": True,
            "must_beat_at_least_one_matched_control": True,
        },
        "missing_outcome_rule": "ACCEPTED_SIGNAL_MAKES_WINDOW_DATA_CENSORED",
        "promotion_eligible": False,
        "data_purchase": False,
        "q4_access": False,
    }
    return {**core, "decision_card_hash": stable_hash(core)}


def niche_key(candidate: Mapping[str, Any]) -> tuple[str, str, int, str]:
    return (
        str(candidate["execution_market"]),
        str(candidate["mechanism"]),
        int(candidate["session_code"]),
        str(candidate["timeframe"]),
    )


def hierarchical_posterior(
    *,
    global_total: float,
    global_count: int,
    niche_total: float,
    niche_count: int,
    source_total: float,
    source_count: int,
) -> tuple[float, float]:
    """Return niche then source posterior stressed USD per completed event."""

    if global_count <= 0 or niche_count < 0 or source_count < 0:
        raise ValueError("invalid shrinkage counts")
    global_mean = float(global_total) / int(global_count)
    niche_mean = (
        float(niche_total) + GLOBAL_PRIOR_EVENTS * global_mean
    ) / (int(niche_count) + GLOBAL_PRIOR_EVENTS)
    source_mean = (
        float(source_total) + NICHE_PRIOR_EVENTS * niche_mean
    ) / (int(source_count) + NICHE_PRIOR_EVENTS)
    return float(niche_mean), float(source_mean)


def _iter_stage2(project: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    root = project / SOURCE_ROOT
    for wave in (1, 2):
        for path in sorted((root / f"wave_{wave:02d}/stage2_batches").glob("batch_*.jsonl")):
            with path.open("rt", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        row = json.loads(line)
                        row["_source_wave"] = wave
                        rows.append(row)
    if len(rows) != 5000 or len({str(row["candidate_id"]) for row in rows}) != 5000:
        raise HierarchicalExpertRouterError(
            f"expected 5,000 unique Stage-2 sources, got {len(rows)}"
        )
    return rows


def score_and_select_sources(
    rows: Sequence[Mapping[str, Any]], *, top_k: int = TOP_K
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fit B1-only source/niche posteriors and freeze a diverse top-k."""

    niche_totals: dict[tuple[str, str, int, str], list[float]] = defaultdict(
        lambda: [0.0, 0.0]
    )
    global_total = 0.0
    global_count = 0
    for row in rows:
        block = dict(row["block_economics"]["B1"])
        total = float(block["stressed_net"])
        count = int(block["completed_event_count"])
        key = niche_key(row["candidate"])
        niche_totals[key][0] += total
        niche_totals[key][1] += count
        global_total += total
        global_count += count

    scored: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        block = dict(row["block_economics"]["B1"])
        source_count = int(block["completed_event_count"])
        source_total = float(block["stressed_net"])
        key = niche_key(row["candidate"])
        niche_total, niche_count = niche_totals[key]
        niche_mean, source_mean = hierarchical_posterior(
            global_total=global_total,
            global_count=global_count,
            niche_total=niche_total,
            niche_count=int(niche_count),
            source_total=source_total,
            source_count=source_count,
        )
        # Target velocity matters alongside per-event quality.  The rate is B1
        # only and is capped so a dense source cannot dominate by churn alone.
        event_rate = source_count / 65.0
        velocity_score = source_mean * min(event_rate, 4.0)
        row["_router_score"] = float(velocity_score)
        row["_source_posterior_usd_per_event"] = float(source_mean)
        row["_niche_posterior_usd_per_event"] = float(niche_mean)
        row["_niche"] = key
        scored.append(row)

    scored.sort(
        key=lambda row: (
            float(row["_router_score"]),
            float(row["_source_posterior_usd_per_event"]),
            int(row["block_economics"]["B1"]["completed_event_count"]),
            str(row["candidate_id"]),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    market_counts: Counter[str] = Counter()
    mechanism_counts: Counter[str] = Counter()
    used_niches: set[tuple[str, str, int, str]] = set()
    used_behaviors: set[str] = set()
    for row in scored:
        market = str(row["candidate"]["execution_market"])
        mechanism = str(row["candidate"]["mechanism"])
        niche = tuple(row["_niche"])
        behavior = str(row["realized_behavioral_fingerprint"])
        if (
            float(row["_router_score"]) <= 0.0
            or niche in used_niches
            or behavior in used_behaviors
            or market_counts[market] >= MAX_PER_MARKET
            or mechanism_counts[mechanism] >= MAX_PER_MECHANISM
        ):
            continue
        selected.append(row)
        market_counts[market] += 1
        mechanism_counts[mechanism] += 1
        used_niches.add(niche)
        used_behaviors.add(behavior)
        if len(selected) == top_k:
            break
    if len(selected) < min(8, top_k):
        raise HierarchicalExpertRouterError("insufficient positive diverse B1 experts")

    audit = {
        "source_count": len(rows),
        "global_B1_completed_event_count": global_count,
        "global_B1_stressed_net_usd": global_total,
        "global_B1_stressed_usd_per_event": global_total / global_count,
        "niche_count": len(niche_totals),
        "selected_count": len(selected),
        "market_counts": dict(market_counts),
        "mechanism_counts": dict(mechanism_counts),
        "selection_uses_only_B1_aggregate_economics": True,
        "coverage_status_used_in_selection": False,
        "B2_B3_B4_economics_used_in_selection": False,
        "all_5000_source_identity_hash": stable_hash(
            sorted(
                (str(row["candidate_id"]), str(row["candidate_fingerprint"]))
                for row in rows
            )
        ),
    }
    return selected, audit


def _neutral_event_id(value: str) -> str:
    for suffix in (":NORMAL", ":STRESSED_1_5X"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def load_selected_opportunities(
    project: Path, selected: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, float], dict[str, Any]]:
    proposals: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    risk_charges: dict[str, float] = {}
    source_receipts: list[dict[str, Any]] = []
    raw_count = 0
    causal_defects = 0
    for source in selected:
        candidate_id = str(source["candidate_id"])
        events, receipt = _read_verified_event_evidence(
            project, dict(source["event_evidence"])
        )
        source_receipts.append(receipt)
        raw_count += len(events)
        risk_charges[candidate_id] = float(
            _declared_stop_risk_charge_per_mini(events, source["candidate"])
        )
        for raw in events:
            if not (
                int(raw["event_time_ns"]) <= int(raw["decision_time_ns"])
                and int(raw["available_at_ns"]) <= int(raw["decision_time_ns"])
                and int(raw["order_submit_time_ns"]) >= int(raw["decision_time_ns"])
                and int(raw["earliest_executable_time_ns"])
                >= int(raw["decision_time_ns"])
            ):
                causal_defects += 1
            event = _hazard_event(raw)
            completed = event.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
            normal = stressed = None
            if completed:
                normal = _hazard_trajectory(
                    event, _trade_path_event(event, scenario="NORMAL"), scenario="NORMAL"
                )
                stressed = _hazard_trajectory(
                    event,
                    _trade_path_event(event, scenario="STRESSED_1_5X"),
                    scenario="STRESSED_1_5X",
                )
            # One market/timestamp is one simultaneous initial-position choice,
            # irrespective of how many source experts emitted a raw trigger.
            proposals[(str(event.execution_market), int(event.decision_time_ns))].append(
                {
                    "candidate_id": candidate_id,
                    "source_score": float(source["_router_score"]),
                    "session_day": int(event.session_day),
                    "decision_time_ns": int(event.decision_time_ns),
                    "completed": bool(completed),
                    "normal": normal,
                    "stressed": stressed,
                    "opportunity_member_id": str(event.event_id),
                }
            )
    if causal_defects:
        raise HierarchicalExpertRouterError(
            f"selected reachable event causality defects: {causal_defects}"
        )
    opportunities: list[dict[str, Any]] = []
    for (market, decision_ns), members in sorted(proposals.items()):
        members.sort(
            key=lambda row: (
                float(row["source_score"]), str(row["candidate_id"])
            ),
            reverse=True,
        )
        opportunities.append(
            {
                "opportunity_id": stable_hash(
                    {"market": market, "decision_time_ns": decision_ns}
                ),
                "market": market,
                "decision_time_ns": decision_ns,
                "session_day": int(members[0]["session_day"]),
                "members": members,
            }
        )
    return opportunities, risk_charges, {
        "selected_source_raw_event_count": raw_count,
        "simultaneous_opportunity_count": len(opportunities),
        "multi_source_opportunity_count": sum(
            len(row["members"]) > 1 for row in opportunities
        ),
        "causality_defect_count": causal_defects,
        "source_receipt_set_hash": stable_hash(source_receipts),
    }


def route_actions(
    opportunities: Sequence[Mapping[str, Any]],
    *,
    daily_budget: int,
    mode: str,
    best_candidate_id: str | None = None,
) -> list[dict[str, Any]]:
    """Route causally; future signals cannot displace an already accepted one."""

    if daily_budget <= 0:
        raise ValueError("daily budget must be positive")
    accepted_by_day: Counter[int] = Counter()
    routed: list[dict[str, Any]] = []
    for opportunity in sorted(
        opportunities,
        key=lambda row: (int(row["decision_time_ns"]), str(row["opportunity_id"])),
    ):
        members = list(opportunity["members"])
        if mode == "HIERARCHICAL":
            chosen = max(
                members,
                key=lambda row: (float(row["source_score"]), str(row["candidate_id"])),
            )
        elif mode == "RANDOM":
            chosen = min(
                members,
                key=lambda row: stable_hash(
                    {
                        "seed": 730031,
                        "opportunity": opportunity["opportunity_id"],
                        "candidate": row["candidate_id"],
                    }
                ),
            )
        elif mode == "BEST_EXPERT":
            eligible = [
                row for row in members if str(row["candidate_id"]) == best_candidate_id
            ]
            if not eligible:
                continue
            chosen = eligible[0]
        else:
            raise ValueError(f"unknown route mode: {mode}")
        day = int(opportunity["session_day"])
        if accepted_by_day[day] >= daily_budget:
            continue
        accepted_by_day[day] += 1
        routed.append({**dict(chosen), "opportunity_id": opportunity["opportunity_id"]})
    return routed


def _scenario_trajectories(
    routed: Sequence[Mapping[str, Any]], *, scenario: str, account: str, risk_tier: float
) -> tuple[dict[str, tuple[Any, ...]], set[int], int]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    censored_days: set[int] = set()
    for row in routed:
        if not bool(row["completed"]):
            # The router accepted this signal causally.  It is unknown, not a
            # flat/abstain action, so every intersecting evaluation is censored.
            censored_days.add(int(row["session_day"]))
            continue
        trajectory = row["normal"] if scenario == "NORMAL" else row["stressed"]
        grouped[str(row["candidate_id"])].append(
            _resize(
                trajectory,
                account_scale=ACCOUNT_RISK_SCALE[account],
                action=float(risk_tier),
            )
        )
    checked: dict[str, tuple[Any, ...]] = {}
    violation_count = 0
    for candidate_id, values in grouped.items():
        rows, violations = _apply_session_contract(tuple(values))
        # Keep the non-compliant event in the exact replay with its explicit
        # false flag.  The account engine, not the research adapter, owns the
        # hard-rule outcome.  Dropping it would manufacture a flat period.
        violation_count += int(violations)
        checked[candidate_id] = tuple(rows)
    return checked, censored_days, violation_count


def _router_policy(
    label: str,
    rules: Mapping[str, Any],
    component_ids: Sequence[str],
    risk_charges: Mapping[str, float],
) -> ActiveRiskPoolPolicy:
    """Local policy adapter that also supports a one-expert control."""

    if not component_ids:
        raise HierarchicalExpertRouterError("account policy has no components")
    target = float(rules["profit_target_usd"])
    mll = float(rules["maximum_loss_limit_usd"])
    return ActiveRiskPoolPolicy(
        policy_id=f"hierarchical-expert-router:{label}",
        component_priority=tuple(component_ids),
        nominal_risk_charge_per_mini=tuple(
            (value, float(risk_charges[value])) for value in component_ids
        ),
        maximum_concurrent_sleeves=min(4, len(component_ids)),
        aggregate_open_risk_ceiling=mll,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(rules["maximum_mini_contracts"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll,
        daily_consistency_profit_guard=target
        * float(rules["consistency_target_fraction"]),
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


@contextmanager
def _constant_priority(policy: Any):
    original = causal_account_replay._priority
    lookup = {value: index for index, value in enumerate(policy.component_priority)}

    def priority(active_policy: Any, component_id: str) -> int:
        if active_policy is policy:
            return lookup[component_id]
        return original(active_policy, component_id)

    causal_account_replay._priority = priority
    try:
        yield
    finally:
        causal_account_replay._priority = original


def _summary(receipts: Sequence[Mapping[str, Any]], *, requested: int) -> dict[str, Any]:
    completed = [row for row in receipts if row["terminal_class"] != "DATA_CENSORED"]
    if not completed:
        return {
            "requested": requested,
            "full_coverage": 0,
            "data_censored": requested,
            "passes": 0,
            "pass_rate": 0.0,
            "mll_breaches": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": None,
            "net_total_usd": 0.0,
            "target_progress_p25": None,
            "target_progress_median": None,
            "minimum_mll_buffer_usd": None,
        }
    progress = [float(row["target_progress"]) for row in completed]
    return {
        "requested": requested,
        "full_coverage": len(completed),
        "data_censored": requested - len(completed),
        "passes": sum(bool(row["passed"]) for row in completed),
        "pass_rate": sum(bool(row["passed"]) for row in completed) / len(completed),
        "mll_breaches": sum(bool(row["mll_breached"]) for row in completed),
        "mll_breach_rate": sum(bool(row["mll_breached"]) for row in completed)
        / len(completed),
        "consistency_compliance_rate": sum(
            bool(row["consistency_ok"]) for row in completed
        )
        / len(completed),
        "net_total_usd": sum(float(row["net_pnl_usd"]) for row in completed),
        "target_progress_p25": float(np.quantile(progress, 0.25)),
        "target_progress_median": statistics.median(progress),
        "minimum_mll_buffer_usd": min(
            float(row["minimum_mll_buffer_usd"]) for row in completed
        ),
    }


def exact_evaluate(
    *,
    routed: Sequence[Mapping[str, Any]],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rules: Mapping[str, Any],
    risk_charges: Mapping[str, float],
    risk_tier: float,
    block: str,
    policy_label: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    matrix: dict[str, Any] = {}
    receipts: list[dict[str, Any]] = []
    component_ids = sorted({str(row["candidate_id"]) for row in routed})
    for account in ("50K", "100K", "150K"):
        policy = _router_policy(
            f"hierarchical:{policy_label}:{account}",
            rules[account],
            component_ids,
            risk_charges,
        )
        config = _account_config(rules[account])
        matrix[account] = {}
        with _constant_priority(policy):
            for scenario in SCENARIOS:
                trajectories, censored_days, session_violations = _scenario_trajectories(
                    routed, scenario=scenario, account=account, risk_tier=risk_tier
                )
                matrix[account][scenario] = {}
                for horizon in HORIZONS:
                    frozen = [
                        int(day)
                        for day, value in starts[horizon]
                        if str(value) == block
                    ]
                    cell: list[dict[str, Any]] = []
                    for start_day in frozen:
                        position = calendar.index(start_day)
                        window = tuple(int(day) for day in calendar[position : position + horizon])
                        if len(window) != horizon:
                            cell.append(
                                {
                                    "terminal_class": "DATA_CENSORED",
                                    "start_day": start_day,
                                    "reason": "INCOMPLETE_CALENDAR_HORIZON",
                                }
                            )
                            continue
                        if set(window) & censored_days:
                            cell.append(
                                {
                                    "terminal_class": "DATA_CENSORED",
                                    "start_day": start_day,
                                    "reason": "ACCEPTED_SIGNAL_MISSING_OUTCOME",
                                }
                            )
                            continue
                        episode = run_causal_shared_account_episode(
                            trajectories,
                            calendar,
                            policy=policy,
                            start_day=start_day,
                            maximum_duration_days=horizon,
                            config=config,
                        )
                        receipt = {
                            "terminal_class": str(episode.terminal.value),
                            "policy": policy_label,
                            "account": account,
                            "scenario": scenario,
                            "block": block,
                            "horizon": horizon,
                            "start_day": start_day,
                            "passed": bool(episode.passed),
                            "mll_breached": bool(episode.mll_breached),
                            "consistency_ok": bool(episode.consistency_ok),
                            "net_pnl_usd": float(episode.net_pnl),
                            "target_progress": float(episode.target_progress),
                            "minimum_mll_buffer_usd": float(episode.minimum_mll_buffer),
                            "days_to_target": episode.days_to_target,
                            "path_hash": stable_hash(episode.to_dict(include_paths=True)),
                        }
                        cell.append(receipt)
                        receipts.append(receipt)
                    matrix[account][scenario][f"P{horizon}"] = _summary(
                        cell, requested=len(frozen)
                    )
                    matrix[account][scenario][f"P{horizon}"][
                        "source_session_contract_violation_events"
                    ] = session_violations
    return matrix, receipts


def aggregate_matrix(matrix: Mapping[str, Any]) -> dict[str, Any]:
    cells = [
        value
        for account in matrix.values()
        for scenario in account.values()
        for value in scenario.values()
    ]
    normal = [
        value
        for account in matrix.values()
        for value in account["NORMAL"].values()
    ]
    stressed = [
        value
        for account in matrix.values()
        for value in account["STRESSED_1_5X"].values()
    ]
    return {
        "normal_passes": sum(int(value["passes"]) for value in normal),
        "stressed_passes": sum(int(value["passes"]) for value in stressed),
        "normal_mll_breaches": sum(int(value["mll_breaches"]) for value in normal),
        "stressed_mll_breaches": sum(int(value["mll_breaches"]) for value in stressed),
        "normal_full_coverage": sum(int(value["full_coverage"]) for value in normal),
        "stressed_full_coverage": sum(int(value["full_coverage"]) for value in stressed),
        "normal_net_total_usd": sum(float(value["net_total_usd"]) for value in normal),
        "stressed_net_total_usd": sum(
            float(value["net_total_usd"]) for value in stressed
        ),
        "median_target_progress_across_cells": statistics.median(
            float(value["target_progress_median"])
            for value in cells
            if value["target_progress_median"] is not None
        )
        if any(value["target_progress_median"] is not None for value in cells)
        else None,
    }


def frozen_grid_receipt(
    calendar: Sequence[int], starts: Mapping[int, Sequence[tuple[int, str]]]
) -> dict[str, Any]:
    payload = {
        "construction": "TIMESTAMP_AND_AVAILABILITY_ONLY_NO_OUTCOME_FILTER",
        "calendar_first": int(calendar[0]),
        "calendar_last": int(calendar[-1]),
        "calendar_session_count": len(calendar),
        "B2": {
            f"P{horizon}": [int(day) for day, block in starts[horizon] if block == "B2"]
            for horizon in HORIZONS
        },
        "B3": {
            f"P{horizon}": [int(day) for day, block in starts[horizon] if block == "B3"]
            for horizon in HORIZONS
        },
        "accepted_missing_outcome_treatment": "DATA_CENSORED_NOT_ABSTAIN",
    }
    return {**payload, "common_grid_hash": stable_hash(payload)}


def run_router(project: Path) -> dict[str, Any]:
    started = time.perf_counter()
    card = decision_card()
    sources = _iter_stage2(project)
    selected, source_audit = score_and_select_sources(sources)
    opportunities, risk_charges, opportunity_audit = load_selected_opportunities(
        project, selected
    )
    manifest = _load_self_hashed_manifest(project / DEFAULT_FAST_PASS_MANIFEST)
    calendar, starts, grid_audit = _load_frozen_grid(project, manifest)
    rules, rule_audit = _load_rule_snapshot(project / DEFAULT_RULE_SNAPSHOT)
    common_grid = frozen_grid_receipt(calendar, starts)
    best_id = str(selected[0]["candidate_id"])

    b2_profiles: dict[str, Any] = {}
    for budget in TRADE_BUDGETS:
        routed = route_actions(
            opportunities, daily_budget=budget, mode="HIERARCHICAL"
        )
        for risk_tier in RISK_TIERS:
            label = f"BUDGET_{budget}_RISK_{risk_tier:.1f}"
            matrix, _ = exact_evaluate(
                routed=routed,
                calendar=calendar,
                starts=starts,
                rules=rules,
                risk_charges=risk_charges,
                risk_tier=risk_tier,
                block="B2",
                policy_label=label,
            )
            b2_profiles[label] = {
                "daily_budget": budget,
                "risk_tier": risk_tier,
                "accepted_opportunities": len(routed),
                "matrix": matrix,
                "aggregate": aggregate_matrix(matrix),
            }
            print(
                f"B2_PROFILE_DONE {label} "
                f"normal_pass={b2_profiles[label]['aggregate']['normal_passes']} "
                f"stress_pass={b2_profiles[label]['aggregate']['stressed_passes']}",
                flush=True,
            )
    selected_profile = max(
        b2_profiles,
        key=lambda label: (
            int(b2_profiles[label]["aggregate"]["normal_passes"]),
            int(b2_profiles[label]["aggregate"]["stressed_passes"]),
            -int(b2_profiles[label]["aggregate"]["normal_mll_breaches"]),
            -int(b2_profiles[label]["aggregate"]["stressed_mll_breaches"]),
            float(b2_profiles[label]["aggregate"]["stressed_net_total_usd"]),
            float(
                b2_profiles[label]["aggregate"]["median_target_progress_across_cells"]
                or -1e9
            ),
            -float(b2_profiles[label]["risk_tier"]),
            -int(b2_profiles[label]["daily_budget"]),
        ),
    )
    chosen = b2_profiles[selected_profile]
    chosen_budget = int(chosen["daily_budget"])
    chosen_risk = float(chosen["risk_tier"])

    control_routes = {
        "HIERARCHICAL_ROUTER": route_actions(
            opportunities, daily_budget=chosen_budget, mode="HIERARCHICAL"
        ),
        "BEST_EXPERT": route_actions(
            opportunities,
            daily_budget=chosen_budget,
            mode="BEST_EXPERT",
            best_candidate_id=best_id,
        ),
        "EQUAL_RISK_ALL_SELECTED": route_actions(
            opportunities, daily_budget=10**9, mode="HIERARCHICAL"
        ),
        "RANDOM_EXPOSURE_MATCHED": route_actions(
            opportunities, daily_budget=chosen_budget, mode="RANDOM"
        ),
    }
    b3: dict[str, Any] = {}
    b3_receipts: list[dict[str, Any]] = []
    for label, routed in control_routes.items():
        matrix, receipts = exact_evaluate(
            routed=routed,
            calendar=calendar,
            starts=starts,
            rules=rules,
            risk_charges=risk_charges,
            risk_tier=chosen_risk,
            block="B3",
            policy_label=label,
        )
        b3[label] = {
            "accepted_opportunities": len(routed),
            "matrix": matrix,
            "aggregate": aggregate_matrix(matrix),
        }
        b3_receipts.extend(receipts)
        print(
            f"B3_CONTROL_DONE {label} "
            f"normal_pass={b3[label]['aggregate']['normal_passes']} "
            f"stress_pass={b3[label]['aggregate']['stressed_passes']}",
            flush=True,
        )

    router = b3["HIERARCHICAL_ROUTER"]["aggregate"]
    matched = (
        b3["BEST_EXPERT"]["aggregate"],
        b3["EQUAL_RISK_ALL_SELECTED"]["aggregate"],
        b3["RANDOM_EXPOSURE_MATCHED"]["aggregate"],
    )
    normal_mll_rate = int(router["normal_mll_breaches"]) / max(
        1, int(router["normal_full_coverage"])
    )
    beats_control = any(
        int(router["normal_passes"]) > int(value["normal_passes"])
        or (
            int(router["normal_passes"]) == int(value["normal_passes"])
            and float(router["median_target_progress_across_cells"] or -1e9)
            > float(value["median_target_progress_across_cells"] or -1e9)
        )
        for value in matched
    )
    advanced = (
        int(router["normal_passes"]) >= 1
        and int(router["stressed_passes"]) >= 1
        and normal_mll_rate <= 0.10
        and float(router["stressed_net_total_usd"]) > 0.0
        and beats_control
    )
    selected_report = [
        {
            "rank": index,
            "candidate_id": str(row["candidate_id"]),
            "candidate_fingerprint": str(row["candidate_fingerprint"]),
            "behavioral_fingerprint": str(row["realized_behavioral_fingerprint"]),
            "niche": list(row["_niche"]),
            "B1_completed_events": int(
                row["block_economics"]["B1"]["completed_event_count"]
            ),
            "B1_stressed_net_usd": float(row["block_economics"]["B1"]["stressed_net"]),
            "niche_posterior_usd_per_event": float(
                row["_niche_posterior_usd_per_event"]
            ),
            "source_posterior_usd_per_event": float(
                row["_source_posterior_usd_per_event"]
            ),
            "router_velocity_score": float(row["_router_score"]),
            "source_coverage_status_not_used_for_selection": str(
                row["coverage_status"]
            ),
        }
        for index, row in enumerate(selected, start=1)
    ]
    core = {
        "schema": SCHEMA,
        "status": (
            "HIERARCHICAL_ROUTER_B3_ADVANCEMENT"
            if advanced
            else "HIERARCHICAL_ROUTER_FALSIFIED_ONE_ATTEMPT"
        ),
        "decision_card": card,
        "source_audit": source_audit,
        "selected_experts": selected_report,
        "opportunity_audit": opportunity_audit,
        "common_evaluation_grid": common_grid,
        "B2_profile_results": b2_profiles,
        "selected_profile": {
            "profile_id": selected_profile,
            "daily_trade_budget": chosen_budget,
            "static_risk_tier": chosen_risk,
            "selection_role": "B2_ONLY",
        },
        "B3_opened_once": True,
        "B3_controls": b3,
        "B3_episode_receipts": b3_receipts,
        "advancement_gate_result": {
            "advanced": advanced,
            "normal_mll_breach_rate": normal_mll_rate,
            "beats_at_least_one_control": beats_control,
        },
        "manifest_hash": str(manifest["manifest_hash"]),
        "frozen_grid_hash": str(grid_audit["grid_hash"]),
        "official_rule_snapshot_hash": str(rule_audit["parsed_rule_hash"]),
        "B4_access_count": 0,
        "data_purchase_count": 0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "promotion_status": None,
        "evidence_tier_change": False,
        "next_action": (
            "FREEZE_FOR_NEXT_EVIDENCE_TIER_WITHOUT_RETUNING"
            if advanced
            else "CLOSE_HIERARCHICAL_ROUTER_AFTER_ONE_ATTEMPT"
        ),
        "wall_clock_seconds": time.perf_counter() - started,
    }
    return {**core, "result_hash": stable_hash(core)}


def write_router(project: Path, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    target = output if output.is_absolute() else project / output
    target.parent.mkdir(parents=True, exist_ok=True)
    card = decision_card()
    (target.parent / "decision_card.json").write_text(
        json.dumps(card, indent=2, sort_keys=True) + "\n"
    )
    result = run_router(project.resolve())
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    project = Path.cwd().resolve()
    print(
        f"HIERARCHICAL_EXPERT_ROUTER_STARTED pid={os.getpid()} "
        "sources=5000 B1=FIT B2=PROFILE B3=OPEN_ONCE B4=UNTOUCHED",
        flush=True,
    )
    result = write_router(project)
    print(
        json.dumps(
            {
                "status": result["status"],
                "selected_profile": result["selected_profile"],
                "router_B3": result["B3_controls"]["HIERARCHICAL_ROUTER"][
                    "aggregate"
                ],
                "result_hash": result["result_hash"],
                "output": str(project / DEFAULT_OUTPUT),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
