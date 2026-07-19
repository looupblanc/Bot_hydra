"""Bounded, read-only account exploration for frozen V7.1 event-time candidates.

The worker deliberately lives outside the persistent controller.  It rebuilds
only immutable, already-viewed V7.1 development candidates, prices their
existing causal trade paths under the frozen BASE and STRESS_1_5X cost models,
and runs the authoritative chronological Combine episode simulator for every
legal whole-contract tier in the official 50K/100K/150K snapshots.

It performs no writes, consumes no protected data, assigns no promotion, and
has no broker or order capability.  Returned results are Tier-E development
diagnostics only; overlapping rolling starts are reported separately from the
greedy non-overlapping effective count.
"""

from __future__ import annotations

import hashlib
import json
import statistics
import time
from collections import Counter
from dataclasses import replace
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
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    TradePathEvent,
    run_combine_episode,
)
from hydra.research import v71_cross_clock_flow_grammar as grammar4
from hydra.research import v71_trade_size_composition as grammar6
from hydra.research.v71_event_mechanism_grammar import signal_path_hash
from hydra.research.v7_graveyard import class_feedback
from hydra.validation.v71_opportunity_density_tripwire import (
    build_candidate_events,
)
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year


SCHEMA = "hydra_v71_event_time_account_exploration_v1"
HORIZONS = (5, 10, 20)
ACCOUNT_LABELS = ("50K", "100K", "150K")
SCENARIOS = (CostStress.BASE, CostStress.STRESS_1_5X)

G4_CANDIDATE_ID = (
    "v71g4_cross_clock_flow_confirmation_"
    "flow_and_progress_agreement_reversal_h60"
)
G6_CANDIDATE_IDS = (
    "v71g6_trade_size_composition_large_clip_absorption_reversal_h60",
    "v71g6_trade_size_composition_large_clip_flow_onset_continuation_h60",
)
PROHIBITED_GRAMMAR_PREFIXES = ("v71g3_", "v71g5_")

G4_COHORT_PATH = Path(
    "WORM/v7.1-underpowered-combine-cohort-0001-2026-07-13.json"
)
G4_COHORT_SHA256 = (
    "a2973de8e8ad11607d807b7cea5216db9f860dedff3ade815f34fd360b1c28d5"
)
G6_SIGNAL_PATH = Path(
    "reports/v7_1/discovery_0006/"
    "v71_trade_size_composition_signal_manifest.json"
)
G6_SIGNAL_SHA256 = (
    "ea883cbf7ee460b5467f0023f171e67528f696f748c09aa7c95d11a87e7db752"
)
G6_FUNNEL_PATH = Path(
    "reports/v7_1/discovery_0006/"
    "v71_trade_size_composition_funnel_result.json"
)
G6_FUNNEL_SHA256 = (
    "c99dd8aeca6bdcb9f908f6b0b7e39f4d2cf06b8671c95b9e190a276ffef9ec67"
)
G6_TRIPWIRE_PATH = Path(
    "reports/v7_1/discovery_0006/"
    "v71_trade_size_composition_tripwire_result.json"
)
G6_TRIPWIRE_SHA256 = (
    "c3a0a53105ed260acb83c65b42312915bb4ed6f8047f061ca34d3ada81679596"
)
G6_POWER_PATH = Path(
    "reports/v7_1/discovery_0006/"
    "v71_trade_size_composition_power_audit_result.json"
)
G6_POWER_SHA256 = (
    "ab7fd3885e23943c4abd532f82902629f1a689e962ca4d8a1d7dc9869a5f32de"
)


class V71EventTimeAccountExplorationError(RuntimeError):
    """Frozen evidence or account semantics failed closed."""


def run_v71_event_time_account_exploration(
    root: str | Path,
    *,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
    maximum_quantity_per_account: int | None = None,
) -> dict[str, Any]:
    """Return a bounded exact matrix without writing to the repository.

    ``maximum_quantity_per_account`` exists only as a compute-bound test hook.
    Production callers should leave it unset so every legal whole-contract
    tier in each official account snapshot is evaluated.
    """

    started = time.perf_counter()
    project = Path(root).resolve()
    rule_path = _inside(project, rule_snapshot_path)
    rules, rule_receipt = _load_rule_snapshot(rule_path)
    if tuple(rules) != ACCOUNT_LABELS:
        raise V71EventTimeAccountExplorationError(
            "official account-size ordering/inventory drift"
        )
    if maximum_quantity_per_account is not None and not (
        1 <= int(maximum_quantity_per_account) <= 15
    ):
        raise V71EventTimeAccountExplorationError(
            "bounded quantity maximum must be in [1,15]"
        )

    population = _load_frozen_event_population(project)
    minute = population.pop("_minute")
    source_audit = population.pop("_source_audit")
    candidate_metadata = population.pop("_candidate_metadata")
    integration = population.pop("_integration")
    eligible_by_year = _eligible_days_by_year(minute)
    calendar = tuple(
        sorted(
            int(day)
            for year_days in eligible_by_year.values()
            for day in year_days
        )
    )
    starts = build_complete_rolling_start_grid(eligible_by_year)

    results: list[dict[str, Any]] = []
    exact_replays = 0
    passing_candidate_ids: set[str] = set()
    for candidate_id in sorted(population):
        scenario_events = population[candidate_id]
        candidate_result = {
            **candidate_metadata[candidate_id],
            "candidate_id": candidate_id,
            "event_count": len(scenario_events[CostStress.BASE.value]),
            "unique_event_net_usd": {
                scenario.value: float(
                    sum(row.net_pnl for row in scenario_events[scenario.value])
                )
                for scenario in SCENARIOS
            },
            "account_size_matrix": [],
            "evidence_tier": "E",
            "candidate_status": "EXECUTABLE_DEVELOPMENT_DIAGNOSTIC",
            "promotion_status": None,
        }
        for account_label in ACCOUNT_LABELS:
            rule = rules[account_label]
            legal_maximum = int(rule["maximum_mini_contracts"])
            evaluated_maximum = (
                legal_maximum
                if maximum_quantity_per_account is None
                else min(legal_maximum, int(maximum_quantity_per_account))
            )
            frontier, replays = evaluate_candidate_frontier(
                candidate_id=candidate_id,
                scenario_events=scenario_events,
                calendar=calendar,
                starts=starts,
                rule=rule,
                quantities=tuple(range(1, evaluated_maximum + 1)),
            )
            exact_replays += replays
            if any(
                int(cell["stressed"]["pass_count"]) > 0 for cell in frontier
            ):
                passing_candidate_ids.add(candidate_id)
            candidate_result["account_size_matrix"].append(
                {
                    "account_label": account_label,
                    "account_size_usd": int(rule["account_size_usd"]),
                    "profit_target_usd": float(rule["profit_target_usd"]),
                    "maximum_loss_limit_usd": float(
                        rule["maximum_loss_limit_usd"]
                    ),
                    "maximum_mini_contracts": legal_maximum,
                    "evaluated_quantity_maximum": evaluated_maximum,
                    "frontier": frontier,
                    "best_development_cell_by_horizon": {
                        str(horizon): _best_cell(
                            [
                                cell
                                for cell in frontier
                                if int(cell["horizon_trading_days"]) == horizon
                            ]
                        )
                        for horizon in HORIZONS
                    },
                }
            )
        candidate_result["candidate_result_hash"] = stable_hash(candidate_result)
        results.append(candidate_result)

    all_cells = [
        cell
        for candidate in results
        for account in candidate["account_size_matrix"]
        for cell in account["frontier"]
    ]
    safe_pass_cells = [
        cell
        for cell in all_cells
        if bool(cell["development_safety_gate"])
        and cell.get("account_rule_compliant") is True
        and float(cell["stressed"].get("net_total_usd", 0.0)) > 0.0
    ]
    normal_episode_count = sum(
        int(cell["normal"].get("episode_count", 0)) for cell in all_cells
    )
    stressed_episode_count = sum(
        int(cell["stressed"].get("episode_count", 0)) for cell in all_cells
    )
    if (
        normal_episode_count != stressed_episode_count
        or normal_episode_count + stressed_episode_count != exact_replays
    ):
        raise V71EventTimeAccountExplorationError(
            "paired normal/stressed exact replay denominator drift"
        )
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_READ_ONLY_EVENT_TIME_EXPLORATION",
        "branch_id": "V71_EVENT_TIME_ACCOUNT_SIZE_EXPLORATION",
        "source_population": {
            "selected_candidate_ids": [row["candidate_id"] for row in results],
            "selected_candidate_count": len(results),
            "source_audit": source_audit,
            "integration": integration,
            "prohibited_grammar_prefixes": list(PROHIBITED_GRAMMAR_PREFIXES),
            "g3_or_g5_candidate_count": 0,
        },
        "official_rule_snapshot": rule_receipt,
        "cost_model": {
            "scenarios": [scenario.value for scenario in SCENARIOS],
            "normal": CostStress.BASE.value,
            "stressed": CostStress.STRESS_1_5X.value,
            "costs_are_scaled_with_whole_contract_quantity": True,
        },
        "evaluation_grid": {
            "horizons_trading_days": list(HORIZONS),
            "calendar_session_count": len(calendar),
            "starts": {
                str(horizon): _start_grid_receipt(starts[horizon], horizon)
                for horizon in HORIZONS
            },
            "headline_denominator": "FULL_COVERAGE_ROLLING_DEVELOPMENT_STARTS",
            "overlap_warning": (
                "Rolling starts are dependent development diagnostics; effective "
                "non-overlapping counts are reported and no confirmation is claimed."
            ),
        },
        "candidate_results": results,
        "strongest_development_cell": _best_cell(all_cells),
        "counters": {
            "candidate_count": len(results),
            "account_size_count": len(ACCOUNT_LABELS),
            "account_horizon_quantity_cells": len(all_cells),
            "exact_chronological_account_replays": exact_replays,
            "exact_normal_account_replays": normal_episode_count,
            "exact_stressed_account_replays": stressed_episode_count,
            "candidate_with_normal_pass_count": sum(
                any(int(cell["normal"]["pass_count"]) > 0 for cell in row_cells)
                for row_cells in (
                    [
                        cell
                        for account in candidate["account_size_matrix"]
                        for cell in account["frontier"]
                    ]
                    for candidate in results
                )
            ),
            "candidate_with_stressed_pass_count": len(passing_candidate_ids),
            "safe_stressed_pass_cell_count": len(safe_pass_cells),
            "tier_q_count": 0,
            "tier_g_count": 0,
            "tier_c_count": 0,
            "tier_f_count": 0,
            "xfa_paths_started": 0,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "evidence_tier": "E",
        "promotion_status": None,
        "interpretation_boundary": (
            "Already-viewed development evidence only. Exact Combine mechanics "
            "do not constitute qualification, graduation, confirmation or forward evidence."
        ),
        "decision": (
            "EVENT_TIME_SAFE_EXACT_STRESSED_PASS_OBSERVED_DEVELOPMENT_ONLY"
            if safe_pass_cells
            else (
                "EVENT_TIME_PASSES_OBSERVED_MLL_EXCESS_DEVELOPMENT_ONLY"
                if passing_candidate_ids
                else "EVENT_TIME_LEGAL_FRONTIER_NO_STRESSED_PASS"
            )
        ),
        "result_hash_excludes_runtime_telemetry": True,
    }
    return {
        **core,
        "result_hash": stable_hash(core),
        "runtime_seconds": time.perf_counter() - started,
    }


def event_time_account_exploration_worker(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Pickle-safe economic-worker adapter."""

    return run_v71_event_time_account_exploration(
        str(payload["root"]),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
        ),
        maximum_quantity_per_account=(
            None
            if payload.get("maximum_quantity_per_account") is None
            else int(payload["maximum_quantity_per_account"])
        ),
    )


def build_complete_rolling_start_grid(
    eligible_by_year: Mapping[int, Sequence[int]],
) -> dict[int, tuple[tuple[int, int], ...]]:
    """Freeze every complete rolling start while preserving its year block."""

    grid: dict[int, tuple[tuple[int, int], ...]] = {}
    for horizon in HORIZONS:
        starts: list[tuple[int, int]] = []
        for year in sorted(eligible_by_year):
            days = tuple(sorted({int(value) for value in eligible_by_year[year]}))
            usable = max(0, len(days) - horizon + 1)
            starts.extend((int(days[index]), int(year)) for index in range(usable))
        grid[horizon] = tuple(starts)
    return grid


def evaluate_candidate_frontier(
    *,
    candidate_id: str,
    scenario_events: Mapping[str, Sequence[TradePathEvent]],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, int]]],
    rule: Mapping[str, Any],
    quantities: Sequence[int],
) -> tuple[list[dict[str, Any]], int]:
    """Evaluate one frozen candidate over a legal static whole-contract frontier."""

    if set(scenario_events) != {scenario.value for scenario in SCENARIOS}:
        raise V71EventTimeAccountExplorationError(
            "normal/stressed event scenarios are incomplete"
        )
    _require_scenario_identity(
        scenario_events[CostStress.BASE.value],
        scenario_events[CostStress.STRESS_1_5X.value],
    )
    legal_limit = int(rule["maximum_mini_contracts"])
    frozen_quantities = tuple(sorted({int(value) for value in quantities}))
    if (
        not frozen_quantities
        or frozen_quantities[0] < 1
        or frozen_quantities[-1] > legal_limit
    ):
        raise V71EventTimeAccountExplorationError(
            "quantity frontier exceeds official contract limit"
        )
    config = _account_config(rule)
    scaled = {
        (scenario.value, quantity): tuple(
            _scale_event(row, quantity)
            for row in scenario_events[scenario.value]
        )
        for scenario in SCENARIOS
        for quantity in frozen_quantities
    }
    cells: list[dict[str, Any]] = []
    exact_replays = 0
    for quantity in frozen_quantities:
        for horizon in HORIZONS:
            summaries: dict[str, Any] = {}
            for scenario in SCENARIOS:
                episodes = [
                    (
                        run_combine_episode(
                            scaled[(scenario.value, quantity)],
                            calendar,
                            start_day=int(start_day),
                            maximum_duration_days=horizon,
                            config=config,
                            maximum_mini_equivalent=float(legal_limit),
                        ),
                        int(year),
                    )
                    for start_day, year in starts[horizon]
                ]
                exact_replays += len(episodes)
                summaries[scenario.value] = _summarize_episodes(episodes)
            cells.append(
                {
                    "candidate_id": candidate_id,
                    "account_label": str(rule["account_label"]),
                    "account_size_usd": int(rule["account_size_usd"]),
                    "static_whole_contract_quantity": quantity,
                    "horizon_trading_days": horizon,
                    "full_coverage_start_count": len(starts[horizon]),
                    "effective_nonoverlapping_start_count": _effective_start_count(
                        starts[horizon], horizon, calendar
                    ),
                    "data_censored_start_count": 0,
                    "legally_executable": True,
                    "account_rule_compliant": (
                        summaries[CostStress.BASE.value][
                            "compliance_failure_count"
                        ]
                        == 0
                        and summaries[CostStress.STRESS_1_5X.value][
                            "compliance_failure_count"
                        ]
                        == 0
                    ),
                    "development_safety_gate": (
                        summaries[CostStress.BASE.value]["pass_count"] > 0
                        and summaries[CostStress.STRESS_1_5X.value]["pass_count"]
                        > 0
                        and summaries[CostStress.STRESS_1_5X.value][
                            "mll_breach_rate"
                        ]
                        <= 0.10
                    ),
                    "normal": summaries[CostStress.BASE.value],
                    "stressed": summaries[CostStress.STRESS_1_5X.value],
                    "evidence_tier": "E",
                    "promotion_status": None,
                }
            )
    return cells, exact_replays


def _load_frozen_event_population(root: Path) -> dict[str, Any]:
    dead = {str(row["mechanism_class"]) for row in class_feedback(root / "mission/state/graveyard.db")}
    if "v71g4_cross_clock_flow_confirmation" in dead:
        raise V71EventTimeAccountExplorationError("frozen G4 class is tombstoned")

    cohort = _read_hashed_json(root / G4_COHORT_PATH, G4_COHORT_SHA256)
    frozen_g4 = next(
        (
            dict(row)
            for row in cohort.get("candidates", ())
            if str(row.get("candidate_id")) == G4_CANDIDATE_ID
        ),
        None,
    )
    if frozen_g4 is None or frozen_g4.get("prior_power_status") != "PROMISING_UNDERPOWERED":
        raise V71EventTimeAccountExplorationError("frozen G4 candidate is unavailable")
    minute4, pairs, audit4 = grammar4.load_cross_clock_sources(root)
    specs4 = {row.candidate_id: row for row in grammar4.candidate_specs(root)}
    signals4 = grammar4.generate_signal_population(
        minute4, pairs, project_root=root, graveyard_path=None
    )
    spec4 = specs4[G4_CANDIDATE_ID]
    signal4 = signals4[G4_CANDIDATE_ID]
    _verify_frozen_candidate(frozen_g4, spec4, signal4)

    selected: dict[str, tuple[Any, Sequence[Any], Any]] = {
        G4_CANDIDATE_ID: (spec4, signal4, minute4)
    }
    metadata: dict[str, dict[str, Any]] = {
        G4_CANDIDATE_ID: {
            "grammar_id": grammar4.GRAMMAR_ID,
            "family_id": spec4.family_id,
            "holding_minutes": spec4.holding_minutes,
            "specification_hash": spec4.specification_hash,
            "signal_path_hash": signal_path_hash(signal4),
            "prior_scientific_status": "PROMISING_UNDERPOWERED",
            "formal_rolling_combine_research_eligible": False,
        }
    }

    g6_status = "G6_INTEGRATED_BOUNDED_DEVELOPMENT_ONLY"
    g6_reason: str | None = None
    if "v71g6_trade_size_composition_transitions" in dead:
        g6_status = "G6_FAIL_CLOSED_TOMBSTONED"
        g6_reason = "mechanism class is present in the authoritative cemetery"
    else:
        try:
            manifest6 = _read_hashed_json(root / G6_SIGNAL_PATH, G6_SIGNAL_SHA256)
            funnel6 = _read_hashed_json(root / G6_FUNNEL_PATH, G6_FUNNEL_SHA256)
            tripwire6 = _read_hashed_json(
                root / G6_TRIPWIRE_PATH, G6_TRIPWIRE_SHA256
            )
            power6 = _read_hashed_json(root / G6_POWER_PATH, G6_POWER_SHA256)
            frozen6 = {
                str(row["candidate_id"]): dict(row)
                for row in manifest6.get("candidate_paths", ())
            }
            funnel_by_id = {
                str(row["candidate_id"]): dict(row)
                for row in funnel6.get("candidate_results", ())
            }
            power_by_id = {
                str(row["candidate_id"]): dict(row)
                for row in power6.get("candidate_results", ())
            }
            if tripwire6.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE":
                raise V71EventTimeAccountExplorationError(
                    "G6 grammar tripwire is not GREEN"
                )
            minute6, states6, audit6 = grammar6.load_trade_size_composition_sources(
                root
            )
            if not _same_minute_source(minute4, minute6):
                raise V71EventTimeAccountExplorationError(
                    "G4/G6 minute source identity drift"
                )
            specs6 = {row.candidate_id: row for row in grammar6.candidate_specs(root)}
            signals6 = grammar6.generate_signal_population(
                states6, project_root=root, graveyard_path=None
            )
            for candidate_id in G6_CANDIDATE_IDS:
                frozen = frozen6.get(candidate_id)
                funnel = funnel_by_id.get(candidate_id, {})
                power = power_by_id.get(candidate_id, {})
                if (
                    frozen is None
                    or int(frozen.get("holding_minutes", 0)) != 60
                    or funnel.get("classification")
                    != "WALK_FORWARD_POSITIVE_PENDING_TRIPWIRE_AND_POWER"
                    or power.get("status") != "PROMISING_UNDERPOWERED"
                ):
                    raise V71EventTimeAccountExplorationError(
                        f"G6 bounded eligibility drift: {candidate_id}"
                    )
                spec = specs6[candidate_id]
                signal = signals6[candidate_id]
                _verify_frozen_candidate(frozen, spec, signal)
                selected[candidate_id] = (spec, signal, minute6)
                metadata[candidate_id] = {
                    "grammar_id": grammar6.GRAMMAR_ID,
                    "family_id": spec.family_id,
                    "holding_minutes": spec.holding_minutes,
                    "specification_hash": spec.specification_hash,
                    "signal_path_hash": grammar6.signal_path_hash(signal),
                    "prior_scientific_status": "PROMISING_UNDERPOWERED",
                    "formal_rolling_combine_research_eligible": False,
                    "bounded_master_exploration_only": True,
                }
        except (V71EventTimeAccountExplorationError, grammar6.V71TradeSizeCompositionError) as exc:
            g6_status = "G6_FAIL_CLOSED_NOT_INTEGRATED"
            g6_reason = f"{type(exc).__name__}: {exc}"
            selected = {G4_CANDIDATE_ID: selected[G4_CANDIDATE_ID]}
            metadata = {G4_CANDIDATE_ID: metadata[G4_CANDIDATE_ID]}
            audit6 = None

    costs = load_cost_model()
    output: dict[str, Any] = {}
    for candidate_id, (spec, signals, minute) in sorted(selected.items()):
        per_scenario: dict[str, tuple[TradePathEvent, ...]] = {}
        for scenario in SCENARIOS:
            per_scenario[scenario.value] = build_candidate_events(
                minute,
                {candidate_id: signals},
                {candidate_id: spec},
                costs,
                stress=scenario,
            )[candidate_id]
        _require_scenario_identity(
            per_scenario[CostStress.BASE.value],
            per_scenario[CostStress.STRESS_1_5X.value],
        )
        output[candidate_id] = per_scenario
    output["_minute"] = minute4
    output["_candidate_metadata"] = metadata
    output["_source_audit"] = {
        "G4": audit4.to_dict(),
        "G6": None if g6_status != "G6_INTEGRATED_BOUNDED_DEVELOPMENT_ONLY" else audit6.to_dict(),
        "cost_model_source": costs.source,
        "cost_model_source_checked_utc": costs.source_checked_utc,
    }
    output["_integration"] = {
        "G4": "FROZEN_CANDIDATE_INTEGRATED",
        "G6": g6_status,
        "G6_failure_reason": g6_reason,
        "g3_resurrected": False,
        "g5_resurrected": False,
    }
    return output


def _verify_frozen_candidate(
    frozen: Mapping[str, Any], spec: Any, signals: Sequence[Any]
) -> None:
    candidate_id = str(frozen.get("candidate_id") or "")
    if candidate_id != str(spec.candidate_id):
        raise V71EventTimeAccountExplorationError("candidate identity drift")
    if any(candidate_id.startswith(prefix) for prefix in PROHIBITED_GRAMMAR_PREFIXES):
        raise V71EventTimeAccountExplorationError(
            "tombstoned G3/G5 candidate cannot enter exploration"
        )
    if str(frozen.get("specification_hash")) != str(spec.specification_hash):
        raise V71EventTimeAccountExplorationError(
            f"specification hash drift: {candidate_id}"
        )
    observed_signal_hash = signal_path_hash(signals)
    if str(frozen.get("signal_path_hash")) != observed_signal_hash:
        raise V71EventTimeAccountExplorationError(
            f"signal path hash drift: {candidate_id}"
        )


def _same_minute_source(left: Any, right: Any) -> bool:
    columns = ("minute_start_ns", "availability_ns", "contract")
    return len(left) == len(right) and all(
        np.array_equal(left[column].to_numpy(), right[column].to_numpy())
        for column in columns
    )


def _require_scenario_identity(
    normal: Sequence[TradePathEvent], stressed: Sequence[TradePathEvent]
) -> None:
    def identity(row: TradePathEvent) -> tuple[Any, ...]:
        event_id = row.event_id.rsplit(":", 1)[0]
        return (
            event_id,
            row.decision_ns,
            row.exit_ns,
            row.session_day,
            row.quantity,
            row.mini_equivalent,
            row.gross_pnl,
        )

    if [identity(row) for row in normal] != [identity(row) for row in stressed]:
        raise V71EventTimeAccountExplorationError(
            "normal/stressed decision identity drift"
        )


def _scale_event(event: TradePathEvent, quantity: int) -> TradePathEvent:
    return replace(
        event,
        event_id=f"{event.event_id}:Q{quantity}",
        net_pnl=float(event.net_pnl * quantity),
        gross_pnl=float(event.gross_pnl * quantity),
        worst_unrealized_pnl=float(event.worst_unrealized_pnl * quantity),
        best_unrealized_pnl=float(event.best_unrealized_pnl * quantity),
        quantity=int(event.quantity * quantity),
        mini_equivalent=float(event.mini_equivalent * quantity),
    )


def _summarize_episodes(
    values: Sequence[tuple[CombineEpisodeResult, int]],
) -> dict[str, Any]:
    episodes = [row for row, _year in values]
    if not episodes:
        return {
            "episode_count": 0,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "compliance_failure_count": 0,
            "consistency_compliance_rate": 0.0,
            "net_total_usd": 0.0,
            "net_median_usd": 0.0,
            "target_progress_p25": 0.0,
            "target_progress_median": 0.0,
            "minimum_mll_buffer_usd": None,
            "median_days_to_target": None,
            "terminal_distribution": {},
            "by_year": {},
            "episode_path_hash": stable_hash([]),
        }
    terminal = Counter(row.terminal.value for row in episodes)
    passing_days = [
        int(row.days_to_target)
        for row in episodes
        if row.days_to_target is not None
    ]
    by_year: dict[str, Any] = {}
    for year in sorted({year for _row, year in values}):
        selected = [row for row, observed in values if observed == year]
        by_year[str(year)] = {
            "episode_count": len(selected),
            "pass_count": sum(row.passed for row in selected),
            "mll_breach_count": sum(row.mll_breached for row in selected),
            "net_total_usd": float(sum(row.net_pnl for row in selected)),
            "target_progress_median": float(
                statistics.median(row.target_progress for row in selected)
            ),
        }
    target = [float(row.target_progress) for row in episodes]
    net = [float(row.net_pnl) for row in episodes]
    return {
        "episode_count": len(episodes),
        "pass_count": sum(row.passed for row in episodes),
        "pass_rate": sum(row.passed for row in episodes) / len(episodes),
        "mll_breach_count": sum(row.mll_breached for row in episodes),
        "mll_breach_rate": sum(row.mll_breached for row in episodes) / len(episodes),
        "compliance_failure_count": terminal["COMPLIANCE_FAILURE"],
        "consistency_compliance_rate": sum(row.consistency_ok for row in episodes)
        / len(episodes),
        "net_total_usd": float(sum(net)),
        "net_median_usd": float(statistics.median(net)),
        "target_progress_p25": _percentile(target, 25),
        "target_progress_median": float(statistics.median(target)),
        "minimum_mll_buffer_usd": float(
            min(row.minimum_mll_buffer for row in episodes)
        ),
        "median_days_to_target": (
            float(statistics.median(passing_days)) if passing_days else None
        ),
        "terminal_distribution": dict(sorted(terminal.items())),
        "by_year": by_year,
        "episode_path_hash": stable_hash([row.to_dict() for row in episodes]),
    }


def _best_cell(cells: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not cells:
        return None

    def key(cell: Mapping[str, Any]) -> tuple[Any, ...]:
        stressed = dict(cell["stressed"])
        normal = dict(cell["normal"])
        pass_or_breach = int(stressed["pass_count"]) + int(
            stressed["mll_breach_count"]
        )
        pass_before_breach = (
            int(stressed["pass_count"]) / pass_or_breach
            if pass_or_breach
            else 0.0
        )
        return (
            bool(cell.get("account_rule_compliant")),
            bool(cell.get("development_safety_gate")),
            pass_before_breach,
            -float(stressed["mll_breach_rate"]),
            float(stressed["pass_rate"]),
            float(normal["pass_rate"]),
            float(stressed["target_progress_median"]),
            float(stressed["net_median_usd"]),
            -int(cell["static_whole_contract_quantity"]),
            -int(cell["account_size_usd"]),
        )

    return dict(max(cells, key=key))


def _effective_start_count(
    starts: Sequence[tuple[int, int]], horizon: int, calendar: Sequence[int]
) -> int:
    position = {int(day): index for index, day in enumerate(calendar)}
    count = 0
    next_position = -1
    for start_day, _year in starts:
        current = position[int(start_day)]
        if current < next_position:
            continue
        count += 1
        next_position = current + horizon
    return count


def _start_grid_receipt(
    starts: Sequence[tuple[int, int]], horizon: int
) -> dict[str, Any]:
    by_year = Counter(year for _day, year in starts)
    return {
        "full_coverage_rolling_start_count": len(starts),
        "start_count_by_year": {
            str(year): count for year, count in sorted(by_year.items())
        },
        "start_hash": stable_hash(list(starts)),
        "horizon_trading_days": int(horizon),
    }


def _read_hashed_json(path: Path, expected_sha256: str) -> dict[str, Any]:
    observed = _sha256(path)
    if observed != expected_sha256:
        raise V71EventTimeAccountExplorationError(
            f"frozen input hash drift: {path}"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise V71EventTimeAccountExplorationError(
            f"frozen input is not an object: {path}"
        )
    return payload


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise V71EventTimeAccountExplorationError(
            f"path escapes project root: {value}"
        ) from exc
    return resolved


def _percentile(values: Sequence[float], percentile: float) -> float:
    return float(np.percentile(np.asarray(values, dtype=float), percentile))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "G4_CANDIDATE_ID",
    "G6_CANDIDATE_IDS",
    "SCHEMA",
    "V71EventTimeAccountExplorationError",
    "build_complete_rolling_start_grid",
    "evaluate_candidate_frontier",
    "event_time_account_exploration_worker",
    "run_v71_event_time_account_exploration",
]
