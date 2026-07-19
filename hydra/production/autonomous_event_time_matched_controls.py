"""Read-only matched controls for the frozen event-time safety candidate.

This is deliberately a narrow follow-on to the bounded event-time safety
frontier.  It does not select a new signal or governor.  It freezes the one
specified candidate/profile pair and compares it with three deterministic
controls built from the same sessions, opportunity count, holding horizon and
account rules.

All evidence is already-viewed development evidence.  The module performs no
writes and cannot assign an authoritative evidence tier or promotion.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.economic_evolution.schema import stable_hash
from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.production.autonomous_event_time_safety_frontier import (
    COMPOSITE_SCHEMA as SOURCE_SCHEMA,
    EventTimeSafetyProfile,
    _chronological_block_contract,
    _evaluate_profile_roles,
    frozen_event_time_safety_profiles,
)
from hydra.production.autonomous_exact_replay import (
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _load_rule_snapshot,
)
from hydra.production.v71_event_time_account_exploration import (
    _load_frozen_event_population,
    _require_scenario_identity,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research import v71_trade_size_composition as grammar6
from hydra.research.v71_event_mechanism_grammar import V71Signal
from hydra.validation.v71_opportunity_density_tripwire import (
    build_candidate_events,
)
from hydra.validation.v7_d1_new_dataset_tripwire import _eligible_days_by_year


SCHEMA = "hydra_autonomous_event_time_matched_controls_v1"
STATUS = "COMPLETE_EVENT_TIME_MATCHED_CONTROLS"
ACCOUNT_LABEL = "50K"
FROZEN_CANDIDATE_ID = (
    "v71g6_trade_size_composition_"
    "large_clip_flow_onset_continuation_h60"
)
FROZEN_PROFILE_ID = "event_safe_guard_10_01"
FROZEN_SOURCE_POLICY_ID = "event_time_safety_selected_82f869e660185f366af7"
FROZEN_SPECIFICATION_HASH = (
    "d2c75b72012b099ca0a355ca1f01891506b7b5027a0652116c1fa7d4ce051e71"
)
FROZEN_SIGNAL_PATH_HASH = (
    "b3ac40aa599d759c27c593d19b2abcc611ae292659b469d169dff1ac4513a3e0"
)
DEFAULT_SOURCE_COMPOSITE = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/"
    "branch_results/post_source_exhaustion/post_composite/"
    "event_time_safety_composite.json"
)
CONTROL_IDS = (
    "DIRECTION_FLIP",
    "SESSION_EXPOSURE_MATCHED_DIRECTION_PERMUTATION",
    "SESSION_MATCHED_TIMING_GRID",
)


class AutonomousEventTimeMatchedControlsError(RuntimeError):
    """Frozen input or matched-control semantics failed closed."""


def run_event_time_matched_controls(
    root: str | Path,
    *,
    source_composite_path: str | Path = DEFAULT_SOURCE_COMPOSITE,
    rule_snapshot_path: str | Path = DEFAULT_RULE_SNAPSHOT,
) -> dict[str, Any]:
    """Evaluate the frozen candidate and three deterministic matched controls."""

    project = Path(root).resolve()
    source = _load_source_composite(
        _inside(project, source_composite_path)
    )
    source_candidate = _freeze_source_candidate(source)
    profile = _frozen_profile()

    rules, rule_receipt = _load_rule_snapshot(
        _inside(project, rule_snapshot_path)
    )
    rule = dict(rules[ACCOUNT_LABEL])
    config = _account_config(rule)
    maximum_mini_equivalent = float(rule["maximum_mini_contracts"])
    costs = load_cost_model()
    micro_costs = {
        scenario.value: costs.round_turn_cost(
            "MES", "60m", stress=scenario
        )
        for scenario in (CostStress.BASE, CostStress.STRESS_1_5X)
    }

    population = _load_frozen_event_population(project)
    minute = population.pop("_minute")
    population.pop("_source_audit")
    metadata = population.pop("_candidate_metadata")
    population.pop("_integration")
    if FROZEN_CANDIDATE_ID not in population:
        raise AutonomousEventTimeMatchedControlsError(
            "frozen event-time candidate is absent"
        )
    if (
        str(metadata[FROZEN_CANDIDATE_ID].get("specification_hash"))
        != FROZEN_SPECIFICATION_HASH
        or str(metadata[FROZEN_CANDIDATE_ID].get("signal_path_hash"))
        != FROZEN_SIGNAL_PATH_HASH
    ):
        raise AutonomousEventTimeMatchedControlsError(
            "frozen candidate specification/signal identity drift"
        )
    primary_events = dict(population[FROZEN_CANDIDATE_ID])
    _require_scenario_identity(
        primary_events[CostStress.BASE.value],
        primary_events[CostStress.STRESS_1_5X.value],
    )

    calendar = tuple(
        sorted(
            int(day)
            for days in _eligible_days_by_year(minute).values()
            for day in days
        )
    )
    block_contract = _chronological_block_contract(calendar)
    minute6, states6, _audit6 = grammar6.load_trade_size_composition_sources(
        project
    )
    if not _same_minute_identity(minute, minute6):
        raise AutonomousEventTimeMatchedControlsError(
            "event-time minute source identity drift"
        )
    specs = {row.candidate_id: row for row in grammar6.candidate_specs(project)}
    signals = grammar6.generate_signal_population(
        states6, project_root=project, graveyard_path=None
    )[FROZEN_CANDIDATE_ID]
    if grammar6.signal_path_hash(signals) != FROZEN_SIGNAL_PATH_HASH:
        raise AutonomousEventTimeMatchedControlsError(
            "frozen signal path changed before matched controls"
        )
    spec = specs[FROZEN_CANDIDATE_ID]

    control_signals, control_audits = _matched_signal_controls(
        signals, states6
    )
    control_events: dict[str, Mapping[str, Sequence[TradePathEvent]]] = {
        "DIRECTION_FLIP": {
            scenario.value: tuple(
                _direction_flip_event(row, "DIRECTION_FLIP")
                for row in primary_events[scenario.value]
            )
            for scenario in (CostStress.BASE, CostStress.STRESS_1_5X)
        }
    }
    for control_id, rows in control_signals.items():
        control_events[control_id] = {
            scenario.value: build_candidate_events(
                minute6,
                {FROZEN_CANDIDATE_ID: rows},
                {FROZEN_CANDIDATE_ID: spec},
                costs,
                stress=scenario,
            )[FROZEN_CANDIDATE_ID]
            for scenario in (CostStress.BASE, CostStress.STRESS_1_5X)
        }
    for events in control_events.values():
        _require_scenario_identity(
            events[CostStress.BASE.value],
            events[CostStress.STRESS_1_5X.value],
        )

    arms = {"PRIMARY": primary_events, **control_events}
    evaluated: dict[str, dict[str, Any]] = {}
    for arm_id, events in arms.items():
        full = _evaluate_profile_roles(
            candidate_id=FROZEN_CANDIDATE_ID,
            profile=profile,
            scenario_events=events,
            block_contract=block_contract,
            config=config,
            maximum_mini_equivalent=maximum_mini_equivalent,
            micro_costs=micro_costs,
            roles=("DESIGN", "HELD_OUT_DEVELOPMENT"),
        )
        evaluated[arm_id] = {
            "arm_id": arm_id,
            "source_event_count": len(events[CostStress.BASE.value]),
            "normal_stressed_decision_identity": True,
            "event_path_hashes": {
                scenario.value: stable_hash(
                    [row.to_dict() for row in events[scenario.value]]
                )
                for scenario in (CostStress.BASE, CostStress.STRESS_1_5X)
            },
            "exact_episode_count": int(full["exact_episode_count"]),
            "evaluation": _compact_roles(full["roles"]),
            "evaluation_hash": stable_hash(full["roles"]),
        }

    primary_roles = evaluated["PRIMARY"]["evaluation"]
    if stable_hash(primary_roles) != stable_hash(
        _compact_roles(source_candidate["selected_result"]["roles"])
    ):
        raise AutonomousEventTimeMatchedControlsError(
            "primary replay no longer matches the frozen safety artifact"
        )
    deltas = {
        control_id: _paired_summary_delta(
            primary_roles, evaluated[control_id]["evaluation"]
        )
        for control_id in CONTROL_IDS
    }
    verdict = _control_verdict(primary_roles, evaluated)
    core = {
        "schema": SCHEMA,
        "status": STATUS,
        "branch_id": "EVENT_TIME_FROZEN_CANDIDATE_MATCHED_CONTROLS",
        "frozen_candidate": {
            "candidate_id": FROZEN_CANDIDATE_ID,
            "specification_hash": FROZEN_SPECIFICATION_HASH,
            "signal_path_hash": FROZEN_SIGNAL_PATH_HASH,
            "profile": asdict(profile),
            "profile_hash": stable_hash(asdict(profile)),
            "source_policy_id": FROZEN_SOURCE_POLICY_ID,
            "source_composite_result_hash": source["result_hash"],
            "signal_logic_retuned": False,
            "profile_retuned": False,
        },
        "official_rule_snapshot": rule_receipt,
        "evaluation_contract": block_contract["receipt"],
        "control_contract": {
            "control_ids": list(CONTROL_IDS),
            "direction_flip": (
                "same events/times/exposure; gross direction and intratrade "
                "excursion are inverted while original costs are retained"
            ),
            "direction_permutation": (
                "same signal times, session counts, holding and long/short count "
                "within each session; directions rotate deterministically"
            ),
            "timing_grid": (
                "same session/event count/direction sequence/holding; entries use "
                "a deterministic non-overlapping 60-minute session grid"
            ),
            "control_signal_audits": control_audits,
            "no_outcome_tuning": True,
            "no_new_data": True,
        },
        "arms": evaluated,
        "primary_minus_control": deltas,
        "control_verdict": verdict,
        "evidence_role": "VIEWED_DEVELOPMENT_CONTROL_DIAGNOSTIC_ONLY",
        "evidence_tier": "E",
        "promotion_status": None,
        "counts": {
            "arm_count": len(evaluated),
            "control_count": len(CONTROL_IDS),
            "source_event_count": len(primary_events[CostStress.BASE.value]),
            "exact_episode_count": sum(
                int(row["exact_episode_count"]) for row in evaluated.values()
            ),
            "authoritative_promotion_count": 0,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "registry_writes": 0,
            "database_writes": 0,
        },
        "next_action": (
            "FREEZE_CONTROL_DISTINCT_CANDIDATE_FOR_ONE_FRESH_CONFIRMATION"
            if verdict == "EVENT_TIME_CONTROL_DISTINCT_DEVELOPMENT_ONLY"
            else "CLOSE_EVENT_TIME_CANDIDATE_AND_REALLOCATE_EXPLORATION"
        ),
    }
    return {**core, "result_hash": stable_hash(core)}


def event_time_matched_controls_worker(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Pickle-safe adapter for an economic worker process."""

    return run_event_time_matched_controls(
        str(payload["root"]),
        source_composite_path=str(
            payload.get("source_composite_path", DEFAULT_SOURCE_COMPOSITE)
        ),
        rule_snapshot_path=str(
            payload.get("rule_snapshot_path", DEFAULT_RULE_SNAPSHOT)
        ),
    )


def _matched_signal_controls(
    signals: Sequence[V71Signal], states: Any
) -> tuple[dict[str, tuple[V71Signal, ...]], dict[str, Any]]:
    ordered = tuple(sorted(signals, key=lambda row: (row.decision_ns, row.contract)))
    grouped: dict[tuple[str, str], list[V71Signal]] = {}
    for row in ordered:
        grouped.setdefault((row.contract, row.session_day), []).append(row)

    permuted: list[V71Signal] = []
    timing: list[V71Signal] = []
    changed_direction = 0
    changed_timing = 0
    for (contract, session_day), session_signals in sorted(grouped.items()):
        session_signals.sort(key=lambda row: row.decision_ns)
        sides = [row.side for row in session_signals]
        # A singleton cannot be permuted without changing that session's frozen
        # directional exposure, so it remains unchanged.  Multi-event sessions
        # use a one-position rotation and therefore preserve the exact side
        # multiset within the session.
        rotated = sides[1:] + sides[:1] if len(sides) > 1 else list(sides)
        for row, side in zip(session_signals, rotated, strict=True):
            changed_direction += int(side != row.side)
            permuted.append(
                replace(
                    row,
                    side=int(side),
                    feature_snapshot_hash=stable_hash(
                        {
                            "control": "SESSION_EXPOSURE_MATCHED_DIRECTION_PERMUTATION",
                            "source": row.feature_snapshot_hash,
                            "side": int(side),
                        }
                    ),
                )
            )

        eligible = states.loc[
            (states["contract"].astype(str) == contract)
            & (states["session_day"].astype(str) == session_day)
            & states["executable_60"].astype(bool)
        ].sort_values("minute_start_ns", kind="stable")
        count = len(session_signals)
        stride = 60
        required = (count - 1) * stride + 1
        if len(eligible) < required:
            raise AutonomousEventTimeMatchedControlsError(
                f"session cannot support exposure-matched timing grid: {session_day}"
            )
        slack = len(eligible) - required
        seed = int.from_bytes(
            hashlib.sha256(
                f"{FROZEN_CANDIDATE_ID}:{contract}:{session_day}".encode()
            ).digest()[:8],
            "big",
        )
        offset = seed % (slack + 1)
        selected = [eligible.iloc[offset + index * stride] for index in range(count)]
        for source, row in zip(session_signals, selected, strict=True):
            decision = int(row["availability_ns"])
            entry = int(row["entry_ns_60"])
            exit_ns = int(row["exit_ns_60"])
            changed_timing += int(entry != source.entry_minute_start_ns)
            timing.append(
                V71Signal(
                    candidate_id=source.candidate_id,
                    family_id=source.family_id,
                    motif=source.motif,
                    response_policy=source.response_policy,
                    holding_minutes=source.holding_minutes,
                    calendar_year=int(row["calendar_year"]),
                    session_day=session_day,
                    source_position=int(row.name),
                    availability_ns=decision,
                    decision_ns=decision,
                    entry_minute_start_ns=entry,
                    exit_minute_start_ns=exit_ns,
                    side=source.side,
                    contract=contract,
                    feature_snapshot_hash=stable_hash(
                        {
                            "control": "SESSION_MATCHED_TIMING_GRID",
                            "contract": contract,
                            "session_day": session_day,
                            "source_signal": source.feature_snapshot_hash,
                            "decision_ns": decision,
                        }
                    ),
                )
            )
    permuted_rows = tuple(sorted(permuted, key=lambda row: row.decision_ns))
    timing_rows = tuple(sorted(timing, key=lambda row: row.decision_ns))
    source_session_counts = _session_counts(ordered)
    for rows in (permuted_rows, timing_rows):
        if _session_counts(rows) != source_session_counts or len(rows) != len(ordered):
            raise AutonomousEventTimeMatchedControlsError(
                "matched-control session/exposure count drift"
            )
    if changed_direction == 0 or changed_timing == 0:
        raise AutonomousEventTimeMatchedControlsError(
            "matched control did not alter its intended field"
        )
    return {
        "SESSION_EXPOSURE_MATCHED_DIRECTION_PERMUTATION": permuted_rows,
        "SESSION_MATCHED_TIMING_GRID": timing_rows,
    }, {
        "source_signal_count": len(ordered),
        "session_count": len(source_session_counts),
        "source_session_count_hash": stable_hash(source_session_counts),
        "direction_permutation_changed_count": changed_direction,
        "timing_grid_changed_count": changed_timing,
        "session_and_exposure_count_matched": True,
    }


def _direction_flip_event(
    event: TradePathEvent, control_id: str
) -> TradePathEvent:
    cost = float(event.gross_pnl - event.net_pnl)
    raw_worst = float(event.worst_unrealized_pnl + cost)
    raw_best = float(event.best_unrealized_pnl + cost)
    gross = -float(event.gross_pnl)
    net = gross - cost
    base, scenario = event.event_id.rsplit(":", 1)
    return replace(
        event,
        event_id=f"{base}:{control_id}:{scenario}",
        gross_pnl=gross,
        net_pnl=net,
        worst_unrealized_pnl=float(min(-raw_best - cost, net, 0.0)),
        best_unrealized_pnl=float(max(-raw_worst - cost, net, 0.0)),
    )


def _compact_roles(roles: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "episode_count",
        "pass_count",
        "pass_rate",
        "mll_breach_count",
        "mll_breach_rate",
        "net_total_usd",
        "net_median_usd",
        "target_progress_p25",
        "target_progress_median",
        "minimum_mll_buffer_usd",
        "consistency_compliant_pass_count",
        "all_passing_paths_consistency_compliant",
        "median_days_to_target",
        "accepted_event_count",
        "rejected_event_count",
        "acceptance_rate",
        "by_block",
        "episode_path_hash",
        "decision_path_hash",
    )
    return {
        role: {
            str(horizon): {
                scenario: {
                    field: summary[field]
                    for field in fields
                }
                for scenario, summary in scenarios.items()
            }
            for horizon, scenarios in horizons.items()
        }
        for role, horizons in roles.items()
    }


def _paired_summary_delta(
    primary: Mapping[str, Any], control: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for role in primary:
        output[role] = {}
        for horizon in primary[role]:
            output[role][horizon] = {}
            for scenario in primary[role][horizon]:
                left = primary[role][horizon][scenario]
                right = control[role][horizon][scenario]
                output[role][horizon][scenario] = {
                    "pass_count_delta": int(left["pass_count"])
                    - int(right["pass_count"]),
                    "mll_breach_count_delta": int(left["mll_breach_count"])
                    - int(right["mll_breach_count"]),
                    "net_total_usd_delta": float(left["net_total_usd"])
                    - float(right["net_total_usd"]),
                    "target_progress_median_delta": float(
                        left["target_progress_median"]
                    )
                    - float(right["target_progress_median"]),
                }
    return output


def _control_verdict(
    primary: Mapping[str, Any], arms: Mapping[str, Mapping[str, Any]]
) -> str:
    heldout = primary["HELD_OUT_DEVELOPMENT"]
    primary_10 = heldout["10"][CostStress.STRESS_1_5X.value]
    positive_blocks = sum(
        int(summary["pass_count"]) > 0
        for summary in primary_10["by_block"].values()
    )
    controls = [
        arm["evaluation"]["HELD_OUT_DEVELOPMENT"]["10"]
        [CostStress.STRESS_1_5X.value]
        for key, arm in arms.items()
        if key != "PRIMARY"
    ]
    distinct = (
        int(primary_10["pass_count"]) > 0
        and float(primary_10["net_total_usd"]) > 0.0
        and float(primary_10["mll_breach_rate"]) <= 0.10
        and positive_blocks >= 2
        and all(
            int(primary_10["pass_count"]) > int(row["pass_count"])
            and float(primary_10["net_total_usd"])
            > float(row["net_total_usd"])
            for row in controls
        )
    )
    return (
        "EVENT_TIME_CONTROL_DISTINCT_DEVELOPMENT_ONLY"
        if distinct
        else "EVENT_TIME_MATCHED_CONTROLS_NOT_DISTINCT"
    )


def _load_source_composite(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    source = dict(payload.get("event_time_safety_composite") or payload)
    if source.get("schema") != SOURCE_SCHEMA or source.get("status") != (
        "COMPLETE_RECONCILED_EVENT_TIME_SAFETY_SHARDS"
    ):
        raise AutonomousEventTimeMatchedControlsError(
            "source safety composite schema/status drift"
        )
    expected = stable_hash(
        {key: value for key, value in source.items() if key != "result_hash"}
    )
    if str(source.get("result_hash")) != expected:
        raise AutonomousEventTimeMatchedControlsError(
            "source safety composite result hash drift"
        )
    return source


def _freeze_source_candidate(source: Mapping[str, Any]) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in source.get("candidate_results", ())
        if str(row.get("candidate_id")) == FROZEN_CANDIDATE_ID
    ]
    if len(matches) != 1:
        raise AutonomousEventTimeMatchedControlsError(
            "source candidate inventory is not unique"
        )
    row = matches[0]
    selected = dict(row.get("selected_result") or {})
    metadata = dict(row.get("source_candidate_metadata") or {})
    if (
        row.get("selected_profile_id") != FROZEN_PROFILE_ID
        or selected.get("policy_id") != FROZEN_SOURCE_POLICY_ID
        or metadata.get("specification_hash") != FROZEN_SPECIFICATION_HASH
        or metadata.get("signal_path_hash") != FROZEN_SIGNAL_PATH_HASH
    ):
        raise AutonomousEventTimeMatchedControlsError(
            "source candidate/profile freeze drift"
        )
    return row


def _frozen_profile() -> EventTimeSafetyProfile:
    matches = [
        row
        for row in frozen_event_time_safety_profiles()
        if row.profile_id == FROZEN_PROFILE_ID
    ]
    if len(matches) != 1:
        raise AutonomousEventTimeMatchedControlsError(
            "frozen event-time safety profile is unavailable"
        )
    return matches[0]


def _session_counts(signals: Sequence[V71Signal]) -> dict[str, int]:
    output: dict[str, int] = {}
    for row in signals:
        key = f"{row.contract}:{row.session_day}"
        output[key] = output.get(key, 0) + 1
    return dict(sorted(output.items()))


def _same_minute_identity(left: Any, right: Any) -> bool:
    columns = ("minute_start_ns", "availability_ns", "contract")
    return len(left) == len(right) and all(
        np.array_equal(left[column].to_numpy(), right[column].to_numpy())
        for column in columns
    )


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AutonomousEventTimeMatchedControlsError(
            f"path escapes project root: {value}"
        ) from exc
    return resolved


__all__ = [
    "AutonomousEventTimeMatchedControlsError",
    "CONTROL_IDS",
    "FROZEN_CANDIDATE_ID",
    "FROZEN_PROFILE_ID",
    "SCHEMA",
    "STATUS",
    "event_time_matched_controls_worker",
    "run_event_time_matched_controls",
]
