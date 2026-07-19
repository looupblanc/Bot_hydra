"""Pure runtime helpers for the manifest-driven FAST-PASS 0029 campaign.

The module intentionally contains no coordinator and performs no signal
discovery during sprint replay.  It consumes immutable exact causal sleeve
replays, resizes their already-observed executable trajectories on a frozen
integer contract frontier, and evaluates them on the candidate-independent
union account calendar.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

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
from hydra.economic_evolution.schema import stable_hash
from hydra.portfolio.marginal_contribution_builder import (
    GovernorProfile,
    SprintMetrics,
)
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15
from hydra.production.causal_target_velocity_runtime import (
    _block_map,
    _candidate_evaluation_days,
    _candidate_failure_boundary,
    _evaluate_candidate,
    _stage1_event_evidence_record,
)
from hydra.propfirm.combine_episode import CombineTerminal, TradePathEvent
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)
from hydra.research.causal_target_velocity import (
    HazardOutcome,
    direction_flipped_intents,
    matched_random_intents,
    observe_outcomes,
    realized_behavioral_fingerprint,
    screen_result,
)


class FastPassHelperError(RuntimeError):
    """A helper contract is incomplete, inconsistent, or non-causal."""


_SPRINT_REPLAYS: dict[str, Any] = {}
_SPRINT_CALENDAR: tuple[int, ...] = ()
_SPRINT_STARTS: dict[int, tuple[tuple[int, str], ...]] = {}
_SPRINT_CENSORED_DAYS: dict[str, frozenset[int]] = {}
_SPRINT_GOVERNORS: tuple[dict[str, Any], ...] = ()
_SPRINT_ACCOUNT_RULES = Topstep150KConfig()
_SPRINT_MAXIMUM_MINI_EQUIVALENT = 15.0
_SPRINT_BLOCK_ROLES: dict[str, tuple[str, ...]] = {
    "DESIGN": ("B1", "B2"),
    "HELD_OUT_DEVELOPMENT": ("B3", "B4"),
}


def _fast_screen_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Level-0 event screens: causal and duplicate evidence only."""

    return [
        _candidate_failure_boundary(_fast_screen_one, payload, "STAGE_1")
        for payload in payloads
    ]


def _fast_screen_one(payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate, calibrated, matrix, _intents, events = _evaluate_candidate(payload)
    days = _candidate_evaluation_days(candidate, matrix)
    screen = screen_result(
        calibrated,
        events,
        eligible_session_days=days,
        block_by_session_day=_block_map(matrix, _worker_blocks()),
    )
    block_economics = _event_block_economics(
        [row.to_dict() for row in events], _worker_blocks()
    )
    return {
        "schema": "hydra_fast_pass_stage1_row_v1",
        "candidate_id": candidate.candidate_id,
        "candidate": candidate.payload,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "behavioral_fingerprint": candidate.behavioral_fingerprint,
        "realized_behavioral_fingerprint": realized_behavioral_fingerprint(events),
        "calibration": asdict(calibrated),
        "screen": screen.to_dict(),
        "block_economics": block_economics,
        "event_contract": {
            "emitted": len(events),
            "completed": screen.completed_event_count,
            "censored_future_coverage": screen.censored_event_count,
            "future_coverage_suppressed_signals": 0,
        },
        "control_level": 0,
        "hard_causality_defect_count": 0,
        "_event_evidence": [
            _stage1_event_evidence_record(row.to_dict()) for row in events
        ],
        "row_hash": stable_hash(
            {"candidate": candidate.payload, "screen": screen.to_dict()}
        ),
    }


def _level1_control_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Progressive controls for the selected top fraction only."""

    return [
        _candidate_failure_boundary(_level1_control_one, payload, "CONTROL_LEVEL_1")
        for payload in payloads
    ]


def _level1_control_one(payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate, calibrated, matrix, intents, events = _evaluate_candidate(payload)
    days = _candidate_evaluation_days(candidate, matrix)
    blocks = _block_map(matrix, _worker_blocks())
    observed = screen_result(
        calibrated, events, eligible_session_days=days, block_by_session_day=blocks
    )
    # Importing the worker period through the owning module preserves the exact
    # process-initializer contract rather than copying mutable worker globals.
    from hydra.production import causal_target_velocity_runtime as causal_runtime

    period = causal_runtime._WORKER_PERIOD
    if period is None:
        raise FastPassHelperError("causal worker period is not initialized")
    _, evaluation_start, evaluation_end = period
    seed = int(candidate.structural_fingerprint[:16], 16) % (2**32)
    random_intents = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
        seed=seed,
    )
    session_intents = matched_random_intents(
        calibrated,
        matrix,
        intents,
        evaluation_start_ns=evaluation_start,
        evaluation_end_exclusive_ns=evaluation_end,
        seed=seed ^ 0x5E5510A7,
    )
    control_events = {
        "RANDOM_EVENT_TIMING_MATCHED": observe_outcomes(
            calibrated, matrix, random_intents
        ),
        "SESSION_MATCHED_NULL": observe_outcomes(
            calibrated, matrix, session_intents
        ),
        "DIRECTION_FLIPPED": observe_outcomes(
            calibrated, matrix, direction_flipped_intents(intents)
        ),
    }
    controls: dict[str, Any] = {}
    for name, values in control_events.items():
        summary = screen_result(
            calibrated,
            values,
            eligible_session_days=days,
            block_by_session_day=blocks,
        )
        controls[name] = {
            "summary": summary.to_dict(),
            "deltas": {
                "favorable_before_adverse_rate": float(
                    observed.favorable_before_adverse_rate
                    - summary.favorable_before_adverse_rate
                ),
                "stressed_net_pnl": float(
                    observed.stressed_net_pnl - summary.stressed_net_pnl
                ),
                "cost_adjusted_target_velocity": float(
                    observed.cost_adjusted_target_velocity
                    - summary.cost_adjusted_target_velocity
                ),
            },
            "opportunity_count_matched": len(values) == len(events),
        }
    return {
        "schema": "hydra_fast_pass_level1_controls_v1",
        "candidate_id": candidate.candidate_id,
        "candidate_fingerprint": candidate.structural_fingerprint,
        "control_level": 1,
        "observed": observed.to_dict(),
        "controls": controls,
        "status": "CONTROL_LEVEL_1_COMPLETE",
        "control_hash": stable_hash(controls),
    }


def _worker_blocks() -> tuple[dict[str, Any], ...]:
    from hydra.production import causal_target_velocity_runtime as causal_runtime

    return tuple(dict(row) for row in causal_runtime._WORKER_BLOCKS)


def _stage1_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    screen = dict(row.get("screen") or {})
    candidate = dict(row.get("candidate") or {})
    five_day_contribution = float(
        screen.get("stressed_target_contribution_per_20_sessions", 0.0)
    ) / 4.0
    design = _design_block_economics(row)
    design_stressed = sum(
        float(value.get("stressed_net", 0.0)) for value in design.values()
    )
    design_completed = sum(
        int(value.get("completed_event_count", 0)) for value in design.values()
    )
    return (
        -int(sum(float(value.get("stressed_net", 0.0)) > 0.0 for value in design.values())),
        -design_stressed,
        -design_completed,
        -five_day_contribution,
        -float(screen.get("cost_adjusted_target_velocity", 0.0)),
        -float(screen.get("favorable_before_adverse_rate", 0.0)),
        -float(screen.get("independent_events_per_20_sessions", 0.0)),
        float(screen.get("day_concentration", 1.0)),
        str(candidate.get("market", "")),
        str(row.get("candidate_id", "")),
    )


def _design_block_economics(
    value: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    blocks = dict(value.get("block_economics") or {})
    return {
        block: dict(blocks[block])
        for block in ("B1", "B2")
        if block in blocks
    }


def _proposal_qd_cell(row: Mapping[str, Any]) -> str:
    candidate = dict(row.get("candidate") or {})
    screen = dict(row.get("screen") or {})
    density = _bucket(
        float(screen.get("independent_events_per_20_sessions", 0.0)),
        (1.0, 3.0, 8.0),
        ("RARE", "LOW", "MEDIUM", "HIGH"),
    )
    payload = {
        "market": candidate.get("market"),
        "session": candidate.get("session_code"),
        "timeframe": candidate.get("timeframe"),
        "holding": candidate.get("horizon"),
        "mechanism": candidate.get("mechanism"),
        "direction": candidate.get("direction_rule"),
        "opportunity_density": density,
        "risk": candidate.get("risk_level"),
    }
    return f"proposal_qd_{stable_hash(payload)[:24]}"


def _realized_qd_cell(
    exact_row: Mapping[str, Any], screen: Mapping[str, Any]
) -> str:
    candidate = dict(exact_row.get("candidate") or {})
    completed = int(exact_row.get("completed_event_count", 0))
    eligible_days = len(exact_row.get("eligible_session_days") or ())
    per_20 = completed * 20.0 / max(eligible_days, 1)
    minimum_buffer = float(
        (exact_row.get("stressed") or {}).get("minimum_mll_buffer", 0.0)
    )
    payload = {
        "market": candidate.get("market"),
        "session": candidate.get("session_code"),
        "timeframe": candidate.get("timeframe"),
        "holding": candidate.get("horizon"),
        "mechanism": candidate.get("mechanism"),
        "direction": candidate.get("direction_rule"),
        "opportunity_density": _bucket(
            per_20, (1.0, 3.0, 8.0), ("RARE", "LOW", "MEDIUM", "HIGH")
        ),
        "risk": candidate.get("risk_level"),
        "trade_frequency": _bucket(
            per_20, (2.0, 6.0, 12.0), ("VERY_LOW", "LOW", "MEDIUM", "HIGH")
        ),
        "mll_consumption": _bucket(
            4_500.0 - minimum_buffer,
            (250.0, 750.0, 1_500.0),
            ("MINIMAL", "LOW", "MEDIUM", "HIGH"),
        ),
        "screen_velocity_sign": math.copysign(
            1.0, float(screen.get("cost_adjusted_target_velocity", 0.0))
        ),
    }
    return f"realized_qd_{stable_hash(payload)[:24]}"


def _quality_multiplier(value: Mapping[str, Any]) -> float:
    """Frozen executable quality tier; zero is reserved for explicit rejection."""

    screen = dict(value.get("screen") or value)
    design = _design_block_economics(value)
    favorable = float(screen.get("favorable_before_adverse_rate", 0.0))
    velocity = float(screen.get("cost_adjusted_target_velocity", 0.0))
    density = float(screen.get("independent_events_per_20_sessions", 0.0))
    stressed = (
        sum(float(row.get("stressed_net", 0.0)) for row in design.values())
        if design
        else float(screen.get("stressed_net_pnl", 0.0))
    )
    if stressed > 0.0 and favorable >= 0.60 and velocity > 0.0 and density >= 3.0:
        return 2.0
    if stressed > 0.0 and favorable >= 0.55 and velocity > 0.0:
        return 1.5
    if stressed > 0.0 and favorable >= 0.50:
        return 1.0
    return 0.5


def _bank_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        -float(row.get("quality_multiplier", 0.0)),
        -int(row.get("positive_stressed_block_count", 0)),
        -float(row.get("stressed_full_net", 0.0)),
        -float(row.get("normal_full_net", 0.0)),
        -float(row.get("minimum_stressed_mll_buffer", 0.0)),
        -int(row.get("completed_event_count", 0)),
        str(row.get("candidate_id", "")),
    )


def _event_block_economics(
    events: Sequence[Mapping[str, Any]],
    blocks: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for raw_block in blocks:
        block = dict(raw_block)
        block_id = str(block["block_id"])
        start = _date_value(str(block["start"]))
        end = _date_value(str(block["end"]))
        selected = [
            row
            for row in events
            if start <= _epoch_date(int(row["session_day"])) <= end
        ]
        complete = [
            row
            for row in selected
            if str(row.get("outcome"))
            != HazardOutcome.CENSORED_FUTURE_COVERAGE.value
        ]
        output[block_id] = {
            "event_count": len(selected),
            "completed_event_count": len(complete),
            "censored_event_count": len(selected) - len(complete),
            "favorable_first_count": sum(
                str(row.get("outcome")) == HazardOutcome.FAVORABLE_FIRST.value
                for row in complete
            ),
            "normal_net": float(
                sum(float(row.get("normal_net_pnl") or 0.0) for row in complete)
            ),
            "stressed_net": float(
                sum(float(row.get("stressed_net_pnl") or 0.0) for row in complete)
            ),
        }
    return output


def _exact_hashes(row: Mapping[str, Any]) -> dict[str, str]:
    names = (
        "decision_hash",
        "normal_event_hash",
        "stressed_event_hash",
        "normal_trajectory_hash",
        "stressed_trajectory_hash",
        "fill_policy_hash",
    )
    values = {name: str(row.get(name, "")) for name in names}
    if any(not value for value in values.values()):
        raise FastPassHelperError("exact replay hash inventory is incomplete")
    return values


def _read_event_receipt(receipt: Mapping[str, Any]) -> list[dict[str, Any]]:
    base = Path(str(receipt.get("base_dir", ""))).resolve()
    relative = Path(str(receipt.get("relative_path", "")))
    if relative.is_absolute() or ".." in relative.parts:
        raise FastPassHelperError("event receipt path escapes its base")
    path = (base / relative).resolve()
    if base not in path.parents:
        raise FastPassHelperError("event receipt path escapes its base")
    compressed = path.read_bytes()
    if hashlib.sha256(compressed).hexdigest() != str(receipt.get("sha256", "")):
        raise FastPassHelperError("compressed event receipt checksum drift")
    try:
        payload = gzip.decompress(compressed)
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise FastPassHelperError("event receipt is not valid gzip") from exc
    expected_plain = receipt.get("uncompressed_sha256")
    if expected_plain and hashlib.sha256(payload).hexdigest() != str(expected_plain):
        raise FastPassHelperError("uncompressed event receipt checksum drift")
    values = _decode_jsonl(payload, path)
    if len(values) != int(receipt.get("record_count", -1)):
        raise FastPassHelperError("event receipt record-count drift")
    return values


def _governor_profiles(manifest: Mapping[str, Any]) -> tuple[GovernorProfile, ...]:
    contract = dict(manifest["risk_governor"])
    frozen = list(contract.get("frozen_profiles") or ())
    if not frozen:
        raise FastPassHelperError("governor profiles must be explicitly frozen")
    if str(contract.get("frozen_profiles_hash") or "") != stable_hash(frozen):
        raise FastPassHelperError("frozen governor profile hash drift")
    if (
        contract.get("executable_quality_quantization")
        != "CAUSAL_EVENT_IDENTITY_HASH_INTEGER_MICRO_QUANTIZATION_V1"
    ):
        raise FastPassHelperError("governor quality quantization is not frozen")
    try:
        profiles = tuple(
            GovernorProfile(
                profile_id=str(row["profile_id"]),
                signal_quality_tiers=tuple(
                    float(value) for value in row["signal_quality_tiers"]
                ),
                open_risk_ceiling_fraction=float(
                    row["open_risk_ceiling_fraction"]
                ),
                daily_loss_budget_fraction=float(row["daily_loss_budget_fraction"]),
                daily_profit_lock_fraction=float(row["daily_profit_lock_fraction"]),
                maximum_concurrent_sleeves=int(row["maximum_concurrent_sleeves"]),
                target_protection_fraction=float(row["target_protection_fraction"]),
                same_instrument_conflict_policy=str(
                    row["same_instrument_conflict_policy"]
                ),
            )
            for row in frozen
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise FastPassHelperError("frozen governor profile is malformed") from exc
    expected_tiers = (0.0, 0.5, 1.0, 1.5, 2.0)
    if (
        len(profiles) != 4
        or len({row.profile_id for row in profiles}) != len(profiles)
        or {row.maximum_concurrent_sleeves for row in profiles} != {1, 2, 3, 4}
        or any(
            row.signal_quality_tiers != expected_tiers
            or row.same_instrument_conflict_policy != "priority"
            or not 0.0 < row.open_risk_ceiling_fraction <= 1.0
            or not 0.0 < row.daily_loss_budget_fraction <= 1.0
            or not 0.0 < row.daily_profit_lock_fraction <= 1.0
            or not 0.0 < row.target_protection_fraction < 1.0
            for row in profiles
        )
    ):
        raise FastPassHelperError("frozen governor profile frontier drift")
    return profiles


def _book_spec(
    *,
    policy_id: str,
    members: Sequence[str],
    bank_by_id: Mapping[str, Mapping[str, Any]],
    profile: GovernorProfile,
    role: str,
    wave: int,
    predecessor_id: str | None,
    force_quality: float | None = None,
) -> dict[str, Any]:
    component_ids = tuple(str(value) for value in members)
    if not component_ids or len(set(component_ids)) != len(component_ids):
        raise FastPassHelperError("book membership must be non-empty and unique")
    missing = sorted(set(component_ids) - set(bank_by_id))
    if missing:
        raise FastPassHelperError(f"book references unknown sleeves: {missing}")
    qualities = {
        value: float(
            force_quality
            if force_quality is not None
            else bank_by_id[value]["quality_multiplier"]
        )
        for value in component_ids
    }
    if any(value not in profile.signal_quality_tiers for value in qualities.values()):
        raise FastPassHelperError("book quality tier escaped frozen governor profile")
    value: dict[str, Any] = {
        "schema": "hydra_fast_pass_sprint_policy_v1",
        "policy_id": str(policy_id),
        "component_ids": list(component_ids),
        "quality_multipliers": qualities,
        "governor_profile_id": profile.profile_id,
        "governor_profile": asdict(profile),
        "policy_role": str(role),
        "wave": int(wave),
        "predecessor_policy_id": predecessor_id,
        "signal_recomputation_performed": False,
    }
    value["policy_spec_hash"] = stable_hash(value)
    return value


def _book_component_key(summary: Mapping[str, Mapping[str, Any]]) -> tuple[Any, ...]:
    five = dict(summary.get("5") or {})
    ten = dict(summary.get("10") or {})
    twenty = dict(summary.get("20") or {})
    return (
        float(five.get("pass_rate", 0.0)),
        float(ten.get("pass_rate", 0.0)),
        float(five.get("target_progress_p25", 0.0)),
        -float(five.get("mll_breach_rate", 1.0)),
        float(five.get("net_total", 0.0)),
        float(twenty.get("pass_rate", 0.0)),
    )


def _book_result_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    stressed = dict(
        (
            (row.get("summaries_by_role") or {}).get("HELD_OUT_DEVELOPMENT")
            or {}
        ).get("STRESSED_1_5X")
        or (row.get("summaries") or {}).get("STRESSED_1_5X")
        or {}
    )
    five = dict(stressed.get("5") or {})
    ten = dict(stressed.get("10") or {})
    return (
        -float(five.get("pass_rate", 0.0)),
        -float(ten.get("pass_rate", 0.0)),
        -float(five.get("target_progress_p25", 0.0)),
        float(five.get("mll_breach_rate", 1.0)),
        -float(five.get("net_total", 0.0)),
        str(row.get("policy_id", "")),
    )


def _sprint_metrics(summary: Mapping[str, Mapping[str, Any]]) -> SprintMetrics:
    five = dict(summary.get("5") or {})
    ten = dict(summary.get("10") or {})
    twenty = dict(summary.get("20") or {})
    return SprintMetrics(
        pass_rate_5d=float(five.get("pass_rate", 0.0)),
        pass_rate_10d=float(ten.get("pass_rate", 0.0)),
        pass_rate_20d=float(twenty.get("pass_rate", 0.0)),
        p25_target_progress=float(five.get("target_progress_p25", 0.0)),
        mll_survival_rate=float(
            1.0
            - max(
                float(five.get("mll_breach_rate", 0.0)),
                float(ten.get("mll_breach_rate", 0.0)),
                float(twenty.get("mll_breach_rate", 0.0)),
            )
        ),
        consistency_rate=float(
            min(
                float(five.get("consistency_rate", 0.0)),
                float(ten.get("consistency_rate", 0.0)),
                float(twenty.get("consistency_rate", 0.0)),
            )
        ),
        stressed_net=float(five.get("net_total", 0.0)),
    )


def _control_delta(
    source: Mapping[str, Any], control: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "source_policy_id": str(source["policy_id"]),
        "control_policy_id": str(control["policy_id"]),
        "by_scenario_horizon": {},
    }
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        output["by_scenario_horizon"][scenario] = {}
        for horizon in ("5", "10", "20"):
            left = dict(source["summaries"][scenario][horizon])
            right = dict(control["summaries"][scenario][horizon])
            output["by_scenario_horizon"][scenario][horizon] = {
                "pass_rate_delta": float(left["pass_rate"] - right["pass_rate"]),
                "target_progress_p25_delta": float(
                    left["target_progress_p25"] - right["target_progress_p25"]
                ),
                "target_progress_median_delta": float(
                    left["target_progress_median"]
                    - right["target_progress_median"]
                ),
                "net_total_delta": float(left["net_total"] - right["net_total"]),
                "mll_breach_rate_delta": float(
                    left["mll_breach_rate"] - right["mll_breach_rate"]
                ),
                "consistency_rate_delta": float(
                    left["consistency_rate"] - right["consistency_rate"]
                ),
                "maximum_mini_equivalent_mean_delta": float(
                    left.get("maximum_mini_equivalent_mean", 0.0)
                    - right.get("maximum_mini_equivalent_mean", 0.0)
                ),
                "mean_daily_contract_utilization_delta": float(
                    left.get("mean_daily_contract_utilization", 0.0)
                    - right.get("mean_daily_contract_utilization", 0.0)
                ),
                "accepted_event_count_delta": int(
                    left.get("accepted_event_count", 0)
                    - right.get("accepted_event_count", 0)
                ),
            }
    output["delta_hash"] = stable_hash(output)
    return output


def _install_sprint_globals(
    *,
    replays: Mapping[str, Any],
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    governors: Sequence[Mapping[str, Any]],
    account_rule_snapshot: Mapping[str, Any] | None = None,
    block_roles: Mapping[str, Sequence[str]] | None = None,
) -> None:
    global _SPRINT_ACCOUNT_RULES
    global _SPRINT_CALENDAR
    global _SPRINT_CENSORED_DAYS
    global _SPRINT_GOVERNORS
    global _SPRINT_MAXIMUM_MINI_EQUIVALENT
    global _SPRINT_REPLAYS
    global _SPRINT_STARTS
    global _SPRINT_BLOCK_ROLES

    ordered = tuple(sorted({int(value) for value in calendar}))
    if len(ordered) != len(calendar):
        raise FastPassHelperError("sprint account calendar must be sorted and unique")
    start_map = {
        int(horizon): tuple((int(day), str(block)) for day, block in values)
        for horizon, values in starts.items()
    }
    if set(start_map) != {5, 10, 20}:
        raise FastPassHelperError("sprint horizons must be exactly 5/10/20")
    index = {day: position for position, day in enumerate(ordered)}
    for horizon, values in start_map.items():
        for day, _block in values:
            if day not in index or index[day] + horizon > len(ordered):
                raise FastPassHelperError("sprint start lacks full account coverage")
    replay_map = {str(key): value for key, value in replays.items()}
    calendar_days = set(ordered)
    censored: dict[str, frozenset[int]] = {}
    for component_id, replay in replay_map.items():
        if not hasattr(replay, "eligible_session_days"):
            raise FastPassHelperError(
                f"sprint replay lacks eligible-session provenance: {component_id}"
            )
        eligible_days = {int(value) for value in replay.eligible_session_days}
        missing_eligible_days = calendar_days - eligible_days
        future_coverage_days = {
            int(row.session_day)
            for row in replay.events
            if str(row.outcome) == HazardOutcome.CENSORED_FUTURE_COVERAGE.value
        }
        censored[component_id] = frozenset(
            missing_eligible_days | future_coverage_days
        )
    if account_rule_snapshot is None:
        raise FastPassHelperError("official account rule snapshot is required")
    snapshot = dict(account_rule_snapshot)
    official = official_rule_snapshot_2026_07_15()
    try:
        snapshot_matches = bool(
            snapshot.get("rule_snapshot_version") == official.rule_version
            and snapshot.get("rule_snapshot_hash") == official.fingerprint
            and snapshot.get("official_snapshot") == official.to_dict()
            and float(snapshot.get("account_size_usd", -1.0))
            == official.account_size
            and float(snapshot.get("profit_target_usd", -1.0))
            == official.combine_profit_target
            and float(snapshot.get("maximum_loss_limit_usd", -1.0))
            == official.maximum_loss_limit
            and float(snapshot.get("best_day_consistency_fraction", -1.0))
            == official.combine_consistency_limit
            and int(snapshot.get("maximum_mini_contracts", -1))
            == official.combine_maximum_mini_equivalent
            and int(snapshot.get("maximum_micro_contracts", -1))
            == official.combine_maximum_micros
        )
    except (TypeError, ValueError):
        snapshot_matches = False
    if not snapshot_matches:
        raise FastPassHelperError("official account rule snapshot drift")
    rules = official.combine_config()
    _SPRINT_REPLAYS = replay_map
    _SPRINT_CALENDAR = ordered
    _SPRINT_STARTS = start_map
    _SPRINT_CENSORED_DAYS = censored
    _SPRINT_GOVERNORS = tuple(dict(row) for row in governors)
    _SPRINT_ACCOUNT_RULES = rules
    _SPRINT_MAXIMUM_MINI_EQUIVALENT = float(
        official.combine_maximum_mini_equivalent
    )
    resolved_roles = {
        str(role): tuple(str(block) for block in blocks)
        for role, blocks in (
            block_roles
            or {
                "DESIGN": ("B1", "B2"),
                "HELD_OUT_DEVELOPMENT": ("B3", "B4"),
            }
        ).items()
    }
    if set(resolved_roles) != {"DESIGN", "HELD_OUT_DEVELOPMENT"}:
        raise FastPassHelperError("sprint block roles must freeze design and held-out development")
    if set(resolved_roles["DESIGN"]) & set(resolved_roles["HELD_OUT_DEVELOPMENT"]):
        raise FastPassHelperError("sprint block roles overlap")
    declared_blocks = {block for values in start_map.values() for _day, block in values}
    if not declared_blocks.issubset(
        set(resolved_roles["DESIGN"]) | set(resolved_roles["HELD_OUT_DEVELOPMENT"])
    ):
        raise FastPassHelperError("sprint start has no frozen selection role")
    _SPRINT_BLOCK_ROLES = resolved_roles


def _sprint_batch_worker(
    payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [_evaluate_sprint_policy(dict(payload)) for payload in payloads]


def _evaluate_sprint_policy(spec: Mapping[str, Any]) -> dict[str, Any]:
    if not _SPRINT_CALENDAR or not _SPRINT_REPLAYS:
        raise FastPassHelperError("sprint globals are not initialized")
    component_ids = tuple(str(value) for value in spec["component_ids"])
    if not component_ids or len(set(component_ids)) != len(component_ids):
        raise FastPassHelperError("sprint policy membership is invalid")
    missing = sorted(set(component_ids) - set(_SPRINT_REPLAYS))
    if missing:
        raise FastPassHelperError(f"sprint policy replay missing: {missing}")
    multipliers = {
        str(key): float(value)
        for key, value in dict(spec["quality_multipliers"]).items()
    }
    if set(multipliers) != set(component_ids) or any(
        value not in {0.5, 1.0, 1.5, 2.0} for value in multipliers.values()
    ):
        raise FastPassHelperError("sprint quality tier escaped 0.5/1/1.5/2")
    policy = _active_policy_from_spec(spec)
    scenario_trajectories: dict[str, dict[str, tuple[CausalTradeTrajectory, ...]]] = {
        "NORMAL": {},
        "STRESSED_1_5X": {},
    }
    quality_scaling: dict[str, dict[str, Any]] = {}
    for component_id in component_ids:
        replay = _SPRINT_REPLAYS[component_id]
        quality = multipliers[component_id]
        normal_resized, normal_receipt = _quality_trajectories(
            replay.normal_trajectories, quality
        )
        stressed_resized, stressed_receipt = _quality_trajectories(
            replay.stressed_trajectories, quality
        )
        if normal_receipt["selection_scaling_hash"] != stressed_receipt[
            "selection_scaling_hash"
        ]:
            raise FastPassHelperError("normal/stressed quality selection drift")
        scenario_trajectories["NORMAL"][component_id] = normal_resized
        scenario_trajectories["STRESSED_1_5X"][component_id] = stressed_resized
        quality_scaling[component_id] = normal_receipt

    all_evidence: list[dict[str, Any]] = []
    summaries: dict[str, dict[str, dict[str, Any]]] = {
        "NORMAL": {},
        "STRESSED_1_5X": {},
    }
    by_block_episodes: dict[
        str, dict[str, dict[str, list[tuple[Any, str]]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    by_block_censored: Counter[tuple[str, str, str]] = Counter()
    calendar_index = {day: index for index, day in enumerate(_SPRINT_CALENDAR)}
    censored_days = set().union(
        *(_SPRINT_CENSORED_DAYS.get(value, frozenset()) for value in component_ids)
    )
    for scenario in ("NORMAL", "STRESSED_1_5X"):
        for horizon in (5, 10, 20):
            episodes: list[tuple[Any, str]] = []
            censored_count = 0
            for start_day, block in _SPRINT_STARTS[horizon]:
                start_index = calendar_index[start_day]
                window = _SPRINT_CALENDAR[start_index : start_index + horizon]
                if any(day in censored_days for day in window):
                    censored_count += 1
                    by_block_censored[(block, scenario, str(horizon))] += 1
                    continue
                episode = run_causal_shared_account_episode(
                    scenario_trajectories[scenario],
                    _SPRINT_CALENDAR,
                    policy=policy,
                    start_day=start_day,
                    maximum_duration_days=horizon,
                    config=_SPRINT_ACCOUNT_RULES,
                )
                state = _episode_state(episode)
                episodes.append((episode, block))
                by_block_episodes[block][scenario][str(horizon)].append(
                    (episode, block)
                )
                all_evidence.append(
                    {
                        "policy_id": str(spec["policy_id"]),
                        "episode_id": (
                            f"{spec['policy_id']}:{horizon}:{start_day}"
                        ),
                        "scenario": scenario,
                        "horizon": f"{horizon}_TRADING_DAYS",
                        "horizon_trading_days": horizon,
                        "requested_duration_trading_days": horizon,
                        "temporal_block": block,
                        "start_day": start_day,
                        "coverage_state": state,
                        "episode": episode.to_dict(include_paths=True),
                    }
                )
            summaries[scenario][str(horizon)] = _summarize_sprint_episodes(
                episodes,
                requested_start_count=len(_SPRINT_STARTS[horizon]),
                data_censored_count=censored_count,
            )
    by_block: dict[str, Any] = {}
    declared_blocks = sorted(
        {
            block
            for values in _SPRINT_STARTS.values()
            for _start_day, block in values
        }
    )
    for block in declared_blocks:
        by_block[block] = {}
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            by_block[block][scenario] = {}
            for horizon in (5, 10, 20):
                values = by_block_episodes[block][scenario][str(horizon)]
                by_block[block][scenario][str(horizon)] = _summarize_sprint_episodes(
                    values,
                    requested_start_count=sum(
                        declared == block for _day, declared in _SPRINT_STARTS[horizon]
                    ),
                    data_censored_count=by_block_censored[
                        (block, scenario, str(horizon))
                    ],
                )
    summaries_by_role: dict[str, Any] = {}
    for role, role_blocks in _SPRINT_BLOCK_ROLES.items():
        summaries_by_role[role] = {}
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            summaries_by_role[role][scenario] = {}
            for horizon in (5, 10, 20):
                role_episodes = [
                    value
                    for block in role_blocks
                    for value in by_block_episodes[block][scenario][str(horizon)]
                ]
                role_censored = sum(
                    by_block_censored[(block, scenario, str(horizon))]
                    for block in role_blocks
                )
                summaries_by_role[role][scenario][str(horizon)] = (
                    _summarize_sprint_episodes(
                        role_episodes,
                        requested_start_count=sum(
                            block in role_blocks
                            for _day, block in _SPRINT_STARTS[horizon]
                        ),
                        data_censored_count=role_censored,
                    )
                )
    row = {
        "schema": "hydra_fast_pass_sprint_result_v1",
        "policy_id": str(spec["policy_id"]),
        "component_ids": list(component_ids),
        "quality_multipliers": multipliers,
        "governor_profile_id": str(spec["governor_profile"]["profile_id"]),
        "governor_policy": policy.to_dict(),
        "policy": policy.to_dict(),
        "policy_role": str(spec.get("policy_role", "UNSPECIFIED")),
        "wave": int(spec.get("wave", 0)),
        "predecessor_policy_id": spec.get("predecessor_policy_id"),
        "summaries": summaries,
        "summaries_by_role": summaries_by_role,
        "by_block": by_block,
        "quality_scaling": quality_scaling,
        "scenario_trajectory_hashes": {
            scenario: stable_hash(
                {
                    component_id: [row.to_dict() for row in trajectories]
                    for component_id, trajectories in values.items()
                }
            )
            for scenario, values in scenario_trajectories.items()
        },
        "signal_recomputation_performed": False,
        "status": "SPRINT_COMPLETE",
        "completed_episode_count": sum(
            value.get("episode") is not None for value in all_evidence
        ),
        "data_censored_episode_count": int(sum(by_block_censored.values())),
        "_episode_evidence": all_evidence,
    }
    held_out_stressed_5 = summaries_by_role["HELD_OUT_DEVELOPMENT"][
        "STRESSED_1_5X"
    ]["5"]
    contribution = dict(held_out_stressed_5["component_contribution"])
    positive_contribution = [max(float(value), 0.0) for value in contribution.values()]
    positive_total = sum(positive_contribution)
    row["single_trade_domination"] = bool(
        held_out_stressed_5["single_trade_domination"]
    )
    row["single_day_domination"] = bool(
        float(held_out_stressed_5["best_day_concentration_max"]) > 0.50
    )
    row["single_sleeve_domination"] = bool(
        len(component_ids) > 1
        and positive_total > 0.0
        and max(positive_contribution, default=0.0) / positive_total > 0.50
    )
    row["account_behavioral_cluster"] = stable_hash(
        {
            "component_ids": component_ids,
            "quality_multipliers": multipliers,
            "governor_structural_fingerprint": policy.structural_fingerprint,
        }
    )[:24]
    row["result_hash"] = stable_hash(
        {key: value for key, value in row.items() if key != "_episode_evidence"}
    )
    return row


def _resize_quality_trajectory(
    trajectory: CausalTradeTrajectory, resolved_integer_multiplier: float
) -> CausalTradeTrajectory:
    """Resize one selected event using an executable whole-number multiplier."""

    quality = float(resolved_integer_multiplier)
    if quality not in {1.0, 2.0}:
        raise FastPassHelperError("resolved event multiplier must be 1x or 2x")
    source_quantity = int(trajectory.event.quantity)
    quantity = max(1, int(math.floor(source_quantity * quality + 1e-12)))
    ratio = quantity / source_quantity
    event = replace(
        trajectory.event,
        quantity=quantity,
        mini_equivalent=float(trajectory.event.mini_equivalent * ratio),
        net_pnl=float(trajectory.event.net_pnl * ratio),
        gross_pnl=float(trajectory.event.gross_pnl * ratio),
        worst_unrealized_pnl=float(
            trajectory.event.worst_unrealized_pnl * ratio
        ),
        best_unrealized_pnl=float(trajectory.event.best_unrealized_pnl * ratio),
    )
    marks = tuple(
        CausalTradeMark(
            availability_time_ns=row.availability_time_ns,
            worst_unrealized_pnl=float(row.worst_unrealized_pnl * ratio),
            best_unrealized_pnl=float(row.best_unrealized_pnl * ratio),
            current_unrealized_pnl=(
                None
                if row.current_unrealized_pnl is None
                else float(row.current_unrealized_pnl * ratio)
            ),
        )
        for row in trajectory.marks
    )
    return replace(
        trajectory,
        event=event,
        marks=marks,
        initial_unrealized_pnl=float(trajectory.initial_unrealized_pnl * ratio),
    )


def _quality_trajectories(
    trajectories: Sequence[CausalTradeTrajectory], quality_multiplier: float
) -> tuple[tuple[CausalTradeTrajectory, ...], dict[str, Any]]:
    """Resolve 0.5/1/1.5/2 tiers without fractional contracts or outcomes.

    The stable choice uses only decision-time identity fields.  It is therefore
    identical under normal and stressed outcomes and cannot select on PnL.
    """

    quality = float(quality_multiplier)
    if quality not in {0.5, 1.0, 1.5, 2.0}:
        raise FastPassHelperError("unsupported frozen quality multiplier")
    selected: list[CausalTradeTrajectory] = []
    decisions: list[dict[str, Any]] = []
    for trajectory in trajectories:
        identity = {
            # Exact causal replay appends the cost scenario to the underlying
            # event id (``:NORMAL`` or ``:STRESSED_1_5X``).  That suffix is an
            # accounting namespace, not a decision-time field.  Including it
            # here made the otherwise identical normal/stressed quality draw
            # diverge.  Normalize only these two explicit terminal suffixes;
            # every other part of the immutable causal identity is retained.
            "event_id": _scenario_neutral_event_id(trajectory.event.event_id),
            "component_id": trajectory.component_id,
            "market": trajectory.market,
            "side": trajectory.side,
            "decision_ns": trajectory.event.decision_ns,
            "session_day": trajectory.event.session_day,
        }
        parity = int(stable_hash(identity)[-1], 16) % 2
        if quality == 0.5 and parity == 1:
            decisions.append({**identity, "selected": False, "resolved_multiplier": 0})
            continue
        resolved = (
            2.0
            if quality == 2.0 or (quality == 1.5 and parity == 1)
            else 1.0
        )
        selected.append(_resize_quality_trajectory(trajectory, resolved))
        decisions.append(
            {**identity, "selected": True, "resolved_multiplier": int(resolved)}
        )
    receipt = {
        "schema": "hydra_fast_pass_quality_scaling_receipt_v1",
        "quality_tier": quality,
        "input_event_count": len(trajectories),
        "selected_event_count": len(selected),
        "resolved_multiplier_counts": _counts(
            row["resolved_multiplier"] for row in decisions
        ),
        "selection_rule": "SHA256_CAUSAL_EVENT_IDENTITY_PARITY_V1",
        "uses_outcome_fields": False,
        "fractional_contracts_created": False,
    }
    receipt["selection_scaling_hash"] = stable_hash(decisions)
    return tuple(selected), receipt


def _scenario_neutral_event_id(event_id: str) -> str:
    """Return the causal event id without its explicit accounting scenario."""

    value = str(event_id)
    for suffix in (":STRESSED_1_5X", ":NORMAL"):
        if value.endswith(suffix):
            resolved = value[: -len(suffix)]
            if not resolved:
                raise FastPassHelperError("scenario-only event id is invalid")
            return resolved
    return value


def _active_policy_from_spec(spec: Mapping[str, Any]) -> ActiveRiskPoolPolicy:
    components = tuple(str(value) for value in spec["component_ids"])
    profile = dict(spec["governor_profile"])
    conflict = str(profile["same_instrument_conflict_policy"])
    if conflict not in {"priority", "reject_both"}:
        raise FastPassHelperError(
            "net-to-flat is unavailable without mutating an admitted sleeve"
        )
    rules = _SPRINT_ACCOUNT_RULES
    maximum_concurrent = min(
        int(profile["maximum_concurrent_sleeves"]), len(components)
    )
    open_fraction = float(profile["open_risk_ceiling_fraction"])
    daily_loss_fraction = float(profile["daily_loss_budget_fraction"])
    profit_lock_fraction = float(profile["daily_profit_lock_fraction"])
    protection_fraction = float(profile["target_protection_fraction"])
    return ActiveRiskPoolPolicy(
        policy_id=str(spec["policy_id"]),
        component_priority=components,
        nominal_risk_charge_per_mini=tuple((value, 2_250.0) for value in components),
        maximum_concurrent_sleeves=maximum_concurrent,
        aggregate_open_risk_ceiling=float(
            rules.combine_max_loss_limit * open_fraction
        ),
        maximum_mll_buffer_fraction=open_fraction,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=_SPRINT_MAXIMUM_MINI_EQUIVALENT,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=float(
            rules.combine_max_loss_limit * daily_loss_fraction
        ),
        daily_consistency_profit_guard=float(
            rules.combine_profit_target
            * rules.consistency_best_day_max_pct_of_profit_target
            * profit_lock_fraction
        ),
        target_protection_distance=float(
            rules.combine_profit_target * (1.0 - protection_fraction)
        ),
        target_protection_mode=TargetProtectionMode.SCALE_50,
        # Quality tiers were resolved into executable base trajectories above.
        # A second router multiplier would double count them.
        static_risk_tier=1.0,
    )


def _summarize_sprint_episodes(
    episodes_with_blocks: Sequence[tuple[Any, str]],
    *,
    requested_start_count: int,
    data_censored_count: int,
) -> dict[str, Any]:
    episodes = [row for row, _block in episodes_with_blocks]
    count = len(episodes)
    if not episodes:
        return {
            "requested_start_count": int(requested_start_count),
            "episode_count": 0,
            "full_coverage_start_count": 0,
            "data_censored_count": int(data_censored_count),
            "pass_count": 0,
            "pass_rate": 0.0,
            "net_total": 0.0,
            "net_median": 0.0,
            "target_progress_p25": 0.0,
            "target_progress_median": 0.0,
            "target_progress_p75": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "minimum_mll_buffer": 4_500.0,
            "consistency_rate": 0.0,
            "passing_episode_count": 0,
            "passing_consistency_rate": 0.0,
            "all_passing_paths_consistency_compliant": False,
            "passing_best_day_concentration_max": 0.0,
            "median_days_to_target": None,
            "block_pass_counts": {},
            "component_contribution": {},
            "best_day_concentration_max": 0.0,
            "maximum_positive_session_day_aggregate_share": 0.0,
            "positive_session_day_aggregate_profit_denominator": 0.0,
            "single_episode_day_observation_domination": False,
            "single_trade_domination": False,
            "single_trade_domination_metric_qualification": (
                "LEGACY_FIELD_IS_EPISODE_DAY_OBSERVATION_CONCENTRATION;"
                "NOT_TRADE_LEVEL_EVIDENCE"
            ),
            "accepted_event_count": 0,
            "skipped_event_count": 0,
            "maximum_mini_equivalent_mean": 0.0,
            "maximum_mini_equivalent_max": 0.0,
            "maximum_net_directional_exposure_mean": 0.0,
            "maximum_net_directional_exposure_max": 0.0,
            "mean_daily_maximum_mini_equivalent": 0.0,
            "mean_daily_contract_utilization": 0.0,
            "blocks_with_passes": [],
            "terminal_distribution": {},
        }
    progress = np.asarray([row.target_progress for row in episodes], dtype=float)
    net = np.asarray([row.net_pnl for row in episodes], dtype=float)
    passes = sum(row.passed for row in episodes)
    breach = sum(row.mll_breached for row in episodes)
    contributions: dict[str, float] = defaultdict(float)
    daily_values: list[float] = []
    daily_values_by_session_day: dict[int, float] = defaultdict(float)
    for episode in episodes:
        for component_id, value in episode.component_contribution.items():
            contributions[component_id] += float(value)
        for daily_row in episode.daily_path:
            value = float(daily_row.get("day_pnl", 0.0))
            daily_values.append(value)
            daily_values_by_session_day[int(daily_row["session_day"])] += value
    positive_daily = [max(value, 0.0) for value in daily_values]
    positive_sum = sum(positive_daily)
    single_episode_day_observation_domination = bool(
        positive_sum > 0.0 and max(positive_daily, default=0.0) / positive_sum > 0.50
    )
    positive_session_days = {
        session_day: max(value, 0.0)
        for session_day, value in daily_values_by_session_day.items()
    }
    positive_session_day_total = sum(positive_session_days.values())
    maximum_positive_session_day_aggregate_share = (
        max(positive_session_days.values(), default=0.0)
        / positive_session_day_total
        if positive_session_day_total > 0.0
        else 0.0
    )
    passing_episodes = [row for row in episodes if row.passed]
    passing_consistency_count = sum(
        bool(row.consistency_ok) for row in passing_episodes
    )
    passing_days = [row.days_to_target for row in episodes if row.days_to_target is not None]
    episode_maximum_mini = np.asarray(
        [row.maximum_mini_equivalent for row in episodes], dtype=float
    )
    episode_maximum_direction = np.asarray(
        [row.maximum_net_directional_exposure for row in episodes], dtype=float
    )
    daily_maximum_mini = np.asarray(
        [
            float(dict(row.get("exposure") or {}).get("maximum_mini_equivalent", 0.0))
            for episode in episodes
            for row in episode.daily_path
        ],
        dtype=float,
    )
    block_passes: Counter[str] = Counter()
    for episode, block in episodes_with_blocks:
        if episode.passed:
            block_passes[str(block)] += 1
    return {
        "requested_start_count": int(requested_start_count),
        "episode_count": count,
        "full_coverage_start_count": count,
        "data_censored_count": int(data_censored_count),
        "pass_count": int(passes),
        "pass_rate": float(passes / count),
        "net_total": float(np.sum(net)),
        "net_median": float(np.median(net)),
        "target_progress_p25": float(np.quantile(progress, 0.25)),
        "target_progress_median": float(np.median(progress)),
        "target_progress_p75": float(np.quantile(progress, 0.75)),
        "mll_breach_count": int(breach),
        "mll_breach_rate": float(breach / count),
        "minimum_mll_buffer": float(
            min(row.minimum_mll_buffer for row in episodes)
        ),
        "consistency_rate": float(sum(row.consistency_ok for row in episodes) / count),
        "passing_episode_count": len(passing_episodes),
        "passing_consistency_rate": (
            float(passing_consistency_count / len(passing_episodes))
            if passing_episodes
            else 0.0
        ),
        "all_passing_paths_consistency_compliant": bool(
            passing_episodes
            and passing_consistency_count == len(passing_episodes)
        ),
        "passing_best_day_concentration_max": float(
            max(
                (row.best_day_concentration for row in passing_episodes),
                default=0.0,
            )
        ),
        "median_days_to_target": (
            float(median(passing_days)) if passing_days else None
        ),
        "block_pass_counts": dict(sorted(block_passes.items())),
        "component_contribution": dict(sorted(contributions.items())),
        "best_day_concentration_max": float(
            max(row.best_day_concentration for row in episodes)
        ),
        "maximum_positive_session_day_aggregate_share": float(
            maximum_positive_session_day_aggregate_share
        ),
        "positive_session_day_aggregate_profit_denominator": float(
            positive_session_day_total
        ),
        "single_episode_day_observation_domination": (
            single_episode_day_observation_domination
        ),
        # Compatibility only.  The historical field was built from daily PnL
        # observations, not trade-level PnL, so it must not be used to claim a
        # trade-concentration control.  Authoritative trade concentration is a
        # later control over the immutable trade ledger.
        "single_trade_domination": single_episode_day_observation_domination,
        "single_trade_domination_metric_qualification": (
            "LEGACY_FIELD_IS_EPISODE_DAY_OBSERVATION_CONCENTRATION;"
            "NOT_TRADE_LEVEL_EVIDENCE"
        ),
        "accepted_event_count": int(sum(row.accepted_events for row in episodes)),
        "skipped_event_count": int(sum(row.skipped_events for row in episodes)),
        "maximum_mini_equivalent_mean": float(np.mean(episode_maximum_mini)),
        "maximum_mini_equivalent_max": float(np.max(episode_maximum_mini)),
        "maximum_net_directional_exposure_mean": float(
            np.mean(episode_maximum_direction)
        ),
        "maximum_net_directional_exposure_max": float(
            np.max(episode_maximum_direction)
        ),
        "mean_daily_maximum_mini_equivalent": (
            float(np.mean(daily_maximum_mini)) if daily_maximum_mini.size else 0.0
        ),
        "mean_daily_contract_utilization": (
            float(
                np.mean(daily_maximum_mini)
                / max(_SPRINT_MAXIMUM_MINI_EQUIVALENT, 1e-12)
            )
            if daily_maximum_mini.size
            else 0.0
        ),
        "blocks_with_passes": sorted(block_passes),
        "terminal_distribution": _counts(row.terminal.value for row in episodes),
    }


def _episode_state(episode: Any) -> str:
    if episode.terminal is CombineTerminal.PASSED:
        return "TARGET_REACHED"
    if episode.terminal is CombineTerminal.MLL_BREACH:
        return "MLL_BREACHED"
    if episode.terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return "HARD_RULE_FAILURE"
    if episode.terminal_reason == "CENSORED_FUTURE_COVERAGE":
        return "DATA_CENSORED"
    return "FULL_COVERAGE"


def _selection_receipt(
    *,
    campaign_id: str,
    wave: int,
    stage: str,
    rows: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[str],
    policy: str,
) -> dict[str, Any]:
    input_ids = sorted(
        str(row.get("candidate_id") or row.get("policy_id") or row.get("book_id"))
        for row in rows
    )
    selected = tuple(str(value) for value in selected_ids)
    if len(set(selected)) != len(selected) or not set(selected).issubset(input_ids):
        raise FastPassHelperError("selection receipt identity mismatch")
    value: dict[str, Any] = {
        "schema": "hydra_fast_pass_selection_receipt_v1",
        "campaign_id": str(campaign_id),
        "wave": int(wave),
        "stage": str(stage),
        "input_count": len(input_ids),
        "input_identity_hash": stable_hash(input_ids),
        "selected_count": len(selected),
        "selected_ids": list(selected),
        "policy": str(policy),
        "opaque_learned_score": False,
        "thresholds_changed_after_outcomes": False,
    }
    value["selection_hash"] = stable_hash(value)
    return value


def _verify_snapshot(
    value: Mapping[str, Any], hash_field: str, manifest: Mapping[str, Any]
) -> None:
    payload = dict(value)
    claimed = str(payload.pop(hash_field, ""))
    if (
        claimed != stable_hash(payload)
        or value.get("campaign_id") != manifest.get("campaign_id")
        or value.get("manifest_hash") != manifest.get("manifest_hash")
        or value.get("source_commit") != manifest.get("source_commit")
    ):
        raise FastPassHelperError("live fast-pass snapshot integrity failure")


def _balanced_sample(values: Sequence[Any], count: int) -> list[Any]:
    if count < 0:
        raise ValueError("balanced sample count must be non-negative")
    groups: dict[tuple[str, str, str], list[Any]] = defaultdict(list)
    for value in values:
        groups[
            (
                str(getattr(value, "market", "")),
                str(getattr(value, "mechanism", "")),
                str(getattr(value, "timeframe", "")),
            )
        ].append(value)
    queues = [
        sorted(
            rows,
            key=lambda row: str(
                getattr(row, "structural_fingerprint", stable_hash(str(row)))
            ),
        )
        for _key, rows in sorted(groups.items())
    ]
    retained: list[Any] = []
    position = 0
    while len(retained) < min(count, len(values)):
        progressed = False
        for rows in queues:
            if position < len(rows):
                retained.append(rows[position])
                progressed = True
                if len(retained) >= count:
                    break
        if not progressed:
            break
        position += 1
    return retained


def _canonical_jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(dict(row), sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
        for row in rows
    ).encode("utf-8")


def _load_batches(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if path.is_dir():
        for batch in sorted(path.glob("batch_*.jsonl")):
            rows.extend(_read_jsonl(batch))
    identifiers = [
        str(row.get("candidate_id") or row.get("policy_id") or row.get("book_id"))
        for row in rows
    ]
    if any(value in {"", "None"} for value in identifiers) or len(identifiers) != len(
        set(identifiers)
    ):
        raise FastPassHelperError(f"duplicate or missing identity across batches: {path}")
    return rows


def _next_batch_index(path: Path) -> int:
    values = [int(row.stem.rsplit("_", 1)[-1]) for row in path.glob("batch_*.jsonl")]
    return max(values, default=-1) + 1


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FastPassHelperError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise FastPassHelperError(f"JSON object required: {path}")
    return value


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    try:
        payload = gzip.decompress(target.read_bytes()) if target.suffix == ".gz" else target.read_bytes()
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise FastPassHelperError(f"invalid JSONL: {path}") from exc
    return _decode_jsonl(payload, target)


def _decode_jsonl(payload: bytes, path: Path) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    try:
        lines = payload.decode("utf-8").splitlines()
        for line in lines:
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise FastPassHelperError(f"JSONL object required: {path}")
            output.append(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FastPassHelperError(f"invalid JSONL: {path}") from exc
    return output


def _counts(values: Iterable[Any]) -> dict[str, int]:
    return dict(sorted(Counter(str(value) for value in values).items()))


def _project_root(path: Path) -> Path:
    for candidate in (path.parent, *path.parents):
        if (candidate / "MISSION_CONTRACT.md").is_file():
            return candidate
    raise FastPassHelperError("project root not found")


def _date_value(value: str) -> date:
    return date.fromisoformat(value[:10])


def _epoch_date(value: int) -> date:
    return date(1970, 1, 1) + timedelta(days=int(value))


def _system_cpu_ticks() -> tuple[int, int]:
    try:
        fields = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()
        values = [int(value) for value in fields[1:]]
        total = sum(values)
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return total - idle, total
    except (OSError, ValueError, IndexError):
        return 0, 0


def _system_cpu_fraction(start: tuple[int, int]) -> float:
    busy, total = _system_cpu_ticks()
    busy_delta = busy - int(start[0])
    total_delta = total - int(start[1])
    if total_delta <= 0:
        return 0.0
    return min(max(busy_delta / total_delta, 0.0), 1.0)


def _parse_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise FastPassHelperError("timestamp lacks timezone")
    return parsed


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _horizon_summary(
    row: Mapping[str, Any], scenario: str, horizon: int
) -> dict[str, Any]:
    return dict(
        ((row.get("summaries") or {}).get(str(scenario)) or {}).get(
            str(int(horizon))
        )
        or {}
    )


def _role_horizon_summary(
    row: Mapping[str, Any], role: str, scenario: str, horizon: int
) -> dict[str, Any]:
    return dict(
        (
            (
                ((row.get("summaries_by_role") or {}).get(str(role)) or {}).get(
                    str(scenario)
                )
                or {}
            ).get(str(int(horizon)))
        )
        or {}
    )


def _tier_result_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    stressed5 = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "STRESSED_1_5X", 5
    )
    normal5 = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "NORMAL", 5
    )
    stressed10 = _role_horizon_summary(
        row, "HELD_OUT_DEVELOPMENT", "STRESSED_1_5X", 10
    )
    return (
        -float(stressed5.get("pass_rate", 0.0)),
        -float(normal5.get("pass_rate", 0.0)),
        -float(stressed10.get("pass_rate", 0.0)),
        -float(stressed5.get("target_progress_p25", 0.0)),
        float(stressed5.get("mll_breach_rate", 1.0)),
        -float(stressed5.get("net_total", 0.0)),
        str(row.get("policy_id", "")),
    )


def _jaccard(left: Iterable[Any], right: Iterable[Any]) -> float:
    left_set = set(left)
    right_set = set(right)
    union = left_set | right_set
    return float(len(left_set & right_set) / len(union)) if union else 0.0


def _read_episode_receipt(
    base_dir: str | Path, receipt: Mapping[str, Any]
) -> list[dict[str, Any]]:
    base = Path(base_dir).resolve()
    relative = Path(str(receipt.get("relative_path", "")))
    if relative.is_absolute() or ".." in relative.parts:
        raise FastPassHelperError("episode receipt path is unsafe")
    path = (base / relative).resolve()
    if base not in path.parents or _sha256(path) != str(receipt.get("sha256", "")):
        raise FastPassHelperError("episode receipt checksum/path drift")
    rows = _read_jsonl(path)
    if len(rows) != int(receipt.get("record_count", -1)):
        raise FastPassHelperError("episode receipt record-count drift")
    return rows


def _resume_counter_inventory(payload_dir: str | Path) -> dict[str, int]:
    root = Path(payload_dir)
    exact = 0
    episodes = 0
    books = 0
    for wave in (1, 2):
        exact += sum(
            row.get("status") == "STAGE_2_COMPLETE"
            for row in _load_batches(root / f"wave_{wave:02d}/stage2_batches")
        )
        for category in ("sleeves", "books", "book_controls"):
            episodes += sum(
                int(row.get("completed_episode_count", 0))
                for row in _load_batches(
                    root / f"wave_{wave:02d}/{category}_sprint_batches"
                )
            )
        decisions = root / f"wave_{wave:02d}/marginal_book_decisions.jsonl"
        if decisions.is_file():
            books += len(_read_jsonl(decisions))
    return {"exact": int(exact), "episodes": int(episodes), "books": int(books)}


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bucket(value: float, thresholds: Sequence[float], labels: Sequence[str]) -> str:
    if len(labels) != len(thresholds) + 1:
        raise ValueError("bucket labels must have one more member than thresholds")
    for threshold, label in zip(thresholds, labels):
        if value < threshold:
            return label
    return labels[-1]


__all__ = [
    "FastPassHelperError",
    "_active_policy_from_spec",
    "_balanced_sample",
    "_bank_key",
    "_book_component_key",
    "_book_result_key",
    "_book_spec",
    "_canonical_jsonl_bytes",
    "_control_delta",
    "_counts",
    "_date_value",
    "_epoch_date",
    "_event_block_economics",
    "_exact_hashes",
    "_fast_screen_batch_worker",
    "_governor_profiles",
    "_horizon_summary",
    "_install_sprint_globals",
    "_level1_control_batch_worker",
    "_load_batches",
    "_next_batch_index",
    "_project_root",
    "_proposal_qd_cell",
    "_quality_multiplier",
    "_quality_trajectories",
    "_read_episode_receipt",
    "_read_event_receipt",
    "_read_json",
    "_read_jsonl",
    "_realized_qd_cell",
    "_resize_quality_trajectory",
    "_resume_counter_inventory",
    "_role_horizon_summary",
    "_selection_receipt",
    "_sha256",
    "_sprint_batch_worker",
    "_sprint_metrics",
    "_stage1_key",
    "_summarize_sprint_episodes",
    "_system_cpu_ticks",
    "_system_cpu_fraction",
    "_parse_datetime",
    "_tier_result_key",
    "_utc_now",
    "_jaccard",
    "_verify_snapshot",
]
