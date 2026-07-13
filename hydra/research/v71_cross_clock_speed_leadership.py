from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import (
    V71CandidateSpec,
    V71Signal,
    signal_path_hash,
)
from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_cross_clock_speed_leadership_grammar_0005"
GRAMMAR_PATH = (
    "WORM/v7.1-cross-clock-speed-leadership-grammar-0005-2026-07-13.json"
)
GRAMMAR_SHA256 = "27a937a112dd4963402f8c12feb69cf9cd347b020ce47396cfffff0e253726c2"
HORIZONS = (30, 60)
RESPONSES = ("CONTINUATION", "REVERSAL")
MOTIFS = (
    "VOLUME_SPEED_TAKES_LEAD",
    "DOLLAR_SPEED_TAKES_LEAD",
    "SPEED_LEAD_FLIP_FLOW_DISAGREEMENT",
)
MINUTE_NS = 60_000_000_000


class V71CrossClockSpeedLeadershipError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SpeedLeadershipSourceAudit:
    volume_event_count: int
    dollar_event_count: int
    completed_event_timestamp_count: int
    speed_state_count: int
    speed_leadership_transition_count: int
    volume_takes_lead_count: int
    dollar_takes_lead_count: int
    flow_disagreement_transition_count: int
    executable_transition_count_30m: int
    executable_transition_count_60m: int

    def to_dict(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


def candidate_specs(
    project_root: str | Path = ".",
) -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    rows: list[V71CandidateSpec] = []
    for motif in MOTIFS:
        for response in RESPONSES:
            for horizon in HORIZONS:
                candidate_id = (
                    "v71g5_cross_clock_speed_leadership_"
                    f"{motif.lower()}_{response.lower()}_h{horizon}"
                )
                payload = {
                    "grammar_id": GRAMMAR_ID,
                    "grammar_sha256": GRAMMAR_SHA256,
                    "candidate_id": candidate_id,
                    "family_id": "CROSS_CLOCK_SPEED_LEADERSHIP",
                    "mechanism_class": "v71g5_cross_clock_speed_leadership",
                    "motif": motif,
                    "response_policy": response,
                    "holding_minutes": horizon,
                    "cost_horizon": f"{horizon}m",
                    "product": "ES",
                }
                rows.append(
                    V71CandidateSpec(
                        candidate_id=candidate_id,
                        family_id="CROSS_CLOCK_SPEED_LEADERSHIP",
                        mechanism_class="v71g5_cross_clock_speed_leadership",
                        motif=motif,
                        response_policy=response,
                        holding_minutes=horizon,
                        cost_horizon=f"{horizon}m",
                        product="ES",
                        specification_hash=_stable_hash(payload),
                    )
                )
    if len(rows) != 12 or len({row.candidate_id for row in rows}) != 12:
        raise V71CrossClockSpeedLeadershipError(
            "speed-leadership grammar must contain 12 candidates"
        )
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_speed_leadership_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, SpeedLeadershipSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    minute, event, _ = load_event_time_sources(root)
    transitions, audit = build_speed_leadership_transitions(minute, event)
    return minute, transitions, audit


def build_speed_leadership_transitions(
    minute: pd.DataFrame,
    event: pd.DataFrame,
) -> tuple[pd.DataFrame, SpeedLeadershipSourceAudit]:
    selected = event[event["bar_type"].isin(("VOLUME_BAR", "DOLLAR_BAR"))].copy()
    selected = selected.sort_values(
        [
            "calendar_year",
            "contract",
            "session_day",
            "availability_ns",
            "bar_type",
            "bar_sequence",
        ],
        kind="stable",
    )
    group_keys = ["calendar_year", "contract", "session_day"]
    dedup_keys = [*group_keys, "availability_ns", "bar_type"]
    selected = selected.groupby(dedup_keys, sort=True, as_index=False).tail(1)
    value_columns = ("duration_seconds", "signed_aggressor_volume")
    clocks: dict[str, pd.DataFrame] = {}
    for bar_type, prefix in (("VOLUME_BAR", "volume"), ("DOLLAR_BAR", "dollar")):
        frame = selected[selected["bar_type"] == bar_type][
            [*group_keys, "availability_ns", *value_columns]
        ].copy()
        frame = frame.rename(
            columns={
                "availability_ns": f"{prefix}_availability_ns",
                "duration_seconds": f"{prefix}_duration_seconds",
                "signed_aggressor_volume": f"{prefix}_flow",
            }
        )
        frame["decision_ns"] = frame[f"{prefix}_availability_ns"]
        clocks[prefix] = frame
    timeline = clocks["volume"].merge(
        clocks["dollar"],
        on=[*group_keys, "decision_ns"],
        how="outer",
        validate="one_to_one",
    ).sort_values([*group_keys, "decision_ns"], kind="stable")
    timestamp_count = len(timeline)
    state_columns = [
        "volume_availability_ns",
        "volume_duration_seconds",
        "volume_flow",
        "dollar_availability_ns",
        "dollar_duration_seconds",
        "dollar_flow",
    ]
    timeline[state_columns] = timeline.groupby(
        group_keys, sort=False
    )[state_columns].ffill()
    timeline = timeline.dropna(
        subset=["volume_duration_seconds", "dollar_duration_seconds"]
    ).copy()
    volume_duration = timeline["volume_duration_seconds"].to_numpy(float)
    dollar_duration = timeline["dollar_duration_seconds"].to_numpy(float)
    timeline["speed_state"] = np.where(
        volume_duration < dollar_duration,
        1,
        np.where(dollar_duration < volume_duration, -1, 0),
    ).astype(np.int8)
    timeline["prior_speed_state"] = timeline.groupby(
        group_keys, sort=False
    )["speed_state"].shift(1)
    speed_state_count = int((timeline["speed_state"] != 0).sum())
    transition_mask = (
        timeline["prior_speed_state"].notna()
        & timeline["speed_state"].ne(0)
        & timeline["speed_state"].ne(timeline["prior_speed_state"])
    )
    transitions = timeline.loc[transition_mask].copy()
    transitions["prior_speed_leader"] = np.select(
        [
            transitions["prior_speed_state"].eq(1),
            transitions["prior_speed_state"].eq(-1),
        ],
        ["VOLUME", "DOLLAR"],
        default="TIE",
    )
    transitions["speed_leader"] = np.where(
        transitions["speed_state"].eq(1), "VOLUME", "DOLLAR"
    )
    transitions["leader_direction"] = np.sign(
        np.where(
            transitions["speed_state"].eq(1),
            transitions["volume_flow"],
            transitions["dollar_flow"],
        )
    ).astype(np.int8)
    volume_sign = np.sign(transitions["volume_flow"].to_numpy(float)).astype(np.int8)
    dollar_sign = np.sign(transitions["dollar_flow"].to_numpy(float)).astype(np.int8)
    transitions["flow_disagreement"] = (
        (volume_sign != 0)
        & (dollar_sign != 0)
        & (volume_sign == -dollar_sign)
    )
    transitions = transitions[
        [
            *group_keys,
            "decision_ns",
            "prior_speed_leader",
            "speed_leader",
            "leader_direction",
            "volume_duration_seconds",
            "dollar_duration_seconds",
            "volume_availability_ns",
            "dollar_availability_ns",
            "volume_flow",
            "dollar_flow",
            "flow_disagreement",
        ]
    ]
    if transitions.empty:
        raise V71CrossClockSpeedLeadershipError(
            "speed-leadership representation produced no transitions"
        )
    transitions = transitions.sort_values(
        ["calendar_year", "decision_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    starts = minute["minute_start_ns"].to_numpy(np.int64)
    contracts = minute["contract"].astype(str).to_numpy()
    session_days = _minute_session_days(minute)
    for horizon in HORIZONS:
        decisions = transitions["decision_ns"].to_numpy(np.int64)
        entries = np.searchsorted(starts, decisions, side="left")
        valid_entry = entries < len(starts)
        entry_starts = np.full(len(transitions), -1, dtype=np.int64)
        entry_starts[valid_entry] = starts[entries[valid_entry]]
        targets = entry_starts + horizon * MINUTE_NS
        exits = np.searchsorted(starts, targets, side="left")
        valid = valid_entry & (exits < len(starts))
        safe_entries = np.minimum(entries, len(starts) - 1)
        safe_exits = np.minimum(exits, len(starts) - 1)
        valid &= np.where(valid, starts[safe_exits] == targets, False)
        expected_contracts = transitions["contract"].astype(str).to_numpy()
        expected_days = transitions["session_day"].astype(str).to_numpy()
        valid &= np.where(valid, contracts[safe_entries] == expected_contracts, False)
        valid &= np.where(valid, contracts[safe_exits] == expected_contracts, False)
        valid &= np.where(valid, session_days[safe_entries] == expected_days, False)
        valid &= np.where(valid, session_days[safe_exits] == expected_days, False)
        transitions[f"entry_ns_{horizon}"] = entry_starts
        transitions[f"exit_ns_{horizon}"] = np.where(valid, targets, -1)
        transitions[f"executable_{horizon}"] = valid
    audit = SpeedLeadershipSourceAudit(
        volume_event_count=int((selected["bar_type"] == "VOLUME_BAR").sum()),
        dollar_event_count=int((selected["bar_type"] == "DOLLAR_BAR").sum()),
        completed_event_timestamp_count=timestamp_count,
        speed_state_count=speed_state_count,
        speed_leadership_transition_count=len(transitions),
        volume_takes_lead_count=int((transitions["speed_leader"] == "VOLUME").sum()),
        dollar_takes_lead_count=int((transitions["speed_leader"] == "DOLLAR").sum()),
        flow_disagreement_transition_count=int(transitions["flow_disagreement"].sum()),
        executable_transition_count_30m=int(transitions["executable_30"].sum()),
        executable_transition_count_60m=int(transitions["executable_60"].sum()),
    )
    return transitions, audit


def generate_signal_population(
    minute: pd.DataFrame,
    transitions: pd.DataFrame,
    *,
    project_root: str | Path = ".",
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V71Signal, ...]]:
    specs = candidate_specs(project_root)
    if graveyard_path is not None:
        dead = {
            str(row["mechanism_class"])
            for row in class_feedback(graveyard_path)
        }
        if "v71g5_cross_clock_speed_leadership" in dead:
            raise V71CrossClockSpeedLeadershipError(
                "speed-leadership mechanism is tombstoned"
            )
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, transitions)
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V71CrossClockSpeedLeadershipError(
            "speed-leadership signal population drift"
        )
    return dict(sorted(output.items()))


def _signals_for_spec(
    spec: V71CandidateSpec,
    transitions: pd.DataFrame,
) -> list[V71Signal]:
    if spec.motif == "VOLUME_SPEED_TAKES_LEAD":
        mask = transitions["speed_leader"].eq("VOLUME").to_numpy(bool, copy=True)
    elif spec.motif == "DOLLAR_SPEED_TAKES_LEAD":
        mask = transitions["speed_leader"].eq("DOLLAR").to_numpy(bool, copy=True)
    elif spec.motif == "SPEED_LEAD_FLIP_FLOW_DISAGREEMENT":
        mask = transitions["flow_disagreement"].to_numpy(bool, copy=True)
    else:
        raise V71CrossClockSpeedLeadershipError(f"unknown motif: {spec.motif}")
    leader_direction = transitions["leader_direction"].to_numpy(np.int8)
    mask &= leader_direction != 0
    side = (
        leader_direction
        if spec.response_policy == "CONTINUATION"
        else -leader_direction
    )
    signals: list[V71Signal] = []
    next_allowed_by_day: dict[str, int] = {}
    executable = transitions[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    for position in np.flatnonzero(mask & executable):
        row = transitions.iloc[int(position)]
        day = str(row["session_day"])
        decision = int(row["decision_ns"])
        if decision < next_allowed_by_day.get(day, -1):
            continue
        entry = int(row[f"entry_ns_{spec.holding_minutes}"])
        exit_ns = int(row[f"exit_ns_{spec.holding_minutes}"])
        snapshot = {
            "grammar_sha256": GRAMMAR_SHA256,
            "candidate_id": spec.candidate_id,
            "position": int(position),
            "decision_ns": decision,
            "prior_speed_leader": str(row["prior_speed_leader"]),
            "speed_leader": str(row["speed_leader"]),
            "volume_duration_seconds": float(row["volume_duration_seconds"]),
            "dollar_duration_seconds": float(row["dollar_duration_seconds"]),
            "volume_availability_ns": int(row["volume_availability_ns"]),
            "dollar_availability_ns": int(row["dollar_availability_ns"]),
            "volume_flow": float(row["volume_flow"]),
            "dollar_flow": float(row["dollar_flow"]),
        }
        signals.append(
            V71Signal(
                candidate_id=spec.candidate_id,
                family_id=spec.family_id,
                motif=spec.motif,
                response_policy=spec.response_policy,
                holding_minutes=spec.holding_minutes,
                calendar_year=int(row["calendar_year"]),
                session_day=day,
                source_position=int(position),
                availability_ns=decision,
                decision_ns=decision,
                entry_minute_start_ns=entry,
                exit_minute_start_ns=exit_ns,
                side=int(side[int(position)]),
                contract=str(row["contract"]),
                feature_snapshot_hash=_stable_hash(snapshot),
            )
        )
        next_allowed_by_day[day] = exit_ns
    return signals


def _minute_session_days(minute: pd.DataFrame) -> np.ndarray:
    timestamps = pd.to_datetime(
        minute["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    return np.asarray([value.isoformat() for value in timestamps.date])


def _load_grammar(root: Path) -> Mapping[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71CrossClockSpeedLeadershipError(
            "speed-leadership WORM hash drift"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("grammar_id") != GRAMMAR_ID
        or int(payload.get("candidate_count", 0)) != 12
    ):
        raise V71CrossClockSpeedLeadershipError(
            "speed-leadership grammar identity drift"
        )
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "GRAMMAR_ID",
    "SpeedLeadershipSourceAudit",
    "V71CrossClockSpeedLeadershipError",
    "build_speed_leadership_transitions",
    "candidate_specs",
    "generate_signal_population",
    "load_speed_leadership_sources",
    "signal_path_hash",
]
