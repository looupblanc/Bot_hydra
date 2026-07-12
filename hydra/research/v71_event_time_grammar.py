from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import (
    V71CandidateSpec,
    V71Signal,
    signal_path_hash,
)
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_event_time_grammar_0003"
GRAMMAR_PATH = "WORM/v7.1-event-time-grammar-0003-2026-07-12.json"
GRAMMAR_SHA256 = "df9ffd7c6c87707838f53c30e474d7477bf17532ba29bffc1baa2b2a5bd0903f"
EVENT_STORE_PATH = "data/cache/v7_d1/date_matched_event_bars_v1.parquet"
EVENT_STORE_SHA256 = "ea0208fc3666f912b39e9b21b302a466bd7ee00c802140d6a6beed73098aa4a3"
MINUTE_STORE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_STORE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
HORIZONS = (5, 15, 30, 60)
RESPONSES = ("CONTINUATION", "REVERSAL")
ROLLING_EVENTS = 40


class V71EventTimeError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class EventTimeSourceAudit:
    raw_es_event_count: int
    nonpositive_duration_count: int
    cross_chicago_date_count: int
    availability_before_end_count: int
    retained_event_count: int

    def to_dict(self) -> dict[str, int]:
        return {
            "raw_es_event_count": self.raw_es_event_count,
            "nonpositive_duration_count": self.nonpositive_duration_count,
            "cross_chicago_date_count": self.cross_chicago_date_count,
            "availability_before_end_count": self.availability_before_end_count,
            "retained_event_count": self.retained_event_count,
        }


@dataclass(frozen=True, slots=True)
class MinuteExecutionMap:
    valid: np.ndarray
    entry_ns: np.ndarray
    exit_ns: np.ndarray


def candidate_specs(
    project_root: str | Path = ".",
) -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    rows: list[V71CandidateSpec] = []
    for family in grammar["families"]:
        family_id = str(family["family_id"])
        mechanism_class = "v71g3_" + family_id.lower()
        for motif in family["motifs"]:
            for response in RESPONSES:
                for horizon in HORIZONS:
                    candidate_id = (
                        f"v71g3_{family_id.lower()}_{str(motif).lower()}_"
                        f"{response.lower()}_h{horizon}"
                    )
                    payload = {
                        "grammar_id": GRAMMAR_ID,
                        "grammar_sha256": GRAMMAR_SHA256,
                        "candidate_id": candidate_id,
                        "family_id": family_id,
                        "mechanism_class": mechanism_class,
                        "motif": str(motif),
                        "response_policy": response,
                        "holding_minutes": horizon,
                        "cost_horizon": f"{horizon}m",
                        "product": "ES",
                    }
                    rows.append(
                        V71CandidateSpec(
                            candidate_id=candidate_id,
                            family_id=family_id,
                            mechanism_class=mechanism_class,
                            motif=str(motif),
                            response_policy=response,
                            holding_minutes=horizon,
                            cost_horizon=f"{horizon}m",
                            product="ES",
                            specification_hash=_stable_hash(payload),
                        )
                    )
    counts = Counter(row.family_id for row in rows)
    if len(rows) != 128 or len({row.candidate_id for row in rows}) != 128:
        raise V71EventTimeError("event-time grammar must contain 128 candidates")
    if len(counts) != 4 or set(counts.values()) != {32}:
        raise V71EventTimeError("event-time family allocation drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_event_time_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, EventTimeSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        EVENT_STORE_PATH: EVENT_STORE_SHA256,
        MINUTE_STORE_PATH: MINUTE_STORE_SHA256,
    }
    drift = [path for path, expected in checks.items() if _sha256(root / path) != expected]
    if drift:
        raise V71EventTimeError("event-time frozen source drift: " + ",".join(drift))
    event = pd.read_parquet(root / EVENT_STORE_PATH)
    event = event[event["product"] == "ES"].copy()
    minute = pd.read_parquet(root / MINUTE_STORE_PATH)
    minute = minute[minute["product"] == "ES"].copy()
    minute = minute.sort_values(
        ["calendar_year", "minute_start_ns"], kind="stable"
    ).reset_index(drop=True)
    raw_count = len(event)
    duration = event["end_event_ns"].to_numpy(np.int64) - event[
        "start_event_ns"
    ].to_numpy(np.int64)
    start = pd.to_datetime(
        event["start_event_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    end = pd.to_datetime(
        event["end_event_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    start_day = np.asarray([value.isoformat() for value in start.date])
    end_day = np.asarray([value.isoformat() for value in end.date])
    nonpositive = duration <= 0
    cross_day = start_day != end_day
    availability_invalid = (
        event["availability_ns"].to_numpy(np.int64)
        < event["end_event_ns"].to_numpy(np.int64)
    )
    valid = ~(nonpositive | cross_day | availability_invalid)
    event = event.loc[valid].copy()
    event["session_day"] = start_day[valid]
    event["duration_seconds"] = duration[valid] / 1_000_000_000.0
    event = event.sort_values(
        [
            "calendar_year",
            "session_day",
            "contract",
            "bar_type",
            "start_event_ns",
        ],
        kind="stable",
    ).reset_index(drop=True)
    audit = EventTimeSourceAudit(
        raw_es_event_count=raw_count,
        nonpositive_duration_count=int(np.sum(nonpositive)),
        cross_chicago_date_count=int(np.sum(cross_day)),
        availability_before_end_count=int(np.sum(availability_invalid)),
        retained_event_count=len(event),
    )
    if event.empty or minute.empty:
        raise V71EventTimeError("event-time source is empty")
    return minute, event, audit


def generate_signal_population(
    minute: pd.DataFrame,
    event: pd.DataFrame,
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
        collisions = sorted(
            {row.mechanism_class for row in specs if row.mechanism_class in dead}
        )
        if collisions:
            raise V71EventTimeError(
                "event-time grammar repeats cemetery classes: "
                + ",".join(collisions)
            )
    states = _event_time_states(event)
    execution_maps = _build_execution_maps(event, minute)
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        mask, direction = states[(spec.family_id, spec.motif)]
        side = direction if spec.response_policy == "CONTINUATION" else -direction
        output[spec.candidate_id] = tuple(
            _signals_for_spec(
                spec,
                event,
                execution_maps[spec.holding_minutes],
                mask=mask,
                side=side,
            )
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V71EventTimeError("event-time signal population drift")
    return dict(sorted(output.items()))


def _event_time_states(
    event: pd.DataFrame,
) -> dict[tuple[str, str], tuple[np.ndarray, np.ndarray]]:
    duration = event["duration_seconds"].to_numpy(float)
    price_change = event["price_change_points"].to_numpy(float)
    path = event["path_length_points"].to_numpy(float)
    signed_volume = event["signed_aggressor_volume"].to_numpy(float)
    direction_price = _direction(price_change)
    direction_flow = _direction(signed_volume)
    signed_rate = np.divide(
        signed_volume,
        duration,
        out=np.zeros_like(signed_volume),
        where=duration > 0.0,
    )
    absolute_rate = np.abs(signed_rate)
    path_rate = np.divide(
        path,
        duration,
        out=np.zeros_like(path),
        where=duration > 0.0,
    )
    efficiency = np.divide(
        np.abs(price_change),
        path,
        out=np.zeros_like(path),
        where=path > 0.0,
    )
    group_columns = ["calendar_year", "session_day", "contract", "bar_type"]
    duration_lo = _past_quantile(event, duration, group_columns, 0.20)
    duration_hi = _past_quantile(event, duration, group_columns, 0.80)
    rate_hi = _past_quantile(event, absolute_rate, group_columns, 0.80)
    path_rate_hi = _past_quantile(event, path_rate, group_columns, 0.80)
    displacement_lo = _past_quantile(
        event, np.abs(price_change), group_columns, 0.20
    )
    efficiency_lo = _past_quantile(event, efficiency, group_columns, 0.20)
    efficiency_hi = _past_quantile(event, efficiency, group_columns, 0.80)
    prior_duration = _group_shift(event, duration, group_columns)
    prior_efficiency = _group_shift(event, efficiency, group_columns)
    valid = np.isfinite(duration_lo) & np.isfinite(rate_hi)
    bar_type = event["bar_type"].astype(str).to_numpy()
    states: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}

    def put(
        family: str,
        motif: str,
        source_type: str,
        raw_mask: np.ndarray,
        raw_direction: np.ndarray,
    ) -> None:
        mask = valid & (bar_type == source_type) & raw_mask & (raw_direction != 0)
        states[(family, motif)] = (mask, raw_direction)

    put(
        "EVENT_COMPLETION_SPEED",
        "FAST_DOLLAR_COMPLETION",
        "DOLLAR_BAR",
        duration < duration_lo,
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_COMPLETION_SPEED",
        "SLOW_DOLLAR_COMPLETION",
        "DOLLAR_BAR",
        duration > duration_hi,
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_COMPLETION_SPEED",
        "FAST_VOLUME_COMPLETION",
        "VOLUME_BAR",
        duration < duration_lo,
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_COMPLETION_SPEED",
        "SLOW_VOLUME_COMPLETION",
        "VOLUME_BAR",
        duration > duration_hi,
        _fallback_direction(direction_price, direction_flow),
    )

    put(
        "EVENT_CLOCK_ACCELERATION",
        "DOLLAR_CLOCK_ACCELERATION",
        "DOLLAR_BAR",
        (duration < duration_lo) & (prior_duration > duration_hi),
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_CLOCK_ACCELERATION",
        "DOLLAR_CLOCK_DECELERATION",
        "DOLLAR_BAR",
        (duration > duration_hi) & (prior_duration < duration_lo),
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_CLOCK_ACCELERATION",
        "VOLUME_CLOCK_ACCELERATION",
        "VOLUME_BAR",
        (duration < duration_lo) & (prior_duration > duration_hi),
        _fallback_direction(direction_price, direction_flow),
    )
    put(
        "EVENT_CLOCK_ACCELERATION",
        "VOLUME_CLOCK_DECELERATION",
        "VOLUME_BAR",
        (duration > duration_hi) & (prior_duration < duration_lo),
        _fallback_direction(direction_price, direction_flow),
    )

    put(
        "SIGNED_FLOW_RATE",
        "DOLLAR_SIGNED_RATE",
        "DOLLAR_BAR",
        absolute_rate > rate_hi,
        direction_flow,
    )
    put(
        "SIGNED_FLOW_RATE",
        "VOLUME_SIGNED_RATE",
        "VOLUME_BAR",
        absolute_rate > rate_hi,
        direction_flow,
    )
    put(
        "SIGNED_FLOW_RATE",
        "IMBALANCE_SIGNED_RATE",
        "SIGNED_IMBALANCE_BAR",
        absolute_rate > rate_hi,
        direction_flow,
    )
    put(
        "SIGNED_FLOW_RATE",
        "HIGH_RATE_LOW_PROGRESS",
        "DOLLAR_BAR",
        (absolute_rate > rate_hi) & (np.abs(price_change) < displacement_lo),
        direction_flow,
    )

    put(
        "EVENT_TIME_PATH_GEOMETRY",
        "HIGH_DOLLAR_PATH_RATE",
        "DOLLAR_BAR",
        path_rate > path_rate_hi,
        direction_price,
    )
    put(
        "EVENT_TIME_PATH_GEOMETRY",
        "HIGH_VOLUME_PATH_RATE",
        "VOLUME_BAR",
        path_rate > path_rate_hi,
        direction_price,
    )
    put(
        "EVENT_TIME_PATH_GEOMETRY",
        "LOW_EFFICIENCY_HIGH_RATE",
        "DOLLAR_BAR",
        (efficiency < efficiency_lo) & (path_rate > path_rate_hi),
        _fallback_direction(direction_flow, direction_price),
    )
    put(
        "EVENT_TIME_PATH_GEOMETRY",
        "TIME_EFFICIENCY_TRANSITION",
        "VOLUME_BAR",
        (prior_efficiency < efficiency_lo) & (efficiency > efficiency_hi),
        direction_price,
    )
    expected = {
        (spec.family_id, spec.motif)
        for spec in candidate_specs(Path(__file__).resolve().parents[2])
    }
    if set(states) != expected:
        raise V71EventTimeError("event-time motif implementation drift")
    return states


def _signals_for_spec(
    spec: V71CandidateSpec,
    event: pd.DataFrame,
    execution: MinuteExecutionMap,
    *,
    mask: np.ndarray,
    side: np.ndarray,
) -> list[V71Signal]:
    availability = event["availability_ns"].to_numpy(np.int64)
    years = event["calendar_year"].to_numpy(int)
    session_days = event["session_day"].astype(str).to_numpy()
    contracts = event["contract"].astype(str).to_numpy()
    bar_types = event["bar_type"].astype(str).to_numpy()
    positions = np.flatnonzero(mask & execution.valid)
    if positions.size == 0:
        return []
    decisions = availability[positions]
    if np.any(decisions[1:] < decisions[:-1]):
        raise V71EventTimeError("candidate event order is not chronological")
    output: list[V71Signal] = []
    cursor = 0
    while cursor < len(positions):
        position = int(positions[cursor])
        decision_ns = int(availability[position])
        entry_stamp = int(execution.entry_ns[position])
        exit_stamp = int(execution.exit_ns[position])
        feature = {
            "candidate_id": spec.candidate_id,
            "source_event_position": int(position),
            "source_bar_type": str(bar_types[position]),
            "availability_ns": decision_ns,
            "side": int(side[position]),
            "contract": str(contracts[position]),
        }
        output.append(
            V71Signal(
                candidate_id=spec.candidate_id,
                family_id=spec.family_id,
                motif=spec.motif,
                response_policy=spec.response_policy,
                holding_minutes=spec.holding_minutes,
                calendar_year=int(years[position]),
                session_day=str(session_days[position]),
                source_position=int(position),
                availability_ns=decision_ns,
                decision_ns=decision_ns,
                entry_minute_start_ns=entry_stamp,
                exit_minute_start_ns=exit_stamp,
                side=int(side[position]),
                contract=str(contracts[position]),
                feature_snapshot_hash=_stable_hash(feature),
            )
        )
        cursor = int(np.searchsorted(decisions, exit_stamp, side="left"))
    return output


def _build_execution_maps(
    event: pd.DataFrame, minute: pd.DataFrame
) -> dict[int, MinuteExecutionMap]:
    availability = event["availability_ns"].to_numpy(np.int64)
    event_years = event["calendar_year"].to_numpy(int)
    event_days = event["session_day"].astype(str).to_numpy()
    event_contracts = event["contract"].astype(str).to_numpy()
    maps: dict[int, MinuteExecutionMap] = {}
    minute_by_year = {
        int(year): frame.reset_index(drop=True)
        for year, frame in minute.groupby("calendar_year", sort=True)
    }
    for horizon in HORIZONS:
        valid = np.zeros(len(event), dtype=bool)
        entry_ns = np.full(len(event), -1, dtype=np.int64)
        exit_ns = np.full(len(event), -1, dtype=np.int64)
        for year, execution in minute_by_year.items():
            event_positions = np.flatnonzero(event_years == year)
            starts = execution["minute_start_ns"].to_numpy(np.int64)
            contracts = execution["contract"].astype(str).to_numpy()
            timestamps = pd.to_datetime(starts, unit="ns", utc=True).tz_convert(
                "America/Chicago"
            )
            days = np.asarray([value.isoformat() for value in timestamps.date])
            entries = np.searchsorted(
                starts, availability[event_positions], side="left"
            )
            exits = entries + horizon
            bounded = (entries < len(starts)) & (exits < len(starts))
            bounded_positions = event_positions[bounded]
            bounded_entries = entries[bounded]
            bounded_exits = exits[bounded]
            if bounded_positions.size == 0:
                continue
            coherent = (
                (starts[bounded_entries] >= availability[bounded_positions])
                & (days[bounded_entries] == event_days[bounded_positions])
                & (days[bounded_entries] == days[bounded_exits])
                & (contracts[bounded_entries] == event_contracts[bounded_positions])
                & (contracts[bounded_entries] == contracts[bounded_exits])
            )
            retained = bounded_positions[coherent]
            retained_entries = bounded_entries[coherent]
            retained_exits = bounded_exits[coherent]
            valid[retained] = True
            entry_ns[retained] = starts[retained_entries]
            exit_ns[retained] = starts[retained_exits]
        maps[horizon] = MinuteExecutionMap(
            valid=valid, entry_ns=entry_ns, exit_ns=exit_ns
        )
    return maps


def _past_quantile(
    event: pd.DataFrame,
    values: np.ndarray,
    group_columns: list[str],
    quantile: float,
) -> np.ndarray:
    source = pd.Series(values, index=event.index)
    grouping = [event[column] for column in group_columns]
    return (
        source.groupby(grouping, sort=False)
        .transform(
            lambda rows: rows.shift(1)
            .rolling(ROLLING_EVENTS, min_periods=ROLLING_EVENTS)
            .quantile(quantile)
        )
        .to_numpy(float)
    )


def _group_shift(
    event: pd.DataFrame, values: np.ndarray, group_columns: list[str]
) -> np.ndarray:
    source = pd.Series(values, index=event.index)
    grouping = [event[column] for column in group_columns]
    return source.groupby(grouping, sort=False).shift(1).to_numpy(float)


def _fallback_direction(primary: np.ndarray, fallback: np.ndarray) -> np.ndarray:
    output = np.asarray(primary, dtype=np.int8).copy()
    missing = output == 0
    output[missing] = np.asarray(fallback, dtype=np.int8)[missing]
    return output


def _direction(values: np.ndarray) -> np.ndarray:
    return np.sign(np.nan_to_num(values, nan=0.0)).astype(np.int8)


def _chicago_day(timestamp_ns: int) -> str:
    return (
        pd.Timestamp(timestamp_ns, unit="ns", tz="UTC")
        .tz_convert("America/Chicago")
        .date()
        .isoformat()
    )


def _load_grammar(root: Path) -> dict[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71EventTimeError("event-time WORM hash mismatch")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID:
        raise V71EventTimeError("event-time grammar ID drift")
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EventTimeSourceAudit",
    "GRAMMAR_ID",
    "V71EventTimeError",
    "candidate_specs",
    "generate_signal_population",
    "load_event_time_sources",
    "signal_path_hash",
]
