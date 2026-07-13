from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import V71CandidateSpec, V71Signal
from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_trade_size_composition_grammar_0006"
GRAMMAR_PATH = "WORM/v7.1-trade-size-composition-grammar-0006-2026-07-13.json"
GRAMMAR_SHA256 = "3913324e3ab9b707461da4c32a5c4bddfa025af98c5a5f6ba942b7ae0ba7cc29"
HORIZONS = (30, 60)
MOTIF_POLICIES = {
    "LARGE_CLIP_FLOW_ONSET": "CONTINUATION",
    "LARGE_CLIP_ABSORPTION": "REVERSAL",
    "SMALL_CLIP_PARTICIPATION_BURST": "CONTINUATION",
}
MINUTE_NS = 60_000_000_000


class V71TradeSizeCompositionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TradeSizeCompositionSourceAudit:
    minute_count: int
    session_count: int
    same_contract_prior_session_baseline_count: int
    large_clip_flow_onset_count: int
    large_clip_absorption_count: int
    small_clip_participation_burst_count: int
    executable_state_count_30m: int
    executable_state_count_60m: int

    def to_dict(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


def candidate_specs(
    project_root: str | Path = ".",
) -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = set(str(value) for value in grammar["candidate_ids"])
    rows: list[V71CandidateSpec] = []
    for motif, response in MOTIF_POLICIES.items():
        for horizon in HORIZONS:
            candidate_id = (
                "v71g6_trade_size_composition_"
                f"{motif.lower()}_{response.lower()}_h{horizon}"
            )
            payload = {
                "grammar_id": GRAMMAR_ID,
                "grammar_sha256": GRAMMAR_SHA256,
                "candidate_id": candidate_id,
                "family_id": "TRADE_SIZE_COMPOSITION_TRANSITIONS",
                "mechanism_class": "v71g6_trade_size_composition_transitions",
                "motif": motif,
                "response_policy": response,
                "holding_minutes": horizon,
                "cost_horizon": f"{horizon}m",
                "product": "ES",
            }
            rows.append(
                V71CandidateSpec(
                    candidate_id=candidate_id,
                    family_id="TRADE_SIZE_COMPOSITION_TRANSITIONS",
                    mechanism_class="v71g6_trade_size_composition_transitions",
                    motif=motif,
                    response_policy=response,
                    holding_minutes=horizon,
                    cost_horizon=f"{horizon}m",
                    product="ES",
                    specification_hash=_stable_hash(payload),
                )
            )
    if (
        len(rows) != 6
        or len({row.candidate_id for row in rows}) != 6
        or {row.candidate_id for row in rows} != frozen_ids
    ):
        raise V71TradeSizeCompositionError(
            "trade-size composition candidate identity drift"
        )
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_trade_size_composition_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, TradeSizeCompositionSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    minute, _, _ = load_event_time_sources(root)
    states, audit = build_trade_size_composition_states(minute)
    return minute, states, audit


def build_trade_size_composition_states(
    minute: pd.DataFrame,
) -> tuple[pd.DataFrame, TradeSizeCompositionSourceAudit]:
    required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "trade_count",
        "total_volume",
        "signed_aggressor_volume",
        "signed_aggressor_fraction",
        "price_change_points",
    }
    missing = sorted(required.difference(minute.columns))
    if missing:
        raise V71TradeSizeCompositionError(
            "trade-size source fields missing: " + ",".join(missing)
        )
    frame = minute.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True).copy()
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["session_day"] = np.asarray(
        [value.isoformat() for value in timestamps.date]
    )
    trade_count = frame["trade_count"].to_numpy(np.float64)
    total_volume = frame["total_volume"].to_numpy(np.float64)
    frame["average_trade_size"] = np.divide(
        total_volume,
        trade_count,
        out=np.full(len(frame), np.nan, dtype=np.float64),
        where=trade_count > 0.0,
    )
    frame["absolute_signed_fraction"] = frame[
        "signed_aggressor_fraction"
    ].abs()
    group_keys = ["contract", "session_day"]
    session = (
        frame.groupby(group_keys, sort=True, as_index=False)
        .agg(
            session_start_ns=("minute_start_ns", "min"),
            baseline_average_trade_size=("average_trade_size", "median"),
            baseline_trade_count=("trade_count", "median"),
            baseline_absolute_signed_fraction=(
                "absolute_signed_fraction",
                "median",
            ),
        )
        .sort_values(["contract", "session_start_ns"], kind="stable")
    )
    baseline_columns = [
        "baseline_average_trade_size",
        "baseline_trade_count",
        "baseline_absolute_signed_fraction",
    ]
    for column in baseline_columns:
        session[f"prior_{column}"] = session.groupby(
            "contract", sort=False
        )[column].shift(1)
    frame = frame.merge(
        session[
            [
                *group_keys,
                *[f"prior_{column}" for column in baseline_columns],
            ]
        ],
        on=group_keys,
        how="left",
        validate="many_to_one",
    ).sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    has_baseline = frame[
        [f"prior_{column}" for column in baseline_columns]
    ].notna().all(axis=1).to_numpy(bool)
    avg_size = frame["average_trade_size"].to_numpy(np.float64)
    avg_baseline = frame[
        "prior_baseline_average_trade_size"
    ].to_numpy(np.float64)
    count = frame["trade_count"].to_numpy(np.float64)
    count_baseline = frame["prior_baseline_trade_count"].to_numpy(np.float64)
    abs_fraction = frame["absolute_signed_fraction"].to_numpy(np.float64)
    fraction_baseline = frame[
        "prior_baseline_absolute_signed_fraction"
    ].to_numpy(np.float64)
    flow_direction = np.sign(
        frame["signed_aggressor_volume"].to_numpy(np.float64)
    ).astype(np.int8)
    progress_direction = np.sign(
        frame["price_change_points"].to_numpy(np.float64)
    ).astype(np.int8)
    valid_flow = flow_direction != 0
    raw_states = {
        "LARGE_CLIP_FLOW_ONSET": (
            has_baseline & (avg_size > avg_baseline) & valid_flow
        ),
        "LARGE_CLIP_ABSORPTION": (
            has_baseline
            & (avg_size > avg_baseline)
            & (abs_fraction > fraction_baseline)
            & valid_flow
            & ((progress_direction == 0) | (progress_direction == -flow_direction))
        ),
        "SMALL_CLIP_PARTICIPATION_BURST": (
            has_baseline
            & (avg_size < avg_baseline)
            & (count > count_baseline)
            & valid_flow
            & (progress_direction == flow_direction)
        ),
    }
    same_group = (
        frame["contract"].eq(frame["contract"].shift(1))
        & frame["session_day"].eq(frame["session_day"].shift(1))
        & frame["minute_start_ns"].sub(frame["minute_start_ns"].shift(1)).eq(MINUTE_NS)
    ).to_numpy(bool)
    for motif, raw in raw_states.items():
        prior = np.zeros(len(frame), dtype=bool)
        prior[1:] = raw[:-1] & same_group[1:]
        frame[f"state_{motif}"] = raw & ~prior
    starts = frame["minute_start_ns"].to_numpy(np.int64)
    contracts = frame["contract"].astype(str).to_numpy()
    session_days = frame["session_day"].astype(str).to_numpy()
    decisions = frame["availability_ns"].to_numpy(np.int64)
    for horizon in HORIZONS:
        entries = np.searchsorted(starts, decisions, side="left")
        valid_entry = entries < len(starts)
        entry_starts = np.full(len(frame), -1, dtype=np.int64)
        entry_starts[valid_entry] = starts[entries[valid_entry]]
        targets = entry_starts + horizon * MINUTE_NS
        exits = np.searchsorted(starts, targets, side="left")
        safe_entry = np.minimum(entries, len(starts) - 1)
        safe_exit = np.minimum(exits, len(starts) - 1)
        valid = valid_entry & (exits < len(starts))
        valid &= starts[safe_entry] >= decisions
        valid &= starts[safe_exit] == targets
        valid &= contracts[safe_entry] == contracts
        valid &= contracts[safe_exit] == contracts
        valid &= session_days[safe_entry] == session_days
        valid &= session_days[safe_exit] == session_days
        frame[f"entry_ns_{horizon}"] = entry_starts
        frame[f"exit_ns_{horizon}"] = np.where(valid, targets, -1)
        frame[f"executable_{horizon}"] = valid
    audit = TradeSizeCompositionSourceAudit(
        minute_count=len(frame),
        session_count=len(session),
        same_contract_prior_session_baseline_count=int(has_baseline.sum()),
        large_clip_flow_onset_count=int(
            frame["state_LARGE_CLIP_FLOW_ONSET"].sum()
        ),
        large_clip_absorption_count=int(
            frame["state_LARGE_CLIP_ABSORPTION"].sum()
        ),
        small_clip_participation_burst_count=int(
            frame["state_SMALL_CLIP_PARTICIPATION_BURST"].sum()
        ),
        executable_state_count_30m=int(
            frame["executable_30"].sum()
        ),
        executable_state_count_60m=int(
            frame["executable_60"].sum()
        ),
    )
    return frame, audit


def generate_signal_population(
    states: pd.DataFrame,
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
        if "v71g6_trade_size_composition_transitions" in dead:
            raise V71TradeSizeCompositionError(
                "trade-size composition mechanism is tombstoned"
            )
    output = {
        spec.candidate_id: tuple(_signals_for_spec(spec, states))
        for spec in specs
    }
    if set(output) != {row.candidate_id for row in specs}:
        raise V71TradeSizeCompositionError(
            "trade-size composition signal population drift"
        )
    return dict(sorted(output.items()))


def signal_path_hash(signals: Sequence[V71Signal]) -> str:
    return _stable_hash(
        [
            (
                row.decision_ns,
                row.entry_minute_start_ns,
                row.exit_minute_start_ns,
                row.side,
                row.contract,
            )
            for row in signals
        ]
    )


def _signals_for_spec(
    spec: V71CandidateSpec,
    states: pd.DataFrame,
) -> list[V71Signal]:
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    mask &= states[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    direction = np.sign(
        states["signed_aggressor_volume"].to_numpy(np.float64)
    ).astype(np.int8)
    side = direction if spec.response_policy == "CONTINUATION" else -direction
    mask &= side != 0
    signals: list[V71Signal] = []
    next_allowed_by_contract_day: dict[tuple[str, str], int] = {}
    for position in np.flatnonzero(mask):
        row = states.iloc[int(position)]
        key = (str(row["contract"]), str(row["session_day"]))
        decision = int(row["availability_ns"])
        if decision < next_allowed_by_contract_day.get(key, -1):
            continue
        entry = int(row[f"entry_ns_{spec.holding_minutes}"])
        exit_ns = int(row[f"exit_ns_{spec.holding_minutes}"])
        snapshot = {
            "grammar_sha256": GRAMMAR_SHA256,
            "candidate_id": spec.candidate_id,
            "position": int(position),
            "decision_ns": decision,
            "average_trade_size": float(row["average_trade_size"]),
            "prior_average_trade_size_median": float(
                row["prior_baseline_average_trade_size"]
            ),
            "trade_count": float(row["trade_count"]),
            "prior_trade_count_median": float(
                row["prior_baseline_trade_count"]
            ),
            "absolute_signed_fraction": float(
                row["absolute_signed_fraction"]
            ),
            "prior_absolute_signed_fraction_median": float(
                row["prior_baseline_absolute_signed_fraction"]
            ),
            "signed_aggressor_volume": float(
                row["signed_aggressor_volume"]
            ),
            "price_change_points": float(row["price_change_points"]),
        }
        signals.append(
            V71Signal(
                candidate_id=spec.candidate_id,
                family_id=spec.family_id,
                motif=spec.motif,
                response_policy=spec.response_policy,
                holding_minutes=spec.holding_minutes,
                calendar_year=int(row["calendar_year"]),
                session_day=key[1],
                source_position=int(position),
                availability_ns=decision,
                decision_ns=decision,
                entry_minute_start_ns=entry,
                exit_minute_start_ns=exit_ns,
                side=int(side[int(position)]),
                contract=key[0],
                feature_snapshot_hash=_stable_hash(snapshot),
            )
        )
        next_allowed_by_contract_day[key] = exit_ns
    return signals


def _load_grammar(root: Path) -> Mapping[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71TradeSizeCompositionError(
            "trade-size composition WORM hash drift"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("grammar_id") != GRAMMAR_ID
        or int(payload.get("candidate_count", 0)) != 6
    ):
        raise V71TradeSizeCompositionError(
            "trade-size composition grammar identity drift"
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
    "TradeSizeCompositionSourceAudit",
    "V71TradeSizeCompositionError",
    "build_trade_size_composition_states",
    "candidate_specs",
    "generate_signal_population",
    "load_trade_size_composition_sources",
    "signal_path_hash",
]
