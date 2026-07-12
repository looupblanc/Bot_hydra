from __future__ import annotations

import hashlib
import json
from collections import Counter
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


GRAMMAR_ID = "hydra_v7_1_cross_clock_flow_grammar_0004"
GRAMMAR_PATH = "WORM/v7.1-cross-clock-flow-grammar-0004-2026-07-12.json"
GRAMMAR_SHA256 = "9341e576b4090f2626079f1678170ad738b523ca10394b89261292a3ee1b2c0e"
HORIZONS = (30, 60)
RESPONSES = ("CONTINUATION", "REVERSAL")
MOTIFS = (
    "FLOW_SIGN_AGREEMENT",
    "FLOW_AND_PROGRESS_AGREEMENT",
    "FLOW_SIGN_DISAGREEMENT",
)
MINUTE_NS = 60_000_000_000


class V71CrossClockFlowError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CrossClockSourceAudit:
    volume_event_count: int
    dollar_event_count: int
    aligned_availability_minute_count: int
    same_contract_session_pair_count: int
    executable_pair_count_30m: int
    executable_pair_count_60m: int

    def to_dict(self) -> dict[str, int]:
        return {
            "volume_event_count": self.volume_event_count,
            "dollar_event_count": self.dollar_event_count,
            "aligned_availability_minute_count": self.aligned_availability_minute_count,
            "same_contract_session_pair_count": self.same_contract_session_pair_count,
            "executable_pair_count_30m": self.executable_pair_count_30m,
            "executable_pair_count_60m": self.executable_pair_count_60m,
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
                    f"v71g4_cross_clock_flow_confirmation_{motif.lower()}_"
                    f"{response.lower()}_h{horizon}"
                )
                payload = {
                    "grammar_id": GRAMMAR_ID,
                    "grammar_sha256": GRAMMAR_SHA256,
                    "candidate_id": candidate_id,
                    "family_id": "CROSS_CLOCK_FLOW_CONFIRMATION",
                    "mechanism_class": "v71g4_cross_clock_flow_confirmation",
                    "motif": motif,
                    "response_policy": response,
                    "holding_minutes": horizon,
                    "cost_horizon": f"{horizon}m",
                    "product": "ES",
                }
                rows.append(
                    V71CandidateSpec(
                        candidate_id=candidate_id,
                        family_id="CROSS_CLOCK_FLOW_CONFIRMATION",
                        mechanism_class="v71g4_cross_clock_flow_confirmation",
                        motif=motif,
                        response_policy=response,
                        holding_minutes=horizon,
                        cost_horizon=f"{horizon}m",
                        product="ES",
                        specification_hash=_stable_hash(payload),
                    )
                )
    if len(rows) != 12 or len({row.candidate_id for row in rows}) != 12:
        raise V71CrossClockFlowError("cross-clock grammar must contain 12 candidates")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_cross_clock_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, CrossClockSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    minute, event, _ = load_event_time_sources(root)
    pairs, audit = build_cross_clock_pairs(minute, event)
    return minute, pairs, audit


def build_cross_clock_pairs(
    minute: pd.DataFrame,
    event: pd.DataFrame,
) -> tuple[pd.DataFrame, CrossClockSourceAudit]:
    """Build the frozen cross-clock representation from supplied price worlds."""
    selected = event[event["bar_type"].isin(("VOLUME_BAR", "DOLLAR_BAR"))].copy()
    selected["availability_minute"] = (
        selected["availability_ns"].to_numpy(np.int64) // MINUTE_NS
    )
    keys = ["calendar_year", "contract", "session_day", "availability_minute"]
    by_type: dict[str, pd.DataFrame] = {}
    for bar_type in ("VOLUME_BAR", "DOLLAR_BAR"):
        frame = selected[selected["bar_type"] == bar_type].sort_values(
            [*keys, "availability_ns", "bar_sequence"], kind="stable"
        )
        frame = frame.groupby(keys, sort=True, as_index=False).tail(1)
        rename = {
            column: f"{bar_type.lower()}__{column}"
            for column in (
                "start_event_ns",
                "end_event_ns",
                "availability_ns",
                "signed_aggressor_volume",
                "price_change_points",
                "path_length_points",
            )
        }
        by_type[bar_type] = frame[[*keys, *rename]].rename(columns=rename)
    pairs = by_type["VOLUME_BAR"].merge(
        by_type["DOLLAR_BAR"], on=keys, how="inner", validate="one_to_one"
    )
    pairs["decision_ns"] = np.maximum(
        pairs["volume_bar__availability_ns"].to_numpy(np.int64),
        pairs["dollar_bar__availability_ns"].to_numpy(np.int64),
    )
    pairs = pairs.sort_values(
        ["calendar_year", "decision_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    starts = minute["minute_start_ns"].to_numpy(np.int64)
    contracts = minute["contract"].astype(str).to_numpy()
    session_days = _minute_session_days(minute)
    for horizon in HORIZONS:
        entries = np.searchsorted(starts, pairs["decision_ns"].to_numpy(np.int64), side="left")
        valid_entry = entries < len(starts)
        entry_starts = np.full(len(pairs), -1, dtype=np.int64)
        entry_starts[valid_entry] = starts[entries[valid_entry]]
        targets = entry_starts + horizon * MINUTE_NS
        exits = np.searchsorted(starts, targets, side="left")
        valid = valid_entry & (exits < len(starts))
        valid &= np.where(valid, starts[np.minimum(exits, len(starts) - 1)] == targets, False)
        valid &= np.where(valid, contracts[np.minimum(entries, len(starts) - 1)] == pairs["contract"].astype(str), False)
        valid &= np.where(valid, contracts[np.minimum(exits, len(starts) - 1)] == pairs["contract"].astype(str), False)
        valid &= np.where(valid, session_days[np.minimum(entries, len(starts) - 1)] == pairs["session_day"].astype(str), False)
        valid &= np.where(valid, session_days[np.minimum(exits, len(starts) - 1)] == pairs["session_day"].astype(str), False)
        pairs[f"entry_ns_{horizon}"] = entry_starts
        pairs[f"exit_ns_{horizon}"] = np.where(valid, targets, -1)
        pairs[f"executable_{horizon}"] = valid
    audit = CrossClockSourceAudit(
        volume_event_count=int((selected["bar_type"] == "VOLUME_BAR").sum()),
        dollar_event_count=int((selected["bar_type"] == "DOLLAR_BAR").sum()),
        aligned_availability_minute_count=len(pairs),
        same_contract_session_pair_count=len(pairs),
        executable_pair_count_30m=int(pairs["executable_30"].sum()),
        executable_pair_count_60m=int(pairs["executable_60"].sum()),
    )
    return pairs, audit


def generate_signal_population(
    minute: pd.DataFrame,
    pairs: pd.DataFrame,
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
        if "v71g4_cross_clock_flow_confirmation" in dead:
            raise V71CrossClockFlowError("cross-clock mechanism is tombstoned")
    states = _motif_states(pairs)
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        mask, direction = states[spec.motif]
        side = direction if spec.response_policy == "CONTINUATION" else -direction
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, pairs, mask=mask, side=side)
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V71CrossClockFlowError("cross-clock signal population drift")
    return dict(sorted(output.items()))


def _motif_states(pairs: pd.DataFrame) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    volume_flow = np.sign(
        pairs["volume_bar__signed_aggressor_volume"].to_numpy(float)
    ).astype(np.int8)
    dollar_flow = np.sign(
        pairs["dollar_bar__signed_aggressor_volume"].to_numpy(float)
    ).astype(np.int8)
    volume_progress = np.sign(
        pairs["volume_bar__price_change_points"].to_numpy(float)
    ).astype(np.int8)
    dollar_progress = np.sign(
        pairs["dollar_bar__price_change_points"].to_numpy(float)
    ).astype(np.int8)
    nonzero = (volume_flow != 0) & (dollar_flow != 0)
    agreement = nonzero & (volume_flow == dollar_flow)
    return {
        "FLOW_SIGN_AGREEMENT": (agreement, volume_flow),
        "FLOW_AND_PROGRESS_AGREEMENT": (
            agreement
            & (volume_progress == volume_flow)
            & (dollar_progress == dollar_flow),
            volume_flow,
        ),
        "FLOW_SIGN_DISAGREEMENT": (
            nonzero & (volume_flow == -dollar_flow), volume_flow
        ),
    }


def _signals_for_spec(
    spec: V71CandidateSpec,
    pairs: pd.DataFrame,
    *,
    mask: np.ndarray,
    side: np.ndarray,
) -> list[V71Signal]:
    signals: list[V71Signal] = []
    next_allowed_by_day: dict[str, int] = {}
    for position in np.flatnonzero(mask & pairs[f"executable_{spec.holding_minutes}"].to_numpy(bool)):
        row = pairs.iloc[int(position)]
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
            "volume_availability_ns": int(row["volume_bar__availability_ns"]),
            "dollar_availability_ns": int(row["dollar_bar__availability_ns"]),
            "volume_flow": float(row["volume_bar__signed_aggressor_volume"]),
            "dollar_flow": float(row["dollar_bar__signed_aggressor_volume"]),
            "volume_progress": float(row["volume_bar__price_change_points"]),
            "dollar_progress": float(row["dollar_bar__price_change_points"]),
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
        raise V71CrossClockFlowError("cross-clock WORM hash drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID or int(payload.get("candidate_count", 0)) != 12:
        raise V71CrossClockFlowError("cross-clock grammar identity drift")
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CrossClockSourceAudit",
    "GRAMMAR_ID",
    "V71CrossClockFlowError",
    "build_cross_clock_pairs",
    "candidate_specs",
    "generate_signal_population",
    "load_cross_clock_sources",
    "signal_path_hash",
]
