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


GRAMMAR_ID = "hydra_v7_1_flow_sign_sequence_grammar_0007"
GRAMMAR_PATH = "WORM/v7.1-flow-sign-sequence-grammar-0007-2026-07-13.json"
GRAMMAR_SHA256 = "4cb89b0e774f754037fde8a6f86703cda0047eefcd01174e1f65bb8d37fc45ab"
HORIZONS = (30, 60)
MOTIFS = (
    "RUN_TERMINATION_HANDOFF",
    "RUN_RESTART_AFTER_ONE_COUNTER",
    "ALTERNATION_BREAK_TO_PERSISTENCE",
)
RESPONSE_POLICY = "CURRENT_FLOW_CONTINUATION"
MINUTE_NS = 60_000_000_000


class V71FlowSignSequenceError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FlowSignSequenceSourceAudit:
    minute_count: int
    session_count: int
    nonzero_flow_minute_count: int
    contiguous_five_minute_count: int
    run_termination_handoff_count: int
    run_restart_after_one_counter_count: int
    alternation_break_to_persistence_count: int
    executable_state_count_30m: int
    executable_state_count_60m: int

    def to_dict(self) -> dict[str, int]:
        return {
            field: int(getattr(self, field))
            for field in self.__dataclass_fields__
        }


def candidate_specs(project_root: str | Path = ".") -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = {str(value) for value in grammar["candidate_ids"]}
    rows: list[V71CandidateSpec] = []
    for motif in MOTIFS:
        for horizon in HORIZONS:
            candidate_id = (
                "v71g7_flow_sign_sequence_"
                f"{motif.lower()}_{RESPONSE_POLICY.lower()}_h{horizon}"
            )
            payload = {
                "grammar_id": GRAMMAR_ID,
                "grammar_sha256": GRAMMAR_SHA256,
                "candidate_id": candidate_id,
                "family_id": "AGGRESSOR_FLOW_SIGN_SEQUENCES",
                "mechanism_class": "v71g7_aggressor_flow_sign_sequences",
                "motif": motif,
                "response_policy": RESPONSE_POLICY,
                "holding_minutes": horizon,
                "cost_horizon": f"{horizon}m",
                "product": "ES",
            }
            rows.append(
                V71CandidateSpec(
                    candidate_id=candidate_id,
                    family_id="AGGRESSOR_FLOW_SIGN_SEQUENCES",
                    mechanism_class="v71g7_aggressor_flow_sign_sequences",
                    motif=motif,
                    response_policy=RESPONSE_POLICY,
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
        raise V71FlowSignSequenceError("flow-sign candidate identity drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_flow_sign_sequence_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, FlowSignSequenceSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    minute, _, _ = load_event_time_sources(root)
    states, audit = build_flow_sign_sequence_states(minute)
    return minute, states, audit


def build_flow_sign_sequence_states(
    minute: pd.DataFrame,
) -> tuple[pd.DataFrame, FlowSignSequenceSourceAudit]:
    required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "signed_aggressor_volume",
    }
    missing = sorted(required.difference(minute.columns))
    if missing:
        raise V71FlowSignSequenceError(
            "flow-sign source fields missing: " + ",".join(missing)
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
    sign = np.sign(
        frame["signed_aggressor_volume"].to_numpy(np.float64)
    ).astype(np.int8)
    starts = frame["minute_start_ns"].to_numpy(np.int64)
    contracts = frame["contract"].astype(str).to_numpy()
    session_days = frame["session_day"].astype(str).to_numpy()
    contiguous = np.zeros(len(frame), dtype=bool)
    contiguous[1:] = (
        (starts[1:] - starts[:-1] == MINUTE_NS)
        & (contracts[1:] == contracts[:-1])
        & (session_days[1:] == session_days[:-1])
    )

    lag1 = _lag(sign, 1)
    lag2 = _lag(sign, 2)
    lag3 = _lag(sign, 3)
    lag4 = _lag(sign, 4)
    contig3 = contiguous & _lag(contiguous, 1) & _lag(contiguous, 2)
    contig4 = contig3 & _lag(contiguous, 3)
    all_nonzero4 = (sign != 0) & (lag1 != 0) & (lag2 != 0) & (lag3 != 0)
    all_nonzero5 = all_nonzero4 & (lag4 != 0)

    run_termination = (
        contig3
        & all_nonzero4
        & (lag1 == lag2)
        & (lag2 == lag3)
        & (sign == -lag1)
    )
    run_restart = (
        contig4
        & all_nonzero5
        & (lag2 == lag3)
        & (lag3 == lag4)
        & (lag1 == -lag2)
        & (sign == lag2)
    )
    alternation_break = (
        contig4
        & all_nonzero5
        & (lag4 == -lag3)
        & (lag3 == -lag2)
        & (lag2 == -lag1)
        & (sign == lag1)
    )
    raw_states = {
        "RUN_TERMINATION_HANDOFF": run_termination,
        "RUN_RESTART_AFTER_ONE_COUNTER": run_restart,
        "ALTERNATION_BREAK_TO_PERSISTENCE": alternation_break,
    }
    for motif, mask in raw_states.items():
        frame[f"state_{motif}"] = mask
    frame["flow_sign"] = sign

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

    audit = FlowSignSequenceSourceAudit(
        minute_count=len(frame),
        session_count=int(frame[["contract", "session_day"]].drop_duplicates().shape[0]),
        nonzero_flow_minute_count=int((sign != 0).sum()),
        contiguous_five_minute_count=int(contig4.sum()),
        run_termination_handoff_count=int(run_termination.sum()),
        run_restart_after_one_counter_count=int(run_restart.sum()),
        alternation_break_to_persistence_count=int(alternation_break.sum()),
        executable_state_count_30m=int(frame["executable_30"].sum()),
        executable_state_count_60m=int(frame["executable_60"].sum()),
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
        dead = {str(row["mechanism_class"]) for row in class_feedback(graveyard_path)}
        if "v71g7_aggressor_flow_sign_sequences" in dead:
            raise V71FlowSignSequenceError("flow-sign mechanism is tombstoned")
    output = {
        spec.candidate_id: tuple(_signals_for_spec(spec, states))
        for spec in specs
    }
    if set(output) != {row.candidate_id for row in specs}:
        raise V71FlowSignSequenceError("flow-sign signal population drift")
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
    side = states["flow_sign"].to_numpy(np.int8)
    mask &= side != 0
    signals: list[V71Signal] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for position in np.flatnonzero(mask):
        row = states.iloc[int(position)]
        key = (str(row["contract"]), str(row["session_day"]))
        decision = int(row["availability_ns"])
        if decision < next_allowed.get(key, -1):
            continue
        entry = int(row[f"entry_ns_{spec.holding_minutes}"])
        exit_ns = int(row[f"exit_ns_{spec.holding_minutes}"])
        snapshot = {
            "grammar_sha256": GRAMMAR_SHA256,
            "candidate_id": spec.candidate_id,
            "position": int(position),
            "decision_ns": decision,
            "motif": spec.motif,
            "flow_signs": [
                int(value)
                for value in states["flow_sign"].iloc[max(0, int(position) - 4): int(position) + 1]
            ],
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
        next_allowed[key] = exit_ns
    return signals


def _lag(values: np.ndarray, periods: int) -> np.ndarray:
    result = np.zeros(len(values), dtype=values.dtype)
    if periods < len(values):
        result[periods:] = values[:-periods]
    return result


def _load_grammar(root: Path) -> Mapping[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71FlowSignSequenceError("flow-sign WORM hash drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID or int(payload.get("candidate_count", 0)) != 6:
        raise V71FlowSignSequenceError("flow-sign grammar identity drift")
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
    "GRAMMAR_ID",
    "FlowSignSequenceSourceAudit",
    "V71FlowSignSequenceError",
    "build_flow_sign_sequence_states",
    "candidate_specs",
    "generate_signal_population",
    "load_flow_sign_sequence_sources",
    "signal_path_hash",
]
