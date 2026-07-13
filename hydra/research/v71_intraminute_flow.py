from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import V71CandidateSpec, V71Signal
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_intraminute_flow_grammar_0008"
GRAMMAR_PATH = "WORM/v7.1-intraminute-flow-grammar-0008-2026-07-13.json"
GRAMMAR_SHA256 = "36f5d4f8dd2582979d809925782881fb1e159d23ddfbd50dc6a9d348cf5c18dc"
FEATURE_PATH = "data/cache/v7_d1/date_matched_intraminute_flow_v1.parquet"
FEATURE_SHA256 = "13dd79815211e8461dea2b708691c656841738d7af06c22cab7d7b7d688c0196"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_intraminute_flow_v1.json"
FEATURE_MANIFEST_SHA256 = "b228dc89ed36d1b47660073dd6e68703eb44c85e6c0e5897a62f3a14168f6ad4"
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
HORIZONS = (30, 60)
MOTIF_POLICIES = {
    "BACK_LOADED_SAME_SIGN_ACCELERATION": "CONTINUATION",
    "FRONT_LOADED_FLOW_DECAY": "REVERSAL",
    "LATE_FLOW_HANDOFF": "CONTINUATION",
}
MINUTE_NS = 60_000_000_000


class V71IntraminuteFlowError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class IntraminuteFlowSourceAudit:
    minute_count: int
    session_count: int
    exact_source_match_count: int
    back_loaded_same_sign_acceleration_count: int
    front_loaded_flow_decay_count: int
    late_flow_handoff_count: int
    executable_state_count_30m: int
    executable_state_count_60m: int

    def to_dict(self) -> dict[str, int]:
        return {field: int(getattr(self, field)) for field in self.__dataclass_fields__}


def candidate_specs(project_root: str | Path = ".") -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = {str(value) for value in grammar["candidate_ids"]}
    rows: list[V71CandidateSpec] = []
    for motif, response in MOTIF_POLICIES.items():
        for horizon in HORIZONS:
            candidate_id = f"v71g8_intraminute_flow_{motif.lower()}_{response.lower()}_h{horizon}"
            payload = {
                "grammar_id": GRAMMAR_ID,
                "grammar_sha256": GRAMMAR_SHA256,
                "feature_sha256": FEATURE_SHA256,
                "candidate_id": candidate_id,
                "family_id": "INTRAMINUTE_AGGRESSOR_FLOW_ALLOCATION",
                "mechanism_class": "v71g8_intraminute_aggressor_flow_allocation",
                "motif": motif,
                "response_policy": response,
                "holding_minutes": horizon,
                "cost_horizon": f"{horizon}m",
                "product": "ES",
            }
            rows.append(
                V71CandidateSpec(
                    candidate_id=candidate_id,
                    family_id="INTRAMINUTE_AGGRESSOR_FLOW_ALLOCATION",
                    mechanism_class="v71g8_intraminute_aggressor_flow_allocation",
                    motif=motif,
                    response_policy=response,
                    holding_minutes=horizon,
                    cost_horizon=f"{horizon}m",
                    product="ES",
                    specification_hash=_stable_hash(payload),
                )
            )
    if len(rows) != 6 or len({row.candidate_id for row in rows}) != 6 or {row.candidate_id for row in rows} != frozen_ids:
        raise V71IntraminuteFlowError("intraminute candidate identity drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_intraminute_flow_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, IntraminuteFlowSourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        FEATURE_PATH: FEATURE_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        MINUTE_PATH: MINUTE_SHA256,
    }
    drift = [path for path, sha in checks.items() if _sha256(root / path) != sha]
    if drift:
        raise V71IntraminuteFlowError("intraminute frozen source drift: " + ",".join(drift))
    feature = pd.read_parquet(root / FEATURE_PATH)
    minute = pd.read_parquet(root / MINUTE_PATH)
    minute = minute[minute["product"] == "ES"].copy()
    minute = minute.sort_values(["calendar_year", "minute_start_ns", "contract"], kind="stable").reset_index(drop=True)
    states, audit = build_intraminute_flow_states(feature, minute)
    return minute, states, audit


def build_intraminute_flow_states(
    feature: pd.DataFrame,
    minute: pd.DataFrame,
) -> tuple[pd.DataFrame, IntraminuteFlowSourceAudit]:
    feature_required = {
        "calendar_year", "contract", "minute_start_ns", "availability_ns",
        "first_total_volume", "second_total_volume", "first_signed_flow", "second_signed_flow",
    }
    minute_required = {"calendar_year", "contract", "minute_start_ns", "availability_ns"}
    if feature_required.difference(feature.columns) or minute_required.difference(minute.columns):
        raise V71IntraminuteFlowError("intraminute source fields missing")
    keys = ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    frame = feature.merge(minute[keys], on=keys, how="inner", validate="one_to_one")
    frame = frame.sort_values(["calendar_year", "minute_start_ns", "contract"], kind="stable").reset_index(drop=True)
    if len(frame) != len(feature) or len(frame) != len(minute):
        raise V71IntraminuteFlowError("intraminute/minute exact source mismatch")
    timestamps = pd.to_datetime(frame["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True).tz_convert("America/Chicago")
    frame["session_day"] = np.asarray([value.isoformat() for value in timestamps.date])
    first = frame["first_signed_flow"].to_numpy(np.float64)
    second = frame["second_signed_flow"].to_numpy(np.float64)
    first_volume = frame["first_total_volume"].to_numpy(np.float64)
    second_volume = frame["second_total_volume"].to_numpy(np.float64)
    first_sign = np.sign(first).astype(np.int8)
    second_sign = np.sign(second).astype(np.int8)
    nonzero = (first_sign != 0) & (second_sign != 0)
    same = first_sign == second_sign
    states = {
        "BACK_LOADED_SAME_SIGN_ACCELERATION": nonzero & same & (np.abs(second) > np.abs(first)) & (second_volume > first_volume),
        "FRONT_LOADED_FLOW_DECAY": nonzero & same & (np.abs(first) > np.abs(second)) & (first_volume > second_volume),
        "LATE_FLOW_HANDOFF": nonzero & ~same & (np.abs(second) > np.abs(first)),
    }
    for motif, mask in states.items():
        frame[f"state_{motif}"] = mask
    frame["first_flow_sign"] = first_sign
    frame["second_flow_sign"] = second_sign
    starts = frame["minute_start_ns"].to_numpy(np.int64)
    decisions = frame["availability_ns"].to_numpy(np.int64)
    contracts = frame["contract"].astype(str).to_numpy()
    session_days = frame["session_day"].astype(str).to_numpy()
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
    audit = IntraminuteFlowSourceAudit(
        minute_count=len(frame),
        session_count=int(frame[["contract", "session_day"]].drop_duplicates().shape[0]),
        exact_source_match_count=len(frame),
        back_loaded_same_sign_acceleration_count=int(states["BACK_LOADED_SAME_SIGN_ACCELERATION"].sum()),
        front_loaded_flow_decay_count=int(states["FRONT_LOADED_FLOW_DECAY"].sum()),
        late_flow_handoff_count=int(states["LATE_FLOW_HANDOFF"].sum()),
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
        if "v71g8_intraminute_aggressor_flow_allocation" in dead:
            raise V71IntraminuteFlowError("intraminute mechanism is tombstoned")
    output = {spec.candidate_id: tuple(_signals_for_spec(spec, states)) for spec in specs}
    if set(output) != {row.candidate_id for row in specs}:
        raise V71IntraminuteFlowError("intraminute signal population drift")
    return dict(sorted(output.items()))


def signal_path_hash(signals: Sequence[V71Signal]) -> str:
    return _stable_hash([(row.decision_ns, row.entry_minute_start_ns, row.exit_minute_start_ns, row.side, row.contract) for row in signals])


def _signals_for_spec(spec: V71CandidateSpec, states: pd.DataFrame) -> list[V71Signal]:
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    mask &= states[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    if spec.motif == "FRONT_LOADED_FLOW_DECAY":
        side = -states["first_flow_sign"].to_numpy(np.int8)
    else:
        side = states["second_flow_sign"].to_numpy(np.int8)
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
            "feature_sha256": FEATURE_SHA256,
            "candidate_id": spec.candidate_id,
            "position": int(position),
            "decision_ns": decision,
            "first_total_volume": int(row["first_total_volume"]),
            "second_total_volume": int(row["second_total_volume"]),
            "first_signed_flow": int(row["first_signed_flow"]),
            "second_signed_flow": int(row["second_signed_flow"]),
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


def _load_grammar(root: Path) -> Mapping[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71IntraminuteFlowError("intraminute WORM hash drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID or int(payload.get("candidate_count", 0)) != 6:
        raise V71IntraminuteFlowError("intraminute grammar identity drift")
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "GRAMMAR_ID",
    "IntraminuteFlowSourceAudit",
    "V71IntraminuteFlowError",
    "build_intraminute_flow_states",
    "candidate_specs",
    "generate_signal_population",
    "load_intraminute_flow_sources",
    "signal_path_hash",
]
