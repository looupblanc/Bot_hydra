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


GRAMMAR_ID = "hydra_v7_1_aggressor_run_topology_grammar_0009"
GRAMMAR_PATH = "WORM/v7.1-aggressor-run-topology-grammar-0009-2026-07-13.json"
GRAMMAR_SHA256 = "05ff83f0fbf902381371d3d840ce7393adadfa8e51d6c75e51a76c12a275bce2"
FEATURE_PATH = "data/cache/v7_d1/date_matched_aggressor_run_topology_v1.parquet"
FEATURE_SHA256 = "f7edf987c4280f467fe92bcbd9e1918774ff4fa29a607ed973ad91cc909e0039"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_aggressor_run_topology_v1.json"
FEATURE_MANIFEST_SHA256 = "b1151bdc493f569eda85d13983ce73df9f92cbfe9f2416fd4ac63183e251127c"
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
HORIZONS = (30, 60)
MOTIF_POLICIES = {
    "DOMINANT_RUN_WITH_PROGRESS": "CONTINUATION",
    "DOMINANT_RUN_WITHOUT_PROGRESS": "REVERSAL",
}
MINUTE_NS = 60_000_000_000


class V71AggressorRunTopologyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AggressorRunTopologySourceAudit:
    minute_count: int
    session_count: int
    exact_source_match_count: int
    unique_dominant_run_count: int
    dominant_run_with_progress_count: int
    dominant_run_without_progress_count: int
    tied_run_count: int
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
            candidate_id = (
                f"v71g9_aggressor_run_topology_{motif.lower()}_"
                f"{response.lower()}_h{horizon}"
            )
            payload = {
                "grammar_id": GRAMMAR_ID,
                "grammar_sha256": GRAMMAR_SHA256,
                "feature_sha256": FEATURE_SHA256,
                "candidate_id": candidate_id,
                "family_id": "INTRAMINUTE_AGGRESSOR_RUN_TOPOLOGY",
                "mechanism_class": "v71g9_intraminute_aggressor_run_topology",
                "motif": motif,
                "response_policy": response,
                "holding_minutes": horizon,
                "cost_horizon": f"{horizon}m",
                "product": "ES",
            }
            rows.append(
                V71CandidateSpec(
                    candidate_id=candidate_id,
                    family_id="INTRAMINUTE_AGGRESSOR_RUN_TOPOLOGY",
                    mechanism_class="v71g9_intraminute_aggressor_run_topology",
                    motif=motif,
                    response_policy=response,
                    holding_minutes=horizon,
                    cost_horizon=f"{horizon}m",
                    product="ES",
                    specification_hash=_stable_hash(payload),
                )
            )
    if (
        len(rows) != 4
        or len({row.candidate_id for row in rows}) != 4
        or {row.candidate_id for row in rows} != frozen_ids
    ):
        raise V71AggressorRunTopologyError("aggressor-run candidate identity drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_aggressor_run_topology_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, pd.DataFrame, AggressorRunTopologySourceAudit]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        FEATURE_PATH: FEATURE_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        MINUTE_PATH: MINUTE_SHA256,
    }
    drift = [path for path, sha in checks.items() if _sha256(root / path) != sha]
    if drift:
        raise V71AggressorRunTopologyError(
            "aggressor-run frozen source drift: " + ",".join(drift)
        )
    feature = pd.read_parquet(root / FEATURE_PATH)
    minute = pd.read_parquet(root / MINUTE_PATH)
    minute = minute[minute["product"] == "ES"].copy()
    minute = minute.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    states, audit = build_aggressor_run_topology_states(feature, minute)
    return minute, states, audit


def build_aggressor_run_topology_states(
    feature: pd.DataFrame,
    minute: pd.DataFrame,
) -> tuple[pd.DataFrame, AggressorRunTopologySourceAudit]:
    feature_required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "longest_buy_run",
        "longest_sell_run",
        "first_price",
        "last_price",
    }
    minute_required = {"calendar_year", "contract", "minute_start_ns", "availability_ns"}
    if feature_required.difference(feature.columns) or minute_required.difference(minute.columns):
        raise V71AggressorRunTopologyError("aggressor-run source fields missing")
    keys = ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    frame = feature.merge(minute[keys], on=keys, how="inner", validate="one_to_one")
    frame = frame.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    if len(frame) != len(feature) or len(frame) != len(minute):
        raise V71AggressorRunTopologyError("aggressor-run/minute exact source mismatch")
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["session_day"] = np.asarray([value.isoformat() for value in timestamps.date])
    buy_run = frame["longest_buy_run"].to_numpy(np.int64)
    sell_run = frame["longest_sell_run"].to_numpy(np.int64)
    dominant_side = np.sign(buy_run - sell_run).astype(np.int8)
    price_progress = np.sign(
        frame["last_price"].to_numpy(np.int64)
        - frame["first_price"].to_numpy(np.int64)
    ).astype(np.int8)
    unique = dominant_side != 0
    with_progress = unique & (price_progress == dominant_side)
    without_progress = unique & (price_progress != dominant_side)
    frame["state_DOMINANT_RUN_WITH_PROGRESS"] = with_progress
    frame["state_DOMINANT_RUN_WITHOUT_PROGRESS"] = without_progress
    frame["dominant_run_side"] = dominant_side
    frame["minute_price_progress_sign"] = price_progress
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
    audit = AggressorRunTopologySourceAudit(
        minute_count=len(frame),
        session_count=int(frame[["contract", "session_day"]].drop_duplicates().shape[0]),
        exact_source_match_count=len(frame),
        unique_dominant_run_count=int(unique.sum()),
        dominant_run_with_progress_count=int(with_progress.sum()),
        dominant_run_without_progress_count=int(without_progress.sum()),
        tied_run_count=int((~unique).sum()),
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
        if "v71g9_intraminute_aggressor_run_topology" in dead:
            raise V71AggressorRunTopologyError("aggressor-run mechanism is tombstoned")
    output = {spec.candidate_id: tuple(_signals_for_spec(spec, states)) for spec in specs}
    if set(output) != {row.candidate_id for row in specs}:
        raise V71AggressorRunTopologyError("aggressor-run signal population drift")
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


def _signals_for_spec(spec: V71CandidateSpec, states: pd.DataFrame) -> list[V71Signal]:
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    mask &= states[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    dominant = states["dominant_run_side"].to_numpy(np.int8)
    side = -dominant if spec.motif == "DOMINANT_RUN_WITHOUT_PROGRESS" else dominant
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
            "longest_buy_run": int(row["longest_buy_run"]),
            "longest_sell_run": int(row["longest_sell_run"]),
            "first_price": int(row["first_price"]),
            "last_price": int(row["last_price"]),
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
        raise V71AggressorRunTopologyError("aggressor-run WORM hash drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("grammar_id") != GRAMMAR_ID or int(payload.get("candidate_count", 0)) != 4:
        raise V71AggressorRunTopologyError("aggressor-run grammar identity drift")
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
    "AggressorRunTopologySourceAudit",
    "GRAMMAR_ID",
    "V71AggressorRunTopologyError",
    "build_aggressor_run_topology_states",
    "candidate_specs",
    "generate_signal_population",
    "load_aggressor_run_topology_sources",
    "signal_path_hash",
]
