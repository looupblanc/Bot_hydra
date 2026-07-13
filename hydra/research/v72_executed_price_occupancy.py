from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v71_event_mechanism_grammar import V71Signal
from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_2_executed_price_occupancy_grammar_0012"
GRAMMAR_PATH = "WORM/v7.2-executed-price-occupancy-grammar-0012-2026-07-13.json"
GRAMMAR_SHA256 = "d0fa4eb200f47e1df9d3323c09f9e0c3729802a001b9c946bdf43824846a4c0c"
FEATURE_PATH = "data/cache/v7_d1/date_matched_executed_price_occupancy_v1.parquet"
FEATURE_SHA256 = "46dede4ba706eb955ce523f8b7d117a0382a31291b40f9c236209bda47cd2374"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_executed_price_occupancy_v1.json"
FEATURE_MANIFEST_SHA256 = "e6c41b6f0b819668798af90aaf7246c07be499badd338cb87bd646f7f95f5408"
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
FAMILY_ID = "EXECUTED_PRICE_OCCUPANCY_TOPOLOGY"
MECHANISM_CLASS = "v72g12_executed_price_occupancy_topology"
HISTORY_WINDOWS = (20, 60)
HORIZONS = (30, 60)
MINUTE_NS = 60_000_000_000
MOTIF_POLICIES = {
    "CONCENTRATED_MODE_ESCAPE": "CONTINUATION",
    "CONCENTRATED_MODE_RECAPTURE": "REVERSAL",
    "REVISIT_PRESSURE_BREAK": "CONTINUATION",
    "REVISIT_PRESSURE_FAILURE": "REVERSAL",
    "MODE_MIGRATION_PERSISTENCE": "CONTINUATION",
    "BIMODAL_AUCTION_RESOLUTION": "CONTINUATION",
}


class V72ExecutedPriceOccupancyError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V72OccupancyCandidateSpec:
    candidate_id: str
    family_id: str
    mechanism_class: str
    motif: str
    response_policy: str
    history_window: int
    holding_minutes: int
    cost_horizon: str
    product: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_specs(
    project_root: str | Path = ".",
) -> tuple[V72OccupancyCandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = {str(value) for value in grammar["candidate_ids"]}
    rows: list[V72OccupancyCandidateSpec] = []
    for motif, response in MOTIF_POLICIES.items():
        for window in HISTORY_WINDOWS:
            for horizon in HORIZONS:
                candidate_id = (
                    f"v72g12_price_occupancy_{motif.lower()}_"
                    f"{response.lower()}_w{window}_h{horizon}"
                )
                payload = {
                    "grammar_id": GRAMMAR_ID,
                    "grammar_sha256": GRAMMAR_SHA256,
                    "feature_sha256": FEATURE_SHA256,
                    "candidate_id": candidate_id,
                    "family_id": FAMILY_ID,
                    "mechanism_class": MECHANISM_CLASS,
                    "motif": motif,
                    "response_policy": response,
                    "history_window": window,
                    "holding_minutes": horizon,
                    "cost_horizon": f"{horizon}m",
                    "product": "ES",
                }
                rows.append(
                    V72OccupancyCandidateSpec(
                        candidate_id=candidate_id,
                        family_id=FAMILY_ID,
                        mechanism_class=MECHANISM_CLASS,
                        motif=motif,
                        response_policy=response,
                        history_window=window,
                        holding_minutes=horizon,
                        cost_horizon=f"{horizon}m",
                        product="ES",
                        specification_hash=_stable_hash(payload),
                    )
                )
    if (
        len(rows) != 24
        or len({row.candidate_id for row in rows}) != 24
        or {row.candidate_id for row in rows} != frozen_ids
    ):
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy candidate identity drift"
        )
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_executed_price_occupancy_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame], dict[str, Any]]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        FEATURE_PATH: FEATURE_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        MINUTE_PATH: MINUTE_SHA256,
    }
    drift = [path for path, sha in checks.items() if _sha256(root / path) != sha]
    if drift:
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy frozen source drift: " + ",".join(drift)
        )
    manifest = json.loads(
        (root / FEATURE_MANIFEST_PATH).read_text(encoding="utf-8")
    )
    if manifest.get("outcome_or_future_pnl_columns") != []:
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy source contains outcomes"
        )
    feature = pd.read_parquet(root / FEATURE_PATH)
    minute = pd.read_parquet(root / MINUTE_PATH)
    minute = minute[minute["product"] == "ES"].copy()
    minute = minute.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    states, audit = build_executed_price_occupancy_states(feature, minute)
    return minute, states, audit


def build_executed_price_occupancy_states(
    feature: pd.DataFrame,
    minute: pd.DataFrame,
) -> tuple[dict[int, pd.DataFrame], dict[str, Any]]:
    feature_required = {
        "calendar_year",
        "session_date",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "occupancy_entropy",
        "mode_volume_share",
        "top_two_volume_share",
        "second_to_first_mode_ratio",
        "revisit_ratio",
        "signed_flow_fraction",
        "mode_signed_flow_fraction",
        "mode_tick",
        "second_mode_tick",
        "last_tick",
        "last_minus_mode_ticks",
        "maximum_excursion_from_mode_ticks",
        "maximum_excursion_direction",
        "mode_migration_ticks",
    }
    minute_required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
    }
    if feature_required.difference(feature.columns) or minute_required.difference(
        minute.columns
    ):
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy source fields missing"
        )
    keys = ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    frame = feature.merge(minute[keys], on=keys, how="inner", validate="one_to_one")
    frame = frame.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    if len(frame) != len(feature) or len(frame) != len(minute):
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy/minute exact source mismatch"
        )
    frame["session_day"] = frame["session_date"].astype(str)
    states: dict[int, pd.DataFrame] = {}
    motif_counts: dict[str, int] = {}
    for window in HISTORY_WINDOWS:
        state = _build_window_states(frame, window)
        _add_execution_paths(state)
        states[window] = state
        for motif in MOTIF_POLICIES:
            motif_counts[f"w{window}:{motif}"] = int(
                state[f"state_{motif}"].sum()
            )
    audit = {
        "minute_count": int(len(frame)),
        "session_count": int(
            frame[["calendar_year", "contract", "session_day"]]
            .drop_duplicates()
            .shape[0]
        ),
        "exact_source_match_count": int(len(frame)),
        "history_windows": list(HISTORY_WINDOWS),
        "motif_state_counts": dict(sorted(motif_counts.items())),
        "executable_counts": {
            f"w{window}:h{horizon}": int(
                states[window][f"executable_{horizon}"].sum()
            )
            for window in HISTORY_WINDOWS
            for horizon in HORIZONS
        },
        "outcome_or_future_price_columns_used": [],
    }
    return states, audit


def generate_signal_population(
    states: Mapping[int, pd.DataFrame],
    *,
    project_root: str | Path = ".",
    graveyard_path: str | Path | None = "mission/state/graveyard.db",
) -> dict[str, tuple[V71Signal, ...]]:
    specs = candidate_specs(project_root)
    if graveyard_path is not None:
        dead = {str(row["mechanism_class"]) for row in class_feedback(graveyard_path)}
        if MECHANISM_CLASS in dead:
            raise V72ExecutedPriceOccupancyError(
                "executed-price occupancy mechanism is tombstoned"
            )
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        if spec.history_window not in states:
            raise V72ExecutedPriceOccupancyError(
                "executed-price occupancy history-window drift"
            )
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, states[spec.history_window])
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy signal population drift"
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


def _build_window_states(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    state = frame.copy()
    state["abs_flow"] = np.abs(state["signed_flow_fraction"].to_numpy(float))
    state["abs_departure"] = np.abs(
        state["last_minus_mode_ticks"].to_numpy(float)
    )
    state["abs_mode_migration"] = np.abs(
        state["mode_migration_ticks"].to_numpy(float)
    )
    state["mode_separation"] = np.abs(
        state["second_mode_tick"].to_numpy(float)
        - state["mode_tick"].to_numpy(float)
    )
    quantile_columns = {
        "entropy_q20": ("occupancy_entropy", 0.20),
        "mode_share_q50": ("mode_volume_share", 0.50),
        "mode_share_q80": ("mode_volume_share", 0.80),
        "revisit_q80": ("revisit_ratio", 0.80),
        "abs_flow_q50": ("abs_flow", 0.50),
        "departure_q20": ("abs_departure", 0.20),
        "departure_q80": ("abs_departure", 0.80),
        "excursion_q80": ("maximum_excursion_from_mode_ticks", 0.80),
        "migration_q80": ("abs_mode_migration", 0.80),
        "top_two_q80": ("top_two_volume_share", 0.80),
        "second_ratio_q50": ("second_to_first_mode_ratio", 0.50),
        "mode_separation_q80": ("mode_separation", 0.80),
    }
    for output_column in quantile_columns:
        state[output_column] = np.nan
    group_columns = ["calendar_year", "contract", "session_day"]
    for _, positions in state.groupby(group_columns, sort=False).indices.items():
        idx = np.asarray(positions, dtype=np.int64)
        for output_column, (source_column, quantile) in quantile_columns.items():
            values = state.loc[idx, source_column]
            state.loc[idx, output_column] = (
                values.shift(1)
                .rolling(window=window, min_periods=window)
                .quantile(quantile)
                .to_numpy()
            )

    entropy = state["occupancy_entropy"].to_numpy(float)
    mode_share = state["mode_volume_share"].to_numpy(float)
    revisit = state["revisit_ratio"].to_numpy(float)
    abs_flow = state["abs_flow"].to_numpy(float)
    departure = state["abs_departure"].to_numpy(float)
    excursion = state["maximum_excursion_from_mode_ticks"].to_numpy(float)
    migration = state["abs_mode_migration"].to_numpy(float)
    top_two = state["top_two_volume_share"].to_numpy(float)
    second_ratio = state["second_to_first_mode_ratio"].to_numpy(float)
    separation = state["mode_separation"].to_numpy(float)
    flow_side = np.sign(state["signed_flow_fraction"].to_numpy(float)).astype(np.int8)
    departure_side = np.sign(
        state["last_minus_mode_ticks"].to_numpy(float)
    ).astype(np.int8)
    migration_side = np.sign(
        state["mode_migration_ticks"].fillna(0.0).to_numpy(float)
    ).astype(np.int8)
    excursion_side = state["maximum_excursion_direction"].to_numpy(np.int8)
    midpoint = (
        state["mode_tick"].to_numpy(float)
        + state["second_mode_tick"].to_numpy(float)
    ) / 2.0
    resolution_delta = state["last_tick"].to_numpy(float) - midpoint
    resolution_side = np.where(
        np.isfinite(resolution_delta), np.sign(resolution_delta), 0
    ).astype(np.int8)

    concentrated = (
        np.isfinite(state["entropy_q20"].to_numpy(float))
        & (entropy < state["entropy_q20"].to_numpy(float))
        & (mode_share > state["mode_share_q80"].to_numpy(float))
    )
    high_revisit = (
        np.isfinite(state["revisit_q80"].to_numpy(float))
        & (revisit > state["revisit_q80"].to_numpy(float))
    )
    directional = (
        np.isfinite(state["abs_flow_q50"].to_numpy(float))
        & (abs_flow > state["abs_flow_q50"].to_numpy(float))
        & (flow_side != 0)
    )
    material_departure = (
        np.isfinite(state["departure_q80"].to_numpy(float))
        & (departure > state["departure_q80"].to_numpy(float))
        & (departure_side != 0)
    )
    large_excursion = (
        np.isfinite(state["excursion_q80"].to_numpy(float))
        & (excursion > state["excursion_q80"].to_numpy(float))
    )
    recaptured = (
        np.isfinite(state["departure_q20"].to_numpy(float))
        & (departure <= state["departure_q20"].to_numpy(float))
    )
    material_migration = (
        np.isfinite(state["migration_q80"].to_numpy(float))
        & (migration > state["migration_q80"].to_numpy(float))
        & (migration_side != 0)
    )
    bimodal = (
        np.isfinite(state["mode_separation_q80"].to_numpy(float))
        & (top_two > state["top_two_q80"].to_numpy(float))
        & (second_ratio > state["second_ratio_q50"].to_numpy(float))
        & (separation > state["mode_separation_q80"].to_numpy(float))
    )

    state["state_CONCENTRATED_MODE_ESCAPE"] = concentrated & material_departure
    state["state_CONCENTRATED_MODE_RECAPTURE"] = (
        concentrated & large_excursion & recaptured & (excursion_side != 0)
    )
    state["state_REVISIT_PRESSURE_BREAK"] = (
        high_revisit
        & directional
        & material_departure
        & (departure_side == flow_side)
    )
    state["state_REVISIT_PRESSURE_FAILURE"] = (
        high_revisit & directional & (departure_side != flow_side)
    )
    state["state_MODE_MIGRATION_PERSISTENCE"] = (
        material_migration
        & directional
        & (migration_side == flow_side)
        & (mode_share > state["mode_share_q50"].to_numpy(float))
    )
    state["state_BIMODAL_AUCTION_RESOLUTION"] = bimodal & (resolution_side != 0)
    state["flow_direction"] = flow_side
    state["departure_direction"] = departure_side
    state["migration_direction"] = migration_side
    state["excursion_direction"] = excursion_side
    state["resolution_direction"] = resolution_side
    state["history_window"] = window
    return state


def _add_execution_paths(state: pd.DataFrame) -> None:
    starts = state["minute_start_ns"].to_numpy(np.int64)
    decisions = state["availability_ns"].to_numpy(np.int64)
    contracts = state["contract"].astype(str).to_numpy()
    session_days = state["session_day"].astype(str).to_numpy()
    for horizon in HORIZONS:
        entries = np.searchsorted(starts, decisions, side="left")
        valid_entry = entries < len(starts)
        entry_starts = np.full(len(state), -1, dtype=np.int64)
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
        state[f"entry_ns_{horizon}"] = entry_starts
        state[f"exit_ns_{horizon}"] = np.where(valid, targets, -1)
        state[f"executable_{horizon}"] = valid


def _signals_for_spec(
    spec: V72OccupancyCandidateSpec,
    states: pd.DataFrame,
) -> list[V71Signal]:
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    mask &= states[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    if spec.motif == "CONCENTRATED_MODE_ESCAPE":
        side = states["departure_direction"].to_numpy(np.int8)
    elif spec.motif == "CONCENTRATED_MODE_RECAPTURE":
        side = -states["excursion_direction"].to_numpy(np.int8)
    elif spec.motif == "REVISIT_PRESSURE_BREAK":
        side = states["flow_direction"].to_numpy(np.int8)
    elif spec.motif == "REVISIT_PRESSURE_FAILURE":
        side = -states["flow_direction"].to_numpy(np.int8)
    elif spec.motif == "MODE_MIGRATION_PERSISTENCE":
        side = states["migration_direction"].to_numpy(np.int8)
    elif spec.motif == "BIMODAL_AUCTION_RESOLUTION":
        side = states["resolution_direction"].to_numpy(np.int8)
    else:
        raise V72ExecutedPriceOccupancyError(
            f"unknown executed-price occupancy motif: {spec.motif}"
        )
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
            "history_window": spec.history_window,
            "occupancy_entropy": float(row["occupancy_entropy"]),
            "mode_volume_share": float(row["mode_volume_share"]),
            "top_two_volume_share": float(row["top_two_volume_share"]),
            "second_to_first_mode_ratio": float(
                row["second_to_first_mode_ratio"]
            ),
            "revisit_ratio": float(row["revisit_ratio"]),
            "signed_flow_fraction": float(row["signed_flow_fraction"]),
            "mode_signed_flow_fraction": float(row["mode_signed_flow_fraction"]),
            "last_minus_mode_ticks": int(row["last_minus_mode_ticks"]),
            "maximum_excursion_from_mode_ticks": int(
                row["maximum_excursion_from_mode_ticks"]
            ),
            "mode_migration_ticks": float(row["mode_migration_ticks"]),
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
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy WORM hash drift"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("grammar_id") != GRAMMAR_ID
        or int(payload.get("candidate_count", 0)) != 24
        or payload.get("this_grammar_signal_or_pnl_results_seen_before_freeze")
        is not False
    ):
        raise V72ExecutedPriceOccupancyError(
            "executed-price occupancy grammar identity drift"
        )
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
    "GRAMMAR_ID",
    "V72ExecutedPriceOccupancyError",
    "V72OccupancyCandidateSpec",
    "build_executed_price_occupancy_states",
    "candidate_specs",
    "generate_signal_population",
    "load_executed_price_occupancy_sources",
    "signal_path_hash",
]
