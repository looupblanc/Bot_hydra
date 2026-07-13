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


GRAMMAR_ID = "hydra_v7_2_trade_arrival_renewal_grammar_0011"
GRAMMAR_PATH = "WORM/v7.2-trade-arrival-renewal-grammar-0011-2026-07-13.json"
GRAMMAR_SHA256 = "d69f021bf4de5b4e5a0fe92d318eba9f00b08c80d99cb3941c43daab4a6b10c2"
FEATURE_PATH = "data/cache/v7_d1/date_matched_trade_arrival_renewal_v1.parquet"
FEATURE_SHA256 = "83529896a80aa4bc08cb3fa3ba44602ec23c408404ab5fc6c3320555e0d6b95e"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_trade_arrival_renewal_v1.json"
FEATURE_MANIFEST_SHA256 = "e31407d772c4540d786efc8016ebb15333ccc06e420d4282a44a31b0658d4c26"
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
FAMILY_ID = "INTRAMINUTE_TRADE_ARRIVAL_RENEWAL"
MECHANISM_CLASS = "v72g11_intraminute_trade_arrival_renewal"
HISTORY_WINDOWS = (20, 60)
HORIZONS = (30, 60)
MINUTE_NS = 60_000_000_000
MOTIF_POLICIES = {
    "CLUSTERED_DIRECTIONAL_ARRIVAL": "CONTINUATION",
    "CLUSTERED_ABSORBED_ARRIVAL": "REVERSAL",
    "DISTRIBUTED_DIRECTIONAL_SLICING": "CONTINUATION",
    "SILENCE_TO_BURST_RELEASE": "CONTINUATION",
    "BURST_TO_SILENCE_EXHAUSTION": "REVERSAL",
    "TWO_SIDED_CLUSTERED_INVENTORY": "REVERSAL",
}


class V72TradeArrivalRenewalError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V72ArrivalCandidateSpec:
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
) -> tuple[V72ArrivalCandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = {str(value) for value in grammar["candidate_ids"]}
    rows: list[V72ArrivalCandidateSpec] = []
    for motif, response in MOTIF_POLICIES.items():
        for window in HISTORY_WINDOWS:
            for horizon in HORIZONS:
                candidate_id = (
                    f"v72g11_trade_arrival_renewal_{motif.lower()}_"
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
                    V72ArrivalCandidateSpec(
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
        raise V72TradeArrivalRenewalError("trade-arrival candidate identity drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_trade_arrival_renewal_sources(
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
        raise V72TradeArrivalRenewalError(
            "trade-arrival frozen source drift: " + ",".join(drift)
        )
    manifest = json.loads(
        (root / FEATURE_MANIFEST_PATH).read_text(encoding="utf-8")
    )
    if manifest.get("outcome_or_future_pnl_columns") != []:
        raise V72TradeArrivalRenewalError("trade-arrival source contains outcomes")
    feature = pd.read_parquet(root / FEATURE_PATH)
    minute = pd.read_parquet(root / MINUTE_PATH)
    minute = minute[minute["product"] == "ES"].copy()
    minute = minute.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    states, audit = build_trade_arrival_renewal_states(feature, minute)
    return minute, states, audit


def build_trade_arrival_renewal_states(
    feature: pd.DataFrame,
    minute: pd.DataFrame,
) -> tuple[dict[int, pd.DataFrame], dict[str, Any]]:
    feature_required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "trade_count",
        "positive_gap_median_ns",
        "arrival_entropy",
        "maximum_five_second_share",
        "signed_flow_fraction",
        "price_progress_points",
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
        raise V72TradeArrivalRenewalError("trade-arrival source fields missing")
    keys = ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    frame = feature.merge(minute[keys], on=keys, how="inner", validate="one_to_one")
    frame = frame.sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    if len(frame) != len(feature) or len(frame) != len(minute):
        raise V72TradeArrivalRenewalError(
            "trade-arrival/minute exact source mismatch"
        )
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["session_day"] = np.asarray(
        [value.isoformat() for value in timestamps.date]
    )
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
            raise V72TradeArrivalRenewalError(
                "trade-arrival mechanism is tombstoned"
            )
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        if spec.history_window not in states:
            raise V72TradeArrivalRenewalError("trade-arrival history-window drift")
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, states[spec.history_window])
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V72TradeArrivalRenewalError("trade-arrival signal population drift")
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
    quantile_columns = {
        "gap_q20": ("positive_gap_median_ns", 0.20),
        "gap_q80": ("positive_gap_median_ns", 0.80),
        "entropy_q20": ("arrival_entropy", 0.20),
        "entropy_q80": ("arrival_entropy", 0.80),
        "share_q20": ("maximum_five_second_share", 0.20),
        "share_q80": ("maximum_five_second_share", 0.80),
        "trade_count_q20": ("trade_count", 0.20),
        "trade_count_q80": ("trade_count", 0.80),
        "abs_flow_q20": ("abs_flow", 0.20),
        "abs_flow_q50": ("abs_flow", 0.50),
    }
    state["abs_flow"] = np.abs(state["signed_flow_fraction"].to_numpy(float))
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
    gap = state["positive_gap_median_ns"].to_numpy(float)
    entropy = state["arrival_entropy"].to_numpy(float)
    share = state["maximum_five_second_share"].to_numpy(float)
    count = state["trade_count"].to_numpy(float)
    abs_flow = state["abs_flow"].to_numpy(float)
    flow_side = np.sign(state["signed_flow_fraction"].to_numpy(float)).astype(np.int8)
    price_side = np.sign(state["price_progress_points"].to_numpy(float)).astype(np.int8)
    finite = np.isfinite(state["gap_q20"].to_numpy(float))
    clustered = (
        finite
        & (gap < state["gap_q20"].to_numpy(float))
        & (entropy < state["entropy_q20"].to_numpy(float))
        & (share > state["share_q80"].to_numpy(float))
    )
    distributed = (
        finite
        & (entropy > state["entropy_q80"].to_numpy(float))
        & (share < state["share_q20"].to_numpy(float))
    )
    high_activity = finite & (count > state["trade_count_q80"].to_numpy(float))
    low_activity = finite & (count < state["trade_count_q20"].to_numpy(float))
    directional = (
        finite
        & (abs_flow > state["abs_flow_q50"].to_numpy(float))
        & (flow_side != 0)
    )
    two_sided = finite & (abs_flow < state["abs_flow_q20"].to_numpy(float))
    high_gap = finite & (gap > state["gap_q80"].to_numpy(float))
    same_price_flow = (flow_side == price_side) & (flow_side != 0)
    clustered_directional = clustered & high_activity & directional

    contracts = state["contract"].astype(str).to_numpy()
    session_days = state["session_day"].astype(str).to_numpy()
    previous_contiguous = np.zeros(len(state), dtype=bool)
    if len(state) > 1:
        previous_contiguous[1:] = (
            (state["minute_start_ns"].to_numpy(np.int64)[1:]
             - state["minute_start_ns"].to_numpy(np.int64)[:-1])
            == MINUTE_NS
        ) & (contracts[1:] == contracts[:-1]) & (
            session_days[1:] == session_days[:-1]
        )
    prior_low_high_gap = np.zeros(len(state), dtype=bool)
    prior_clustered_directional = np.zeros(len(state), dtype=bool)
    prior_flow_side = np.zeros(len(state), dtype=np.int8)
    prior_low_high_gap[1:] = (low_activity & high_gap)[:-1]
    prior_clustered_directional[1:] = clustered_directional[:-1]
    prior_flow_side[1:] = flow_side[:-1]
    prior_low_high_gap &= previous_contiguous
    prior_clustered_directional &= previous_contiguous

    state["state_CLUSTERED_DIRECTIONAL_ARRIVAL"] = (
        clustered_directional & same_price_flow
    )
    state["state_CLUSTERED_ABSORBED_ARRIVAL"] = (
        clustered_directional & (price_side != flow_side)
    )
    state["state_DISTRIBUTED_DIRECTIONAL_SLICING"] = (
        distributed & high_activity & directional & same_price_flow
    )
    state["state_SILENCE_TO_BURST_RELEASE"] = (
        prior_low_high_gap & clustered_directional & same_price_flow
    )
    state["state_BURST_TO_SILENCE_EXHAUSTION"] = (
        prior_clustered_directional & low_activity & high_gap
    )
    state["state_TWO_SIDED_CLUSTERED_INVENTORY"] = (
        clustered & high_activity & two_sided & (price_side != 0)
    )
    state["flow_direction"] = flow_side
    state["price_direction"] = price_side
    state["prior_flow_direction"] = prior_flow_side
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
    spec: V72ArrivalCandidateSpec,
    states: pd.DataFrame,
) -> list[V71Signal]:
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    mask &= states[f"executable_{spec.holding_minutes}"].to_numpy(bool)
    flow_side = states["flow_direction"].to_numpy(np.int8)
    price_side = states["price_direction"].to_numpy(np.int8)
    prior_flow_side = states["prior_flow_direction"].to_numpy(np.int8)
    if spec.motif == "CLUSTERED_ABSORBED_ARRIVAL":
        side = -flow_side
    elif spec.motif == "BURST_TO_SILENCE_EXHAUSTION":
        side = -prior_flow_side
    elif spec.motif == "TWO_SIDED_CLUSTERED_INVENTORY":
        side = -price_side
    else:
        side = flow_side
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
            "trade_count": int(row["trade_count"]),
            "positive_gap_median_ns": float(row["positive_gap_median_ns"]),
            "arrival_entropy": float(row["arrival_entropy"]),
            "maximum_five_second_share": float(
                row["maximum_five_second_share"]
            ),
            "signed_flow_fraction": float(row["signed_flow_fraction"]),
            "price_progress_points": float(row["price_progress_points"]),
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
        raise V72TradeArrivalRenewalError("trade-arrival WORM hash drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("grammar_id") != GRAMMAR_ID
        or int(payload.get("candidate_count", 0)) != 24
        or payload.get("this_grammar_signal_or_pnl_results_seen_before_freeze")
        is not False
    ):
        raise V72TradeArrivalRenewalError("trade-arrival grammar identity drift")
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
    "V72ArrivalCandidateSpec",
    "V72TradeArrivalRenewalError",
    "build_trade_arrival_renewal_states",
    "candidate_specs",
    "generate_signal_population",
    "load_trade_arrival_renewal_sources",
    "signal_path_hash",
]
