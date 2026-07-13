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


GRAMMAR_ID = "hydra_v7_2_flow_impact_relaxation_grammar_0010"
GRAMMAR_PATH = "WORM/v7.2-flow-impact-relaxation-grammar-0010-2026-07-13.json"
GRAMMAR_SHA256 = "2513038d857e3599449fbe347bec1d4738ae2adfe9558d5daf4c2c26d322e1cd"
FEATURE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
FEATURE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_date_matched_event_store_v1.json"
FEATURE_MANIFEST_SHA256 = "5c700f1ec38ab03a1206af7013a28600a681a0b3334781979b37ec356b5421ab"
MECHANISM_CLASS = "v72g10_delayed_flow_impact_relaxation"
RESPONSE_WINDOWS = (2, 4)
HORIZONS = (30, 60)
MINUTE_NS = 60_000_000_000
MIN_HISTORY = 30
MOTIF_POLICIES = {
    "QUIET_IMPACT_RETENTION": "CONTINUATION",
    "QUIET_PASSIVE_EXTENSION": "CONTINUATION",
    "QUIET_LIQUIDITY_REVERSION": "REVERSAL",
    "SAME_FLOW_REPRICING": "CONTINUATION",
    "SAME_FLOW_EXHAUSTION": "REVERSAL",
    "COUNTERFLOW_ABSORPTION": "CONTINUATION",
    "COUNTERFLOW_REPRICING": "REVERSAL",
    "EXTEND_THEN_FAIL": "REVERSAL",
    "RETRACE_THEN_RESUME": "CONTINUATION",
}


class V72FlowImpactRelaxationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V72ImpactCandidateSpec:
    candidate_id: str
    family_id: str
    mechanism_class: str
    motif: str
    response_policy: str
    response_window_minutes: int
    holding_minutes: int
    cost_horizon: str
    product: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_specs(project_root: str | Path = ".") -> tuple[V72ImpactCandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    frozen_ids = {str(value) for value in grammar["candidate_ids"]}
    rows: list[V72ImpactCandidateSpec] = []
    for motif, response in MOTIF_POLICIES.items():
        for response_window in RESPONSE_WINDOWS:
            for horizon in HORIZONS:
                candidate_id = (
                    f"v72g10_flow_impact_relaxation_{motif.lower()}_"
                    f"{response.lower()}_r{response_window}_h{horizon}"
                )
                payload = {
                    "grammar_id": GRAMMAR_ID,
                    "grammar_sha256": GRAMMAR_SHA256,
                    "feature_sha256": FEATURE_SHA256,
                    "candidate_id": candidate_id,
                    "family_id": "DELAYED_FLOW_IMPACT_RELAXATION",
                    "mechanism_class": MECHANISM_CLASS,
                    "motif": motif,
                    "response_policy": response,
                    "response_window_minutes": response_window,
                    "holding_minutes": horizon,
                    "cost_horizon": f"{horizon}m",
                    "product": "ES",
                }
                rows.append(
                    V72ImpactCandidateSpec(
                        candidate_id=candidate_id,
                        family_id="DELAYED_FLOW_IMPACT_RELAXATION",
                        mechanism_class=MECHANISM_CLASS,
                        motif=motif,
                        response_policy=response,
                        response_window_minutes=response_window,
                        holding_minutes=horizon,
                        cost_horizon=f"{horizon}m",
                        product="ES",
                        specification_hash=_stable_hash(payload),
                    )
                )
    if (
        len(rows) != 36
        or len({row.candidate_id for row in rows}) != 36
        or {row.candidate_id for row in rows} != frozen_ids
    ):
        raise V72FlowImpactRelaxationError("flow-impact candidate identity drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_flow_impact_sources(
    project_root: str | Path = ".",
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame], dict[str, Any]]:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        FEATURE_PATH: FEATURE_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
    }
    drift = [path for path, sha in checks.items() if _sha256(root / path) != sha]
    if drift:
        raise V72FlowImpactRelaxationError(
            "flow-impact frozen source drift: " + ",".join(drift)
        )
    manifest = json.loads((root / FEATURE_MANIFEST_PATH).read_text(encoding="utf-8"))
    if manifest.get("outcome_or_pnl_columns") != []:
        raise V72FlowImpactRelaxationError("flow-impact source manifest has outcomes")
    minute = pd.read_parquet(root / FEATURE_PATH)
    minute = minute[minute["product"] == "ES"].sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    if minute.empty or set(minute["product"]) != {"ES"}:
        raise V72FlowImpactRelaxationError("flow-impact ES source is empty")
    if np.any(
        minute["availability_ns"].to_numpy(np.int64)
        < minute["source_close_ns"].to_numpy(np.int64)
    ):
        raise V72FlowImpactRelaxationError("flow-impact availability precedes close")
    states, audit = build_flow_impact_states(minute)
    return minute, states, audit


def build_flow_impact_states(
    minute: pd.DataFrame,
) -> tuple[dict[int, pd.DataFrame], dict[str, Any]]:
    required = {
        "calendar_year",
        "contract",
        "minute_start_ns",
        "availability_ns",
        "open",
        "close",
        "total_volume",
        "signed_aggressor_fraction",
        "price_change_points",
    }
    missing = sorted(required.difference(minute.columns))
    if missing:
        raise V72FlowImpactRelaxationError(
            "flow-impact source fields missing: " + ",".join(missing)
        )
    frame = minute.copy().sort_values(
        ["calendar_year", "minute_start_ns", "contract"], kind="stable"
    ).reset_index(drop=True)
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["session_day"] = np.asarray([value.isoformat() for value in timestamps.date])
    frame["abs_flow"] = np.abs(frame["signed_aggressor_fraction"].to_numpy(float))
    flow_q80 = np.full(len(frame), np.nan, dtype=float)
    volume_median = np.full(len(frame), np.nan, dtype=float)
    for _, positions in frame.groupby(
        ["calendar_year", "contract", "session_day"], sort=False
    ).indices.items():
        idx = np.asarray(positions, dtype=np.int64)
        group_flow = frame.loc[idx, "abs_flow"]
        group_volume = frame.loc[idx, "total_volume"]
        flow_q80[idx] = (
            group_flow.expanding(min_periods=MIN_HISTORY).quantile(0.80).shift(1).to_numpy()
        )
        volume_median[idx] = (
            group_volume.expanding(min_periods=MIN_HISTORY).median().shift(1).to_numpy()
        )
    flow = frame["signed_aggressor_fraction"].to_numpy(float)
    displacement = frame["price_change_points"].to_numpy(float)
    impulse_side = np.sign(displacement).astype(np.int8)
    impulse = (
        np.isfinite(flow_q80)
        & np.isfinite(volume_median)
        & (np.abs(flow) > flow_q80)
        & (frame["total_volume"].to_numpy(float) > volume_median)
        & (np.abs(displacement) >= 0.25)
        & (np.sign(flow).astype(np.int8) == impulse_side)
        & (impulse_side != 0)
    )
    states: dict[int, pd.DataFrame] = {}
    motif_counts: dict[str, int] = {}
    for response_window in RESPONSE_WINDOWS:
        rows = _response_rows(frame, impulse, response_window)
        states[response_window] = rows
        for motif in MOTIF_POLICIES:
            motif_counts[f"r{response_window}:{motif}"] = int(
                rows[f"state_{motif}"].sum()
            )
    audit = {
        "minute_count": int(len(frame)),
        "calendar_year_count": int(frame["calendar_year"].nunique()),
        "contract_count": int(frame["contract"].nunique()),
        "session_count": int(
            frame[["calendar_year", "contract", "session_day"]]
            .drop_duplicates()
            .shape[0]
        ),
        "impulse_count": int(impulse.sum()),
        "response_state_counts": {
            str(window): int(len(rows)) for window, rows in states.items()
        },
        "motif_state_counts": dict(sorted(motif_counts.items())),
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
            raise V72FlowImpactRelaxationError("flow-impact mechanism is tombstoned")
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        if spec.response_window_minutes not in states:
            raise V72FlowImpactRelaxationError("flow-impact response-window drift")
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, states[spec.response_window_minutes])
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V72FlowImpactRelaxationError("flow-impact signal population drift")
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


def _response_rows(
    frame: pd.DataFrame, impulse: np.ndarray, response_window: int
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, positions in frame.groupby(
        ["calendar_year", "contract", "session_day"], sort=False
    ).indices.items():
        idx = np.asarray(positions, dtype=np.int64)
        starts = frame.loc[idx, "minute_start_ns"].to_numpy(np.int64)
        for local_position in np.flatnonzero(impulse[idx]):
            local_position = int(local_position)
            final_local = local_position + response_window
            entry_local = final_local + 1
            if entry_local >= len(idx):
                continue
            expected = starts[local_position] + np.arange(
                response_window + 2, dtype=np.int64
            ) * MINUTE_NS
            observed = starts[local_position : entry_local + 1]
            if len(observed) != len(expected) or not np.array_equal(observed, expected):
                continue
            impulse_row = frame.iloc[int(idx[local_position])]
            response_positions = idx[local_position + 1 : final_local + 1]
            response = frame.iloc[response_positions]
            side = int(np.sign(float(impulse_row["price_change_points"])))
            impulse_move = abs(float(impulse_row["price_change_points"]))
            response_flow_mean = float(response["signed_aggressor_fraction"].mean())
            response_flow_ratio = abs(response_flow_mean) / max(
                abs(float(impulse_row["signed_aggressor_fraction"])), 1e-12
            )
            response_flow_side = int(np.sign(response_flow_mean) * side)
            response_price_ratio = side * (
                float(response.iloc[-1]["close"]) - float(impulse_row["close"])
            ) / impulse_move
            half_count = response_window // 2
            first_half_price_ratio = side * (
                float(response.iloc[half_count - 1]["close"])
                - float(impulse_row["close"])
            ) / impulse_move
            payload: dict[str, Any] = {
                "calendar_year": int(impulse_row["calendar_year"]),
                "contract": str(impulse_row["contract"]),
                "session_day": str(impulse_row["session_day"]),
                "impulse_position": int(idx[local_position]),
                "impulse_minute_start_ns": int(impulse_row["minute_start_ns"]),
                "decision_ns": int(frame.iloc[int(idx[final_local])]["availability_ns"]),
                "entry_minute_start_ns": int(frame.iloc[int(idx[entry_local])]["minute_start_ns"]),
                "impulse_side": side,
                "impulse_flow_fraction": float(
                    impulse_row["signed_aggressor_fraction"]
                ),
                "impulse_price_change_points": float(
                    impulse_row["price_change_points"]
                ),
                "response_flow_mean": response_flow_mean,
                "response_flow_ratio": response_flow_ratio,
                "response_flow_side": response_flow_side,
                "response_price_ratio": response_price_ratio,
                "first_half_price_ratio": first_half_price_ratio,
            }
            quiet = response_flow_ratio <= 0.5
            active = response_flow_ratio > 0.5
            payload.update(
                {
                    "state_QUIET_IMPACT_RETENTION": quiet
                    and -0.25 <= response_price_ratio <= 0.25,
                    "state_QUIET_PASSIVE_EXTENSION": quiet
                    and response_price_ratio > 0.25,
                    "state_QUIET_LIQUIDITY_REVERSION": quiet
                    and response_price_ratio < -0.5,
                    "state_SAME_FLOW_REPRICING": active
                    and response_flow_side == 1
                    and response_price_ratio > 0.25,
                    "state_SAME_FLOW_EXHAUSTION": active
                    and response_flow_side == 1
                    and response_price_ratio < -0.25,
                    "state_COUNTERFLOW_ABSORPTION": active
                    and response_flow_side == -1
                    and response_price_ratio >= -0.25,
                    "state_COUNTERFLOW_REPRICING": active
                    and response_flow_side == -1
                    and response_price_ratio < -0.25,
                    "state_EXTEND_THEN_FAIL": first_half_price_ratio > 0.25
                    and response_price_ratio < -0.25,
                    "state_RETRACE_THEN_RESUME": first_half_price_ratio < -0.25
                    and response_price_ratio > 0.25,
                }
            )
            rows.append(payload)
    columns = [
        "calendar_year",
        "contract",
        "session_day",
        "impulse_position",
        "impulse_minute_start_ns",
        "decision_ns",
        "entry_minute_start_ns",
        "impulse_side",
        "impulse_flow_fraction",
        "impulse_price_change_points",
        "response_flow_mean",
        "response_flow_ratio",
        "response_flow_side",
        "response_price_ratio",
        "first_half_price_ratio",
        *[f"state_{motif}" for motif in MOTIF_POLICIES],
    ]
    return pd.DataFrame(rows, columns=columns)


def _signals_for_spec(
    spec: V72ImpactCandidateSpec, states: pd.DataFrame
) -> list[V71Signal]:
    if states.empty:
        return []
    mask = states[f"state_{spec.motif}"].to_numpy(bool, copy=True)
    impulse_side = states["impulse_side"].to_numpy(np.int8)
    side = impulse_side if spec.response_policy == "CONTINUATION" else -impulse_side
    signals: list[V71Signal] = []
    next_allowed: dict[tuple[str, str], int] = {}
    for position in np.flatnonzero(mask & (side != 0)):
        row = states.iloc[int(position)]
        key = (str(row["contract"]), str(row["session_day"]))
        decision = int(row["decision_ns"])
        entry = int(row["entry_minute_start_ns"])
        exit_ns = entry + spec.holding_minutes * MINUTE_NS
        session_end = pd.Timestamp(
            f"{row['session_day']} 15:10:00", tz="America/Chicago"
        ).tz_convert("UTC").value
        if decision < next_allowed.get(key, -1) or exit_ns >= session_end:
            continue
        snapshot = {
            "grammar_sha256": GRAMMAR_SHA256,
            "feature_sha256": FEATURE_SHA256,
            "candidate_id": spec.candidate_id,
            "impulse_position": int(row["impulse_position"]),
            "decision_ns": decision,
            "impulse_flow_fraction": float(row["impulse_flow_fraction"]),
            "impulse_price_change_points": float(
                row["impulse_price_change_points"]
            ),
            "response_flow_ratio": float(row["response_flow_ratio"]),
            "response_flow_side": int(row["response_flow_side"]),
            "response_price_ratio": float(row["response_price_ratio"]),
            "first_half_price_ratio": float(row["first_half_price_ratio"]),
        }
        signals.append(
            V71Signal(
                candidate_id=spec.candidate_id,
                family_id=spec.family_id,
                motif=spec.motif,
                response_policy=spec.response_policy,
                holding_minutes=spec.holding_minutes,
                calendar_year=int(row["calendar_year"]),
                session_day=str(row["session_day"]),
                source_position=int(row["impulse_position"]),
                availability_ns=decision,
                decision_ns=decision,
                entry_minute_start_ns=entry,
                exit_minute_start_ns=exit_ns,
                side=int(side[int(position)]),
                contract=str(row["contract"]),
                feature_snapshot_hash=_stable_hash(snapshot),
            )
        )
        next_allowed[key] = exit_ns
    return signals


def _load_grammar(root: Path) -> Mapping[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V72FlowImpactRelaxationError("flow-impact WORM grammar drift")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("grammar_id") != GRAMMAR_ID
        or int(payload.get("candidate_count", -1)) != 36
        or payload.get("this_grammar_signal_or_pnl_results_seen_before_freeze") is not False
        or payload.get("new_data_purchase") is not False
        or payload.get("protected_holdout_access") is not False
        or int(payload.get("outbound_orders", -1)) != 0
    ):
        raise V72FlowImpactRelaxationError("flow-impact WORM policy drift")
    return payload


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "FEATURE_MANIFEST_PATH",
    "FEATURE_MANIFEST_SHA256",
    "FEATURE_PATH",
    "FEATURE_SHA256",
    "GRAMMAR_ID",
    "GRAMMAR_PATH",
    "GRAMMAR_SHA256",
    "HORIZONS",
    "MECHANISM_CLASS",
    "MOTIF_POLICIES",
    "RESPONSE_WINDOWS",
    "V72FlowImpactRelaxationError",
    "V72ImpactCandidateSpec",
    "build_flow_impact_states",
    "candidate_specs",
    "generate_signal_population",
    "load_flow_impact_sources",
    "signal_path_hash",
]
