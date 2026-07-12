from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v7_graveyard import class_feedback


GRAMMAR_ID = "hydra_v7_1_event_mechanism_grammar_0001"
GRAMMAR_PATH = "WORM/v7.1-event-mechanism-grammar-0001-2026-07-12.json"
GRAMMAR_SHA256 = "e1c8de955302da2be836bbcebf2bfedc07768b2d9b987ea32258a85a2b0caf8a"
POWER_PATH = "WORM/v7.1-powered-promotion-minimum-2026-07-12.json"
POWER_SHA256 = "3e0211c6a5acea81713431802fc1576da4d5be2a0cc37bf900cd02eabd68c6fa"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_date_matched_event_store_v1.json"
FEATURE_MANIFEST_SHA256 = "5c700f1ec38ab03a1206af7013a28600a681a0b3334781979b37ec356b5421ab"
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
HORIZONS = (5, 15, 30, 60)
RESPONSES = ("CONTINUATION", "REVERSAL")
ROLLING = 20


class V71GrammarError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V71CandidateSpec:
    candidate_id: str
    family_id: str
    mechanism_class: str
    motif: str
    response_policy: str
    holding_minutes: int
    cost_horizon: str
    product: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class V71Signal:
    candidate_id: str
    family_id: str
    motif: str
    response_policy: str
    holding_minutes: int
    calendar_year: int
    session_day: str
    source_position: int
    availability_ns: int
    decision_ns: int
    entry_minute_start_ns: int
    exit_minute_start_ns: int
    side: int
    contract: str
    feature_snapshot_hash: str

    def __post_init__(self) -> None:
        if self.side not in {-1, 1}:
            raise V71GrammarError("V7.1 signal side must be -1 or +1")
        if self.availability_ns > self.decision_ns:
            raise V71GrammarError("V7.1 signal uses unavailable information")
        if not self.decision_ns <= self.entry_minute_start_ns < self.exit_minute_start_ns:
            raise V71GrammarError("V7.1 signal execution chronology is invalid")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_specs(project_root: str | Path = ".") -> tuple[V71CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar = _load_grammar(root)
    rows: list[V71CandidateSpec] = []
    for family in grammar["families"]:
        family_id = str(family["family_id"])
        mechanism = "v71_" + family_id.lower()
        for motif in family["motifs"]:
            for response in RESPONSES:
                for horizon in HORIZONS:
                    candidate_id = (
                        f"v71g1_{family_id.lower()}_{str(motif).lower()}_"
                        f"{response.lower()}_h{horizon}"
                    )
                    payload = {
                        "grammar_id": GRAMMAR_ID,
                        "candidate_id": candidate_id,
                        "family_id": family_id,
                        "mechanism_class": mechanism,
                        "motif": str(motif),
                        "response_policy": response,
                        "holding_minutes": horizon,
                        "cost_horizon": f"{horizon}m",
                        "product": "ES",
                        "grammar_sha256": GRAMMAR_SHA256,
                    }
                    rows.append(
                        V71CandidateSpec(
                            candidate_id=candidate_id,
                            family_id=family_id,
                            mechanism_class=mechanism,
                            motif=str(motif),
                            response_policy=response,
                            holding_minutes=horizon,
                            cost_horizon=f"{horizon}m",
                            product="ES",
                            specification_hash=_stable_hash(payload),
                        )
                    )
    if len(rows) != 256 or len({row.candidate_id for row in rows}) != 256:
        raise V71GrammarError("V7.1 grammar must contain 256 unique candidates")
    if any(
        sum(row.family_id == family for row in rows) != 32
        for family in {row.family_id for row in rows}
    ):
        raise V71GrammarError("V7.1 family allocation drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def load_v71_minute_features(project_root: str | Path = ".") -> pd.DataFrame:
    root = Path(project_root).resolve()
    _load_grammar(root)
    checks = {
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        MINUTE_PATH: MINUTE_SHA256,
        POWER_PATH: POWER_SHA256,
    }
    drift = [path for path, sha in checks.items() if _sha256(root / path) != sha]
    if drift:
        raise V71GrammarError("V7.1 frozen input hash drift: " + ",".join(drift))
    manifest = json.loads((root / FEATURE_MANIFEST_PATH).read_text(encoding="utf-8"))
    if manifest.get("outcome_or_pnl_columns") != []:
        raise V71GrammarError("V7.1 input manifest contains outcomes")
    frame = pd.read_parquet(root / MINUTE_PATH)
    frame = frame[frame["product"] == "ES"].sort_values(
        ["calendar_year", "minute_start_ns"], kind="stable"
    ).reset_index(drop=True)
    if frame.empty or set(frame["product"]) != {"ES"}:
        raise V71GrammarError("V7.1 ES minute source is empty")
    if np.any(frame["availability_ns"].to_numpy() < frame["source_close_ns"].to_numpy()):
        raise V71GrammarError("V7.1 source availability precedes close")
    return frame


def generate_signal_population(
    minute: pd.DataFrame,
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
            raise V71GrammarError(
                "V7.1 repeats exact cemetery classes: " + ",".join(collisions)
            )
    frame, states = _state_matrix(minute)
    output: dict[str, tuple[V71Signal, ...]] = {}
    for spec in specs:
        mask, direction = states[(spec.family_id, spec.motif)]
        side = direction if spec.response_policy == "CONTINUATION" else -direction
        output[spec.candidate_id] = tuple(
            _signals_for_spec(spec, frame, mask=mask, side=side)
        )
    if set(output) != {row.candidate_id for row in specs}:
        raise V71GrammarError("V7.1 signal population drift")
    return dict(sorted(output.items()))


def signal_path_hash(signals: Sequence[V71Signal]) -> str:
    path = [
        (
            row.decision_ns,
            row.entry_minute_start_ns,
            row.exit_minute_start_ns,
            row.side,
            row.contract,
        )
        for row in signals
    ]
    return _stable_hash(path)


def _state_matrix(
    minute: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[tuple[str, str], tuple[np.ndarray, np.ndarray]]]:
    frame = minute.copy()
    timestamps = pd.to_datetime(frame["minute_start_ns"], unit="ns", utc=True).dt.tz_convert(
        "America/Chicago"
    )
    frame["session_day"] = timestamps.dt.strftime("%Y-%m-%d")
    frame["local_minute"] = timestamps.dt.hour * 60 + timestamps.dt.minute
    group = frame.groupby("session_day", sort=False)
    intensity = frame["trade_count"].to_numpy(dtype=np.float64)
    volume = frame["total_volume"].to_numpy(dtype=np.float64)
    flow = frame["signed_aggressor_fraction"].to_numpy(dtype=np.float64)
    abs_flow = np.abs(flow)
    displacement = frame["price_change_points"].to_numpy(dtype=np.float64)
    abs_displacement = np.abs(displacement)
    path = frame["path_length_points"].to_numpy(dtype=np.float64)
    efficiency = np.abs(frame["signed_path_efficiency"].to_numpy(dtype=np.float64))
    minute_range = (frame["high"] - frame["low"]).to_numpy(dtype=np.float64)
    q = {
        "intensity_hi": _past_quantile(group, "trade_count", 0.80),
        "intensity_lo": _past_quantile(group, "trade_count", 0.20),
        "volume_hi": _past_quantile(group, "total_volume", 0.80),
        "flow_hi": _past_quantile_array(frame, abs_flow, 0.80),
        "flow_lo": _past_quantile_array(frame, abs_flow, 0.20),
        "disp_hi": _past_quantile_array(frame, abs_displacement, 0.80),
        "disp_lo": _past_quantile_array(frame, abs_displacement, 0.20),
        "path_hi": _past_quantile(group, "path_length_points", 0.80),
        "eff_hi": _past_quantile_array(frame, efficiency, 0.80),
        "eff_lo": _past_quantile_array(frame, efficiency, 0.20),
        "range_hi": _past_quantile_array(frame, minute_range, 0.80),
        "range_lo": _past_quantile_array(frame, minute_range, 0.20),
    }
    prev_intensity = _group_shift(frame, intensity, 1)
    prev_flow = _group_shift(frame, flow, 1)
    prev_abs_flow = np.abs(prev_flow)
    prev_disp = _group_shift(frame, displacement, 1)
    prev_range = _group_shift(frame, minute_range, 1)
    rolling_flow3 = _group_rolling_mean(frame, flow, 3)
    rolling_disp3 = _group_rolling_sum(frame, displacement, 3)
    rolling_disp5 = _group_rolling_sum(frame, displacement, 5)
    rolling_path5 = _group_rolling_sum(frame, path, 5)
    direction_flow = _direction(flow)
    direction_price = _direction(displacement)
    direction_roll3 = _direction(rolling_disp3)
    valid = np.isfinite(q["intensity_hi"]) & np.isfinite(q["flow_hi"])
    states: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}

    def put(family: str, motif: str, mask: np.ndarray, direction: np.ndarray) -> None:
        states[(family, motif)] = (valid & mask & (direction != 0), direction)

    put("EVENT_PARTICIPATION_STATE", "INTENSITY_BURST", intensity > q["intensity_hi"], direction_flow)
    put(
        "EVENT_PARTICIPATION_STATE",
        "DISTRIBUTED_PARTICIPATION",
        (intensity < q["intensity_hi"])
        & (abs_flow > q["flow_lo"])
        & (_direction(rolling_flow3) == direction_flow),
        _direction(rolling_flow3),
    )
    put(
        "EVENT_PARTICIPATION_STATE",
        "PARTICIPATION_ACCELERATION",
        (intensity > q["intensity_hi"])
        & (prev_intensity > 0.0)
        & (intensity > 1.5 * prev_intensity),
        direction_flow,
    )
    put(
        "EVENT_PARTICIPATION_STATE",
        "PARTICIPATION_DECAY",
        (prev_intensity > q["intensity_hi"])
        & (intensity < q["intensity_lo"]),
        _direction(prev_flow),
    )

    put(
        "EFFORT_WITHOUT_PROGRESS",
        "PRESSURE_LOW_DISPLACEMENT",
        (abs_flow > q["flow_hi"]) & (abs_displacement < q["disp_lo"]),
        direction_flow,
    )
    put(
        "EFFORT_WITHOUT_PROGRESS",
        "HIGH_PATH_LOW_EFFICIENCY",
        (path > q["path_hi"]) & (efficiency < q["eff_lo"]),
        direction_flow,
    )
    put(
        "EFFORT_WITHOUT_PROGRESS",
        "FAILED_EXTENSION",
        (np.abs(prev_disp) > q["disp_hi"])
        & (direction_price == -_direction(prev_disp)),
        _direction(prev_disp),
    )
    put(
        "EFFORT_WITHOUT_PROGRESS",
        "FLOW_PRICE_DISAGREEMENT",
        (direction_flow == -direction_price) & (abs_flow > q["flow_hi"]),
        direction_flow,
    )

    prior_high20 = _group_rolling_extreme(frame, frame["high"].to_numpy(float), 20, "max")
    prior_low20 = _group_rolling_extreme(frame, frame["low"].to_numpy(float), 20, "min")
    session_high = group["high"].cummax().groupby(frame["session_day"]).shift(1).to_numpy(float)
    session_low = group["low"].cummin().groupby(frame["session_day"]).shift(1).to_numpy(float)
    high = frame["high"].to_numpy(float)
    low = frame["low"].to_numpy(float)
    put("EXTREME_ACCEPTANCE_REJECTION", "ROLLING_HIGH_APPROACH", high >= prior_high20, np.ones(len(frame)))
    put("EXTREME_ACCEPTANCE_REJECTION", "ROLLING_LOW_APPROACH", low <= prior_low20, -np.ones(len(frame)))
    put("EXTREME_ACCEPTANCE_REJECTION", "SESSION_HIGH_TEST", high >= session_high, np.ones(len(frame)))
    put("EXTREME_ACCEPTANCE_REJECTION", "SESSION_LOW_TEST", low <= session_low, -np.ones(len(frame)))

    put(
        "EVENT_STATE_TRANSITIONS",
        "COMPRESSION_TO_EXPANSION",
        (prev_range < q["range_lo"]) & (minute_range > q["range_hi"]),
        direction_price,
    )
    put(
        "EVENT_STATE_TRANSITIONS",
        "TWO_SIDED_TO_ONE_SIDED",
        (prev_abs_flow < q["flow_lo"]) & (abs_flow > q["flow_hi"]),
        direction_flow,
    )
    put(
        "EVENT_STATE_TRANSITIONS",
        "FLOW_ACCELERATION_TRANSITION",
        (prev_abs_flow < q["flow_lo"]) & (abs_flow > q["flow_hi"]),
        direction_flow,
    )
    put(
        "EVENT_STATE_TRANSITIONS",
        "FLOW_DECAY_TRANSITION",
        (prev_abs_flow > q["flow_hi"]) & (abs_flow < q["flow_lo"]),
        _direction(prev_flow),
    )

    _session_transfer_states(frame, flow, states, valid)

    progress_efficiency = np.divide(
        np.abs(rolling_disp5),
        rolling_path5,
        out=np.zeros_like(rolling_disp5),
        where=rolling_path5 > 0.0,
    )
    progress_hi = _past_quantile_array(frame, progress_efficiency, 0.80)
    put("FUTURE_PATH_HAZARD", "TARGET_PROGRESS_STATE", progress_efficiency > progress_hi, _direction(rolling_disp5))
    put(
        "FUTURE_PATH_HAZARD",
        "CONTINUATION_HAZARD_STATE",
        (np.abs(rolling_flow3) > q["flow_hi"]) & (direction_roll3 == _direction(rolling_flow3)),
        direction_roll3,
    )
    put(
        "FUTURE_PATH_HAZARD",
        "REVERSAL_HAZARD_STATE",
        (np.abs(rolling_disp3) > q["disp_hi"]) & (abs_flow < q["flow_lo"]),
        direction_roll3,
    )
    put(
        "FUTURE_PATH_HAZARD",
        "TAIL_LOSS_AVOIDANCE_STATE",
        (minute_range < q["range_hi"]) & (efficiency > q["eff_hi"]),
        direction_price,
    )

    curvature = np.divide(path, np.maximum(abs_displacement, 0.25))
    curvature_hi = _past_quantile_array(frame, curvature, 0.80)
    prev_eff = _group_shift(frame, efficiency, 1)
    put("EVENT_PATH_GEOMETRY", "HIGH_EFFICIENCY_PATH", efficiency > q["eff_hi"], direction_price)
    put("EVENT_PATH_GEOMETRY", "HIGH_CURVATURE_PATH", curvature > curvature_hi, direction_price)
    put(
        "EVENT_PATH_GEOMETRY",
        "FAST_RECOVERY_PATH",
        (direction_price == -_direction(prev_disp))
        & (abs_displacement > 0.5 * np.abs(prev_disp)),
        direction_price,
    )
    put(
        "EVENT_PATH_GEOMETRY",
        "GEOMETRY_TRANSITION_MOTIF",
        (prev_eff < q["eff_lo"]) & (efficiency > q["eff_hi"]),
        direction_price,
    )

    align = direction_flow == direction_price
    put(
        "COST_RESILIENT_LOW_TURNOVER",
        "RARE_INTENSITY_WITH_PROGRESS",
        (intensity > q["intensity_hi"]) & (abs_displacement > q["disp_hi"]),
        direction_price,
    )
    put(
        "COST_RESILIENT_LOW_TURNOVER",
        "RARE_EFFICIENCY_WITH_FLOW",
        (efficiency > q["eff_hi"]) & (abs_flow > q["flow_hi"]) & align,
        direction_price,
    )
    put(
        "COST_RESILIENT_LOW_TURNOVER",
        "COMPOUND_FLOW_GEOMETRY",
        (intensity > q["intensity_hi"])
        & (path > q["path_hi"])
        & (efficiency > q["eff_lo"])
        & align,
        direction_price,
    )
    close = frame["close"].to_numpy(float)
    open_ = frame["open"].to_numpy(float)
    close_location = np.divide(close - low, high - low, out=np.full(len(frame), 0.5), where=(high - low) > 0.0)
    put(
        "COST_RESILIENT_LOW_TURNOVER",
        "SESSION_RARE_EXTREME",
        (minute_range > q["range_hi"])
        & ((close_location >= 0.80) | (close_location <= 0.20)),
        _direction(close - open_),
    )
    return frame, states


def _session_transfer_states(
    frame: pd.DataFrame,
    flow: np.ndarray,
    states: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]],
    valid: np.ndarray,
) -> None:
    local = frame["local_minute"].to_numpy(int)
    session = frame["session_day"].to_numpy(str)
    motifs = {
        "OPEN_TO_MORNING": (9 * 60, 8 * 60 + 30, 9 * 60),
        "MORNING_TO_MIDDAY": (11 * 60, 9 * 60, 11 * 60),
        "MIDDAY_TO_AFTERNOON": (13 * 60, 11 * 60, 13 * 60),
        "AFTERNOON_RESOLUTION": (14 * 60 + 30, 13 * 60, 14 * 60 + 30),
    }
    for motif, (decision_minute, start, end) in motifs.items():
        mask = np.zeros(len(frame), dtype=bool)
        direction = np.zeros(len(frame), dtype=np.int8)
        for day in np.unique(session):
            positions = np.flatnonzero(session == day)
            history = positions[(local[positions] >= start) & (local[positions] < end)]
            decision = positions[local[positions] == decision_minute]
            if history.size and decision.size:
                signed = float(np.sum(flow[history]))
                if signed != 0.0:
                    mask[decision[0]] = True
                    direction[decision[0]] = 1 if signed > 0.0 else -1
        states[("SESSION_TRANSFER", motif)] = (valid & mask, direction)


def _signals_for_spec(
    spec: V71CandidateSpec,
    frame: pd.DataFrame,
    *,
    mask: np.ndarray,
    side: np.ndarray,
) -> list[V71Signal]:
    output: list[V71Signal] = []
    starts = frame["minute_start_ns"].to_numpy(np.int64)
    availability = frame["availability_ns"].to_numpy(np.int64)
    session = frame["session_day"].to_numpy(str)
    contracts = frame["contract"].astype(str).to_numpy()
    years = frame["calendar_year"].to_numpy(int)
    next_allowed = 0
    for position in np.flatnonzero(mask):
        entry = int(position + 1)
        exit_position = int(entry + spec.holding_minutes)
        if position < next_allowed or exit_position >= len(frame):
            continue
        if (
            session[position] != session[entry]
            or session[entry] != session[exit_position]
            or contracts[position] != contracts[entry]
            or contracts[entry] != contracts[exit_position]
        ):
            continue
        decision = int(availability[position])
        if int(starts[entry]) < decision:
            raise V71GrammarError("V7.1 entry precedes feature availability")
        feature = {
            "candidate_id": spec.candidate_id,
            "source_position": int(position),
            "availability_ns": decision,
            "contract": contracts[position],
            "side": int(side[position]),
        }
        output.append(
            V71Signal(
                candidate_id=spec.candidate_id,
                family_id=spec.family_id,
                motif=spec.motif,
                response_policy=spec.response_policy,
                holding_minutes=spec.holding_minutes,
                calendar_year=int(years[position]),
                session_day=str(session[position]),
                source_position=int(position),
                availability_ns=decision,
                decision_ns=decision,
                entry_minute_start_ns=int(starts[entry]),
                exit_minute_start_ns=int(starts[exit_position]),
                side=int(side[position]),
                contract=str(contracts[position]),
                feature_snapshot_hash=_stable_hash(feature),
            )
        )
        next_allowed = exit_position
    return output


def _past_quantile(group: Any, column: str, quantile: float) -> np.ndarray:
    return (
        group[column]
        .transform(lambda values: values.shift(1).rolling(ROLLING, min_periods=ROLLING).quantile(quantile))
        .to_numpy(dtype=np.float64)
    )


def _past_quantile_array(frame: pd.DataFrame, values: np.ndarray, quantile: float) -> np.ndarray:
    source = pd.Series(values, index=frame.index)
    return (
        source.groupby(frame["session_day"], sort=False)
        .transform(lambda row: row.shift(1).rolling(ROLLING, min_periods=ROLLING).quantile(quantile))
        .to_numpy(dtype=np.float64)
    )


def _group_shift(frame: pd.DataFrame, values: np.ndarray, periods: int) -> np.ndarray:
    return pd.Series(values, index=frame.index).groupby(frame["session_day"], sort=False).shift(periods).to_numpy(float)


def _group_rolling_mean(frame: pd.DataFrame, values: np.ndarray, window: int) -> np.ndarray:
    return pd.Series(values, index=frame.index).groupby(frame["session_day"], sort=False).transform(lambda row: row.rolling(window, min_periods=window).mean()).to_numpy(float)


def _group_rolling_sum(frame: pd.DataFrame, values: np.ndarray, window: int) -> np.ndarray:
    return pd.Series(values, index=frame.index).groupby(frame["session_day"], sort=False).transform(lambda row: row.rolling(window, min_periods=window).sum()).to_numpy(float)


def _group_rolling_extreme(
    frame: pd.DataFrame, values: np.ndarray, window: int, operation: str
) -> np.ndarray:
    source = pd.Series(values, index=frame.index).groupby(frame["session_day"], sort=False)
    if operation == "max":
        return source.transform(lambda row: row.shift(1).rolling(window, min_periods=window).max()).to_numpy(float)
    return source.transform(lambda row: row.shift(1).rolling(window, min_periods=window).min()).to_numpy(float)


def _direction(values: np.ndarray) -> np.ndarray:
    return np.sign(np.nan_to_num(values, nan=0.0)).astype(np.int8)


def _load_grammar(root: Path) -> dict[str, Any]:
    path = root / GRAMMAR_PATH
    if _sha256(path) != GRAMMAR_SHA256:
        raise V71GrammarError("V7.1 grammar WORM hash mismatch")
    return json.loads(path.read_text(encoding="utf-8"))


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
    "V71CandidateSpec",
    "V71GrammarError",
    "V71Signal",
    "candidate_specs",
    "generate_signal_population",
    "load_v71_minute_features",
    "signal_path_hash",
]
