from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


GRAMMAR_ID = "hydra_v7_d1_microstructure_grammar_0001"
PREREGISTRATION_SHA256 = (
    "f7b7f3d8d0a43749d31986e4848eeb7285123654dd9ca47eb327284831dd1691"
)
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_date_matched_event_store_v1.json"
FEATURE_MANIFEST_SHA256 = (
    "5c700f1ec38ab03a1206af7013a28600a681a0b3334781979b37ec356b5421ab"
)
MINUTE_PATH = "data/cache/v7_d1/date_matched_minute_print_features_v2.parquet"
MINUTE_SHA256 = "2bf13b332118392673247f5c564a3d1533d84c61177398e28a9832b3ca116cbb"
EVENT_PATH = "data/cache/v7_d1/date_matched_event_bars_v1.parquet"
EVENT_SHA256 = "ea0208fc3666f912b39e9b21b302a466bd7ee00c802140d6a6beed73098aa4a3"
PRODUCTS = ("ES", "MES")
ROLLING_BARS = 1000


class D1GrammarError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class D1CandidateSpec:
    candidate_id: str
    hypothesis_id: str
    mechanism_class: str
    product: str
    source_bar_type: str
    holding_units: int
    cost_horizon: str
    side_relation: str
    economic_hypothesis: str
    specification_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class D1Signal:
    candidate_id: str
    hypothesis_id: str
    product: str
    calendar_year: int
    source_bar_type: str
    source_position: int
    decision_ns: int
    availability_ns: int
    side: int
    contract: str
    holding_units: int
    execution_rule: str
    feature_snapshot_hash: str

    def __post_init__(self) -> None:
        if self.side not in {-1, 1}:
            raise D1GrammarError("D1 signal side must be -1 or +1")
        if self.availability_ns > self.decision_ns:
            raise D1GrammarError("D1 signal uses unavailable features")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_specs() -> tuple[D1CandidateSpec, ...]:
    rows: list[tuple[str, str, str, str, int, str, str, str]] = [
        (
            "aggressor_persistence",
            "D1H1_AGGRESSOR_IMBALANCE_PERSISTENCE",
            "print_aggressor_imbalance_persistence",
            "VOLUME_BAR",
            5,
            "5m",
            "same",
            "Urgent informed or mandate-driven aggressors split a parent order "
            "across successive volume bars, so efficient signed flow continues.",
        ),
        (
            "absorption_reversal",
            "D1H2_PRINT_ABSORPTION_REVERSAL",
            "print_absorption_exhaustion_reversal",
            "SIGNED_IMBALANCE_BAR",
            5,
            "5m",
            "opposite",
            "Extreme signed flow with little price progress is absorbed and "
            "reverses after the aggressive burst exhausts.",
        ),
        (
            "sweep_quality",
            "D1H3_SWEEP_QUALITY_CONTINUATION",
            "print_sweep_quality_continuation",
            "DOLLAR_BAR",
            3,
            "5m",
            "same",
            "A dense same-side burst with efficient price follow-through "
            "identifies a genuine urgent sweep that continues.",
        ),
        (
            "cash_open_aggression",
            "D1H4_CASH_OPEN_AGGRESSION_STATE",
            "cash_open_aggression_state_persistence",
            "MINUTE_PRINT_FEATURES",
            59,
            "60m",
            "same",
            "A large signed cash-open aggression state reflects inventory "
            "transfer that persists through the next hour.",
        ),
    ]
    specs: list[D1CandidateSpec] = []
    for short, hypothesis, mechanism, bar_type, hold, horizon, relation, economic in rows:
        for product in PRODUCTS:
            candidate_id = f"v7d1g1_{short}_{product}"
            payload = {
                "grammar_id": GRAMMAR_ID,
                "candidate_id": candidate_id,
                "hypothesis_id": hypothesis,
                "mechanism_class": mechanism,
                "product": product,
                "source_bar_type": bar_type,
                "holding_units": hold,
                "cost_horizon": horizon,
                "side_relation": relation,
                "economic_hypothesis": economic,
                "preregistration_sha256": PREREGISTRATION_SHA256,
            }
            specs.append(
                D1CandidateSpec(
                    candidate_id=candidate_id,
                    hypothesis_id=hypothesis,
                    mechanism_class=mechanism,
                    product=product,
                    source_bar_type=bar_type,
                    holding_units=hold,
                    cost_horizon=horizon,
                    side_relation=relation,
                    economic_hypothesis=economic,
                    specification_hash=_stable_hash(payload),
                )
            )
    if len(specs) != 8 or len({row.specification_hash for row in specs}) != 8:
        raise D1GrammarError("D1 grammar must contain 8 fixed structures")
    return tuple(specs)


def load_feature_store(project_root: str | Path = ".") -> tuple[pd.DataFrame, pd.DataFrame]:
    root = Path(project_root).resolve()
    checks = {
        "feature manifest": (_sha256(root / FEATURE_MANIFEST_PATH), FEATURE_MANIFEST_SHA256),
        "minute store": (_sha256(root / MINUTE_PATH), MINUTE_SHA256),
        "event store": (_sha256(root / EVENT_PATH), EVENT_SHA256),
    }
    drift = [name for name, (actual, expected) in checks.items() if actual != expected]
    if drift:
        raise D1GrammarError("D1 frozen feature hash mismatch: " + ",".join(drift))
    manifest = json.loads((root / FEATURE_MANIFEST_PATH).read_text(encoding="utf-8"))
    if manifest.get("outcome_or_pnl_columns") != []:
        raise D1GrammarError("D1 feature manifest contains outcomes")
    minute = pd.read_parquet(root / MINUTE_PATH)
    event = pd.read_parquet(root / EVENT_PATH)
    return minute, event


def generate_signal_population(
    minute: pd.DataFrame,
    event: pd.DataFrame,
) -> dict[str, tuple[D1Signal, ...]]:
    specs = {row.candidate_id: row for row in candidate_specs()}
    output: dict[str, tuple[D1Signal, ...]] = {}
    for product in PRODUCTS:
        output[f"v7d1g1_aggressor_persistence_{product}"] = tuple(
            _event_signals(
                specs[f"v7d1g1_aggressor_persistence_{product}"], event
            )
        )
        output[f"v7d1g1_absorption_reversal_{product}"] = tuple(
            _event_signals(specs[f"v7d1g1_absorption_reversal_{product}"], event)
        )
        output[f"v7d1g1_sweep_quality_{product}"] = tuple(
            _event_signals(specs[f"v7d1g1_sweep_quality_{product}"], event)
        )
        output[f"v7d1g1_cash_open_aggression_{product}"] = tuple(
            _cash_open_signals(
                specs[f"v7d1g1_cash_open_aggression_{product}"], minute
            )
        )
    if set(output) != set(specs):
        raise D1GrammarError("D1 signal population drift")
    return dict(sorted(output.items()))


def _event_signals(
    spec: D1CandidateSpec, event: pd.DataFrame
) -> list[D1Signal]:
    source = event[
        (event["product"] == spec.product)
        & (event["bar_type"] == spec.source_bar_type)
    ].sort_values(["calendar_year", "start_event_ns"], kind="stable")
    output: list[D1Signal] = []
    for year, raw in source.groupby("calendar_year", sort=True):
        frame = raw.reset_index(drop=True)
        signed_fraction = (
            frame["signed_aggressor_volume"].to_numpy(dtype=np.float64)
            / frame["total_volume"].to_numpy(dtype=np.float64)
        )
        price_change = frame["price_change_points"].to_numpy(dtype=np.float64)
        path = frame["path_length_points"].to_numpy(dtype=np.float64)
        efficiency = np.divide(
            price_change,
            path,
            out=np.zeros_like(price_change),
            where=path > 0.0,
        )
        abs_signed = np.abs(signed_fraction)
        q90_signed = _past_rolling_quantile(abs_signed, ROLLING_BARS, 0.90)
        trade_count = frame["trade_count"].to_numpy(dtype=np.float64)
        q90_trades = _past_rolling_quantile(trade_count, ROLLING_BARS, 0.90)
        q75_signed = _past_rolling_quantile(abs_signed, ROLLING_BARS, 0.75)
        next_allowed = 0
        starts = frame["start_event_ns"].to_numpy(dtype=np.int64)
        availability = frame["availability_ns"].to_numpy(dtype=np.int64)
        contracts = frame["contract"].astype(str).to_numpy()
        for position in range(ROLLING_BARS, len(frame)):
            if position < next_allowed or signed_fraction[position] == 0.0:
                continue
            flow_side = int(math.copysign(1, signed_fraction[position]))
            condition = False
            if spec.source_bar_type == "VOLUME_BAR":
                condition = bool(
                    abs_signed[position] >= q90_signed[position]
                    and price_change[position] != 0.0
                    and int(math.copysign(1, price_change[position])) == flow_side
                    and abs(efficiency[position]) >= 0.25
                )
            elif spec.source_bar_type == "SIGNED_IMBALANCE_BAR":
                condition = bool(
                    abs_signed[position] >= q90_signed[position]
                    and abs(efficiency[position]) <= 0.10
                )
            elif spec.source_bar_type == "DOLLAR_BAR":
                condition = bool(
                    trade_count[position] >= q90_trades[position]
                    and abs_signed[position] >= q75_signed[position]
                    and price_change[position] != 0.0
                    and int(math.copysign(1, price_change[position])) == flow_side
                    and abs(efficiency[position]) >= 0.50
                )
            if not condition:
                continue
            entry_position = int(
                np.searchsorted(starts, availability[position], side="left")
            )
            entry_position = max(entry_position, position + 1)
            exit_position = entry_position + spec.holding_units - 1
            if exit_position >= len(frame):
                continue
            if len(set(contracts[position : exit_position + 1])) != 1:
                continue
            side = -flow_side if spec.side_relation == "opposite" else flow_side
            output.append(
                D1Signal(
                    candidate_id=spec.candidate_id,
                    hypothesis_id=spec.hypothesis_id,
                    product=spec.product,
                    calendar_year=int(year),
                    source_bar_type=spec.source_bar_type,
                    source_position=position,
                    decision_ns=int(availability[position]),
                    availability_ns=int(availability[position]),
                    side=side,
                    contract=str(contracts[position]),
                    holding_units=spec.holding_units,
                    execution_rule="NEXT_EVENT_OPEN_THEN_FIXED_EVENT_COUNT_CLOSE",
                    feature_snapshot_hash=_stable_hash(
                        {
                            "signed_fraction": signed_fraction[position],
                            "q90_signed": q90_signed[position],
                            "q75_signed": q75_signed[position],
                            "trade_count": trade_count[position],
                            "q90_trades": q90_trades[position],
                            "efficiency": efficiency[position],
                        }
                    ),
                )
            )
            next_allowed = exit_position + 1
    return output


def _cash_open_signals(
    spec: D1CandidateSpec, minute: pd.DataFrame
) -> list[D1Signal]:
    source = minute[minute["product"] == spec.product].copy()
    timestamps = pd.to_datetime(
        source["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    source["local_date"] = timestamps.date
    source["local_minute"] = timestamps.hour * 60 + timestamps.minute
    output: list[D1Signal] = []
    for year, year_frame in source.groupby("calendar_year", sort=True):
        sessions: list[tuple[Any, float, int, str]] = []
        by_date = list(year_frame.groupby("local_date", sort=True))
        for local_date, day in by_date:
            opening = day[
                (day["local_minute"] >= 8 * 60 + 30)
                & (day["local_minute"] <= 9 * 60)
            ]
            decision_rows = day[day["local_minute"] == 9 * 60]
            if opening.empty or len(decision_rows) != 1:
                continue
            total = float(opening["total_volume"].sum())
            signed = float(opening["signed_aggressor_volume"].sum())
            if total <= 0.0:
                continue
            decision = decision_rows.iloc[0]
            sessions.append(
                (
                    local_date,
                    signed / total,
                    int(decision["availability_ns"]),
                    str(decision["contract"]),
                )
            )
        history: list[float] = []
        for position, (local_date, fraction, availability, contract) in enumerate(sessions):
            threshold = float(np.median(history[-5:])) if len(history) >= 5 else None
            if threshold is not None and abs(fraction) > threshold and fraction != 0.0:
                output.append(
                    D1Signal(
                        candidate_id=spec.candidate_id,
                        hypothesis_id=spec.hypothesis_id,
                        product=spec.product,
                        calendar_year=int(year),
                        source_bar_type=spec.source_bar_type,
                        source_position=position,
                        decision_ns=availability,
                        availability_ns=availability,
                        side=int(math.copysign(1, fraction)),
                        contract=contract,
                        holding_units=spec.holding_units,
                        execution_rule="NEXT_MINUTE_0901_OPEN_TO_1000_CLOSE",
                        feature_snapshot_hash=_stable_hash(
                            {
                                "local_date": str(local_date),
                                "opening_signed_fraction": fraction,
                                "prior_5_session_median": threshold,
                            }
                        ),
                    )
                )
            history.append(abs(fraction))
    return output


def _past_rolling_quantile(
    values: Sequence[float], window: int, quantile: float
) -> np.ndarray:
    if window <= 0 or not 0.0 <= quantile <= 1.0:
        raise D1GrammarError("invalid rolling quantile policy")
    return (
        pd.Series(np.asarray(values, dtype=np.float64))
        .rolling(window=window, min_periods=window)
        .quantile(quantile)
        .shift(1)
        .to_numpy(dtype=np.float64)
    )


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda value: value.item() if hasattr(value, "item") else str(value),
        ).encode("utf-8")
    ).hexdigest()


__all__ = [
    "D1CandidateSpec",
    "D1GrammarError",
    "D1Signal",
    "candidate_specs",
    "generate_signal_population",
    "load_feature_store",
]
