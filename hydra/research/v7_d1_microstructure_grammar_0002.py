from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.research.v7_d1_microstructure_grammar import load_feature_store


GRAMMAR_ID = "hydra_v7_d1_microstructure_grammar_0002"
GRAMMAR_PATH = Path("WORM/v7-d1-microstructure-grammar-0002-2026-07-12.json")
GRAMMAR_SHA256 = "fac0b5166351940d1fde5334bdeaf846d56e56efc8cef9772a9599b8b86feee9"
PRODUCTS = ("ES", "MES")
BLOCK_MINUTES = 5
ROLLING_BLOCKS = 20


class D1Grammar0002Error(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class D1G2CandidateSpec:
    candidate_id: str
    hypothesis_id: str
    mechanism_class: str
    product: str
    holding_minutes: int
    cost_horizon: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class D1G2Signal:
    candidate_id: str
    hypothesis_id: str
    mechanism_class: str
    product: str
    calendar_year: int
    source_bar_type: str
    source_block_start_ns: int
    source_close_ns: int
    decision_ns: int
    availability_ns: int
    entry_minute_start_ns: int
    exit_minute_start_ns: int
    side: int
    contract: str
    holding_minutes: int
    execution_rule: str
    feature_snapshot_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_specs(project_root: str | Path = ".") -> tuple[D1G2CandidateSpec, ...]:
    root = Path(project_root).resolve()
    grammar_path = root / GRAMMAR_PATH
    if _sha256(grammar_path) != GRAMMAR_SHA256:
        raise D1Grammar0002Error("D1 grammar 0002 WORM hash mismatch")
    payload = json.loads(grammar_path.read_text(encoding="utf-8"))
    rows: list[D1G2CandidateSpec] = []
    for structure in payload["candidate_structures"]:
        holding = 30 if structure["hypothesis_id"] == "D1H8_VWAP_ACCEPTANCE_WITH_FLOW" else 15
        for product in structure["products"]:
            rows.append(
                D1G2CandidateSpec(
                    candidate_id=str(structure["candidate_id_pattern"]).format(
                        product=product
                    ),
                    hypothesis_id=str(structure["hypothesis_id"]),
                    mechanism_class=str(structure["mechanism_class"]),
                    product=str(product),
                    holding_minutes=holding,
                    cost_horizon="30m" if holding == 30 else "15m",
                )
            )
    if len(rows) != 8 or len({row.candidate_id for row in rows}) != 8:
        raise D1Grammar0002Error("D1 grammar 0002 candidate count drift")
    return tuple(sorted(rows, key=lambda row: row.candidate_id))


def build_five_minute_features(minute: pd.DataFrame) -> pd.DataFrame:
    required = {
        "product",
        "contract",
        "calendar_year",
        "minute_start_ns",
        "source_close_ns",
        "availability_ns",
        "open",
        "high",
        "low",
        "close",
        "total_volume",
        "signed_aggressor_volume",
    }
    missing = required - set(minute.columns)
    if missing:
        raise D1Grammar0002Error(f"minute features missing: {sorted(missing)}")
    rows: list[dict[str, Any]] = []
    for (product, year), raw in minute.groupby(
        ["product", "calendar_year"], sort=True
    ):
        frame = raw.sort_values("minute_start_ns", kind="stable").reset_index(
            drop=True
        )
        timestamps = pd.to_datetime(
            frame["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
        ).tz_convert("America/Chicago")
        frame["_date"] = [value.isoformat() for value in timestamps.date]
        frame["_minute"] = timestamps.hour * 60 + timestamps.minute
        for local_date, day in frame.groupby("_date", sort=True):
            day = day.sort_values("minute_start_ns", kind="stable").reset_index(
                drop=True
            )
            for start in range(0, len(day) - BLOCK_MINUTES + 1, BLOCK_MINUTES):
                block = day.iloc[start : start + BLOCK_MINUTES]
                minute_values = block["_minute"].to_numpy(dtype=np.int64)
                if (
                    len(block) != BLOCK_MINUTES
                    or not np.all(np.diff(minute_values) == 1)
                    or len(set(block["contract"].astype(str))) != 1
                ):
                    continue
                total = float(block["total_volume"].sum())
                signed = float(block["signed_aggressor_volume"].sum())
                rows.append(
                    {
                        "product": str(product),
                        "calendar_year": int(year),
                        "local_date": str(local_date),
                        "contract": str(block.iloc[0]["contract"]),
                        "block_start_ns": int(block.iloc[0]["minute_start_ns"]),
                        "source_close_ns": int(block.iloc[-1]["source_close_ns"]),
                        "availability_ns": int(block.iloc[-1]["availability_ns"]),
                        "open": float(block.iloc[0]["open"]),
                        "high": float(block["high"].max()),
                        "low": float(block["low"].min()),
                        "close": float(block.iloc[-1]["close"]),
                        "total_volume": total,
                        "signed_aggressor_volume": signed,
                        "signed_fraction": signed / total if total > 0.0 else 0.0,
                    }
                )
    output = pd.DataFrame(rows)
    if output.empty:
        raise D1Grammar0002Error("no complete five-minute blocks")
    return output.sort_values(
        ["calendar_year", "block_start_ns", "product"], kind="stable"
    ).reset_index(drop=True)


def generate_signal_population(
    minute: pd.DataFrame,
    *,
    project_root: str | Path = ".",
) -> dict[str, tuple[D1G2Signal, ...]]:
    specs = {row.candidate_id: row for row in candidate_specs(project_root)}
    blocks = build_five_minute_features(minute)
    minute_lookup = _minute_execution_lookup(minute)
    output: dict[str, tuple[D1G2Signal, ...]] = {}
    for product in PRODUCTS:
        own = blocks[blocks["product"] == product]
        sibling_product = "MES" if product == "ES" else "ES"
        sibling = blocks[blocks["product"] == sibling_product]
        aligned = own.merge(
            sibling[
                [
                    "calendar_year",
                    "block_start_ns",
                    "signed_fraction",
                ]
            ],
            on=["calendar_year", "block_start_ns"],
            how="inner",
            suffixes=("", "_sibling"),
            validate="one_to_one",
        )
        by_hypothesis = {
            "D1H5_DELTA_EXTREME_REJECTION": _delta_extreme_signals,
            "D1H6_MINI_MICRO_PARTICIPATION_DIVERGENCE": _participation_divergence_signals,
            "D1H7_AGGRESSOR_REGIME_FLIP": _regime_flip_signals,
            "D1H8_VWAP_ACCEPTANCE_WITH_FLOW": _vwap_acceptance_signals,
        }
        for hypothesis_id, generator in by_hypothesis.items():
            spec = next(
                value
                for value in specs.values()
                if value.product == product
                and value.hypothesis_id == hypothesis_id
            )
            raw_signals = generator(spec, aligned)
            output[spec.candidate_id] = tuple(
                _attach_execution(spec, raw_signals, minute_lookup)
            )
    if set(output) != set(specs):
        raise D1Grammar0002Error("D1 grammar 0002 signal population drift")
    return dict(sorted(output.items()))


def _delta_extreme_signals(
    spec: D1G2CandidateSpec, frame: pd.DataFrame
) -> list[tuple[pd.Series, int, Mapping[str, Any]]]:
    output: list[tuple[pd.Series, int, Mapping[str, Any]]] = []
    for _, raw in frame.groupby("calendar_year", sort=True):
        rows = raw.sort_values("block_start_ns", kind="stable").reset_index(drop=True)
        signed = rows["signed_fraction"].to_numpy(dtype=np.float64)
        threshold = _past_quantile(np.abs(signed), ROLLING_BLOCKS, 0.75)
        for position in range(ROLLING_BLOCKS, len(rows)):
            prior = rows.iloc[position - 12 : position]
            current = rows.iloc[position]
            prior_high = float(prior["high"].max())
            prior_low = float(prior["low"].min())
            side = 0
            if (
                float(current["high"]) > prior_high
                and float(current["close"]) <= prior_high
                and signed[position] > 0.0
            ):
                side = -1
            elif (
                float(current["low"]) < prior_low
                and float(current["close"]) >= prior_low
                and signed[position] < 0.0
            ):
                side = 1
            if side and abs(signed[position]) >= threshold[position]:
                output.append(
                    (
                        current,
                        side,
                        {
                            "prior_high": prior_high,
                            "prior_low": prior_low,
                            "signed_fraction": signed[position],
                            "q75": threshold[position],
                        },
                    )
                )
    return output


def _participation_divergence_signals(
    spec: D1G2CandidateSpec, frame: pd.DataFrame
) -> list[tuple[pd.Series, int, Mapping[str, Any]]]:
    output: list[tuple[pd.Series, int, Mapping[str, Any]]] = []
    for _, raw in frame.groupby("calendar_year", sort=True):
        rows = raw.sort_values("block_start_ns", kind="stable").reset_index(drop=True)
        own = rows["signed_fraction"].to_numpy(dtype=np.float64)
        sibling = rows["signed_fraction_sibling"].to_numpy(dtype=np.float64)
        own_q90 = _past_quantile(np.abs(own), ROLLING_BLOCKS, 0.90)
        sibling_median = _past_quantile(
            np.abs(sibling), ROLLING_BLOCKS, 0.50
        )
        for position in range(ROLLING_BLOCKS, len(rows)):
            if own[position] == 0.0 or abs(own[position]) < own_q90[position]:
                continue
            opposite = own[position] * sibling[position] < 0.0
            muted = abs(sibling[position]) <= sibling_median[position]
            if opposite or muted:
                output.append(
                    (
                        rows.iloc[position],
                        -int(math.copysign(1, own[position])),
                        {
                            "own_signed_fraction": own[position],
                            "own_q90": own_q90[position],
                            "sibling_signed_fraction": sibling[position],
                            "sibling_median": sibling_median[position],
                        },
                    )
                )
    return output


def _regime_flip_signals(
    spec: D1G2CandidateSpec, frame: pd.DataFrame
) -> list[tuple[pd.Series, int, Mapping[str, Any]]]:
    output: list[tuple[pd.Series, int, Mapping[str, Any]]] = []
    for _, raw in frame.groupby("calendar_year", sort=True):
        rows = raw.sort_values("block_start_ns", kind="stable").reset_index(drop=True)
        signed = rows["signed_fraction"].to_numpy(dtype=np.float64)
        threshold = _past_quantile(np.abs(signed), ROLLING_BLOCKS, 0.75)
        for position in range(ROLLING_BLOCKS, len(rows)):
            prior = np.sign(signed[position - 3 : position])
            current_sign = int(np.sign(signed[position]))
            if (
                current_sign
                and np.all(prior == prior[0])
                and prior[0] != 0.0
                and current_sign == -int(prior[0])
                and abs(signed[position]) >= threshold[position]
            ):
                output.append(
                    (
                        rows.iloc[position],
                        current_sign,
                        {
                            "prior_sign": int(prior[0]),
                            "current_signed_fraction": signed[position],
                            "q75": threshold[position],
                        },
                    )
                )
    return output


def _vwap_acceptance_signals(
    spec: D1G2CandidateSpec, frame: pd.DataFrame
) -> list[tuple[pd.Series, int, Mapping[str, Any]]]:
    output: list[tuple[pd.Series, int, Mapping[str, Any]]] = []
    for _, raw in frame.groupby("calendar_year", sort=True):
        rows = raw.sort_values("block_start_ns", kind="stable").reset_index(drop=True)
        close = rows["close"].to_numpy(dtype=np.float64)
        volume = rows["total_volume"].to_numpy(dtype=np.float64)
        signed = rows["signed_aggressor_volume"].to_numpy(dtype=np.float64)
        prior_vwap = np.full(len(rows), np.nan, dtype=np.float64)
        for position in range(6, len(rows)):
            window = slice(position - 6, position)
            denominator = float(np.sum(volume[window]))
            if denominator > 0.0:
                prior_vwap[position] = float(
                    np.sum(close[window] * volume[window]) / denominator
                )
        for position in range(7, len(rows)):
            if not (
                np.isfinite(prior_vwap[position - 1])
                and np.isfinite(prior_vwap[position])
            ):
                continue
            previous_side = int(np.sign(close[position - 1] - prior_vwap[position - 1]))
            current_side = int(np.sign(close[position] - prior_vwap[position]))
            if (
                current_side
                and current_side == previous_side
                and int(np.sign(signed[position - 1])) == current_side
                and int(np.sign(signed[position])) == current_side
            ):
                output.append(
                    (
                        rows.iloc[position],
                        current_side,
                        {
                            "previous_close": close[position - 1],
                            "previous_prior_vwap": prior_vwap[position - 1],
                            "current_close": close[position],
                            "current_prior_vwap": prior_vwap[position],
                        },
                    )
                )
    return output


def _attach_execution(
    spec: D1G2CandidateSpec,
    raw_signals: Sequence[tuple[pd.Series, int, Mapping[str, Any]]],
    minute_lookup: Mapping[tuple[str, int], pd.DataFrame],
) -> list[D1G2Signal]:
    output: list[D1G2Signal] = []
    next_allowed_ns = -1
    for row, side, snapshot in sorted(
        raw_signals, key=lambda value: int(value[0]["availability_ns"])
    ):
        decision_ns = int(row["availability_ns"])
        if decision_ns < next_allowed_ns:
            continue
        year = int(row["calendar_year"])
        frame = minute_lookup[(spec.product, year)]
        starts = frame["minute_start_ns"].to_numpy(dtype=np.int64)
        entry_position = int(np.searchsorted(starts, decision_ns, side="left"))
        exit_position = entry_position + spec.holding_minutes
        if entry_position >= len(frame) or exit_position >= len(frame):
            continue
        segment = frame.iloc[entry_position : exit_position + 1]
        timestamps = segment["minute_start_ns"].to_numpy(dtype=np.int64)
        if (
            len(set(segment["contract"].astype(str))) != 1
            or str(segment.iloc[0]["contract"]) != str(row["contract"])
            or not np.all(np.diff(timestamps) == 60_000_000_000)
        ):
            continue
        entry_ns = int(segment.iloc[0]["minute_start_ns"])
        exit_ns = int(segment.iloc[-1]["minute_start_ns"])
        output.append(
            D1G2Signal(
                candidate_id=spec.candidate_id,
                hypothesis_id=spec.hypothesis_id,
                mechanism_class=spec.mechanism_class,
                product=spec.product,
                calendar_year=year,
                source_bar_type=(
                    "SYNCHRONIZED_FIVE_MINUTE_PRINT_AGGREGATE"
                    if spec.hypothesis_id
                    == "D1H6_MINI_MICRO_PARTICIPATION_DIVERGENCE"
                    else "FIVE_MINUTE_PRINT_AGGREGATE"
                ),
                source_block_start_ns=int(row["block_start_ns"]),
                source_close_ns=int(row["source_close_ns"]),
                decision_ns=decision_ns,
                availability_ns=decision_ns,
                entry_minute_start_ns=entry_ns,
                exit_minute_start_ns=exit_ns,
                side=int(side),
                contract=str(row["contract"]),
                holding_minutes=spec.holding_minutes,
                execution_rule="NEXT_MINUTE_OPEN_TO_FIXED_LATER_MINUTE_OPEN",
                feature_snapshot_hash=_stable_hash(snapshot),
            )
        )
        next_allowed_ns = exit_ns
    return output


def _minute_execution_lookup(
    minute: pd.DataFrame,
) -> dict[tuple[str, int], pd.DataFrame]:
    return {
        (str(product), int(year)): frame.sort_values(
            "minute_start_ns", kind="stable"
        ).reset_index(drop=True)
        for (product, year), frame in minute.groupby(
            ["product", "calendar_year"], sort=True
        )
    }


def _past_quantile(
    values: Sequence[float], window: int, quantile: float
) -> np.ndarray:
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
    "D1G2CandidateSpec",
    "D1G2Signal",
    "D1Grammar0002Error",
    "build_five_minute_features",
    "candidate_specs",
    "generate_signal_population",
]
