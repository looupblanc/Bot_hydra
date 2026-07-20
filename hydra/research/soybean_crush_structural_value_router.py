"""Causal soybean-crush structural-value router tripwire.

The research relationship observes all three soybean-complex legs, but every
economic event executes exactly one frozen leg.  Candidate selection is
strictly staged: discovery for all 24 specifications, validation only for the
discovery-selected specifications, and final development only for validation
passers.
"""

from __future__ import annotations

import gc
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.data.budget import sha256_file
from hydra.data.databento_loader import _import_databento
from hydra.economic_evolution.schema import stable_hash


MANIFEST = Path("config/research/soybean_crush_structural_value_router_v1.json")
RECEIPT = Path(
    "reports/data_access/soybean_crush_structural_value_router_acquisition_receipt.json"
)
CHICAGO = "America/Chicago"
LEGS = ("ZS", "ZM", "ZL")
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
CONTROLS = (
    "MATCHED_EXECUTION_LEG_PRICE_ONLY_DISPLACEMENT",
    "DIRECTION_FLIP",
    "SESSION_AND_EXPOSURE_MATCHED_RANDOM_ROUTING",
)
MONTH_CODE = {
    "F": "JAN",
    "H": "MAR",
    "K": "MAY",
    "N": "JUL",
    "Q": "AUG",
    "U": "SEP",
    "V": "OCT",
    "X": "NOV",
    "Z": "DEC",
}


class SoybeanCrushRouterError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Candidate:
    execution_leg: str
    mechanism: str
    lookback_minutes: int
    robust_z_threshold: float


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    if resolved != root and root not in resolved.parents:
        raise SoybeanCrushRouterError("input path escapes project root")
    if not resolved.is_file():
        raise SoybeanCrushRouterError(f"required input missing: {resolved}")
    return resolved


def _read_manifest(root: Path) -> dict[str, Any]:
    manifest = _read_json(_inside(root, MANIFEST))
    core = dict(manifest)
    claimed = str(core.pop("manifest_hash", ""))
    if stable_hash(core) != claimed:
        raise SoybeanCrushRouterError("frozen manifest hash drift")
    return manifest


def audit_inputs(root: str | Path) -> dict[str, Any]:
    """Validate immutable inputs; deliberately fail closed before acquisition."""

    project = Path(root).resolve()
    manifest = _read_manifest(project)
    frozen = manifest["frozen_existing_zs_input"]
    frozen_receipt_path = _inside(project, frozen["receipt_path"])
    if sha256_file(frozen_receipt_path) != frozen["receipt_file_sha256"]:
        raise SoybeanCrushRouterError("frozen ZS receipt hash drift")
    frozen_receipt = _read_json(frozen_receipt_path)
    if frozen_receipt.get("receipt_hash") != frozen["receipt_hash"]:
        raise SoybeanCrushRouterError("frozen ZS receipt semantic drift")
    zs_files: dict[str, Path] = {}
    for kind in ("ohlcv", "definition"):
        path = _inside(project, frozen[f"{kind}_path"])
        if sha256_file(path) != frozen[f"{kind}_sha256"]:
            raise SoybeanCrushRouterError(f"frozen ZS {kind} drift")
        zs_files[kind] = path

    receipt_path = project / RECEIPT
    if not receipt_path.is_file():
        raise SoybeanCrushRouterError("governed soybean-crush acquisition receipt unavailable")
    receipt = _read_json(receipt_path)
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("manifest_hash") != manifest["manifest_hash"]
        or receipt.get("download_status") != "DOWNLOADED"
        or int(receipt.get("q4_access_count_delta", -1)) != 0
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
        or float(receipt.get("actual_incremental_spend_usd", math.inf))
        > float(manifest["budget"]["maximum_branch_purchase_usd"]) + 1e-9
    ):
        raise SoybeanCrushRouterError("acquisition receipt semantic drift")
    files: dict[str, dict[str, Any]] = {}
    for row in receipt.get("files", []):
        path = _inside(project, row["path"])
        if path.stat().st_size != int(row["size_bytes"]) or sha256_file(path) != row["sha256"]:
            raise SoybeanCrushRouterError("acquired artifact drift")
        files[str(row["kind"])] = {**dict(row), "path": path}
    required = {"RAW_DBN_OHLCV_1M", "RAW_DBN_DEFINITION", "CONTINUOUS_SYMBOLOGY"}
    if set(files) != required:
        raise SoybeanCrushRouterError("acquisition inventory drift")

    rule_evidence = manifest["official_rule_evidence"]
    rule_path = _inside(project, rule_evidence["snapshot_path"])
    if sha256_file(rule_path) != rule_evidence["snapshot_file_sha256"]:
        raise SoybeanCrushRouterError("official rule snapshot hash drift")
    rule_snapshot = _read_json(rule_path)
    if rule_snapshot.get("parsed_rule_hash") != rule_evidence["parsed_rule_hash"]:
        raise SoybeanCrushRouterError("official rule snapshot semantic drift")
    reference_50k_mll_usd = float(
        rule_snapshot["combine"]["50K"]["maximum_loss_limit_usd"]
    )
    if not math.isfinite(reference_50k_mll_usd) or reference_50k_mll_usd <= 0:
        raise SoybeanCrushRouterError("invalid official 50K MLL")
    return {
        "manifest": manifest,
        "receipt": receipt,
        "rule_snapshot": rule_snapshot,
        "reference_50k_mll_usd": reference_50k_mll_usd,
        "zs_files": zs_files,
        "files": files,
        "audit_hash": stable_hash(
            {
                "manifest_hash": manifest["manifest_hash"],
                "receipt_hash": receipt["receipt_hash"],
                "rule_snapshot_sha256": sha256_file(rule_path),
                "zs": {key: sha256_file(value) for key, value in sorted(zs_files.items())},
                "acquired": {key: row["sha256"] for key, row in sorted(files.items())},
            }
        ),
    }


def _candidates(manifest: Mapping[str, Any]) -> list[Candidate]:
    lattice = manifest["candidate_lattice"]
    candidates = [
        Candidate(leg, mechanism, int(lookback), float(threshold))
        for leg in lattice["execution_legs"]
        for mechanism in lattice["mechanisms"]
        for lookback in lattice["causal_lookback_minutes"]
        for threshold in lattice["past_only_robust_z_thresholds"]
    ]
    if len(candidates) != int(lattice["proposal_count"]) or len(set(candidates)) != len(candidates):
        raise SoybeanCrushRouterError("candidate lattice drift")
    return candidates


def _candidate_id(candidate: Candidate, manifest: Mapping[str, Any]) -> str:
    return "soy_crush_" + stable_hash(
        {
            "candidate": asdict(candidate),
            "manifest_hash": manifest["manifest_hash"],
            "fill": manifest["execution"],
        }
    )[:20]


def _session_day(timestamp: pd.Series) -> pd.Series:
    local = timestamp.dt.tz_convert(CHICAGO)
    day = local.dt.date.astype("object")
    return pd.Series(
        [value + timedelta(days=1) if hour >= 19 else value for value, hour in zip(day, local.dt.hour)],
        index=timestamp.index,
        dtype="object",
    )


def _raw_contract(value: str, expected_root: str) -> tuple[str, int]:
    raw = str(value).strip().upper()
    if not raw.startswith(expected_root) or len(raw) < len(expected_root) + 2:
        raise SoybeanCrushRouterError(f"invalid explicit contract {raw!r} for {expected_root}")
    month_code = raw[len(expected_root)]
    suffix = raw[len(expected_root) + 1 :]
    if month_code not in MONTH_CODE or not suffix.isdigit():
        raise SoybeanCrushRouterError(f"unparseable explicit contract: {raw}")
    year = int(suffix)
    year += 2000 if year < 100 else 0
    return MONTH_CODE[month_code], year


def _valid_triplet(
    raw_contracts: Mapping[str, str],
    manifest: Mapping[str, Any],
    *,
    session_day: date | None = None,
) -> tuple[bool, str | None]:
    parsed = {leg: _raw_contract(raw_contracts[leg], leg) for leg in LEGS}
    years = {year for _month, year in parsed.values()}
    if len(years) != 1:
        return False, None
    contract_year = next(iter(years))
    if session_day is not None and contract_year not in {
        session_day.year,
        session_day.year + 1,
    }:
        return False, None
    months = {leg: parsed[leg][0] for leg in LEGS}
    for row in manifest["contract_alignment"]["valid_processing_month_triplets"]:
        if all(months[leg] == row[f"{leg}_month"] for leg in LEGS):
            return True, f"{contract_year}:{row['processing_month']}"
    return False, None


def _definition_map(path: Path, legs: set[str]) -> tuple[dict[str, str], dict[str, dict[str, float]]]:
    store = _import_databento().DBNStore.from_file(str(path))
    frame = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    raw_symbol = frame["raw_symbol"].astype(str).str.upper()
    frame = frame.loc[
        pd.concat([raw_symbol.str.startswith(leg) for leg in sorted(legs)], axis=1).any(axis=1)
    ].copy()
    if frame.empty:
        raise SoybeanCrushRouterError("definition input contains no requested symbols")
    mapping: dict[str, str] = {}
    for instrument_id, group in frame.groupby(frame["instrument_id"].astype(str), sort=False):
        values: set[str] = set()
        for raw_value, maturity_value in zip(group["raw_symbol"], group["maturity_year"]):
            raw = str(raw_value).strip().upper()
            if not raw:
                continue
            root = next(
                (leg for leg in sorted(legs, key=len, reverse=True) if raw.startswith(leg)),
                None,
            )
            if root is None or len(raw) <= len(root):
                continue
            month_code = raw[len(root)]
            if month_code not in MONTH_CODE or not math.isfinite(float(maturity_value)):
                raise SoybeanCrushRouterError("definition maturity cannot identify contract")
            values.add(f"{root}{month_code}{int(maturity_value)}")
        if len(values) != 1:
            raise SoybeanCrushRouterError("instrument id maps to multiple explicit contracts")
        mapping[str(instrument_id)] = next(iter(values))
    expected = {
        "ZS": {"tick_size": 0.25, "point_value_usd": 50.0, "tick_value_usd": 12.5},
        "ZM": {"tick_size": 0.10, "point_value_usd": 100.0, "tick_value_usd": 10.0},
        "ZL": {"tick_size": 0.01, "point_value_usd": 600.0, "tick_value_usd": 6.0},
    }
    specs: dict[str, dict[str, float]] = {}
    for leg in legs:
        part = frame.loc[frame["raw_symbol"].astype(str).str.upper().str.startswith(leg)]
        ticks = {round(float(value), 10) for value in part["min_price_increment"] if float(value) > 0}
        if ticks != {expected[leg]["tick_size"]}:
            raise SoybeanCrushRouterError(f"ambiguous {leg} tick definition: {sorted(ticks)}")
        specs[leg] = expected[leg]
    return mapping, specs


def _continuous_symbology(path: Path) -> dict[str, list[tuple[pd.Timestamp, pd.Timestamp, str]]]:
    payload = _read_json(path)
    mapping = payload.get("continuous_mapping")
    if not isinstance(mapping, Mapping):
        raise SoybeanCrushRouterError("continuous symbology is malformed")
    output: dict[str, list[tuple[pd.Timestamp, pd.Timestamp, str]]] = {}
    for symbol, rows in mapping.items():
        intervals: list[tuple[pd.Timestamp, pd.Timestamp, str]] = []
        for row in rows:
            start = pd.Timestamp(str(row["d0"]), tz="UTC")
            end = pd.Timestamp(str(row["d1"]), tz="UTC")
            if start >= end:
                raise SoybeanCrushRouterError("continuous symbology interval is empty")
            intervals.append((start, end, str(row["s"])))
        intervals.sort(key=lambda value: value[0])
        if any(left[1] != right[0] for left, right in zip(intervals, intervals[1:])):
            raise SoybeanCrushRouterError("continuous symbology has a gap or overlap")
        output[str(symbol)] = intervals
    return output


def _assert_continuous_membership(
    frame: pd.DataFrame,
    symbology: Mapping[str, Sequence[tuple[pd.Timestamp, pd.Timestamp, str]]],
) -> None:
    for symbol, group in frame.groupby("symbol", sort=False):
        intervals = list(symbology.get(str(symbol), ()))
        if not intervals:
            raise SoybeanCrushRouterError(f"continuous symbology missing {symbol}")
        timestamps = pd.to_datetime(group["timestamp"], utc=True).to_numpy(dtype="datetime64[ns]")
        starts = np.asarray([value[0].to_datetime64() for value in intervals])
        ends = np.asarray([value[1].to_datetime64() for value in intervals])
        ids = np.asarray([value[2] for value in intervals], dtype=object)
        positions = np.searchsorted(starts, timestamps, side="right") - 1
        valid = positions >= 0
        bounded = np.clip(positions, 0, len(intervals) - 1)
        valid &= timestamps < ends[bounded]
        actual = group["instrument_key"].astype(str).to_numpy(dtype=object)
        valid &= actual == ids[bounded]
        if not bool(np.all(valid)):
            raise SoybeanCrushRouterError(
                f"bar inventory disagrees with frozen continuous symbology for {symbol}"
            )


def _read_bars(
    path: Path,
    legs: set[str],
    mapping: Mapping[str, str],
    *,
    strict_inventory: bool,
    continuous_symbology: Mapping[
        str, Sequence[tuple[pd.Timestamp, pd.Timestamp, str]]
    ] | None = None,
) -> pd.DataFrame:
    store = _import_databento().DBNStore.from_file(str(path))
    frame = store.to_df(pretty_ts=True, map_symbols=True, price_type="float").reset_index()
    frame["timestamp"] = pd.to_datetime(frame["ts_event"], utc=True)
    frame["instrument_key"] = frame["instrument_id"].astype(str)
    expected_symbols = {f"{leg}.c.0" for leg in legs}
    if "symbol" not in frame:
        raise SoybeanCrushRouterError("continuous symbol identity missing from bars")
    frame["symbol"] = frame["symbol"].astype(str)
    frame = frame.loc[frame["symbol"].isin(expected_symbols)].copy()
    if frame.empty or set(frame["symbol"]) != expected_symbols:
        raise SoybeanCrushRouterError("requested continuous inventory is incomplete")
    frame["raw_contract"] = frame["instrument_key"].map(mapping)
    if strict_inventory and frame["raw_contract"].isna().any():
        missing = sorted(set(frame.loc[frame["raw_contract"].isna(), "instrument_key"]))
        raise SoybeanCrushRouterError(
            f"acquired bar lacks explicit definition mapping: {missing[:5]}"
        )
    frame = frame.loc[frame["raw_contract"].notna()].copy()
    if continuous_symbology is not None:
        _assert_continuous_membership(frame, continuous_symbology)
    frame["leg"] = frame["raw_contract"].map(
        lambda value: next((leg for leg in sorted(legs, key=len, reverse=True) if str(value).upper().startswith(leg)), "")
    )
    frame = frame.loc[frame["leg"].isin(legs)].copy()
    if frame.empty:
        raise SoybeanCrushRouterError("not every requested leg maps to an explicit raw contract")
    _assert_timestamp_bounds(frame["timestamp"])
    if frame.duplicated(["leg", "timestamp"]).any():
        raise SoybeanCrushRouterError("duplicate one-minute bar")
    frame["available_at"] = frame["timestamp"] + pd.Timedelta(minutes=1)
    frame["session_day"] = _session_day(frame["timestamp"])
    local = frame["timestamp"].dt.tz_convert(CHICAGO)
    minute = local.dt.hour * 60 + local.dt.minute
    trading = minute.ge(19 * 60) | minute.le(7 * 60 + 44) | minute.between(8 * 60 + 30, 13 * 60 + 19)
    columns = [
        "leg", "timestamp", "available_at", "session_day", "instrument_key", "raw_contract",
        "open", "high", "low", "close", "volume",
    ]
    return frame.loc[trading, columns].sort_values(["leg", "timestamp"], kind="mergesort")


def _assert_timestamp_bounds(timestamps: pd.Series) -> None:
    values = pd.to_datetime(timestamps, utc=True)
    if (
        values.empty
        or values.lt(pd.Timestamp("2018-01-02", tz="UTC")).any()
        or values.ge(pd.Timestamp("2024-10-01", tz="UTC")).any()
    ):
        raise SoybeanCrushRouterError("input timestamp escapes frozen pre-Q4 interval")


def _guard_sessions(frame: pd.DataFrame) -> tuple[set[date], dict[str, Any]]:
    """Return true-session roll guards and reject intra-session contract mixing."""

    sessions = sorted(set(frame["session_day"]))
    position = {day: index for index, day in enumerate(sessions)}
    guarded: set[date] = set()
    diagnostics: dict[str, Any] = {}
    for leg in LEGS:
        part = frame.loc[frame["leg"].eq(leg)]
        counts = part.groupby("session_day")["instrument_key"].nunique()
        mixed = set(counts.loc[counts.ne(1)].index)
        guarded.update(mixed)
        ordered = (
            part.loc[~part["session_day"].isin(mixed)]
            .groupby("session_day", sort=True)["instrument_key"]
            .first()
        )
        boundaries = [day for day, changed in ordered.ne(ordered.shift()).items() if changed][1:]
        for boundary in boundaries:
            index = position[boundary]
            guarded.update(sessions[max(0, index - 1) : index + 2])
        diagnostics[leg] = {
            "mixed_instrument_session_count": len(mixed),
            "roll_boundary_count": len(boundaries),
        }
    return guarded, diagnostics


def _load_market_data(
    audit: Mapping[str, Any]
) -> tuple[dict[str, dict[date, pd.DataFrame]], dict[str, dict[str, float]], dict[str, Any]]:
    zs_map, zs_specs = _definition_map(audit["zs_files"]["definition"], {"ZS"})
    new_map, new_specs = _definition_map(audit["files"]["RAW_DBN_DEFINITION"]["path"], {"ZM", "ZL"})
    new_symbology = _continuous_symbology(
        audit["files"]["CONTINUOUS_SYMBOLOGY"]["path"]
    )
    zs = _read_bars(
        audit["zs_files"]["ohlcv"], {"ZS"}, zs_map, strict_inventory=True
    )
    products = _read_bars(
        audit["files"]["RAW_DBN_OHLCV_1M"]["path"],
        {"ZM", "ZL"},
        new_map,
        strict_inventory=True,
        continuous_symbology=new_symbology,
    )
    frame = pd.concat([zs, products], ignore_index=True).sort_values(["leg", "timestamp"])
    if set(frame["leg"]) != set(LEGS):
        raise SoybeanCrushRouterError("three-leg market reconstruction incomplete")
    guards, guard_diagnostics = _guard_sessions(frame)
    all_days = sorted(set(frame["session_day"]))
    grouped = {
        (str(leg), day): group.copy()
        for (leg, day), group in frame.groupby(["leg", "session_day"], sort=False)
    }
    kept: dict[str, dict[date, pd.DataFrame]] = {leg: {} for leg in LEGS}
    unsupported = 0
    incomplete = 0
    retained = 0
    for day in all_days:
        if day in guards:
            continue
        parts = {leg: grouped.get((leg, day), pd.DataFrame()) for leg in LEGS}
        if any(part.empty for part in parts.values()):
            incomplete += 1
            continue
        raw = {leg: str(part["raw_contract"].iloc[0]) for leg, part in parts.items()}
        if any(part["instrument_key"].nunique() != 1 for part in parts.values()):
            raise SoybeanCrushRouterError("mixed raw contract escaped roll guard")
        valid, triplet = _valid_triplet(
            raw, audit["manifest"], session_day=day
        )
        if not valid:
            unsupported += 1
            continue
        for leg, part in parts.items():
            part["triplet_key"] = str(triplet)
            kept[leg][day] = part.reset_index(drop=True)
        retained += 1
    del frame, zs, products, grouped
    gc.collect()
    return kept, {**zs_specs, **new_specs}, {
        "retained_triplet_session_count": retained,
        "unsupported_triplet_session_count": unsupported,
        "incomplete_triplet_session_count": incomplete,
        "roll_guard_session_count": len(guards),
        "roll_diagnostics": guard_diagnostics,
        "q4_2024_rows": 0,
    }


def _align_session(
    sessions: Mapping[str, pd.DataFrame], maximum_staleness_minutes: int
) -> pd.DataFrame:
    """Backward-asof align only completed bars at each causal decision time."""

    timeline = pd.DataFrame(
        {
            "decision_time": sorted(
                set(pd.concat([sessions[leg]["available_at"] for leg in LEGS], ignore_index=True))
            )
        }
    )
    output = timeline
    for leg in LEGS:
        source = sessions[leg][
            [
                "timestamp", "available_at", "instrument_key", "raw_contract", "triplet_key",
                "open", "high", "low", "close", "volume",
            ]
        ].sort_values("available_at")
        source = source.rename(columns={column: f"{column}_{leg}" for column in source if column != "available_at"})
        output = pd.merge_asof(
            output.sort_values("decision_time"),
            source,
            left_on="decision_time",
            right_on="available_at",
            direction="backward",
            tolerance=pd.Timedelta(minutes=maximum_staleness_minutes),
        ).drop(columns=["available_at"])
    required = [f"timestamp_{leg}" for leg in LEGS]
    output = output.dropna(subset=required).reset_index(drop=True)
    for leg in LEGS:
        if (output[f"timestamp_{leg}"] + pd.Timedelta(minutes=1) > output["decision_time"]).any():
            raise SoybeanCrushRouterError("incomplete bar entered causal feature frame")
    triplet_columns = [f"triplet_key_{leg}" for leg in LEGS]
    output = output.loc[output[triplet_columns].nunique(axis=1).eq(1)].copy()
    return output


def _prior_robust_z(values: pd.Series, minimum: int, window: int) -> pd.Series:
    prior = values.shift(1)
    median = prior.rolling(window, min_periods=minimum).median()
    deviation = (prior - median).abs().rolling(window, min_periods=minimum).median()
    scale = 1.4826 * deviation
    return (values - median) / scale.replace(0.0, np.nan)


def _continuity_segments(decision_time: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(decision_time, utc=True)
    return timestamps.diff().ne(pd.Timedelta(minutes=1)).cumsum().astype(np.int64)


def _exact_elapsed_change(
    values: pd.Series,
    decision_time: pd.Series,
    segments: pd.Series,
    minutes: int,
) -> pd.Series:
    output = pd.Series(np.nan, index=values.index, dtype=float)
    offset = pd.Timedelta(minutes=int(minutes))
    for _segment, indices in segments.groupby(segments, sort=False).groups.items():
        ordered = list(indices)
        timestamps = pd.DatetimeIndex(pd.to_datetime(decision_time.loc[ordered], utc=True))
        series = pd.Series(values.loc[ordered].to_numpy(float), index=timestamps)
        prior = series.reindex(timestamps - offset).to_numpy(float)
        output.loc[ordered] = series.to_numpy(float) - prior
    return output


def _prior_time_robust_z(
    values: pd.Series,
    decision_time: pd.Series,
    segments: pd.Series,
    *,
    minimum: int,
    window_minutes: int,
) -> pd.Series:
    output = pd.Series(np.nan, index=values.index, dtype=float)
    window = f"{int(window_minutes)}min"
    for _segment, indices in segments.groupby(segments, sort=False).groups.items():
        ordered = list(indices)
        timestamps = pd.DatetimeIndex(pd.to_datetime(decision_time.loc[ordered], utc=True))
        series = pd.Series(values.loc[ordered].to_numpy(float), index=timestamps)
        median = series.rolling(window, min_periods=minimum, closed="left").median()
        deviation = (series - median).abs()
        mad = deviation.rolling(window, min_periods=minimum, closed="left").median()
        score = (series - median) / (1.4826 * mad).replace(0.0, np.nan)
        output.loc[ordered] = score.to_numpy(float)
    return output


def _component_specific_residual(
    changes: Mapping[str, pd.Series],
    leg: str,
    decision_time: pd.Series,
    segments: pd.Series,
    *,
    minimum: int,
    window_minutes: int,
) -> pd.Series:
    """Past-only rolling residual of one leg on the other two legs.

    Solving the crush identity for each component produces scaled copies of the
    same margin error.  This leave-one-leg-out rolling regression instead gives
    every routed component a genuinely distinct, causal residual.
    """

    others = [value for value in LEGS if value != leg]
    output = pd.Series(np.nan, index=decision_time.index, dtype=float)
    window = f"{int(window_minutes)}min"
    for _segment, indices in segments.groupby(segments, sort=False).groups.items():
        ordered = list(indices)
        timestamps = pd.DatetimeIndex(pd.to_datetime(decision_time.loc[ordered], utc=True))
        data = pd.DataFrame(
            {
                "y": changes[leg].loc[ordered].to_numpy(float),
                "x1": changes[others[0]].loc[ordered].to_numpy(float),
                "x2": changes[others[1]].loc[ordered].to_numpy(float),
            },
            index=timestamps,
        ).replace([np.inf, -np.inf], np.nan)
        data = data.where(data.notna().all(axis=1), np.nan)
        rolling = data.rolling(window, min_periods=minimum, closed="left")
        means = rolling.mean()
        ex1x1 = data["x1"].pow(2).rolling(window, min_periods=minimum, closed="left").mean()
        ex2x2 = data["x2"].pow(2).rolling(window, min_periods=minimum, closed="left").mean()
        ex1x2 = (data["x1"] * data["x2"]).rolling(
            window, min_periods=minimum, closed="left"
        ).mean()
        ex1y = (data["x1"] * data["y"]).rolling(
            window, min_periods=minimum, closed="left"
        ).mean()
        ex2y = (data["x2"] * data["y"]).rolling(
            window, min_periods=minimum, closed="left"
        ).mean()
        var1 = ex1x1 - means["x1"].pow(2)
        var2 = ex2x2 - means["x2"].pow(2)
        cov12 = ex1x2 - means["x1"] * means["x2"]
        cov1y = ex1y - means["x1"] * means["y"]
        cov2y = ex2y - means["x2"] * means["y"]
        ridge = (var1.abs() + var2.abs()).fillna(0.0) * 1e-9 + 1e-12
        a = var1 + ridge
        d = var2 + ridge
        determinant = a * d - cov12.pow(2)
        beta1 = (cov1y * d - cov2y * cov12) / determinant.replace(0.0, np.nan)
        beta2 = (cov2y * a - cov1y * cov12) / determinant.replace(0.0, np.nan)
        predicted = (
            means["y"]
            + beta1 * (data["x1"] - means["x1"])
            + beta2 * (data["x2"] - means["x2"])
        )
        output.loc[ordered] = (data["y"] - predicted).to_numpy(float)
    return output


def _feature_frame(aligned: pd.DataFrame, lookback: int, specs: Mapping[str, Any]) -> pd.DataFrame:
    frame = aligned.copy()
    segments = _continuity_segments(frame["decision_time"])
    frame["continuity_segment"] = segments
    zs = frame["close_ZS"].astype(float)
    zm = frame["close_ZM"].astype(float)
    zl = frame["close_ZL"].astype(float)
    frame["crush_margin"] = 0.022 * zm + 0.11 * zl - zs / 100.0
    frame["crush_change"] = _exact_elapsed_change(
        frame["crush_margin"], frame["decision_time"], segments, lookback
    )
    history_window = max(60, lookback * 4)
    minimum = max(15, lookback // 2)
    frame["crush_score"] = _prior_time_robust_z(
        frame["crush_change"], frame["decision_time"], segments,
        minimum=minimum, window_minutes=history_window,
    )
    changes: dict[str, pd.Series] = {}
    for leg in LEGS:
        close = frame[f"close_{leg}"].astype(float)
        changes[leg] = _exact_elapsed_change(
            close, frame["decision_time"], segments, lookback
        )
        frame[f"change_{leg}"] = changes[leg]
    for leg in LEGS:
        frame[f"residual_{leg}"] = _component_specific_residual(
            changes, leg, frame["decision_time"], segments,
            minimum=minimum, window_minutes=history_window,
        )
        frame[f"residual_score_{leg}"] = _prior_time_robust_z(
            frame[f"residual_{leg}"], frame["decision_time"], segments,
            minimum=minimum, window_minutes=history_window,
        )
        risk = pd.Series(np.nan, index=frame.index, dtype=float)
        for _segment, indices in segments.groupby(segments, sort=False).groups.items():
            ordered = list(indices)
            timestamps = pd.DatetimeIndex(
                pd.to_datetime(frame.loc[ordered, "decision_time"], utc=True)
            )
            high = pd.Series(frame.loc[ordered, f"high_{leg}"].to_numpy(float), index=timestamps)
            low = pd.Series(frame.loc[ordered, f"low_{leg}"].to_numpy(float), index=timestamps)
            rolling_high = high.rolling(
                f"{lookback}min", min_periods=lookback, closed="both"
            ).max()
            rolling_low = low.rolling(
                f"{lookback}min", min_periods=lookback, closed="both"
            ).min()
            risk.loc[ordered] = (rolling_high - rolling_low).to_numpy(float)
        frame[f"risk_{leg}"] = risk.clip(lower=4.0 * float(specs[leg]["tick_size"]))
    return frame


def _role(day: date, manifest: Mapping[str, Any]) -> str | None:
    stamp = pd.Timestamp(day)
    for row in manifest["chronological_roles"]:
        if pd.Timestamp(row["start"]) <= stamp < pd.Timestamp(row["end"]):
            return str(row["role"])
    return None


def _candidate_direction(row: Mapping[str, Any], candidate: Candidate) -> int:
    leg = candidate.execution_leg
    if candidate.mechanism == "CRUSH_EXPANSION_CONTINUATION_ROUTER":
        score = float(row["crush_score"])
        if not math.isfinite(score) or abs(score) < candidate.robust_z_threshold:
            return 0
        sign = int(np.sign(float(row["crush_change"])))
        return -sign if leg == "ZS" else sign
    if candidate.mechanism == "COMPONENT_RESIDUAL_REVERSION_ROUTER":
        score = float(row[f"residual_score_{leg}"])
        if not math.isfinite(score) or abs(score) < candidate.robust_z_threshold:
            return 0
        return -int(np.sign(float(row[f"residual_{leg}"])))
    raise SoybeanCrushRouterError(f"unknown mechanism: {candidate.mechanism}")


def _next_entry(session: pd.DataFrame, decision_time: pd.Timestamp) -> int | None:
    # Pandas may preserve a microsecond datetime unit here while Timestamp.value
    # is always nanoseconds.  Normalize explicitly before binary search.
    timestamps = session["timestamp"].map(lambda value: pd.Timestamp(value).value).to_numpy(np.int64)
    index = int(np.searchsorted(timestamps, pd.Timestamp(decision_time).value, side="right"))
    return index if index < len(session) else None


def _tick_price(value: float, tick: float, *, upward: bool) -> float:
    units = value / tick
    rounded = math.ceil(units - 1e-10) if upward else math.floor(units + 1e-10)
    return float(round(rounded * tick, 10))


def _canonical_feature_float(value: Any) -> float | None:
    """Return a strict-JSON causal feature scalar without hiding missing context."""

    observed = float(value)
    return observed if math.isfinite(observed) else None


def _censored_path(
    *,
    entry_time: pd.Timestamp,
    entry_price: float,
    stop_price: float,
    target_price: float,
    instrument: str,
) -> dict[str, Any]:
    return {
        "outcome_status": "CENSORED_FUTURE_COVERAGE",
        "entry_time": entry_time.isoformat(),
        "entry_price": entry_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "exit_time": None,
        "exit_price": None,
        "exit_reason": "CENSORED_FUTURE_COVERAGE",
        "gross_pnl_usd": None,
        "minimum_open_pnl_usd": None,
        "instrument_id": instrument,
    }


def _path(
    bars: pd.DataFrame,
    entry_index: int,
    direction: int,
    risk: float,
    target_multiple: float,
    holding_minutes: int,
    tick: float,
    point_value: float,
    slippage_ticks: int,
) -> dict[str, Any] | None:
    entry_row = bars.iloc[entry_index]
    entry_time = pd.Timestamp(entry_row["timestamp"])
    instrument = str(entry_row["instrument_key"])
    entry_local = entry_time.tz_convert(CHICAGO)
    logical_day = entry_local.date() + (
        timedelta(days=1) if entry_local.hour >= 19 else timedelta(0)
    )
    deadline = min(
        entry_time + pd.Timedelta(minutes=holding_minutes),
        pd.Timestamp(f"{logical_day} 13:20", tz=CHICAGO).tz_convert("UTC"),
    )
    eligible = bars.iloc[entry_index:].loc[bars.iloc[entry_index:]["timestamp"].lt(deadline)]
    slip = float(slippage_ticks) * tick
    entry = float(entry_row["open"]) + direction * slip
    raw_stop = entry - direction * risk
    raw_target = entry + direction * risk * target_multiple
    stop = _tick_price(raw_stop, tick, upward=direction < 0)
    target = _tick_price(raw_target, tick, upward=direction > 0)
    if eligible.empty or eligible["instrument_key"].astype(str).ne(instrument).any():
        return _censored_path(
            entry_time=entry_time,
            entry_price=entry,
            stop_price=stop,
            target_price=target,
            instrument=instrument,
        )
    exit_price: float | None = None
    exit_row: pd.Series | None = None
    reason = ""
    minimum_open = 0.0
    for _index, row in eligible.iterrows():
        low, high = float(row["low"]), float(row["high"])
        stop_hit = low <= stop if direction > 0 else high >= stop
        target_hit = high >= target if direction > 0 else low <= target
        if stop_hit:
            raw = min(float(row["open"]), stop) if direction > 0 else max(float(row["open"]), stop)
            exit_price = raw - direction * slip
            minimum_open = min(
                minimum_open, direction * (exit_price - entry) * point_value
            )
            exit_row, reason = row, "STOP_FIRST"
            break
        adverse_mark = low if direction > 0 else high
        minimum_open = min(minimum_open, direction * (adverse_mark - entry) * point_value)
        if target_hit:
            exit_price = target - direction * slip
            exit_row, reason = row, "TARGET"
            break
    if exit_row is None:
        last_available = pd.Timestamp(eligible.iloc[-1]["timestamp"]) + pd.Timedelta(minutes=1)
        if last_available < deadline:
            return _censored_path(
                entry_time=entry_time,
                entry_price=entry,
                stop_price=stop,
                target_price=target,
                instrument=instrument,
            )
        exit_row = eligible.iloc[-1]
        exit_price = float(exit_row["close"]) - direction * slip
        reason = "TIME_OR_SESSION_FLATTEN"
        minimum_open = min(minimum_open, direction * (exit_price - entry) * point_value)
    return {
        "outcome_status": "COMPLETED",
        "entry_time": entry_time.isoformat(),
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "exit_time": (pd.Timestamp(exit_row["timestamp"]) + pd.Timedelta(minutes=1)).isoformat(),
        "exit_price": float(exit_price),
        "exit_reason": reason,
        "gross_pnl_usd": direction * (float(exit_price) - entry) * point_value,
        "minimum_open_pnl_usd": minimum_open,
        "instrument_id": instrument,
    }


def _simulate(
    candidate: Candidate,
    row: Mapping[str, Any],
    session: pd.DataFrame,
    direction: int,
    role: str,
    day: date,
    spec: Mapping[str, float],
    manifest: Mapping[str, Any],
    control: str,
    *,
    execution_leg: str | None = None,
    risk_usd_override: float | None = None,
) -> dict[str, Any] | None:
    routed_leg = execution_leg or candidate.execution_leg
    if routed_leg not in LEGS:
        raise SoybeanCrushRouterError("control routed to unsupported leg")
    decision = pd.Timestamp(row["decision_time"])
    entry_index = _next_entry(session, decision)
    if entry_index is None or pd.Timestamp(session.iloc[entry_index]["timestamp"]) <= decision:
        return None
    if str(session.iloc[entry_index]["instrument_key"]) != str(row[f"instrument_key_{routed_leg}"]):
        return None
    risk = (
        float(risk_usd_override) / float(spec["point_value_usd"])
        if risk_usd_override is not None
        else float(row[f"risk_{routed_leg}"])
    )
    if not math.isfinite(risk) or risk <= 0:
        return None
    holding = int(
        manifest["candidate_lattice"]["maximum_holding_minutes_by_lookback"][
            str(candidate.lookback_minutes)
        ]
    )
    target_multiple = float(manifest["candidate_lattice"]["target_stop_multiple"])
    normal = _path(
        session, entry_index, direction, risk, target_multiple, holding,
        float(spec["tick_size"]), float(spec["point_value_usd"]),
        int(manifest["execution"]["normal_extra_slippage_ticks_per_side"]),
    )
    stressed = _path(
        session, entry_index, direction, risk, target_multiple, holding,
        float(spec["tick_size"]), float(spec["point_value_usd"]),
        int(manifest["execution"]["stressed_extra_slippage_ticks_per_side"]),
    )
    if normal is None or stressed is None:
        return None
    fee = float(manifest["execution"]["normal_round_turn_fees_usd"][routed_leg])
    stressed_ticks = int(manifest["execution"]["stressed_extra_slippage_ticks_per_side"])
    completed = (
        normal["outcome_status"] == "COMPLETED"
        and stressed["outcome_status"] == "COMPLETED"
    )
    feature_values = {
        "decision_time": decision.isoformat(),
        "available_at_by_leg": {
            leg: (pd.Timestamp(row[f"timestamp_{leg}"]) + pd.Timedelta(minutes=1)).isoformat()
            for leg in LEGS
        },
        "crush_margin": _canonical_feature_float(row["crush_margin"]),
        "crush_change": _canonical_feature_float(row["crush_change"]),
        "crush_score": _canonical_feature_float(row["crush_score"]),
        "leg_context": {
            leg: {
                "change": _canonical_feature_float(row[f"change_{leg}"]),
                "residual": _canonical_feature_float(row[f"residual_{leg}"]),
                "residual_score": _canonical_feature_float(
                    row[f"residual_score_{leg}"]
                ),
                "risk": _canonical_feature_float(row[f"risk_{leg}"]),
                "triplet_key": str(row[f"triplet_key_{leg}"]),
                "instrument_key": str(row[f"instrument_key_{leg}"]),
                "raw_contract": str(row[f"raw_contract_{leg}"]),
            }
            for leg in LEGS
        },
    }
    minimum_event_equity_stressed_usd = (
        min(
            float(stressed["minimum_open_pnl_usd"]) - fee / 2.0,
            float(stressed["gross_pnl_usd"]) - fee,
        )
        if completed
        else None
    )
    core = {
        "candidate_id": _candidate_id(candidate, manifest),
        "candidate": asdict(candidate),
        "control": control,
        "role": role,
        "session_day": day.isoformat(),
        "decision_time": decision.isoformat(),
        "executed_leg": routed_leg,
        "executed_leg_count": 1,
        "observed_context_legs": list(LEGS),
        "direction": int(direction),
        "causal_feature_values": feature_values,
        "feature_hash": stable_hash(feature_values),
        "normal": normal,
        "stressed": stressed,
        "normal_cost_usd": fee,
        "stressed_nominal_all_in_cost_usd": fee
        + 2.0 * stressed_ticks * float(spec["tick_value_usd"]),
        "outcome_status": "COMPLETED" if completed else "CENSORED_FUTURE_COVERAGE",
        "normal_net_usd": (
            float(normal["gross_pnl_usd"] - fee) if completed else None
        ),
        "stressed_net_usd": (
            float(stressed["gross_pnl_usd"] - fee) if completed else None
        ),
        "minimum_event_equity_stressed_usd": minimum_event_equity_stressed_usd,
        "minimum_open_pnl_stressed_including_entry_fee_usd": (
            float(stressed["minimum_open_pnl_usd"]) - fee / 2.0
            if completed
            else None
        ),
        "fill_policy_id": manifest["execution"]["fill_model"],
        "stressed_fill_semantics": "ADVERSE_ONE_TICK_EACH_SIDE_RECOMPUTED_PATH",
        "planned_stop_risk_usd": float(risk * float(spec["point_value_usd"])),
    }
    return {**core, "event_hash": stable_hash(core)}


def _opportunities(
    candidate: Candidate,
    prepared: Mapping[date, pd.DataFrame],
    sessions: Mapping[date, pd.DataFrame],
    spec: Mapping[str, float],
    manifest: Mapping[str, Any],
    roles: set[str],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for day in sorted(prepared):
        role = _role(day, manifest)
        if role not in roles:
            continue
        frame = prepared[day]
        if candidate.mechanism == "CRUSH_EXPANSION_CONTINUATION_ROUTER":
            source = frame["crush_change"].to_numpy(float)
            score = frame["crush_score"].to_numpy(float)
            direction = np.where(np.isfinite(source), np.sign(source), 0).astype(np.int8)
            if candidate.execution_leg == "ZS":
                direction *= -1
        elif candidate.mechanism == "COMPONENT_RESIDUAL_REVERSION_ROUTER":
            source = frame[f"residual_{candidate.execution_leg}"].to_numpy(float)
            score = frame[f"residual_score_{candidate.execution_leg}"].to_numpy(float)
            direction = -np.where(np.isfinite(source), np.sign(source), 0).astype(np.int8)
        else:
            raise SoybeanCrushRouterError(f"unknown mechanism: {candidate.mechanism}")
        eligible = np.flatnonzero(
            np.isfinite(source)
            & np.isfinite(score)
            & (np.abs(score) >= candidate.robust_z_threshold)
            & (direction != 0)
        )
        if not len(eligible):
            continue
        row = frame.iloc[int(eligible[0])]
        event = _simulate(
            candidate, row, sessions[day], int(direction[eligible[0]]), role, day,
            spec, manifest, "PRIMARY",
        )
        if event is not None:
            event["source_row"] = int(row.name)
            events.append(event)
    return events


def _control_route(
    event: Mapping[str, Any], row: Mapping[str, Any], control: str
) -> tuple[str, int]:
    primary_leg = str(event["executed_leg"])
    if control == "DIRECTION_FLIP":
        return primary_leg, -int(event["direction"])
    if control == "MATCHED_EXECUTION_LEG_PRICE_ONLY_DISPLACEMENT":
        value = float(row[f"change_{primary_leg}"])
        if value != 0.0 and math.isfinite(value):
            return primary_leg, int(np.sign(value))
        return primary_leg, 0
    if control == "SESSION_AND_EXPOSURE_MATCHED_RANDOM_ROUTING":
        # The seed contains only information frozen at decision time.  Outcome
        # hashes, fills and PnL are deliberately excluded.
        draw = stable_hash(
            {
                "candidate_id": event["candidate_id"],
                "session_day": event["session_day"],
                "decision_time": event["decision_time"],
                "role": event["role"],
                "control": control,
            }
        )
        leg = LEGS[int(draw[:8], 16) % len(LEGS)]
        direction = 1 if int(draw[8:16], 16) & 1 else -1
        return leg, direction
    raise SoybeanCrushRouterError(f"unknown control: {control}")


def _control_direction(event: Mapping[str, Any], row: Mapping[str, Any], control: str) -> int:
    """Compatibility wrapper for callers that only inspect direction."""

    return _control_route(event, row, control)[1]


def _controls(
    candidate: Candidate,
    primary: Sequence[Mapping[str, Any]],
    prepared: Mapping[date, pd.DataFrame],
    sessions: Mapping[str, Mapping[date, pd.DataFrame]],
    specs: Mapping[str, Mapping[str, float]],
    manifest: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {name: [] for name in CONTROLS}
    for event in primary:
        day = date.fromisoformat(str(event["session_day"]))
        row = prepared[day].loc[int(event["source_row"])]
        for control in CONTROLS:
            routed_leg, direction = _control_route(event, row, control)
            if direction == 0:
                continue
            result = _simulate(
                candidate, row, sessions[routed_leg][day], direction,
                str(event["role"]), day, specs[routed_leg], manifest, control,
                execution_leg=routed_leg,
                risk_usd_override=float(event["planned_stop_risk_usd"]),
            )
            if result is not None:
                result["source_row"] = int(event["source_row"])
                output[control].append(result)
    return output


def _summary(
    events: Sequence[Mapping[str, Any]], *, reference_mll_usd: float = 2_000.0
) -> dict[str, Any]:
    completed = [row for row in events if row.get("outcome_status") == "COMPLETED"]
    gross = [float(row["normal"]["gross_pnl_usd"]) for row in completed]
    normal = [float(row["normal_net_usd"]) for row in completed]
    stressed = [float(row["stressed_net_usd"]) for row in completed]
    costs = [float(row["stressed_nominal_all_in_cost_usd"]) for row in completed]
    positives = [value for value in stressed if value > 0]
    by_day: dict[str, float] = {}
    for row in completed:
        by_day[str(row["session_day"])] = by_day.get(str(row["session_day"]), 0.0) + float(row["stressed_net_usd"])
    positive_days = [value for value in by_day.values() if value > 0]
    return {
        "opportunity_count": len(events),
        "event_count": len(completed),
        "censored_future_coverage_count": len(events) - len(completed),
        "independent_session_count": len(by_day),
        "gross_pnl_usd": float(sum(gross)),
        "normal_net_usd": float(sum(normal)),
        "stressed_net_usd": float(sum(stressed)),
        "stressed_net_per_event_usd": float(np.mean(stressed)) if stressed else None,
        "stressed_edge_to_cost_ratio": float(sum(gross) / sum(costs)) if sum(costs) > 0 else None,
        "positive_stressed_event_rate": float(np.mean(np.asarray(stressed) > 0)) if stressed else None,
        "maximum_single_trade_positive_profit_share": max(positives, default=0.0) / sum(positives) if sum(positives) > 0 else None,
        "maximum_positive_day_profit_share": max(positive_days, default=0.0) / sum(positive_days) if sum(positive_days) > 0 else None,
        "reference_50k_mll_usd": float(reference_mll_usd),
        "minimum_open_pnl_stressed_usd": min(
            (
                float(row["minimum_open_pnl_stressed_including_entry_fee_usd"])
                for row in completed
            ),
            default=None,
        ),
        "minimum_event_equity_stressed_usd": min(
            (float(row["minimum_event_equity_stressed_usd"]) for row in completed),
            default=None,
        ),
        "event_level_50k_mll_breach_count": sum(
            float(row["minimum_event_equity_stressed_usd"]) <= -float(reference_mll_usd)
            for row in completed
        ),
        "target_count": sum(row["stressed"]["exit_reason"] == "TARGET" for row in completed),
        "stop_count": sum(row["stressed"]["exit_reason"] == "STOP_FIRST" for row in completed),
        "event_path_hash": stable_hash([row["event_hash"] for row in events]),
    }


def _gate(summary: Mapping[str, Any], manifest: Mapping[str, Any], *, discovery: bool) -> bool:
    gate = manifest["selection_gate"]
    minimum = gate[
        "minimum_discovery_independent_events" if discovery else "minimum_validation_independent_events"
    ]
    ratio = summary["stressed_edge_to_cost_ratio"]
    trade_share = summary["maximum_single_trade_positive_profit_share"]
    day_share = summary["maximum_positive_day_profit_share"]
    return bool(
        int(summary["event_count"]) >= int(minimum)
        and int(summary["independent_session_count"]) >= int(minimum)
        and float(summary["stressed_net_usd"]) > float(gate["validation_stressed_net_usd_minimum_exclusive"])
        and ratio is not None
        and float(ratio) >= float(gate["minimum_validation_stressed_edge_to_cost_ratio"])
        and trade_share is not None
        and day_share is not None
        and float(trade_share) <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
        and float(day_share) <= float(gate["maximum_single_trade_or_day_positive_profit_share"])
        and int(summary["event_level_50k_mll_breach_count"]) == 0
    )


def _controls_beaten(
    primary: Mapping[str, Any], controls: Mapping[str, Mapping[str, Any]], minimum: int
) -> tuple[bool, bool]:
    if int(primary["event_count"]) < minimum or primary["stressed_net_per_event_usd"] is None:
        return False, False
    resolved = set(controls) == set(CONTROLS) and all(
        int(row["event_count"]) >= minimum
        and int(row["event_count"]) == int(primary["event_count"])
        and row["stressed_net_per_event_usd"] is not None
        for row in controls.values()
    )
    if not resolved:
        return False, False
    value = float(primary["stressed_net_per_event_usd"])
    return all(value > float(row["stressed_net_per_event_usd"]) for row in controls.values()), True


def _rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    summary = row["discovery"]
    return (
        float(summary["stressed_net_usd"]),
        float(-math.inf if summary["stressed_net_per_event_usd"] is None else summary["stressed_net_per_event_usd"]),
        int(summary["event_count"]),
        str(row["candidate_id"]),
    )


def run_tripwire(root: str | Path) -> dict[str, Any]:
    started = time.perf_counter()
    project = Path(root).resolve()
    audit = audit_inputs(project)
    manifest = audit["manifest"]
    sessions, specs, reconstruction = _load_market_data(audit)
    days = sorted(set.intersection(*(set(sessions[leg]) for leg in LEGS)))
    aligned: dict[date, pd.DataFrame] = {}
    features: dict[int, dict[date, pd.DataFrame]] = {
        int(value): {} for value in manifest["candidate_lattice"]["causal_lookback_minutes"]
    }
    for day in days:
        frame = _align_session(
            {leg: sessions[leg][day] for leg in LEGS},
            int(manifest["causal_contract"]["maximum_leg_staleness_minutes"]),
        )
        if frame.empty:
            continue
        aligned[day] = frame
        for lookback in features:
            features[lookback][day] = _feature_frame(frame, lookback, specs)

    candidates = _candidates(manifest)
    by_id = {_candidate_id(candidate, manifest): candidate for candidate in candidates}
    reference_mll_usd = float(audit["reference_50k_mll_usd"])
    discovery_rows: list[dict[str, Any]] = []
    discovery_event_ledgers: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        cid = _candidate_id(candidate, manifest)
        events = _opportunities(
            candidate, features[candidate.lookback_minutes], sessions[candidate.execution_leg],
            specs[candidate.execution_leg], manifest, {"DISCOVERY"},
        )
        summary = _summary(events, reference_mll_usd=reference_mll_usd)
        discovery_rows.append(
            {"candidate_id": cid, "candidate": asdict(candidate), "discovery": summary}
        )
        if _gate(summary, manifest, discovery=True):
            discovery_event_ledgers[cid] = events
    eligible = [row for row in discovery_rows if _gate(row["discovery"], manifest, discovery=True)]
    eligible.sort(key=_rank, reverse=True)
    selected: list[dict[str, Any]] = []
    niches: set[tuple[str, str]] = set()
    for row in eligible:
        niche = (row["candidate"]["execution_leg"], row["candidate"]["mechanism"])
        if niche in niches:
            continue
        niches.add(niche)
        selected.append(row)
        if len(selected) >= int(manifest["selection_gate"]["maximum_selected_specs"]):
            break

    selected_results: list[dict[str, Any]] = []
    validation_passers: list[str] = []
    final_passers: list[str] = []
    minimum_validation = int(manifest["selection_gate"]["minimum_validation_independent_events"])
    for selected_row in selected:
        cid = str(selected_row["candidate_id"])
        candidate = by_id[cid]
        validation_events = _opportunities(
            candidate, features[candidate.lookback_minutes], sessions[candidate.execution_leg],
            specs[candidate.execution_leg], manifest, {"VALIDATION"},
        )
        validation_summary = _summary(
            validation_events, reference_mll_usd=reference_mll_usd
        )
        validation_control_events = _controls(
            candidate, validation_events, features[candidate.lookback_minutes],
            sessions, specs, manifest,
        )
        validation_controls = {
            name: _summary(rows, reference_mll_usd=reference_mll_usd)
            for name, rows in validation_control_events.items()
        }
        beaten, resolved = _controls_beaten(validation_summary, validation_controls, minimum_validation)
        validation_passed = _gate(validation_summary, manifest, discovery=False) and resolved and beaten
        final_summary: dict[str, Any] | None = None
        final_controls: dict[str, Any] = {}
        final_events: list[dict[str, Any]] = []
        final_control_events: dict[str, list[dict[str, Any]]] = {
            name: [] for name in CONTROLS
        }
        final_passed = False
        final_resolved = False
        final_beaten = False
        if validation_passed:
            validation_passers.append(cid)
            final_events = _opportunities(
                candidate, features[candidate.lookback_minutes], sessions[candidate.execution_leg],
                specs[candidate.execution_leg], manifest, {"FINAL_DEVELOPMENT"},
            )
            final_summary = _summary(
                final_events, reference_mll_usd=reference_mll_usd
            )
            final_control_events = _controls(
                candidate, final_events, features[candidate.lookback_minutes],
                sessions, specs, manifest,
            )
            final_controls = {
                name: _summary(rows, reference_mll_usd=reference_mll_usd)
                for name, rows in final_control_events.items()
            }
            final_beaten, final_resolved = _controls_beaten(
                final_summary, final_controls, minimum_validation
            )
            final_passed = _gate(final_summary, manifest, discovery=False) and final_resolved and final_beaten
            if final_passed:
                final_passers.append(cid)
        selected_results.append(
            {
                "candidate_id": cid,
                "candidate": asdict(candidate),
                "discovery": selected_row["discovery"],
                "discovery_event_ledger": discovery_event_ledgers[cid],
                "validation": validation_summary,
                "validation_controls": validation_controls,
                "validation_event_ledger": validation_events,
                "validation_control_event_ledgers": validation_control_events,
                "validation_controls_resolved": resolved,
                "validation_controls_beaten": beaten,
                "validation_gate_passed": validation_passed,
                "final_development": final_summary,
                "final_controls": final_controls,
                "final_development_event_ledger": final_events,
                "final_development_control_event_ledgers": final_control_events,
                "final_controls_resolved": final_resolved,
                "final_controls_beaten": final_beaten,
                "final_gate_passed": final_passed,
                "selected_event_ledger_hash": stable_hash(
                    {
                        "discovery": [
                            row["event_hash"] for row in discovery_event_ledgers[cid]
                        ],
                        "validation": [row["event_hash"] for row in validation_events],
                        "validation_controls": {
                            name: [row["event_hash"] for row in rows]
                            for name, rows in sorted(validation_control_events.items())
                        },
                        "final_development": [row["event_hash"] for row in final_events],
                        "final_controls": {
                            name: [row["event_hash"] for row in rows]
                            for name, rows in sorted(final_control_events.items())
                        },
                    }
                ),
            }
        )
    status = (
        "SOYBEAN_CRUSH_STRUCTURAL_VALUE_ROUTER_EVENT_GATE_GREEN"
        if final_passers
        else manifest["selection_gate"]["failure_status"]
    )
    core: dict[str, Any] = {
        "schema": "hydra_soybean_crush_structural_value_router_result_v1",
        "branch_id": manifest["branch_id"],
        "status": status,
        "manifest_hash": manifest["manifest_hash"],
        "source_audit_hash": audit["audit_hash"],
        "official_rule_snapshot_hash": audit["rule_snapshot"]["parsed_rule_hash"],
        "reference_50k_mll_usd": reference_mll_usd,
        "acquisition_receipt_hash": audit["receipt"]["receipt_hash"],
        "actual_incremental_spend_usd": float(
            audit["receipt"]["actual_incremental_spend_usd"]
        ),
        "contract_specs": specs,
        "reconstruction": {
            **reconstruction,
            "aligned_session_count": len(aligned),
            "aligned_rows": sum(len(frame) for frame in aligned.values()),
        },
        "proposal_count": len(candidates),
        "discovery_candidate_count": len(discovery_rows),
        "discovery_eligible_count": len(eligible),
        "validation_candidate_count": len(selected),
        "final_development_candidate_count": len(validation_passers),
        "validation_access_policy": "DISCOVERY_SELECT_THEN_SELECTED_VALIDATION_ONLY",
        "final_access_policy": "VALIDATION_PASS_THEN_FINAL_DEVELOPMENT_ONLY",
        "all_candidate_discovery_results": discovery_rows,
        "best_discovery_diagnostics": sorted(discovery_rows, key=_rank, reverse=True)[:8],
        "selected_candidate_ids": [row["candidate_id"] for row in selected],
        "selected_results": selected_results,
        "validation_event_gate_passer_ids": validation_passers,
        "final_development_event_gate_passer_ids": final_passers,
        "account_replay_executed": False,
        "account_replay_status": (
            "READY_AFTER_FINAL_EVENT_GATE" if final_passers else "BLOCKED_UNTIL_FINAL_DEVELOPMENT_EVENT_GATE"
        ),
        "combine_pass_count": 0,
        "xfa_paths_started": 0,
        "runtime_seconds": time.perf_counter() - started,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "next_action": (
            "FREEZE_FINAL_EVENT_PASSERS_AND_RUN_ACCOUNT_SIZE_MATRIX"
            if final_passers
            else manifest["decision_card"]["next_materially_distinct_alternative"]
        ),
    }
    core["result_hash"] = stable_hash(
        {key: value for key, value in core.items() if key != "runtime_seconds"}
    )
    return core


__all__ = [
    "Candidate",
    "SoybeanCrushRouterError",
    "audit_inputs",
    "run_tripwire",
]
