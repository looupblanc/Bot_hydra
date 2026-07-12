from __future__ import annotations

import gc
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hydra.features.canonical_store import (
    CanonicalFeatureKey,
    CanonicalFeatureStore,
    FeatureStoreWriteResult,
)
from hydra.mission.calibration_retest_execution import (
    DEFAULT_HISTORICAL_REPORT,
    _build_past_only_feature_frame,
    _load_governed_development_frame,
    _load_markdown_json,
    _stable_hash,
)
from hydra.research.accelerated_context_tournament import _context_bars
from hydra.research.equity_open_gap_reversal import MAP_TYPE
from hydra.research.qd_economic_tournament import (
    FEATURES,
    MARKET_PAIRS,
    SESSION_CLOCKS,
    _prepare_feature_frame,
    _round_turn_cost_all,
)
from hydra.markets.instruments import instrument_spec


FEATURE_BUNDLE_VERSION = "hydra_turbo_feature_bundle_v3_risk_path"
FEATURE_DAG_HASH = hashlib.sha256(
    b"past_only_v3|closed_5_15_30_60_v1|session_day_datetime64D_v2|one_bar_delay|explicit_contracts|conservative_ohlc_risk_path_v1"
).hexdigest()
HORIZONS = (5, 15, 30, 60)
CONTEXT_MINUTES = (5, 15, 30, 60)
DISCOVERY_MARKETS = tuple(MARKET_PAIRS)


@dataclass(frozen=True)
class TurboFeatureBuild:
    market_paths: dict[str, str]
    cache_hits: int
    cache_misses: int
    rows: int
    source_fingerprint: str
    seconds: float
    peak_frame_rows: int


def build_or_open_turbo_feature_bundles(
    *,
    cache_root: str | Path,
    contract_map_path: str | Path,
) -> TurboFeatureBuild:
    """Build one immutable mmap bundle per primary market, or reuse it warm."""
    started = time.perf_counter()
    map_path = Path(contract_map_path)
    historical = _load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT))
    coverage = []
    for row in historical.get("cached_coverage") or []:
        if not row.get("path"):
            continue
        path = Path(str(row["path"]))
        if not path.is_file():
            raise FileNotFoundError(f"Frozen development source is missing: {path}")
        coverage.append(
            {
                "path": str(path),
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
        )
    source_fingerprint = _stable_hash(
        {
            "coverage": coverage,
            "period": ["2023-01-01", "2024-10-01"],
            "markets": list(DISCOVERY_MARKETS),
            "map_sha256": _sha256(map_path),
            "feature_dag_hash": FEATURE_DAG_HASH,
        }
    )
    store = CanonicalFeatureStore(cache_root)
    keys = {
        market: _feature_key(market, source_fingerprint, map_path)
        for market in DISCOVERY_MARKETS
    }
    existing = {market: store.get(key) for market, key in keys.items()}
    if all(value is not None for value in existing.values()):
        return TurboFeatureBuild(
            market_paths={market: str(value.root) for market, value in existing.items() if value},
            cache_hits=len(existing),
            cache_misses=0,
            rows=sum(int(value.row_count) for value in existing.values() if value),
            source_fingerprint=source_fingerprint,
            seconds=time.perf_counter() - started,
            peak_frame_rows=0,
        )

    raw, provenance = _load_governed_development_frame(
        historical,
        [{"target_markets": list(DISCOVERY_MARKETS)}],
        contract_map_path=map_path,
        required_contract_map_type=MAP_TYPE,
    )
    features = _prepare_feature_frame(_build_past_only_feature_frame(raw))
    peak_frame_rows = len(features)
    del raw
    gc.collect()
    paths: dict[str, str] = {}
    hits = 0
    misses = 0
    rows = 0
    for market in DISCOVERY_MARKETS:
        key = _feature_key(market, source_fingerprint, map_path)
        cached = store.get(key)
        if cached is not None:
            paths[market] = str(cached.root)
            hits += 1
            rows += cached.row_count
            continue
        frame = features.loc[features["symbol"].astype(str).eq(market)].copy()
        arrays, metadata = _market_arrays(frame, market)
        result: FeatureStoreWriteResult = store.put(
            key,
            arrays,
            provenance={
                "data_fingerprint": source_fingerprint,
                "contract_map_sha256": provenance["contract_map_sha256"],
                "market": market,
                "execution_market": MARKET_PAIRS[market],
                "point_value": instrument_spec(market).point_value,
                "round_turn_cost": _round_turn_cost_all(market),
                "features": list(metadata["feature_names"]),
                "horizons": list(HORIZONS),
                "latest_timestamp_ns": int(arrays["timestamp_ns"].max()),
                "q4_access_count_delta": 0,
                "outbound_order_capability": False,
            },
        )
        paths[market] = str(result.path)
        hits += int(result.cache_hit)
        misses += int(not result.cache_hit)
        rows += result.row_count
        del frame, arrays
        gc.collect()
    del features
    gc.collect()
    return TurboFeatureBuild(
        market_paths=paths,
        cache_hits=hits,
        cache_misses=misses,
        rows=rows,
        source_fingerprint=source_fingerprint,
        seconds=time.perf_counter() - started,
        peak_frame_rows=peak_frame_rows,
    )


def feature_names_for_bundle() -> tuple[str, ...]:
    return tuple(FEATURES) + tuple(
        name
        for minutes in CONTEXT_MINUTES
        for name in (f"ctx_{minutes}m_return", f"ctx_{minutes}m_volatility_expansion")
    )


def _feature_key(
    market: str, source_fingerprint: str, map_path: Path
) -> CanonicalFeatureKey:
    return CanonicalFeatureKey(
        market=market,
        explicit_contract_scope=f"{market}_date_aware_active_contracts",
        start_inclusive="2023-01-01",
        end_exclusive="2024-10-01",
        source_data_sha256=source_fingerprint,
        roll_map_hash=_sha256(map_path),
        transformation_version=FEATURE_BUNDLE_VERSION,
        feature_dag_hash=FEATURE_DAG_HASH,
        timeframes=("1m", "5m", "15m", "30m", "60m", "session", "daily"),
    )


def _market_arrays(
    frame: pd.DataFrame, market: str
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    if frame.empty:
        raise ValueError(f"No governed development rows for {market}.")
    data = frame.sort_values("timestamp").reset_index(drop=True).copy()
    grouping = ["symbol", "active_contract", "trading_session_id", "contiguous_segment_id"]
    groups = data.groupby(grouping, sort=False, observed=True)
    timestamp = pd.to_datetime(data["timestamp"], utc=True)
    next_timestamp = groups["timestamp"].shift(-1)
    entry_price = groups["close"].shift(-1)
    arrays: dict[str, np.ndarray] = {
        "timestamp_ns": timestamp.astype("int64").to_numpy(dtype=np.int64),
        "decision_ns": (timestamp + pd.Timedelta(minutes=1)).astype("int64").to_numpy(dtype=np.int64),
        "availability_ns": (timestamp + pd.Timedelta(minutes=1)).astype("int64").to_numpy(dtype=np.int64),
        "segment_code": data["contiguous_segment_id"].to_numpy(dtype=np.int64),
        "session_day": _session_days(data["trading_session_id"]),
        "session_code": _session_codes(data),
        "contract_code": pd.factorize(data["active_contract"].astype(str), sort=True)[0].astype(np.int16),
        "entry_price": entry_price.to_numpy(dtype=np.float64),
        "bar_open": pd.to_numeric(data["open"], errors="coerce").to_numpy(dtype=np.float64),
        "bar_high": pd.to_numeric(data["high"], errors="coerce").to_numpy(dtype=np.float64),
        "bar_low": pd.to_numeric(data["low"], errors="coerce").to_numpy(dtype=np.float64),
        "bar_close": pd.to_numeric(data["close"], errors="coerce").to_numpy(dtype=np.float64),
    }
    for feature in FEATURES:
        arrays[f"feature__{feature}"] = pd.to_numeric(
            data[feature], errors="coerce"
        ).to_numpy(dtype=np.float64)
    for horizon in HORIZONS:
        exit_timestamp = groups["timestamp"].shift(-(horizon + 1))
        exit_price = groups["close"].shift(-(horizon + 1))
        valid = (
            next_timestamp.eq(timestamp + pd.Timedelta(minutes=1))
            & exit_timestamp.eq(timestamp + pd.Timedelta(minutes=horizon + 1))
        )
        move = (exit_price - entry_price).where(valid)
        arrays[f"forward_move__{horizon}"] = move.to_numpy(dtype=np.float64)
    decision = pd.DataFrame(
        {
            "decision_timestamp": timestamp + pd.Timedelta(minutes=1),
            "active_contract": data["active_contract"].astype(str),
            "position": np.arange(len(data), dtype=np.int64),
        }
    )
    for minutes in CONTEXT_MINUTES:
        context = _context_bars(data, minutes)
        joined_parts: list[pd.DataFrame] = []
        for contract, left in decision.groupby("active_contract", sort=True):
            right = context.loc[context["active_contract"].astype(str).eq(str(contract))]
            if right.empty:
                continue
            joined_parts.append(
                pd.merge_asof(
                    left.sort_values("decision_timestamp"),
                    right.sort_values("availability_timestamp"),
                    left_on="decision_timestamp",
                    right_on="availability_timestamp",
                    direction="backward",
                    allow_exact_matches=True,
                )
            )
        values = np.full(len(data), np.nan, dtype=np.float64)
        volatility = np.full(len(data), np.nan, dtype=np.float64)
        if joined_parts:
            joined = pd.concat(joined_parts, ignore_index=True)
            available = joined["availability_timestamp"].notna()
            if not (
                joined.loc[available, "availability_timestamp"]
                <= joined.loc[available, "decision_timestamp"]
            ).all():
                raise RuntimeError("Turbo feature bundle joined an incomplete HTF bar.")
            positions = joined.loc[available, "position"].to_numpy(dtype=np.int64)
            values[positions] = joined.loc[available, "context_return"].to_numpy(dtype=float)
            volatility[positions] = joined.loc[
                available, "context_volatility_expansion"
            ].astype(float).to_numpy(dtype=float)
        arrays[f"feature__ctx_{minutes}m_return"] = values
        arrays[f"feature__ctx_{minutes}m_volatility_expansion"] = volatility
    return arrays, {"feature_names": feature_names_for_bundle()}


def _session_codes(data: pd.DataFrame) -> np.ndarray:
    minute = data["minutes_from_market_open"].to_numpy(dtype=float)
    length = data["market_session_length"].to_numpy(dtype=float)
    output = np.full(len(data), -2, dtype=np.int16)
    valid = (minute >= 0) & (minute < length)
    output[valid & (minute < 120)] = 0
    output[valid & (minute >= 120) & (minute < 240)] = 1
    output[valid & (minute >= 240)] = 2
    return output


def _session_days(values: pd.Series) -> np.ndarray:
    parsed = pd.to_datetime(values.astype(str), utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("Trading-session identifiers are not valid dates.")
    # Pandas may preserve a microsecond input resolution; casting its raw
    # integer values and dividing by nanoseconds silently maps 2023 to day 19.
    # Normalize explicitly to NumPy calendar days instead.
    return (
        parsed.dt.tz_localize(None)
        .to_numpy(dtype="datetime64[D]")
        .astype(np.int32)
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
