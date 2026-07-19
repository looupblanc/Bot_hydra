"""Bounded three-tenor Treasury curvature-to-belly development tripwire.

The module is deliberately isolated from HYDRA's persistent writer, registry,
cemetery mutation, controller, broker, network and order paths.  ``audit_inputs``
validates only immutable metadata and hashes; it never decodes Parquet row data.
Economic replay is guarded by an explicit root-authorization token.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from hydra.production import autonomous_exact_replay as exact
from hydra.propfirm.combine_episode import (
    CombineEpisodeResult,
    TradePathEvent,
    run_combine_episode,
)
from hydra.research.curve_relative_value_tripwire import TREASURY_SPECS


SCHEMA = "hydra_treasury_three_tenor_curvature_tripwire_v1"
AUDIT_SCHEMA = "hydra_treasury_three_tenor_curvature_tripwire_audit_v1"
BRANCH_ID = "TREASURY_THREE_TENOR_CURVATURE_TO_BELLY_OUTRIGHT_TRIPWIRE_V1"
DEFAULT_CARD = (
    "config/research/treasury_three_tenor_curvature_to_belly_tripwire_v1.json"
)
RUN_AUTHORIZATION = "ROOT_AUTHORIZED_TREASURY_CURVATURE_REPLAY_V1"
ROLES = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
CONTROLS = (
    "PRIMARY",
    "BELLY_LEVEL_ONLY",
    "NEAREST_ADJACENT_SLOPE",
    "DIRECTION_FLIP",
    "TIMING_DELAY_5_BARS",
)
MATCHED_CONTROL_KEYS = ("PRIMARY_MATCHED", *CONTROLS[1:])
ACCOUNT_CONTROL_KEYS = ("PRIMARY", *MATCHED_CONTROL_KEYS)
DIAGNOSTIC_HORIZONS = (5, 10)
HEADLINE_HORIZON = 20
HORIZONS = (*DIAGNOSTIC_HORIZONS, HEADLINE_HORIZON)
RISK_FRACTIONS = (0.1, 0.2)
MAXIMUM_RULES = 8
SESSION_FLATTEN_MINUTE = 15 * 60 + 10
EARLIEST_ENTRY_MINUTE = 7 * 60 + 20
COVERAGE_START_MINUTE = 6 * 60 + 19
COVERAGE_END_MINUTE = SESSION_FLATTEN_MINUTE
FORBIDDEN_DECISION_TOKENS = (
    "future",
    "forward",
    "lead_",
    "next_",
    "outcome",
    "label",
    "mfe",
    "mae",
    "target_reached",
)


class TreasuryCurvatureError(RuntimeError):
    """The bounded tripwire cannot preserve its frozen causal contract."""


@dataclass(frozen=True, slots=True)
class TriangleSpec:
    triangle_id: str
    short_root: str
    belly_root: str
    long_root: str

    @property
    def execution_root(self) -> str:
        return self.belly_root

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "execution_root": self.execution_root,
        }


@dataclass(frozen=True, slots=True)
class CurvatureRule:
    rule_id: str
    triangle_id: str
    mechanism: str
    source_return_minutes: int
    holding_minutes: int
    trigger_z: float = 2.0
    stop_r_multiple: float = 1.0
    target_r_multiple: float = 2.0
    fill_policy: str = "NEXT_TRADABLE_BELLY_OPEN"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _CausalPathIndex:
    """Immutable positional index for exact causal belly paths.

    The production frames are dense ``RangeIndex`` tables.  Building the
    validity frontier once turns the formerly repeated pandas path scans into
    an O(1) contiguity query.  Price-bar inspection remains a bounded NumPy
    slice (at most the preregistered 120 bars), preserving the exact stop-first
    and next-open semantics without materialising a future-aware label.
    """

    __slots__ = (
        "belly_root",
        "length",
        "timestamp_ns",
        "open",
        "high",
        "low",
        "delivery_aligned",
        "roll_unsafe",
        "run_end",
    )

    def __init__(self, frame: pd.DataFrame, *, belly_root: str) -> None:
        length = len(frame)
        expected_index = np.arange(length, dtype=np.int64)
        observed_index = frame.index.to_numpy(dtype=np.int64, copy=False)
        if not np.array_equal(observed_index, expected_index):
            raise TreasuryCurvatureError(
                "causal path index requires a dense zero-based RangeIndex"
            )
        required = {
            "timestamp",
            "session_id",
            "local_minute",
            "roll_unsafe",
            f"{belly_root}_contract",
            f"{belly_root}_open",
            f"{belly_root}_high",
            f"{belly_root}_low",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise TreasuryCurvatureError(
                f"causal path index is missing columns: {missing}"
            )

        # pandas may preserve a microsecond source dtype.  Normalize explicitly
        # so the integer clock and the frozen one-minute nanosecond constant
        # cannot silently disagree.
        timestamp_ns = (
            pd.to_datetime(frame["timestamp"], utc=True)
            .astype("datetime64[ns, UTC]")
            .array.asi8.copy()
        )
        sessions = frame["session_id"].astype(str).to_numpy(copy=False)
        contracts = frame[f"{belly_root}_contract"].astype(str).to_numpy(copy=False)
        local_minutes = frame["local_minute"].to_numpy(dtype=np.int64, copy=False)
        roll_unsafe = frame["roll_unsafe"].to_numpy(dtype=bool, copy=False)
        delivery_columns = [
            column
            for column in frame.columns
            if str(column).endswith("_delivery_month")
        ]
        if delivery_columns:
            delivery = frame[delivery_columns].astype(str).to_numpy(copy=False)
            delivery_aligned = np.all(delivery == delivery[:, :1], axis=1)
        else:
            delivery_aligned = np.ones(length, dtype=bool)
        row_valid = (
            ~roll_unsafe
            & delivery_aligned
            & (local_minutes <= SESSION_FLATTEN_MINUTE)
        )

        if length:
            transition_good = np.ones(max(length - 1, 0), dtype=bool)
            if length > 1:
                transition_good &= np.diff(timestamp_ns) == 60_000_000_000
                transition_good &= sessions[1:] == sessions[:-1]
                transition_good &= contracts[1:] == contracts[:-1]
                transition_good &= row_valid[1:] & row_valid[:-1]
            boundary = np.zeros(length, dtype=bool)
            if length > 1:
                boundary[:-1] = ~transition_good
            boundary[-1] = True
            positions = np.arange(length, dtype=np.int64)
            candidates = np.where(boundary, positions, length - 1)
            run_end = np.minimum.accumulate(candidates[::-1])[::-1]
            run_end = run_end.astype(np.int64, copy=False)
            run_end[~row_valid] = positions[~row_valid] - 1
        else:
            run_end = np.empty(0, dtype=np.int64)

        self.belly_root = str(belly_root)
        self.length = int(length)
        self.timestamp_ns = timestamp_ns
        self.open = frame[f"{belly_root}_open"].to_numpy(dtype=float, copy=True)
        self.high = frame[f"{belly_root}_high"].to_numpy(dtype=float, copy=True)
        self.low = frame[f"{belly_root}_low"].to_numpy(dtype=float, copy=True)
        self.delivery_aligned = np.asarray(delivery_aligned, dtype=bool).copy()
        self.roll_unsafe = np.asarray(roll_unsafe, dtype=bool).copy()
        self.run_end = run_end
        for value in (
            self.timestamp_ns,
            self.open,
            self.high,
            self.low,
            self.delivery_aligned,
            self.roll_unsafe,
            self.run_end,
        ):
            value.flags.writeable = False

    def is_contiguous(self, start: int, end: int) -> bool:
        """Return the legacy path-validity result in O(1)."""

        return bool(
            0 <= start <= end < self.length
            and int(self.run_end[start]) >= end
        )

    def extrema(self, start: int, end_exclusive: int) -> tuple[float, float]:
        """Return (low, high) for held bars; the exit-open bar is excluded."""

        if not (0 <= start < end_exclusive <= self.length):
            raise TreasuryCurvatureError("executable path has no held bar")
        return (
            float(np.min(self.low[start:end_exclusive])),
            float(np.max(self.high[start:end_exclusive])),
        )

    def first_barrier_hit(
        self,
        start: int,
        end_inclusive: int,
        *,
        direction: int,
        stop_price: float,
        target_price: float,
    ) -> tuple[int, bool, bool] | None:
        """Find the first frozen barrier hit with conservative stop priority."""

        if end_inclusive < start:
            return None
        low = self.low[start : end_inclusive + 1]
        high = self.high[start : end_inclusive + 1]
        if direction > 0:
            stop = low <= stop_price
            target = high >= target_price
        else:
            stop = high >= stop_price
            target = low <= target_price
        hit = stop | target
        offsets = np.flatnonzero(hit)
        if offsets.size == 0:
            return None
        offset = int(offsets[0])
        return start + offset, bool(stop[offset]), bool(target[offset])


TRIANGLES: tuple[TriangleSpec, ...] = (
    TriangleSpec("ZT_ZF_ZN", "ZT", "ZF", "ZN"),
    TriangleSpec("ZF_ZN_ZB", "ZF", "ZN", "ZB"),
)


def frozen_rule_specs() -> tuple[CurvatureRule, ...]:
    """Return the complete preregistered lattice: exactly eight rules."""

    rules = tuple(
        CurvatureRule(
            rule_id=(
                f"treasury_curvature_v1:{triangle.triangle_id}:"
                f"{mechanism}:lb{lookback}:h{holding}"
            ),
            triangle_id=triangle.triangle_id,
            mechanism=mechanism,
            source_return_minutes=lookback,
            holding_minutes=holding,
        )
        for triangle in TRIANGLES
        for mechanism in (
            "CURVATURE_RESIDUAL_REVERSION",
            "CURVATURE_RESIDUAL_CONTINUATION",
        )
        for lookback, holding in ((15, 30), (60, 120))
    )
    if len(rules) != MAXIMUM_RULES:
        raise TreasuryCurvatureError("frozen rule count drift")
    hashes = {_stable_hash(row.to_dict()) for row in rules}
    if len(hashes) != MAXIMUM_RULES:
        raise TreasuryCurvatureError("frozen rule lattice is not unique")
    return rules


def audit_inputs(
    root: str | Path,
    *,
    card_path: str | Path = DEFAULT_CARD,
) -> dict[str, Any]:
    """Perform a real hash/metadata audit without decoding economic outcomes."""

    project = Path(root).resolve()
    card_file = _inside_file(project, card_path)
    card = _read_json(card_file)
    _validate_card(card)

    bindings: dict[str, Any] = {}
    for key in ("input_contract", "roll_receipt", "rule_snapshot"):
        bindings[key] = _audit_file_binding(project, card["frozen_inputs"][key])

    input_contract = _read_json(
        project / bindings["input_contract"]["path"]
    )
    if (
        input_contract.get("schema") != "hydra_curve_relative_value_input_contract_v1"
        or input_contract.get("tripwire_input_contract_hash")
        != card["frozen_inputs"]["input_contract"]["contract_hash"]
        or input_contract.get("q4_excluded") is not True
    ):
        raise TreasuryCurvatureError("bound input contract drift")

    roll = _read_json(project / bindings["roll_receipt"]["path"])
    if (
        roll.get("no_forward_fill") is not True
        or roll.get("q4_access_count_delta") != 0
        or roll.get("broker_connections") != 0
        or roll.get("orders") != 0
    ):
        raise TreasuryCurvatureError("roll receipt violates frozen governance")

    parquet_bindings: dict[str, Any] = {}
    required_columns = {
        "timestamp",
        "symbol",
        "contract",
        "delivery_month",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_id",
        "instrument_id",
        "roll_segment_id",
    }
    for root_name, row in sorted(card["frozen_inputs"]["root_files"].items()):
        binding = _audit_file_binding(project, row)
        parquet_file = pq.ParquetFile(project / binding["path"])
        columns = set(parquet_file.schema_arrow.names)
        if not required_columns.issubset(columns):
            raise TreasuryCurvatureError(
                f"{root_name} Parquet schema missing {sorted(required_columns - columns)}"
            )
        if parquet_file.metadata.num_rows != int(row["record_count"]):
            raise TreasuryCurvatureError(f"{root_name} Parquet row-count drift")
        parquet_bindings[root_name] = {
            **binding,
            "record_count": int(parquet_file.metadata.num_rows),
            "row_group_count": int(parquet_file.metadata.num_row_groups),
            "column_names": sorted(columns),
            "row_groups_decoded": 0,
        }

    collision = _read_only_cemetery_collision_count(project)
    core = {
        "schema": AUDIT_SCHEMA,
        "status": "READY_FOR_ROOT_ECONOMIC_REPLAY_AUTHORIZATION",
        "branch_id": BRANCH_ID,
        "decision_card_hash": card["card_hash"],
        "card_file_sha256": _sha256(card_file),
        "frozen_bindings": bindings,
        "parquet_bindings": parquet_bindings,
        "input_contract_hash": input_contract["tripwire_input_contract_hash"],
        "roll_receipt_hash": roll.get("receipt_hash"),
        "chronological_roles": card["chronological_roles"],
        "rule_count": len(frozen_rule_specs()),
        "control_count": len(CONTROLS) - 1,
        "headline_gate_horizon_trading_days": HEADLINE_HORIZON,
        "diagnostic_horizons_trading_days": list(DIAGNOSTIC_HORIZONS),
        "headline_passes_counted_once": True,
        "economic_outcome_rows_read": 0,
        "parquet_row_groups_decoded": 0,
        "network_requests": 0,
        "data_purchase_count": 0,
        "q4_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "mission_database_writes": 0,
        "registry_writes": 0,
        "cemetery_writes": 0,
        "cemetery_exact_branch_collision_count": collision,
        "tier_ceiling": "E",
        "promotion_allowed": False,
        "implementation_hashes": {
            "economic_runner_sha256": _sha256(Path(__file__).resolve()),
            "cli_sha256": _sha256(
                project / "scripts/run_treasury_three_tenor_curvature_tripwire.py"
            ),
            "targeted_tests_sha256": _sha256(
                project / "tests/test_treasury_three_tenor_curvature_tripwire.py"
            ),
        },
    }
    return {**core, "audit_hash": _stable_hash(core)}


def prepare_curvature_features(
    short: pd.DataFrame,
    belly: pd.DataFrame,
    long: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    source_return_minutes: int,
    beta_window_bars: int = 7_800,
    beta_minimum_bars: int = 1_950,
    normalization_window_bars: int = 7_800,
    normalization_minimum_bars: int = 1_950,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Create prior-only three-tenor residual features on an exact common clock."""

    if source_return_minutes not in {15, 60}:
        raise TreasuryCurvatureError("source-return horizon is outside frozen lattice")
    frames = {
        triangle.short_root: short,
        triangle.belly_root: belly,
        triangle.long_root: long,
    }
    for root_name, frame in frames.items():
        _validate_root_frame(frame, root_name)
    canonical_session_inventory = _canonical_session_inventory(frames)

    output: pd.DataFrame | None = None
    for root_name in (triangle.short_root, triangle.belly_root, triangle.long_root):
        renamed = _root_view(frames[root_name], root_name)
        output = (
            renamed
            if output is None
            else output.merge(renamed, on="timestamp", how="inner", validate="one_to_one")
        )
    assert output is not None
    output = output.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    if output.empty or output["timestamp"].duplicated().any():
        raise TreasuryCurvatureError("empty or duplicate exact common triangle clock")

    session_columns = [f"{root_name}_session_id" for root_name in frames]
    if not _rows_have_one_distinct_non_null_value(output, session_columns).all():
        raise TreasuryCurvatureError("triangle session identity mismatch")
    output["session_id"] = output[session_columns[0]].astype(str)
    aligned_rows_before_delivery = len(output)
    delivery_columns = [f"{root_name}_delivery_month" for root_name in frames]
    delivery_synchronized = _rows_have_one_distinct_non_null_value(
        output, delivery_columns
    )
    mismatch_sessions = set(
        output.loc[~delivery_synchronized, "session_id"].astype(str)
    )
    output = output.loc[delivery_synchronized].copy().reset_index(drop=True)
    if output.empty:
        raise TreasuryCurvatureError("no exact same-delivery three-leg rows remain")
    output["available_at"] = output["timestamp"] + pd.Timedelta(minutes=1)
    output["local_minute"] = _local_minute(output["timestamp"])
    output["session_day"] = output["session_id"].map(_session_ordinal).astype(int)

    contract_identity = output[
        [f"{root_name}_contract" for root_name in frames]
    ].astype(str).agg("|".join, axis=1)
    output["contract_segment"] = contract_identity.ne(contract_identity.shift()).cumsum()
    output["roll_unsafe"] = _roll_unsafe_mask(output, contract_identity)
    output["delivery_mismatch_session"] = output["session_id"].astype(str).isin(
        mismatch_sessions
    )
    exact_previous_minute = output["timestamp"].diff().eq(pd.Timedelta(minutes=1))

    one_returns: dict[str, pd.Series] = {}
    horizon_returns: dict[str, pd.Series] = {}
    for root_name in frames:
        log_close = np.log(output[f"{root_name}_close"].astype(float))
        one_returns[root_name] = log_close.diff().where(exact_previous_minute)
        shifted_time = output["timestamp"].shift(source_return_minutes)
        exact_horizon = output["timestamp"].sub(shifted_time).eq(
            pd.Timedelta(minutes=source_return_minutes)
        )
        same_segment = output["contract_segment"].eq(
            output["contract_segment"].shift(source_return_minutes)
        )
        horizon_returns[root_name] = log_close.diff(source_return_minutes).where(
            exact_horizon & same_segment
        )

    beta_short, beta_long = _prior_two_factor_betas(
        one_returns[triangle.belly_root],
        one_returns[triangle.short_root],
        one_returns[triangle.long_root],
        segments=output["contract_segment"],
        window=beta_window_bars,
        minimum=beta_minimum_bars,
    )
    curvature = (
        horizon_returns[triangle.belly_root]
        - beta_short * horizon_returns[triangle.short_root]
        - beta_long * horizon_returns[triangle.long_root]
    )
    prior_mean = _prior_group_rolling(
        curvature,
        output["contract_segment"],
        window=normalization_window_bars,
        minimum=normalization_minimum_bars,
        statistic="mean",
    )
    prior_std = _prior_group_rolling(
        curvature,
        output["contract_segment"],
        window=normalization_window_bars,
        minimum=normalization_minimum_bars,
        statistic="std",
    )
    belly_points = output[f"{triangle.belly_root}_close"].astype(float)
    prior_belly_volatility = _prior_group_rolling(
        belly_points.diff().where(exact_previous_minute),
        output["contract_segment"],
        window=60,
        minimum=30,
        statistic="std",
    )
    output["short_return"] = horizon_returns[triangle.short_root]
    output["belly_return"] = horizon_returns[triangle.belly_root]
    output["long_return"] = horizon_returns[triangle.long_root]
    output["prior_beta_short"] = beta_short
    output["prior_beta_long"] = beta_long
    output["curvature_residual"] = curvature
    output["prior_only_curvature_z"] = (curvature - prior_mean) / prior_std.replace(
        0.0, np.nan
    )
    output["prior_belly_volatility"] = prior_belly_volatility
    output["nearest_adjacent_slope_return"] = (
        horizon_returns[triangle.short_root]
        - horizon_returns[triangle.belly_root]
    )
    output["decision_eligible"] = (
        ~output["roll_unsafe"]
        & output["prior_only_curvature_z"].notna()
        & output["prior_belly_volatility"].gt(0.0)
        & output["available_at"].eq(output["timestamp"] + pd.Timedelta(minutes=1))
    )
    output["session_full_coverage"] = _session_full_coverage_mask(output)
    present_coverage = {
        str(session_id): bool(rows["session_full_coverage"].all())
        for session_id, rows in output.groupby("session_id", sort=True)
    }
    canonical_calendar: list[dict[str, Any]] = []
    for row in canonical_session_inventory:
        value = dict(row)
        value["session_full_coverage"] = bool(
            not value["missing_roots"]
            and present_coverage.get(str(value["session_id"]), False)
        )
        if value["missing_roots"]:
            value["coverage_reason"] = "ROOT_SESSION_ABSENT_BEFORE_INNER_JOIN"
        elif str(value["session_id"]) in mismatch_sessions:
            value["coverage_reason"] = "THREE_LEG_DELIVERY_MISMATCH"
        elif not value["session_full_coverage"]:
            value["coverage_reason"] = "INCOMPLETE_REQUIRED_SESSION_WINDOW"
        else:
            value["coverage_reason"] = "FULL_COVERAGE"
        canonical_calendar.append(value)
    output.attrs["canonical_session_calendar"] = canonical_calendar
    _validate_decision_columns(output.columns)
    audit_core = {
        "triangle_id": triangle.triangle_id,
        "input_rows": {root_name: len(frame) for root_name, frame in frames.items()},
        "exact_common_clock_rows": len(output),
        "exact_common_clock_rows_before_delivery_filter": aligned_rows_before_delivery,
        "delivery_mismatch_rows_excluded": aligned_rows_before_delivery - len(output),
        "delivery_mismatch_session_count": len(mismatch_sessions),
        "canonical_union_session_count": len(canonical_calendar),
        "canonical_full_coverage_session_count": sum(
            bool(row["session_full_coverage"]) for row in canonical_calendar
        ),
        "canonical_censored_session_count": sum(
            not bool(row["session_full_coverage"]) for row in canonical_calendar
        ),
        "same_delivery_month_required": True,
        "roll_unsafe_rows": int(output["roll_unsafe"].sum()),
        "contract_segment_count": int(output["contract_segment"].nunique()),
        "source_return_minutes": source_return_minutes,
        "prior_beta_window_bars": beta_window_bars,
        "prior_beta_minimum_bars": beta_minimum_bars,
        "normalization_window_bars": normalization_window_bars,
        "normalization_minimum_bars": normalization_minimum_bars,
        "no_forward_fill": True,
    }
    return output, {**audit_core, "audit_hash": _stable_hash(audit_core)}


def _rows_have_one_distinct_non_null_value(
    frame: pd.DataFrame, columns: Sequence[str]
) -> pd.Series:
    """Vectorized equivalent of ``nunique(axis=1, dropna=True).eq(1)``.

    The source columns can be Arrow-backed and contain millions of rows.  The
    pandas row-wise implementation constructs one Series per row, which made
    the bounded Treasury replay spend minutes in an identity guard before any
    economic rule ran.  This implementation preserves the exact null behavior:
    all-null rows are false, nulls beside one repeated value are ignored, and
    two distinct non-null values are false.
    """

    if not columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    values = frame.loc[:, list(columns)].to_numpy(dtype=object, na_value=None)
    non_null = pd.notna(values)
    has_value = non_null.any(axis=1)
    first_position = non_null.argmax(axis=1)
    first_value = values[np.arange(len(values)), first_position]
    equal_or_null = ~non_null | (values == first_value[:, None])
    return pd.Series(has_value & equal_or_null.all(axis=1), index=frame.index)


def causal_intent(row: Mapping[str, Any], rule: CurvatureRule) -> int:
    """Map one completed decision row to an outright belly direction."""

    _validate_decision_columns(row.keys())
    if not bool(row.get("decision_eligible", False)):
        return 0
    score = float(row["prior_only_curvature_z"])
    if not math.isfinite(score) or abs(score) < rule.trigger_z:
        return 0
    direction = 1 if score > 0.0 else -1
    if rule.mechanism == "CURVATURE_RESIDUAL_REVERSION":
        return -direction
    if rule.mechanism == "CURVATURE_RESIDUAL_CONTINUATION":
        return direction
    raise TreasuryCurvatureError("unknown frozen curvature mechanism")


def run_economic_tripwire(
    root: str | Path,
    *,
    authorization: str,
    card_path: str | Path = DEFAULT_CARD,
    checkpoint_dir: str | Path | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    """Execute the bounded development replay after explicit root authorization."""

    if authorization != RUN_AUTHORIZATION:
        raise TreasuryCurvatureError("explicit root economic-replay authorization absent")
    project = Path(root).resolve()
    audit = audit_inputs(project, card_path=card_path)
    card = _read_json(_inside_file(project, card_path))
    root_frames = {
        root_name: _load_root_frame(
            project / binding["path"],
            root_name=root_name,
            expected_sha256=binding["sha256"],
        )
        for root_name, binding in card["frozen_inputs"]["root_files"].items()
    }
    account_rules, rule_receipt = exact._load_rule_snapshot(
        project / card["frozen_inputs"]["rule_snapshot"]["path"]
    )
    prepared: dict[tuple[str, int], pd.DataFrame] = {}
    path_indexes: dict[tuple[str, int], _CausalPathIndex] = {}
    feature_audits: dict[str, Any] = {}
    for triangle in TRIANGLES:
        for lookback in (15, 60):
            frame, feature_audit = prepare_curvature_features(
                root_frames[triangle.short_root],
                root_frames[triangle.belly_root],
                root_frames[triangle.long_root],
                triangle=triangle,
                source_return_minutes=lookback,
                beta_window_bars=int(card["causal_contract"]["prior_beta_window_bars"]),
                beta_minimum_bars=int(card["causal_contract"]["prior_beta_minimum_bars"]),
                normalization_window_bars=int(
                    card["causal_contract"]["normalization_window_bars"]
                ),
                normalization_minimum_bars=int(
                    card["causal_contract"]["normalization_minimum_bars"]
                ),
            )
            # The bound source may contain warm-up or trailing sessions outside
            # the preregistered economic roles.  They are valid feature-history
            # inputs, but never economic observations and must not be assigned a
            # role by extending the frozen dates.
            source_attrs = dict(frame.attrs)
            frame = frame.loc[_role_scope_mask(frame["timestamp"], card)].copy()
            frame.attrs.update(source_attrs)
            if frame.empty:
                raise TreasuryCurvatureError("no rows fall inside frozen temporal roles")
            frame["temporal_role"] = _assign_roles(frame["timestamp"], card)
            _attach_canonical_calendar_roles(frame, card)
            prepared[(triangle.triangle_id, lookback)] = frame
            path_indexes[(triangle.triangle_id, lookback)] = _CausalPathIndex(
                frame,
                belly_root=triangle.belly_root,
            )
            feature_audits[f"{triangle.triangle_id}:lb{lookback}"] = feature_audit

    checkpoint_root: Path | None = None
    checkpoint_contract_hash: str | None = None
    if checkpoint_dir is not None:
        checkpoint_root = _inside_output_path(project, checkpoint_dir)
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        checkpoint_contract_hash = _stable_hash(
            {
                "schema": "hydra_treasury_curvature_checkpoint_contract_v1",
                "branch_id": BRANCH_ID,
                "decision_card_hash": card["card_hash"],
                "source_audit_hash": audit["audit_hash"],
                "official_rule_snapshot_hash": _stable_hash(rule_receipt),
                "rule_specs": [row.to_dict() for row in frozen_rule_specs()],
                "economic_runner_sha256": _sha256(Path(__file__).resolve()),
            }
        )
    decisions: list[dict[str, Any]] = []
    for rule_index, rule in enumerate(frozen_rule_specs()):
        checkpoint_path = (
            checkpoint_root
            / f"rule_{rule_index:02d}_{_stable_hash(rule.to_dict())[:16]}.json"
            if checkpoint_root is not None
            else None
        )
        if (
            resume
            and checkpoint_path is not None
            and checkpoint_path.is_file()
            and checkpoint_contract_hash is not None
        ):
            decision = _read_rule_checkpoint(
                checkpoint_path,
                contract_hash=checkpoint_contract_hash,
                rule_index=rule_index,
                rule=rule,
            )
        else:
            decision = _evaluate_rule(
                prepared[(rule.triangle_id, rule.source_return_minutes)],
                triangle=_triangle(rule.triangle_id),
                rule=rule,
                account_rules=account_rules,
                card=card,
                path_index=path_indexes[
                    (rule.triangle_id, rule.source_return_minutes)
                ],
            )
            if checkpoint_path is not None and checkpoint_contract_hash is not None:
                _write_rule_checkpoint(
                    checkpoint_path,
                    contract_hash=checkpoint_contract_hash,
                    rule_index=rule_index,
                    rule=rule,
                    decision=decision,
                )
        decisions.append(decision)
    power = _power_preflight(decisions, card)
    branch_gate = _branch_gate(decisions, power)
    core = {
        "schema": SCHEMA,
        "branch_id": BRANCH_ID,
        "status": branch_gate["status"],
        "evidence_role": "VIEWED_PRE_Q4_DEVELOPMENT_TRIPWIRE_ONLY",
        "evidence_tier_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "source_audit": audit,
        "official_rule_snapshot": rule_receipt,
        "feature_audits": feature_audits,
        "rule_specs": [row.to_dict() for row in frozen_rule_specs()],
        "candidate_decisions": decisions,
        "power_preflight": power,
        "branch_gate": branch_gate,
        "headline_gate_horizon_trading_days": HEADLINE_HORIZON,
        "headline_passes_counted_once": True,
        "implementation_hashes": {
            "decision_card_sha256": _sha256(_inside_file(project, card_path)),
            "economic_runner_sha256": _sha256(Path(__file__).resolve()),
            "cli_sha256": _sha256(
                project / "scripts/run_treasury_three_tenor_curvature_tripwire.py"
            ),
        },
        "governance": {
            "incremental_data_spend_usd": 0.0,
            "q4_access_count_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "mission_database_writes": 0,
            "registry_writes": 0,
            "cemetery_writes": 0,
            "promotion_allowed": False,
            "tier_q_allowed": False,
        },
    }
    return {**core, "result_hash": _stable_hash(core)}


def _evaluate_rule(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    account_rules: Mapping[str, Mapping[str, Any]],
    card: Mapping[str, Any],
    path_index: _CausalPathIndex | None = None,
) -> dict[str, Any]:
    primary_complete, matched_by_control, decision_ledger = _build_matched_control_paths(
        frame,
        triangle=triangle,
        rule=rule,
        path_index=path_index,
    )
    role_counts = {
        role: sum(
            str(row.get("temporal_role")) == role
            and row.get("control") == "PRIMARY"
            and row.get("outcome_status") == "EXECUTABLE_COMPLETE"
            for row in decision_ledger
        )
        for role in ROLES
    }
    minimum_power = card["power_preflight"]["minimum_independent_events"]
    candidate_power_passed = all(
        int(role_counts[role]) >= int(minimum_power[role]) for role in ROLES
    )
    account_points: list[dict[str, Any]] = []
    for account_label in ("50K", "100K", "150K"):
        account_rule = account_rules[account_label]
        for risk_fraction in RISK_FRACTIONS:
            account_points.append(
                _evaluate_account_point(
                    frame,
                    primary_complete,
                    matched_by_control,
                    triangle=triangle,
                    account_label=account_label,
                    account_rule=account_rule,
                    risk_fraction=risk_fraction,
                )
            )
    gates = [
        _candidate_gate(
            point,
            card=card,
            candidate_power_passed=candidate_power_passed,
        )
        for point in account_points
    ]
    for point, gate in zip(account_points, gates, strict=True):
        point["gate"] = gate
    core = {
        "candidate_id": rule.rule_id,
        "triangle_id": triangle.triangle_id,
        "execution_root": triangle.execution_root,
        "mechanism": rule.mechanism,
        "source_return_minutes": rule.source_return_minutes,
        "holding_minutes": rule.holding_minutes,
        "role_executable_opportunity_counts": role_counts,
        "candidate_power_preflight_passed": candidate_power_passed,
        "primary_complete_opportunity_count": len(primary_complete),
        "paired_common_complete_opportunity_count": len(
            matched_by_control["PRIMARY_MATCHED"]
        ),
        "control_event_counts": {
            "PRIMARY": len(primary_complete),
            **{control: len(rows) for control, rows in matched_by_control.items()},
        },
        "control_matching": _control_matching_receipt(matched_by_control),
        "decision_status_counts": dict(
            sorted(Counter(str(row["outcome_status"]) for row in decision_ledger).items())
        ),
        "decision_ledger": decision_ledger,
        "decision_ledger_hash": _stable_hash(decision_ledger),
        "primary_complete_path_hash": _stable_hash(primary_complete),
        "matched_path_hashes": {
            control: _stable_hash(rows)
            for control, rows in sorted(matched_by_control.items())
        },
        "account_points": account_points,
        "tier_e_gate_passed": any(row["passed"] for row in gates),
        "evidence_tier": "E_EXECUTABLE_DIAGNOSTIC",
        "promotion_status": None,
    }
    return {**core, "candidate_hash": _stable_hash(core)}


def _build_matched_control_paths(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    path_index: _CausalPathIndex | None = None,
) -> tuple[
    list[dict[str, Any]],
    dict[str, list[dict[str, Any]]],
    list[dict[str, Any]],
]:
    """Create common-complete path controls and retain every causal censor."""

    index = path_index or _CausalPathIndex(
        frame,
        belly_root=triangle.belly_root,
    )
    if index.belly_root != triangle.belly_root or index.length != len(frame):
        raise TreasuryCurvatureError("causal path index/frame binding drift")

    strength = frame["prior_only_curvature_z"].abs()
    previous = strength.shift(1)
    exact_previous = frame["timestamp"].diff().eq(pd.Timedelta(minutes=1))
    selected = (
        frame["decision_eligible"].astype(bool)
        & strength.ge(rule.trigger_z)
        & previous.lt(rule.trigger_z)
        & exact_previous
    )
    maximum_entry_minute = SESSION_FLATTEN_MINUTE - rule.holding_minutes
    selected &= frame["local_minute"].between(
        EARLIEST_ENTRY_MINUTE - 1, maximum_entry_minute - 1
    )
    signal_indices = tuple(int(value) for value in np.flatnonzero(selected.to_numpy()))
    primary_complete: list[dict[str, Any]] = []
    matched = {control: [] for control in MATCHED_CONTROL_KEYS}
    decisions: list[dict[str, Any]] = []
    primary_blocked_until = -1
    for signal_index in signal_indices:
        if signal_index <= primary_blocked_until:
            decisions.append(
                _decision_record(
                    frame,
                    triangle=triangle,
                    rule=rule,
                    signal_index=signal_index,
                    control="PRIMARY",
                    outcome_status="CAUSAL_ABSTAIN",
                    censor_reason="ONE_POSITION_AT_A_TIME_GOVERNOR",
                )
            )
            continue
        row = frame.iloc[signal_index]
        primary_direction = causal_intent(row, rule)
        primary = _replay_primary_path(
            frame,
            triangle=triangle,
            rule=rule,
            signal_index=signal_index,
            direction=primary_direction,
            path_index=index,
        )
        decisions.append(primary["decision"])
        if primary["event"] is None:
            continue
        primary_event = primary["event"]
        primary_complete.append(primary_event)
        primary_blocked_until = int(primary_event["exit_index"])
        duration_bars = int(primary_event["duration_bars"])
        candidates: dict[str, dict[str, Any] | None] = {"PRIMARY": primary_event}
        for control in CONTROLS[1:]:
            direction = _control_direction(
                row,
                primary_direction=primary_direction,
                mechanism=rule.mechanism,
                control=control,
            )
            entry_lag = 6 if control == "TIMING_DELAY_5_BARS" else 1
            replay = _replay_fixed_path_control(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                direction=direction,
                entry_lag=entry_lag,
                duration_bars=duration_bars,
                control=control,
                primary_opportunity_id=str(primary_event["opportunity_id"]),
                declared_stop_distance_points=float(
                    primary_event["declared_stop_distance_points"]
                ),
                path_index=index,
            )
            decisions.append(replay["decision"])
            candidates[control] = replay["event"]
        if any(candidates[control] is None for control in CONTROLS):
            decisions.append(
                _decision_record(
                    frame,
                    triangle=triangle,
                    rule=rule,
                    signal_index=signal_index,
                    control="MATCHING_GATE",
                    outcome_status="DATA_CENSORED",
                    censor_reason="PAIRED_COMMON_COMPLETE_PATH_UNAVAILABLE",
                )
            )
            continue
        complete = {key: value for key, value in candidates.items() if value is not None}
        mapped_complete = {
            "PRIMARY_MATCHED": complete["PRIMARY"],
            **{control: complete[control] for control in CONTROLS[1:]},
        }
        if any(
            matched[control]
            and int(mapped_complete[control]["entry_index"])
            <= int(matched[control][-1]["exit_index"])
            for control in MATCHED_CONTROL_KEYS
        ):
            decisions.append(
                _decision_record(
                    frame,
                    triangle=triangle,
                    rule=rule,
                    signal_index=signal_index,
                    control="MATCHING_GATE",
                    outcome_status="CAUSAL_ABSTAIN",
                    censor_reason="MATCHED_CONTROL_OVERLAP",
                )
            )
            continue
        for control in MATCHED_CONTROL_KEYS:
            matched[control].append(mapped_complete[control])
    _require_matched_raw_paths(matched)
    return primary_complete, matched, decisions


def _replay_primary_path(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    signal_index: int,
    direction: int,
    path_index: _CausalPathIndex | None = None,
) -> dict[str, Any]:
    """Replay one primary opportunity through the immutable NumPy path index."""

    control = "PRIMARY"
    if direction == 0:
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="CAUSAL_ABSTAIN",
                censor_reason="NO_CAUSAL_DIRECTION",
            ),
        }
    index = path_index or _CausalPathIndex(
        frame,
        belly_root=triangle.belly_root,
    )
    if index.belly_root != triangle.belly_root or index.length != len(frame):
        raise TreasuryCurvatureError("causal path index/frame binding drift")
    entry_index = signal_index + 1
    if not index.is_contiguous(signal_index, entry_index):
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="DATA_CENSORED",
                censor_reason="CENSORED_NEXT_TRADABLE_ENTRY_UNAVAILABLE",
            ),
        }

    tick = TREASURY_SPECS[triangle.belly_root].tick_size_points
    entry_open = _tick_price(float(index.open[entry_index]), tick)
    volatility = float(frame.at[signal_index, "prior_belly_volatility"])
    stop_distance = _ceil_ticks(
        max(4.0 * tick, 3.0 * math.sqrt(rule.holding_minutes) * volatility), tick
    )
    stop_price = _tick_price(entry_open - direction * stop_distance, tick)
    target_price = _tick_price(
        entry_open + direction * stop_distance * rule.target_r_multiple,
        tick,
    )

    # The completed bar exactly ``holding_minutes`` after entry is the
    # time-exit open.  Its high/low cannot participate in the barrier test.
    time_exit_index = entry_index + rule.holding_minutes
    time_exit_ns = int(index.timestamp_ns[entry_index]) + (
        rule.holding_minutes * 60_000_000_000
    )
    contiguous_end = int(index.run_end[entry_index])
    barrier_end = min(
        contiguous_end,
        time_exit_index - 1,
        index.length - 1,
    )
    hit = index.first_barrier_hit(
        entry_index,
        barrier_end,
        direction=direction,
        stop_price=stop_price,
        target_price=target_price,
    )
    trigger_index: int | None = None
    trigger_reason: str | None = None
    same_bar_ambiguous = False
    exit_index: int | None = None
    if hit is not None:
        trigger_index, stop_hit, target_hit = hit
        same_bar_ambiguous = bool(stop_hit and target_hit)
        trigger_reason = "STOP" if stop_hit else "TARGET"
        candidate_exit = trigger_index + 1
        if index.is_contiguous(entry_index, candidate_exit):
            exit_index = candidate_exit
    elif (
        time_exit_index < index.length
        and index.is_contiguous(entry_index, time_exit_index)
        and int(index.timestamp_ns[time_exit_index]) == time_exit_ns
    ):
        exit_index = time_exit_index
        trigger_reason = "TIME"

    if exit_index is None or trigger_reason is None:
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="DATA_CENSORED",
                censor_reason=(
                    "CENSORED_CAUSAL_EXIT_OPEN_UNAVAILABLE"
                    if trigger_index is not None
                    else "CENSORED_FUTURE_COVERAGE_TIME_EXIT"
                ),
            ),
        }
    opportunity_id = _stable_hash(
        {
            "rule_id": rule.rule_id,
            "signal_time": frame.at[signal_index, "timestamp"].isoformat(),
            "triangle_id": triangle.triangle_id,
        }
    )[:24]
    event = _raw_path_event(
        frame,
        triangle=triangle,
        rule=rule,
        signal_index=signal_index,
        entry_index=entry_index,
        exit_index=exit_index,
        direction=direction,
        control=control,
        opportunity_id=opportunity_id,
        declared_stop_distance_points=stop_distance,
        exit_reason=trigger_reason,
        same_bar_ambiguous=same_bar_ambiguous,
        trigger_index=trigger_index,
        path_index=index,
    )
    return {
        "event": event,
        "decision": _decision_record(
            frame,
            triangle=triangle,
            rule=rule,
            signal_index=signal_index,
            control=control,
            outcome_status="EXECUTABLE_COMPLETE",
            censor_reason=None,
            event=event,
        ),
    }


def _replay_primary_path_pandas_reference(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    signal_index: int,
    direction: int,
) -> dict[str, Any]:
    control = "PRIMARY"
    if direction == 0:
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="CAUSAL_ABSTAIN",
                censor_reason="NO_CAUSAL_DIRECTION",
            ),
        }
    entry_index = signal_index + 1
    if not _path_is_contiguous(
        frame, signal_index, entry_index, belly_root=triangle.belly_root
    ):
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="DATA_CENSORED",
                censor_reason="CENSORED_NEXT_TRADABLE_ENTRY_UNAVAILABLE",
            ),
        }
    tick = TREASURY_SPECS[triangle.belly_root].tick_size_points
    entry_open = _tick_price(float(frame.at[entry_index, f"{triangle.belly_root}_open"]), tick)
    volatility = float(frame.at[signal_index, "prior_belly_volatility"])
    stop_distance = _ceil_ticks(
        max(4.0 * tick, 3.0 * math.sqrt(rule.holding_minutes) * volatility), tick
    )
    stop_price = _tick_price(entry_open - direction * stop_distance, tick)
    target_price = _tick_price(
        entry_open + direction * stop_distance * rule.target_r_multiple,
        tick,
    )
    time_exit_timestamp = frame.at[entry_index, "timestamp"] + pd.Timedelta(
        minutes=rule.holding_minutes
    )
    trigger_index: int | None = None
    trigger_reason: str | None = None
    same_bar_ambiguous = False
    exit_index: int | None = None
    cursor = entry_index
    while cursor < len(frame):
        timestamp = pd.Timestamp(frame.at[cursor, "timestamp"])
        if timestamp == time_exit_timestamp:
            exit_index = cursor
            trigger_reason = "TIME"
            break
        if timestamp > time_exit_timestamp:
            break
        low = float(frame.at[cursor, f"{triangle.belly_root}_low"])
        high = float(frame.at[cursor, f"{triangle.belly_root}_high"])
        stop_hit = low <= stop_price if direction > 0 else high >= stop_price
        target_hit = high >= target_price if direction > 0 else low <= target_price
        if stop_hit or target_hit:
            trigger_index = cursor
            same_bar_ambiguous = bool(stop_hit and target_hit)
            trigger_reason = "STOP" if stop_hit else "TARGET"
            candidate_exit = cursor + 1
            if _path_is_contiguous(
                frame,
                entry_index,
                candidate_exit,
                belly_root=triangle.belly_root,
            ):
                exit_index = candidate_exit
            break
        candidate_next = cursor + 1
        if not _path_is_contiguous(
            frame,
            cursor,
            candidate_next,
            belly_root=triangle.belly_root,
        ):
            break
        cursor = candidate_next
    if exit_index is None or trigger_reason is None:
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="DATA_CENSORED",
                censor_reason=(
                    "CENSORED_CAUSAL_EXIT_OPEN_UNAVAILABLE"
                    if trigger_index is not None
                    else "CENSORED_FUTURE_COVERAGE_TIME_EXIT"
                ),
            ),
        }
    opportunity_id = _stable_hash(
        {
            "rule_id": rule.rule_id,
            "signal_time": frame.at[signal_index, "timestamp"].isoformat(),
            "triangle_id": triangle.triangle_id,
        }
    )[:24]
    event = _raw_path_event(
        frame,
        triangle=triangle,
        rule=rule,
        signal_index=signal_index,
        entry_index=entry_index,
        exit_index=exit_index,
        direction=direction,
        control=control,
        opportunity_id=opportunity_id,
        declared_stop_distance_points=stop_distance,
        exit_reason=trigger_reason,
        same_bar_ambiguous=same_bar_ambiguous,
        trigger_index=trigger_index,
    )
    return {
        "event": event,
        "decision": _decision_record(
            frame,
            triangle=triangle,
            rule=rule,
            signal_index=signal_index,
            control=control,
            outcome_status="EXECUTABLE_COMPLETE",
            censor_reason=None,
            event=event,
        ),
    }


def _replay_fixed_path_control(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    signal_index: int,
    direction: int,
    entry_lag: int,
    duration_bars: int,
    control: str,
    primary_opportunity_id: str,
    declared_stop_distance_points: float,
    path_index: _CausalPathIndex | None = None,
) -> dict[str, Any]:
    index = path_index or _CausalPathIndex(
        frame,
        belly_root=triangle.belly_root,
    )
    if index.belly_root != triangle.belly_root or index.length != len(frame):
        raise TreasuryCurvatureError("causal path index/frame binding drift")
    entry_index = signal_index + entry_lag
    exit_index = entry_index + duration_bars
    if direction == 0 or not index.is_contiguous(signal_index, exit_index):
        return {
            "event": None,
            "decision": _decision_record(
                frame,
                triangle=triangle,
                rule=rule,
                signal_index=signal_index,
                control=control,
                outcome_status="DATA_CENSORED",
                censor_reason="CENSORED_MATCHED_CONTROL_PATH_UNAVAILABLE",
            ),
        }
    event = _raw_path_event(
        frame,
        triangle=triangle,
        rule=rule,
        signal_index=signal_index,
        entry_index=entry_index,
        exit_index=exit_index,
        direction=direction,
        control=control,
        opportunity_id=primary_opportunity_id,
        declared_stop_distance_points=declared_stop_distance_points,
        exit_reason="PRIMARY_PATH_DURATION_MATCH",
        same_bar_ambiguous=False,
        trigger_index=None,
        path_index=index,
    )
    return {
        "event": event,
        "decision": _decision_record(
            frame,
            triangle=triangle,
            rule=rule,
            signal_index=signal_index,
            control=control,
            outcome_status="EXECUTABLE_COMPLETE",
            censor_reason=None,
            event=event,
        ),
    }


def _raw_path_event(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    signal_index: int,
    entry_index: int,
    exit_index: int,
    direction: int,
    control: str,
    opportunity_id: str,
    declared_stop_distance_points: float,
    exit_reason: str,
    same_bar_ambiguous: bool,
    trigger_index: int | None,
    path_index: _CausalPathIndex | None = None,
) -> dict[str, Any]:
    root_name = triangle.belly_root
    tick = TREASURY_SPECS[root_name].tick_size_points
    # The exit executes at ``exit_index`` open.  Its later high/low are not
    # observable before the position is flat and must never enter MLL extrema.
    index = path_index
    if index is not None and (
        index.belly_root != root_name or index.length != len(frame)
    ):
        raise TreasuryCurvatureError("causal path index/frame binding drift")
    path = None if index is not None else frame.loc[entry_index : exit_index - 1]
    if index is None and (path is None or path.empty):
        raise TreasuryCurvatureError("executable path has no held bar")
    entry_open = _tick_price(
        float(
            index.open[entry_index]
            if index is not None
            else frame.at[entry_index, f"{root_name}_open"]
        ),
        tick,
    )
    exit_open = _tick_price(
        float(
            index.open[exit_index]
            if index is not None
            else frame.at[exit_index, f"{root_name}_open"]
        ),
        tick,
    )
    if index is not None:
        raw_minimum, raw_maximum = index.extrema(entry_index, exit_index)
    else:
        assert path is not None
        raw_minimum = float(path[f"{root_name}_low"].min())
        raw_maximum = float(path[f"{root_name}_high"].max())
    minimum = _tick_price(raw_minimum, tick)
    maximum = _tick_price(raw_maximum, tick)
    event_id = _stable_hash(
        {
            "opportunity_id": opportunity_id,
            "control": control,
            "entry_time": frame.at[entry_index, "timestamp"].isoformat(),
            "exit_time": frame.at[exit_index, "timestamp"].isoformat(),
            "direction": direction,
        }
    )[:24]
    entry_local_minute = int(frame.at[entry_index, "local_minute"])
    exit_local_minute = int(frame.at[exit_index, "local_minute"])
    entry_session = str(frame.at[entry_index, "session_id"])
    exit_session = str(frame.at[exit_index, "session_id"])
    if index is not None:
        same_delivery = bool(
            np.all(index.delivery_aligned[entry_index : exit_index + 1])
        )
        no_roll_unsafe = not bool(
            np.any(index.roll_unsafe[entry_index : exit_index + 1])
        )
    else:
        delivery_columns = (
            f"{triangle.short_root}_delivery_month",
            f"{triangle.belly_root}_delivery_month",
            f"{triangle.long_root}_delivery_month",
        )
        same_delivery = bool(
            frame.loc[entry_index:exit_index, list(delivery_columns)]
            .astype(str)
            .nunique(axis=1)
            .eq(1)
            .all()
        )
        no_roll_unsafe = not bool(
            frame.loc[entry_index:exit_index, "roll_unsafe"].astype(bool).any()
        )
    session_compliant = bool(
        entry_session == exit_session
        and entry_local_minute >= EARLIEST_ENTRY_MINUTE
        and exit_local_minute <= SESSION_FLATTEN_MINUTE
        and same_delivery
        and no_roll_unsafe
    )
    return {
        "event_id": f"treasury_curvature:{event_id}",
        "opportunity_id": opportunity_id,
        "control": control,
        "temporal_role": str(frame.at[signal_index, "temporal_role"]),
        "session_id": str(frame.at[signal_index, "session_id"]),
        "session_day": int(frame.at[signal_index, "session_day"]),
        "signal_index": signal_index,
        "entry_index": entry_index,
        "exit_index": exit_index,
        "duration_bars": exit_index - entry_index,
        "signal_time": frame.at[signal_index, "timestamp"].isoformat(),
        "decision_time": frame.at[signal_index, "available_at"].isoformat(),
        "earliest_executable_time": frame.at[entry_index, "timestamp"].isoformat(),
        "entry_time": frame.at[entry_index, "timestamp"].isoformat(),
        "exit_time": frame.at[exit_index, "timestamp"].isoformat(),
        "exit_trigger_bar_time": (
            frame.at[trigger_index, "timestamp"].isoformat()
            if trigger_index is not None
            else None
        ),
        "exit_trigger_available_at": (
            (frame.at[trigger_index, "timestamp"] + pd.Timedelta(minutes=1)).isoformat()
            if trigger_index is not None
            else None
        ),
        "execution_root": root_name,
        "contract": str(frame.at[entry_index, f"{root_name}_contract"]),
        "delivery_month": str(
            frame.at[entry_index, f"{root_name}_delivery_month"]
        ),
        "direction": direction,
        "entry_open": entry_open,
        "exit_open": exit_open,
        "path_low": minimum,
        "path_high": maximum,
        "tick_size": tick,
        "point_value_usd": TREASURY_SPECS[root_name].point_value_usd,
        "declared_stop_distance_points": declared_stop_distance_points,
        "exit_reason": exit_reason,
        "same_bar_ambiguous": same_bar_ambiguous,
        "session_compliant": session_compliant,
        "same_delivery_month_compliant": bool(same_delivery),
    }


def _evaluate_account_point(
    frame: pd.DataFrame,
    primary_complete: Sequence[Mapping[str, Any]],
    matched_by_control: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    triangle: TriangleSpec,
    account_label: str,
    account_rule: Mapping[str, Any],
    risk_fraction: float,
) -> dict[str, Any]:
    sizing = _freeze_discovery_sizing(
        primary_complete,
        execution_root=triangle.execution_root,
        account_rule=account_rule,
        risk_fraction=risk_fraction,
    )
    quantity = int(sizing["integer_quantity"])
    controls: dict[str, Any] = {}
    event_identity: dict[str, Any] = {}
    trade_ledgers: dict[str, Any] = {}
    primary_stressed_events: tuple[TradePathEvent, ...] = ()
    for control in ACCOUNT_CONTROL_KEYS:
        source_rows = (
            primary_complete
            if control == "PRIMARY"
            else matched_by_control[control]
        )
        scenario_events = {
            scenario: _scenario_events(
                source_rows,
                execution_root=triangle.execution_root,
                quantity=quantity,
                maximum_contracts=int(account_rule["maximum_mini_contracts"]),
                scenario=scenario,
            )
            for scenario in SCENARIOS
        }
        _require_scenario_identity(
            scenario_events["NORMAL"], scenario_events["STRESSED_1_5X"]
        )
        if control == "PRIMARY":
            primary_stressed_events = scenario_events["STRESSED_1_5X"]
        controls[control] = _account_role_matrix(
            frame,
            scenario_events,
            account_rule=account_rule,
        )
        event_identity[control] = {
            scenario: _event_identity_hash(events)
            for scenario, events in scenario_events.items()
        }
        trade_ledgers[control] = {
            scenario: {
                "events": [event.to_dict() for event in events],
                "ledger_hash": _stable_hash([event.to_dict() for event in events]),
                "cost_contract": _scenario_cost_contract(
                    triangle.execution_root, scenario
                ),
            }
            for scenario, events in scenario_events.items()
        }
    headline_days = set(
        controls["PRIMARY"]["STRESSED_1_5X"]["FINAL_DEVELOPMENT"][
            str(HEADLINE_HORIZON)
        ]["headline_traversed_session_days"]
    )
    return {
        "account_label": account_label,
        "account_size_usd": int(account_rule["account_size_usd"]),
        "risk_fraction_of_initial_mll": risk_fraction,
        "integer_quantity": quantity,
        "sizing_freeze": sizing,
        "controls": controls,
        "paired_headline_deltas": _paired_headline_deltas(controls),
        "event_identity_hashes": event_identity,
        "trade_ledgers": trade_ledgers,
        "trade_ledgers_hash": _stable_hash(trade_ledgers),
        "final_stressed_profit_concentration": _headline_profit_concentration(
            primary_stressed_events,
            traversed_session_days=headline_days,
        ),
        "concentration_population": (
            "ACTUALLY_TRAVERSED_DAILY_PATHS_FROM_FULL_COVERAGE_FINAL_DEVELOPMENT_HEADLINE_EPISODES_ONLY"
        ),
    }


def _freeze_discovery_sizing(
    events: Sequence[Mapping[str, Any]],
    *,
    execution_root: str,
    account_rule: Mapping[str, Any],
    risk_fraction: float,
) -> dict[str, Any]:
    discovery = tuple(
        row for row in events if str(row.get("temporal_role")) == "DISCOVERY"
    )
    if not discovery:
        core = {
            "policy": "STATIC_INITIAL_MLL_FRACTION_WITH_DISCOVERY_ONLY_MAX_DECLARED_STOP_RISK",
            "discovery_event_count": 0,
            "maximum_declared_stop_risk_one_contract_usd": None,
            "initial_mll_risk_budget_usd": float(
                account_rule["maximum_loss_limit_usd"]
            )
            * risk_fraction,
            "initial_mll_fraction": float(risk_fraction),
            "integer_quantity": 0,
            "validation_or_final_inputs_used": False,
        }
        return {**core, "sizing_hash": _stable_hash(core)}
    spec = TREASURY_SPECS[execution_root]
    worst_declared = max(
        float(row["declared_stop_distance_points"]) * spec.point_value_usd
        + 4.0 * spec.tick_value_usd
        + spec.round_turn_commission_usd
        for row in discovery
    )
    budget = float(account_rule["maximum_loss_limit_usd"]) * risk_fraction
    by_risk = int(math.floor(budget / worst_declared))
    quantity = max(0, min(by_risk, int(account_rule["maximum_mini_contracts"])))
    core = {
        "policy": "STATIC_INITIAL_MLL_FRACTION_WITH_DISCOVERY_ONLY_MAX_DECLARED_STOP_RISK",
        "discovery_event_count": len(discovery),
        "discovery_opportunity_ids": sorted(
            str(row["opportunity_id"]) for row in discovery
        ),
        "maximum_declared_stop_risk_one_contract_usd": float(worst_declared),
        "initial_mll_risk_budget_usd": float(budget),
        "initial_mll_fraction": float(risk_fraction),
        "integer_quantity": quantity,
        "validation_or_final_inputs_used": False,
    }
    return {**core, "sizing_hash": _stable_hash(core)}


def _scenario_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    execution_root: str,
    quantity: int,
    maximum_contracts: int,
    scenario: str,
) -> tuple[TradePathEvent, ...]:
    if scenario not in SCENARIOS:
        raise TreasuryCurvatureError("unknown frozen cost scenario")
    if quantity <= 0:
        return ()
    spec = TREASURY_SPECS[execution_root]
    slippage_ticks = 1
    normal_all_in_one_contract = (
        2.0 * slippage_ticks * spec.tick_value_usd
        + spec.round_turn_commission_usd
    )
    target_all_in_one_contract = normal_all_in_one_contract * (
        1.0 if scenario == "NORMAL" else 1.5
    )
    nonprice_charge_one_contract = (
        target_all_in_one_contract
        - 2.0 * slippage_ticks * spec.tick_value_usd
    )
    if nonprice_charge_one_contract < 0.0:
        raise TreasuryCurvatureError("invalid exact 1.5x stressed cost bridge")
    output: list[TradePathEvent] = []
    for row in rows:
        direction = int(row["direction"])
        tick = float(row["tick_size"])
        entry_open = float(row["entry_open"])
        exit_open = float(row["exit_open"])
        entry_fill = _tick_price(entry_open + direction * slippage_ticks * tick, tick)
        exit_fill = _tick_price(exit_open - direction * slippage_ticks * tick, tick)
        gross = (
            direction
            * (exit_fill - entry_fill)
            * spec.point_value_usd
            * quantity
        )
        nonprice_charge = nonprice_charge_one_contract * quantity
        net = gross - nonprice_charge
        if direction > 0:
            adverse_mark = float(row["path_low"])
            favorable_mark = float(row["path_high"])
            worst = (adverse_mark - entry_fill) * spec.point_value_usd * quantity
            best = (favorable_mark - entry_fill) * spec.point_value_usd * quantity
        else:
            adverse_mark = float(row["path_high"])
            favorable_mark = float(row["path_low"])
            worst = (entry_fill - adverse_mark) * spec.point_value_usd * quantity
            best = (entry_fill - favorable_mark) * spec.point_value_usd * quantity
        output.append(
            TradePathEvent(
                event_id=f"{row['event_id']}:{scenario}",
                decision_ns=int(pd.Timestamp(row["decision_time"]).value),
                exit_ns=int(pd.Timestamp(row["exit_time"]).value),
                session_day=int(row["session_day"]),
                net_pnl=float(net),
                gross_pnl=float(gross),
                worst_unrealized_pnl=float(
                    min(0.0, worst - nonprice_charge, net)
                ),
                best_unrealized_pnl=float(
                    max(0.0, best - nonprice_charge, net)
                ),
                quantity=quantity,
                mini_equivalent=float(quantity),
                regime=f"{row['temporal_role']}:{row['control']}",
                session_compliant=bool(row["session_compliant"]),
                contract_limit_compliant=quantity <= maximum_contracts,
                same_bar_ambiguous=bool(row["same_bar_ambiguous"]),
            )
        )
    return tuple(output)


def _scenario_cost_contract(execution_root: str, scenario: str) -> dict[str, Any]:
    spec = TREASURY_SPECS[execution_root]
    normal = 2.0 * spec.tick_value_usd + spec.round_turn_commission_usd
    multiplier = 1.0 if scenario == "NORMAL" else 1.5
    total = normal * multiplier
    core = {
        "execution_root": execution_root,
        "scenario": scenario,
        "tick_executable_slippage_ticks_per_side": 1,
        "price_slippage_cost_one_contract_usd": 2.0 * spec.tick_value_usd,
        "nonprice_charge_one_contract_usd": total
        - 2.0 * spec.tick_value_usd,
        "normal_all_in_cost_one_contract_usd": normal,
        "scenario_all_in_cost_one_contract_usd": total,
        "all_in_multiplier_vs_normal": multiplier,
    }
    return {**core, "cost_contract_hash": _stable_hash(core)}


def _account_role_matrix(
    frame: pd.DataFrame,
    events_by_scenario: Mapping[str, Sequence[TradePathEvent]],
    *,
    account_rule: Mapping[str, Any],
) -> dict[str, Any]:
    config = exact._account_config(account_rule)
    output: dict[str, Any] = {}
    canonical_rows = frame.attrs.get("canonical_session_calendar")
    if canonical_rows:
        canonical_calendar = pd.DataFrame([dict(row) for row in canonical_rows])
        required = {
            "session_id",
            "session_day",
            "session_full_coverage",
            "temporal_role",
        }
        if not required.issubset(canonical_calendar.columns):
            raise TreasuryCurvatureError("canonical session calendar is incomplete")
    else:
        canonical_calendar = (
            frame.loc[
                :, ["session_id", "session_day", "session_full_coverage", "temporal_role"]
            ]
            .groupby(
                ["session_id", "session_day", "temporal_role"],
                as_index=False,
                sort=True,
            )
            .agg(session_full_coverage=("session_full_coverage", "all"))
        )
    for scenario in SCENARIOS:
        roles: dict[str, Any] = {}
        for role in ROLES:
            session_rows = (
                canonical_calendar.loc[
                    canonical_calendar["temporal_role"].eq(role),
                    ["session_id", "session_day", "session_full_coverage"],
                ]
                .sort_values("session_id", kind="mergesort")
            )
            days = tuple(int(value) for value in session_rows["session_day"])
            coverage_by_day = {
                int(row.session_day): bool(row.session_full_coverage)
                for row in session_rows.itertuples(index=False)
            }
            horizons: dict[str, Any] = {}
            for horizon in HORIZONS:
                start_positions = tuple(
                    range(0, len(days) - horizon + 1, horizon)
                )
                starts = tuple(days[position] for position in start_positions)
                episodes_list: list[CombineEpisodeResult] = []
                censored_starts: list[dict[str, Any]] = []
                full_coverage_episode_days: list[int] = []
                traversed_session_days: list[int] = []
                for position in start_positions:
                    episode_days = days[position : position + horizon]
                    incomplete = tuple(
                        day for day in episode_days if not coverage_by_day[day]
                    )
                    if incomplete:
                        censored_starts.append(
                            {
                                "start_day": int(episode_days[0]),
                                "horizon_trading_days": horizon,
                                "status": "DATA_CENSORED",
                                "reason": "INCOMPLETE_REQUIRED_SESSION_WINDOW",
                                "incomplete_session_days": list(incomplete),
                            }
                        )
                        continue
                    episode = run_combine_episode(
                        events_by_scenario[scenario],
                        days,
                        start_day=int(episode_days[0]),
                        maximum_duration_days=horizon,
                        config=config,
                        maximum_mini_equivalent=float(
                            account_rule["maximum_mini_contracts"]
                        ),
                    )
                    episodes_list.append(episode)
                    full_coverage_episode_days.extend(int(day) for day in episode_days)
                    traversed_session_days.extend(
                        int(row["session_day"]) for row in episode.daily_path
                    )
                episodes = tuple(episodes_list)
                episode_ledger = [row.to_dict() for row in episodes]
                summary = _episode_summary(episodes)
                horizons[str(horizon)] = {
                    **summary,
                    "start_days": list(starts),
                    "all_start_count": len(starts),
                    "full_coverage_start_count": len(episodes),
                    "data_censored_start_count": len(censored_starts),
                    "censored_start_ledger": censored_starts,
                    "censored_start_ledger_hash": _stable_hash(censored_starts),
                    "episode_ledger": episode_ledger,
                    "episode_ledger_hash": _stable_hash(episode_ledger),
                    "headline_full_coverage_episode_session_days": sorted(
                        set(full_coverage_episode_days)
                    )
                    if horizon == HEADLINE_HORIZON
                    else [],
                    "headline_traversed_session_days": sorted(
                        set(traversed_session_days)
                    )
                    if horizon == HEADLINE_HORIZON
                    else [],
                    "coverage_status": (
                        "FULL_COVERAGE"
                        if not censored_starts
                        else "DATA_CENSORED"
                        if not episodes
                        else "MIXED_FULL_COVERAGE_AND_DATA_CENSORED"
                    ),
                }
            roles[role] = horizons
        output[scenario] = roles
    return output


def _episode_summary(episodes: Sequence[CombineEpisodeResult]) -> dict[str, Any]:
    if not episodes:
        return {
            "episodes": 0,
            "passes": 0,
            "pass_rate": 0.0,
            "mll_breaches": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "all_passing_paths_consistency_compliant": False,
            "net_total_usd": 0.0,
            "net_median_usd": 0.0,
            "target_progress_median": 0.0,
            "target_progress_p25": 0.0,
            "minimum_mll_buffer_usd": None,
            "median_days_to_target": None,
            "terminal_distribution": {},
        }
    passed = [row for row in episodes if row.passed]
    breached = sum(row.mll_breached for row in episodes)
    target_days = [row.days_to_target for row in passed if row.days_to_target is not None]
    return {
        "episodes": len(episodes),
        "passes": len(passed),
        "pass_rate": float(len(passed) / len(episodes)),
        "mll_breaches": breached,
        "mll_breach_rate": float(breached / len(episodes)),
        "consistency_compliance_rate": float(
            sum(row.consistency_ok for row in episodes) / len(episodes)
        ),
        "all_passing_paths_consistency_compliant": bool(passed)
        and all(row.consistency_ok for row in passed),
        "net_total_usd": float(sum(row.net_pnl for row in episodes)),
        "net_median_usd": float(np.median([row.net_pnl for row in episodes])),
        "target_progress_median": float(
            np.median([row.target_progress for row in episodes])
        ),
        "target_progress_p25": float(
            np.percentile([row.target_progress for row in episodes], 25)
        ),
        "minimum_mll_buffer_usd": float(
            min(row.minimum_mll_buffer for row in episodes)
        ),
        "median_days_to_target": (
            float(np.median(target_days)) if target_days else None
        ),
        "terminal_distribution": dict(
            sorted(Counter(row.terminal.value for row in episodes).items())
        ),
    }


def _paired_headline_deltas(controls: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for control in CONTROLS[1:]:
        for scenario in SCENARIOS:
            for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
                primary = controls["PRIMARY_MATCHED"][scenario][role][
                    str(HEADLINE_HORIZON)
                ]
                matched = controls[control][scenario][role][str(HEADLINE_HORIZON)]
                rows.append(
                    {
                        "control": control,
                        "scenario": scenario,
                        "temporal_role": role,
                        "headline_horizon_trading_days": HEADLINE_HORIZON,
                        "start_days_identical": primary["start_days"] == matched["start_days"],
                        "pass_delta": int(primary["passes"] - matched["passes"]),
                        "net_total_delta_usd": float(
                            primary["net_total_usd"] - matched["net_total_usd"]
                        ),
                        "median_target_progress_delta": float(
                            primary["target_progress_median"]
                            - matched["target_progress_median"]
                        ),
                    }
                )
    return rows


def _candidate_gate(
    point: Mapping[str, Any],
    *,
    card: Mapping[str, Any],
    candidate_power_passed: bool = True,
) -> dict[str, Any]:
    controls = point["controls"]
    primary = controls["PRIMARY"]
    validation = primary["STRESSED_1_5X"]["VALIDATION"][str(HEADLINE_HORIZON)]
    final_stress = primary["STRESSED_1_5X"]["FINAL_DEVELOPMENT"][
        str(HEADLINE_HORIZON)
    ]
    final_normal = primary["NORMAL"]["FINAL_DEVELOPMENT"][str(HEADLINE_HORIZON)]
    deltas = [
        row
        for row in point["paired_headline_deltas"]
        if row["scenario"] == "STRESSED_1_5X"
        and row["temporal_role"] == "FINAL_DEVELOPMENT"
    ]
    progress_controls_passed = all(
        float(row["median_target_progress_delta"]) > 0.0 for row in deltas
    )
    extra_pass_override = int(final_stress["passes"]) >= 2
    checks = {
        "candidate_power_preflight": bool(candidate_power_passed),
        "headline_is_20d_only": HEADLINE_HORIZON == int(
            card["frozen_gate"]["headline_horizon_trading_days"]
        ),
        "positive_stressed_validation_net": float(validation["net_total_usd"]) > 0.0,
        "positive_stressed_final_net": float(final_stress["net_total_usd"]) > 0.0,
        "minimum_final_normal_passes": int(final_normal["passes"]) >= 2,
        "minimum_final_stressed_passes": int(final_stress["passes"]) >= 1,
        "two_positive_temporal_contexts": sum(
            float(primary["STRESSED_1_5X"][role][str(HEADLINE_HORIZON)]["net_total_usd"])
            > 0.0
            for role in ("VALIDATION", "FINAL_DEVELOPMENT")
        )
        >= 2,
        "controlled_mll": float(final_stress["mll_breach_rate"]) <= 0.10,
        "passing_consistency": bool(
            final_stress["all_passing_paths_consistency_compliant"]
        ),
        "nonnegative_p25_progress": float(final_stress["target_progress_p25"]) >= 0.0,
        "beats_each_control_stressed_net": all(
            float(row["net_total_delta_usd"]) > 0.0 for row in deltas
        ),
        "beats_each_control_progress_or_extra_pass": progress_controls_passed
        or extra_pass_override,
        "identical_account_starts": all(bool(row["start_days_identical"]) for row in deltas),
        "single_trade_concentration": float(
            point["final_stressed_profit_concentration"]["maximum_single_trade_share"]
        )
        <= 0.50,
        "single_day_concentration": float(
            point["final_stressed_profit_concentration"]["maximum_single_day_share"]
        )
        <= 0.50,
        "quantity_is_legal": 0 < int(point["integer_quantity"]),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "headline_horizon_trading_days": HEADLINE_HORIZON,
        "headline_final_normal_passes": int(final_normal["passes"]),
        "headline_final_stressed_passes": int(final_stress["passes"]),
        "final_stressed_net_usd": float(final_stress["net_total_usd"]),
        "evidence_ceiling": "TIER_E_EXECUTABLE_DIAGNOSTIC",
        "promotion_allowed": False,
    }


def _profit_concentration(events: Sequence[TradePathEvent]) -> dict[str, float]:
    positive = [max(0.0, float(row.net_pnl)) for row in events]
    positive_total = sum(positive)
    by_day: dict[int, float] = {}
    for row in events:
        by_day[int(row.session_day)] = by_day.get(int(row.session_day), 0.0) + float(
            row.net_pnl
        )
    positive_days = [max(0.0, value) for value in by_day.values()]
    day_total = sum(positive_days)
    return {
        "maximum_single_trade_share": (
            float(max(positive) / positive_total) if positive_total > 0.0 else 0.0
        ),
        "maximum_single_day_share": (
            float(max(positive_days) / day_total) if day_total > 0.0 else 0.0
        ),
    }


def _headline_profit_concentration(
    events: Sequence[TradePathEvent], *, traversed_session_days: set[int]
) -> dict[str, float]:
    return _profit_concentration(
        tuple(
            row
            for row in events
            if str(row.regime).startswith("FINAL_DEVELOPMENT:")
            and int(row.session_day) in traversed_session_days
        )
    )


def _power_preflight(
    decisions: Sequence[Mapping[str, Any]], card: Mapping[str, Any]
) -> dict[str, Any]:
    minimum = card["power_preflight"]["minimum_independent_events"]
    rows = []
    for decision in decisions:
        counts = decision["role_executable_opportunity_counts"]
        passed = bool(decision["candidate_power_preflight_passed"])
        rows.append(
            {
                "candidate_id": decision["candidate_id"],
                "counts": counts,
                "passed": passed,
            }
        )
    return {
        "passed": any(row["passed"] for row in rows),
        "candidate_results": rows,
        "when_underpowered": "TREASURY_CURVATURE_UNDERPOWERED_NO_THRESHOLD_RELAXATION",
    }


def _branch_gate(
    decisions: Sequence[Mapping[str, Any]], power: Mapping[str, Any]
) -> dict[str, Any]:
    tier_e = sorted(
        f"{decision['candidate_id']}:{point['account_label']}:r{point['risk_fraction_of_initial_mll']}"
        for decision in decisions
        for point in decision["account_points"]
        if point["gate"]["passed"]
    )
    if tier_e:
        status = "TREASURY_CURVATURE_TO_BELLY_GREEN_TIER_E"
    elif not power["passed"]:
        status = "TREASURY_CURVATURE_UNDERPOWERED_NO_THRESHOLD_RELAXATION"
    else:
        any_positive = any(
            float(point["gate"]["final_stressed_net_usd"]) > 0.0
            for decision in decisions
            for point in decision["account_points"]
        )
        status = (
            "TREASURY_CURVATURE_TO_BELLY_WEAK"
            if any_positive
            else "TREASURY_CURVATURE_TO_BELLY_FALSIFIED"
        )
    return {
        "status": status,
        "tier_e_candidate_ids": tier_e,
        "tier_q_candidate_ids": [],
        "headline_horizon_trading_days": HEADLINE_HORIZON,
        "diagnostic_horizons_excluded_from_gate": list(DIAGNOSTIC_HORIZONS),
        "promotion_allowed": False,
    }


def _control_direction(
    row: Mapping[str, Any],
    *,
    primary_direction: int,
    mechanism: str,
    control: str,
) -> int:
    if control == "DIRECTION_FLIP":
        return -primary_direction
    if control == "TIMING_DELAY_5_BARS":
        return primary_direction
    if control == "BELLY_LEVEL_ONLY":
        source = float(row["belly_return"])
    elif control == "NEAREST_ADJACENT_SLOPE":
        source = float(row["nearest_adjacent_slope_return"])
    else:
        raise TreasuryCurvatureError(f"unknown matched control: {control}")
    direction = 1 if source > 0.0 else -1 if source < 0.0 else primary_direction
    if mechanism == "CURVATURE_RESIDUAL_REVERSION":
        direction *= -1
    return direction


def _decision_record(
    frame: pd.DataFrame,
    *,
    triangle: TriangleSpec,
    rule: CurvatureRule,
    signal_index: int,
    control: str,
    outcome_status: str,
    censor_reason: str | None,
    event: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    row = frame.iloc[signal_index]
    core = {
        "rule_id": rule.rule_id,
        "triangle_id": triangle.triangle_id,
        "control": control,
        "signal_time": pd.Timestamp(row["timestamp"]).isoformat(),
        "available_at": pd.Timestamp(row["available_at"]).isoformat(),
        "temporal_role": str(row["temporal_role"]),
        "session_id": str(row["session_id"]),
        "feature_hash": _stable_hash(
            {
                "short_return": _finite_or_none(row["short_return"]),
                "belly_return": _finite_or_none(row["belly_return"]),
                "long_return": _finite_or_none(row["long_return"]),
                "prior_beta_short": _finite_or_none(row["prior_beta_short"]),
                "prior_beta_long": _finite_or_none(row["prior_beta_long"]),
                "curvature_residual": _finite_or_none(row["curvature_residual"]),
                "prior_only_curvature_z": _finite_or_none(
                    row["prior_only_curvature_z"]
                ),
                "contract_segment": int(row["contract_segment"]),
            }
        ),
        "outcome_status": outcome_status,
        "censor_reason": censor_reason,
        "opportunity_id": event.get("opportunity_id") if event else None,
        "entry_time": event.get("entry_time") if event else None,
        "exit_time": event.get("exit_time") if event else None,
    }
    return {**core, "decision_hash": _stable_hash(core)}


def _control_matching_receipt(
    rows: Mapping[str, Sequence[Mapping[str, Any]]]
) -> dict[str, Any]:
    primary = tuple(rows["PRIMARY_MATCHED"])
    primary_ids = [str(row["opportunity_id"]) for row in primary]
    primary_durations = [int(row["duration_bars"]) for row in primary]
    checks: dict[str, Any] = {}
    for control in CONTROLS[1:]:
        values = tuple(rows[control])
        checks[control] = {
            "event_count_identical": len(values) == len(primary),
            "opportunity_ids_identical": [
                str(row["opportunity_id"]) for row in values
            ]
            == primary_ids,
            "duty_duration_bars_identical": [
                int(row["duration_bars"]) for row in values
            ]
            == primary_durations,
            "session_ids_identical": [str(row["session_id"]) for row in values]
            == [str(row["session_id"]) for row in primary],
            "temporal_roles_identical": [
                str(row["temporal_role"]) for row in values
            ]
            == [str(row["temporal_role"]) for row in primary],
        }
    passed = all(all(row.values()) for row in checks.values())
    if not passed:
        raise TreasuryCurvatureError("path/exposure/duty matching drift")
    core = {
        "policy": "PAIRED_COMMON_COMPLETE_PRIMARY_OPPORTUNITY_PATHS",
        "same_account_start_grid": True,
        "same_quantity_frozen_from_primary_stop_risk": True,
        "same_scenario_cost_schedule": True,
        "checks": checks,
        "passed": True,
    }
    return {**core, "matching_hash": _stable_hash(core)}


def _require_matched_raw_paths(
    rows: Mapping[str, Sequence[Mapping[str, Any]]]
) -> None:
    counts = {control: len(rows[control]) for control in MATCHED_CONTROL_KEYS}
    if len(set(counts.values())) != 1:
        raise TreasuryCurvatureError(f"matched control count drift: {counts}")
    primary = rows["PRIMARY_MATCHED"]
    for control in CONTROLS[1:]:
        for observed, matched in zip(primary, rows[control], strict=True):
            if (
                observed["opportunity_id"] != matched["opportunity_id"]
                or observed["session_id"] != matched["session_id"]
                or observed["temporal_role"] != matched["temporal_role"]
                or int(observed["duration_bars"]) != int(matched["duration_bars"])
            ):
                raise TreasuryCurvatureError("matched raw path identity drift")


def _require_scenario_identity(
    normal: Sequence[TradePathEvent], stressed: Sequence[TradePathEvent]
) -> None:
    def identity(row: TradePathEvent) -> tuple[Any, ...]:
        event_id = str(row.event_id)
        for suffix in (":NORMAL", ":STRESSED_1_5X"):
            if event_id.endswith(suffix):
                event_id = event_id[: -len(suffix)]
        return (
            event_id,
            row.decision_ns,
            row.exit_ns,
            row.session_day,
            row.quantity,
            row.mini_equivalent,
        )

    if [identity(row) for row in normal] != [identity(row) for row in stressed]:
        raise TreasuryCurvatureError("normal/stressed path identity drift")


def _event_identity_hash(events: Sequence[TradePathEvent]) -> str:
    return _stable_hash(
        [
            {
                "event_id": row.event_id.rsplit(":", 1)[0],
                "decision_ns": row.decision_ns,
                "exit_ns": row.exit_ns,
                "session_day": row.session_day,
                "quantity": row.quantity,
            }
            for row in events
        ]
    )


def _load_root_frame(
    path: Path, *, root_name: str, expected_sha256: str
) -> pd.DataFrame:
    if _sha256(path) != expected_sha256:
        raise TreasuryCurvatureError(f"{root_name} input hash drift before decode")
    frame = pd.read_parquet(path)
    _validate_root_frame(frame, root_name)
    output = frame.copy().sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    output["timestamp"] = pd.to_datetime(output["timestamp"], utc=True)
    output = output.loc[
        output["timestamp"].ge(pd.Timestamp("2023-01-03", tz="UTC"))
        & output["timestamp"].lt(pd.Timestamp("2024-10-01", tz="UTC"))
    ].reset_index(drop=True)
    if output.empty or output["timestamp"].duplicated().any():
        raise TreasuryCurvatureError(f"{root_name} decoded input is empty or duplicated")
    spec = TREASURY_SPECS[root_name]
    for column in ("open", "high", "low", "close"):
        values = output[column].astype(float).to_numpy()
        ticks = values / spec.tick_size_points
        if not np.allclose(ticks, np.rint(ticks), rtol=0.0, atol=1e-8):
            raise TreasuryCurvatureError(f"{root_name} {column} is off tick grid")
    return output


def _validate_root_frame(frame: pd.DataFrame, root_name: str) -> None:
    required = {
        "timestamp",
        "symbol",
        "contract",
        "delivery_month",
        "open",
        "high",
        "low",
        "close",
        "session_id",
        "roll_segment_id",
    }
    missing = required - set(frame.columns)
    if missing:
        raise TreasuryCurvatureError(
            f"{root_name} source columns missing: {sorted(missing)}"
        )
    _validate_decision_columns(frame.columns)
    if not frame["symbol"].astype(str).eq(root_name).all():
        raise TreasuryCurvatureError(f"{root_name} source contains another symbol")
    if frame["contract"].astype(str).str.strip().eq("").any():
        raise TreasuryCurvatureError(f"{root_name} contract identity is empty")
    numeric = frame[["open", "high", "low", "close"]].apply(
        pd.to_numeric, errors="coerce"
    )
    if numeric.isna().any(axis=None) or (numeric <= 0.0).any(axis=None):
        raise TreasuryCurvatureError(f"{root_name} source has invalid OHLC")


def _root_view(frame: pd.DataFrame, root_name: str) -> pd.DataFrame:
    columns = [
        "timestamp",
        "session_id",
        "contract",
        "delivery_month",
        "roll_segment_id",
        "open",
        "high",
        "low",
        "close",
    ]
    output = frame[columns].copy()
    output["timestamp"] = pd.to_datetime(output["timestamp"], utc=True)
    return output.rename(
        columns={column: f"{root_name}_{column}" for column in columns if column != "timestamp"}
    )


def _roll_unsafe_mask(
    frame: pd.DataFrame, contract_identity: pd.Series
) -> pd.Series:
    sessions = tuple(dict.fromkeys(frame["session_id"].astype(str)))
    position = {session: index for index, session in enumerate(sessions)}
    changed = contract_identity.ne(contract_identity.shift())
    roll_sessions = {
        str(frame.at[index, "session_id"])
        for index in np.flatnonzero(changed.to_numpy())
        if index > 0
    }
    unsafe: set[str] = set()
    for session in roll_sessions:
        center = position[session]
        unsafe.update(
            sessions[index]
            for index in range(max(0, center - 1), min(len(sessions), center + 2))
        )
    return frame["session_id"].astype(str).isin(unsafe)


def _session_full_coverage_mask(frame: pd.DataFrame) -> pd.Series:
    """Freeze an outcome-independent exact minute mask for account windows."""

    required_minutes = set(range(COVERAGE_START_MINUTE, COVERAGE_END_MINUTE + 1))
    complete: dict[str, bool] = {}
    for session_id, rows in frame.groupby("session_id", sort=True):
        window = rows.loc[
            rows["local_minute"].between(
                COVERAGE_START_MINUTE, COVERAGE_END_MINUTE
            )
        ].sort_values("timestamp", kind="mergesort")
        minutes = [int(value) for value in window["local_minute"]]
        exact_grid = (
            len(minutes) == len(required_minutes)
            and len(set(minutes)) == len(required_minutes)
            and set(minutes) == required_minutes
            and (
                len(window) <= 1
                or window["timestamp"].diff().iloc[1:].eq(
                    pd.Timedelta(minutes=1)
                ).all()
            )
        )
        complete[str(session_id)] = bool(
            exact_grid
            and not window.empty
            and not window["roll_unsafe"].astype(bool).any()
            and not window["delivery_mismatch_session"].astype(bool).any()
        )
    return frame["session_id"].astype(str).map(complete).fillna(False).astype(bool)


def _canonical_session_inventory(
    frames: Mapping[str, pd.DataFrame]
) -> list[dict[str, Any]]:
    """Freeze the pre-join union so absent sessions cannot compress P20."""

    observed: dict[str, set[str]] = {}
    for root_name, frame in frames.items():
        for session_id in frame["session_id"].astype(str).unique():
            observed.setdefault(str(session_id), set()).add(str(root_name))
    all_roots = set(str(root_name) for root_name in frames)
    return [
        {
            "session_id": session_id,
            "session_day": _session_ordinal(session_id),
            "observed_roots": sorted(observed[session_id]),
            "missing_roots": sorted(all_roots - observed[session_id]),
        }
        for session_id in sorted(observed, key=_session_ordinal)
    ]


def _attach_canonical_calendar_roles(
    frame: pd.DataFrame, card: Mapping[str, Any]
) -> None:
    calendar = [dict(row) for row in frame.attrs.get("canonical_session_calendar", ())]
    if not calendar:
        raise TreasuryCurvatureError("canonical pre-join session inventory absent")
    timestamps = pd.Series(
        [pd.Timestamp(str(row["session_id"]), tz="UTC") for row in calendar]
    )
    scoped = _role_scope_mask(timestamps, card)
    calendar = [row for row, keep in zip(calendar, scoped, strict=True) if bool(keep)]
    if not calendar:
        raise TreasuryCurvatureError("canonical calendar has no in-scope sessions")
    for row in calendar:
        timestamp = pd.Series([pd.Timestamp(str(row["session_id"]), tz="UTC")])
        row["temporal_role"] = str(_assign_roles(timestamp, card).iloc[0])
    frame.attrs["canonical_session_calendar"] = calendar


def _prior_two_factor_betas(
    belly: pd.Series,
    short: pd.Series,
    long: pd.Series,
    *,
    segments: pd.Series,
    window: int,
    minimum: int,
) -> tuple[pd.Series, pd.Series]:
    beta_short = pd.Series(np.nan, index=belly.index, dtype=float)
    beta_long = pd.Series(np.nan, index=belly.index, dtype=float)
    for indexes in segments.groupby(segments, sort=False).groups.values():
        idx = list(indexes)
        y = belly.loc[idx].shift(1)
        x1 = short.loc[idx].shift(1)
        x2 = long.loc[idx].shift(1)
        cov11 = x1.rolling(window, min_periods=minimum).var()
        cov22 = x2.rolling(window, min_periods=minimum).var()
        cov12 = x1.rolling(window, min_periods=minimum).cov(x2)
        cov1y = x1.rolling(window, min_periods=minimum).cov(y)
        cov2y = x2.rolling(window, min_periods=minimum).cov(y)
        determinant = cov11 * cov22 - cov12.pow(2)
        valid = determinant.abs().gt(1e-18)
        local_short = ((cov22 * cov1y - cov12 * cov2y) / determinant).where(valid)
        local_long = ((cov11 * cov2y - cov12 * cov1y) / determinant).where(valid)
        beta_short.loc[idx] = local_short.clip(-5.0, 5.0).to_numpy()
        beta_long.loc[idx] = local_long.clip(-5.0, 5.0).to_numpy()
    return beta_short, beta_long


def _prior_group_rolling(
    values: pd.Series,
    groups: pd.Series,
    *,
    window: int,
    minimum: int,
    statistic: str,
) -> pd.Series:
    output = pd.Series(np.nan, index=values.index, dtype=float)
    for indexes in groups.groupby(groups, sort=False).groups.values():
        idx = list(indexes)
        prior = values.loc[idx].shift(1)
        rolling = prior.rolling(window, min_periods=minimum)
        if statistic == "mean":
            result = rolling.mean()
        elif statistic == "std":
            result = rolling.std(ddof=0)
        else:
            raise TreasuryCurvatureError("unknown prior rolling statistic")
        output.loc[idx] = result.to_numpy()
    return output


def _role_scope_mask(timestamp: pd.Series, card: Mapping[str, Any]) -> pd.Series:
    utc = pd.to_datetime(timestamp, utc=True)
    selected = pd.Series(False, index=timestamp.index, dtype=bool)
    for row in card["chronological_roles"]:
        selected |= utc.ge(pd.Timestamp(row["start"], tz="UTC")) & utc.lt(
            pd.Timestamp(row["end"], tz="UTC")
        )
    return selected


def _assign_roles(timestamp: pd.Series, card: Mapping[str, Any]) -> pd.Series:
    values = pd.Series(index=timestamp.index, dtype="object")
    utc = pd.to_datetime(timestamp, utc=True)
    for row in card["chronological_roles"]:
        selected = utc.ge(pd.Timestamp(row["start"], tz="UTC")) & utc.lt(
            pd.Timestamp(row["end"], tz="UTC")
        )
        values.loc[selected] = str(row["role"])
    if values.isna().any():
        raise TreasuryCurvatureError("frozen temporal roles do not cover input")
    return values.astype(str)


def _path_is_contiguous(
    frame: pd.DataFrame, start: int, end: int, *, belly_root: str
) -> bool:
    if start < 0 or end < start or end >= len(frame):
        return False
    path = frame.loc[start:end]
    if len(path) != end - start + 1:
        return False
    if path["roll_unsafe"].astype(bool).any():
        return False
    if path["session_id"].astype(str).nunique() != 1:
        return False
    if path[f"{belly_root}_contract"].astype(str).nunique() != 1:
        return False
    delivery_columns = [
        column for column in path.columns if str(column).endswith("_delivery_month")
    ]
    if delivery_columns and not path[delivery_columns].astype(str).nunique(axis=1).eq(1).all():
        return False
    if len(path) > 1 and not path["timestamp"].diff().iloc[1:].eq(
        pd.Timedelta(minutes=1)
    ).all():
        return False
    return int(path["local_minute"].max()) <= SESSION_FLATTEN_MINUTE


def _local_minute(timestamp: pd.Series) -> pd.Series:
    local = pd.to_datetime(timestamp, utc=True).dt.tz_convert("America/Chicago")
    return local.dt.hour * 60 + local.dt.minute


def _session_ordinal(value: Any) -> int:
    return int(pd.Timestamp(str(value)).date().toordinal())


def _triangle(triangle_id: str) -> TriangleSpec:
    for triangle in TRIANGLES:
        if triangle.triangle_id == triangle_id:
            return triangle
    raise TreasuryCurvatureError(f"unknown frozen triangle: {triangle_id}")


def _tick_price(value: float, tick: float) -> float:
    if not math.isfinite(value) or tick <= 0.0:
        raise TreasuryCurvatureError("invalid tick-price input")
    return float(round(value / tick) * tick)


def _ceil_ticks(value: float, tick: float) -> float:
    if not math.isfinite(value) or value <= 0.0 or tick <= 0.0:
        raise TreasuryCurvatureError("invalid tick-distance input")
    return float(math.ceil(value / tick - 1e-12) * tick)


def _validate_decision_columns(columns: Sequence[Any]) -> None:
    offending = sorted(
        str(column)
        for column in columns
        if any(token in str(column).lower() for token in FORBIDDEN_DECISION_TOKENS)
    )
    if offending:
        raise TreasuryCurvatureError(
            f"future/outcome decision columns forbidden: {offending}"
        )


def _validate_card(card: Mapping[str, Any]) -> None:
    core = dict(card)
    claimed = str(core.pop("card_hash", ""))
    if not claimed or claimed == "CARD_HASH_PENDING" or _stable_hash(core) != claimed:
        raise TreasuryCurvatureError("decision-card self-hash drift")
    if card.get("selected_branch") != BRANCH_ID:
        raise TreasuryCurvatureError("decision-card branch drift")
    governance = card.get("governance", {})
    required_false = (
        "tier_q_allowed",
        "promotion_allowed",
        "q4_access_allowed",
        "data_purchase_allowed",
        "network_access_allowed",
        "broker_connection_allowed",
        "orders_allowed",
        "runtime_service_or_database_change_allowed",
        "registry_or_cemetery_write_allowed",
        "authoritative_writer_change_allowed",
    )
    if governance.get("tier_ceiling") != "E" or any(
        governance.get(key) is not False for key in required_false
    ):
        raise TreasuryCurvatureError("decision-card governance drift")
    account = card.get("account_frontier", {})
    if (
        tuple(account.get("diagnostic_horizons_trading_days", ()))
        != DIAGNOSTIC_HORIZONS
        or int(account.get("headline_gate_horizon_trading_days", 0))
        != HEADLINE_HORIZON
        or account.get("headline_passes_counted_once") is not True
        or tuple(float(row) for row in account.get("risk_fraction_of_initial_mll", ()))
        != RISK_FRACTIONS
    ):
        raise TreasuryCurvatureError("account frontier or headline horizon drift")
    roles = card.get("chronological_roles", ())
    if (
        tuple(row.get("role") for row in roles) != ROLES
        or roles[0].get("start") != "2023-01-03"
        or roles[-1].get("end") != "2024-10-01"
    ):
        raise TreasuryCurvatureError("chronological role or Q4 boundary drift")
    if len(frozen_rule_specs()) != int(
        card["smallest_decisive_falsification_experiment"]["maximum_rule_count"]
    ):
        raise TreasuryCurvatureError("manifest rule cardinality drift")
    if card["economic_run_authorization"]["required_cli_token"] != RUN_AUTHORIZATION:
        raise TreasuryCurvatureError("economic authorization token drift")
    causal = card.get("causal_contract", {})
    if (
        causal.get("three_leg_delivery_alignment")
        != "EXACT_SAME_DELIVERY_MONTH_AT_DECISION_AND_THROUGH_EXECUTION_PATH"
        or float(causal.get("stressed_all_in_cost_multiplier", 0.0)) != 1.5
        or int(causal.get("normal_slippage_ticks_per_side", 0)) != 1
        or int(causal.get("stressed_slippage_ticks_per_side", 0)) != 1
    ):
        raise TreasuryCurvatureError("delivery or exact stressed-cost contract drift")
    sizing = account.get("sizing_freeze", {})
    coverage = account.get("coverage_contract", {})
    if (
        sizing.get("role") != "DISCOVERY"
        or sizing.get("policy")
        != "STATIC_INTEGER_QUANTITY_FROM_INITIAL_MLL_FRACTION_AND_MAXIMUM_CAUSAL_DECLARED_STOP_RISK_OBSERVED_IN_DISCOVERY_ONLY"
        or sizing.get("validation_or_final_development_inputs_allowed") is not False
        or coverage.get("required_local_window_start") != "06:19"
        or coverage.get("required_local_window_end_inclusive") != "15:10"
        or coverage.get("incomplete_headline_start_in_denominator") is not False
        or card["frozen_gate"].get("candidate_power_preflight_required") is not True
    ):
        raise TreasuryCurvatureError("sizing, coverage or candidate-power contract drift")


def _audit_file_binding(project: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    artifact = _inside_file(project, str(row["path"]))
    digest = _sha256(artifact)
    if digest != str(row["sha256"]):
        raise TreasuryCurvatureError(f"frozen binding hash drift: {row['path']}")
    if "size_bytes" in row and artifact.stat().st_size != int(row["size_bytes"]):
        raise TreasuryCurvatureError(f"frozen binding size drift: {row['path']}")
    return {
        "path": str(artifact.relative_to(project)),
        "sha256": digest,
        "size_bytes": artifact.stat().st_size,
    }


def _read_only_cemetery_collision_count(project: Path) -> int:
    path = project / "mission/state/graveyard.db"
    if not path.is_file():
        return 0
    uri = f"file:{path}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM class_tombstones
            WHERE lower(mechanism_class) LIKE '%three_tenor_curvature%'
               OR lower(mechanism_class) LIKE '%curvature_to_belly%'
            """
        ).fetchone()
    count = int(row[0]) if row else 0
    if count:
        raise TreasuryCurvatureError("exact curvature branch is already tombstoned")
    return count


def _inside_file(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise TreasuryCurvatureError(f"path escapes repository: {value}") from exc
    if not resolved.is_file():
        raise TreasuryCurvatureError(f"required frozen input is absent: {resolved}")
    return resolved


def _inside_output_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise TreasuryCurvatureError(f"path escapes repository: {value}") from exc
    return resolved


def _write_rule_checkpoint(
    path: Path,
    *,
    contract_hash: str,
    rule_index: int,
    rule: CurvatureRule,
    decision: Mapping[str, Any],
) -> None:
    if str(decision.get("candidate_id")) != rule.rule_id:
        raise TreasuryCurvatureError("checkpoint candidate/rule binding drift")
    decision_core = dict(decision)
    claimed_candidate_hash = str(decision_core.pop("candidate_hash", ""))
    if not claimed_candidate_hash or _stable_hash(decision_core) != claimed_candidate_hash:
        raise TreasuryCurvatureError("checkpoint candidate payload is not self-consistent")
    core = {
        "schema": "hydra_treasury_curvature_rule_checkpoint_v1",
        "branch_id": BRANCH_ID,
        "contract_hash": str(contract_hash),
        "rule_index": int(rule_index),
        "rule_id": rule.rule_id,
        "rule_hash": _stable_hash(rule.to_dict()),
        "decision": dict(decision),
        "decision_hash": _stable_hash(decision),
    }
    payload = {**core, "checkpoint_hash": _stable_hash(core)}
    _atomic_json(path, payload)


def _read_rule_checkpoint(
    path: Path,
    *,
    contract_hash: str,
    rule_index: int,
    rule: CurvatureRule,
) -> dict[str, Any]:
    payload = _read_json(path)
    core = dict(payload)
    claimed_checkpoint_hash = str(core.pop("checkpoint_hash", ""))
    if not claimed_checkpoint_hash or _stable_hash(core) != claimed_checkpoint_hash:
        raise TreasuryCurvatureError("rule checkpoint self-hash drift")
    expected = {
        "schema": "hydra_treasury_curvature_rule_checkpoint_v1",
        "branch_id": BRANCH_ID,
        "contract_hash": str(contract_hash),
        "rule_index": int(rule_index),
        "rule_id": rule.rule_id,
        "rule_hash": _stable_hash(rule.to_dict()),
    }
    if any(core.get(key) != value for key, value in expected.items()):
        raise TreasuryCurvatureError("rule checkpoint contract binding drift")
    decision = core.get("decision")
    if not isinstance(decision, dict):
        raise TreasuryCurvatureError("rule checkpoint decision is not an object")
    if _stable_hash(decision) != str(core.get("decision_hash", "")):
        raise TreasuryCurvatureError("rule checkpoint decision hash drift")
    candidate_core = dict(decision)
    claimed_candidate_hash = str(candidate_core.pop("candidate_hash", ""))
    if (
        str(decision.get("candidate_id")) != rule.rule_id
        or not claimed_candidate_hash
        or _stable_hash(candidate_core) != claimed_candidate_hash
    ):
        raise TreasuryCurvatureError("rule checkpoint candidate integrity drift")
    return decision


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            allow_nan=False,
            default=str,
        )
        + "\n"
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TreasuryCurvatureError(f"invalid JSON binding: {path}") from exc
    if not isinstance(value, dict):
        raise TreasuryCurvatureError(f"JSON binding is not an object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _finite_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


__all__ = [
    "AUDIT_SCHEMA",
    "BRANCH_ID",
    "CONTROLS",
    "CurvatureRule",
    "DEFAULT_CARD",
    "HEADLINE_HORIZON",
    "RUN_AUTHORIZATION",
    "TRIANGLES",
    "TriangleSpec",
    "TreasuryCurvatureError",
    "audit_inputs",
    "causal_intent",
    "frozen_rule_specs",
    "prepare_curvature_features",
    "run_economic_tripwire",
]
