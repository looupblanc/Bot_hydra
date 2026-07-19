"""Bounded causal primitives for the CL front/second term-structure tripwire.

This module contains no downloader, database writer, controller hook, account
promotion, or broker capability.  It prepares decision-time source features
and freezes the eight-rule lattice consumed by a later exact account replay.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.economic_evolution.schema import stable_hash


FORBIDDEN_DECISION_TOKENS = (
    "future",
    "forward",
    "lead",
    "next_close",
    "next_open",
    "outcome",
    "mfe",
    "mae",
    "target_reached",
)


@dataclass(frozen=True)
class CLTermStructureRule:
    rule_id: str
    mechanism: str
    lookback_minutes: int
    holding_minutes: int
    trigger_score: float
    target_r_multiple: float
    stop_r_multiple: float
    execution_market: str = "MCL.c.0"
    fill_policy: str = "NEXT_TRADABLE_OPEN"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def frozen_rule_specs() -> tuple[CLTermStructureRule, ...]:
    """Return the complete preregistered lattice; cardinality is always eight."""

    rules: list[CLTermStructureRule] = []
    for mechanism in ("BASIS_RESIDUAL_CONTINUATION", "BASIS_RESIDUAL_REVERSION"):
        for lookback in (15, 60):
            for holding in (30, 120):
                rules.append(
                    CLTermStructureRule(
                        rule_id=f"cl_term_v1:{mechanism}:lb{lookback}:h{holding}",
                        mechanism=mechanism,
                        lookback_minutes=lookback,
                        holding_minutes=holding,
                        trigger_score=2.0,
                        target_r_multiple=2.5 if holding == 30 else 3.0,
                        stop_r_multiple=1.0,
                    )
                )
    if len(rules) != 8 or len({stable_hash(rule.to_dict()) for rule in rules}) != 8:
        raise RuntimeError("CL term-structure rule lattice drift")
    return tuple(rules)


def validate_decision_columns(columns: Sequence[str]) -> None:
    lowered = [str(column).lower() for column in columns]
    offending = sorted(
        column
        for column in lowered
        if any(token in column for token in FORBIDDEN_DECISION_TOKENS)
    )
    if offending:
        raise ValueError(f"future/outcome decision columns forbidden: {offending}")


def prepare_causal_source_features(
    front: pd.DataFrame,
    second: pd.DataFrame,
    *,
    lookback_minutes: int,
    beta_window_bars: int = 60,
    normalization_sessions: int = 20,
) -> pd.DataFrame:
    """Align completed CL rank bars and derive decision-time-only features.

    Inputs require timestamp, close, available_at, session_id, and roll_unsafe.
    The same timestamp must represent the completed source bar in both ranks.
    Robust normalization uses only earlier sessions at the same Chicago minute.
    """

    if lookback_minutes not in {15, 60}:
        raise ValueError("lookback is outside the frozen lattice")
    required = {"timestamp", "close", "available_at", "session_id", "roll_unsafe"}
    for label, frame in (("front", front), ("second", second)):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{label} source columns missing: {sorted(missing)}")
        if frame["timestamp"].duplicated().any():
            raise ValueError(f"duplicate {label} source timestamp")

    left = _rank_frame(front, "front")
    right = _rank_frame(second, "second")
    merged = left.merge(right, on="timestamp", how="inner", validate="one_to_one")
    merged = merged.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    if merged.empty:
        return merged
    if not (merged["session_id_front"].astype(str) == merged["session_id_second"].astype(str)).all():
        raise ValueError("front/second session identity mismatch")
    merged["available_at"] = pd.concat(
        [merged["available_at_front"], merged["available_at_second"]], axis=1
    ).max(axis=1)
    if (merged["available_at"] < merged["timestamp"]).any():
        raise ValueError("source availability precedes event timestamp")

    timestamp = pd.to_datetime(merged["timestamp"], utc=True)
    chicago = timestamp.dt.tz_convert("America/Chicago")
    local_minute = chicago.dt.strftime("%H:%M")
    pair = (
        merged["rank_contract_front"].astype(str)
        + "|"
        + merged["rank_contract_second"].astype(str)
    )
    discontinuity = timestamp.diff().ne(pd.Timedelta(minutes=1))
    discontinuity |= merged["session_id_front"].astype(str).ne(
        merged["session_id_front"].astype(str).shift()
    )
    discontinuity |= pair.ne(pair.shift())
    causal_segment = discontinuity.cumsum().astype("int64")

    front_log = np.log(merged["close_front"].astype(float))
    second_log = np.log(merged["close_second"].astype(float))
    basis = front_log - second_log
    front_return = _segment_diff(front_log, causal_segment, lookback_minutes)
    second_return = _segment_diff(second_log, causal_segment, lookback_minutes)
    basis_change = _segment_diff(basis, causal_segment, lookback_minutes)
    beta = _prior_segment_beta(
        front_log,
        second_log,
        segment=causal_segment,
        window=beta_window_bars,
    )
    residual = front_return - beta * second_return
    prior_volatility = _prior_segment_volatility(
        front_log, segment=causal_segment, window=beta_window_bars
    )
    days_front = pd.to_numeric(
        merged["days_to_delivery_front"], errors="coerce"
    ).astype(float)
    days_second = pd.to_numeric(
        merged["days_to_delivery_second"], errors="coerce"
    ).astype(float)
    roll_adjusted_basis = _prior_roll_distance_residual(
        basis,
        days_front=days_front,
        local_minute=local_minute,
        prior_sessions=max(40, normalization_sessions * 2),
    )
    roll_adjusted_innovation = _segment_diff(
        roll_adjusted_basis, causal_segment, lookback_minutes
    )
    score_input = roll_adjusted_innovation.where(
        roll_adjusted_innovation.notna(), basis_change
    )
    robust_score = _prior_same_minute_robust_score(
        score_input,
        local_minute=local_minute,
        session_id=merged["session_id_front"].astype(str),
        prior_sessions=normalization_sessions,
    )
    output = pd.DataFrame(
        {
            "timestamp": timestamp,
            "available_at": pd.to_datetime(merged["available_at"], utc=True),
            "session_id": merged["session_id_front"].astype(str),
            "local_minute_chicago": local_minute,
            "close_front": merged["close_front"].astype(float),
            "close_second": merged["close_second"].astype(float),
            "log_front_second_basis": basis,
            "basis_change": basis_change,
            "roll_distance_adjusted_basis": roll_adjusted_basis,
            "roll_distance_adjusted_basis_innovation": roll_adjusted_innovation,
            "front_days_to_delivery": days_front,
            "second_days_to_delivery": days_second,
            "delivery_tenor_gap_days": days_second - days_front,
            "rolling_beta_prior_only": beta,
            "front_residual_return": residual,
            "front_prior_realized_volatility": prior_volatility,
            "current_spread_state": roll_adjusted_basis,
            "basis_robust_score_prior_sessions": robust_score,
            "rank_pair": pair,
            "causal_segment_id": causal_segment,
            "roll_unsafe": merged["roll_unsafe_front"].astype(bool)
            | merged["roll_unsafe_second"].astype(bool),
        }
    )
    output["decision_eligible"] = (
        ~output["roll_unsafe"]
        & output["basis_robust_score_prior_sessions"].notna()
        & output["front_residual_return"].notna()
        & (output["available_at"] <= output["timestamp"] + pd.Timedelta(minutes=1))
    )
    if days_front.notna().any() or days_second.notna().any():
        output["decision_eligible"] &= (
            days_front.notna()
            & days_second.notna()
            & days_front.ge(2.0)
            & days_second.gt(days_front)
            & output["roll_distance_adjusted_basis_innovation"].notna()
            & output["front_prior_realized_volatility"].notna()
            & output["current_spread_state"].notna()
        )
    validate_decision_columns(output.columns)
    return output


def causal_intent(feature_row: Mapping[str, Any], rule: CLTermStructureRule) -> int:
    """Return -1, 0, or +1 using only the frozen decision fields."""

    validate_decision_columns(feature_row.keys())
    if not bool(feature_row.get("decision_eligible", False)):
        return 0
    score = float(feature_row["basis_robust_score_prior_sessions"])
    residual = float(feature_row["front_residual_return"])
    if not np.isfinite(score) or not np.isfinite(residual) or abs(score) < rule.trigger_score:
        return 0
    source_direction = int(np.sign(residual if residual != 0.0 else score))
    if rule.mechanism == "BASIS_RESIDUAL_CONTINUATION":
        return source_direction
    if rule.mechanism == "BASIS_RESIDUAL_REVERSION":
        return -source_direction
    raise ValueError("unknown frozen mechanism")


def next_tradable_open(
    target_bars: pd.DataFrame, *, decision_bar_timestamp: Any
) -> dict[str, Any] | None:
    """Select the first target bar strictly after the completed decision bar."""

    required = {"timestamp", "open"}
    missing = required - set(target_bars.columns)
    if missing:
        raise ValueError(f"target columns missing: {sorted(missing)}")
    bars = target_bars.copy()
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    bars = bars.sort_values("timestamp", kind="mergesort")
    decision = pd.Timestamp(decision_bar_timestamp)
    decision = decision.tz_localize("UTC") if decision.tzinfo is None else decision.tz_convert("UTC")
    eligible = bars.loc[bars["timestamp"] > decision]
    if eligible.empty:
        return None
    row = eligible.iloc[0]
    return {"fill_time": row["timestamp"], "fill_price": float(row["open"])}


def _rank_frame(frame: pd.DataFrame, rank: str) -> pd.DataFrame:
    columns = ["timestamp", "close", "available_at", "session_id", "roll_unsafe"]
    value = frame[columns].copy()
    value["rank_contract"] = (
        frame["rank_contract"].astype(str)
        if "rank_contract" in frame
        else f"{rank}:UNSPECIFIED"
    )
    value["days_to_delivery"] = (
        pd.to_numeric(frame["days_to_delivery"], errors="coerce")
        if "days_to_delivery" in frame
        else np.nan
    )
    value["timestamp"] = pd.to_datetime(value["timestamp"], utc=True)
    value["available_at"] = pd.to_datetime(value["available_at"], utc=True)
    return value.rename(
        columns={column: f"{column}_{rank}" for column in value.columns if column != "timestamp"}
    )


def _segment_diff(
    values: pd.Series, segment: pd.Series, periods: int
) -> pd.Series:
    return values.groupby(segment, sort=False).diff(int(periods))


def _prior_segment_beta(
    front_log: pd.Series,
    second_log: pd.Series,
    *,
    segment: pd.Series,
    window: int,
) -> pd.Series:
    same = segment.eq(segment.shift(1))
    one_front = front_log.diff().where(same)
    one_second = second_log.diff().where(same)
    prior_front = one_front.shift(1).where(same)
    prior_second = one_second.shift(1).where(same)
    # Require a full contiguous window.  This makes the global vectorized
    # rolling calculation exactly segment-local without a slow Python loop or
    # any carry across a missing minute/session/contract boundary.
    run_position = pd.Series(segment).groupby(segment, sort=False).cumcount()
    covariance = prior_front.rolling(window, min_periods=window).cov(prior_second)
    variance = prior_second.rolling(window, min_periods=window).var()
    return (covariance / variance.replace(0.0, np.nan)).clip(-3.0, 3.0).where(
        run_position >= window + 1
    )


def _prior_segment_volatility(
    values: pd.Series, *, segment: pd.Series, window: int
) -> pd.Series:
    same = segment.eq(segment.shift(1))
    one = values.diff().where(same)
    prior = one.shift(1).where(same)
    run_position = pd.Series(segment).groupby(segment, sort=False).cumcount()
    return prior.rolling(window, min_periods=window).std().where(
        run_position >= window + 1
    )


def _prior_roll_distance_residual(
    basis: pd.Series,
    *,
    days_front: pd.Series,
    local_minute: pd.Series,
    prior_sessions: int,
) -> pd.Series:
    """Remove the causal, known time-to-delivery basis state.

    At each Chicago minute the regression uses only earlier sessions.  The
    current observation is never included in its expected-basis estimate.
    """

    output = pd.Series(np.nan, index=basis.index, dtype=float)
    frame = pd.DataFrame(
        {
            "basis": basis.astype(float),
            "days": days_front.astype(float),
            "minute": local_minute.astype(str),
        }
    )
    minimum = max(20, prior_sessions // 2)
    for indexes in frame.groupby("minute", sort=False).groups.values():
        positions = list(indexes)
        y = frame.loc[positions, "basis"]
        x = frame.loc[positions, "days"]
        prior_x = x.shift(1)
        prior_y = y.shift(1)
        mean_x = prior_x.rolling(prior_sessions, min_periods=minimum).mean()
        mean_y = prior_y.rolling(prior_sessions, min_periods=minimum).mean()
        covariance = prior_x.rolling(prior_sessions, min_periods=minimum).cov(prior_y)
        variance = prior_x.rolling(prior_sessions, min_periods=minimum).var()
        slope = (covariance / variance.replace(0.0, np.nan)).clip(-0.25, 0.25)
        expected = mean_y + slope * (x - mean_x)
        output.loc[positions] = (y - expected).to_numpy()
    return output


def _prior_same_minute_robust_score(
    values: pd.Series,
    *,
    local_minute: pd.Series,
    session_id: pd.Series,
    prior_sessions: int,
) -> pd.Series:
    frame = pd.DataFrame(
        {"value": values.astype(float), "minute": local_minute.astype(str), "session": session_id.astype(str)}
    )
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    for _minute, indexes in frame.groupby("minute", sort=False).groups.items():
        positions = list(indexes)
        sample = frame.loc[positions, "value"]
        prior = sample.shift(1)
        median = prior.rolling(prior_sessions, min_periods=max(5, prior_sessions // 2)).median()
        q25 = prior.rolling(prior_sessions, min_periods=max(5, prior_sessions // 2)).quantile(0.25)
        q75 = prior.rolling(prior_sessions, min_periods=max(5, prior_sessions // 2)).quantile(0.75)
        scale = (q75 - q25).replace(0.0, np.nan)
        result.loc[positions] = ((sample - median) / scale).to_numpy()
    return result
