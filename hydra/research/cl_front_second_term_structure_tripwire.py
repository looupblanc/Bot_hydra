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

    front_log = np.log(merged["close_front"].astype(float))
    second_log = np.log(merged["close_second"].astype(float))
    front_return = front_log.diff(lookback_minutes)
    second_return = second_log.diff(lookback_minutes)
    one_front = front_log.diff()
    one_second = second_log.diff()
    prior_covariance = one_front.shift(1).rolling(beta_window_bars, min_periods=30).cov(one_second.shift(1))
    prior_variance = one_second.shift(1).rolling(beta_window_bars, min_periods=30).var()
    beta = (prior_covariance / prior_variance.replace(0.0, np.nan)).clip(-3.0, 3.0)
    basis = front_log - second_log
    basis_change = basis.diff(lookback_minutes)
    residual = front_return - beta * second_return

    timestamp = pd.to_datetime(merged["timestamp"], utc=True)
    chicago = timestamp.dt.tz_convert("America/Chicago")
    local_minute = chicago.dt.strftime("%H:%M")
    robust_score = _prior_same_minute_robust_score(
        basis_change,
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
            "rolling_beta_prior_only": beta,
            "front_residual_return": residual,
            "basis_robust_score_prior_sessions": robust_score,
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
    value = frame[list({"timestamp", "close", "available_at", "session_id", "roll_unsafe"})].copy()
    value["timestamp"] = pd.to_datetime(value["timestamp"], utc=True)
    value["available_at"] = pd.to_datetime(value["available_at"], utc=True)
    return value.rename(
        columns={column: f"{column}_{rank}" for column in value.columns if column != "timestamp"}
    )


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
