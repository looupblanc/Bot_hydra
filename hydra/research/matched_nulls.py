from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from hydra.research.null_models import block_shuffle, delayed_null
from hydra.research.opportunity_matched_nulls import opportunity_count_matched_signal


@dataclass(frozen=True)
class MatchedNullResult:
    signal_id: str
    event_count: int
    real_effect: float
    random_matched_effect: float
    block_shuffled_effect: float
    delayed_effect: float
    sign_flipped_effect: float
    momentum_baseline_effect: float
    mean_reversion_baseline_effect: float
    session_only_effect: float
    volatility_only_effect: float
    opportunity_count_effect: float
    beats_all_required: bool
    probability_beats_matched_null: float
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_matched_nulls(
    frame: pd.DataFrame,
    signal: pd.Series,
    forward_return: pd.Series,
    *,
    signal_id: str,
    seed: int = 0,
    max_events: int = 500,
) -> MatchedNullResult:
    aligned = pd.concat(
        [
            signal.rename("signal").fillna(0).astype(float),
            forward_return.rename("forward").astype(float),
            frame.get("close", pd.Series(index=frame.index, dtype=float)).rename("close"),
        ],
        axis=1,
    ).replace([np.inf, -np.inf], np.nan).dropna()
    if aligned.empty:
        return _empty(signal_id)
    aligned = _cap_signal_events(aligned, max_events=max_events)
    real = _effect(aligned["signal"], aligned["forward"])
    matched_signal = opportunity_count_matched_signal(frame.loc[aligned.index], aligned["signal"], seed=seed)
    shuffled_signal = block_shuffle(aligned["signal"], block_size=120, seed=seed + 1)
    delayed_signal = delayed_null(aligned["signal"], 60).fillna(0)
    momentum = np.sign(aligned["close"].pct_change(20)).fillna(0)
    mean_reversion = -momentum
    session_only = _session_only_signal(frame.loc[aligned.index], aligned["signal"])
    volatility_only = _volatility_only_signal(frame.loc[aligned.index], aligned["signal"], seed=seed + 2)
    null_effects = {
        "random_matched_effect": _effect(matched_signal.reindex(aligned.index).fillna(0), aligned["forward"]),
        "block_shuffled_effect": _effect(shuffled_signal, aligned["forward"]),
        "delayed_effect": _effect(delayed_signal, aligned["forward"]),
        "sign_flipped_effect": _effect(-aligned["signal"], aligned["forward"]),
        "momentum_baseline_effect": _effect(momentum, aligned["forward"]),
        "mean_reversion_baseline_effect": _effect(mean_reversion, aligned["forward"]),
        "session_only_effect": _effect(session_only, aligned["forward"]),
        "volatility_only_effect": _effect(volatility_only, aligned["forward"]),
        "opportunity_count_effect": _effect(matched_signal.reindex(aligned.index).fillna(0), aligned["forward"]),
    }
    required = [
        abs(real) > abs(null_effects["random_matched_effect"]),
        abs(real) > abs(null_effects["block_shuffled_effect"]),
        abs(real) > abs(null_effects["delayed_effect"]),
        abs(real) > abs(null_effects["momentum_baseline_effect"]),
        abs(real) > abs(null_effects["mean_reversion_baseline_effect"]),
    ]
    beats = bool(all(required))
    probability = sum(abs(real) > abs(value) for value in null_effects.values()) / max(len(null_effects), 1)
    return MatchedNullResult(
        signal_id=signal_id,
        event_count=int(aligned["signal"].ne(0).sum()),
        real_effect=float(real),
        beats_all_required=beats,
        probability_beats_matched_null=float(probability),
        status="MATCHED_NULL_BEATEN" if beats else "FALSIFIED",
        **{key: float(value) for key, value in null_effects.items()},
    )


def multiple_testing_adjusted_alpha(alpha: float, effective_trials: int) -> float:
    return float(alpha) / max(int(effective_trials), 1)


def _effect(signal: pd.Series, forward: pd.Series) -> float:
    event = signal.astype(float).ne(0)
    if not event.any():
        return 0.0
    return float((np.sign(signal[event]) * forward[event]).mean())


def _cap_signal_events(aligned: pd.DataFrame, *, max_events: int) -> pd.DataFrame:
    events = aligned["signal"].astype(float).ne(0)
    if events.sum() <= max_events:
        return aligned
    keep_events = list(aligned.index[events][:max_events])
    keep_nonevents = list(aligned.index[~events])
    return aligned.loc[keep_events + keep_nonevents].sort_index()


def _session_only_signal(frame: pd.DataFrame, signal: pd.Series) -> pd.Series:
    if "timestamp" not in frame.columns:
        return pd.Series(0, index=signal.index)
    hours = pd.to_datetime(frame["timestamp"], utc=True).dt.hour
    active_hour = hours[signal.ne(0)].mode()
    out = pd.Series(0, index=signal.index, dtype=int)
    if len(active_hour):
        out.loc[hours == int(active_hour.iloc[0])] = 1
    return out


def _volatility_only_signal(frame: pd.DataFrame, signal: pd.Series, seed: int) -> pd.Series:
    close = frame.get("close", pd.Series(index=signal.index, dtype=float))
    vol = close.pct_change().abs().rolling(60, min_periods=20).mean()
    threshold = vol.quantile(0.75)
    out = pd.Series(0, index=signal.index, dtype=int)
    direction = 1 if signal.sum() >= 0 else -1
    out.loc[vol >= threshold] = direction
    return out


def _empty(signal_id: str) -> MatchedNullResult:
    return MatchedNullResult(signal_id, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, False, 0.0, "INSUFFICIENT_EVIDENCE")
