from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from hydra.atoms.atom_library import atom_signal, future_return
from hydra.atoms.schema import EdgeAtomHypothesis


ADVERSARIAL_POLICY_VERSION = "atom_adversarial_policy_v1"
MANDATORY_ATTACKS = (
    "delayed_signal",
    "sign_flipped_signal",
    "block_shuffled_signal",
    "event_time_jitter",
    "best_event_removed",
    "cost_stress",
    "momentum_baseline",
    "mean_reversion_baseline",
    "session_only_baseline",
    "volatility_only_baseline",
    "opportunity_count_matched_random",
)


@dataclass(frozen=True)
class AdversarialValidationResult:
    atom_id: str
    policy_version: str
    real_effect: float
    attacks_attempted: tuple[str, ...]
    attacks_survived: tuple[str, ...]
    attacks_failed: tuple[str, ...]
    effect_retention_after_best_event_removed: float
    simplest_competing_explanation: str
    passed: bool
    decision_reason: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def adversarial_validate_atom(
    atom: EdgeAtomHypothesis,
    frame: pd.DataFrame,
    *,
    seed: int = 0,
    cost_multiplier: float = 2.0,
) -> AdversarialValidationResult:
    subset = frame[frame["symbol"].isin(atom.target_markets)].copy()
    if subset.empty:
        return _empty(atom.atom_id, "target_market_missing")
    signal = atom_signal(subset, atom.feature_key, atom.expected_direction, str(atom.parameters.get("threshold", "moderate")))
    fwd = future_return(subset, atom.horizon_bars)
    aligned = pd.concat(
        [
            subset[["timestamp", "symbol", "close"]].reset_index(drop=True),
            signal.reset_index(drop=True).rename("signal"),
            fwd.reset_index(drop=True).rename("future_return"),
            subset.groupby("symbol")["close"].pct_change(atom.horizon_bars).reset_index(drop=True).rename("past_return"),
        ],
        axis=1,
    ).replace([np.inf, -np.inf], np.nan).dropna()
    events = aligned[aligned["signal"] != 0].copy()
    if len(events) < 50:
        return _empty(atom.atom_id, "valid_event_count_below_50")
    real_signed = np.sign(events["signal"].astype(float)) * events["future_return"].astype(float)
    real_effect = float(real_signed.mean())
    rng = np.random.default_rng(seed)
    details: dict[str, Any] = {"event_count": int(len(events)), "real_effect": real_effect}
    comparisons: dict[str, bool] = {}
    attack_effects: dict[str, float] = {}

    delayed = aligned.assign(signal=aligned.groupby("symbol")["signal"].shift(atom.horizon_bars).fillna(0))
    attack_effects["delayed_signal"] = _effect(delayed[delayed["signal"] != 0])
    comparisons["delayed_signal"] = abs(real_effect) > abs(attack_effects["delayed_signal"])

    flipped = events.assign(signal=-events["signal"])
    attack_effects["sign_flipped_signal"] = _effect(flipped)
    comparisons["sign_flipped_signal"] = real_effect * attack_effects["sign_flipped_signal"] < 0

    shuffled = events.copy()
    shuffled["signal"] = _block_shuffle(shuffled["signal"].to_numpy(), rng)
    attack_effects["block_shuffled_signal"] = _effect(shuffled)
    comparisons["block_shuffled_signal"] = abs(real_effect) > abs(attack_effects["block_shuffled_signal"])

    jittered = aligned.assign(signal=aligned.groupby("symbol")["signal"].shift(1).fillna(0))
    attack_effects["event_time_jitter"] = _effect(jittered[jittered["signal"] != 0])
    comparisons["event_time_jitter"] = abs(real_effect) > abs(attack_effects["event_time_jitter"])

    if len(real_signed) > 1:
        without_best = real_signed.drop(real_signed.abs().idxmax())
        best_removed_effect = float(without_best.mean())
    else:
        best_removed_effect = 0.0
    retention = abs(best_removed_effect) / max(abs(real_effect), 1e-12)
    attack_effects["best_event_removed"] = best_removed_effect
    comparisons["best_event_removed"] = retention >= 0.50 and np.sign(best_removed_effect) == np.sign(real_effect)

    cost_hurdle = max(float(atom.minimum_effect), abs(real_effect) * 0.10)
    attack_effects["cost_stress"] = abs(real_effect) - cost_multiplier * cost_hurdle
    comparisons["cost_stress"] = attack_effects["cost_stress"] > 0

    momentum = events.assign(signal=np.sign(events["past_return"]).replace(0, np.nan)).dropna()
    attack_effects["momentum_baseline"] = _effect(momentum)
    comparisons["momentum_baseline"] = abs(real_effect) > abs(attack_effects["momentum_baseline"])

    meanrev = events.assign(signal=-np.sign(events["past_return"]).replace(0, np.nan)).dropna()
    attack_effects["mean_reversion_baseline"] = _effect(meanrev)
    comparisons["mean_reversion_baseline"] = abs(real_effect) > abs(attack_effects["mean_reversion_baseline"])

    session = events.copy()
    session["session_hour"] = pd.to_datetime(session["timestamp"], utc=True).dt.hour
    session_effect = float(session.groupby("session_hour")["future_return"].mean().abs().mean())
    attack_effects["session_only_baseline"] = session_effect
    comparisons["session_only_baseline"] = abs(real_effect) > abs(session_effect)

    vol_baseline = aligned.copy()
    vol_baseline["vol_rank"] = vol_baseline.groupby("symbol")["past_return"].transform(lambda item: item.abs().rank(pct=True))
    vol_events = vol_baseline[vol_baseline["vol_rank"] >= 0.65].assign(signal=np.sign(vol_baseline["past_return"]).fillna(0))
    attack_effects["volatility_only_baseline"] = _effect(vol_events)
    comparisons["volatility_only_baseline"] = abs(real_effect) > abs(attack_effects["volatility_only_baseline"])

    random_events = aligned.sample(n=min(len(events), len(aligned)), random_state=int(seed)).copy()
    random_events["signal"] = rng.choice([-1, 1], size=len(random_events))
    attack_effects["opportunity_count_matched_random"] = _effect(random_events)
    comparisons["opportunity_count_matched_random"] = abs(real_effect) > abs(attack_effects["opportunity_count_matched_random"])

    survived = tuple(name for name in MANDATORY_ATTACKS if comparisons.get(name, False))
    failed = tuple(name for name in MANDATORY_ATTACKS if not comparisons.get(name, False))
    details["attack_effects"] = attack_effects
    details["attack_passes"] = comparisons
    if failed:
        explanation = "mandatory_attack_explains_or_matches_effect:" + ",".join(failed[:4])
    else:
        explanation = "no_mandatory_adversarial_attack_explained_effect"
    return AdversarialValidationResult(
        atom_id=atom.atom_id,
        policy_version=ADVERSARIAL_POLICY_VERSION,
        real_effect=real_effect,
        attacks_attempted=MANDATORY_ATTACKS,
        attacks_survived=survived,
        attacks_failed=failed,
        effect_retention_after_best_event_removed=float(retention),
        simplest_competing_explanation=explanation,
        passed=not failed,
        decision_reason="adversarial_policy_passed" if not failed else explanation,
        details=details,
    )


def _empty(atom_id: str, reason: str) -> AdversarialValidationResult:
    return AdversarialValidationResult(
        atom_id=atom_id,
        policy_version=ADVERSARIAL_POLICY_VERSION,
        real_effect=0.0,
        attacks_attempted=MANDATORY_ATTACKS,
        attacks_survived=(),
        attacks_failed=MANDATORY_ATTACKS,
        effect_retention_after_best_event_removed=0.0,
        simplest_competing_explanation="not_testable",
        passed=False,
        decision_reason=reason,
        details={"reason": reason},
    )


def _effect(events: pd.DataFrame) -> float:
    if events.empty:
        return 0.0
    signed = np.sign(events["signal"].astype(float)) * events["future_return"].astype(float)
    return float(signed.replace([np.inf, -np.inf], np.nan).dropna().mean() or 0.0)


def _block_shuffle(values: np.ndarray, rng: np.random.Generator, block_size: int = 20) -> np.ndarray:
    blocks = [values[i : i + block_size] for i in range(0, len(values), block_size)]
    order = np.arange(len(blocks))
    rng.shuffle(order)
    return np.concatenate([blocks[int(i)] for i in order])[: len(values)]

