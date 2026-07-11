"""Past-only defensive policy replay with matched deactivation controls."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from hydra.portfolio.account_contribution import (
    AccountPathMetrics,
    AccountReplayConfig,
    _replay_normalized_account,
    normalize_trade_ledger,
)


MLL_PROTECTION_POLICY_VERSION = "mll_protection_role_v1"


class MllProtectionError(ValueError):
    pass


@dataclass(frozen=True)
class MatchedDeactivationControlSummary:
    control_count: int
    control_scores: tuple[float, ...]
    one_sided_p_value: float
    seed: int
    matching: str
    deactivation_count_by_match_group: dict[str, int]
    operational_policy: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MllProtectionEvaluation:
    policy_id: str
    base: AccountPathMetrics
    protected: AccountPathMetrics
    decision_count: int
    deactivation_decision_count: int
    removed_trade_count: int
    avoided_net_loss: float
    foregone_net_profit: float
    min_mll_buffer_delta: float
    maximum_drawdown_reduction: float
    shared_loss_days_reduction: int
    target_velocity_delta: float
    consistency_margin_delta: float
    mll_survival_improved: bool
    hard_risk_violation: bool
    observed_defensive_score: float
    controls: MatchedDeactivationControlSummary
    past_only_policy_verified: bool
    research_status: str
    inherited_status: bool = False
    promotion_eligible: bool = False
    paper_shadow_ready: bool = False
    policy_version: str = MLL_PROTECTION_POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_DECISION_REQUIRED = {
    "decision_id",
    "event_session_id",
    "decision_timestamp",
    "available_timestamp",
    "source_window_end",
    "deactivate",
    "match_group",
    "policy_version",
}


def normalize_deactivation_decisions(
    decisions: pd.DataFrame | Iterable[Mapping[str, Any]],
) -> pd.DataFrame:
    frame = decisions.copy(deep=True) if isinstance(decisions, pd.DataFrame) else pd.DataFrame(list(decisions))
    missing = sorted(_DECISION_REQUIRED - set(frame.columns))
    if missing:
        raise MllProtectionError(f"Deactivation decisions missing columns: {missing}")
    frame["decision_id"] = frame["decision_id"].astype(str)
    frame["event_session_id"] = frame["event_session_id"].astype(str)
    frame["match_group"] = frame["match_group"].astype(str)
    frame["policy_version"] = frame["policy_version"].astype(str)
    if frame["decision_id"].duplicated().any():
        raise MllProtectionError("Duplicate decision_id")
    if frame["policy_version"].str.strip().eq("").any():
        raise MllProtectionError("Every decision needs an immutable policy_version")
    for column in ("decision_timestamp", "available_timestamp", "source_window_end"):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="raise")
    if (frame["available_timestamp"] > frame["decision_timestamp"]).any():
        raise MllProtectionError("Decision uses information available in the future")
    if (frame["source_window_end"] > frame["decision_timestamp"]).any():
        raise MllProtectionError("Decision source window ends in the future")
    if frame["deactivate"].isna().any():
        raise MllProtectionError("deactivate cannot be null")
    frame["deactivate"] = frame["deactivate"].astype(bool)
    return frame.sort_values(
        ["decision_timestamp", "event_session_id", "decision_id"], kind="mergesort"
    ).reset_index(drop=True)


def apply_deactivation_policy(
    base_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    decisions: pd.DataFrame | Iterable[Mapping[str, Any]],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Apply decisions only to trades entered at or after each decision."""

    policy = normalize_deactivation_decisions(decisions)
    normalized_base = {
        str(key): normalize_trade_ledger(value, strategy_id=str(key))
        for key, value in sorted(base_ledgers.items())
    }
    return _apply_normalized_deactivation(normalized_base, policy)


def _apply_normalized_deactivation(
    base_ledgers: Mapping[str, pd.DataFrame], policy: pd.DataFrame
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    active = policy[policy["deactivate"]]
    output: dict[str, pd.DataFrame] = {}
    removed: list[pd.DataFrame] = []
    for strategy_id, ledger in sorted(base_ledgers.items()):
        remove_mask = pd.Series(False, index=ledger.index)
        for session_id, session_decisions in active.groupby("event_session_id", sort=False):
            cutoff = session_decisions["decision_timestamp"].min()
            remove_mask |= ledger["event_session_id"].eq(str(session_id)) & ledger[
                "entry_timestamp"
            ].ge(cutoff)
        if remove_mask.any():
            removed.append(ledger.loc[remove_mask].copy())
        output[str(strategy_id)] = ledger.loc[~remove_mask].reset_index(drop=True)
    removed_frame = (
        pd.concat(removed, ignore_index=True)
        if removed
        else pd.DataFrame(columns=next(iter(output.values())).columns if output else [])
    )
    return output, removed_frame


def evaluate_mll_protection_role(
    policy_id: str,
    base_ledgers: Mapping[str, pd.DataFrame | Iterable[Mapping[str, Any]]],
    decisions: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    control_count: int = 255,
    seed: int = 0,
    config: AccountReplayConfig | None = None,
) -> MllProtectionEvaluation:
    if control_count < 1:
        raise MllProtectionError("control_count must be positive")
    cfg = config or AccountReplayConfig()
    policy = normalize_deactivation_decisions(decisions)
    normalized_base = {
        str(key): normalize_trade_ledger(value, strategy_id=str(key))
        for key, value in sorted(base_ledgers.items())
    }
    base = _replay_normalized_account(normalized_base, cfg)
    protected_ledgers, removed = _apply_normalized_deactivation(normalized_base, policy)
    protected = _replay_normalized_account(protected_ledgers, cfg)
    observed_score = defensive_effect_score(base, protected, cfg)
    # The matched-null statistic is the avoided-loss contribution of the exact
    # deactivation schedule.  Generate every requested permutation in one
    # matrix while preserving the deactivation count inside each preregistered
    # match group.  This removes thousands of identical pandas account replays
    # without changing the randomization test or its sample size.
    rng = np.random.default_rng(seed)
    decision_sessions = policy["event_session_id"].astype(str).to_numpy()
    combined_events = pd.concat(normalized_base.values(), ignore_index=True)
    daily_net = combined_events.groupby("event_session_id", sort=False)["net_pnl"].sum()
    decision_net = np.asarray(
        [float(daily_net.get(session, 0.0)) for session in decision_sessions],
        dtype=float,
    )
    masks = np.zeros((control_count, len(policy)), dtype=bool)
    for _group, raw_indices in policy.groupby("match_group", sort=True).groups.items():
        indices = np.asarray(list(raw_indices), dtype=int)
        selected = int(policy.loc[indices, "deactivate"].sum())
        if selected <= 0:
            continue
        if selected >= len(indices):
            masks[:, indices] = True
            continue
        random_order = np.argsort(rng.random((control_count, len(indices))), axis=1)
        chosen = random_order[:, :selected]
        row_index = np.arange(control_count)[:, None]
        masks[row_index, indices[chosen]] = True

    def matched_avoided_loss_utility(mask: np.ndarray) -> np.ndarray:
        removed = mask.astype(float) * decision_net
        avoided_loss = -removed.sum(axis=1) / max(cfg.mll_distance, 1.0)
        losing = decision_net < 0.0
        winning = decision_net > 0.0
        loss_capture = (
            (mask[:, losing].sum(axis=1) / max(int(losing.sum()), 1))
            if bool(losing.any())
            else np.zeros(mask.shape[0])
        )
        foregone = (
            removed[:, winning].sum(axis=1)
            / max(float(decision_net[losing].__abs__().sum()), 1.0)
            if bool(winning.any())
            else np.zeros(mask.shape[0])
        )
        return avoided_loss + 0.50 * loss_capture - 0.25 * foregone

    observed_mask = policy["deactivate"].to_numpy(dtype=bool)[None, :]
    observed_null_statistic = float(matched_avoided_loss_utility(observed_mask)[0])
    control_scores_array = matched_avoided_loss_utility(masks)
    control_scores = [float(value) for value in control_scores_array]
    exceed = int(np.count_nonzero(control_scores_array >= observed_null_statistic - 1e-15))
    controls = MatchedDeactivationControlSummary(
        control_count=control_count,
        control_scores=tuple(float(score) for score in control_scores),
        one_sided_p_value=float((1 + exceed) / (1 + control_count)),
        seed=seed,
        matching=(
            "vectorized_within_preregistered_match_group_preserving_deactivation_count_"
            "matched_avoided_loss_utility_v2"
        ),
        deactivation_count_by_match_group={
            str(group): int(values["deactivate"].sum())
            for group, values in policy.groupby("match_group", sort=True)
        },
    )
    hard_risk = bool(
        (not base.mll_breached and protected.mll_breached)
        or (not base.contract_limit_breached and protected.contract_limit_breached)
    )
    drawdown_reduction = base.maximum_drawdown - protected.maximum_drawdown
    buffer_delta = protected.min_mll_buffer - base.min_mll_buffer
    shared_loss_reduction = base.shared_loss_days - protected.shared_loss_days
    if hard_risk:
        status = "DEFENSIVE_ROLE_HARD_RISK_REJECTED"
    elif (
        len(removed) > 0
        and observed_score > 0.0
        and drawdown_reduction > 0.0
        and buffer_delta >= 0.0
        and shared_loss_reduction >= 0
        and controls.one_sided_p_value <= 0.10
    ):
        status = "DEFENSIVE_ROLE_RESEARCH_CANDIDATE"
    else:
        status = "INSUFFICIENT_DEFENSIVE_ROLE_EVIDENCE"
    removed_net = float(removed["net_pnl"].sum()) if len(removed) else 0.0
    return MllProtectionEvaluation(
        policy_id=str(policy_id),
        base=base,
        protected=protected,
        decision_count=int(len(policy)),
        deactivation_decision_count=int(policy["deactivate"].sum()),
        removed_trade_count=int(len(removed)),
        avoided_net_loss=float(max(0.0, -removed_net)),
        foregone_net_profit=float(max(0.0, removed_net)),
        min_mll_buffer_delta=float(buffer_delta),
        maximum_drawdown_reduction=float(drawdown_reduction),
        shared_loss_days_reduction=int(shared_loss_reduction),
        target_velocity_delta=float(
            protected.target_velocity_dollars_per_day
            - base.target_velocity_dollars_per_day
        ),
        consistency_margin_delta=float(
            protected.consistency_margin - base.consistency_margin
        ),
        mll_survival_improved=bool(base.mll_breached and not protected.mll_breached),
        hard_risk_violation=hard_risk,
        observed_defensive_score=float(observed_score),
        controls=controls,
        past_only_policy_verified=True,
        research_status=status,
    )


def defensive_effect_score(
    base: AccountPathMetrics,
    protected: AccountPathMetrics,
    config: AccountReplayConfig | None = None,
) -> float:
    cfg = config or AccountReplayConfig()
    buffer_delta = (protected.min_mll_buffer - base.min_mll_buffer) / cfg.mll_distance
    drawdown_delta = (base.maximum_drawdown - protected.maximum_drawdown) / cfg.mll_distance
    shared_loss_delta = (base.shared_loss_days - protected.shared_loss_days) / max(
        base.shared_loss_days, 1
    )
    velocity_scale = max(abs(base.target_velocity_dollars_per_day), 1.0)
    velocity_delta = (
        protected.target_velocity_dollars_per_day
        - base.target_velocity_dollars_per_day
    ) / velocity_scale
    consistency_delta = protected.consistency_margin - base.consistency_margin
    score = (
        0.35 * buffer_delta
        + 0.25 * drawdown_delta
        + 0.20 * shared_loss_delta
        + 0.10 * velocity_delta
        + 0.10 * consistency_delta
    )
    if base.mll_breached and not protected.mll_breached:
        score += 1.0
    if not base.mll_breached and protected.mll_breached:
        score -= 2.0
    return float(score)
