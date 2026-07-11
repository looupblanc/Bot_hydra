from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pytest

from hydra.portfolio.account_contribution import (
    AccountContributionError,
    aggregate_combine_passer_utility,
    aggregate_defensive_account_utility,
    aggregate_xfa_payout_utility,
    compare_account_contribution,
    matched_random_inclusion_controls,
    normalize_trade_ledger,
    replay_shared_account,
)
from hydra.portfolio.mll_protection_role import (
    MllProtectionError,
    evaluate_mll_protection_role,
    normalize_deactivation_decisions,
)
from hydra.portfolio.portfolio_role_search import search_portfolio_roles
from hydra.portfolio.strategy_role import (
    StrategyPool,
    StrategyRole,
    classify_strategy_role,
)


def _ledger(
    strategy_id: str,
    pnls: list[float],
    *,
    start: str = "2024-01-02",
    mae_scale: float = 1.0,
    with_provenance: bool = True,
) -> pd.DataFrame:
    first = pd.Timestamp(start, tz="UTC")
    rows = []
    for index, pnl in enumerate(pnls):
        entry = first + timedelta(days=index, hours=14)
        row = {
            "strategy_id": strategy_id,
            "trade_id": f"{strategy_id}-{index}",
            "event_session_id": entry.date().isoformat(),
            "entry_timestamp": entry,
            "exit_timestamp": entry + timedelta(hours=1),
            "net_pnl": float(pnl),
            "mae_dollars": float(min(-10.0, pnl * mae_scale)),
            "cost": 2.0,
            "contracts": 1,
            "underlying": strategy_id,
            "side": 1.0,
        }
        if with_provenance:
            row["source_bar_close"] = entry - timedelta(minutes=1)
            row["availability_timestamp"] = entry - timedelta(seconds=30)
        rows.append(row)
    return pd.DataFrame(rows)


def _decisions(pnls: list[float], *, start: str = "2024-01-02") -> pd.DataFrame:
    first = pd.Timestamp(start, tz="UTC")
    rows = []
    for index, pnl in enumerate(pnls):
        session = first + timedelta(days=index)
        decision = session + timedelta(hours=13, minutes=55)
        rows.append(
            {
                "decision_id": f"risk-{index}",
                "event_session_id": session.date().isoformat(),
                "decision_timestamp": decision,
                "available_timestamp": decision - timedelta(seconds=10),
                "source_window_end": decision - timedelta(minutes=1),
                "deactivate": pnl < 0,
                "match_group": "same_preregistered_opportunity_set",
                "policy_version": "frozen_risk_policy_v1",
            }
        )
    return pd.DataFrame(rows)


def test_role_classification_selects_one_phase_pool_and_never_promotes() -> None:
    combine = classify_strategy_role(
        {
            "candidate_id": "alpha",
            "portfolio_role": "state_conditioned_alpha",
            "target_pool": "COMBINE_PASSER_POOL",
            "validation_status": "PAPER_SHADOW_READY",  # deliberately ignored
            "net_pnl": 1_000_000.0,
        }
    )
    xfa = classify_strategy_role(
        {
            "candidate_id": "payout",
            "portfolio_role": "alpha",
            "target_pool": "XFA_PAYOUT_POOL",
        }
    )
    defensive = classify_strategy_role(
        {
            "candidate_id": "guard",
            "mechanism_family": "past_only_mll_protection_deactivation",
        }
    )

    assert combine.role is StrategyRole.ALPHA
    assert combine.target_pool is StrategyPool.COMBINE_PASSER_POOL
    assert "target_before_mll" in combine.required_evidence
    assert xfa.target_pool is StrategyPool.XFA_PAYOUT_POOL
    assert "payout_cycles_before_ruin" in xfa.required_evidence
    assert defensive.role is StrategyRole.DEFENSIVE
    assert defensive.target_pool is StrategyPool.DEFENSIVE_ACCOUNT_POOL
    assert not combine.inherited_status
    assert not combine.promotion_eligible
    assert combine.evidence_status == "UNASSESSED"


def test_shared_account_replays_joint_mae_shared_losses_and_phase_utilities() -> None:
    left = _ledger("left", [-100.0, 200.0])
    right = _ledger("right", [-200.0, 100.0])
    left.loc[0, "mae_dollars"] = -400.0
    right.loc[0, "mae_dollars"] = -500.0

    metrics = replay_shared_account({"left": left, "right": right})

    assert metrics.trade_count == 4
    assert metrics.maximum_simultaneous_contracts == 2
    assert metrics.shared_loss_days == 1
    assert metrics.min_mll_buffer == pytest.approx(3_600.0)
    assert metrics.target_velocity_dollars_per_day == 0.0
    assert metrics.combine_utility != metrics.xfa_utility
    assert metrics.xfa_utility != metrics.defensive_utility
    assert metrics.utility_for(StrategyPool.COMBINE_PASSER_POOL) == metrics.combine_utility
    assert metrics.utility_for(StrategyPool.XFA_PAYOUT_POOL) == metrics.xfa_utility


def test_trade_ledger_rejects_future_feature_availability() -> None:
    ledger = _ledger("leaky", [10.0])
    ledger.loc[0, "availability_timestamp"] = ledger.loc[0, "entry_timestamp"] + timedelta(seconds=1)

    with pytest.raises(AccountContributionError, match="Future information"):
        normalize_trade_ledger(ledger)


def test_matched_inclusion_controls_are_deterministic_and_non_operational() -> None:
    base = {"base": _ledger("base", [-500.0, 50.0, -400.0, 50.0, 50.0, 50.0])}
    candidate = _ledger("candidate", [300.0, 0.0, 300.0, 0.0, 0.0, 0.0])

    first = matched_random_inclusion_controls(
        base,
        "candidate",
        candidate,
        target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
        control_count=31,
        seed=71,
    )
    second = matched_random_inclusion_controls(
        base,
        "candidate",
        candidate,
        target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
        control_count=31,
        seed=71,
    )

    assert first == second
    assert first.control_count == 31
    assert not first.operational_policy
    assert 0.0 < first.one_sided_p_value <= 1.0


def test_mll_protection_uses_past_only_policy_and_beats_matched_controls() -> None:
    pnls = [-1_200.0 if index in {2, 6, 10, 14, 18} else 100.0 for index in range(20)]
    base = {
        "alpha_a": _ledger("alpha_a", pnls, mae_scale=1.0),
        "alpha_b": _ledger("alpha_b", pnls, mae_scale=1.0),
    }
    decisions = _decisions(pnls)

    result = evaluate_mll_protection_role(
        "risk_guard_v1",
        base,
        decisions,
        control_count=63,
        seed=19,
    )

    assert result.past_only_policy_verified
    assert result.removed_trade_count == 10
    assert result.avoided_net_loss == 12_000.0
    assert result.shared_loss_days_reduction == 5
    assert result.maximum_drawdown_reduction > 0.0
    assert result.min_mll_buffer_delta > 0.0
    assert result.controls.one_sided_p_value <= 0.10
    assert result.research_status == "DEFENSIVE_ROLE_RESEARCH_CANDIDATE"
    assert not result.controls.operational_policy
    assert not result.promotion_eligible
    assert not result.paper_shadow_ready


def test_phase_utility_aggregates_use_only_the_selected_account_objective() -> None:
    profitable = replay_shared_account(
        {"fast": _ledger("fast", [2_000.0] * 6)}
    )
    slower = replay_shared_account(
        {"slow": _ledger("slow", [1_000.0] * 10)}
    )
    combine = aggregate_combine_passer_utility([profitable, slower])
    xfa = aggregate_xfa_payout_utility([profitable, slower])
    contribution = compare_account_contribution(
        {"base": _ledger("base", [-300.0, 100.0, -200.0, 100.0])},
        "diversifier",
        _ledger("diversifier", [250.0, 0.0, 150.0, 0.0]),
        target_pool=StrategyPool.DEFENSIVE_ACCOUNT_POOL,
    )
    defensive = aggregate_defensive_account_utility([contribution])

    assert combine.path_count == 2
    assert combine.target_before_mll_probability == 1.0
    assert combine.median_time_to_target_days is not None
    assert xfa.expected_payout_cycles_before_ruin > 0.0
    assert xfa.mean_qualifying_day_frequency == 1.0
    assert xfa.mll_survival_probability == 1.0
    assert defensive.path_count == 1
    assert defensive.mean_drawdown_reduction >= 0.0
    assert not combine.promotion_eligible
    assert not xfa.promotion_eligible
    assert not defensive.promotion_eligible


def test_mll_policy_rejects_future_source_window() -> None:
    decisions = _decisions([100.0, -100.0])
    decisions.loc[1, "source_window_end"] = decisions.loc[1, "decision_timestamp"] + timedelta(minutes=1)

    with pytest.raises(MllProtectionError, match="source window ends in the future"):
        normalize_deactivation_decisions(decisions)


def test_role_search_keeps_pool_evidence_separate_and_emits_no_promotion() -> None:
    base_pnls = [-900.0, 100.0, -700.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    base = {"base": _ledger("base", base_pnls)}
    candidates = {
        "combine_child": _ledger("combine_child", [500.0, 20.0, 500.0, 20.0, 20.0, 20.0, 20.0, 20.0]),
        "xfa_child": _ledger("xfa_child", [200.0] * 8),
        "risk_model": _ledger("risk_model", [0.0] * 8),
    }
    specifications = {
        "combine_child": {"portfolio_role": "alpha", "target_pool": "COMBINE_PASSER_POOL"},
        "xfa_child": {"portfolio_role": "alpha", "target_pool": "XFA_PAYOUT_POOL"},
        "risk_model": {"portfolio_role": "defensive"},
    }

    result = search_portfolio_roles(
        base,
        candidates,
        specifications,
        control_count=15,
        seed=11,
    )
    by_id = {row.candidate_id: row for row in result.candidates}

    assert by_id["combine_child"].optimized_utility_name == "combine_utility"
    assert by_id["xfa_child"].optimized_utility_name == "xfa_utility"
    assert by_id["risk_model"].research_status == "PAST_ONLY_DEACTIVATION_POLICY_REQUIRED"
    assert all(not row.promotion_eligible for row in result.candidates)
    assert all(not row.shadow_research_active for row in result.candidates)
    assert all(not row.paper_shadow_ready for row in result.candidates)
    assert result.paper_shadow_ready == 0
    assert result.inherited_statuses == 0
    assert set(result.pool_counts) == {
        "COMBINE_PASSER_POOL",
        "XFA_PAYOUT_POOL",
        "DEFENSIVE_ACCOUNT_POOL",
    }
    serialized = result.to_dict()
    assert serialized["paper_shadow_ready"] == 0
    assert serialized["candidates"][0]["classification"]["role"] in {
        "alpha",
        "defensive",
    }
