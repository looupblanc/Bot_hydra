from __future__ import annotations

from hydra.propfirm.xfa_post_payout import (
    DllScenario,
    FrontierRole,
    PayoutAmountMode,
    RecoveryCondition,
    RequestTiming,
    XfaPostPayoutPolicy,
)
from hydra.research.pnl_state_xfa_survival_frontier import (
    _dominates,
    _payout_request,
    _survival_summary,
)


def _policy(role: FrontierRole, risk: float = 0.25) -> XfaPostPayoutPolicy:
    values = {
        FrontierRole.HARVEST: (
            RequestTiming.EARLIEST_ELIGIBLE_CLIPPED,
            PayoutAmountMode.OFFICIAL_MAX,
            0.0,
            RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
        ),
        FrontierRole.BALANCED: (
            RequestTiming.FULL_TARGET_BUFFER_SAFE,
            PayoutAmountMode.HALF_ALLOWED,
            1_000.0,
            RecoveryCondition.RECOVER_TO_LAST_PRE_PAYOUT_BALANCE,
        ),
        FrontierRole.LONGEVITY: (
            RequestTiming.FULL_TARGET_BUFFER_SAFE,
            PayoutAmountMode.MINIMUM_125,
            2_000.0,
            RecoveryCondition.HOLD_REDUCED_RISK,
        ),
    }
    timing, amount, buffer, recovery = values[role]
    return XfaPostPayoutPolicy(
        book_id="book",
        path="XFA_STANDARD",
        role=role,
        request_timing=timing,
        payout_amount_mode=amount,
        retained_buffer_usd=buffer,
        post_payout_risk_scale=risk,
        recovery_condition=recovery,
        dll_scenario=DllScenario.NO_DLL,
    )


def test_harvest_matches_official_maximum_request() -> None:
    result = _payout_request(
        balance=4_000.0,
        floor=0.0,
        eligible=True,
        official_cap=3_000.0,
        minimum=125.0,
        payout_fraction=0.5,
        policy=_policy(FrontierRole.HARVEST, 1.0),
    )
    assert result["balance_fraction_limit"] == 2_000.0
    assert result["gross_payout"] == 2_000.0


def test_longgevity_waits_until_retained_buffer_is_legal() -> None:
    too_early = _payout_request(
        balance=2_050.0,
        floor=0.0,
        eligible=True,
        official_cap=3_000.0,
        minimum=125.0,
        payout_fraction=0.5,
        policy=_policy(FrontierRole.LONGEVITY),
    )
    assert too_early["gross_payout"] == 0.0
    eligible = _payout_request(
        balance=2_500.0,
        floor=0.0,
        eligible=True,
        official_cap=3_000.0,
        minimum=125.0,
        payout_fraction=0.5,
        policy=_policy(FrontierRole.LONGEVITY),
    )
    assert eligible["gross_payout"] == 125.0


def test_survival_keeps_censoring_out_of_evaluable_denominator() -> None:
    rows = [
        {
            "first_payout_day": 2,
            "post_payout_observed_days": 40,
            "terminal": "DATA_CENSORED",
        },
        {
            "first_payout_day": 2,
            "post_payout_observed_days": 20,
            "terminal": "MLL_BREACHED",
        },
        {
            "first_payout_day": 2,
            "post_payout_observed_days": 20,
            "terminal": "DATA_CENSORED",
        },
    ]
    summary = _survival_summary(rows)["checkpoints"]["30"]
    assert summary["survived_count"] == 1
    assert summary["failed_before_checkpoint_count"] == 1
    assert summary["data_censored_before_checkpoint_count"] == 1
    assert summary["survival_rate_among_evaluable"] == 0.5


def test_pareto_dominance_requires_one_strict_improvement() -> None:
    assert _dominates((2.0, 1.0), (1.0, 1.0))
    assert not _dominates((1.0, 1.0), (1.0, 1.0))
    assert not _dominates((2.0, 0.0), (1.0, 1.0))
