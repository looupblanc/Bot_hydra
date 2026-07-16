from __future__ import annotations

from copy import deepcopy

import pytest

from hydra.production.active_risk_decision_report import LifecyclePathAccumulator
from hydra.propfirm.xfa_payout_events import (
    CANONICAL_PAYOUT_EVENT_SCHEMA,
    CanonicalPayoutEvent,
    XfaPayoutEventError,
    canonical_payout_totals,
    reconcile_payout_path,
    validate_unique_payout_events,
)


def _executed_row(
    *,
    path: str,
    session_day: int = 20260701,
    opening: float = 0.0,
    day_pnl: float = 10_000.0,
    cycle: int = 1,
) -> dict[str, object]:
    pre = opening + day_pnl
    cap = 5_000.0 if path == "XFA_STANDARD" else 6_000.0
    gross = min(pre * 0.50, cap)
    closing = pre - gross
    return {
        "session_day": session_day,
        "opening_balance": opening,
        "day_pnl": day_pnl,
        "closing_balance": closing,
        "mll_floor_open": -4_500.0,
        "mll_floor_close": 0.0,
        "mll_before_payout": min(0.0, max(-4_500.0, pre - 4_500.0)),
        "mll_after_payout": 0.0,
        "payout_eligible": True,
        "payout_requested": True,
        "gross_payout": gross,
        "trader_net_payout": gross * 0.90,
        "payout_cycles": cycle,
        "post_payout_mll_locked_at_zero": True,
        "terminal": None,
    }


def _path(path: str, rows: list[dict[str, object]]) -> dict[str, object]:
    executed = [row for row in rows if row.get("payout_requested")]
    return {
        "path": path,
        "daily_ledger": rows,
        "gross_payout": sum(float(row["gross_payout"]) for row in executed),
        "trader_net_payout": sum(
            float(row["trader_net_payout"]) for row in executed
        ),
        "payout_cycles": len(executed),
        "first_payout_day": (
            rows.index(executed[0]) + 1 if executed else None
        ),
    }


def _reconcile(path: dict[str, object], **kwargs: object):
    return reconcile_payout_path(
        path,
        policy_id="policy-a",
        scenario="STRESSED_1_5X",
        combine_start_id=19541,
        **kwargs,
    )


def test_canonical_event_separates_standard_and_consistency_alternatives() -> None:
    standard = _reconcile(
        _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    )
    consistency = _reconcile(
        _path(
            "XFA_CONSISTENCY",
            [_executed_row(path="XFA_CONSISTENCY", day_pnl=20_000.0)],
        )
    )

    standard_event = standard.payout_events[0]
    consistency_event = consistency.payout_events[0]
    assert standard_event.schema == CANONICAL_PAYOUT_EVENT_SCHEMA
    assert standard_event.path_key != consistency_event.path_key
    assert standard_event.gross_payout_request == 5_000.0
    assert consistency_event.gross_payout_request == 6_000.0
    assert standard_event.event_fingerprint != consistency_event.event_fingerprint

    totals = canonical_payout_totals(
        [standard_event, consistency_event]
    )
    assert totals["alternatives_are_mutually_exclusive"] is True
    assert totals["paths"]["XFA_STANDARD"]["first_payout_count"] == 1
    assert totals["paths"]["XFA_CONSISTENCY"]["first_payout_count"] == 1


def test_first_payout_event_is_unique_per_path_and_cycle() -> None:
    result = _reconcile(
        _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    )
    event = result.payout_events[0]

    with pytest.raises(XfaPayoutEventError, match="duplicate canonical"):
        validate_unique_payout_events([event, event])


def test_reset_marker_does_not_create_an_additional_first_payout() -> None:
    first = _executed_row(path="XFA_STANDARD")
    reset_only = {
        "session_day": 20260702,
        "opening_balance": 5_000.0,
        "day_pnl": 0.0,
        "closing_balance": 5_000.0,
        "mll_floor_open": 0.0,
        "mll_floor_close": 0.0,
        "payout_eligible": False,
        "payout_requested": False,
        "gross_payout": 0.0,
        "trader_net_payout": 0.0,
        "payout_cycles": 1,
        "post_payout_mll_locked_at_zero": True,
        "payout_reset_marker": False,
        "terminal": None,
    }
    result = _reconcile(_path("XFA_STANDARD", [first, reset_only]))

    assert len(result.payout_events) == 1
    assert result.first_payout_count == 1
    assert result.payout_events[0].payout_cycle == 1


def test_split_post_balance_and_mll_reset_are_reconciled() -> None:
    result = _reconcile(
        _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    )
    event = result.payout_events[0]

    assert event.payout_split == 0.90
    assert event.trader_net_payout == 4_500.0
    assert event.pre_payout_balance == 10_000.0
    assert event.post_payout_balance == 5_000.0
    assert event.mll_after_payout == 0.0
    assert event.reset_marker is True

    legacy_floor_row = _executed_row(
        path="XFA_STANDARD",
        day_pnl=1_000.0,
    )
    legacy_floor_row.pop("mll_before_payout")
    legacy_floor_result = _reconcile(
        _path("XFA_STANDARD", [legacy_floor_row])
    )
    assert legacy_floor_result.payout_events[0].mll_before_payout == -3_500.0
    assert legacy_floor_result.payout_events[0].mll_after_payout == 0.0

    bad_split = _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    bad_split["daily_ledger"][0]["trader_net_payout"] = 4_400.0
    bad_split["trader_net_payout"] = 4_400.0
    with pytest.raises(XfaPayoutEventError, match="split-adjusted"):
        _reconcile(bad_split)

    bad_balance = _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    bad_balance["daily_ledger"][0]["closing_balance"] = 5_001.0
    with pytest.raises(XfaPayoutEventError, match="post-payout balance"):
        _reconcile(bad_balance)

    bad_reset = _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    bad_reset["daily_ledger"][0]["mll_floor_close"] = -1.0
    with pytest.raises(XfaPayoutEventError, match="MLL after payout"):
        _reconcile(bad_reset)


def test_minimum_marker_is_not_an_event_and_exact_bridge_is_audited() -> None:
    executed = {
        **_executed_row(
            path="XFA_STANDARD",
            opening=0.0,
            day_pnl=1_176.99,
        ),
        "gross_payout": 588.495,
        "trader_net_payout": 529.6455,
        "closing_balance": 588.495,
    }
    subminimum = {
        "session_day": 20260702,
        "opening_balance": 588.495,
        "day_pnl": -391.445,
        "closing_balance": 197.05,
        "mll_floor_open": 0.0,
        "mll_floor_close": 0.0,
        "payout_eligible": True,
        "payout_requested": False,
        # Historical marker defect: candidate amount, not an execution.
        "gross_payout": 98.525,
        "trader_net_payout": 0.0,
        "payout_cycles": 1,
        "post_payout_mll_locked_at_zero": True,
        "terminal": None,
    }
    path = _path("XFA_STANDARD", [executed, subminimum])

    with pytest.raises(
        XfaPayoutEventError,
        match="non-executed XFA payout carried a gross amount",
    ):
        _reconcile(path)

    result = _reconcile(path, allow_legacy_subminimum_marker=True)
    assert len(result.payout_events) == 1
    assert result.legacy_subminimum_marker_count == 1
    assert result.legacy_subminimum_marker_gross == pytest.approx(98.525)
    assert result.canonical_gross_payout == pytest.approx(588.495)
    assert sum(
        float(row["gross_payout"]) for row in path["daily_ledger"]
    ) == pytest.approx(687.020)
    assert 687.020 - 98.525 == pytest.approx(588.495)

    accumulator = LifecyclePathAccumulator()
    accumulator.add_path(path, payout_reconciliation=result)
    report_value = accumulator.to_dict()
    assert report_value["first_payouts"] == 1
    assert report_value["payout_cycles"] == 1
    assert report_value["canonical_payout_event_count"] == 1
    assert report_value["legacy_subminimum_marker_count"] == 1
    assert report_value["legacy_subminimum_marker_gross"] == pytest.approx(98.525)
    assert report_value["trader_net_payout"] == pytest.approx(529.6455)


def test_payout_cap_and_aggregate_to_event_summary_are_enforced() -> None:
    consistency = _path(
        "XFA_CONSISTENCY",
        [_executed_row(path="XFA_CONSISTENCY", day_pnl=20_000.0)],
    )
    result = _reconcile(consistency)
    event = result.payout_events[0]
    assert event.balance_fraction_limit == 10_000.0
    assert event.account_size_payout_cap == 6_000.0
    assert event.gross_payout_request == 6_000.0

    summary_drift = deepcopy(consistency)
    summary_drift["gross_payout"] = 6_001.0
    with pytest.raises(XfaPayoutEventError, match="path gross payout summary"):
        _reconcile(summary_drift)


def test_canonical_event_fingerprint_detects_mutation() -> None:
    result = _reconcile(
        _path("XFA_STANDARD", [_executed_row(path="XFA_STANDARD")])
    )
    event = result.payout_events[0]
    mutated = CanonicalPayoutEvent(
        **{**event.to_dict(), "trader_net_payout": 4_499.0}
    )

    with pytest.raises(XfaPayoutEventError):
        mutated.verify()
