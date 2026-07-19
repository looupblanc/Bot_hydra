from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hydra.production.autonomous_exact_replay import (
    DEFAULT_RULE_SNAPSHOT,
    _load_rule_snapshot,
)
from hydra.research import curve_relative_value_tripwire as curve


ROOT = Path(__file__).resolve().parents[1]


def _toy_pair(pair: curve.PairSpec, *, sessions: int = 15, bars: int = 240) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    left_base = {
        "ZT": 102.0,
        "ZF": 108.0,
        "ZN": 112.0,
        "ZB": 120.0,
    }[pair.shorter_root]
    right_base = {
        "ZF": 108.0,
        "ZN": 112.0,
        "TN": 114.0,
        "UB": 126.0,
    }[pair.longer_root]
    for session_index, day in enumerate(
        pd.bdate_range("2024-01-02", periods=sessions)
    ):
        local = pd.date_range(
            pd.Timestamp(day.date(), tz="America/Chicago") + pd.Timedelta(hours=7),
            periods=bars,
            freq="min",
        )
        index = np.arange(bars, dtype=float) + session_index * bars
        common = 0.004 * index + 0.025 * np.sin(index / 31.0)
        relative = 0.08 * np.sin(index / 11.0) + 0.03 * np.sin(index / 3.0)
        left_close = left_base + common + relative
        right_close = right_base + common * 0.75 - relative * 0.35
        delivery = "202403" if session_index < sessions // 2 else "202406"
        suffix = "H24" if session_index < sessions // 2 else "M24"
        frame = pd.DataFrame(
            {
                "timestamp": local.tz_convert("UTC"),
                "session_id": str(day.date()),
                f"{pair.shorter_root}_contract": f"{pair.shorter_root}{suffix}",
                f"{pair.shorter_root}_delivery_month": delivery,
                f"{pair.longer_root}_contract": f"{pair.longer_root}{suffix}",
                f"{pair.longer_root}_delivery_month": delivery,
                f"{pair.shorter_root}_open": left_close - 0.002,
                f"{pair.shorter_root}_high": left_close + 0.008,
                f"{pair.shorter_root}_low": left_close - 0.008,
                f"{pair.shorter_root}_close": left_close,
                f"{pair.longer_root}_open": right_close - 0.002,
                f"{pair.longer_root}_high": right_close + 0.008,
                f"{pair.longer_root}_low": right_close - 0.008,
                f"{pair.longer_root}_close": right_close,
            }
        )
        rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def _toy_inventory() -> dict[str, pd.DataFrame]:
    return {pair.pair_id: _toy_pair(pair) for pair in curve.PAIR_SPECS}


def _execution_fixture(pair: curve.PairSpec, *, rows: int = 10) -> pd.DataFrame:
    timestamp = pd.date_range("2024-01-02 14:00:00Z", periods=rows, freq="min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "session_id": ["2024-01-02"] * rows,
            "roll_segment": np.ones(rows, dtype=int),
            "local_minute": np.arange(8 * 60, 8 * 60 + rows, dtype=int),
            "session_day": np.full(
                rows, pd.Timestamp("2024-01-02").date().toordinal(), dtype=int
            ),
            "relative_z": np.full(rows, 2.5),
            "left_dollar_sigma_60": np.ones(rows),
            "right_dollar_sigma_60": np.ones(rows),
        }
    )
    for root in (pair.shorter_root, pair.longer_root):
        for field in ("open", "high", "low", "close"):
            frame[f"{root}_{field}"] = np.full(rows, 100.0)
    return frame


def test_missing_bound_input_fails_closed_deterministically() -> None:
    first = curve.build_curve_relative_value_tripwire(ROOT)
    second = curve.build_curve_relative_value_tripwire(ROOT)
    assert first == second
    assert first["status"] == curve.WAITING_STATUS
    assert first["decision"] == curve.WAITING_STATUS
    assert first["economic_result_created"] is False
    assert first["data_purchase_count"] == 0
    assert first["q4_access_count_delta"] == 0
    assert first["official_cost_receipt"] == curve.OFFICIAL_COST_RECEIPT
    assert first["required_input_contract"]["required_roots"] == [
        "TN",
        "UB",
        "ZB",
        "ZF",
        "ZN",
        "ZT",
    ]


def test_frozen_inventory_is_six_treasuries_and_sixteen_rules() -> None:
    rules = curve.frozen_rules()
    assert len(rules) == 16
    assert len({row.rule_id for row in rules}) == 16
    assert {root for pair in curve.PAIR_SPECS for root in (pair.shorter_root, pair.longer_root)} == {
        "ZT",
        "ZF",
        "ZN",
        "TN",
        "ZB",
        "UB",
    }
    assert all(
        curve.TREASURY_SPECS[pair.shorter_root].tenor_years
        < curve.TREASURY_SPECS[pair.longer_root].tenor_years
        for pair in curve.PAIR_SPECS
    )
    assert {row.mechanism for row in rules} == {"REVERSION", "CONTINUATION"}
    assert {row.session_role for row in rules} == {"OPEN", "MID"}


def test_treasury_commission_snapshot_is_conservative_and_self_hashed() -> None:
    expected_applied = {
        "ZT": 2.34,
        "ZF": 2.34,
        "ZN": 2.60,
        "TN": 2.64,
        "ZB": 2.78,
        "UB": 2.94,
    }
    assert {
        root: spec.round_turn_commission_usd
        for root, spec in curve.TREASURY_SPECS.items()
    } == expected_applied
    snapshot = curve.treasury_commission_snapshot()
    assert snapshot["status"] == (
        "OFFICIAL_SOURCE_CONTENT_CONFLICT_CONSERVATIVE_UPPER_BOUND"
    )
    assert snapshot["exact_current_fee_claimed"] is False
    assert snapshot["source_url"] == curve.COMMISSION_SOURCE_URL
    assert snapshot["retrieved_at_utc"] == curve.COMMISSION_RETRIEVED_AT_UTC
    assert snapshot["direct_rendered_round_turn_usd"] == {
        "ZT": 2.32,
        "ZF": 2.32,
        "ZN": 2.58,
        "TN": 2.62,
        "ZB": 2.76,
        "UB": 2.92,
    }
    assert snapshot["applied_round_turn_usd"] == expected_applied
    core = dict(snapshot)
    claimed = core.pop("provenance_hash")
    assert curve._stable_hash(core) == claimed

    waiting = curve.build_curve_relative_value_tripwire(ROOT)
    assert waiting["treasury_commission_snapshot"] == snapshot


def test_roll_delivery_sync_excludes_mismatch_and_resets_features() -> None:
    pair = curve.PAIR_SPECS[0]
    raw = _toy_pair(pair)
    mismatch_index = len(raw) // 3
    raw.loc[mismatch_index, f"{pair.longer_root}_delivery_month"] = "202409"
    prepared, audit = curve.prepare_pair_frame(raw, pair)
    assert audit["delivery_mismatch_rows_excluded"] == 1
    assert len(prepared) == len(raw) - 1
    assert audit["roll_segment_count"] >= 2
    assert audit["features_reset_at_roll"] is True
    assert audit["trades_may_cross_roll"] is False
    assert not prepared[
        f"{pair.shorter_root}_delivery_month"
    ].astype(str).ne(
        prepared[f"{pair.longer_root}_delivery_month"].astype(str)
    ).any()


def test_future_outcome_columns_are_physically_rejected() -> None:
    pair = curve.PAIR_SPECS[0]
    raw = _toy_pair(pair)
    raw["future_outcome_label"] = 1
    with pytest.raises(curve.CurveTripwireError, match="physically forbidden"):
        curve.prepare_pair_frame(raw, pair)


def test_trade_uses_next_tradable_open_not_decision_bar_open() -> None:
    pair = curve.PAIR_SPECS[0]
    prepared, _ = curve.prepare_pair_frame(_toy_pair(pair), pair)
    rules, _ = _load_rule_snapshot(ROOT / DEFAULT_RULE_SNAPSHOT)
    rule = next(
        row
        for row in curve.frozen_rules()
        if row.pair_id == pair.pair_id and row.mechanism == "REVERSION"
    )
    # A 150K risk budget admits one complete 1:4 DV01/volatility-normalised
    # toy group.  Index 500 is safely inside a session and after warm-up.
    signal_index = 500
    original = curve._generate_gross_events(
        prepared,
        pair=pair,
        rule=rule,
        account_rule=rules["150K"],
        signal_indices=(signal_index,),
        control="PRIMARY",
    )
    assert len(original) == 1

    changed_decision_open = prepared.copy()
    changed_decision_open.loc[
        signal_index, f"{pair.shorter_root}_open"
    ] += 10.0
    decision_bar_ignored = curve._generate_gross_events(
        changed_decision_open,
        pair=pair,
        rule=rule,
        account_rule=rules["150K"],
        signal_indices=(signal_index,),
        control="PRIMARY",
    )
    assert decision_bar_ignored[0].gross_pnl == pytest.approx(original[0].gross_pnl)

    changed_next_open = prepared.copy()
    changed_next_open.loc[
        signal_index + 1, f"{pair.shorter_root}_open"
    ] += 0.25
    next_open_used = curve._generate_gross_events(
        changed_next_open,
        pair=pair,
        rule=rule,
        account_rule=rules["150K"],
        signal_indices=(signal_index,),
        control="PRIMARY",
    )
    assert next_open_used[0].gross_pnl != pytest.approx(original[0].gross_pnl)
    assert original[0].decision_ns == int(prepared.at[signal_index, "timestamp"].value)
    assert original[0].exit_ns > original[0].decision_ns


def test_toy_tripwire_has_complete_account_matrix_and_deterministic_hash() -> None:
    result = curve.build_curve_relative_value_tripwire(
        ROOT,
        pair_frames=_toy_inventory(),
    )
    assert result["status"] == "COMPLETE_DEVELOPMENT_TRIPWIRE"
    assert result["evidence_role"] == curve.EVIDENCE_ROLE
    assert len(result["candidate_results"]) == 16
    assert result["data_purchase_count"] == 0
    assert result["q4_access_count_delta"] == 0
    assert result["broker_connections"] == 0
    assert result["orders"] == 0
    for candidate in result["candidate_results"]:
        assert {row["account_label"] for row in candidate["account_matrix"]} == {
            "50K",
            "100K",
            "150K",
        }
        for account in candidate["account_matrix"]:
            assert set(account["controls"]) == set(curve.CONTROLS)
            primary = account["controls"]["PRIMARY"]
            assert set(primary) == {"NORMAL", "STRESSED_1_5X"}
            assert set(primary["NORMAL"]) == set(curve.ROLES)
            assert set(primary["NORMAL"]["FINAL_DEVELOPMENT"]) == {
                "5",
                "10",
                "20",
            }
    core = dict(result)
    claimed = core.pop("result_hash")
    assert curve._stable_hash(core) == claimed


def test_execution_view_preserves_causal_exit_boundaries() -> None:
    pair = curve.PAIR_SPECS[0]
    base = _execution_fixture(pair)
    left = pair.shorter_root
    exit_kwargs = {
        "pair": pair,
        "direction": 1,
        "quantity_a": 1,
        "quantity_b": 1,
        "entry_index": 2,
        "stop_usd": 1_000.0,
        "target_usd": 2_000.0,
        "holding_minutes": 10,
    }

    view = curve._pair_execution_view(base, pair)
    assert int(view.timestamp_ns[2]) == int(base.at[2, "timestamp"].value)

    stopped = base.copy()
    stopped.loc[2, f"{left}_low"] = 99.5
    stop_exit = curve._causal_exit(
        curve._pair_execution_view(stopped, pair), **exit_kwargs
    )
    assert stop_exit is not None
    assert stop_exit[0] == 3
    assert stop_exit[1] == -1_000.0

    targeted = base.copy()
    targeted.loc[3, f"{left}_high"] = 102.0
    targeted.loc[4, [f"{left}_high", f"{left}_close"]] = 101.0
    target_exit = curve._causal_exit(
        curve._pair_execution_view(targeted, pair), **exit_kwargs
    )
    assert target_exit is not None
    assert target_exit == (5, 0.0, 4_000.0)

    deadline_exit = curve._causal_exit(
        view,
        **{
            **exit_kwargs,
            "stop_usd": 1e9,
            "target_usd": 1e9,
            "holding_minutes": 2,
        },
    )
    assert deadline_exit is not None
    assert deadline_exit[0] == 5

    flattened = base.copy()
    flattened.loc[2, "local_minute"] = 15 * 60 + 9
    flatten_exit = curve._causal_exit(
        curve._pair_execution_view(flattened, pair),
        **{**exit_kwargs, "stop_usd": 1e9, "target_usd": 1e9},
    )
    assert flatten_exit is not None
    assert flatten_exit[0] == 3

    for boundary_column, boundary_value in (
        ("session_id", "2024-01-03"),
        ("roll_segment", 2),
    ):
        crossed = targeted.copy()
        crossed.loc[5:, boundary_column] = boundary_value
        assert (
            curve._causal_exit(
                curve._pair_execution_view(crossed, pair), **exit_kwargs
            )
            is None
        )

        entry_crossed = base.copy()
        entry_crossed.loc[5:, boundary_column] = boundary_value
        boundary_rule = curve.RuleSpec(
            rule_id=f"test:array_view:{boundary_column}",
            pair_id=pair.pair_id,
            mechanism="CONTINUATION",
            session_role="OPEN",
            trigger_z=1.0,
            holding_minutes=0,
        )
        boundary_account = {
            "account_label": "150K",
            "maximum_loss_limit_usd": 4_500,
            "maximum_mini_contracts": 15,
        }
        for control, signal_index in (("PRIMARY", 4), ("TIMING_DELAY_5_BARS", 0)):
            assert not curve._generate_gross_events(
                entry_crossed,
                pair=pair,
                rule=boundary_rule,
                account_rule=boundary_account,
                signal_indices=(signal_index,),
                control=control,
            )


def test_execution_view_preserves_controls_sizing_and_next_free() -> None:
    pair = curve.PAIR_SPECS[0]
    frame = _execution_fixture(pair)
    left = pair.shorter_root
    frame.loc[3, f"{left}_open"] = 100.01
    frame.loc[6, f"{left}_open"] = 100.02
    frame.loc[7, f"{left}_open"] = 100.05
    account = {
        "account_label": "150K",
        "maximum_loss_limit_usd": 4_500,
        "maximum_mini_contracts": 15,
    }
    immediate = curve.RuleSpec(
        rule_id="test:array_view:controls",
        pair_id=pair.pair_id,
        mechanism="CONTINUATION",
        session_role="OPEN",
        trigger_z=1.0,
        holding_minutes=0,
    )

    primary = curve._generate_gross_events(
        frame,
        pair=pair,
        rule=immediate,
        account_rule=account,
        signal_indices=(1,),
        control="PRIMARY",
    )
    flipped = curve._generate_gross_events(
        frame,
        pair=pair,
        rule=immediate,
        account_rule=account,
        signal_indices=(1,),
        control="SIGN_FLIP",
    )
    delayed = curve._generate_gross_events(
        frame,
        pair=pair,
        rule=immediate,
        account_rule=account,
        signal_indices=(1,),
        control="TIMING_DELAY_5_BARS",
    )
    assert len(primary) == len(flipped) == len(delayed) == 1
    assert flipped[0].gross_pnl == -primary[0].gross_pnl
    assert flipped[0].decision_ns == primary[0].decision_ns
    assert flipped[0].exit_ns == primary[0].exit_ns
    assert flipped[0].quantity == primary[0].quantity
    assert flipped[0].worst_unrealized_pnl == -primary[0].best_unrealized_pnl
    assert primary[0].exit_ns == int(frame.at[3, "timestamp"].value)
    assert delayed[0].exit_ns == int(frame.at[7, "timestamp"].value)

    no_capacity = {**account, "maximum_mini_contracts": 1}
    assert not curve._generate_gross_events(
        frame,
        pair=pair,
        rule=immediate,
        account_rule=no_capacity,
        signal_indices=(1,),
        control="PRIMARY",
    )

    held = curve.RuleSpec(
        rule_id="test:array_view:next_free",
        pair_id=pair.pair_id,
        mechanism="CONTINUATION",
        session_role="OPEN",
        trigger_z=1.0,
        holding_minutes=2,
        stop_risk_multiple=1e9,
        target_risk_multiple=1e9,
    )
    sequence = _execution_fixture(pair, rows=12)
    sequence.loc[1, "left_dollar_sigma_60"] = 0.0
    sequence_account = {
        **account,
        "maximum_loss_limit_usd": 1_000,
        "maximum_mini_contracts": 2,
    }
    non_overlapping = curve._generate_gross_events(
        sequence,
        pair=pair,
        rule=held,
        account_rule=sequence_account,
        signal_indices=(1, 2, 6, 7),
        control="PRIMARY",
    )
    assert [row.decision_ns for row in non_overlapping] == [
        int(sequence.at[index, "timestamp"].value) for index in (2, 7)
    ]
    assert [row.exit_ns for row in non_overlapping] == [
        int(sequence.at[index, "timestamp"].value) for index in (6, 11)
    ]
