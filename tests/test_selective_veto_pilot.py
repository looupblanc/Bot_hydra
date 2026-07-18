from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from multiprocessing import get_context
from pathlib import Path
import hashlib
import json

import pandas as pd
import pytest

from hydra.evidence import EvidenceBundleWriter, REQUIRED_DATASETS, verify_evidence_bundle
from hydra.evidence.schema import RECORD_SPECS, validate_identity
from hydra.economic_evolution.schema import stable_hash
from hydra.production.selective_veto_pilot import (
    CausalEntryQuote,
    EventWindow,
    SelectiveVetoPilotError,
    StructuralAnchor,
    TargetedCostConfig,
    build_long_anchor_universe,
    generate_targeted_cost_matrix,
    make_event_windows,
    run_selective_veto_campaign,
    _account_matrix,
    _account_rules,
    _account_speed_gate,
    _acquire_selected_offer,
    _canonical_material,
    _causal_action_trajectory,
    _extract_features_from_frame_task,
    _exact_action_quantity,
    _feature_for_anchor,
    _outcome_row,
    _select_fastest_viable_account,
    _sequential_evidence_checkpoints,
    _target_progress_uplift_matrix,
    evaluate_long_sample,
)


ROOT = Path(__file__).resolve().parents[1]


class _Metadata:
    def __init__(self, cost: float = 100.0) -> None:
        self.cost = cost
        self.calls: list[tuple[str, str]] = []

    def get_record_count(self, **kwargs):
        self.calls.append(("records", kwargs["schema"]))
        return 10

    def get_billable_size(self, **kwargs):
        self.calls.append(("bytes", kwargs["schema"]))
        return 1_000

    def get_cost(self, **kwargs):
        self.calls.append(("cost", kwargs["schema"]))
        return self.cost


class _Timeseries:
    def __init__(self, fail_on_call: int | None = None) -> None:
        self.fail_on_call = fail_on_call
        self.calls: list[dict] = []

    def get_range(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self.fail_on_call == len(self.calls):
            raise ValueError("bounded test interruption")
        Path(kwargs["path"]).write_bytes(b"real-dbn-test-payload")


class _Historical:
    def __init__(self, cost: float = 1.0, *, fail_on_call: int | None = None) -> None:
        self.metadata = _Metadata(cost=cost)
        self.timeseries = _Timeseries(fail_on_call=fail_on_call)


def _authenticated_offer(requests: list[dict], *, cost: float = 1.0) -> dict:
    normalized = []
    for row in requests:
        normalized.append(
            {
                **row,
                "estimated_records": int(row.get("estimated_records", 10)),
                "estimated_bytes": int(row.get("estimated_bytes", 1_000)),
                "estimated_cost_usd": float(
                    row.get("estimated_cost_usd", cost / len(requests))
                ),
                "zero_records": False,
            }
        )
    anchor_ids = [
        str(anchor_id)
        for row in normalized
        for anchor_id in row["anchor_ids"]
    ]
    markets = sorted({str(row["market"]) for row in normalized})
    schema = str(normalized[0]["request"]["schema"])
    core = {
        "dataset": "GLBX.MDP3",
        "schema": schema,
        "anchor_window_count": len(anchor_ids),
        "effective_anchor_count": len(anchor_ids),
        "whole_session_prefix": True,
        "market_count": len(markets),
        "markets": markets,
        "strongest_market": markets[0],
        "control_market": markets[1] if len(markets) > 1 else None,
        "merged_window_count": len(normalized),
        "merged_window_duration_seconds": sum(
            float(row["duration_seconds"]) for row in normalized
        ),
        "estimated_records": sum(int(row["estimated_records"]) for row in normalized),
        "estimated_bytes": sum(int(row["estimated_bytes"]) for row in normalized),
        "estimated_cost_usd": float(
            sum(float(row["estimated_cost_usd"]) for row in normalized)
        ),
        "zero_record_window_count": 0,
        "contains_zero_record_windows": False,
        "feature_coverage": ["TRADES", "BBO"],
        "requests": normalized,
        "anchor_ids": anchor_ids,
    }
    return {**core, "estimate_fingerprint": stable_hash(core)}


def _anchor(index: int, market: str = "NQ") -> StructuralAnchor:
    day = index // 10
    minute = index % 10
    timestamp = int(
        datetime(2024, 1, 2, 14, minute, tzinfo=UTC).timestamp() * 1e9
    ) + day * 86_400_000_000_000
    contract = "NQH4" if market == "NQ" else "YMH4"
    return StructuralAnchor(
        anchor_event_id=f"{market}-{index:04d}",
        source_candidate_id=f"hazard-{index % 5}",
        market=market,
        execution_market="MNQ" if market == "NQ" else "MYM",
        structural_family="OPENING_RANGE",
        source_mechanism="RANGE_BREAKOUT_WITH_ROOM",
        contract=contract,
        decision_time_ns=timestamp,
        event_time_ns=timestamp - 60_000_000_000,
        direction=1,
        quantity=4,
        timeframe="1m",
        normal_net_pnl_usd=20.0,
        stressed_net_pnl_usd=18.0,
        normal_worst_unrealized_pnl_usd=-10.0,
        stressed_worst_unrealized_pnl_usd=-12.0,
        normal_best_unrealized_pnl_usd=24.0,
        stressed_best_unrealized_pnl_usd=22.0,
        normal_initial_unrealized_pnl_usd=-4.0,
        stressed_initial_unrealized_pnl_usd=-4.0,
        normal_marks=(
            {
                "availability_time_ns": timestamp + 60_000_000_000,
                "worst_unrealized_pnl": -10.0,
                "best_unrealized_pnl": 24.0,
                "current_unrealized_pnl": 20.0,
            },
        ),
        stressed_marks=(
            {
                "availability_time_ns": timestamp + 60_000_000_000,
                "worst_unrealized_pnl": -12.0,
                "best_unrealized_pnl": 22.0,
                "current_unrealized_pnl": 18.0,
            },
        ),
        normal_fill_price=15_000.0,
        stressed_fill_price=15_000.25,
        raw_exit_price=15_003.0,
        stop_price=14_995.0,
        target_price=15_005.0,
        fill_time_ns=timestamp + 1_000_000_000,
        outcome_time_ns=timestamp + 60_000_000_000,
        session_day=int(timestamp // 86_400_000_000_000),
        outcome="NEITHER_REACHED",
        same_bar_ambiguous=False,
        session_compliant=True,
        contract_limit_compliant=True,
        source_event_hash=f"{index:064x}",
    )


def _audit() -> dict:
    return {
        "seeds": [
            {
                "policy_id": "hybrid_0033_01_f0345ecb99af8c25",
                "market_attribution": [
                    {"market": "NQ", "stressed_net_usd": 100.0},
                    {"market": "YM", "stressed_net_usd": 10.0},
                ],
            }
        ]
    }


def test_event_windows_merge_only_same_contract_and_overlap() -> None:
    first = _anchor(0)
    second = replace(
        _anchor(1),
        decision_time_ns=first.decision_time_ns + 60_000_000_000,
        event_time_ns=first.event_time_ns + 60_000_000_000,
    )
    control = replace(_anchor(0, "YM"), decision_time_ns=first.decision_time_ns)
    windows = make_event_windows([first, second, control])
    assert len(windows) == 2
    nq = next(row for row in windows if row.market == "NQ")
    assert set(nq.anchor_ids) == {first.anchor_event_id, second.anchor_event_id}
    assert nq.duration_seconds == 240.0


def test_targeted_cost_matrix_covers_frozen_grid_and_never_selects_trades() -> None:
    anchors = [_anchor(index, "NQ") for index in range(1_100)] + [
        _anchor(index + 2_000, "YM") for index in range(300)
    ]
    metadata = _Metadata(cost=0.0001)
    matrix = generate_targeted_cost_matrix(metadata, anchors, _audit())
    assert len(matrix["rows"]) == 2 * 3 * 4
    assert {row["market_count"] for row in matrix["rows"]} == {1, 2}
    assert {row["schema"] for row in matrix["rows"]} == {
        "trades",
        "tbbo",
        "mbp-1",
    }
    assert {row["anchor_window_count"] for row in matrix["rows"]} == {
        100,
        250,
        500,
        1_000,
    }
    assert matrix["selected_offer"]["schema"] == "tbbo"
    assert matrix["selected_offer"]["anchor_window_count"] == 1_000
    assert matrix["official_metadata_get_cost_used"] is True
    assert matrix["full_session_matrix_reused_as_final"] is False
    assert all(row["whole_session_prefix"] is True for row in matrix["rows"])
    assert all(
        row["effective_anchor_count"] <= row["anchor_window_count"]
        for row in matrix["rows"]
    )


def test_real_long_universe_reuses_22_sources_without_q4() -> None:
    anchors, provenance = build_long_anchor_universe(ROOT)
    assert provenance["source_candidate_count"] == 22
    assert provenance["anchors_generated"] == len(anchors)
    assert len(anchors) >= 1_000
    assert {row.market for row in anchors} == {"NQ", "YM"}
    assert all(row.decision_time_ns < int(datetime(2024, 10, 1, tzinfo=UTC).timestamp() * 1e9) for row in anchors)
    assert provenance["microstructure_outcomes_used"] is False
    assert provenance["roll_map_hash"] == (
        "705ce6fe27bac7dea9cb9d492413a5112bb60765c66aa75d03f9711bef348208"
    )
    assert all(row.session_day in set(provenance["eligible_session_days"]) for row in anchors)


def test_backend_no_affordable_offer_is_evidence_complete_and_no_purchase(tmp_path: Path) -> None:
    metadata = _Metadata(cost=100.0)
    manifest = {
        "manifest_hash": "a" * 64,
        "source_commit": "0" * 40,
        "anchor_conditioned_windows": {
            "pre_decision_lookback_seconds": 120,
            "post_decision_safety_seconds": 60,
        },
        "targeted_cost_policy": {
            "maximum_incremental_spend_usd": 8.0,
            "minimum_budget_reserve_usd": 20.0,
            "current_remaining_budget_usd": 28.498462508622012,
        },
    }
    result = run_selective_veto_campaign(
        manifest=manifest,
        project_root=ROOT,
        output_dir=tmp_path,
        metadata_client=metadata,
        acquire=False,
    )
    assert result["seed_audit"]["decision"] in {
        "SELECTIVE_VETO_SEED_ROBUST",
        "SELECTIVE_VETO_SEED_FRAGILE",
    }
    assert result["window_cost_matrix"]["status"] == "NO_AFFORDABLE_OFFER"
    assert result["acquisition"]["purchase_performed"] is False
    assert result["acquisition"]["actual_spend_usd"] == 0.0
    assert result["runtime_metrics"]["cpu_worker_count"] == 0
    assert result["runtime_metrics"]["worker_utilization_claimed"] is False
    assert result["long_sample"]["status"] == "NOT_STARTED_NO_AFFORDABLE_SAMPLE"
    assert result["long_sample"]["decision"] == "LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK"
    assert set(result["evidence_identity"]["policy_fingerprints"]) == {
        "hybrid_0033_01_f0345ecb99af8c25",
        "hybrid_0033_07_5f93891cf737e51a",
    }
    validate_identity(result["evidence_identity"])
    for dataset, rows in result["evidence_datasets"].items():
        assert rows
        for row in rows:
            RECORD_SPECS[dataset].validate(
                row, campaign_id=result["evidence_identity"]["campaign_id"]
            )
    assert "SENTINEL" not in str(result["evidence_datasets"])
    assert all(
        float(row["quantity"]) > 0.0
        for row in result["evidence_datasets"]["component_trades"]
    )
    writer = EvidenceBundleWriter.create(
        tmp_path / "bundles", result["evidence_identity"], writer_id="0034-test"
    )
    for dataset in REQUIRED_DATASETS:
        writer.append_records(
            dataset,
            result["evidence_datasets"][dataset],
            batch_id=f"0034-test-{dataset}",
        )
    for name, value in result["compact_outputs"].items():
        writer.write_compact_output(name, value)
    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=tmp_path / "receipt.json",
    )
    verify_evidence_bundle(receipt.bundle_path, deep=True)


def test_frozen_cost_config_preserves_twenty_dollar_reserve() -> None:
    config = TargetedCostConfig()
    config.validate()
    assert (
        config.current_remaining_budget_usd
        - config.maximum_incremental_spend_usd
        >= config.minimum_budget_reserve_usd
    )


def test_integer_one_point_five_trajectory_preserves_intraday_marks() -> None:
    anchor = _anchor(0)
    trajectory = _causal_action_trajectory(anchor, "STRESSED_1_5X", 1.5)
    assert trajectory.event.quantity == 6
    assert trajectory.event.mini_equivalent == 0.6
    assert trajectory.event.decision_ns == anchor.fill_time_ns
    assert trajectory.event.exit_ns == anchor.outcome_time_ns
    assert trajectory.event.net_pnl == 27.0
    assert trajectory.marks[-1].availability_time_ns == anchor.outcome_time_ns
    assert trajectory.marks[-1].current_unrealized_pnl == 27.0


def test_one_point_five_tier_never_rounds_above_nominal_ceiling() -> None:
    anchor = replace(_anchor(0), quantity=3)
    assert _exact_action_quantity(anchor, 1.5) == 4
    assert _exact_action_quantity(anchor, 1.5) <= anchor.quantity * 1.5


def test_mbp_one_flow_counts_only_trade_actions() -> None:
    anchor = _anchor(0)
    timestamps = pd.DatetimeIndex(
        [
            pd.Timestamp(anchor.decision_time_ns - 1_000_000_000, unit="ns", tz="UTC"),
            pd.Timestamp(anchor.decision_time_ns, unit="ns", tz="UTC"),
            pd.Timestamp(anchor.decision_time_ns + 500_000_000, unit="ns", tz="UTC"),
        ]
    )
    frame = pd.DataFrame(
        {
            "ts_recv": timestamps,
            "action": [b"A", b"T", b"M"],
            "price": [15_000.0, 15_000.25, 15_000.5],
            "size": [100.0, 2.0, 200.0],
            "side": [b"A", b"A", b"A"],
            "bid_px_00": [14_999.75, 15_000.0, 15_000.25],
            "ask_px_00": [15_000.0, 15_000.25, 15_000.5],
            "bid_sz_00": [10.0, 10.0, 10.0],
            "ask_sz_00": [8.0, 8.0, 8.0],
        },
        index=timestamps,
    )
    features, _, quote = _feature_for_anchor(frame, anchor, schema="mbp-1")
    assert features[0] == 2.0
    assert features[1] == 2.0
    assert quote.available_at_ns == anchor.decision_time_ns + 500_000_000


def test_zero_latency_legacy_anchor_uses_first_post_decision_quote() -> None:
    """Real 0028 anchors record fill_time == decision_time.

    The microstructure overlay must still use the first genuinely available
    post-decision quote inside its frozen sixty-second acquisition window.
    """

    anchor = replace(_anchor(0), fill_time_ns=_anchor(0).decision_time_ns)
    timestamps = pd.DatetimeIndex(
        [
            pd.Timestamp(anchor.decision_time_ns, unit="ns", tz="UTC"),
            pd.Timestamp(
                anchor.decision_time_ns + 500_000_000, unit="ns", tz="UTC"
            ),
        ]
    )
    frame = pd.DataFrame(
        {
            "ts_recv": timestamps,
            "action": [b"T", b"T"],
            "price": [15_000.0, 15_000.25],
            "size": [2.0, 2.0],
            "side": [b"A", b"A"],
            "bid_px_00": [14_999.75, 15_000.0],
            "ask_px_00": [15_000.0, 15_000.25],
            "bid_sz_00": [10.0, 10.0],
            "ask_sz_00": [8.0, 8.0],
        },
        index=timestamps,
    )

    _, _, quote = _feature_for_anchor(frame, anchor, schema="tbbo")
    assert quote.available_at_ns == anchor.decision_time_ns + 500_000_000
    row = _outcome_row(
        anchor, "VALIDATION", 1.0, "TRADE_1X", "c" * 64, quote
    )
    assert row["entry_time_ns"] == quote.available_at_ns


def test_execution_quote_uses_true_event_time_not_receive_index() -> None:
    anchor = replace(_anchor(0), fill_time_ns=_anchor(0).decision_time_ns)
    receive_times = pd.DatetimeIndex(
        [
            pd.Timestamp(anchor.decision_time_ns - 100_000_000, unit="ns", tz="UTC"),
            pd.Timestamp(anchor.decision_time_ns + 1_000_000, unit="ns", tz="UTC"),
            pd.Timestamp(anchor.decision_time_ns + 2_000_000, unit="ns", tz="UTC"),
        ]
    )
    event_times = pd.DatetimeIndex(
        [
            pd.Timestamp(anchor.decision_time_ns - 200_000_000, unit="ns", tz="UTC"),
            # This stale event arrived after the decision and must not fill.
            pd.Timestamp(anchor.decision_time_ns - 1_000_000, unit="ns", tz="UTC"),
            pd.Timestamp(anchor.decision_time_ns + 1_000_000, unit="ns", tz="UTC"),
        ]
    )
    frame = pd.DataFrame(
        {
            "ts_event": event_times,
            "action": [b"T", b"T", b"T"],
            "price": [15_000.0, 15_000.0, 15_000.25],
            "size": [2.0, 2.0, 2.0],
            "side": [b"A", b"A", b"A"],
            "bid_px_00": [14_999.75, 14_999.75, 15_000.0],
            "ask_px_00": [15_000.0, 15_000.0, 15_000.25],
            "bid_sz_00": [10.0, 10.0, 10.0],
            "ask_sz_00": [8.0, 8.0, 8.0],
        },
        index=receive_times,
    )

    _, _, quote = _feature_for_anchor(frame, anchor, schema="tbbo")
    assert quote.event_time_ns == anchor.decision_time_ns + 1_000_000
    assert quote.available_at_ns == anchor.decision_time_ns + 2_000_000


def test_acquired_bbo_rebuilds_initial_and_first_account_mark() -> None:
    anchor = replace(_anchor(0), fill_time_ns=_anchor(0).decision_time_ns)
    quote = CausalEntryQuote(
        schema="tbbo",
        event_time_ns=anchor.decision_time_ns + 1_000_000,
        available_at_ns=anchor.decision_time_ns + 2_000_000,
        bid_price=15_000.75,
        ask_price=15_001.0,
        bid_size=10.0,
        ask_size=10.0,
        first_mark_available_at_ns=anchor.normal_marks[0]["availability_time_ns"],
        post_fill_worst_liquidation_price=15_000.0,
        post_fill_best_liquidation_price=15_002.0,
        post_fill_last_liquidation_price=15_001.5,
    )

    trajectory = _causal_action_trajectory(anchor, "NORMAL", 1.0, quote)
    assert trajectory.event.decision_ns == quote.available_at_ns
    assert trajectory.initial_unrealized_pnl == -6.0
    assert trajectory.marks[0].worst_unrealized_pnl == -18.0
    assert trajectory.marks[0].best_unrealized_pnl == 4.0
    assert trajectory.marks[0].current_unrealized_pnl == 0.0
    assert quote.available_at_ns < trajectory.marks[0].availability_time_ns


def test_entry_quote_after_frozen_event_window_is_rejected() -> None:
    anchor = replace(_anchor(0), fill_time_ns=_anchor(0).decision_time_ns)
    quote = CausalEntryQuote(
        schema="tbbo",
        event_time_ns=anchor.decision_time_ns + 61_000_000_000,
        available_at_ns=anchor.decision_time_ns + 61_000_000_000,
        bid_price=15_000.0,
        ask_price=15_000.25,
        bid_size=10.0,
        ask_size=8.0,
    )

    with pytest.raises(
        SelectiveVetoPilotError, match="after frozen event-window bound"
    ):
        _outcome_row(
            anchor, "VALIDATION", 1.0, "TRADE_1X", "d" * 64, quote
        )


def test_acquired_bbo_and_depth_drive_entry_fill_without_changing_structure() -> None:
    anchor = _anchor(0)
    quote = CausalEntryQuote(
        schema="tbbo",
        event_time_ns=anchor.decision_time_ns + 500_000_000,
        available_at_ns=anchor.decision_time_ns + 500_000_000,
        bid_price=15_000.75,
        ask_price=15_001.0,
        bid_size=0.25,
        ask_size=0.25,
    )
    one = _outcome_row(anchor, "VALIDATION", 1.0, "TRADE_1X", "a" * 64, quote)
    high = _outcome_row(
        anchor, "VALIDATION", 2.0, "TRADE_1_5X", "b" * 64, quote
    )
    assert one["normal_entry_price"] == 15_001.25
    assert high["normal_entry_price"] == 15_001.5
    assert high["stressed_entry_price"] == 15_001.75
    assert high["quantity"] == 6
    assert high["quantity"] <= high["source_quantity"] * 1.5
    assert high["entry_fill_model"] == "ACQUIRED_BBO_AGGRESSIVE_DEPTH_SLIPPAGE_V1"
    assert high["direction"] == anchor.direction
    assert high["stop_price"] == anchor.stop_price
    assert high["target_price"] == anchor.target_price


def test_feature_extraction_worker_matches_sequential() -> None:
    anchors = (_anchor(0), _anchor(1))
    timestamps = pd.DatetimeIndex(
        [
            pd.Timestamp(anchor.decision_time_ns - 1_000_000_000, unit="ns", tz="UTC")
            for anchor in anchors
        ]
        + [
            pd.Timestamp(anchor.decision_time_ns, unit="ns", tz="UTC")
            for anchor in anchors
        ]
    )
    frame = pd.DataFrame(
        {
            "ts_recv": timestamps,
            "price": [15_000.0, 15_001.0, 15_000.25, 15_001.25],
            "size": [2.0, 3.0, 4.0, 5.0],
            "side": ["B", "A", "A", "B"],
            "bid_px_00": [14_999.75, 15_000.75, 15_000.0, 15_001.0],
            "ask_px_00": [15_000.0, 15_001.0, 15_000.25, 15_001.25],
            "bid_sz_00": [10.0, 9.0, 12.0, 11.0],
            "ask_sz_00": [8.0, 7.0, 9.0, 10.0],
        },
        index=timestamps,
    )
    task = (frame, anchors)
    sequential = _extract_features_from_frame_task(task)
    with ProcessPoolExecutor(
        max_workers=2, mp_context=get_context("spawn")
    ) as executor:
        worker_results = list(
            executor.map(
                _extract_features_from_frame_task,
                (task, task),
                chunksize=1,
            )
        )
    assert worker_results == [sequential, sequential]


def test_exact_account_matrix_uses_complete_calendar_not_signal_days() -> None:
    anchors = [_anchor(index) for index in range(15)]
    # Give every anchor its own immutable session day while retaining sparse
    # trade activity: the account calendar below also contains ten no-signal days.
    anchors = [
        replace(
            row,
            session_day=20_000 + index * 2,
            decision_time_ns=row.decision_time_ns + index * 2 * 86_400_000_000_000,
            event_time_ns=row.event_time_ns + index * 2 * 86_400_000_000_000,
            fill_time_ns=row.fill_time_ns + index * 2 * 86_400_000_000_000,
            outcome_time_ns=row.outcome_time_ns + index * 2 * 86_400_000_000_000,
            normal_marks=tuple(
                {
                    **mark,
                    "availability_time_ns": int(mark["availability_time_ns"])
                    + index * 2 * 86_400_000_000_000,
                }
                for mark in row.normal_marks
            ),
            stressed_marks=tuple(
                {
                    **mark,
                    "availability_time_ns": int(mark["availability_time_ns"])
                    + index * 2 * 86_400_000_000_000,
                }
                for mark in row.stressed_marks
            ),
        )
        for index, row in enumerate(anchors)
    ]
    roles = ("DISCOVERY", "VALIDATION", "FINAL_DEVELOPMENT")
    rows = [
        _outcome_row(anchor, roles[index // 5], 1.0, "TRADE_1X", "f" * 64)
        for index, anchor in enumerate(anchors)
    ]
    calendar = tuple(range(20_000, 20_029))
    matrix = _account_matrix(
        rows, {row.anchor_event_id: row for row in anchors}, calendar
    )
    fifty = next(row for row in matrix if row["account_label"] == "50K")
    # Validation and final roles each span nine full trading days, so each has
    # one non-overlapping P5 episode despite only five signal days.
    assert fifty["p5"]["full_coverage_windows"] == 2
    assert fifty["p10"]["full_coverage_windows"] == 0
    assert fifty["rule_snapshot_sha256"] == (
        "cb135983710b5c62755d8f38b1c9c283f90f403ee1a239ca8a670e5af505268f"
    )
    assert {
        row["rule_snapshot"]["status"] for row in matrix
    } == {"OFFICIAL_VERSIONED_RULE_SNAPSHOT"}


def test_account_rules_explicitly_disable_optional_daily_loss_limit() -> None:
    for account_label in ("50K", "100K", "150K"):
        rules = _account_rules(account_label)
        assert rules.no_daily_loss_limit is True
        assert rules.use_optional_daily_loss_limit is False


def test_sequential_checkpoint_decision_uses_only_its_prefix() -> None:
    rows = []
    for session in range(10):
        family = "OPENING_RANGE" if session % 2 == 0 else "FAILED_BREAKOUT"
        positive_prefix = session < 5
        rows.extend(
            [
                {
                    "temporal_role": "FINAL_DEVELOPMENT",
                    "session_id": f"2024-01-{session + 1:02d}",
                    "decision_time_ns": session * 2,
                    "structural_family": family,
                    "risk_tier": 1.0,
                    "normal_net_pnl_usd": 100.0 if positive_prefix else -200.0,
                    "stressed_net_pnl_usd": 100.0 if positive_prefix else -200.0,
                    "baseline_normal_net_pnl_usd": -10.0,
                    "baseline_stressed_net_pnl_usd": -10.0,
                    "paired_normal_uplift_usd": 110.0 if positive_prefix else -190.0,
                    "paired_stressed_uplift_usd": 110.0 if positive_prefix else -190.0,
                },
                {
                    "temporal_role": "FINAL_DEVELOPMENT",
                    "session_id": f"2024-01-{session + 1:02d}",
                    "decision_time_ns": session * 2 + 1,
                    "structural_family": family,
                    "risk_tier": 0.0,
                    "normal_net_pnl_usd": 0.0,
                    "stressed_net_pnl_usd": 0.0,
                    "baseline_normal_net_pnl_usd": -10.0,
                    "baseline_stressed_net_pnl_usd": -10.0,
                    "paired_normal_uplift_usd": 10.0,
                    "paired_stressed_uplift_usd": 10.0,
                },
            ]
        )
    checkpoints = _sequential_evidence_checkpoints(rows)
    assert checkpoints[0]["checkpoint_complete_session_count"] == 5
    assert checkpoints[0]["decision"] == "SUCCESS_EVIDENCE_SUFFICIENT"
    assert checkpoints[0]["summary"]["stressed_net_usd"] == 500.0
    assert checkpoints[-1]["summary"]["stressed_net_usd"] < 0.0
    assert all(
        row["evidence_role"] == "FINAL_DEVELOPMENT_POST_FREEZE_PREFIX_ONLY"
        and row["validation_rows_used_for_checkpoint_decision"] == 0
        for row in checkpoints
    )


def test_sequential_checkpoint_cannot_leak_validation_outcomes() -> None:
    rows = []
    for session in range(5):
        common = {
            "session_id": f"2024-02-{session + 1:02d}",
            "decision_time_ns": session,
            "structural_family": (
                "OPENING_RANGE" if session % 2 == 0 else "FAILED_BREAKOUT"
            ),
            "risk_tier": 1.0,
            "baseline_normal_net_pnl_usd": 0.0,
            "baseline_stressed_net_pnl_usd": 0.0,
        }
        rows.extend(
            [
                {
                    **common,
                    "temporal_role": "VALIDATION",
                    "normal_net_pnl_usd": 10_000.0,
                    "stressed_net_pnl_usd": 10_000.0,
                    "paired_normal_uplift_usd": 10_000.0,
                    "paired_stressed_uplift_usd": 10_000.0,
                },
                {
                    **common,
                    "temporal_role": "FINAL_DEVELOPMENT",
                    "normal_net_pnl_usd": -100.0,
                    "stressed_net_pnl_usd": -100.0,
                    "paired_normal_uplift_usd": -100.0,
                    "paired_stressed_uplift_usd": -100.0,
                },
            ]
        )
    checkpoints = _sequential_evidence_checkpoints(rows)
    assert len(checkpoints) == 1
    assert checkpoints[0]["summary"]["stressed_net_usd"] == -500.0
    assert checkpoints[0]["decision"] == "FUTILITY_STOP"


def test_target_progress_or_branch_and_fastest_official_account() -> None:
    def summary(progress: float, *, passes: int = 0) -> dict[str, float | int]:
        return {
            "full_coverage_windows": 2,
            "pass_count": passes,
            "pass_rate": passes / 2.0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 1.0,
            "median_target_progress": progress,
            "minimum_mll_buffer_usd": 1_500.0,
            "net_total_usd": progress * 10_000.0,
        }

    def cell(label: str, validation: float, final: float) -> dict:
        roles = {
            "VALIDATION": {
                "STRESSED_1_5X": {
                    "p5": summary(validation),
                    "p10": summary(validation),
                }
            },
            "FINAL_DEVELOPMENT": {
                "STRESSED_1_5X": {
                    "p5": summary(final),
                    "p10": summary(final),
                }
            },
        }
        return {
            "account_label": label,
            "by_role": roles,
            "p5": summary((validation + final) / 2.0),
            "p10": summary((validation + final) / 2.0),
        }

    baseline = [cell(label, 0.10, 0.10) for label in ("50K", "100K", "150K")]
    selected = [
        cell("50K", 0.18, 0.17),
        cell("100K", 0.16, 0.16),
        cell("150K", 0.14, 0.14),
    ]
    uplift = _target_progress_uplift_matrix(selected, baseline)
    assert any(
        row["account_label"] == "50K"
        and row["material_stable_uplift"] is True
        for row in uplift
    )
    assert _account_speed_gate(
        any_stressed_pass=False, target_progress_uplift=uplift
    ) is True
    assert (
        _select_fastest_viable_account(
            selected,
            uplift,
            global_decision="LONG_SAMPLE_SELECTIVE_OVERLAY_WEAK",
        )
        is None
    )
    fastest = _select_fastest_viable_account(
        selected,
        uplift,
        global_decision="LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN",
    )
    assert fastest is not None
    assert fastest["account_label"] == "50K"
    account_ineligible = json.loads(json.dumps(selected))
    for account in account_ineligible:
        for role in ("VALIDATION", "FINAL_DEVELOPMENT"):
            for horizon in ("p5", "p10"):
                account["by_role"][role]["STRESSED_1_5X"][horizon][
                    "net_total_usd"
                ] = -1.0
    assert (
        _select_fastest_viable_account(
            account_ineligible,
            uplift,
            global_decision="LONG_SAMPLE_SELECTIVE_OVERLAY_GREEN",
        )
        is None
    )


def test_manifest_bound_acquisition_is_resumable_and_charged_once(tmp_path: Path) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    request = {
        **window.to_dict(),
        "request": window.request("tbbo"),
        "estimated_cost_usd": 1.0,
    }
    offer = _authenticated_offer([request])
    manifest = {"manifest_hash": "a" * 64}
    client = _Historical()
    first = _acquire_selected_offer(
        client,
        offer,
        root=tmp_path,
        manifest=manifest,
        config=TargetedCostConfig(),
    )
    second = _acquire_selected_offer(
        client,
        offer,
        root=tmp_path,
        manifest=manifest,
        config=TargetedCostConfig(),
    )
    assert first == second
    assert first["manifest_bound_data_purchase_count"] == 1
    assert first["unmanifested_data_purchase_count"] == 0
    assert first["budget_ledger_before_sha256"] != first["budget_ledger_after_sha256"]
    assert first["data_access_ledger_before_sha256"] != first["data_access_ledger_after_sha256"]
    budget_rows = [
        __import__("json").loads(line)
        for line in (
            tmp_path / "reports/data_budget/databento_spend_ledger.jsonl"
        ).read_text().splitlines()
    ]
    assert sum(row["download_status"] == "DOWNLOADED" for row in budget_rows) == 1


def test_bounded_repair_reuses_exact_prior_bundle_without_api_or_charge(
    tmp_path: Path,
) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    offer = _authenticated_offer(
        [
            {
                **window.to_dict(),
                "request": window.request("tbbo"),
                "estimated_cost_usd": 1.0,
            }
        ]
    )
    original_client = _Historical()
    original = _acquire_selected_offer(
        original_client,
        offer,
        root=tmp_path,
        manifest={"manifest_hash": "a" * 64},
        config=TargetedCostConfig(),
    )
    receipt_path = next(
        (tmp_path / "data/cache/databento/selective_veto_0034").glob(
            "*_receipt.json"
        )
    )
    intent_path = next(
        (tmp_path / "data/cache/databento/selective_veto_0034").glob(
            "*_intent.json"
        )
    )
    authorization_path = next(
        (tmp_path / "data/cache/databento/selective_veto_0034").glob(
            "*_authorization.json"
        )
    )
    intent = json.loads(intent_path.read_text())
    authorization = json.loads(authorization_path.read_text())
    repair = {
        "classification": "POST_PURCHASE_PRE_OUTCOME_EMPTY_EXECUTION_INTERVAL_DEFECT",
        "repair_scope": "FIRST_POST_DECISION_QUOTE_WITHIN_FROZEN_EVENT_WINDOW",
        "prior_manifest_hash": "a" * 64,
        "prior_request_id": original["request_id"],
        "prior_receipt_path": receipt_path.relative_to(tmp_path).as_posix(),
        "prior_receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        "prior_acquisition_receipt_fingerprint": original[
            "acquisition_receipt_fingerprint"
        ],
        "prior_intent_path": intent_path.relative_to(tmp_path).as_posix(),
        "prior_intent_sha256": hashlib.sha256(intent_path.read_bytes()).hexdigest(),
        "prior_intent_fingerprint": intent["intent_fingerprint"],
        "prior_authorization_path": authorization_path.relative_to(
            tmp_path
        ).as_posix(),
        "prior_authorization_sha256": hashlib.sha256(
            authorization_path.read_bytes()
        ).hexdigest(),
        "prior_authorization_fingerprint": authorization[
            "authorization_fingerprint"
        ],
        "prior_metadata_revalidation_hash": intent[
            "metadata_revalidation_hash"
        ],
        "prior_bundle_hash": original["bundle_hash"],
        "post_decision_entry_bound_seconds": 60,
        "prior_raw_bundle_reuse_allowed": True,
        "new_purchase_after_repair_allowed": False,
        "raw_records_changed": False,
        "anchor_set_changed": False,
        "temporal_roles_changed": False,
        "actions_or_thresholds_changed": False,
    }
    repaired_client = _Historical(cost=999.0)
    reused = _acquire_selected_offer(
        repaired_client,
        offer,
        root=tmp_path,
        manifest={
            "manifest_hash": "b" * 64,
            "post_purchase_execution_bound_repair": repair,
        },
        config=TargetedCostConfig(),
    )

    assert repaired_client.metadata.calls == []
    assert repaired_client.timeseries.calls == []
    assert reused["prior_raw_bundle_reused"] is True
    assert reused["new_purchase_performed_after_repair"] is False
    assert reused["additional_spend_after_repair_usd"] == 0.0
    assert reused["original_manifest_hash"] == "a" * 64
    assert reused["manifest_hash"] == "b" * 64
    budget_rows = [
        json.loads(line)
        for line in (
            tmp_path / "reports/data_budget/databento_spend_ledger.jsonl"
        ).read_text().splitlines()
    ]
    assert sum(row["download_status"] == "DOWNLOADED" for row in budget_rows) == 1


def test_acquisition_resumes_per_window_without_redownload_or_double_charge(
    tmp_path: Path,
) -> None:
    anchors = [_anchor(0), _anchor(20)]
    requests = []
    for anchor in anchors:
        window = make_event_windows([anchor])[0]
        requests.append(
            {
                **window.to_dict(),
                "request": window.request("tbbo"),
                "estimated_cost_usd": 0.5,
            }
        )
    offer = _authenticated_offer(requests)
    manifest = {"manifest_hash": "a" * 64}
    interrupted = _Historical(cost=0.5, fail_on_call=2)
    with pytest.raises(Exception, match="event-window download failed"):
        _acquire_selected_offer(
            interrupted,
            offer,
            root=tmp_path,
            manifest=manifest,
            config=TargetedCostConfig(),
        )
    assert len(interrupted.timeseries.calls) == 2

    resumed = _Historical(cost=999.0)
    receipt = _acquire_selected_offer(
        resumed,
        offer,
        root=tmp_path,
        manifest=manifest,
        config=TargetedCostConfig(),
    )
    # The first completed window and the frozen metadata revalidation are both
    # reused; only the interrupted second window reaches the vendor again.
    assert len(resumed.timeseries.calls) == 1
    assert resumed.metadata.calls == []
    assert receipt["completed_window_count"] == 2
    assert receipt["per_window_incremental_cost_accounting"] is True
    budget_rows = [
        __import__("json").loads(line)
        for line in (
            tmp_path / "reports/data_budget/databento_spend_ledger.jsonl"
        ).read_text().splitlines()
    ]
    downloaded = [row for row in budget_rows if row["download_status"] == "DOWNLOADED"]
    assert len(downloaded) == 2
    assert sum(float(row["actual_cost_usd"]) for row in downloaded) == 1.0


def test_completed_acquisition_fails_closed_on_immutable_window_drift(
    tmp_path: Path,
) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    offer = _authenticated_offer(
        [
            {
                **window.to_dict(),
                "request": window.request("tbbo"),
                "estimated_cost_usd": 1.0,
            }
        ]
    )
    client = _Historical()
    receipt = _acquire_selected_offer(
        client,
        offer,
        root=tmp_path,
        manifest={"manifest_hash": "a" * 64},
        config=TargetedCostConfig(),
    )
    Path(receipt["files"][0]["raw_path"]).write_bytes(b"tampered")
    with pytest.raises(Exception, match="checksum drift"):
        _acquire_selected_offer(
            client,
            offer,
            root=tmp_path,
            manifest={"manifest_hash": "a" * 64},
            config=TargetedCostConfig(),
        )


@pytest.mark.parametrize("artifact", ["intent", "authorization", "receipt"])
def test_acquisition_resume_rejects_rehashed_chain_corruption(
    tmp_path: Path, artifact: str
) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    offer = _authenticated_offer(
        [
            {
                **window.to_dict(),
                "request": window.request("tbbo"),
                "estimated_cost_usd": 1.0,
            }
        ]
    )
    manifest = {"manifest_hash": "a" * 64}
    client = _Historical()
    _acquire_selected_offer(
        client,
        offer,
        root=tmp_path,
        manifest=manifest,
        config=TargetedCostConfig(),
    )
    artifact_path = next(
        (tmp_path / "data/cache/databento/selective_veto_0034").glob(
            f"*_{artifact}.json"
        )
    )
    value = json.loads(artifact_path.read_text(encoding="utf-8"))
    if artifact == "intent":
        value["windows"][0]["anchor_ids"] = ["tampered-anchor"]
        fingerprint_field = "intent_fingerprint"
    elif artifact == "authorization":
        value["data_schema"] = "mbp-1"
        fingerprint_field = "authorization_fingerprint"
    else:
        value["files"][0]["anchor_ids"] = ["tampered-anchor"]
        value["bundle_hash"] = stable_hash(value["files"])
        fingerprint_field = "acquisition_receipt_fingerprint"
    value[fingerprint_field] = stable_hash(
        {key: item for key, item in value.items() if key != fingerprint_field}
    )
    artifact_path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(SelectiveVetoPilotError, match="drift"):
        _acquire_selected_offer(
            client,
            offer,
            root=tmp_path,
            manifest=manifest,
            config=TargetedCostConfig(),
        )


@pytest.mark.parametrize("defect", ["SYMBOL", "Q4"])
def test_acquisition_rejects_offer_identity_or_q4_leak_before_api(
    tmp_path: Path, defect: str
) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    offer = _authenticated_offer(
        [{**window.to_dict(), "request": window.request("tbbo")}]
    )
    broken = json.loads(json.dumps(offer))
    if defect == "SYMBOL":
        broken["requests"][0]["request"]["symbols"] = ["ESH4"]
    else:
        broken["requests"][0]["start"] = "2024-10-01T00:00:00Z"
        broken["requests"][0]["end"] = "2024-10-01T00:03:00Z"
        broken["requests"][0]["request"]["start"] = "2024-10-01T00:00:00Z"
        broken["requests"][0]["request"]["end"] = "2024-10-01T00:03:00Z"
    broken["estimate_fingerprint"] = stable_hash(
        {key: item for key, item in broken.items() if key != "estimate_fingerprint"}
    )
    client = _Historical()
    with pytest.raises(SelectiveVetoPilotError, match="drift|Q4"):
        _acquire_selected_offer(
            client,
            broken,
            root=tmp_path,
            manifest={"manifest_hash": "a" * 64},
            config=TargetedCostConfig(),
        )
    assert client.metadata.calls == []
    assert client.timeseries.calls == []


def test_long_sample_emits_paired_metrics_and_exact_account_episodes(tmp_path: Path) -> None:
    base = int(datetime(2024, 1, 2, 14, tzinfo=UTC).timestamp() * 1e9)
    anchors = []
    frame_rows = []
    frame_index = []
    for index in range(30):
        timestamp = base + index * 86_400_000_000_000
        source = _anchor(0)
        outcome = timestamp + 60_000_000_000
        anchor = replace(
            source,
            anchor_event_id=f"long-{index:03d}",
            source_candidate_id=f"hazard-{index % 3}",
            decision_time_ns=timestamp,
            event_time_ns=timestamp - 60_000_000_000,
            fill_time_ns=timestamp + 1_000_000_000,
            outcome_time_ns=outcome,
            session_day=19_543 + index,
            normal_marks=(
                {
                    "availability_time_ns": outcome,
                    "worst_unrealized_pnl": -10.0,
                    "best_unrealized_pnl": 24.0,
                    "current_unrealized_pnl": 20.0,
                },
            ),
            stressed_marks=(
                {
                    "availability_time_ns": outcome,
                    "worst_unrealized_pnl": -12.0,
                    "best_unrealized_pnl": 22.0,
                    "current_unrealized_pnl": 18.0,
                },
            ),
        )
        anchors.append(anchor)
        for offset_ns in (0, 500_000_000):
            event_timestamp = pd.Timestamp(timestamp + offset_ns, unit="ns", tz="UTC")
            frame_index.append(event_timestamp)
            frame_rows.append(
                {
                    "ts_recv": event_timestamp,
                    "action": "T",
                    "price": 15_000.0 + index,
                    "size": 1.0 + index % 5,
                    "side": "A" if index % 2 else "B",
                    "bid_px_00": 14_999.75 + index,
                    "ask_px_00": 15_000.0 + index,
                    "bid_sz_00": 10.0 + index % 3,
                    "ask_sz_00": 8.0 + index % 4,
                }
            )
    frame = pd.DataFrame(frame_rows, index=pd.DatetimeIndex(frame_index))
    receipt = {
        "files": [
            {
                "raw_path": str(tmp_path / "sample.dbn.zst"),
                "anchor_ids": [row.anchor_event_id for row in anchors],
            }
        ]
    }
    result = evaluate_long_sample(
        anchors,
        receipt,
        schema="tbbo",
        frame_loader=lambda _path: frame,
        eligible_session_days=tuple(range(19_543, 19_573)),
    )
    assert result["status"] == "COMPLETE"
    assert result["feature_coverage_invariant"][
        "final_development_all_eligible_anchors_included"
    ] is True
    assert result["feature_extraction_runtime"] == {
        "process_pool_executed": False,
        "cpu_worker_count": 0,
    }
    assert len(result["account_size_matrix"]) == 3
    assert result["account_size_matrix"][0]["scenarios"]["STRESSED_1_5X"]["p5"][
        "full_coverage_windows"
    ] == 2
    required = {
        "paired_entry_cost_delta_usd",
        "paired_mae_delta_usd",
        "paired_mfe_delta_usd",
        "paired_stop_rate_delta",
        "paired_target_rate_delta",
        "paired_holding_duration_delta_seconds",
        "paired_target_contribution_delta_usd",
        "paired_mll_contribution_delta_usd",
    }
    assert required <= set(result["paired_results"][0])
    assert result["sequential_checkpoints"]
    assert all(
        row["policy_refit_since_prior_checkpoint"] is False
        for row in result["sequential_checkpoints"]
    )

    identity, datasets, compact = _canonical_material(
        {"manifest_hash": "a" * 64, "source_commit": "0" * 40},
        tmp_path,
        {},
        {"anchor_universe_hash": "b" * 64},
        {"cost_matrix_hash": "c" * 64},
        {"acquisition_receipt_fingerprint": "d" * 64},
        result,
    )
    expected_policy_ids = {
        f"{result['policy']['policy_id']}:{account_label}"
        for account_label in ("50K", "100K", "150K")
    }
    assert set(identity["policy_fingerprints"]) == expected_policy_ids
    assert {row["policy_id"] for row in datasets["account_policy_membership"]} == (
        expected_policy_ids
    )
    assert all(
        ".NORMAL." not in row["episode_id"]
        and ".STRESSED_1_5X." not in row["episode_id"]
        for row in datasets["episodes"]
    )

    writer = EvidenceBundleWriter.create(
        tmp_path / "long-bundles", identity, writer_id="0034-long-test"
    )
    for dataset in REQUIRED_DATASETS:
        writer.append_records(
            dataset,
            datasets[dataset],
            batch_id=f"0034-long-test-{dataset}",
        )
    for name, value in compact.items():
        writer.write_compact_output(name, value)
    receipt = writer.finalize(
        evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
        lightweight_manifest_path=tmp_path / "long-receipt.json",
    )
    verify_evidence_bundle(receipt.bundle_path, deep=True)


def test_long_sample_fails_closed_when_final_anchor_has_no_executable_quote(
    tmp_path: Path,
) -> None:
    base = int(datetime(2024, 1, 2, 14, tzinfo=UTC).timestamp() * 1e9)
    anchors = []
    rows = []
    index = []
    for ordinal in range(6):
        decision = base + ordinal * 86_400_000_000_000
        anchor = replace(
            _anchor(0),
            anchor_event_id=f"coverage-{ordinal}",
            decision_time_ns=decision,
            event_time_ns=decision - 60_000_000_000,
            fill_time_ns=decision + 1_000_000_000,
            outcome_time_ns=decision + 60_000_000_000,
            session_day=20_000 + ordinal,
            normal_marks=tuple(
                {**mark, "availability_time_ns": decision + 60_000_000_000}
                for mark in _anchor(0).normal_marks
            ),
            stressed_marks=tuple(
                {**mark, "availability_time_ns": decision + 60_000_000_000}
                for mark in _anchor(0).stressed_marks
            ),
        )
        anchors.append(anchor)
        offsets = (0,) if ordinal == 5 else (0, 500_000_000)
        for offset in offsets:
            timestamp = pd.Timestamp(decision + offset, unit="ns", tz="UTC")
            index.append(timestamp)
            rows.append(
                {
                    "ts_recv": timestamp,
                    "action": "T",
                    "price": 15_000.0,
                    "size": 1.0,
                    "side": "A",
                    "bid_px_00": 14_999.75,
                    "ask_px_00": 15_000.0,
                    "bid_sz_00": 10.0,
                    "ask_sz_00": 10.0,
                }
            )
    frame = pd.DataFrame(rows, index=pd.DatetimeIndex(index))
    receipt = {
        "files": [
            {
                "raw_path": str(tmp_path / "incomplete.dbn.zst"),
                "anchor_ids": [row.anchor_event_id for row in anchors],
            }
        ]
    }
    with pytest.raises(SelectiveVetoPilotError, match="feature coverage is not exact"):
        evaluate_long_sample(
            anchors,
            receipt,
            schema="tbbo",
            frame_loader=lambda _path: frame,
            eligible_session_days=tuple(range(20_000, 20_006)),
        )
