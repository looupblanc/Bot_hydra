from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from multiprocessing import get_context
from pathlib import Path

import pandas as pd

from hydra.evidence import EvidenceBundleWriter, REQUIRED_DATASETS, verify_evidence_bundle
from hydra.evidence.schema import RECORD_SPECS, validate_identity
from hydra.production.selective_veto_pilot import (
    EventWindow,
    StructuralAnchor,
    TargetedCostConfig,
    build_long_anchor_universe,
    generate_targeted_cost_matrix,
    make_event_windows,
    run_selective_veto_campaign,
    _account_matrix,
    _acquire_selected_offer,
    _canonical_material,
    _causal_action_trajectory,
    _extract_features_from_frame_task,
    _outcome_row,
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
    def get_range(self, **kwargs):
        Path(kwargs["path"]).write_bytes(b"real-dbn-test-payload")


class _Historical:
    def __init__(self, cost: float = 1.0) -> None:
        self.metadata = _Metadata(cost=cost)
        self.timeseries = _Timeseries()


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
        "0d5039eea0e51b89343a5a5f32279a6bd3ded88092922f527fdf82c699826d54"
    )


def test_manifest_bound_acquisition_is_resumable_and_charged_once(tmp_path: Path) -> None:
    anchor = _anchor(0)
    window = make_event_windows([anchor])[0]
    request = {
        **window.to_dict(),
        "request": window.request("tbbo"),
        "estimated_cost_usd": 1.0,
    }
    offer = {
        "estimated_cost_usd": 1.0,
        "estimate_fingerprint": "e" * 64,
        "schema": "tbbo",
        "anchor_window_count": 100,
        "effective_anchor_count": 1,
        "anchor_ids": [anchor.anchor_event_id],
        "requests": [request],
    }
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
        frame_index.append(pd.Timestamp(timestamp, unit="ns", tz="UTC"))
        frame_rows.append(
            {
                "ts_recv": pd.Timestamp(timestamp, unit="ns", tz="UTC"),
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
