from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from hydra.evidence.schema import RECORD_SPECS, validate_identity
from hydra.evidence import EvidenceBundleWriter, verify_evidence_bundle
from hydra.production import microstructure_foundry_pilot as pilot
from hydra.production.microstructure_event_engine import (
    BookStateError,
    CausalityViolation,
    F_LAST,
    F_MAYBE_BAD_BOOK,
    F_SNAPSHOT,
    MarketEvent,
)


def _market_events(market: str, instrument: str, base_price: float, days: int = 5):
    values: list[MarketEvent] = []
    sequence = 0
    base = datetime(2024, 7, 8, 13, 30, tzinfo=UTC)
    tick = 0.25 if market == "NQ" else 1.0
    for day in range(days):
        start = base + timedelta(days=day)
        session = start.date().isoformat()

        def add(
            second: float,
            action: str,
            side: str = "N",
            price: float | None = None,
            size: int = 0,
            order_id: str | None = None,
        ) -> None:
            nonlocal sequence
            sequence += 1
            event_ns = int((start + timedelta(seconds=second)).timestamp() * 1e9)
            values.append(
                MarketEvent(
                    ts_event_ns=event_ns,
                    available_at_ns=event_ns + 1_000_000,
                    sequence=sequence,
                    publisher_id="GLBX",
                    instrument_id=instrument,
                    action=action,
                    side=side,
                    price=price,
                    size=size,
                    order_id=order_id,
                    flags=(F_SNAPSHOT | F_LAST) if action == "R" else 0,
                    session_id=session,
                    is_snapshot=action == "R",
                )
            )

        add(0.0, "R")
        add(0.01, "A", "B", base_price - tick, 100, "bid")
        add(0.02, "A", "A", base_price + tick, 100, "ask")
        for offset in range(1, 66):
            middle = base_price + tick * (4 * math.sin((offset + day * 5) / 6) + day)
            add(offset + 0.01, "M", "B", middle - tick, 40 + (offset * 7) % 90, "bid")
            add(offset + 0.02, "M", "A", middle + tick, 40 + (offset * 11) % 90, "ask")
            side = "B" if (offset // 3 + day) % 2 == 0 else "A"
            add(offset + 0.03, "T", side, middle, 20 + (offset * 17) % 170)
    return values


def _event_bank(days: int = 5):
    return {
        "NQ": _market_events("NQ", "NQU4", 20_000.0, days),
        "YM": _market_events("YM", "YMU4", 40_000.0, days),
    }


def test_foundry_pilot_e2e_is_causal_atomic_and_runtime_compatible(tmp_path: Path):
    result = pilot.run_microstructure_foundry_pilot_from_events(
        _event_bank(), tmp_path / "pilot"
    )

    assert result.event_store_status == "BOOK_STATE_RECONSTRUCTION_GREEN"
    assert result.pilot_status in pilot.PILOT_DECISIONS
    summary = result.compact_outputs["campaign_summary"]
    assert summary["candidate_count"] == 24
    assert summary["exact_replay_count"] == 24
    assert summary["control_replay_count"] == 72
    assert summary["combine_episode_count"] == 720
    assert summary["normal_episode_count"] == 360
    assert summary["stressed_episode_count"] == 360
    assert result.store_receipt["receipt_hash"]
    assert len(result.decision_report["selected_sessions"]) == 5

    root = Path(result.output_dir)
    report = json.loads((root / "decision_report.json").read_text())
    assert tuple(report["candidate_mechanism_families"]) == pilot.EXPERT_FAMILIES
    assert set(report["chronological_roles"].values()) == {
        "DISCOVERY",
        "VALIDATION",
        "FINAL_DEVELOPMENT",
    }
    assert report["governance"]["live_trading"] is False
    assert report["governance"]["mbo_teacher_direct_deployment"] is False
    reconstruction = report["event_reconstruction"]
    assert reconstruction["initial_snapshot_complete_by_market"] == {
        "NQ": True,
        "YM": True,
    }
    assert reconstruction["snapshot_resets_by_market"] == {"NQ": 5, "YM": 5}
    assert reconstruction["snapshot_f_last_gating"] is True
    assert reconstruction["maybe_bad_book_fail_closed"] is True

    features = pq.read_table(result.event_store_paths["feature_matrices"])
    labels = pq.read_table(result.event_store_paths["outcome_labels"])
    assert "future_markout_ticks" not in features.column_names
    assert "favorable_first" not in features.column_names
    assert "future_markout_ticks" in labels.column_names
    assert any(name.startswith("teacher_") for name in labels.column_names)
    assert all(
        path.exists() for path in map(Path, result.event_store_paths.values())
    )
    validate_identity(result.evidence_identity)
    assert set(result.evidence_datasets) == set(RECORD_SPECS)
    for dataset, rows in result.evidence_datasets.items():
        assert rows
        for row in rows:
            RECORD_SPECS[dataset].validate(
                row, campaign_id=result.campaign_id
            )
    writer = EvidenceBundleWriter.create(
        tmp_path / "bundle", result.evidence_identity, writer_id="0031-test"
    )
    try:
        for dataset, rows in result.evidence_datasets.items():
            writer.append_records(dataset, rows, batch_id=f"0031-{dataset}")
        for name, value in result.compact_outputs.items():
            writer.write_compact_output(name, value)
        receipt = writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=tmp_path / "bundle_receipt.json",
        )
    finally:
        writer.close()
    verify_evidence_bundle(receipt.bundle_path, deep=True)

    technical_trades = pq.read_table(result.event_store_paths["trades"]).to_pylist()
    for market in ("NQ", "YM"):
        trade = next(value for value in technical_trades if value["market"] == market)
        assert trade["base_slippage_cost_usd"] == pytest.approx(20.0)
        assert trade["normal_total_cost_usd"] == pytest.approx(23.80)
        assert trade["stressed_total_cost_usd"] == pytest.approx(33.80)
        assert trade["normal_costs_usd"] == pytest.approx(3.80)
        assert trade["stressed_costs_usd"] == pytest.approx(13.80)
        assert trade["normal_net_pnl_usd"] - trade["stressed_net_pnl_usd"] == pytest.approx(10.0)


def test_pilot_fails_closed_when_five_chronological_sessions_are_absent(tmp_path: Path):
    with pytest.raises(pilot.FoundryPilotError, match="complete common sessions"):
        pilot.run_microstructure_foundry_pilot_from_events(
            _event_bank(days=4), tmp_path / "pilot"
        )


def test_market_event_future_availability_is_rejected_before_research(tmp_path: Path):
    bank = _event_bank()
    original = bank["NQ"][0]
    bank["NQ"][0] = MarketEvent(
        **{
            **original.to_record(),
            "available_at_ns": original.ts_event_ns - 1,
        }
    )
    with pytest.raises(CausalityViolation):
        pilot.run_microstructure_foundry_pilot_from_events(
            bank, tmp_path / "pilot"
        )


def test_pilot_requires_completed_initial_snapshot_for_each_selected_market() -> None:
    base = int(datetime(2024, 7, 8, 13, 30, tzinfo=UTC).timestamp() * 1e9)
    values = tuple(
        (
            market,
            MarketEvent(
                ts_event_ns=base + index,
                available_at_ns=base + index + 1,
                sequence=1,
                publisher_id="GLBX",
                instrument_id=instrument,
                action="T",
                side="B",
                price=price,
                size=1,
                session_id="2024-07-08",
            ),
        )
        for index, (market, instrument, price) in enumerate(
            (("NQ", "NQU4", 20_000.0), ("YM", "YMU4", 40_000.0))
        )
    )
    with pytest.raises(
        pilot.FoundryPilotError,
        match="initial snapshot never completed",
    ):
        pilot._reconstruct_features(values, cfg=pilot.FoundryPilotConfig())


def test_pilot_fails_closed_on_vendor_maybe_bad_book_flag(tmp_path: Path) -> None:
    bank = _event_bank()
    original = bank["NQ"][1]
    bank["NQ"][1] = MarketEvent(
        **{**original.to_record(), "flags": F_MAYBE_BAD_BOOK}
    )
    with pytest.raises(BookStateError, match="F_MAYBE_BAD_BOOK"):
        pilot.run_microstructure_foundry_pilot_from_events(
            bank, tmp_path / "pilot"
        )


def test_depth_fill_is_partial_and_never_fabricates_missing_liquidity():
    assert pilot._depth_fill(((101.0, 2), (102.0, 1)), 5) == (3, pytest.approx(101.3333333333))
    assert pilot._depth_fill((), 1) == (0, None)


def test_dbn_snapshot_bit_is_not_inferred_from_clear_action():
    assert pilot._dbn_snapshot_flag(0x20) is True
    assert pilot._dbn_snapshot_flag(0x21) is True
    assert pilot._dbn_snapshot_flag(0x00) is False


def test_combined_dbn_routes_each_instrument_once_to_its_market():
    timestamp = int(datetime(2024, 7, 16, 13, 30, tzinfo=UTC).timestamp() * 1e9)
    dtype = np.dtype(
        [
            ("ts_event", "u8"),
            ("ts_recv", "u8"),
            ("publisher_id", "u2"),
            ("instrument_id", "u4"),
            ("action", "u1"),
            ("side", "u1"),
            ("price", "i8"),
            ("size", "u4"),
            ("sequence", "u4"),
            ("flags", "u1"),
        ]
    )
    records = np.asarray(
        [
            (timestamp, timestamp + 10, 1, 101, ord("T"), ord("B"), 20_000_000_000_000, 3, 1, 0),
            (timestamp + 20, timestamp + 30, 1, 202, ord("T"), ord("A"), 40_000_000_000_000, 4, 2, 0),
            (timestamp + 40, timestamp + 50, 1, 101, ord("T"), ord("A"), 20_001_000_000_000, 5, 3, 0),
            (timestamp + 60, timestamp + 70, 1, 202, ord("T"), ord("B"), 40_001_000_000_000, 6, 4, 0),
        ],
        dtype=dtype,
    )

    class _Metadata:
        mappings = {
            "NQU4": [
                {
                    "start_date": "2024-07-01",
                    "end_date": "2024-08-01",
                    "symbol": "101",
                }
            ],
            "YMU4": [
                {
                    "start_date": "2024-07-01",
                    "end_date": "2024-08-01",
                    "symbol": "202",
                }
            ],
        }

    class _CombinedStore:
        metadata = _Metadata()

        def __init__(self):
            self.iterator_count = 0

        def to_ndarray(self, *, count: int):
            assert count == 2
            self.iterator_count += 1
            yield records[:2]
            yield records[2:]

    store = _CombinedStore()
    routed = list(
        pilot.iter_dbn_mbo_events_multi_from_store(
            store,
            market_contracts=(("NQ", "NQU4"), ("YM", "YMU4")),
            chunk_size=2,
        )
    )

    assert store.iterator_count == 1
    assert [(market, event.instrument_id) for market, event in routed] == [
        ("NQ", "101"),
        ("YM", "202"),
        ("NQ", "101"),
        ("YM", "202"),
    ]
    assert len(routed) == len(records)
    assert len({event.fingerprint for _, event in routed}) == len(records)


def test_passive_queue_requires_contra_volume_beyond_quantity_ahead():
    tape = pilot._CompactTape(
        available_ns=np.asarray([10, 20, 30], dtype=np.int64),
        price=np.asarray([100.0, 100.0, 100.0]),
        size=np.asarray([100, 3, 4], dtype=np.int32),
        side=np.asarray([1, -1, -1], dtype=np.int8),
        session_code=np.zeros(3, dtype=np.uint16),
        sessions=("2024-07-08",),
    )
    # A same-side trade at the touched bid cannot fill a passive buy.  Six
    # contracts are ahead, so the first three contra contracts also cannot.
    assert pilot._passive_queue_fill(
        tape,
        start_index=0,
        deadline_ns=20,
        session_id="2024-07-08",
        direction=1,
        limit_price=100.0,
        quantity_ahead=6,
        requested_quantity=1,
    ) == (0, -1)
    # Cumulative observed contra volume reaches seven, consuming six ahead and
    # filling exactly one contract at the third trade.
    assert pilot._passive_queue_fill(
        tape,
        start_index=0,
        deadline_ns=30,
        session_id="2024-07-08",
        direction=1,
        limit_price=100.0,
        quantity_ahead=6,
        requested_quantity=1,
    ) == (1, 30)


def test_rolling_mbo_partial_cancel_preserves_remaining_queue_state():
    state = pilot._RollingMarketState()
    base = {
        "ts_event_ns": 1,
        "available_at_ns": 1,
        "sequence": 1,
        "publisher_id": "P",
        "instrument_id": "I",
        "side": "B",
        "price": 100.0,
        "order_id": "o1",
        "session_id": "2024-07-08",
    }
    pilot._update_rolling_state(
        state, MarketEvent(**base, action="A", size=10)
    )
    pilot._update_rolling_state(
        state,
        MarketEvent(**{**base, "sequence": 2}, action="C", size=4),
    )
    assert state.order_size["o1"] == 6
    assert "o1" in state.order_birth_ns
    pilot._update_rolling_state(
        state,
        MarketEvent(**{**base, "sequence": 3}, action="C", size=6),
    )
    assert "o1" not in state.order_size
    assert "o1" not in state.order_birth_ns
