from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from hydra.data.contract_mapping import ContractInfo, RollMap
from hydra.shadow.activation import audit_zero_order_surface
from hydra.shadow.contract_resolver import (
    ContractResolution,
    ResolvedContract,
    resolve_current_contracts,
)
from hydra.shadow.data_heartbeat import (
    ForwardDataHeartbeat,
    ForwardHeartbeatError,
    HeartbeatPublisher,
)
from hydra.shadow.feed_health import assess_feed_health
from hydra.shadow.forward_bar_store import (
    CmeSessionCalendar,
    ForwardBar,
    ForwardBarIntegrityError,
    ForwardBarStore,
    MarketClosure,
)


UTC = timezone.utc


def _contract(root: str, contract: str, instrument_id: str) -> ContractInfo:
    return ContractInfo(
        root=root,
        contract=contract,
        month_code="U",
        year=2026,
        expiry_date="2026-09-18",
        last_trade_date="2026-09-18",
        active_start="2026-06-20T00:00:00+00:00",
        active_end="2026-09-15T00:00:00+00:00",
        roll_date="2026-06-20T00:00:00+00:00",
        tick_size=1.0,
        tick_value=0.5,
        point_value=0.5,
        contract_multiplier=0.5,
        is_micro=root.startswith("M"),
        instrument_id=instrument_id,
        parent_symbol=root,
        continuous_symbol=f"{root}.c.0",
        activation_time="2025-09-19T00:00:00+00:00",
        deactivation_time="2026-09-15T00:00:00+00:00",
        roll_reason="explicit_test_transition",
        transition_uncertainty="none",
    )


def _write_map(path: Path, *, historical: bool = False) -> RollMap:
    contract = _contract("MYM", "MYMU6", "101")
    if historical:
        contract = ContractInfo(
            **{
                **contract.__dict__,
                "contract": "MYMU4",
                "year": 2024,
                "expiry_date": "2024-09-20",
                "last_trade_date": "2024-09-20",
                "active_start": "2024-06-20T00:00:00+00:00",
                "active_end": "2024-09-15T00:00:00+00:00",
                "roll_date": "2024-06-20T00:00:00+00:00",
            }
        )
    roll_map = RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2",
        symbols=["MYM"],
        contracts=[contract],
        unsafe_window_days=3,
        notes=["test explicit definitions"],
        source_metadata={"period_end": "2026-09-15" if not historical else "2024-09-15"},
    )
    path.write_text(json.dumps(roll_map.to_dict(), sort_keys=True), encoding="utf-8")
    return roll_map


def _resolution(path: Path) -> ContractResolution:
    result = resolve_current_contracts(
        [path], ["MYM"], as_of="2026-07-13T14:00:00+00:00"
    )
    assert result.ready
    return result


def _calendar(*, closure: MarketClosure | None = None) -> CmeSessionCalendar:
    return CmeSessionCalendar(
        version="cme_calendar_test_v1",
        holiday_schedule_through=date(2026, 12, 31),
        closures=(closure,) if closure else (),
    )


def _bar(
    minute: int,
    *,
    sequence: int,
    close: float = 100.5,
    start_hour: int = 14,
) -> ForwardBar:
    start = datetime(2026, 7, 13, start_hour, minute, tzinfo=UTC)
    return ForwardBar(
        source_id="local_read_only_feed",
        root="MYM",
        contract="MYMU6",
        timeframe="1m",
        bar_start_at_utc=start,
        bar_close_at_utc=start + timedelta(minutes=1),
        availability_at_utc=start + timedelta(minutes=1),
        open=100.0,
        high=max(101.0, close),
        low=99.0,
        close=close,
        volume=10.0,
        source_sequence=sequence,
    )


def test_historical_explicit_map_is_not_extrapolated_to_2026(tmp_path: Path) -> None:
    path = tmp_path / "roll_map_historical.json"
    _write_map(path, historical=True)

    resolution = resolve_current_contracts(
        [path], ["MYM"], as_of="2026-07-11T12:00:00+00:00"
    )

    assert resolution.status == "SOURCE_REQUIRED"
    assert resolution.missing_roots == ("MYM",)
    assert "no_exact_dated_explicit_contract_coverage" in resolution.reason
    assert not resolution.contracts


def test_current_explicit_contract_resolves_without_continuous_execution(
    tmp_path: Path,
) -> None:
    path = tmp_path / "roll_map_current.json"
    _write_map(path)

    resolution = _resolution(path)

    assert resolution.contract_for("MYM").contract == "MYMU6"
    assert resolution.contract_for("MYM").instrument_id == "101"
    assert resolution.contract_for("MYM").contract != "MYM.c.0"


def test_forward_store_restarts_deduplicates_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "map.json"
    _write_map(map_path)
    resolution = _resolution(map_path)
    store_path = tmp_path / "forward_bars.db"
    store = ForwardBarStore(store_path, calendar=_calendar())
    observed = datetime(2026, 7, 13, 14, 2, tzinfo=UTC)
    first = _bar(0, sequence=1)

    with store.writer(writer_id="test_writer") as writer:
        assert writer.append(first, observed_at=observed, resolution=resolution)["status"] == "ACCEPTED"
    restarted = ForwardBarStore(store_path, calendar=_calendar())
    with restarted.writer(writer_id="test_writer") as writer:
        assert writer.append(first, observed_at=observed, resolution=resolution)["status"] == "DUPLICATE_IGNORED"
        with pytest.raises(ForwardBarIntegrityError, match="Divergent duplicate"):
            writer.append(
                _bar(0, sequence=1, close=100.75),
                observed_at=observed,
                resolution=resolution,
            )

    summary = restarted.summary()
    assert summary["sqlite_integrity"] == "ok"
    assert summary["bar_count"] == 1
    assert summary["duplicate_bar_count"] == 1
    assert summary["rejected_bar_count"] == 1


def test_forward_store_allows_exactly_one_writer(tmp_path: Path) -> None:
    store = ForwardBarStore(tmp_path / "bars.db", calendar=_calendar())

    with store.writer(writer_id="writer_one"):
        with pytest.raises(RuntimeError, match="Another forward-bar writer"):
            with store.writer(writer_id="writer_two"):
                pass


def test_missing_bars_and_non_monotonic_sequences_fail_closed(tmp_path: Path) -> None:
    map_path = tmp_path / "map.json"
    _write_map(map_path)
    resolution = _resolution(map_path)
    store = ForwardBarStore(tmp_path / "bars.db", calendar=_calendar())
    observed = datetime(2026, 7, 13, 14, 5, tzinfo=UTC)
    with store.writer(writer_id="test_writer") as writer:
        writer.append(_bar(0, sequence=1), observed_at=observed, resolution=resolution)
        result = writer.append(_bar(2, sequence=2), observed_at=observed, resolution=resolution)
        assert result["missing_before"] == 1
        with pytest.raises(ForwardBarIntegrityError, match="strictly monotonic"):
            writer.append(_bar(3, sequence=2), observed_at=observed, resolution=resolution)

    assert store.summary()["missing_bar_count"] == 1
    health = assess_feed_health(
        resolution,
        store,
        now=observed,
        max_age_seconds=180,
        source_authorization_proven=True,
    )
    assert health.status == "INTEGRITY_BLOCKED"
    assert not health.can_publish_candidate_heartbeat


def test_feed_becomes_ready_only_with_fresh_exact_contract_and_verified_calendar(
    tmp_path: Path,
) -> None:
    map_path = tmp_path / "map.json"
    _write_map(map_path)
    resolution = _resolution(map_path)
    store = ForwardBarStore(tmp_path / "bars.db", calendar=_calendar())
    observed = datetime(2026, 7, 13, 14, 1, 30, tzinfo=UTC)
    with store.writer(writer_id="test_writer") as writer:
        writer.append(
            _bar(0, sequence=1), observed_at=observed, resolution=resolution
        )

    health = assess_feed_health(
        resolution,
        store,
        now=observed,
        max_age_seconds=75,
        source_authorization_proven=True,
    )

    assert health.status == "READY"
    assert health.can_publish_candidate_heartbeat
    assert health.roots[0]["contract"] == "MYMU6"
    assert health.outbound_orders == 0


def test_closed_multitimeframe_requires_every_source_minute(tmp_path: Path) -> None:
    map_path = tmp_path / "map.json"
    _write_map(map_path)
    resolution = _resolution(map_path)
    store = ForwardBarStore(tmp_path / "bars.db", calendar=_calendar())
    observed = datetime(2026, 7, 13, 14, 7, tzinfo=UTC)
    with store.writer(writer_id="test_writer") as writer:
        for minute in range(5):
            writer.append(
                _bar(minute, sequence=minute + 1),
                observed_at=observed,
                resolution=resolution,
            )
    complete = store.closed_multitimeframe(
        root="MYM", contract="MYMU6", minutes=5, as_of=observed
    )
    incomplete = store.closed_multitimeframe(
        root="MYM",
        contract="MYMU6",
        minutes=5,
        as_of=datetime(2026, 7, 13, 14, 4, 30, tzinfo=UTC),
    )

    assert len(complete) == 1
    assert complete.loc[0, "source_row_count"] == 5
    assert complete.loc[0, "availability_timestamp"] == complete.loc[0, "source_bar_close"]
    assert incomplete.empty


def test_session_calendar_handles_dst_maintenance_weekends_and_holidays() -> None:
    closure = MarketClosure(
        datetime(2026, 7, 3, 17, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 22, 0, tzinfo=UTC),
        "Independence Day closure test",
    )
    calendar = _calendar(closure=closure)

    # 2026-03-08 is already CDT: 22:30 UTC is 17:30 Chicago.
    assert calendar.market_state(datetime(2026, 3, 8, 22, 30, tzinfo=UTC)) == "OPEN"
    assert (
        calendar.market_state(datetime(2026, 3, 9, 21, 30, tzinfo=UTC))
        == "MAINTENANCE_CLOSED"
    )
    assert calendar.market_state(datetime(2026, 7, 4, 14, 0, tzinfo=UTC)) == "HOLIDAY_CLOSED"
    assert calendar.market_state(datetime(2026, 7, 11, 14, 0, tzinfo=UTC)) == "WEEKEND_CLOSED"


def test_heartbeat_is_atomic_checksummed_monotonic_and_pipeline_compatible(
    tmp_path: Path,
) -> None:
    contract = ResolvedContract(
        root="MYM",
        contract="MYMU6",
        instrument_id="101",
        active_start="2026-06-20T00:00:00+00:00",
        active_end="2026-09-15T00:00:00+00:00",
        expiry_date="2026-09-18",
        tick_size=1.0,
        point_value=0.5,
        map_path="map.json",
        map_sha256="a" * 64,
        roll_map_hash="b" * 64,
    )
    heartbeat = ForwardDataHeartbeat.build(
        source_id="local_read_only_feed",
        dataset="GLBX.MDP3",
        contracts={"MYM": contract},
        latest_completed_bar_at=datetime(2026, 7, 13, 14, 1, tzinfo=UTC),
        observed_at=datetime(2026, 7, 13, 14, 1, 5, tzinfo=UTC),
        source_sequence=1,
        source_payload_checksum="c" * 64,
        store_checkpoint="d" * 64,
    )
    publisher = HeartbeatPublisher(tmp_path)
    with publisher.writer(writer_id="feed_writer") as writer:
        path = writer.publish(
            candidate_id="strategy_test_v1",
            heartbeat=heartbeat,
            required_roots=("MYM",),
            stale_data_seconds=75,
            health_status="READY",
            now=datetime(2026, 7, 13, 14, 1, 30, tzinfo=UTC),
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["latest_completed_bar_at_utc"] == "2026-07-13T14:01:00+00:00"
        assert payload["outbound_orders"] == 0
        with pytest.raises(ForwardHeartbeatError, match="increase monotonically"):
            writer.publish(
                candidate_id="strategy_test_v1",
                heartbeat=heartbeat,
                required_roots=("MYM",),
                stale_data_seconds=75,
                health_status="READY",
                now=datetime(2026, 7, 13, 14, 1, 31, tzinfo=UTC),
            )
    assert len((tmp_path / "heartbeat_audit.jsonl").read_text().splitlines()) == 1


def test_no_heartbeat_can_be_published_for_stale_or_unhealthy_data(
    tmp_path: Path,
) -> None:
    heartbeat = ForwardDataHeartbeat.build(
        source_id="local_read_only_feed",
        dataset="GLBX.MDP3",
        contracts={"MYM": "MYMU6"},
        latest_completed_bar_at=datetime(2026, 7, 13, 14, 0, tzinfo=UTC),
        observed_at=datetime(2026, 7, 13, 14, 0, 5, tzinfo=UTC),
        source_sequence=1,
        source_payload_checksum="a" * 64,
        store_checkpoint="b" * 64,
    )
    with HeartbeatPublisher(tmp_path).writer(writer_id="feed_writer") as writer:
        with pytest.raises(ForwardHeartbeatError, match="health"):
            writer.publish(
                candidate_id="strategy_test_v1",
                heartbeat=heartbeat,
                required_roots=("MYM",),
                stale_data_seconds=75,
                health_status="SOURCE_REQUIRED",
                now=datetime(2026, 7, 13, 14, 0, 30, tzinfo=UTC),
            )
        with pytest.raises(ForwardHeartbeatError, match="Stale"):
            writer.publish(
                candidate_id="strategy_test_v1",
                heartbeat=heartbeat,
                required_roots=("MYM",),
                stale_data_seconds=75,
                health_status="READY",
                now=datetime(2026, 7, 13, 14, 3, tzinfo=UTC),
            )
    assert not (tmp_path / "strategy_test_v1.heartbeat.json").exists()


def test_status_script_reports_source_required_without_network_or_key(
    tmp_path: Path,
) -> None:
    maps = tmp_path / "maps"
    maps.mkdir()
    _write_map(maps / "roll_map_historical.json", historical=True)
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/shadow_feed_status.py",
            "--state-dir",
            str(tmp_path / "shadow"),
            "--contract-map-dir",
            str(maps),
            "--roots",
            "MYM",
            "--as-of",
            "2026-07-11T12:00:00+00:00",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    status = json.loads(completed.stdout)

    assert status["status"] == "SOURCE_REQUIRED"
    assert status["scientific_conclusion"] == "FORWARD_DATA_SOURCE_REQUIRED"
    assert status["network_requests"] == 0
    assert status["incremental_databento_spend_usd"] == 0.0
    assert status["candidate_heartbeats_published"] == 0


def test_new_feed_surface_has_no_order_or_broker_capability() -> None:
    paths = [
        Path("hydra/shadow/contract_resolver.py"),
        Path("hydra/shadow/forward_bar_store.py"),
        Path("hydra/shadow/data_heartbeat.py"),
        Path("hydra/shadow/feed_health.py"),
        Path("scripts/shadow_feed_status.py"),
    ]

    audit = audit_zero_order_surface(paths)

    assert audit["passed"]
    assert audit["outbound_order_capability"] is False
