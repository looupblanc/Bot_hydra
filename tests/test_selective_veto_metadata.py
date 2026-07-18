from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from hydra.production.selective_veto_metadata import (
    CACHE_SCHEMA,
    AppendOnlyMetadataEstimateCache,
    MetadataRetryPolicy,
    ResilientMetadataEstimator,
    SelectiveVetoMetadataError,
    metadata_request_fingerprint,
)
from hydra.production.selective_veto_pilot import generate_targeted_cost_matrix


REQUEST = {
    "dataset": "GLBX.MDP3",
    "symbols": ["NQH4"],
    "schema": "tbbo",
    "stype_in": "raw_symbol",
    "start": "2024-01-02T14:00:00Z",
    "end": "2024-01-02T14:03:00Z",
}


class _Metadata:
    def __init__(self, *, records: int = 10, size: int = 1_000, cost: float = 0.25) -> None:
        self.records = records
        self.size = size
        self.cost = cost
        self.calls: list[str] = []

    def get_record_count(self, **_kwargs: object) -> int:
        self.calls.append("records")
        return self.records

    def get_billable_size(self, **_kwargs: object) -> int:
        self.calls.append("bytes")
        return self.size

    def get_cost(self, **_kwargs: object) -> float:
        self.calls.append("cost")
        return self.cost


def test_complete_triple_is_fsynced_once_and_resumed_without_endpoint_calls(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.jsonl"
    metadata = _Metadata()
    estimator = ResilientMetadataEstimator(
        metadata, cache_path=path, enforce_rate_limit=False
    )
    first = estimator.estimate(REQUEST)
    second = estimator.estimate(REQUEST)

    assert first == second
    assert metadata.calls == ["records", "bytes", "cost"]
    assert estimator.cache_miss_count == 1
    assert estimator.cache_hit_count == 1
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["schema"] == CACHE_SCHEMA
    assert rows[0]["request_fingerprint"] == metadata_request_fingerprint(REQUEST)

    fresh_metadata = _Metadata(records=999, size=999, cost=999.0)
    resumed = ResilientMetadataEstimator(
        fresh_metadata, cache_path=path, enforce_rate_limit=False
    ).estimate(REQUEST)
    assert resumed == first
    assert fresh_metadata.calls == []


def test_zero_record_estimate_is_explicit_and_resumable(tmp_path: Path) -> None:
    estimator = ResilientMetadataEstimator(
        _Metadata(records=0, size=0, cost=0.0),
        cache_path=tmp_path / "zero.jsonl",
        enforce_rate_limit=False,
    )
    estimate = estimator.estimate(REQUEST)
    assert estimate.zero_records is True
    assert estimate.estimated_records == 0
    assert estimate.estimated_bytes == 0
    assert estimate.estimated_cost_usd == 0.0
    cached = AppendOnlyMetadataEstimateCache(tmp_path / "zero.jsonl").get(REQUEST)
    assert cached is not None and cached.zero_records is True


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _HttpError(RuntimeError):
    def __init__(self, status: int, retry_after: str | None = None) -> None:
        super().__init__(f"HTTP {status}")
        self.http_status = status
        self.headers = {} if retry_after is None else {"Retry-After": retry_after}


class _RetryingMetadata:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.call_times: list[float] = []
        self.record_attempts = 0
        self.size_attempts = 0

    def _called(self) -> None:
        self.call_times.append(self.clock.now)

    def get_record_count(self, **_kwargs: object) -> int:
        self._called()
        self.record_attempts += 1
        if self.record_attempts == 1:
            raise _HttpError(429, "0.3")
        return 10

    def get_billable_size(self, **_kwargs: object) -> int:
        self._called()
        self.size_attempts += 1
        if self.size_attempts == 1:
            raise _HttpError(503)
        return 1_000

    def get_cost(self, **_kwargs: object) -> float:
        self._called()
        return 0.25


def test_rate_limit_retry_after_and_5xx_retries_are_bounded(tmp_path: Path) -> None:
    clock = _Clock()
    metadata = _RetryingMetadata(clock)
    estimator = ResilientMetadataEstimator(
        metadata,
        cache_path=tmp_path / "retry.jsonl",
        retry_policy=MetadataRetryPolicy(
            maximum_endpoint_calls_per_second=10.0,
            maximum_retries=2,
            base_retry_seconds=0.2,
            maximum_retry_seconds=1.0,
        ),
        enforce_rate_limit=True,
        monotonic=clock.monotonic,
        sleeper=clock.sleep,
    )
    estimate = estimator.estimate(REQUEST)

    assert estimate.estimated_records == 10
    assert estimator.endpoint_call_count == 5
    assert estimator.retry_count == 2
    assert all(
        right - left >= 0.1 - 1e-12
        for left, right in zip(metadata.call_times, metadata.call_times[1:])
    )
    assert any(value == pytest.approx(0.3) for value in clock.sleeps)
    assert any(value == pytest.approx(0.2) for value in clock.sleeps)


class _ConcurrentMetadata:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.maximum_active = 0
        self.calls: list[tuple[str, str]] = []
        self.start_times: list[float] = []

    def _value(self, endpoint: str, **kwargs: object) -> tuple[str, int]:
        symbol = str(list(kwargs["symbols"])[0])
        with self._lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            self.calls.append((endpoint, symbol))
            self.start_times.append(time.monotonic())
        # Deliberately complete later first-seen requests last.
        time.sleep(0.02 if symbol == "SLOW" else 0.005)
        with self._lock:
            self.active -= 1
        return symbol, {"SLOW": 30, "FAST": 20, "OTHER": 10}[symbol]

    def get_record_count(self, **kwargs: object) -> int:
        return self._value("records", **kwargs)[1]

    def get_billable_size(self, **kwargs: object) -> int:
        return self._value("bytes", **kwargs)[1] * 100

    def get_cost(self, **kwargs: object) -> float:
        return self._value("cost", **kwargs)[1] / 100.0


def _request_for(symbol: str) -> dict[str, object]:
    return {**REQUEST, "symbols": [symbol]}


def test_estimate_many_is_concurrent_deduplicated_and_appended_in_input_order(
    tmp_path: Path,
) -> None:
    metadata = _ConcurrentMetadata()
    path = tmp_path / "many.jsonl"
    estimator = ResilientMetadataEstimator(
        metadata, cache_path=path, enforce_rate_limit=False
    )
    requests = [
        _request_for("SLOW"),
        _request_for("FAST"),
        _request_for("SLOW"),
        _request_for("OTHER"),
    ]

    estimates = estimator.estimate_many(requests)

    assert [row.estimated_records for row in estimates] == [30, 20, 30, 10]
    assert estimates[0] is estimates[2]
    assert metadata.maximum_active > 1
    assert len(metadata.calls) == 3 * 3
    assert estimator.cache_miss_count == 3
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["request"]["symbols"][0] for row in rows] == [
        "SLOW",
        "FAST",
        "OTHER",
    ]

    resumed = estimator.estimate_many(reversed(requests))
    assert [row.estimated_records for row in resumed] == [10, 30, 20, 30]
    assert len(metadata.calls) == 9
    assert estimator.cache_hit_count == 3


def test_estimate_many_rate_limit_is_shared_by_all_worker_threads(
    tmp_path: Path,
) -> None:
    metadata = _ConcurrentMetadata()
    estimator = ResilientMetadataEstimator(
        metadata,
        cache_path=tmp_path / "rate-many.jsonl",
        enforce_rate_limit=True,
    )

    estimator.estimate_many(
        _request_for(symbol) for symbol in ("SLOW", "FAST", "OTHER")
    )

    assert len(metadata.start_times) == 9
    assert all(
        right - left >= 0.09
        for left, right in zip(metadata.start_times, metadata.start_times[1:])
    )


def test_truncated_or_conflicting_append_only_cache_fails_closed(tmp_path: Path) -> None:
    truncated = tmp_path / "truncated.jsonl"
    truncated.write_text('{"schema":"incomplete"')
    with pytest.raises(SelectiveVetoMetadataError, match="truncated tail"):
        AppendOnlyMetadataEstimateCache(truncated)

    valid = tmp_path / "valid.jsonl"
    estimator = ResilientMetadataEstimator(
        _Metadata(), cache_path=valid, enforce_rate_limit=False
    )
    estimator.estimate(REQUEST)
    row = json.loads(valid.read_text())
    row["estimated_cost_usd"] = 999.0
    valid.write_text(valid.read_text() + json.dumps(row, sort_keys=True) + "\n")
    with pytest.raises(SelectiveVetoMetadataError, match="hash drift"):
        AppendOnlyMetadataEstimateCache(valid)


def test_full_cost_grid_resumes_from_cache_and_flags_zero_record_windows(
    tmp_path: Path,
) -> None:
    anchors = []
    start = datetime(2024, 1, 2, 14, 0, tzinfo=UTC)
    for day in range(10):
        timestamp = int((start + timedelta(days=day)).timestamp() * 1e9)
        session = (start + timedelta(days=day)).date().isoformat()
        anchors.extend(
            SimpleNamespace(
                market="NQ",
                contract="NQH4",
                decision_time_ns=timestamp,
                anchor_event_id=f"anchor-{day:02d}-{index:03d}",
                session_id=session,
            )
            for index in range(100)
        )
    audit = {
        "seeds": [
            {
                "policy_id": "hybrid_0033_01_f0345ecb99af8c25",
                "market_attribution": [
                    {"market": "NQ", "stressed_net_usd": 1.0},
                    {"market": "YM", "stressed_net_usd": 0.0},
                ],
            }
        ]
    }
    metadata = _Metadata(records=0, size=0, cost=0.0)
    cache = tmp_path / "grid.jsonl"

    first = generate_targeted_cost_matrix(
        metadata, anchors, audit, metadata_cache_path=cache
    )
    call_count = len(metadata.calls)
    second = generate_targeted_cost_matrix(
        metadata, anchors, audit, metadata_cache_path=cache
    )
    sequential = generate_targeted_cost_matrix(
        _Metadata(records=0, size=0, cost=0.0), anchors, audit
    )

    assert len(first["rows"]) == 24
    assert first == second
    assert first["rows"] == sequential["rows"]
    assert first["selected_offer"] == sequential["selected_offer"]
    assert first["chronological_role_costs"] == sequential["chronological_role_costs"]
    assert first["selected_offer"] is None
    assert all(row["contains_zero_record_windows"] for row in first["rows"])
    assert all(row["zero_record_window_count"] > 0 for row in first["rows"])
    assert len(metadata.calls) == call_count
    assert call_count == 90
    assert len(cache.read_text().splitlines()) == 30
