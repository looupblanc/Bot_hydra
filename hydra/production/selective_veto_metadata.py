"""Resumable, metadata-only Databento estimates for campaign 0034.

The cache stores only complete ``record_count / billable_size / cost`` triples.
It never invokes ``timeseries.get_range`` and therefore has no acquisition
capability.  Records contain no wall-clock timestamp so an interrupted run and
its resumed equivalent produce the same append-only material.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Iterable, Mapping, Protocol


CACHE_SCHEMA = "hydra_selective_veto_metadata_estimate_cache_v1"
MAX_METADATA_IO_WORKERS = 32


class SelectiveVetoMetadataError(RuntimeError):
    """A metadata estimate cannot be resumed without contract drift."""


class MetadataAPI(Protocol):
    def get_record_count(self, **kwargs: Any) -> int: ...

    def get_billable_size(self, **kwargs: Any) -> int: ...

    def get_cost(self, **kwargs: Any) -> float: ...


@dataclass(frozen=True, slots=True)
class MetadataRetryPolicy:
    maximum_endpoint_calls_per_second: float = 10.0
    maximum_retries: int = 3
    base_retry_seconds: float = 0.5
    maximum_retry_seconds: float = 30.0

    def validate(self) -> None:
        if (
            not math.isfinite(self.maximum_endpoint_calls_per_second)
            or not 0.0 < self.maximum_endpoint_calls_per_second <= 10.0
            or not 0 <= self.maximum_retries <= 5
            or not math.isfinite(self.base_retry_seconds)
            or self.base_retry_seconds <= 0.0
            or not math.isfinite(self.maximum_retry_seconds)
            or self.maximum_retry_seconds < self.base_retry_seconds
        ):
            raise SelectiveVetoMetadataError("invalid bounded metadata retry policy")


@dataclass(frozen=True, slots=True)
class MetadataEstimate:
    request_fingerprint: str
    request: Mapping[str, Any]
    estimated_records: int
    estimated_bytes: int
    estimated_cost_usd: float
    zero_records: bool
    estimate_hash: str

    @classmethod
    def create(
        cls,
        request: Mapping[str, Any],
        *,
        estimated_records: int,
        estimated_bytes: int,
        estimated_cost_usd: float,
    ) -> "MetadataEstimate":
        normalized = normalize_metadata_request(request)
        records = int(estimated_records)
        size = int(estimated_bytes)
        cost = float(estimated_cost_usd)
        if records < 0 or size < 0 or not math.isfinite(cost) or cost < 0.0:
            raise SelectiveVetoMetadataError("Databento metadata estimate is invalid")
        fingerprint = metadata_request_fingerprint(normalized)
        core = {
            "schema": CACHE_SCHEMA,
            "request_fingerprint": fingerprint,
            "request": normalized,
            "estimated_records": records,
            "estimated_bytes": size,
            "estimated_cost_usd": cost,
            "zero_records": records == 0,
        }
        return cls(
            request_fingerprint=fingerprint,
            request=normalized,
            estimated_records=records,
            estimated_bytes=size,
            estimated_cost_usd=cost,
            zero_records=records == 0,
            estimate_hash=_stable_hash(core),
        )

    @classmethod
    def from_record(cls, value: Mapping[str, Any]) -> "MetadataEstimate":
        if value.get("schema") != CACHE_SCHEMA:
            raise SelectiveVetoMetadataError("metadata cache schema drift")
        estimate = cls.create(
            _mapping(value.get("request"), "request"),
            estimated_records=int(value.get("estimated_records", -1)),
            estimated_bytes=int(value.get("estimated_bytes", -1)),
            estimated_cost_usd=float(value.get("estimated_cost_usd", math.nan)),
        )
        if (
            value.get("request_fingerprint") != estimate.request_fingerprint
            or value.get("zero_records") is not estimate.zero_records
            or value.get("estimate_hash") != estimate.estimate_hash
        ):
            raise SelectiveVetoMetadataError("metadata cache record hash drift")
        return estimate

    def to_record(self) -> dict[str, Any]:
        return {
            "schema": CACHE_SCHEMA,
            "request_fingerprint": self.request_fingerprint,
            "request": dict(self.request),
            "estimated_records": self.estimated_records,
            "estimated_bytes": self.estimated_bytes,
            "estimated_cost_usd": self.estimated_cost_usd,
            "zero_records": self.zero_records,
            "estimate_hash": self.estimate_hash,
        }


class AppendOnlyMetadataEstimateCache:
    """Crash-detecting JSONL cache with locked, fsynced appends."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records = self._read_path()
        self._file_size = self.path.stat().st_size if self.path.is_file() else 0

    def get(self, request: Mapping[str, Any]) -> MetadataEstimate | None:
        return self._records.get(metadata_request_fingerprint(request))

    def append(self, estimate: MetadataEstimate) -> MetadataEstimate:
        prior = self._records.get(estimate.request_fingerprint)
        if prior is not None:
            if prior.estimate_hash != estimate.estimate_hash:
                raise SelectiveVetoMetadataError(
                    "metadata estimate changed for an immutable request"
                )
            return prior
        encoded = (
            json.dumps(
                estimate.to_record(),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        created = not self.path.exists()
        descriptor = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            size_before = os.fstat(descriptor).st_size
            if size_before < self._file_size:
                raise SelectiveVetoMetadataError("metadata cache was truncated")
            current = dict(self._records)
            if size_before > self._file_size:
                additions = self._read_descriptor(
                    descriptor, offset=self._file_size
                )
                for fingerprint, additional in additions.items():
                    prior = current.get(fingerprint)
                    if prior is not None and prior.estimate_hash != additional.estimate_hash:
                        raise SelectiveVetoMetadataError(
                            "conflicting duplicate metadata cache fingerprint"
                        )
                    current[fingerprint] = additional
            prior = current.get(estimate.request_fingerprint)
            if prior is not None:
                if prior.estimate_hash != estimate.estimate_hash:
                    raise SelectiveVetoMetadataError(
                        "metadata estimate changed for an immutable request"
                    )
                self._records = current
                self._file_size = size_before
                return prior
            written = os.write(descriptor, encoded)
            if written != len(encoded):
                raise SelectiveVetoMetadataError("metadata cache append was incomplete")
            os.fsync(descriptor)
            current[estimate.request_fingerprint] = estimate
            self._records = current
            self._file_size = size_before + len(encoded)
            return estimate
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
            if created and self.path.exists():
                _fsync_directory(self.path.parent)

    def _read_path(self) -> dict[str, MetadataEstimate]:
        if not self.path.is_file():
            return {}
        descriptor = os.open(self.path, os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH)
            return self._read_descriptor(descriptor)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _read_descriptor(
        descriptor: int, *, offset: int = 0
    ) -> dict[str, MetadataEstimate]:
        os.lseek(descriptor, offset, os.SEEK_SET)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
        if raw and not raw.endswith(b"\n"):
            raise SelectiveVetoMetadataError("metadata cache has a truncated tail")
        records: dict[str, MetadataEstimate] = {}
        for line_number, line in enumerate(raw.splitlines(), start=1):
            if not line:
                continue
            try:
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SelectiveVetoMetadataError(
                    f"invalid metadata cache JSONL record {line_number}"
                ) from exc
            estimate = MetadataEstimate.from_record(
                _mapping(value, f"record {line_number}")
            )
            prior = records.get(estimate.request_fingerprint)
            if prior is not None and prior.estimate_hash != estimate.estimate_hash:
                raise SelectiveVetoMetadataError(
                    "conflicting duplicate metadata cache fingerprint"
                )
            records[estimate.request_fingerprint] = estimate
        return records


class ResilientMetadataEstimator:
    """Estimate metadata triples concurrently with bounded endpoint retries.

    Worker threads only perform I/O.  Cache inspection and append operations
    remain on the calling thread so an identical input order always produces
    an identical append-only cache, regardless of completion order.
    """

    def __init__(
        self,
        metadata: MetadataAPI,
        *,
        cache_path: str | Path,
        retry_policy: MetadataRetryPolicy | None = None,
        enforce_rate_limit: bool = True,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.metadata = metadata
        self.cache = AppendOnlyMetadataEstimateCache(cache_path)
        self.retry_policy = retry_policy or MetadataRetryPolicy()
        self.retry_policy.validate()
        self.enforce_rate_limit = bool(enforce_rate_limit)
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._last_endpoint_call: float | None = None
        # One start-rate gate is shared by every endpoint and worker owned by
        # this estimator.  Holding it across the short pacing sleep prevents
        # concurrent workers from reserving the same start slot.
        self._throttle_lock = threading.Lock()
        self._counter_lock = threading.Lock()
        self.endpoint_call_count = 0
        self.retry_count = 0
        self.cache_hit_count = 0
        self.cache_miss_count = 0

    def estimate(self, request: Mapping[str, Any]) -> MetadataEstimate:
        return self.estimate_many((request,))[0]

    def estimate_many(
        self,
        requests: Iterable[Mapping[str, Any]],
        *,
        max_workers: int = MAX_METADATA_IO_WORKERS,
    ) -> list[MetadataEstimate]:
        """Estimate unique requests concurrently and restore input ordering."""

        if isinstance(max_workers, bool) or not 1 <= int(max_workers) <= MAX_METADATA_IO_WORKERS:
            raise SelectiveVetoMetadataError(
                f"metadata I/O workers must be between 1 and {MAX_METADATA_IO_WORKERS}"
            )

        fingerprints: list[str] = []
        unique: dict[str, Mapping[str, Any]] = {}
        for request in requests:
            normalized = normalize_metadata_request(request)
            fingerprint = metadata_request_fingerprint(normalized)
            fingerprints.append(fingerprint)
            unique.setdefault(fingerprint, normalized)
        if not fingerprints:
            return []

        estimates: dict[str, MetadataEstimate] = {}
        misses: list[tuple[str, Mapping[str, Any]]] = []
        cache_hits = 0
        for fingerprint, normalized in unique.items():
            cached = self.cache.get(normalized)
            if cached is None:
                misses.append((fingerprint, normalized))
            else:
                estimates[fingerprint] = cached
                cache_hits += 1
        with self._counter_lock:
            self.cache_hit_count += cache_hits
            self.cache_miss_count += len(misses)

        if misses:
            worker_count = min(int(max_workers), len(misses))
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="selective-veto-metadata",
            ) as executor:
                futures = [
                    executor.submit(self._estimate_uncached, normalized)
                    for _, normalized in misses
                ]
                # Resolve in first-seen order, not completion order.  Only this
                # calling thread mutates the append-only cache.
                for (fingerprint, _), future in zip(misses, futures, strict=True):
                    estimates[fingerprint] = self.cache.append(future.result())

        return [estimates[fingerprint] for fingerprint in fingerprints]

    def _estimate_uncached(self, normalized: Mapping[str, Any]) -> MetadataEstimate:
        records = int(self._call("get_record_count", normalized))
        size = int(self._call("get_billable_size", normalized))
        cost = float(self._call("get_cost", normalized))
        return MetadataEstimate.create(
            normalized,
            estimated_records=records,
            estimated_bytes=size,
            estimated_cost_usd=cost,
        )

    def _call(self, method_name: str, request: Mapping[str, Any]) -> Any:
        method = getattr(self.metadata, method_name)
        for attempt in range(self.retry_policy.maximum_retries + 1):
            self._throttle()
            with self._counter_lock:
                self.endpoint_call_count += 1
            try:
                return method(**dict(request))
            except Exception as exc:
                status = _http_status(exc)
                retryable = status == 429 or (status is not None and 500 <= status <= 599)
                if not retryable or attempt >= self.retry_policy.maximum_retries:
                    raise SelectiveVetoMetadataError(
                        f"Databento metadata {method_name} failed"
                    ) from exc
                with self._counter_lock:
                    self.retry_count += 1
                delay = _retry_delay(exc, attempt, self.retry_policy)
                self._sleeper(delay)
        raise AssertionError("bounded metadata retry loop exhausted unexpectedly")

    def _throttle(self) -> None:
        with self._throttle_lock:
            now = self._monotonic()
            if self.enforce_rate_limit and self._last_endpoint_call is not None:
                interval = 1.0 / self.retry_policy.maximum_endpoint_calls_per_second
                remaining = interval - (now - self._last_endpoint_call)
                if remaining > 0.0:
                    self._sleeper(remaining)
                    now = self._monotonic()
            self._last_endpoint_call = now


def normalize_metadata_request(request: Mapping[str, Any]) -> dict[str, Any]:
    required = ("dataset", "symbols", "schema", "stype_in", "start", "end")
    missing = [name for name in required if name not in request]
    if missing:
        raise SelectiveVetoMetadataError(
            "metadata request fields missing: " + ", ".join(missing)
        )
    symbols_raw = request["symbols"]
    if isinstance(symbols_raw, (str, int)):
        symbols = [str(symbols_raw)]
    else:
        symbols = [str(value) for value in symbols_raw]
    normalized: dict[str, Any] = {
        "dataset": str(request["dataset"]),
        "symbols": symbols,
        "schema": str(request["schema"]),
        "stype_in": str(request["stype_in"]),
        "start": str(request["start"]),
        "end": str(request["end"]),
    }
    if not normalized["dataset"] or not symbols or not normalized["schema"]:
        raise SelectiveVetoMetadataError("metadata request identity is empty")
    if "limit" in request and request["limit"] is not None:
        normalized["limit"] = int(request["limit"])
    return normalized


def metadata_request_fingerprint(request: Mapping[str, Any]) -> str:
    return _stable_hash(normalize_metadata_request(request))


def _http_status(exc: Exception) -> int | None:
    for name in ("http_status", "status_code", "status"):
        value = getattr(exc, name, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _retry_delay(
    exc: Exception, attempt: int, policy: MetadataRetryPolicy
) -> float:
    status = _http_status(exc)
    if status == 429:
        headers = getattr(exc, "headers", None)
        if not isinstance(headers, Mapping):
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
        raw = None
        if isinstance(headers, Mapping):
            raw = next(
                (value for key, value in headers.items() if str(key).lower() == "retry-after"),
                None,
            )
        parsed = _parse_retry_after(raw)
        if parsed is not None:
            return min(max(parsed, 0.0), policy.maximum_retry_seconds)
    return min(
        policy.base_retry_seconds * (2**attempt),
        policy.maximum_retry_seconds,
    )


def _parse_retry_after(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max((parsed - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SelectiveVetoMetadataError(f"metadata cache {label} is not a mapping")
    return value


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "AppendOnlyMetadataEstimateCache",
    "CACHE_SCHEMA",
    "MAX_METADATA_IO_WORKERS",
    "MetadataEstimate",
    "MetadataRetryPolicy",
    "ResilientMetadataEstimator",
    "SelectiveVetoMetadataError",
    "metadata_request_fingerprint",
    "normalize_metadata_request",
]
