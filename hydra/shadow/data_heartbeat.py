from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from hydra.shadow.contract_resolver import ResolvedContract


HEARTBEAT_SCHEMA = "hydra_forward_data_heartbeat_v1"
SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class ForwardHeartbeatError(RuntimeError):
    pass


@dataclass(frozen=True)
class ForwardDataHeartbeat:
    source_id: str
    dataset: str
    explicit_contracts: tuple[tuple[str, str], ...]
    latest_completed_bar_at_utc: str
    observed_at_utc: str
    source_sequence: int
    source_payload_checksum: str
    store_checkpoint: str
    schema: str = HEARTBEAT_SCHEMA

    @classmethod
    def build(
        cls,
        *,
        source_id: str,
        dataset: str,
        contracts: Mapping[str, ResolvedContract | str],
        latest_completed_bar_at: datetime,
        observed_at: datetime,
        source_sequence: int,
        source_payload_checksum: str,
        store_checkpoint: str,
    ) -> "ForwardDataHeartbeat":
        explicit = tuple(
            sorted(
                (
                    str(root),
                    value.contract if isinstance(value, ResolvedContract) else str(value),
                )
                for root, value in contracts.items()
            )
        )
        heartbeat = cls(
            source_id=str(source_id),
            dataset=str(dataset),
            explicit_contracts=explicit,
            latest_completed_bar_at_utc=_utc(latest_completed_bar_at).isoformat(),
            observed_at_utc=_utc(observed_at).isoformat(),
            source_sequence=int(source_sequence),
            source_payload_checksum=str(source_payload_checksum),
            store_checkpoint=str(store_checkpoint),
        )
        heartbeat.validate()
        return heartbeat

    @property
    def heartbeat_checksum(self) -> str:
        raw = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()

    def validate(self) -> None:
        if self.schema != HEARTBEAT_SCHEMA:
            raise ForwardHeartbeatError("Unsupported forward heartbeat schema.")
        if not self.source_id or not self.dataset or not self.explicit_contracts:
            raise ForwardHeartbeatError("Source, dataset and explicit contracts are required.")
        if not SAFE_ID.fullmatch(self.source_id):
            raise ForwardHeartbeatError("Unsafe heartbeat source identity.")
        roots = [root for root, _contract in self.explicit_contracts]
        contracts = [contract for _root, contract in self.explicit_contracts]
        if len(set(roots)) != len(roots) or any(not value for value in contracts):
            raise ForwardHeartbeatError("Explicit contract mapping is incomplete or duplicated.")
        latest = _utc(self.latest_completed_bar_at_utc)
        observed = _utc(self.observed_at_utc)
        if latest > observed:
            raise ForwardHeartbeatError("Future completed-bar timestamps are prohibited.")
        if self.source_sequence <= 0:
            raise ForwardHeartbeatError("Heartbeat sequence must be positive.")
        for value, label in (
            (self.source_payload_checksum, "source payload checksum"),
            (self.store_checkpoint, "store checkpoint"),
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ForwardHeartbeatError(f"Invalid {label}.")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        payload = asdict(self)
        payload["explicit_contracts"] = dict(self.explicit_contracts)
        payload["heartbeat_checksum"] = self.heartbeat_checksum
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ForwardDataHeartbeat":
        supplied = str(payload.get("heartbeat_checksum") or "")
        contracts = payload.get("explicit_contracts") or {}
        if not isinstance(contracts, Mapping):
            raise ForwardHeartbeatError("Heartbeat explicit contracts must be a mapping.")
        heartbeat = cls(
            schema=str(payload.get("schema") or ""),
            source_id=str(payload.get("source_id") or ""),
            dataset=str(payload.get("dataset") or ""),
            explicit_contracts=tuple(sorted((str(k), str(v)) for k, v in contracts.items())),
            latest_completed_bar_at_utc=str(payload.get("latest_completed_bar_at_utc") or ""),
            observed_at_utc=str(payload.get("observed_at_utc") or ""),
            source_sequence=int(payload.get("source_sequence") or 0),
            source_payload_checksum=str(payload.get("source_payload_checksum") or ""),
            store_checkpoint=str(payload.get("store_checkpoint") or ""),
        )
        heartbeat.validate()
        if supplied != heartbeat.heartbeat_checksum:
            raise ForwardHeartbeatError("Heartbeat checksum does not recompute.")
        return heartbeat


class HeartbeatPublisher:
    """Atomic publisher for the per-candidate files consumed by shadow_pipeline."""

    def __init__(self, forward_data_dir: str | Path) -> None:
        self.root = Path(forward_data_dir)
        self.lock_path = self.root / ".heartbeat_writer.lock"
        self.audit_path = self.root / "heartbeat_audit.jsonl"

    @contextmanager
    def writer(self, *, writer_id: str) -> Iterator["HeartbeatWriter"]:
        if not writer_id or not SAFE_ID.fullmatch(writer_id):
            raise ValueError("A safe stable heartbeat writer identity is required.")
        self.root.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another forward-heartbeat writer holds the lock.") from exc
            lock.seek(0)
            lock.truncate()
            lock.write(f"{os.getpid()}:{writer_id}")
            lock.flush()
            try:
                yield HeartbeatWriter(self, writer_id)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)


class HeartbeatWriter:
    def __init__(self, publisher: HeartbeatPublisher, writer_id: str) -> None:
        self.publisher = publisher
        self.writer_id = writer_id

    def publish(
        self,
        *,
        candidate_id: str,
        heartbeat: ForwardDataHeartbeat,
        required_roots: tuple[str, ...],
        stale_data_seconds: int,
        health_status: str,
        now: datetime,
    ) -> Path:
        if not SAFE_ID.fullmatch(candidate_id):
            raise ForwardHeartbeatError("Unsafe candidate identity.")
        heartbeat.validate()
        current = _utc(now)
        mapped = dict(heartbeat.explicit_contracts)
        if health_status != "READY":
            raise ForwardHeartbeatError(
                f"Synthetic freshness prohibited while feed health is {health_status}."
            )
        if set(required_roots) - set(mapped):
            raise ForwardHeartbeatError("Candidate contract coverage is incomplete.")
        age = (current - _utc(heartbeat.latest_completed_bar_at_utc)).total_seconds()
        if age < 0 or age > stale_data_seconds:
            raise ForwardHeartbeatError("Stale or future forward data cannot publish freshness.")
        path = self.publisher.root / f"{candidate_id}.heartbeat.json"
        previous: ForwardDataHeartbeat | None = None
        if path.exists():
            try:
                previous = ForwardDataHeartbeat.from_dict(
                    json.loads(path.read_text(encoding="utf-8"))
                )
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ForwardHeartbeatError("Existing heartbeat is invalid; fail closed.") from exc
        if previous:
            if heartbeat.source_id != previous.source_id or heartbeat.dataset != previous.dataset:
                raise ForwardHeartbeatError("Heartbeat source identity cannot change in place.")
            if heartbeat.source_sequence <= previous.source_sequence:
                raise ForwardHeartbeatError("Heartbeat sequence must increase monotonically.")
            if _utc(heartbeat.latest_completed_bar_at_utc) < _utc(
                previous.latest_completed_bar_at_utc
            ):
                raise ForwardHeartbeatError("Completed-bar time cannot move backward.")
            if _utc(heartbeat.observed_at_utc) < _utc(previous.observed_at_utc):
                raise ForwardHeartbeatError("Observation time cannot move backward.")
        payload = heartbeat.to_dict()
        payload.update(
            candidate_id=candidate_id,
            required_roots=list(required_roots),
            health_status="READY",
            stale_data_seconds=int(stale_data_seconds),
            outbound_orders=0,
            broker_connections=0,
        )
        _atomic_json(path, payload)
        self._append_audit(
            {
                "event": "FORWARD_HEARTBEAT_PUBLISHED",
                "candidate_id": candidate_id,
                "writer_id": self.writer_id,
                "observed_at_utc": heartbeat.observed_at_utc,
                "latest_completed_bar_at_utc": heartbeat.latest_completed_bar_at_utc,
                "source_sequence": heartbeat.source_sequence,
                "heartbeat_checksum": heartbeat.heartbeat_checksum,
                "outbound_orders": 0,
            }
        )
        return path

    def _append_audit(self, payload: dict[str, Any]) -> None:
        previous_hash = "0" * 64
        if self.publisher.audit_path.exists():
            lines = [
                line
                for line in self.publisher.audit_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            if lines:
                previous_hash = str(json.loads(lines[-1]).get("event_hash") or previous_hash)
        event = {"schema": "hydra_forward_heartbeat_audit_v1", **payload, "previous_hash": previous_hash}
        raw = json.dumps(event, sort_keys=True, separators=(",", ":"))
        event["event_hash"] = hashlib.sha256(raw.encode()).hexdigest()
        with self.publisher.audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _utc(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ForwardHeartbeatError("Naive heartbeat timestamps are prohibited.")
    return parsed.astimezone(timezone.utc)
