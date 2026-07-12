"""Atomic file-only result serialization owned by the control-plane PID."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


class ResultWriterError(RuntimeError):
    pass


@dataclass(frozen=True)
class AtomicWriteReceipt:
    relative_path: str
    sha256: str
    size_bytes: int
    idempotent_existing: bool
    writer_pid: int


class AtomicResultWriter:
    """Single-owner writer for result artifacts, never SQLite or registries."""

    def __init__(self, root: str | Path, *, immutable: bool = True) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.immutable = bool(immutable)
        self._owner_pid = os.getpid()
        self._lock = threading.Lock()

    def write_json(self, relative_path: str | Path, value: Any) -> AtomicWriteReceipt:
        payload = (
            json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
        ).encode("utf-8")
        return self.write_bytes(relative_path, payload)

    def write_jsonl_batch(
        self, relative_path: str | Path, rows: list[Mapping[str, Any]]
    ) -> AtomicWriteReceipt:
        payload = "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
            for row in rows
        ).encode("utf-8")
        return self.write_bytes(relative_path, payload)

    def write_text(self, relative_path: str | Path, text: str) -> AtomicWriteReceipt:
        return self.write_bytes(relative_path, text.encode("utf-8"))

    def write_bytes(
        self, relative_path: str | Path, payload: bytes
    ) -> AtomicWriteReceipt:
        self._check_owner()
        destination = self._resolve(relative_path)
        digest = hashlib.sha256(payload).hexdigest()
        with self._lock:
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                existing = destination.read_bytes()
                if existing == payload:
                    return self._receipt(destination, digest, len(payload), True)
                if self.immutable:
                    raise ResultWriterError(
                        f"refusing to overwrite divergent immutable result: {destination}"
                    )
            temporary = destination.with_name(
                f".{destination.name}.tmp-{self._owner_pid}-{threading.get_ident()}"
            )
            try:
                with temporary.open("xb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, destination)
                directory_fd = os.open(destination.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                if temporary.exists():
                    temporary.unlink()
        return self._receipt(destination, digest, len(payload), False)

    def write_batch(
        self, artifacts: Mapping[str | Path, bytes]
    ) -> tuple[AtomicWriteReceipt, ...]:
        # The caller publishes any manifest last.  Every artifact is individually
        # atomic and this method ensures no interleaving by another coordinator
        # thread.
        self._check_owner()
        with self._lock:
            receipts = [self._write_bytes_locked(path, payload) for path, payload in artifacts.items()]
        return tuple(receipts)

    def _write_bytes_locked(
        self, relative_path: str | Path, payload: bytes
    ) -> AtomicWriteReceipt:
        destination = self._resolve(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(payload).hexdigest()
        if destination.exists():
            existing = destination.read_bytes()
            if existing == payload:
                return self._receipt(destination, digest, len(payload), True)
            if self.immutable:
                raise ResultWriterError(
                    f"refusing to overwrite divergent immutable result: {destination}"
                )
        temporary = destination.with_name(
            f".{destination.name}.tmp-{self._owner_pid}-{threading.get_ident()}"
        )
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if temporary.exists():
                temporary.unlink()
        return self._receipt(destination, digest, len(payload), False)

    def _resolve(self, relative_path: str | Path) -> Path:
        path = Path(relative_path)
        if path.is_absolute():
            raise ResultWriterError("result path must be relative")
        destination = (self.root / path).resolve()
        try:
            destination.relative_to(self.root)
        except ValueError as exc:
            raise ResultWriterError("result path escapes writer root") from exc
        if destination.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            raise ResultWriterError("result writer cannot write database files")
        return destination

    def _check_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise ResultWriterError("result writer is bound to its coordinator PID")

    def _receipt(
        self, destination: Path, digest: str, size: int, existing: bool
    ) -> AtomicWriteReceipt:
        return AtomicWriteReceipt(
            relative_path=str(destination.relative_to(self.root)),
            sha256=digest,
            size_bytes=size,
            idempotent_existing=existing,
            writer_pid=self._owner_pid,
        )
