from __future__ import annotations

import fcntl
import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterator

from hydra.utils.config import project_path
from hydra.utils.time import utc_now_iso


MISSION_SCHEMA_VERSION = "mission_state_v1"


@dataclass(frozen=True)
class MissionPaths:
    state_dir: Path
    db_path: Path
    decision_ledger: Path
    evidence_ledger: Path
    engineering_ledger: Path
    data_access_ledger: Path
    heartbeat_path: Path
    lock_path: Path
    stop_path: Path


def mission_paths(state_dir: str = "mission/state") -> MissionPaths:
    root = project_path(state_dir)
    return MissionPaths(
        state_dir=root,
        db_path=root / "hydra_mission.db",
        decision_ledger=root / "decision_ledger.jsonl",
        evidence_ledger=root / "evidence_ledger.jsonl",
        engineering_ledger=root / "engineering_ledger.jsonl",
        data_access_ledger=root / "data_access_ledger.jsonl",
        heartbeat_path=root / "heartbeat.json",
        lock_path=root / "hydra_mission.lock",
        stop_path=root / "STOP",
    )


def connect_state(paths: MissionPaths) -> sqlite3.Connection:
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(paths.db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS experiments (experiment_id TEXT PRIMARY KEY, status TEXT NOT NULL, payload TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    conn.commit()
    return conn


def connect_state_readonly(paths: MissionPaths) -> sqlite3.Connection:
    """Open existing mission state without DDL, WAL changes, or commits."""
    if not paths.db_path.exists():
        raise FileNotFoundError(paths.db_path)
    conn = sqlite3.connect(f"file:{paths.db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


def set_kv(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES (?, ?, ?)",
        (key, json.dumps(value, sort_keys=True, default=str), utc_now_iso()),
    )
    conn.commit()


def get_kv(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
    return default if row is None else json.loads(row[0])


def append_event(conn: sqlite3.Connection, event_type: str, payload: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO events(event_type, payload, created_at) VALUES (?, ?, ?)",
        (event_type, json.dumps(payload, sort_keys=True, default=str), utc_now_iso()),
    )
    conn.commit()


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def write_heartbeat(paths: MissionPaths, payload: dict[str, Any]) -> None:
    payload = {"heartbeat_at_utc": utc_now_iso(), "pid": os.getpid(), **payload}
    paths.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = paths.heartbeat_path.with_name(f".{paths.heartbeat_path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(temporary, paths.heartbeat_path)


def request_stop(paths: MissionPaths, reason: str = "manual_stop") -> None:
    paths.stop_path.parent.mkdir(parents=True, exist_ok=True)
    paths.stop_path.write_text(json.dumps({"requested_at_utc": utc_now_iso(), "reason": reason}, sort_keys=True), encoding="utf-8")


def clear_stop(paths: MissionPaths) -> None:
    if paths.stop_path.exists():
        paths.stop_path.unlink()


def stop_requested(paths: MissionPaths) -> bool:
    return paths.stop_path.exists()


@contextmanager
def mission_lock(paths: MissionPaths) -> Iterator[None]:
    paths.state_dir.mkdir(parents=True, exist_ok=True)
    with paths.lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another HYDRA autonomous mission instance already holds the lock.") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def state_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute("SELECT key, value FROM kv ORDER BY key").fetchall()
    return {key: json.loads(value) for key, value in rows}


def event_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"created_at_utc": utc_now_iso(), "event_type": event_type, **payload}


def dataclass_payload(item: Any) -> dict[str, Any]:
    return asdict(item) if hasattr(item, "__dataclass_fields__") else dict(item)
