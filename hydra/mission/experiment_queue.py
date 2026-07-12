from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from hydra.utils.time import utc_now_iso


TERMINAL_EXPERIMENT_STATES = {"COMPLETED", "FAILED", "BLOCKED"}


class ExperimentSpecificationConflict(RuntimeError):
    pass


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def ensure_experiment_schema(conn: sqlite3.Connection) -> None:
    """Add lifecycle metadata without rebuilding or deleting the v1 table."""
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(experiments)").fetchall()}
    additions = {
        "experiment_type": "TEXT",
        "specification_hash": "TEXT",
        "result": "TEXT",
        "priority": "REAL NOT NULL DEFAULT 0",
        "attempt_count": "INTEGER NOT NULL DEFAULT 0",
        "max_attempts": "INTEGER NOT NULL DEFAULT 3",
        "created_at": "TEXT",
        "started_at": "TEXT",
        "completed_at": "TEXT",
        "last_error": "TEXT",
        "claim_token": "TEXT",
        "claimed_by": "TEXT",
        "lease_expires_at": "TEXT",
    }
    for name, declaration in additions.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE experiments ADD COLUMN {name} {declaration}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_experiments_schedulable "
        "ON experiments(status, priority DESC, updated_at, experiment_id)"
    )
    legacy_rows = conn.execute(
        "SELECT experiment_id, payload, updated_at FROM experiments "
        "WHERE specification_hash IS NULL OR experiment_type IS NULL OR created_at IS NULL"
    ).fetchall()
    for experiment_id, payload_text, updated_at in legacy_rows:
        payload = json.loads(payload_text)
        conn.execute(
            "UPDATE experiments SET experiment_type=COALESCE(experiment_type, ?), "
            "specification_hash=COALESCE(specification_hash, ?), created_at=COALESCE(created_at, ?) "
            "WHERE experiment_id=?",
            (
                str(payload.get("experiment_type") or experiment_id),
                _stable_hash(payload),
                updated_at,
                experiment_id,
            ),
        )
    conn.execute("PRAGMA user_version=2")
    conn.commit()


def enqueue_experiment(conn: sqlite3.Connection, experiment_id: str, payload: dict[str, Any]) -> bool:
    ensure_experiment_schema(conn)
    specification = dict(payload)
    experiment_type = str(specification.get("experiment_type") or experiment_id)
    specification_hash = _stable_hash(specification)
    existing = conn.execute(
        "SELECT payload, specification_hash FROM experiments WHERE experiment_id=?",
        (experiment_id,),
    ).fetchone()
    if existing is not None:
        existing_payload = json.loads(existing[0])
        existing_hash = str(existing[1] or _stable_hash(existing_payload))
        if existing_hash != specification_hash:
            raise ExperimentSpecificationConflict(
                f"Experiment {experiment_id} already exists with a different immutable specification."
            )
        return False
    now = utc_now_iso()
    conn.execute(
        "INSERT INTO experiments("
        "experiment_id,status,payload,updated_at,experiment_type,specification_hash,priority,"
        "attempt_count,max_attempts,created_at"
        ") VALUES (?, 'QUEUED', ?, ?, ?, ?, ?, 0, ?, ?)",
        (
            experiment_id,
            json.dumps(specification, sort_keys=True, default=str),
            now,
            experiment_type,
            specification_hash,
            float(specification.get("priority", 0.0)),
            int(specification.get("max_attempts", 3)),
            now,
        ),
    )
    conn.commit()
    return True


def claim_next_experiment(
    conn: sqlite3.Connection, *, claimed_by: str = "mission_controller", lease_seconds: float = 180.0
) -> dict[str, Any] | None:
    ensure_experiment_schema(conn)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT experiment_id, payload, experiment_type, specification_hash, attempt_count, max_attempts "
            "FROM experiments WHERE status='QUEUED' "
            "ORDER BY priority DESC, updated_at, experiment_id LIMIT 1"
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        now = utc_now_iso()
        claim_token = uuid.uuid4().hex
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(float(lease_seconds), 1.0))
        ).replace(microsecond=0).isoformat()
        updated = conn.execute(
            "UPDATE experiments SET status='RUNNING', started_at=?, updated_at=?, "
            "attempt_count=attempt_count+1, last_error=NULL, claim_token=?, claimed_by=?, lease_expires_at=? "
            "WHERE experiment_id=? AND status='QUEUED'",
            (now, now, claim_token, claimed_by, lease_expires_at, row[0]),
        ).rowcount
        if updated != 1:
            conn.rollback()
            return None
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    specification = json.loads(row[1])
    return {
        **specification,
        "experiment_id": str(row[0]),
        "experiment_type": str(row[2] or specification.get("experiment_type") or row[0]),
        "specification_hash": str(row[3] or _stable_hash(specification)),
        "attempt_count": int(row[4]) + 1,
        "max_attempts": int(row[5]),
        "claim_token": claim_token,
        "claimed_by": claimed_by,
        "lease_expires_at": lease_expires_at,
    }


def peek_next_experiment(conn: sqlite3.Connection) -> dict[str, Any] | None:
    ensure_experiment_schema(conn)
    row = conn.execute(
        "SELECT experiment_id,payload,experiment_type,specification_hash,attempt_count,max_attempts "
        "FROM experiments WHERE status='QUEUED' "
        "ORDER BY priority DESC, updated_at, experiment_id LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    specification = json.loads(row[1])
    return {
        **specification,
        "experiment_id": str(row[0]),
        "experiment_type": str(row[2] or specification.get("experiment_type") or row[0]),
        "specification_hash": str(row[3] or _stable_hash(specification)),
        "attempt_count": int(row[4]),
        "max_attempts": int(row[5]),
    }


def complete_experiment(
    conn: sqlite3.Connection,
    experiment_id: str,
    result: dict[str, Any],
    *,
    claim_token: str,
) -> None:
    ensure_experiment_schema(conn)
    now = utc_now_iso()
    parameters: tuple[Any, ...] = (
        json.dumps(result, sort_keys=True, default=str),
        now,
        now,
        experiment_id,
        claim_token,
    )
    updated = conn.execute(
        "UPDATE experiments SET status='COMPLETED', result=?, completed_at=?, updated_at=?, last_error=NULL, "
        "claim_token=NULL, claimed_by=NULL, lease_expires_at=NULL "
        "WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
        parameters,
    ).rowcount
    if updated != 1:
        conn.rollback()
        raise RuntimeError(f"Experiment {experiment_id} is not in RUNNING state.")
    conn.commit()


def fail_experiment(
    conn: sqlite3.Connection,
    experiment_id: str,
    error: str,
    *,
    retryable: bool = True,
    claim_token: str,
) -> str:
    ensure_experiment_schema(conn)
    row = conn.execute(
        "SELECT attempt_count, max_attempts, claim_token FROM experiments WHERE experiment_id=? AND status='RUNNING'",
        (experiment_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Experiment {experiment_id} is not in RUNNING state.")
    if str(row[2]) != claim_token:
        raise RuntimeError(f"Experiment {experiment_id} claim token is stale.")
    if retryable and int(row[0]) < int(row[1]):
        status = "QUEUED"
    else:
        status = "FAILED"
    updated = conn.execute(
        "UPDATE experiments SET status=?, updated_at=?, last_error=?, claim_token=NULL, claimed_by=NULL, "
        "lease_expires_at=NULL WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
        (status, utc_now_iso(), str(error)[:4000], experiment_id, claim_token),
    ).rowcount
    if updated != 1:
        conn.rollback()
        raise RuntimeError(f"Experiment {experiment_id} claim token is stale.")
    conn.commit()
    return status


def block_experiment(
    conn: sqlite3.Connection, experiment_id: str, error: str, *, claim_token: str
) -> None:
    ensure_experiment_schema(conn)
    updated = conn.execute(
        "UPDATE experiments SET status='BLOCKED', updated_at=?, last_error=?, claim_token=NULL, claimed_by=NULL, "
        "lease_expires_at=NULL WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
        (utc_now_iso(), str(error)[:4000], experiment_id, claim_token),
    ).rowcount
    if updated != 1:
        conn.rollback()
        raise RuntimeError(f"Experiment {experiment_id} claim token is stale.")
    conn.commit()


def block_queued_experiments_by_type(
    conn: sqlite3.Connection,
    experiment_type: str,
    reason: str,
) -> list[str]:
    """Terminally retire queued work superseded by a scientific stop rule.

    This is intentionally limited to unclaimed rows.  Running experiments must
    still follow normal lease/shutdown handling, so the controller can never
    invalidate work owned by another process.
    """

    ensure_experiment_schema(conn)
    rows = conn.execute(
        "SELECT experiment_id FROM experiments WHERE status='QUEUED' "
        "AND experiment_type=? ORDER BY experiment_id",
        (str(experiment_type),),
    ).fetchall()
    experiment_ids = [str(row[0]) for row in rows]
    if not experiment_ids:
        return []
    updated = conn.execute(
        "UPDATE experiments SET status='BLOCKED', updated_at=?, last_error=?, "
        "claim_token=NULL, claimed_by=NULL, lease_expires_at=NULL "
        "WHERE status='QUEUED' AND experiment_type=?",
        (utc_now_iso(), str(reason)[:4000], str(experiment_type)),
    ).rowcount
    if int(updated) != len(experiment_ids):
        conn.rollback()
        raise RuntimeError(
            "Queued experiment retirement raced with another scheduler claim."
        )
    conn.commit()
    return experiment_ids


def release_experiment_claim_for_shutdown(
    conn: sqlite3.Connection, experiment_id: str, *, claim_token: str, reason: str
) -> None:
    """Return interrupted work to the queue without consuming a scientific retry."""
    ensure_experiment_schema(conn)
    updated = conn.execute(
        "UPDATE experiments SET status='QUEUED', updated_at=?, last_error=?, "
        "attempt_count=CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END, "
        "started_at=NULL, claim_token=NULL, claimed_by=NULL, lease_expires_at=NULL "
        "WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
        (utc_now_iso(), str(reason)[:4000], experiment_id, claim_token),
    ).rowcount
    if updated != 1:
        conn.rollback()
        raise RuntimeError(f"Experiment {experiment_id} claim token is stale.")
    conn.commit()


def recover_running_experiments(conn: sqlite3.Connection) -> dict[str, int]:
    ensure_experiment_schema(conn)
    rows = conn.execute(
        "SELECT experiment_id, attempt_count, max_attempts FROM experiments WHERE status='RUNNING'"
    ).fetchall()
    requeued = 0
    failed = 0
    now = utc_now_iso()
    for experiment_id, attempt_count, max_attempts in rows:
        if int(attempt_count) < int(max_attempts):
            status = "QUEUED"
            requeued += 1
        else:
            status = "FAILED"
            failed += 1
        conn.execute(
            "UPDATE experiments SET status=?, updated_at=?, last_error=?, claim_token=NULL, claimed_by=NULL, "
            "lease_expires_at=NULL WHERE experiment_id=?",
            (status, now, "controller_restart_recovery", experiment_id),
        )
    conn.commit()
    return {"requeued": requeued, "failed": failed}


def recover_resolved_missing_handler_experiments(
    conn: sqlite3.Connection, experiment_type: str
) -> int:
    """Requeue only legacy rows blocked because their exact handler was absent.

    New controllers detect missing handlers before claim.  This narrow recovery
    exists so a row blocked by an older controller can resume after the handler
    has been installed, without reopening governance or scientific failures.
    """
    ensure_experiment_schema(conn)
    updated = conn.execute(
        "UPDATE experiments SET status='QUEUED', updated_at=?, last_error=?, "
        "attempt_count=CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END, "
        "started_at=NULL, claim_token=NULL, claimed_by=NULL, lease_expires_at=NULL "
        "WHERE status='BLOCKED' AND experiment_type=? "
        "AND last_error LIKE 'No approved handler for experiment type%'",
        (utc_now_iso(), "missing_handler_resolved", str(experiment_type)),
    ).rowcount
    conn.commit()
    return int(updated)


def renew_experiment_lease(
    conn: sqlite3.Connection, experiment_id: str, claim_token: str, *, lease_seconds: float = 180.0
) -> str:
    lease_expires_at = (
        datetime.now(timezone.utc) + timedelta(seconds=max(float(lease_seconds), 1.0))
    ).replace(microsecond=0).isoformat()
    updated = conn.execute(
        "UPDATE experiments SET lease_expires_at=?, updated_at=? "
        "WHERE experiment_id=? AND status='RUNNING' AND claim_token=?",
        (lease_expires_at, utc_now_iso(), experiment_id, claim_token),
    ).rowcount
    if updated != 1:
        conn.rollback()
        raise RuntimeError(f"Experiment {experiment_id} lease cannot be renewed with a stale claim.")
    conn.commit()
    return lease_expires_at


def queue_size(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM experiments WHERE status='QUEUED'").fetchone()[0])


def experiment_counts(conn: sqlite3.Connection) -> dict[str, int]:
    counts = {"QUEUED": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0, "BLOCKED": 0}
    for status, count in conn.execute("SELECT status, COUNT(*) FROM experiments GROUP BY status"):
        counts[str(status)] = int(count)
    counts["TOTAL"] = sum(value for key, value in counts.items() if key != "TOTAL")
    return counts


def experiment_record(conn: sqlite3.Connection, experiment_id: str) -> dict[str, Any] | None:
    ensure_experiment_schema(conn)
    row = conn.execute(
        "SELECT experiment_id,status,payload,updated_at,experiment_type,specification_hash,result,"
        "attempt_count,max_attempts,created_at,started_at,completed_at,last_error,claim_token,claimed_by,lease_expires_at "
        "FROM experiments WHERE experiment_id=?",
        (experiment_id,),
    ).fetchone()
    if row is None:
        return None
    return _record_from_row(row)


def latest_completed_experiment(conn: sqlite3.Connection) -> dict[str, Any] | None:
    ensure_experiment_schema(conn)
    row = conn.execute(
        "SELECT experiment_id,status,payload,updated_at,experiment_type,specification_hash,result,"
        "attempt_count,max_attempts,created_at,started_at,completed_at,last_error,claim_token,claimed_by,lease_expires_at "
        "FROM experiments WHERE status='COMPLETED' ORDER BY completed_at DESC, experiment_id DESC LIMIT 1"
    ).fetchone()
    return None if row is None else _record_from_row(row)


def _record_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "experiment_id": str(row[0]),
        "status": str(row[1]),
        "specification": json.loads(row[2]),
        "updated_at": row[3],
        "experiment_type": row[4],
        "specification_hash": row[5],
        "result": json.loads(row[6]) if row[6] else None,
        "attempt_count": int(row[7]),
        "max_attempts": int(row[8]),
        "created_at": row[9],
        "started_at": row[10],
        "completed_at": row[11],
        "last_error": row[12],
        "claim_token": row[13],
        "claimed_by": row[14],
        "lease_expires_at": row[15],
    }
