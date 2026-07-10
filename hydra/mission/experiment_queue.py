from __future__ import annotations

import json
import sqlite3
from typing import Any

from hydra.utils.time import utc_now_iso


def enqueue_experiment(conn: sqlite3.Connection, experiment_id: str, payload: dict[str, Any]) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO experiments(experiment_id, status, payload, updated_at) VALUES (?, 'QUEUED', ?, ?)",
        (experiment_id, json.dumps(payload, sort_keys=True, default=str), utc_now_iso()),
    )
    conn.commit()


def claim_next_experiment(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute("SELECT experiment_id, payload FROM experiments WHERE status='QUEUED' ORDER BY updated_at LIMIT 1").fetchone()
    if row is None:
        return None
    conn.execute("UPDATE experiments SET status='RUNNING', updated_at=? WHERE experiment_id=?", (utc_now_iso(), row[0]))
    conn.commit()
    payload = json.loads(row[1])
    payload["experiment_id"] = row[0]
    return payload


def complete_experiment(conn: sqlite3.Connection, experiment_id: str, result: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE experiments SET status='COMPLETED', payload=?, updated_at=? WHERE experiment_id=?",
        (json.dumps(result, sort_keys=True, default=str), utc_now_iso(), experiment_id),
    )
    conn.commit()


def queue_size(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM experiments WHERE status='QUEUED'").fetchone()[0])

