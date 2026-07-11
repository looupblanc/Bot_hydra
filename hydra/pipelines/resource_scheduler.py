from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from hydra.utils.time import utc_now_iso


@dataclass(frozen=True)
class ParallelBatchPlan:
    experiments: tuple[dict[str, Any], ...]
    pipelines: tuple[str, ...]
    worker_limit: int


def claim_parallel_batch(
    conn: sqlite3.Connection,
    *,
    claimed_by: str,
    worker_limit: int,
    lease_seconds: float = 180.0,
) -> ParallelBatchPlan:
    """Atomically claim one explicit parallel-safe job per pipeline.

    The caller owns the sole mission writer lock. This helper never opens a
    second connection and never commits experiment results.
    """
    limit = max(1, int(worker_limit))
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT experiment_id,payload,experiment_type,specification_hash,"
            "attempt_count,max_attempts FROM experiments WHERE status='QUEUED' "
            "ORDER BY priority DESC,updated_at,experiment_id"
        ).fetchall()
        selected: list[tuple[Any, ...]] = []
        pipelines: set[str] = set()
        data_access_writer_selected = False
        for row in rows:
            specification = json.loads(row[1])
            if not bool(specification.get("parallel_safe")):
                continue
            pipeline = str(specification.get("pipeline") or "UNSPECIFIED")
            if pipeline in pipelines:
                continue
            writes_data_access = bool(
                specification.get("writes_data_access_ledger")
            )
            if writes_data_access and data_access_writer_selected:
                continue
            selected.append(row)
            pipelines.add(pipeline)
            data_access_writer_selected = (
                data_access_writer_selected or writes_data_access
            )
            if len(selected) >= limit:
                break
        now = utc_now_iso()
        claimed: list[dict[str, Any]] = []
        for row in selected:
            specification = json.loads(row[1])
            token = uuid.uuid4().hex
            lease_expires_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=max(float(lease_seconds), 1.0))
            ).replace(microsecond=0).isoformat()
            updated = conn.execute(
                "UPDATE experiments SET status='RUNNING',started_at=?,updated_at=?,"
                "attempt_count=attempt_count+1,last_error=NULL,claim_token=?,"
                "claimed_by=?,lease_expires_at=? WHERE experiment_id=? AND status='QUEUED'",
                (
                    now,
                    now,
                    token,
                    claimed_by,
                    lease_expires_at,
                    row[0],
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"Parallel claim race: {row[0]}")
            claimed.append(
                {
                    **specification,
                    "experiment_id": str(row[0]),
                    "experiment_type": str(
                        row[2] or specification.get("experiment_type") or row[0]
                    ),
                    "specification_hash": str(row[3]),
                    "attempt_count": int(row[4]) + 1,
                    "max_attempts": int(row[5]),
                    "claim_token": token,
                    "claimed_by": claimed_by,
                    "lease_expires_at": lease_expires_at,
                }
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return ParallelBatchPlan(
        experiments=tuple(claimed),
        pipelines=tuple(str(row.get("pipeline") or "UNSPECIFIED") for row in claimed),
        worker_limit=limit,
    )
