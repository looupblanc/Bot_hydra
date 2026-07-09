from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from hydra.utils.config import project_path


ALLOWED_STATUSES = {
    "GENERATED", "BACKTESTED", "REJECTED_NO_EDGE", "REJECTED_TOO_FEW_TRADES",
    "REJECTED_TOO_MUCH_DRAWDOWN", "REJECTED_MLL_BREACH", "REJECTED_MLL_BUFFER_TOO_LOW",
    "REJECTED_DUPLICATE", "REJECTED_CORRELATED", "REJECTED_NOT_ROBUST", "QUALIFIED",
    "PROMOTED_TO_PORTFOLIO", "SHADOW_TESTING", "PAPER_TESTING", "LIVE_FORBIDDEN",
}


def connect(db_path: str) -> sqlite3.Connection:
    path = project_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    conn.executescript(schema)
    return conn


def query(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, tuple(params)))
