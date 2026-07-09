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
    "TOPSTEP_COMBINE_FAILED_MLL", "TOPSTEP_COMBINE_FAILED_TARGET",
    "TOPSTEP_COMBINE_FAILED_CONSISTENCY", "TOPSTEP_COMBINE_PASSED",
    "TOPSTEP_FUNDED_FAILED_MLL", "TOPSTEP_PAYOUT_ELIGIBLE",
    "TOPSTEP_PAYOUT_SURVIVED", "TOPSTEP_REJECTED_SPIKE_DAY_DEPENDENCY",
    "TOPSTEP_REJECTED_LOW_MLL_BUFFER", "TOPSTEP_REJECTED_BAD_PAYOUT_PROFILE",
    "TOPSTEP_RESEARCH_CANDIDATE", "TOPSTEP_PORTFOLIO_CANDIDATE",
    "DEAD_STRATEGY", "PROMISING_NEEDS_MUTATION", "TOPSTEP_NEAR_MISS",
    "ECONOMICALLY_VIABLE", "TOPSTEP_VIABLE", "TRADING_READY_CANDIDATE",
}


TOPSTEP_COLUMNS = {
    "topstep_passed": "INTEGER NOT NULL DEFAULT 0",
    "topstep_score": "REAL NOT NULL DEFAULT 0",
    "combine_days_to_pass": "INTEGER",
    "combine_profit_target_hit": "INTEGER NOT NULL DEFAULT 0",
    "combine_mll_breached": "INTEGER NOT NULL DEFAULT 0",
    "combine_min_mll_buffer": "REAL NOT NULL DEFAULT 0",
    "combine_best_day_profit": "REAL NOT NULL DEFAULT 0",
    "combine_best_day_pct_of_total_profit": "REAL NOT NULL DEFAULT 0",
    "combine_consistency_ok": "INTEGER NOT NULL DEFAULT 0",
    "target_inflation_required": "INTEGER NOT NULL DEFAULT 0",
    "funded_sim_survived": "INTEGER NOT NULL DEFAULT 0",
    "payout_eligible": "INTEGER NOT NULL DEFAULT 0",
    "payout_days_to_eligibility": "INTEGER",
    "payout_cycles_survived": "INTEGER NOT NULL DEFAULT 0",
    "gross_payout_available": "REAL NOT NULL DEFAULT 0",
    "trader_net_payout": "REAL NOT NULL DEFAULT 0",
    "post_payout_mll_breach": "INTEGER NOT NULL DEFAULT 0",
    "internal_daily_stop_used": "REAL NOT NULL DEFAULT 0",
    "daily_profit_lock_used": "REAL NOT NULL DEFAULT 0",
    "worst_day_loss": "REAL NOT NULL DEFAULT 0",
    "max_consecutive_losing_days": "INTEGER NOT NULL DEFAULT 0",
    "winning_days_150_count": "INTEGER NOT NULL DEFAULT 0",
    "topstep_split_scores_json": "TEXT NOT NULL DEFAULT '{}'",
    "strategy_fingerprint": "TEXT NOT NULL DEFAULT ''",
    "parameter_zone": "TEXT NOT NULL DEFAULT ''",
    "research_lane": "TEXT NOT NULL DEFAULT ''",
    "promotion_stage": "TEXT NOT NULL DEFAULT 'GENERATED'",
    "promotion_classification": "TEXT NOT NULL DEFAULT ''",
    "promotion_score": "REAL NOT NULL DEFAULT 0",
    "economic_score": "REAL NOT NULL DEFAULT 0",
    "execution_readiness_score": "REAL NOT NULL DEFAULT 0",
    "gate_history_json": "TEXT NOT NULL DEFAULT '[]'",
    "recommended_action": "TEXT NOT NULL DEFAULT ''",
    "config_export_path": "TEXT",
    "risk_export_path": "TEXT",
    "branch_action": "TEXT NOT NULL DEFAULT ''",
    "lineage_json": "TEXT NOT NULL DEFAULT '{}'",
}


def connect(db_path: str) -> sqlite3.Connection:
    path = project_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    conn.executescript(schema)
    _migrate_topstep_columns(conn)
    return conn


def query(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, tuple(params)))


def _migrate_topstep_columns(conn: sqlite3.Connection) -> None:
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(candidates)")}
    for column, definition in TOPSTEP_COLUMNS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE candidates ADD COLUMN {column} {definition}")
    conn.commit()
