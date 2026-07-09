from __future__ import annotations

import json
import sqlite3

from hydra.strategies.dsl import StrategyCandidate
from hydra.utils.time import utc_now_iso


def upsert_candidate(conn: sqlite3.Connection, candidate: StrategyCandidate, metrics: dict, prop: dict, status: str, rejection_reason: str | None, robustness: float, cluster: str | None) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO candidates (
            candidate_id, family, symbol, timeframe, parameters_json, risk_json,
            net_profit, max_drawdown, profit_factor, sharpe, trade_count, win_rate,
            mll_breached, mll_buffer, correlation_cluster, validation_status,
            rejection_reason, robustness_score, parent_candidate_id, mutation_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.candidate_id, candidate.family, candidate.symbol, candidate.timeframe,
            json.dumps(candidate.parameters, sort_keys=True), json.dumps(candidate.risk_parameters, sort_keys=True),
            metrics.get("net_profit", 0.0), metrics.get("max_drawdown", 0.0), metrics.get("profit_factor", 0.0),
            metrics.get("sharpe", 0.0), int(metrics.get("trade_count", 0)), metrics.get("win_rate", 0.0),
            int(bool(prop.get("mll_breached", False))), prop.get("mll_buffer", 0.0), cluster,
            status, rejection_reason, robustness, candidate.parent_candidate_id, candidate.mutation_type, utc_now_iso(),
        ),
    )
    conn.commit()


def load_candidates(conn: sqlite3.Connection, status: str | None = None) -> list[sqlite3.Row]:
    if status:
        return list(conn.execute("SELECT * FROM candidates WHERE validation_status = ? ORDER BY net_profit DESC", (status,)))
    return list(conn.execute("SELECT * FROM candidates ORDER BY created_at DESC"))
