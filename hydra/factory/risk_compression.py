from __future__ import annotations

import sqlite3

from hydra.factory.promotion import risk_adjusted_score


def run_risk_compression(conn: sqlite3.Connection, min_buffer: float, target_buffer: float, max_strategies: int) -> list[str]:
    rows = list(conn.execute("SELECT * FROM candidates WHERE validation_status='QUALIFIED'"))
    for row in rows:
        if row["mll_buffer"] < min_buffer:
            conn.execute("UPDATE candidates SET validation_status='REJECTED_MLL_BUFFER_TOO_LOW', rejection_reason='mll_buffer_below_v4_minimum' WHERE candidate_id=?", (row["candidate_id"],))
    conn.commit()
    eligible = list(conn.execute("SELECT * FROM candidates WHERE validation_status='QUALIFIED' AND mll_buffer >= ? ORDER BY robustness_score DESC, mll_buffer DESC", (min_buffer,)))
    selected: list[sqlite3.Row] = []
    clusters: set[str] = set()
    for row in sorted(eligible, key=risk_adjusted_score, reverse=True):
        cluster = row["correlation_cluster"] or row["candidate_id"]
        if cluster in clusters:
            conn.execute("UPDATE candidates SET validation_status='REJECTED_CORRELATED', rejection_reason='portfolio_correlation_cluster_duplicate' WHERE candidate_id=?", (row["candidate_id"],))
            continue
        selected.append(row)
        clusters.add(cluster)
        if len(selected) >= max_strategies:
            break
    for row in selected:
        status = "PROMOTED_TO_PORTFOLIO" if row["mll_buffer"] >= min_buffer else "REJECTED_MLL_BUFFER_TOO_LOW"
        conn.execute("UPDATE candidates SET validation_status=? WHERE candidate_id=?", (status, row["candidate_id"]))
    conn.commit()
    return [r["candidate_id"] for r in selected if r["mll_buffer"] >= target_buffer or r["mll_buffer"] >= min_buffer]
