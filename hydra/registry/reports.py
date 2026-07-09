from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from hydra.utils.time import utc_now_iso


def build_markdown_report(conn: sqlite3.Connection, output_folder: str = "reports", metadata: dict[str, Any] | None = None) -> Path:
    metadata = metadata or {}
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    qualified = conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status IN ('QUALIFIED','PROMOTED_TO_PORTFOLIO')").fetchone()[0]
    rejected = conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status LIKE 'REJECTED%'").fetchone()[0]
    status_distribution = conn.execute("SELECT validation_status, COUNT(*) c FROM candidates GROUP BY validation_status ORDER BY c DESC").fetchall()
    top_families = conn.execute("SELECT family, COUNT(*) c FROM candidates GROUP BY family ORDER BY c DESC").fetchall()
    reasons = conn.execute("SELECT rejection_reason reason, COUNT(*) c FROM candidates WHERE rejection_reason IS NOT NULL GROUP BY rejection_reason ORDER BY c DESC").fetchall()
    best = conn.execute("SELECT candidate_id,family,symbol,timeframe,net_profit,max_drawdown,mll_buffer,robustness_score,validation_status FROM candidates ORDER BY robustness_score DESC, mll_buffer DESC LIMIT 15").fetchall()
    portfolio = conn.execute("SELECT candidate_id,family,symbol,timeframe,net_profit,max_drawdown,mll_buffer,robustness_score FROM candidates WHERE validation_status='PROMOTED_TO_PORTFOLIO' ORDER BY robustness_score DESC").fetchall()
    mll = conn.execute("SELECT MIN(mll_buffer), AVG(mll_buffer), SUM(mll_breached) FROM candidates").fetchone()
    warnings = metadata.get("warnings", [])
    symbols = ", ".join(metadata.get("symbols", [])) if metadata.get("symbols") else "not recorded"
    selected_count = metadata.get("v4_selected_portfolio_count", len(portfolio))
    lines = [
        "# HYDRA Research Report",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "## Run Context",
        f"- Run mode: {metadata.get('run_mode', 'synthetic strict')}",
        f"- Requested candidate count: {metadata.get('candidate_count', 'not recorded')}",
        f"- Symbols: {symbols}",
        f"- Seed: {metadata.get('seed', 'not recorded')}",
        f"- Report tag: {metadata.get('report_tag', 'not set')}",
        "",
        "## Warnings",
    ]
    lines += [f"- {warning}" for warning in warnings] or ["- None."]
    lines += [
        "",
        "## Summary",
        f"- Total candidates: {total}",
        f"- Qualified candidates: {qualified}",
        f"- Rejected candidates: {rejected}",
        f"- V4 selected portfolio count: {selected_count}",
        f"- MLL buffer min/avg: {mll[0] or 0:.2f} / {mll[1] or 0:.2f}",
        f"- MLL breaches: {mll[2] or 0}",
        "",
        "## Status Distribution",
    ]
    lines += [f"- {r['validation_status']}: {r['c']}" for r in status_distribution] or ["- No candidates logged."]
    lines += ["", "## Top Families"]
    lines += [f"- {r['family']}: {r['c']}" for r in top_families] or ["- No candidates logged."]
    lines += ["", "## Rejection Reasons"]
    lines += [f"- {r['reason']}: {r['c']}" for r in reasons] or ["- No rejections logged."]
    lines += ["", "## Best Candidates"]
    for r in best:
        lines.append(f"- {r['candidate_id']} {r['family']} {r['symbol']} {r['timeframe']} status={r['validation_status']} net={r['net_profit']:.2f} dd={r['max_drawdown']:.2f} buffer={r['mll_buffer']:.2f} robust={r['robustness_score']:.3f}")
    if not best:
        lines.append("- No candidates logged.")
    lines += ["", "## Risk-Compressed Portfolio"]
    lines += [f"- {r['candidate_id']} {r['family']} {r['symbol']} {r['timeframe']} net={r['net_profit']:.2f} dd={r['max_drawdown']:.2f} buffer={r['mll_buffer']:.2f} robust={r['robustness_score']:.3f}" for r in portfolio] or ["- No portfolio promotions yet."]
    lines += [
        "",
        "## MLL Summary",
        f"- Minimum buffer: {mll[0] or 0:.2f}",
        f"- Average buffer: {mll[1] or 0:.2f}",
        f"- Breached candidates: {mll[2] or 0}",
        "",
        "## Next Recommended Action",
        f"- {metadata.get('next_recommended_action', 'Add Databento historical futures ingestion and strict no-lookahead tests before any paper or shadow validation.')}",
    ]
    tag = _safe_tag(metadata.get("report_tag"))
    suffix = f"_{tag}" if tag else ""
    path = Path(output_folder) / f"hydra_report_{utc_now_iso().replace(':', '').replace('+', 'Z')}{suffix}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _safe_tag(tag: Any) -> str:
    if not tag:
        return ""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(tag)).strip("._-")
