from __future__ import annotations

import sqlite3
from pathlib import Path

from hydra.utils.time import utc_now_iso


def build_markdown_report(conn: sqlite3.Connection, output_folder: str = "reports") -> Path:
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    qualified = conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status IN ('QUALIFIED','PROMOTED_TO_PORTFOLIO')").fetchone()[0]
    rejected = conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status LIKE 'REJECTED%'").fetchone()[0]
    top_families = conn.execute("SELECT family, COUNT(*) c FROM candidates GROUP BY family ORDER BY c DESC").fetchall()
    reasons = conn.execute("SELECT COALESCE(rejection_reason, validation_status) reason, COUNT(*) c FROM candidates GROUP BY reason ORDER BY c DESC").fetchall()
    best = conn.execute("SELECT candidate_id,family,symbol,timeframe,net_profit,max_drawdown,mll_buffer,robustness_score,validation_status FROM candidates ORDER BY robustness_score DESC, mll_buffer DESC LIMIT 15").fetchall()
    portfolio = conn.execute("SELECT candidate_id,family,symbol,timeframe,net_profit,max_drawdown,mll_buffer,robustness_score FROM candidates WHERE validation_status='PROMOTED_TO_PORTFOLIO' ORDER BY robustness_score DESC").fetchall()
    mll = conn.execute("SELECT MIN(mll_buffer), AVG(mll_buffer), SUM(mll_breached) FROM candidates").fetchone()
    lines = [
        "# HYDRA Research Report",
        "",
        f"Generated: {utc_now_iso()}",
        "",
        "Synthetic smoke-test results are not evidence of real trading edge.",
        "",
        f"- Total candidates: {total}",
        f"- Qualified candidates: {qualified}",
        f"- Rejected candidates: {rejected}",
        f"- MLL buffer min/avg: {mll[0] or 0:.2f} / {mll[1] or 0:.2f}",
        f"- MLL breaches: {mll[2] or 0}",
        "",
        "## Top Families",
    ]
    lines += [f"- {r['family']}: {r['c']}" for r in top_families]
    lines += ["", "## Rejection Reasons"]
    lines += [f"- {r['reason']}: {r['c']}" for r in reasons]
    lines += ["", "## Best Candidates"]
    for r in best:
        lines.append(f"- {r['candidate_id']} {r['family']} {r['symbol']} {r['timeframe']} status={r['validation_status']} net={r['net_profit']:.2f} dd={r['max_drawdown']:.2f} buffer={r['mll_buffer']:.2f} robust={r['robustness_score']:.3f}")
    lines += ["", "## Risk-Compressed Portfolio"]
    lines += [f"- {r['candidate_id']} {r['family']} {r['symbol']} {r['timeframe']} net={r['net_profit']:.2f} dd={r['max_drawdown']:.2f} buffer={r['mll_buffer']:.2f} robust={r['robustness_score']:.3f}" for r in portfolio] or ["- No portfolio promotions yet."]
    lines += ["", "## Next Recommended Action", "- Add real historical futures data and rerun V3/V4 with no-lookahead and walk-forward audits before considering paper or shadow validation."]
    path = Path(output_folder) / f"hydra_report_{utc_now_iso().replace(':', '').replace('+', 'Z')}.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
