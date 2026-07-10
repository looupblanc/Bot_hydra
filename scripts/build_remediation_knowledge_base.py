#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.portfolio.remediation_portfolio import build_remediation_portfolio_candidates
from hydra.promotion.candidate_dossier import build_candidate_dossier, write_dossiers
from hydra.promotion.equivalence_clusters import cluster_summary
from hydra.promotion.failure_attribution import attribute_candidate_failure
from hydra.promotion.pareto import pareto_frontier
from hydra.validation.family_fdr import family_false_discovery_proxy
from hydra.validation.multiple_testing import effective_independent_trials, family_trial_counts, selection_adjusted_score


def main() -> int:
    parser = argparse.ArgumentParser(description="Build HYDRA gate-aware remediation knowledge base from registry.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--output-folder", default="reports/gate_aware_remediation")
    parser.add_argument("--max-economic", type=int, default=200)
    parser.add_argument("--report-tag", default="knowledge_base")
    args = parser.parse_args()
    conn = sqlite3.connect(args.registry)
    conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in conn.execute("SELECT * FROM candidates")]
    selected = select_knowledge_rows(rows, args.max_economic)
    dossiers = [build_candidate_dossier(row) for row in selected]
    dossier_paths = write_dossiers(dossiers, folder=f"{args.output_folder}/dossiers")
    attributions = [attribute_candidate_failure(row) for row in selected]
    one_gate = sum(1 for item in attributions if item["failed_gate_count"] == 1)
    two_gate = sum(1 for item in attributions if item["failed_gate_count"] == 2)
    hard_invalid = sum(1 for item in attributions if item["policy_classification"] == "HARD_INVALID")
    repairable = sum(1 for item in attributions if item["policy_classification"] == "REPAIRABLE_NEAR_MISS")
    clusters = cluster_summary(selected)
    portfolios = build_remediation_portfolio_candidates(selected)
    frontier = pareto_frontier(selected, limit=50)
    effective_trials = effective_independent_trials(len(rows), len(cluster_summary(rows)))
    summary = {
        "registry_total": len(rows),
        "selected_for_dossiers": len(selected),
        "dossier_count": len(dossier_paths),
        "topstep_viable_analyzed": count_status(selected, "TOPSTEP_VIABLE"),
        "near_misses_analyzed": sum(1 for row in selected if row["validation_status"] in {"PROMISING_NEEDS_MUTATION", "TOPSTEP_NEAR_MISS"}),
        "economically_viable_analyzed": count_status(selected, "ECONOMICALLY_VIABLE"),
        "target_reaching_analyzed": sum(1 for row in selected if row.get("combine_profit_target_hit")),
        "hard_invalid_count": hard_invalid,
        "repairable_count": repairable,
        "candidates_failing_exactly_one_gate": one_gate,
        "candidates_failing_exactly_two_gates": two_gate,
        "equivalence_clusters": len(clusters),
        "economic_strategy_units": len(clusters),
        "pareto_candidate_count": len(frontier),
        "portfolio_basket_count": len(portfolios),
        "family_trial_counts": family_trial_counts(rows),
        "family_fdr_proxy": family_false_discovery_proxy(rows),
        "effective_independent_trials_proxy": effective_trials,
        "best_promotion_score_adjusted_for_selection_proxy": selection_adjusted_score(
            max(float(row.get("promotion_score") or 0.0) for row in rows), len(rows), effective_trials
        ),
        "failure_policy_distribution": dict(Counter(item["policy_classification"] for item in attributions)),
        "dossier_paths_sample": dossier_paths[:20],
        "pareto_candidate_ids": [row["candidate_id"] for row in frontier[:20]],
        "portfolio_baskets": portfolios,
    }
    out = Path(args.output_folder)
    out.mkdir(parents=True, exist_ok=True)
    summary_path = out / f"remediation_knowledge_base_{args.report_tag}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    report_path = out / f"remediation_knowledge_base_{args.report_tag}.md"
    write_report(report_path, summary)
    print(json.dumps({"summary_path": str(summary_path), "report_path": str(report_path), **summary}, indent=2, sort_keys=True))
    return 0


def select_knowledge_rows(rows: list[dict], max_economic: int) -> list[dict]:
    selected: dict[str, dict] = {}
    for row in rows:
        if row["validation_status"] in {"TOPSTEP_VIABLE", "TOPSTEP_NEAR_MISS", "PROMISING_NEEDS_MUTATION"}:
            selected[row["candidate_id"]] = row
        if row.get("combine_profit_target_hit"):
            selected[row["candidate_id"]] = row
    one_gate_candidates = []
    for row in rows:
        attr = attribute_candidate_failure(row)
        if attr["failed_gate_count"] == 1:
            one_gate_candidates.append(row)
    for row in sorted(one_gate_candidates, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:200]:
        selected[row["candidate_id"]] = row
    econ = [row for row in rows if row["validation_status"] == "ECONOMICALLY_VIABLE"]
    for row in sorted(econ, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:max_economic]:
        selected[row["candidate_id"]] = row
    diversifiers = [row for row in rows if row.get("rejection_reason") == "high_correlation_needs_portfolio_role"]
    for row in sorted(diversifiers, key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)[:50]:
        selected[row["candidate_id"]] = row
    return sorted(selected.values(), key=lambda r: float(r.get("promotion_score") or 0.0), reverse=True)


def count_status(rows: list[dict], status: str) -> int:
    return sum(1 for row in rows if row["validation_status"] == status)


def write_report(path: Path, summary: dict) -> None:
    lines = [
        "# Gate-Aware Remediation Knowledge Base",
        "",
        f"- Registry total: {summary['registry_total']}",
        f"- Dossiers generated: {summary['dossier_count']}",
        f"- Topstep viable analyzed: {summary['topstep_viable_analyzed']}",
        f"- Near-misses analyzed: {summary['near_misses_analyzed']}",
        f"- Economically viable analyzed: {summary['economically_viable_analyzed']}",
        f"- Hard-invalid count: {summary['hard_invalid_count']}",
        f"- Repairable count: {summary['repairable_count']}",
        f"- Exactly one failed gate: {summary['candidates_failing_exactly_one_gate']}",
        f"- Exactly two failed gates: {summary['candidates_failing_exactly_two_gates']}",
        f"- Economic strategy units: {summary['economic_strategy_units']}",
        f"- Effective independent trials proxy: {summary['effective_independent_trials_proxy']:.2f}",
        f"- Selection-adjusted best promotion proxy: {summary['best_promotion_score_adjusted_for_selection_proxy']:.6f}",
        "",
        "## Failure Policy Distribution",
    ]
    for key, value in summary["failure_policy_distribution"].items():
        lines.append(f"- {key}: {value}")
    lines += ["", "## Pareto Candidate IDs"]
    for cid in summary["pareto_candidate_ids"]:
        lines.append(f"- {cid}")
    lines += ["", "## Portfolio Baskets"]
    for basket in summary["portfolio_baskets"]:
        lines.append(f"- {basket}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

