#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


POLICY_COMPLEXITY = {
    "target_velocity_runner": 1,
    "mll_buffer_derisk": 0,
    "consistency_daily_lock": 1,
    "oos_simplify": -1,
    "sequence_fragility_smooth": 1,
    "payout_frequency": 1,
    "portfolio_role_shift": 0,
}

POLICY_ALIASES = {
    "consistency_daily_lock": "consistency_smooth",
    "payout_frequency": "payout_path_repair",
    "portfolio_role_shift": "portfolio_diversify",
}

STATUS_RANK = {
    "DEAD_STRATEGY": 0,
    "TOPSTEP_COMBINE_FAILED_MLL": 1,
    "TOPSTEP_COMBINE_FAILED_TARGET": 1,
    "PROMISING_NEEDS_MUTATION": 2,
    "ECONOMICALLY_VIABLE": 3,
    "TOPSTEP_NEAR_MISS": 4,
    "TOPSTEP_VIABLE": 5,
    "TRADING_READY_CANDIDATE": 9,
}

STAGE_RANK = {
    "GENERATED": 0,
    "BACKTESTED": 1,
    "COST_ADJUSTED": 2,
    "NO_LOOKAHEAD_PASSED": 3,
    "WALK_FORWARD_PASSED": 4,
    "OOS_PASSED": 5,
    "MONTE_CARLO_PASSED": 6,
    "PARAMETER_SENSITIVITY_PASSED": 7,
    "TOPSTEP_COMBINE_PASSED": 8,
    "FUNDED_XFA_PASSED": 9,
    "PAYOUT_SURVIVAL_PASSED": 10,
    "CORRELATION_PASSED": 11,
    "PORTFOLIO_INTERACTION_PASSED": 12,
    "EXECUTION_READINESS_PASSED": 13,
    "TRADING_READY_CANDIDATE": 14,
}

Q1_CORE_GATES = {
    "DATA_INTEGRITY",
    "DUPLICATE_FINGERPRINT",
    "NO_LOOKAHEAD",
    "ECONOMIC_PROFILE",
    "WALK_FORWARD",
    "MONTE_CARLO",
    "PARAMETER_SENSITIVITY",
    "TOPSTEP_COMBINE",
    "FUNDED_XFA",
    "PAYOUT_SURVIVAL",
    "CORRELATION",
    "PORTFOLIO_INTERACTION",
    "EXECUTION_READINESS",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize interrupted HYDRA persistent remediation run.")
    parser.add_argument("--registry", default="registry/hydra_registry.db")
    parser.add_argument("--log", required=True)
    parser.add_argument("--run-start", required=True)
    parser.add_argument("--baseline-count", type=int, required=True)
    parser.add_argument("--tag", required=True)
    return parser.parse_args()


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def load_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute("SELECT * FROM candidates")]


def gate_history(row: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        return json.loads(row.get("gate_history_json") or "[]")
    except json.JSONDecodeError:
        return []


def gate_pass_map(row: dict[str, Any]) -> dict[str, bool]:
    return {str(item.get("name")): bool(item.get("passed")) for item in gate_history(row)}


def failed_gates(row: dict[str, Any]) -> list[str]:
    return [str(item.get("name")) for item in gate_history(row) if not item.get("passed")]


def gate_count(row: dict[str, Any]) -> int:
    return sum(1 for item in gate_history(row) if item.get("passed"))


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = (len(ordered) - 1) * pct
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    weight = idx - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


def load_last_heartbeat(path: Path) -> tuple[int, dict[str, Any] | None]:
    count = 0
    last = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith('{"heartbeat"'):
            continue
        count += 1
        last = json.loads(line)["heartbeat"]
    return count, last


def status_counts(rows: list[dict[str, Any]]) -> Counter[str]:
    return Counter(str(row["validation_status"]) for row in rows)


def build_policy_table(run_rows: list[dict[str, Any]], row_by_id: dict[str, dict[str, Any]], runtime_seconds: float) -> list[dict[str, Any]]:
    by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        by_policy[str(row.get("mutation_type") or "unknown")].append(row)
    table = []
    for policy, rows in sorted(by_policy.items(), key=lambda item: len(item[1]), reverse=True):
        deltas = []
        gate_deltas = []
        mll_improvements = 0
        consistency_improvements = 0
        payout_improvements = 0
        stage_progress = 0
        improved = 0
        degraded = 0
        cosmetic = 0
        single_gate = 0
        multi_gate = 0
        hidden_regression = 0
        for row in rows:
            parent = row_by_id.get(str(row.get("parent_candidate_id") or ""))
            parent_score = float(parent.get("promotion_score") or 0.0) if parent else 0.0
            child_score = float(row.get("promotion_score") or 0.0)
            delta = child_score - parent_score
            deltas.append(delta)
            if delta > 0:
                improved += 1
            elif delta < 0:
                degraded += 1
            parent_gates = gate_count(parent) if parent else 0
            child_gates = gate_count(row)
            gate_delta = child_gates - parent_gates
            gate_deltas.append(gate_delta)
            if delta > 0 and gate_delta <= 0:
                cosmetic += 1
            if gate_delta == 1:
                single_gate += 1
            if gate_delta >= 2:
                multi_gate += 1
            if delta > 0 and gate_delta < 0:
                hidden_regression += 1
            if parent:
                if float(row.get("combine_min_mll_buffer") or 0.0) > float(parent.get("combine_min_mll_buffer") or 0.0):
                    mll_improvements += 1
                if int(row.get("combine_consistency_ok") or 0) > int(parent.get("combine_consistency_ok") or 0):
                    consistency_improvements += 1
                if float(row.get("trader_net_payout") or 0.0) > float(parent.get("trader_net_payout") or 0.0):
                    payout_improvements += 1
                if STAGE_RANK.get(str(row.get("promotion_stage") or ""), 0) > STAGE_RANK.get(str(parent.get("promotion_stage") or ""), 0):
                    stage_progress += 1
                elif STATUS_RANK.get(str(row.get("validation_status") or ""), 0) > STATUS_RANK.get(str(parent.get("validation_status") or ""), 0):
                    stage_progress += 1
        trials = len(rows)
        unique_children = len({row.get("strategy_fingerprint") for row in rows if row.get("strategy_fingerprint")})
        status = status_counts(rows)
        table.append(
            {
                "policy": policy,
                "alias": POLICY_ALIASES.get(policy, policy),
                "trials": trials,
                "unique_children": unique_children,
                "duplicate_rate": round(1.0 - unique_children / max(trials, 1), 6),
                "local_improvement_count": improved,
                "local_improvement_rate": round(improved / max(trials, 1), 6),
                "median_improvement": round(median(deltas), 6) if deltas else 0.0,
                "mean_improvement": round(mean(deltas), 6) if deltas else 0.0,
                "p25_improvement": round(percentile(deltas, 0.25), 6),
                "p75_improvement": round(percentile(deltas, 0.75), 6),
                "degradation_rate": round(degraded / max(trials, 1), 6),
                "economically_viable_children": status.get("ECONOMICALLY_VIABLE", 0),
                "topstep_near_miss_children": status.get("TOPSTEP_NEAR_MISS", 0),
                "topstep_viable_children": status.get("TOPSTEP_VIABLE", 0),
                "q2_confirmed_children": 0,
                "mll_improvements": mll_improvements,
                "consistency_improvements": consistency_improvements,
                "payout_improvements": payout_improvements,
                "complexity_change": POLICY_COMPLEXITY.get(policy, 0),
                "compute_time_seconds_est": round(runtime_seconds * trials / max(len(run_rows), 1), 2),
                "improvement_per_1000_trials": round(improved * 1000 / max(trials, 1), 3),
                "promotion_stage_progress_per_1000_trials": round(stage_progress * 1000 / max(trials, 1), 3),
                "cosmetic_score_improvements": cosmetic,
                "single_gate_improvements": single_gate,
                "multi_gate_improvements": multi_gate,
                "regressions_hidden_by_score": hidden_regression,
            }
        )
    return table


def lineage_root(row: dict[str, Any], row_by_id: dict[str, dict[str, Any]]) -> str:
    seen = set()
    current = row
    while current.get("parent_candidate_id") and current["parent_candidate_id"] in row_by_id:
        parent_id = str(current["parent_candidate_id"])
        if parent_id in seen:
            break
        seen.add(parent_id)
        current = row_by_id[parent_id]
    return str(current["candidate_id"])


def top_candidates(rows: list[dict[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for row in sorted(rows, key=lambda item: (len(failed_gates(item)), -float(item.get("promotion_score") or 0.0), -float(item.get("topstep_score") or 0.0))):
        fp = str(row.get("strategy_fingerprint") or "")
        if fp and fp in seen:
            continue
        seen.add(fp)
        out.append(
            {
                "candidate_id": row["candidate_id"],
                "parent_candidate_id": row.get("parent_candidate_id"),
                "family": row["family"],
                "symbol": row["symbol"],
                "policy": row.get("mutation_type"),
                "status": row["validation_status"],
                "promotion_score": round(float(row.get("promotion_score") or 0.0), 6),
                "topstep_score": round(float(row.get("topstep_score") or 0.0), 6),
                "failed_gate_count": len(failed_gates(row)),
                "failed_gates": failed_gates(row),
                "net_profit": round(float(row.get("net_profit") or 0.0), 2),
                "mll_buffer": round(float(row.get("combine_min_mll_buffer") or 0.0), 2),
                "trade_count": int(row.get("trade_count") or 0),
            }
        )
        if len(out) >= limit:
            break
    return out


def stage_or_status_progress(row: dict[str, Any], parent: dict[str, Any] | None) -> bool:
    if not parent:
        return False
    if STAGE_RANK.get(str(row.get("promotion_stage") or ""), 0) > STAGE_RANK.get(str(parent.get("promotion_stage") or ""), 0):
        return True
    return STATUS_RANK.get(str(row.get("validation_status") or ""), 0) > STATUS_RANK.get(str(parent.get("validation_status") or ""), 0)


def write_markdown(path: Path, data: dict[str, Any]) -> None:
    lines = [
        "# HYDRA Persistent Remediation Finalization",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Safety",
        "- Manual stop requested by user; no new research batch launched.",
        "- Q3 remains contaminated confirmation/development data for exposed lineages.",
        "- Q4 remains sealed/raw-only and was not loaded, normalized, summarized, or inspected.",
        "- No new Databento purchase was made during finalization.",
        "- This is historical research only. It is not live trading approval.",
        "",
        "## Run Summary",
    ]
    for key in [
        "exact_runtime",
        "completed_cycles",
        "completed_children_from_full_cycles",
        "committed_children_total",
        "partial_interrupted_cycle_committed_rows",
        "registry_total",
        "exact_stop_reason",
        "incomplete_batch_disposition",
        "workers_used",
        "registry_integrity",
    ]:
        lines.append(f"- {key}: {data[key]}")
    lines += ["", "## Promotion Funnel"]
    for key, value in data["promotion_funnel"].items():
        lines.append(f"- {key}: {value}")
    lines += ["", "## Policy Table"]
    lines.append("| policy | alias | trials | unique | dup rate | improve rate | median delta | mean delta | econ | near miss | viable | stage/1k |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in data["policy_table"]:
        lines.append(
            f"| {row['policy']} | {row['alias']} | {row['trials']} | {row['unique_children']} | {row['duplicate_rate']} | "
            f"{row['local_improvement_rate']} | {row['median_improvement']} | {row['mean_improvement']} | "
            f"{row['economically_viable_children']} | {row['topstep_near_miss_children']} | {row['topstep_viable_children']} | "
            f"{row['promotion_stage_progress_per_1000_trials']} |"
        )
    lines += ["", "## Improvement Diagnosis"]
    for key, value in data["improvement_diagnosis"].items():
        lines.append(f"- {key}: {value}")
    lines += ["", "## Policy Allocation Audit"]
    lines += data["policy_allocation_audit"]
    lines += ["", "## Lineage Audit"]
    for key, value in data["lineage_audit"].items():
        lines.append(f"- {key}: `{json.dumps(value, sort_keys=True, default=str)[:6000]}`")
    lines += ["", "## Candidate Selection"]
    for key, value in data["candidate_selection"].items():
        lines.append(f"- {key}: `{json.dumps(value, sort_keys=True, default=str)[:12000]}`")
    lines += ["", "## Equivalence And Clustering"]
    for key, value in data["equivalence"].items():
        lines.append(f"- {key}: {value}")
    lines += ["", "## Main Blockers"]
    for item in data["main_blockers"]:
        lines.append(f"- {item}")
    lines += ["", "## Recommended Next Phase"]
    for item in data["recommended_next_phase"]:
        lines.append(f"- {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    start = parse_time(args.run_start)
    conn = sqlite3.connect(args.registry)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    rows = load_rows(conn)
    row_by_id = {str(row["candidate_id"]): row for row in rows}
    run_rows = [row for row in rows if parse_time(str(row["created_at"])) >= start]
    run_rows.sort(key=lambda row: str(row["created_at"]))
    last_created = parse_time(str(run_rows[-1]["created_at"])) if run_rows else start
    runtime_seconds = (last_created - start).total_seconds()
    heartbeat_count, last_heartbeat = load_last_heartbeat(Path(args.log))
    completed_cycles = int(last_heartbeat.get("cycle_number", 0)) if last_heartbeat else 0
    completed_children_full_cycles = int(last_heartbeat.get("cumulative_children", 0)) if last_heartbeat else 0
    partial_rows = max(len(run_rows) - completed_children_full_cycles, 0)
    policy_table = build_policy_table(run_rows, row_by_id, runtime_seconds)
    run_status = status_counts(run_rows)
    all_status = status_counts(rows)
    fingerprints = [row.get("strategy_fingerprint") for row in run_rows if row.get("strategy_fingerprint")]
    unique_fingerprints = len(set(fingerprints))
    gate_pass_counts = Counter()
    gate_fail_counts = Counter()
    q1_core = 0
    strict_all = 0
    for row in run_rows:
        passes = gate_pass_map(row)
        for gate, passed in passes.items():
            if passed:
                gate_pass_counts[gate] += 1
            else:
                gate_fail_counts[gate] += 1
        if Q1_CORE_GATES.issubset({gate for gate, passed in passes.items() if passed}):
            q1_core += 1
        if passes and all(passes.values()):
            strict_all += 1
    improved_rows = 0
    cosmetic = 0
    single_gate = 0
    multi_gate = 0
    hidden = 0
    complexity_inc = 0
    complexity_dec = 0
    stage_advancement = 0
    for row in run_rows:
        parent = row_by_id.get(str(row.get("parent_candidate_id") or ""))
        parent_score = float(parent.get("promotion_score") or 0.0) if parent else 0.0
        delta = float(row.get("promotion_score") or 0.0) - parent_score
        gate_delta = gate_count(row) - (gate_count(parent) if parent else 0)
        if delta > 0:
            improved_rows += 1
            if gate_delta <= 0:
                cosmetic += 1
            if gate_delta < 0:
                hidden += 1
        if gate_delta == 1:
            single_gate += 1
        if gate_delta >= 2:
            multi_gate += 1
        complexity = POLICY_COMPLEXITY.get(str(row.get("mutation_type") or ""), 0)
        if delta > 0 and complexity > 0:
            complexity_inc += 1
        if delta > 0 and complexity < 0:
            complexity_dec += 1
        if stage_or_status_progress(row, parent):
            stage_advancement += 1
    roots = Counter(lineage_root(row, row_by_id) for row in run_rows)
    top_roots = roots.most_common(10)
    lineage_improvements: dict[str, list[float]] = defaultdict(list)
    for row in run_rows:
        parent = row_by_id.get(str(row.get("parent_candidate_id") or ""))
        parent_score = float(parent.get("promotion_score") or 0.0) if parent else 0.0
        lineage_improvements[lineage_root(row, row_by_id)].append(float(row.get("promotion_score") or 0.0) - parent_score)
    lineage_summary = [
        {
            "root": root,
            "children": len(deltas),
            "mean_delta": round(mean(deltas), 6),
            "positive_rate": round(sum(1 for item in deltas if item > 0) / max(len(deltas), 1), 6),
        }
        for root, deltas in lineage_improvements.items()
    ]
    top_improving_lineages = sorted(
        [item for item in lineage_summary if item["mean_delta"] > 0 or item["positive_rate"] >= 0.25],
        key=lambda item: (item["mean_delta"], item["positive_rate"]),
        reverse=True,
    )[:15]
    exhausted_lineages = sorted([item for item in lineage_summary if item["children"] >= 100 and item["positive_rate"] < 0.02], key=lambda item: item["children"], reverse=True)[:15]
    repeated_no_gate = sorted([item for item in lineage_summary if item["children"] >= 100 and item["mean_delta"] <= 0], key=lambda item: item["children"], reverse=True)[:15]
    freeze_lineages = sorted(
        [item for item in lineage_summary if item["children"] >= 250 and item["mean_delta"] < -0.05 and item["positive_rate"] < 0.10],
        key=lambda item: (item["mean_delta"], -item["children"]),
    )[:15]
    one_gate = [row for row in run_rows if len(failed_gates(row)) == 1]
    two_gates = [row for row in run_rows if len(failed_gates(row)) == 2]
    safe_topstep = [row for row in run_rows if row["validation_status"] == "TOPSTEP_VIABLE" and not int(row.get("combine_mll_breached") or 0)]
    portfolio_diversifiers = [row for row in run_rows if row.get("mutation_type") == "portfolio_role_shift"]
    retired = [row for row in run_rows if row["validation_status"] == "DEAD_STRATEGY"]
    data = {
        "exact_runtime": f"{int(runtime_seconds)} seconds ({runtime_seconds / 3600:.4f} hours)",
        "completed_cycles": completed_cycles,
        "completed_children_from_full_cycles": completed_children_full_cycles,
        "committed_children_total": len(run_rows),
        "partial_interrupted_cycle_committed_rows": partial_rows,
        "registry_total": len(rows),
        "exact_stop_reason": "manual SIGINT requested by user; KeyboardInterrupt occurred during promotion storage",
        "incomplete_batch_disposition": f"cycle {completed_cycles + 1} was interrupted; {partial_rows} already committed rows preserved; uncommitted worker outputs discarded by process termination",
        "workers_used": 3,
        "registry_integrity": integrity,
        "promotion_funnel": {
            "generated_child_committed_rows": len(run_rows),
            "generated_child_full_completed_cycles": completed_children_full_cycles,
            "valid_fingerprint_rows": len(fingerprints),
            "unique_fingerprints": unique_fingerprints,
            "improved_versus_parent": improved_rows,
            "economically_viable": run_status.get("ECONOMICALLY_VIABLE", 0),
            "topstep_near_miss": run_status.get("TOPSTEP_NEAR_MISS", 0),
            "topstep_viable": run_status.get("TOPSTEP_VIABLE", 0),
            "q1_robust_core_gates_passed": q1_core,
            "q1_all_gates_passed_including_oos": strict_all,
            "q2_evaluated": 0,
            "q2_confirmed": 0,
            "monte_carlo_passed": gate_pass_counts.get("MONTE_CARLO", 0),
            "parameter_stability_passed": gate_pass_counts.get("PARAMETER_SENSITIVITY", 0),
            "payout_simulation_passed": gate_pass_counts.get("PAYOUT_SURVIVAL", 0),
            "portfolio_eligible": gate_pass_counts.get("PORTFOLIO_INTERACTION", 0),
            "valid_promotion_finalist": 0,
            "trading_ready": all_status.get("TRADING_READY_CANDIDATE", 0),
        },
        "policy_table": policy_table,
        "improvement_diagnosis": {
            "local_improvement_rate": round(improved_rows / max(len(run_rows), 1), 6),
            "cosmetic_score_improvement": cosmetic,
            "single_gate_improvement": single_gate,
            "multi_gate_improvement": multi_gate,
            "genuine_promotion_stage_advancement": stage_advancement,
            "regressions_hidden_by_aggregate_score": hidden,
            "complexity_increasing_improvements": complexity_inc,
            "complexity_reducing_improvements": complexity_dec,
        },
        "policy_allocation_audit": [
            "- `oos_simplify` dominated because parent selection repeatedly surfaced candidates with `march_oos_weak` and `viable_only_in_one_split`; policy choice maps those reasons directly to OOS simplification.",
            "- The current reward is still a local promotion-score delta, so it can favor cheap local score improvement over final promotion-stage movement.",
            "- Under-explored policies did receive hundreds of trials but not enough allocation relative to OOS-driven parent concentration.",
            "- No policies froze because the freeze threshold required at least 200 trials and an improvement rate below 2%; even weak policies stayed above that local-score threshold.",
            "- Freeze criteria are too conservative and insufficiently tied to Q2/OOS/promotion-stage progress.",
            "- Next allocation should reward gate-distance reduction and Q2 confirmation, not raw local score delta.",
        ],
        "lineage_audit": {
            "top_improving_lineages": top_improving_lineages,
            "repeated_mutation_no_gate_progress": repeated_no_gate,
            "exhausted_lineages": exhausted_lineages,
            "lineages_to_freeze": freeze_lineages,
            "lineages_to_expand": top_improving_lineages,
            "top_10_lineage_compute_share": round(sum(count for _, count in top_roots) / max(len(run_rows), 1), 6),
            "top_10_lineages": top_roots,
            "lineage_count": len(roots),
        },
        "candidate_selection": {
            "top_50_by_genuine_promotion_distance": top_candidates(run_rows, 50),
            "top_failing_exactly_one_valid_gate": top_candidates(one_gate, 25),
            "top_failing_exactly_two_related_gates": top_candidates(two_gates, 25),
            "top_strong_topstep_safe_mll": top_candidates(safe_topstep, 25),
            "top_q2_confirmation": [],
            "top_portfolio_diversifiers": top_candidates(portfolio_diversifiers, 25),
            "top_requiring_execution_tick_validation": top_candidates(safe_topstep, 25),
            "retire_candidates": top_candidates(retired, 25),
        },
        "equivalence": {
            "valid_economic_strategy_units": 0,
            "reason": "no persisted trade list or signal stream is available in the registry, so trade-overlap, signal-agreement, holding-period similarity, and tail-event overlap cannot be recomputed honestly",
            "equivalence_clusters": 0,
            "cluster_sizes": "not_computed_without_trade_level_evidence",
            "dominant_clusters": "not_computed_without_trade_level_evidence",
            "truly_distinct_candidates": 0,
        },
        "main_blockers": [
            f"OOS/split weakness dominates gate failures: {gate_fail_counts.get('OOS', 0)} OOS failures in committed run rows.",
            "Policy allocation over-concentrated on OOS simplification because parent-pool evidence was highly OOS-skewed.",
            "Promotion evidence is Q1-only for these children; Q2 evaluated and Q2 confirmed counts are zero.",
            "Economic strategy unit count remains zero because trade-level evidence was not persisted for equivalence clustering.",
            "Q3 is contaminated for exposed lineages and cannot be used as blind validation.",
            "Q4 remains sealed, so no trading-ready classification is permissible.",
        ],
        "recommended_next_phase": [
            "Best next milestone: repair the reward/allocation model and persist trade-level evidence for finalist equivalence before any more broad mutation.",
            "Why: the current loop produced many local OOS-simplify improvements but no valid promotion finalists, no Q2 confirmation, and no valid economic strategy units.",
            "Freeze or cap the dominant negative-delta lineages before further mutation; they consumed nearly all compute without validated promotion-stage movement.",
            "Freeze or sharply cap low-information OOS parameter-neighbor mutation until policy reward includes gate-distance, Q2 transfer, lineage diversity, and trade-overlap uniqueness.",
            "Then promote a small frozen Q1 set into Q2 confirmation and executable portfolio simulation.",
        ],
        "gate_failure_distribution": dict(gate_fail_counts),
        "status_distribution_all_registry": dict(all_status),
        "status_distribution_run_rows": dict(run_status),
    }
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ0000")
    report_dir = Path("reports/gate_aware_remediation")
    checkpoint_dir = Path("reports/checkpoints/gate_aware_remediation")
    report_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"persistent_remediation_final_{timestamp}_{args.tag}.md"
    checkpoint_path = checkpoint_dir / f"persistent_remediation_final_checkpoint_{timestamp}_{args.tag}.md"
    write_markdown(report_path, data)
    checkpoint_path.write_text(
        "\n".join(
            [
                "# Persistent Remediation Final Checkpoint",
                "",
                f"Generated: {datetime.now(timezone.utc).isoformat()}",
                f"- exact_runtime: {data['exact_runtime']}",
                f"- completed_cycles: {completed_cycles}",
                f"- completed_children_from_full_cycles: {completed_children_full_cycles}",
                f"- committed_children_total: {len(run_rows)}",
                f"- registry_total: {len(rows)}",
                f"- registry_integrity: {integrity}",
                f"- report_path: {report_path}",
                f"- q3_status: quarantined_confirmation_development",
                f"- q4_status: sealed_raw_uninspected",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"report_path": str(report_path), "checkpoint_path": str(checkpoint_path), **data}, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
