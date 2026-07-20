"""Full discrete-action upper bound over the immutable trade-ecology universe.

This is deliberately an isolated, non-deployable diagnostic.  It asks whether
allowing the hindsight oracle to choose among ABSTAIN/0.5x/1.0x/1.5x changes
the exact account result relative to the existing binary ABSTAIN/1.5x oracle.
No result emitted here is promotion-eligible.
"""

from __future__ import annotations

from collections import Counter
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
from typing import Any, Mapping, Sequence

from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.research.direct_trade_ecology_oracle import (
    ACCOUNT_RISK_SCALE,
    HORIZONS,
    SCENARIOS,
    _account_config,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _opportunities,
    _policy,
    _resize,
    _source_inventory,
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
)


SCHEMA = "hydra_direct_trade_ecology_full_action_upper_bound_v1"
ACTION_FRONTIER = (0.0, 0.5, 1.0, 1.5)
SELECTORS = ("FULL_0_0_5_1_1_5", "BINARY_0_1_5")
DEFAULT_OUTPUT_DIR = Path(
    "reports/economic_evolution/direct_trade_ecology_full_action_oracle_v1"
)


def rounded_contract_quantity(
    source_quantity: int, *, account_scale: float, action: float
) -> int:
    """Apply the exact frozen floor rounding used by the account replay."""

    if action <= 0.0:
        return 0
    return max(
        1,
        int(math.floor(int(source_quantity) * float(account_scale) * action + 1e-12)),
    )


def choose_action(
    action_rows: Mapping[str, Mapping[str, Any]], *, binary: bool
) -> float:
    """Choose the hindsight action using completed stressed outcome only.

    Contract and aggregate-risk limits are still enforced later by the exact
    causal governor.  Ties go to the smaller action to avoid inventing risk.
    """

    allowed = (0.0, 1.5) if binary else ACTION_FRONTIER
    candidates = [
        (
            float(action_rows[_action_key(action)]["stressed_net_pnl_usd"]),
            -float(action),
            float(action),
        )
        for action in allowed
    ]
    score, _, selected = max(candidates)
    return selected if score > 0.0 else 0.0


def _action_key(action: float) -> str:
    return f"{float(action):.1f}X"


def _scaled_event_metrics(trajectory: Any, quantity: int) -> dict[str, float]:
    if quantity <= 0:
        return {
            "gross_pnl_usd": 0.0,
            "net_pnl_usd": 0.0,
            "all_in_cost_usd": 0.0,
            "worst_unrealized_pnl_usd": 0.0,
            "best_unrealized_pnl_usd": 0.0,
            "initial_unrealized_pnl_usd": 0.0,
        }
    source = int(trajectory.event.quantity)
    ratio = float(quantity / source)
    gross = float(trajectory.event.gross_pnl * ratio)
    net = float(trajectory.event.net_pnl * ratio)
    return {
        "gross_pnl_usd": gross,
        "net_pnl_usd": net,
        "all_in_cost_usd": max(0.0, gross - net),
        "worst_unrealized_pnl_usd": float(
            trajectory.event.worst_unrealized_pnl * ratio
        ),
        "best_unrealized_pnl_usd": float(
            trajectory.event.best_unrealized_pnl * ratio
        ),
        "initial_unrealized_pnl_usd": float(
            trajectory.initial_unrealized_pnl * ratio
        ),
    }


def action_counterfactuals(
    opportunity: Mapping[str, Any],
    *,
    account_label: str,
    rules: Mapping[str, Any],
    risk_charge_per_mini: float,
) -> dict[str, dict[str, Any]]:
    """Resolve all four action alternatives for one completed opportunity."""

    normal = opportunity["normal"]
    stressed = opportunity["stressed"]
    source_quantity = int(stressed.event.quantity)
    mini_per_quantity = float(stressed.event.mini_equivalent / source_quantity)
    result: dict[str, dict[str, Any]] = {}
    for action in ACTION_FRONTIER:
        quantity = rounded_contract_quantity(
            source_quantity,
            account_scale=ACCOUNT_RISK_SCALE[account_label],
            action=action,
        )
        requested_mini = quantity * mini_per_quantity
        normal_metrics = _scaled_event_metrics(normal, quantity)
        stressed_metrics = _scaled_event_metrics(stressed, quantity)
        result[_action_key(action)] = {
            "action": action,
            "rounded_quantity": quantity,
            "requested_mini_equivalent": requested_mini,
            "within_standalone_contract_cap": bool(
                requested_mini <= float(rules["maximum_mini_contracts"]) + 1e-12
            ),
            "declared_nominal_risk_usd": requested_mini
            * float(risk_charge_per_mini),
            "within_standalone_mll_risk_cap": bool(
                requested_mini * float(risk_charge_per_mini)
                <= float(rules["maximum_loss_limit_usd"]) + 1e-12
            ),
            "normal_gross_pnl_usd": normal_metrics["gross_pnl_usd"],
            "normal_cost_usd": normal_metrics["all_in_cost_usd"],
            "normal_net_pnl_usd": normal_metrics["net_pnl_usd"],
            "stressed_gross_pnl_usd": stressed_metrics["gross_pnl_usd"],
            "stressed_cost_usd": stressed_metrics["all_in_cost_usd"],
            "stressed_net_pnl_usd": stressed_metrics["net_pnl_usd"],
            "stressed_worst_unrealized_pnl_usd": stressed_metrics[
                "worst_unrealized_pnl_usd"
            ],
            "stressed_best_unrealized_pnl_usd": stressed_metrics[
                "best_unrealized_pnl_usd"
            ],
        }
    return result


def _trajectories_for_actions(
    opportunities: Sequence[Mapping[str, Any]],
    actions: Mapping[str, float],
    *,
    scenario: str,
    account_label: str,
) -> dict[str, tuple[Any, ...]]:
    grouped: dict[str, list[Any]] = {}
    for opportunity in opportunities:
        action = float(actions[str(opportunity["opportunity_id"])])
        if action <= 0.0:
            continue
        candidate_id = str(opportunity["candidate_id"])
        trajectory = (
            opportunity["normal"]
            if scenario == "NORMAL"
            else opportunity["stressed"]
        )
        grouped.setdefault(candidate_id, []).append(
            _resize(
                trajectory,
                account_scale=ACCOUNT_RISK_SCALE[account_label],
                action=action,
            )
        )
    return {
        key: tuple(sorted(values, key=lambda row: row.event.decision_ns))
        for key, values in grouped.items()
    }


def _episode_summary(episodes: Sequence[Any]) -> dict[str, Any]:
    if not episodes:
        raise ValueError("full-action oracle needs at least one episode")
    progress = [float(row.target_progress) for row in episodes]
    nets = [float(row.net_pnl) for row in episodes]
    buffers = [float(row.minimum_mll_buffer) for row in episodes]
    status = Counter(
        allocation["decision_status"]
        for row in episodes
        for allocation in row.risk_allocation_path
    )
    reasons = Counter(
        allocation["reason"]
        for row in episodes
        for allocation in row.risk_allocation_path
        if not allocation["allow"]
    )
    requested_quantity = sum(
        int(allocation["requested_quantity"])
        for row in episodes
        for allocation in row.risk_allocation_path
    )
    admitted_quantity = sum(
        int(allocation["quantity"])
        for row in episodes
        for allocation in row.risk_allocation_path
    )
    return {
        "episode_count": len(episodes),
        "pass_count": sum(bool(row.passed) for row in episodes),
        "pass_rate": sum(bool(row.passed) for row in episodes) / len(episodes),
        "mll_breach_count": sum(bool(row.mll_breached) for row in episodes),
        "mll_breach_rate": sum(bool(row.mll_breached) for row in episodes)
        / len(episodes),
        "consistency_compliance_count": sum(
            bool(row.consistency_ok) for row in episodes
        ),
        "consistency_compliance_rate": sum(
            bool(row.consistency_ok) for row in episodes
        )
        / len(episodes),
        "net_total_usd": sum(nets),
        "net_median_usd": statistics.median(nets),
        "target_progress_p25": statistics.quantiles(progress, n=4)[0]
        if len(progress) >= 4
        else min(progress),
        "target_progress_median": statistics.median(progress),
        "target_progress_maximum": max(progress),
        "minimum_mll_buffer_usd": min(buffers),
        "total_cost_usd": sum(float(row.total_cost) for row in episodes),
        "accepted_event_count": sum(int(row.accepted_events) for row in episodes),
        "skipped_event_count": sum(int(row.skipped_events) for row in episodes),
        "requested_contract_quantity": requested_quantity,
        "admitted_contract_quantity": admitted_quantity,
        "governor_quantity_suppressed": requested_quantity - admitted_quantity,
        "governor_decision_status_counts": dict(sorted(status.items())),
        "governor_rejection_reason_counts": dict(sorted(reasons.items())),
    }


def _open_deterministic_gzip(path: Path):
    raw = path.open("wb")
    compressed = gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0)
    return raw, compressed


def run_full_action_oracle(
    project: Path, *, ledger_path: Path | None = None
) -> dict[str, Any]:
    project = project.resolve()
    sources, bank_receipt = _source_inventory(project)
    manifest = _load_self_hashed_manifest(project / DEFAULT_FAST_PASS_MANIFEST)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(project / DEFAULT_RULE_SNAPSHOT)
    opportunities, censored_days, risk_charges, audit = _opportunities(project, sources)

    logical_ledger_hash = hashlib.sha256()
    ledger_count = 0
    raw_handle = compressed_handle = None
    if ledger_path is not None:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        raw_handle, compressed_handle = _open_deterministic_gzip(ledger_path)

    action_maps: dict[str, dict[str, dict[str, float]]] = {}
    action_aggregates: dict[str, Any] = {}
    try:
        for account_label in ("50K", "100K", "150K"):
            action_maps[account_label] = {selector: {} for selector in SELECTORS}
            selected_counts = {selector: Counter() for selector in SELECTORS}
            aggregates = {
                _action_key(action): {
                    "opportunity_count": 0,
                    "rounded_quantity_total": 0,
                    "normal_gross_pnl_usd": 0.0,
                    "normal_cost_usd": 0.0,
                    "normal_net_pnl_usd": 0.0,
                    "stressed_gross_pnl_usd": 0.0,
                    "stressed_cost_usd": 0.0,
                    "stressed_net_pnl_usd": 0.0,
                    "contract_cap_exceed_count": 0,
                    "standalone_mll_risk_cap_exceed_count": 0,
                }
                for action in ACTION_FRONTIER
            }
            for opportunity in opportunities:
                candidate_id = str(opportunity["candidate_id"])
                rows = action_counterfactuals(
                    opportunity,
                    account_label=account_label,
                    rules=rules[account_label],
                    risk_charge_per_mini=float(risk_charges[candidate_id]),
                )
                full = choose_action(rows, binary=False)
                binary = choose_action(rows, binary=True)
                opportunity_id = str(opportunity["opportunity_id"])
                action_maps[account_label][SELECTORS[0]][opportunity_id] = full
                action_maps[account_label][SELECTORS[1]][opportunity_id] = binary
                selected_counts[SELECTORS[0]][_action_key(full)] += 1
                selected_counts[SELECTORS[1]][_action_key(binary)] += 1
                for action_key, row in rows.items():
                    aggregate = aggregates[action_key]
                    aggregate["opportunity_count"] += 1
                    aggregate["rounded_quantity_total"] += int(
                        row["rounded_quantity"]
                    )
                    for field in (
                        "normal_gross_pnl_usd",
                        "normal_cost_usd",
                        "normal_net_pnl_usd",
                        "stressed_gross_pnl_usd",
                        "stressed_cost_usd",
                        "stressed_net_pnl_usd",
                    ):
                        aggregate[field] += float(row[field])
                    aggregate["contract_cap_exceed_count"] += int(
                        not row["within_standalone_contract_cap"]
                    )
                    aggregate["standalone_mll_risk_cap_exceed_count"] += int(
                        not row["within_standalone_mll_risk_cap"]
                    )
                ledger_row = {
                    "account_label": account_label,
                    "opportunity_id": opportunity_id,
                    "candidate_id": candidate_id,
                    "session_day": int(opportunity["session_day"]),
                    "actions": rows,
                    "selected_full_action": full,
                    "selected_binary_action": binary,
                }
                encoded = (
                    json.dumps(
                        ledger_row,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    + "\n"
                ).encode("utf-8")
                logical_ledger_hash.update(encoded)
                if compressed_handle is not None:
                    compressed_handle.write(encoded)
                ledger_count += 1
            action_aggregates[account_label] = {
                "counterfactual_action_totals": aggregates,
                "selected_action_counts": {
                    selector: dict(sorted(counts.items()))
                    for selector, counts in selected_counts.items()
                },
                "full_and_binary_selection_identical_count": sum(
                    action_maps[account_label][SELECTORS[0]][key]
                    == action_maps[account_label][SELECTORS[1]][key]
                    for key in action_maps[account_label][SELECTORS[0]]
                ),
            }
    finally:
        if compressed_handle is not None:
            compressed_handle.close()
        if raw_handle is not None:
            raw_handle.close()

    account_matrix: dict[str, Any] = {}
    episode_receipts: list[dict[str, Any]] = []
    for account_label in ("50K", "100K", "150K"):
        config = _account_config(rules[account_label])
        account_matrix[account_label] = {}
        for selector in SELECTORS:
            selected_actions = action_maps[account_label][selector]
            selected_components = sorted(
                {
                    str(row["candidate_id"])
                    for row in opportunities
                    if selected_actions[str(row["opportunity_id"])] > 0.0
                }
            )
            policy = _policy(
                f"{account_label}:{selector}",
                rules[account_label],
                selected_components,
                risk_charges,
            )
            account_matrix[account_label][selector] = {}
            for scenario in SCENARIOS:
                trajectories = _trajectories_for_actions(
                    opportunities,
                    selected_actions,
                    scenario=scenario,
                    account_label=account_label,
                )
                account_matrix[account_label][selector][scenario] = {}
                for horizon in HORIZONS:
                    frozen = [
                        (int(day), str(block))
                        for day, block in starts[horizon]
                        if str(block) in {"B1", "B2"}
                    ]
                    episodes = []
                    blocks: Counter[str] = Counter()
                    for start_day, block in frozen:
                        index = calendar.index(start_day)
                        if len(calendar[index : index + horizon]) != horizon:
                            raise RuntimeError("frozen B1/B2 start lost full coverage")
                        episode = run_causal_shared_account_episode(
                            trajectories,
                            calendar,
                            policy=policy,
                            start_day=start_day,
                            maximum_duration_days=horizon,
                            config=config,
                        )
                        episodes.append(episode)
                        blocks[block] += int(episode.passed)
                        episode_receipts.append(
                            {
                                "account_label": account_label,
                                "selector": selector,
                                "scenario": scenario,
                                "horizon_trading_days": horizon,
                                "start_day": start_day,
                                "temporal_block": block,
                                "terminal": episode.terminal.value,
                                "passed": bool(episode.passed),
                                "mll_breached": bool(episode.mll_breached),
                                "consistency_ok": bool(episode.consistency_ok),
                                "net_pnl_usd": float(episode.net_pnl),
                                "target_progress": float(episode.target_progress),
                                "minimum_mll_buffer_usd": float(
                                    episode.minimum_mll_buffer
                                ),
                                "episode_path_hash": stable_hash(
                                    episode.to_dict(include_paths=True)
                                ),
                            }
                        )
                    summary = _episode_summary(episodes)
                    summary["pass_count_by_temporal_block"] = dict(sorted(blocks.items()))
                    account_matrix[account_label][selector][scenario][str(horizon)] = summary

    full_normal_passes = sum(
        int(account_matrix[label][SELECTORS[0]]["NORMAL"][str(h)]["pass_count"])
        for label in account_matrix
        for h in HORIZONS
    )
    binary_normal_passes = sum(
        int(account_matrix[label][SELECTORS[1]]["NORMAL"][str(h)]["pass_count"])
        for label in account_matrix
        for h in HORIZONS
    )
    core = {
        "schema": SCHEMA,
        "status": (
            "FULL_ACTION_ORACLE_ADDS_FEASIBILITY"
            if full_normal_passes > binary_normal_passes
            else "FULL_ACTION_ORACLE_NO_PASS_ADVANTAGE_OVER_BINARY"
        ),
        "evaluation_role": "VIEWED_DEVELOPMENT_B1_B2_NON_DEPLOYABLE_UPPER_BOUND",
        "non_deployable_upper_bound": True,
        "future_outcomes_used_for_action_selection": True,
        "promotion_eligible": False,
        "confirmation_claim": False,
        "action_frontier": list(ACTION_FRONTIER),
        "binary_comparator": [0.0, 1.5],
        "source_inventory": {
            "unique_trajectory_source_count": len(sources),
            "bank_receipt": bank_receipt,
            "fast_pass_manifest_hash": manifest["manifest_hash"],
            "frozen_grid_hash": grid_receipt["grid_hash"],
            "official_rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        },
        "opportunity_consolidation": audit,
        "censored_session_day_count": len(censored_days),
        "censored_opportunities_have_zero_action_and_zero_positive_imputation": True,
        "counterfactual_ledger": {
            "logical_row_count": ledger_count,
            "logical_sha256": logical_ledger_hash.hexdigest(),
            "path": str(ledger_path.relative_to(project))
            if ledger_path is not None and ledger_path.is_relative_to(project)
            else (str(ledger_path) if ledger_path is not None else None),
        },
        "action_diagnostics": action_aggregates,
        "account_matrix": account_matrix,
        "counts": {
            "completed_opportunity_count": len(opportunities),
            "counterfactual_action_evaluation_count": len(opportunities)
            * 3
            * len(ACTION_FRONTIER),
            "exact_account_episode_count": len(episode_receipts),
            "full_action_normal_pass_count": full_normal_passes,
            "binary_normal_pass_count": binary_normal_passes,
        },
        "episode_receipts": episode_receipts,
        "mll_and_consistency_enforced_exactly": True,
        "costs_preserved_from_immutable_causal_trajectories": True,
        "governor_caps_and_rejections_enforced_by_exact_replay": True,
        "data_purchase_count": 0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_state_modified": False,
    }
    return {**core, "result_hash": stable_hash(core)}


def write_full_action_oracle(
    project: Path, output_dir: Path = DEFAULT_OUTPUT_DIR
) -> dict[str, Any]:
    target_dir = output_dir if output_dir.is_absolute() else project / output_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    ledger_tmp = target_dir / "opportunity_action_counterfactuals.jsonl.gz.tmp"
    ledger_final = target_dir / "opportunity_action_counterfactuals.jsonl.gz"
    result = run_full_action_oracle(project, ledger_path=ledger_tmp)
    ledger_tmp.replace(ledger_final)
    result["counterfactual_ledger"]["path"] = str(ledger_final.relative_to(project))
    result["counterfactual_ledger"]["physical_sha256"] = hashlib.sha256(
        ledger_final.read_bytes()
    ).hexdigest()
    core = dict(result)
    core.pop("result_hash", None)
    result["result_hash"] = stable_hash(core)
    output = target_dir / "economic_result.json"
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(output)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    project = Path.cwd().resolve()
    print(
        f"FULL_ACTION_ORACLE_STARTED pid={os.getpid()} actions={ACTION_FRONTIER}",
        flush=True,
    )
    result = write_full_action_oracle(project)
    print(
        json.dumps(
            {
                "status": result["status"],
                "counts": result["counts"],
                "result_hash": result["result_hash"],
                "output": str(project / DEFAULT_OUTPUT_DIR / "economic_result.json"),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
