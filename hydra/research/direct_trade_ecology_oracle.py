"""Bounded non-deployable trade-ecology oracle over immutable 0029 events."""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
)
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_target_velocity_adapter import (
    reconstruct_exact_hazard_replay,
)
from hydra.production.autonomous_exact_replay import (
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_RULE_SNAPSHOT,
    _account_config,
    _apply_session_contract,
    _declared_stop_risk_charge_per_mini,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
    _read_verified_event_evidence,
)
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


SCHEMA = "hydra_direct_trade_ecology_non_deployable_oracle_v1"
DEFAULT_OUTPUT = Path(
    "reports/economic_evolution/direct_trade_ecology_account_policy_v1/"
    "oracle_result.json"
)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
HORIZONS = (5, 10, 20)
ACCOUNT_RISK_SCALE = {"50K": 1.0, "100K": 1.5, "150K": 2.25}
HORIZON_ORDER = {
    "5": 5,
    "15": 15,
    "30": 30,
    "60": 60,
    "SESSION": 390,
    "OVERNIGHT": 960,
}


class DirectTradeEcologyOracleError(RuntimeError):
    """The oracle cannot run without weakening an immutable contract."""


def decision_card() -> dict[str, Any]:
    core = {
        "schema": "hydra_direct_trade_ecology_decision_card_v1",
        "hypothesis": (
            "A hindsight selector over consolidated causal opportunities can "
            "reach a legal Combine target on B1/B2."
        ),
        "strongest_argument_against": (
            "The accumulated opportunities may lack sufficient payoff geometry "
            "even under perfect selection."
        ),
        "smallest_decisive_experiment": (
            "NON_DEPLOYABLE_ORACLE_B1_B2_EXACT_50K_100K_150K_P5_P10_P20"
        ),
        "actions": ["ABSTAIN", "0.5X", "1.0X", "1.5X"],
        "oracle_action_rule": "1.5X_IF_STRESSED_NET_POSITIVE_ELSE_ABSTAIN",
        "censored_opportunity_rule": (
            "ASSUME_ABSTAIN_ZERO_CONTRIBUTION_FOR_NON_DEPLOYABLE_UPPER_BOUND_ONLY"
        ),
        "selection_uses_future_outcome": True,
        "deployable": False,
        "promotion_eligible": False,
        "next_branch_if_feasible": "CHRONOLOGICAL_CROSS_FITTED_DEPLOYABLE_POLICY",
        "next_branch_if_infeasible": "CLOSE_CURRENT_OPPORTUNITY_GEOMETRY",
        "data_purchase": False,
        "q4_access": False,
    }
    return {**core, "decision_card_hash": stable_hash(core)}


def consolidation_key(event: Mapping[str, Any]) -> tuple[str, int, int]:
    """One market/side/decision boundary is one economic opportunity."""

    return (
        str(event["execution_market"]),
        int(event["direction"]),
        int(event["decision_time_ns"]),
    )


def representative_rank(candidate: Mapping[str, Any], candidate_id: str) -> tuple[Any, ...]:
    """Decision-time-only deterministic representative rank."""

    adverse = max(float(candidate["adverse_r"]), 1e-12)
    return (
        float(candidate["favorable_r"]) / adverse,
        -HORIZON_ORDER[str(candidate["horizon"])],
        float(candidate["trigger_quantile"]),
        float(candidate.get("context_quantile") or 0.0),
        stable_hash({"candidate_id": candidate_id}),
    )


def oracle_action(stressed_net_pnl: float | None) -> float:
    """Explicitly non-deployable outcome-aware upper-bound action."""

    return 1.5 if stressed_net_pnl is not None and stressed_net_pnl > 0.0 else 0.0


def _neutral_event_id(value: str) -> str:
    for suffix in (":STRESSED_1_5X", ":NORMAL"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _resize(
    trajectory: CausalTradeTrajectory, *, account_scale: float, action: float
) -> CausalTradeTrajectory:
    source = int(trajectory.event.quantity)
    quantity = max(1, int(math.floor(source * account_scale * action + 1e-12)))
    ratio = quantity / source
    event = replace(
        trajectory.event,
        quantity=quantity,
        mini_equivalent=float(trajectory.event.mini_equivalent * ratio),
        net_pnl=float(trajectory.event.net_pnl * ratio),
        gross_pnl=float(trajectory.event.gross_pnl * ratio),
        worst_unrealized_pnl=float(trajectory.event.worst_unrealized_pnl * ratio),
        best_unrealized_pnl=float(trajectory.event.best_unrealized_pnl * ratio),
    )
    marks = tuple(
        CausalTradeMark(
            availability_time_ns=row.availability_time_ns,
            worst_unrealized_pnl=float(row.worst_unrealized_pnl * ratio),
            best_unrealized_pnl=float(row.best_unrealized_pnl * ratio),
            current_unrealized_pnl=(
                None
                if row.current_unrealized_pnl is None
                else float(row.current_unrealized_pnl * ratio)
            ),
        )
        for row in trajectory.marks
    )
    return replace(
        trajectory,
        event=event,
        marks=marks,
        initial_unrealized_pnl=float(trajectory.initial_unrealized_pnl * ratio),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_inventory(project: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    entries, bank_receipt = _load_banks(project)
    latest: dict[str, dict[str, Any]] = {}
    for raw in entries:
        row = dict(raw)
        candidate_id = str(row["candidate_id"])
        prior = latest.get(candidate_id)
        if prior is None or int(row["_source_wave"]) > int(prior["_source_wave"]):
            latest[candidate_id] = row
    if len(latest) != 180:
        raise DirectTradeEcologyOracleError("expected 180 unique trajectory sources")
    return [latest[key] for key in sorted(latest)], dict(bank_receipt)


def _opportunities(
    project: Path, sources: Sequence[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], set[int], dict[str, float], dict[str, Any]]:
    proposals: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    source_hashes: list[str] = []
    risk_charges: dict[str, float] = {}
    raw_count = 0
    for raw_source in sources:
        source = dict(raw_source)
        candidate_id = str(source["candidate_id"])
        events, receipt = _read_verified_event_evidence(
            project, dict(source["event_evidence"])
        )
        source_hashes.append(str(receipt["sha256"]))
        raw_count += len(events)
        replay = reconstruct_exact_hazard_replay(
            candidate_payload=source["candidate"],
            event_mappings=events,
            eligible_session_days=source["eligible_session_days"],
            expected_hashes=source["exact_hashes"],
        )
        normal, normal_violations = _apply_session_contract(replay.normal_trajectories)
        stressed, stress_violations = _apply_session_contract(
            replay.stressed_trajectories
        )
        if normal_violations != stress_violations:
            raise DirectTradeEcologyOracleError("normal/stress session drift")
        normal_by_id = {
            _neutral_event_id(row.event.event_id): row for row in normal
        }
        stressed_by_id = {
            _neutral_event_id(row.event.event_id): row for row in stressed
        }
        risk_charges[candidate_id] = float(
            _declared_stop_risk_charge_per_mini(events, source["candidate"])
        )
        rank = representative_rank(source["candidate"], candidate_id)
        for raw_event in events:
            event = dict(raw_event)
            event_id = str(event["event_id"])
            proposals.setdefault(consolidation_key(event), []).append(
                {
                    "candidate_id": candidate_id,
                    "candidate": dict(source["candidate"]),
                    "rank": rank,
                    "event": event,
                    "normal": normal_by_id.get(event_id),
                    "stressed": stressed_by_id.get(event_id),
                }
            )

    opportunities: list[dict[str, Any]] = []
    censored_days: set[int] = set()
    support_counts: list[int] = []
    for key, rows in sorted(proposals.items()):
        selected = max(rows, key=lambda row: row["rank"])
        event = dict(selected["event"])
        support_counts.append(len(rows))
        normal = selected["normal"]
        stressed = selected["stressed"]
        if normal is None or stressed is None:
            censored_days.add(int(event["session_day"]))
            continue
        if _neutral_event_id(normal.event.event_id) != _neutral_event_id(
            stressed.event.event_id
        ):
            raise DirectTradeEcologyOracleError("scenario opportunity identity drift")
        opportunities.append(
            {
                "opportunity_id": stable_hash(
                    {"market": key[0], "side": key[1], "decision_ns": key[2]}
                ),
                "candidate_id": selected["candidate_id"],
                "session_day": int(event["session_day"]),
                "normal": normal,
                "stressed": stressed,
                "action": oracle_action(float(event["stressed_net_pnl"])),
                "support_count": len(rows),
            }
        )
    audit = {
        "raw_event_count": raw_count,
        "consolidated_key_count": len(proposals),
        "completed_opportunity_count": len(opportunities),
        "censored_session_day_count": len(censored_days),
        "abstained_opportunity_count": sum(row["action"] == 0.0 for row in opportunities),
        "traded_opportunity_count": sum(row["action"] > 0.0 for row in opportunities),
        "median_raw_support_per_opportunity": statistics.median(support_counts),
        "source_event_receipt_set_hash": stable_hash(sorted(source_hashes)),
        "explicit_horizon_order_minutes": dict(HORIZON_ORDER),
    }
    return opportunities, censored_days, risk_charges, audit


def _policy(
    label: str,
    rules: Mapping[str, Any],
    component_ids: Sequence[str],
    risk_charges: Mapping[str, float],
) -> ActiveRiskPoolPolicy:
    target = float(rules["profit_target_usd"])
    mll = float(rules["maximum_loss_limit_usd"])
    return ActiveRiskPoolPolicy(
        policy_id=f"direct-trade-ecology-oracle:{label}",
        component_priority=tuple(component_ids),
        nominal_risk_charge_per_mini=tuple(
            (value, float(risk_charges[value])) for value in component_ids
        ),
        maximum_concurrent_sleeves=4,
        aggregate_open_risk_ceiling=mll,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=float(rules["maximum_mini_contracts"]),
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=mll,
        daily_consistency_profit_guard=target * float(
            rules["consistency_target_fraction"]
        ),
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _scenario_trajectories(
    opportunities: Sequence[Mapping[str, Any]], *, scenario: str, label: str
) -> dict[str, tuple[CausalTradeTrajectory, ...]]:
    grouped: dict[str, list[CausalTradeTrajectory]] = {}
    for row in opportunities:
        action = float(row["action"])
        if action <= 0.0:
            continue
        candidate_id = str(row["candidate_id"])
        trajectory = row["normal"] if scenario == "NORMAL" else row["stressed"]
        grouped.setdefault(candidate_id, []).append(
            _resize(
                trajectory,
                account_scale=ACCOUNT_RISK_SCALE[label],
                action=action,
            )
        )
    return {
        key: tuple(sorted(values, key=lambda row: row.event.decision_ns))
        for key, values in grouped.items()
    }


def _summarize(episodes: Sequence[Any], *, requested: int, censored: int) -> dict[str, Any]:
    if not episodes:
        return {
            "requested_start_count": requested,
            "full_coverage_start_count": 0,
            "data_censored_start_count": censored,
            "pass_count": 0,
            "pass_rate": 0.0,
            "mll_breach_count": 0,
            "mll_breach_rate": 0.0,
            "consistency_compliance_rate": 0.0,
            "net_total_usd": 0.0,
            "target_progress_median": None,
            "minimum_mll_buffer_usd": None,
        }
    return {
        "requested_start_count": requested,
        "full_coverage_start_count": len(episodes),
        "data_censored_start_count": censored,
        "pass_count": sum(bool(row.passed) for row in episodes),
        "pass_rate": sum(bool(row.passed) for row in episodes) / len(episodes),
        "mll_breach_count": sum(bool(row.mll_breached) for row in episodes),
        "mll_breach_rate": sum(bool(row.mll_breached) for row in episodes)
        / len(episodes),
        "consistency_compliance_rate": sum(bool(row.consistency_ok) for row in episodes)
        / len(episodes),
        "net_total_usd": sum(float(row.net_pnl) for row in episodes),
        "target_progress_median": statistics.median(
            float(row.target_progress) for row in episodes
        ),
        "minimum_mll_buffer_usd": min(float(row.minimum_mll_buffer) for row in episodes),
    }


def run_oracle(project: Path) -> dict[str, Any]:
    project = project.resolve()
    sources, bank_receipt = _source_inventory(project)
    manifest = _load_self_hashed_manifest(project / DEFAULT_FAST_PASS_MANIFEST)
    calendar, starts, grid_receipt = _load_frozen_grid(project, manifest)
    rules, rule_receipt = _load_rule_snapshot(project / DEFAULT_RULE_SNAPSHOT)
    opportunities, censored_days, risk_charges, audit = _opportunities(
        project, sources
    )
    selected_components = sorted(
        {str(row["candidate_id"]) for row in opportunities if row["action"] > 0.0}
    )
    matrix: dict[str, Any] = {}
    episode_receipts: list[dict[str, Any]] = []
    for label in ("50K", "100K", "150K"):
        config = _account_config(rules[label])
        policy = _policy(label, rules[label], selected_components, risk_charges)
        matrix[label] = {}
        for scenario in SCENARIOS:
            trajectories = _scenario_trajectories(
                opportunities, scenario=scenario, label=label
            )
            matrix[label][scenario] = {}
            for horizon in HORIZONS:
                frozen = [
                    (int(day), str(block))
                    for day, block in starts[horizon]
                    if str(block) in {"B1", "B2"}
                ]
                episodes = []
                censored = 0
                for start_day, block in frozen:
                    index = calendar.index(start_day)
                    window = calendar[index : index + horizon]
                    if len(window) != horizon:
                        censored += 1
                        continue
                    episode = run_causal_shared_account_episode(
                        trajectories,
                        calendar,
                        policy=policy,
                        start_day=start_day,
                        maximum_duration_days=horizon,
                        config=config,
                    )
                    episodes.append(episode)
                    episode_receipts.append(
                        {
                            "account_label": label,
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
                matrix[label][scenario][str(horizon)] = _summarize(
                    episodes, requested=len(frozen), censored=censored
                )

    normal_passes = sum(
        int(matrix[label]["NORMAL"][str(horizon)]["pass_count"])
        for label in matrix
        for horizon in HORIZONS
    )
    stressed_passes = sum(
        int(matrix[label]["STRESSED_1_5X"][str(horizon)]["pass_count"])
        for label in matrix
        for horizon in HORIZONS
    )
    core = {
        "schema": SCHEMA,
        "status": (
            "NON_DEPLOYABLE_ORACLE_FEASIBLE"
            if normal_passes > 0
            else "NON_DEPLOYABLE_ORACLE_INFEASIBLE"
        ),
        "decision_card": decision_card(),
        "source_inventory": {
            "unique_trajectory_source_count": len(sources),
            "bank_receipt": bank_receipt,
            "fast_pass_manifest_hash": manifest["manifest_hash"],
            "frozen_grid_hash": grid_receipt["grid_hash"],
            "official_rule_snapshot_hash": rule_receipt["parsed_rule_hash"],
        },
        "opportunity_consolidation": audit,
        "evaluation_role": "VIEWED_DEVELOPMENT_B1_B2_ONLY",
        "non_deployable_oracle": True,
        "oracle_future_outcomes_used": True,
        "censored_opportunities_assigned_oracle_abstain": True,
        "censored_opportunity_positive_outcomes_imputed": 0,
        "censored_rule_permitted_for_deployable_policy": False,
        "promotion_status": None,
        "stress_role_for_observed_normal_pass": "ADVISORY",
        "mll_and_consistency_enforced_exactly": True,
        "account_matrix": matrix,
        "counts": {
            "normal_pass_observation_count": normal_passes,
            "stressed_pass_observation_count_advisory": stressed_passes,
            "episode_count": len(episode_receipts),
        },
        "episode_receipts": episode_receipts,
        "next_action": (
            "START_CHRONOLOGICAL_CROSS_FITTED_DEPLOYABLE_POLICY_PILOT"
            if normal_passes > 0
            else "CLOSE_CURRENT_OPPORTUNITY_GEOMETRY_AND_PIVOT"
        ),
        "data_purchase_count": 0,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "authoritative_state_modified": False,
    }
    return {**core, "result_hash": stable_hash(core)}


def write_oracle(project: Path, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    target = output if output.is_absolute() else project / output
    target.parent.mkdir(parents=True, exist_ok=True)
    card = decision_card()
    card_path = target.parent / "decision_card.json"
    card_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    result = run_oracle(project)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    del argv
    project = Path.cwd().resolve()
    print(
        f"TRADE_ECOLOGY_ORACLE_STARTED pid={os.getpid()} scope=B1_B2 sources=180",
        flush=True,
    )
    result = write_oracle(project)
    print(
        json.dumps(
            {
                "status": result["status"],
                "counts": result["counts"],
                "result_hash": result["result_hash"],
                "output": str(project / DEFAULT_OUTPUT),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
