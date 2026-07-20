"""Read-only XFA alternatives and block validation for the PnL-state frontier.

This adapter binds the reconciled development receipt, reconstructs only its
clean normal Combine pass paths, and feeds the existing account-size-aware
causal XFA engine.  Standard and Consistency remain separate alternatives.
No evidence tier, registry, database, service, or controller is modified.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.economic_evolution.schema import stable_hash
from hydra.propfirm.account_size_xfa import (
    freeze_account_size_xfa_handoff,
    load_account_size_xfa_rules,
    run_account_size_xfa_alternatives,
)
from hydra.production.autonomous_exact_replay import (
    HORIZONS,
    _account_config,
    _load_banks,
    _load_frozen_grid,
    _load_rule_snapshot,
    _load_self_hashed_manifest,
)
from hydra.research.causal_sleeve_replay import CausalTradeTrajectory
from hydra.research.pnl_state_risk_frontier import (
    ALL_BLOCKS,
    DEFAULT_CANDIDATE_BANK,
    DEFAULT_CLEAN_LEDGER,
    DEFAULT_EXACT_SOURCE_PATHS,
    DEFAULT_FAST_PASS_MANIFEST,
    DEFAULT_MARGINAL_BOOKS,
    DEFAULT_PASS_BANK,
    DEFAULT_QUARANTINE,
    DEFAULT_RECONCILIATION,
    DEFAULT_RULE_SNAPSHOT,
    _clean_inventory,
    _inside,
    _isolated_router_patch,
    _load_exact_cell_sources,
    _prepare_policy,
    _read_json,
    _unwrap,
    _valid_starts,
    _verify_clean_ledger,
    _verify_mapping_hash,
    frozen_pnl_state_profiles,
)
import hydra.account_policy.causal_active_pool_replay as causal_replay


SCHEMA = "hydra_pnl_state_xfa_diagnostic_v1"
VALIDATION_SCHEMA = "hydra_pnl_state_chronological_validation_v1"
DECISION_SUMMARY_SCHEMA = "hydra_pnl_state_xfa_decision_summary_v1"
DEFAULT_RECONCILED = Path(
    "reports/economic_evolution/pnl_state_risk_frontier_v1/"
    "economic_result_reconciled.json"
)
EXPECTED_RECONCILED_HASH = (
    "02236c9997d064bc9712584fec3060ee20e923e03811b1c00804821cd2cd3817"
)
DYNAMIC_SURVIVOR_IDS = (
    "hazard_09ee35bc24190bb213469e6a",
    "hazard_10ffb41856432af08259e32b",
    "hazard_294627b596f6b477df6de5ac",
)


class PnLStateXfaDiagnosticError(RuntimeError):
    """The isolated diagnostic cannot reconcile an immutable input."""


def build_pnl_state_xfa_diagnostic(
    root: str | Path,
    *,
    policy_ids: Sequence[str] | None = None,
    reconciled_path: str | Path = DEFAULT_RECONCILED,
    xfa_horizon_days: int = 120,
) -> dict[str, Any]:
    """Materialize XFA alternatives for selected clean normal pass handoffs."""

    project = Path(root).resolve()
    source = _read_json(_inside(project, reconciled_path))
    _verify_reconciled(source)
    requested_ids = None if policy_ids is None else {str(value) for value in policy_ids}
    handoff_rows = [
        dict(row)
        for row in source["diagnostic_xfa_handoff"]
        if requested_ids is None or str(row["policy_id"]) in requested_ids
    ]
    if not handoff_rows:
        raise PnLStateXfaDiagnosticError("selected XFA handoff inventory is empty")
    selected_ids = {str(row["policy_id"]) for row in handoff_rows}
    if requested_ids is not None and selected_ids != requested_ids:
        raise PnLStateXfaDiagnosticError("requested policy lacks a clean handoff")

    context = _load_context(project, selected_ids)
    result_rows = {
        str(row["policy_id"]): dict(row) for row in source["policy_results"]
    }
    profiles = {row.profile_id: row for row in frozen_pnl_state_profiles()}
    rule_cache: dict[str, Any] = {}
    transitions: list[dict[str, Any]] = []
    engine_results: list[dict[str, Any]] = []
    path_records: list[dict[str, Any]] = []
    replay_cache: dict[tuple[str, int], list[tuple[Any, str, int]]] = {}

    for handoff_row in sorted(
        handoff_rows,
        key=lambda row: (str(row["policy_id"]), int(row["horizon_trading_days"])),
    ):
        policy_id = str(handoff_row["policy_id"])
        prepared = context["prepared"][policy_id]
        policy_result = result_rows[policy_id]
        profile_id = str(policy_result["selected_profile_id"])
        if profile_id != str(handoff_row["selected_profile_id"]):
            raise PnLStateXfaDiagnosticError("handoff/profile identity drift")
        profile = profiles[profile_id]
        horizon = int(handoff_row["horizon_trading_days"])
        cache_key = (policy_id, horizon)
        episodes = replay_cache.setdefault(
            cache_key,
            _replay_normal_grid(
                prepared,
                profile,
                horizon=horizon,
                calendar=context["calendar"],
                starts=context["starts"],
                rule=context["rules"][prepared.account_label],
            ),
        )
        episode_payloads = [
            episode.to_dict(include_paths=True) for episode, _block, _start in episodes
        ]
        if (
            stable_hash(episode_payloads)
            != str(handoff_row["normal_episode_path_hash"])
            or sum(int(episode.passed) for episode, _block, _start in episodes)
            != int(handoff_row["normal_pass_count"])
        ):
            raise PnLStateXfaDiagnosticError(
                f"reconstructed Combine paths drift: {policy_id}:{horizon}"
            )
        rules = rule_cache.setdefault(
            prepared.account_label,
            load_account_size_xfa_rules(
                prepared.account_label,
                snapshot_path=project / "config/rulesets/topstep_official_2026-07-19.json",
            ),
        )
        combine_book_hash = stable_hash(
            {
                "policy_id": policy_id,
                "account_label": prepared.account_label,
                "baseline_policy": prepared.baseline_policy.to_dict(),
                "selected_profile": profile.to_dict(),
                "selected_result_hash": policy_result["selected"]["result_hash"],
            }
        )
        frozen = freeze_account_size_xfa_handoff(
            candidate_id=policy_id,
            combine_book_hash=combine_book_hash,
            component_priority=prepared.baseline_policy.component_ids,
            rules=rules,
            risk_multiplier=1.0,
            maximum_simultaneous_positions=(
                prepared.baseline_policy.maximum_concurrent_sleeves
            ),
            maximum_mini_equivalent=min(
                float(prepared.baseline_policy.maximum_mini_equivalent),
                float(rules.combine_maximum_mini_equivalent),
            ),
            same_market_exclusive=True,
            profile_id=f"{policy_id}:pnl-state-xfa-static-1x:{prepared.account_label}",
        )
        pass_index = 0
        for episode, block, start_day in episodes:
            if not episode.passed:
                continue
            if episode.mll_breached:
                raise PnLStateXfaDiagnosticError("MLL-breached pass cannot enter XFA")
            pass_index += 1
            xfa_days = _continuation_days(
                context["calendar"],
                after_day=int(episode.end_day),
                unavailable=prepared.unavailable_days,
            )
            if not xfa_days:
                raise PnLStateXfaDiagnosticError(
                    "clean Combine pass has no causal XFA continuation day"
                )
            trajectories = _continuation_trajectories(
                prepared.trajectories["NORMAL"],
                start_day=xfa_days[0],
                end_day=xfa_days[-1],
            )
            combine_path_hash = stable_hash(episode.to_dict(include_paths=True))
            transition_identity = {
                "source_handoff_hash": handoff_row["handoff_hash"],
                "policy_id": policy_id,
                "horizon": horizon,
                "block": block,
                "start_day": start_day,
                "combine_path_hash": combine_path_hash,
            }
            transition_id = "pnl-xfa-" + stable_hash(transition_identity)[:24]
            result = run_account_size_xfa_alternatives(
                trajectories,
                xfa_days,
                handoff=frozen,
                rules=rules,
                transition_id=transition_id,
                combine_path_hash=combine_path_hash,
                start_day=xfa_days[0],
                horizon_days=int(xfa_horizon_days),
            ).to_dict()
            transition = {
                "transition_id": transition_id,
                "source_handoff_hash": handoff_row["handoff_hash"],
                "policy_id": policy_id,
                "source_evidence_tier": handoff_row["source_evidence_tier"],
                "selected_profile_id": profile_id,
                "account_label": prepared.account_label,
                "horizon_trading_days": horizon,
                "temporal_block": block,
                "combine_start_day": start_day,
                "combine_end_day": int(episode.end_day),
                "combine_path_hash": combine_path_hash,
                "combine_book_hash": combine_book_hash,
                "xfa_start_day": xfa_days[0],
                "available_xfa_days": len(xfa_days),
                "frozen_xfa_handoff_hash": frozen.handoff_hash,
                "engine_result_hash": result["result_hash"],
                "promotion_implied": False,
            }
            transitions.append(transition)
            engine_results.append(result)
            for path in ("STANDARD", "CONSISTENCY"):
                value = dict(result["alternatives"][path])
                core = {
                    "transition_id": transition_id,
                    "policy_id": policy_id,
                    "account_label": prepared.account_label,
                    "horizon_trading_days": horizon,
                    "path": path,
                    "engine_path_hash": value["path_hash"],
                    "terminal": value["terminal"],
                    "first_payout_count": value["first_payout_count"],
                    "first_payout_day": value["first_payout_day"],
                    "payout_cycles": value["payout_cycles"],
                    "trader_net_payout_usd": value["trader_net_payout"],
                    "minimum_mll_buffer_usd": value["minimum_mll_buffer"],
                    "post_payout_survived": value["post_payout_survived"],
                    "standard_and_consistency_are_alternatives": True,
                    "promotion_implied": False,
                }
                path_records.append({**core, "path_record_hash": stable_hash(core)})
        if pass_index != int(handoff_row["normal_pass_count"]):
            raise PnLStateXfaDiagnosticError("handoff transition count drift")

    aggregates = _aggregate_paths(path_records, handoff_rows)
    counts = {
        "source_handoff_record_count": len(handoff_rows),
        "clean_normal_combine_transition_count": len(transitions),
        "alternative_path_count": len(path_records),
        "standard_path_count": sum(row["path"] == "STANDARD" for row in path_records),
        "consistency_path_count": sum(
            row["path"] == "CONSISTENCY" for row in path_records
        ),
        "standard_first_payout_count": sum(
            int(row["first_payout_count"])
            for row in path_records
            if row["path"] == "STANDARD"
        ),
        "consistency_first_payout_count": sum(
            int(row["first_payout_count"])
            for row in path_records
            if row["path"] == "CONSISTENCY"
        ),
        "authoritative_promotion_count": 0,
        "database_writes": 0,
        "registry_writes": 0,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_READ_ONLY_PNL_STATE_XFA_DIAGNOSTIC",
        "source_reconciled_result_hash": source["result_hash"],
        "scope_policy_ids": sorted(selected_ids),
        "official_rule_snapshot_hashes": {
            label: rules.fingerprint for label, rules in sorted(rule_cache.items())
        },
        "xfa_profile_contract": (
            "FROZEN_STATIC_1X_SAME_MEMBERSHIP_SCALING_PLAN_CLIPPED"
        ),
        "requested_xfa_horizon_days": int(xfa_horizon_days),
        "transitions": transitions,
        "alternative_results": engine_results,
        "path_records": path_records,
        "aggregates": aggregates,
        "counts": counts,
        "standard_and_consistency_are_alternatives": True,
        "sum_standard_and_consistency_ev_allowed": False,
        "selected_path": None,
        "evidence_role": "DIAGNOSTIC_XFA_FROM_VIEWED_DEVELOPMENT_PASSES",
        "independent_confirmation_claimed": False,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def build_dynamic_survivor_chronological_validation(
    root: str | Path,
    *,
    reconciled_path: str | Path = DEFAULT_RECONCILED,
) -> dict[str, Any]:
    """Seal B3/B4 post-selection evidence without claiming fresh confirmation."""

    project = Path(root).resolve()
    source = _read_json(_inside(project, reconciled_path))
    _verify_reconciled(source)
    survivors = {
        str(row["policy_id"]): dict(row) for row in source["dynamic_survivors"]
    }
    if set(survivors) != set(DYNAMIC_SURVIVOR_IDS):
        raise PnLStateXfaDiagnosticError("dynamic survivor inventory drift")
    rows = {str(row["policy_id"]): dict(row) for row in source["policy_results"]}
    results = []
    for policy_id in DYNAMIC_SURVIVOR_IDS:
        row = rows[policy_id]
        by_horizon = {}
        for horizon in HORIZONS:
            scenarios = {}
            for scenario in ("NORMAL", "STRESSED_1_5X"):
                summary = row["selected"]["summaries"][scenario][str(horizon)]
                blocks = {
                    block: dict(summary["by_block"].get(block) or {})
                    for block in ("B3", "B4")
                }
                scenarios[scenario] = {
                    "episode_count": sum(
                        int(value.get("episode_count", 0)) for value in blocks.values()
                    ),
                    "pass_count": sum(
                        int(value.get("pass_count", 0)) for value in blocks.values()
                    ),
                    "mll_breach_count": sum(
                        int(value.get("mll_breach_count", 0))
                        for value in blocks.values()
                    ),
                    "net_total_usd": sum(
                        float(value.get("net_total_usd", 0.0))
                        for value in blocks.values()
                    ),
                    "blocks": blocks,
                }
            by_horizon[str(horizon)] = scenarios
        core = {
            "policy_id": policy_id,
            "selected_profile_id": row["selected_profile_id"],
            "source_policy_result_hash": row["result_hash"],
            "selection_blocks": ["B1", "B2"],
            "post_selection_blocks": ["B3", "B4"],
            "results": by_horizon,
            "profile_mutated": False,
            "retuning_performed": False,
            "evidence_role": "HELD_OUT_DEVELOPMENT_ALREADY_VIEWED_ONCE",
            "fresh_confirmation_claimed": False,
        }
        results.append({**core, "validation_hash": stable_hash(core)})
    core = {
        "schema": VALIDATION_SCHEMA,
        "status": "DYNAMIC_SURVIVORS_FROZEN_HELD_OUT_DEVELOPMENT_SEALED",
        "source_reconciled_result_hash": source["result_hash"],
        "frozen_policy_ids": list(DYNAMIC_SURVIVOR_IDS),
        "frozen_profile_ids": {
            policy_id: survivors[policy_id]["selected_profile_id"]
            for policy_id in DYNAMIC_SURVIVOR_IDS
        },
        "results": results,
        "fresh_untouched_role_available": False,
        "fresh_role_blocker": (
            "B3_B4_WERE_EVALUATED_ONCE_AFTER_B1_B2_SELECTION;_THEY_ARE_"
            "DEFENSIBLE_HELD_OUT_DEVELOPMENT_BUT_NOT_FRESH_CONFIRMATION"
        ),
        "next_role": "GENUINELY_NEW_OR_PREVIOUSLY_UNTOUCHED_CONFIRMATION_DATA_REQUIRED",
        "promotion_status": None,
        "database_writes": 0,
        "registry_writes": 0,
        "broker_connections": 0,
        "orders": 0,
    }
    return {**core, "result_hash": stable_hash(core)}


def build_xfa_decision_summary(
    xfa_result: Mapping[str, Any],
    validation_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Summarize the two XFA alternatives without combining their value."""

    _verify_materialized_result(xfa_result, schema=SCHEMA)
    _verify_materialized_result(validation_result, schema=VALIDATION_SCHEMA)
    transitions = tuple(xfa_result["transitions"])
    engine_results = tuple(xfa_result["alternative_results"])
    path_records = tuple(xfa_result["path_records"])
    transition_ids = [str(row["transition_id"]) for row in transitions]
    if len(set(transition_ids)) != len(transition_ids):
        raise PnLStateXfaDiagnosticError("duplicate XFA transition identity")
    if len(engine_results) != len(transitions):
        raise PnLStateXfaDiagnosticError("XFA engine/transition count drift")
    if len(path_records) != 2 * len(transitions):
        raise PnLStateXfaDiagnosticError("XFA alternatives are not exactly two per pass")
    path_keys = [(str(row["transition_id"]), str(row["path"])) for row in path_records]
    if len(set(path_keys)) != len(path_keys):
        raise PnLStateXfaDiagnosticError("duplicate XFA path alternative")
    if set(path for _transition, path in path_keys) != {"STANDARD", "CONSISTENCY"}:
        raise PnLStateXfaDiagnosticError("XFA alternative identity drift")
    if any(int(row["first_payout_count"]) not in (0, 1) for row in path_records):
        raise PnLStateXfaDiagnosticError("first payout is not unique per XFA path")

    by_path: dict[str, dict[str, Any]] = {}
    for path in ("STANDARD", "CONSISTENCY"):
        records = [row for row in path_records if str(row["path"]) == path]
        alternatives = [dict(row["alternatives"][path]) for row in engine_results]
        first_payouts = sum(int(row["first_payout_count"]) for row in records)
        if first_payouts != sum(
            int(row["first_payout_count"]) for row in alternatives
        ):
            raise PnLStateXfaDiagnosticError("first-payout summary drift")
        total_net = sum(float(row["trader_net_payout_usd"]) for row in records)
        cycles = sum(int(row["payout_cycles"]) for row in records)
        closed_before_payout = sum(
            int(row["first_payout_count"]) == 0
            and str(row["terminal"]) in {"MLL_BREACHED", "HARD_RULE_FAILURE"}
            for row in records
        )
        censored_before_payout = sum(
            int(row["first_payout_count"]) == 0
            and str(row["terminal"]) == "DATA_CENSORED"
            for row in records
        )
        by_path[path] = {
            "path_count": len(records),
            "first_payout_count": first_payouts,
            "first_payout_rate_per_successful_combine": (
                first_payouts / len(records) if records else 0.0
            ),
            "account_closed_before_first_payout_count": closed_before_payout,
            "data_censored_before_first_payout_count": censored_before_payout,
            "payout_cycles_total": cycles,
            "trader_net_payout_total_usd": total_net,
            "trader_net_payout_per_successful_combine_usd": (
                total_net / len(records) if records else 0.0
            ),
            "trader_net_payout_per_first_payout_path_usd": (
                total_net / first_payouts if first_payouts else 0.0
            ),
            "terminal_distribution": dict(
                sorted(Counter(str(row["terminal"]) for row in records).items())
            ),
            "post_payout_survival": _post_payout_survival(alternatives),
        }

    eligible = [
        dict(row)
        for row in xfa_result["aggregates"]
        if int(row["first_payout_count"]) > 0
        and float(row["minimum_mll_buffer_usd"]) >= 0.0
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["expected_trader_payout_per_new_combine_attempt_usd"]),
            str(row["policy_id"]),
            int(row["horizon_trading_days"]),
            str(row["path"]),
        )
    )
    maximum_post_payout_days = max(
        (
            int(row["observed_days"]) - int(row["first_payout_day"])
            for result in engine_results
            for row in result["alternatives"].values()
            if row["first_payout_day"] is not None
        ),
        default=0,
    )
    core = {
        "schema": DECISION_SUMMARY_SCHEMA,
        "status": "COMPLETE_SEPARATE_XFA_ALTERNATIVE_DECISION_SUMMARY",
        "source_xfa_result_hash": xfa_result["result_hash"],
        "source_validation_result_hash": validation_result["result_hash"],
        "clean_normal_combine_transition_count": len(transitions),
        "alternative_path_count": len(path_records),
        "alternative_summaries": by_path,
        "standard_and_consistency_are_alternatives": True,
        "sum_standard_and_consistency_ev_allowed": False,
        "selected_path": None,
        "full_120_day_post_payout_survivor_count": sum(
            bool(row["post_payout_survived"]) for row in path_records
        ),
        "maximum_observed_post_payout_trading_days": maximum_post_payout_days,
        "post_payout_survival_zero_explanation": (
            "NO_PATH_WITH_A_PAYOUT_REACHED_SURVIVED_HORIZON_AT_120_DAYS;_"
            "PATHS_EITHER_BREACHED_MLL_OR_WERE_DATA_CENSORED_BEFORE_120_DAYS"
        ),
        "nonnegative_buffer_payout_policy_horizons": eligible,
        "best_nonnegative_buffer_ev_per_new_combine_attempt": (
            eligible[0] if eligible else None
        ),
        "validation_status": validation_result["status"],
        "fresh_confirmation_claimed": False,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def _load_context(project: Path, selected_ids: set[str]) -> dict[str, Any]:
    pass_bank = _unwrap(_read_json(_inside(project, DEFAULT_PASS_BANK)), "combine_pass_observed_bank")
    candidate_bank = _unwrap(_read_json(_inside(project, DEFAULT_CANDIDATE_BANK)), "candidate_bank")
    marginal = _unwrap(_read_json(_inside(project, DEFAULT_MARGINAL_BOOKS)), "marginal_book_composite")
    quarantine = _read_json(_inside(project, DEFAULT_QUARANTINE))
    reconciliation = _read_json(_inside(project, DEFAULT_RECONCILIATION))
    ledger = _read_json(_inside(project, DEFAULT_CLEAN_LEDGER))
    _verify_mapping_hash(quarantine)
    _verify_mapping_hash(reconciliation)
    _verify_clean_ledger(ledger, reconciliation)
    clean, _excluded = _clean_inventory(pass_bank, ledger)
    clean_by_id = {str(row["policy_id"]): row for row in clean}
    if not selected_ids.issubset(clean_by_id):
        raise PnLStateXfaDiagnosticError("XFA scope escaped canonical clean ledger")
    manifest = _load_self_hashed_manifest(_inside(project, DEFAULT_FAST_PASS_MANIFEST))
    calendar, starts, _grid = _load_frozen_grid(project, manifest)
    rules, _rule = _load_rule_snapshot(_inside(project, DEFAULT_RULE_SNAPSHOT))
    banks, _receipt = _load_banks(project)
    classified = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_bank.get("candidates", ())
    }
    books = {str(row["policy_id"]): dict(row) for row in marginal.get("book_results", ())}
    exact_cells, _receipt = _load_exact_cell_sources(project, DEFAULT_EXACT_SOURCE_PATHS)
    raw_cache = {}
    prepared = {
        policy_id: _prepare_policy(
            project,
            clean_by_id[policy_id],
            classified=classified,
            books=books,
            bank_entries=banks,
            calendar=calendar,
            rules=rules,
            raw_cache=raw_cache,
            exact_cells=exact_cells,
        )
        for policy_id in sorted(selected_ids)
    }
    return {"prepared": prepared, "calendar": calendar, "starts": starts, "rules": rules}


def _replay_normal_grid(
    prepared: Any,
    profile: Any,
    *,
    horizon: int,
    calendar: Sequence[int],
    starts: Mapping[int, Sequence[tuple[int, str]]],
    rule: Mapping[str, Any],
) -> list[tuple[Any, str, int]]:
    requested = [(int(day), str(block)) for day, block in starts[horizon] if block in ALL_BLOCKS]
    valid, _censored = _valid_starts(
        requested,
        horizon=horizon,
        calendar=calendar,
        unavailable=prepared.unavailable_days,
    )
    policy = prepared.baseline_policy.__class__.from_mapping(
        {
            **prepared.baseline_policy.to_dict(),
            "policy_id": f"{prepared.policy_id}:pnl-state:{profile.profile_id}",
        }
    )
    output = []
    with _isolated_router_patch(
        profile,
        target_usd=float(rule["profit_target_usd"]),
        mll_usd=float(rule["maximum_loss_limit_usd"]),
        consistency_fraction=float(rule["consistency_target_fraction"]),
    ):
        for start_day, block in valid:
            episode = causal_replay.run_causal_shared_account_episode(
                prepared.trajectories["NORMAL"],
                calendar,
                policy=policy,
                start_day=start_day,
                maximum_duration_days=horizon,
                config=_account_config(rule),
            )
            output.append((episode, block, start_day))
    return output


def _continuation_days(
    calendar: Sequence[int], *, after_day: int, unavailable: frozenset[int]
) -> tuple[int, ...]:
    values = [int(day) for day in calendar if int(day) > after_day]
    output = []
    for day in values:
        if day in unavailable:
            break
        output.append(day)
    return tuple(output)


def _continuation_trajectories(
    trajectories: Mapping[str, Sequence[Any]], *, start_day: int, end_day: int
) -> dict[str, tuple[CausalTradeTrajectory, ...]]:
    output = {}
    for component_id, values in trajectories.items():
        selected = tuple(
            row
            for row in values
            if start_day <= int(row.event.session_day) <= end_day
        )
        if any(not isinstance(row, CausalTradeTrajectory) for row in selected):
            raise PnLStateXfaDiagnosticError("XFA continuation includes censored trajectory")
        output[str(component_id)] = selected
    return output


def _aggregate_paths(
    records: Sequence[Mapping[str, Any]], handoffs: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    denominators = {
        (str(row["policy_id"]), int(row["horizon_trading_days"])): int(
            row["normal_full_coverage_start_count"]
        )
        for row in handoffs
    }
    grouped: dict[tuple[str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        grouped[(str(row["policy_id"]), int(row["horizon_trading_days"]), str(row["path"]))].append(row)
    output = []
    for (policy_id, horizon, path), values in sorted(grouped.items()):
        denominator = denominators[(policy_id, horizon)]
        first = sum(int(row["first_payout_count"]) for row in values)
        net = sum(float(row["trader_net_payout_usd"]) for row in values)
        cycles = sum(int(row["payout_cycles"]) for row in values)
        core = {
            "policy_id": policy_id,
            "horizon_trading_days": horizon,
            "path": path,
            "combine_attempt_count": denominator,
            "combine_pass_count": len(values),
            "first_payout_count": first,
            "first_payout_rate_per_successful_combine": first / len(values),
            "payout_cycles_total": cycles,
            "trader_net_payout_total_usd": net,
            "expected_trader_payout_per_successful_combine_usd": net / len(values),
            "expected_trader_payout_per_new_combine_attempt_usd": net / denominator,
            "minimum_mll_buffer_usd": min(
                float(row["minimum_mll_buffer_usd"]) for row in values
            ),
            "terminal_distribution": dict(
                sorted(Counter(str(row["terminal"]) for row in values).items())
            ),
            "alternative_value_not_additive": True,
        }
        output.append({**core, "aggregate_hash": stable_hash(core)})
    return output


def _post_payout_survival(
    alternatives: Sequence[Mapping[str, Any]],
    *,
    checkpoints: Sequence[int] = (30, 60, 90),
) -> dict[str, Any]:
    first_payout_paths = [
        row for row in alternatives if row.get("first_payout_day") is not None
    ]
    values: dict[str, Any] = {
        "first_payout_path_count": len(first_payout_paths),
        "checkpoint_unit": "TRADING_DAYS_AFTER_FIRST_PAYOUT",
        "checkpoints": {},
    }
    for checkpoint in checkpoints:
        survived = failed = censored = 0
        for row in first_payout_paths:
            post_days = int(row["observed_days"]) - int(row["first_payout_day"])
            terminal = str(row["terminal"])
            if terminal in {"MLL_BREACHED", "HARD_RULE_FAILURE"} and post_days <= checkpoint:
                failed += 1
            elif post_days >= checkpoint:
                survived += 1
            elif terminal in {"DATA_CENSORED", "SURVIVED_HORIZON"}:
                censored += 1
            else:
                raise PnLStateXfaDiagnosticError(
                    "unclassified post-payout survival state"
                )
        evaluable = survived + failed
        values["checkpoints"][str(int(checkpoint))] = {
            "survived_count": survived,
            "failed_before_or_on_checkpoint_count": failed,
            "data_censored_before_checkpoint_count": censored,
            "evaluable_count": evaluable,
            "survival_rate_among_evaluable": (
                survived / evaluable if evaluable else None
            ),
            "demonstrated_survival_rate_all_first_payout_paths": (
                survived / len(first_payout_paths) if first_payout_paths else None
            ),
        }
    return values


def _verify_materialized_result(
    value: Mapping[str, Any], *, schema: str
) -> None:
    if value.get("schema") != schema:
        raise PnLStateXfaDiagnosticError("materialized diagnostic schema drift")
    expected = stable_hash(
        {key: item for key, item in value.items() if key != "result_hash"}
    )
    if value.get("result_hash") != expected:
        raise PnLStateXfaDiagnosticError("materialized diagnostic hash drift")


def _verify_reconciled(value: Mapping[str, Any]) -> None:
    core = {
        key: item
        for key, item in value.items()
        if key not in {"result_hash", "runtime_seconds"}
    }
    if (
        value.get("result_hash") != EXPECTED_RECONCILED_HASH
        or stable_hash(core) != EXPECTED_RECONCILED_HASH
        or value.get("status")
        != "PNL_STATE_SIZING_FRONTIER_RECONCILED_DEVELOPMENT_DIAGNOSTIC"
    ):
        raise PnLStateXfaDiagnosticError("reconciled frontier identity/hash drift")


__all__ = [
    "DECISION_SUMMARY_SCHEMA",
    "DYNAMIC_SURVIVOR_IDS",
    "PnLStateXfaDiagnosticError",
    "build_dynamic_survivor_chronological_validation",
    "build_pnl_state_xfa_diagnostic",
    "build_xfa_decision_summary",
]
