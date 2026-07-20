"""Bounded Tier-G audit for the four immutable marginal Combine books.

This runner deliberately consumes the already reconstructed causal trajectory
ledgers.  It does not regenerate signals, mutate a book, access confirmation/Q4
data, start XFA, or write to the registry/mission database.  Stress results and
matched controls are robustness diagnostics; exact normal passes plus MLL and
consistency compliance remain the hard economic gate.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import policy_from_mapping
from hydra.account_policy.causal_active_pool_replay import (
    run_causal_shared_account_episode,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_marginal_combine_books as books
from hydra.production import autonomous_tier_g_controls as controls
from hydra.production.autonomous_exact_replay import _account_config
from hydra.production.fast_pass_runtime_helpers import _summarize_sprint_episodes


SCHEMA = "hydra_marginal_book_tier_g_batch_v1"
TARGET_POLICY_IDS = (
    "autonomous_marginal_book_b09b8e7b30f90b34737eb724",
    "autonomous_marginal_book_2f3752128ff0fd44a71b2327",
    "autonomous_marginal_book_bc8188389b6938ed6a9dd36f",
    "autonomous_marginal_book_74271a65d77ce0c7fe144170",
)
HORIZONS = (5, 10, 20)
SCENARIOS = ("NORMAL", "STRESSED_1_5X")
BLOCKS = ("B1", "B2", "B3", "B4")
RANDOM_CONTROL_COUNT = 5
MAX_DAY_TRADE_SHARE = 0.50
MAX_SLEEVE_SHARE = 0.65
MAX_BLOCK_PASS_SHARE = 0.75

BASE = Path(
    "reports/economic_evolution/"
    "autonomous_economic_discovery_director_0035_revision_02/branch_results"
)
PASS_BANK = BASE / "post_source_exhaustion/post_composite/combine_pass_observed_bank.json"
CANDIDATE_BANK = BASE / "post_source_exhaustion/post_composite/combine_candidate_bank.json"
SEMANTIC_BOOKS = BASE / (
    "post_source_exhaustion/post_composite/"
    "marginal_books_semantic_reconciliation_composite.json"
)
INITIAL_EXACT = BASE / "epoch_0002_exact_0029_account_race.json"
CONTINUATIONS = tuple(
    BASE / f"post_source_exhaustion/exact_0029_offset_{offset:04d}.json"
    for offset in (32, 64, 96, 128, 160)
)
DEFAULT_OUTPUT = Path("reports/economic_evolution/marginal_book_tier_g_batch_v1")


class MarginalBookTierGError(RuntimeError):
    """The bounded audit cannot preserve its immutable contract."""


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _verify_hashed(value: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    row = dict(value)
    claimed = str(row.pop("result_hash", ""))
    if not claimed or stable_hash(row) != claimed:
        raise MarginalBookTierGError(f"{label} result hash drift")
    return dict(value)


def _load_sources(root: Path) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    candidate = _load(root / CANDIDATE_BANK)["candidate_bank"]
    candidate = books._verify_candidate_bank(candidate)
    initial = _load(root / INITIAL_EXACT)
    continuations = [
        _load(root / path)["continuation_result"] for path in CONTINUATIONS
    ]
    _composite, exact_results = books._verified_exact_results(initial, continuations)
    tier_q_rows = tuple(
        sorted(
            (
                dict(row)
                for row in candidate["candidates"]
                if row.get("tier_q_contract_cleared") is True
            ),
            key=lambda row: str(row["candidate_id"]),
        )
    )
    context = books._prepare_replay_context(
        root,
        tier_q_rows,
        exact_results,
        fast_pass_manifest_path=books.DEFAULT_FAST_PASS_MANIFEST,
        rule_snapshot_path=books.DEFAULT_RULE_SNAPSHOT,
    )
    semantic = _load(root / SEMANTIC_BOOKS)["semantic_marginal_book_composite"]
    semantic = _verify_hashed(semantic, label="semantic marginal-book composite")
    pass_bank = _load(root / PASS_BANK)["combine_pass_observed_bank"]
    pass_bank = _verify_hashed(pass_bank, label="observed-pass bank")
    return context, semantic, pass_bank


def _unavailable(context: Any, members: Sequence[str]) -> set[int]:
    unavailable: set[int] = set()
    calendar = set(context.calendar)
    for member in members:
        component = context.components[member]
        unavailable.update(calendar.difference(component.eligible_session_days))
        unavailable.update(component.censored_session_days)
    return unavailable


def _episode_grid(
    context: Any,
    members: Sequence[str],
    policy: Any,
    *,
    trajectory_override: Mapping[str, Mapping[str, Sequence[Any]]] | None = None,
) -> tuple[dict[str, dict[int, list[tuple[Any, str]]]], dict[str, Any]]:
    account_label = next(iter({context.components[row].account_label for row in members}))
    config = _account_config(context.rules[account_label])
    unavailable = _unavailable(context, members)
    index = {day: offset for offset, day in enumerate(context.calendar)}
    episodes: dict[str, dict[int, list[tuple[Any, str]]]] = {
        scenario: {horizon: [] for horizon in HORIZONS}
        for scenario in SCENARIOS
    }
    censored: dict[str, dict[int, int]] = {
        scenario: {horizon: 0 for horizon in HORIZONS}
        for scenario in SCENARIOS
    }
    for scenario in SCENARIOS:
        trajectories = {
            member: (
                trajectory_override[scenario][member]
                if trajectory_override is not None
                else (
                    context.components[member].normal_trajectories
                    if scenario == "NORMAL"
                    else context.components[member].stressed_trajectories
                )
            )
            for member in members
        }
        for horizon in HORIZONS:
            for start_day, block in context.starts[horizon]:
                offset = index[start_day]
                window = context.calendar[offset : offset + horizon]
                if len(window) != horizon or any(day in unavailable for day in window):
                    censored[scenario][horizon] += 1
                    continue
                episode = run_causal_shared_account_episode(
                    trajectories,
                    context.calendar,
                    policy=policy,
                    start_day=int(start_day),
                    maximum_duration_days=int(horizon),
                    config=config,
                )
                episodes[scenario][horizon].append((episode, str(block)))
    return episodes, {"censored": censored, "config": config}


def _summary_grid(
    context: Any,
    episodes: Mapping[str, Mapping[int, Sequence[tuple[Any, str]]]],
    censored: Mapping[str, Mapping[int, int]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for scenario in SCENARIOS:
        result[scenario] = {}
        for horizon in HORIZONS:
            values = list(episodes[scenario][horizon])
            overall = _summarize_sprint_episodes(
                values,
                requested_start_count=len(context.starts[horizon]),
                data_censored_count=int(censored[scenario][horizon]),
            )
            by_block = {}
            for block in BLOCKS:
                selected = [row for row in values if row[1] == block]
                requested = sum(
                    observed == block for _day, observed in context.starts[horizon]
                )
                by_block[block] = _summarize_sprint_episodes(
                    selected,
                    requested_start_count=requested,
                    data_censored_count=max(0, requested - len(selected)),
                )
            result[scenario][str(horizon)] = {
                "overall": overall,
                "by_block": by_block,
            }
    return result


def _identity_matches(source: Mapping[str, Any], summary: Mapping[str, Any]) -> bool:
    for scenario in SCENARIOS:
        for horizon in HORIZONS:
            observed = dict(source["summaries"][scenario][str(horizon)])
            replayed = dict(summary[scenario][str(horizon)]["overall"])
            if stable_hash(observed) != stable_hash(replayed):
                return False
    return True


def _profit_share(values: Sequence[float]) -> float:
    positive = [max(float(value), 0.0) for value in values]
    total = sum(positive)
    return max(positive, default=0.0) / total if total > 0.0 else 1.0


def _concentration(
    context: Any,
    members: Sequence[str],
    episodes: Mapping[str, Mapping[int, Sequence[tuple[Any, str]]]],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for scenario in SCENARIOS:
        # Frozen 5-day starts are non-overlapping within each block and are the
        # least duplicated exact account ledger available for concentration.
        selected = list(episodes[scenario][5])
        days: dict[int, float] = {}
        trades: list[float] = []
        sleeves: dict[str, float] = defaultdict(float)
        blocks: dict[str, float] = defaultdict(float)
        accepted_ids: set[str] = set()
        source = {
            trajectory.event.event_id: trajectory
            for member in members
            for trajectory in (
                context.components[member].normal_trajectories
                if scenario == "NORMAL"
                else context.components[member].stressed_trajectories
            )
        }
        pass_by_block: dict[str, int] = defaultdict(int)
        for episode, block in selected:
            blocks[block] += float(episode.net_pnl)
            pass_by_block[block] += int(episode.passed)
            for day in episode.daily_path:
                session_day = int(day["session_day"])
                if session_day in days:
                    raise MarginalBookTierGError("non-overlapping 5d ledger reused a day")
                days[session_day] = float(day["day_pnl"])
                for component_id, value in dict(day["component_attribution"]).items():
                    sleeves[str(component_id)] += float(value)
            for decision in episode.risk_allocation_path:
                if int(decision.get("quantity", 0)) <= 0:
                    continue
                event_id = str(decision["event_id"])
                if event_id in accepted_ids:
                    raise MarginalBookTierGError("non-overlapping ledger duplicated a trade")
                accepted_ids.add(event_id)
                trajectory = source[event_id]
                ratio = int(decision["quantity"]) / max(int(trajectory.event.quantity), 1)
                trades.append(float(trajectory.event.net_pnl) * ratio)
        total_pass = sum(pass_by_block.values())
        block_pass_share = (
            max(pass_by_block.values(), default=0) / total_pass if total_pass else 1.0
        )
        result = {
            "denominator": "UNIQUE_NONOVERLAPPING_5D_ACCOUNT_EPISODES",
            "episode_count": len(selected),
            "unique_day_count": len(days),
            "unique_accepted_trade_count": len(trades),
            "maximum_single_day_positive_profit_share": _profit_share(list(days.values())),
            "maximum_single_trade_positive_profit_share": _profit_share(trades),
            "maximum_single_sleeve_positive_profit_share": _profit_share(list(sleeves.values())),
            "maximum_block_pass_share": float(block_pass_share),
            "pass_count_by_block": dict(sorted(pass_by_block.items())),
            "net_by_block_usd": dict(sorted(blocks.items())),
            "net_by_sleeve_usd": dict(sorted(sleeves.items())),
            "accepted_event_inventory_hash": stable_hash(sorted(accepted_ids)),
            "daily_pnl_hash": stable_hash(dict(sorted(days.items()))),
            # Retained in this bounded four-book report so pairwise behaviour is
            # measured from actual account decisions rather than source labels.
            "accepted_event_ids": sorted(accepted_ids),
            "daily_pnl_by_day": {
                str(day): float(value) for day, value in sorted(days.items())
            },
        }
        result["cleared"] = bool(
            result["maximum_single_day_positive_profit_share"] <= MAX_DAY_TRADE_SHARE
            and result["maximum_single_trade_positive_profit_share"] <= MAX_DAY_TRADE_SHARE
            and result["maximum_single_sleeve_positive_profit_share"] <= MAX_SLEEVE_SHARE
            and result["maximum_block_pass_share"] <= MAX_BLOCK_PASS_SHARE
        )
        output[scenario] = result
    return output


def _control_summary(
    summary: Mapping[str, Any], *, role: str, policy_id: str
) -> dict[str, Any]:
    return {
        "control_role": role,
        "policy_id": policy_id,
        "metrics": summary,
        "result_hash": stable_hash(summary),
    }


def _equal_risk_policy_for_context(
    context: Any, source_policy: Any, members: Sequence[str]
) -> Any:
    """Equalise median full-event declared risk without changing trade ledgers."""

    charges = dict(source_policy.nominal_risk_charge_per_mini)
    median_mini = {
        member: float(
            statistics.median(
                trajectory.event.mini_equivalent
                for trajectory in context.components[member].normal_trajectories
            )
        )
        for member in members
    }
    event_risk = {
        member: float(charges[member]) * max(median_mini[member], 1e-12)
        for member in members
    }
    equal_event_risk = statistics.fmean(event_risk.values())
    return replace(
        source_policy,
        policy_id=f"{source_policy.policy_id}:EQUAL_MEDIAN_EVENT_RISK_CONTROL",
        nominal_risk_charge_per_mini=tuple(
            (member, float(equal_event_risk / max(median_mini[member], 1e-12)))
            for member in members
        ),
    )


def _random_trajectory_overrides(
    context: Any,
    members: Sequence[str],
    *,
    control_index: int,
) -> dict[str, dict[str, Sequence[Any]]]:
    output: dict[str, dict[str, Sequence[Any]]] = {scenario: {} for scenario in SCENARIOS}
    for scenario in SCENARIOS:
        for member in members:
            source = (
                context.components[member].normal_trajectories
                if scenario == "NORMAL"
                else context.components[member].stressed_trajectories
            )
            fingerprint = stable_hash(
                {"member": member, "control_index": control_index, "scenario": scenario}
            )
            offsets = controls._control_offsets(len(source), fingerprint)
            offset = offsets[(control_index - 1) % len(offsets)]
            shifted = controls._circular_shift_trajectories(
                source,
                offset=offset,
                control_id=f"EXPOSURE_RANDOM_{control_index:02d}_{member}",
                scenario=scenario,
            )
            if not controls._exposure_count_match(source, shifted):
                raise MarginalBookTierGError("random control exposure inventory drift")
            output[scenario][member] = shifted
    return output


def _exposure_signature(summary: Mapping[str, Any]) -> tuple[float, float, int]:
    normal = dict(summary["NORMAL"]["5"]["overall"])
    return (
        float(normal.get("maximum_mini_equivalent_mean", 0.0)),
        float(normal.get("mean_daily_maximum_mini_equivalent", 0.0)),
        int(normal.get("accepted_event_count", 0)),
    )


def _exposure_distance(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(abs(float(a) - float(b)) / max(abs(float(a)), 1.0) for a, b in zip(left, right))


def _graduation_horizon(summary: Mapping[str, Any]) -> int | None:
    for horizon in HORIZONS:
        normal = dict(summary["NORMAL"][str(horizon)]["overall"])
        blocks = dict(normal.get("block_pass_counts") or {})
        if int(normal.get("pass_count", 0)) >= 2 and len([v for v in blocks.values() if int(v) > 0]) >= 2:
            return horizon
    return None


def _behavior_diversity(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    hashes = [str(row["behavioral_fingerprint"]) for row in results]
    pairs = []
    for index, left in enumerate(results):
        left_ids = set(left["concentration"]["NORMAL"]["accepted_event_ids"])
        left_daily = {
            int(day): float(value)
            for day, value in left["concentration"]["NORMAL"]["daily_pnl_by_day"].items()
        }
        for right in results[index + 1 :]:
            right_ids = set(right["concentration"]["NORMAL"]["accepted_event_ids"])
            right_daily = {
                int(day): float(value)
                for day, value in right["concentration"]["NORMAL"]["daily_pnl_by_day"].items()
            }
            union = left_ids | right_ids
            common_days = sorted(set(left_daily) & set(right_daily))
            pairs.append(
                {
                    "left": left["policy_id"],
                    "right": right["policy_id"],
                    "behavioral_fingerprint_equal": left["behavioral_fingerprint"] == right["behavioral_fingerprint"],
                    "component_jaccard": len(set(left["component_ids"]) & set(right["component_ids"])) / max(len(set(left["component_ids"]) | set(right["component_ids"])), 1),
                    "accepted_trade_jaccard": len(left_ids & right_ids) / max(len(union), 1),
                    "common_daily_observation_count": len(common_days),
                    "daily_pnl_correlation": _pearson(
                        [left_daily[day] for day in common_days],
                        [right_daily[day] for day in common_days],
                    ),
                }
            )
    return {
        "unique_behavioral_fingerprint_count": len(set(hashes)),
        "all_four_behaviorally_distinct": len(set(hashes)) == len(results),
        "pairwise": pairs,
    }


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0.0 or right_var <= 0.0:
        return None
    covariance = sum(
        (a - left_mean) * (b - right_mean) for a, b in zip(left, right)
    )
    return float(covariance / math.sqrt(left_var * right_var))


def _control_deltas(
    candidate: Mapping[str, Any], controls_by_role: Mapping[str, Any]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for role, control in controls_by_role.items():
        metrics = (
            control["selected"]["metrics"]
            if role == "EXPOSURE_MATCHED_RANDOM"
            else control["metrics"]
        )
        output[role] = {}
        for scenario in SCENARIOS:
            output[role][scenario] = {}
            for horizon in HORIZONS:
                observed = candidate[scenario][str(horizon)]["overall"]
                baseline = metrics[scenario][str(horizon)]["overall"]
                output[role][scenario][str(horizon)] = {
                    "pass_count_delta": int(observed["pass_count"])
                    - int(baseline["pass_count"]),
                    "pass_rate_delta": float(observed["pass_rate"])
                    - float(baseline["pass_rate"]),
                    "net_total_delta_usd": float(observed["net_total"])
                    - float(baseline["net_total"]),
                    "target_progress_p25_delta": float(observed["target_progress_p25"])
                    - float(baseline["target_progress_p25"]),
                    "mll_breach_count_delta": int(observed["mll_breach_count"])
                    - int(baseline["mll_breach_count"]),
                }
    return output


def build_batch(root: Path) -> dict[str, Any]:
    context, semantic, pass_bank = _load_sources(root)
    source_by_id = {str(row["policy_id"]): dict(row) for row in semantic["book_results"]}
    supporting = {str(row["policy_id"]): dict(row) for row in semantic["supporting_policy_results"]}
    observed = {str(row["policy_id"]): dict(row) for row in pass_bank["policies"]}
    if set(TARGET_POLICY_IDS) - set(source_by_id) or set(TARGET_POLICY_IDS) - set(observed):
        raise MarginalBookTierGError("frozen four-book inventory is incomplete")

    results: list[dict[str, Any]] = []
    for policy_id in TARGET_POLICY_IDS:
        source = source_by_id[policy_id]
        bank_row = observed[policy_id]
        members = tuple(str(row) for row in source["component_ids"])
        policy = policy_from_mapping(source["governor_policy"])
        episodes, metadata = _episode_grid(context, members, policy)
        summary = _summary_grid(context, episodes, metadata["censored"])
        identity = _identity_matches(source, summary)
        if not identity:
            raise MarginalBookTierGError(f"semantic identity replay drift: {policy_id}")
        concentration = _concentration(context, members, episodes)

        controls_by_role: dict[str, Any] = {}
        for role, control_policy_id in (
            ("BEST_COMPONENT", str(source["best_component_policy_id"])),
            ("PRECEDING_SMALLER_BOOK", str(source["predecessor_policy_id"])),
        ):
            control_source = supporting[control_policy_id]
            control_members = tuple(str(row) for row in control_source["component_ids"])
            control_policy = policy_from_mapping(control_source["governor_policy"])
            grid, meta = _episode_grid(context, control_members, control_policy)
            control_summary = _summary_grid(context, grid, meta["censored"])
            if not _identity_matches(control_source, control_summary):
                raise MarginalBookTierGError(f"{role} identity replay drift: {policy_id}")
            controls_by_role[role] = _control_summary(
                control_summary, role=role, policy_id=control_policy_id
            )

        equal_policy = _equal_risk_policy_for_context(context, policy, members)
        equal_grid, equal_meta = _episode_grid(context, members, equal_policy)
        equal_summary = _summary_grid(context, equal_grid, equal_meta["censored"])
        controls_by_role["EQUAL_RISK_ACTIVE_POOL"] = _control_summary(
            equal_summary,
            role="EQUAL_RISK_ACTIVE_POOL",
            policy_id=equal_policy.policy_id,
        )

        candidate_exposure = _exposure_signature(summary)
        random_rows = []
        for control_index in range(1, RANDOM_CONTROL_COUNT + 1):
            override = _random_trajectory_overrides(
                context, members, control_index=control_index
            )
            grid, meta = _episode_grid(
                context, members, policy, trajectory_override=override
            )
            control_summary = _summary_grid(context, grid, meta["censored"])
            exposure = _exposure_signature(control_summary)
            random_rows.append(
                {
                    "control_index": control_index,
                    "input_opportunity_exposure_exactly_matched": True,
                    "realized_exposure_signature": list(exposure),
                    "realized_exposure_distance": _exposure_distance(candidate_exposure, exposure),
                    "metrics": control_summary,
                    "result_hash": stable_hash(control_summary),
                }
            )
        selected_random = min(
            random_rows,
            key=lambda row: (float(row["realized_exposure_distance"]), int(row["control_index"])),
        )
        controls_by_role["EXPOSURE_MATCHED_RANDOM"] = {
            "control_role": "EXPOSURE_MATCHED_RANDOM_OUTCOME_PATH",
            "selection_uses_outcomes": False,
            "selection_rule": "MINIMUM_REALIZED_EXPOSURE_DISTANCE_THEN_CONTROL_INDEX",
            "candidate_exposure_signature": list(candidate_exposure),
            "selected": selected_random,
            "all_controls": random_rows,
        }
        control_deltas = _control_deltas(summary, controls_by_role)

        horizon = _graduation_horizon(summary)
        observed_normal_passes = [
            dict(summary["NORMAL"][str(value)]["overall"])
            for value in HORIZONS
        ]
        hard = {
            "semantic_identity_reconciled": identity,
            "exact_normal_multiple_passes": horizon is not None,
            "exact_normal_passes_in_multiple_blocks": horizon is not None,
            "normal_mll_zero_all_horizons": all(
                int(row.get("mll_breach_count", 1)) == 0
                for row in observed_normal_passes
            ),
            "all_observed_normal_passing_paths_consistency_compliant": all(
                int(row.get("pass_count", 0)) == 0
                or bool(row.get("all_passing_paths_consistency_compliant", False))
                for row in observed_normal_passes
            ),
            "normal_unique_account_concentration_cleared": bool(
                concentration["NORMAL"]["cleared"]
                or (
                    horizon is None
                    and concentration["NORMAL"][
                        "maximum_single_day_positive_profit_share"
                    ]
                    <= MAX_DAY_TRADE_SHARE
                    and concentration["NORMAL"][
                        "maximum_single_trade_positive_profit_share"
                    ]
                    <= MAX_DAY_TRADE_SHARE
                    and concentration["NORMAL"][
                        "maximum_single_sleeve_positive_profit_share"
                    ]
                    <= MAX_SLEEVE_SHARE
                )
            ),
            "marginal_contribution_preaccepted_on_design_blocks": bool(
                source.get("marginally_accepted") is True
            ),
            "all_required_controls_complete": len(controls_by_role) == 4,
        }
        status = "G" if all(hard.values()) else "Q_RETAINED"
        robustness = {
            "stress_is_advisory_not_a_hard_promotion_gate": True,
            "positive_stressed_net_all_horizons": all(
                float(summary["STRESSED_1_5X"][str(h)]["overall"].get("net_total", 0.0)) > 0.0
                for h in HORIZONS
            ),
            "stressed_mll_zero_all_horizons": all(
                int(summary["STRESSED_1_5X"][str(h)]["overall"].get("mll_breach_count", 1)) == 0
                for h in HORIZONS
            ),
            "stressed_concentration_cleared": bool(
                concentration["STRESSED_1_5X"]["cleared"]
            ),
        }
        row_core = {
            "policy_id": policy_id,
            "policy_spec_hash": str(source["policy_spec_hash"]),
            "behavioral_fingerprint": str(bank_row["fingerprints"]["episode_behavior_hash"]),
            "account_label": str(source["account_label"]),
            "component_ids": list(members),
            "governor_profile_id": str(source["governor_profile_id"]),
            "graduation_horizon_trading_days": horizon,
            "exact_metrics": summary,
            "concentration": concentration,
            "controls": controls_by_role,
            "control_deltas": control_deltas,
            "hard_gate_results": hard,
            "stress_robustness": robustness,
            "decision": status,
            "failure_reasons": [key for key, value in hard.items() if not value],
            "evidence_role": "VIEWED_FINAL_DEVELOPMENT_ONLY",
            "independent_confirmation_claimed": False,
            "xfa_paths_started": 0,
        }
        results.append({**row_core, "result_hash": stable_hash(row_core)})

    diversity = _behavior_diversity(results)
    core = {
        "schema": SCHEMA,
        "status": "COMPLETE_BOUNDED_MARGINAL_BOOK_TIER_G_AUDIT",
        "contract": {
            "target_policy_ids": list(TARGET_POLICY_IDS),
            "book_membership_sizing_governor_and_data_roles_mutated": False,
            "market_signal_replay_performed": False,
            "causal_trajectory_ledgers_reused": True,
            "stress_role": "ADVISORY_ROBUSTNESS",
            "hard_gates": "EXACT_NORMAL_PASS_MLL_CONSISTENCY_AND_CONCENTRATION",
            "random_control_count_per_book": RANDOM_CONTROL_COUNT,
            "no_xfa": True,
            "no_q4": True,
            "no_data_purchase": True,
        },
        "source_hashes": {
            "semantic_marginal_book_composite": str(semantic["result_hash"]),
            "combine_pass_observed_bank": str(pass_bank["result_hash"]),
        },
        "policy_results": results,
        "behavior_diversity": diversity,
        "counts": {
            "policy_count": len(results),
            "tier_g_count": sum(row["decision"] == "G" for row in results),
            "q_retained_count": sum(row["decision"] == "Q_RETAINED" for row in results),
            "exact_candidate_episode_count": sum(
                int(row["exact_metrics"][scenario][str(h)]["overall"]["episode_count"])
                for row in results for scenario in SCENARIOS for h in HORIZONS
            ),
            "control_policy_replay_count": len(results) * (3 + RANDOM_CONTROL_COUNT),
            "registry_writes": 0,
            "database_writes": 0,
            "data_purchase_count": 0,
            "q4_access_count_delta": 0,
            "xfa_paths_started": 0,
            "orders": 0,
            "broker_connections": 0,
        },
        "decision": (
            "PROMOTE_CLEARED_BOOKS_TO_TIER_G_DEVELOPMENT_ONLY"
            if any(row["decision"] == "G" for row in results)
            else "RETAIN_ALL_AS_TIER_Q_WITHOUT_REPLAYING_THIS_TERMINAL_EPOCH"
        ),
        "authoritative_state_modified": False,
        "promotion_status": None,
    }
    return {**core, "result_hash": stable_hash(core)}


def write_batch(root: Path, output_dir: Path) -> Path:
    result = build_batch(root)
    target = root / output_dir
    target.mkdir(parents=True, exist_ok=True)
    path = target / "economic_result.json"
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    output = write_batch(args.root.resolve(), args.output_dir)
    print(json.dumps({"output": str(output)}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
