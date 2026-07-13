from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import statistics
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    compile_account_policy,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.cross_session_account import (
    CROSS_SESSION_CLASS_ID,
    AccountPolicyPair,
    generate_cross_session_account_population,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _runtime_row,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


CROSS_SESSION_ENGINE_VERSION = "hydra_cross_session_account_campaign_v1"
_PAIR_RUNTIMES: Mapping[str, ExactSleeveRuntime] = {}
_PAIR_STARTS: tuple[int, ...] = ()
_PAIR_EPISODE_POLICY: EpisodeStartPolicy | None = None


class CrossSessionAccountCampaignError(RuntimeError):
    pass


def run_cross_session_account_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Run the frozen account-first 0009 development campaign."""

    started = time.perf_counter()
    phases: dict[str, float] = {}
    phase_started = started
    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_cross_session_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise CrossSessionAccountCampaignError("source seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise CrossSessionAccountCampaignError("source seed semantic hash drift")
    population = _population(seed, prereg)
    expected_manifest = str(
        prereg["structural_population"]["policy_manifest_hash"]
    )
    if population.manifest_hash != expected_manifest:
        raise CrossSessionAccountCampaignError(
            "frozen cross-session policy manifest drift"
        )
    writer.write_json(
        "account_population.json",
        {
            **population.summary(),
            "components": [row.to_dict() for row in population.components],
            "pairs": [row.to_dict() for row in population.pairs],
            "real_policies": [row.to_dict() for row in population.real_policies],
            "matched_control_policies": [
                row.to_dict() for row in population.matched_control_policies
            ],
        },
    )
    _stage(state_writer, prereg, "ACCOUNT_POPULATION_FROZEN")
    phases["preregistration_and_population"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=cache_root,
        contract_map_path=contract_map_path,
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    _verify_data_fingerprint(
        prereg,
        feature_build.source_fingerprint,
        contract_map_path,
        feature_build.market_paths,
    )
    _stage(state_writer, prereg, "FEATURE_STORE_VERIFIED")
    phases["feature_store_verification"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    sleeves = tuple(row.sleeve for row in population.components)
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    screen = run_ultra_cheap_screen(sleeves, matrices, policy=screen_policy)
    writer.write_json("cheap_screen_summary.json", screen.summary())
    writer.write_jsonl_batch("cheap_screen_results.jsonl", list(screen.rows))
    _stage(state_writer, prereg, "COMPONENT_SCREEN_COMPLETE")
    phases["component_screen"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    bound = _bind_selected(sleeves, matrices, policy=screen_policy)
    runtimes, exact_failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    writer.write_jsonl_batch(
        "exact_component_results.jsonl",
        [_runtime_row(row) for row in runtimes.values()],
    )
    writer.write_json("exact_component_failures.json", exact_failures)
    if len(runtimes) != len(sleeves) or exact_failures:
        raise CrossSessionAccountCampaignError(
            "account-first replay requires every frozen component runtime"
        )
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")
    phases["exact_component_replay"] = time.perf_counter() - phase_started

    common_days = _common_days(list(runtimes.values()))
    starts = select_episode_starts(
        common_days,
        policy=EpisodeStartPolicy(**prereg["rolling_episode_policy"]),
    )
    if len(starts) != int(prereg["rolling_episode_policy"]["maximum_starts"]):
        raise CrossSessionAccountCampaignError(
            "cross-session campaign has incomplete frozen episode starts"
        )

    phase_started = time.perf_counter()
    pair_rows = evaluate_account_policy_pairs(
        population.pairs,
        runtimes,
        starts=starts,
        episode_policy=EpisodeStartPolicy(**prereg["rolling_episode_policy"]),
        worker_count=int(prereg["compute"]["account_worker_count"]),
    )
    if len(pair_rows) != int(
        prereg["structural_population"]["policy_pair_count"]
    ):
        raise CrossSessionAccountCampaignError(
            "cross-session account evaluation is incomplete"
        )
    writer.write_jsonl_batch("account_pair_results.jsonl", pair_rows)
    _stage(state_writer, prereg, "PAIRED_ACCOUNT_REPLAY_COMPLETE")
    phases["paired_account_replay"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    tripwire = paired_account_tripwire(pair_rows, prereg["family_tripwire"])
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")
    phases["family_tripwire"] = time.perf_counter() - phase_started

    result = cross_session_final_result(
        prereg,
        population_summary=population.summary(),
        screen_summary=screen.summary(),
        exact_runtime_count=len(runtimes),
        exact_failure_count=len(exact_failures),
        pair_rows=pair_rows,
        starts=starts,
        tripwire=tripwire,
        elapsed_seconds=time.perf_counter() - started,
        phase_seconds=phases,
    )
    result["result_sha256"] = stable_hash(result)
    writer.write_json("cross_session_account_result.json", result)
    writer.write_text(
        "cross_session_account_report.md", cross_session_report(result, prereg)
    )
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_cross_session_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_json(Path(path).resolve())
    claimed = prereg.get("preregistration_hash")
    payload = dict(prereg)
    payload.pop("preregistration_hash", None)
    structural = prereg.get("structural_population") or {}
    statuses = prereg.get("statuses") or {}
    governance = prereg.get("governance") or {}
    if (
        prereg.get("schema") != "hydra_cross_session_account_preregistration_v1"
        or prereg.get("class_id") != CROSS_SESSION_CLASS_ID
        or stable_hash(payload) != claimed
        or int(structural.get("policy_pair_count", -1)) < 256
        or int(structural.get("component_count", -1)) < 12
        or structural.get("new_market_outcomes_seen_during_generation") is not False
        or structural.get("same_class_0008_rescue") is not False
        or statuses.get("validated_allowed") is not False
        or statuses.get("pre_holdout_ready_allowed") is not False
        or statuses.get("paper_shadow_ready_allowed") is not False
        or statuses.get("status_inheritance") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
    ):
        raise CrossSessionAccountCampaignError(
            "invalid cross-session preregistration"
        )
    return prereg


def load_and_verify_cross_session_result(
    path: str | Path, prereg: Mapping[str, Any]
) -> dict[str, Any]:
    result = _load_json(Path(path).resolve())
    claimed = result.get("result_sha256")
    payload = dict(result)
    payload.pop("result_sha256", None)
    population = result.get("population") or {}
    governance = result.get("governance") or {}
    economics = result.get("account_policy_economics") or {}
    expected_pairs = int(prereg["structural_population"]["policy_pair_count"])
    expected_episodes = expected_pairs * int(
        prereg["rolling_episode_policy"]["maximum_starts"]
    )
    if (
        result.get("campaign_id") != prereg.get("campaign_id")
        or result.get("class_id") != CROSS_SESSION_CLASS_ID
        or claimed != stable_hash(payload)
        or population.get("manifest_hash")
        != prereg["structural_population"]["policy_manifest_hash"]
        or int(result.get("policy_pair_evaluated_count", -1)) != expected_pairs
        or int(economics.get("primary_rolling_combine_episode_count", -1))
        != expected_episodes
        or int(result.get("pre_holdout_ready_count", -1)) != 0
        or int(result.get("paper_shadow_ready_count", -1)) != 0
        or int(governance.get("proof_windows_consumed", -1)) != 0
        or int(governance.get("new_data_purchase_count", -1)) != 0
        or int(governance.get("q4_access_delta", -1)) != 0
        or int(governance.get("broker_connections", -1)) != 0
        or int(governance.get("orders", -1)) != 0
    ):
        raise CrossSessionAccountCampaignError(
            "cross-session result integrity failure"
        )
    return result


def _population(seed: Mapping[str, Any], prereg: Mapping[str, Any]):
    structural = prereg["structural_population"]
    return generate_cross_session_account_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        policy_pair_count=int(structural["policy_pair_count"]),
        maximum_components=int(structural["maximum_components"]),
        minimum_component_events=int(structural["minimum_component_events"]),
        maximum_components_per_market=int(
            structural["maximum_components_per_market"]
        ),
        maximum_components_per_session=int(
            structural["maximum_components_per_session"]
        ),
        maximum_components_per_mechanism=int(
            structural["maximum_components_per_mechanism"]
        ),
    )


def evaluate_account_policy_pairs(
    pairs: Sequence[AccountPolicyPair],
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
    worker_count: int,
) -> list[dict[str, Any]]:
    """Evaluate immutable paired policies with shared read-only fork state."""

    if worker_count < 1:
        raise ValueError("worker count must be positive")
    ordered = sorted(pairs, key=lambda row: row.pair_id)
    if worker_count == 1:
        return [
            evaluate_account_policy_pair(
                row,
                runtimes,
                starts=starts,
                episode_policy=episode_policy,
            )
            for row in ordered
        ]
    global _PAIR_RUNTIMES, _PAIR_STARTS, _PAIR_EPISODE_POLICY
    _PAIR_RUNTIMES = runtimes
    _PAIR_STARTS = tuple(int(value) for value in starts)
    _PAIR_EPISODE_POLICY = episode_policy
    context = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=context,
    ) as pool:
        rows = list(pool.map(_evaluate_pair_from_fork_state, ordered, chunksize=4))
    _PAIR_RUNTIMES = {}
    _PAIR_STARTS = ()
    _PAIR_EPISODE_POLICY = None
    return sorted(rows, key=lambda row: str(row["pair_id"]))


def _evaluate_pair_from_fork_state(pair: AccountPolicyPair) -> dict[str, Any]:
    if not _PAIR_RUNTIMES or _PAIR_EPISODE_POLICY is None or not _PAIR_STARTS:
        raise RuntimeError("paired account worker has no frozen fork state")
    return evaluate_account_policy_pair(
        pair,
        _PAIR_RUNTIMES,
        starts=_PAIR_STARTS,
        episode_policy=_PAIR_EPISODE_POLICY,
    )


def evaluate_account_policy_pair(
    pair: AccountPolicyPair,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    starts: Sequence[int],
    episode_policy: EpisodeStartPolicy,
) -> dict[str, Any]:
    real = evaluate_compiled_account_policy(
        compile_account_policy(pair.real_policy, runtimes),
        episode_policy=episode_policy,
        explicit_start_days=starts,
        evaluate_xfa=False,
    )
    control = evaluate_compiled_account_policy(
        compile_account_policy(pair.matched_control_policy, runtimes),
        episode_policy=episode_policy,
        explicit_start_days=real.episode_start_days,
        evaluate_xfa=False,
    )
    if real.episode_start_days != control.episode_start_days:
        raise CrossSessionAccountCampaignError(
            "paired policies did not use identical episode starts"
        )
    real_normal = real.controlled_base
    real_stress = real.controlled_stress_1_5x
    control_normal = control.controlled_base
    control_stress = control.controlled_stress_1_5x
    deltas = {
        "normal_median_net_usd": (
            real_normal.median_episode_net_pnl
            - control_normal.median_episode_net_pnl
        ),
        "stressed_median_net_usd": (
            real_stress.median_episode_net_pnl
            - control_stress.median_episode_net_pnl
        ),
        "normal_target_progress": (
            real_normal.target_progress_median
            - control_normal.target_progress_median
        ),
        "stressed_target_progress": (
            real_stress.target_progress_median
            - control_stress.target_progress_median
        ),
        "normal_mll_breach_rate": (
            real_normal.mll_breach_rate - control_normal.mll_breach_rate
        ),
        "stressed_mll_breach_rate": (
            real_stress.mll_breach_rate - control_stress.mll_breach_rate
        ),
        "normal_consistency_pass_rate": (
            real_normal.consistency_pass_rate
            - control_normal.consistency_pass_rate
        ),
        "stressed_consistency_pass_rate": (
            real_stress.consistency_pass_rate
            - control_stress.consistency_pass_rate
        ),
    }
    blocks = _temporal_blocks(real_stress.episodes, count=4)
    contribution = {
        key: max(0.0, float(value))
        for key, value in real_stress.component_contribution.items()
    }
    positive_total = sum(contribution.values())
    maximum_share = (
        max(contribution.values(), default=0.0) / positive_total
        if positive_total > 0.0
        else 1.0
    )
    behavior = stable_hash(
        {
            "starts": list(real.episode_start_days),
            "paths": [
                {
                    "terminal": row.terminal.value,
                    "net": round(row.net_pnl, 8),
                    "progress": round(row.target_progress, 10),
                    "mll": row.mll_breached,
                    "consistency": row.consistency_ok,
                }
                for row in real_normal.episodes
            ],
        }
    )
    return {
        **pair.to_dict(),
        "real_policy": pair.real_policy.to_dict(),
        "matched_control_policy": pair.matched_control_policy.to_dict(),
        "identical_episode_starts": True,
        "episode_start_count": len(real.episode_start_days),
        "behavioral_fingerprint": behavior,
        "real_evaluation": real.to_dict(include_episodes=False),
        "matched_control_evaluation": control.to_dict(include_episodes=False),
        "real_stressed_temporal_blocks": blocks,
        "real_positive_temporal_block_count": sum(
            float(row["median_net_usd"]) > 0.0 for row in blocks
        ),
        "real_maximum_positive_component_share": maximum_share,
        "paired_delta": deltas,
        "development_only": True,
        "validated": False,
        "proof_window_consumed": False,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "orders": 0,
    }


def paired_account_tripwire(
    rows: Sequence[Mapping[str, Any]], gate: Mapping[str, Any]
) -> dict[str, Any]:
    minimum_net = float(gate["minimum_stressed_median_net_delta_usd"])
    minimum_progress = float(gate["minimum_stressed_target_progress_delta"])
    maximum_mll = float(gate["maximum_mll_breach_rate_deterioration"])
    real_wins = 0
    control_wins = 0
    ties = 0
    for row in rows:
        delta = row["paired_delta"]
        real = bool(
            float(delta["stressed_median_net_usd"]) >= minimum_net
            and float(delta["stressed_target_progress"]) >= minimum_progress
            and float(delta["stressed_mll_breach_rate"]) <= maximum_mll
        )
        control = bool(
            float(delta["stressed_median_net_usd"]) <= -minimum_net
            and float(delta["stressed_target_progress"]) <= -minimum_progress
            and float(delta["stressed_mll_breach_rate"]) >= -maximum_mll
        )
        if real and not control:
            real_wins += 1
        elif control and not real:
            control_wins += 1
        else:
            ties += 1
    informative = real_wins + control_wins
    real_rate = real_wins / max(len(rows), 1)
    control_rate = control_wins / max(len(rows), 1)
    ratio = None if real_wins == 0 else control_wins / real_wins
    p_value = _binomial_tail(real_wins, informative, 0.5)
    green = bool(
        len(rows) >= int(gate["minimum_policy_pairs"])
        and informative >= int(gate["minimum_informative_pairs"])
        and real_wins > 0
        and ratio is not None
        and ratio < float(gate["maximum_NULL_RATIO"])
        and p_value <= float(gate["net_evidence_p_value"])
    )
    if green:
        verdict, strength = "GREEN_NULL_ADJUSTED_BASELINE", "VERT_NET"
    elif informative < int(gate["minimum_informative_pairs"]):
        verdict, strength = "INSUFFICIENT_ACCOUNT_PAIR_INFORMATION", "NULL"
    elif real_wins == 0:
        verdict, strength = "FORMULATION_FALSIFIED_NO_REAL_PAIR_WIN", "NULL"
    else:
        verdict, strength = "ARTEFACT_GEOMETRY_ONLY", "ARTEFACT"
    return {
        "class_id": CROSS_SESSION_CLASS_ID,
        "real_win_count": real_wins,
        "real_policy_pair_count": len(rows),
        "real_win_rate": real_rate,
        "matched_control_win_count": control_wins,
        "matched_control_win_rate": control_rate,
        "tie_count": ties,
        "informative_pair_count": informative,
        "NULL_RATIO": ratio,
        "maximum_NULL_RATIO": float(gate["maximum_NULL_RATIO"]),
        "exact_one_sided_binomial_p_value": p_value,
        "family_green": green,
        "evidence_strength": strength,
        "verdict": verdict,
        "thresholds_changed_after_outcome": False,
    }


def cross_session_final_result(
    prereg: Mapping[str, Any],
    *,
    population_summary: Mapping[str, Any],
    screen_summary: Mapping[str, Any],
    exact_runtime_count: int,
    exact_failure_count: int,
    pair_rows: Sequence[Mapping[str, Any]],
    starts: Sequence[int],
    tripwire: Mapping[str, Any],
    elapsed_seconds: float,
    phase_seconds: Mapping[str, float],
) -> dict[str, Any]:
    gate = prereg["account_gate"]
    annotated = [
        _account_decision(row, gate=gate, family_green=bool(tripwire["family_green"]))
        for row in pair_rows
    ]
    diagnostic = sum(row["diagnostic_account_gate_passed"] for row in annotated)
    research = sum(row["account_research_gate_passed"] for row in annotated)
    combine = sum(row["combine_path_diagnostic"] for row in annotated)
    if not tripwire["family_green"]:
        status = str(tripwire["verdict"])
        verdict = "ARTEFACT" if tripwire["evidence_strength"] == "ARTEFACT" else "NULL"
    elif combine:
        status, verdict = "DEVELOPMENT_COMBINE_PATH_CANDIDATES_FOUND", "GREEN"
    elif research:
        status, verdict = "DEVELOPMENT_ACCOUNT_RESEARCH_CANDIDATES_FOUND", "GREEN"
    else:
        status, verdict = "GREEN_TRIPWIRE_NO_ACCOUNT_SURVIVOR", "NULL"
    account_metrics = _policy_metrics(annotated, real=True)
    control_metrics = _policy_metrics(annotated, real=False)
    paired_metrics = _paired_metrics(annotated)
    phases = {str(key): float(value) for key, value in phase_seconds.items()}
    research_phase_names = {
        "feature_store_verification",
        "component_screen",
        "exact_component_replay",
        "paired_account_replay",
        "family_tripwire",
    }
    research_seconds = sum(
        value for key, value in phases.items() if key in research_phase_names
    )
    administrative = max(0.0, elapsed_seconds - research_seconds)
    best = sorted(
        annotated,
        key=lambda row: (
            -float(
                row["real_evaluation"]["controlled_stress_1_5x"]
                ["target_progress_median"]
            ),
            -float(
                row["real_evaluation"]["controlled_stress_1_5x"]
                ["median_episode_net_pnl"]
            ),
            str(row["real_policy"]["policy_id"]),
        ),
    )[:12]
    return {
        "schema": "hydra_cross_session_account_result_v1",
        "engine_version": CROSS_SESSION_ENGINE_VERSION,
        "campaign_id": str(prereg["campaign_id"]),
        "class_id": CROSS_SESSION_CLASS_ID,
        "completed_at_utc": utc_now_iso(),
        "scientific_status": status,
        "report_verdict": verdict,
        "validated": False,
        "development_only": True,
        "population": dict(population_summary),
        "cheap_screen": dict(screen_summary),
        "exact_component_runtime_count": exact_runtime_count,
        "exact_component_failure_count": exact_failure_count,
        "policy_pair_evaluated_count": len(annotated),
        "global_episode_start_count": len(starts),
        "global_episode_starts": list(starts),
        "family_tripwire": dict(tripwire),
        "diagnostic_account_gate_pass_count": diagnostic,
        "account_research_candidate_count": research,
        "combine_path_diagnostic_count": combine,
        "account_policy_economics": account_metrics,
        "matched_control_economics": control_metrics,
        "paired_account_economics": paired_metrics,
        "best_development_policies": [
            {
                "policy_id": row["real_policy"]["policy_id"],
                "status": row["status"],
                "stress_median_net_usd": row["real_evaluation"]
                ["controlled_stress_1_5x"]["median_episode_net_pnl"],
                "stress_target_progress": row["real_evaluation"]
                ["controlled_stress_1_5x"]["target_progress_median"],
                "stress_mll_breach_rate": row["real_evaluation"]
                ["controlled_stress_1_5x"]["mll_breach_rate"],
                "paired_stress_net_delta_usd": row["paired_delta"]
                ["stressed_median_net_usd"],
            }
            for row in best
        ],
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "multiplicity": {
            "reserved_delta": int(prereg["multiplicity"]["reserved_delta_trials"]),
            "expensive_DSR_BH_executed": False,
        },
        "governance": {
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "outbound_order_capability": False,
        },
        "wall_clock_accounting": {
            "total_seconds_to_result_assembly": elapsed_seconds,
            "research_seconds": research_seconds,
            "tests_and_reporting_seconds_inside_campaign": administrative,
            "research_percent": (
                100.0 * research_seconds / elapsed_seconds
                if elapsed_seconds > 0.0
                else 0.0
            ),
            "tests_and_reporting_percent": (
                100.0 * administrative / elapsed_seconds
                if elapsed_seconds > 0.0
                else 0.0
            ),
            "phase_seconds": phases,
            "full_repository_regression_is_outside_campaign_hot_loop": True,
        },
        "next_action": (
            "SCALE_AND_FAILURE_DIRECT_MUTATE_ACCOUNT_SURVIVORS"
            if combine or research
            else "TOMBSTONE_EXACT_0009_AND_PREREGISTER_DISTINCT_0010"
        ),
        "CONTRE": (
            "The 512 pairs share a selected development component bank and are "
            "correlated. Their paired tripwire measures development enrichment, "
            "not independent confirmation or shadow eligibility."
        ),
    }


def _account_decision(
    row: Mapping[str, Any],
    *,
    gate: Mapping[str, Any],
    family_green: bool,
) -> dict[str, Any]:
    value = dict(row)
    normal = row["real_evaluation"]["controlled_base"]
    stress = row["real_evaluation"]["controlled_stress_1_5x"]
    hard_ok = int(normal["compliance_failure_count"]) == 0 and int(
        stress["compliance_failure_count"]
    ) == 0
    diagnostic = bool(
        hard_ok
        and float(normal["median_episode_net_pnl"])
        > float(gate["minimum_normal_median_net_usd"])
        and float(stress["median_episode_net_pnl"])
        > float(gate["minimum_stressed_median_net_usd"])
        and float(stress["target_progress_median"])
        >= float(gate["minimum_median_target_progress"])
        and float(stress["mll_breach_rate"])
        <= float(gate["maximum_mll_breach_rate"])
        and float(stress["consistency_pass_rate"])
        >= float(gate["minimum_consistency_pass_rate"])
        and int(row["real_positive_temporal_block_count"])
        >= int(gate["minimum_positive_temporal_blocks"])
        and float(row["real_maximum_positive_component_share"])
        <= float(gate["maximum_positive_component_share"])
        and float(row["paired_delta"]["stressed_median_net_usd"])
        >= float(gate["minimum_matched_control_net_delta_usd"])
    )
    research = bool(family_green and diagnostic)
    combine = bool(
        research
        and int(stress["pass_count"])
        >= int(gate["minimum_combine_path_pass_count"])
    )
    failures = _failure_vectors(
        row,
        gate=gate,
        hard_ok=hard_ok,
        family_green=family_green,
    )
    value.update(
        {
            "status": (
                "COMBINE_PATH_CANDIDATE_DEVELOPMENT_ONLY"
                if combine
                else "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
                if research
                else "CROSS_SESSION_ACCOUNT_DIAGNOSTIC_ONLY"
                if diagnostic
                else "CROSS_SESSION_ACCOUNT_RESEARCH_FAILED"
            ),
            "diagnostic_account_gate_passed": diagnostic,
            "account_research_gate_passed": research,
            "combine_path_diagnostic": combine,
            "failure_vectors": failures,
        }
    )
    return value


def _failure_vectors(
    row: Mapping[str, Any],
    *,
    gate: Mapping[str, Any],
    hard_ok: bool,
    family_green: bool,
) -> list[str]:
    normal = row["real_evaluation"]["controlled_base"]
    stress = row["real_evaluation"]["controlled_stress_1_5x"]
    failures: list[str] = []
    if not family_green:
        failures.append("FAMILY_TRIPWIRE_FAILED")
    if not hard_ok:
        failures.append("HARD_COMPLIANCE_FAILURE")
    if float(normal["median_episode_net_pnl"]) <= float(
        gate["minimum_normal_median_net_usd"]
    ):
        failures.append("NORMAL_ECONOMICS_NONPOSITIVE")
    if float(stress["median_episode_net_pnl"]) <= float(
        gate["minimum_stressed_median_net_usd"]
    ):
        failures.append("STRESSED_ECONOMICS_NONPOSITIVE")
    if float(stress["target_progress_median"]) < float(
        gate["minimum_median_target_progress"]
    ):
        failures.append("TARGET_VELOCITY_LOW")
    if float(stress["mll_breach_rate"]) > float(
        gate["maximum_mll_breach_rate"]
    ):
        failures.append("MLL_BREACH_EXCESS")
    if float(stress["consistency_pass_rate"]) < float(
        gate["minimum_consistency_pass_rate"]
    ):
        failures.append("CONSISTENCY_FAILURE")
    if int(row["real_positive_temporal_block_count"]) < int(
        gate["minimum_positive_temporal_blocks"]
    ):
        failures.append("TEMPORAL_BLOCK_INSTABILITY")
    if float(row["real_maximum_positive_component_share"]) > float(
        gate["maximum_positive_component_share"]
    ):
        failures.append("COMPONENT_CONCENTRATION")
    if float(row["paired_delta"]["stressed_median_net_usd"]) < float(
        gate["minimum_matched_control_net_delta_usd"]
    ):
        failures.append("MATCHED_CONTROL_NOT_BEATEN")
    if int(stress["pass_count"]) < int(gate["minimum_combine_path_pass_count"]):
        failures.append("NO_COMBINE_PASS")
    return failures or ["NO_FAILURE_VECTOR"]


def _policy_metrics(
    rows: Sequence[Mapping[str, Any]], *, real: bool
) -> dict[str, Any]:
    prefix = "real_evaluation" if real else "matched_control_evaluation"
    evaluations = [row[prefix] for row in rows]
    base = [row["controlled_base"] for row in evaluations]
    stress = [row["controlled_stress_1_5x"] for row in evaluations]
    failures = Counter(
        value for row in rows for value in row.get("failure_vectors", ())
    ) if real else Counter()
    economic = Counter(failures)
    economic.pop("FAMILY_TRIPWIRE_FAILED", None)
    economic.pop("NO_FAILURE_VECTOR", None)
    dominant = (
        sorted(economic.items(), key=lambda item: (-item[1], item[0]))[0][0]
        if economic
        else None
    )
    return {
        "primary_rolling_combine_episode_count": sum(
            int(row["episode_start_count"]) for row in base
        ),
        "stressed_rolling_combine_episode_count": sum(
            int(row["episode_start_count"]) for row in stress
        ),
        "policies_passing_at_least_one_combine_episode": sum(
            int(row["pass_count"]) > 0 for row in base
        ),
        "stressed_policies_passing_at_least_one_combine_episode": sum(
            int(row["pass_count"]) > 0 for row in stress
        ),
        "combine_pass_probability": _distribution(
            [float(row["pass_rate"]) for row in base]
        ),
        "stressed_combine_pass_probability": _distribution(
            [float(row["pass_rate"]) for row in stress]
        ),
        "median_target_progress_distribution": _distribution(
            [float(row["target_progress_median"]) for row in base]
        ),
        "maximum_target_progress": max(
            (float(row["maximum_target_progress"]) for row in base), default=0.0
        ),
        "stressed_target_progress_distribution": _distribution(
            [float(row["target_progress_median"]) for row in stress]
        ),
        "mll_breach_rate_distribution": _distribution(
            [float(row["mll_breach_rate"]) for row in base]
        ),
        "stressed_mll_breach_rate_distribution": _distribution(
            [float(row["mll_breach_rate"]) for row in stress]
        ),
        "stressed_median_episode_net_usd": _distribution(
            [float(row["median_episode_net_pnl"]) for row in stress]
        ),
        "stressed_positive_policy_count": sum(
            float(row["median_episode_net_pnl"]) > 0.0 for row in stress
        ),
        "structurally_distinct_policy_count": len(
            {
                str(row["real_policy" if real else "matched_control_policy"]
                    ["structural_fingerprint"])
                for row in rows
            }
        ),
        "behaviorally_distinct_policy_count": (
            len({str(row["behavioral_fingerprint"]) for row in rows})
            if real
            else None
        ),
        "failure_vector_distribution": dict(sorted(failures.items())),
        "economic_failure_vector_distribution": dict(sorted(economic.items())),
        "dominant_economic_failure": dominant,
        "targeted_mutations_selected": (
            _targeted_mutations(economic) if real else []
        ),
    }


def _paired_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "normal_median_net_delta_usd": _distribution(
            [float(row["paired_delta"]["normal_median_net_usd"]) for row in rows]
        ),
        "stressed_median_net_delta_usd": _distribution(
            [float(row["paired_delta"]["stressed_median_net_usd"]) for row in rows]
        ),
        "normal_target_progress_delta": _distribution(
            [float(row["paired_delta"]["normal_target_progress"]) for row in rows]
        ),
        "stressed_target_progress_delta": _distribution(
            [float(row["paired_delta"]["stressed_target_progress"]) for row in rows]
        ),
        "stressed_mll_breach_rate_delta": _distribution(
            [float(row["paired_delta"]["stressed_mll_breach_rate"]) for row in rows]
        ),
    }


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "TARGET_VELOCITY_LOW": "ADD_DISTINCT_SESSION_TARGET_ACCELERATOR",
        "NO_COMBINE_PASS": "INCREASE_OPPORTUNITY_DENSITY_WITH_BOUNDED_CONCURRENCY",
        "MLL_BREACH_EXCESS": "REDUCE_CORRELATED_EXPOSURE_AND_DAILY_RISK",
        "CONSISTENCY_FAILURE": "LOWER_ACCOUNT_PROFIT_LOCK",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COSTLY_OR_REDUNDANT_SLEEVE",
        "TEMPORAL_BLOCK_INSTABILITY": "REPLACE_WITH_DISTINCT_TEMPORAL_COMPONENT",
        "COMPONENT_CONCENTRATION": "REPLACE_DOMINANT_COMPONENT_WITH_DIVERSIFIER",
        "MATCHED_CONTROL_NOT_BEATEN": "CHANGE_ACCOUNT_REPRESENTATION_NOT_PARAMETERS",
    }
    output: list[dict[str, Any]] = []
    for failure, count in sorted(
        failures.items(), key=lambda item: (-item[1], item[0])
    ):
        if failure not in actions:
            continue
        output.append(
            {
                "priority": len(output) + 1,
                "failure_vector": failure,
                "affected_policy_count": int(count),
                "action": actions[failure],
                "identical_episode_starts_required": True,
            }
        )
        if len(output) == 4:
            break
    return output


def cross_session_report(
    result: Mapping[str, Any], prereg: Mapping[str, Any]
) -> str:
    tripwire = result["family_tripwire"]
    policies = result["account_policy_economics"]
    paired = result["paired_account_economics"]
    wall = result["wall_clock_accounting"]
    return "\n".join(
        [
            "# HYDRA economic evolution — cross-session account synthesis 0009",
            "",
            f"- Verdict: `{result['scientific_status']}` / `{result['report_verdict']}`",
            f"- Policies: {result['policy_pair_evaluated_count']} real + matched controls",
            f"- Tripwire: real {tripwire['real_win_count']}/{tripwire['real_policy_pair_count']} vs control {tripwire['matched_control_win_count']}/{tripwire['real_policy_pair_count']}",
            f"- NULL_RATIO: {tripwire['NULL_RATIO']}",
            f"- p-value binomiale exacte: {tripwire['exact_one_sided_binomial_p_value']:.6g}",
            f"- Passes Combine: {policies['policies_passing_at_least_one_combine_episode']}",
            f"- Progression cible maximale: {policies['maximum_target_progress']:.2%}",
            f"- Médiane delta net stressé vs contrôle: {paired['stressed_median_net_delta_usd']['median']:.2f} USD",
            f"- Recherche / administration: {wall['research_percent']:.2f}% / {wall['tests_and_reporting_percent']:.2f}%",
            "- Données achetées: 0",
            "- Q4: inchangé",
            "- Ordres/broker: 0/0",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
            f"Préréglage WORM: `{prereg['preregistration_hash']}`",
            "",
        ]
    )


def _temporal_blocks(episodes: Sequence[Any], *, count: int) -> list[dict[str, Any]]:
    ordered = sorted(episodes, key=lambda row: row.start_day)
    output: list[dict[str, Any]] = []
    for index in range(count):
        chunk = ordered[index::count]
        values = [float(row.net_pnl) for row in chunk]
        output.append(
            {
                "block_id": f"B{index + 1}",
                "episode_count": len(chunk),
                "median_net_usd": statistics.median(values) if values else 0.0,
                "pass_count": sum(row.passed for row in chunk),
                "mll_breach_count": sum(row.mll_breached for row in chunk),
            }
        )
    return output


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {
            "count": 0,
            "minimum": None,
            "p25": None,
            "median": None,
            "p75": None,
            "maximum": None,
        }
    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "p25": _percentile(ordered, 0.25),
        "median": _percentile(ordered, 0.5),
        "p75": _percentile(ordered, 0.75),
        "maximum": ordered[-1],
    }


def _percentile(ordered: Sequence[float], quantile: float) -> float:
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * quantile
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return float(ordered[low])
    weight = position - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def _binomial_tail(successes: int, trials: int, probability: float) -> float:
    if trials <= 0:
        return 1.0
    return min(
        1.0,
        sum(
            math.comb(trials, index)
            * probability**index
            * (1.0 - probability) ** (trials - index)
            for index in range(successes, trials + 1)
        ),
    )


def _stage(
    writer: AtomicResultWriter,
    prereg: Mapping[str, Any],
    stage: str,
) -> None:
    writer.write_json(
        "cross_session_campaign_state.json",
        {
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "broker_connections": 0,
            "orders": 0,
        },
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise CrossSessionAccountCampaignError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CrossSessionAccountCampaignError(f"expected JSON object: {path}")
    return value


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise CrossSessionAccountCampaignError("project root not found")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "CROSS_SESSION_ENGINE_VERSION",
    "CrossSessionAccountCampaignError",
    "cross_session_final_result",
    "cross_session_report",
    "evaluate_account_policy_pair",
    "evaluate_account_policy_pairs",
    "load_and_verify_cross_session_preregistration",
    "load_and_verify_cross_session_result",
    "paired_account_tripwire",
    "run_cross_session_account_campaign",
]
