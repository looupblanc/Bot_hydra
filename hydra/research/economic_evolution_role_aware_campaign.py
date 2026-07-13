from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.role_aware_account import (
    ROLE_AWARE_CLASS_ID,
    RoleAwarePolicyPair,
    generate_role_aware_account_population,
)
from hydra.economic_evolution.role_aware_account_evaluation import (
    evaluate_role_aware_policy_pairs,
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


ROLE_AWARE_ENGINE_VERSION = "hydra_role_aware_account_allocator_campaign_v1"


class RoleAwareAccountCampaignError(RuntimeError):
    pass


def run_role_aware_account_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Run frozen same-membership role allocation campaign 0010."""

    started = time.perf_counter()
    phases: dict[str, float] = {}
    phase_started = started
    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_role_aware_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    hypothesis_path = root / str(prereg["hypothesis_worm"]["path"])
    if _sha256(hypothesis_path) != str(
        prereg["hypothesis_worm"]["file_sha256"]
    ):
        raise RoleAwareAccountCampaignError("hypothesis WORM checksum drift")
    hypothesis = _load_json(hypothesis_path)
    if hypothesis.get("hypothesis_hash") != prereg["hypothesis_worm"][
        "semantic_hash"
    ]:
        raise RoleAwareAccountCampaignError("hypothesis WORM semantic drift")

    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise RoleAwareAccountCampaignError("source seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise RoleAwareAccountCampaignError("source seed semantic hash drift")
    population = _population(seed, prereg)
    expected_manifest = str(
        prereg["structural_population"]["policy_manifest_hash"]
    )
    if population.manifest_hash != expected_manifest:
        raise RoleAwareAccountCampaignError(
            "frozen role-aware policy manifest drift"
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
    phases["preregistration_and_population"] = (
        time.perf_counter() - phase_started
    )

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
        raise RoleAwareAccountCampaignError(
            "role-aware replay requires every frozen component runtime"
        )
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")
    phases["exact_component_replay"] = time.perf_counter() - phase_started

    common_days = _common_days(list(runtimes.values()))
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    if len(starts) != int(prereg["rolling_episode_policy"]["maximum_starts"]):
        raise RoleAwareAccountCampaignError(
            "role-aware campaign has incomplete frozen episode starts"
        )

    phase_started = time.perf_counter()
    pair_rows = evaluate_role_aware_policy_pairs(
        population.pairs,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=int(prereg["compute"]["account_worker_count"]),
    )
    if len(pair_rows) != int(
        prereg["structural_population"]["policy_pair_count"]
    ):
        raise RoleAwareAccountCampaignError(
            "role-aware paired account evaluation is incomplete"
        )
    writer.write_jsonl_batch("account_pair_results.jsonl", pair_rows)
    _stage(state_writer, prereg, "PAIRED_ACCOUNT_REPLAY_COMPLETE")
    phases["paired_account_replay"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    tripwire = role_aware_paired_tripwire(
        pair_rows, prereg["family_tripwire"]
    )
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")
    phases["family_tripwire"] = time.perf_counter() - phase_started

    result = role_aware_final_result(
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
    writer.write_json("role_aware_account_result.json", result)
    writer.write_text(
        "role_aware_account_report.md", role_aware_report(result, prereg)
    )
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_role_aware_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg = _load_json(Path(path).resolve())
    claimed = prereg.get("preregistration_hash")
    payload = dict(prereg)
    payload.pop("preregistration_hash", None)
    structural = prereg.get("structural_population") or {}
    statuses = prereg.get("statuses") or {}
    governance = prereg.get("governance") or {}
    tripwire = prereg.get("family_tripwire") or {}
    if (
        prereg.get("schema")
        != "hydra_role_aware_account_allocator_preregistration_v1"
        or prereg.get("class_id") != ROLE_AWARE_CLASS_ID
        or stable_hash(payload) != claimed
        or int(structural.get("policy_pair_count", -1)) != 512
        or int(structural.get("component_count", -1)) != 48
        or structural.get("same_sleeve_membership_within_pair") is not True
        or structural.get("same_total_risk_units_within_pair") is not True
        or structural.get("same_account_limits_within_pair") is not True
        or structural.get("new_market_outcomes_seen_during_generation") is not False
        or structural.get("same_class_0009_rescue") is not False
        or float(
            tripwire.get("maximum_stressed_consistency_pass_rate_deterioration", -1)
        )
        != 0.05
        or statuses.get("validated_allowed") is not False
        or statuses.get("pre_holdout_ready_allowed") is not False
        or statuses.get("paper_shadow_ready_allowed") is not False
        or statuses.get("status_inheritance") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
    ):
        raise RoleAwareAccountCampaignError(
            "invalid role-aware account preregistration"
        )
    return prereg


def load_and_verify_role_aware_result(
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
        or result.get("class_id") != ROLE_AWARE_CLASS_ID
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
        raise RoleAwareAccountCampaignError(
            "role-aware result integrity failure"
        )
    return result


def role_aware_paired_tripwire(
    rows: Sequence[Mapping[str, Any]], gate: Mapping[str, Any]
) -> dict[str, Any]:
    minimum_net = float(gate["minimum_stressed_median_net_delta_usd"])
    minimum_progress = float(gate["minimum_stressed_target_progress_delta"])
    maximum_mll = float(gate["maximum_stressed_mll_breach_rate_deterioration"])
    maximum_consistency = float(
        gate["maximum_stressed_consistency_pass_rate_deterioration"]
    )
    real_wins = 0
    control_wins = 0
    ties = 0
    for row in rows:
        delta = row["paired_delta"]
        real = bool(
            float(delta["stressed_median_net_usd"]) >= minimum_net
            and float(delta["stressed_target_progress"]) >= minimum_progress
            and float(delta["stressed_mll_breach_rate"]) <= maximum_mll
            and float(delta["stressed_consistency_pass_rate"])
            >= -maximum_consistency
        )
        control = bool(
            float(delta["stressed_median_net_usd"]) <= -minimum_net
            and float(delta["stressed_target_progress"]) <= -minimum_progress
            and float(delta["stressed_mll_breach_rate"]) >= -maximum_mll
            and float(delta["stressed_consistency_pass_rate"])
            <= maximum_consistency
        )
        if real and not control:
            real_wins += 1
        elif control and not real:
            control_wins += 1
        else:
            ties += 1
    informative = real_wins + control_wins
    ratio = None if real_wins == 0 else control_wins / real_wins
    p_value = _binomial_tail(real_wins, informative, 0.5)
    median_net = _median_delta(rows, "stressed_median_net_usd")
    median_progress = _median_delta(rows, "stressed_target_progress")
    median_mll = _median_delta(rows, "stressed_mll_breach_rate")
    median_consistency = _median_delta(
        rows, "stressed_consistency_pass_rate"
    )
    green = bool(
        len(rows) >= int(gate["minimum_policy_pairs"])
        and informative >= int(gate["minimum_informative_pairs"])
        and median_net >= minimum_net
        and median_progress >= minimum_progress
        and median_mll <= maximum_mll
        and median_consistency >= -maximum_consistency
        and real_wins > 0
        and ratio is not None
        and ratio < float(gate["maximum_NULL_RATIO"])
        and p_value <= float(gate["exact_one_sided_binomial_p_value"])
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
        "class_id": ROLE_AWARE_CLASS_ID,
        "real_win_count": real_wins,
        "real_policy_pair_count": len(rows),
        "real_win_rate": real_wins / max(len(rows), 1),
        "matched_control_win_count": control_wins,
        "matched_control_win_rate": control_wins / max(len(rows), 1),
        "tie_count": ties,
        "informative_pair_count": informative,
        "median_stressed_net_delta_usd": median_net,
        "median_stressed_target_progress_delta": median_progress,
        "median_stressed_mll_breach_rate_delta": median_mll,
        "median_stressed_consistency_pass_rate_delta": median_consistency,
        "NULL_RATIO": ratio,
        "maximum_NULL_RATIO": float(gate["maximum_NULL_RATIO"]),
        "exact_one_sided_binomial_p_value": p_value,
        "family_green": green,
        "evidence_strength": strength,
        "verdict": verdict,
        "thresholds_changed_after_outcome": False,
    }


def role_aware_final_result(
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
        _account_decision(
            row, gate=gate, family_green=bool(tripwire["family_green"])
        )
        for row in pair_rows
    ]
    diagnostic = sum(row["diagnostic_account_gate_passed"] for row in annotated)
    research = sum(row["account_research_gate_passed"] for row in annotated)
    combine = sum(row["combine_path_diagnostic"] for row in annotated)
    if not tripwire["family_green"]:
        status = str(tripwire["verdict"])
        verdict = (
            "ARTEFACT"
            if tripwire["evidence_strength"] == "ARTEFACT"
            else "NULL"
        )
    elif combine:
        status, verdict = "DEVELOPMENT_COMBINE_PATH_CANDIDATES_FOUND", "GREEN"
    elif research:
        status, verdict = (
            "DEVELOPMENT_ACCOUNT_RESEARCH_CANDIDATES_FOUND",
            "GREEN",
        )
    else:
        status, verdict = "GREEN_TRIPWIRE_NO_ACCOUNT_SURVIVOR", "NULL"
    account_metrics = _policy_metrics(annotated, real=True)
    control_metrics = _policy_metrics(annotated, real=False)
    paired_metrics = _paired_metrics(annotated)
    phases = {str(key): float(value) for key, value in phase_seconds.items()}
    research_names = {
        "feature_store_verification",
        "component_screen",
        "exact_component_replay",
        "paired_account_replay",
        "family_tripwire",
    }
    research_seconds = sum(
        value for key, value in phases.items() if key in research_names
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
        "schema": "hydra_role_aware_account_result_v1",
        "engine_version": ROLE_AWARE_ENGINE_VERSION,
        "campaign_id": str(prereg["campaign_id"]),
        "class_id": ROLE_AWARE_CLASS_ID,
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
            "SCALE_ROLE_AWARE_ACCOUNT_SURVIVORS"
            if combine or research
            else "TOMBSTONE_EXACT_0010_AND_CHANGE_ACCOUNT_REPRESENTATION"
        ),
        "CONTRE": (
            "Role labels were assigned in development and may encode selection "
            "bias. Same-membership permutation controls test allocation uplift "
            "but do not create independent confirmation or shadow eligibility."
        ),
    }


def _population(seed: Mapping[str, Any], prereg: Mapping[str, Any]):
    structural = prereg["structural_population"]
    return generate_role_aware_account_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        policy_pair_count=int(structural["policy_pair_count"]),
        maximum_components=int(structural["component_count"]),
        minimum_component_events=int(structural["minimum_component_events"]),
        minimum_markets=int(structural["minimum_markets"]),
        minimum_sessions=int(structural["minimum_sessions"]),
        minimum_roles=int(structural["minimum_roles"]),
    )


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
        row, gate=gate, hard_ok=hard_ok, family_green=family_green
    )
    value.update(
        {
            "status": (
                "COMBINE_PATH_CANDIDATE_DEVELOPMENT_ONLY"
                if combine
                else "ACCOUNT_POLICY_RESEARCH_CANDIDATE"
                if research
                else "ROLE_AWARE_ACCOUNT_DIAGNOSTIC_ONLY"
                if diagnostic
                else "ROLE_AWARE_ACCOUNT_RESEARCH_FAILED"
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
    failures = (
        Counter(value for row in rows for value in row.get("failure_vectors", ()))
        if real
        else Counter()
    )
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
        "median_episode_net_usd": _distribution(
            [float(row["median_episode_net_pnl"]) for row in base]
        ),
        "stressed_median_episode_net_usd": _distribution(
            [float(row["median_episode_net_pnl"]) for row in stress]
        ),
        "normal_positive_policy_count": sum(
            float(row["median_episode_net_pnl"]) > 0.0 for row in base
        ),
        "stressed_positive_policy_count": sum(
            float(row["median_episode_net_pnl"]) > 0.0 for row in stress
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
        "consistency_pass_rate_distribution": _distribution(
            [float(row["consistency_pass_rate"]) for row in base]
        ),
        "stressed_consistency_pass_rate_distribution": _distribution(
            [float(row["consistency_pass_rate"]) for row in stress]
        ),
        "structurally_distinct_policy_count": len(
            {
                str(
                    row[
                        "real_policy" if real else "matched_control_policy"
                    ]["structural_fingerprint"]
                )
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
        key: _distribution(
            [float(row["paired_delta"][source]) for row in rows]
        )
        for key, source in (
            ("normal_median_net_delta_usd", "normal_median_net_usd"),
            ("stressed_median_net_delta_usd", "stressed_median_net_usd"),
            ("normal_target_progress_delta", "normal_target_progress"),
            ("stressed_target_progress_delta", "stressed_target_progress"),
            ("stressed_mll_breach_rate_delta", "stressed_mll_breach_rate"),
            (
                "stressed_consistency_pass_rate_delta",
                "stressed_consistency_pass_rate",
            ),
        )
    }


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "TARGET_VELOCITY_LOW": "CHANGE_ACCOUNT_REPRESENTATION_NOT_ROLE_THRESHOLDS",
        "NO_COMBINE_PASS": "ADD_PREREGISTERED_ACCOUNT_STATE_ROUTING",
        "MLL_BREACH_EXCESS": "REDUCE_CORRELATED_CONCURRENT_EXPOSURE",
        "CONSISTENCY_FAILURE": "PREREGISTER_ACCOUNT_LEVEL_PROFIT_GUARD",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COSTLY_REDUNDANT_ROLE",
        "TEMPORAL_BLOCK_INSTABILITY": "REPLACE_TEMPORALLY_UNSTABLE_ROLE",
        "COMPONENT_CONCENTRATION": "REPLACE_DOMINANT_ROLE_WITH_DIVERSIFIER",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_ROLE_ALLOCATOR_CLASS",
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
                "same_class_parameter_rescue": False,
            }
        )
        if len(output) == 4:
            break
    return output


def role_aware_report(
    result: Mapping[str, Any], prereg: Mapping[str, Any]
) -> str:
    tripwire = result["family_tripwire"]
    policies = result["account_policy_economics"]
    paired = result["paired_account_economics"]
    wall = result["wall_clock_accounting"]
    return "\n".join(
        [
            "# HYDRA economic evolution — role-aware allocator 0010",
            "",
            f"- Verdict: `{result['scientific_status']}` / `{result['report_verdict']}`",
            f"- Policies: {result['policy_pair_evaluated_count']} real + same-membership controls",
            f"- Tripwire: real {tripwire['real_win_count']}/{tripwire['real_policy_pair_count']} vs control {tripwire['matched_control_win_count']}/{tripwire['real_policy_pair_count']}",
            f"- NULL_RATIO: {tripwire['NULL_RATIO']}",
            f"- p-value binomiale exacte: {tripwire['exact_one_sided_binomial_p_value']:.6g}",
            f"- Médiane delta net stressé: {tripwire['median_stressed_net_delta_usd']:.2f} USD",
            f"- Médiane delta progression: {tripwire['median_stressed_target_progress_delta']:.2%}",
            f"- Passes Combine: {policies['policies_passing_at_least_one_combine_episode']}",
            f"- Progression cible maximale: {policies['maximum_target_progress']:.2%}",
            f"- Médiane delta consistance: {paired['stressed_consistency_pass_rate_delta']['median']:.2%}",
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


def _median_delta(rows: Sequence[Mapping[str, Any]], key: str) -> float:
    values = [float(row["paired_delta"][key]) for row in rows]
    return float(statistics.median(values)) if values else 0.0


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
        "role_aware_campaign_state.json",
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
        raise RoleAwareAccountCampaignError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise RoleAwareAccountCampaignError(f"expected JSON object: {path}")
    return value


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise RoleAwareAccountCampaignError("project root not found")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "ROLE_AWARE_ENGINE_VERSION",
    "RoleAwareAccountCampaignError",
    "load_and_verify_role_aware_preregistration",
    "load_and_verify_role_aware_result",
    "role_aware_final_result",
    "role_aware_paired_tripwire",
    "role_aware_report",
    "run_role_aware_account_campaign",
]
