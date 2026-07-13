from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import (
    ExactSleeveRuntime,
    compile_account_policy,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.directional_agreement import (
    AGREEMENT_CLASS_ID,
    generate_directional_agreement_population,
)
from hydra.economic_evolution.schema import AccountPolicyGenome, stable_hash
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


AGREEMENT_ENGINE_VERSION = "hydra_directional_agreement_campaign_v1"


class DirectionalAgreementCampaignError(RuntimeError):
    pass


def run_directional_agreement_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    """Run the outcome-frozen 0008 directional agreement campaign."""

    started = time.perf_counter()
    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_agreement_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise DirectionalAgreementCampaignError("source seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise DirectionalAgreementCampaignError("source seed semantic hash drift")

    population = _population(seed, prereg)
    expected_manifest = str(
        prereg["structural_population"]["candidate_manifest_hash"]
    )
    if population.candidate_manifest_hash != expected_manifest:
        raise DirectionalAgreementCampaignError(
            "frozen agreement candidate manifest drift"
        )
    writer.write_json(
        "structural_population.json",
        {
            **population.summary(),
            "sources": [row.to_dict() for row in population.sources],
            "source_by_candidate": dict(population.source_by_candidate),
            "horizon_by_candidate": dict(population.horizon_by_candidate),
            "components": [row.to_dict() for row in population.components],
            "real_sleeves": [row.to_dict() for row in population.real_sleeves],
            "matched_null_sleeves": [
                row.to_dict() for row in population.matched_null_sleeves
            ],
            "policies": [row.to_dict() for row in population.policies],
            "policy_archetypes": dict(population.policy_archetypes),
        },
    )
    _stage(state_writer, prereg, "STRUCTURAL_POPULATION_FROZEN")

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

    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    all_sleeves = (*population.real_sleeves, *population.matched_null_sleeves)
    screen = run_ultra_cheap_screen(all_sleeves, matrices, policy=screen_policy)
    writer.write_json("cheap_screen_summary.json", screen.summary())
    writer.write_jsonl_batch("cheap_screen_results.jsonl", list(screen.rows))
    _stage(state_writer, prereg, "CHEAP_SCREEN_COMPLETE")

    bound = _bind_selected(all_sleeves, matrices, policy=screen_policy)
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
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")

    tripwire = family_tripwire(population, runtimes, prereg["component_gate"])
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")

    account_rows: list[dict[str, Any]] = []
    global_starts: tuple[int, ...] = ()
    if tripwire["family_green"]:
        real_ids = {row.sleeve_id for row in population.real_sleeves}
        eligible = {
            sleeve_id: runtime
            for sleeve_id, runtime in runtimes.items()
            if sleeve_id in real_ids
            and component_pass(runtime, prereg["component_gate"])
        }
        frozen_policies = [
            policy
            for policy in population.policies
            if set(policy.sleeve_ids).issubset(eligible)
        ]
        ranked = sorted(
            frozen_policies,
            key=lambda policy: (
                -sum(
                    eligible[row].cost_stress_1_5x_net
                    for row in policy.sleeve_ids
                ),
                policy.structural_fingerprint,
            ),
        )[: int(prereg["funnel"]["maximum_account_policy_evaluations"])]
        if ranked:
            used = sorted({item for row in ranked for item in row.sleeve_ids})
            common_days = _common_days([eligible[row] for row in used])
            global_starts = select_episode_starts(
                common_days,
                policy=EpisodeStartPolicy(**prereg["rolling_episode_policy"]),
            )
            if len(global_starts) < int(
                prereg["account_gate"]["minimum_episode_starts"]
            ):
                raise DirectionalAgreementCampaignError(
                    "frozen agreement policies produced insufficient common starts"
                )
            for policy in ranked:
                account_rows.append(
                    evaluate_policy(
                        policy,
                        eligible,
                        global_starts=global_starts,
                        prereg=prereg,
                    )
                )
    writer.write_jsonl_batch("account_policy_results.jsonl", account_rows)
    _stage(state_writer, prereg, "ACCOUNT_POLICY_EVALUATION_COMPLETE")

    result = final_result(
        prereg,
        population_summary=population.summary(),
        screen_summary=screen.summary(),
        exact_runtime_count=len(runtimes),
        exact_failure_count=len(exact_failures),
        tripwire=tripwire,
        account_rows=account_rows,
        global_starts=global_starts,
        elapsed_seconds=time.perf_counter() - started,
    )
    result["result_sha256"] = stable_hash(result)
    writer.write_json("directional_agreement_result.json", result)
    writer.write_text("directional_agreement_report.md", report(result, prereg))
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_agreement_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg_path = Path(path).resolve()
    try:
        raw = json.loads(prereg_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise DirectionalAgreementCampaignError(
            "invalid agreement preregistration"
        ) from exc
    if raw.get("schema") != "hydra_directional_agreement_preregistration_v1":
        raise DirectionalAgreementCampaignError(
            "agreement preregistration schema drift"
        )
    expected = str(raw.get("preregistration_hash") or "")
    payload = dict(raw)
    payload.pop("preregistration_hash", None)
    if stable_hash(payload) != expected:
        raise DirectionalAgreementCampaignError(
            "agreement preregistration hash drift"
        )
    if raw.get("campaign_id") != (
        "hydra_economic_evolution_multi_horizon_agreement_0008"
    ):
        raise DirectionalAgreementCampaignError("agreement campaign identity drift")
    if raw.get("class_id") != AGREEMENT_CLASS_ID:
        raise DirectionalAgreementCampaignError("agreement class identity drift")
    if raw.get("q4_access_allowed") is not False:
        raise DirectionalAgreementCampaignError("Q4 must remain unavailable")
    if raw.get("new_data_purchase_allowed") is not False:
        raise DirectionalAgreementCampaignError(
            "new data purchase must remain disabled"
        )
    if raw.get("broker_or_orders_allowed") is not False:
        raise DirectionalAgreementCampaignError(
            "broker/order path must remain disabled"
        )
    if raw.get("source_outcomes_from_0007_used") is not False:
        raise DirectionalAgreementCampaignError(
            "0007 outcome feedback is forbidden"
        )
    root = _project_root(prereg_path)
    for relative, digest in raw["implementation_files"].items():
        if _sha256(root / str(relative)) != str(digest):
            raise DirectionalAgreementCampaignError(
                f"agreement implementation checksum drift: {relative}"
            )
    return raw


def load_and_verify_agreement_result(
    path: str | Path, prereg: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        result = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise DirectionalAgreementCampaignError("invalid agreement result") from exc
    expected = str(result.get("result_sha256") or "")
    payload = dict(result)
    payload.pop("result_sha256", None)
    if stable_hash(payload) != expected:
        raise DirectionalAgreementCampaignError("agreement result hash drift")
    if result.get("campaign_id") != prereg.get("campaign_id"):
        raise DirectionalAgreementCampaignError("agreement result campaign drift")
    if result.get("validated") is not False:
        raise DirectionalAgreementCampaignError(
            "development result claimed validation"
        )
    if result.get("governance") != {
        "proof_windows_consumed": 0,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
    }:
        raise DirectionalAgreementCampaignError(
            "agreement result governance drift"
        )
    return result


def _population(seed: Mapping[str, Any], prereg: Mapping[str, Any]):
    structural = prereg["structural_population"]
    return generate_directional_agreement_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        excluded_source_sleeve_ids=tuple(
            structural["excluded_source_sleeve_ids"]
        ),
        maximum_sources=int(structural["maximum_sources"]),
        maximum_sources_per_market=int(
            structural["maximum_sources_per_market"]
        ),
        maximum_sources_per_market_session=int(
            structural["maximum_sources_per_market_session"]
        ),
        maximum_sources_per_market_mechanism=int(
            structural["maximum_sources_per_market_mechanism"]
        ),
        minimum_source_events=int(structural["minimum_source_events"]),
        contexts_per_source=int(structural["contexts_per_source"]),
        agreement_quantile=float(structural["agreement_quantile"]),
        policy_count=int(structural["account_policy_count"]),
    )


def family_tripwire(population, runtimes, gate: Mapping[str, Any]) -> dict[str, Any]:
    real_ids = [row.sleeve_id for row in population.real_sleeves]
    null_ids = [row.sleeve_id for row in population.matched_null_sleeves]
    real_pass = sum(
        sleeve_id in runtimes and component_pass(runtimes[sleeve_id], gate)
        for sleeve_id in real_ids
    )
    null_pass = sum(
        sleeve_id in runtimes and component_pass(runtimes[sleeve_id], gate)
        for sleeve_id in null_ids
    )
    real_rate = real_pass / max(len(real_ids), 1)
    null_rate = null_pass / max(len(null_ids), 1)
    ratio = None if real_rate == 0.0 else null_rate / real_rate
    p_value = _binomial_tail(real_pass, len(real_ids), null_rate)
    maximum_ratio = float(gate["maximum_null_ratio"])
    real_missing = len(real_ids) - sum(
        sleeve_id in runtimes for sleeve_id in real_ids
    )
    null_missing = len(null_ids) - sum(
        sleeve_id in runtimes for sleeve_id in null_ids
    )
    family_green = bool(
        real_missing == 0
        and null_missing == 0
        and real_pass > 0
        and ratio is not None
        and ratio < maximum_ratio
    )
    if real_missing or null_missing:
        verdict, strength = "INCOMPLETE_EXACT_REPLAY_FAIL_CLOSED", "BLOCKED"
    elif family_green:
        verdict = "GREEN_NULL_ADJUSTED_BASELINE"
        strength = (
            "VERT_NET"
            if p_value <= float(gate["net_evidence_p_value"])
            else "VERT_MINCE"
        )
    elif real_pass == 0:
        verdict, strength = "FORMULATION_FALSIFIED_NO_REAL_COMPONENT_PASS", "NULL"
    else:
        verdict, strength = "ARTEFACT_GEOMETRY_ONLY", "ARTEFACT"
    return {
        "class_id": AGREEMENT_CLASS_ID,
        "real_pass_count": real_pass,
        "real_candidate_count": len(real_ids),
        "real_exact_replay_missing_count": real_missing,
        "real_pass_rate": real_rate,
        "null_pass_count": null_pass,
        "null_candidate_count": len(null_ids),
        "null_exact_replay_missing_count": null_missing,
        "null_pass_rate": null_rate,
        "NULL_RATIO": ratio,
        "maximum_NULL_RATIO": maximum_ratio,
        "exact_one_sided_binomial_p_value": p_value,
        "evidence_strength": strength,
        "verdict": verdict,
        "family_green": family_green,
        "thresholds_changed_after_outcome": False,
    }


def component_pass(runtime: ExactSleeveRuntime, gate: Mapping[str, Any]) -> bool:
    return bool(
        runtime.event_count >= int(gate["minimum_events"])
        and runtime.net_pnl > 0.0
        and runtime.cost_stress_1_5x_net > 0.0
        and runtime.best_positive_event_share
        <= float(gate["maximum_best_positive_event_share"])
        and runtime.maximum_drawdown <= float(gate["maximum_drawdown_usd"])
    )


def evaluate_policy(
    policy: AccountPolicyGenome,
    runtimes: Mapping[str, ExactSleeveRuntime],
    *,
    global_starts: tuple[int, ...],
    prereg: Mapping[str, Any],
) -> dict[str, Any]:
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    full = evaluate_compiled_account_policy(
        compile_account_policy(policy, runtimes),
        episode_policy=episode_policy,
        explicit_start_days=global_starts,
        evaluate_xfa=False,
    )
    controls: list[dict[str, Any]] = []
    for removed in policy.sleeve_ids:
        control = _without(policy, removed)
        evaluation = evaluate_compiled_account_policy(
            compile_account_policy(control, runtimes),
            episode_policy=episode_policy,
            explicit_start_days=full.episode_start_days,
            evaluate_xfa=False,
        )
        full_stress = full.controlled_stress_1_5x
        control_stress = evaluation.controlled_stress_1_5x
        controls.append(
            {
                "removed_sleeve_id": removed,
                "control_policy_id": control.policy_id,
                "identical_episode_starts": (
                    evaluation.episode_start_days == full.episode_start_days
                ),
                "add_one_stressed_median_net_delta": (
                    full_stress.median_episode_net_pnl
                    - control_stress.median_episode_net_pnl
                ),
                "add_one_target_progress_delta": (
                    full_stress.target_progress_median
                    - control_stress.target_progress_median
                ),
                "add_one_mll_breach_rate_delta": (
                    full_stress.mll_breach_rate - control_stress.mll_breach_rate
                ),
                "leave_one_out_stressed_median_net": (
                    control_stress.median_episode_net_pnl
                ),
                "leave_one_out_target_progress": (
                    control_stress.target_progress_median
                ),
                "leave_one_out_mll_breach_rate": control_stress.mll_breach_rate,
                "leave_one_out_dominates": bool(
                    control_stress.median_episode_net_pnl
                    > full_stress.median_episode_net_pnl
                    and control_stress.mll_breach_rate
                    <= full_stress.mll_breach_rate
                    and control_stress.target_progress_median
                    >= full_stress.target_progress_median
                ),
            }
        )
    blocks = _temporal_blocks(full.controlled_stress_1_5x.episodes, count=4)
    positive_blocks = sum(row["median_net_usd"] > 0.0 for row in blocks)
    positive_contribution = {
        key: max(0.0, value)
        for key, value in full.controlled_stress_1_5x.component_contribution.items()
    }
    total_positive = sum(positive_contribution.values())
    maximum_component_share = (
        max(positive_contribution.values(), default=0.0) / total_positive
        if total_positive > 0.0
        else 1.0
    )
    gate = prereg["account_gate"]
    normal, stress = full.controlled_base, full.controlled_stress_1_5x
    hard_ok = (
        normal.compliance_failure_count == 0
        and stress.compliance_failure_count == 0
    )
    base_gate = bool(
        hard_ok
        and normal.median_episode_net_pnl
        > float(gate["minimum_normal_median_net_usd"])
        and stress.median_episode_net_pnl
        > float(gate["minimum_stressed_median_net_usd"])
        and stress.target_progress_median
        >= float(gate["minimum_median_target_progress"])
        and stress.mll_breach_rate <= float(gate["maximum_mll_breach_rate"])
        and stress.consistency_pass_rate
        >= float(gate["minimum_consistency_pass_rate"])
        and positive_blocks >= int(gate["minimum_positive_temporal_blocks"])
        and maximum_component_share
        <= float(gate["maximum_positive_component_share"])
        and not any(row["leave_one_out_dominates"] for row in controls)
    )
    combine_path = bool(
        base_gate
        and stress.pass_count >= int(gate["minimum_combine_path_pass_count"])
    )
    status = (
        "COMBINE_PATH_CANDIDATE_DEVELOPMENT_ONLY"
        if combine_path
        else "AGREEMENT_ASSEMBLY_RESEARCH_CANDIDATE"
        if base_gate
        else "AGREEMENT_ASSEMBLY_RESEARCH_FAILED"
    )
    return {
        "policy": policy.to_dict(),
        "status": status,
        "validated": False,
        "development_only": True,
        "account_research_gate_passed": base_gate,
        "combine_path_diagnostic": combine_path,
        "evaluation": full.to_dict(include_episodes=False),
        "temporal_blocks_1_5x": blocks,
        "positive_temporal_block_count": positive_blocks,
        "maximum_positive_component_share": maximum_component_share,
        "matched_add_one_leave_one_out_controls": controls,
        "all_controls_identical_starts": all(
            row["identical_episode_starts"] for row in controls
        ),
        "proof_window_consumed": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "orders": 0,
    }


def final_result(
    prereg: Mapping[str, Any],
    *,
    population_summary: Mapping[str, Any],
    screen_summary: Mapping[str, Any],
    exact_runtime_count: int,
    exact_failure_count: int,
    tripwire: Mapping[str, Any],
    account_rows: Sequence[Mapping[str, Any]],
    global_starts: Sequence[int],
    elapsed_seconds: float,
) -> dict[str, Any]:
    research = [row for row in account_rows if row["account_research_gate_passed"]]
    combine = [row for row in account_rows if row["combine_path_diagnostic"]]
    if not tripwire["family_green"]:
        status = str(tripwire["verdict"])
        verdict = "ARTEFACT" if status == "ARTEFACT_GEOMETRY_ONLY" else "NULL"
    elif combine:
        status, verdict = "DEVELOPMENT_COMBINE_PATH_CANDIDATES_FOUND", "GREEN"
    elif research:
        status, verdict = "DEVELOPMENT_ACCOUNT_RESEARCH_CANDIDATES_FOUND", "GREEN"
    else:
        status, verdict = "GREEN_TRIPWIRE_NO_ACCOUNT_SURVIVOR", "NULL"
    best = sorted(
        account_rows,
        key=lambda row: (
            -float(
                row["evaluation"]["controlled_stress_1_5x"]
                ["target_progress_median"]
            ),
            -float(
                row["evaluation"]["controlled_stress_1_5x"]
                ["median_episode_net_pnl"]
            ),
            str(row["policy"]["policy_id"]),
        ),
    )[:10]
    return {
        "schema": "hydra_directional_agreement_result_v1",
        "engine_version": AGREEMENT_ENGINE_VERSION,
        "campaign_id": str(prereg["campaign_id"]),
        "class_id": AGREEMENT_CLASS_ID,
        "completed_at_utc": utc_now_iso(),
        "scientific_status": status,
        "report_verdict": verdict,
        "validated": False,
        "development_only": True,
        "population": dict(population_summary),
        "cheap_screen": dict(screen_summary),
        "exact_component_runtime_count": exact_runtime_count,
        "exact_component_failure_count": exact_failure_count,
        "family_tripwire": dict(tripwire),
        "global_episode_start_count": len(global_starts),
        "global_episode_starts": list(global_starts),
        "account_policy_evaluated_count": len(account_rows),
        "account_research_candidate_count": len(research),
        "combine_path_diagnostic_count": len(combine),
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "best_development_policies": [
            {
                "policy_id": row["policy"]["policy_id"],
                "status": row["status"],
                "stress_median_net_usd": row["evaluation"]
                ["controlled_stress_1_5x"]["median_episode_net_pnl"],
                "stress_median_target_progress": row["evaluation"]
                ["controlled_stress_1_5x"]["target_progress_median"],
                "stress_mll_breach_rate": row["evaluation"]
                ["controlled_stress_1_5x"]["mll_breach_rate"],
                "stress_consistency_pass_rate": row["evaluation"]
                ["controlled_stress_1_5x"]["consistency_pass_rate"],
            }
            for row in best
        ],
        "multiplicity": {
            "reserved_delta": int(
                prereg["multiplicity"]["reserved_delta_trials"]
            ),
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
        "elapsed_seconds": elapsed_seconds,
        "next_action": (
            "CLASS_TOMBSTONE_AND_NEW_REPRESENTATION"
            if not tripwire["family_green"]
            else "POWER_AUDIT_BEST_AGREEMENT_ACCOUNT_POLICIES"
            if research
            else "CLASS_LEVEL_FAILURE_REVIEW_NO_PARAMETER_RESCUE"
        ),
        "CONTRE": (
            "The 0008 sources and context graphs are selected on reused development "
            "history. A green paired tripwire or account path remains development "
            "evidence and cannot authorize shadow or promotion without powered "
            "independent confirmation."
        ),
    }


def report(result: Mapping[str, Any], prereg: Mapping[str, Any]) -> str:
    tripwire = result["family_tripwire"]
    against = str(result["CONTRE"]).replace(" ", "_")
    budget = prereg["budget"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0008 verdict={result['report_verdict']}",
            (
                "gate=DIRECTIONAL_AGREEMENT_0008 "
                "preuve=reports/economic_evolution/directional_agreement_0008/"
                "directional_agreement_result.json#"
                f"{str(result['result_sha256'])[:8]} "
                f"tests={prereg['deployment_evidence']['minimum_full_regression_passes']}+_verts"
            ),
            (
                "budget_llm=usage_API_non_exposee/solde "
                f"budget_data={budget['actual_spend_usd']}/{budget['hard_cap_usd']}_USD "
                f"N_trials={budget['N_trials_after_reservation']} burned=1"
            ),
            (
                "diff_validation=hydra/research/"
                "economic_evolution_agreement_campaign.py," 
                "tests/test_economic_evolution_agreement_campaign.py "
                f"CONTRE={against}"
            ),
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA Economic Evolution — accord directionnel 0008",
            "",
            f"- Statut : `{result['scientific_status']}`.",
            (
                "- Sources / sleeves réelles / nulls : "
                f"{result['population']['source_count']} / "
                f"{result['population']['real_sleeve_count']} / "
                f"{result['population']['matched_null_sleeve_count']}."
            ),
            (
                "- Politiques gelées / évaluées : "
                f"{result['population']['account_policy_count']} / "
                f"{result['account_policy_evaluated_count']}."
            ),
            (
                f"- Tripwire réel : {tripwire['real_pass_count']}/"
                f"{tripwire['real_candidate_count']}; null : "
                f"{tripwire['null_pass_count']}/{tripwire['null_candidate_count']}; "
                f"NULL_RATIO={tripwire['NULL_RATIO']}; "
                f"p={tripwire['exact_one_sided_binomial_p_value']:.6g}."
            ),
            f"- Account research candidates : {result['account_research_candidate_count']}.",
            f"- Combine paths développement : {result['combine_path_diagnostic_count']}.",
            "- PRE_HOLDOUT_READY : 0 ; PAPER_SHADOW_READY : 0.",
            "- Achat data : 0 ; Q4 : 0 nouvel accès ; ordres/broker : 0/0.",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )


def _without(policy: AccountPolicyGenome, removed: str) -> AccountPolicyGenome:
    retained = [
        (sleeve, units)
        for sleeve, units in zip(
            policy.sleeve_ids, policy.allocation_units, strict=True
        )
        if sleeve != removed
    ]
    if not retained:
        raise ValueError("leave-one-out control cannot be empty")
    sleeves = tuple(row[0] for row in retained)
    return replace(
        policy,
        policy_id=f"{policy.policy_id}::LOO::{removed}",
        sleeve_ids=sleeves,
        allocation_units=tuple(row[1] for row in retained),
        maximum_simultaneous_positions=min(
            policy.maximum_simultaneous_positions, len(sleeves)
        ),
        parent_policy_ids=(policy.policy_id,),
    )


def _temporal_blocks(episodes: Sequence[Any], *, count: int) -> list[dict[str, Any]]:
    ordered = sorted(episodes, key=lambda row: row.start_day)
    output: list[dict[str, Any]] = []
    for block in range(count):
        values = [
            row
            for index, row in enumerate(ordered)
            if min(count - 1, index * count // max(len(ordered), 1)) == block
        ]
        if not values:
            continue
        output.append(
            {
                "block_id": f"B{block + 1}",
                "start_count": len(values),
                "first_start_day": values[0].start_day,
                "last_start_day": values[-1].start_day,
                "median_net_usd": statistics.median(
                    row.net_pnl for row in values
                ),
                "median_target_progress": statistics.median(
                    row.target_progress for row in values
                ),
                "mll_breach_rate": sum(row.mll_breached for row in values)
                / len(values),
                "consistency_pass_rate": sum(
                    row.consistency_ok for row in values
                )
                / len(values),
            }
        )
    return output


def _binomial_tail(k: int, n: int, probability: float) -> float:
    if n <= 0 or k <= 0:
        return 1.0
    if probability <= 0.0:
        return 0.0
    if probability >= 1.0:
        return 1.0
    return min(
        1.0,
        sum(
            math.comb(n, value)
            * probability**value
            * (1.0 - probability) ** (n - value)
            for value in range(k, n + 1)
        ),
    )


def _stage(writer: AtomicResultWriter, prereg: Mapping[str, Any], stage: str) -> None:
    writer.write_json(
        "agreement_campaign_state.json",
        {
            "schema": "hydra_directional_agreement_runtime_state_v1",
            "campaign_id": prereg["campaign_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "orders": 0,
        },
    )


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise DirectionalAgreementCampaignError("project root not found")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "AGREEMENT_ENGINE_VERSION",
    "DirectionalAgreementCampaignError",
    "component_pass",
    "family_tripwire",
    "final_result",
    "load_and_verify_agreement_preregistration",
    "load_and_verify_agreement_result",
    "run_directional_agreement_campaign",
]
