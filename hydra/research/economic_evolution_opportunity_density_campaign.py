from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Mapping

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_opportunity_density import (
    OPPORTUNITY_DENSITY_CLASS_ID,
    OPPORTUNITY_DENSITY_LIMITS,
    generate_opportunity_density_population,
)
from hydra.economic_evolution.account_opportunity_density_evaluation import (
    evaluate_opportunity_density_policy_pairs,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_account_timeline_campaign import (
    account_timeline_final_result,
    account_timeline_paired_tripwire,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _runtime_row,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


OPPORTUNITY_DENSITY_ENGINE_VERSION = "hydra_opportunity_density_campaign_v1"


class OpportunityDensityCampaignError(RuntimeError):
    pass


def run_opportunity_density_campaign(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    phases: dict[str, float] = {}
    phase_started = started
    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_opportunity_density_preregistration(prereg_path)
    root = _project_root(prereg_path)
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    state_writer = AtomicResultWriter(output, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    _verify_frozen_json_reference(
        root, prereg["hypothesis_worm"], semantic_key="hypothesis_hash"
    )
    _verify_frozen_json_reference(
        root, prereg["parent_terminal_verdict"], semantic_key="verdict_hash"
    )
    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    if _sha256(seed_path) != str(prereg["source_seed"]["file_sha256"]):
        raise OpportunityDensityCampaignError("source seed checksum drift")
    if seed["archive_hash"] != str(prereg["source_seed"]["archive_hash"]):
        raise OpportunityDensityCampaignError("source seed semantic hash drift")

    structural = prereg["structural_population"]
    population = generate_opportunity_density_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        policy_pair_count=int(structural["policy_pair_count"]),
        maximum_components=int(structural["component_count"]),
        minimum_component_events=int(structural["minimum_component_events"]),
        minimum_markets=int(structural["minimum_markets"]),
        minimum_sessions=int(structural["minimum_sessions"]),
    )
    if population.manifest_hash != str(structural["policy_manifest_hash"]):
        raise OpportunityDensityCampaignError("frozen density manifest drift")
    writer.write_json(
        "opportunity_density_population.json",
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
    _stage(state_writer, prereg, "OPPORTUNITY_DENSITY_POPULATION_FROZEN")
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
    if len(screen.rows) != len(sleeves):
        raise OpportunityDensityCampaignError("component screen is incomplete")
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
        raise OpportunityDensityCampaignError(
            "density replay requires every frozen component runtime"
        )
    _stage(state_writer, prereg, "EXACT_COMPONENT_REPLAY_COMPLETE")
    phases["exact_component_replay"] = time.perf_counter() - phase_started

    common_days = _common_days(list(runtimes.values()))
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(common_days, policy=episode_policy)
    if len(starts) != int(prereg["rolling_episode_policy"]["maximum_starts"]):
        raise OpportunityDensityCampaignError("incomplete frozen episode starts")

    phase_started = time.perf_counter()
    pair_rows = evaluate_opportunity_density_policy_pairs(
        population.pairs,
        runtimes,
        starts=starts,
        episode_policy=episode_policy,
        worker_count=int(prereg["compute"]["account_worker_count"]),
    )
    expected_pairs = int(structural["policy_pair_count"])
    if len(pair_rows) != expected_pairs:
        raise OpportunityDensityCampaignError("paired evaluation is incomplete")
    writer.write_jsonl_batch("opportunity_density_pair_results.jsonl", pair_rows)
    _stage(state_writer, prereg, "PAIRED_ACCOUNT_REPLAY_COMPLETE")
    phases["paired_account_replay"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    tripwire = account_timeline_paired_tripwire(
        pair_rows, prereg["family_tripwire"]
    )
    tripwire["class_id"] = OPPORTUNITY_DENSITY_CLASS_ID
    writer.write_json("family_tripwire.json", tripwire)
    _stage(state_writer, prereg, "FAMILY_TRIPWIRE_COMPLETE")
    phases["family_tripwire"] = time.perf_counter() - phase_started

    result = account_timeline_final_result(
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
    result.update(
        {
            "schema": "hydra_opportunity_density_result_v1",
            "engine_version": OPPORTUNITY_DENSITY_ENGINE_VERSION,
            "class_id": OPPORTUNITY_DENSITY_CLASS_ID,
            "next_action": (
                "SCALE_OPPORTUNITY_DENSITY_SURVIVORS"
                if int(result["combine_path_diagnostic_count"])
                or int(result["account_research_candidate_count"])
                else "TOMBSTONE_EXACT_0013_AND_CHANGE_ACCOUNT_REPRESENTATION"
            ),
            "CONTRE": str(prereg["CONTRE"]),
        }
    )
    for row in result["best_development_policies"]:
        row["status"] = str(row["status"]).replace(
            "ACCOUNT_TIMELINE", "OPPORTUNITY_DENSITY"
        )
    result["account_policy_economics"]["targeted_mutations_selected"] = (
        _targeted_mutations(
            result["account_policy_economics"][
                "economic_failure_vector_distribution"
            ]
        )
    )
    result.pop("result_sha256", None)
    result["result_sha256"] = stable_hash(result)
    writer.write_json("opportunity_density_result.json", result)
    writer.write_text(
        "opportunity_density_report.md",
        opportunity_density_report(result, prereg),
    )
    _stage(state_writer, prereg, "COMPLETE")
    return result


def load_and_verify_opportunity_density_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    resolved = Path(path).resolve()
    prereg = _load_json(resolved)
    if prereg.get("schema") == "hydra_opportunity_density_preregistration_revision_v1":
        prereg = _resolve_preregistration_revision(resolved, prereg)
    claimed = prereg.get("preregistration_hash")
    payload = dict(prereg)
    payload.pop("preregistration_hash", None)
    structural = prereg.get("structural_population") or {}
    governance = prereg.get("governance") or {}
    statuses = prereg.get("statuses") or {}
    if (
        prereg.get("schema") != "hydra_opportunity_density_preregistration_v1"
        or prereg.get("class_id") != OPPORTUNITY_DENSITY_CLASS_ID
        or stable_hash(payload) != claimed
        or int(structural.get("policy_pair_count", -1)) != 512
        or int(structural.get("component_count", -1)) != 48
        or structural.get("same_ordered_membership_within_pair") is not True
        or structural.get("same_component_event_paths_within_pair") is not True
        or structural.get("same_graph_degree_multiset_within_pair") is not True
        or structural.get("same_graph_source_multiset_within_pair") is not True
        or structural.get("past_only_signal_observations") is not True
        or structural.get("same_class_0012_rescue") is not False
        or prereg.get("opportunity_density_policy")
        != OPPORTUNITY_DENSITY_LIMITS
        or statuses.get("validated_allowed") is not False
        or statuses.get("status_inheritance") is not False
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
    ):
        raise OpportunityDensityCampaignError("invalid density preregistration")
    root = _project_root(resolved)
    for relative, expected in prereg.get("implementation_files", {}).items():
        if _sha256(root / str(relative)) != str(expected):
            raise OpportunityDensityCampaignError(
                f"opportunity-density implementation drift: {relative}"
            )
    return prereg


def _resolve_preregistration_revision(
    revision_path: Path,
    revision: Mapping[str, Any],
) -> dict[str, Any]:
    claimed = revision.get("revision_hash")
    revision_payload = dict(revision)
    revision_payload.pop("revision_hash", None)
    if claimed != stable_hash(revision_payload):
        raise OpportunityDensityCampaignError("density revision hash drift")
    root = _project_root(revision_path)
    base_reference = revision.get("base_preregistration") or {}
    base_path = root / str(base_reference.get("path") or "")
    if _sha256(base_path) != str(base_reference.get("file_sha256") or ""):
        raise OpportunityDensityCampaignError("density base preregistration drift")
    base = _load_json(base_path)
    if base.get("preregistration_hash") != base_reference.get("semantic_hash"):
        raise OpportunityDensityCampaignError(
            "density base preregistration semantic drift"
        )
    effective = dict(base)
    effective["issued_at_utc"] = str(revision["issued_at_utc"])
    effective["source_commit"] = str(revision["source_commit"])
    effective["implementation_files"] = dict(revision["implementation_files"])
    effective["preregistration_hash"] = str(
        revision["effective_preregistration_hash"]
    )
    payload = dict(effective)
    payload.pop("preregistration_hash", None)
    if stable_hash(payload) != effective["preregistration_hash"]:
        raise OpportunityDensityCampaignError(
            "density effective revision semantic drift"
        )
    return effective


def load_and_verify_opportunity_density_result(
    path: str | Path,
    prereg: Mapping[str, Any],
) -> dict[str, Any]:
    result = _load_json(Path(path).resolve())
    claimed = result.get("result_sha256")
    payload = dict(result)
    payload.pop("result_sha256", None)
    population = result.get("population") or {}
    economics = result.get("account_policy_economics") or {}
    governance = result.get("governance") or {}
    expected_pairs = int(prereg["structural_population"]["policy_pair_count"])
    expected_episodes = expected_pairs * int(
        prereg["rolling_episode_policy"]["maximum_starts"]
    )
    if (
        result.get("schema") != "hydra_opportunity_density_result_v1"
        or result.get("campaign_id") != prereg.get("campaign_id")
        or result.get("class_id") != OPPORTUNITY_DENSITY_CLASS_ID
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
        raise OpportunityDensityCampaignError("density result integrity failure")
    return result


def _targeted_mutations(failures: Mapping[str, int]) -> list[dict[str, Any]]:
    actions = {
        "NO_COMBINE_PASS": "ADD_STRUCTURALLY_DISTINCT_TARGET_VELOCITY_MECHANISM",
        "TARGET_VELOCITY_LOW": "CHANGE_ACCOUNT_REPRESENTATION_NOT_DENSITY_THRESHOLDS",
        "MLL_BREACH_EXCESS": "ADD_CAUSAL_BUFFER_DERISK_NEW_CLASS_ONLY",
        "CONSISTENCY_FAILURE": "ADD_CAUSAL_PROFIT_SMOOTHER_NEW_CLASS_ONLY",
        "MATCHED_CONTROL_NOT_BEATEN": "TOMBSTONE_OPPORTUNITY_DENSITY_CLASS",
        "STRESSED_ECONOMICS_NONPOSITIVE": "REMOVE_COSTLY_DENSITY_ACTION",
        "TEMPORAL_BLOCK_INSTABILITY": "CHANGE_REPRESENTATION_NEW_CLASS_ONLY",
        "COMPONENT_CONCENTRATION": "ADD_DISTINCT_CAUSAL_DIVERSIFIER",
    }
    output: list[dict[str, Any]] = []
    for failure, count in sorted(
        failures.items(), key=lambda item: (-int(item[1]), item[0])
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


def opportunity_density_report(
    result: Mapping[str, Any], prereg: Mapping[str, Any]
) -> str:
    tripwire = result["family_tripwire"]
    economics = result["account_policy_economics"]
    wall = result["wall_clock_accounting"]
    return "\n".join(
        [
            f"[HYDRA-V7] phase=4 step=0013 verdict={result['report_verdict']}",
            "gate=OPPORTUNITY_DENSITY_TRIPWIRE preuve=reports/economic_evolution/opportunity_density_0013/opportunity_density_result.json#PENDING_WRITE tests=targeted/targeted",
            f"budget_llm=0/{prereg['budget']['llm_phase_max_usd']} budget_data={prereg['budget']['actual_spend_usd']}/{prereg['budget']['hard_cap_usd']} N_trials={prereg['multiplicity']['expected_global_N_trials_after_reservation']} burned={prereg['reporting']['burned_window_count']}",
            "diff_validation=hydra/economic_evolution/account_opportunity_density_evaluation.py",
            f"CONTRE={result['CONTRE']}",
            f"prochaine_action={result['next_action']}",
            "",
            "# HYDRA economic evolution — opportunity density 0013",
            "",
            f"- Verdict: `{result['scientific_status']}`",
            f"- Politiques: {result['policy_pair_evaluated_count']} réelles + contrôles appariés",
            f"- Tripwire: {tripwire['real_win_count']} réelles / {tripwire['matched_control_win_count']} contrôles",
            f"- NULL_RATIO: {tripwire['NULL_RATIO']}",
            f"- Passes Combine: {economics['policies_passing_at_least_one_combine_episode']}",
            f"- Progression médiane: {economics['median_target_progress_distribution']['median']:.2%}",
            f"- Progression maximale: {economics['maximum_target_progress']:.2%}",
            f"- MLL médiane/max: {economics['mll_breach_rate_distribution']['median']:.2%}/{economics['mll_breach_rate_distribution']['maximum']:.2%}",
            f"- Recherche / administration: {wall['research_percent']:.2f}% / {wall['tests_and_reporting_percent']:.2f}%",
            "- Données/Q4/ordres: 0/0/0",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )


def _verify_frozen_json_reference(
    root: Path, reference: Mapping[str, Any], *, semantic_key: str
) -> None:
    path = root / str(reference["path"])
    if _sha256(path) != str(reference["file_sha256"]):
        raise OpportunityDensityCampaignError(f"frozen JSON drift: {path}")
    if _load_json(path).get(semantic_key) != reference["semantic_hash"]:
        raise OpportunityDensityCampaignError(f"frozen semantic drift: {path}")


def _stage(
    writer: AtomicResultWriter, prereg: Mapping[str, Any], stage: str
) -> None:
    writer.write_json(
        "opportunity_density_campaign_state.json",
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
        raise OpportunityDensityCampaignError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise OpportunityDensityCampaignError(f"expected JSON object: {path}")
    return value


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise OpportunityDensityCampaignError("project root not found")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "OPPORTUNITY_DENSITY_ENGINE_VERSION",
    "OpportunityDensityCampaignError",
    "load_and_verify_opportunity_density_preregistration",
    "load_and_verify_opportunity_density_result",
    "opportunity_density_report",
    "run_opportunity_density_campaign",
]
