from __future__ import annotations

import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import hydra.economic_evolution.account_coverage_sizing_evaluation as sizing_eval
from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_complementary_sleeve_evaluation import (
    _bound_complementary_router,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy, run_ultra_cheap_screen
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.rolling_combine import EpisodeStartPolicy, select_episode_starts
from hydra.research.economic_evolution_complementary_sleeve_confirmation_campaign import (
    _generate_frozen_population,
    load_and_verify_complementary_sleeve_confirmation_preregistration,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


SOURCE_CAMPAIGN_ID = "hydra_economic_evolution_complementary_sleeve_confirmation_0018"
SOURCE_RESULT_NAME = "coverage_three_zone_result.json"
SOURCE_PAIRS_NAME = "coverage_three_zone_pair_results.jsonl"
SOURCE_POPULATION_NAME = "coverage_three_zone_population.json"
ELITE_SCHEMA = "hydra_0018_canonical_elite_manifest_v1"


class EliteRecoveryError(RuntimeError):
    pass


def run_0018_elite_recovery(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
    worm_manifest_path: str | Path,
) -> dict[str, Any]:
    source = Path(source_dir).resolve()
    output = Path(output_dir).resolve()
    writer = AtomicResultWriter(output)
    result_path = source / SOURCE_RESULT_NAME
    pair_path = source / SOURCE_PAIRS_NAME
    population_path = source / SOURCE_POPULATION_NAME
    result = _load_object(result_path)
    _verify_result_hash(result)
    pair_rows = _load_jsonl(pair_path)
    if len(pair_rows) != 512:
        raise EliteRecoveryError("0018 pair ledger must contain 512 policies")
    if len({str(row["real_policy_id"]) for row in pair_rows}) != len(pair_rows):
        raise EliteRecoveryError("0018 pair ledger contains duplicate policy IDs")

    selection = select_0018_elites(pair_rows)
    selected_ids = set(selection["selected_policy_ids"])
    selected_rows = [
        row for row in pair_rows if str(row["real_policy_id"]) in selected_ids
    ]
    if len(selected_rows) != len(selected_ids):
        raise EliteRecoveryError("0018 elite selection lost a policy")

    prereg_path = Path(preregistration_path).resolve()
    prereg = load_and_verify_complementary_sleeve_confirmation_preregistration(
        prereg_path
    )
    root = _project_root(prereg_path)
    seed_path = root / str(prereg["source_seed"]["path"])
    seed = load_and_verify_seed_archive(seed_path)
    population = _generate_frozen_population(
        seed,
        campaign_id=str(prereg["campaign_id"]),
        parent_campaign_id=str(
            prereg["structural_population"]["parent_campaign_id"]
        ),
        coverage_parent_campaign_id=str(
            prereg["structural_population"]["coverage_parent_campaign_id"]
        ),
        policy_pair_count=int(
            prereg["structural_population"]["policy_pair_count"]
        ),
        maximum_components=int(
            prereg["structural_population"]["component_count"]
        ),
        minimum_component_events=int(
            prereg["structural_population"]["minimum_component_events"]
        ),
    )
    if population.manifest_hash != result["population"]["manifest_hash"]:
        raise EliteRecoveryError("0018 regenerated population hash drift")

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
    sleeves = tuple(row.sleeve for row in population.components)
    screen_policy = CheapScreenPolicy(**prereg["cheap_screen_policy"])
    screen = run_ultra_cheap_screen(sleeves, matrices, policy=screen_policy)
    if len(screen.rows) != len(sleeves):
        raise EliteRecoveryError("0018 elite recovery screen is incomplete")
    bound = _bind_selected(sleeves, matrices, policy=screen_policy)
    runtimes, failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(prereg["exact_replay_period"][0]),
        end_exclusive=str(prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    if failures or len(runtimes) != len(sleeves):
        raise EliteRecoveryError("0018 elite exact-runtime reconstruction failed")
    episode_policy = EpisodeStartPolicy(**prereg["rolling_episode_policy"])
    starts = select_episode_starts(
        _common_days(list(runtimes.values())), policy=episode_policy
    )
    if len(starts) != 36:
        raise EliteRecoveryError("0018 elite recovery did not reproduce 36 starts")

    source_by_id = {str(row["real_policy_id"]): row for row in selected_rows}
    replay_rows: list[dict[str, Any]] = []
    policy_entries: list[dict[str, Any]] = []
    component_by_id = {
        row.sleeve.sleeve_id: row for row in population.components
    }
    for pair in sorted(population.pairs, key=lambda value: value.real_policy.policy_id):
        policy_id = pair.real_policy.policy_id
        if policy_id not in selected_ids:
            continue
        with _bound_complementary_router():
            replay = sizing_eval._evaluate_policy(  # type: ignore[attr-defined]
                pair.real_policy,
                runtimes,
                starts=starts,
                episode_policy=episode_policy,
            )
        normal = replay["normal"]
        stressed = replay["stress"]
        source_row = source_by_id[policy_id]
        _verify_replay_matches_source(source_row, normal, stressed)
        normal_episodes = [row.to_dict() for row in normal.episodes]
        stressed_by_start = {row.start_day: row for row in stressed.episodes}
        decomposition = [
            decompose_episode_failure(
                row,
                stressed_by_start[row.start_day],
                median_accepted_events=statistics.median(
                    value.accepted_events for value in normal.episodes
                ),
            )
            for row in normal.episodes
        ]
        replay_rows.append(
            {
                "policy_id": policy_id,
                "normal": normal.to_dict(include_episodes=True),
                "stressed_1_5x": stressed.to_dict(include_episodes=True),
                "failure_decomposition": decomposition,
                "failure_counts": _counts(
                    str(row["failure_cause"]) for row in decomposition
                ),
                "transition_actions": _counts(
                    str(row["highest_information_change"]) for row in decomposition
                ),
                "evidence_hash": stable_hash(
                    {
                        "normal": normal_episodes,
                        "stressed": [row.to_dict() for row in stressed.episodes],
                        "decomposition": decomposition,
                    }
                ),
            }
        )
        component_rows = [component_by_id[value] for value in pair.real_policy.component_ids]
        policy_entries.append(
            {
                "policy_id": policy_id,
                "immutable_policy_fingerprint": pair.real_policy.structural_fingerprint,
                "behavioral_fingerprint": source_row["behavioral_fingerprint"],
                "parent_policy_id": pair.parent_policy_id,
                "added_sleeve_id": pair.added_sleeve_id,
                "selection_reasons": selection["selection_reasons"][policy_id],
                "policy": pair.real_policy.to_dict(),
                "component_ids": list(pair.real_policy.component_ids),
                "component_lineages": sorted(
                    {
                        str(row.sleeve.lineage_id)
                        for row in component_rows
                    }
                ),
                "markets": sorted(
                    {str(row.sleeve.market) for row in component_rows}
                ),
                "execution_markets": sorted(
                    {
                        str(row.sleeve.execution_market)
                        for row in component_rows
                    }
                ),
                "timeframes": sorted(
                    {str(row.sleeve.timeframe) for row in component_rows}
                ),
                "sessions": sorted(
                    {int(row.sleeve.session_code) for row in component_rows}
                ),
                "normal_evidence": normal.to_dict(),
                "stressed_evidence": stressed.to_dict(),
                "paired_delta": source_row["paired_delta"],
                "temporal_blocks": source_row["real_stressed_temporal_blocks"],
                "episode_evidence_hash": replay_rows[-1]["evidence_hash"],
                "validated": False,
                "development_only": True,
                "status_inheritance": False,
            }
        )

    evidence_path = output / "elite_episode_evidence.jsonl"
    writer.write_jsonl_batch("elite_episode_evidence.jsonl", replay_rows)
    recovery_report = {
        "schema": "hydra_0018_elite_recovery_report_v1",
        "campaign_id": SOURCE_CAMPAIGN_ID,
        "completed_at_utc": utc_now_iso(),
        "selection": selection,
        "selected_policy_count": len(policy_entries),
        "passing_policy_count": len(selection["passing_policy_ids"]),
        "top_decile_both_count": len(selection["top_decile_both_policy_ids"]),
        "near_pass_policy_count": len(selection["near_pass_policy_ids"]),
        "episode_start_count": len(starts),
        "normal_episode_count": len(policy_entries) * len(starts),
        "stressed_episode_count": len(policy_entries) * len(starts),
        "source_result_sha256": _sha256(result_path),
        "source_pair_ledger_sha256": _sha256(pair_path),
        "source_population_sha256": _sha256(population_path),
        "episode_evidence_sha256": _sha256(evidence_path),
        "governance": {
            "development_only": True,
            "proof_windows_consumed": 0,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        },
        "CONTRE": (
            "Selection and failure decomposition use already-observed development "
            "outcomes from 0018. They direct development mutations but are not "
            "independent confirmation and cannot justify shadow promotion."
        ),
    }
    recovery_report["report_hash"] = stable_hash(recovery_report)
    writer.write_json("elite_recovery_report.json", recovery_report)

    manifest_payload = {
        "schema": ELITE_SCHEMA,
        "issued_at_utc": utc_now_iso(),
        "source_campaign_id": SOURCE_CAMPAIGN_ID,
        "source_population_manifest_hash": population.manifest_hash,
        "source_result": {
            "path": str(result_path.relative_to(root)),
            "file_sha256": _sha256(result_path),
            "semantic_hash": result["result_sha256"],
        },
        "source_pair_ledger": {
            "path": str(pair_path.relative_to(root)),
            "file_sha256": _sha256(pair_path),
        },
        "episode_evidence": {
            "path": str(evidence_path.relative_to(root)),
            "file_sha256": _sha256(evidence_path),
            "policy_count": len(replay_rows),
            "normal_episode_count": len(replay_rows) * len(starts),
            "stressed_episode_count": len(replay_rows) * len(starts),
        },
        "selection_policy": selection["selection_policy"],
        "passing_policy_ids": selection["passing_policy_ids"],
        "maximum_progress_policy_id": selection["maximum_progress_policy_id"],
        "top_decile_both_policy_ids": selection["top_decile_both_policy_ids"],
        "near_pass_policy_ids": selection["near_pass_policy_ids"],
        "selected_policy_count": len(policy_entries),
        "policies": policy_entries,
        "development_only": True,
        "validated": False,
        "proof_window_consumed": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "CONTRE": recovery_report["CONTRE"],
    }
    manifest_payload["manifest_hash"] = stable_hash(manifest_payload)
    _write_immutable_json(Path(worm_manifest_path).resolve(), manifest_payload)
    return manifest_payload


def select_0018_elites(pair_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not pair_rows:
        raise ValueError("0018 elite selection requires policy evidence")
    count = len(pair_rows)
    decile_count = max(1, math.ceil(count * 0.10))
    passers = {
        str(row["real_policy_id"])
        for row in pair_rows
        if int(row["real_evaluation"]["controlled_base"]["pass_count"]) > 0
    }
    normal_top = {
        str(row["real_policy_id"])
        for row in sorted(
            pair_rows,
            key=lambda value: (
                -float(
                    value["real_evaluation"]["controlled_base"][
                        "median_episode_net_pnl"
                    ]
                ),
                str(value["real_policy_id"]),
            ),
        )[:decile_count]
    }
    stress_top = {
        str(row["real_policy_id"])
        for row in sorted(
            pair_rows,
            key=lambda value: (
                -float(
                    value["real_evaluation"]["controlled_stress_1_5x"][
                        "median_episode_net_pnl"
                    ]
                ),
                str(value["real_policy_id"]),
            ),
        )[:decile_count]
    }
    top_both = normal_top & stress_top
    maximum = max(
        pair_rows,
        key=lambda value: (
            float(
                value["real_evaluation"]["controlled_base"][
                    "maximum_target_progress"
                ]
            ),
            str(value["real_policy_id"]),
        ),
    )
    nonpassers = [row for row in pair_rows if str(row["real_policy_id"]) not in passers]
    near_ranked = sorted(
        nonpassers,
        key=lambda value: (
            -_near_pass_score(value),
            str(value["real_policy_id"]),
        ),
    )
    near_pass = {str(row["real_policy_id"]) for row in near_ranked[:16]}
    selected = passers | top_both | near_pass | {str(maximum["real_policy_id"])}
    reasons: dict[str, list[str]] = {value: [] for value in selected}
    for policy_id in sorted(selected):
        if policy_id in passers:
            reasons[policy_id].append("PASSED_AT_LEAST_ONE_NORMAL_COMBINE_EPISODE")
        if policy_id in top_both:
            reasons[policy_id].append("TOP_DECILE_NORMAL_AND_STRESSED_MEDIAN_NET")
        if policy_id in near_pass:
            reasons[policy_id].append("TOP_16_MULTI_OBJECTIVE_NEAR_PASS")
        if policy_id == str(maximum["real_policy_id"]):
            reasons[policy_id].append("MAXIMUM_0018_TARGET_PROGRESS")
    return {
        "selection_policy": {
            "passing_policy_rule": "normal pass_count >= 1",
            "top_decile_rule": (
                "intersection of top ceil(10% * 512) normal and stressed median net"
            ),
            "near_pass_rule": (
                "top 16 non-passers by frozen equal-weight normalized target, "
                "stress economics, consistency, MLL safety and velocity score"
            ),
            "outcomes_already_development_only": True,
            "status_inheritance": False,
        },
        "passing_policy_ids": sorted(passers),
        "maximum_progress_policy_id": str(maximum["real_policy_id"]),
        "top_decile_both_policy_ids": sorted(top_both),
        "near_pass_policy_ids": sorted(near_pass),
        "selected_policy_ids": sorted(selected),
        "selection_reasons": reasons,
    }


def decompose_episode_failure(
    normal: Any,
    stressed: Any,
    *,
    median_accepted_events: float,
) -> dict[str, Any]:
    if normal.start_day != stressed.start_day:
        raise ValueError("normal and stressed episodes must share a start")
    if normal.passed:
        cause = "TARGET_REACHED"
        change = "FREEZE_PATH_AND_TEST_REPEATABILITY"
    elif normal.mll_breached:
        cause = "MLL_BREACH"
        change = "DERISK_CORRELATED_EXPOSURE"
    elif normal.terminal.value == "COMPLIANCE_FAILURE":
        cause = "HARD_RULE_FAILURE"
        change = "KILL_EXACT_POLICY_VERSION"
    elif normal.consistency_ok is False:
        cause = "EXCESSIVE_PROFIT_CONCENTRATION"
        change = "TEST_BOUNDED_ACCOUNT_PROFIT_SMOOTHER"
    elif normal.net_pnl > 0.0 and stressed.net_pnl <= 0.0:
        cause = "ADVERSE_COST_SENSITIVITY"
        change = "REMOVE_LOW_MARGIN_OPPORTUNITIES"
    elif normal.target_progress >= 0.90:
        cause = "PROFITABLE_BUT_TOO_SLOW"
        change = "INCREASE_QUALIFIED_OPPORTUNITY_DENSITY"
    elif normal.accepted_events < max(1.0, 0.60 * median_accepted_events):
        cause = "INSUFFICIENT_NUMBER_OF_OPPORTUNITIES"
        change = "ADD_DISTINCT_SESSION_OR_MARKET_SLEEVE"
    elif normal.maximum_target_progress >= 0.90 and normal.target_progress < 0.75:
        cause = "SEQUENCE_PATH_DEPENDENCY"
        change = "REORDER_OR_REMOVE_LATE_LOSS_CONTRIBUTOR"
    elif normal.target_progress > 0.0:
        cause = "INSUFFICIENT_PROFIT_VELOCITY"
        change = "REPLACE_LOW_CONTRIBUTION_SLEEVE"
    elif normal.accepted_events == 0:
        cause = "INACTIVITY"
        change = "ADD_ECONOMICALLY_DISTINCT_OPPORTUNITY_SOURCE"
    else:
        cause = "ADVERSE_REGIME_OR_ENTRY_SEQUENCE"
        change = "TEST_PAST_ONLY_REGIME_OR_SESSION_ROUTING"
    return {
        "start_day": int(normal.start_day),
        "failure_cause": cause,
        "highest_information_change": change,
        "terminal": normal.terminal.value,
        "net_pnl_usd": float(normal.net_pnl),
        "stressed_net_pnl_usd": float(stressed.net_pnl),
        "target_progress": float(normal.target_progress),
        "stressed_target_progress": float(stressed.target_progress),
        "maximum_target_progress": float(normal.maximum_target_progress),
        "accepted_events": int(normal.accepted_events),
        "consistency_ok": bool(normal.consistency_ok),
        "mll_breached": bool(normal.mll_breached),
        "minimum_mll_buffer": float(normal.minimum_mll_buffer),
        "days_to_target": normal.days_to_target,
        "terminal_reason": str(normal.terminal_reason),
    }


def _near_pass_score(row: Mapping[str, Any]) -> float:
    normal = row["real_evaluation"]["controlled_base"]
    stress = row["real_evaluation"]["controlled_stress_1_5x"]
    projected = normal.get("projected_days_to_target")
    velocity = 0.0 if projected in (None, 0) else min(1.0, 60.0 / float(projected))
    return statistics.fmean(
        (
            min(1.0, float(normal["maximum_target_progress"])),
            min(1.0, float(stress["maximum_target_progress"])),
            min(1.0, float(normal["target_progress_median"])),
            min(1.0, float(stress["target_progress_median"])),
            min(1.0, max(0.0, float(stress["median_episode_net_pnl"]) / 9_000.0)),
            float(normal["consistency_pass_rate"]),
            1.0 - min(1.0, float(normal["mll_breach_rate"])),
            velocity,
        )
    )


def _verify_replay_matches_source(
    source: Mapping[str, Any], normal: Any, stressed: Any
) -> None:
    expected_normal = source["real_evaluation"]["controlled_base"]
    expected_stress = source["real_evaluation"]["controlled_stress_1_5x"]
    checks = (
        (normal.pass_count, expected_normal["pass_count"]),
        (normal.mll_breach_count, expected_normal["mll_breach_count"]),
        (normal.median_episode_net_pnl, expected_normal["median_episode_net_pnl"]),
        (normal.maximum_target_progress, expected_normal["maximum_target_progress"]),
        (stressed.pass_count, expected_stress["pass_count"]),
        (stressed.median_episode_net_pnl, expected_stress["median_episode_net_pnl"]),
    )
    for actual, expected in checks:
        if not math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=1e-8):
            raise EliteRecoveryError("0018 deterministic elite replay drift")


def _verify_result_hash(result: Mapping[str, Any]) -> None:
    claimed = result.get("result_sha256")
    payload = dict(result)
    payload.pop("result_sha256", None)
    if claimed != stable_hash(payload):
        raise EliteRecoveryError("0018 result semantic hash drift")


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EliteRecoveryError(f"expected JSON object: {path}")
    return value


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise EliteRecoveryError(f"expected JSONL objects: {path}")
        output.append(value)
    return output


def _write_immutable_json(path: Path, value: Mapping[str, Any]) -> None:
    text = json.dumps(value, sort_keys=True, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise EliteRecoveryError(f"refusing divergent WORM manifest: {path}")
        return
    path.write_text(text, encoding="utf-8")


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise EliteRecoveryError("project root not found")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _counts(values: Iterable[str]) -> dict[str, int]:
    output: dict[str, int] = {}
    for value in values:
        output[value] = output.get(value, 0) + 1
    return dict(sorted(output.items()))


__all__ = [
    "EliteRecoveryError",
    "decompose_episode_failure",
    "run_0018_elite_recovery",
    "select_0018_elites",
]
