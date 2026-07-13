from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import (
    AccountEvaluationResult,
    compile_account_policy,
    evaluate_compiled_account_policy,
)
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy
from hydra.features.feature_matrix import FeatureMatrix
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.rolling_combine import EpisodeStartPolicy
from hydra.research.economic_evolution_campaign import (
    _policy_from_dict,
    _sleeve_from_dict,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.utils.time import utc_now_iso


REVIEW_SCHEMA = "hydra_economic_evolution_information_review_v1"
PREREGISTRATION_SCHEMA = (
    "hydra_economic_evolution_information_review_preregistration_v1"
)


class EconomicEvolutionInformationReviewError(RuntimeError):
    pass


def load_information_review_preregistration(path: str | Path) -> dict[str, Any]:
    prereg_path = Path(path).resolve()
    value = json.loads(prereg_path.read_text(encoding="utf-8"))
    if value.get("schema") != PREREGISTRATION_SCHEMA:
        raise EconomicEvolutionInformationReviewError(
            "unexpected information-review preregistration schema"
        )
    payload = dict(value)
    frozen_hash = str(payload.pop("preregistration_hash", ""))
    if not frozen_hash or stable_hash(payload) != frozen_hash:
        raise EconomicEvolutionInformationReviewError(
            "information-review preregistration hash drift"
        )
    if value.get("development_only") is not True:
        raise EconomicEvolutionInformationReviewError(
            "information review must remain development-only"
        )
    if value.get("status_inheritance") is not False:
        raise EconomicEvolutionInformationReviewError(
            "information review cannot inherit a promotion status"
        )
    for key in (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "network_access_allowed",
        "broker_or_orders_allowed",
        "shadow_admission_allowed",
    ):
        if value.get(key) is not False:
            raise EconomicEvolutionInformationReviewError(
                f"protected action enabled in information review: {key}"
            )
    selected = value.get("selected_policies") or []
    if not 3 <= len(selected) <= 5:
        raise EconomicEvolutionInformationReviewError(
            "information review requires three to five frozen policies"
        )
    ids = [str(row["policy_id"]) for row in selected]
    if len(ids) != len(set(ids)):
        raise EconomicEvolutionInformationReviewError(
            "information-review policy IDs must be unique"
        )
    horizon = value["horizon_policy"]
    if tuple(int(row) for row in horizon["reporting_horizons_sessions"]) != (
        20,
        40,
        60,
        90,
    ):
        raise EconomicEvolutionInformationReviewError(
            "information-review horizons must remain 20/40/60/90"
        )
    if horizon.get("include_full_available") is not True:
        raise EconomicEvolutionInformationReviewError(
            "full-available censored horizon is mandatory"
        )
    starts = tuple(int(row) for row in horizon["frozen_episode_start_days"])
    if len(starts) != 24 or len(set(starts)) != len(starts):
        raise EconomicEvolutionInformationReviewError(
            "information review requires 24 unique frozen source starts"
        )
    project_root = prereg_path.parents[2]
    for relative, digest in value["implementation_files"].items():
        candidate = project_root / str(relative)
        if not candidate.is_file() or _sha256(candidate) != str(digest):
            raise EconomicEvolutionInformationReviewError(
                f"frozen implementation drift: {relative}"
            )
    implementation_commit = str(value["implementation_commit"])
    if (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
            cwd=project_root,
            check=False,
        ).returncode
        != 0
    ):
        raise EconomicEvolutionInformationReviewError(
            "information-review implementation commit is not an ancestor"
        )
    return value


def run_economic_evolution_information_review(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    prereg_path = Path(preregistration_path).resolve()
    root = prereg_path.parents[2]
    prereg = load_information_review_preregistration(prereg_path)
    writer = AtomicResultWriter(output_dir)
    state_writer = AtomicResultWriter(output_dir, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    source = prereg["source_campaign"]
    source_files = {
        key: _resolve(root, str(relative))
        for key, relative in source["artifact_paths"].items()
    }
    for key, path in source_files.items():
        expected = str(source["artifact_sha256"][key])
        if not path.is_file() or _sha256(path) != expected:
            raise EconomicEvolutionInformationReviewError(
                f"source campaign artifact drift: {key}"
            )
    source_result = json.loads(
        source_files["campaign_result"].read_text(encoding="utf-8")
    )
    if source_result.get("campaign_id") != source["campaign_id"]:
        raise EconomicEvolutionInformationReviewError(
            "information-review source campaign ID drift"
        )
    if int(source_result["funnel"]["combine_path_candidates"]) != 0:
        raise EconomicEvolutionInformationReviewError(
            "source campaign already contains a Combine path"
        )

    source_prereg = json.loads(
        source_files["source_preregistration"].read_text(encoding="utf-8")
    )
    rolling_rows = _load_jsonl(source_files["rolling_elites"])
    rolling_by_id = {
        str(row["policy"]["policy_id"]): row for row in rolling_rows
    }
    exact_by_id = {
        str(row["sleeve_id"]): row
        for row in _load_jsonl(source_files["exact_components"])
    }
    sleeve_specs = _load_sleeve_specs(
        source_files["structural_sleeves"], source_files["seed_archive"]
    )

    frozen_policies = []
    required_sleeve_ids: set[str] = set()
    frozen_starts = tuple(
        int(row) for row in prereg["horizon_policy"]["frozen_episode_start_days"]
    )
    for frozen in prereg["selected_policies"]:
        policy_id = str(frozen["policy_id"])
        row = rolling_by_id.get(policy_id)
        if row is None:
            raise EconomicEvolutionInformationReviewError(
                f"frozen policy is absent from source elites: {policy_id}"
            )
        if stable_hash(row) != str(frozen["source_row_hash"]):
            raise EconomicEvolutionInformationReviewError(
                f"source elite row drift: {policy_id}"
            )
        policy_payload = row["policy"]
        if stable_hash(policy_payload) != str(frozen["policy_specification_hash"]):
            raise EconomicEvolutionInformationReviewError(
                f"frozen account-policy specification drift: {policy_id}"
            )
        if row.get("development_only") is not True or row.get("validated") is not False:
            raise EconomicEvolutionInformationReviewError(
                f"source policy status is not development-only: {policy_id}"
            )
        if tuple(int(day) for day in row["evaluation"]["episode_start_days"]) != frozen_starts:
            raise EconomicEvolutionInformationReviewError(
                f"source episode starts drift: {policy_id}"
            )
        policy = _policy_from_dict(policy_payload)
        expected_runtime_hashes = {
            str(key): str(value)
            for key, value in frozen["sleeve_runtime_hashes"].items()
        }
        if set(expected_runtime_hashes) != set(policy.sleeve_ids):
            raise EconomicEvolutionInformationReviewError(
                f"frozen sleeve set drift: {policy_id}"
            )
        for sleeve_id, runtime_hash in expected_runtime_hashes.items():
            if sleeve_id not in sleeve_specs or sleeve_id not in exact_by_id:
                raise EconomicEvolutionInformationReviewError(
                    f"frozen sleeve is absent: {sleeve_id}"
                )
            if str(exact_by_id[sleeve_id]["specification_hash"]) != runtime_hash:
                raise EconomicEvolutionInformationReviewError(
                    f"source runtime hash drift: {sleeve_id}"
                )
        required_sleeve_ids.update(policy.sleeve_ids)
        frozen_policies.append((frozen, policy))
    _stage(state_writer, prereg, "SOURCE_POLICIES_VERIFIED")

    feature_build = build_or_open_turbo_feature_bundles(
        cache_root=cache_root,
        contract_map_path=contract_map_path,
    )
    matrices = {
        market: FeatureMatrix.open(path, mmap=True)
        for market, path in feature_build.market_paths.items()
    }
    _verify_data_fingerprint(
        source_prereg,
        feature_build.source_fingerprint,
        contract_map_path,
        feature_build.market_paths,
    )
    selected_sleeves = tuple(
        sleeve_specs[sleeve_id] for sleeve_id in sorted(required_sleeve_ids)
    )
    bound = _bind_selected(
        selected_sleeves,
        matrices,
        policy=CheapScreenPolicy(**source_prereg["cheap_screen_policy"]),
    )
    runtimes, failures = _build_exact_runtimes(
        bound,
        matrices,
        start_inclusive=str(source_prereg["exact_replay_period"][0]),
        end_exclusive=str(source_prereg["exact_replay_period"][1]),
        worker_count=int(prereg["compute"]["exact_worker_count"]),
    )
    if failures or set(runtimes) != required_sleeve_ids:
        raise EconomicEvolutionInformationReviewError(
            f"frozen runtime reconstruction failed: {failures}"
        )
    for frozen, _policy in frozen_policies:
        for sleeve_id, expected in frozen["sleeve_runtime_hashes"].items():
            if runtimes[str(sleeve_id)].specification_hash != str(expected):
                raise EconomicEvolutionInformationReviewError(
                    f"reconstructed runtime drift: {sleeve_id}"
                )
    _stage(state_writer, prereg, "EXACT_RUNTIMES_RECONSTRUCTED")

    horizons = tuple(
        int(row) for row in prereg["horizon_policy"]["reporting_horizons_sessions"]
    )
    policy_results: list[dict[str, Any]] = []
    for frozen, policy in frozen_policies:
        compiled = compile_account_policy(policy, runtimes)
        common_days = _common_days(runtimes[key] for key in policy.sleeve_ids)
        if any(day not in common_days for day in frozen_starts):
            raise EconomicEvolutionInformationReviewError(
                f"frozen start is outside reconstructed chronology: {policy.policy_id}"
            )
        horizon_rows: dict[str, Any] = {}
        for horizon in (*horizons, None):
            duration = len(common_days) if horizon is None else int(horizon)
            episode_policy = EpisodeStartPolicy(
                maximum_starts=len(frozen_starts),
                minimum_spacing_sessions=1,
                minimum_observation_sessions=1,
                maximum_duration_sessions=duration,
                regime_balanced=False,
            )
            evaluation = evaluate_compiled_account_policy(
                compiled,
                episode_policy=episode_policy,
                explicit_start_days=frozen_starts,
                evaluate_xfa=False,
            )
            label = "full_available" if horizon is None else str(horizon)
            horizon_rows[label] = _summarize_evaluation(
                evaluation,
                requested_horizon=horizon,
                common_days=common_days,
                temporal_blocks=prereg["horizon_policy"]["temporal_blocks"],
            )
        full = horizon_rows["full_available"]
        base = full["controlled_base"]
        stress = full["controlled_stress_1_5x"]
        queue_eligible = bool(
            stress["target_reached_count"] >= int(
                prereg["next_stage_queue_policy"][
                    "minimum_stressed_full_available_passes"
                ]
            )
            and stress["net_pnl_median"]
            > float(prereg["next_stage_queue_policy"]["minimum_stressed_median_net"])
            and base["mll_breach_probability"]
            <= float(
                prereg["next_stage_queue_policy"]["maximum_base_mll_breach_rate"]
            )
            and stress["mll_breach_probability"]
            <= float(
                prereg["next_stage_queue_policy"]["maximum_stressed_mll_breach_rate"]
            )
            and stress["pass_temporal_block_count"]
            >= int(
                prereg["next_stage_queue_policy"]["minimum_stressed_pass_blocks"]
            )
        )
        policy_results.append(
            {
                "policy_id": policy.policy_id,
                "policy_specification_hash": str(
                    frozen["policy_specification_hash"]
                ),
                "selection_role": str(frozen["selection_role"]),
                "development_only": True,
                "validated": False,
                "status_inheritance": False,
                "horizons": horizon_rows,
                "diagnostic_status": (
                    "DEVELOPMENT_STRESSED_COMBINE_PATH_OBSERVED"
                    if stress["target_reached_count"] > 0
                    else (
                        "DEVELOPMENT_COMBINE_PATH_OBSERVED"
                        if base["target_reached_count"] > 0
                        else "DEVELOPMENT_NO_COMBINE_PATH"
                    )
                ),
                "expensive_validation_queue_eligible": queue_eligible,
                "promotion_status": "UNCHANGED_DEVELOPMENT_ONLY",
            }
        )
    writer.write_jsonl_batch("policy_horizon_results.jsonl", policy_results)

    eligible_ids = [
        row["policy_id"]
        for row in policy_results
        if row["expensive_validation_queue_eligible"]
    ]
    full_base_passes = sum(
        int(row["horizons"]["full_available"]["controlled_base"]["target_reached_count"])
        for row in policy_results
    )
    full_stress_passes = sum(
        int(
            row["horizons"]["full_available"]["controlled_stress_1_5x"][
                "target_reached_count"
            ]
        )
        for row in policy_results
    )
    summary = {
        "schema": REVIEW_SCHEMA,
        "review_id": prereg["review_id"],
        "completed_at_utc": utc_now_iso(),
        "preregistration_hash": prereg["preregistration_hash"],
        "source_campaign_id": source["campaign_id"],
        "source_campaign_result_sha256": source["artifact_sha256"][
            "campaign_result"
        ],
        "selected_policy_count": len(policy_results),
        "selected_policy_ids": [row["policy_id"] for row in policy_results],
        "episode_start_count_per_policy": len(frozen_starts),
        "reporting_horizons_sessions": list(horizons),
        "full_available_included": True,
        "full_available_base_pass_count": full_base_passes,
        "full_available_stressed_pass_count": full_stress_passes,
        "expensive_validation_queue_eligible_count": len(eligible_ids),
        "expensive_validation_queue_eligible_ids": eligible_ids,
        "scientific_status": (
            "DEVELOPMENT_PATH_JUSTIFIES_EXPENSIVE_VALIDATION_QUEUE"
            if eligible_ids
            else "DEVELOPMENT_TARGET_VELOCITY_REMAINS_UNRESOLVED"
        ),
        "development_only": True,
        "validated_policy_count": 0,
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "proof_window_consumed": False,
        "new_data_purchase_count": 0,
        "q4_access_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
        "CONTRE": (
            "Les politiques ont ete choisies apres observation du developpement et "
            "les 24 departs se chevauchent; un passage ici resterait diagnostique, "
            "jamais une confirmation independante."
        ),
        "policy_results_path": str(Path(output_dir) / "policy_horizon_results.jsonl"),
    }
    receipt = writer.write_json("information_review_result.json", summary)
    state_writer.write_json(
        "review_state.json",
        {
            "schema": "hydra_economic_evolution_information_review_state_v1",
            "review_id": prereg["review_id"],
            "stage": "COMPLETE",
            "completed_at_utc": utc_now_iso(),
            "result_path": str(Path(output_dir) / receipt.relative_path),
            "result_sha256": receipt.sha256,
            "orders": 0,
        },
    )
    return {**summary, "result_sha256": receipt.sha256}


def classify_account_observation(
    terminal: CombineTerminal,
    *,
    requested_horizon: int | None,
    available_sessions: int,
) -> str:
    if terminal is CombineTerminal.PASSED:
        return "TARGET_REACHED"
    if terminal is CombineTerminal.MLL_BREACH:
        return "MLL_BREACHED"
    if terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return "HARD_RULE_FAILURE"
    if terminal is not CombineTerminal.TIMEOUT:
        raise ValueError(f"unsupported account terminal: {terminal}")
    if requested_horizon is None or available_sessions < requested_horizon:
        return "DATA_CENSORED"
    return "OPERATIONAL_HORIZON_NOT_REACHED"


def _summarize_evaluation(
    evaluation: AccountEvaluationResult,
    *,
    requested_horizon: int | None,
    common_days: Sequence[int],
    temporal_blocks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "policy_id": evaluation.policy_id,
        "episode_start_days": list(evaluation.episode_start_days),
        "static_base": _summarize_account_episodes(
            evaluation.static_base.episodes,
            requested_horizon=requested_horizon,
            common_days=common_days,
            temporal_blocks=temporal_blocks,
        ),
        "controlled_base": _summarize_account_episodes(
            evaluation.controlled_base.episodes,
            requested_horizon=requested_horizon,
            common_days=common_days,
            temporal_blocks=temporal_blocks,
        ),
        "controlled_stress_1_5x": _summarize_account_episodes(
            evaluation.controlled_stress_1_5x.episodes,
            requested_horizon=requested_horizon,
            common_days=common_days,
            temporal_blocks=temporal_blocks,
        ),
        "identical_episode_starts": True,
        "development_only": True,
        "validated": False,
    }
    return output


def _summarize_account_episodes(
    episodes: Sequence[Any],
    *,
    requested_horizon: int | None,
    common_days: Sequence[int],
    temporal_blocks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    days = tuple(sorted({int(day) for day in common_days}))
    positions = {day: index for index, day in enumerate(days)}
    statuses: Counter[str] = Counter()
    episode_rows: list[dict[str, Any]] = []
    pass_blocks: set[str] = set()
    for episode in episodes:
        available = len(days) - positions[int(episode.start_day)]
        status = classify_account_observation(
            episode.terminal,
            requested_horizon=requested_horizon,
            available_sessions=available,
        )
        statuses[status] += 1
        block_id = _block_id(int(episode.start_day), temporal_blocks)
        if status == "TARGET_REACHED":
            pass_blocks.add(block_id)
        episode_rows.append(
            {
                "start_day": int(episode.start_day),
                "end_day": int(episode.end_day),
                "temporal_block": block_id,
                "requested_horizon_sessions": requested_horizon,
                "available_sessions": available,
                "observed_sessions": int(episode.eligible_days),
                "observation_status": status,
                "legacy_terminal": episode.terminal.value,
                "terminal_reason": str(episode.terminal_reason),
                "target_progress": float(episode.target_progress),
                "maximum_target_progress": float(episode.maximum_target_progress),
                "net_pnl": float(episode.net_pnl),
                "minimum_mll_buffer": float(episode.minimum_mll_buffer),
                "consistency_ok": bool(episode.consistency_ok),
                "days_to_target": episode.days_to_target,
                "accepted_events": int(episode.accepted_events),
                "skipped_events": int(episode.skipped_events),
            }
        )
    progress = np.asarray([row["target_progress"] for row in episode_rows])
    maximum_progress = np.asarray(
        [row["maximum_target_progress"] for row in episode_rows]
    )
    net = np.asarray([row["net_pnl"] for row in episode_rows])
    buffers = np.asarray([row["minimum_mll_buffer"] for row in episode_rows])
    count = len(episode_rows)
    passing_days = [
        int(row["days_to_target"])
        for row in episode_rows
        if row["days_to_target"] is not None
    ]
    return {
        "episode_count": count,
        "target_reached_count": statuses["TARGET_REACHED"],
        "target_reached_probability": statuses["TARGET_REACHED"] / count,
        "mll_breached_count": statuses["MLL_BREACHED"],
        "mll_breach_probability": statuses["MLL_BREACHED"] / count,
        "data_censored_count": statuses["DATA_CENSORED"],
        "operational_horizon_not_reached_count": statuses[
            "OPERATIONAL_HORIZON_NOT_REACHED"
        ],
        "hard_rule_failure_count": statuses["HARD_RULE_FAILURE"],
        "target_progress_p25": float(np.percentile(progress, 25)),
        "target_progress_median": float(np.median(progress)),
        "target_progress_p75": float(np.percentile(progress, 75)),
        "maximum_target_progress": float(np.max(maximum_progress)),
        "net_pnl_p25": float(np.percentile(net, 25)),
        "net_pnl_median": float(np.median(net)),
        "net_pnl_p75": float(np.percentile(net, 75)),
        "minimum_mll_buffer": float(np.min(buffers)),
        "consistency_pass_rate": float(
            np.mean([row["consistency_ok"] for row in episode_rows])
        ),
        "median_days_to_target_conditional": (
            float(np.median(passing_days)) if passing_days else None
        ),
        "pass_temporal_block_count": len(pass_blocks),
        "pass_temporal_blocks": sorted(pass_blocks),
        "observation_distribution": dict(sorted(statuses.items())),
        "episodes": episode_rows,
    }


def _load_sleeve_specs(
    structural_path: Path, seed_path: Path
) -> dict[str, Any]:
    specs: dict[str, Any] = {}
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    for row in seed["sleeves"]:
        sleeve = _sleeve_from_dict(row["specification"])
        specs[sleeve.sleeve_id] = sleeve
    for row in _load_jsonl(structural_path):
        sleeve = _sleeve_from_dict(row)
        specs[sleeve.sleeve_id] = sleeve
    return specs


def _block_id(day: int, blocks: Sequence[Mapping[str, Any]]) -> str:
    for row in blocks:
        if int(row["start_day_inclusive"]) <= day <= int(row["end_day_inclusive"]):
            return str(row["block_id"])
    raise EconomicEvolutionInformationReviewError(
        f"episode start lacks frozen temporal block: {day}"
    )


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise EconomicEvolutionInformationReviewError(
                    f"non-object JSONL row at {path}:{line_number}"
                )
            rows.append(value)
    return rows


def _stage(
    writer: AtomicResultWriter,
    prereg: Mapping[str, Any],
    stage: str,
) -> None:
    writer.write_json(
        "review_state.json",
        {
            "schema": "hydra_economic_evolution_information_review_state_v1",
            "review_id": prereg["review_id"],
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "orders": 0,
        },
    )


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EconomicEvolutionInformationReviewError",
    "PREREGISTRATION_SCHEMA",
    "REVIEW_SCHEMA",
    "classify_account_observation",
    "load_information_review_preregistration",
    "run_economic_evolution_information_review",
]
