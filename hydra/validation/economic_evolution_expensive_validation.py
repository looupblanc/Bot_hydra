from __future__ import annotations

import hashlib
import json
import math
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.basket import (
    AccountPolicyEpisode,
    RoutedTrade,
    run_shared_account_episode,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.account_evaluation import compile_account_policy
from hydra.economic_evolution.schema import stable_hash
from hydra.economic_evolution.screen import CheapScreenPolicy
from hydra.features.feature_matrix import FeatureMatrix
from hydra.research.economic_evolution_campaign import (
    _policy_from_dict,
)
from hydra.research.economic_evolution_information_review import (
    _load_jsonl,
    _load_sleeve_specs,
)
from hydra.research.economic_evolution_pilot import (
    _bind_selected,
    _build_exact_runtimes,
    _common_days,
    _verify_data_fingerprint,
)
from hydra.research.turbo_feature_builder import build_or_open_turbo_feature_bundles
from hydra.validation.v7_phase2_multiplicity import (
    benjamini_hochberg,
    deflated_sharpe_statistics,
)
from hydra.utils.time import utc_now_iso


VALIDATION_SCHEMA = "hydra_economic_evolution_expensive_validation_v1"
PREREGISTRATION_SCHEMA = (
    "hydra_economic_evolution_expensive_validation_preregistration_v1"
)


class EconomicEvolutionExpensiveValidationError(RuntimeError):
    pass


def load_expensive_validation_preregistration(
    path: str | Path,
) -> dict[str, Any]:
    prereg_path = Path(path).resolve()
    value = json.loads(prereg_path.read_text(encoding="utf-8"))
    if value.get("schema") != PREREGISTRATION_SCHEMA:
        raise EconomicEvolutionExpensiveValidationError(
            "unexpected expensive-validation preregistration schema"
        )
    semantic = dict(value)
    frozen_hash = str(semantic.pop("preregistration_hash", ""))
    if not frozen_hash or stable_hash(semantic) != frozen_hash:
        raise EconomicEvolutionExpensiveValidationError(
            "expensive-validation preregistration hash drift"
        )
    if value.get("development_only") is not True:
        raise EconomicEvolutionExpensiveValidationError(
            "expensive validation must remain development-only"
        )
    if value.get("status_inheritance") is not False:
        raise EconomicEvolutionExpensiveValidationError(
            "expensive validation cannot inherit status"
        )
    for key in (
        "q4_access_allowed",
        "new_data_purchase_allowed",
        "network_access_allowed",
        "broker_or_orders_allowed",
        "shadow_admission_allowed",
        "proof_window_consumption_allowed",
    ):
        if value.get(key) is not False:
            raise EconomicEvolutionExpensiveValidationError(
                f"protected action enabled in expensive validation: {key}"
            )
    candidate = value.get("candidate") or {}
    if not str(candidate.get("policy_id") or ""):
        raise EconomicEvolutionExpensiveValidationError(
            "expensive validation requires one frozen policy"
        )
    if len(candidate.get("sleeve_runtime_hashes") or {}) < 2:
        raise EconomicEvolutionExpensiveValidationError(
            "frozen account policy must contain multiple sleeves"
        )
    statistics = value["statistics_policy"]
    if float(statistics["final_confirmation_power_minimum"]) != 0.80:
        raise EconomicEvolutionExpensiveValidationError(
            "final confirmation power requirement must remain 80 percent"
        )
    if float(statistics["BH_FDR_q"]) != 0.10:
        raise EconomicEvolutionExpensiveValidationError(
            "BH FDR must remain ten percent"
        )
    if int(statistics["DSR_N_trials"]) < int(statistics["BH_family_size"]):
        raise EconomicEvolutionExpensiveValidationError(
            "DSR trial count cannot be smaller than the BH selection family"
        )
    if int(statistics["bootstrap_repetitions"]) < 1_000:
        raise EconomicEvolutionExpensiveValidationError(
            "block bootstrap is too small"
        )
    if int(statistics["power_repetitions"]) < 500:
        raise EconomicEvolutionExpensiveValidationError(
            "power calibration is too small"
        )
    blocks = value["temporal_policy"]["nonoverlapping_blocks"]
    if len(blocks) < 4:
        raise EconomicEvolutionExpensiveValidationError(
            "expensive validation requires at least four frozen temporal blocks"
        )
    ordered = sorted(
        (
            int(row["start_day_inclusive"]),
            int(row["end_day_inclusive"]),
        )
        for row in blocks
    )
    if any(start > end for start, end in ordered) or any(
        ordered[index][1] >= ordered[index + 1][0]
        for index in range(len(ordered) - 1)
    ):
        raise EconomicEvolutionExpensiveValidationError(
            "validation temporal blocks must be ordered and non-overlapping"
        )
    project_root = prereg_path.parents[2]
    for relative, digest in value["implementation_files"].items():
        candidate_path = project_root / str(relative)
        if not candidate_path.is_file() or _sha256(candidate_path) != str(digest):
            raise EconomicEvolutionExpensiveValidationError(
                f"frozen implementation drift: {relative}"
            )
    implementation_commit = str(value["implementation_commit"])
    if (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
            cwd=project_root,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        != 0
    ):
        raise EconomicEvolutionExpensiveValidationError(
            "expensive-validation implementation commit is not an ancestor"
        )
    return value


def effective_independent_observations(
    values: Sequence[float], *, maximum_lag: int = 20
) -> dict[str, float | int]:
    array = np.asarray(values, dtype=np.float64)
    array = array[np.isfinite(array)]
    count = int(array.size)
    if count < 3:
        raise ValueError("effective sample size requires at least three values")
    centered = array - float(np.mean(array))
    variance = float(np.dot(centered, centered))
    if variance <= 0.0:
        return {
            "raw_observations": count,
            "effective_independent_observations": 1.0,
            "positive_autocorrelation_sum": float(count - 1),
            "maximum_lag": 0,
        }
    retained: list[float] = []
    for lag in range(1, min(int(maximum_lag), count - 1) + 1):
        rho = float(np.dot(centered[:-lag], centered[lag:]) / variance)
        if not math.isfinite(rho) or rho <= 0.0:
            break
        retained.append(min(rho, 0.999999))
    inflation = max(1.0, 1.0 + 2.0 * float(sum(retained)))
    effective = min(float(count), max(1.0, float(count) / inflation))
    return {
        "raw_observations": count,
        "effective_independent_observations": effective,
        "positive_autocorrelation_sum": float(sum(retained)),
        "maximum_lag": len(retained),
    }


def moving_block_bootstrap_means(
    values: Sequence[float],
    *,
    repetitions: int,
    block_length: int,
    seed: int,
) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or array.size < 3:
        raise ValueError("moving-block bootstrap requires a one-dimensional sample")
    if repetitions < 1 or not 1 <= block_length <= int(array.size):
        raise ValueError("invalid moving-block bootstrap configuration")
    rng = np.random.default_rng(int(seed))
    count = int(array.size)
    block_count = int(math.ceil(count / block_length))
    starts = rng.integers(0, count, size=(repetitions, block_count))
    offsets = np.arange(block_length, dtype=np.int64)
    indices = (starts[:, :, None] + offsets[None, None, :]) % count
    samples = array[indices.reshape(repetitions, -1)[:, :count]]
    return np.mean(samples, axis=1)


def calibrate_statistical_power(
    residual_daily_pnl: Sequence[float],
    *,
    minimum_useful_daily_net: float,
    repetitions: int,
    block_length: int,
    seed: int,
    dsr_n_trials: int,
    bh_family_size: int,
    bh_fdr_q: float,
) -> dict[str, Any]:
    residuals = np.asarray(residual_daily_pnl, dtype=np.float64)
    residuals = residuals[np.isfinite(residuals)]
    if residuals.size < 3:
        raise ValueError("power calibration requires at least three residuals")
    centered = residuals - float(np.mean(residuals))
    rng = np.random.default_rng(int(seed))
    count = int(centered.size)
    block_count = int(math.ceil(count / block_length))
    starts = rng.integers(0, count, size=(repetitions, block_count))
    offsets = np.arange(block_length, dtype=np.int64)
    indices = (starts[:, :, None] + offsets[None, None, :]) % count
    controls = centered[indices.reshape(repetitions, -1)[:, :count]]
    bh_rank_one_threshold = float(bh_fdr_q) / int(bh_family_size)
    null_accepts = 0
    positive_accepts = 0
    for sample in controls:
        null_dsr = deflated_sharpe_statistics(sample, n_trials=dsr_n_trials)
        positive_dsr = deflated_sharpe_statistics(
            sample + float(minimum_useful_daily_net), n_trials=dsr_n_trials
        )
        null_accepts += int(
            float(null_dsr["deflated_z"]) > 0.0
            and float(null_dsr["one_sided_p_value"])
            <= bh_rank_one_threshold
        )
        positive_accepts += int(
            float(positive_dsr["deflated_z"]) > 0.0
            and float(positive_dsr["one_sided_p_value"])
            <= bh_rank_one_threshold
        )
    return {
        "schema": "hydra_economic_evolution_power_calibration_v1",
        "repetitions": int(repetitions),
        "block_length_sessions": int(block_length),
        "minimum_useful_daily_net_usd": float(minimum_useful_daily_net),
        "DSR_N_trials": int(dsr_n_trials),
        "BH_family_size": int(bh_family_size),
        "BH_FDR_q": float(bh_fdr_q),
        "BH_rank_one_threshold": bh_rank_one_threshold,
        "null_false_positive_rate": float(null_accepts / repetitions),
        "power_on_minimum_useful_effect": float(positive_accepts / repetitions),
        "null_accept_count": int(null_accepts),
        "positive_accept_count": int(positive_accepts),
    }


def sign_invert_routed_trade(trade: RoutedTrade) -> RoutedTrade:
    event = trade.event
    cost = max(0.0, float(event.gross_pnl) - float(event.net_pnl))
    worst_gross = float(event.worst_unrealized_pnl) + cost
    best_gross = float(event.best_unrealized_pnl) + cost
    inverted = replace(
        event,
        event_id=f"{event.event_id}:SIGN_INVERSION",
        net_pnl=float(-event.gross_pnl - cost),
        gross_pnl=float(-event.gross_pnl),
        worst_unrealized_pnl=float(-best_gross - cost),
        best_unrealized_pnl=float(-worst_gross - cost),
    )
    return replace(trade, side=-trade.side, event=inverted)


def block_sign_randomization_test(
    daily_pnl: Sequence[float],
    *,
    repetitions: int,
    block_length: int,
    seed: int,
) -> dict[str, Any]:
    values = np.asarray(daily_pnl, dtype=np.float64)
    if values.ndim != 1 or values.size < block_length:
        raise ValueError("block sign test has insufficient observations")
    count = int(values.size)
    block_count = int(math.ceil(count / block_length))
    padded = np.pad(
        values,
        (0, block_count * block_length - count),
        constant_values=0.0,
    ).reshape(block_count, block_length)
    rng = np.random.default_rng(int(seed))
    signs = rng.choice(np.asarray([-1.0, 1.0]), size=(repetitions, block_count))
    null_means = np.mean(
        (signs[:, :, None] * padded[None, :, :]).reshape(repetitions, -1)[
            :, :count
        ],
        axis=1,
    )
    actual = float(np.mean(values))
    p_value = float((1 + int(np.sum(null_means >= actual))) / (repetitions + 1))
    return {
        "schema": "hydra_block_sign_randomization_v1",
        "repetitions": int(repetitions),
        "block_length_sessions": int(block_length),
        "actual_mean_daily_net": actual,
        "null_mean_p95": float(np.percentile(null_means, 95)),
        "null_mean_p99": float(np.percentile(null_means, 99)),
        "one_sided_p_value": p_value,
    }


def run_economic_evolution_expensive_validation(
    output_dir: str | Path,
    *,
    preregistration_path: str | Path,
    contract_map_path: str | Path,
    cache_root: str | Path,
) -> dict[str, Any]:
    prereg_path = Path(preregistration_path).resolve()
    root = prereg_path.parents[2]
    prereg = load_expensive_validation_preregistration(prereg_path)
    writer = AtomicResultWriter(output_dir)
    state_writer = AtomicResultWriter(output_dir, immutable=False)
    writer.write_json("preregistration_copy.json", prereg)
    _stage(state_writer, prereg, "PREREGISTRATION_VERIFIED")

    source_paths = {
        key: _resolve(root, relative)
        for key, relative in prereg["source_artifacts"]["paths"].items()
    }
    for key, path in source_paths.items():
        expected = str(prereg["source_artifacts"]["sha256"][key])
        if not path.is_file() or _sha256(path) != expected:
            raise EconomicEvolutionExpensiveValidationError(
                f"source artifact drift: {key}"
            )
    review_result = json.loads(source_paths["review_result"].read_text())
    policy_id = str(prereg["candidate"]["policy_id"])
    if review_result.get("scientific_status") != (
        "DEVELOPMENT_PATH_JUSTIFIES_EXPENSIVE_VALIDATION_QUEUE"
    ):
        raise EconomicEvolutionExpensiveValidationError(
            "source review did not justify expensive validation"
        )
    if review_result.get("expensive_validation_queue_eligible_ids") != [policy_id]:
        raise EconomicEvolutionExpensiveValidationError(
            "source review eligible candidate drift"
        )
    if int(review_result.get("validated_policy_count") or 0) != 0:
        raise EconomicEvolutionExpensiveValidationError(
            "source review cannot contain a validated policy"
        )

    source_prereg = json.loads(source_paths["campaign_preregistration"].read_text())
    rolling_rows = _load_jsonl(source_paths["rolling_elites"])
    rolling_row = next(
        (
            row
            for row in rolling_rows
            if str(row["policy"]["policy_id"]) == policy_id
        ),
        None,
    )
    if rolling_row is None:
        raise EconomicEvolutionExpensiveValidationError(
            "frozen policy is absent from source elites"
        )
    candidate = prereg["candidate"]
    if stable_hash(rolling_row) != str(candidate["source_row_hash"]):
        raise EconomicEvolutionExpensiveValidationError("source elite row drift")
    if stable_hash(rolling_row["policy"]) != str(
        candidate["policy_specification_hash"]
    ):
        raise EconomicEvolutionExpensiveValidationError(
            "frozen policy specification drift"
        )
    if rolling_row.get("development_only") is not True or rolling_row.get(
        "validated"
    ) is not False:
        raise EconomicEvolutionExpensiveValidationError(
            "frozen policy is not development-only"
        )
    policy = _policy_from_dict(rolling_row["policy"])
    runtime_hashes = {
        str(key): str(value)
        for key, value in candidate["sleeve_runtime_hashes"].items()
    }
    if set(policy.sleeve_ids) != set(runtime_hashes):
        raise EconomicEvolutionExpensiveValidationError("frozen sleeve set drift")
    exact_rows = {
        str(row["sleeve_id"]): row
        for row in _load_jsonl(source_paths["exact_components"])
    }
    for sleeve_id, digest in runtime_hashes.items():
        if sleeve_id not in exact_rows or str(
            exact_rows[sleeve_id]["specification_hash"]
        ) != digest:
            raise EconomicEvolutionExpensiveValidationError(
                f"frozen runtime drift: {sleeve_id}"
            )
    sleeve_specs = _load_sleeve_specs(
        source_paths["structural_sleeves"], source_paths["seed_archive"]
    )
    if any(sleeve_id not in sleeve_specs for sleeve_id in policy.sleeve_ids):
        raise EconomicEvolutionExpensiveValidationError(
            "frozen sleeve specification is absent"
        )
    _verify_selection_family(source_paths, prereg)
    _stage(state_writer, prereg, "SOURCE_POLICY_VERIFIED")

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
    selected_specs = tuple(sleeve_specs[row] for row in sorted(policy.sleeve_ids))
    bound = _bind_selected(
        selected_specs,
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
    if failures or set(runtimes) != set(policy.sleeve_ids):
        raise EconomicEvolutionExpensiveValidationError(
            f"frozen runtime reconstruction failed: {failures}"
        )
    for sleeve_id, digest in runtime_hashes.items():
        if runtimes[sleeve_id].specification_hash != digest:
            raise EconomicEvolutionExpensiveValidationError(
                f"reconstructed runtime hash drift: {sleeve_id}"
            )
    compiled = compile_account_policy(policy, runtimes)
    common_days = _common_days(runtimes[row] for row in policy.sleeve_ids)
    block_days = _resolve_temporal_blocks(
        common_days, prereg["temporal_policy"]["nonoverlapping_blocks"]
    )
    _stage(state_writer, prereg, "EXACT_RUNTIME_RECONSTRUCTED")

    profile_results: dict[str, Any] = {}
    profile_daily: dict[str, np.ndarray] = {}
    for profile, controller, multiplier in (
        ("CONTROLLED_BASE", compiled.controller, 1.0),
        ("CONTROLLED_STRESS_1_5X", compiled.controller, 1.5),
        ("CONTROLLED_STRESS_2X", compiled.controller, 2.0),
        ("STATIC_BASE", None, 1.0),
        ("STATIC_STRESS_1_5X", None, 1.5),
    ):
        events = _restress_component_events(compiled.component_events, multiplier)
        summary, daily = _evaluate_block_paths(
            events,
            common_days,
            basket=compiled.basket,
            controller=controller,
            block_days=block_days,
        )
        profile_results[profile] = summary
        profile_daily[profile] = daily

    inverted_events = {
        component_id: tuple(sign_invert_routed_trade(row) for row in values)
        for component_id, values in compiled.component_events.items()
    }
    inverted_events = _restress_component_events(inverted_events, 1.5)
    inverted_summary, _inverted_daily = _evaluate_block_paths(
        inverted_events,
        common_days,
        basket=compiled.basket,
        controller=compiled.controller,
        block_days=block_days,
    )

    leave_one_out: dict[str, Any] = {}
    for removed in policy.sleeve_ids:
        retained = tuple(row for row in policy.sleeve_ids if row != removed)
        subset_events = {
            key: value
            for key, value in _restress_component_events(
                compiled.component_events, 1.5
            ).items()
            if key in retained
        }
        basket = replace(
            compiled.basket,
            policy_id=f"{compiled.basket.policy_id}:WITHOUT:{removed}",
            component_ids=retained,
            component_priority=retained,
        )
        controller = replace(
            compiled.controller,
            controller_id=f"{compiled.controller.controller_id}:WITHOUT:{removed}",
            basket_policy_id=basket.policy_id,
            component_priority=retained,
        )
        summary, _daily = _evaluate_block_paths(
            subset_events,
            common_days,
            basket=basket,
            controller=controller,
            block_days=block_days,
        )
        leave_one_out[removed] = summary

    statistics_policy = prereg["statistics_policy"]
    stress_daily = profile_daily["CONTROLLED_STRESS_1_5X"]
    dsr = deflated_sharpe_statistics(
        stress_daily, n_trials=int(statistics_policy["DSR_N_trials"])
    )
    family_ids = _selection_family_ids(source_paths)
    candidate_p_values = {candidate_id: 1.0 for candidate_id in family_ids}
    candidate_p_values[policy_id] = float(dsr["one_sided_p_value"])
    bh = benjamini_hochberg(
        candidate_p_values, q=float(statistics_policy["BH_FDR_q"])
    )[policy_id]
    effective = effective_independent_observations(
        stress_daily,
        maximum_lag=int(statistics_policy["effective_sample_maximum_lag"]),
    )
    bootstrap_means = moving_block_bootstrap_means(
        stress_daily,
        repetitions=int(statistics_policy["bootstrap_repetitions"]),
        block_length=int(statistics_policy["bootstrap_block_length_sessions"]),
        seed=int(statistics_policy["bootstrap_seed"]),
    )
    bootstrap = {
        "repetitions": int(statistics_policy["bootstrap_repetitions"]),
        "block_length_sessions": int(
            statistics_policy["bootstrap_block_length_sessions"]
        ),
        "mean_daily_net": float(np.mean(stress_daily)),
        "median_daily_net": float(np.median(stress_daily)),
        "confidence_interval_95": [
            float(np.percentile(bootstrap_means, 2.5)),
            float(np.percentile(bootstrap_means, 97.5)),
        ],
        "probability_mean_net_positive": float(np.mean(bootstrap_means > 0.0)),
    }
    power = calibrate_statistical_power(
        stress_daily,
        minimum_useful_daily_net=float(
            statistics_policy["minimum_useful_daily_net_usd"]
        ),
        repetitions=int(statistics_policy["power_repetitions"]),
        block_length=int(statistics_policy["bootstrap_block_length_sessions"]),
        seed=int(statistics_policy["power_seed"]),
        dsr_n_trials=int(statistics_policy["DSR_N_trials"]),
        bh_family_size=int(statistics_policy["BH_family_size"]),
        bh_fdr_q=float(statistics_policy["BH_FDR_q"]),
    )
    sign_null = block_sign_randomization_test(
        stress_daily,
        repetitions=int(statistics_policy["sign_null_repetitions"]),
        block_length=int(statistics_policy["sign_null_block_length_sessions"]),
        seed=int(statistics_policy["sign_null_seed"]),
    )

    best_day = float(np.max(stress_daily, initial=0.0))
    positive_sum = float(stress_daily[stress_daily > 0.0].sum())
    concentration = best_day / positive_sum if positive_sum > 0.0 else 1.0
    net_without_best_day = float(np.sum(stress_daily) - best_day)
    candidate_summary = profile_results["CONTROLLED_STRESS_1_5X"]
    static_summary = profile_results["STATIC_STRESS_1_5X"]
    static_dominates = _account_summary_dominates(static_summary, candidate_summary)
    dominating_leave_one_out = [
        sleeve_id
        for sleeve_id, summary in leave_one_out.items()
        if _account_summary_dominates(summary, candidate_summary)
    ]
    gates = {
        "no_hard_rule_failure": all(
            int(row["hard_rule_failure_count"]) == 0
            for row in profile_results.values()
        ),
        "normal_net_positive": float(
            profile_results["CONTROLLED_BASE"]["pooled_net_pnl"]
        )
        > 0.0,
        "stress_1_5x_net_positive": float(candidate_summary["pooled_net_pnl"]) > 0.0,
        "stress_2x_net_positive_SIM_EXPLOIT_survived": float(
            profile_results["CONTROLLED_STRESS_2X"]["pooled_net_pnl"]
        )
        > 0.0,
        "minimum_two_positive_temporal_blocks": int(
            candidate_summary["positive_block_count"]
        )
        >= int(statistics_policy["minimum_positive_temporal_blocks"]),
        "maximum_MLL_breach_rate": float(candidate_summary["mll_breach_rate"])
        <= float(statistics_policy["maximum_MLL_breach_rate"]),
        "best_day_concentration": concentration
        <= float(statistics_policy["maximum_best_day_positive_pnl_share"]),
        "best_day_removed_net_positive": net_without_best_day > 0.0,
        "sign_inversion_advantage": float(candidate_summary["pooled_net_pnl"])
        > float(inverted_summary["pooled_net_pnl"]),
        "block_sign_null_rejected": float(sign_null["one_sided_p_value"])
        <= float(statistics_policy["relevant_null_p_value_maximum"]),
        "DSR_deflated_z_positive": float(dsr["deflated_z"]) > 0.0,
        "BH_FDR_10pct_rejected": bool(bh["rejected"]),
        "validator_null_FPR_calibrated": float(power["null_false_positive_rate"])
        <= float(statistics_policy["maximum_null_false_positive_rate"]),
        "validator_power_at_least_80pct": float(
            power["power_on_minimum_useful_effect"]
        )
        >= float(statistics_policy["final_confirmation_power_minimum"]),
        "static_control_does_not_dominate": not static_dominates,
        "no_leave_one_out_policy_dominates": not dominating_leave_one_out,
    }
    if not gates["no_hard_rule_failure"]:
        scientific_status = "EXPENSIVE_VALIDATION_HARD_INTEGRITY_FAILURE"
    elif not gates["stress_2x_net_positive_SIM_EXPLOIT_survived"]:
        scientific_status = "EXPENSIVE_VALIDATION_SIM_EXPLOIT"
    elif not gates["validator_null_FPR_calibrated"]:
        scientific_status = "EXPENSIVE_VALIDATOR_CALIBRATION_FAILURE"
    elif not gates["validator_power_at_least_80pct"]:
        scientific_status = "EXPENSIVE_VALIDATION_UNDERPOWERED"
    elif not gates["block_sign_null_rejected"] or not gates[
        "sign_inversion_advantage"
    ]:
        scientific_status = "EXPENSIVE_VALIDATION_NULL_INDISTINGUISHABLE"
    elif not gates["static_control_does_not_dominate"] or not gates[
        "no_leave_one_out_policy_dominates"
    ]:
        scientific_status = "EXPENSIVE_VALIDATION_SIMPLER_CONTROL_DOMINATES"
    elif not all(gates.values()):
        scientific_status = "EXPENSIVE_VALIDATION_FRAGILE_OR_UNSUPPORTED"
    else:
        scientific_status = (
            "DEVELOPMENT_EXPENSIVE_VALIDATION_SUPPORTED_"
            "INDEPENDENT_CONFIRMATION_REQUIRED"
        )

    statistical = {
        "DSR": dsr,
        "BH": {**bh, "family_size": len(family_ids)},
        "effective_sample": effective,
        "block_bootstrap": bootstrap,
        "power_calibration": power,
        "block_sign_randomization": sign_null,
        "best_day_positive_pnl_share": concentration,
        "net_after_best_day_removal": net_without_best_day,
    }
    writer.write_json("account_profile_results.json", profile_results)
    writer.write_json(
        "matched_controls.json",
        {
            "sign_inversion": inverted_summary,
            "static_control": static_summary,
            "static_control_dominates": static_dominates,
            "leave_one_out": leave_one_out,
            "dominating_leave_one_out_sleeves": dominating_leave_one_out,
        },
    )
    writer.write_json("statistical_validation.json", statistical)
    result = {
        "schema": VALIDATION_SCHEMA,
        "validation_id": str(prereg["validation_id"]),
        "completed_at_utc": utc_now_iso(),
        "candidate_id": policy_id,
        "candidate_specification_hash": str(
            candidate["policy_specification_hash"]
        ),
        "development_only": True,
        "validated": False,
        "status_inheritance": False,
        "scientific_status": scientific_status,
        "all_frozen_gates_passed": all(gates.values()),
        "gates": gates,
        "profile_results_path": str(Path(output_dir) / "account_profile_results.json"),
        "matched_controls_path": str(Path(output_dir) / "matched_controls.json"),
        "statistical_validation_path": str(
            Path(output_dir) / "statistical_validation.json"
        ),
        "independent_confirmation_queue_eligible": all(gates.values()),
        "pre_holdout_ready_count": 0,
        "paper_shadow_ready_count": 0,
        "proof_window_consumed": False,
        "q4_access_delta": 0,
        "new_data_purchase_count": 0,
        "broker_connections": 0,
        "orders": 0,
        "outbound_order_capability": False,
        "CONTRE": (
            "La politique a ete selectionnee sur le meme developpement; meme "
            "tous les gates verts ne constitueraient pas une preuve independante."
        ),
    }
    result["result_sha256"] = stable_hash(result)
    writer.write_json("expensive_validation_result.json", result)
    state_writer.write_json(
        "validation_state.json",
        {
            "schema": "hydra_economic_evolution_expensive_validation_state_v1",
            "validation_id": str(prereg["validation_id"]),
            "stage": "COMPLETE",
            "completed_at_utc": result["completed_at_utc"],
            "result_path": str(
                Path(output_dir) / "expensive_validation_result.json"
            ),
            "result_sha256": result["result_sha256"],
            "orders": 0,
        },
    )
    return result


def _verify_selection_family(
    paths: Mapping[str, Path], prereg: Mapping[str, Any]
) -> None:
    family_ids = _selection_family_ids(paths)
    expected = int(prereg["statistics_policy"]["BH_family_size"])
    if len(family_ids) != expected:
        raise EconomicEvolutionExpensiveValidationError(
            f"selection family size drift: {len(family_ids)} != {expected}"
        )
    policy_id = str(prereg["candidate"]["policy_id"])
    if policy_id not in family_ids:
        raise EconomicEvolutionExpensiveValidationError(
            "frozen candidate is absent from the selection family"
        )


def _selection_family_ids(paths: Mapping[str, Path]) -> tuple[str, ...]:
    ids: set[str] = set()
    for key in ("predecessor_account_policies", "source_account_policies"):
        for row in _load_jsonl(paths[key]):
            ids.add(str(row["policy"]["policy_id"]))
    return tuple(sorted(ids))


def _resolve_temporal_blocks(
    eligible_days: Sequence[int], blocks: Sequence[Mapping[str, Any]]
) -> dict[str, tuple[int, ...]]:
    eligible = tuple(sorted({int(row) for row in eligible_days}))
    output: dict[str, tuple[int, ...]] = {}
    consumed: set[int] = set()
    for raw in blocks:
        block_id = str(raw["block_id"])
        start = int(raw["start_day_inclusive"])
        end = int(raw["end_day_inclusive"])
        days = tuple(day for day in eligible if start <= day <= end)
        if len(days) < 10 or days[0] != start or days[-1] != end:
            raise EconomicEvolutionExpensiveValidationError(
                f"temporal block is incomplete: {block_id}"
            )
        if consumed.intersection(days):
            raise EconomicEvolutionExpensiveValidationError(
                f"temporal block overlaps a prior block: {block_id}"
            )
        consumed.update(days)
        output[block_id] = days
    return output


def _evaluate_block_paths(
    component_events: Mapping[str, Sequence[RoutedTrade]],
    eligible_days: Sequence[int],
    *,
    basket: Any,
    controller: Any,
    block_days: Mapping[str, Sequence[int]],
) -> tuple[dict[str, Any], np.ndarray]:
    episodes: list[AccountPolicyEpisode] = []
    daily_values: list[float] = []
    block_rows: list[dict[str, Any]] = []
    for block_id, days in block_days.items():
        episode = run_shared_account_episode(
            component_events,
            eligible_days,
            basket=basket,
            controller=controller,
            start_day=int(days[0]),
            maximum_duration_days=len(days),
        )
        episodes.append(episode)
        daily = [float(row["day_pnl"]) for row in episode.daily_path]
        daily_values.extend(daily)
        block_rows.append(
            {
                "block_id": block_id,
                "start_day": int(days[0]),
                "end_day": int(days[-1]),
                "available_sessions": len(days),
                "observed_sessions": len(daily),
                "terminal": episode.terminal.value,
                "terminal_reason": episode.terminal_reason,
                "net_pnl": episode.net_pnl,
                "target_progress": episode.target_progress,
                "minimum_mll_buffer": episode.minimum_mll_buffer,
                "mll_breached": episode.mll_breached,
                "consistency_ok": episode.consistency_ok,
                "best_day_concentration": episode.best_day_concentration,
                "accepted_events": episode.accepted_events,
                "skipped_events": episode.skipped_events,
                "total_cost": episode.total_cost,
            }
        )
    daily_array = np.asarray(daily_values, dtype=np.float64)
    net = np.asarray([row["net_pnl"] for row in block_rows], dtype=np.float64)
    progress = np.asarray(
        [row["target_progress"] for row in block_rows], dtype=np.float64
    )
    minimum_buffer = min(float(row["minimum_mll_buffer"]) for row in block_rows)
    return (
        {
            "block_count": len(block_rows),
            "daily_observation_count": int(daily_array.size),
            "pooled_net_pnl": float(np.sum(net)),
            "median_block_net_pnl": float(np.median(net)),
            "positive_block_count": int(np.sum(net > 0.0)),
            "target_progress_median": float(np.median(progress)),
            "target_progress_maximum": float(np.max(progress)),
            "minimum_mll_buffer": minimum_buffer,
            "mll_breach_count": int(
                sum(bool(row["mll_breached"]) for row in block_rows)
            ),
            "mll_breach_rate": float(
                np.mean([bool(row["mll_breached"]) for row in block_rows])
            ),
            "consistency_pass_rate": float(
                np.mean([bool(row["consistency_ok"]) for row in block_rows])
            ),
            "hard_rule_failure_count": int(
                sum(row["terminal"] == "COMPLIANCE_FAILURE" for row in block_rows)
            ),
            "block_results": block_rows,
        },
        daily_array,
    )


def _account_summary_dominates(
    challenger: Mapping[str, Any], incumbent: Mapping[str, Any]
) -> bool:
    comparisons = (
        float(challenger["pooled_net_pnl"])
        >= float(incumbent["pooled_net_pnl"]),
        float(challenger["target_progress_median"])
        >= float(incumbent["target_progress_median"]),
        float(challenger["mll_breach_rate"])
        <= float(incumbent["mll_breach_rate"]),
        float(challenger["minimum_mll_buffer"])
        >= float(incumbent["minimum_mll_buffer"]),
        float(challenger["consistency_pass_rate"])
        >= float(incumbent["consistency_pass_rate"]),
    )
    if not all(comparisons):
        return False
    strict = (
        float(challenger["pooled_net_pnl"])
        > float(incumbent["pooled_net_pnl"]),
        float(challenger["target_progress_median"])
        > float(incumbent["target_progress_median"]),
        float(challenger["mll_breach_rate"])
        < float(incumbent["mll_breach_rate"]),
        float(challenger["minimum_mll_buffer"])
        > float(incumbent["minimum_mll_buffer"]),
        float(challenger["consistency_pass_rate"])
        > float(incumbent["consistency_pass_rate"]),
    )
    return any(strict)


def _restress_component_events(
    values: Mapping[str, Sequence[RoutedTrade]], multiplier: float
) -> dict[str, tuple[RoutedTrade, ...]]:
    if multiplier < 1.0:
        raise ValueError("cost stress cannot be below one")
    output: dict[str, tuple[RoutedTrade, ...]] = {}
    for component_id, events in values.items():
        stressed: list[RoutedTrade] = []
        for trade in events:
            event = trade.event
            cost = max(0.0, float(event.gross_pnl) - float(event.net_pnl))
            extra = (float(multiplier) - 1.0) * cost
            stressed.append(
                replace(
                    trade,
                    event=replace(
                        event,
                        event_id=f"{event.event_id}:VALIDATION_COST_{multiplier:g}",
                        net_pnl=float(event.net_pnl - extra),
                        worst_unrealized_pnl=float(
                            event.worst_unrealized_pnl - extra
                        ),
                        best_unrealized_pnl=float(
                            event.best_unrealized_pnl - extra
                        ),
                    ),
                )
            )
        output[component_id] = tuple(stressed)
    return output


def _stage(
    writer: AtomicResultWriter, prereg: Mapping[str, Any], stage: str
) -> None:
    writer.write_json(
        "validation_state.json",
        {
            "schema": "hydra_economic_evolution_expensive_validation_state_v1",
            "validation_id": str(prereg["validation_id"]),
            "stage": stage,
            "updated_at_utc": utc_now_iso(),
            "orders": 0,
        },
    )


def _resolve(root: Path, relative: str | Path) -> Path:
    candidate = Path(relative)
    return candidate if candidate.is_absolute() else root / candidate


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EconomicEvolutionExpensiveValidationError",
    "PREREGISTRATION_SCHEMA",
    "VALIDATION_SCHEMA",
    "block_sign_randomization_test",
    "calibrate_statistical_power",
    "effective_independent_observations",
    "load_expensive_validation_preregistration",
    "moving_block_bootstrap_means",
    "run_economic_evolution_expensive_validation",
    "sign_invert_routed_trade",
]
