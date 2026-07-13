from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research import v71_event_mechanism_grammar as grammar1
from hydra.research import v71_event_time_grammar as grammar3
from hydra.research import v71_opportunity_density_grammar as grammar2
from hydra.validation.v71_event_funnel import _folds, _minute_replay_cache
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
FREEZE_PATH = "WORM/v7.1-power-aware-candidate-freeze-0001-2026-07-12.json"
FREEZE_SHA256 = "b66e462989213356106f0cbcd88d31ba4547a61f9900eb1de3e6010cb3d35d83"
CALIBRATION_PATH = (
    "reports/v7_1/power_aware_0001/"
    "v71_candidate_specific_power_calibration_result.json"
)
CALIBRATION_SHA256 = (
    "edd3bcdb2ec56bcef2830be7783d74df02041a57b4234b76c1c1803e40b647f5"
)
FUNNEL_HASHES = {
    "reports/v7_1/discovery/v71_development_funnel_result.json": (
        "b8767eb9a2c5a8f9ef7c85d640cf5b1368f2607f49da3cc0b0c9a92a73f16fe2"
    ),
    "reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json": (
        "2a45c4da55875f90438cd6cb19f1ce79ec8de7d934f7a442e78000364aff5897"
    ),
    "reports/v7_1/discovery_0003/v71_event_time_funnel_result.json": (
        "22f9816aeb2bae8734571dcd84485f0ccbfdb21b4735cbe0ed11356dcbc0358b"
    ),
}
EXPECTED_GLOBAL_N_TRIALS = 263_604
POINT_VALUE = 50.0
CRITICAL_Z = 1.6448536269514722


class V71PowerAwareAuditError(RuntimeError):
    pass


def run_power_aware_candidate_audit(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/power_aware_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, freeze = _verify_inputs(root, proof_registry_path)
    ledgers, frozen_results = _load_frozen_ledgers(root, freeze)
    rows: list[dict[str, Any]] = []
    for candidate in freeze["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        events = ledgers[candidate_id]
        result = _candidate_diagnostics(
            candidate,
            events,
            policy,
            frozen_results[candidate_id],
        )
        rows.append(result)
    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row["status"])] += 1
    powered = [
        str(row["candidate_id"])
        for row in rows
        if row["status"] == "POWERED_WF_POSITIVE"
    ]
    rolling = [
        str(row["candidate_id"])
        for row in rows
        if bool(row["rolling_combine_research_eligible"])
    ]
    diagnostic = [
        str(row["candidate_id"])
        for row in rows
        if bool(row["principal_named_bounded_diagnostic"])
        and not bool(row["rolling_combine_research_eligible"])
    ]
    result = {
        "schema": "hydra_v7_1_power_aware_candidate_audit_result_v1",
        "audit_id": "hydra_v7_1_power_aware_candidate_audit_0001",
        "verdict": "GREEN",
        "candidate_count": len(rows),
        "status_counts": dict(sorted(counts.items())),
        "powered_candidate_ids": powered,
        "rolling_combine_research_eligible_ids": rolling,
        "principal_named_bounded_diagnostic_ids": diagnostic,
        "candidate_results": rows,
        "universal_raw_event_threshold_used": False,
        "calibrated_candidate_specific_policy_used": True,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "rolling_combine_executed": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "CONTRE": (
            "All sixteen candidates were selected for positive walk-forward "
            "expectancy before this audit; even a powered classification is "
            "post-selection research evidence, not independent proof."
        ),
        "prochaine_action": (
            "preregister_and_run_bounded_rolling_combine_for_powered_and_principal_named_diagnostics"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _load_frozen_ledgers(
    root: Path, freeze: Mapping[str, Any]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    candidate_ids = {str(row["candidate_id"]) for row in freeze["candidates"]}
    frozen_results: dict[str, dict[str, Any]] = {}
    for path in FUNNEL_HASHES:
        payload = json.loads((root / path).read_text(encoding="utf-8"))
        for row in payload["candidate_results"]:
            if bool(row.get("walk_forward_positive")):
                frozen_results[str(row["candidate_id"])] = row
    if set(frozen_results) != candidate_ids:
        raise V71PowerAwareAuditError("frozen walk-forward candidate set drift")

    minute1 = grammar1.load_v71_minute_features(root)
    replay_cache = _minute_replay_cache(minute1)
    bundles: dict[str, tuple[dict[str, Any], dict[str, Sequence[Any]], tuple[str, ...]]] = {}

    specs1 = {row.candidate_id: row for row in grammar1.candidate_specs(root)}
    signals1 = grammar1.generate_signal_population(
        minute1, project_root=root, graveyard_path=None
    )
    bundles[grammar1.GRAMMAR_ID] = (
        specs1,
        signals1,
        _retained_walk_forward_days(signals1),
    )

    specs2 = {row.candidate_id: row for row in grammar2.candidate_specs(root)}
    signals2 = grammar2.generate_signal_population(
        minute1, project_root=root, graveyard_path=None
    )
    bundles[grammar2.GRAMMAR_ID] = (
        specs2,
        signals2,
        _retained_walk_forward_days(signals2),
    )

    minute3, event3, _ = grammar3.load_event_time_sources(root)
    if not np.array_equal(
        minute1["minute_start_ns"].to_numpy(np.int64),
        minute3["minute_start_ns"].to_numpy(np.int64),
    ):
        raise V71PowerAwareAuditError("minute execution sources differ across grammars")
    specs3 = {row.candidate_id: row for row in grammar3.candidate_specs(root)}
    signals3 = grammar3.generate_signal_population(
        minute3, event3, project_root=root, graveyard_path=None
    )
    bundles[grammar3.GRAMMAR_ID] = (
        specs3,
        signals3,
        _retained_walk_forward_days(signals3),
    )

    costs = load_cost_model()
    ledgers: dict[str, list[dict[str, Any]]] = {}
    for candidate in freeze["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        grammar_id = str(candidate["grammar_id"])
        specs, signals, retained_days = bundles[grammar_id]
        spec = specs[candidate_id]
        if spec.specification_hash != candidate["specification_hash"]:
            raise V71PowerAwareAuditError("candidate specification hash drift")
        selected = [
            row
            for row in signals[candidate_id]
            if row.session_day in set(retained_days)
        ]
        ledger = _replay_signals(spec, selected, replay_cache, costs)
        _verify_frozen_walk_forward_result(
            candidate_id, ledger, frozen_results[candidate_id]
        )
        ledgers[candidate_id] = ledger
    return ledgers, frozen_results


def _retained_walk_forward_days(
    signals: Mapping[str, Sequence[Any]],
) -> tuple[str, ...]:
    all_days = tuple(
        sorted(
            {
                str(signal.session_day)
                for candidate_signals in signals.values()
                for signal in candidate_signals
            }
        )
    )
    folds = _folds(all_days, 4, embargo_days=5)
    return tuple(day for fold in folds for day in fold)


def _replay_signals(
    spec: Any,
    signals: Sequence[Any],
    replay_cache: Mapping[int, Mapping[str, Any]],
    costs: Any,
) -> list[dict[str, Any]]:
    cost_by_stress = {
        stress.value: costs.round_turn_cost(
            "ES", spec.cost_horizon, stress=stress, contracts=1.0
        )
        for stress in CostStress
    }
    events: list[dict[str, Any]] = []
    for signal in signals:
        entry = replay_cache.get(int(signal.entry_minute_start_ns))
        exit_row = replay_cache.get(int(signal.exit_minute_start_ns))
        if entry is None or exit_row is None:
            raise V71PowerAwareAuditError("power audit execution timestamp missing")
        if entry["contract"] != signal.contract or exit_row["contract"] != signal.contract:
            raise V71PowerAwareAuditError("power audit explicit contract drift")
        gross = (
            (float(exit_row["open"]) - float(entry["open"]))
            * int(signal.side)
            * POINT_VALUE
        )
        timestamp = pd.Timestamp(int(signal.decision_ns), unit="ns", tz="UTC").tz_convert(
            "America/Chicago"
        )
        events.append(
            {
                "session_day": str(signal.session_day),
                "calendar_year": int(signal.calendar_year),
                "month": timestamp.strftime("%Y-%m"),
                "quarter": f"{timestamp.year}-Q{timestamp.quarter}",
                "contract": str(signal.contract),
                "decision_ns": int(signal.decision_ns),
                "entry_ns": int(signal.entry_minute_start_ns),
                "exit_ns": int(signal.exit_minute_start_ns),
                "gross_pnl": float(gross),
                "costs": dict(cost_by_stress),
                "net": {
                    stress: float(gross - cost)
                    for stress, cost in cost_by_stress.items()
                },
            }
        )
    return sorted(events, key=lambda row: (row["decision_ns"], row["exit_ns"]))


def _verify_frozen_walk_forward_result(
    candidate_id: str,
    ledger: Sequence[Mapping[str, Any]],
    frozen: Mapping[str, Any],
) -> None:
    values = np.asarray(
        [float(row["net"][CostStress.STRESS_1_5X.value]) for row in ledger]
    )
    expected = frozen["walk_forward"]
    if len(values) != int(expected["retained_event_count"]):
        raise V71PowerAwareAuditError(
            f"{candidate_id} walk-forward event count drift"
        )
    if not math.isclose(
        float(np.mean(values)),
        float(expected["pooled_expectancy_per_trade"]),
        rel_tol=0.0,
        abs_tol=1.0e-10,
    ):
        raise V71PowerAwareAuditError(
            f"{candidate_id} walk-forward expectancy drift"
        )


def _candidate_diagnostics(
    candidate: Mapping[str, Any],
    ledger: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    frozen_result: Mapping[str, Any],
) -> dict[str, Any]:
    primary_key = CostStress.STRESS_1_5X.value
    values = np.asarray([float(row["net"][primary_key]) for row in ledger])
    effective = effective_independent_events(ledger, values)
    blocks = _temporal_blocks(ledger, values)
    seed = int(policy["temporal_blocks"]["seed"]) + int(
        hashlib.sha256(str(candidate["candidate_id"]).encode()).hexdigest()[:8], 16
    )
    bootstrap = block_bootstrap_statistics(
        blocks,
        observed_mean=float(np.mean(values)),
        minimum_effect=float(policy["minimum_useful_effect"]["usd_net_per_trade"]),
        shrinkage=float(
            policy["minimum_useful_effect"]["selection_shrinkage_factor"]
        ),
        draws=int(policy["temporal_blocks"]["draws"]),
        seed=seed,
    )
    cost_results = {
        stress.value: _distribution_summary(
            np.asarray([float(row["net"][stress.value]) for row in ledger])
        )
        for stress in CostStress
    }
    stability = {
        "calendar_year": _slice_stability(ledger, values, "calendar_year"),
        "month": _slice_stability(ledger, values, "month"),
        "quarter": _slice_stability(ledger, values, "quarter"),
        "contract": _slice_stability(ledger, values, "contract"),
    }
    concentration = float(np.max(np.abs(values)) / np.sum(np.abs(values)))
    best_index = int(np.argmax(values))
    best_removed_net = float(np.sum(np.delete(values, best_index)))
    overlap = exposure_overlap_inflation(ledger)
    status, reasons = classify_power_status(
        mean=float(np.mean(values)),
        effective_events=float(effective["effective_independent_event_count"]),
        temporal_blocks=len(blocks),
        ci_lower=float(bootstrap["confidence_interval_95"][0]),
        ci_upper=float(bootstrap["confidence_interval_95"][1]),
        probability_positive=float(bootstrap["probability_net_effect_positive"]),
        power=float(bootstrap["power_at_candidate_effect"]),
        stress_2x_mean=float(cost_results[CostStress.STRESS_2X.value]["mean_net"]),
        top_event_concentration=concentration,
        best_event_removed_net=best_removed_net,
        positive_year_fraction=float(stability["calendar_year"]["positive_fraction"]),
        positive_contract_fraction=float(stability["contract"]["positive_fraction"]),
        policy=policy,
    )
    principal_named = str(candidate["candidate_id"]) in set(
        policy["rolling_combine_policy"]["principal_named_diagnostic_candidates"]
    )
    return {
        "candidate_id": candidate["candidate_id"],
        "grammar_id": candidate["grammar_id"],
        "family_id": candidate["family_id"],
        "motif": candidate["motif"],
        "direction_policy": candidate["direction_policy"],
        "holding_minutes": candidate["holding_minutes"],
        "specification_hash": candidate["specification_hash"],
        "signal_path_hash": candidate["signal_path_hash"],
        "raw_event_count": len(values),
        "effective_sample": effective,
        "event_overlap_inflation": overlap,
        "temporal_block_count": len(blocks),
        "temporal_block_ids": sorted(blocks),
        "primary_STRESS_1_5X_mean_net": float(np.mean(values)),
        "primary_STRESS_1_5X_median_net": float(np.median(values)),
        "block_bootstrap": bootstrap,
        "stability": stability,
        "top_event_concentration": concentration,
        "best_event_removed_net": best_removed_net,
        "cost_results": cost_results,
        "frozen_walk_forward_positive_fold_count": int(
            frozen_result["walk_forward"]["positive_fold_count"]
        ),
        "status": status,
        "status_reasons": reasons,
        "rolling_combine_research_eligible": status == "POWERED_WF_POSITIVE",
        "principal_named_bounded_diagnostic": principal_named,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
    }


def effective_independent_events(
    ledger: Sequence[Mapping[str, Any]], values: np.ndarray
) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64)
    count = len(values)
    maximum_lag = min(10, count // 5)
    centered = values - float(np.mean(values))
    variance = float(np.dot(centered, centered))
    correlations: list[float] = []
    for lag in range(1, maximum_lag + 1):
        if variance <= 0.0:
            rho = 0.0
        else:
            left = centered[:-lag]
            right = centered[lag:]
            denominator = math.sqrt(float(np.dot(left, left) * np.dot(right, right)))
            rho = float(np.dot(left, right) / denominator) if denominator > 0.0 else 0.0
        correlations.append(rho)
    serial_factor = max(1.0, 1.0 + 2.0 * sum(max(rho, 0.0) for rho in correlations))
    serial_effective = count / serial_factor
    overlap_factor = exposure_overlap_inflation(ledger)
    overlap_effective = count / overlap_factor
    return {
        "raw_event_count": count,
        "maximum_autocorrelation_lag": maximum_lag,
        "autocorrelations": correlations,
        "serial_dependence_factor": serial_factor,
        "serial_effective_events": serial_effective,
        "overlap_effective_events": overlap_effective,
        "effective_independent_event_count": min(
            float(count), serial_effective, overlap_effective
        ),
    }


def exposure_overlap_inflation(ledger: Sequence[Mapping[str, Any]]) -> float:
    intervals = sorted(
        (int(row["entry_ns"]), int(row["exit_ns"])) for row in ledger
    )
    total = sum(max(0, end - start) for start, end in intervals)
    if total <= 0:
        return 1.0
    union = 0
    current_start, current_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
        else:
            union += current_end - current_start
            current_start, current_end = start, end
    union += current_end - current_start
    return max(1.0, total / max(union, 1))


def _temporal_blocks(
    ledger: Sequence[Mapping[str, Any]], values: np.ndarray
) -> dict[str, np.ndarray]:
    grouped: defaultdict[str, list[float]] = defaultdict(list)
    for row, value in zip(ledger, values, strict=True):
        date = pd.Timestamp(str(row["session_day"]))
        iso = date.isocalendar()
        block_id = f"{int(iso.year)}-W{int(iso.week):02d}"
        grouped[block_id].append(float(value))
    return {
        key: np.asarray(rows, dtype=np.float64)
        for key, rows in sorted(grouped.items())
    }


def block_bootstrap_statistics(
    blocks: Mapping[str, np.ndarray],
    *,
    observed_mean: float,
    minimum_effect: float,
    shrinkage: float,
    draws: int,
    seed: int,
) -> dict[str, Any]:
    if not blocks or draws <= 0:
        raise ValueError("block bootstrap requires blocks and draws")
    by_year: defaultdict[str, list[np.ndarray]] = defaultdict(list)
    for block_id, values in sorted(blocks.items()):
        by_year[block_id.split("-W", 1)[0]].append(np.asarray(values, dtype=float))
    rng = np.random.default_rng(seed)
    actual_means = np.empty(draws, dtype=np.float64)
    centered_means = np.empty(draws, dtype=np.float64)
    centered = {
        block_id: values - observed_mean for block_id, values in blocks.items()
    }
    centered_by_year: defaultdict[str, list[np.ndarray]] = defaultdict(list)
    for block_id, values in sorted(centered.items()):
        centered_by_year[block_id.split("-W", 1)[0]].append(values)
    for draw in range(draws):
        actual_sum = centered_sum = 0.0
        count = 0
        for year in sorted(by_year):
            year_blocks = by_year[year]
            year_centered = centered_by_year[year]
            picks = rng.integers(0, len(year_blocks), size=len(year_blocks))
            for pick in picks:
                actual = year_blocks[int(pick)]
                residual = year_centered[int(pick)]
                actual_sum += float(np.sum(actual))
                centered_sum += float(np.sum(residual))
                count += len(actual)
        actual_means[draw] = actual_sum / count
        centered_means[draw] = centered_sum / count
    standard_error = float(np.std(centered_means, ddof=1))
    candidate_effect = max(float(minimum_effect), shrinkage * observed_mean)
    if standard_error <= 0.0:
        power = float(candidate_effect > 0.0)
        null_fpr = 0.0
    else:
        power = float(
            np.mean((centered_means + candidate_effect) / standard_error > CRITICAL_Z)
        )
        null_fpr = float(np.mean(centered_means / standard_error > CRITICAL_Z))
    return {
        "draws": draws,
        "seed": seed,
        "confidence_interval_95": [
            float(np.quantile(actual_means, 0.025)),
            float(np.quantile(actual_means, 0.975)),
        ],
        "probability_net_effect_positive": float(np.mean(actual_means > 0.0)),
        "bootstrap_standard_error": standard_error,
        "minimum_useful_effect_usd": minimum_effect,
        "candidate_power_effect_usd": candidate_effect,
        "power_at_candidate_effect": power,
        "estimated_null_false_positive_rate": null_fpr,
    }


def _distribution_summary(values: np.ndarray) -> dict[str, Any]:
    return {
        "event_count": len(values),
        "net_pnl": float(np.sum(values)),
        "mean_net": float(np.mean(values)),
        "median_net": float(np.median(values)),
        "positive_event_fraction": float(np.mean(values > 0.0)),
    }


def _slice_stability(
    ledger: Sequence[Mapping[str, Any]], values: np.ndarray, key: str
) -> dict[str, Any]:
    grouped: defaultdict[str, list[float]] = defaultdict(list)
    for row, value in zip(ledger, values, strict=True):
        grouped[str(row[key])].append(float(value))
    rows = {
        group: _distribution_summary(np.asarray(group_values, dtype=float))
        for group, group_values in sorted(grouped.items())
    }
    return {
        "slice_count": len(rows),
        "positive_count": sum(row["mean_net"] > 0.0 for row in rows.values()),
        "positive_fraction": (
            sum(row["mean_net"] > 0.0 for row in rows.values()) / len(rows)
            if rows
            else 0.0
        ),
        "slices": rows,
    }


def classify_power_status(
    *,
    mean: float,
    effective_events: float,
    temporal_blocks: int,
    ci_lower: float,
    ci_upper: float,
    probability_positive: float,
    power: float,
    stress_2x_mean: float,
    top_event_concentration: float,
    best_event_removed_net: float,
    positive_year_fraction: float,
    positive_contract_fraction: float,
    policy: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if mean <= 0.0 or probability_positive <= 0.5 or ci_upper <= 0.0:
        if mean <= 0.0:
            reasons.append("primary_mean_not_positive")
        if probability_positive <= 0.5:
            reasons.append("bootstrap_probability_not_above_half")
        if ci_upper <= 0.0:
            reasons.append("bootstrap_CI_upper_not_positive")
        return "WF_FALSE_POSITIVE", reasons
    fragility = policy["stability_and_fragility"]
    if stress_2x_mean <= 0.0:
        reasons.append("STRESS_2X_mean_not_positive")
    if top_event_concentration > float(
        fragility["maximum_top_event_concentration_before_fragile"]
    ):
        reasons.append("top_event_concentration_fragile")
    if best_event_removed_net <= 0.0:
        reasons.append("best_event_removal_destroys_net")
    if positive_contract_fraction < 0.5:
        reasons.append("less_than_half_contracts_positive")
    if reasons:
        return "WF_POSITIVE_BUT_FRAGILE", reasons
    powered_failures = []
    if effective_events < float(
        policy["effective_sample_size"][
            "minimum_effective_events_for_POWERED_WF_POSITIVE"
        ]
    ):
        powered_failures.append("effective_events_below_minimum")
    if temporal_blocks < int(
        policy["temporal_blocks"]["minimum_blocks_for_POWERED_WF_POSITIVE"]
    ):
        powered_failures.append("temporal_blocks_below_minimum")
    if ci_lower <= 0.0:
        powered_failures.append("bootstrap_CI_crosses_zero")
    if probability_positive < float(
        policy["temporal_blocks"]["positive_probability_threshold"]
    ):
        powered_failures.append("bootstrap_probability_below_threshold")
    if power < float(
        policy["power_estimator"]["minimum_power_for_POWERED_WF_POSITIVE"]
    ):
        powered_failures.append("candidate_power_below_threshold")
    if top_event_concentration > float(
        fragility["maximum_top_event_concentration_for_powered"]
    ):
        powered_failures.append("top_event_concentration_above_powered_limit")
    if positive_year_fraction < float(
        fragility["minimum_positive_calendar_year_fraction_for_powered"]
    ):
        powered_failures.append("not_all_calendar_years_positive")
    if positive_contract_fraction < float(
        fragility["minimum_positive_contract_fraction_for_powered"]
    ):
        powered_failures.append("contract_stability_below_powered_limit")
    if powered_failures:
        return "PROMISING_UNDERPOWERED", powered_failures
    return "POWERED_WF_POSITIVE", ["all_preregistered_power_rules_pass"]


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = {
        POLICY_PATH: POLICY_SHA256,
        FREEZE_PATH: FREEZE_SHA256,
        CALIBRATION_PATH: CALIBRATION_SHA256,
        **FUNNEL_HASHES,
    }
    drift = [
        path for path, expected_sha in expected.items() if _sha256(root / path) != expected_sha
    ]
    if drift:
        raise V71PowerAwareAuditError(
            "power-aware audit frozen input drift: " + ",".join(drift)
        )
    calibration = json.loads((root / CALIBRATION_PATH).read_text(encoding="utf-8"))
    if calibration.get("verdict") != "GREEN" or calibration.get(
        "candidate_diagnostics_read"
    ) is not False:
        raise V71PowerAwareAuditError("power calibration is not clean GREEN")
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71PowerAwareAuditError("power-aware multiplicity reservation absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71PowerAwareAuditError("unexpected proof-window state")
    return (
        json.loads((root / POLICY_PATH).read_text(encoding="utf-8")),
        json.loads((root / FREEZE_PATH).read_text(encoding="utf-8")),
    )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_power_aware_candidate_audit_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_power_aware_candidate_audit_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Power-aware candidate audit",
            "",
            f"[HYDRA-V7] phase=4 step=144 verdict={result['verdict']}",
            f"gate=V71_POWER_AWARE_CANDIDATES preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=16_frozen_candidates",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_power_aware_candidate_audit.py CONTRE=le_biais_de_selection_persiste",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Statuts: `{json.dumps(result['status_counts'], sort_keys=True)}`",
            f"- Powered: `{len(result['powered_candidate_ids'])}`",
            f"- Rolling Combine éligibles: `{len(result['rolling_combine_research_eligible_ids'])}`",
            f"- Diagnostics nommés: `{len(result['principal_named_bounded_diagnostic_ids'])}`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )
    validate_v7_report_text(report)
    report_path.write_text(report, encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = result_hash
    result["report_path"] = str(report_path)
    return result


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71PowerAwareAuditError",
    "block_bootstrap_statistics",
    "classify_power_status",
    "effective_independent_events",
    "exposure_overlap_inflation",
    "run_power_aware_candidate_audit",
]
