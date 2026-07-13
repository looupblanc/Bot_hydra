from __future__ import annotations

import hashlib
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.research import v71_cross_clock_flow_grammar as grammar4
from hydra.research import v71_event_mechanism_grammar as grammar1
from hydra.research import v71_opportunity_density_grammar as grammar2
from hydra.research import v71_trade_size_composition as grammar6
from hydra.validation.v71_event_funnel import _minute_replay_cache
from hydra.validation.v71_power_aware_candidate_audit import (
    _replay_signals,
    _retained_walk_forward_days,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


POLICY_PATH = "WORM/v7.1-underpowered-combine-research-policy-0001-2026-07-13.json"
POLICY_SHA256 = "33193b3afaf662a7a2b1fe4bcdfb5f9aa2868f6afec55f365e5ea421cd1f3f88"
POWER_AUDIT_PATHS = (
    "reports/v7_1/power_aware_0001/v71_power_aware_candidate_audit_result.json",
    "reports/v7_1/discovery_0004/v71_cross_clock_flow_power_audit_result.json",
    "reports/v7_1/discovery_0006/v71_trade_size_composition_power_audit_result.json",
)
POWER_AUDIT_HASHES = (
    "f0eb23117b5703b3d50823365cff7cf9d37c7faeb6ce5628ca7e6c19f04c930b",
    "204b79bcc0f75b22351c638469f4be1bc84bfaf636d09c88e1462f2a67c62f67",
    "ab7fd3885e23943c4abd532f82902629f1a689e962ca4d8a1d7dc9869a5f32de",
)
G5_FUNNEL_PATH = (
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_funnel_result.json"
)
G5_FUNNEL_SHA256 = "06d9a1f5600bbe51fc516841482e406a26ab2fab49cf6e599e97311cb4a49648"
G5_TRIPWIRE_PATH = (
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_tripwire_result.json"
)
G5_TRIPWIRE_SHA256 = "ea7755aa5ab60f78298557da422d497d98467457a24a259ff3f3a9919048fc1d"
GEOMETRY_GRAMMAR = "hydra_v7_1_event_time_grammar_0003"


class V71UnderpoweredCombineSelectionError(RuntimeError):
    pass


def build_underpowered_combine_selection(
    *,
    project_root: str | Path = ".",
    output_dir: str | Path = "reports/v7_1/combine_research_0001",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, audit_rows, all_decisions = _verify_inputs(root)
    underpowered = [
        row for row in audit_rows if row["status"] == "PROMISING_UNDERPOWERED"
    ]
    if len(underpowered) != 16:
        raise V71UnderpoweredCombineSelectionError(
            "underpowered population count drift"
        )
    ledgers = _candidate_ledgers(root, underpowered)
    eligible: list[dict[str, Any]] = []
    exclusions: list[dict[str, str]] = []
    for row in underpowered:
        candidate_id = str(row["candidate_id"])
        if row["grammar_id"] == GEOMETRY_GRAMMAR:
            exclusions.append(
                {
                    "candidate_id": candidate_id,
                    "status": "GEOMETRY_ONLY_DIAGNOSTIC_ALREADY_COMPLETED",
                    "reason": "G3 permanent tripwire is GEOMETRY_ONLY",
                }
            )
            continue
        if float(row["cost_results"]["STRESS_1_5X"]["mean_net"]) <= 0.0:
            exclusions.append(
                {
                    "candidate_id": candidate_id,
                    "status": "INELIGIBLE_NONPOSITIVE_STRESSED_ECONOMICS",
                    "reason": "STRESS_1_5X mean is not positive",
                }
            )
            continue
        if float(row["best_event_removed_net"]) <= 0.0:
            exclusions.append(
                {
                    "candidate_id": candidate_id,
                    "status": "INELIGIBLE_BEST_EVENT_DEPENDENCE",
                    "reason": "best-event-removed net is not positive",
                }
            )
            continue
        if float(row["top_event_concentration"]) > 0.2:
            exclusions.append(
                {
                    "candidate_id": candidate_id,
                    "status": "INELIGIBLE_CONCENTRATION",
                    "reason": "top-event concentration exceeds frozen 0.2 cap",
                }
            )
            continue
        _verify_ledger(row, ledgers[candidate_id])
        eligible.append(dict(row))
    if len(eligible) < int(policy["selection"]["minimum_candidates"]):
        raise V71UnderpoweredCombineSelectionError(
            "fewer than three eligible underpowered candidates"
        )
    component_values = {
        "STRESS_1_5X_mean_net": [
            float(row["cost_results"]["STRESS_1_5X"]["mean_net"])
            for row in eligible
        ],
        "effective_independent_event_count": [
            float(row["effective_sample"]["effective_independent_event_count"])
            for row in eligible
        ],
        "block_stability": [_block_stability(row) for row in eligible],
        "one_minus_top_event_concentration": [
            1.0 - float(row["top_event_concentration"]) for row in eligible
        ],
        "STRESS_1_5X_total_net_as_Topstep_progress_proxy": [
            float(row["cost_results"]["STRESS_1_5X"]["net_pnl"])
            for row in eligible
        ],
    }
    percentiles = {
        key: _percentile_ranks(values)
        for key, values in component_values.items()
    }
    weights = {
        key: float(value)
        for key, value in policy["selection"][
            "component_percentile_weights"
        ].items()
    }
    scored: list[dict[str, Any]] = []
    for position, row in enumerate(eligible):
        components = {
            key: float(percentiles[key][position]) for key in weights
        }
        scored.append(
            {
                "candidate": row,
                "raw_components": {
                    key: float(component_values[key][position]) for key in weights
                },
                "percentile_components": components,
                "information_score": float(
                    sum(weights[key] * components[key] for key in weights)
                ),
            }
        )
    scored.sort(
        key=lambda row: (
            -float(row["information_score"]),
            str(row["candidate"]["candidate_id"]),
        )
    )
    selected: list[dict[str, Any]] = []
    rejected_by_distinctness: list[dict[str, Any]] = []
    maximum_correlation = float(
        policy["selection"]["greedy_distinctness"]
        ["maximum_absolute_daily_PnL_correlation"]
    )
    maximum_jaccard = float(
        policy["selection"]["greedy_distinctness"]
        ["maximum_signal_timestamp_Jaccard"]
    )
    for row in scored:
        candidate = row["candidate"]
        comparisons = [
            _behavioral_comparison(
                ledgers[str(candidate["candidate_id"])],
                ledgers[str(existing["candidate"]["candidate_id"])],
                candidate_id=str(candidate["candidate_id"]),
                other_id=str(existing["candidate"]["candidate_id"]),
            )
            for existing in selected
        ]
        same_family = any(
            existing["candidate"]["family_id"] == candidate["family_id"]
            for existing in selected
        )
        too_correlated = any(
            comparison["correlation_defined"]
            and abs(float(comparison["daily_pnl_correlation"]))
            > maximum_correlation
            for comparison in comparisons
        )
        too_overlapping = any(
            float(comparison["signal_timestamp_jaccard"]) > maximum_jaccard
            for comparison in comparisons
        )
        if same_family or too_correlated or too_overlapping:
            rejected_by_distinctness.append(
                {
                    "candidate_id": str(candidate["candidate_id"]),
                    "same_family": same_family,
                    "too_correlated": too_correlated,
                    "too_overlapping": too_overlapping,
                    "comparisons": comparisons,
                }
            )
            continue
        selected.append({**row, "comparisons_to_prior_selected": comparisons})
        if len(selected) == int(policy["selection"]["target_candidates"]):
            break
    if not (
        int(policy["selection"]["minimum_candidates"])
        <= len(selected)
        <= int(policy["selection"]["maximum_candidates"])
    ):
        raise V71UnderpoweredCombineSelectionError(
            "deterministic distinctness selection produced invalid cohort size"
        )
    selected_rows = [
        _selected_candidate_row(position + 1, row)
        for position, row in enumerate(selected)
    ]
    reconciliation = _evidence_reconciliation(all_decisions, root)
    result = {
        "schema": "hydra_v7_1_underpowered_combine_selection_manifest_v1",
        "selection_id": "hydra_v7_1_underpowered_combine_selection_0001",
        "policy_path": POLICY_PATH,
        "policy_sha256": POLICY_SHA256,
        "source_underpowered_count": len(underpowered),
        "eligible_count": len(eligible),
        "excluded_count": len(exclusions),
        "selected_count": len(selected_rows),
        "selected_candidates": selected_rows,
        "eligibility_exclusions": exclusions,
        "distinctness_rejections": rejected_by_distinctness,
        "full_score_table": [
            {
                "candidate_id": str(row["candidate"]["candidate_id"]),
                "family_id": str(row["candidate"]["family_id"]),
                "information_score": float(row["information_score"]),
                "raw_components": row["raw_components"],
                "percentile_components": row["percentile_components"],
            }
            for row in scored
        ],
        "population_reconciliation": reconciliation,
        "diagnostic_status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
        "scientific_power_gate_passed_count": 0,
        "shadow_promotion_authorized": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "contains_combine_results": False,
        "CONTRE": (
            "Selection is post-walk-forward and the weighted score can overstate "
            "differences among noisy candidates; the diagnostic cannot promote any candidate."
        ),
        "prochaine_action": "freeze_exact_selected_specs_and_run_24_start_diagnostic",
    }
    result["selection_manifest_hash"] = _stable_hash(result)
    return _write_result(result, root, Path(output_dir))


def _verify_inputs(
    root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    expected = {POLICY_PATH: POLICY_SHA256, G5_FUNNEL_PATH: G5_FUNNEL_SHA256, G5_TRIPWIRE_PATH: G5_TRIPWIRE_SHA256}
    expected.update(dict(zip(POWER_AUDIT_PATHS, POWER_AUDIT_HASHES, strict=True)))
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71UnderpoweredCombineSelectionError(
            "underpowered selection frozen input drift: " + ",".join(drift)
        )
    policy = json.loads((root / POLICY_PATH).read_text(encoding="utf-8"))
    all_rows: list[dict[str, Any]] = []
    for path in POWER_AUDIT_PATHS:
        payload = json.loads((root / path).read_text(encoding="utf-8"))
        if payload.get("verdict") != "GREEN":
            raise V71UnderpoweredCombineSelectionError(
                "power audit source is not GREEN"
            )
        all_rows.extend(dict(row) for row in payload["candidate_results"])
    if len(all_rows) != 20:
        raise V71UnderpoweredCombineSelectionError(
            "power-decision population must contain exactly 20 candidates after G6"
        )
    return policy, all_rows, all_rows


def _candidate_ledgers(
    root: Path, candidates: Sequence[Mapping[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    wanted_by_grammar: defaultdict[str, set[str]] = defaultdict(set)
    for row in candidates:
        wanted_by_grammar[str(row["grammar_id"])].add(str(row["candidate_id"]))
    outputs: dict[str, list[dict[str, Any]]] = {}
    costs = load_cost_model()
    for grammar_id, wanted in sorted(wanted_by_grammar.items()):
        if grammar_id == grammar1.GRAMMAR_ID:
            minute = grammar1.load_v71_minute_features(root)
            specs = {row.candidate_id: row for row in grammar1.candidate_specs(root)}
            signals = grammar1.generate_signal_population(
                minute, project_root=root, graveyard_path=None
            )
        elif grammar_id == grammar2.GRAMMAR_ID:
            minute = grammar2.load_v71_minute_features(root)
            specs = {row.candidate_id: row for row in grammar2.candidate_specs(root)}
            signals = grammar2.generate_signal_population(
                minute, project_root=root, graveyard_path=None
            )
        elif grammar_id == grammar4.GRAMMAR_ID:
            minute, pairs, _ = grammar4.load_cross_clock_sources(root)
            specs = {row.candidate_id: row for row in grammar4.candidate_specs(root)}
            signals = grammar4.generate_signal_population(
                minute, pairs, project_root=root, graveyard_path=None
            )
        elif grammar_id == grammar6.GRAMMAR_ID:
            minute, states, _ = grammar6.load_trade_size_composition_sources(root)
            specs = {row.candidate_id: row for row in grammar6.candidate_specs(root)}
            signals = grammar6.generate_signal_population(
                states, project_root=root, graveyard_path=None
            )
        elif grammar_id == GEOMETRY_GRAMMAR:
            continue
        else:
            raise V71UnderpoweredCombineSelectionError(
                f"unsupported candidate grammar: {grammar_id}"
            )
        retained_days = set(_retained_walk_forward_days(signals))
        replay_cache = _minute_replay_cache(minute)
        for candidate_id in wanted:
            selected_signals = [
                row for row in signals[candidate_id] if row.session_day in retained_days
            ]
            outputs[candidate_id] = _replay_signals(
                specs[candidate_id], selected_signals, replay_cache, costs
            )
    return outputs


def _verify_ledger(row: Mapping[str, Any], ledger: Sequence[Mapping[str, Any]]) -> None:
    expected_count = int(row["raw_event_count"])
    values = np.asarray(
        [float(event["net"][CostStress.STRESS_1_5X.value]) for event in ledger]
    )
    if len(values) != expected_count:
        raise V71UnderpoweredCombineSelectionError(
            f"{row['candidate_id']} ledger count drift"
        )
    expected_mean = float(row["cost_results"]["STRESS_1_5X"]["mean_net"])
    if not math.isclose(float(np.mean(values)), expected_mean, abs_tol=1.0e-10):
        raise V71UnderpoweredCombineSelectionError(
            f"{row['candidate_id']} ledger mean drift"
        )


def _block_stability(row: Mapping[str, Any]) -> float:
    stability = row["stability"]
    return float(
        np.mean(
            [
                float(stability[key]["positive_fraction"])
                for key in ("calendar_year", "month", "quarter", "contract")
            ]
        )
    )


def _percentile_ranks(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=np.float64)
    if len(array) == 1:
        return np.ones(1, dtype=np.float64)
    order = np.argsort(array, kind="stable")
    ranks = np.empty(len(array), dtype=np.float64)
    position = 0
    while position < len(array):
        end = position + 1
        while end < len(array) and array[order[end]] == array[order[position]]:
            end += 1
        average = 0.5 * (position + end - 1)
        ranks[order[position:end]] = average / (len(array) - 1)
        position = end
    return ranks


def _behavioral_comparison(
    left: Sequence[Mapping[str, Any]],
    right: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
    other_id: str,
) -> dict[str, Any]:
    left_daily = _daily_pnl(left)
    right_daily = _daily_pnl(right)
    days = sorted(set(left_daily) | set(right_daily))
    left_values = np.asarray([left_daily.get(day, 0.0) for day in days], dtype=float)
    right_values = np.asarray([right_daily.get(day, 0.0) for day in days], dtype=float)
    defined = bool(
        len(days) >= 5 and np.std(left_values) > 0.0 and np.std(right_values) > 0.0
    )
    correlation = (
        float(np.corrcoef(left_values, right_values)[0, 1]) if defined else 0.0
    )
    left_signals = {int(row["decision_ns"]) for row in left}
    right_signals = {int(row["decision_ns"]) for row in right}
    union = left_signals | right_signals
    jaccard = len(left_signals & right_signals) / max(len(union), 1)
    return {
        "candidate_id": candidate_id,
        "other_candidate_id": other_id,
        "daily_union_day_count": len(days),
        "correlation_defined": defined,
        "daily_pnl_correlation": correlation,
        "signal_timestamp_jaccard": float(jaccard),
    }


def _daily_pnl(ledger: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    daily: defaultdict[str, float] = defaultdict(float)
    for row in ledger:
        daily[str(row["session_day"])] += float(
            row["net"][CostStress.STRESS_1_5X.value]
        )
    return dict(daily)


def _selected_candidate_row(rank: int, row: Mapping[str, Any]) -> dict[str, Any]:
    candidate = row["candidate"]
    return {
        "selection_rank": rank,
        "candidate_id": str(candidate["candidate_id"]),
        "grammar_id": str(candidate["grammar_id"]),
        "family_id": str(candidate["family_id"]),
        "motif": str(candidate["motif"]),
        "direction_policy": str(candidate["direction_policy"]),
        "holding_minutes": int(candidate["holding_minutes"]),
        "specification_hash": str(candidate["specification_hash"]),
        "signal_path_hash": str(candidate["signal_path_hash"]),
        "prior_power_status": str(candidate["status"]),
        "diagnostic_status": "PROMISING_UNDERPOWERED_COMBINE_RESEARCH",
        "information_score": float(row["information_score"]),
        "raw_components": row["raw_components"],
        "percentile_components": row["percentile_components"],
        "comparisons_to_prior_selected": row["comparisons_to_prior_selected"],
        "entry_exit_parameters_changed": False,
        "position_quantity": 1,
    }


def _evidence_reconciliation(
    decisions: Sequence[Mapping[str, Any]], root: Path
) -> dict[str, Any]:
    rows = [
        {
            "candidate_id": str(row["candidate_id"]),
            "status": str(row["status"]),
            "decision_type": "CANDIDATE_SPECIFIC_POWER",
        }
        for row in decisions
    ]
    g5_funnel = json.loads((root / G5_FUNNEL_PATH).read_text(encoding="utf-8"))
    g5_positive = [
        row
        for row in g5_funnel["candidate_results"]
        if bool(row.get("walk_forward_positive"))
    ]
    if len(g5_positive) != 2:
        raise V71UnderpoweredCombineSelectionError(
            "G5 walk-forward-positive reconciliation count drift"
        )
    rows.extend(
        {
            "candidate_id": str(row["candidate_id"]),
            "status": "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT",
            "decision_type": "CLASS_LEVEL_TRIPWIRE_TERMINAL",
        }
        for row in g5_positive
    )
    if len(rows) != 22 or len({row["candidate_id"] for row in rows}) != 22:
        raise V71UnderpoweredCombineSelectionError(
            "walk-forward evidence ledger is not one-to-one"
        )
    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["status"]] += 1
    return {
        "before_G6_explanation": (
            "20 walk-forward-positive candidates yielded 18 power decisions because "
            "the two G5 positives received the explicit terminal class status "
            "GEOMETRY_ONLY_CLASS_TOMBSTONED_NO_POWER_AUDIT before power audit."
        ),
        "after_G6_walk_forward_positive_count": 22,
        "after_G6_accounted_count": len(rows),
        "unaccounted_count": 0,
        "status_counts": dict(sorted(counts.items())),
        "candidate_ledger": sorted(rows, key=lambda row: row["candidate_id"]),
    }


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_underpowered_combine_selection_manifest.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root)
        if result_path.is_relative_to(root)
        else result_path
    )
    report_path = destination / "v71_underpowered_combine_selection_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Underpowered Combine research selection",
            "",
            "[HYDRA-V7] phase=4 step=164 verdict=GREEN",
            f"gate=V71_UNDERPOWERED_COMBINE_SELECTION preuve={displayed}#{result_hash[:8]} tests=deterministic_score_plus_distinctness",
            "budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials=263770 burned=1",
            "diff_validation=hydra/validation/v71_underpowered_combine_selection.py CONTRE=selection_post_walk_forward",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Sous-puissants source: `{result['source_underpowered_count']}`",
            f"- Sélectionnés: `{result['selected_count']}`",
            f"- Non comptabilisés: `{result['population_reconciliation']['unaccounted_count']}`",
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


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71UnderpoweredCombineSelectionError",
    "build_underpowered_combine_selection",
]
