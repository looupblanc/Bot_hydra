from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v71_cross_clock_speed_leadership import (
    build_speed_leadership_transitions,
    candidate_specs,
    generate_signal_population,
    load_speed_leadership_sources,
    signal_path_hash,
)
from hydra.research.v71_event_mechanism_grammar import V71CandidateSpec, V71Signal
from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.validation.v71_opportunity_density_tripwire import (
    build_candidate_events,
    classify_tripwire,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    D1NullControl,
    _eligible_days_by_year,
    _evaluate_world,
    _within_year_price_null,
    _year_permuted_prices,
)
from hydra.validation.v7_report_schema import validate_v7_report_text
from hydra.validation.v7_tripwire_evidence import exact_tripwire_evidence


TRIPWIRE_POLICY_PATH = (
    "WORM/v7.1-cross-clock-speed-leadership-tripwire-0005-2026-07-13.json"
)
TRIPWIRE_POLICY_SHA256 = (
    "03513d2b8f9dda7bd4208581ea96d9c5ab1f8c6ca21444fc80bbfe95deb67d8d"
)
GRAMMAR_PATH = (
    "WORM/v7.1-cross-clock-speed-leadership-grammar-0005-2026-07-13.json"
)
GRAMMAR_SHA256 = "27a937a112dd4963402f8c12feb69cf9cd347b020ce47396cfffff0e253726c2"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = (
    "fdae549a4542eae64b86208d295794b6ce5e58f4f791ae5a7d262da0ac5b3032"
)
FUNNEL_RESULT_PATH = (
    "reports/v7_1/discovery_0005/"
    "v71_cross_clock_speed_leadership_funnel_result.json"
)
FUNNEL_RESULT_SHA256 = (
    "06d9a1f5600bbe51fc516841482e406a26ab2fab49cf6e599e97311cb4a49648"
)
EXPECTED_GLOBAL_N_TRIALS = 263_732
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 970301, (2024, "ES"): 970401}
RANDOM_SEEDS = {(2023, "ES"): 980301, (2024, "ES"): 980401}


class V71CrossClockSpeedLeadershipTripwireError(RuntimeError):
    pass


def run_speed_leadership_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0005",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute, real_transitions, source_audit = load_speed_leadership_sources(root)
    _, event, _ = load_event_time_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(
        minute, real_transitions, project_root=root, graveyard_path=None
    )
    _verify_signal_manifest(root, real_signals, specs)
    costs = load_cost_model()
    real_events = build_candidate_events(
        minute, real_signals, specs, costs, stress=CostStress.BASE
    )
    eligible = _eligible_days_by_year(minute)
    real = _evaluate_world(real_events, eligible)

    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute, null_event = build_speed_leadership_null_world(
            minute, event, control=control
        )
        _verify_preserved_event_observables(event, null_event)
        null_transitions, null_audit = build_speed_leadership_transitions(
            null_minute, null_event
        )
        if null_audit != source_audit:
            raise V71CrossClockSpeedLeadershipTripwireError(
                "speed-leadership null changed source eligibility"
            )
        null_signals = generate_signal_population(
            null_minute,
            null_transitions,
            project_root=root,
            graveyard_path=None,
        )
        null_events = build_candidate_events(
            null_minute,
            null_signals,
            specs,
            costs,
            stress=CostStress.BASE,
        )
        summary = _evaluate_world(null_events, eligible)
        summary["signal_count"] = sum(len(rows) for rows in null_signals.values())
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])

    verdict, null_ratio = classify_tripwire(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
    )
    if pooled_episodes < int(real["episode_count"]):
        raise V71CrossClockSpeedLeadershipTripwireError(
            "null episode count is below real"
        )
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_1_cross_clock_speed_leadership_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_cross_clock_speed_leadership_tripwire_0005",
        "grammar_id": "hydra_v7_1_cross_clock_speed_leadership_grammar_0005",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "raw_pass_counts": {
            "real": f"{int(real['pass_count'])}/{int(real['episode_count'])}",
            "null": f"{pooled_passes}/{pooled_episodes}",
        },
        "exact_binomial_test": exact.to_dict(),
        "evidence_strength": exact.evidence_strength,
        "real": {
            **real,
            "signal_count": sum(len(rows) for rows in real_signals.values()),
        },
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": pooled_passes / max(pooled_episodes, 1),
        },
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": [1, 2, 4, 8],
        "source_audit": source_audit.to_dict(),
        "speed_leadership_recomputation": True,
        "event_availability_duration_and_aggressor_flow_preserved_in_price_nulls": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Event durations and flow are preserved in the price nulls and only "
            "two D1 blocks exist; even a green result cannot establish forward persistence."
        ),
        "prochaine_action": (
            "audit_power_of_speed_leadership_walk_forward_positive_candidates"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "freeze_speed_leadership_grammar_as_geometry_contaminated"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def build_speed_leadership_null_world(
    minute: pd.DataFrame,
    event: pd.DataFrame,
    *,
    control: D1NullControl,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return (
            _year_permuted_prices(minute, "minute_start_ns"),
            _year_permuted_prices(event, "start_event_ns"),
        )
    seeds = (
        SHUFFLE_SEEDS
        if control == D1NullControl.DAILY_BLOCK_SHUFFLE
        else RANDOM_SEEDS
    )
    return (
        _within_year_price_null(
            minute,
            "minute_start_ns",
            control=control,
            seeds=seeds,
        ),
        _within_year_price_null(
            event,
            "start_event_ns",
            control=control,
            seeds=seeds,
        ),
    )


def _verify_preserved_event_observables(
    real_event: pd.DataFrame, null_event: pd.DataFrame
) -> None:
    columns = (
        "start_event_ns",
        "end_event_ns",
        "availability_ns",
        "duration_seconds",
        "signed_aggressor_volume",
        "total_volume",
        "contract",
        "bar_type",
    )
    for column in columns:
        if not real_event[column].equals(null_event[column]):
            raise V71CrossClockSpeedLeadershipTripwireError(
                f"speed-leadership null changed preserved observable: {column}"
            )


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> None:
    expected = {
        TRIPWIRE_POLICY_PATH: TRIPWIRE_POLICY_SHA256,
        GRAMMAR_PATH: GRAMMAR_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        FUNNEL_RESULT_PATH: FUNNEL_RESULT_SHA256,
    }
    drift = [
        path
        for path, expected_sha in expected.items()
        if _sha256(root / path) != expected_sha
    ]
    if drift:
        raise V71CrossClockSpeedLeadershipTripwireError(
            "speed-leadership tripwire frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71CrossClockSpeedLeadershipTripwireError(
            "speed-leadership tripwire reservation is absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71CrossClockSpeedLeadershipTripwireError(
            "unexpected proof-window state"
        )


def _verify_signal_manifest(
    root: Path,
    signals: Mapping[str, Sequence[V71Signal]],
    specs: Mapping[str, V71CandidateSpec],
) -> None:
    manifest = json.loads(
        (root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8")
    )
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs):
        raise V71CrossClockSpeedLeadershipTripwireError(
            "speed-leadership signal manifest drift"
        )
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71CrossClockSpeedLeadershipTripwireError(
            "speed-leadership manifest contains outcomes"
        )
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71CrossClockSpeedLeadershipTripwireError(
                "speed-leadership specification drift"
            )
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V71CrossClockSpeedLeadershipTripwireError(
                "speed-leadership signal path drift"
            )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_cross_clock_speed_leadership_tripwire_result.json"
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
    report_path = destination / "v71_cross_clock_speed_leadership_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Speed-leadership grammar tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=157 verdict={result['verdict']}",
            f"gate=V71_G5_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=12_real_plus_36_null_paths",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_cross_clock_speed_leadership_tripwire.py CONTRE=deux_blocs_D1_et_durees_preservees",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Réel: `{result['raw_pass_counts']['real']}`",
            f"- Null: `{result['raw_pass_counts']['null']}`",
            f"- NULL_RATIO: `{result['NULL_RATIO']}`",
            f"- P-value exacte unilatérale: `{result['exact_binomial_test']['exact_binomial_one_sided_p_value']}`",
            f"- Force: `{result['evidence_strength']}`",
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
    "V71CrossClockSpeedLeadershipTripwireError",
    "build_speed_leadership_null_world",
    "run_speed_leadership_tripwire",
]
