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
from hydra.research.v71_event_mechanism_grammar import V71CandidateSpec, V71Signal
from hydra.research.v71_event_time_grammar import (
    candidate_specs,
    generate_signal_population,
    load_event_time_sources,
    signal_path_hash,
)
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


TRIPWIRE_POLICY_PATH = "WORM/v7.1-event-time-tripwire-0003-2026-07-12.json"
TRIPWIRE_POLICY_SHA256 = (
    "6119d44841456f5a13798cdb4e310de9de6bed388f032b6b3dab2fc00a94229b"
)
GRAMMAR_PATH = "WORM/v7.1-event-time-grammar-0003-2026-07-12.json"
GRAMMAR_SHA256 = "df9ffd7c6c87707838f53c30e474d7477bf17532ba29bffc1baa2b2a5bd0903f"
SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery_0003/v71_event_time_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = (
    "e515a0ab84600edfd8552c46b3471f77d0ba17ad3b761cf7757d5fdaa89c736d"
)
FUNNEL_RESULT_PATH = "reports/v7_1/discovery_0003/v71_event_time_funnel_result.json"
FUNNEL_RESULT_SHA256 = (
    "22f9816aeb2bae8734571dcd84485f0ccbfdb21b4735cbe0ed11356dcbc0358b"
)
EXPECTED_GLOBAL_N_TRIALS = 263_508
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 930301, (2024, "ES"): 930401}
RANDOM_SEEDS = {(2023, "ES"): 940301, (2024, "ES"): 940401}


class V71EventTimeTripwireError(RuntimeError):
    pass


def run_event_time_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0003",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute, event, source_audit = load_event_time_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(minute, event, project_root=root)
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
        null_minute, null_event = build_event_time_null_world(
            minute, event, control=control
        )
        _verify_preserved_event_observables(event, null_event)
        null_signals = generate_signal_population(
            null_minute,
            null_event,
            project_root=root,
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
        raise V71EventTimeTripwireError("null episode count is below real")
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_1_event_time_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_event_time_tripwire_0003",
        "grammar_id": "hydra_v7_1_event_time_grammar_0003",
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
        "event_time_state_recomputation": True,
        "event_duration_and_aggressor_flow_preserved_in_price_nulls": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Event duration and aggressor flow are preserved in the price nulls; "
            "the tripwire isolates directional relation to price but cannot prove "
            "stability beyond the two available D1 calendar blocks."
        ),
        "prochaine_action": (
            "freeze_null_adjusted_event_time_baseline_and_queue_independent_confirmation"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "freeze_event_time_grammar_as_geometry_contaminated"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def build_event_time_null_world(
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
            raise V71EventTimeTripwireError(
                f"event-time null changed preserved observable: {column}"
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
        raise V71EventTimeTripwireError(
            "event-time tripwire frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71EventTimeTripwireError(
            "event-time tripwire multiplicity reservation is absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71EventTimeTripwireError("unexpected proof-window state")


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
        raise V71EventTimeTripwireError("event-time signal manifest drift")
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71EventTimeTripwireError("event-time manifest contains outcomes")
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71EventTimeTripwireError("event-time specification drift")
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V71EventTimeTripwireError("event-time signal path drift")


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_event_time_tripwire_result.json"
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
    report_path = destination / "v71_event_time_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Event-time grammar tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=142 verdict={result['verdict']}",
            f"gate=V71_G3_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=128_real_plus_384_null_paths",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_event_time_tripwire.py CONTRE=deux_blocs_D1_limitent_la_stabilite",
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
    "V71EventTimeTripwireError",
    "build_event_time_null_world",
    "run_event_time_tripwire",
]
