from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v72_flow_impact_relaxation import (
    build_flow_impact_states,
    candidate_specs,
    generate_signal_population,
    load_flow_impact_sources,
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


TRIPWIRE_POLICY_PATH = (
    "WORM/v7.2-flow-impact-relaxation-tripwire-0010-2026-07-13.json"
)
TRIPWIRE_POLICY_SHA256 = (
    "77f861b94cc6a190b86508c4adcc15b7e67c5e5af1d22e66e51110714f52741d"
)
GRAMMAR_PATH = "WORM/v7.2-flow-impact-relaxation-grammar-0010-2026-07-13.json"
GRAMMAR_SHA256 = "2513038d857e3599449fbe347bec1d4738ae2adfe9558d5daf4c2c26d322e1cd"
ADDENDUM_PATH = (
    "WORM/v7.2-flow-impact-relaxation-validation-addendum-0010-2026-07-13.json"
)
ADDENDUM_SHA256 = "d3f0505f036be27d46c6eaa712dc3f785f1902a71dd402a3ad4c9d591ae16421"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_2/discovery_0010/"
    "v72_flow_impact_relaxation_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = (
    "480b4c3434dadf0f48f46d7434caa0e392b625f59791f391d25be6f5c398ddef"
)
EXPECTED_GLOBAL_N_TRIALS = 264_947
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 7210001, (2024, "ES"): 7210001}
RANDOM_SEEDS = {(2023, "ES"): 7210002, (2024, "ES"): 7210002}


class V72FlowImpactRelaxationTripwireError(RuntimeError):
    pass


def run_flow_impact_relaxation_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_2/discovery_0010",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute, real_states, source_audit = load_flow_impact_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(
        real_states, project_root=root, graveyard_path=None
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
        null_minute = build_flow_impact_null_world(minute, control=control)
        _verify_preserved_observables(minute, null_minute)
        null_states, null_audit = build_flow_impact_states(null_minute)
        _verify_invariant_source_counts(source_audit, null_audit)
        null_signals = generate_signal_population(
            null_states, project_root=root, graveyard_path=None
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
        summary["source_audit"] = null_audit
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])
    if int(real["episode_count"]) != 720 or pooled_episodes != 2160:
        raise V72FlowImpactRelaxationTripwireError(
            "flow-impact tripwire denominator drift"
        )
    verdict, null_ratio = classify_tripwire(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
    )
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_2_flow_impact_relaxation_tripwire_result_v1",
        "tripwire_id": "hydra_v7_2_flow_impact_relaxation_tripwire_0010",
        "grammar_id": "hydra_v7_2_flow_impact_relaxation_grammar_0010",
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
            "pass_rate": pooled_passes / pooled_episodes,
        },
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": [1, 2, 4, 8],
        "source_audit": source_audit,
        "flow_and_timestamp_observables_preserved_in_price_nulls": True,
        "price_dependent_impulse_and_response_states_recomputed": True,
        "signal_paths_allowed_to_change_in_price_nulls": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The larger episode denominator improves the geometry test but still derives "
            "from only two calendar-year blocks; a green tripwire cannot establish an edge."
        ),
        "prochaine_action": (
            "audit_power_and_relevant_nulls_for_2x_surviving_walk_forward_candidates"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "tombstone_delayed_flow_impact_class_as_geometry_only"
            if verdict == "ARTEFACT_GEOMETRY_ONLY"
            else "record_underpowered_tripwire_and_no_candidate_promotion"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def build_flow_impact_null_world(
    minute: pd.DataFrame, *, control: D1NullControl
) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = SHUFFLE_SEEDS if control == D1NullControl.DAILY_BLOCK_SHUFFLE else RANDOM_SEEDS
    return _within_year_price_null(
        minute, "minute_start_ns", control=control, seeds=seeds
    )


def _verify_preserved_observables(real: pd.DataFrame, null: pd.DataFrame) -> None:
    for column in (
        "minute_start_ns",
        "availability_ns",
        "calendar_year",
        "contract",
        "total_volume",
        "signed_aggressor_volume",
        "signed_aggressor_fraction",
    ):
        if not real[column].equals(null[column]):
            raise V72FlowImpactRelaxationTripwireError(
                f"flow-impact null changed invariant field: {column}"
            )


def _verify_invariant_source_counts(
    real: Mapping[str, Any], null: Mapping[str, Any]
) -> None:
    for field in (
        "minute_count",
        "calendar_year_count",
        "contract_count",
        "session_count",
    ):
        if int(real[field]) != int(null[field]):
            raise V72FlowImpactRelaxationTripwireError(
                f"flow-impact null source-count drift: {field}"
            )


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> None:
    expected = {
        TRIPWIRE_POLICY_PATH: TRIPWIRE_POLICY_SHA256,
        GRAMMAR_PATH: GRAMMAR_SHA256,
        ADDENDUM_PATH: ADDENDUM_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72FlowImpactRelaxationTripwireError(
            "flow-impact tripwire frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V72FlowImpactRelaxationTripwireError(
            "flow-impact tripwire multiplicity drift"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V72FlowImpactRelaxationTripwireError("unexpected proof-window state")


def _verify_signal_manifest(
    root: Path, signals: Mapping[str, Any], specs: Mapping[str, Any]
) -> None:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8"))
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if (
        set(rows) != set(specs)
        or set(signals) != set(specs)
        or manifest.get("contains_outcomes_or_pnl") is not False
    ):
        raise V72FlowImpactRelaxationTripwireError(
            "flow-impact tripwire signal manifest drift"
        )
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V72FlowImpactRelaxationTripwireError(
                "flow-impact tripwire specification drift"
            )
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V72FlowImpactRelaxationTripwireError(
                "flow-impact tripwire signal path drift"
            )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_flow_impact_relaxation_tripwire_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = (
        result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    )
    report_path = destination / "v72_flow_impact_relaxation_tripwire_report.md"
    ratio = result["NULL_RATIO"]
    ratio_text = "n/a" if ratio is None else f"{float(ratio):.6f}"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Delayed flow-impact permanent tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=191 verdict={result['verdict']}",
            f"gate=V72_G10_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=real_vs_3_null_worlds",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v72_flow_impact_relaxation_tripwire.py,tests/test_v72_flow_impact_relaxation_tripwire.py CONTRE=deux_blocs_malgre_un_denominator_plus_large",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Réel: `{result['raw_pass_counts']['real']}`",
            f"- Null: `{result['raw_pass_counts']['null']}`",
            f"- NULL_RATIO: `{ratio_text}`",
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
    "EXPECTED_GLOBAL_N_TRIALS",
    "V72FlowImpactRelaxationTripwireError",
    "build_flow_impact_null_world",
    "run_flow_impact_relaxation_tripwire",
]
