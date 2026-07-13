from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import burned_window_ids, load_and_verify, multiplicity_trial_count
from hydra.research.v71_event_mechanism_grammar import V71CandidateSpec, V71Signal
from hydra.research.v71_intraminute_flow import (
    build_intraminute_flow_states,
    candidate_specs,
    generate_signal_population,
    signal_path_hash,
)
from hydra.validation.v71_opportunity_density_tripwire import build_candidate_events, classify_tripwire
from hydra.validation.v7_d1_new_dataset_tripwire import (
    D1NullControl,
    _eligible_days_by_year,
    _evaluate_world,
    _within_year_price_null,
    _year_permuted_prices,
)
from hydra.validation.v7_report_schema import validate_v7_report_text
from hydra.validation.v7_tripwire_evidence import exact_tripwire_evidence


TRIPWIRE_POLICY_PATH = "WORM/v7.1-intraminute-flow-tripwire-0008-2026-07-13.json"
TRIPWIRE_POLICY_SHA256 = "e4968cc24c5574a42ace695a8ec65f56578d5ca66a8954eac1239efbbfa4a535"
GRAMMAR_PATH = "WORM/v7.1-intraminute-flow-grammar-0008-2026-07-13.json"
GRAMMAR_SHA256 = "36f5d4f8dd2582979d809925782881fb1e159d23ddfbd50dc6a9d348cf5c18dc"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_intraminute_flow_v1.json"
FEATURE_MANIFEST_SHA256 = "b228dc89ed36d1b47660073dd6e68703eb44c85e6c0e5897a62f3a14168f6ad4"
SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery_0008/v71_intraminute_flow_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = "7b5af1090f219055f62180c7bbc03dac4c18af8573452d148f15354d55f95979"
FUNNEL_RESULT_PATH = "reports/v7_1/discovery_0008/v71_intraminute_flow_funnel_result.json"
FUNNEL_RESULT_SHA256 = "cdf426936a6350a997958f4f6bfb326466538881f290d7943bd057d9361dd69b"
EXPECTED_GLOBAL_N_TRIALS = 263_874
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 994301, (2024, "ES"): 994401}
RANDOM_SEEDS = {(2023, "ES"): 995301, (2024, "ES"): 995401}


class V71IntraminuteFlowTripwireError(RuntimeError):
    pass


def run_intraminute_flow_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0008",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    real_minute, feature, source_audit = _load_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(feature, project_root=root, graveyard_path=None)
    _verify_signal_manifest(root, real_signals, specs)
    costs = load_cost_model()
    real_events = build_candidate_events(real_minute, real_signals, specs, costs, stress=CostStress.BASE)
    eligible = _eligible_days_by_year(real_minute)
    real = _evaluate_world(real_events, eligible)
    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute = build_intraminute_flow_null_world(real_minute, control=control)
        _verify_preserved_minute_identity(real_minute, null_minute)
        null_states, null_audit = build_intraminute_flow_states(_feature_only(feature), null_minute)
        _verify_invariant_source_counts(source_audit, null_audit.to_dict())
        null_signals = generate_signal_population(null_states, project_root=root, graveyard_path=None)
        _verify_signal_paths_invariant(real_signals, null_signals)
        null_events = build_candidate_events(null_minute, null_signals, specs, costs, stress=CostStress.BASE)
        summary = _evaluate_world(null_events, eligible)
        summary["signal_count"] = sum(len(rows) for rows in null_signals.values())
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])
    verdict, null_ratio = classify_tripwire(
        real_passes=int(real["pass_count"]), real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes, null_episodes=pooled_episodes,
    )
    if pooled_episodes < int(real["episode_count"]):
        raise V71IntraminuteFlowTripwireError("null episode count is below real")
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]), real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes, null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_1_intraminute_flow_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_intraminute_flow_tripwire_0008",
        "grammar_id": "hydra_v7_1_intraminute_flow_grammar_0008",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "raw_pass_counts": {"real": f"{int(real['pass_count'])}/{int(real['episode_count'])}", "null": f"{pooled_passes}/{pooled_episodes}"},
        "exact_binomial_test": exact.to_dict(),
        "evidence_strength": exact.evidence_strength,
        "real": {**real, "signal_count": sum(len(rows) for rows in real_signals.values())},
        "pooled_null": {"episode_count": pooled_episodes, "pass_count": pooled_passes, "pass_rate": pooled_passes / max(pooled_episodes, 1)},
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": [1, 2, 4, 8],
        "source_audit": source_audit,
        "intraminute_flow_allocation_invariant_in_price_nulls": True,
        "price_derived_fields_recomputed_in_each_null": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": "Only two D1 calendar blocks exist and intraminute flow allocation is fixed in every price-null world; even a green result remains selected development evidence requiring calibrated power and fresh confirmation.",
        "prochaine_action": (
            "freeze_walk_forward_positive_candidates_for_candidate_specific_power_audit"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "tombstone_intraminute_flow_class_as_geometry_only"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _load_sources(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    from hydra.research.v71_intraminute_flow import load_intraminute_flow_sources

    minute, states, audit = load_intraminute_flow_sources(root)
    return minute, states, audit.to_dict()


def _feature_only(states: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "calendar_year", "product", "contract", "instrument_id", "minute_start_ns", "availability_ns",
        "first_trade_count", "second_trade_count", "first_total_volume", "second_total_volume",
        "first_signed_flow", "second_signed_flow", "transformation_version",
    ]
    return states[columns].copy()


def build_intraminute_flow_null_world(minute: pd.DataFrame, *, control: D1NullControl) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = SHUFFLE_SEEDS if control == D1NullControl.DAILY_BLOCK_SHUFFLE else RANDOM_SEEDS
    return _within_year_price_null(minute, "minute_start_ns", control=control, seeds=seeds)


def _verify_preserved_minute_identity(real: pd.DataFrame, null: pd.DataFrame) -> None:
    for column in ("minute_start_ns", "source_close_ns", "availability_ns", "contract", "calendar_year"):
        if not real[column].equals(null[column]):
            raise V71IntraminuteFlowTripwireError(f"intraminute null changed minute identity: {column}")


def _verify_invariant_source_counts(real: Mapping[str, int], null: Mapping[str, int]) -> None:
    for field in (
        "minute_count", "session_count", "exact_source_match_count",
        "back_loaded_same_sign_acceleration_count", "front_loaded_flow_decay_count",
        "late_flow_handoff_count", "executable_state_count_30m", "executable_state_count_60m",
    ):
        if int(real[field]) != int(null[field]):
            raise V71IntraminuteFlowTripwireError(f"intraminute null changed eligibility: {field}")


def _verify_signal_paths_invariant(real: Mapping[str, Sequence[V71Signal]], null: Mapping[str, Sequence[V71Signal]]) -> None:
    if set(real) != set(null):
        raise V71IntraminuteFlowTripwireError("intraminute null candidate set drift")
    for candidate_id in real:
        if signal_path_hash(real[candidate_id]) != signal_path_hash(null[candidate_id]):
            raise V71IntraminuteFlowTripwireError("intraminute null changed signal path")


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> None:
    expected = {
        TRIPWIRE_POLICY_PATH: TRIPWIRE_POLICY_SHA256,
        GRAMMAR_PATH: GRAMMAR_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        FUNNEL_RESULT_PATH: FUNNEL_RESULT_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71IntraminuteFlowTripwireError("intraminute tripwire frozen input drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71IntraminuteFlowTripwireError("intraminute tripwire reservation absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71IntraminuteFlowTripwireError("unexpected proof-window state")


def _verify_signal_manifest(root: Path, signals: Mapping[str, Sequence[V71Signal]], specs: Mapping[str, V71CandidateSpec]) -> None:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text())
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs) or manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71IntraminuteFlowTripwireError("intraminute signal manifest drift")
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71IntraminuteFlowTripwireError("intraminute specification drift")
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71IntraminuteFlowTripwireError("intraminute signal path drift")


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_intraminute_flow_tripwire_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    report_path = destination / "v71_intraminute_flow_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Intraminute-flow permanent tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=174 verdict={result['verdict']}",
            f"gate=V71_G8_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=real_vs_3_null_worlds",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_intraminute_flow_tripwire.py CONTRE=deux_blocs_calendaires_et_flux_intraminute_fige",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Réel: `{result['raw_pass_counts']['real']}`",
            f"- Null: `{result['raw_pass_counts']['null']}`",
            f"- NULL_RATIO: `{result['NULL_RATIO']:.6f}`",
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


__all__ = ["EXPECTED_GLOBAL_N_TRIALS", "V71IntraminuteFlowTripwireError", "build_intraminute_flow_null_world", "run_intraminute_flow_tripwire"]
