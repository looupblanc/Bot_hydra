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
from hydra.research.v71_event_time_grammar import load_event_time_sources
from hydra.research.v71_flow_sign_sequence import (
    build_flow_sign_sequence_states,
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


TRIPWIRE_POLICY_PATH = "WORM/v7.1-flow-sign-sequence-tripwire-0007-2026-07-13.json"
TRIPWIRE_POLICY_SHA256 = "c7806c7ac4c512a05ca468388857419bd87a4a316967366ac8892ca38d25ff7a"
GRAMMAR_PATH = "WORM/v7.1-flow-sign-sequence-grammar-0007-2026-07-13.json"
GRAMMAR_SHA256 = "4cb89b0e774f754037fde8a6f86703cda0047eefcd01174e1f65bb8d37fc45ab"
SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery_0007/v71_flow_sign_sequence_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = "eae86b8260596bb1ba9b8155769dfb16cb528736764ad57c194f5bdf3db48ee6"
FUNNEL_RESULT_PATH = "reports/v7_1/discovery_0007/v71_flow_sign_sequence_funnel_result.json"
FUNNEL_RESULT_SHA256 = "ec2570bcf75185238751815ffd259ff9e08c2ec9577e30c840ba2eaa188322ba"
EXPECTED_GLOBAL_N_TRIALS = 263_844
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 992301, (2024, "ES"): 992401}
RANDOM_SEEDS = {(2023, "ES"): 993301, (2024, "ES"): 993401}


class V71FlowSignSequenceTripwireError(RuntimeError):
    pass


def run_flow_sign_sequence_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0007",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute, _, _ = load_event_time_sources(root)
    real_states, source_audit = build_flow_sign_sequence_states(minute)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(real_states, project_root=root, graveyard_path=None)
    _verify_signal_manifest(root, real_signals, specs)
    costs = load_cost_model()
    real_events = build_candidate_events(minute, real_signals, specs, costs, stress=CostStress.BASE)
    eligible = _eligible_days_by_year(minute)
    real = _evaluate_world(real_events, eligible)
    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute = build_flow_sign_sequence_null_world(minute, control=control)
        _verify_preserved_observables(minute, null_minute)
        null_states, null_audit = build_flow_sign_sequence_states(null_minute)
        _verify_invariant_source_counts(source_audit.to_dict(), null_audit.to_dict())
        null_signals = generate_signal_population(null_states, project_root=root, graveyard_path=None)
        _verify_signal_paths_invariant(real_signals, null_signals)
        null_events = build_candidate_events(minute=null_minute, signals=null_signals, specs=specs, cost_model=costs, stress=CostStress.BASE)
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
        raise V71FlowSignSequenceTripwireError("null episode count is below real")
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_1_flow_sign_sequence_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_flow_sign_sequence_tripwire_0007",
        "grammar_id": "hydra_v7_1_flow_sign_sequence_grammar_0007",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "raw_pass_counts": {
            "real": f"{int(real['pass_count'])}/{int(real['episode_count'])}",
            "null": f"{pooled_passes}/{pooled_episodes}",
        },
        "exact_binomial_test": exact.to_dict(),
        "evidence_strength": exact.evidence_strength,
        "real": {**real, "signal_count": sum(len(rows) for rows in real_signals.values())},
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": pooled_passes / max(pooled_episodes, 1),
        },
        "controls": controls,
        "candidate_count": len(specs),
        "diagnostic_quantities": [1, 2, 4, 8],
        "source_audit": source_audit.to_dict(),
        "flow_sign_sequences_invariant_in_price_nulls": True,
        "price_derived_fields_recomputed_in_each_null": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": "Only two D1 calendar blocks exist and the observed flow-sign sequences are held fixed in price nulls; even a green tripwire cannot rescue six formulations that failed the preregistered economic screen.",
        "prochaine_action": (
            "record_clean_null_and_review_next_distinct_hypothesis_without_parameter_feedback"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "tombstone_flow_sign_sequence_class_as_geometry_only"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def build_flow_sign_sequence_null_world(minute: pd.DataFrame, *, control: D1NullControl) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = SHUFFLE_SEEDS if control == D1NullControl.DAILY_BLOCK_SHUFFLE else RANDOM_SEEDS
    return _within_year_price_null(minute, "minute_start_ns", control=control, seeds=seeds)


def _verify_preserved_observables(real: pd.DataFrame, null: pd.DataFrame) -> None:
    for column in (
        "minute_start_ns", "source_close_ns", "availability_ns", "contract",
        "calendar_year", "trade_count", "total_volume", "buy_aggressor_volume",
        "sell_aggressor_volume", "unknown_side_volume", "signed_aggressor_volume",
        "signed_aggressor_fraction",
    ):
        if not real[column].equals(null[column]):
            raise V71FlowSignSequenceTripwireError(f"flow-sign null changed observable: {column}")


def _verify_invariant_source_counts(real: Mapping[str, int], null: Mapping[str, int]) -> None:
    for field in (
        "minute_count", "session_count", "nonzero_flow_minute_count",
        "contiguous_five_minute_count", "run_termination_handoff_count",
        "run_restart_after_one_counter_count", "alternation_break_to_persistence_count",
        "executable_state_count_30m", "executable_state_count_60m",
    ):
        if int(real[field]) != int(null[field]):
            raise V71FlowSignSequenceTripwireError(f"flow-sign null changed eligibility: {field}")


def _verify_signal_paths_invariant(real: Mapping[str, Sequence[V71Signal]], null: Mapping[str, Sequence[V71Signal]]) -> None:
    if set(real) != set(null):
        raise V71FlowSignSequenceTripwireError("flow-sign null candidate set drift")
    for candidate_id in real:
        if signal_path_hash(real[candidate_id]) != signal_path_hash(null[candidate_id]):
            raise V71FlowSignSequenceTripwireError("flow-sign null changed signal path")


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> None:
    expected = {
        TRIPWIRE_POLICY_PATH: TRIPWIRE_POLICY_SHA256,
        GRAMMAR_PATH: GRAMMAR_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        FUNNEL_RESULT_PATH: FUNNEL_RESULT_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71FlowSignSequenceTripwireError("flow-sign tripwire frozen input drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71FlowSignSequenceTripwireError("flow-sign tripwire reservation absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71FlowSignSequenceTripwireError("unexpected proof-window state")


def _verify_signal_manifest(root: Path, signals: Mapping[str, Sequence[V71Signal]], specs: Mapping[str, V71CandidateSpec]) -> None:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text())
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs) or manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71FlowSignSequenceTripwireError("flow-sign manifest drift")
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71FlowSignSequenceTripwireError("flow-sign specification drift")
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71FlowSignSequenceTripwireError("flow-sign signal path drift")


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_flow_sign_sequence_tripwire_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    report_path = destination / "v71_flow_sign_sequence_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Flow-sign sequence permanent tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=170 verdict={result['verdict']}",
            f"gate=V71_G7_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=real_vs_3_null_worlds",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_flow_sign_sequence_tripwire.py CONTRE=deux_blocs_calendaires_et_sequences_de_flux_figees",
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


__all__ = ["EXPECTED_GLOBAL_N_TRIALS", "V71FlowSignSequenceTripwireError", "build_flow_sign_sequence_null_world", "run_flow_sign_sequence_tripwire"]
