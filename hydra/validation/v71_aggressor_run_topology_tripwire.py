from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v71_aggressor_run_topology import (
    build_aggressor_run_topology_states,
    candidate_specs,
    generate_signal_population,
    load_aggressor_run_topology_sources,
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


TRIPWIRE_POLICY_PATH = "WORM/v7.1-aggressor-run-topology-tripwire-0009-2026-07-13.json"
TRIPWIRE_POLICY_SHA256 = "b26460dc7cf277acf68369ea244b24f7bec743d260863283ff3a711093a3318f"
GRAMMAR_PATH = "WORM/v7.1-aggressor-run-topology-grammar-0009-2026-07-13.json"
GRAMMAR_SHA256 = "05ff83f0fbf902381371d3d840ce7393adadfa8e51d6c75e51a76c12a275bce2"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_aggressor_run_topology_v1.json"
FEATURE_MANIFEST_SHA256 = "b1151bdc493f569eda85d13983ce73df9f92cbfe9f2416fd4ac63183e251127c"
SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery_0009/v71_aggressor_run_topology_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = "f678b6049cb46ed81502f63894082aa5546ce4335054ffe630551cba8b4faaa4"
FUNNEL_RESULT_PATH = "reports/v7_1/discovery_0009/v71_aggressor_run_topology_funnel_result.json"
FUNNEL_RESULT_SHA256 = "65d6a30ef61c74b1e8dd3dbfc194c062ade5ea34465fac9dbdf122da87fa7623"
EXPECTED_GLOBAL_N_TRIALS = 263_902
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {(2023, "ES"): 7109001, (2024, "ES"): 7109001 + 100}
RANDOM_SEEDS = {(2023, "ES"): 7109002, (2024, "ES"): 7109002 + 100}


class V71AggressorRunTopologyTripwireError(RuntimeError):
    pass


def run_aggressor_run_topology_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0009",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    real_minute, feature, source_audit = load_aggressor_run_topology_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(feature, project_root=root, graveyard_path=None)
    _verify_signal_manifest(root, real_signals, specs)
    costs = load_cost_model()
    real_events = build_candidate_events(
        real_minute, real_signals, specs, costs, stress=CostStress.BASE
    )
    eligible = _eligible_days_by_year(real_minute)
    real = _evaluate_world(real_events, eligible)
    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute = build_aggressor_run_topology_null_world(real_minute, control=control)
        _verify_preserved_minute_identity(real_minute, null_minute)
        null_feature = _feature_for_null(feature, null_minute)
        null_states, null_audit = build_aggressor_run_topology_states(
            null_feature, null_minute
        )
        _verify_invariant_topology(source_audit.to_dict(), null_audit.to_dict())
        null_signals = generate_signal_population(
            null_states, project_root=root, graveyard_path=None
        )
        if set(null_signals) != set(real_signals):
            raise V71AggressorRunTopologyTripwireError(
                "aggressor-run null candidate set drift"
            )
        null_events = build_candidate_events(
            null_minute, null_signals, specs, costs, stress=CostStress.BASE
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
        raise V71AggressorRunTopologyTripwireError("null episode count is below real")
    exact = exact_tripwire_evidence(
        real_passes=int(real["pass_count"]),
        real_episodes=int(real["episode_count"]),
        null_passes=pooled_passes,
        null_episodes=pooled_episodes,
        tripwire_verdict=verdict,
    )
    result = {
        "schema": "hydra_v7_1_aggressor_run_topology_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_aggressor_run_topology_tripwire_0009",
        "grammar_id": "hydra_v7_1_aggressor_run_topology_grammar_0009",
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
        "trade_run_topology_invariant_in_price_nulls": True,
        "minute_price_progress_recomputed_in_each_null": True,
        "signal_paths_allowed_to_change_with_recomputed_price_progress": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": "Only two D1 calendar blocks exist and the candidate conditions include completed-minute price progress; even a green tripwire cannot rescue four formulations already negative at Stage 1.",
        "prochaine_action": (
            "record_stage1_formulation_falsification_and_no_power_audit"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "tombstone_aggressor_run_topology_class_as_geometry_only"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _feature_for_null(states: pd.DataFrame, null_minute: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "calendar_year",
        "product",
        "contract",
        "instrument_id",
        "minute_start_ns",
        "availability_ns",
        "trade_count",
        "neutral_trade_count",
        "side_change_count",
        "longest_buy_run",
        "longest_sell_run",
        "tail_side",
        "tail_run",
        "first_price",
        "last_price",
        "transformation_version",
    ]
    feature = states[columns].copy()
    keys = ["calendar_year", "contract", "minute_start_ns", "availability_ns"]
    prices = null_minute[keys + ["open", "close"]]
    joined = feature.drop(columns=["first_price", "last_price"]).merge(
        prices, on=keys, how="inner", validate="one_to_one"
    )
    if len(joined) != len(feature):
        raise V71AggressorRunTopologyTripwireError("null price feature alignment failed")
    joined["first_price"] = np.rint(joined.pop("open").to_numpy(float) * 1_000_000_000).astype(np.int64)
    joined["last_price"] = np.rint(joined.pop("close").to_numpy(float) * 1_000_000_000).astype(np.int64)
    return joined


def build_aggressor_run_topology_null_world(
    minute: pd.DataFrame, *, control: D1NullControl
) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = SHUFFLE_SEEDS if control == D1NullControl.DAILY_BLOCK_SHUFFLE else RANDOM_SEEDS
    return _within_year_price_null(
        minute, "minute_start_ns", control=control, seeds=seeds
    )


def _verify_preserved_minute_identity(real: pd.DataFrame, null: pd.DataFrame) -> None:
    for column in (
        "minute_start_ns",
        "source_close_ns",
        "availability_ns",
        "contract",
        "calendar_year",
    ):
        if not real[column].equals(null[column]):
            raise V71AggressorRunTopologyTripwireError(
                f"aggressor-run null changed minute identity: {column}"
            )


def _verify_invariant_topology(real: Mapping[str, int], null: Mapping[str, int]) -> None:
    for field in (
        "minute_count",
        "session_count",
        "exact_source_match_count",
        "unique_dominant_run_count",
        "tied_run_count",
        "executable_state_count_30m",
        "executable_state_count_60m",
    ):
        if int(real[field]) != int(null[field]):
            raise V71AggressorRunTopologyTripwireError(
                f"aggressor-run null changed run topology: {field}"
            )


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
        raise V71AggressorRunTopologyTripwireError(
            "aggressor-run tripwire frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71AggressorRunTopologyTripwireError(
            "aggressor-run tripwire reservation absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71AggressorRunTopologyTripwireError("unexpected proof-window state")


def _verify_signal_manifest(
    root: Path, signals: Mapping[str, Any], specs: Mapping[str, Any]
) -> None:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text())
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if (
        set(rows) != set(specs)
        or set(signals) != set(specs)
        or manifest.get("contains_outcomes_or_pnl") is not False
    ):
        raise V71AggressorRunTopologyTripwireError(
            "aggressor-run signal manifest drift"
        )
    from hydra.research.v71_aggressor_run_topology import signal_path_hash

    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71AggressorRunTopologyTripwireError(
                "aggressor-run specification drift"
            )
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71AggressorRunTopologyTripwireError(
                "aggressor-run signal path drift"
            )


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_aggressor_run_topology_tripwire_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    report_path = destination / "v71_aggressor_run_topology_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Aggressor-run topology permanent tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=179 verdict={result['verdict']}",
            f"gate=V71_G9_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=real_vs_3_null_worlds",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_aggressor_run_topology_tripwire.py CONTRE=deux_blocs_et_condition_de_progress_prix_contemporaine",
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


__all__ = [
    "EXPECTED_GLOBAL_N_TRIALS",
    "V71AggressorRunTopologyTripwireError",
    "build_aggressor_run_topology_null_world",
    "run_aggressor_run_topology_tripwire",
]
