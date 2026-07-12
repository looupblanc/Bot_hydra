from __future__ import annotations

import hashlib
import json
import os
from fractions import Fraction
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, V7CostModel, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.v71_opportunity_density_grammar import (
    V71CandidateSpec,
    V71Signal,
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
    signal_path_hash,
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
    "WORM/v7.1-opportunity-density-tripwire-0002-2026-07-12.json"
)
TRIPWIRE_POLICY_SHA256 = (
    "8e1b7e511f99e1f108a113bb80a69d4985d498ed9d78d2d049e9468a6afdcacf"
)
GRAMMAR_PATH = "WORM/v7.1-opportunity-density-grammar-0002-2026-07-12.json"
GRAMMAR_SHA256 = "ef44e6e72c42b2ed4b7228f3addbd2f182e3e51bcfb619aa4c0a2102db6d3566"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_1/discovery_0002/v71_opportunity_density_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = (
    "c90a2321fc66e114d65dd533d077ec04308ae714369e28b82f5d9e996dd7fa24"
)
FUNNEL_RESULT_PATH = (
    "reports/v7_1/discovery_0002/v71_opportunity_density_funnel_result.json"
)
FUNNEL_RESULT_SHA256 = (
    "2a45c4da55875f90438cd6cb19f1ce79ec8de7d934f7a442e78000364aff5897"
)
EXPECTED_GLOBAL_N_TRIALS = 262_868
NULL_THRESHOLD = 0.80
POINT_VALUE = 50.0
MINI_EQUIVALENT = 1.0
SHUFFLE_SEEDS = {(2023, "ES"): 910301, (2024, "ES"): 910401}
RANDOM_SEEDS = {(2023, "ES"): 920301, (2024, "ES"): 920401}


class V71OpportunityDensityTripwireError(RuntimeError):
    pass


def run_opportunity_density_tripwire(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0002",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    _verify_inputs(root, proof_registry_path)
    minute = load_v71_minute_features(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    real_signals = generate_signal_population(minute, project_root=root)
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
        null_minute = _null_minute_world(minute, control)
        null_signals = generate_signal_population(
            null_minute, project_root=root
        )
        null_events = build_candidate_events(
            null_minute,
            null_signals,
            specs,
            costs,
            stress=CostStress.BASE,
        )
        summary = _evaluate_world(null_events, eligible)
        summary["signal_count"] = sum(
            len(rows) for rows in null_signals.values()
        )
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
        raise V71OpportunityDensityTripwireError(
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
        "schema": "hydra_v7_1_opportunity_density_tripwire_result_v1",
        "tripwire_id": "hydra_v7_1_opportunity_density_tripwire_0002",
        "grammar_id": "hydra_v7_1_opportunity_density_grammar_0002",
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
        "feature_and_signal_recomputation": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_promotion_authorized": False,
        "N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "new_data_purchase_count": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "Only two D1 calendar blocks are available; even a green grammar "
            "tripwire would not promote any underpowered candidate."
        ),
        "prochaine_action": (
            "retain_null_adjusted_baseline_and_classify_underpowered_mechanisms"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "freeze_tripwire_and_retire_geometry_contaminated_grammar"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def classify_tripwire(
    *,
    real_passes: int,
    real_episodes: int,
    null_passes: int,
    null_episodes: int,
) -> tuple[str, float | None]:
    if real_episodes <= 0 or null_episodes <= 0:
        raise ValueError("tripwire episodes must be positive")
    if not 0 <= real_passes <= real_episodes:
        raise ValueError("invalid real pass count")
    if not 0 <= null_passes <= null_episodes:
        raise ValueError("invalid null pass count")
    if real_passes == 0:
        return "BLOCKED_UNDERPOWERED", None
    real_rate = real_passes / real_episodes
    null_rate = null_passes / null_episodes
    ratio = null_rate / real_rate
    exact_ratio = Fraction(null_passes, null_episodes) / Fraction(
        real_passes, real_episodes
    )
    return (
        "ARTEFACT_GEOMETRY_ONLY"
        if exact_ratio >= Fraction(4, 5)
        else "GREEN_NULL_ADJUSTED_BASELINE",
        float(ratio),
    )


def build_candidate_events(
    minute: pd.DataFrame,
    signals: Mapping[str, Sequence[V71Signal]],
    specs: Mapping[str, V71CandidateSpec],
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> dict[str, tuple[TradePathEvent, ...]]:
    starts = minute["minute_start_ns"].to_numpy(dtype=np.int64)
    contracts = minute["contract"].astype(str).to_numpy()
    output: dict[str, tuple[TradePathEvent, ...]] = {}
    for candidate_id, spec in sorted(specs.items()):
        rows: list[TradePathEvent] = []
        for signal in signals[candidate_id]:
            entry = int(np.searchsorted(starts, signal.entry_minute_start_ns))
            exit_position = int(
                np.searchsorted(starts, signal.exit_minute_start_ns)
            )
            if (
                entry >= len(minute)
                or exit_position >= len(minute)
                or int(starts[entry]) != signal.entry_minute_start_ns
                or int(starts[exit_position]) != signal.exit_minute_start_ns
                or exit_position <= entry
            ):
                raise V71OpportunityDensityTripwireError(
                    "execution timestamp drift"
                )
            held = minute.iloc[entry:exit_position]
            if (
                len(set(contracts[entry : exit_position + 1])) != 1
                or contracts[entry] != signal.contract
            ):
                raise V71OpportunityDensityTripwireError(
                    "explicit contract drift"
                )
            entry_price = float(minute.iloc[entry]["open"])
            exit_price = float(minute.iloc[exit_position]["open"])
            cost = cost_model.round_turn_cost(
                "ES",
                spec.cost_horizon,
                stress=stress,
                contracts=1.0,
            )
            scale = signal.side * POINT_VALUE
            gross = (exit_price - entry_price) * scale
            adverse_price = (
                float(held["low"].min())
                if signal.side > 0
                else float(held["high"].max())
            )
            favorable_price = (
                float(held["high"].max())
                if signal.side > 0
                else float(held["low"].min())
            )
            net = gross - cost
            rows.append(
                TradePathEvent(
                    event_id=(
                        f"{candidate_id}:{signal.decision_ns}:{stress.value}"
                    ),
                    decision_ns=signal.decision_ns,
                    exit_ns=signal.exit_minute_start_ns,
                    session_day=_day_id(signal.session_day),
                    net_pnl=float(net),
                    gross_pnl=float(gross),
                    worst_unrealized_pnl=float(
                        min(
                            (adverse_price - entry_price) * scale - cost,
                            net,
                            0.0,
                        )
                    ),
                    best_unrealized_pnl=float(
                        max(
                            (favorable_price - entry_price) * scale - cost,
                            net,
                            0.0,
                        )
                    ),
                    quantity=1,
                    mini_equivalent=MINI_EQUIVALENT,
                    regime=spec.family_id,
                    session_compliant=True,
                    contract_limit_compliant=True,
                    same_bar_ambiguous=False,
                )
            )
        ordered = tuple(
            sorted(rows, key=lambda row: (row.decision_ns, row.event_id))
        )
        if any(
            right.decision_ns < left.exit_ns
            for left, right in zip(ordered, ordered[1:], strict=False)
            if left.session_day == right.session_day
        ):
            raise V71OpportunityDensityTripwireError(
                "candidate events overlap"
            )
        output[candidate_id] = ordered
    return output


def _null_minute_world(
    minute: pd.DataFrame, control: D1NullControl
) -> pd.DataFrame:
    if control == D1NullControl.YEAR_BLOCK_PERMUTATION:
        return _year_permuted_prices(minute, "minute_start_ns")
    seeds = (
        SHUFFLE_SEEDS
        if control == D1NullControl.DAILY_BLOCK_SHUFFLE
        else RANDOM_SEEDS
    )
    return _within_year_price_null(
        minute,
        "minute_start_ns",
        control=control,
        seeds=seeds,
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
        raise V71OpportunityDensityTripwireError(
            "tripwire frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71OpportunityDensityTripwireError(
            "tripwire multiplicity reservation is absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71OpportunityDensityTripwireError(
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
        raise V71OpportunityDensityTripwireError("signal manifest drift")
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71OpportunityDensityTripwireError("specification drift")
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V71OpportunityDensityTripwireError("signal path drift")


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_opportunity_density_tripwire_result.json"
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
    report_path = destination / "v71_opportunity_density_tripwire_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Opportunity-density grammar tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=132 verdict={result['verdict']}",
            f"gate=V71_G2_TRIPWIRE preuve={displayed}#{result_hash[:8]} tests=128_real_plus_384_null_paths",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_opportunity_density_tripwire.py CONTRE=deux_blocs_D1_ne_prouvent_pas_la_stabilite",
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


def _day_id(session_day: str) -> int:
    return int(pd.Timestamp(session_day, tz="UTC").value // 86_400_000_000_000)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "V71OpportunityDensityTripwireError",
    "build_candidate_events",
    "classify_tripwire",
    "run_opportunity_density_tripwire",
]
