from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from hydra.execution.v7_cost_model import load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.rolling_combine import evaluate_rolling_combine
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.v7_hypothesis_grammar import (
    V7CandidateSpec,
    V7MarketBars,
    V7Signal,
)
from hydra.validation.v7_grammar_0001_validation import (
    GrammarValidationError,
    _build_candidate_events,
    _compact_combine,
    _daily_vector,
    _empty_walk_forward,
    _episode_policy,
    _event_compliance,
    _event_metrics,
    _select_shadow_queue,
    _walk_forward,
)
from hydra.validation.v7_grammar_0002_validation import (
    _empty_dsr,
    _is_sim_exploit,
)
from hydra.validation.v7_grammar_0003_validation import _candidate_null_suite
from hydra.validation.v7_phase2_multiplicity import (
    benjamini_hochberg,
    deflated_sharpe_statistics,
)


CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
PERMANENT_NULL_RATIO = 0.04954068241469816


@dataclass(frozen=True, slots=True)
class FrozenGrammarTribunalConfig:
    grammar_number: int
    grammar_id: str
    result_schema: str
    preregistration_sha256: str
    validation_policy_sha256: str
    signal_manifest_sha256: str
    tripwire_attestation_sha256: str
    n_trials: int
    diff_validation: tuple[str, ...]
    contre: str
    report_contre_slug: str

    @property
    def result_filename(self) -> str:
        return f"grammar{self.grammar_number:04d}_validation_result.json"

    @property
    def report_filename(self) -> str:
        return f"grammar{self.grammar_number:04d}_validation_report.md"


def run_frozen_grammar_tribunal(
    *,
    config: FrozenGrammarTribunalConfig,
    project_root: str | Path,
    preregistration_path: str | Path,
    validation_policy_path: str | Path,
    signal_manifest_path: str | Path,
    tripwire_attestation_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
    candidate_specs_fn: Callable[[], Sequence[V7CandidateSpec]],
    generate_signals_fn: Callable[..., Mapping[str, Sequence[V7Signal]]],
    load_bars_fn: Callable[[str | Path], Mapping[str, V7MarketBars]],
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    preregistration = Path(preregistration_path).resolve()
    validation_policy = Path(validation_policy_path).resolve()
    signal_manifest = Path(signal_manifest_path).resolve()
    tripwire_attestation = Path(tripwire_attestation_path).resolve()
    _verify_inputs(
        config,
        root,
        preregistration,
        validation_policy,
        signal_manifest,
        tripwire_attestation,
        proof_registry_path,
    )

    bars = dict(load_bars_fn(root))
    signals = dict(
        generate_signals_fn(
            bars, graveyard_path=root / "mission/state/graveyard.db"
        )
    )
    _verify_signal_manifest(config, signal_manifest, signals)
    specs = {row.candidate_id: row for row in candidate_specs_fn()}
    if set(specs) != set(signals):
        raise GrammarValidationError("frozen grammar specification drift")
    cost_model = load_cost_model()
    real_events = _build_candidate_events(specs, signals, bars, cost_model)

    candidate_rows: list[dict[str, Any]] = []
    p_values: dict[str, float] = {}
    for candidate_id in sorted(specs):
        bundle = real_events[candidate_id]
        base_metrics = _event_metrics(bundle.base, bundle.eligible_days)
        stress_1_5 = _event_metrics(bundle.stress_1_5x, bundle.eligible_days)
        stress_2 = _event_metrics(bundle.stress_2x, bundle.eligible_days)
        sim_exploit = _is_sim_exploit(
            float(base_metrics["expectancy_per_trade"]),
            float(stress_2["expectancy_per_trade"]),
        )
        stage1_pass = (
            int(base_metrics["event_count"]) >= 30
            and float(base_metrics["expectancy_per_trade"]) > 0.0
        )
        trajectory_compliance = _event_compliance(bundle.base)
        stage2_pass = (
            stage1_pass
            and float(stress_1_5["expectancy_per_trade"]) > 0.0
            and float(stress_2["expectancy_per_trade"]) > 0.0
            and not sim_exploit
            and trajectory_compliance
        )
        null_suite = _candidate_null_suite(
            spec=specs[candidate_id],
            signals=signals[candidate_id],
            bars=bars[specs[candidate_id].market],
            actual_events=bundle.stress_1_5x,
            eligible_days=bundle.eligible_days,
            cost_model=cost_model,
        )
        if stage2_pass:
            walk_forward = _walk_forward(bundle.stress_1_5x, bundle.eligible_days)
            daily = _daily_vector(bundle.stress_1_5x, bundle.eligible_days)
            dsr = deflated_sharpe_statistics(daily, n_trials=config.n_trials)
            p_values[candidate_id] = float(dsr["one_sided_p_value"])
        else:
            walk_forward = _empty_walk_forward()
            dsr = _empty_dsr(len(bundle.eligible_days))
            p_values[candidate_id] = 1.0

        default_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(mll_mode=MllMode.EOD_LEVEL_RT_BREACH),
        )
        intraday_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(mll_mode=MllMode.INTRADAY_HWM),
        )
        dll_combine = evaluate_rolling_combine(
            bundle.base,
            bundle.eligible_days,
            policy=_episode_policy(),
            config=Topstep150KConfig(
                mll_mode=MllMode.EOD_LEVEL_RT_BREACH,
                use_optional_daily_loss_limit=True,
            ),
        )
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                "specification": specs[candidate_id].to_dict(),
                "signal_count": len(signals[candidate_id]),
                "stage0_valid": True,
                "stage1_pass": stage1_pass,
                "stage2_pass": stage2_pass,
                "base": base_metrics,
                "stress_1_5x": stress_1_5,
                "stress_2x": stress_2,
                "SIM_EXPLOIT": sim_exploit,
                "trajectory_compliance": trajectory_compliance,
                "candidate_null_suite": null_suite,
                "walk_forward": walk_forward,
                "DSR": dsr,
                "combine_diagnostic_not_fitness": {
                    "eod_level_rt_breach": _compact_combine(default_combine),
                    "intraday_hwm": _compact_combine(intraday_combine),
                    "optional_DLL_3000": _compact_combine(dll_combine),
                },
            }
        )

    bh = benjamini_hochberg(p_values, q=0.10)
    for row in candidate_rows:
        candidate_id = str(row["candidate_id"])
        row["BH"] = bh[candidate_id]
        row["promotion_gates"] = _promotion_gates(row)
        row["shadow_queue_eligible"] = all(row["promotion_gates"].values())

    selected = _select_shadow_queue(candidate_rows, maximum=5)
    early_killed = sum(not bool(row["stage2_pass"]) for row in candidate_rows)
    result: dict[str, Any] = {
        "schema": config.result_schema,
        "grammar_id": config.grammar_id,
        "verdict": "GREEN" if selected else "NULL",
        "preregistration_sha256": config.preregistration_sha256,
        "validation_policy_sha256": config.validation_policy_sha256,
        "signal_manifest_sha256": config.signal_manifest_sha256,
        "permanent_tripwire_attestation_sha256": (
            config.tripwire_attestation_sha256
        ),
        "permanent_tripwire": {
            "verdict": "GREEN",
            "NULL_RATIO": PERMANENT_NULL_RATIO,
            "real_object_count": 1015,
            "real_episode_count": 25680,
            "pooled_null_episode_count": 77040,
            "full_evidence_byte_identical": True,
        },
        "N_trials": config.n_trials,
        "candidate_count": len(candidate_rows),
        "signal_count": sum(int(row["signal_count"]) for row in candidate_rows),
        "stage1_survivor_count": sum(
            bool(row["stage1_pass"]) for row in candidate_rows
        ),
        "stage2_survivor_count": sum(
            bool(row["stage2_pass"]) for row in candidate_rows
        ),
        "candidate_null_pass_count": sum(
            bool(row["candidate_null_suite"]["passed"])
            for row in candidate_rows
        ),
        "killed_before_walk_forward_count": early_killed,
        "kill_before_walk_forward_rate": early_killed / len(candidate_rows),
        "DSR_positive_count": sum(
            float(row["DSR"]["deflated_z"]) > 0.0 for row in candidate_rows
        ),
        "BH_rejection_count": sum(
            bool(row["BH"]["rejected"]) for row in candidate_rows
        ),
        "SIM_EXPLOIT_count": sum(
            bool(row["SIM_EXPLOIT"]) for row in candidate_rows
        ),
        "selected_shadow_queue_candidate_ids": selected,
        "candidate_results": candidate_rows,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "phase_data_spend_usd": 0.0,
        "outbound_order_count": 0,
        "combine_pass_rate_used_as_fitness": False,
        "combine_pass_rate_used_as_promotion_gate": False,
        "diff_validation": list(config.diff_validation),
        "CONTRE": config.contre,
        "prochaine_action": (
            "freeze_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "tombstone_grammar_classes_and_preregister_new_hypotheses"
        ),
    }
    return _write_result(config, result, output_dir)


def _promotion_gates(row: Mapping[str, Any]) -> dict[str, bool]:
    return {
        "stage1_minimum_events_and_base_expectancy": bool(row["stage1_pass"]),
        "cost_1_5x_positive": float(
            row["stress_1_5x"]["expectancy_per_trade"]
        )
        > 0.0,
        "SIM_EXPLOIT_2x_survived": not bool(row["SIM_EXPLOIT"]),
        "ruleset_trajectory_compliance": bool(row["trajectory_compliance"]),
        "walk_forward_1_5x_positive": float(
            row["walk_forward"]["pooled_expectancy_per_trade"]
        )
        > 0.0,
        "candidate_null_suite_passed": bool(row["candidate_null_suite"]["passed"]),
        "DSR_deflated_z_gt_0": float(row["DSR"]["deflated_z"]) > 0.0,
        "BH_FDR_10pct_rejected": bool(row["BH"]["rejected"]),
        "permanent_tripwire_GREEN": True,
    }


def _verify_inputs(
    config: FrozenGrammarTribunalConfig,
    root: Path,
    preregistration: Path,
    validation_policy: Path,
    signal_manifest: Path,
    tripwire_attestation: Path,
    proof_registry_path: str | Path,
) -> None:
    checks = {
        "grammar WORM": (
            _sha256(preregistration),
            config.preregistration_sha256,
        ),
        "validation policy": (
            _sha256(validation_policy),
            config.validation_policy_sha256,
        ),
        "signal manifest": (
            _sha256(signal_manifest),
            config.signal_manifest_sha256,
        ),
        "tripwire attestation": (
            _sha256(tripwire_attestation),
            config.tripwire_attestation_sha256,
        ),
        "mission contract": (
            _sha256(root / "MISSION_CONTRACT.md"),
            CONTRACT_SHA256,
        ),
    }
    drift = [name for name, (actual, expected) in checks.items() if actual != expected]
    if drift:
        raise GrammarValidationError("frozen input hash mismatch: " + ",".join(drift))
    proof = load_and_verify(proof_registry_path)
    if multiplicity_trial_count(proof) != config.n_trials:
        raise GrammarValidationError("grammar multiplicity reservation mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise GrammarValidationError("unexpected proof window state")
    attestation = json.loads(tripwire_attestation.read_text(encoding="utf-8"))
    if (
        attestation.get("verdict") != "GREEN"
        or not attestation.get("full_evidence_byte_identical")
        or int(attestation.get("real", {}).get("object_count", -1)) != 1015
        or not math.isclose(
            float(attestation.get("NULL_RATIO")),
            PERMANENT_NULL_RATIO,
            abs_tol=1e-15,
        )
    ):
        raise GrammarValidationError("permanent tripwire is not exact GREEN")


def _verify_signal_manifest(
    config: FrozenGrammarTribunalConfig,
    path: Path,
    signals: Mapping[str, Sequence[V7Signal]],
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise GrammarValidationError("frozen signal logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload["signals"]:
        raise GrammarValidationError("frozen signal regeneration drift")
    if payload.get("grammar_id") != config.grammar_id:
        raise GrammarValidationError("frozen signal grammar identity drift")
    if payload.get("contains_outcomes_or_pnl") is not False:
        raise GrammarValidationError("frozen signal manifest has outcomes")


def _write_result(
    config: FrozenGrammarTribunalConfig,
    result: dict[str, Any],
    output_dir: str | Path,
) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / config.result_filename
    report_path = destination / config.report_filename
    temporary = result_path.with_name(result_path.name + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    report_path.write_text(_render_report(config, result), encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    result["report_path"] = str(report_path)
    return result


def _render_report(
    config: FrozenGrammarTribunalConfig, result: Mapping[str, Any]
) -> str:
    number = config.grammar_number
    return "\n".join(
        [
            f"# HYDRA V7 — Validation grammaire {number:04d}",
            "",
            f"[HYDRA-V7] phase=4 step={number + 4} verdict={result['verdict']}",
            f"gate=GRAMMAR_{number:04d} preuve=reports/v7/phase4/{config.result_filename}#pending tests=pending",
            f"budget_llm=usage_API_non_exposee/solde budget_data=40.401063/60.00 N_trials={result['N_trials']} burned=1",
            "diff_validation="
            + ",".join(config.diff_validation)
            + f" CONTRE={config.report_contre_slug}",
            f"prochaine_action={result['prochaine_action']}",
            "",
            "## Tripwire permanent",
            "",
            f"- Verdict : `{result['permanent_tripwire']['verdict']}`",
            f"- NULL_RATIO : `{result['permanent_tripwire']['NULL_RATIO']:.12f}`",
            "- 1 015 objets; preuve complète byte-identique au G1 fondateur.",
            "",
            "## Funnel",
            "",
            f"- Structures : `{result['candidate_count']}`",
            f"- Signaux : `{result['signal_count']}`",
            f"- Stage 1 : `{result['stage1_survivor_count']}`",
            f"- Stage 2 : `{result['stage2_survivor_count']}`",
            f"- Null suite : `{result['candidate_null_pass_count']}`",
            f"- DSR positifs : `{result['DSR_positive_count']}`",
            f"- Rejets BH : `{result['BH_rejection_count']}`",
            f"- SIM_EXPLOIT : `{result['SIM_EXPLOIT_count']}`",
            f"- File shadow : `{len(result['selected_shadow_queue_candidate_ids'])}`",
            "",
            "## CONTRE",
            "",
            str(result["CONTRE"]),
            "",
        ]
    )


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "FrozenGrammarTribunalConfig",
    "run_frozen_grammar_tribunal",
]
