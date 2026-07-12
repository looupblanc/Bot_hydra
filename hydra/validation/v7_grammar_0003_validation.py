from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.propfirm.mll_variants import MllMode
from hydra.propfirm.rolling_combine import evaluate_rolling_combine
from hydra.propfirm.topstep_150k import Topstep150KConfig
from hydra.research.v7_hypothesis_grammar import (
    V7CandidateSpec,
    V7MarketBars,
    V7Signal,
)
from hydra.research.v7_hypothesis_grammar_0003 import (
    candidate_specs,
    generate_signal_population,
    load_v7_market_bars,
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
    signal_to_event,
)
from hydra.validation.v7_grammar_0002_validation import (
    _day_positions,
    _decision_minute,
    _empty_dsr,
    _invert_event,
    _is_sim_exploit,
    _null_metrics,
    _shift_signals_by_sessions,
    _signal_at_clocks,
)
from hydra.validation.v7_phase2_multiplicity import (
    benjamini_hochberg,
    deflated_sharpe_statistics,
)


PREREGISTRATION_SHA256 = (
    "e2f28a415ebd3676df917df1c37a072ab2829a3df7ef33ad438ec0e4312a31e5"
)
VALIDATION_POLICY_SHA256 = (
    "e1a3f18c4b7110913bbe24f89c5bc0876edf042eca83811bd7f94a46749188cd"
)
SIGNAL_MANIFEST_SHA256 = (
    "7e013bfad0d46598b4cd3536ce14ab6774ffdcd9c1d794a931eac3c869976aa5"
)
TRIPWIRE_ATTESTATION_SHA256 = (
    "ea9dc84eb49d3bbe44c3502a8a1ccf86bb5c534a1d11841e91947328211aa91e"
)
CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
N_TRIALS = 247_366
FDR_Q = 0.10
MINIMUM_NULL_RETENTION = 0.80


def run_grammar_0003_validation(
    *,
    project_root: str | Path,
    preregistration_path: str | Path,
    validation_policy_path: str | Path,
    signal_manifest_path: str | Path,
    tripwire_attestation_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    preregistration = Path(preregistration_path).resolve()
    validation_policy = Path(validation_policy_path).resolve()
    signal_manifest = Path(signal_manifest_path).resolve()
    tripwire_attestation = Path(tripwire_attestation_path).resolve()
    _verify_inputs(
        root,
        preregistration,
        validation_policy,
        signal_manifest,
        tripwire_attestation,
        proof_registry_path,
    )

    bars = load_v7_market_bars(root)
    signals = generate_signal_population(
        bars, graveyard_path=root / "mission/state/graveyard.db"
    )
    _verify_signal_manifest(signal_manifest, signals)
    specs = {row.candidate_id: row for row in candidate_specs()}
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
            dsr = deflated_sharpe_statistics(daily, n_trials=N_TRIALS)
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

    bh = benjamini_hochberg(p_values, q=FDR_Q)
    for row in candidate_rows:
        candidate_id = str(row["candidate_id"])
        row["BH"] = bh[candidate_id]
        gates = {
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
            "candidate_null_suite_passed": bool(
                row["candidate_null_suite"]["passed"]
            ),
            "DSR_deflated_z_gt_0": float(row["DSR"]["deflated_z"]) > 0.0,
            "BH_FDR_10pct_rejected": bool(row["BH"]["rejected"]),
            "permanent_tripwire_GREEN": True,
        }
        row["promotion_gates"] = gates
        row["shadow_queue_eligible"] = all(gates.values())

    selected = _select_shadow_queue(candidate_rows, maximum=5)
    early_killed = sum(not bool(row["stage2_pass"]) for row in candidate_rows)
    result: dict[str, Any] = {
        "schema": "hydra_v7_grammar_0003_validation_result_v1",
        "grammar_id": "hydra_v7_grammar_0003_multiday_calendar_transfer",
        "verdict": "GREEN" if selected else "NULL",
        "preregistration_sha256": PREREGISTRATION_SHA256,
        "validation_policy_sha256": VALIDATION_POLICY_SHA256,
        "signal_manifest_sha256": SIGNAL_MANIFEST_SHA256,
        "permanent_tripwire_attestation_sha256": TRIPWIRE_ATTESTATION_SHA256,
        "permanent_tripwire": {
            "verdict": "GREEN",
            "NULL_RATIO": 0.04954068241469816,
            "real_object_count": 1015,
            "real_episode_count": 25680,
            "pooled_null_episode_count": 77040,
            "full_evidence_byte_identical": True,
        },
        "N_trials": N_TRIALS,
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
        "diff_validation": [
            "hydra/validation/v7_grammar_0003_validation.py",
            "scripts/run_v7_grammar_0003_validation.py",
            "tests/test_v7_grammar_0003_validation.py",
        ],
        "CONTRE": (
            "The matched non-signal-day control preserves clock and opportunity "
            "count but cannot exactly match every latent regime. Even a promoted "
            "candidate would require its WORM fiche and untouched forward proof."
        ),
        "prochaine_action": (
            "freeze_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "tombstone_grammar_classes_and_preregister_new_hypotheses"
        ),
    }
    return _write_result(result, output_dir)


def _candidate_null_suite(
    *,
    spec: V7CandidateSpec,
    signals: Sequence[V7Signal],
    bars: V7MarketBars,
    actual_events: Sequence[TradePathEvent],
    eligible_days: Sequence[int],
    cost_model: Any,
) -> dict[str, Any]:
    actual_metrics = _event_metrics(actual_events, eligible_days)
    inverted = tuple(_invert_event(row) for row in actual_events)
    delayed_signals = _shift_signals_by_sessions(signals, bars, sessions=5)
    matched_signals = _matched_non_signal_day_signals(signals, bars)
    delayed = tuple(
        signal_to_event(
            row, spec, bars, cost_model, stress=CostStress.STRESS_1_5X
        )
        for row in delayed_signals
    )
    matched = tuple(
        signal_to_event(
            row, spec, bars, cost_model, stress=CostStress.STRESS_1_5X
        )
        for row in matched_signals
    )
    controls = {
        "SIGN_INVERSION": _null_metrics(
            inverted, eligible_days, len(actual_events), minimum_retention=1.0
        ),
        "FIVE_SESSION_SIGNAL_DELAY": _null_metrics(
            delayed,
            eligible_days,
            len(actual_events),
            minimum_retention=MINIMUM_NULL_RETENTION,
        ),
        "OPPORTUNITY_COUNT_MATCHED_NON_SIGNAL_DAY": _null_metrics(
            matched,
            eligible_days,
            len(actual_events),
            minimum_retention=MINIMUM_NULL_RETENTION,
        ),
    }
    retention_valid = all(
        bool(row["retention_passed"]) for row in controls.values()
    )
    maximum_null = max(
        (float(row["expectancy_per_trade"]) for row in controls.values()),
        default=0.0,
    )
    actual_expectancy = float(actual_metrics["expectancy_per_trade"])
    return {
        "actual_stress_1_5x_expectancy_per_trade": actual_expectancy,
        "maximum_null_expectancy_per_trade": maximum_null,
        "actual_minus_maximum_null": actual_expectancy - maximum_null,
        "retention_all_passed": retention_valid,
        "passed": retention_valid and actual_expectancy > maximum_null,
        "controls": controls,
    }


def _matched_non_signal_day_signals(
    signals: Sequence[V7Signal], bars: V7MarketBars
) -> tuple[V7Signal, ...]:
    days = sorted({int(value) for value in bars.session_day})
    signal_days = {int(row.session_day) for row in signals}
    available = [day for day in days if day not in signal_days]
    used: set[int] = set()
    day_positions = _day_positions(bars)
    output: list[V7Signal] = []
    for source in sorted(signals, key=lambda row: (row.session_day, row.decision_ns)):
        for target_day in available:
            if target_day <= int(source.session_day) or target_day in used:
                continue
            shifted = _signal_at_clocks(
                source,
                bars,
                target_day=target_day,
                decision_minute=_decision_minute(source, bars),
                entry_minute=int(bars.local_minute[source.entry_index]),
                exit_minute=int(bars.local_minute[source.exit_index]),
                day_positions=day_positions,
                suffix="MATCHED_NON_SIGNAL_DAY",
            )
            if shifted is None:
                continue
            used.add(target_day)
            output.append(shifted)
            break
    return tuple(output)


def _verify_inputs(
    root: Path,
    preregistration: Path,
    validation_policy: Path,
    signal_manifest: Path,
    tripwire_attestation: Path,
    proof_registry_path: str | Path,
) -> None:
    checks = {
        "grammar WORM": (_sha256(preregistration), PREREGISTRATION_SHA256),
        "validation policy": (
            _sha256(validation_policy),
            VALIDATION_POLICY_SHA256,
        ),
        "signal manifest": (_sha256(signal_manifest), SIGNAL_MANIFEST_SHA256),
        "tripwire attestation": (
            _sha256(tripwire_attestation),
            TRIPWIRE_ATTESTATION_SHA256,
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
    if multiplicity_trial_count(proof) != N_TRIALS:
        raise GrammarValidationError("grammar 0003 multiplicity reservation mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise GrammarValidationError("unexpected proof window state")
    attestation = json.loads(tripwire_attestation.read_text(encoding="utf-8"))
    if (
        attestation.get("verdict") != "GREEN"
        or not attestation.get("full_evidence_byte_identical")
        or int(attestation.get("real", {}).get("object_count", -1)) != 1015
        or not math.isclose(
            float(attestation.get("NULL_RATIO")),
            0.04954068241469816,
            abs_tol=1e-15,
        )
    ):
        raise GrammarValidationError("permanent tripwire is not exact GREEN")


def _verify_signal_manifest(
    path: Path, signals: Mapping[str, Sequence[V7Signal]]
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise GrammarValidationError("grammar 0003 signal logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload["signals"]:
        raise GrammarValidationError("grammar 0003 signal regeneration drift")
    if payload.get("contains_outcomes_or_pnl") is not False:
        raise GrammarValidationError("grammar 0003 signal manifest has outcomes")


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "grammar0003_validation_result.json"
    report_path = destination / "grammar0003_validation_report.md"
    temporary = result_path.with_name(result_path.name + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    report_path.write_text(_render_report(result), encoding="utf-8")
    result["result_path"] = str(result_path)
    result["result_sha256"] = _sha256(result_path)
    result["report_path"] = str(report_path)
    return result


def _render_report(result: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# HYDRA V7 — Validation grammaire 0003",
            "",
            f"[HYDRA-V7] phase=4 step=7 verdict={result['verdict']}",
            "gate=GRAMMAR_0003 preuve=reports/v7/phase4/grammar0003_validation_result.json#pending tests=pending",
            f"budget_llm=0.000000/solde budget_data=40.401063/60.00 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v7_grammar_0003_validation.py,scripts/run_v7_grammar_0003_validation.py,tests/test_v7_grammar_0003_validation.py CONTRE=le_controle_non_signal_ne_matche_pas_tous_les_regimes_latents",
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
    "run_grammar_0003_validation",
]
