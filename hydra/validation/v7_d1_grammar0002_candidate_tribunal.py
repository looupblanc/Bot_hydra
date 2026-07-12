from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    D1G2CandidateSpec,
    D1G2Signal,
    candidate_specs,
    generate_signal_population,
)
from hydra.validation.v7_d1_candidate_tribunal import _shift_frame_flow
from hydra.validation.v7_d1_grammar0002_tripwire import (
    build_grammar0002_events,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    DAY_NS,
    _ct_date,
    _day_id,
    _eligible_days_by_year,
)
from hydra.validation.v7_grammar_0001_validation import (
    _daily_vector,
    _empty_walk_forward,
    _event_metrics,
    _walk_forward,
)
from hydra.validation.v7_grammar_0002_validation import (
    _empty_dsr,
    _invert_event,
    _is_sim_exploit,
    _null_metrics,
)
from hydra.validation.v7_phase2_multiplicity import (
    benjamini_hochberg,
    deflated_sharpe_statistics,
)


GRAMMAR_SHA256 = "fac0b5166351940d1fde5334bdeaf846d56e56efc8cef9772a9599b8b86feee9"
TRIPWIRE_POLICY_SHA256 = "a2e5c568345b0250255c59f6829da20cab9b280a65b1cbfcfa727343f0d023b7"
VALIDATION_POLICY_SHA256 = "14fd8c2df5a4fd33816236d9cdc72af3a2c7595f52d6b62fd7de730c60f0e525"
NULL_POWER_ADDENDUM_SHA256 = "3a94a683219a0762577a106982023dad36df928b994e8852af51cdd1bd455bd3"
SIGNAL_MANIFEST_SHA256 = "db05bdcbe830d4079881fe03ae61759d622c45876468a113bd4344ee50dc1aea"
TRIPWIRE_RESULT_SHA256 = "119b58e9fd27061bd53b59871e07764c28170e80696e0d90eb1990ab7146d154"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
N_TRIALS = 247_892
FDR_Q = 0.10
MINIMUM_EVENTS = 30


class D1Grammar0002TribunalError(RuntimeError):
    pass


def run_d1_grammar0002_candidate_tribunal(
    *,
    project_root: str | Path,
    grammar_path: str | Path,
    tripwire_policy_path: str | Path,
    validation_policy_path: str | Path,
    null_power_addendum_path: str | Path,
    signal_manifest_path: str | Path,
    tripwire_result_path: str | Path,
    proof_registry_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    paths = {
        "grammar": Path(grammar_path).resolve(),
        "tripwire policy": Path(tripwire_policy_path).resolve(),
        "validation policy": Path(validation_policy_path).resolve(),
        "null power addendum": Path(null_power_addendum_path).resolve(),
        "signal manifest": Path(signal_manifest_path).resolve(),
        "tripwire result": Path(tripwire_result_path).resolve(),
    }
    _verify_inputs(root, paths, proof_registry_path)
    minute, _event = load_feature_store(root)
    signals = generate_signal_population(minute, project_root=root)
    _verify_signal_manifest(paths["signal manifest"], signals)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    costs = load_cost_model()
    events_by_stress = {
        stress: build_grammar0002_events(
            minute, signals, specs, costs, stress=stress
        )
        for stress in CostStress
    }
    delayed_signals = delay_grammar0002_signals_five_sessions(signals, minute)
    delayed_events = build_grammar0002_events(
        minute,
        delayed_signals,
        specs,
        costs,
        stress=CostStress.STRESS_1_5X,
    )
    shifted_minute = _shift_frame_flow(minute, "minute_start_ns")
    shifted_signals = generate_signal_population(shifted_minute, project_root=root)
    shifted_events = build_grammar0002_events(
        shifted_minute,
        shifted_signals,
        specs,
        costs,
        stress=CostStress.STRESS_1_5X,
    )
    eligible_days = tuple(
        sorted(
            day
            for rows in _eligible_days_by_year(minute).values()
            for day in rows
        )
    )

    rows: list[dict[str, Any]] = []
    p_values: dict[str, float] = {}
    for candidate_id, spec in sorted(specs.items()):
        base_events = events_by_stress[CostStress.BASE][candidate_id]
        stress_1_5_events = events_by_stress[CostStress.STRESS_1_5X][candidate_id]
        stress_2_events = events_by_stress[CostStress.STRESS_2X][candidate_id]
        base = _event_metrics(base_events, eligible_days)
        stress_1_5 = _event_metrics(stress_1_5_events, eligible_days)
        stress_2 = _event_metrics(stress_2_events, eligible_days)
        sim_exploit = _is_sim_exploit(
            float(base["expectancy_per_trade"]),
            float(stress_2["expectancy_per_trade"]),
        )
        stage1 = (
            int(base["event_count"]) >= MINIMUM_EVENTS
            and float(base["expectancy_per_trade"]) > 0.0
        )
        compliance = _trajectory_compliance(base_events, spec.product)
        stage2 = bool(
            stage1
            and float(stress_1_5["expectancy_per_trade"]) > 0.0
            and float(stress_2["expectancy_per_trade"]) > 0.0
            and not sim_exploit
            and compliance
        )
        null_suite = _candidate_null_suite(
            actual=stress_1_5_events,
            delayed=delayed_events[candidate_id],
            flow_shifted=shifted_events[candidate_id],
            eligible_days=eligible_days,
        )
        if stage2:
            walk_forward = _walk_forward(stress_1_5_events, eligible_days)
            daily = _daily_vector(stress_1_5_events, eligible_days)
            dsr = deflated_sharpe_statistics(daily, n_trials=N_TRIALS)
            p_values[candidate_id] = float(dsr["one_sided_p_value"])
        else:
            walk_forward = _empty_walk_forward()
            dsr = _empty_dsr(len(eligible_days))
            p_values[candidate_id] = 1.0
        rows.append(
            {
                "candidate_id": candidate_id,
                "specification": spec.to_dict(),
                "signal_count": len(signals[candidate_id]),
                "stage0_valid": True,
                "stage1_pass": stage1,
                "stage2_pass": stage2,
                "base": base,
                "stress_1_5x": stress_1_5,
                "stress_2x": stress_2,
                "SIM_EXPLOIT": sim_exploit,
                "trajectory_compliance": compliance,
                "year_results_stress_1_5x": _year_results(
                    stress_1_5_events, eligible_days
                ),
                "candidate_null_suite": null_suite,
                "walk_forward": walk_forward,
                "DSR": dsr,
                "combine_diagnostic": {
                    "status": "DEFERRED_UNTIL_CANDIDATE_PROMOTION",
                    "used_as_fitness": False,
                    "used_as_gate": False,
                },
            }
        )

    bh = benjamini_hochberg(p_values, q=FDR_Q)
    for row in rows:
        row["BH"] = bh[str(row["candidate_id"])]
        gates = _promotion_gates(row)
        row["promotion_gates"] = gates
        row["shadow_queue_eligible"] = all(gates.values())
    selected = _select_distinct(rows, maximum=5)
    result = {
        "schema": "hydra_v7_d1_grammar0002_candidate_tribunal_result_v1",
        "grammar_id": "hydra_v7_d1_microstructure_grammar_0002",
        "verdict": "GREEN" if selected else "NULL",
        "tripwire": {
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "NULL_RATIO": 0.41666666666666663,
            "result_sha256": TRIPWIRE_RESULT_SHA256,
        },
        "N_trials": N_TRIALS,
        "candidate_count": len(rows),
        "signal_count": sum(int(row["signal_count"]) for row in rows),
        "stage1_survivor_count": sum(bool(row["stage1_pass"]) for row in rows),
        "stage2_survivor_count": sum(bool(row["stage2_pass"]) for row in rows),
        "candidate_null_pass_count": sum(
            bool(row["candidate_null_suite"]["passed"]) for row in rows
        ),
        "DSR_positive_count": sum(
            float(row["DSR"]["deflated_z"]) > 0.0 for row in rows
        ),
        "BH_rejection_count": sum(bool(row["BH"]["rejected"]) for row in rows),
        "SIM_EXPLOIT_count": sum(bool(row["SIM_EXPLOIT"]) for row in rows),
        "selected_shadow_queue_candidate_ids": selected,
        "candidate_results": rows,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "combine_pass_rate_used_as_fitness": False,
        "combine_pass_rate_used_as_promotion_gate": False,
        "diff_validation": [
            "hydra/validation/v7_d1_grammar0002_candidate_tribunal.py",
            "scripts/run_v7_d1_grammar0002_candidate_tribunal.py",
            "tests/test_v7_d1_grammar0002_candidate_tribunal.py",
        ],
        "CONTRE": (
            "Only two August year blocks are available. Even a selected "
            "candidate would require a WORM fiche before untouched gap ingestion."
        ),
        "prochaine_action": (
            "freeze_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "tombstone_grammar0002_classes_and_preregister_next_hypothesis"
        ),
    }
    return _write_result(result, output_dir)


def delay_grammar0002_signals_five_sessions(
    signals: Mapping[str, Sequence[D1G2Signal]],
    minute: pd.DataFrame,
) -> dict[str, tuple[D1G2Signal, ...]]:
    cache = _minute_session_cache(minute)
    day_lists = {
        (product, year): tuple(
            sorted(
                day
                for candidate_product, candidate_year, day in cache
                if candidate_product == product and candidate_year == year
            )
        )
        for product, year, _day in cache
    }
    output: dict[str, tuple[D1G2Signal, ...]] = {}
    for candidate_id, candidate_signals in signals.items():
        mapped: list[D1G2Signal] = []
        for signal in candidate_signals:
            source_day = _ct_date(signal.decision_ns)
            days = day_lists[(signal.product, signal.calendar_year)]
            target_day = _fifth_later(days, source_day)
            if target_day is None:
                continue
            target = cache[(signal.product, signal.calendar_year, target_day)]
            entry_minute = _ct_minute(signal.entry_minute_start_ns)
            exit_minute = _ct_minute(signal.exit_minute_start_ns)
            block_minute = _ct_minute(signal.source_block_start_ns)
            entry_rows = target[target["_minute"] == entry_minute]
            exit_rows = target[target["_minute"] == exit_minute]
            block_rows = target[target["_minute"] == block_minute]
            held = target[
                (target["_minute"] >= block_minute)
                & (target["_minute"] <= exit_minute)
            ]
            if (
                len(entry_rows) != 1
                or len(exit_rows) != 1
                or len(block_rows) != 1
                or held.empty
                or len(set(held["contract"].astype(str))) != 1
            ):
                continue
            entry = entry_rows.iloc[0]
            exit_row = exit_rows.iloc[0]
            block = block_rows.iloc[0]
            if str(entry["contract"]) != str(block["contract"]):
                continue
            mapped.append(
                replace(
                    signal,
                    source_block_start_ns=int(block["minute_start_ns"]),
                    source_close_ns=int(entry["minute_start_ns"]),
                    decision_ns=int(entry["minute_start_ns"]),
                    availability_ns=int(entry["minute_start_ns"]),
                    entry_minute_start_ns=int(entry["minute_start_ns"]),
                    exit_minute_start_ns=int(exit_row["minute_start_ns"]),
                    contract=str(entry["contract"]),
                    feature_snapshot_hash=_stable_hash(
                        {
                            "null": "FIVE_SESSION_SIGNAL_DELAY",
                            "target_day": target_day,
                            "entry_minute": entry_minute,
                        }
                    ),
                )
            )
        retained: list[D1G2Signal] = []
        last_exit_by_day: dict[str, int] = {}
        for signal in sorted(mapped, key=lambda row: row.decision_ns):
            day = _ct_date(signal.decision_ns)
            if signal.decision_ns < last_exit_by_day.get(day, -1):
                continue
            retained.append(signal)
            last_exit_by_day[day] = signal.exit_minute_start_ns
        output[candidate_id] = tuple(retained)
    return output


def _candidate_null_suite(
    *,
    actual: Sequence[TradePathEvent],
    delayed: Sequence[TradePathEvent],
    flow_shifted: Sequence[TradePathEvent],
    eligible_days: Sequence[int],
) -> dict[str, Any]:
    actual_metrics = _event_metrics(actual, eligible_days)
    controls = {
        "SIGN_INVERSION": _powered_null(
            tuple(_invert_event(row) for row in actual),
            eligible_days,
            len(actual),
            minimum_retention=1.0,
        ),
        "FIVE_SESSION_SIGNAL_DELAY": _powered_null(
            delayed,
            eligible_days,
            len(actual),
            minimum_retention=0.5,
        ),
        "FLOW_PRICE_DECOUPLING": _powered_null(
            flow_shifted,
            eligible_days,
            len(actual),
            minimum_retention=0.8,
        ),
    }
    power = all(bool(row["power_passed"]) for row in controls.values())
    maximum_null = max(
        (float(row["expectancy_per_trade"]) for row in controls.values()),
        default=0.0,
    )
    actual_expectancy = float(actual_metrics["expectancy_per_trade"])
    return {
        "actual_stress_1_5x_expectancy_per_trade": actual_expectancy,
        "maximum_null_expectancy_per_trade": maximum_null,
        "actual_minus_maximum_null": actual_expectancy - maximum_null,
        "power_all_passed": power,
        "passed": power and actual_expectancy > maximum_null,
        "controls": controls,
    }


def _powered_null(
    events: Sequence[TradePathEvent],
    eligible_days: Sequence[int],
    real_count: int,
    *,
    minimum_retention: float,
) -> dict[str, Any]:
    row = _null_metrics(
        events,
        eligible_days,
        real_count,
        minimum_retention=minimum_retention,
    )
    row["minimum_events"] = MINIMUM_EVENTS
    row["minimum_events_passed"] = int(row["event_count"]) >= MINIMUM_EVENTS
    row["power_passed"] = bool(
        row["retention_passed"] and row["minimum_events_passed"]
    )
    return row


def _trajectory_compliance(
    events: Sequence[TradePathEvent], product: str
) -> bool:
    ordered = sorted(events, key=lambda row: (row.decision_ns, row.event_id))
    overlap = any(
        right.decision_ns < left.exit_ns
        for left, right in zip(ordered, ordered[1:], strict=False)
        if left.session_day == right.session_day
    )
    expected = 1.0 if product == "ES" else 0.1
    return bool(
        not overlap
        and all(row.session_compliant for row in events)
        and all(row.contract_limit_compliant for row in events)
        and all(row.quantity == 1 for row in events)
        and all(math.isclose(row.mini_equivalent, expected) for row in events)
    )


def _promotion_gates(row: Mapping[str, Any]) -> dict[str, bool]:
    return {
        "new_grammar_tripwire_GREEN": True,
        "minimum_events_and_base_expectancy": bool(row["stage1_pass"]),
        "cost_1_5x_positive": float(row["stress_1_5x"]["expectancy_per_trade"])
        > 0.0,
        "SIM_EXPLOIT_2x_survived": not bool(row["SIM_EXPLOIT"]),
        "ruleset_trajectory_compliance": bool(row["trajectory_compliance"]),
        "candidate_null_suite_passed": bool(row["candidate_null_suite"]["passed"]),
        "walk_forward_1_5x_positive": float(
            row["walk_forward"]["pooled_expectancy_per_trade"]
        )
        > 0.0,
        "DSR_deflated_z_gt_0": float(row["DSR"]["deflated_z"]) > 0.0,
        "BH_FDR_10pct_rejected": bool(row["BH"]["rejected"]),
    }


def _select_distinct(
    rows: Sequence[Mapping[str, Any]], *, maximum: int
) -> list[str]:
    eligible = [row for row in rows if bool(row["shadow_queue_eligible"])]
    eligible.sort(
        key=lambda row: (
            -float(row["DSR"]["deflated_z"]),
            -float(row["walk_forward"]["pooled_expectancy_per_trade"]),
            str(row["candidate_id"]),
        )
    )
    selected: list[str] = []
    mechanisms: set[str] = set()
    for row in eligible:
        mechanism = str(row["specification"]["mechanism_class"])
        if mechanism in mechanisms:
            continue
        selected.append(str(row["candidate_id"]))
        mechanisms.add(mechanism)
        if len(selected) >= maximum:
            break
    return selected


def _year_results(
    events: Sequence[TradePathEvent], eligible_days: Sequence[int]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for year in (2023, 2024):
        days = [
            day
            for day in eligible_days
            if pd.Timestamp(day * DAY_NS, unit="ns", tz="UTC").year == year
        ]
        selected = [row for row in events if row.session_day in set(days)]
        output[str(year)] = _event_metrics(selected, days)
    return output


def _minute_session_cache(
    minute: pd.DataFrame,
) -> dict[tuple[str, int, str], pd.DataFrame]:
    frame = minute.copy()
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["_date"] = [value.isoformat() for value in timestamps.date]
    frame["_minute"] = timestamps.hour * 60 + timestamps.minute
    return {
        (str(product), int(year), str(day)): rows.sort_values(
            "minute_start_ns", kind="stable"
        ).reset_index(drop=True)
        for (product, year, day), rows in frame.groupby(
            ["product", "calendar_year", "_date"], sort=True
        )
    }


def _fifth_later(days: Sequence[str], source: str) -> str | None:
    try:
        position = list(days).index(source) + 5
    except ValueError:
        return None
    return str(days[position]) if position < len(days) else None


def _ct_minute(timestamp_ns: int) -> int:
    timestamp = pd.Timestamp(timestamp_ns, unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    )
    return int(timestamp.hour * 60 + timestamp.minute)


def _verify_inputs(
    root: Path,
    paths: Mapping[str, Path],
    proof_registry_path: str | Path,
) -> None:
    expected = {
        "grammar": GRAMMAR_SHA256,
        "tripwire policy": TRIPWIRE_POLICY_SHA256,
        "validation policy": VALIDATION_POLICY_SHA256,
        "null power addendum": NULL_POWER_ADDENDUM_SHA256,
        "signal manifest": SIGNAL_MANIFEST_SHA256,
        "tripwire result": TRIPWIRE_RESULT_SHA256,
    }
    drift = [name for name, path in paths.items() if _sha256(path) != expected[name]]
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        drift.append("mission contract")
    if drift:
        raise D1Grammar0002TribunalError(
            "grammar0002 frozen input hash mismatch: " + ",".join(drift)
        )
    tripwire = json.loads(paths["tripwire result"].read_text(encoding="utf-8"))
    if (
        tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE"
        or not math.isclose(
            float(tripwire.get("NULL_RATIO", -1.0)),
            0.41666666666666663,
            abs_tol=1.0e-15,
        )
    ):
        raise D1Grammar0002TribunalError("grammar0002 tripwire is not frozen GREEN")
    proof = load_and_verify(proof_registry_path)
    reservation = next(
        (
            entry
            for entry in proof.get("entries", ())
            if entry.get("event_id")
            == "v7_d1_microstructure_grammar_0002_multiplicity_reservation"
        ),
        None,
    )
    reserved = (
        int(reservation.get("multiplicity", {}).get("cumulative_N_trials", -1))
        if isinstance(reservation, Mapping)
        else -1
    )
    if reserved != N_TRIALS or multiplicity_trial_count(proof) < N_TRIALS:
        raise D1Grammar0002TribunalError("grammar0002 multiplicity mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise D1Grammar0002TribunalError("unexpected proof-window state")


def _verify_signal_manifest(
    path: Path, signals: Mapping[str, Sequence[D1G2Signal]]
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise D1Grammar0002TribunalError("grammar0002 signal logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload.get("signals"):
        raise D1Grammar0002TribunalError("grammar0002 signal regeneration drift")


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "d1_grammar0002_candidate_tribunal_result.json"
    report_path = destination / "d1_grammar0002_candidate_tribunal_report.md"
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
            "# HYDRA V7 — D1 grammar 0002 candidate tribunal",
            "",
            f"[HYDRA-V7] phase=4 step=3 verdict={result['verdict']}",
            "gate=D1_GRAMMAR0002_CANDIDATES preuve=reports/v7/data/d1_grammar0002_candidate_tribunal_result.json#pending tests=pending",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v7_d1_grammar0002_candidate_tribunal.py,scripts/run_v7_d1_grammar0002_candidate_tribunal.py,tests/test_v7_d1_grammar0002_candidate_tribunal.py CONTRE=deux_blocs_aout_limitent_la_puissance",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Structures : `{result['candidate_count']}`",
            f"- Signaux : `{result['signal_count']}`",
            f"- Stage 1 : `{result['stage1_survivor_count']}`",
            f"- Stage 2 : `{result['stage2_survivor_count']}`",
            f"- Null suite : `{result['candidate_null_pass_count']}`",
            f"- DSR positifs : `{result['DSR_positive_count']}`",
            f"- BH : `{result['BH_rejection_count']}`",
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
    "D1Grammar0002TribunalError",
    "delay_grammar0002_signals_five_sessions",
    "run_d1_grammar0002_candidate_tribunal",
]
