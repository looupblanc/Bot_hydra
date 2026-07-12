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
from hydra.research.v7_d1_microstructure_grammar import (
    D1CandidateSpec,
    D1Signal,
    candidate_specs,
    generate_signal_population,
    load_feature_store,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    DAY_NS,
    MINI_EQUIVALENT,
    _ct_date,
    _day_id,
    _eligible_days_by_year,
    build_candidate_events,
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


GRAMMAR_SHA256 = "f7b7f3d8d0a43749d31986e4848eeb7285123654dd9ca47eb327284831dd1691"
TRIPWIRE_POLICY_SHA256 = "8bf84b324be6292de1213e5ade0d88bf1f83cae67502ef4ff7321c8c50c4be7d"
VALIDATION_POLICY_SHA256 = "f35fd49a91c38abfb975cacd1270249801d3e6289525559400155232ea7079ab"
EXECUTION_ADDENDUM_SHA256 = "cf4d0a4c355e58279439c1ead1c9dd6bd875a0b3deacec0432315fe716abd5f1"
SIGNAL_MANIFEST_SHA256 = "448017ea1afce1630ff722c8ea1df677775db20253c32e24a9b85429305db235"
TRIPWIRE_RESULT_SHA256 = "99caf1885687d8ecaa79f77a768256753a89133275c6c9a9d206a747203c4f0a"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
N_TRIALS = 247_684
FDR_Q = 0.10
MINIMUM_NULL_RETENTION = 0.80


class D1CandidateTribunalError(RuntimeError):
    pass


def run_d1_candidate_tribunal(
    *,
    project_root: str | Path,
    grammar_path: str | Path,
    tripwire_policy_path: str | Path,
    validation_policy_path: str | Path,
    execution_addendum_path: str | Path,
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
        "execution addendum": Path(execution_addendum_path).resolve(),
        "signal manifest": Path(signal_manifest_path).resolve(),
        "tripwire result": Path(tripwire_result_path).resolve(),
    }
    _verify_inputs(root, paths, proof_registry_path)
    minute, event = load_feature_store(root)
    signals = generate_signal_population(minute, event)
    _verify_signal_manifest(paths["signal manifest"], signals)
    specs = {row.candidate_id: row for row in candidate_specs()}
    cost_model = load_cost_model()
    events_by_stress = {
        stress: build_candidate_events(
            minute,
            event,
            signals,
            specs,
            cost_model,
            stress=stress,
        )
        for stress in CostStress
    }
    eligible_days = tuple(
        sorted(
            day
            for rows in _eligible_days_by_year(minute).values()
            for day in rows
        )
    )
    delayed_signals = _delay_signals_five_sessions(signals, minute, event)
    delayed_events = build_candidate_events(
        minute,
        event,
        delayed_signals,
        specs,
        cost_model,
        stress=CostStress.STRESS_1_5X,
    )
    shifted_minute, shifted_event = _shift_flow_one_prior_session(minute, event)
    shifted_signals = generate_signal_population(shifted_minute, shifted_event)
    shifted_events = build_candidate_events(
        shifted_minute,
        shifted_event,
        shifted_signals,
        specs,
        cost_model,
        stress=CostStress.STRESS_1_5X,
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
            int(base["event_count"]) >= 30
            and float(base["expectancy_per_trade"]) > 0.0
        )
        compliance = _d1_event_compliance(base_events, spec.product)
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
        "schema": "hydra_v7_d1_candidate_tribunal_result_v1",
        "grammar_id": "hydra_v7_d1_microstructure_grammar_0001",
        "verdict": "GREEN" if selected else "NULL",
        "tripwire": {
            "verdict": "GREEN_NULL_ADJUSTED_BASELINE",
            "NULL_RATIO": 0.0,
            "result_sha256": TRIPWIRE_RESULT_SHA256,
            "real_episode_count": 160,
            "pooled_null_episode_count": 480,
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
            "hydra/validation/v7_d1_candidate_tribunal.py",
            "scripts/run_v7_d1_candidate_tribunal.py",
            "tests/test_v7_d1_candidate_tribunal.py",
        ],
        "CONTRE": (
            "Only two August blocks provide daily observations, so DSR has low "
            "power under 247684 historical trials. A survivor would still need "
            "a WORM fiche before any untouched gap ingestion."
        ),
        "prochaine_action": (
            "freeze_candidate_fiches_WORM_before_gap_ingestion"
            if selected
            else "tombstone_D1_classes_and_report_current_data_scope_null"
        ),
    }
    return _write_result(result, output_dir)


def _candidate_null_suite(
    *,
    actual: Sequence[TradePathEvent],
    delayed: Sequence[TradePathEvent],
    flow_shifted: Sequence[TradePathEvent],
    eligible_days: Sequence[int],
) -> dict[str, Any]:
    actual_metrics = _event_metrics(actual, eligible_days)
    controls = {
        "SIGN_INVERSION": _null_metrics(
            tuple(_invert_event(row) for row in actual),
            eligible_days,
            len(actual),
            minimum_retention=1.0,
        ),
        "FIVE_SESSION_SIGNAL_DELAY": _null_metrics(
            delayed,
            eligible_days,
            len(actual),
            minimum_retention=MINIMUM_NULL_RETENTION,
        ),
        "FLOW_PRICE_DECOUPLING": _null_metrics(
            flow_shifted,
            eligible_days,
            len(actual),
            minimum_retention=MINIMUM_NULL_RETENTION,
        ),
    }
    retention = all(bool(row["retention_passed"]) for row in controls.values())
    maximum_null = max(
        (float(row["expectancy_per_trade"]) for row in controls.values()),
        default=0.0,
    )
    actual_expectancy = float(actual_metrics["expectancy_per_trade"])
    return {
        "actual_stress_1_5x_expectancy_per_trade": actual_expectancy,
        "maximum_null_expectancy_per_trade": maximum_null,
        "actual_minus_maximum_null": actual_expectancy - maximum_null,
        "retention_all_passed": retention,
        "passed": retention and actual_expectancy > maximum_null,
        "controls": controls,
    }


def _delay_signals_five_sessions(
    signals: Mapping[str, Sequence[D1Signal]],
    minute: pd.DataFrame,
    event: pd.DataFrame,
) -> dict[str, tuple[D1Signal, ...]]:
    minute_days = _minute_decision_rows(minute)
    minute_day_lists = {
        (product, year): tuple(
            sorted(
                date
                for candidate_product, candidate_year, date in minute_days
                if candidate_product == product and candidate_year == year
            )
        )
        for product, year, _date in minute_days
    }
    event_groups = {
        (str(product), str(bar_type), int(year)): _event_with_clock(frame)
        for (product, bar_type, year), frame in event.groupby(
            ["product", "bar_type", "calendar_year"], sort=True
        )
    }
    event_day_lists = {
        key: tuple(sorted(set(frame["_local_date"].astype(str))))
        for key, frame in event_groups.items()
    }
    output: dict[str, tuple[D1Signal, ...]] = {}
    for candidate_id, rows in signals.items():
        delayed: list[D1Signal] = []
        for signal in rows:
            source_date = _ct_date(signal.decision_ns)
            source_minute = _ct_minute(signal.decision_ns)
            if signal.source_bar_type == "MINUTE_PRINT_FEATURES":
                days = minute_day_lists[(signal.product, signal.calendar_year)]
                target = _fifth_later(days, source_date)
                if target is None:
                    continue
                row = minute_days.get((signal.product, signal.calendar_year, target))
                if row is None:
                    continue
                delayed.append(
                    replace(
                        signal,
                        source_position=days.index(target),
                        decision_ns=int(row["availability_ns"]),
                        availability_ns=int(row["availability_ns"]),
                        contract=str(row["contract"]),
                        feature_snapshot_hash=_stable_hash(
                            {"null": "FIVE_SESSION_DELAY", "target": target}
                        ),
                    )
                )
            else:
                key = (signal.product, signal.source_bar_type, signal.calendar_year)
                frame = event_groups[key]
                days = event_day_lists[key]
                target = _fifth_later(days, source_date)
                if target is None:
                    continue
                candidates = frame[
                    (frame["_local_date"] == target)
                    & (frame["_local_minute"] >= source_minute)
                ]
                if candidates.empty:
                    continue
                target_position = int(candidates.index[0])
                target_row = frame.loc[target_position]
                delayed.append(
                    replace(
                        signal,
                        source_position=target_position,
                        decision_ns=int(target_row["availability_ns"]),
                        availability_ns=int(target_row["availability_ns"]),
                        contract=str(target_row["contract"]),
                        feature_snapshot_hash=_stable_hash(
                            {
                                "null": "FIVE_SESSION_DELAY",
                                "target": target,
                                "minute": source_minute,
                            }
                        ),
                    )
                )
        output[candidate_id] = _drop_overlapping_delayed_signals(
            delayed, event_groups
        )
    return output


def _drop_overlapping_delayed_signals(
    signals: Sequence[D1Signal],
    event_groups: Mapping[tuple[str, str, int], pd.DataFrame],
) -> tuple[D1Signal, ...]:
    """Preserve the earliest mapped null signal when target-session paths collide.

    Event density differs across sessions.  Mapping two non-overlapping source
    signals to the same local clocks five sessions later can therefore compress
    their conservative holding paths.  The frozen null requires valid,
    non-overlapping execution and separately enforces at least 80% retention;
    deterministically dropping the later collision preserves both constraints.
    """

    retained: list[D1Signal] = []
    last_exit_by_day: dict[str, int] = {}
    for signal in sorted(
        signals,
        key=lambda row: (row.decision_ns, row.candidate_id, row.source_position),
    ):
        day = _ct_date(signal.decision_ns)
        if signal.source_bar_type == "MINUTE_PRINT_FEATURES":
            # There is at most one frozen cash-open signal per candidate/day.
            exit_ns = signal.decision_ns + 61 * 60 * 1_000_000_000
        else:
            key = (
                signal.product,
                signal.source_bar_type,
                signal.calendar_year,
            )
            frame = event_groups[key]
            position = int(signal.source_position)
            starts = frame["start_event_ns"].to_numpy(dtype=np.int64)
            entry = max(
                position + 1,
                int(np.searchsorted(starts, signal.availability_ns, side="left")),
            )
            exit_position = entry + int(signal.holding_units)
            if exit_position >= len(frame):
                continue
            contracts = frame["contract"].astype(str).to_numpy()
            if (
                len(set(contracts[position : exit_position + 1])) != 1
                or contracts[entry] != signal.contract
            ):
                continue
            exit_ns = int(frame.iloc[exit_position]["start_event_ns"])
        if signal.decision_ns < last_exit_by_day.get(day, -1):
            continue
        retained.append(signal)
        last_exit_by_day[day] = exit_ns
    return tuple(retained)


def _shift_flow_one_prior_session(
    minute: pd.DataFrame, event: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return (
        _shift_frame_flow(minute, "minute_start_ns"),
        _shift_frame_flow(event, "start_event_ns"),
    )


def _shift_frame_flow(frame: pd.DataFrame, timestamp_column: str) -> pd.DataFrame:
    output = frame.copy()
    timestamps = pd.to_datetime(
        output[timestamp_column].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    output["_local_date"] = [value.isoformat() for value in timestamps.date]
    group_columns = ["calendar_year", "product"]
    if "bar_type" in output.columns:
        group_columns.append("bar_type")
    flow_columns = [
        "trade_count",
        "total_volume",
        "buy_aggressor_volume",
        "sell_aggressor_volume",
        "unknown_side_volume",
        "signed_aggressor_volume",
    ]
    if "signed_aggressor_fraction" in output.columns:
        flow_columns.append("signed_aggressor_fraction")
    for _, group in output.groupby(group_columns, sort=True):
        day_groups = [
            np.asarray(indices, dtype=np.int64)
            for _, indices in group.groupby("_local_date", sort=True).groups.items()
        ]
        if not day_groups:
            continue
        output.loc[day_groups[0], flow_columns] = 0
        for donor, recipient in zip(day_groups, day_groups[1:], strict=False):
            source_positions = np.rint(
                np.linspace(0, len(donor) - 1, len(recipient))
            ).astype(np.int64)
            for column in flow_columns:
                output.loc[recipient, column] = output.loc[
                    donor[source_positions], column
                ].to_numpy()
    return output.drop(columns=["_local_date"])


def _minute_decision_rows(
    minute: pd.DataFrame,
) -> dict[tuple[str, int, str], Mapping[str, Any]]:
    frame = minute.copy()
    timestamps = pd.to_datetime(
        frame["minute_start_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    frame["_local_date"] = [value.isoformat() for value in timestamps.date]
    frame["_local_minute"] = timestamps.hour * 60 + timestamps.minute
    selected = frame[frame["_local_minute"] == 9 * 60]
    return {
        (str(row["product"]), int(row["calendar_year"]), str(row["_local_date"])): row
        for _, row in selected.iterrows()
    }


def _event_with_clock(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.reset_index(drop=True).copy()
    timestamps = pd.to_datetime(
        output["availability_ns"].to_numpy(dtype=np.int64), unit="ns", utc=True
    ).tz_convert("America/Chicago")
    output["_local_date"] = [value.isoformat() for value in timestamps.date]
    output["_local_minute"] = timestamps.hour * 60 + timestamps.minute
    return output


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


def _d1_event_compliance(
    events: Sequence[TradePathEvent], product: str
) -> bool:
    ordered = sorted(events, key=lambda row: (row.decision_ns, row.event_id))
    overlap = any(
        right.decision_ns < left.exit_ns
        for left, right in zip(ordered, ordered[1:], strict=False)
        if left.session_day == right.session_day
    )
    expected = MINI_EQUIVALENT[product]
    return bool(
        not overlap
        and all(row.session_compliant for row in events)
        and all(row.contract_limit_compliant for row in events)
        and all(row.quantity == 1 for row in events)
        and all(math.isclose(row.mini_equivalent, expected) for row in events)
    )


def _promotion_gates(row: Mapping[str, Any]) -> dict[str, bool]:
    return {
        "new_dataset_tripwire_GREEN": True,
        "minimum_events_and_base_expectancy": bool(row["stage1_pass"]),
        "cost_1_5x_positive": float(
            row["stress_1_5x"]["expectancy_per_trade"]
        )
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


def _verify_inputs(
    root: Path,
    paths: Mapping[str, Path],
    proof_registry_path: str | Path,
) -> None:
    expected = {
        "grammar": GRAMMAR_SHA256,
        "tripwire policy": TRIPWIRE_POLICY_SHA256,
        "validation policy": VALIDATION_POLICY_SHA256,
        "execution addendum": EXECUTION_ADDENDUM_SHA256,
        "signal manifest": SIGNAL_MANIFEST_SHA256,
        "tripwire result": TRIPWIRE_RESULT_SHA256,
    }
    drift = [name for name, path in paths.items() if _sha256(path) != expected[name]]
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        drift.append("mission contract")
    if drift:
        raise D1CandidateTribunalError(
            "D1 candidate frozen input hash mismatch: " + ",".join(drift)
        )
    tripwire = json.loads(paths["tripwire result"].read_text(encoding="utf-8"))
    if (
        tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE"
        or float(tripwire.get("NULL_RATIO", -1.0)) != 0.0
        or int(tripwire.get("real", {}).get("pass_count", 0)) != 6
    ):
        raise D1CandidateTribunalError("D1 tripwire is not frozen GREEN")
    proof = load_and_verify(proof_registry_path)
    reservation = next(
        (
            entry
            for entry in proof.get("entries", ())
            if entry.get("event_id")
            == "v7_d1_microstructure_grammar_0001_multiplicity_reservation"
        ),
        None,
    )
    reserved = (
        int(reservation.get("multiplicity", {}).get("cumulative_N_trials", -1))
        if isinstance(reservation, Mapping)
        else -1
    )
    if reserved != N_TRIALS or multiplicity_trial_count(proof) < N_TRIALS:
        raise D1CandidateTribunalError("D1 multiplicity reservation mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise D1CandidateTribunalError("unexpected D1 proof-window state")


def _verify_signal_manifest(
    path: Path, signals: Mapping[str, Sequence[D1Signal]]
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if expected != _stable_hash(unhashed):
        raise D1CandidateTribunalError("D1 signal logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload.get("signals"):
        raise D1CandidateTribunalError("D1 signal regeneration drift")


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "d1_candidate_tribunal_result.json"
    report_path = destination / "d1_candidate_tribunal_report.md"
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
            "# HYDRA V7 — D1 candidate tribunal",
            "",
            f"[HYDRA-V7] phase=D step=5 verdict={result['verdict']}",
            "gate=D1_CANDIDATES preuve=reports/v7/data/d1_candidate_tribunal_result.json#pending tests=pending",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v7_d1_candidate_tribunal.py,scripts/run_v7_d1_candidate_tribunal.py,tests/test_v7_d1_candidate_tribunal.py CONTRE=deux_blocs_aout_limitent_la_puissance_DSR",
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
    "D1CandidateTribunalError",
    "run_d1_candidate_tribunal",
]
