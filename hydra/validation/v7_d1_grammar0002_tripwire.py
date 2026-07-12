from __future__ import annotations

import hashlib
import json
import os
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
from hydra.research.v7_d1_microstructure_grammar import load_feature_store
from hydra.research.v7_d1_microstructure_grammar_0002 import (
    D1G2CandidateSpec,
    D1G2Signal,
    candidate_specs,
    generate_signal_population,
)
from hydra.validation.v7_d1_new_dataset_tripwire import (
    D1NullControl,
    MINI_EQUIVALENT,
    POINT_VALUES,
    _eligible_days_by_year,
    _evaluate_world,
    _within_year_price_null,
    _year_permuted_prices,
)


GRAMMAR_SHA256 = "fac0b5166351940d1fde5334bdeaf846d56e56efc8cef9772a9599b8b86feee9"
TRIPWIRE_POLICY_SHA256 = "a2e5c568345b0250255c59f6829da20cab9b280a65b1cbfcfa727343f0d023b7"
VALIDATION_POLICY_SHA256 = "14fd8c2df5a4fd33816236d9cdc72af3a2c7595f52d6b62fd7de730c60f0e525"
NULL_POWER_ADDENDUM_SHA256 = "3a94a683219a0762577a106982023dad36df928b994e8852af51cdd1bd455bd3"
SIGNAL_MANIFEST_SHA256 = "db05bdcbe830d4079881fe03ae61759d622c45876468a113bd4344ee50dc1aea"
CONTRACT_SHA256 = "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
N_TRIALS = 247_892
NULL_THRESHOLD = 0.80
SHUFFLE_SEEDS = {
    (2023, "ES"): 810301,
    (2023, "MES"): 810302,
    (2024, "ES"): 810401,
    (2024, "MES"): 810402,
}
RANDOM_SEEDS = {
    (2023, "ES"): 820301,
    (2023, "MES"): 820302,
    (2024, "ES"): 820401,
    (2024, "MES"): 820402,
}


class D1Grammar0002TripwireError(RuntimeError):
    pass


def run_d1_grammar0002_tripwire(
    *,
    project_root: str | Path,
    grammar_path: str | Path,
    tripwire_policy_path: str | Path,
    validation_policy_path: str | Path,
    null_power_addendum_path: str | Path,
    signal_manifest_path: str | Path,
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
    }
    _verify_inputs(root, paths, proof_registry_path)
    minute, _event = load_feature_store(root)
    real_signals = generate_signal_population(minute, project_root=root)
    _verify_signal_manifest(paths["signal manifest"], real_signals, root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    costs = load_cost_model()
    real_events = build_grammar0002_events(
        minute, real_signals, specs, costs, stress=CostStress.BASE
    )
    eligible = _eligible_days_by_year(minute)
    real = _evaluate_world(real_events, eligible)

    controls: dict[str, Any] = {}
    pooled_passes = 0
    pooled_episodes = 0
    for control in D1NullControl:
        null_minute = _null_minute_world(minute, control)
        null_signals = generate_signal_population(null_minute, project_root=root)
        null_events = build_grammar0002_events(
            null_minute, null_signals, specs, costs, stress=CostStress.BASE
        )
        summary = _evaluate_world(null_events, eligible)
        summary["signal_count"] = sum(len(rows) for rows in null_signals.values())
        controls[control.value] = summary
        pooled_passes += int(summary["pass_count"])
        pooled_episodes += int(summary["episode_count"])

    real_rate = float(real["pass_rate"])
    pooled_rate = pooled_passes / max(pooled_episodes, 1)
    if int(real["pass_count"]) == 0:
        verdict = "BLOCKED_UNDERPOWERED"
        null_ratio = None
    else:
        null_ratio = pooled_rate / real_rate
        verdict = (
            "ARTEFACT_GEOMETRY_ONLY"
            if null_ratio >= NULL_THRESHOLD
            else "GREEN_NULL_ADJUSTED_BASELINE"
        )
    if pooled_episodes < int(real["episode_count"]):
        raise D1Grammar0002TripwireError(
            "grammar0002 null episode count is below real"
        )
    result = {
        "schema": "hydra_v7_d1_grammar0002_tripwire_result_v1",
        "tripwire_id": "hydra_v7_d1_new_grammar_tripwire_0002",
        "grammar_id": "hydra_v7_d1_microstructure_grammar_0002",
        "verdict": verdict,
        "threshold": NULL_THRESHOLD,
        "NULL_RATIO": null_ratio,
        "real": {
            **real,
            "signal_count": sum(len(rows) for rows in real_signals.values()),
        },
        "pooled_null": {
            "episode_count": pooled_episodes,
            "pass_count": pooled_passes,
            "pass_rate": pooled_rate,
        },
        "controls": controls,
        "candidate_count": len(specs),
        "feature_and_signal_recomputation": True,
        "combine_pass_rate_is_diagnostic_not_fitness": True,
        "candidate_validation_executed": False,
        "N_trials": N_TRIALS,
        "q4_access_count_delta": 0,
        "forward_gap_access_count": 0,
        "proof_window_burn_delta": 0,
        "outbound_order_count": 0,
        "diff_validation": [
            "hydra/validation/v7_d1_grammar0002_tripwire.py",
            "scripts/run_v7_d1_grammar0002_tripwire.py",
            "tests/test_v7_d1_grammar0002_tripwire.py",
        ],
        "CONTRE": (
            "Only two August blocks are available. A GREEN tripwire rejects "
            "gross account geometry but does not validate any candidate edge."
        ),
        "prochaine_action": (
            "run_separately_committed_grammar0002_candidate_tribunal"
            if verdict == "GREEN_NULL_ADJUSTED_BASELINE"
            else "freeze_grammar0002_tripwire_and_do_not_validate_candidates"
        ),
    }
    return _write_result(result, output_dir)


def build_grammar0002_events(
    minute: pd.DataFrame,
    signals: Mapping[str, Sequence[D1G2Signal]],
    specs: Mapping[str, D1G2CandidateSpec],
    cost_model: V7CostModel,
    *,
    stress: CostStress,
) -> dict[str, tuple[TradePathEvent, ...]]:
    cache = {
        (str(product), int(year)): frame.sort_values(
            "minute_start_ns", kind="stable"
        ).reset_index(drop=True)
        for (product, year), frame in minute.groupby(
            ["product", "calendar_year"], sort=True
        )
    }
    output: dict[str, tuple[TradePathEvent, ...]] = {}
    for candidate_id, spec in sorted(specs.items()):
        events: list[TradePathEvent] = []
        for signal in signals[candidate_id]:
            frame = cache[(signal.product, signal.calendar_year)]
            starts = frame["minute_start_ns"].to_numpy(dtype=np.int64)
            entry = int(np.searchsorted(starts, signal.entry_minute_start_ns))
            exit_position = int(
                np.searchsorted(starts, signal.exit_minute_start_ns)
            )
            if (
                entry >= len(frame)
                or exit_position >= len(frame)
                or int(starts[entry]) != signal.entry_minute_start_ns
                or int(starts[exit_position]) != signal.exit_minute_start_ns
                or exit_position <= entry
            ):
                raise D1Grammar0002TripwireError("grammar0002 execution timestamp drift")
            held = frame.iloc[entry:exit_position]
            contracts = frame.iloc[entry : exit_position + 1]["contract"].astype(str)
            if len(set(contracts)) != 1 or str(contracts.iloc[0]) != signal.contract:
                raise D1Grammar0002TripwireError("grammar0002 explicit contract drift")
            entry_price = float(frame.iloc[entry]["open"])
            exit_price = float(frame.iloc[exit_position]["open"])
            point_value = POINT_VALUES[signal.product]
            cost = cost_model.round_turn_cost(
                signal.product,
                spec.cost_horizon,
                stress=stress,
                contracts=1.0,
            )
            scale = signal.side * point_value
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
            day = _session_day(signal.decision_ns)
            events.append(
                TradePathEvent(
                    event_id=f"{candidate_id}:{signal.decision_ns}:{stress.value}",
                    decision_ns=signal.decision_ns,
                    exit_ns=signal.exit_minute_start_ns,
                    session_day=day,
                    net_pnl=float(net),
                    gross_pnl=float(gross),
                    worst_unrealized_pnl=float(
                        min((adverse_price - entry_price) * scale - cost, net, 0.0)
                    ),
                    best_unrealized_pnl=float(
                        max((favorable_price - entry_price) * scale - cost, net, 0.0)
                    ),
                    quantity=1,
                    mini_equivalent=MINI_EQUIVALENT[signal.product],
                    regime=spec.hypothesis_id,
                    session_compliant=True,
                    contract_limit_compliant=True,
                    same_bar_ambiguous=False,
                )
            )
        ordered = tuple(sorted(events, key=lambda row: (row.decision_ns, row.event_id)))
        if any(
            right.decision_ns < left.exit_ns
            for left, right in zip(ordered, ordered[1:], strict=False)
            if left.session_day == right.session_day
        ):
            raise D1Grammar0002TripwireError("grammar0002 candidate events overlap")
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
    }
    drift = [name for name, path in paths.items() if _sha256(path) != expected[name]]
    if _sha256(root / "MISSION_CONTRACT.md") != CONTRACT_SHA256:
        drift.append("mission contract")
    if drift:
        raise D1Grammar0002TripwireError(
            "grammar0002 frozen input hash mismatch: " + ",".join(drift)
        )
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
        raise D1Grammar0002TripwireError("grammar0002 multiplicity mismatch")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise D1Grammar0002TripwireError("unexpected proof-window state")


def _verify_signal_manifest(
    path: Path,
    signals: Mapping[str, Sequence[D1G2Signal]],
    root: Path,
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    stored_hash = str(payload.get("manifest_hash") or "")
    unhashed = dict(payload)
    unhashed.pop("manifest_hash", None)
    if stored_hash != _stable_hash(unhashed):
        raise D1Grammar0002TripwireError("grammar0002 signal logical hash mismatch")
    regenerated = {
        candidate_id: [row.to_dict() for row in rows]
        for candidate_id, rows in signals.items()
    }
    if regenerated != payload.get("signals"):
        raise D1Grammar0002TripwireError("grammar0002 signal regeneration drift")
    if payload.get("contains_outcomes_or_pnl") is not False:
        raise D1Grammar0002TripwireError("grammar0002 signal manifest contains outcomes")


def _session_day(timestamp_ns: int) -> int:
    timestamp = pd.Timestamp(timestamp_ns, unit="ns", tz="UTC").tz_convert(
        "America/Chicago"
    )
    date = pd.Timestamp(timestamp.date(), tz="UTC")
    return int(date.value // 86_400_000_000_000)


def _write_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "d1_grammar0002_tripwire_result.json"
    report_path = destination / "d1_grammar0002_tripwire_report.md"
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
            "# HYDRA V7 — D1 grammar 0002 tripwire",
            "",
            f"[HYDRA-V7] phase=4 step=1 verdict={result['verdict']}",
            "gate=D1_GRAMMAR0002_TRIPWIRE preuve=reports/v7/data/d1_grammar0002_tripwire_result.json#pending tests=pending",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00 N_trials={result['N_trials']} burned=1",
            "diff_validation=hydra/validation/v7_d1_grammar0002_tripwire.py,scripts/run_v7_d1_grammar0002_tripwire.py,tests/test_v7_d1_grammar0002_tripwire.py CONTRE=deux_blocs_aout_ne_prouvent_pas_la_stabilite",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Real episodes: `{result['real']['episode_count']}`",
            f"- Real passes: `{result['real']['pass_count']}`",
            f"- Null episodes: `{result['pooled_null']['episode_count']}`",
            f"- Null passes: `{result['pooled_null']['pass_count']}`",
            f"- NULL_RATIO: `{result['NULL_RATIO']}`",
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
    "D1Grammar0002TripwireError",
    "build_grammar0002_events",
    "run_d1_grammar0002_tripwire",
]
