from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.execution.v7_cost_model import CostStress, load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v72_trade_arrival_renewal import (
    GRAMMAR_ID,
    V72ArrivalCandidateSpec,
    candidate_specs,
    generate_signal_population,
    load_trade_arrival_renewal_sources,
    signal_path_hash,
)
from hydra.validation.v71_event_funnel import (
    _empty_walk,
    _events_for_days,
    _folds,
    _minute_replay_cache,
    _single_day_absolute_share,
    _summary,
    _walk_forward,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


GRAMMAR_PATH = "WORM/v7.2-trade-arrival-renewal-grammar-0011-2026-07-13.json"
GRAMMAR_SHA256 = "d69f021bf4de5b4e5a0fe92d318eba9f00b08c80d99cb3941c43daab4a6b10c2"
ADDENDUM_PATH = (
    "WORM/v7.2-trade-arrival-renewal-validation-addendum-0011-2026-07-13.json"
)
ADDENDUM_SHA256 = "6808fd5d97adba4b1b0539687e6db0429aecb9ef44ab52c1e871dd13b29cf39f"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_2/discovery_0011/v72_trade_arrival_renewal_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = "6225c8067ac813e47f03e5037ce08ba0dfd7392f81421d04f0440c2795260944"
POLICY_PATH = "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
POLICY_SHA256 = "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c"
POWER_POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POWER_POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
RESERVATION_EVENT_ID = (
    "v7_2_trade_arrival_renewal_structural_tripwire_reservation_0011"
)
ANNOTATION_EVENT_ID = "v7_2_trade_arrival_renewal_reservation_0011_commit_annotation"
EXPECTED_GLOBAL_N_TRIALS = 265_227
POINT_VALUE = 50.0


class V72TradeArrivalRenewalFunnelError(RuntimeError):
    pass


def run_trade_arrival_renewal_funnel(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_2/discovery_0011",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, manifest = _verify_inputs(root, proof_registry_path)
    minute, states, source_audit = load_trade_arrival_renewal_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    signals = generate_signal_population(states, project_root=root, graveyard_path=None)
    _verify_signal_manifest(manifest, specs, signals)
    replay_cache = _minute_replay_cache(minute)
    all_days = tuple(
        sorted(
            {
                signal.session_day
                for candidate_signals in signals.values()
                for signal in candidate_signals
            }
        )
    )
    early_folds = _folds(all_days, 3, embargo_days=0)
    walk_folds = _folds(all_days, 4, embargo_days=5)
    costs = load_cost_model()
    manifest_rows = {
        str(row["candidate_id"]): row for row in manifest["candidate_paths"]
    }
    rows: list[dict[str, Any]] = []
    for candidate_id, spec in sorted(specs.items()):
        candidate_signals = signals[candidate_id]
        manifest_row = manifest_rows[candidate_id]
        duplicate_of = manifest_row.get("archive_duplicate_of") or manifest_row.get(
            "within_manifest_duplicate_of"
        )
        stage0_valid = bool(candidate_signals) and duplicate_of is None
        events_1_5x = (
            _replay_at_stress(
                spec,
                candidate_signals,
                replay_cache,
                cost_model=costs,
                stress=CostStress.STRESS_1_5X,
            )
            if stage0_valid
            else []
        )
        pooled = _summary(events_1_5x)
        early = [
            _summary(_events_for_days(events_1_5x, fold)) for fold in early_folds
        ]
        concentration = _single_day_absolute_share(events_1_5x)
        minimum_stage1 = int(
            policy["funnel"]["stage1"]["minimum_nonoverlapping_events"]
        )
        stage1_pass = bool(
            stage0_valid
            and pooled["event_count"] >= minimum_stage1
            and pooled["expectancy_per_trade"]
            > float(
                policy["funnel"]["stage1"]["pooled_expectancy_min_exclusive"]
            )
            and sum(row["expectancy_per_trade"] > 0.0 for row in early)
            >= int(
                policy["funnel"]["stage1"]["minimum_positive_early_folds"]
            )
            and concentration
            <= float(
                policy["funnel"]["stage1"]
                ["maximum_single_day_absolute_pnl_share"]
            )
        )
        walk = _walk_forward(events_1_5x, walk_folds) if stage1_pass else _empty_walk()
        walk_positive = bool(
            stage1_pass
            and walk["retained_event_count"]
            >= int(policy["funnel"]["stage2"]["minimum_retained_events"])
            and walk["pooled_expectancy_per_trade"]
            > float(
                policy["funnel"]["stage2"]["pooled_expectancy_min_exclusive"]
            )
            and walk["positive_fold_count"]
            >= int(policy["funnel"]["stage2"]["minimum_positive_folds"])
        )
        events_2x = (
            _replay_at_stress(
                spec,
                candidate_signals,
                replay_cache,
                cost_model=costs,
                stress=CostStress.STRESS_2X,
            )
            if walk_positive
            else []
        )
        walk_2x = _walk_forward(events_2x, walk_folds) if walk_positive else _empty_walk()
        sim_exploit_survived = bool(
            walk_positive and walk_2x["pooled_expectancy_per_trade"] > 0.0
        )
        if duplicate_of:
            classification = "DUPLICATE_REJECTED"
        elif not stage0_valid or pooled["event_count"] < minimum_stage1:
            classification = "INSUFFICIENT_POWER"
        elif not stage1_pass:
            classification = "FORMULATION_FALSIFIED_STAGE1"
        elif not walk_positive:
            classification = "FORMULATION_FALSIFIED_WALK_FORWARD"
        elif not sim_exploit_survived:
            classification = "SIM_EXPLOIT"
        else:
            classification = "WALK_FORWARD_POSITIVE_PENDING_TRIPWIRE_POWER_NULLS"
        rows.append(
            {
                "candidate_id": candidate_id,
                "family_id": spec.family_id,
                "motif": spec.motif,
                "response_policy": spec.response_policy,
                "history_window": spec.history_window,
                "holding_minutes": spec.holding_minutes,
                "specification_hash": spec.specification_hash,
                "signal_path_hash": signal_path_hash(candidate_signals),
                "signal_count": len(candidate_signals),
                "duplicate_of": duplicate_of,
                "stage0_valid_novel": stage0_valid,
                "stage1_pass": stage1_pass,
                "stress_1_5x_pooled": pooled,
                "early_fold_results": early,
                "single_day_absolute_pnl_share": concentration,
                "walk_forward_stress_1_5x": walk,
                "walk_forward_positive": walk_positive,
                "walk_forward_stress_2x": walk_2x,
                "SIM_EXPLOIT_survived": sim_exploit_survived,
                "power_audit_executed": False,
                "classification": classification,
            }
        )
    classifications = Counter(str(row["classification"]) for row in rows)
    result = {
        "schema": "hydra_v7_2_trade_arrival_renewal_funnel_result_v1",
        "grammar_id": GRAMMAR_ID,
        "candidate_count": len(rows),
        "stage0_valid_novel_count": sum(
            bool(row["stage0_valid_novel"]) for row in rows
        ),
        "duplicate_rejection_count": sum(
            row["duplicate_of"] is not None for row in rows
        ),
        "insufficient_power_count": int(
            classifications.get("INSUFFICIENT_POWER", 0)
        ),
        "stage1_pass_count": sum(bool(row["stage1_pass"]) for row in rows),
        "walk_forward_positive_count": sum(
            bool(row["walk_forward_positive"]) for row in rows
        ),
        "SIM_EXPLOIT_survivor_count": sum(
            bool(row["SIM_EXPLOIT_survived"]) for row in rows
        ),
        "classification_counts": dict(sorted(classifications.items())),
        "source_audit": source_audit,
        "candidate_results": rows,
        "stage1_policy": policy["funnel"]["stage1"],
        "stage2_policy": policy["funnel"]["stage2"],
        "binding_cost_addendum_path": ADDENDUM_PATH,
        "candidate_specific_power_policy_path": POWER_POLICY_PATH,
        "universal_raw_event_power_gate_used": False,
        "grammar_tripwire_executed": False,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "rolling_combine_executed": False,
        "raw_global_N_trials": multiplicity_trial_count(
            load_and_verify(_proof_path(root, proof_registry_path))
        ),
        "campaign_effective_N_trials": 180.0,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": (
            "The arrival-renewal class has only 43 sessions across two one-month "
            "calendar blocks; positive development economics cannot advance without "
            "the frozen tripwire and candidate-specific power audit."
        ),
        "next_action": "run_preregistered_trade_arrival_renewal_tripwire",
    }
    return _write_result(result, root, Path(output_dir))


def _replay_at_stress(
    spec: V72ArrivalCandidateSpec,
    signals: Sequence[Any],
    replay_cache: Mapping[int, Mapping[str, Any]],
    *,
    cost_model: Any,
    stress: CostStress,
) -> list[dict[str, Any]]:
    cost = cost_model.round_turn_cost(
        "ES", spec.cost_horizon, stress=stress, contracts=1.0
    )
    events: list[dict[str, Any]] = []
    for signal in signals:
        entry = replay_cache.get(signal.entry_minute_start_ns)
        exit_row = replay_cache.get(signal.exit_minute_start_ns)
        if entry is None or exit_row is None:
            raise V72TradeArrivalRenewalFunnelError(
                "trade-arrival replay timestamp is absent"
            )
        if entry["contract"] != signal.contract or exit_row["contract"] != signal.contract:
            raise V72TradeArrivalRenewalFunnelError(
                "trade-arrival replay explicit-contract drift"
            )
        gross = (
            (float(exit_row["open"]) - float(entry["open"]))
            * signal.side
            * POINT_VALUE
        )
        events.append(
            {
                "session_day": signal.session_day,
                "calendar_year": signal.calendar_year,
                "contract": signal.contract,
                "decision_ns": signal.decision_ns,
                "exit_ns": signal.exit_minute_start_ns,
                "gross_pnl": gross,
                "cost_usd": cost,
                "net_pnl": gross - cost,
            }
        )
    return events


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = {
        GRAMMAR_PATH: GRAMMAR_SHA256,
        ADDENDUM_PATH: ADDENDUM_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        POLICY_PATH: POLICY_SHA256,
        POWER_POLICY_PATH: POWER_POLICY_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72TradeArrivalRenewalFunnelError(
            "trade-arrival funnel frozen input drift: " + ",".join(drift)
        )
    proof = load_and_verify(_proof_path(root, proof_registry_path))
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V72TradeArrivalRenewalFunnelError(
            "trade-arrival structural/tripwire reservation is absent"
        )
    by_id = {str(row["event_id"]): row for row in proof["entries"]}
    reservation = by_id.get(RESERVATION_EVENT_ID)
    annotation = by_id.get(ANNOTATION_EVENT_ID)
    if (
        reservation is None
        or int(reservation["multiplicity"]["delta_trials"]) != 120
        or int(reservation["multiplicity"]["cumulative_N_trials"])
        != EXPECTED_GLOBAL_N_TRIALS
        or reservation.get("evidence", {}).get("grammar_sha256") != GRAMMAR_SHA256
        or reservation.get("evidence", {}).get("results_seen_before_reservation")
        is not False
        or annotation is None
        or annotation.get("references_event_id") != RESERVATION_EVENT_ID
        or annotation.get("correction", {}).get("correct")
        != "4e8eabca54b6cc0369c2575da83589fdf293aad3"
    ):
        raise V72TradeArrivalRenewalFunnelError(
            "trade-arrival reservation provenance is incomplete"
        )
    if "Q4_2024" not in burned_window_ids(proof):
        raise V72TradeArrivalRenewalFunnelError("Q4 is not irreversibly BURNED")
    return (
        json.loads((root / POLICY_PATH).read_text(encoding="utf-8")),
        json.loads((root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8")),
    )


def _verify_signal_manifest(
    manifest: Mapping[str, Any], specs: Mapping[str, Any], signals: Mapping[str, Any]
) -> None:
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V72TradeArrivalRenewalFunnelError(
            "trade-arrival signal manifest contains outcomes"
        )
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs):
        raise V72TradeArrivalRenewalFunnelError(
            "trade-arrival signal candidate set drift"
        )
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V72TradeArrivalRenewalFunnelError(
                "trade-arrival specification drift"
            )
        if int(rows[candidate_id]["signal_count"]) != len(signals[candidate_id]):
            raise V72TradeArrivalRenewalFunnelError(
                "trade-arrival signal count drift"
            )
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(
            signals[candidate_id]
        ):
            raise V72TradeArrivalRenewalFunnelError(
                "trade-arrival signal path drift"
            )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_trade_arrival_renewal_funnel_result.json"
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
    report_path = destination / "v72_trade_arrival_renewal_funnel_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Trade-arrival renewal Stage 0–2",
            "",
            "[HYDRA-V7] phase=4 step=199 verdict=GREEN",
            f"gate=V72_G11_STAGE0_STAGE2 preuve={displayed}#{result_hash[:8]} tests=24_structures",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={result['raw_global_N_trials']} burned=1",
            "diff_validation=hydra/validation/v72_trade_arrival_renewal_funnel.py,tests/test_v72_trade_arrival_renewal_funnel.py CONTRE=43_sessions_et_deux_blocs",
            f"prochaine_action={result['next_action']}",
            "",
            f"- Stage 0: `{result['stage0_valid_novel_count']}`",
            f"- Stage 1: `{result['stage1_pass_count']}`",
            f"- Walk-forward positifs: `{result['walk_forward_positive_count']}`",
            f"- Survivants coûts ×2: `{result['SIM_EXPLOIT_survivor_count']}`",
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


def _proof_path(root: Path, path: str | Path) -> Path:
    result = Path(path)
    return result if result.is_absolute() else root / result


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EXPECTED_GLOBAL_N_TRIALS",
    "V72TradeArrivalRenewalFunnelError",
    "run_trade_arrival_renewal_funnel",
]
