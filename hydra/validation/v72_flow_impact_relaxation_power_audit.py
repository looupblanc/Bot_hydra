from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

from hydra.execution.v7_cost_model import load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research import v72_flow_impact_relaxation as grammar10
from hydra.validation.v71_event_funnel import _minute_replay_cache
from hydra.validation.v71_power_aware_candidate_audit import (
    _candidate_diagnostics,
    _replay_signals,
    _retained_walk_forward_days,
    _verify_frozen_walk_forward_result,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


FREEZE_PATH = (
    "WORM/v7.2-flow-impact-relaxation-power-audit-0010-2026-07-13.json"
)
FREEZE_SHA256 = "4cb9220c242d41af1e3a5cf0d077d122eb37566cf53423100e127dc718a91004"
POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
CALIBRATION_PATH = (
    "reports/v7_1/power_aware_0001/"
    "v71_candidate_specific_power_calibration_result.json"
)
CALIBRATION_SHA256 = (
    "edd3bcdb2ec56bcef2830be7783d74df02041a57b4234b76c1c1803e40b647f5"
)
FUNNEL_PATH = (
    "reports/v7_2/discovery_0010/"
    "v72_flow_impact_relaxation_funnel_result.json"
)
FUNNEL_SHA256 = "ebe44080d1bb953048e768c54de564dcf88789f6f0e4ff9cbd7f8225094edc87"
TRIPWIRE_PATH = (
    "reports/v7_2/discovery_0010/"
    "v72_flow_impact_relaxation_tripwire_result.json"
)
TRIPWIRE_SHA256 = "1e6f26a2e5a8aa4175e67b403b6f1e523f4834407dbc9b27dcf689c1093f9eba"
RECONCILIATION_PATH = (
    "reports/v7_2/discovery_0010/v72_g10_multiplicity_reconciliation.json"
)
RECONCILIATION_SHA256 = (
    "8978542d5b250c57de7c5f623b18ec9913abe8e070b6a909eec1b404c6c7264a"
)
EXPECTED_GLOBAL_N_TRIALS = 265_107
POWER_RESERVATION_EVENT_ID = "v7_2_flow_impact_relaxation_power_reservation_0010"
TRIPWIRE_ACCOUNTING_EVENT_ID = (
    "v7_2_flow_impact_relaxation_tripwire_accounting_0010"
)


class V72FlowImpactRelaxationPowerAuditError(RuntimeError):
    pass


def run_flow_impact_relaxation_power_audit(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_2/discovery_0010",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, freeze, frozen_results, current_trials = _verify_inputs(
        root, proof_registry_path
    )
    minute, states, _ = grammar10.load_flow_impact_sources(root)
    specs = {row.candidate_id: row for row in grammar10.candidate_specs(root)}
    signals = grammar10.generate_signal_population(
        states, project_root=root, graveyard_path=None
    )
    retained_days = set(_retained_walk_forward_days(signals))
    replay_cache = _minute_replay_cache(minute)
    costs = load_cost_model()
    rows: list[dict[str, Any]] = []
    for candidate in freeze["candidates"]:
        candidate_id = str(candidate["candidate_id"])
        spec = specs[candidate_id]
        if spec.specification_hash != candidate["specification_hash"]:
            raise V72FlowImpactRelaxationPowerAuditError(
                "G10 candidate specification drift"
            )
        candidate_signals = signals[candidate_id]
        if grammar10.signal_path_hash(candidate_signals) != candidate["signal_path_hash"]:
            raise V72FlowImpactRelaxationPowerAuditError(
                "G10 candidate signal path drift"
            )
        selected = [
            row for row in candidate_signals if row.session_day in retained_days
        ]
        ledger = _replay_signals(spec, selected, replay_cache, costs)
        frozen = frozen_results[candidate_id]
        normalized_frozen = {
            **frozen,
            "walk_forward": frozen["walk_forward_stress_1_5x"],
        }
        _verify_frozen_walk_forward_result(
            candidate_id, ledger, normalized_frozen
        )
        candidate_view = {
            **candidate,
            "grammar_id": freeze["grammar_id"],
        }
        rows.append(
            _candidate_diagnostics(
                candidate_view,
                ledger,
                policy,
                normalized_frozen,
            )
        )
    counts = Counter(str(row["status"]) for row in rows)
    powered = [
        str(row["candidate_id"])
        for row in rows
        if row["status"] == "POWERED_WF_POSITIVE"
    ]
    result = {
        "schema": "hydra_v7_2_flow_impact_relaxation_power_audit_result_v1",
        "audit_id": "hydra_v7_2_flow_impact_relaxation_power_audit_0010",
        "verdict": "GREEN",
        "candidate_count": len(rows),
        "status_counts": dict(sorted(counts.items())),
        "powered_candidate_ids": powered,
        "underpowered_candidate_ids": [
            str(row["candidate_id"])
            for row in rows
            if row["status"] == "PROMISING_UNDERPOWERED"
        ],
        "fragile_candidate_ids": [
            str(row["candidate_id"])
            for row in rows
            if row["status"] == "WF_POSITIVE_BUT_FRAGILE"
        ],
        "false_positive_candidate_ids": [
            str(row["candidate_id"])
            for row in rows
            if row["status"] == "WF_FALSE_POSITIVE"
        ],
        "candidate_results": rows,
        "universal_raw_event_threshold_used": False,
        "calibrated_candidate_specific_policy_used": True,
        "tripwire_verdict": "GREEN_NULL_ADJUSTED_BASELINE",
        "tripwire_NULL_RATIO": 0.6488095238095238,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "rolling_combine_executed": False,
        "rolling_combine_research_eligible_ids": [],
        "rolling_combine_gate": (
            "separate_WORM_candidate_nulls_and_DSR_BH_required_after_POWERED_WF_POSITIVE"
        ),
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "raw_global_N_trials_at_reservation": EXPECTED_GLOBAL_N_TRIALS,
        "raw_global_N_trials_current": current_trials,
        "campaign_effective_N_trials_after_audit_reservation": 294.0,
        "CONTRE": (
            "The four candidates were selected on positive D1 walk-forward results "
            "and only two calendar years are available; power classification is "
            "post-selection development evidence, not fresh confirmation."
        ),
        "prochaine_action": (
            "freeze_candidate_null_and_DSR_BH_suite_for_powered_candidates"
            if powered
            else "retain_nonfragile_underpowered_candidates_without_promotion"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]], int]:
    expected = {
        FREEZE_PATH: FREEZE_SHA256,
        POLICY_PATH: POLICY_SHA256,
        CALIBRATION_PATH: CALIBRATION_SHA256,
        FUNNEL_PATH: FUNNEL_SHA256,
        TRIPWIRE_PATH: TRIPWIRE_SHA256,
        RECONCILIATION_PATH: RECONCILIATION_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V72FlowImpactRelaxationPowerAuditError(
            "G10 power audit frozen input drift: " + ",".join(drift)
        )
    calibration = json.loads((root / CALIBRATION_PATH).read_text(encoding="utf-8"))
    if calibration.get("verdict") != "GREEN" or calibration.get(
        "candidate_diagnostics_read"
    ) is not False:
        raise V72FlowImpactRelaxationPowerAuditError(
            "power calibration is not clean GREEN"
        )
    tripwire = json.loads((root / TRIPWIRE_PATH).read_text(encoding="utf-8"))
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE":
        raise V72FlowImpactRelaxationPowerAuditError("G10 tripwire is not GREEN")
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    current_trials = multiplicity_trial_count(proof)
    if current_trials < EXPECTED_GLOBAL_N_TRIALS:
        raise V72FlowImpactRelaxationPowerAuditError(
            "G10 power audit reservation is absent"
        )
    by_id = {str(row["event_id"]): row for row in proof["entries"]}
    power_reservation = by_id.get(POWER_RESERVATION_EVENT_ID)
    tripwire_accounting = by_id.get(TRIPWIRE_ACCOUNTING_EVENT_ID)
    if (
        power_reservation is None
        or int(power_reservation["multiplicity"]["cumulative_N_trials"])
        != EXPECTED_GLOBAL_N_TRIALS
        or power_reservation.get("evidence", {}).get("freeze_sha256")
        != FREEZE_SHA256
        or tripwire_accounting is None
        or int(tripwire_accounting["multiplicity"]["delta_trials"]) != 144
    ):
        raise V72FlowImpactRelaxationPowerAuditError(
            "G10 multiplicity provenance is incomplete"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V72FlowImpactRelaxationPowerAuditError(
            "unexpected proof-window state"
        )
    funnel = json.loads((root / FUNNEL_PATH).read_text(encoding="utf-8"))
    frozen_results = {
        str(row["candidate_id"]): row
        for row in funnel["candidate_results"]
        if row.get("classification")
        == "WALK_FORWARD_POSITIVE_PENDING_TRIPWIRE_POWER_NULLS"
    }
    freeze = json.loads((root / FREEZE_PATH).read_text(encoding="utf-8"))
    frozen_ids = {str(row["candidate_id"]) for row in freeze["candidates"]}
    if set(frozen_results) != frozen_ids:
        raise V72FlowImpactRelaxationPowerAuditError(
            "G10 frozen candidate set drift"
        )
    return (
        json.loads((root / POLICY_PATH).read_text(encoding="utf-8")),
        freeze,
        frozen_results,
        current_trials,
    )


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v72_flow_impact_relaxation_power_audit_result.json"
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
    report_path = destination / "v72_flow_impact_relaxation_power_audit_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.2 — Delayed flow-impact power-aware audit",
            "",
            "[HYDRA-V7] phase=4 step=193 verdict=GREEN",
            f"gate=V72_G10_POWER_AUDIT preuve={displayed}#{result_hash[:8]} tests=4_frozen_candidates",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={result['raw_global_N_trials_current']} burned=1",
            "diff_validation=hydra/validation/v72_flow_impact_relaxation_power_audit.py,tests/test_v72_flow_impact_relaxation_power_audit.py CONTRE=biais_de_selection_D1_et_deux_annees",
            f"prochaine_action={result['prochaine_action']}",
            "",
            f"- Statuts: `{json.dumps(result['status_counts'], sort_keys=True)}`",
            f"- Powered: `{len(result['powered_candidate_ids'])}`",
            f"- Sous-puissants: `{len(result['underpowered_candidate_ids'])}`",
            f"- Fragiles: `{len(result['fragile_candidate_ids'])}`",
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
    "V72FlowImpactRelaxationPowerAuditError",
    "run_flow_impact_relaxation_power_audit",
]
