from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

from hydra.execution.v7_cost_model import load_cost_model
from hydra.governance.proof_registry import burned_window_ids, load_and_verify, multiplicity_trial_count
from hydra.research import v71_intraminute_flow as grammar8
from hydra.validation.v71_event_funnel import _minute_replay_cache
from hydra.validation.v71_power_aware_candidate_audit import (
    _candidate_diagnostics,
    _replay_signals,
    _retained_walk_forward_days,
    _verify_frozen_walk_forward_result,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


FREEZE_PATH = "WORM/v7.1-intraminute-flow-power-audit-0001-2026-07-13.json"
FREEZE_SHA256 = "3f1b8fb8eca73bebf5582071c9c79b75971a5d2d57afda9c006ea9edff9e5104"
POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
CALIBRATION_PATH = "reports/v7_1/power_aware_0001/v71_candidate_specific_power_calibration_result.json"
CALIBRATION_SHA256 = "edd3bcdb2ec56bcef2830be7783d74df02041a57b4234b76c1c1803e40b647f5"
FUNNEL_PATH = "reports/v7_1/discovery_0008/v71_intraminute_flow_funnel_result.json"
FUNNEL_SHA256 = "cdf426936a6350a997958f4f6bfb326466538881f290d7943bd057d9361dd69b"
TRIPWIRE_PATH = "reports/v7_1/discovery_0008/v71_intraminute_flow_tripwire_result.json"
TRIPWIRE_SHA256 = "b8e43659e47c0ccf68bd68e95dfea4328035babb0eca3433282bb1a2606000f8"
EXPECTED_GLOBAL_N_TRIALS = 263_882


class V71IntraminuteFlowPowerAuditError(RuntimeError):
    pass


def run_intraminute_flow_power_audit(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0008",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, freeze, frozen_results = _verify_inputs(root, proof_registry_path)
    minute, states, _ = grammar8.load_intraminute_flow_sources(root)
    specs = {row.candidate_id: row for row in grammar8.candidate_specs(root)}
    signals = grammar8.generate_signal_population(states, project_root=root, graveyard_path=None)
    retained_days = set(_retained_walk_forward_days(signals))
    replay_cache = _minute_replay_cache(minute)
    costs = load_cost_model()
    rows: list[dict[str, Any]] = []
    for candidate in freeze["candidates"]:
        candidate_view = {**candidate, "grammar_id": freeze["grammar_id"]}
        candidate_id = str(candidate["candidate_id"])
        spec = specs[candidate_id]
        if spec.specification_hash != candidate["specification_hash"]:
            raise V71IntraminuteFlowPowerAuditError("candidate specification drift")
        selected = [row for row in signals[candidate_id] if row.session_day in retained_days]
        ledger = _replay_signals(spec, selected, replay_cache, costs)
        _verify_frozen_walk_forward_result(candidate_id, ledger, frozen_results[candidate_id])
        rows.append(_candidate_diagnostics(candidate_view, ledger, policy, frozen_results[candidate_id]))
    counts = Counter(str(row["status"]) for row in rows)
    powered = [str(row["candidate_id"]) for row in rows if row["status"] == "POWERED_WF_POSITIVE"]
    result = {
        "schema": "hydra_v7_1_intraminute_flow_power_audit_result_v1",
        "audit_id": "hydra_v7_1_intraminute_flow_power_audit_0001",
        "verdict": "GREEN",
        "candidate_count": len(rows),
        "status_counts": dict(sorted(counts.items())),
        "powered_candidate_ids": powered,
        "underpowered_candidate_ids": [str(row["candidate_id"]) for row in rows if row["status"] == "PROMISING_UNDERPOWERED"],
        "fragile_candidate_ids": [str(row["candidate_id"]) for row in rows if row["status"] == "WF_POSITIVE_BUT_FRAGILE"],
        "false_positive_candidate_ids": [str(row["candidate_id"]) for row in rows if row["status"] == "WF_FALSE_POSITIVE"],
        "rolling_combine_final_confirmation_eligible_ids": powered,
        "candidate_results": rows,
        "universal_raw_event_threshold_used": False,
        "calibrated_candidate_specific_policy_used": True,
        "final_confirmation_power_requirement": 0.8,
        "tripwire_verdict": "GREEN_NULL_ADJUSTED_BASELINE",
        "tripwire_evidence_strength": "VERT_MINCE",
        "tripwire_NULL_RATIO": 0.7333333333333334,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "rolling_combine_executed": False,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "campaign_effective_N_trials_before_audit": 45.0,
        "CONTRE": "Both candidates were selected after tiny positive pooled D1 walk-forward results with negative early folds and a VERT_MINCE tripwire; this audit is selected development evidence rather than confirmation.",
        "prochaine_action": (
            "run_relevant_nulls_and_DSR_BH_for_powered_candidates" if powered
            else "classify_fragile_or_underpowered_candidates_without_promotion_and_review_next_distinct_hypothesis"
        ),
    }
    return _write_result(result, root, Path(output_dir))


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    expected = {
        FREEZE_PATH: FREEZE_SHA256,
        POLICY_PATH: POLICY_SHA256,
        CALIBRATION_PATH: CALIBRATION_SHA256,
        FUNNEL_PATH: FUNNEL_SHA256,
        TRIPWIRE_PATH: TRIPWIRE_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71IntraminuteFlowPowerAuditError("intraminute power frozen input drift: " + ",".join(drift))
    calibration = json.loads((root / CALIBRATION_PATH).read_text())
    if calibration.get("verdict") != "GREEN" or calibration.get("candidate_diagnostics_read") is not False:
        raise V71IntraminuteFlowPowerAuditError("power calibration is not clean GREEN")
    tripwire = json.loads((root / TRIPWIRE_PATH).read_text())
    if tripwire.get("verdict") != "GREEN_NULL_ADJUSTED_BASELINE":
        raise V71IntraminuteFlowPowerAuditError("intraminute tripwire is not GREEN")
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71IntraminuteFlowPowerAuditError("power audit reservation is absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71IntraminuteFlowPowerAuditError("unexpected proof-window state")
    funnel = json.loads((root / FUNNEL_PATH).read_text())
    frozen_results = {str(row["candidate_id"]): row for row in funnel["candidate_results"] if bool(row.get("walk_forward_positive"))}
    freeze = json.loads((root / FREEZE_PATH).read_text())
    if set(frozen_results) != {str(row["candidate_id"]) for row in freeze["candidates"]}:
        raise V71IntraminuteFlowPowerAuditError("frozen candidate set drift")
    return json.loads((root / POLICY_PATH).read_text()), freeze, frozen_results


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_intraminute_flow_power_audit_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    report_path = destination / "v71_intraminute_flow_power_audit_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Intraminute-flow power-aware audit",
            "",
            "[HYDRA-V7] phase=4 step=175 verdict=GREEN",
            f"gate=V71_G8_POWER_AUDIT preuve={displayed}#{result_hash[:8]} tests=2_frozen_candidates",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_intraminute_flow_power_audit.py CONTRE=biais_de_selection_D1_et_tripwire_vert_mince",
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


__all__ = ["EXPECTED_GLOBAL_N_TRIALS", "V71IntraminuteFlowPowerAuditError", "run_intraminute_flow_power_audit"]
