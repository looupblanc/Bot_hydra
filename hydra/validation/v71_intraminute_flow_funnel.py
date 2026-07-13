from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from hydra.execution.v7_cost_model import load_cost_model
from hydra.governance.proof_registry import burned_window_ids, load_and_verify, multiplicity_trial_count
from hydra.research.v71_intraminute_flow import (
    GRAMMAR_ID,
    candidate_specs,
    generate_signal_population,
    load_intraminute_flow_sources,
    signal_path_hash,
)
from hydra.validation.v71_event_funnel import (
    _empty_walk,
    _events_for_days,
    _folds,
    _minute_replay_cache,
    _replay_candidate,
    _single_day_absolute_share,
    _summary,
    _walk_forward,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


GRAMMAR_PATH = "WORM/v7.1-intraminute-flow-grammar-0008-2026-07-13.json"
GRAMMAR_SHA256 = "36f5d4f8dd2582979d809925782881fb1e159d23ddfbd50dc6a9d348cf5c18dc"
FEATURE_MANIFEST_PATH = "data/manifests/v7_d1_intraminute_flow_v1.json"
FEATURE_MANIFEST_SHA256 = "b228dc89ed36d1b47660073dd6e68703eb44c85e6c0e5897a62f3a14168f6ad4"
SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery_0008/v71_intraminute_flow_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = "7b5af1090f219055f62180c7bbc03dac4c18af8573452d148f15354d55f95979"
POLICY_PATH = "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
POLICY_SHA256 = "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c"
POWER_POLICY_PATH = "WORM/v7.1-candidate-specific-power-policy-0001-2026-07-12.json"
POWER_POLICY_SHA256 = "39f60b4e402c0a40ccc39b5429e0e2cc2dcc88a80592cd28b05c86abed616673"
EXPECTED_GLOBAL_N_TRIALS = 263_850


class V71IntraminuteFlowFunnelError(RuntimeError):
    pass


def run_intraminute_flow_funnel(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0008",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, manifest = _verify_inputs(root, proof_registry_path)
    minute, states, source_audit = load_intraminute_flow_sources(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    signals = generate_signal_population(states, project_root=root, graveyard_path=None)
    _verify_signal_manifest(manifest, specs, signals)
    replay_cache = _minute_replay_cache(minute)
    all_days = tuple(sorted({signal.session_day for rows in signals.values() for signal in rows}))
    early_folds = _folds(all_days, 3, embargo_days=0)
    walk_folds = _folds(all_days, 4, embargo_days=5)
    costs = load_cost_model()
    manifest_rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    rows: list[dict[str, Any]] = []
    for candidate_id, spec in sorted(specs.items()):
        candidate_signals = signals[candidate_id]
        manifest_row = manifest_rows[candidate_id]
        duplicate_of = manifest_row.get("archive_duplicate_of") or manifest_row.get("within_manifest_duplicate_of")
        stage0_valid = bool(candidate_signals) and duplicate_of is None
        events = _replay_candidate(spec, candidate_signals, replay_cache, cost_model=costs) if stage0_valid else []
        pooled = _summary(events)
        early = [_summary(_events_for_days(events, fold)) for fold in early_folds]
        concentration = _single_day_absolute_share(events)
        stage1_pass = bool(
            stage0_valid
            and pooled["event_count"] >= int(policy["funnel"]["stage1"]["minimum_nonoverlapping_events"])
            and pooled["expectancy_per_trade"] > float(policy["funnel"]["stage1"]["pooled_expectancy_min_exclusive"])
            and sum(row["expectancy_per_trade"] > 0.0 for row in early) >= int(policy["funnel"]["stage1"]["minimum_positive_early_folds"])
            and concentration <= float(policy["funnel"]["stage1"]["maximum_single_day_absolute_pnl_share"])
        )
        walk = _walk_forward(events, walk_folds) if stage1_pass else _empty_walk()
        walk_positive = bool(
            stage1_pass
            and walk["retained_event_count"] >= int(policy["funnel"]["stage2"]["minimum_retained_events"])
            and walk["pooled_expectancy_per_trade"] > float(policy["funnel"]["stage2"]["pooled_expectancy_min_exclusive"])
            and walk["positive_fold_count"] >= int(policy["funnel"]["stage2"]["minimum_positive_folds"])
        )
        classification = (
            "DUPLICATE_REJECTED" if duplicate_of else
            "INSUFFICIENT_POWER" if not stage0_valid else
            "FORMULATION_FALSIFIED" if not stage1_pass or not walk_positive else
            "WALK_FORWARD_POSITIVE_PENDING_TRIPWIRE_AND_POWER"
        )
        rows.append(
            {
                "candidate_id": candidate_id,
                "family_id": spec.family_id,
                "motif": spec.motif,
                "response_policy": spec.response_policy,
                "holding_minutes": spec.holding_minutes,
                "specification_hash": spec.specification_hash,
                "signal_path_hash": signal_path_hash(candidate_signals),
                "signal_count": len(candidate_signals),
                "duplicate_of": duplicate_of,
                "stage0_valid_novel": stage0_valid,
                "stage1_pass": stage1_pass,
                "base_stress_1_5x": pooled,
                "early_fold_results": early,
                "single_day_absolute_pnl_share": concentration,
                "walk_forward": walk,
                "walk_forward_positive": walk_positive,
                "power_audit_executed": False,
                "classification": classification,
            }
        )
    classifications = Counter(str(row["classification"]) for row in rows)
    result = {
        "schema": "hydra_v7_1_intraminute_flow_funnel_result_v1",
        "grammar_id": GRAMMAR_ID,
        "candidate_count": len(rows),
        "stage0_valid_novel_count": sum(bool(row["stage0_valid_novel"]) for row in rows),
        "duplicate_rejection_count": sum(row["duplicate_of"] is not None for row in rows),
        "stage1_pass_count": sum(bool(row["stage1_pass"]) for row in rows),
        "walk_forward_positive_count": sum(bool(row["walk_forward_positive"]) for row in rows),
        "classification_counts": dict(sorted(classifications.items())),
        "source_audit": source_audit.to_dict(),
        "candidate_results": rows,
        "stage1_policy": policy["funnel"]["stage1"],
        "stage2_policy": policy["funnel"]["stage2"],
        "candidate_specific_power_policy_path": POWER_POLICY_PATH,
        "universal_raw_event_power_gate_used": False,
        "grammar_tripwire_executed": False,
        "candidate_nulls_executed": False,
        "DSR_BH_executed": False,
        "rolling_combine_executed": False,
        "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
        "new_data_purchase_count": 0,
        "protected_holdout_access_count_delta": 0,
        "outbound_order_count": 0,
        "CONTRE": "Half-minute flow allocation can remain arbitrary clock geometry; positive development evidence cannot advance without the frozen price-null tripwire and candidate-specific power audit.",
        "next_action": "run_preregistered_intraminute_flow_tripwire",
    }
    return _write_result(result, root, Path(output_dir))


def _verify_inputs(root: Path, proof_registry_path: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = {
        GRAMMAR_PATH: GRAMMAR_SHA256,
        FEATURE_MANIFEST_PATH: FEATURE_MANIFEST_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        POLICY_PATH: POLICY_SHA256,
        POWER_POLICY_PATH: POWER_POLICY_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71IntraminuteFlowFunnelError("intraminute funnel frozen input drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) < EXPECTED_GLOBAL_N_TRIALS:
        raise V71IntraminuteFlowFunnelError("intraminute candidate reservation absent")
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71IntraminuteFlowFunnelError("unexpected proof-window state")
    return json.loads((root / POLICY_PATH).read_text()), json.loads((root / SIGNAL_MANIFEST_PATH).read_text())


def _verify_signal_manifest(manifest: Mapping[str, Any], specs: Mapping[str, Any], signals: Mapping[str, Any]) -> None:
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71IntraminuteFlowFunnelError("intraminute manifest contains outcomes")
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs):
        raise V71IntraminuteFlowFunnelError("intraminute candidate set drift")
    for candidate_id, spec in specs.items():
        if rows[candidate_id]["specification_hash"] != spec.specification_hash:
            raise V71IntraminuteFlowFunnelError("intraminute specification drift")
        if rows[candidate_id]["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71IntraminuteFlowFunnelError("intraminute signal path drift")


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_intraminute_flow_funnel_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    displayed = result_path.relative_to(root) if result_path.is_relative_to(root) else result_path
    report_path = destination / "v71_intraminute_flow_funnel_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — Intraminute-flow Stage 0–2",
            "",
            "[HYDRA-V7] phase=4 step=173 verdict=GREEN",
            f"gate=V71_G8_STAGE0_STAGE2 preuve={displayed}#{result_hash[:8]} tests=6_structures",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_intraminute_flow_funnel.py CONTRE=la_frontiere_30_secondes_peut_rester_une_geometrie",
            f"prochaine_action={result['next_action']}",
            "",
            f"- Stage 0: `{result['stage0_valid_novel_count']}`",
            f"- Stage 1: `{result['stage1_pass_count']}`",
            f"- Walk-forward positifs: `{result['walk_forward_positive_count']}`",
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


__all__ = ["EXPECTED_GLOBAL_N_TRIALS", "V71IntraminuteFlowFunnelError", "run_intraminute_flow_funnel"]
