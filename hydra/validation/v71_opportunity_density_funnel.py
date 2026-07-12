from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from hydra.execution.v7_cost_model import load_cost_model
from hydra.governance.proof_registry import (
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.research.v71_opportunity_density_grammar import (
    GRAMMAR_ID,
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
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


GRAMMAR_PATH = "WORM/v7.1-opportunity-density-grammar-0002-2026-07-12.json"
GRAMMAR_SHA256 = "ef44e6e72c42b2ed4b7228f3addbd2f182e3e51bcfb619aa4c0a2102db6d3566"
SIGNAL_MANIFEST_PATH = (
    "reports/v7_1/discovery_0002/v71_opportunity_density_signal_manifest.json"
)
SIGNAL_MANIFEST_SHA256 = (
    "c90a2321fc66e114d65dd533d077ec04308ae714369e28b82f5d9e996dd7fa24"
)
POLICY_PATH = "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
POLICY_SHA256 = "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c"
POWER_PATH = "WORM/v7.1-powered-promotion-minimum-2026-07-12.json"
POWER_SHA256 = "3e0211c6a5acea81713431802fc1576da4d5be2a0cc37bf900cd02eabd68c6fa"
EXPECTED_GLOBAL_N_TRIALS = 262_356


class V71OpportunityDensityFunnelError(RuntimeError):
    pass


def run_opportunity_density_funnel(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery_0002",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, power, manifest = _verify_inputs(root, proof_registry_path)
    minute = load_v71_minute_features(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    signals = generate_signal_population(minute, project_root=root)
    _verify_signal_manifest(manifest, specs, signals)
    replay_cache = _minute_replay_cache(minute)
    days = tuple(
        sorted(
            {
                signal.session_day
                for candidate_signals in signals.values()
                for signal in candidate_signals
            }
        )
    )
    early_folds = _folds(days, 3, embargo_days=0)
    walk_folds = _folds(days, 4, embargo_days=5)
    cost_model = load_cost_model()
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
        events = (
            _replay_candidate(
                spec,
                candidate_signals,
                replay_cache,
                cost_model=cost_model,
            )
            if stage0_valid
            else []
        )
        pooled = _summary(events)
        early = [_summary(_events_for_days(events, fold)) for fold in early_folds]
        concentration = _single_day_absolute_share(events)
        stage1_pass = bool(
            stage0_valid
            and pooled["event_count"]
            >= int(policy["funnel"]["stage1"]["minimum_nonoverlapping_events"])
            and pooled["expectancy_per_trade"]
            > float(
                policy["funnel"]["stage1"][
                    "pooled_expectancy_min_exclusive"
                ]
            )
            and sum(result["expectancy_per_trade"] > 0.0 for result in early)
            >= int(policy["funnel"]["stage1"]["minimum_positive_early_folds"])
            and concentration
            <= float(
                policy["funnel"]["stage1"][
                    "maximum_single_day_absolute_pnl_share"
                ]
            )
        )
        walk = _walk_forward(events, walk_folds) if stage1_pass else _empty_walk()
        walk_positive = bool(
            stage1_pass
            and walk["retained_event_count"]
            >= int(policy["funnel"]["stage2"]["minimum_retained_events"])
            and walk["pooled_expectancy_per_trade"]
            > float(
                policy["funnel"]["stage2"][
                    "pooled_expectancy_min_exclusive"
                ]
            )
            and walk["positive_fold_count"]
            >= int(policy["funnel"]["stage2"]["minimum_positive_folds"])
        )
        powered = bool(
            walk_positive
            and walk["retained_event_count"]
            >= int(
                power[
                    "conservative_minimum_walk_forward_events_for_DSR_BH"
                ]
            )
        )
        if duplicate_of:
            classification = "DUPLICATE_REJECTED"
        elif not candidate_signals:
            classification = "INSUFFICIENT_POWER"
        elif not stage1_pass or not walk_positive:
            classification = "FORMULATION_FALSIFIED"
        elif not powered:
            classification = "MECHANISM_UNDERPOWERED_REQUIRES_INDEPENDENT_CONFIRMATION"
        else:
            classification = "WALK_FORWARD_POSITIVE_POWERED"
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
                "powered_for_DSR_BH": powered,
                "classification": classification,
            }
        )
    result = _aggregate(rows, policy, power)
    result.update(
        {
            "schema": "hydra_v7_1_opportunity_density_funnel_result_v1",
            "grammar_id": GRAMMAR_ID,
            "signal_manifest_path": SIGNAL_MANIFEST_PATH,
            "signal_manifest_sha256": SIGNAL_MANIFEST_SHA256,
            "candidate_results": rows,
            "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
            "tripwire_executed": False,
            "candidate_nulls_executed": False,
            "DSR_BH_executed": False,
            "rolling_combine_executed": False,
            "new_data_purchase_count": 0,
            "protected_holdout_access_count_delta": 0,
            "outbound_order_count": 0,
            "CONTRE": (
                "Structural unions can raise opportunity count by adding weak "
                "trades; only powered positive walk-forward candidates may "
                "reach the grammar tripwire and relevant nulls."
            ),
            "next_action": (
                "run_new_grammar_tripwire_before_candidate_nulls"
                if result["powered_walk_forward_candidate_count"] > 0
                else "classify_density_grammar_and_select_new_mechanism"
            ),
        }
    )
    return _write_result(result, root, Path(output_dir))


def _aggregate(
    rows: list[dict[str, Any]],
    policy: Mapping[str, Any],
    power: Mapping[str, Any],
) -> dict[str, Any]:
    classifications = Counter(str(row["classification"]) for row in rows)
    families = {}
    for family_id in sorted({str(row["family_id"]) for row in rows}):
        selected = [row for row in rows if row["family_id"] == family_id]
        families[family_id] = {
            "candidate_count": len(selected),
            "stage0_valid_novel": sum(
                bool(row["stage0_valid_novel"]) for row in selected
            ),
            "stage1_pass": sum(bool(row["stage1_pass"]) for row in selected),
            "walk_forward_positive": sum(
                bool(row["walk_forward_positive"]) for row in selected
            ),
            "powered_walk_forward": sum(
                bool(row["powered_for_DSR_BH"]) for row in selected
            ),
        }
    return {
        "candidate_count": len(rows),
        "family_count": len(families),
        "stage0_valid_novel_count": sum(
            bool(row["stage0_valid_novel"]) for row in rows
        ),
        "duplicate_rejection_count": sum(
            row["duplicate_of"] is not None for row in rows
        ),
        "zero_signal_count": sum(int(row["signal_count"]) == 0 for row in rows),
        "stage1_pass_count": sum(bool(row["stage1_pass"]) for row in rows),
        "walk_forward_positive_count": sum(
            bool(row["walk_forward_positive"]) for row in rows
        ),
        "powered_walk_forward_candidate_count": sum(
            bool(row["powered_for_DSR_BH"]) for row in rows
        ),
        "classification_counts": dict(sorted(classifications.items())),
        "family_results": families,
        "stage1_policy": policy["funnel"]["stage1"],
        "stage2_policy": policy["funnel"]["stage2"],
        "powered_minimum_events": power[
            "conservative_minimum_walk_forward_events_for_DSR_BH"
        ],
    }


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    expected = {
        GRAMMAR_PATH: GRAMMAR_SHA256,
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        POLICY_PATH: POLICY_SHA256,
        POWER_PATH: POWER_SHA256,
    }
    drift = [path for path, expected_sha in expected.items() if _sha256(root / path) != expected_sha]
    if drift:
        raise V71OpportunityDensityFunnelError(
            "opportunity-density frozen input drift: " + ",".join(drift)
        )
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    if multiplicity_trial_count(proof) != EXPECTED_GLOBAL_N_TRIALS:
        raise V71OpportunityDensityFunnelError(
            "opportunity-density candidate reservation is absent"
        )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71OpportunityDensityFunnelError("unexpected proof-window state")
    return (
        json.loads((root / POLICY_PATH).read_text(encoding="utf-8")),
        json.loads((root / POWER_PATH).read_text(encoding="utf-8")),
        json.loads((root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8")),
    )


def _verify_signal_manifest(
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    signals: Mapping[str, Any],
) -> None:
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71OpportunityDensityFunnelError("signal manifest contains outcomes")
    rows = {str(row["candidate_id"]): row for row in manifest["candidate_paths"]}
    if set(rows) != set(specs) or set(signals) != set(specs):
        raise V71OpportunityDensityFunnelError("manifest candidate drift")
    for candidate_id, spec in specs.items():
        row = rows[candidate_id]
        if row["specification_hash"] != spec.specification_hash:
            raise V71OpportunityDensityFunnelError("specification hash drift")
        if int(row["signal_count"]) != len(signals[candidate_id]):
            raise V71OpportunityDensityFunnelError("signal count drift")
        if row["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71OpportunityDensityFunnelError("signal path drift")


def _write_result(
    result: dict[str, Any], root: Path, output_dir: Path
) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_opportunity_density_funnel_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_opportunity_density_funnel_report.md"
    displayed_result_path = (
        result_path.relative_to(root)
        if result_path.is_relative_to(root)
        else result_path
    )
    report = "\n".join(
        [
            "# HYDRA V7.1 — Opportunity-density Stage 0–2",
            "",
            "[HYDRA-V7] phase=4 step=131 verdict=GREEN",
            f"gate=V71_G2_STAGE0_STAGE2 preuve={displayed_result_path}#{result_hash[:8]} tests=128_structures",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_opportunity_density_funnel.py CONTRE=la_couverture_structurelle_peut_seulement_ajouter_des_trades_faibles",
            f"prochaine_action={result['next_action']}",
            "",
            f"- Stage 0 valides/novel: `{result['stage0_valid_novel_count']}`",
            f"- Stage 1: `{result['stage1_pass_count']}`",
            f"- Walk-forward positifs: `{result['walk_forward_positive_count']}`",
            f"- Walk-forward positifs et >=320 événements: `{result['powered_walk_forward_candidate_count']}`",
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
    "V71OpportunityDensityFunnelError",
    "run_opportunity_density_funnel",
]
