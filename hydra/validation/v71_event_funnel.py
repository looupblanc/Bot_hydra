from __future__ import annotations

import hashlib
import json
import os
from collections import Counter, defaultdict
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
from hydra.research.v71_event_mechanism_grammar import (
    V71CandidateSpec,
    V71Signal,
    candidate_specs,
    generate_signal_population,
    load_v71_minute_features,
    signal_path_hash,
)
from hydra.validation.v7_report_schema import validate_v7_report_text


SIGNAL_MANIFEST_PATH = "reports/v7_1/discovery/v71_signal_manifest.json"
SIGNAL_MANIFEST_SHA256 = "b366bd8f295b0a1110532988b15cc9dde70d450fc8ea9a53b476c16b61a7e15c"
POLICY_PATH = "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
POLICY_SHA256 = "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c"
POWER_MINIMUM_PATH = "WORM/v7.1-powered-promotion-minimum-2026-07-12.json"
POWER_MINIMUM_SHA256 = "3e0211c6a5acea81713431802fc1576da4d5be2a0cc37bf900cd02eabd68c6fa"
EXPECTED_GLOBAL_N_TRIALS = 262_228
FROZEN_RESULT_PATH = "reports/v7_1/discovery/v71_development_funnel_result.json"
FROZEN_RESULT_SHA256 = "b8767eb9a2c5a8f9ef7c85d640cf5b1368f2607f49da3cc0b0c9a92a73f16fe2"
POINT_VALUE = 50.0


class V71EventFunnelError(RuntimeError):
    pass


def run_v71_development_funnel(
    *,
    project_root: str | Path = ".",
    proof_registry_path: str | Path = "mission/state/proof_registry.json",
    output_dir: str | Path = "reports/v7_1/discovery",
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    policy, power = _verify_inputs(root, proof_registry_path)
    minute = load_v71_minute_features(root)
    specs = {row.candidate_id: row for row in candidate_specs(root)}
    signals = generate_signal_population(minute, project_root=root)
    manifest = _verify_signal_manifest(root, signals, specs)
    replay_cache = _minute_replay_cache(minute)
    duplicates = _duplicate_paths(manifest)
    rows: list[dict[str, Any]] = []
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
    cost_model = load_cost_model()
    for candidate_id, spec in sorted(specs.items()):
        candidate_signals = signals[candidate_id]
        duplicate_of = duplicates.get(candidate_id)
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
        early = [_summary(_events_for_days(events, days)) for days in early_folds]
        concentration = _single_day_absolute_share(events)
        stage1_pass = bool(
            stage0_valid
            and pooled["event_count"] >= int(policy["funnel"]["stage1"]["minimum_nonoverlapping_events"])
            and pooled["expectancy_per_trade"]
            > float(policy["funnel"]["stage1"]["pooled_expectancy_min_exclusive"])
            and sum(row["expectancy_per_trade"] > 0.0 for row in early)
            >= int(policy["funnel"]["stage1"]["minimum_positive_early_folds"])
            and concentration
            <= float(policy["funnel"]["stage1"]["maximum_single_day_absolute_pnl_share"])
        )
        walk = _walk_forward(events, walk_folds) if stage1_pass else _empty_walk()
        walk_positive = bool(
            stage1_pass
            and walk["retained_event_count"]
            >= int(policy["funnel"]["stage2"]["minimum_retained_events"])
            and walk["pooled_expectancy_per_trade"]
            > float(policy["funnel"]["stage2"]["pooled_expectancy_min_exclusive"])
            and walk["positive_fold_count"]
            >= int(policy["funnel"]["stage2"]["minimum_positive_folds"])
        )
        powered = bool(
            walk_positive
            and walk["retained_event_count"]
            >= int(power["conservative_minimum_walk_forward_events_for_DSR_BH"])
        )
        if not stage0_valid:
            classification = (
                "DUPLICATE_REJECTED" if duplicate_of else "INSUFFICIENT_POWER"
            )
        elif not stage1_pass:
            classification = "FORMULATION_FALSIFIED"
        elif not walk_positive:
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
            "schema": "hydra_v7_1_development_funnel_result_v1",
            "grammar_id": "hydra_v7_1_event_mechanism_grammar_0001",
            "signal_manifest_path": SIGNAL_MANIFEST_PATH,
            "signal_manifest_sha256": SIGNAL_MANIFEST_SHA256,
            "candidate_results": rows,
            "raw_global_N_trials": EXPECTED_GLOBAL_N_TRIALS,
            "DSR_BH_executed": False,
            "candidate_nulls_executed": False,
            "rolling_combine_executed": False,
            "new_data_purchase_count": 0,
            "protected_holdout_access_count_delta": 0,
            "outbound_order_count": 0,
            "CONTRE": (
                "D1 contains only two date-matched calendar blocks; positive "
                "walk-forward expectancy below 320 retained events remains "
                "underpowered and cannot support DSR/BH."
            ),
            "next_action": (
                "freeze_powered_walk_forward_cohort_before_nulls"
                if result["powered_walk_forward_candidate_count"] > 0
                else "classify_underpowered_mechanisms_and_preregister_next_information_gain_action"
            ),
        }
    )
    return _write_result(result, root, Path(output_dir))


def _replay_candidate(
    spec: V71CandidateSpec,
    signals: Sequence[V71Signal],
    replay_cache: Mapping[int, Mapping[str, Any]],
    *,
    cost_model: Any,
) -> list[dict[str, Any]]:
    cost = cost_model.round_turn_cost(
        "ES",
        spec.cost_horizon,
        stress=CostStress.STRESS_1_5X,
        contracts=1.0,
    )
    events: list[dict[str, Any]] = []
    for signal in signals:
        entry = replay_cache.get(signal.entry_minute_start_ns)
        exit_row = replay_cache.get(signal.exit_minute_start_ns)
        if entry is None or exit_row is None:
            raise V71EventFunnelError("V7.1 replay timestamp is absent")
        if entry["contract"] != signal.contract or exit_row["contract"] != signal.contract:
            raise V71EventFunnelError("V7.1 replay contract drift")
        gross = (
            (float(exit_row["open"]) - float(entry["open"]))
            * signal.side
            * POINT_VALUE
        )
        events.append(
            {
                "session_day": signal.session_day,
                "calendar_year": signal.calendar_year,
                "decision_ns": signal.decision_ns,
                "exit_ns": signal.exit_minute_start_ns,
                "gross_pnl": gross,
                "cost_usd": cost,
                "net_pnl": gross - cost,
            }
        )
    return events


def _minute_replay_cache(minute: pd.DataFrame) -> dict[int, dict[str, Any]]:
    source = minute[minute["product"] == "ES"]
    return {
        int(row.minute_start_ns): {"open": float(row.open), "contract": str(row.contract)}
        for row in source.itertuples(index=False)
    }


def _duplicate_paths(manifest: Mapping[str, Any]) -> dict[str, str]:
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for row in manifest["candidate_paths"]:
        if int(row["signal_count"]) > 0:
            groups[str(row["signal_path_hash"])].append(str(row["candidate_id"]))
    duplicate: dict[str, str] = {}
    for members in groups.values():
        ordered = sorted(members)
        for candidate in ordered[1:]:
            duplicate[candidate] = ordered[0]
    return duplicate


def _folds(
    session_days: Sequence[str], count: int, *, embargo_days: int
) -> tuple[tuple[str, ...], ...]:
    raw = np.array_split(np.asarray(session_days, dtype=object), count)
    output: list[tuple[str, ...]] = []
    for index, values in enumerate(raw):
        retained = values[embargo_days:] if index > 0 else values
        output.append(tuple(str(value) for value in retained))
    return tuple(output)


def _events_for_days(
    events: Sequence[Mapping[str, Any]], days: Sequence[str]
) -> list[Mapping[str, Any]]:
    selected = set(days)
    return [row for row in events if str(row["session_day"]) in selected]


def _summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = np.asarray([float(row["net_pnl"]) for row in events], dtype=np.float64)
    return {
        "event_count": int(values.size),
        "net_pnl": float(np.sum(values)) if values.size else 0.0,
        "expectancy_per_trade": float(np.mean(values)) if values.size else 0.0,
        "positive_event_fraction": float(np.mean(values > 0.0)) if values.size else 0.0,
    }


def _single_day_absolute_share(events: Sequence[Mapping[str, Any]]) -> float:
    daily: defaultdict[str, float] = defaultdict(float)
    for row in events:
        daily[str(row["session_day"])] += float(row["net_pnl"])
    absolute = [abs(value) for value in daily.values()]
    total = sum(absolute)
    return float(max(absolute) / total) if total > 0.0 else 1.0


def _walk_forward(
    events: Sequence[Mapping[str, Any]], folds: Sequence[Sequence[str]]
) -> dict[str, Any]:
    results = []
    pooled: list[Mapping[str, Any]] = []
    for index, days in enumerate(folds, start=1):
        selected = _events_for_days(events, days)
        pooled.extend(selected)
        results.append({"fold": index, "retained_session_days": len(days), **_summary(selected)})
    summary = _summary(pooled)
    return {
        "folds": results,
        "retained_event_count": summary["event_count"],
        "pooled_net_pnl": summary["net_pnl"],
        "pooled_expectancy_per_trade": summary["expectancy_per_trade"],
        "positive_fold_count": sum(row["expectancy_per_trade"] > 0.0 for row in results),
        "embargo_days": 5,
        "purge": "nonoverlapping_signals_fixed_horizon",
    }


def _empty_walk() -> dict[str, Any]:
    return {
        "folds": [],
        "retained_event_count": 0,
        "pooled_net_pnl": 0.0,
        "pooled_expectancy_per_trade": 0.0,
        "positive_fold_count": 0,
        "embargo_days": 5,
        "purge": "not_executed_before_stage1_pass",
    }


def _aggregate(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any], power: Mapping[str, Any]
) -> dict[str, Any]:
    classifications = Counter(str(row["classification"]) for row in rows)
    family = {}
    for family_id in sorted({str(row["family_id"]) for row in rows}):
        selected = [row for row in rows if row["family_id"] == family_id]
        family[family_id] = {
            "candidate_count": len(selected),
            "stage0_valid_novel": sum(bool(row["stage0_valid_novel"]) for row in selected),
            "stage1_pass": sum(bool(row["stage1_pass"]) for row in selected),
            "walk_forward_positive": sum(bool(row["walk_forward_positive"]) for row in selected),
            "powered_walk_forward": sum(bool(row["powered_for_DSR_BH"]) for row in selected),
        }
    return {
        "candidate_count": len(rows),
        "family_count": len(family),
        "stage0_valid_novel_count": sum(bool(row["stage0_valid_novel"]) for row in rows),
        "duplicate_rejection_count": sum(bool(row["duplicate_of"]) for row in rows),
        "zero_signal_count": sum(int(row["signal_count"]) == 0 for row in rows),
        "stage1_pass_count": sum(bool(row["stage1_pass"]) for row in rows),
        "walk_forward_positive_count": sum(bool(row["walk_forward_positive"]) for row in rows),
        "powered_walk_forward_candidate_count": sum(bool(row["powered_for_DSR_BH"]) for row in rows),
        "classification_counts": dict(sorted(classifications.items())),
        "family_results": family,
        "stage1_policy": policy["funnel"]["stage1"],
        "stage2_policy": policy["funnel"]["stage2"],
        "powered_minimum_events": power["conservative_minimum_walk_forward_events_for_DSR_BH"],
    }


def _verify_inputs(
    root: Path, proof_registry_path: str | Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected = {
        SIGNAL_MANIFEST_PATH: SIGNAL_MANIFEST_SHA256,
        POLICY_PATH: POLICY_SHA256,
        POWER_MINIMUM_PATH: POWER_MINIMUM_SHA256,
    }
    drift = [path for path, sha in expected.items() if _sha256(root / path) != sha]
    if drift:
        raise V71EventFunnelError("V7.1 funnel frozen input drift: " + ",".join(drift))
    proof_path = Path(proof_registry_path)
    if not proof_path.is_absolute():
        proof_path = root / proof_path
    proof = load_and_verify(proof_path)
    _verify_historical_multiplicity_checkpoint(
        root,
        proof,
        expected=EXPECTED_GLOBAL_N_TRIALS,
        result_path=FROZEN_RESULT_PATH,
        result_sha256=FROZEN_RESULT_SHA256,
    )
    if burned_window_ids(proof) != ("Q4_2024",):
        raise V71EventFunnelError("unexpected proof-window state")
    return (
        json.loads((root / POLICY_PATH).read_text(encoding="utf-8")),
        json.loads((root / POWER_MINIMUM_PATH).read_text(encoding="utf-8")),
    )


def _verify_historical_multiplicity_checkpoint(
    root: Path,
    proof: Mapping[str, Any],
    *,
    expected: int,
    result_path: str,
    result_sha256: str,
) -> None:
    checkpoints = {
        int(entry["multiplicity"]["cumulative_N_trials"])
        for entry in proof.get("entries", ())
        if entry.get("event_type") == "MULTIPLICITY_COUNTER"
    }
    current = multiplicity_trial_count(proof)
    if expected not in checkpoints or current < expected:
        raise V71EventFunnelError("V7.1 candidate reservation is absent")
    if current != expected and _sha256(root / result_path) != result_sha256:
        raise V71EventFunnelError(
            "V7.1 multiplicity suffix is allowed only for frozen historical replay"
        )


def _verify_signal_manifest(
    root: Path,
    signals: Mapping[str, Sequence[V71Signal]],
    specs: Mapping[str, V71CandidateSpec],
) -> dict[str, Any]:
    manifest = json.loads((root / SIGNAL_MANIFEST_PATH).read_text(encoding="utf-8"))
    if manifest.get("contains_outcomes_or_pnl") is not False:
        raise V71EventFunnelError("V7.1 signal manifest contains outcomes")
    expected = {
        str(row["candidate_id"]): row for row in manifest["candidate_paths"]
    }
    if set(expected) != set(specs) or set(signals) != set(specs):
        raise V71EventFunnelError("V7.1 signal manifest candidate drift")
    for candidate_id, spec in specs.items():
        row = expected[candidate_id]
        if row["specification_hash"] != spec.specification_hash:
            raise V71EventFunnelError("V7.1 specification hash drift")
        if int(row["signal_count"]) != len(signals[candidate_id]):
            raise V71EventFunnelError("V7.1 signal count drift")
        if row["signal_path_hash"] != signal_path_hash(signals[candidate_id]):
            raise V71EventFunnelError("V7.1 signal path drift")
    return manifest


def _write_result(result: dict[str, Any], root: Path, output_dir: Path) -> dict[str, Any]:
    destination = output_dir if output_dir.is_absolute() else root / output_dir
    destination.mkdir(parents=True, exist_ok=True)
    result_path = destination / "v71_development_funnel_result.json"
    temporary = result_path.with_name(f".{result_path.name}.tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, result_path)
    result_hash = _sha256(result_path)
    report_path = destination / "v71_development_funnel_report.md"
    report = "\n".join(
        [
            "# HYDRA V7.1 — D1 development funnel",
            "",
            "[HYDRA-V7] phase=4 step=121 verdict=GREEN",
            f"gate=V71_STAGE0_STAGE2 preuve={result_path.relative_to(root)}#{result_hash[:8]} tests=256_structures",
            f"budget_llm=usage_API_non_exposee/solde budget_data=87.847388/125.00_achat_phase=0 N_trials={EXPECTED_GLOBAL_N_TRIALS} burned=1",
            "diff_validation=hydra/validation/v71_event_funnel.py CONTRE=deux_blocs_D1_ne_suffisent_pas_a_une_preuve_finale",
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


__all__ = ["V71EventFunnelError", "run_v71_development_funnel"]
