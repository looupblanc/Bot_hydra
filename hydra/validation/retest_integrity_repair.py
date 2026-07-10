from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd


PILOT_VERSION = "validator_integrity_repair_pilot_v1"
EXPECTED_BRANCH = "INVALID_VALIDATOR_INTEGRITY_REPAIR"
EXPECTED_EXPERIMENT_TYPE = "validator_integrity_repair_pilot"
FUTURES_MONTH_CODES = "FGHJKMNQUVXZ"


class RetestIntegrityRepairError(RuntimeError):
    pass


def run_validator_integrity_repair_pilot(
    output_dir: str | Path,
    *,
    source_execution_result_path: str | Path,
    source_execution_result_hash: str,
    source_execution_experiment_id: str,
    source_execution_specification_hash: str,
    post_retest_design_path: str | Path,
    engineering_task_path: str | Path,
    engineering_task_hash: str,
    selected_post_retest_branch: str,
    code_commit: str,
) -> dict[str, Any]:
    """Diagnose the frozen invalid retest without rerunning candidate evidence."""
    source_path = Path(source_execution_result_path)
    task_path = Path(engineering_task_path)
    design_path = Path(post_retest_design_path)
    source = _load_hashed_json(source_path, "result_hash", source_execution_result_hash)
    task = _load_hashed_json(task_path, "engineering_task_hash", engineering_task_hash)
    design = _load_hashed_json(design_path, "design_hash", None)
    _verify_frozen_contract(
        source=source,
        task=task,
        design=design,
        selected_branch=selected_post_retest_branch,
        code_commit=code_commit,
        source_execution_result_hash=source_execution_result_hash,
    )

    controls = _replay_frozen_validator_controls(source)
    sentinels = _audit_invariant_sentinels(source)
    contract_map_path = Path(str((source.get("data_provenance") or {}).get("contract_map_path") or ""))
    expected_map_hash = str((source.get("data_provenance") or {}).get("contract_map_sha256") or "")
    if not contract_map_path.is_file() or _file_sha256(contract_map_path) != expected_map_hash:
        raise RetestIntegrityRepairError("Frozen explicit contract map is missing or its checksum changed.")
    contract_map = _load_json(contract_map_path)
    definition_frame, definition_path = _load_best_cached_definition_frame(
        contract_map_path.parent,
        {str(row.get("instrument_id")) for row in contract_map.get("contracts") or []},
    )
    contract_audit = audit_date_aware_contract_symbols(contract_map, definition_frame)
    contract_audit.update(
        {
            "frozen_contract_map_path": str(contract_map_path),
            "frozen_contract_map_sha256": expected_map_hash,
            "definition_dbn_path": str(definition_path),
            "definition_dbn_sha256": _file_sha256(definition_path),
            "definition_dbn_size_bytes": definition_path.stat().st_size,
        }
    )

    map_defect_confirmed = bool(
        contract_audit["invalid_frozen_map_symbol_count"] > 0
        and contract_audit["date_aware_valid_future_symbol_count"] == contract_audit["segment_count"]
        and contract_audit["date_aware_symbol_change_count"]
        == contract_audit["invalid_frozen_map_symbol_count"]
    )
    validator_calibrated = bool(controls["recomputed_validator_controls_passed"])
    invalidity_reproduced = bool(
        str(source.get("scientific_conclusion") or "").startswith("INVALID_")
        and sentinels["invariant_sentinel_count"] > 0
        and sentinels["all_invariant_sentinels_insufficient"]
    )
    if not validator_calibrated:
        conclusion = "VALIDATOR_CALIBRATION_DEFECT_REPRODUCED_NO_CANDIDATE_RERUN"
        disposition = "VALIDATOR_REPAIR_REQUIRED"
        isolated_defect = "REPEATED_POSITIVE_NEGATIVE_CONTROL_CALIBRATION_FAILED"
    elif map_defect_confirmed:
        conclusion = "CONTRACT_MAP_DATE_FLATTENING_INTEGRITY_DEFECT_CONFIRMED_NO_CANDIDATE_RERUN"
        disposition = "CONTRACT_MAP_REBUILD_REQUIRED"
        isolated_defect = "DATE_INSENSITIVE_INSTRUMENT_ID_TO_RAW_SYMBOL_FLATTENING"
    elif sentinels["all_invariant_sentinels_insufficient"]:
        conclusion = "VALIDATOR_CALIBRATED_HISTORICAL_SENTINELS_UNDERPOWERED_NO_CANDIDATE_RERUN"
        disposition = "SENTINEL_REDESIGN_REQUIRED"
        isolated_defect = "HISTORICAL_FAILURES_ARE_NOT_CONCLUSIVE_NEGATIVE_SENTINELS"
    else:
        conclusion = "INTEGRITY_INVALIDITY_NOT_REPRODUCED_FAIL_CLOSED"
        disposition = "INTEGRITY_AUDIT_INCONCLUSIVE"
        isolated_defect = "NONE_ISOLATED"

    implementation_commit = _git_commit()
    _verify_implementation_descends_from(code_commit, implementation_commit)
    payload: dict[str, Any] = {
        "schema": PILOT_VERSION,
        "pilot_version": PILOT_VERSION,
        "scientific_conclusion": conclusion,
        "integrity_disposition": disposition,
        "isolated_defect": isolated_defect,
        "invalidity_reproduced": invalidity_reproduced,
        "validator_controls_calibrated": validator_calibrated,
        "candidate_evidence_valid_for_decision_change": False,
        "candidate_evidence_rerun_count": 0,
        "fully_validated_edge_atoms": 0,
        "validated_strategies": 0,
        "interpretation_boundary": (
            "This pilot diagnoses validator, sentinel, and contract-map integrity only. It does not rerun or "
            "reclassify any candidate, validate any mechanism or strategy, or authorize holdout access."
        ),
        "source": {
            "execution_experiment_id": source_execution_experiment_id,
            "execution_specification_hash": source_execution_specification_hash,
            "execution_result_path": str(source_path),
            "execution_result_hash": source_execution_result_hash,
            "execution_scientific_conclusion": source.get("scientific_conclusion"),
            "post_retest_design_path": str(design_path),
            "post_retest_design_hash": design.get("design_hash"),
            "engineering_task_path": str(task_path),
            "engineering_task_hash": engineering_task_hash,
            "engineering_baseline_commit": code_commit,
            "implementation_commit": implementation_commit,
        },
        "frozen_control_replay": controls,
        "invariant_sentinel_audit": sentinels,
        "contract_map_integrity_audit": contract_audit,
        "required_next_action": (
            "REBUILD_EXPLICIT_CONTRACT_MAP_FROM_DATE_AWARE_CACHED_DEFINITIONS_THEN_RETEST_FROM_ZERO"
            if disposition == "CONTRACT_MAP_REBUILD_REQUIRED"
            else disposition
        ),
        "next_engineering_scope": {
            "objective": (
                "Build a new immutable roll-map artifact from date-aware cached definition records, preserve the "
                "historical map unchanged, validate every root/month/year symbol, then rerun no candidate until "
                "the repaired map passes an independent integrity audit."
            ),
            "paid_data_required": False,
            "q4_access_required": False,
            "source_market_observation_read_required": False,
            "candidate_rerun_allowed_before_repair": False,
        },
        "governance": {
            "q4_access_count_delta": 0,
            "incremental_databento_spend_usd": 0.0,
            "network_requests": 0,
            "live_or_broker_execution": False,
            "market_observation_rows_read": 0,
            "cached_definition_metadata_only": True,
        },
    }
    payload["result_hash"] = _stable_hash(payload)
    destination = Path(output_dir)
    json_path = destination / "validator_integrity_repair_pilot.json"
    report_path = destination / "validator_integrity_repair_pilot.md"
    _write_immutable(json_path, json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n")
    _write_immutable(report_path, _render_report(payload))
    return {
        **payload,
        "artifacts": {"result_json_path": str(json_path), "report_path": str(report_path)},
        "report_path": str(report_path),
    }


def audit_date_aware_contract_symbols(
    contract_map: dict[str, Any], definition_frame: pd.DataFrame
) -> dict[str, Any]:
    contracts = list(contract_map.get("contracts") or [])
    required = {
        "ts_event",
        "instrument_id",
        "raw_symbol",
        "instrument_class",
        "security_type",
        "asset",
    }
    missing = sorted(required - set(definition_frame.columns))
    if missing:
        raise RetestIntegrityRepairError(f"Cached definition frame lacks required columns: {missing}")
    frame = definition_frame.copy()
    frame["instrument_id_key"] = frame["instrument_id"].astype(str)
    frame["ts_event"] = pd.to_datetime(frame["ts_event"], utc=True)
    by_id = {key: group.sort_values("ts_event") for key, group in frame.groupby("instrument_id_key")}
    rows: list[dict[str, Any]] = []
    for contract in contracts:
        root = str(contract.get("root") or "")
        instrument_id = str(contract.get("instrument_id") or "")
        definitions = by_id.get(instrument_id)
        if definitions is None or definitions.empty:
            raise RetestIntegrityRepairError(f"No cached definition history for instrument ID {instrument_id}.")
        active_start = _utc_timestamp(contract.get("active_start"))
        start_day_end = active_start.normalize() + pd.Timedelta(days=1)
        available = definitions[definitions["ts_event"] < start_day_end]
        selected = available.iloc[-1] if not available.empty else definitions.iloc[0]
        frozen_symbol = str(contract.get("contract") or "")
        resolved_symbol = str(selected.get("raw_symbol") or "")
        frozen_valid = _valid_future_symbol(root, frozen_symbol)
        resolved_valid = bool(
            _valid_future_symbol(root, resolved_symbol)
            and str(selected.get("instrument_class") or "") == "F"
            and str(selected.get("security_type") or "") == "FUT"
            and str(selected.get("asset") or "") == root
        )
        rows.append(
            {
                "root": root,
                "instrument_id": instrument_id,
                "active_start": str(contract.get("active_start")),
                "active_end": str(contract.get("active_end")),
                "frozen_map_symbol": frozen_symbol,
                "frozen_map_symbol_valid": frozen_valid,
                "date_aware_definition_symbol": resolved_symbol,
                "date_aware_definition_valid": resolved_valid,
                "definition_event_time": pd.Timestamp(selected["ts_event"]).isoformat(),
                "symbol_changed_by_date_aware_resolution": frozen_symbol != resolved_symbol,
            }
        )
    invalid = [row for row in rows if not row["frozen_map_symbol_valid"]]
    corrected = [row for row in rows if row["symbol_changed_by_date_aware_resolution"]]
    return {
        "map_type": contract_map.get("map_type"),
        "segment_count": len(rows),
        "valid_frozen_map_symbol_count": sum(row["frozen_map_symbol_valid"] for row in rows),
        "invalid_frozen_map_symbol_count": len(invalid),
        "date_aware_valid_future_symbol_count": sum(row["date_aware_definition_valid"] for row in rows),
        "date_aware_symbol_change_count": len(corrected),
        "all_invalid_frozen_symbols_repaired_by_date_aware_resolution": bool(
            invalid
            and len(corrected) == len(invalid)
            and all(row["date_aware_definition_valid"] for row in corrected)
        ),
        "defect_mechanism": (
            "Instrument IDs are reused across dates; flattening instrument_id->raw_symbol to one global value "
            "assigns unrelated futures, spreads, or options to later continuous-symbol intervals."
        ),
        "date_aware_resolution_rule": (
            "For each continuous interval, select the final definition event available by the end of the "
            "interval start UTC calendar day; use the first cached event only for a pre-cache initial interval."
        ),
        "invalid_or_corrected_segments": corrected,
    }


def _replay_frozen_validator_controls(source: dict[str, Any]) -> dict[str, Any]:
    pipeline = ((source.get("validator_controls") or {}).get("pipeline_v2_decisive") or {})
    decisions = list(pipeline.get("decisions") or [])
    positives = [row for row in decisions if bool(row.get("expected_positive"))]
    negatives = [row for row in decisions if not bool(row.get("expected_positive"))]
    if not positives or not negatives:
        raise RetestIntegrityRepairError("Frozen repeated positive/negative controls are absent.")
    positive_successes = sum(bool(row.get("edge_detected")) for row in positives)
    false_positives = sum(bool(row.get("edge_detected")) for row in negatives)
    power = positive_successes / len(positives)
    fpr = false_positives / len(negatives)
    power_lower = _wilson_bound(positive_successes, len(positives), side="lower")
    fpr_upper = _wilson_bound(false_positives, len(negatives), side="upper")
    target_results: dict[str, dict[str, Any]] = {}
    for target in sorted({str(row.get("target_kind")) for row in positives}):
        target_rows = [row for row in positives if str(row.get("target_kind")) == target]
        successes = sum(bool(row.get("edge_detected")) for row in target_rows)
        target_results[target] = {
            "successes": successes,
            "trials": len(target_rows),
            "empirical_power": successes / len(target_rows),
            "one_sided_95pct_wilson_lower_bound": _wilson_bound(
                successes, len(target_rows), side="lower"
            ),
        }
    negatives_conclusive = all(str(row.get("status")) == "RETEST_FALSIFIED" for row in negatives)
    recomputed_pass = bool(
        power_lower >= 0.80
        and fpr_upper <= 0.20
        and negatives_conclusive
        and all(row["one_sided_95pct_wilson_lower_bound"] >= 0.80 for row in target_results.values())
    )
    declared_matches = bool(
        abs(float(pipeline.get("power_on_meaningful_effects", -1.0)) - power) <= 1e-12
        and abs(float(pipeline.get("false_positive_rate", -1.0)) - fpr) <= 1e-12
        and bool(pipeline.get("passed")) == recomputed_pass
    )
    return {
        "control_decision_count": len(decisions),
        "positive_control_trials": len(positives),
        "negative_control_trials": len(negatives),
        "positive_successes": positive_successes,
        "false_positives": false_positives,
        "power_on_meaningful_effects": power,
        "power_one_sided_95pct_wilson_lower_bound": power_lower,
        "false_positive_rate": fpr,
        "false_positive_rate_one_sided_95pct_wilson_upper_bound": fpr_upper,
        "power_by_target": target_results,
        "negative_controls_all_conclusively_falsified": negatives_conclusive,
        "declared_metrics_match_replay": declared_matches,
        "recomputed_validator_controls_passed": recomputed_pass and declared_matches,
        "selection_universe_size_each_run": pipeline.get("selection_universe_size_each_run"),
        "seed_count": pipeline.get("seed_count"),
    }


def _audit_invariant_sentinels(source: dict[str, Any]) -> dict[str, Any]:
    sentinels = [
        row
        for row in source.get("results") or []
        if row.get("selection_role") == "CALIBRATION_INVARIANT_OLD_FAILURE"
    ]
    rows = [
        {
            "atom_id": row.get("atom_id"),
            "historical_atom_id": row.get("historical_atom_id"),
            "status": row.get("status"),
            "reason": row.get("reason"),
            "insufficient_decisive_attacks": row.get("insufficient_decisive_attacks") or [],
            "insufficient_gates": row.get("insufficient_gates") or [],
            "conclusive_failure_gates": row.get("conclusive_failure_gates") or [],
        }
        for row in sentinels
    ]
    return {
        "invariant_sentinel_count": len(rows),
        "all_invariant_sentinels_insufficient": bool(
            rows and all(row["status"] == "INVARIANT_CONTROL_INSUFFICIENT" for row in rows)
        ),
        "unexpected_sentinel_survival_count": sum(
            row["status"] == "INVARIANT_CONTROL_UNEXPECTED_SURVIVAL" for row in rows
        ),
        "sentinels": rows,
        "interpretation": (
            "A historical failure that is not evaluable under the corrected mandatory nulls is not a valid "
            "negative sentinel and cannot establish validator false-positive control."
        ),
    }


def _verify_frozen_contract(
    *,
    source: dict[str, Any],
    task: dict[str, Any],
    design: dict[str, Any],
    selected_branch: str,
    code_commit: str,
    source_execution_result_hash: str,
) -> None:
    if selected_branch != EXPECTED_BRANCH or task.get("selected_branch") != EXPECTED_BRANCH:
        raise RetestIntegrityRepairError("Frozen post-retest branch is not the validator-integrity branch.")
    if task.get("pilot_experiment_type") != EXPECTED_EXPERIMENT_TYPE:
        raise RetestIntegrityRepairError("Frozen engineering task selects a different pilot type.")
    if task.get("immutable_before_implementation") is not True:
        raise RetestIntegrityRepairError("Engineering task was not immutable before implementation.")
    if task.get("source_execution_result_hash") != source_execution_result_hash:
        raise RetestIntegrityRepairError("Engineering task source hash differs from the frozen result.")
    if task.get("code_commit") != code_commit or source.get("code_commit") != code_commit:
        raise RetestIntegrityRepairError("Engineering baseline commit differs across frozen artifacts.")
    if design.get("selected_branch") != EXPECTED_BRANCH:
        raise RetestIntegrityRepairError("Post-retest design branch changed after selection.")
    embedded = design.get("engineering_task_specification") or {}
    if embedded.get("engineering_task_hash") != task.get("engineering_task_hash"):
        raise RetestIntegrityRepairError("Post-retest design embeds a different engineering task.")
    governance = source.get("governance") or {}
    safe = (
        governance.get("q4_access_count_delta") == 0
        and float(governance.get("incremental_databento_spend_usd", -1.0)) == 0.0
        and governance.get("network_requests") == 0
        and governance.get("live_or_broker_execution") is False
        and governance.get("latest_data_end_exclusive") == "2024-10-01"
    )
    if not safe:
        raise RetestIntegrityRepairError("Frozen source violates the development-only governance boundary.")


def _load_best_cached_definition_frame(
    directory: Path, instrument_ids: set[str]
) -> tuple[pd.DataFrame, Path]:
    candidates = sorted(directory.glob("definitions_*.dbn.zst"))
    if not candidates:
        raise RetestIntegrityRepairError("No cached Databento definition DBN is available.")
    try:
        import databento as db
    except ImportError as exc:
        raise RetestIntegrityRepairError("Databento reader is unavailable for cached definitions.") from exc
    best: tuple[int, str, pd.DataFrame, Path] | None = None
    for path in candidates:
        frame = db.DBNStore.from_file(path).to_df(pretty_ts=True, map_symbols=False).reset_index()
        if "instrument_id" not in frame.columns:
            continue
        coverage = len(set(frame["instrument_id"].astype(str)) & instrument_ids)
        candidate = (coverage, str(path), frame, path)
        if best is None or candidate[:2] > best[:2]:
            best = candidate
    if best is None or best[0] < len(instrument_ids):
        raise RetestIntegrityRepairError(
            f"Cached definition coverage is incomplete: {0 if best is None else best[0]}/{len(instrument_ids)}."
        )
    return best[2], best[3]


def _load_hashed_json(path: Path, hash_key: str, expected_hash: str | None) -> dict[str, Any]:
    payload = _load_json(path)
    stored = str(payload.get(hash_key) or "")
    body = {key: value for key, value in payload.items() if key != hash_key}
    calculated = _stable_hash(body)
    if not stored or stored != calculated or (expected_hash is not None and stored != str(expected_hash)):
        raise RetestIntegrityRepairError(f"Frozen artifact hash mismatch: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise RetestIntegrityRepairError(f"Required frozen artifact is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RetestIntegrityRepairError(f"Frozen artifact root is not an object: {path}")
    return payload


def _valid_future_symbol(root: str, symbol: str) -> bool:
    return bool(re.fullmatch(rf"{re.escape(root)}[{FUTURES_MONTH_CODES}]\d{{1,2}}", symbol))


def _wilson_bound(successes: int, trials: int, *, side: str) -> float:
    if trials <= 0:
        return 0.0 if side == "lower" else 1.0
    z = 1.6448536269514722
    proportion = successes / trials
    denominator = 1.0 + z * z / trials
    center = (proportion + z * z / (2.0 * trials)) / denominator
    half_width = z * math.sqrt(
        proportion * (1.0 - proportion) / trials + z * z / (4.0 * trials * trials)
    ) / denominator
    return max(0.0, center - half_width) if side == "lower" else min(1.0, center + half_width)


def _verify_implementation_descends_from(baseline: str, implementation: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", baseline) or not re.fullmatch(r"[0-9a-f]{40}", implementation):
        return
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", baseline, implementation],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise RetestIntegrityRepairError("Implementation commit does not descend from the frozen engineering base.")


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "UNKNOWN"


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_immutable(path: Path, content: str) -> None:
    if path.exists():
        if path.read_text(encoding="utf-8") == content:
            return
        raise RetestIntegrityRepairError(f"Refusing to overwrite divergent immutable artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _render_report(payload: dict[str, Any]) -> str:
    audit = payload["contract_map_integrity_audit"]
    controls = payload["frozen_control_replay"]
    return (
        "# HYDRA Validator / Retest Integrity Repair Pilot\n\n"
        f"- Conclusion: `{payload['scientific_conclusion']}`\n"
        f"- Result hash: `{payload['result_hash']}`\n"
        f"- Validator controls calibrated: `{payload['validator_controls_calibrated']}`\n"
        f"- Frozen-map invalid symbols: `{audit['invalid_frozen_map_symbol_count']}` / "
        f"`{audit['segment_count']}`\n"
        f"- Date-aware valid symbols: `{audit['date_aware_valid_future_symbol_count']}` / "
        f"`{audit['segment_count']}`\n"
        f"- Positive-control power: `{controls['power_on_meaningful_effects']}`\n"
        f"- Negative-control FPR: `{controls['false_positive_rate']}`\n"
        "- Candidate reruns: `0`\n"
        "- Q4 access: `0`\n"
        "- Paid data: `$0`\n\n"
        "## Interpretation boundary\n\n"
        f"{payload['interpretation_boundary']}\n"
    )
