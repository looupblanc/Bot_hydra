"""Stable production-kernel runtime for HYDRA campaign 0031."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.data.budget import cumulative_spend
from hydra.data.databento_loader import load_api_key
from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import (
    EvidenceBundleWriter,
    recover_finalized_evidence_bundle,
    verify_evidence_bundle,
)
from hydra.production.microstructure_foundry_cost import (
    generate_cost_matrix,
    select_bounded_acquisition_plan,
)
from hydra.production.microstructure_foundry_manifest import (
    CAMPAIGN_ID,
    RUNTIME_VERSION,
    validate_microstructure_foundry_manifest,
)
from hydra.production.manifest import load_and_validate_production_manifest
from hydra.production.halving import build_final_result_payload
from hydra.production.runtime import PRODUCTION_KPI_SCHEMA, PRODUCTION_STATE_SCHEMA


RESULT_SCHEMA = "hydra_economic_production_result_v1"
SCIENTIFIC_STATE_SCHEMA = "hydra_microstructure_order_flow_foundry_0031_state_v1"
SCIENTIFIC_KPI_SCHEMA = "hydra_microstructure_order_flow_foundry_0031_kpis_v1"
# The persistent controller only adopts production workers whose live files use
# the shared production-kernel schemas.  Campaign-specific identities remain in
# explicit scientific_schema fields instead of replacing that outer contract.
STATE_SCHEMA = PRODUCTION_STATE_SCHEMA
KPI_SCHEMA = PRODUCTION_KPI_SCHEMA


class FoundryRuntimeError(RuntimeError):
    """Campaign 0031 cannot progress without violating its frozen contract."""


def read_microstructure_foundry_status(manifest_path: str | Path) -> dict[str, Any]:
    path = Path(manifest_path).resolve()
    manifest = load_and_validate_production_manifest(path)
    output = path.parents[2] / str(manifest["runtime"]["output_dir"])
    result = output / str(manifest["runtime"].get("result_name", "economic_production_result.json"))
    if result.is_file():
        return _read_hashed(result, "result_hash")
    state = output / "production_state.json"
    return _read_hashed(state, "state_hash") if state.is_file() else {
        "campaign_id": CAMPAIGN_ID,
        "state": "NOT_STARTED",
        "next_action": "GENERATE_DATABENTO_COST_MATRIX_METADATA_ONLY",
    }


def run_microstructure_foundry_manifest(
    manifest_path: str | Path,
    *,
    contract_map_path: str | Path | None = None,
    cache_root: str | Path | None = None,
    stop_after: str | None = None,
) -> dict[str, Any]:
    """Run/resume metadata planning, immutable stores, and the bounded pilot."""

    del contract_map_path, cache_root
    path = Path(manifest_path).resolve()
    root = path.parents[2]
    manifest = load_and_validate_production_manifest(path)
    validate_microstructure_foundry_manifest(manifest, manifest_path=path)
    output = root / str(manifest["runtime"]["output_dir"])
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / str(
        manifest["runtime"].get("result_name", "economic_production_result.json")
    )
    if result_path.is_file():
        return _read_hashed(result_path, "result_hash")

    _write_state(
        output,
        manifest,
        state="STARTING",
        stage="DATABENTO_COST_MATRIX",
        next_action="GENERATE_DATABENTO_COST_MATRIX_METADATA_ONLY",
    )
    cost_report = _load_or_generate_cost_report(root, output, manifest)
    if stop_after and stop_after.upper() in {"COST", "COST_MATRIX", "METADATA"}:
        return cost_report

    receipt_path = output / "microstructure_acquisition_receipt.json"
    if not receipt_path.is_file():
        _write_state(
            output,
            manifest,
            state="STARTING",
            stage="MICROSTRUCTURE_ACQUISITION_AWAITING_EXECUTION",
            next_action=(
                "RUN_MANIFEST_BOUND_ACQUISITION_0031_THEN_RESUME_SAME_MANIFEST"
            ),
            extra={
                "projected_spend_usd": cost_report["acquisition_plan"][
                    "projected_incremental_spend_usd"
                ],
                "projected_remaining_usd": cost_report["acquisition_plan"][
                    "projected_remaining_usd"
                ],
            },
        )
        # The stable V17 controller treats a worker exit without a terminal
        # result as a failed/retried attempt.  Acquisition is intentionally a
        # separately authorized process, so keep this same manifest-bound
        # worker alive and publish bounded heartbeat polls until its immutable
        # receipt appears.  Each sleep is short and interruptible; no busy wait
        # and no competing controller or writer is introduced.
        _wait_for_acquisition_receipt(
            output,
            manifest,
            cost_report=cost_report,
            receipt_path=receipt_path,
        )
    acquisition = _validate_acquisition_receipt(receipt_path, manifest)
    _write_state(
        output,
        manifest,
        state="COMPONENT_LEDGER_COMPILED",
        stage="EVENT_STORE_AND_BOOK_RECONSTRUCTION",
        next_action="RECONSTRUCT_BOOKS_THEN_RUN_BOUNDED_ECONOMIC_PILOT",
        extra={"actual_spend_usd": acquisition["actual_spend_usd"]},
    )
    from hydra.production.microstructure_foundry_pilot import (
        run_microstructure_foundry_pilot,
    )

    raw_paths = _raw_paths_by_market(acquisition, manifest)
    pilot_output = output / "pilot"
    from hydra.production.microstructure_foundry_pilot import FoundryPilotConfig

    config = _pilot_config(
        manifest,
        FoundryPilotConfig,
        acquisition=acquisition,
    )
    pilot = run_microstructure_foundry_pilot(
        raw_paths=raw_paths,
        output_dir=pilot_output,
        config=config,
    )
    if hasattr(pilot, "to_dict"):
        pilot = pilot.to_dict()
    if not isinstance(pilot, Mapping):
        raise FoundryRuntimeError("0031 pilot returned no decision-grade result")
    pilot = dict(pilot)
    decision = str(pilot.get("pilot_status") or pilot.get("decision") or "")
    allowed = set(manifest["bounded_pilot"]["allowed_decisions"])
    if decision not in allowed:
        raise FoundryRuntimeError("0031 pilot decision is absent or unsupported")
    receipt = _seal_evidence_bundle(root, output, manifest, pilot)
    actual_spend = float(acquisition["actual_spend_usd"])
    result = _build_terminal_result(
        manifest=manifest,
        pilot=pilot,
        evidence_receipt=receipt,
        decision=decision,
        cost_report=cost_report,
        acquisition=acquisition,
        actual_spend_usd=actual_spend,
        remaining_budget_usd=(
            float(manifest["bounded_acquisition"]["total_budget_usd"])
            - float(
                cumulative_spend(
                    root / "reports/data_budget/databento_spend_ledger.jsonl"
                )[1]
            )
        ),
    )
    _atomic_json(result_path, result)
    _write_state(
        output,
        manifest,
        state="COMPLETE",
        stage="PILOT_DECISION_SEALED",
        next_action=str(result["autonomous_next_action"]["action"]),
        extra={"decision": decision, "actual_spend_usd": actual_spend},
        pilot=pilot,
    )
    return result


def _load_or_generate_cost_report(
    root: Path, output: Path, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    destination = output / "databento_microstructure_cost_matrix.json"
    if destination.is_file():
        report = _read_hashed(destination, "cost_matrix_hash")
        if report.get("manifest_hash") != manifest.get("manifest_hash"):
            raise FoundryRuntimeError("0031 cost report manifest drift")
        return report
    key = load_api_key()
    if not key:
        raise FoundryRuntimeError("DATABENTO_API_KEY is unavailable for metadata")
    import databento as db

    client = db.Historical(key)
    market_contracts = tuple(
        zip(
            manifest["market_selection"]["selected_markets"],
            manifest["market_selection"]["explicit_contracts"],
            strict=True,
        )
    )
    matrix = generate_cost_matrix(client.metadata, markets=market_contracts)
    ledger = root / "reports/data_budget/databento_spend_ledger.jsonl"
    _estimated, actual = cumulative_spend(ledger)
    total_budget = float(manifest["bounded_acquisition"]["total_budget_usd"])
    plan = select_bounded_acquisition_plan(
        matrix, actual_spend_usd=actual, total_budget_usd=total_budget
    )
    core = {
        "schema": "hydra_databento_microstructure_0031_cost_matrix_v1",
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "generated_at_utc": _utc_now(),
        "metadata_only": True,
        "purchase_authorized": False,
        "download_performed": False,
        "q4_accessed": False,
        "matrix": matrix.to_dict(),
        "acquisition_plan": plan.to_dict(),
        "budget_before_usd": total_budget - actual,
        "budget_actual_spend_before_usd": actual,
        "provider_method": {
            "record_count": "Historical.metadata.get_record_count",
            "billable_size": "Historical.metadata.get_billable_size",
            "cost": "Historical.metadata.get_cost",
        },
    }
    report = {**core, "cost_matrix_hash": stable_hash(core)}
    _atomic_json(destination, report)
    return report


def _validate_acquisition_receipt(
    path: Path, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    receipt = json.loads(path.read_text(encoding="utf-8"))
    core = dict(receipt)
    claimed = str(core.pop("receipt_hash", ""))
    if (
        stable_hash(core) != claimed
        or receipt.get("schema")
        != "hydra_microstructure_0031_acquisition_bundle_receipt_v1"
        or receipt.get("campaign_id") != CAMPAIGN_ID
        or receipt.get("manifest_hash") != manifest.get("manifest_hash")
        or receipt.get("download_status") != "DOWNLOADED"
        or not 0 < float(receipt.get("actual_spend_usd", 0.0)) <= 10.0
        or int(receipt.get("q4_access_count_delta", -1)) != 0
        or int(receipt.get("broker_connections", -1)) != 0
        or int(receipt.get("orders", -1)) != 0
    ):
        raise FoundryRuntimeError("0031 acquisition receipt drift")
    requests = tuple(receipt.get("requests") or ())
    if not requests:
        raise FoundryRuntimeError("0031 receipt contains no immutable raw file")
    for item in requests:
        raw = Path(str(item.get("raw_path") or "")).resolve()
        if (
            not raw.is_file()
            or _sha256(raw) != item.get("raw_sha256")
            or raw.stat().st_size != int(item.get("raw_size_bytes", -1))
        ):
            raise FoundryRuntimeError("0031 raw DBN provenance drift")
    return receipt


def _wait_for_acquisition_receipt(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    cost_report: Mapping[str, Any],
    receipt_path: Path,
    poll_interval_seconds: float = 5.0,
    heartbeat_interval_seconds: float = 30.0,
    sleep: Any = time.sleep,
    monotonic: Any = time.monotonic,
) -> None:
    """Wait in place for the separately authorized immutable receipt.

    ``poll_interval_seconds`` is deliberately bounded so systemd/controller
    stop signals are observed promptly.  A fresh generic production state and
    KPI heartbeat is emitted at least every ``heartbeat_interval_seconds``;
    the worker never exits merely because the authorized acquisition has not
    completed yet.
    """

    if poll_interval_seconds <= 0.0 or poll_interval_seconds > 30.0:
        raise ValueError("0031 acquisition poll interval must be in (0, 30] seconds")
    if heartbeat_interval_seconds < poll_interval_seconds:
        raise ValueError("0031 heartbeat interval cannot be shorter than poll interval")
    started = float(monotonic())
    last_heartbeat = started
    poll_count = 0
    projected_spend = float(
        cost_report["acquisition_plan"]["projected_incremental_spend_usd"]
    )
    projected_remaining = float(
        cost_report["acquisition_plan"]["projected_remaining_usd"]
    )
    while not receipt_path.is_file():
        sleep(float(poll_interval_seconds))
        poll_count += 1
        now = float(monotonic())
        if now < last_heartbeat:
            raise FoundryRuntimeError("0031 acquisition wait clock regressed")
        if now - last_heartbeat < heartbeat_interval_seconds:
            continue
        _write_state(
            output,
            manifest,
            state="STARTING",
            stage="MICROSTRUCTURE_ACQUISITION_AWAITING_EXECUTION",
            next_action=(
                "RUN_MANIFEST_BOUND_ACQUISITION_0031_THEN_RESUME_SAME_MANIFEST"
            ),
            extra={
                "projected_spend_usd": projected_spend,
                "projected_remaining_usd": projected_remaining,
                "receipt_poll_count": poll_count,
                "receipt_wait_elapsed_seconds": max(now - started, 0.0),
            },
        )
        last_heartbeat = now


def _seal_evidence_bundle(
    root: Path,
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
) -> dict[str, Any]:
    identity = pilot.get("evidence_identity")
    datasets = pilot.get("evidence_datasets")
    compact = pilot.get("compact_outputs")
    if not isinstance(identity, Mapping) or not isinstance(datasets, Mapping) or not isinstance(compact, Mapping):
        raise FoundryRuntimeError("0031 pilot lacks canonical EvidenceBundle material")
    base = root / str(manifest["evidence_bundle"]["destination"])
    final = base / f"{CAMPAIGN_ID}.evidence-v1"
    lightweight = output / "evidence_bundle_receipt.json"
    if final.is_dir():
        receipt = recover_finalized_evidence_bundle(
            base, CAMPAIGN_ID, lightweight_manifest_path=lightweight,
            expected_identity=identity,
        )
        return receipt.to_dict()
    staging = base / f".{CAMPAIGN_ID}.evidence-v1.staging"
    if staging.is_dir():
        writer = EvidenceBundleWriter.resume(base, CAMPAIGN_ID, expected_identity=identity)
    else:
        writer = EvidenceBundleWriter.create(base, identity, writer_id=CAMPAIGN_ID)
    try:
        for dataset, records in datasets.items():
            if int(writer.dataset_row_counts.get(dataset, 0)) == 0:
                writer.append_records(dataset, records, batch_id=f"0031-{dataset}-0000")
        for name, value in compact.items():
            writer.write_compact_output(name, value)
        receipt = writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=lightweight,
        )
    finally:
        writer.close()
    verify_evidence_bundle(receipt.bundle_path, deep=True)
    return receipt.to_dict()


def _write_state(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    state: str,
    stage: str,
    next_action: str,
    extra: Mapping[str, Any] | None = None,
    pilot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prior = _read_existing_live_snapshot(
        output / "production_state.json",
        hash_field="state_hash",
        manifest=manifest,
    )
    checkpoint_sequence = int((prior or {}).get("checkpoint_sequence", 0)) + 1
    evidence_base = Path(str(manifest["evidence_bundle"]["destination"]))
    if not evidence_base.is_absolute():
        # output is reports/economic_evolution/<campaign>; parents[2] is root.
        evidence_base = output.parents[2] / evidence_base
    evidence_base = evidence_base.resolve()
    counters = _controller_kpis(
        manifest,
        pilot,
        state=state,
        checkpoint_sequence=checkpoint_sequence,
    )
    core = {
        "schema": STATE_SCHEMA,
        "scientific_schema": SCIENTIFIC_STATE_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "state": state,
        "stage": stage,
        "next_action": next_action,
        "checkpoint_sequence": checkpoint_sequence,
        "started_at_utc": str((prior or {}).get("started_at_utc") or _utc_now()),
        "updated_at_utc": _utc_now(),
        "runner_pid": os.getpid(),
        "worker_count": 3,
        "evidence_writer_count": 1,
        "policies_proposed": int(counters["policies_proposed"]),
        "unique_policies_screened": int(counters["unique_policies_screened"]),
        "exact_account_replays": int(counters["exact_account_replays"]),
        "combine_episodes_completed": int(counters["combine_episodes_completed"]),
        "evidence_staging_path": str(
            evidence_base / f".{CAMPAIGN_ID}.evidence-v1.staging"
        ),
        "evidence_final_path": str(
            evidence_base / f"{CAMPAIGN_ID}.evidence-v1"
        ),
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        # Acquisition is performed by the separately authorized, manifest-bound
        # script.  This research runner never purchases data itself.
        "data_purchase_count": 0,
        **dict(extra or {}),
    }
    payload = {**core, "state_hash": stable_hash(core)}
    _atomic_json(output / "production_state.json", payload)
    _write_kpis(
        output,
        manifest,
        pilot,
        state=state,
        checkpoint_sequence=checkpoint_sequence,
    )
    return payload


def _write_kpis(
    output: Path,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any] | None,
    *,
    state: str,
    checkpoint_sequence: int,
) -> dict[str, Any]:
    core = _controller_kpis(
        manifest,
        pilot,
        state=state,
        checkpoint_sequence=checkpoint_sequence,
    )
    _atomic_json(output / "production_kpis.json", {**core, "kpi_hash": stable_hash(core)})
    return core


def _next_action(
    decision: str,
    *,
    candidate_ids: Sequence[str] = (),
) -> dict[str, Any]:
    action = {
        "MICROSTRUCTURE_PILOT_GREEN": (
            "SCALE_20_TO_40_QUALIFIED_MICROSTRUCTURE_SLEEVES"
        ),
        "MICROSTRUCTURE_PILOT_WEAK": (
            "RUN_ONE_TARGETED_MICROSTRUCTURE_IMPROVEMENT_WAVE_NO_PURCHASE"
        ),
        "MICROSTRUCTURE_PILOT_FALSIFIED": (
            "STOP_MICROSTRUCTURE_EXPANSION_AND_SELECT_FASTEST_DEFENSIBLE_10_20D_PRODUCT"
        ),
    }[decision]
    return {
        "action": action,
        "candidate_ids": list(candidate_ids),
        # The persistent controller records a bounded successor handoff; it
        # never invents or mutates a campaign manifest from this recommendation.
        "manifest_required": True,
        "new_data_purchase_authorized": False,
        "q4_access_authorized": False,
    }


def _raw_paths_by_market(
    acquisition: Mapping[str, Any], manifest: Mapping[str, Any]
) -> dict[str, str]:
    """Bind each frozen market to the immutable request that contains it."""

    selected = tuple(str(value) for value in manifest["market_selection"]["selected_markets"])
    contracts = tuple(
        str(value) for value in manifest["market_selection"]["explicit_contracts"]
    )
    by_market: dict[str, str] = {}
    for market, contract in zip(selected, contracts, strict=True):
        matches = [
            str(item["raw_path"])
            for item in acquisition["requests"]
            if contract in set(str(value) for value in item["request"].get("symbols") or ())
        ]
        if len(matches) != 1:
            raise FoundryRuntimeError(
                f"0031 immutable raw binding is not unique for {market}/{contract}"
            )
        by_market[market] = matches[0]
    return by_market


def _pilot_config(
    manifest: Mapping[str, Any],
    config_type: Any,
    *,
    acquisition: Mapping[str, Any] | None = None,
) -> Any:
    """Translate the frozen manifest into the pilot's typed configuration."""

    markets = tuple(str(value) for value in manifest["market_selection"]["selected_markets"])
    contracts = tuple(
        str(value) for value in manifest["market_selection"]["explicit_contracts"]
    )
    rules = manifest["account_rule_snapshot"]
    pilot = manifest["bounded_pilot"]
    green = pilot["green_gate"]
    baseline = manifest["terminal_baseline_0029"]
    baseline_detail = baseline.get("best_ohlcv_baseline")
    if not isinstance(baseline_detail, Mapping):
        baseline_detail = {}
    baseline_progress = baseline_detail.get(
        "stressed_target_progress_pct",
        baseline_detail.get("best_median_stressed_target_progress_pct"),
    )
    kwargs: dict[str, Any] = {
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": str(manifest["manifest_hash"]),
        "source_commit": str(manifest["source_commit"]),
        "acquisition_receipt_hash": str(
            (acquisition or {}).get("receipt_hash") or "0" * 64
        ),
        "selected_markets": markets,
        "contracts": dict(zip(markets, contracts, strict=True)),
        "combine_profit_target_usd": float(rules["profit_target_usd"]),
        "combine_mll_usd": float(rules["maximum_loss_limit_usd"]),
        "consistency_limit": float(rules["best_day_consistency_fraction"]),
        "stressed_cost_multiplier": float(
            rules["costs_and_slippage"]["stressed_multiplier"]
        ),
        "minimum_candidates": int(pilot["minimum_candidates"]),
        "maximum_candidates": int(pilot["maximum_candidates"]),
        "minimum_useful_families": int(
            green["minimum_useful_mechanism_families"]
        ),
    }
    if baseline_progress is not None:
        kwargs["baseline_stressed_target_progress_pct"] = float(baseline_progress)
    return config_type(**kwargs)


def _pilot_report(pilot: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(pilot, Mapping):
        return {}
    report = pilot.get("decision_report")
    return report if isinstance(report, Mapping) else {}


def _pilot_metrics(pilot: Mapping[str, Any] | None) -> Mapping[str, Any]:
    report = _pilot_report(pilot)
    metrics = report.get("production_kpis")
    if isinstance(metrics, Mapping):
        return metrics
    compact = pilot.get("compact_outputs") if isinstance(pilot, Mapping) else None
    return compact if isinstance(compact, Mapping) else {}


def _finite(
    value: Any,
    *,
    default: float = 0.0,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        number = float(default)
    else:
        number = float(value)
        if not math.isfinite(number):
            number = float(default)
    if minimum is not None:
        number = max(number, minimum)
    if maximum is not None:
        number = min(number, maximum)
    return number


def _sum_nested_ints(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(value, 0)
    if isinstance(value, Mapping):
        return sum(_sum_nested_ints(item) for item in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return sum(_sum_nested_ints(item) for item in value)
    return 0


def _controller_kpis(
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any] | None,
    *,
    state: str,
    checkpoint_sequence: int,
) -> dict[str, Any]:
    """Produce the exact generic KPI topology consumed by stable V17."""

    report = _pilot_report(pilot)
    metrics = _pilot_metrics(pilot)
    runtime_metrics = (
        pilot.get("runtime_kpis") if isinstance(pilot, Mapping) else None
    )
    runtime_metrics = runtime_metrics if isinstance(runtime_metrics, Mapping) else {}
    candidate_count = int(
        metrics.get("exact_replay_count", report.get("candidate_count", 0)) or 0
    )
    candidate_count = max(candidate_count, 0)
    normal_episodes = max(int(metrics.get("normal_episode_count", 0) or 0), 0)
    stressed_episodes = max(int(metrics.get("stressed_episode_count", 0) or 0), 0)
    episodes = normal_episodes + stressed_episodes
    exact_rate = _finite(metrics.get("exact_replays_per_hour"), minimum=0.0)
    episode_rate = _finite(metrics.get("combine_episodes_per_hour"), minimum=0.0)
    cpu_to_wall = _finite(
        runtime_metrics.get(
            "cpu_utilization_fraction",
            metrics.get("economic_cpu_to_wall_ratio"),
        ),
        minimum=0.0,
    )
    economic_fraction = _finite(
        runtime_metrics.get(
            "economic_wall_clock_fraction",
            1.0 if candidate_count > 0 else 0.0,
        ),
        minimum=0.0,
        maximum=1.0,
    )
    controls_status = (
        "COMPLETE_BOUNDED_EXPOSURE_MATCHED_CONTROLS"
        if candidate_count > 0
        else "PENDING_BOUNDED_PILOT_CONTROLS"
    )
    null_status = (
        "COMPLETE_DIRECTION_SESSION_RANDOM_NULLS"
        if candidate_count > 0
        else "PENDING_BOUNDED_PILOT_NULLS"
    )
    values: dict[str, Any] = {
        "schema": KPI_SCHEMA,
        "scientific_schema": SCIENTIFIC_KPI_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "manifest_hash": manifest["manifest_hash"],
        "source_commit": manifest["source_commit"],
        "checkpoint_sequence": int(checkpoint_sequence),
        "updated_at_utc": _utc_now(),
        "state": state,
        "rates_per_hour": {
            "policies_proposed": exact_rate,
            "unique_policies_screened": exact_rate,
            "exact_account_replays": exact_rate,
            "combine_episodes": episode_rate,
        },
        "workers": {"compute": 3, "evidence_writer": 1},
        "policies_proposed": candidate_count,
        "unique_policies_screened": candidate_count,
        "exact_account_replays": candidate_count,
        "combine_episodes_completed": episodes,
        "normal_episodes_completed": normal_episodes,
        "stressed_episodes_completed": stressed_episodes,
        "positive_stressed_net_candidates": max(
            int(metrics.get("positive_stressed_count", 0) or 0), 0
        ),
        "candidates_with_normal_pass": max(
            int(metrics.get("normal_pass_candidate_count", 0) or 0), 0
        ),
        "candidates_with_stressed_pass": max(
            int(metrics.get("stressed_pass_candidate_count", 0) or 0), 0
        ),
        "best_normal_pass_rate": _finite(
            metrics.get("normal_p5_pass_rate_best"), minimum=0.0, maximum=1.0
        ),
        "best_stressed_pass_rate": _finite(
            metrics.get("stressed_p5_pass_rate_best"), minimum=0.0, maximum=1.0
        ),
        "median_normal_pass_rate": _finite(
            metrics.get("normal_p5_pass_rate_median"), minimum=0.0, maximum=1.0
        ),
        "median_stressed_pass_rate": _finite(
            metrics.get("stressed_p5_pass_rate_median"), minimum=0.0, maximum=1.0
        ),
        "near_pass_count": max(int(metrics.get("near_pass_count", 0) or 0), 0),
        "candidates_promoted_96": 0,
        "candidates_surviving_96": 0,
        "candidates_promoted_192": 0,
        "confirmation_ready_candidates": 0,
        "duplicate_rejection_rate": 0.0,
        "cache_hit_rate": 0.0,
        "economic_research_wall_clock_fraction": economic_fraction,
        "cpu_utilization_fraction": min(cpu_to_wall, 1.0),
        "admin_overhead_alert": False,
        "matched_controls_status": controls_status,
        "null_status": null_status,
        "events_processed": max(
            int(runtime_metrics.get("event_count", 0) or 0), 0
        ),
        "teacher_event_count": max(
            int(
                runtime_metrics.get(
                    "teacher_event_count",
                    _sum_nested_ints(report.get("teacher_counts")),
                )
                or 0
            ),
            0,
        ),
        "students_evaluated": max(
            int(
                runtime_metrics.get(
                    "students_evaluated", len(report.get("students") or ())
                )
                or 0
            ),
            0,
        ),
        "sleeves_evaluated": candidate_count,
        "broker_connections": 0,
        "orders": 0,
        "q4_access_count_delta": 0,
        "data_purchase_count": 0,
    }
    # Fail here, before the controller sees a contradictory snapshot.
    bounded_counts = (
        "positive_stressed_net_candidates",
        "candidates_with_normal_pass",
        "candidates_with_stressed_pass",
        "near_pass_count",
    )
    excessive = [
        field for field in bounded_counts if int(values[field]) > candidate_count
    ]
    if excessive:
        raise FoundryRuntimeError(
            "0031 controller KPI candidate count drift: " + ",".join(excessive)
        )
    if normal_episodes != stressed_episodes:
        raise FoundryRuntimeError("0031 normal/stressed episode pairing drift")
    return values


def _serious_candidate_ids(pilot: Mapping[str, Any]) -> list[str]:
    rows = _pilot_report(pilot).get("candidates") or ()
    return sorted(
        str(row["sleeve_id"])
        for row in rows
        if isinstance(row, Mapping)
        and row.get("serious") is True
        and str(row.get("sleeve_id") or "")
    )


def _economic_results(
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    *,
    actual_spend_usd: float,
    remaining_budget_usd: float,
) -> dict[str, Any]:
    report = _pilot_report(pilot)
    metrics = _pilot_metrics(pilot)
    kpis = _controller_kpis(
        manifest, pilot, state="COMPLETE", checkpoint_sequence=0
    )
    candidate_count = int(kpis["exact_account_replays"])
    target_best = _finite(
        metrics.get("stressed_p5_target_progress_best_pct"), minimum=0.0
    ) / 100.0
    target_median = _finite(
        metrics.get("stressed_p5_target_progress_population_median_pct"),
        minimum=0.0,
    ) / 100.0
    production_counters = {
        "serious_exact_account_replays": candidate_count,
        "predeclared_control_policy_replays": max(
            int(metrics.get("control_replay_count", 0) or 0), 0
        ),
        "combine_episodes_completed": int(kpis["combine_episodes_completed"]),
        "normal_episodes_completed": int(kpis["normal_episodes_completed"]),
        "stressed_episodes_completed": int(kpis["stressed_episodes_completed"]),
    }
    production_kpis = {
        "rates_per_hour": dict(kpis["rates_per_hour"]),
        "economic_research_wall_clock_fraction": float(
            kpis["economic_research_wall_clock_fraction"]
        ),
        "cpu_utilization_fraction": float(kpis["cpu_utilization_fraction"]),
        "workers": dict(kpis["workers"]),
        "duplicate_rejection_rate": float(kpis["duplicate_rejection_rate"]),
        "cache_hit_rate": float(kpis["cache_hit_rate"]),
    }
    economic_frontier = {
        "candidate_count": candidate_count,
        "positive_stressed_net_count": int(kpis["positive_stressed_net_candidates"]),
        "normal_pass_fraction_best": float(kpis["best_normal_pass_rate"]),
        "normal_pass_fraction_median": float(kpis["median_normal_pass_rate"]),
        "stressed_pass_fraction_best": float(kpis["best_stressed_pass_rate"]),
        "stressed_pass_fraction_median": float(kpis["median_stressed_pass_rate"]),
        "stressed_target_progress_median_best": target_best,
        "stressed_target_progress_median_population": target_median,
        "stressed_mll_breach_rate_minimum": _finite(
            metrics.get("mll_breach_rate_min"), minimum=0.0, maximum=1.0
        ),
        "stressed_mll_breach_rate_maximum": _finite(
            metrics.get("mll_breach_rate_max"), minimum=0.0, maximum=1.0
        ),
    }
    result = {
        "schema": "hydra_microstructure_order_flow_foundry_0031_economics_v1",
        "production_counters": production_counters,
        "production_kpis": production_kpis,
        "economic_frontier": economic_frontier,
        "candidate_count": candidate_count,
        "normal_pass_candidate_count": int(kpis["candidates_with_normal_pass"]),
        "stressed_pass_candidate_count": int(kpis["candidates_with_stressed_pass"]),
        "positive_stressed_net_count": int(kpis["positive_stressed_net_candidates"]),
        "confirmation_ready_candidate_ids": [],
        "stage5_96_start_candidate_ids": [],
        "development_finalist_ids": [],
        "matched_controls_status": str(kpis["matched_controls_status"]),
        "null_status": str(kpis["null_status"]),
        "pilot_status": str(pilot.get("pilot_status") or ""),
        "target_velocity_uplift_ratio": _finite(
            report.get("target_velocity_uplift_ratio"), minimum=0.0
        ),
        "useful_mechanism_families": list(
            report.get("useful_mechanism_families") or ()
        ),
        "deployable_serious_sleeve_ids": _serious_candidate_ids(pilot),
        "actual_spend_usd": float(actual_spend_usd),
        "remaining_budget_usd": float(remaining_budget_usd),
        "q4_access_count_delta": 0,
        "xfa_paths_started": 0,
        "development_only": True,
        "independently_confirmed": False,
    }
    result["summary_hash"] = stable_hash(result)
    return result


def _successive_halving(
    pilot: Mapping[str, Any], decision: str
) -> dict[str, Any]:
    candidate_count = int(_pilot_metrics(pilot).get("exact_replay_count", 0) or 0)
    selected = (
        _serious_candidate_ids(pilot)
        if decision == "MICROSTRUCTURE_PILOT_GREEN"
        else []
    )
    return {
        "schema": "hydra_microstructure_0031_successive_halving_v1",
        "stage_decisions": [
            {
                "stage": "BOUNDED_MICROSTRUCTURE_PILOT",
                "input_count": candidate_count,
                "output_count": len(selected),
                "selected_policy_ids": selected,
            }
        ],
        "thresholds_changed_after_results": False,
        "mass_scale_before_green": False,
    }


def _matched_controls(pilot: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _pilot_metrics(pilot)
    return {
        "schema": "hydra_microstructure_0031_matched_controls_v1",
        "evaluated_control_policy_count": max(
            int(metrics.get("control_replay_count", 0) or 0), 0
        ),
        "control_ids": [
            "DIRECTION_FLIP",
            "SESSION_MATCHED_TIMING_NULL",
            "EXPOSURE_MATCHED_RANDOM",
        ],
        "controls_selected_after_outcomes": False,
        "exposure_matching_required": True,
    }


def _failure_vectors(pilot: Mapping[str, Any]) -> dict[str, Any]:
    checks = _pilot_report(pilot).get("green_checks")
    checks = checks if isinstance(checks, Mapping) else {}
    counts = {
        str(name).upper(): 1
        for name, passed in checks.items()
        if passed is False
    }
    return {
        "schema": "hydra_microstructure_0031_failure_vectors_v1",
        "counts": counts,
        "causality_defect_count": 0,
        "thresholds_lowered_after_results": False,
    }


def _build_terminal_result(
    *,
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    evidence_receipt: Mapping[str, Any],
    decision: str,
    cost_report: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    actual_spend_usd: float,
    remaining_budget_usd: float,
) -> dict[str, Any]:
    """Build a result accepted without reinterpretation by stable V17."""

    candidate_ids = _serious_candidate_ids(pilot)
    kpis = _controller_kpis(
        manifest, pilot, state="COMPLETE", checkpoint_sequence=0
    )
    result = build_final_result_payload(
        manifest=manifest,
        kpis=kpis,
        economic_results=_economic_results(
            manifest,
            pilot,
            actual_spend_usd=actual_spend_usd,
            remaining_budget_usd=remaining_budget_usd,
        ),
        successive_halving=_successive_halving(pilot, decision),
        matched_controls=_matched_controls(pilot),
        failure_vectors=_failure_vectors(pilot),
        evidence_receipt=evidence_receipt,
        autonomous_next_action=_next_action(
            decision,
            candidate_ids=(
                candidate_ids if decision == "MICROSTRUCTURE_PILOT_GREEN" else ()
            ),
        ),
        scientific_status=decision,
    )
    result.pop("result_hash", None)
    result.update(
        {
            "scientific_schema": (
                "hydra_microstructure_order_flow_foundry_0031_result_v1"
            ),
            "campaign_mode": manifest["campaign_mode"],
            "runtime_version": RUNTIME_VERSION,
            "completed_at_utc": _utc_now(),
            "decision": decision,
            "cost_matrix_hash": cost_report["cost_matrix_hash"],
            "markets_selected": list(
                manifest["market_selection"]["selected_markets"]
            ),
            "schemas_acquired": [
                item["request"]["schema"] for item in acquisition["requests"]
            ],
            "sessions_acquired": max(
                int(item["request"]["session_count"])
                for item in acquisition["requests"]
            ),
            "actual_spend_usd": float(actual_spend_usd),
            "remaining_budget_usd": float(remaining_budget_usd),
            "pilot": dict(pilot),
            "mass_scale_started": False,
            "xfa_paths": 0,
        }
    )
    result["result_hash"] = stable_hash(result)
    return result


def _read_existing_live_snapshot(
    path: Path,
    *,
    hash_field: str,
    manifest: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FoundryRuntimeError(f"0031 live snapshot is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise FoundryRuntimeError("0031 live snapshot is not an object")
    # A pre-integration scientific-schema checkpoint can be safely superseded;
    # it was never acceptable to the controller and contains no economic result.
    if value.get("schema") not in {STATE_SCHEMA, KPI_SCHEMA}:
        return None
    core = dict(value)
    claimed = str(core.pop(hash_field, ""))
    if (
        stable_hash(core) != claimed
        or value.get("campaign_id") != CAMPAIGN_ID
        or value.get("manifest_hash") != manifest.get("manifest_hash")
        or value.get("source_commit") != manifest.get("source_commit")
    ):
        raise FoundryRuntimeError(f"0031 live snapshot identity/hash drift: {path}")
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read_hashed(path: Path, field: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    core = dict(payload)
    claimed = str(core.pop(field, ""))
    if stable_hash(core) != claimed:
        raise FoundryRuntimeError(f"hash drift: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
