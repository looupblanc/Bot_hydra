"""Bounded static-risk preflight for HYDRA causal target velocity 0028.

The preflight deliberately reuses the causal trajectories and chronological
account simulator sealed by sprint 0027.  It changes neither signals nor fill
times.  The requested normalized frontier is made executable with whole micro
contracts by binding 1.00x to four micros and mapping 0.75/1.00/1.25/1.50 to
three/four/five/six micros respectively.

The six former books are diagnostic references only.  A 1.00x replay is
required to reproduce their clean 0027 result exactly before any other
frontier result can be accepted.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.account_policy.active_risk_pool import ActiveRiskPoolPolicy
from hydra.compute.result_writer import AtomicResultWriter
from hydra.production import causal_salvage_runtime as salvage
from hydra.research.causal_sleeve_replay import (
    CausalCensoredTrajectory,
    CausalTradeMark,
    CausalTradeTrajectory,
)


CAUSAL_RISK_PREFLIGHT_VERSION = "hydra_causal_risk_preflight_v1"
CAUSAL_RISK_PREFLIGHT_SCHEMA = "hydra_causal_risk_preflight_result_v1"
CAMPAIGN_ID = "hydra_causal_target_velocity_0028"
NORMALIZED_RISK_LEVELS = (0.75, 1.0, 1.25, 1.5)
EXECUTABLE_REFERENCE_MICROS = 4
EXECUTABLE_MICRO_QUANTITIES = (3, 4, 5, 6)
DEFAULT_MANIFEST = Path("config/v7/causal_target_velocity_0028.json")
DEFAULT_BASELINE_MANIFEST = Path("config/v7/causal_salvage_sprint_0027.json")
DEFAULT_BASELINE_RESULTS = Path(
    "reports/economic_evolution/causal_salvage_sprint_0027_revision_02/"
    "phase_a_results.json"
)
DEFAULT_RESERVATION = Path(
    "reports/economic_evolution/"
    "hydra_causal_target_velocity_0028_multiplicity_reservation.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "reports/economic_evolution/causal_target_velocity_0028/preflight"
)


class CausalRiskPreflightError(RuntimeError):
    """The bounded preflight failed closed before a valid result was sealed."""


def executable_micro_quantity(normalized_level: float) -> int:
    """Resolve one preregistered normalized level to whole micros."""

    for level, quantity in zip(
        NORMALIZED_RISK_LEVELS, EXECUTABLE_MICRO_QUANTITIES, strict=True
    ):
        if math.isclose(float(normalized_level), level, abs_tol=1e-12):
            return quantity
    raise CausalRiskPreflightError("risk level escaped the frozen frontier")


def scale_causal_trajectory(
    trajectory: CausalTradeTrajectory | CausalCensoredTrajectory,
    *,
    executable_quantity_multiplier: int,
) -> CausalTradeTrajectory | CausalCensoredTrajectory:
    """Scale only executable quantity and its linear economic consequences.

    Signal identity, causal timestamps, raw price-derived direction, and fill
    boundaries remain untouched.  This is equivalent to routing the original
    one-micro intent at an integer static tier, while allowing the shared
    policy object itself to remain at its identity tier.
    """

    multiplier = int(executable_quantity_multiplier)
    if multiplier <= 0:
        raise CausalRiskPreflightError("executable risk multiplier must be positive")
    event = trajectory.event
    scaled_event = replace(
        event,
        net_pnl=float(event.net_pnl) * multiplier,
        gross_pnl=float(event.gross_pnl) * multiplier,
        worst_unrealized_pnl=float(event.worst_unrealized_pnl) * multiplier,
        best_unrealized_pnl=float(event.best_unrealized_pnl) * multiplier,
        quantity=int(event.quantity) * multiplier,
        mini_equivalent=float(event.mini_equivalent) * multiplier,
    )
    scaled_marks = tuple(
        replace(
            mark,
            worst_unrealized_pnl=float(mark.worst_unrealized_pnl) * multiplier,
            best_unrealized_pnl=float(mark.best_unrealized_pnl) * multiplier,
            current_unrealized_pnl=(
                None
                if mark.current_unrealized_pnl is None
                else float(mark.current_unrealized_pnl) * multiplier
            ),
        )
        for mark in trajectory.marks
    )
    return replace(
        trajectory,
        event=scaled_event,
        marks=scaled_marks,
        initial_unrealized_pnl=(
            float(trajectory.initial_unrealized_pnl) * multiplier
        ),
    )


def normalized_policy(
    policy: ActiveRiskPoolPolicy,
    *,
    base_entity_id: str,
    normalized_level: float,
) -> ActiveRiskPoolPolicy:
    """Return the same governor at the identity tier with explicit provenance."""

    level = float(normalized_level)
    quantity = executable_micro_quantity(level)
    suffix = str(level).replace(".", "p")
    return replace(
        policy,
        policy_id=f"risk-preflight:{base_entity_id}:{suffix}x:{quantity}micro",
        static_risk_tier=1.0,
    )


def risk_scale_gate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Apply the frozen 3/48 normal, 2/48 stress, positive-net, <=10% MLL gate."""

    survivors: list[str] = []
    for row in rows:
        normal = row["normal"]
        stressed = row["stressed"]
        if (
            int(normal["pass_count"]) >= 3
            and int(stressed["pass_count"]) >= 2
            and float(stressed["net_total"]) > 0.0
            and float(normal["mll_breach_rate"]) <= 0.10
            and float(stressed["mll_breach_rate"]) <= 0.10
        ):
            survivors.append(str(row["preflight_policy_id"]))
    return {
        "minimum_normal_passes_out_of_48": 3,
        "minimum_stressed_passes_out_of_48": 2,
        "positive_stressed_net_required": True,
        "maximum_mll_breach_rate": 0.10,
        "survivor_count": len(survivors),
        "survivor_ids": sorted(survivors),
        "status": (
            "RISK_SCALE_ONLY_SURVIVORS_FOUND"
            if survivors
            else "RISK_SCALE_ONLY_FALSIFIED"
        ),
        "neighboring_multiplier_search_allowed": False,
    }


def run_causal_risk_preflight(
    manifest_path: str | Path = DEFAULT_MANIFEST,
    *,
    repository_root: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Execute and atomically seal the preregistered 0028 risk preflight."""

    started = time.perf_counter()
    root = (
        Path(repository_root).resolve()
        if repository_root is not None
        else Path(__file__).resolve().parents[2]
    )
    manifest_file = _inside(root, manifest_path)
    manifest = _read_json(manifest_file)
    _validate_campaign_manifest(manifest)
    reservation = _verify_reservation(root)
    output_root = _inside(root, output_dir or DEFAULT_OUTPUT_DIR)
    existing_path = output_root / "risk_frontier_preflight_result.json"
    if existing_path.exists():
        existing = _read_json(existing_path)
        core = {
            key: value
            for key, value in existing.items()
            if key not in {"result_hash", "runtime_seconds"}
        }
        if (
            existing.get("schema") != CAUSAL_RISK_PREFLIGHT_SCHEMA
            or existing.get("campaign_id") != CAMPAIGN_ID
            or existing.get("manifest_sha256") != _sha256(manifest_file)
            or existing.get("multiplicity_reservation", {}).get("sha256")
            != reservation["sha256"]
            or existing.get("result_hash") != salvage.stable_hash(core)
        ):
            raise CausalRiskPreflightError(
                "existing preflight result is not an idempotent sealed result"
            )
        return existing
    baseline_manifest = _read_json(root / DEFAULT_BASELINE_MANIFEST)
    baseline_results_path = root / DEFAULT_BASELINE_RESULTS
    baseline_results = _read_json(baseline_results_path)
    baseline = _verify_clean_baseline(
        baseline_results, phase_a_results_path=baseline_results_path
    )

    reconstructed, package_receipt = salvage._load_frozen_packages(  # noqa: SLF001
        root, baseline_manifest
    )
    temporal, temporal_receipt = salvage._load_frozen_temporal_contract(  # noqa: SLF001
        root, reconstructed
    )
    specs = dict(reconstructed[0].sleeve_specs)
    bindings = dict(reconstructed[0].frozen_signal_bindings)
    matrices = salvage._open_frozen_matrices(root, bindings)  # noqa: SLF001
    replays, failures = salvage._compile_causal_sleeves(  # noqa: SLF001
        specs,
        bindings,
        matrices,
        start_inclusive=str(
            baseline_manifest["economic_contract"]["development_start"]
        ),
        end_exclusive=str(
            baseline_manifest["economic_contract"]["development_end_exclusive"]
        ),
    )
    del matrices
    if failures or len(replays) != 18:
        raise CausalRiskPreflightError(
            "the clean causal eighteen-sleeve bank did not recompile exactly"
        )

    evaluation_manifest = {**baseline_manifest, "temporal_blocks": temporal}
    runtimes = {
        sleeve_id: salvage.causal_runtime(replay, specs[sleeve_id], scenario="NORMAL")
        for sleeve_id, replay in replays.items()
    }
    starts = salvage._block_aware_starts(  # noqa: SLF001
        runtimes, evaluation_manifest, maximum=48
    )
    if len(starts) != 48:
        raise CausalRiskPreflightError("preflight did not resolve exactly 48 starts")
    calendars = salvage._block_calendars(  # noqa: SLF001
        runtimes, evaluation_manifest, starts
    )
    full_calendars = salvage._remaining_chronological_calendars(  # noqa: SLF001
        runtimes, starts
    )

    raw_normal = {
        sleeve_id: (
            *replay.normal_trajectories,
            *tuple(getattr(replay, "normal_censored_trajectories", ())),
        )
        for sleeve_id, replay in replays.items()
    }
    raw_stressed = {
        sleeve_id: (
            *replay.stressed_trajectories,
            *tuple(getattr(replay, "stressed_censored_trajectories", ())),
        )
        for sleeve_id, replay in replays.items()
    }
    positive_sleeves = tuple(baseline["positive_sleeve_ids"])
    base_policies: list[tuple[str, str, ActiveRiskPoolPolicy]] = [
        (sleeve_id, "LOW_VELOCITY_CAUSAL_COMPONENT", salvage._standalone_policy(sleeve_id))  # noqa: SLF001
        for sleeve_id in positive_sleeves
    ] + [
        (
            row.package.candidate_id,
            "FORMER_BOOK_DIAGNOSTIC_REFERENCE",
            row.combine_policy,
        )
        for row in sorted(reconstructed, key=lambda value: value.package.candidate_id)
    ]
    if len(base_policies) != 23:
        raise CausalRiskPreflightError("preflight entity cardinality drift")

    # Reconcile the six former books at their exact clean 0027 sizing before
    # applying the new executable frontier.  The frontier's normalized 1.00x
    # reference is explicitly four micros, so it is not the historical
    # one-micro/tiered identity replay and must never be compared as if it were.
    salvage._set_work(  # noqa: SLF001
        normal=raw_normal,
        stressed=raw_stressed,
        calendars=calendars,
        full_calendars=full_calendars,
        blocks=temporal["blocks"],
    )
    identity_policies = [
        policy.to_dict()
        for _base_id, kind, policy in base_policies
        if kind == "FORMER_BOOK_DIAGNOSTIC_REFERENCE"
    ]
    identity_rows, identity_evidence_parts = salvage._evaluate_policies_parallel(  # noqa: SLF001
        identity_policies,
        starts=starts,
        horizons=(90,),
        include_stress=True,
        batch_size=6,
    )
    if identity_evidence_parts:
        raise CausalRiskPreflightError(
            "identity replay unexpectedly emitted deep evidence"
        )
    identity = _verify_former_book_raw_identity(identity_rows, baseline_results)

    compact_rows: list[dict[str, Any]] = []
    for level in NORMALIZED_RISK_LEVELS:
        quantity = executable_micro_quantity(level)
        scaled_normal = _scale_bank(raw_normal, quantity)
        scaled_stressed = _scale_bank(raw_stressed, quantity)
        salvage._set_work(  # noqa: SLF001
            normal=scaled_normal,
            stressed=scaled_stressed,
            calendars=calendars,
            full_calendars=full_calendars,
            blocks=temporal["blocks"],
        )
        policy_meta: dict[str, tuple[str, str]] = {}
        policies: list[dict[str, Any]] = []
        for base_id, kind, base_policy in base_policies:
            policy = normalized_policy(
                base_policy,
                base_entity_id=base_id,
                normalized_level=level,
            )
            policy_meta[policy.policy_id] = (base_id, kind)
            policies.append(policy.to_dict())
        evaluated, evidence_parts = salvage._evaluate_policies_parallel(  # noqa: SLF001
            policies,
            starts=starts,
            horizons=(90,),
            include_stress=True,
            batch_size=8,
        )
        if evidence_parts:
            raise CausalRiskPreflightError("preflight unexpectedly emitted deep evidence")
        for row in evaluated:
            base_id, kind = policy_meta[str(row["policy_id"])]
            compact_rows.append(
                _compact_row(
                    row,
                    base_entity_id=base_id,
                    entity_kind=kind,
                    normalized_level=level,
                    executable_micro_quantity_value=quantity,
                )
            )

    gate = risk_scale_gate(compact_rows)
    frontier = _frontier_summary(compact_rows)
    result_core = {
        "schema": CAUSAL_RISK_PREFLIGHT_SCHEMA,
        "campaign_id": CAMPAIGN_ID,
        "stage": "BOUNDED_RISK_FRONTIER_PREFLIGHT_COMPLETE",
        "status": gate["status"],
        "runtime_version": CAUSAL_RISK_PREFLIGHT_VERSION,
        "manifest_path": str(manifest_file.relative_to(root)),
        "manifest_sha256": _sha256(manifest_file),
        "source_commit": salvage._git_head(root),  # noqa: SLF001
        "multiplicity_reservation": reservation,
        "clean_baseline": baseline,
        "package_receipt_hash": salvage.stable_hash(package_receipt),
        "temporal_contract": temporal_receipt,
        "risk_contract": {
            "normalized_levels": list(NORMALIZED_RISK_LEVELS),
            "executable_reference_micro_quantity": EXECUTABLE_REFERENCE_MICROS,
            "executable_micro_quantities": list(EXECUTABLE_MICRO_QUANTITIES),
            "mapping": {
                str(level): executable_micro_quantity(level)
                for level in NORMALIZED_RISK_LEVELS
            },
            "frontier_reference_definition": "FOUR_MICRO_EXECUTABLE_REFERENCE",
            "frontier_one_x_is_historical_identity": False,
            "raw_unscaled_former_book_identity_required": True,
            "fractional_contracts_used": False,
            "signals_or_fill_timestamps_changed": False,
            "cost_model_changed": False,
            "neighboring_multiplier_search_allowed": False,
        },
        "evaluation_contract": {
            "entity_count": 23,
            "positive_sleeve_count": 17,
            "former_book_reference_count": 6,
            "risk_level_count": 4,
            "policy_evaluation_count": len(compact_rows),
            "starts": list(starts),
            "starts_count": len(starts),
            "horizon_trading_days": 90,
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "xfa_paths_started": 0,
        },
        "former_book_raw_unscaled_identity": identity,
        "frontier_summary": frontier,
        "gate": gate,
        "results": sorted(
            compact_rows,
            key=lambda value: (
                float(value["normalized_risk_level"]),
                str(value["entity_kind"]),
                str(value["base_entity_id"]),
            ),
        ),
        "safety": {
            "q4_access_delta": 0,
            "market_data_purchase_delta_usd": 0.0,
            "broker_connections": 0,
            "orders": 0,
            "live_trading": False,
            "xfa_paths_started": 0,
        },
    }
    result = {
        **result_core,
        "result_hash": salvage.stable_hash(result_core),
        "runtime_seconds": time.perf_counter() - started,
    }
    AtomicResultWriter(output_root, immutable=True).write_json(
        "risk_frontier_preflight_result.json", result
    )
    return result


def _scale_bank(
    bank: Mapping[
        str, Sequence[CausalTradeTrajectory | CausalCensoredTrajectory]
    ],
    quantity: int,
) -> dict[str, tuple[CausalTradeTrajectory | CausalCensoredTrajectory, ...]]:
    return {
        sleeve_id: tuple(
            scale_causal_trajectory(
                row, executable_quantity_multiplier=int(quantity)
            )
            for row in values
        )
        for sleeve_id, values in bank.items()
    }


def _compact_row(
    row: Mapping[str, Any],
    *,
    base_entity_id: str,
    entity_kind: str,
    normalized_level: float,
    executable_micro_quantity_value: int,
) -> dict[str, Any]:
    normal = row["scenarios"]["NORMAL"]["90_TRADING_DAYS"]
    stressed = row["scenarios"]["STRESSED_1_5X"]["90_TRADING_DAYS"]
    fields = (
        "episode_count",
        "pass_count",
        "pass_rate",
        "mll_breach_count",
        "mll_breach_rate",
        "net_total",
        "net_median",
        "target_progress_p25",
        "target_progress_median",
        "target_progress_maximum",
        "minimum_mll_buffer",
        "minimum_mll_buffer_p25",
        "minimum_mll_buffer_median",
        "minimum_mll_buffer_p75",
        "consistency_rate",
        "median_days_to_target",
        "pass_block_count",
        "pass_block_ids",
        "by_block_pass_count",
        "by_block_net",
        "by_block_target_progress_median",
        "terminal_distribution",
        "accepted_event_count",
        "skipped_event_count",
    )
    return {
        "base_entity_id": base_entity_id,
        "entity_kind": entity_kind,
        "preflight_policy_id": str(row["policy_id"]),
        "normalized_risk_level": float(normalized_level),
        "executable_micro_quantity": int(executable_micro_quantity_value),
        "preflight_fingerprint": salvage.stable_hash(
            {
                "base_entity_id": base_entity_id,
                "normalized_risk_level": float(normalized_level),
                "executable_micro_quantity": int(executable_micro_quantity_value),
                "base_policy_fingerprint": str(row["structural_fingerprint"]),
            }
        ),
        "normal": {key: normal[key] for key in fields},
        "stressed": {key: stressed[key] for key in fields},
    }


def _verify_clean_baseline(
    results: Mapping[str, Any], *, phase_a_results_path: Path
) -> dict[str, Any]:
    rows = list(results.get("results") or ())
    sleeves = [row for row in rows if row.get("entity_kind") == "STANDALONE_SLEEVE"]
    books = [
        row
        for row in rows
        if row.get("entity_kind") == "FORMER_BOOK_DIAGNOSTIC_REFERENCE"
    ]
    primary = "90_TRADING_DAYS"
    positive = [
        str(row["policy_id"]).split("causal-standalone:", 1)[-1]
        for row in sleeves
        if float(row["scenarios"]["STRESSED_1_5X"][primary]["net_total"]) > 0.0
    ]
    pass_total = sum(
        int(row["scenarios"][scenario][primary]["pass_count"])
        for row in rows
        for scenario in ("NORMAL", "STRESSED_1_5X")
    )
    breach_total = sum(
        int(row["scenarios"][scenario][primary]["mll_breach_count"])
        for row in rows
        for scenario in ("NORMAL", "STRESSED_1_5X")
    )
    if (
        len(sleeves) != 18
        or len(positive) != 17
        or len(books) != 6
        or pass_total != 0
        or breach_total != 0
        or bool(results.get("salvage_gate", {}).get("passed"))
    ):
        raise CausalRiskPreflightError("sealed clean causal baseline drift")
    return {
        "phase_a_results_path": str(DEFAULT_BASELINE_RESULTS),
        "phase_a_results_sha256": _sha256(phase_a_results_path),
        "checkpoint_hash": str(results["checkpoint_hash"]),
        "sleeve_count": len(sleeves),
        "positive_stressed_sleeve_count": len(positive),
        "positive_sleeve_ids": sorted(positive),
        "former_book_count": len(books),
        "normal_and_stressed_pass_count": pass_total,
        "mll_breach_count": breach_total,
        "terminal_status": "CAUSAL_SALVAGE_GATE_FALSIFIED",
    }


def _verify_former_book_raw_identity(
    rows: Sequence[Mapping[str, Any]], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    original = {
        str(row["policy_id"]): row
        for row in baseline["results"]
        if row.get("entity_kind") == "FORMER_BOOK_DIAGNOSTIC_REFERENCE"
    }
    candidate = {
        str(row["policy_id"]): row
        for row in rows
    }
    fields = (
        "pass_count",
        "mll_breach_count",
        "net_total",
        "target_progress_p25",
        "target_progress_median",
        "minimum_mll_buffer",
        "consistency_rate",
        "terminal_distribution",
    )
    mismatches: list[dict[str, Any]] = []
    for policy_id, source in sorted(original.items()):
        observed = candidate.get(policy_id)
        if observed is None:
            mismatches.append({"policy_id": policy_id, "reason": "MISSING"})
            continue
        for scenario, key in (("NORMAL", "normal"), ("STRESSED_1_5X", "stressed")):
            expected = source["scenarios"][scenario]["90_TRADING_DAYS"]
            observed_scenario = observed["scenarios"][scenario]["90_TRADING_DAYS"]
            for field in fields:
                left = expected[field]
                right = observed_scenario[field]
                equal = (
                    math.isclose(float(left), float(right), abs_tol=1e-8)
                    if isinstance(left, (int, float)) and isinstance(right, (int, float))
                    else left == right
                )
                if not equal:
                    mismatches.append(
                        {
                            "policy_id": policy_id,
                            "scenario": scenario,
                            "field": field,
                            "expected": left,
                            "observed": right,
                        }
                    )
    if mismatches:
        raise CausalRiskPreflightError(
            "raw unscaled former-book identity reconciliation failed"
        )
    return {
        "required": True,
        "former_book_count": len(original),
        "mismatch_count": 0,
        "status": "FORMER_BOOK_RAW_UNSCALED_IDENTITY_PROVEN",
    }


def _frontier_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for level in NORMALIZED_RISK_LEVELS:
        selected = [
            row
            for row in rows
            if math.isclose(float(row["normalized_risk_level"]), level)
        ]
        normal_progress = [float(row["normal"]["target_progress_median"]) for row in selected]
        stress_progress = [float(row["stressed"]["target_progress_median"]) for row in selected]
        output.append(
            {
                "normalized_risk_level": level,
                "executable_micro_quantity": executable_micro_quantity(level),
                "candidate_count": len(selected),
                "normal_pass_count": sum(int(row["normal"]["pass_count"]) for row in selected),
                "stressed_pass_count": sum(int(row["stressed"]["pass_count"]) for row in selected),
                "candidate_with_any_normal_pass_count": sum(int(row["normal"]["pass_count"]) > 0 for row in selected),
                "candidate_with_any_stressed_pass_count": sum(int(row["stressed"]["pass_count"]) > 0 for row in selected),
                "normal_target_progress_median_across_candidates": statistics.median(normal_progress),
                "stressed_target_progress_median_across_candidates": statistics.median(stress_progress),
                "best_stressed_target_progress_median": max(stress_progress),
                "stressed_net_total_across_candidates": sum(float(row["stressed"]["net_total"]) for row in selected),
                "worst_minimum_mll_buffer": min(float(row["stressed"]["minimum_mll_buffer"]) for row in selected),
                "maximum_stressed_mll_breach_rate": max(float(row["stressed"]["mll_breach_rate"]) for row in selected),
                "stressed_consistency_rate_median": statistics.median(float(row["stressed"]["consistency_rate"]) for row in selected),
            }
        )
    return output


def _validate_campaign_manifest(manifest: Mapping[str, Any]) -> None:
    if str(manifest.get("campaign_id")) != CAMPAIGN_ID:
        raise CausalRiskPreflightError("wrong campaign manifest supplied to preflight")
    preflight = dict(
        manifest.get("risk_frontier_preflight")
        or manifest.get("bounded_risk_frontier_preflight")
        or {}
    )
    levels = tuple(float(value) for value in preflight.get("normalized_levels", ()))
    reference = int(
        preflight.get(
            "executable_reference_micro_quantity", EXECUTABLE_REFERENCE_MICROS
        )
    )
    if levels != NORMALIZED_RISK_LEVELS or reference != EXECUTABLE_REFERENCE_MICROS:
        raise CausalRiskPreflightError("manifest risk frontier does not match frozen contract")
    if preflight.get("outcome_inspection_before_reservation") is True:
        raise CausalRiskPreflightError("manifest permits pre-reservation outcome inspection")


def _verify_reservation(root: Path) -> dict[str, Any]:
    path = root / DEFAULT_RESERVATION
    value = _read_json(path)
    if (
        str(value.get("campaign_id")) != CAMPAIGN_ID
        or int(value.get("reserved_delta_trials", 0)) < 20_000
        or int(value.get("q4_access_delta", -1)) != 0
        or int(value.get("new_data_purchase_count", -1)) != 0
        or int(value.get("orders", -1)) != 0
    ):
        raise CausalRiskPreflightError("campaign multiplicity reservation is invalid")
    return {
        "path": str(DEFAULT_RESERVATION),
        "sha256": _sha256(path),
        "reserved_delta_trials": int(value["reserved_delta_trials"]),
        "cumulative_N_trials": int(value["cumulative_N_trials"]),
        "outcome_inspection_authorized": True,
    }


def _inside(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CausalRiskPreflightError("path escaped repository root") from exc
    return resolved


def _read_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CausalRiskPreflightError(f"JSON object required: {path}")
    return value


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
