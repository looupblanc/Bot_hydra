"""Bounded causal salvage sprint for the frozen eighteen-sleeve bank.

The sprint is deliberately separate from the ordinary production runtime.  It
does not reinterpret or overwrite campaign 0026, and it never calls the legacy
component compiler whose eligibility depended on a future coverage label.
Instead it reconstructs the six immutable diagnostic packages, compiles each
shared sleeve once through :mod:`hydra.research.causal_sleeve_replay`, and
reuses those causal trajectories for every account-policy evaluation.

Only compact, deterministic checkpoints are persisted.  Full XFA simulation,
forward activation, mutations, data access, and EvidenceBundle promotion are
outside this bounded recovery sprint.
"""

from __future__ import annotations

import hashlib
import gzip
import json
import math
import multiprocessing
import statistics
import subprocess
import time
from datetime import UTC, datetime
from dataclasses import asdict
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from hydra.account_policy.active_risk_pool import (
    ActiveRiskPoolPolicy,
    ConcurrencyScaling,
    SameInstrumentConflictRule,
    TargetProtectionMode,
    policy_from_mapping,
)
from hydra.account_policy.causal_active_pool_replay import (
    CAUSAL_ACCOUNT_REPLAY_VERSION,
    run_causal_shared_account_episode,
)
from hydra.compute.result_writer import AtomicResultWriter
from hydra.evidence import verify_evidence_bundle
from hydra.evidence.causal_salvage_adapter import (
    finalize_causal_salvage_evidence_bundle_streaming,
)
from hydra.execution.v7_cost_model import default_preregistration_path
from hydra.features.feature_matrix import FeatureMatrix
from hydra.production.portfolio_runtime import _remaining_chronological_calendars
from hydra.production.runtime import _block_aware_starts, _block_calendars
from hydra.propfirm.combine_episode import CombineTerminal
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15
from hydra.research.causal_sleeve_replay import (
    CAUSAL_DECISION_KERNEL_VERSION,
    CAUSAL_FILL_POLICY_ID,
    CausalSleeveReplay,
    CausalTradeTrajectory,
    causal_runtime,
    replay_causal_sleeve_streaming,
)
from hydra.shadow.active_risk_package import (
    ReconstructedActiveRiskShadowPackage,
    reconstruct_active_risk_shadow_package,
)


CAUSAL_SALVAGE_RUNTIME_VERSION = "hydra_causal_salvage_runtime_v1"
CAUSAL_SALVAGE_CHECKPOINT_SCHEMA = "hydra_causal_salvage_checkpoint_v1"
CAUSAL_SALVAGE_RESULT_SCHEMA = "hydra_causal_salvage_result_v1"

DEFAULT_MANIFEST = Path("config/v7/causal_salvage_sprint_0027.json")
DEFAULT_OPERATING_MANIFEST = Path(
    "reports/operating/hydra_operating_package_v1/OPERATING_PACKAGE_V1.json"
)
DEFAULT_CONTAMINATION_RECEIPT = Path(
    "reports/operating/hydra_operating_package_v1/"
    "F0_SINGLE_SOURCE_ENGINE_PARITY_CONTAMINATION_RECEIPT.json"
)
DEFAULT_GOVERNOR_BANK = Path(
    "data/cache/economic_production/"
    "hydra_active_risk_pool_target_velocity_0026/active_risk_governors.jsonl"
)
DEFAULT_SOURCE_CAMPAIGN_MANIFEST = Path(
    "config/v7/active_risk_pool_target_velocity_0026_revision_02.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "reports/economic_evolution/causal_salvage_sprint_0027"
)

EXPECTED_GOVERNOR_BANK_SHA256 = (
    "974091ecd3d9dea8a0b609a1b55346785b64f79a9dd2e2073b689e7a2dd4e982"
)
EXPECTED_PHASE_B_FIRST_FINGERPRINT = (
    "0001253ec61a47c3952e6ba548b6d0867b8c18ebb03b84400bee5989fa88e657"
)
EXPECTED_PHASE_B_LAST_FINGERPRINT = (
    "3505bdb6ea787dbcbf270f88d76020e789e1e22b1916ee514ecf1986677fffcc"
)
EXPECTED_COMPONENT_COUNT = 18
EXPECTED_REFERENCE_BOOK_COUNT = 6
_HORIZONS: tuple[int | str, ...] = (20, 40, 60, 90, "FULL")


class CausalSalvageRuntimeError(RuntimeError):
    """A frozen input, causal trajectory, or checkpoint failed closed."""


_WORK: dict[str, Any] = {}


def stable_hash(value: Any) -> str:
    """Return strict canonical JSON SHA-256."""

    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            ensure_ascii=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CausalSalvageRuntimeError(
            "causal salvage payload is not canonical JSON"
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def run_causal_salvage_sprint(
    manifest_path: str | Path = DEFAULT_MANIFEST,
    *,
    repository_root: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run or deterministically resume the frozen causal salvage sprint."""

    run_started = time.perf_counter()
    timings: dict[str, Any] = {
        "causal_component_replay_seconds": 0.0,
        "phase_a_evaluation_seconds": 0.0,
        "phase_a_evidence_seal_seconds": 0.0,
        "phase_b_stage_evaluation_seconds": {},
        "phase_b_evidence_seal_seconds": 0.0,
    }

    root = (
        Path(repository_root).resolve()
        if repository_root is not None
        else Path(__file__).resolve().parents[2]
    )
    manifest_file = _inside(root, manifest_path, label="salvage manifest")
    manifest = _read_json(manifest_file)
    _validate_manifest(manifest)
    manifest_sha = _sha256(manifest_file)
    _verify_source_commit(root, str(manifest["source_commit"]))

    output_root = _inside(
        root,
        output_dir or DEFAULT_OUTPUT_DIR,
        label="salvage output directory",
    )
    writer = AtomicResultWriter(output_root, immutable=True)
    # Wall-clock telemetry is operational, not scientific evidence.  Keep it
    # outside immutable checkpoint hashes so an interrupted deterministic
    # replay can resume without timing drift invalidating prior artifacts.
    runtime_metrics_writer = AtomicResultWriter(output_root, immutable=False)

    reconstructed, input_receipt = _load_frozen_packages(root, manifest)
    temporal_blocks, temporal_receipt = _load_frozen_temporal_contract(
        root, reconstructed
    )
    evaluation_manifest = {**manifest, "temporal_blocks": temporal_blocks}
    writer.write_json(
        "input_receipt.json",
        _sealed_checkpoint(
            manifest_sha,
            "FROZEN_INPUTS_VERIFIED",
            {
                **input_receipt,
                "temporal_contract": temporal_receipt,
                "preregistered_source_commit": str(manifest["source_commit"]),
                "live_head": _git_head(root),
                "runtime_version": CAUSAL_SALVAGE_RUNTIME_VERSION,
                "safety": _safety_receipt(),
            },
        ),
    )

    specs = dict(reconstructed[0].sleeve_specs)
    bindings = dict(reconstructed[0].frozen_signal_bindings)
    matrices = _open_frozen_matrices(root, bindings)
    component_replay_started = time.perf_counter()
    replays, failures = _compile_causal_sleeves(
        specs,
        bindings,
        matrices,
        start_inclusive=str(manifest["economic_contract"]["development_start"]),
        end_exclusive=str(
            manifest["economic_contract"]["development_end_exclusive"]
        ),
    )
    timings["causal_component_replay_seconds"] = (
        time.perf_counter() - component_replay_started
    )
    del matrices

    component_checkpoint = _component_checkpoint(
        manifest_sha=manifest_sha,
        replays=replays,
        failures=failures,
    )
    writer.write_json("causal_component_replay.json", component_checkpoint)
    if failures or len(replays) != EXPECTED_COMPONENT_COUNT:
        result = _terminal_result(
            manifest_sha=manifest_sha,
            status="CAUSAL_SALVAGE_FAILED_CLOSED_COMPONENT_COMPILE",
            phase_a=None,
            phase_b=None,
            component_checkpoint_hash=str(component_checkpoint["checkpoint_hash"]),
            reason="one or more frozen sleeves lack a clean causal trajectory",
        )
        writer.write_json("causal_salvage_result.json", result)
        _write_runtime_allocation(
            runtime_metrics_writer, timings=timings, run_started=run_started
        )
        return _public_result(output_root, result)

    runtimes = {
        sleeve_id: causal_runtime(replay, specs[sleeve_id], scenario="NORMAL")
        for sleeve_id, replay in replays.items()
    }
    starts48 = _block_aware_starts(runtimes, evaluation_manifest, maximum=48)
    starts96 = _block_aware_starts(
        runtimes, evaluation_manifest, maximum=96, required_starts=starts48
    )
    starts192 = _block_aware_starts(
        runtimes, evaluation_manifest, maximum=192, required_starts=starts96
    )
    calendars = _block_calendars(runtimes, evaluation_manifest, starts192)
    full_calendars = _remaining_chronological_calendars(runtimes, starts192)
    normal_trajectories = {
        sleeve_id: (
            *replay.normal_trajectories,
            *tuple(getattr(replay, "normal_censored_trajectories", ())),
        )
        for sleeve_id, replay in replays.items()
    }
    stressed_trajectories = {
        sleeve_id: (
            *replay.stressed_trajectories,
            *tuple(getattr(replay, "stressed_censored_trajectories", ())),
        )
        for sleeve_id, replay in replays.items()
    }
    _set_work(
        normal=normal_trajectories,
        stressed=stressed_trajectories,
        calendars=calendars,
        full_calendars=full_calendars,
        blocks=evaluation_manifest["temporal_blocks"]["blocks"],
    )

    phase_a_policies = [
        _standalone_policy(sleeve_id).to_dict()
        for sleeve_id in sorted(specs)
    ] + [
        row.combine_policy.to_dict()
        for row in sorted(
            reconstructed, key=lambda value: value.package.candidate_id
        )
    ]
    phase_a_kinds = {
        policy["policy_id"]: (
            "STANDALONE_SLEEVE"
            if str(policy["policy_id"]).startswith("causal-standalone:")
            else "FORMER_BOOK_DIAGNOSTIC_REFERENCE"
        )
        for policy in phase_a_policies
    }
    phase_a_evaluation_started = time.perf_counter()
    phase_a_rows, phase_a_evidence_parts = _evaluate_policies_parallel(
        phase_a_policies,
        starts=starts48,
        horizons=_HORIZONS,
        include_stress=True,
        batch_size=1,
        evidence_writer=writer,
        evidence_stage="phase_a",
    )
    timings["phase_a_evaluation_seconds"] = (
        time.perf_counter() - phase_a_evaluation_started
    )
    phase_a_rows = [
        {**row, "entity_kind": phase_a_kinds[str(row["policy_id"])]}
        for row in phase_a_rows
    ]
    salvage_gate = _evaluate_salvage_gate(phase_a_rows, manifest)
    phase_a_seal_started = time.perf_counter()
    phase_a_evidence = _seal_phase_a_evidence(
        root=root,
        output_root=output_root,
        manifest=manifest,
        manifest_sha=manifest_sha,
        replays=replays,
        specs=specs,
        bindings=bindings,
        policies={
            str(value["policy_id"]): policy_from_mapping(value)
            for value in phase_a_policies
        },
        starts=starts48,
        evidence_parts=phase_a_evidence_parts,
        temporal_contract=temporal_receipt,
        salvage_gate=salvage_gate,
    )
    timings["phase_a_evidence_seal_seconds"] = (
        time.perf_counter() - phase_a_seal_started
    )
    phase_a_checkpoint = _sealed_checkpoint(
        manifest_sha,
        "PHASE_A_18_STANDALONE_PLUS_6_DIAGNOSTIC",
        {
            "input_count": len(phase_a_policies),
            "standalone_sleeve_count": EXPECTED_COMPONENT_COUNT,
            "former_book_reference_count": EXPECTED_REFERENCE_BOOK_COUNT,
            "former_books_have_selection_advantage": False,
            "starts": _start_inventory(starts48, evaluation_manifest),
            "horizons": [_horizon_label(value) for value in _HORIZONS],
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "results": phase_a_rows,
            "raw_episode_evidence_parts": phase_a_evidence_parts,
            "evidence_bundle": phase_a_evidence,
            "salvage_gate": salvage_gate,
            "xfa_paths_started": 0,
            "safety": _safety_receipt(),
        },
    )
    writer.write_json("phase_a_results.json", phase_a_checkpoint)
    _write_runtime_allocation(
        runtime_metrics_writer, timings=timings, run_started=run_started
    )

    if not bool(salvage_gate["passed"]):
        result = _terminal_result(
            manifest_sha=manifest_sha,
            status="CAUSAL_SALVAGE_GATE_FALSIFIED",
            phase_a=phase_a_checkpoint,
            phase_b=None,
            component_checkpoint_hash=str(component_checkpoint["checkpoint_hash"]),
            reason="the preregistered Phase-A economic gate did not pass",
        )
        writer.write_json("causal_salvage_result.json", result)
        _write_runtime_allocation(
            runtime_metrics_writer, timings=timings, run_started=run_started
        )
        return _public_result(output_root, result)

    governor_file = _inside(root, DEFAULT_GOVERNOR_BANK, label="governor bank")
    policies = _load_phase_b_governors(governor_file, maximum=4096)
    policy_by_id = {row.policy_id: row for row in policies}
    former_ids = {row.package.candidate_id for row in reconstructed}

    stage_specs: tuple[tuple[str, int, tuple[int, ...], tuple[int | str, ...], bool], ...] = (
        (
            "stage1_4096_to_512",
            512,
            _block_anchor_starts(starts48, evaluation_manifest),
            (40,),
            False,
        ),
        (
            "stage2_512_to_64",
            64,
            _block_anchor_starts(starts48, evaluation_manifest),
            (90,),
            True,
        ),
        ("stage3_64_to_8", 8, starts48, _HORIZONS, True),
        ("stage4_8_to_4", 4, starts96, _HORIZONS, True),
        ("stage5_final_4", 4, starts192, _HORIZONS, True),
    )
    current = tuple(policies)
    stage_checkpoints: list[dict[str, Any]] = []
    phase_b_evidence_parts: list[dict[str, Any]] = []
    phase_b_policy_universe: dict[str, ActiveRiskPoolPolicy] = {}
    for stage_index, (
        stage_name,
        limit,
        starts,
        horizons,
        include_stress,
    ) in enumerate(stage_specs, start=1):
        if stage_index == 2:
            phase_b_policy_universe = {row.policy_id: row for row in current}
        stage_evaluation_started = time.perf_counter()
        rows, stage_evidence_parts = _evaluate_policies_parallel(
            [row.to_dict() for row in current],
            starts=starts,
            horizons=horizons,
            include_stress=include_stress,
            batch_size=(32 if stage_index == 1 else 8 if stage_index == 2 else 1),
            evidence_writer=(writer if stage_index >= 2 else None),
            evidence_stage=(stage_name if stage_index >= 2 else None),
        )
        timings["phase_b_stage_evaluation_seconds"][stage_name] = (
            time.perf_counter() - stage_evaluation_started
        )
        phase_b_evidence_parts.extend(stage_evidence_parts)
        selected_ids, selection = _select_stage(
            rows,
            {row.policy_id: row for row in current},
            limit=limit,
            manifest=manifest,
            final_stage=stage_index == len(stage_specs),
        )
        checkpoint = _sealed_checkpoint(
            manifest_sha,
            stage_name.upper(),
            {
                "stage_index": stage_index,
                "input_count": len(current),
                "output_limit": limit,
                "worker_model": "THREE_FORK_WORKERS",
                "worker_count": 3,
                "starts": _start_inventory(starts, evaluation_manifest),
                "horizons": [_horizon_label(value) for value in horizons],
                "cost_scenarios": (
                    ["NORMAL", "STRESSED_1_5X"]
                    if include_stress
                    else ["NORMAL"]
                ),
                "results": rows,
                "raw_episode_evidence_parts": stage_evidence_parts,
                "selection": selection,
                "former_book_reference_ids_in_input": sorted(
                    former_ids & {row.policy_id for row in current}
                ),
                "former_books_have_selection_advantage": False,
                "mutation_count": 0,
                "retuning_count": 0,
                "xfa_paths_started": 0,
                "safety": _safety_receipt(),
            },
        )
        writer.write_json(f"{stage_name}.json", checkpoint)
        _write_runtime_allocation(
            runtime_metrics_writer, timings=timings, run_started=run_started
        )
        stage_checkpoints.append(checkpoint)
        current = tuple(policy_by_id[policy_id] for policy_id in selected_ids)

    if not 1 <= len(phase_b_policy_universe) <= 512:
        raise CausalSalvageRuntimeError(
            "Phase-B exact replay universe is outside the frozen maximum of 512"
        )
    phase_b_seal_started = time.perf_counter()
    phase_b_evidence = _seal_phase_b_evidence(
        root=root,
        output_root=output_root,
        manifest=manifest,
        manifest_sha=manifest_sha,
        replays=replays,
        specs=specs,
        bindings=bindings,
        policies=phase_b_policy_universe,
        stage_checkpoints=stage_checkpoints,
        evidence_parts=phase_b_evidence_parts,
        temporal_contract=temporal_receipt,
    )
    timings["phase_b_evidence_seal_seconds"] = (
        time.perf_counter() - phase_b_seal_started
    )
    final_rows = stage_checkpoints[-1]["results"]
    final_gate = {}
    for row in final_rows:
        gate = _clean_advancement_gate(row, manifest)
        final_gate[str(row["policy_id"])] = {
            **gate,
            "complete_causal_evidence_bundle": True,
            "evidence_bundle_manifest_sha256": phase_b_evidence[
                "manifest_sha256"
            ],
            "promotion_authorized": bool(gate["economic_conditions_passed"]),
        }
    clean_survivors = sorted(
        policy_id
        for policy_id, gate in final_gate.items()
        if gate["promotion_authorized"]
    )
    phase_b_summary = {
        "schema": "hydra_causal_salvage_phase_b_summary_v1",
        "input_governor_count": len(policies),
        "stage_output_counts": [
            len(stage["selection"]["selected_policy_ids"])
            for stage in stage_checkpoints
        ],
        "final_policy_ids": [row.policy_id for row in current],
        "final_clean_advancement_gates": final_gate,
        "complete_causal_evidence_bundle": True,
        "evidence_bundle": phase_b_evidence,
        "clean_survivor_ids": clean_survivors,
        "clean_survivor_count": len(clean_survivors),
        "promotion_authorized": bool(clean_survivors),
        "promotion_blocker": (
            None if clean_survivors else "NO_POLICY_PASSED_FROZEN_CLEAN_GATE"
        ),
        "former_books_have_selection_advantage": False,
        "xfa_deferred": True,
    }
    phase_b_summary["summary_hash"] = stable_hash(phase_b_summary)
    writer.write_json("phase_b_summary.json", phase_b_summary)

    result = _terminal_result(
        manifest_sha=manifest_sha,
        status=(
            "CAUSAL_SALVAGE_PHASE_B_CLEAN_COMBINE_SURVIVORS"
            if clean_survivors
            else "CAUSAL_SALVAGE_PHASE_B_NO_CLEAN_FINALIST"
        ),
        phase_a=phase_a_checkpoint,
        phase_b=phase_b_summary,
        component_checkpoint_hash=str(component_checkpoint["checkpoint_hash"]),
        reason=(
            "bounded causal salvage complete with clean causal Combine survivors"
            if clean_survivors
            else "bounded causal salvage complete without a policy passing the frozen clean gate"
        ),
        promotion_authorized=bool(clean_survivors),
    )
    writer.write_json("causal_salvage_result.json", result)
    _write_runtime_allocation(
        runtime_metrics_writer, timings=timings, run_started=run_started
    )
    return _public_result(output_root, result)


def _seal_phase_a_evidence(
    *,
    root: Path,
    output_root: Path,
    manifest: Mapping[str, Any],
    manifest_sha: str,
    replays: Mapping[str, CausalSleeveReplay],
    specs: Mapping[str, Any],
    bindings: Mapping[str, Any],
    policies: Mapping[str, ActiveRiskPoolPolicy],
    starts: Sequence[int],
    evidence_parts: Sequence[Mapping[str, Any]],
    temporal_contract: Mapping[str, Any],
    salvage_gate: Mapping[str, Any],
) -> dict[str, Any]:
    horizons = tuple(_horizon_label(value) for value in _HORIZONS)
    required = [
        {
            "policy_id": policy_id,
            "episode_id": f"{policy_id}:{int(start)}",
            "horizon": horizon,
        }
        for policy_id in sorted(policies)
        for start in starts
        for horizon in horizons
    ]
    identity = _evidence_identity(
        root=root,
        campaign_id="hydra_causal_salvage_sprint_0027_phase_a",
        manifest=manifest,
        manifest_sha=manifest_sha,
        replays=replays,
        bindings=bindings,
        policies=policies,
        required_episode_keys=required,
        allowed_horizons=horizons,
        allow_additional=False,
        configuration_context={
            "phase": "PHASE_A",
            "starts": [int(value) for value in starts],
            "temporal_contract_hash": temporal_contract["temporal_contract_hash"],
            "salvage_gate_hash": salvage_gate["gate_hash"],
        },
    )
    receipt = finalize_causal_salvage_evidence_bundle_streaming(
        base_dir=(
            root
            / "data/cache/economic_production/hydra_causal_salvage_sprint_0027"
        ),
        lightweight_manifest_path=output_root
        / "phase_a_evidence_bundle_receipt.json",
        identity=identity,
        causal_replays=replays,
        evaluated_policy_record_chunks=_iter_raw_episode_evidence(
            output_root, evidence_parts
        ),
        policies=policies,
        sleeve_specs=specs,
        provenance=_evidence_provenance(
            root=root,
            manifest_sha=manifest_sha,
            temporal_contract=temporal_contract,
            extra_checksums={},
        ),
        compact_context={
            "phase": "PHASE_A",
            "salvage_gate": dict(salvage_gate),
            "xfa_deferred": True,
            "development_only": True,
        },
    )
    verified = verify_evidence_bundle(receipt.bundle_path, deep=True)
    if _read_json(Path(receipt.bundle_path) / "identity.json") != identity:
        raise CausalSalvageRuntimeError("Phase-A EvidenceBundle identity drift")
    return receipt.to_dict()


def _seal_phase_b_evidence(
    *,
    root: Path,
    output_root: Path,
    manifest: Mapping[str, Any],
    manifest_sha: str,
    replays: Mapping[str, CausalSleeveReplay],
    specs: Mapping[str, Any],
    bindings: Mapping[str, Any],
    policies: Mapping[str, ActiveRiskPoolPolicy],
    stage_checkpoints: Sequence[Mapping[str, Any]],
    evidence_parts: Sequence[Mapping[str, Any]],
    temporal_contract: Mapping[str, Any],
) -> dict[str, Any]:
    if len(stage_checkpoints) != 5:
        raise CausalSalvageRuntimeError("Phase-B stage checkpoint coverage drift")
    stage2 = stage_checkpoints[1]
    starts = tuple(int(value) for value in stage2["starts"]["epoch_days"])
    if len(starts) != 4 or stage2["horizons"] != ["90_TRADING_DAYS"]:
        raise CausalSalvageRuntimeError("Phase-B exact base coverage drift")
    required = [
        {
            "policy_id": policy_id,
            "episode_id": f"{policy_id}:{int(start)}",
            "horizon": "90_TRADING_DAYS",
        }
        for policy_id in sorted(policies)
        for start in starts
    ]
    horizons = tuple(_horizon_label(value) for value in _HORIZONS)
    identity = _evidence_identity(
        root=root,
        campaign_id="hydra_causal_salvage_sprint_0027_phase_b",
        manifest=manifest,
        manifest_sha=manifest_sha,
        replays=replays,
        bindings=bindings,
        policies=policies,
        required_episode_keys=required,
        allowed_horizons=horizons,
        allow_additional=True,
        configuration_context={
            "phase": "PHASE_B",
            "stage_selection_hashes": [
                str(stage["selection"]["selection_hash"])
                for stage in stage_checkpoints
            ],
            "temporal_contract_hash": temporal_contract["temporal_contract_hash"],
        },
    )
    receipt = finalize_causal_salvage_evidence_bundle_streaming(
        base_dir=(
            root
            / "data/cache/economic_production/hydra_causal_salvage_sprint_0027"
        ),
        lightweight_manifest_path=output_root
        / "phase_b_evidence_bundle_receipt.json",
        identity=identity,
        causal_replays=replays,
        evaluated_policy_record_chunks=_iter_raw_episode_evidence(
            output_root, evidence_parts
        ),
        policies=policies,
        sleeve_specs=specs,
        provenance=_evidence_provenance(
            root=root,
            manifest_sha=manifest_sha,
            temporal_contract=temporal_contract,
            extra_checksums={
                "governor_bank": _sha256(root / DEFAULT_GOVERNOR_BANK),
                **{
                    f"stage_selection:{index}": str(
                        stage["selection"]["selection_hash"]
                    )
                    for index, stage in enumerate(stage_checkpoints, start=1)
                },
            },
        ),
        compact_context={
            "phase": "PHASE_B",
            "stage_count": len(stage_checkpoints),
            "xfa_deferred": True,
            "development_only": True,
        },
    )
    verified = verify_evidence_bundle(receipt.bundle_path, deep=True)
    if _read_json(Path(receipt.bundle_path) / "identity.json") != identity:
        raise CausalSalvageRuntimeError("Phase-B EvidenceBundle identity drift")
    return receipt.to_dict()


def _evidence_identity(
    *,
    root: Path,
    campaign_id: str,
    manifest: Mapping[str, Any],
    manifest_sha: str,
    replays: Mapping[str, CausalSleeveReplay],
    bindings: Mapping[str, Any],
    policies: Mapping[str, ActiveRiskPoolPolicy],
    required_episode_keys: Sequence[Mapping[str, str]],
    allowed_horizons: Sequence[str],
    allow_additional: bool,
    configuration_context: Mapping[str, Any],
) -> dict[str, Any]:
    data_fingerprints: dict[str, str] = {}
    for binding in bindings.values():
        market = str(binding.feature_matrix_market)
        values = {
            f"feature_bundle:{market}": str(binding.feature_matrix_bundle_hash),
            f"source_data:{market}": str(
                binding.feature_matrix_source_data_sha256
            ),
            f"roll_map:{market}": str(binding.feature_matrix_roll_map_sha256),
        }
        for name, digest in values.items():
            prior = data_fingerprints.setdefault(name, digest)
            if prior != digest:
                raise CausalSalvageRuntimeError(
                    f"causal data fingerprint conflict: {name}"
                )
    configuration_sha = stable_hash(
        {
            "manifest_sha256": manifest_sha,
            "runtime_version": CAUSAL_SALVAGE_RUNTIME_VERSION,
            "account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
            "decision_kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
            "fill_policy_hashes": sorted(
                {row.fill_policy_hash for row in replays.values()}
            ),
            "topstep_rules": asdict(_WORK["rules"]),
            "context": dict(configuration_context),
        }
    )
    return {
        "campaign_id": campaign_id,
        "grammar_id": "causal_salvage_reuse_frozen_governors_v1",
        "policy_fingerprints": {
            policy_id: policy.structural_fingerprint
            for policy_id, policy in sorted(policies.items())
        },
        "component_fingerprints": {
            component_id: replay.specification_hash
            for component_id, replay in sorted(replays.items())
        },
        "source_commit": _git_head(root),
        "data_fingerprints": data_fingerprints,
        "configuration_sha256": configuration_sha,
        "seeds": [0],
        "created_at_utc": str(manifest["created_at_utc"]),
        "expected_coverage": {
            "policy_ids": sorted(policies),
            "component_ids": sorted(replays),
            "required_episode_keys": [dict(value) for value in required_episode_keys],
            "allowed_horizons": list(allowed_horizons),
            "cost_scenarios": ["NORMAL", "STRESSED_1_5X"],
            "allow_additional_episode_keys": bool(allow_additional),
        },
    }


def _evidence_provenance(
    *,
    root: Path,
    manifest_sha: str,
    temporal_contract: Mapping[str, Any],
    extra_checksums: Mapping[str, str],
) -> dict[str, Any]:
    access_ledger = root / "reports/data_access/data_access_ledger.jsonl"
    checksums = {
        "causal_salvage_manifest": manifest_sha,
        "temporal_contract": str(temporal_contract["temporal_contract_hash"]),
        "contamination_receipt": _sha256(root / DEFAULT_CONTAMINATION_RECEIPT),
        **{str(name): str(value) for name, value in extra_checksums.items()},
    }
    return {
        "access_ledger_sha256": _sha256(access_ledger),
        "recorded_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "market_data_role": "PRE_FREEZE_DEVELOPMENT_CACHE",
        "immutable_checksums": checksums,
    }


def _iter_raw_episode_evidence(
    output_root: Path, parts: Sequence[Mapping[str, Any]]
) -> Any:
    for part in parts:
        path = output_root / str(part["relative_path"])
        if _sha256(path) != str(part["sha256"]):
            raise CausalSalvageRuntimeError(
                f"raw causal episode evidence checksum drift: {path}"
            )
        observed = 0
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise CausalSalvageRuntimeError(
                        "raw causal episode evidence row is not an object"
                    )
                observed += 1
                yield value
        if observed != int(part["record_count"]):
            raise CausalSalvageRuntimeError(
                f"raw causal episode evidence cardinality drift: {path}"
            )


def _load_frozen_packages(
    root: Path, manifest: Mapping[str, Any]
) -> tuple[tuple[ReconstructedActiveRiskShadowPackage, ...], dict[str, Any]]:
    operating_path = _inside(root, DEFAULT_OPERATING_MANIFEST, label="operating manifest")
    contamination_path = _inside(
        root, DEFAULT_CONTAMINATION_RECEIPT, label="contamination receipt"
    )
    expected = manifest["retracted_package"]
    if _sha256(operating_path) != str(expected["manifest_sha256"]):
        raise CausalSalvageRuntimeError("retracted operating manifest hash drift")
    if _sha256(contamination_path) != str(
        expected["contamination_receipt_sha256"]
    ):
        raise CausalSalvageRuntimeError("contamination receipt hash drift")
    operating = _read_json(operating_path)
    books = list(operating.get("books") or ())
    if len(books) != EXPECTED_REFERENCE_BOOK_COUNT:
        raise CausalSalvageRuntimeError("frozen package inventory is not six-wide")
    reconstructed: list[ReconstructedActiveRiskShadowPackage] = []
    package_receipts: list[dict[str, Any]] = []
    for book in sorted(books, key=lambda row: str(row["policy_id"])):
        package_meta = dict(book["shadow_package"])
        package_path = _inside(root, package_meta["path"], label="shadow package")
        if _sha256(package_path) != str(package_meta["file_sha256"]):
            raise CausalSalvageRuntimeError("frozen shadow package hash drift")
        rebuilt = reconstruct_active_risk_shadow_package(_read_json(package_path))
        if rebuilt.package.candidate_id != str(book["policy_id"]):
            raise CausalSalvageRuntimeError("shadow package policy identity drift")
        reconstructed.append(rebuilt)
        package_receipts.append(
            {
                "candidate_id": rebuilt.package.candidate_id,
                "package_path": str(package_path.relative_to(root)),
                "file_sha256": _sha256(package_path),
                "package_hash": rebuilt.package.package_hash,
                "legacy_operating_role_ignored": str(book["operating_role"]),
            }
        )
    _assert_common_sleeves(reconstructed)
    return tuple(reconstructed), {
        "operating_manifest_path": str(operating_path.relative_to(root)),
        "operating_manifest_sha256": _sha256(operating_path),
        "contamination_receipt_path": str(contamination_path.relative_to(root)),
        "contamination_receipt_sha256": _sha256(contamination_path),
        "package_count": len(reconstructed),
        "sleeve_count": len(reconstructed[0].sleeve_specs),
        "packages": package_receipts,
        "legacy_roles_used_for_selection": False,
    }


def _load_frozen_temporal_contract(
    root: Path,
    packages: Sequence[ReconstructedActiveRiskShadowPackage],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve B1--B4 from the campaign manifest sealed into every package.

    The 0027 preregistration intentionally binds the retracted package rather
    than copying its block definitions.  The six package envelopes each carry
    the SHA-256 of their source campaign manifest.  We therefore accept the
    temporal contract only from the repository file matching that common
    sealed hash; an unbound or edited block declaration fails closed.
    """

    source_path = _inside(
        root,
        DEFAULT_SOURCE_CAMPAIGN_MANIFEST,
        label="source campaign manifest",
    )
    expected_hashes = {
        str(row.package.evidence_provenance.get("campaign_manifest_sha256") or "")
        for row in packages
    }
    if len(expected_hashes) != 1 or "" in expected_hashes:
        raise CausalSalvageRuntimeError(
            "frozen packages do not bind one source campaign manifest"
        )
    expected_sha = next(iter(expected_hashes))
    observed_sha = _sha256(source_path)
    if observed_sha != expected_sha:
        raise CausalSalvageRuntimeError(
            "source campaign manifest does not match frozen package provenance"
        )
    source = _read_json(source_path)
    temporal = dict(source.get("temporal_blocks") or {})
    blocks = list(temporal.get("blocks") or ())
    if (
        [str(row.get("block_id") or "") for row in blocks]
        != ["B1", "B2", "B3", "B4"]
        or temporal.get("overlapping_starts_independent") is not False
    ):
        raise CausalSalvageRuntimeError("frozen temporal block contract drift")
    previous_end = ""
    for block in blocks:
        start = str(block.get("start") or "")
        end = str(block.get("end") or "")
        if not start or start > end or (previous_end and start <= previous_end):
            raise CausalSalvageRuntimeError(
                "frozen temporal blocks overlap or are not chronological"
            )
        previous_end = end
    receipt = {
        "source": "PACKAGE_PROVENANCE_BOUND_CAMPAIGN_MANIFEST",
        "path": str(source_path.relative_to(root)),
        "sha256": observed_sha,
        "block_method": temporal.get("method"),
        "block_ids": [str(row["block_id"]) for row in blocks],
        "block_count": len(blocks),
        "overlapping_starts_independent": False,
        "temporal_contract_hash": stable_hash(temporal),
    }
    return temporal, receipt


def _assert_common_sleeves(
    rows: Sequence[ReconstructedActiveRiskShadowPackage],
) -> None:
    if not rows:
        raise CausalSalvageRuntimeError("no frozen packages were reconstructed")
    first = rows[0]
    baseline = {
        sleeve_id: {
            "spec": first.sleeve_specs[sleeve_id].to_dict(),
            "record": first.sleeve_records[sleeve_id].to_dict(),
            "binding": first.frozen_signal_bindings[sleeve_id].to_dict(),
        }
        for sleeve_id in sorted(first.sleeve_specs)
    }
    if len(baseline) != EXPECTED_COMPONENT_COUNT:
        raise CausalSalvageRuntimeError("frozen sleeve inventory is not eighteen-wide")
    for row in rows:
        candidate = {
            sleeve_id: {
                "spec": row.sleeve_specs[sleeve_id].to_dict(),
                "record": row.sleeve_records[sleeve_id].to_dict(),
                "binding": row.frozen_signal_bindings[sleeve_id].to_dict(),
            }
            for sleeve_id in sorted(row.sleeve_specs)
        }
        if candidate != baseline:
            raise CausalSalvageRuntimeError("six packages do not share one sleeve bank")
        if tuple(row.combine_policy.component_ids) != tuple(
            row.combine_policy.component_priority
        ) or set(row.combine_policy.component_ids) != set(baseline):
            raise CausalSalvageRuntimeError("former book membership drift")


def _open_frozen_matrices(
    root: Path, bindings: Mapping[str, Any]
) -> dict[str, FeatureMatrix]:
    output: dict[str, FeatureMatrix] = {}
    for binding in bindings.values():
        manifest_path = _inside(
            root, binding.feature_matrix_manifest_path, label="feature manifest"
        )
        if _sha256(manifest_path) != binding.feature_matrix_manifest_sha256:
            raise CausalSalvageRuntimeError("frozen feature manifest hash drift")
        market = str(binding.feature_matrix_market)
        if market not in output:
            output[market] = FeatureMatrix.open(manifest_path.parent, mmap=True)
        matrix = output[market]
        if matrix.fingerprint != binding.feature_matrix_bundle_hash:
            raise CausalSalvageRuntimeError("frozen feature bundle hash drift")
        provenance = dict(matrix.manifest.get("provenance") or {})
        if (
            provenance.get("market") != market
            or provenance.get("data_fingerprint")
            != binding.feature_matrix_source_data_sha256
            or provenance.get("contract_map_sha256")
            != binding.feature_matrix_roll_map_sha256
        ):
            raise CausalSalvageRuntimeError("frozen feature provenance drift")
    return output


def _compile_causal_sleeves(
    specs: Mapping[str, Any],
    bindings: Mapping[str, Any],
    matrices: Mapping[str, FeatureMatrix],
    *,
    start_inclusive: str,
    end_exclusive: str,
) -> tuple[dict[str, CausalSleeveReplay], list[dict[str, str]]]:
    output: dict[str, CausalSleeveReplay] = {}
    failures: list[dict[str, str]] = []
    for sleeve_id in sorted(specs):
        spec = specs[sleeve_id]
        try:
            replay = replay_causal_sleeve_streaming(
                spec,
                bindings[sleeve_id],
                matrices[spec.market],
                start_inclusive=start_inclusive,
                end_exclusive=end_exclusive,
            )
            output[sleeve_id] = replay
        except (KeyError, TypeError, ValueError) as exc:
            failures.append(
                {
                    "sleeve_id": sleeve_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
    return output, failures


def _component_checkpoint(
    *,
    manifest_sha: str,
    replays: Mapping[str, CausalSleeveReplay],
    failures: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    for sleeve_id, replay in sorted(replays.items()):
        normal_net = sum(row.net_pnl for row in replay.normal_events)
        stressed_net = sum(row.net_pnl for row in replay.stressed_events)
        rows.append(
            {
                **replay.to_dict(include_events=False),
                "normal_net_pnl": float(normal_net),
                "stressed_net_pnl": float(stressed_net),
                "causal_trajectory_count": len(replay.normal_trajectories),
                "normal_trajectory_hash": stable_hash(
                    [row.to_dict() for row in replay.normal_trajectories]
                ),
                "stressed_trajectory_hash": stable_hash(
                    [row.to_dict() for row in replay.stressed_trajectories]
                ),
            }
        )
    return _sealed_checkpoint(
        manifest_sha,
        "CAUSAL_COMPONENT_REPLAY",
        {
            "kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
            "fill_policy_id": CAUSAL_FILL_POLICY_ID,
            "replay_entrypoint": "replay_causal_sleeve_streaming",
            "expected_component_count": EXPECTED_COMPONENT_COUNT,
            "compiled_component_count": len(rows),
            "hard_causality_defect_count": len(failures),
            "causally_censored_signal_count": sum(
                int(row["censored_signal_count"]) for row in rows
            ),
            "causal_censoring_is_hard_defect": False,
            "future_outcome_fields_used": False,
            "components": rows,
            "failures": list(failures),
            "safety": _safety_receipt(),
        },
    )


def _standalone_policy(sleeve_id: str) -> ActiveRiskPoolPolicy:
    return ActiveRiskPoolPolicy(
        policy_id=f"causal-standalone:{sleeve_id}",
        component_priority=(sleeve_id,),
        nominal_risk_charge_per_mini=((sleeve_id, 2250.0),),
        maximum_concurrent_sleeves=1,
        aggregate_open_risk_ceiling=4500.0,
        maximum_mll_buffer_fraction=1.0,
        protected_mll_buffer=0.0,
        maximum_mini_equivalent=15.0,
        concurrency_scaling=ConcurrencyScaling.PROPORTIONAL,
        same_instrument_conflict_rule=SameInstrumentConflictRule.PRIORITY,
        daily_loss_guard=4500.0,
        daily_consistency_profit_guard=9000.0,
        target_protection_distance=0.0,
        target_protection_mode=TargetProtectionMode.NONE,
        static_risk_tier=1.0,
    )


def _set_work(
    *,
    normal: Mapping[str, Sequence[CausalTradeTrajectory]],
    stressed: Mapping[str, Sequence[CausalTradeTrajectory]],
    calendars: Mapping[int, Sequence[int]],
    full_calendars: Mapping[int, Sequence[int]],
    blocks: Sequence[Mapping[str, Any]],
) -> None:
    global _WORK
    _WORK = {
        "normal": {key: tuple(value) for key, value in normal.items()},
        "stressed": {key: tuple(value) for key, value in stressed.items()},
        "calendars": {int(key): tuple(value) for key, value in calendars.items()},
        "full_calendars": {
            int(key): tuple(value) for key, value in full_calendars.items()
        },
        "blocks": tuple(dict(value) for value in blocks),
        "rules": official_rule_snapshot_2026_07_15().combine_config(),
    }


def _evaluate_policies_parallel(
    policies: Sequence[Mapping[str, Any]],
    *,
    starts: Sequence[int],
    horizons: Sequence[int | str],
    include_stress: bool,
    batch_size: int,
    evidence_writer: AtomicResultWriter | None = None,
    evidence_stage: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not policies:
        return [], []
    retain_evidence = evidence_writer is not None
    if retain_evidence != bool(evidence_stage):
        raise CausalSalvageRuntimeError(
            "causal evidence writer and stage must be supplied together"
        )
    if not _WORK:
        raise CausalSalvageRuntimeError("causal worker state was not initialized")
    payloads = [
        {
            "policies": [dict(value) for value in policies[index : index + batch_size]],
            "starts": [int(value) for value in starts],
            "horizons": list(horizons),
            "include_stress": bool(include_stress),
            "retain_evidence": retain_evidence,
        }
        for index in range(0, len(policies), batch_size)
    ]
    try:
        context = multiprocessing.get_context("fork")
    except ValueError as exc:
        raise CausalSalvageRuntimeError(
            "causal salvage requires the preregistered fork worker model"
        ) from exc
    rows: list[dict[str, Any]] = []
    evidence_parts: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=3, mp_context=context) as pool:
        for batch_index, batch in enumerate(
            pool.map(_policy_batch_worker, payloads, chunksize=1)
        ):
            batch_records = 0
            batch_payloads: list[bytes] = []
            batch_hashes: list[str] = []
            for row in batch:
                compressed = row.pop("_evidence_jsonl_gzip", None)
                record_count = int(row.pop("_evidence_record_count", 0))
                evidence_hash = row.pop("_evidence_uncompressed_sha256", None)
                if retain_evidence:
                    if not isinstance(compressed, bytes) or not evidence_hash:
                        raise CausalSalvageRuntimeError(
                            "causal worker omitted retained episode evidence"
                        )
                    batch_records += record_count
                    batch_payloads.append(compressed)
                    batch_hashes.append(str(evidence_hash))
                elif compressed is not None or record_count or evidence_hash is not None:
                    raise CausalSalvageRuntimeError(
                        "screening worker unexpectedly returned episode evidence"
                    )
                rows.append(row)
            if retain_evidence:
                # A coordinator-owned immutable part is written for each policy
                # batch.  Concatenated gzip members remain stream-readable while
                # avoiding a second in-memory expansion of daily account paths.
                payload = b"".join(batch_payloads)
                relative_path = (
                    f"raw_episode_evidence/{evidence_stage}/"
                    f"part_{batch_index:06d}.jsonl.gz"
                )
                receipt = evidence_writer.write_bytes(relative_path, payload)
                evidence_parts.append(
                    {
                        "relative_path": receipt.relative_path,
                        "sha256": receipt.sha256,
                        "size_bytes": receipt.size_bytes,
                        "record_count": batch_records,
                        "member_uncompressed_sha256": batch_hashes,
                    }
                )
    if len(rows) != len(policies):
        raise CausalSalvageRuntimeError("causal policy worker cardinality drift")
    return (
        sorted(rows, key=lambda value: str(value["policy_id"])),
        evidence_parts,
    )


def _policy_batch_worker(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _evaluate_policy(
            policy_from_mapping(value),
            starts=tuple(int(day) for day in payload["starts"]),
            horizons=tuple(payload["horizons"]),
            include_stress=bool(payload["include_stress"]),
            retain_evidence=bool(payload.get("retain_evidence", False)),
        )
        for value in payload["policies"]
    ]


def _evaluate_policy(
    policy: ActiveRiskPoolPolicy,
    *,
    starts: Sequence[int],
    horizons: Sequence[int | str],
    include_stress: bool,
    retain_evidence: bool = False,
) -> dict[str, Any]:
    scenario_rows = (("NORMAL", "normal"),)
    if include_stress:
        scenario_rows = (*scenario_rows, ("STRESSED_1_5X", "stressed"))
    summaries: dict[str, dict[str, Any]] = {}
    behavior_rows: list[dict[str, Any]] = []
    evidence_records: list[dict[str, Any]] = []
    for scenario, key in scenario_rows:
        trajectories = {
            component_id: _WORK[key][component_id]
            for component_id in policy.component_ids
        }
        horizon_summaries: dict[str, Any] = {}
        for horizon in horizons:
            label = _horizon_label(horizon)
            episodes: list[Any] = []
            classifications: list[str] = []
            for start in starts:
                calendar = (
                    _WORK["full_calendars"][int(start)]
                    if horizon == "FULL"
                    else _WORK["calendars"][int(start)]
                )
                duration = (
                    len(calendar[calendar.index(int(start)) :])
                    if horizon == "FULL"
                    else int(horizon)
                )
                episode = run_causal_shared_account_episode(
                    trajectories,
                    calendar,
                    policy=policy,
                    start_day=int(start),
                    maximum_duration_days=max(duration, 1),
                    config=_WORK["rules"],
                )
                terminal = _terminal_classification(
                    episode, requested_duration=max(duration, 1), full=horizon == "FULL"
                )
                episodes.append(episode)
                classifications.append(terminal)
                behavior_rows.append(
                    {
                        "scenario": scenario,
                        "horizon": label,
                        "start": int(start),
                        "terminal": terminal,
                        "accepted": int(episode.accepted_events),
                        "skipped": int(episode.skipped_events),
                        "target_progress": round(float(episode.target_progress), 12),
                        "net_pnl": round(float(episode.net_pnl), 8),
                    }
                )
                if retain_evidence:
                    evidence_records.append(
                        {
                            "policy_id": policy.policy_id,
                            "episode_id": f"{policy.policy_id}:{int(start)}",
                            "scenario": scenario,
                            "horizon": label,
                            "temporal_block": _block_for_day(
                                int(start), _WORK["blocks"]
                            ),
                            "requested_duration_trading_days": max(duration, 1),
                            "episode": episode.to_dict(include_paths=True),
                        }
                    )
            horizon_summaries[label] = _summarize_episodes(
                episodes, classifications, _WORK["blocks"]
            )
        summaries[scenario] = horizon_summaries
    result = {
        "schema": "hydra_causal_salvage_policy_metric_v1",
        "policy_id": policy.policy_id,
        "structural_fingerprint": policy.structural_fingerprint,
        "behavior_fingerprint": stable_hash(behavior_rows),
        "policy_shape": {
            "maximum_concurrent_sleeves": policy.maximum_concurrent_sleeves,
            "aggregate_open_risk_ceiling": policy.aggregate_open_risk_ceiling,
            "maximum_mini_equivalent": policy.maximum_mini_equivalent,
            "static_risk_tier": policy.static_risk_tier,
            "concurrency_scaling": policy.concurrency_scaling.value,
            "same_instrument_conflict_rule": (
                policy.same_instrument_conflict_rule.value
            ),
        },
        "scenarios": summaries,
        "causal_account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
        "future_outcome_fields_used": False,
        "development_only": True,
        "validated": False,
    }
    if retain_evidence:
        uncompressed = "".join(
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
            for record in evidence_records
        ).encode("utf-8")
        result["_evidence_jsonl_gzip"] = gzip.compress(
            uncompressed, compresslevel=6, mtime=0
        )
        result["_evidence_record_count"] = len(evidence_records)
        result["_evidence_uncompressed_sha256"] = hashlib.sha256(
            uncompressed
        ).hexdigest()
    return result


def _terminal_classification(
    episode: Any, *, requested_duration: int, full: bool
) -> str:
    if episode.terminal is CombineTerminal.PASSED:
        return "TARGET_REACHED"
    if episode.terminal is CombineTerminal.MLL_BREACH:
        return "MLL_BREACHED"
    if episode.terminal is CombineTerminal.COMPLIANCE_FAILURE:
        return "HARD_RULE_FAILURE"
    if str(episode.terminal_reason) == "CENSORED_FUTURE_COVERAGE":
        return "DATA_CENSORED"
    if full or int(episode.eligible_days) < int(requested_duration):
        return "DATA_CENSORED"
    return "OPERATIONAL_HORIZON_NOT_REACHED"


def _summarize_episodes(
    episodes: Sequence[Any],
    classifications: Sequence[str],
    blocks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not episodes or len(episodes) != len(classifications):
        raise CausalSalvageRuntimeError("causal episode summary input drift")
    progress = [float(row.target_progress) for row in episodes]
    net = [float(row.net_pnl) for row in episodes]
    buffers = [float(row.minimum_mll_buffer) for row in episodes]
    contribution: dict[str, float] = defaultdict(float)
    block_passes: Counter[str] = Counter()
    block_net: dict[str, float] = defaultdict(float)
    block_progress: dict[str, list[float]] = defaultdict(list)
    for episode, terminal in zip(episodes, classifications, strict=True):
        block = _block_for_day(int(episode.start_day), blocks)
        block_net[block] += float(episode.net_pnl)
        block_progress[block].append(float(episode.target_progress))
        if terminal == "TARGET_REACHED":
            block_passes[block] += 1
        for component_id, value in episode.component_contribution.items():
            contribution[str(component_id)] += float(value)
    positive_contribution = sum(max(0.0, value) for value in contribution.values())
    pass_count = classifications.count("TARGET_REACHED")
    positive_day_profits = [
        float(day["day_pnl"])
        for episode in episodes
        for day in episode.daily_path
        if float(day["day_pnl"]) > 0.0
    ]
    return {
        "episode_count": len(episodes),
        "pass_count": pass_count,
        "pass_rate": pass_count / len(episodes),
        "mll_breach_count": classifications.count("MLL_BREACHED"),
        "mll_breach_rate": classifications.count("MLL_BREACHED") / len(episodes),
        "hard_rule_failure_count": classifications.count("HARD_RULE_FAILURE"),
        "censored_count": sum(
            value in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}
            for value in classifications
        ),
        "terminal_distribution": dict(sorted(Counter(classifications).items())),
        "net_total": float(sum(net)),
        "net_median": float(statistics.median(net)),
        "target_progress_p25": _quantile(progress, 0.25),
        "target_progress_median": float(statistics.median(progress)),
        "target_progress_maximum": float(max(progress)),
        "minimum_mll_buffer": float(min(buffers)),
        "minimum_mll_buffer_p25": _quantile(buffers, 0.25),
        "minimum_mll_buffer_median": float(statistics.median(buffers)),
        "minimum_mll_buffer_p75": _quantile(buffers, 0.75),
        "consistency_rate": sum(bool(row.consistency_ok) for row in episodes)
        / len(episodes),
        "median_days_to_target": (
            float(
                statistics.median(
                    [row.days_to_target for row in episodes if row.days_to_target is not None]
                )
            )
            if any(row.days_to_target is not None for row in episodes)
            else None
        ),
        "maximum_best_day_profit_share": (
            max(positive_day_profits) / sum(positive_day_profits)
            if positive_day_profits
            else 1.0
        ),
        "pass_block_count": len(block_passes),
        "pass_block_ids": sorted(block_passes),
        "by_block_pass_count": dict(sorted(block_passes.items())),
        "by_block_net": dict(sorted(block_net.items())),
        "by_block_target_progress_median": {
            key: float(statistics.median(values))
            for key, values in sorted(block_progress.items())
        },
        "maximum_single_block_pass_share": (
            max(block_passes.values()) / pass_count if pass_count else 1.0
        ),
        "maximum_single_sleeve_profit_share": (
            max((max(value, 0.0) for value in contribution.values()), default=0.0)
            / positive_contribution
            if positive_contribution > 0.0
            else 1.0
        ),
        "component_contribution": dict(sorted(contribution.items())),
        "accepted_event_count": sum(row.accepted_events for row in episodes),
        "skipped_event_count": sum(row.skipped_events for row in episodes),
    }


def _evaluate_salvage_gate(
    rows: Sequence[Mapping[str, Any]], manifest: Mapping[str, Any]
) -> dict[str, Any]:
    gate = manifest["salvage_gate"]
    primary = f"{int(manifest['economic_contract']['phase_a_primary_horizon_trading_days'])}_TRADING_DAYS"
    sleeves = [row for row in rows if row["entity_kind"] == "STANDALONE_SLEEVE"]
    books = [
        row
        for row in rows
        if row["entity_kind"] == "FORMER_BOOK_DIAGNOSTIC_REFERENCE"
    ]
    defensible = [
        str(row["policy_id"])
        for row in sleeves
        if float(row["scenarios"]["STRESSED_1_5X"][primary]["net_total"]) > 0.0
    ]
    route_a: list[str] = []
    route_b: list[str] = []
    for row in books:
        normal = row["scenarios"]["NORMAL"][primary]
        stressed = row["scenarios"]["STRESSED_1_5X"][primary]
        if (
            int(normal["pass_count"])
            >= int(gate["diagnostic_book_route_a"]["minimum_normal_passes_out_of_48"])
            and int(stressed["pass_count"])
            >= int(gate["diagnostic_book_route_a"]["minimum_stressed_passes_out_of_48"])
        ):
            route_a.append(str(row["policy_id"]))
        route_b_gate = gate["diagnostic_book_route_b"]
        if (
            float(stressed["target_progress_median"])
            >= float(route_b_gate["minimum_median_target_progress"])
            and float(stressed["net_total"]) > 0.0
            and float(stressed["mll_breach_rate"])
            <= float(route_b_gate["maximum_mll_breach_rate"])
        ):
            route_b.append(str(row["policy_id"]))
    hard_defects = 0
    passed = bool(
        len(defensible) >= int(gate["minimum_economically_defensible_sleeves"])
        and hard_defects <= int(gate["hard_causality_defects_allowed"])
        and (route_a or route_b)
    )
    payload = {
        "passed": passed,
        "primary_horizon": primary,
        "economically_defensible_sleeve_ids": sorted(defensible),
        "economically_defensible_sleeve_count": len(defensible),
        "minimum_economically_defensible_sleeves": int(
            gate["minimum_economically_defensible_sleeves"]
        ),
        "hard_causality_defect_count": hard_defects,
        "route_a_book_ids": sorted(route_a),
        "route_b_book_ids": sorted(route_b),
        "former_books_have_selection_advantage": False,
    }
    payload["gate_hash"] = stable_hash(payload)
    return payload


def _load_phase_b_governors(
    path: Path, *, maximum: int
) -> tuple[ActiveRiskPoolPolicy, ...]:
    if _sha256(path) != EXPECTED_GOVERNOR_BANK_SHA256:
        raise CausalSalvageRuntimeError("frozen governor bank hash drift")
    policies: list[ActiveRiskPoolPolicy] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if len(policies) >= maximum:
                break
            value = json.loads(line)
            policies.append(policy_from_mapping(value))
    if len(policies) != maximum:
        raise CausalSalvageRuntimeError("frozen Phase-B bank lacks 4096 policies")
    fingerprints = [row.structural_fingerprint for row in policies]
    if fingerprints != sorted(fingerprints):
        raise CausalSalvageRuntimeError("frozen governor bank ordering drift")
    if (
        fingerprints[0] != EXPECTED_PHASE_B_FIRST_FINGERPRINT
        or fingerprints[-1] != EXPECTED_PHASE_B_LAST_FINGERPRINT
        or len(set(fingerprints)) != maximum
    ):
        raise CausalSalvageRuntimeError("frozen Phase-B population boundary drift")
    return tuple(policies)


def _select_stage(
    rows: Sequence[Mapping[str, Any]],
    policies: Mapping[str, ActiveRiskPoolPolicy],
    *,
    limit: int,
    manifest: Mapping[str, Any],
    final_stage: bool,
) -> tuple[list[str], dict[str, Any]]:
    ranked: list[tuple[tuple[Any, ...], Mapping[str, Any], list[str]]] = []
    for row in rows:
        reasons = _stage_gate_reasons(row, manifest, final_stage=final_stage)
        ranked.append(
            (
                _transparent_rank_key(
                    row, policies[str(row["policy_id"])], eligible=not reasons
                ),
                row,
                reasons,
            )
        )
    ranked.sort(key=lambda value: value[0])
    selected: list[str] = []
    distinct_behavior_count = len(
        {str(row[1]["behavior_fingerprint"]) for row in ranked}
    )
    effective_output_target = min(limit, distinct_behavior_count)
    observed_behaviors: set[str] = set()
    excluded_clones: list[str] = []
    reason_by_id: dict[str, list[str]] = {}
    for _key, row, reasons in ranked:
        policy_id = str(row["policy_id"])
        reason_by_id[policy_id] = reasons
        behavior = str(row["behavior_fingerprint"])
        if behavior in observed_behaviors:
            excluded_clones.append(policy_id)
            continue
        observed_behaviors.add(behavior)
        if len(selected) < effective_output_target:
            selected.append(policy_id)
    if len(selected) != effective_output_target:
        raise CausalSalvageRuntimeError(
            "behavioral deduplication did not satisfy its bounded output target"
        )
    selection = {
        "schema": "hydra_causal_salvage_transparent_selection_v1",
        "input_count": len(rows),
        "output_limit": limit,
        "distinct_behavior_count": distinct_behavior_count,
        "effective_output_target": effective_output_target,
        "selected_policy_ids": selected,
        "selected_count": len(selected),
        "gate_eligible_count": sum(not reasons for _key, _row, reasons in ranked),
        "diagnostic_fill_count": sum(bool(reason_by_id[value]) for value in selected),
        "behavioral_clone_exclusion_count": len(excluded_clones),
        "behavioral_clone_exclusions": excluded_clones,
        "selected_gate_reasons": {
            value: reason_by_id[value] for value in selected
        },
        "ranking_policy": [
            "GATE_ELIGIBILITY",
            "STRESSED_PASS_COUNT",
            "NORMAL_PASS_COUNT",
            "STRESSED_PASS_BLOCK_COUNT",
            "STRESSED_LOWER_QUARTILE_TARGET_PROGRESS",
            "STRESSED_MEDIAN_TARGET_PROGRESS",
            "STRESSED_NET_TOTAL",
            "LOWER_MLL_BREACH_RATE",
            "HIGHER_MINIMUM_MLL_BUFFER",
            "LOWER_BLOCK_CONCENTRATION",
            "LOWER_SLEEVE_CONCENTRATION",
            "LOWER_BEST_DAY_CONCENTRATION",
            "OPERATIONAL_SIMPLICITY",
            "POLICY_ID_TIE_BREAK",
        ],
        "opaque_score_used": False,
        "former_book_preference_used": False,
        "mutation_used": False,
        "retuning_used": False,
    }
    selection["selection_hash"] = stable_hash(selection)
    return selected, selection


def _write_runtime_allocation(
    writer: AtomicResultWriter,
    *,
    timings: Mapping[str, Any],
    run_started: float,
) -> None:
    """Publish non-scientific monotonic timing without contaminating receipts."""

    total = max(0.0, time.perf_counter() - run_started)
    stage_seconds = {
        str(name): float(value)
        for name, value in dict(
            timings.get("phase_b_stage_evaluation_seconds") or {}
        ).items()
    }
    economic = (
        float(timings.get("causal_component_replay_seconds", 0.0))
        + float(timings.get("phase_a_evaluation_seconds", 0.0))
        + sum(stage_seconds.values())
    )
    payload = {
        "schema": "hydra_causal_salvage_runtime_allocation_v1",
        "campaign_id": "hydra_causal_salvage_sprint_0027",
        "clock": "time.perf_counter_monotonic",
        "scope": "CURRENT_COORDINATOR_PROCESS",
        "total_wall_clock_seconds": total,
        "economic_replay_seconds": economic,
        "economic_replay_wall_clock_fraction": (
            economic / total if total > 0.0 else 0.0
        ),
        "causal_component_replay_seconds": float(
            timings.get("causal_component_replay_seconds", 0.0)
        ),
        "phase_a_evaluation_seconds": float(
            timings.get("phase_a_evaluation_seconds", 0.0)
        ),
        "phase_a_evidence_seal_seconds": float(
            timings.get("phase_a_evidence_seal_seconds", 0.0)
        ),
        "phase_b_stage_evaluation_seconds": stage_seconds,
        "phase_b_evidence_seal_seconds": float(
            timings.get("phase_b_evidence_seal_seconds", 0.0)
        ),
        "non_economic_wall_clock_seconds": max(0.0, total - economic),
        "scientific_checkpoint_hashes_affected": False,
    }
    writer.write_json("runtime_allocation.json", payload)


def _stage_gate_reasons(
    row: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    final_stage: bool,
) -> list[str]:
    normal, stressed = _primary_summaries(row)
    reasons: list[str] = []
    if float(normal["net_total"]) <= 0.0:
        reasons.append("NONPOSITIVE_NORMAL_NET")
    if stressed is not None and float(stressed["net_total"]) <= 0.0:
        reasons.append("NONPOSITIVE_STRESSED_NET")
    if float(normal["mll_breach_rate"]) > 0.15:
        reasons.append("EXCESSIVE_NORMAL_MLL")
    if stressed is not None and float(stressed["mll_breach_rate"]) > 0.15:
        reasons.append("EXCESSIVE_STRESSED_MLL")
    if int(normal["accepted_event_count"]) <= 0:
        reasons.append("INACTIVE_POLICY")
    if final_stage:
        clean = _clean_advancement_gate(row, manifest)
        reasons.extend(
            f"CLEAN_GATE:{value}" for value in clean["reasons"]
        )
    return reasons


def _clean_advancement_gate(
    row: Mapping[str, Any], manifest: Mapping[str, Any]
) -> dict[str, Any]:
    gate = manifest["clean_advancement_gate"]
    normal, stressed = _primary_summaries(row)
    reasons: list[str] = []
    if stressed is None:
        reasons.append("STRESSED_EVIDENCE_MISSING")
    else:
        if int(normal["pass_count"]) < int(gate["minimum_normal_passes"]):
            reasons.append("NORMAL_PASS_COUNT")
        if int(stressed["pass_count"]) < int(gate["minimum_stressed_passes"]):
            reasons.append("STRESSED_PASS_COUNT")
        if float(stressed["net_total"]) <= 0.0:
            reasons.append("NONPOSITIVE_STRESSED_NET")
        if max(
            float(normal["mll_breach_rate"]),
            float(stressed["mll_breach_rate"]),
        ) > float(gate["maximum_mll_breach_rate"]):
            reasons.append("MLL_BREACH_RATE")
        if int(stressed["pass_block_count"]) < int(
            gate["minimum_contributing_blocks"]
        ):
            reasons.append("CONTRIBUTING_BLOCKS")
        if max(
            float(normal["maximum_single_block_pass_share"]),
            float(stressed["maximum_single_block_pass_share"]),
        ) > float(gate["maximum_single_block_pass_share"]):
            reasons.append("BLOCK_CONCENTRATION")
        if max(
            float(normal["maximum_single_sleeve_profit_share"]),
            float(stressed["maximum_single_sleeve_profit_share"]),
        ) > float(gate["maximum_single_sleeve_profit_share"]):
            reasons.append("SLEEVE_CONCENTRATION")
        if max(
            float(normal["maximum_best_day_profit_share"]),
            float(stressed["maximum_best_day_profit_share"]),
        ) > float(gate["maximum_best_day_profit_share"]):
            reasons.append("BEST_DAY_CONCENTRATION")
    payload = {
        "passed": not reasons,
        "economic_conditions_passed": not reasons,
        "promotion_authorized": False,
        "evidence_bundle_required_after_selection": bool(
            gate["complete_causal_evidence_bundle_required"]
        ),
        "reasons": reasons,
    }
    payload["gate_hash"] = stable_hash(payload)
    return payload


def _transparent_rank_key(
    row: Mapping[str, Any], policy: ActiveRiskPoolPolicy, *, eligible: bool
) -> tuple[Any, ...]:
    normal, stressed = _primary_summaries(row)
    economic = stressed or normal
    return (
        0 if eligible else 1,
        -int(economic["pass_count"]),
        -int(normal["pass_count"]),
        -int(economic["pass_block_count"]),
        -float(economic["target_progress_p25"]),
        -float(economic["target_progress_median"]),
        -float(economic["net_total"]),
        max(
            float(normal["mll_breach_rate"]),
            float(economic["mll_breach_rate"]),
        ),
        -min(
            float(normal["minimum_mll_buffer"]),
            float(economic["minimum_mll_buffer"]),
        ),
        max(
            float(normal["maximum_single_block_pass_share"]),
            float(economic["maximum_single_block_pass_share"]),
        ),
        max(
            float(normal["maximum_single_sleeve_profit_share"]),
            float(economic["maximum_single_sleeve_profit_share"]),
        ),
        max(
            float(normal["maximum_best_day_profit_share"]),
            float(economic["maximum_best_day_profit_share"]),
        ),
        int(policy.maximum_concurrent_sleeves),
        str(policy.policy_id),
    )


def _primary_summaries(
    row: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any] | None]:
    scenarios = row["scenarios"]
    normal_horizons = scenarios["NORMAL"]
    label = (
        "90_TRADING_DAYS"
        if "90_TRADING_DAYS" in normal_horizons
        else next(iter(normal_horizons))
    )
    normal = normal_horizons[label]
    stressed_horizons = scenarios.get("STRESSED_1_5X")
    stressed = None if stressed_horizons is None else stressed_horizons[label]
    return normal, stressed


def _block_anchor_starts(
    starts48: Sequence[int], manifest: Mapping[str, Any]
) -> tuple[int, ...]:
    output = []
    for block in manifest["temporal_blocks"]["blocks"]:
        block_id = str(block["block_id"])
        output.append(
            next(
                int(day)
                for day in starts48
                if _block_for_day(
                    int(day), manifest["temporal_blocks"]["blocks"]
                )
                == block_id
            )
        )
    return tuple(output)


def _start_inventory(
    starts: Sequence[int], manifest: Mapping[str, Any]
) -> dict[str, Any]:
    blocks = manifest["temporal_blocks"]["blocks"]
    by_block = Counter(_block_for_day(int(day), blocks) for day in starts)
    return {
        "count": len(starts),
        "epoch_days": [int(value) for value in starts],
        "iso_dates": [_epoch_day_iso(int(value)) for value in starts],
        "by_block": dict(sorted(by_block.items())),
        "overlapping_starts_claimed_independent": False,
    }


def _block_for_day(day: int, blocks: Sequence[Mapping[str, Any]]) -> str:
    value = _epoch_day_iso(day)
    for block in blocks:
        if str(block["start"]) <= value <= str(block["end"]):
            return str(block["block_id"])
    return "OUTSIDE_FROZEN_BLOCK"


def _epoch_day_iso(value: int) -> str:
    return str(np.datetime64("1970-01-01", "D") + np.timedelta64(value, "D"))


def _horizon_label(value: int | str) -> str:
    return (
        "FULL_CHRONOLOGICAL_HORIZON"
        if value == "FULL"
        else f"{int(value)}_TRADING_DAYS"
    )


def _quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    return float(
        ordered[lower]
        + (ordered[upper] - ordered[lower]) * (position - lower)
    )


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if (
        manifest.get("schema") != "hydra_causal_salvage_sprint_manifest_v1"
        or manifest.get("campaign_id") != "hydra_causal_salvage_sprint_0027"
        or manifest.get("status_at_preregistration")
        != "PREREGISTERED_BEFORE_CLEAN_ECONOMIC_OUTCOMES"
    ):
        raise CausalSalvageRuntimeError("causal salvage manifest identity drift")
    contract = manifest.get("economic_contract") or {}
    if (
        int(contract.get("component_count", -1)) != EXPECTED_COMPONENT_COUNT
        or int(contract.get("former_book_reference_count", -1))
        != EXPECTED_REFERENCE_BOOK_COUNT
        or int(contract.get("phase_a_starts", -1)) != 48
        or list(contract.get("cost_scenarios") or ())
        != ["NORMAL", "STRESSED_1_5X"]
        or contract.get("xfa_deferred") is not True
        or contract.get("former_books_have_selection_advantage") is not False
    ):
        raise CausalSalvageRuntimeError("causal salvage economic contract drift")
    phase_b = manifest.get("phase_b") or {}
    expected_phase_b = {
        "stage_1_maximum_fast_screens": 4096,
        "stage_2_maximum_exact_replays": 512,
        "stage_3_maximum_policies": 64,
        "stage_3_starts": 48,
        "stage_4_maximum_policies": 8,
        "stage_4_starts": 96,
        "stage_5_maximum_policies": 4,
        "stage_5_starts": 192,
    }
    if any(int(phase_b.get(key, -1)) != value for key, value in expected_phase_b.items()):
        raise CausalSalvageRuntimeError("causal salvage halving contract drift")
    if (
        phase_b.get("reuse_existing_manifests_only") is not True
        or phase_b.get("mutation_allowed") is not False
        or phase_b.get("retuning_allowed") is not False
        or phase_b.get("one_primary_per_behavior_cluster") is not True
    ):
        raise CausalSalvageRuntimeError("causal salvage policy boundary drift")
    fill = ((manifest.get("causal_repairs") or {}).get("fill_policy") or {})
    frozen_cost_path = default_preregistration_path()
    frozen_cost_schedule = {
        "5m": 1.0,
        "15m": 1.0,
        "30m": 0.75,
        "60m": 0.75,
    }
    if (
        fill.get("policy_id") != CAUSAL_FILL_POLICY_ID
        or fill.get("normal_slippage_basis")
        != "FROZEN_V7_HOLDING_HORIZON_SCHEDULE"
        or fill.get("normal_slippage_resolution_key")
        != "HOLDING_BARS_PLUS_M_SUFFIX"
        or dict(fill.get("normal_slippage_ticks_per_side_by_current_horizon") or {})
        != frozen_cost_schedule
        or not math.isclose(float(fill.get("stressed_slippage_multiplier", -1.0)), 1.5)
        or fill.get("cost_configuration_path")
        != "config/v7/phase0_g0_preregistration.json"
        or fill.get("cost_configuration_sha256") != _sha256(frozen_cost_path)
        or fill.get("missing_or_noncontiguous_fill")
        != "CENSORED_FUTURE_COVERAGE"
        or fill.get("interpolation_allowed") is not False
    ):
        raise CausalSalvageRuntimeError("causal salvage frozen cost contract drift")
    safety = manifest.get("safety") or {}
    if any(bool(value) for value in safety.values()):
        raise CausalSalvageRuntimeError("causal salvage safety authorization drift")


def _verify_source_commit(root: Path, expected: str) -> None:
    completed = subprocess.run(
        ["git", "merge-base", "--is-ancestor", expected, "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise CausalSalvageRuntimeError(
            f"causal salvage source commit is not an ancestor of HEAD: {expected}"
        )


def _git_head(root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _sealed_checkpoint(
    manifest_sha: str, stage: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    checkpoint = {
        "schema": CAUSAL_SALVAGE_CHECKPOINT_SCHEMA,
        "runtime_version": CAUSAL_SALVAGE_RUNTIME_VERSION,
        "campaign_id": "hydra_causal_salvage_sprint_0027",
        "manifest_sha256": manifest_sha,
        "stage": stage,
        **dict(payload),
    }
    checkpoint["checkpoint_hash"] = stable_hash(checkpoint)
    return checkpoint


def _terminal_result(
    *,
    manifest_sha: str,
    status: str,
    phase_a: Mapping[str, Any] | None,
    phase_b: Mapping[str, Any] | None,
    component_checkpoint_hash: str,
    reason: str,
    promotion_authorized: bool = False,
) -> dict[str, Any]:
    result = {
        "schema": CAUSAL_SALVAGE_RESULT_SCHEMA,
        "runtime_version": CAUSAL_SALVAGE_RUNTIME_VERSION,
        "campaign_id": "hydra_causal_salvage_sprint_0027",
        "manifest_sha256": manifest_sha,
        "status": status,
        "reason": reason,
        "component_checkpoint_hash": component_checkpoint_hash,
        "phase_a_checkpoint_hash": (
            None if phase_a is None else phase_a.get("checkpoint_hash")
        ),
        "phase_b_summary_hash": (
            None if phase_b is None else phase_b.get("summary_hash")
        ),
        "xfa_paths_started": 0,
        "forward_activation_authorized": False,
        "promotion_authorized": bool(promotion_authorized),
        "safety": _safety_receipt(),
    }
    result["result_hash"] = stable_hash(result)
    return result


def _public_result(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": "hydra_causal_salvage_public_result_v1",
        "campaign_id": result["campaign_id"],
        "status": result["status"],
        "result_hash": result["result_hash"],
        "output_dir": str(output_root),
        "result_path": str(output_root / "causal_salvage_result.json"),
        "xfa_paths_started": 0,
        "promotion_authorized": bool(result.get("promotion_authorized", False)),
    }


def _safety_receipt() -> dict[str, Any]:
    return {
        "q4_access_count_delta": 0,
        "market_data_purchase_delta_usd": 0.0,
        "broker_connections": 0,
        "orders": 0,
        "live_trading": False,
        "controller_version_changed": False,
        "new_service_created": False,
        "registry_writes": 0,
        "database_writes": 0,
        "xfa_paths_started": 0,
    }


def _inside(root: Path, value: str | Path, *, label: str) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CausalSalvageRuntimeError(f"{label} escapes repository root") from exc
    return resolved


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CausalSalvageRuntimeError(f"cannot read JSON input: {path}") from exc
    if not isinstance(value, dict):
        raise CausalSalvageRuntimeError(f"JSON input is not an object: {path}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CAUSAL_SALVAGE_RUNTIME_VERSION",
    "CausalSalvageRuntimeError",
    "run_causal_salvage_sprint",
]
