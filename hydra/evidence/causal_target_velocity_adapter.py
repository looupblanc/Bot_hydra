"""EvidenceBundle adapter for causal target-velocity campaign 0028.

The hazard search persists an event contract that is richer than the legacy
``CausalSignalEvidence`` shape and, importantly, permits the signal direction
to vary from event to event.  This adapter preserves that event-level direction
when it materializes the canonical component ledgers.  It never substitutes a
candidate-level side for an observed event side.

Only outcome-complete hazard events become exits or chronological trades.
Signals censored before a fill remain signal-only; signals censored after a
real fill retain an unresolved entry and never receive a fabricated exit.

The finalizer deliberately reuses the canonical causal account-episode
normalizer.  It writes all EvidenceBundle V1 datasets through one resumable
writer, verifies the sealed bundle deeply, and refuses summary-only campaign
completion.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence, cast

from hydra.account_policy.causal_active_pool_replay import (
    CAUSAL_ACCOUNT_REPLAY_VERSION,
)
from hydra.evidence import (
    COST_SCENARIOS,
    EvidenceBundleReceipt,
    EvidenceBundleWriter,
    REQUIRED_DATASETS,
    guard_campaign_completion,
    verify_evidence_bundle,
)
from hydra.evidence.causal_salvage_adapter import (
    CausalSalvageEvidenceError,
    _canonical_hash,
    _episode_ledgers,
    _validate_streaming_coverage,
)
from hydra.evidence.schema import validate_identity
from hydra.research.causal_sleeve_replay import (
    CENSORED_FUTURE_COVERAGE,
    TARGET_OBSERVED,
    CausalSignalEvidence,
    CausalSleeveReplay,
    CausalTradeMark,
)
from hydra.research.causal_target_velocity import (
    ENGINE_VERSION,
    ExactHazardSleeveReplay,
    HazardCandidate,
    HazardEventEvidence,
    HazardOutcome,
    exact_sleeve_replay,
    stable_hash,
)


CAUSAL_TARGET_VELOCITY_ADAPTER_VERSION = (
    "hydra_causal_target_velocity_evidence_adapter_v1"
)
_COMPONENT_ROLE = "TARGET_VELOCITY_HAZARD"
_SIGNAL_FEATURE_VALUE_SENTINEL = 0.0
_SMALL_BATCH_SIZE = 5_000
_EPISODE_BATCH_SIZE = 2_000
_DAILY_PATH_BATCH_SIZE = 5_000


class CausalTargetVelocityEvidenceError(CausalSalvageEvidenceError):
    """Campaign-0028 evidence cannot honestly satisfy EvidenceBundle V1."""


def reconstruct_exact_hazard_replay(
    *,
    candidate_payload: Mapping[str, Any],
    event_mappings: Sequence[Mapping[str, Any] | HazardEventEvidence],
    eligible_session_days: Sequence[int],
    expected_hashes: Mapping[str, str],
) -> ExactHazardSleeveReplay:
    """Reconstruct a worker-produced exact replay and reconcile every hash.

    Workers may only cross the coordinator boundary with canonical mappings.
    This helper restores the typed event and mark objects, invokes the same
    exact replay constructor used in-process, and rejects any mismatch with the
    hashes written by the worker.
    """

    payload = dict(candidate_payload)
    payload.pop("schema", None)
    payload.pop("candidate_id", None)
    payload.pop("structural_fingerprint", None)
    candidate = HazardCandidate(**payload)
    events = tuple(_hazard_event(value) for value in event_mappings)
    if any(row.candidate_id != candidate.candidate_id for row in events):
        raise CausalTargetVelocityEvidenceError(
            "hazard event inventory disagrees with candidate identity"
        )
    # ``exact_sleeve_replay`` needs only ``.candidate`` from its calibrated
    # input.  Avoid reconstructing calibration statistics that are irrelevant
    # to event/trade reconciliation.
    replay = exact_sleeve_replay(
        cast(Any, SimpleNamespace(candidate=candidate)),
        events,
        eligible_session_days=eligible_session_days,
    )
    observed = _exact_replay_hashes(replay)
    required = set(observed)
    if set(expected_hashes) != required:
        raise CausalTargetVelocityEvidenceError(
            "expected exact-replay hash inventory drift: required "
            + ", ".join(sorted(required))
        )
    mismatches = {
        name: {"expected": str(expected_hashes[name]), "observed": digest}
        for name, digest in observed.items()
        if str(expected_hashes[name]) != digest
    }
    if mismatches:
        raise CausalTargetVelocityEvidenceError(
            "worker/coordinator exact hazard replay mismatch: "
            + json.dumps(mismatches, sort_keys=True)
        )
    return replay


def adapt_exact_hazard_replay(
    replay: ExactHazardSleeveReplay,
) -> CausalSleeveReplay:
    """Convert exact hazard evidence to the canonical causal replay shape.

    The generic causal signal class predates fingerprint-only feature evidence
    and requires a finite ``trigger_value``.  Campaign 0028 intentionally
    persists the complete immutable feature fingerprint rather than copying a
    mutable feature vector into every event.  The finite zero below is an
    explicitly declared compatibility sentinel; the component signal ledger
    records ``raw_feature_values_embedded=False`` and never represents it as an
    economic trigger value.
    """

    signals = tuple(_causal_signal(row) for row in replay.events)
    censored_count = sum(
        row.outcome == HazardOutcome.CENSORED_FUTURE_COVERAGE
        for row in replay.events
    )
    empty_censor_hash = stable_hash([])
    return CausalSleeveReplay(
        sleeve_id=replay.candidate.candidate_id,
        signal_count=len(signals),
        completed_trade_count=len(replay.normal_events),
        censored_signal_count=censored_count,
        eligible_session_days=tuple(replay.eligible_session_days),
        normal_events=tuple(replay.normal_events),
        stressed_events=tuple(replay.stressed_events),
        normal_trajectories=tuple(replay.normal_trajectories),
        stressed_trajectories=tuple(replay.stressed_trajectories),
        # Hazard events censored after a fill have no fabricated liquidation
        # mark.  Their real entry is retained by the component ledger instead.
        normal_censored_trajectories=(),
        stressed_censored_trajectories=(),
        signals=signals,
        decision_hash=replay.decision_hash,
        normal_event_hash=replay.normal_event_hash,
        stressed_event_hash=replay.stressed_event_hash,
        normal_censored_trajectory_hash=empty_censor_hash,
        stressed_censored_trajectory_hash=empty_censor_hash,
        fill_policy_hash=replay.fill_policy_hash,
        specification_hash=replay.candidate.structural_fingerprint,
    )


def finalize_causal_target_velocity_evidence_bundle(
    *,
    base_dir: str | Path,
    lightweight_manifest_path: str | Path,
    campaign_manifest: Mapping[str, Any],
    exact_replays: Mapping[str, ExactHazardSleeveReplay],
    policies: Mapping[str, Any],
    evaluated_policy_records: Sequence[Mapping[str, Any]],
    data_fingerprints: Mapping[str, str],
    provenance: Mapping[str, Any],
    compact_context: Mapping[str, Any] | None = None,
    writer_id: str | None = None,
) -> EvidenceBundleReceipt:
    """Atomically seal complete causal target-velocity development evidence.

    ``evaluated_policy_records`` is intentionally a materialized sequence.  It
    lets the coordinator freeze the exact policy/episode coverage in identity
    before the single writer appends the first evidence partition.  Resuming
    replays the same logical sequence from the beginning; deterministic batch
    IDs make already committed partitions idempotent.
    """

    if not exact_replays:
        raise CausalTargetVelocityEvidenceError(
            "causal target-velocity evidence requires exact sleeve replays"
        )
    if not policies:
        raise CausalTargetVelocityEvidenceError(
            "causal target-velocity evidence requires executable policies"
        )
    if not evaluated_policy_records:
        raise CausalTargetVelocityEvidenceError(
            "summary-only completion is forbidden: no account episodes"
        )
    if isinstance(evaluated_policy_records, (str, bytes)):
        raise CausalTargetVelocityEvidenceError(
            "evaluated policy records must be a materialized record sequence"
        )
    replay_ids = set(exact_replays)
    if any(
        key != replay.candidate.candidate_id
        for key, replay in exact_replays.items()
    ):
        raise CausalTargetVelocityEvidenceError(
            "exact replay mapping key disagrees with candidate identity"
        )
    causal_replays = {
        component_id: adapt_exact_hazard_replay(replay)
        for component_id, replay in exact_replays.items()
    }
    policy_views = {
        policy_id: _policy_view(policy_id, value)
        for policy_id, value in policies.items()
    }
    _validate_policy_inventory(policy_views, replay_ids)
    identity = _identity(
        campaign_manifest=campaign_manifest,
        replays=causal_replays,
        policies=policy_views,
        records=evaluated_policy_records,
        data_fingerprints=data_fingerprints,
    )
    checked_identity = validate_identity(identity)
    campaign_id = str(checked_identity["campaign_id"])
    component_rows = _component_ledgers(
        campaign_id=campaign_id,
        replays=exact_replays,
        expected_fingerprints=checked_identity["component_fingerprints"],
    )
    memberships = _membership_rows(
        campaign_id=campaign_id,
        policies=policy_views,
        component_ids=replay_ids,
        expected_fingerprints=checked_identity["policy_fingerprints"],
    )
    provenance_row = _provenance_row(
        campaign_id=campaign_id,
        identity=checked_identity,
        exact_replays=exact_replays,
        policies=policy_views,
        value=provenance,
    )
    small_datasets: dict[str, list[dict[str, Any]]] = {
        **component_rows,
        "account_policy_membership": memberships,
        "provenance": [provenance_row],
    }
    empty = sorted(name for name, rows in small_datasets.items() if not rows)
    if empty:
        raise CausalTargetVelocityEvidenceError(
            "causal target-velocity cannot seal empty required datasets: "
            + ", ".join(empty)
        )

    base = Path(base_dir).resolve()
    resolved_writer_id = writer_id or f"causal-target-velocity:{campaign_id}"
    staging = base / f".{campaign_id}.evidence-v1.staging"
    writer = (
        EvidenceBundleWriter.resume(
            base,
            campaign_id,
            writer_id=resolved_writer_id,
            expected_identity=checked_identity,
        )
        if staging.is_dir()
        else EvidenceBundleWriter.create(
            base,
            checked_identity,
            writer_id=resolved_writer_id,
        )
    )
    seen_hashes: dict[tuple[str, str, str, str], str] = {}
    scenario_coverage: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    observed_base_keys: set[tuple[str, str, str]] = set()
    episode_buffer: list[dict[str, Any]] = []
    daily_buffer: list[dict[str, Any]] = []
    episode_part = 0
    daily_part = 0
    daily_count = 0
    sequence_hasher = hashlib.sha256()
    accumulator = _CompactAccumulator(
        campaign_id=campaign_id,
        replays=causal_replays,
    )

    def flush_episodes() -> None:
        nonlocal episode_part
        if not episode_buffer:
            return
        writer.append_records(
            "episodes",
            episode_buffer,
            batch_id=f"causal-target-velocity:episodes:{episode_part:08d}",
        )
        episode_part += 1
        episode_buffer.clear()

    def flush_daily() -> None:
        nonlocal daily_part
        if not daily_buffer:
            return
        writer.append_records(
            "account_daily_paths",
            daily_buffer,
            batch_id=f"causal-target-velocity:account-daily:{daily_part:08d}",
        )
        daily_part += 1
        daily_buffer.clear()

    try:
        for dataset in (
            "component_signals",
            "component_entries",
            "component_exits",
            "component_trades",
            "account_policy_membership",
            "provenance",
        ):
            rows = small_datasets[dataset]
            for index in range(0, len(rows), _SMALL_BATCH_SIZE):
                writer.append_records(
                    dataset,
                    rows[index : index + _SMALL_BATCH_SIZE],
                    batch_id=(
                        "causal-target-velocity:"
                        f"{dataset}:{index // _SMALL_BATCH_SIZE:08d}"
                    ),
                )

        for raw_record in evaluated_policy_records:
            if not isinstance(raw_record, Mapping):
                raise CausalTargetVelocityEvidenceError(
                    "evaluated policy record is not an object"
                )
            episodes, paths = _episode_ledgers(
                campaign_id=campaign_id,
                records=[raw_record],
                policy_ids=set(policy_views),
                component_ids=replay_ids,
            )
            if len(episodes) != 1 or not paths:
                raise CausalTargetVelocityEvidenceError(
                    "one account record must produce one non-empty episode path"
                )
            episode = episodes[0]
            key = (
                str(episode["policy_id"]),
                str(episode["episode_id"]),
                str(episode["horizon"]),
                str(episode["cost_scenario"]),
            )
            digest = _canonical_hash(
                {"episode": episode, "account_daily_paths": paths}
            )
            sequence_hasher.update(
                json.dumps([*key, digest], separators=(",", ":")).encode("utf-8")
                + b"\n"
            )
            prior = seen_hashes.get(key)
            if prior is not None:
                if prior != digest:
                    raise CausalTargetVelocityEvidenceError(
                        "duplicate episode key has divergent evidence: " + repr(key)
                    )
                continue
            seen_hashes[key] = digest
            observed_base_keys.add(key[:3])
            scenario_coverage[key[:3]].add(key[3])
            accumulator.add(episode)
            episode_buffer.append(episode)
            daily_buffer.extend(paths)
            daily_count += len(paths)
            if len(episode_buffer) >= _EPISODE_BATCH_SIZE:
                flush_episodes()
            if len(daily_buffer) >= _DAILY_PATH_BATCH_SIZE:
                flush_daily()
        flush_episodes()
        flush_daily()
        _validate_streaming_coverage(
            checked_identity,
            observed_base_keys=observed_base_keys,
            scenario_coverage=scenario_coverage,
        )
        expected_counts = {
            **{name: len(rows) for name, rows in small_datasets.items()},
            "episodes": len(seen_hashes),
            "account_daily_paths": daily_count,
        }
        count_drift = {
            name: {
                "expected": count,
                "observed": writer.dataset_row_counts.get(name, 0),
            }
            for name, count in expected_counts.items()
            if writer.dataset_row_counts.get(name, 0) != count
        }
        if count_drift:
            raise CausalTargetVelocityEvidenceError(
                "resumable staging row counts disagree with logical evidence: "
                + json.dumps(count_drift, sort_keys=True)
            )
        compact = accumulator.outputs(
            context={
                **dict(compact_context or {}),
                "adapter_version": CAUSAL_TARGET_VELOCITY_ADAPTER_VERSION,
                "input_sequence_sha256": sequence_hasher.hexdigest(),
                "complete_required_datasets": True,
                "summary_only_completion_forbidden": True,
                "development_only": True,
                "xfa_deferred": True,
            }
        )
        for name, value in compact.items():
            writer.write_compact_output(name, value)
        writer.checkpoint(
            {
                "stage": "CAUSAL_TARGET_VELOCITY_EVIDENCE_COMPLETE",
                "required_datasets": list(REQUIRED_DATASETS),
                "dataset_row_counts": expected_counts,
                "input_sequence_sha256": sequence_hasher.hexdigest(),
                "resume_contract": "REPLAY_IDENTICAL_LOGICAL_INPUT_FROM_BEGINNING",
            }
        )
        receipt = writer.finalize(
            evidence_status="FRESH_DEVELOPMENT_EVIDENCE",
            lightweight_manifest_path=Path(lightweight_manifest_path),
        )
    except BaseException:
        writer.close()
        raise
    manifest = verify_evidence_bundle(receipt.bundle_path, deep=True)
    if manifest.get("evidence_status") != "FRESH_DEVELOPMENT_EVIDENCE":
        raise CausalTargetVelocityEvidenceError(
            "sealed target-velocity evidence status drift"
        )
    guard_campaign_completion(
        "COMPLETE", receipt.bundle_path, campaign_id=campaign_id
    )
    return receipt


def _hazard_event(
    value: Mapping[str, Any] | HazardEventEvidence,
) -> HazardEventEvidence:
    if isinstance(value, HazardEventEvidence):
        return value
    payload = dict(value)
    expected_fingerprint = payload.pop("event_fingerprint", None)
    payload.pop("schema", None)
    payload["normal_marks"] = tuple(
        CausalTradeMark.from_mapping(row) for row in payload["normal_marks"]
    )
    payload["stressed_marks"] = tuple(
        CausalTradeMark.from_mapping(row) for row in payload["stressed_marks"]
    )
    event = HazardEventEvidence(**payload)
    if expected_fingerprint is not None and str(expected_fingerprint) != event.fingerprint:
        raise CausalTargetVelocityEvidenceError(
            f"hazard event fingerprint mismatch: {event.event_id}"
        )
    return event


def _exact_replay_hashes(replay: ExactHazardSleeveReplay) -> dict[str, str]:
    return {
        "decision_hash": replay.decision_hash,
        "normal_event_hash": replay.normal_event_hash,
        "stressed_event_hash": replay.stressed_event_hash,
        "normal_trajectory_hash": replay.normal_trajectory_hash,
        "stressed_trajectory_hash": replay.stressed_trajectory_hash,
        "fill_policy_hash": replay.fill_policy_hash,
    }


def _causal_signal(row: HazardEventEvidence) -> CausalSignalEvidence:
    complete = row.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
    normal_exit = _scenario_exit_price(row, stressed=False) if complete else None
    stressed_exit = _scenario_exit_price(row, stressed=True) if complete else None
    resting = str(row.exit_fill_semantics).startswith("RESTING_")
    exit_intent_ns = (
        row.fill_time_ns if complete and resting else row.outcome_time_ns
    )
    return CausalSignalEvidence(
        signal_id=row.event_id,
        sleeve_id=row.candidate_id,
        signal_time_ns=row.event_time_ns,
        decision_time_ns=row.decision_time_ns,
        order_submit_time_ns=row.order_submit_time_ns,
        earliest_executable_time_ns=row.earliest_executable_time_ns,
        fill_time_ns=row.fill_time_ns,
        raw_entry_open=row.raw_fill_price,
        normal_entry_fill_price=row.normal_fill_price,
        stressed_entry_fill_price=row.stressed_fill_price,
        exit_decision_time_ns=exit_intent_ns,
        exit_order_submit_time_ns=exit_intent_ns,
        exit_earliest_executable_time_ns=exit_intent_ns,
        exit_fill_time_ns=row.outcome_time_ns if complete else None,
        raw_exit_open=row.raw_exit_price if complete else None,
        normal_exit_fill_price=normal_exit,
        stressed_exit_fill_price=stressed_exit,
        session_day=row.session_day,
        segment_code=row.segment_code,
        contract_code=row.contract_code,
        direction=row.direction,
        quantity=row.quantity,
        outcome_status=(TARGET_OBSERVED if complete else CENSORED_FUTURE_COVERAGE),
        censor_reason=row.censor_reason,
        censor_time_ns=None,
        trigger_value=_SIGNAL_FEATURE_VALUE_SENTINEL,
        context_value=None,
        kernel_version=ENGINE_VERSION,
        fill_policy_id=row.fill_policy_id,
        fill_policy_hash=row.fill_policy_hash,
    )


def _scenario_exit_price(
    row: HazardEventEvidence, *, stressed: bool
) -> float:
    if row.raw_fill_price is None or row.raw_exit_price is None:
        raise CausalTargetVelocityEvidenceError(
            "completed hazard event lacks raw entry/exit prices"
        )
    entry = row.stressed_fill_price if stressed else row.normal_fill_price
    if entry is None:
        raise CausalTargetVelocityEvidenceError(
            "completed hazard event lacks scenario entry price"
        )
    per_side_slippage = abs(float(entry) - float(row.raw_fill_price))
    return float(row.raw_exit_price) - int(row.direction) * per_side_slippage


def _component_ledgers(
    *,
    campaign_id: str,
    replays: Mapping[str, ExactHazardSleeveReplay],
    expected_fingerprints: Mapping[str, str],
) -> dict[str, list[dict[str, Any]]]:
    signals: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    represented_trades: set[str] = set()
    for component_id, replay in sorted(replays.items()):
        if replay.candidate.structural_fingerprint != str(
            expected_fingerprints[component_id]
        ):
            raise CausalTargetVelocityEvidenceError(
                f"candidate fingerprint drift: {component_id}"
            )
        normal_by_signal = {
            row.event_id.removesuffix(":NORMAL"): row
            for row in replay.normal_events
        }
        stressed_by_signal = {
            row.event_id.removesuffix(":STRESSED_1_5X"): row
            for row in replay.stressed_events
        }
        if len(normal_by_signal) != len(replay.normal_events) or len(
            stressed_by_signal
        ) != len(replay.stressed_events):
            raise CausalTargetVelocityEvidenceError(
                f"duplicate hazard trade event: {component_id}"
            )
        for event in replay.events:
            complete = event.outcome != HazardOutcome.CENSORED_FUTURE_COVERAGE
            signal_time = _ns_iso(event.event_time_ns)
            common_times = {
                "signal_time": signal_time,
                "decision_time": _ns_iso(event.decision_time_ns),
                "order_submit_time": _ns_iso(event.order_submit_time_ns),
                "earliest_executable_time": _ns_iso(
                    event.earliest_executable_time_ns
                ),
                "fill_time": _ns_iso_or_none(event.fill_time_ns),
                "exit_fill_time": _ns_iso_or_none(event.outcome_time_ns),
            }
            signals.append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "signal_id": event.event_id,
                    "event_time": signal_time,
                    "market": event.market,
                    "contract": event.execution_market,
                    "timeframe": event.timeframe,
                    "signal": {
                        "direction": event.direction,
                        "feature_fingerprint": event.feature_fingerprint,
                        "entry_intent": event.entry_intent,
                        "decision_kernel": ENGINE_VERSION,
                    },
                    "sizing": float(event.quantity),
                    "stop": event.adverse_price,
                    "target": event.favorable_price,
                    "veto": False,
                    "component_role": _COMPONENT_ROLE,
                    "outcome_status": (
                        str(event.outcome)
                        if complete
                        else CENSORED_FUTURE_COVERAGE
                    ),
                    "censor_reason": event.censor_reason,
                    "trade_materialized": complete,
                    "feature_fingerprint": event.feature_fingerprint,
                    "raw_feature_values_embedded": False,
                    "feature_value_compatibility_sentinel": (
                        _SIGNAL_FEATURE_VALUE_SENTINEL
                    ),
                    "available_at": _ns_iso(event.available_at_ns),
                    "contract_code": event.contract_code,
                    "session_day": event.session_day,
                    "session_code": event.session_code,
                    "segment_code": event.segment_code,
                    "fill_policy_id": event.fill_policy_id,
                    "fill_policy_hash": event.fill_policy_hash,
                    "causal_event_fingerprint": event.fingerprint,
                    "evidence_role": event.evidence_role,
                    "control_id": event.control_id,
                    **common_times,
                }
            )
            if event.fill_time_ns is None:
                if complete:
                    raise CausalTargetVelocityEvidenceError(
                        "completed hazard event has no fill"
                    )
                continue
            side = "LONG" if event.direction > 0 else "SHORT"
            if event.normal_fill_price is None:
                raise CausalTargetVelocityEvidenceError(
                    "filled hazard event lacks normal fill price"
                )
            entry = {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": event.event_id,
                "entry_time": _ns_iso(event.fill_time_ns),
                "market": event.market,
                "contract": event.execution_market,
                "side": side,
                "quantity": event.quantity,
                "entry_price": event.normal_fill_price,
                "sizing": float(event.quantity),
                "stop_price": event.adverse_price,
                "target_price": event.favorable_price,
                "source_signal_id": event.event_id,
                "decision_time": _ns_iso(event.decision_time_ns),
                "order_submit_time": _ns_iso(event.order_submit_time_ns),
                "earliest_executable_time": _ns_iso(
                    event.earliest_executable_time_ns
                ),
                "fill_time": _ns_iso(event.fill_time_ns),
            }
            if not complete:
                entry.update(
                    {
                        "outcome_status": CENSORED_FUTURE_COVERAGE,
                        "censor_reason": event.censor_reason,
                        "trade_materialized": False,
                        "open_position_unresolved": True,
                    }
                )
                entries.append(entry)
                continue
            normal = normal_by_signal.pop(event.event_id, None)
            stressed = stressed_by_signal.pop(event.event_id, None)
            if normal is None or stressed is None:
                raise CausalTargetVelocityEvidenceError(
                    "completed hazard signal lacks paired trade-path events"
                )
            normal_exit = _scenario_exit_price(event, stressed=False)
            stressed_exit = _scenario_exit_price(event, stressed=True)
            normal_cost = max(0.0, float(normal.gross_pnl - normal.net_pnl))
            stressed_cost = max(0.0, float(stressed.gross_pnl - stressed.net_pnl))
            entries.append(entry)
            exits.append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "trade_id": event.event_id,
                    "exit_time": _ns_iso(int(event.outcome_time_ns)),
                    "exit_price": normal_exit,
                    "exit_reason": event.exit_fill_semantics,
                    "outcome": str(event.outcome),
                    "same_bar_ambiguous": event.same_bar_ambiguous,
                }
            )
            trades.append(
                {
                    "campaign_id": campaign_id,
                    "component_id": component_id,
                    "trade_id": event.event_id,
                    "entry_time": _ns_iso(event.fill_time_ns),
                    "exit_time": _ns_iso(int(event.outcome_time_ns)),
                    "market": event.market,
                    "contract": event.execution_market,
                    "side": side,
                    "quantity": event.quantity,
                    "entry_price": event.normal_fill_price,
                    "exit_price": normal_exit,
                    "gross_pnl": normal.gross_pnl,
                    "costs": normal_cost,
                    "net_pnl": normal.net_pnl,
                    "stressed_entry_price": event.stressed_fill_price,
                    "stressed_exit_price": stressed_exit,
                    "stressed_gross_pnl": stressed.gross_pnl,
                    "stressed_costs": stressed_cost,
                    "stressed_net_pnl": stressed.net_pnl,
                    "source_normal_event_id": normal.event_id,
                    "source_stressed_event_id": stressed.event_id,
                    "outcome": str(event.outcome),
                    "time_to_favorable_minutes": event.time_to_favorable_minutes,
                    "time_to_adverse_minutes": event.time_to_adverse_minutes,
                    "maximum_favorable_excursion_r": (
                        event.maximum_favorable_excursion_r
                    ),
                    "maximum_adverse_excursion_r": (
                        event.maximum_adverse_excursion_r
                    ),
                    "same_bar_ambiguous": event.same_bar_ambiguous,
                }
            )
            represented_trades.add(component_id)
        if normal_by_signal or stressed_by_signal:
            raise CausalTargetVelocityEvidenceError(
                f"unclaimed exact hazard trade events remain: {component_id}"
            )
    missing = set(replays) - represented_trades
    if missing:
        raise CausalTargetVelocityEvidenceError(
            "EvidenceBundle requires a completed trade for every retained component: "
            + ", ".join(sorted(missing))
        )
    return {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
    }


def _policy_view(policy_id: str, value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        row = dict(value)
    else:
        method = getattr(value, "to_dict", None)
        if not callable(method):
            raise CausalTargetVelocityEvidenceError(
                f"policy is not serializable: {policy_id}"
            )
        row = dict(method())
    if str(row.get("policy_id") or "") != policy_id:
        raise CausalTargetVelocityEvidenceError("policy mapping identity drift")
    components = row.get("component_ids", row.get("component_priority"))
    if not isinstance(components, Sequence) or isinstance(components, (str, bytes)):
        raise CausalTargetVelocityEvidenceError(
            f"policy lacks executable component membership: {policy_id}"
        )
    row["component_ids"] = [str(value) for value in components]
    fingerprint = row.get("structural_fingerprint")
    if fingerprint is None and hasattr(value, "structural_fingerprint"):
        fingerprint = getattr(value, "structural_fingerprint")
    if fingerprint is None:
        fingerprint = stable_hash(
            {key: item for key, item in row.items() if key != "policy_id"}
        )
    row["structural_fingerprint"] = str(fingerprint)
    row.setdefault("static_risk_tier", 1.0)
    return row


def _validate_policy_inventory(
    policies: Mapping[str, Mapping[str, Any]], component_ids: set[str]
) -> None:
    represented: set[str] = set()
    for policy_id, row in policies.items():
        components = tuple(row["component_ids"])
        if not components or len(set(components)) != len(components):
            raise CausalTargetVelocityEvidenceError(
                f"policy membership is empty or duplicated: {policy_id}"
            )
        if not set(components) <= component_ids:
            raise CausalTargetVelocityEvidenceError(
                f"policy references unknown causal component: {policy_id}"
            )
        represented.update(components)
    if represented != component_ids:
        raise CausalTargetVelocityEvidenceError(
            "every exact causal component must belong to an evaluated policy"
        )


def _membership_rows(
    *,
    campaign_id: str,
    policies: Mapping[str, Mapping[str, Any]],
    component_ids: set[str],
    expected_fingerprints: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    represented: set[str] = set()
    for policy_id, policy in sorted(policies.items()):
        if policy["structural_fingerprint"] != str(
            expected_fingerprints[policy_id]
        ):
            raise CausalTargetVelocityEvidenceError(
                f"policy fingerprint drift: {policy_id}"
            )
        for component_id in policy["component_ids"]:
            represented.add(component_id)
            rows.append(
                {
                    "campaign_id": campaign_id,
                    "policy_id": policy_id,
                    "component_id": component_id,
                    "risk_allocation": float(policy["static_risk_tier"]),
                    "component_role": _COMPONENT_ROLE,
                    "causal_policy_specification": dict(policy),
                    "future_outcome_fields_used": False,
                    "inactive_sleeve_reserves_risk": False,
                }
            )
    if represented != component_ids:
        raise CausalTargetVelocityEvidenceError(
            "membership rows do not cover the exact causal component bank"
        )
    return rows


def _identity(
    *,
    campaign_manifest: Mapping[str, Any],
    replays: Mapping[str, CausalSleeveReplay],
    policies: Mapping[str, Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    data_fingerprints: Mapping[str, str],
) -> dict[str, Any]:
    campaign_id = str(campaign_manifest.get("campaign_id") or "")
    source_commit = str(campaign_manifest.get("source_commit") or "")
    created_at = str(campaign_manifest.get("created_at_utc") or "")
    if not campaign_id or not source_commit or not created_at:
        raise CausalTargetVelocityEvidenceError(
            "campaign manifest lacks campaign_id/source_commit/created_at_utc"
        )
    if not data_fingerprints:
        raise CausalTargetVelocityEvidenceError(
            "causal target-velocity identity requires data fingerprints"
        )
    required: dict[tuple[str, str, str], None] = {}
    scenarios: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for record in records:
        policy_id = str(record.get("policy_id") or "")
        if policy_id not in policies:
            raise CausalTargetVelocityEvidenceError(
                "episode identity references an unknown policy"
            )
        scenario = str(record.get("scenario") or record.get("cost_scenario") or "")
        if scenario not in COST_SCENARIOS:
            raise CausalTargetVelocityEvidenceError(
                "episode identity has an unsupported cost scenario"
            )
        horizon = str(record.get("horizon") or "")
        raw_episode = record.get("episode", record)
        episode = _episode_mapping(raw_episode)
        start_day = int(episode["start_day"])
        episode_id = str(record.get("episode_id") or f"{policy_id}:{start_day}")
        key = (policy_id, episode_id, horizon)
        if not horizon:
            raise CausalTargetVelocityEvidenceError("episode horizon is empty")
        required[key] = None
        scenarios[key].add(scenario)
    incomplete = {
        key: sorted(set(COST_SCENARIOS) - value)
        for key, value in scenarios.items()
        if value != set(COST_SCENARIOS)
    }
    if incomplete:
        raise CausalTargetVelocityEvidenceError(
            "identity coverage lacks paired cost scenarios: "
            + repr(sorted(incomplete)[0])
        )
    covered_policies = {key[0] for key in required}
    if covered_policies != set(policies):
        raise CausalTargetVelocityEvidenceError(
            "episode identity does not cover every executable policy"
        )
    manifest_hash = str(campaign_manifest.get("manifest_hash") or stable_hash(campaign_manifest))
    configuration_hash = stable_hash(
        {
            "manifest_hash": manifest_hash,
            "adapter_version": CAUSAL_TARGET_VELOCITY_ADAPTER_VERSION,
            "account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
            "decision_kernel_version": ENGINE_VERSION,
            "fill_policy_hashes": sorted(
                {replay.fill_policy_hash for replay in replays.values()}
            ),
            "data_fingerprints": dict(sorted(data_fingerprints.items())),
        }
    )
    raw_seeds = campaign_manifest.get("seeds", [0])
    seeds = [int(value) for value in raw_seeds]
    return {
        "campaign_id": campaign_id,
        "grammar_id": str(
            campaign_manifest.get("class_id")
            or "TARGET_BEFORE_ADVERSE_EXCURSION_HAZARD_OPPORTUNITY_DENSITY_V1"
        ),
        "policy_fingerprints": {
            policy_id: str(row["structural_fingerprint"])
            for policy_id, row in sorted(policies.items())
        },
        "component_fingerprints": {
            component_id: replay.specification_hash
            for component_id, replay in sorted(replays.items())
        },
        "source_commit": source_commit,
        "data_fingerprints": {
            str(name): str(digest)
            for name, digest in sorted(data_fingerprints.items())
        },
        "configuration_sha256": configuration_hash,
        "seeds": seeds,
        "created_at_utc": created_at,
        "expected_coverage": {
            "policy_ids": sorted(policies),
            "component_ids": sorted(replays),
            "required_episode_keys": [
                {"policy_id": key[0], "episode_id": key[1], "horizon": key[2]}
                for key in sorted(required)
            ],
            "allowed_horizons": sorted({key[2] for key in required}),
            "cost_scenarios": list(COST_SCENARIOS),
            "allow_additional_episode_keys": False,
        },
    }


def _provenance_row(
    *,
    campaign_id: str,
    identity: Mapping[str, Any],
    exact_replays: Mapping[str, ExactHazardSleeveReplay],
    policies: Mapping[str, Mapping[str, Any]],
    value: Mapping[str, Any],
) -> dict[str, Any]:
    access_hash = str(value.get("access_ledger_sha256") or "")
    recorded = str(value.get("recorded_at_utc") or "")
    market_role = str(value.get("market_data_role") or "")
    if len(access_hash) != 64 or not recorded or not market_role:
        raise CausalTargetVelocityEvidenceError(
            "provenance requires access hash, timestamp, and market-data role"
        )
    checksums = {
        "configuration": str(identity["configuration_sha256"]),
        **{
            f"data:{name}": str(digest)
            for name, digest in identity["data_fingerprints"].items()
        },
        **{
            f"hazard_replay:{component_id}:{name}": digest
            for component_id, replay in sorted(exact_replays.items())
            for name, digest in _exact_replay_hashes(replay).items()
        },
        **{
            f"policy:{policy_id}": str(row["structural_fingerprint"])
            for policy_id, row in sorted(policies.items())
        },
    }
    supplied = value.get("immutable_checksums", {})
    if not isinstance(supplied, Mapping):
        raise CausalTargetVelocityEvidenceError(
            "provenance immutable_checksums must be an object"
        )
    for name, digest in supplied.items():
        prior = checksums.get(str(name))
        if prior is not None and prior != str(digest):
            raise CausalTargetVelocityEvidenceError(
                f"provenance checksum conflicts with canonical evidence: {name}"
            )
        checksums[str(name)] = str(digest)
    return {
        "campaign_id": campaign_id,
        "validator_version": "hydra_evidence_bundle_validator_v1",
        "replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
        "market_data_role": market_role,
        "access_ledger_sha256": access_hash,
        "reconstruction_flag": False,
        "immutable_checksums": checksums,
        "recorded_at_utc": recorded,
        "adapter_version": CAUSAL_TARGET_VELOCITY_ADAPTER_VERSION,
        "decision_kernel_version": ENGINE_VERSION,
        "development_only": True,
        "future_outcomes_labels_only": True,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
    }


class _CompactAccumulator:
    def __init__(
        self, *, campaign_id: str, replays: Mapping[str, CausalSleeveReplay]
    ) -> None:
        self.campaign_id = campaign_id
        self.replays = replays
        self.rows: list[dict[str, Any]] = []

    def add(self, row: Mapping[str, Any]) -> None:
        self.rows.append(dict(row))

    def outputs(self, *, context: Mapping[str, Any]) -> dict[str, Any]:
        if not self.rows:
            raise CausalTargetVelocityEvidenceError(
                "compact outputs cannot replace missing episode evidence"
            )
        by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
        failures: dict[str, Counter[str]] = defaultdict(Counter)
        for row in self.rows:
            by_scenario[str(row["cost_scenario"])].append(row)
            by_policy[str(row["policy_id"])].append(row)
            failures[str(row["policy_id"])].update(
                str(value) for value in row["failure_vector"]
            )
        if set(by_scenario) != set(COST_SCENARIOS):
            raise CausalTargetVelocityEvidenceError(
                "compact summary lacks paired scenario evidence"
            )

        def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
            return {
                "episode_count": len(rows),
                "pass_count": sum(
                    row["terminal_state"] == "TARGET_REACHED" for row in rows
                ),
                "mll_breach_count": sum(
                    row["terminal_state"] == "MLL_BREACHED" for row in rows
                ),
                "net_pnl_total": math.fsum(float(row["net_pnl"]) for row in rows),
                "median_target_progress": statistics.median(
                    float(row["target_progress"]) for row in rows
                ),
                "minimum_mll_buffer": min(
                    float(row["minimum_mll_buffer"]) for row in rows
                ),
                "consistency_rate": sum(bool(row["consistency_ok"]) for row in rows)
                / len(rows),
            }

        summary = {
            "schema": "hydra_causal_target_velocity_campaign_summary_v1",
            "campaign_id": self.campaign_id,
            "component_count": len(self.replays),
            "policy_count": len(by_policy),
            "signal_count": sum(row.signal_count for row in self.replays.values()),
            "completed_trade_count": sum(
                row.completed_trade_count for row in self.replays.values()
            ),
            "censored_signal_count": sum(
                row.censored_signal_count for row in self.replays.values()
            ),
            "episode_count": len(self.rows),
            "scenario_results": {
                scenario: summarize(rows)
                for scenario, rows in sorted(by_scenario.items())
            },
            "fresh_development_evidence": True,
            "independently_confirmed": False,
            "xfa_deferred": True,
            "context": dict(context),
        }
        return {
            "campaign_summary": summary,
            "failure_vectors": {
                "schema": "hydra_causal_target_velocity_failure_vectors_v1",
                "campaign_id": self.campaign_id,
                "by_policy": {
                    policy_id: dict(sorted(counter.items()))
                    for policy_id, counter in sorted(failures.items())
                },
            },
            "pareto_archive": {
                "schema": "hydra_causal_target_velocity_pareto_archive_v1",
                "campaign_id": self.campaign_id,
                "frontier": [
                    {"policy_id": policy_id, "results": summarize(rows)}
                    for policy_id, rows in sorted(by_policy.items())
                ],
                "opaque_score_used": False,
            },
            "next_campaign_recommendations": {
                "schema": "hydra_causal_target_velocity_next_action_v1",
                "campaign_id": self.campaign_id,
                "action": "APPLY_FROZEN_0028_SUCCESSIVE_HALVING_GATE",
                "mutation_authorized": False,
                "xfa_authorized": False,
                "q4_access_authorized": False,
                "data_purchase_authorized": False,
            },
        }


def _episode_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    method = getattr(value, "to_dict", None)
    if not callable(method):
        raise CausalTargetVelocityEvidenceError(
            "account episode is neither mapping nor serializable"
        )
    try:
        result = method(include_paths=True)
    except TypeError:
        result = method()
    if not isinstance(result, Mapping):
        raise CausalTargetVelocityEvidenceError(
            "account episode serialization did not return an object"
        )
    return dict(result)


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _ns_iso_or_none(value: int | None) -> str | None:
    return None if value is None else _ns_iso(value)


__all__ = [
    "CAUSAL_TARGET_VELOCITY_ADAPTER_VERSION",
    "CausalTargetVelocityEvidenceError",
    "adapt_exact_hazard_replay",
    "finalize_causal_target_velocity_evidence_bundle",
    "reconstruct_exact_hazard_replay",
]
