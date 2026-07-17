"""EvidenceBundle V1 adapter for the bounded causal-salvage runner.

The adapter translates causal sleeve decisions and already evaluated account
episodes into the existing EvidenceBundle V1 contract.  It does not replay a
signal, infer a missing fill, or reconstruct account attribution.  In
particular, a pre-fill censored signal is signal-only, while a filled censored
signal preserves its causal ``component_entries`` row without fabricating an
exit or completed trade.

Account episode records must carry the enriched daily rows produced by
``causal_active_pool_replay``.  Missing per-day costs, attribution, exposure,
MLL, or consistency is a hard error rather than an invitation to allocate an
episode total after the fact.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

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
from hydra.evidence.schema import validate_identity
from hydra.research.causal_sleeve_replay import (
    CAUSAL_DECISION_KERNEL_VERSION,
    CENSORED_FUTURE_COVERAGE,
    TARGET_OBSERVED,
    CausalSleeveReplay,
)


CAUSAL_SALVAGE_ADAPTER_VERSION = "hydra_causal_salvage_evidence_adapter_v2"
_REQUIRED_DAILY_FIELDS = {
    "session_day",
    "balance",
    "mll_floor",
    "mll_buffer",
    "minimum_mll_buffer",
    "day_pnl",
    "realized_pnl",
    "unrealized_pnl",
    "costs",
    "target_progress",
    "consistency",
    "consistency_ok",
    "conflicts",
    "exposure",
    "component_attribution",
    "open_positions",
}


class CausalSalvageEvidenceError(RuntimeError):
    """Causal salvage inputs cannot honestly satisfy EvidenceBundle V1."""


@dataclass(frozen=True, slots=True)
class CausalSalvageMaterialization:
    records: dict[str, list[dict[str, Any]]]
    compact_outputs: dict[str, Any]


def materialize_causal_salvage_evidence(
    *,
    identity: Mapping[str, Any],
    causal_replays: Mapping[str, CausalSleeveReplay],
    evaluated_policy_records: Sequence[Mapping[str, Any]],
    policies: Mapping[str, Any],
    sleeve_specs: Mapping[str, Any],
    provenance: Mapping[str, Any],
    compact_context: Mapping[str, Any] | None = None,
) -> CausalSalvageMaterialization:
    """Materialize all required V1 datasets without writing to disk.

    ``evaluated_policy_records`` uses the runner handoff shape::

        {
          "policy_id": "...",
          "scenario": "NORMAL" | "STRESSED_1_5X",
          "horizon": "90_TRADING_DAYS",
          "temporal_block": "B1",
          "episode": AccountPolicyEpisode | exact episode mapping,
          "episode_id": "optional frozen override"
        }

    Object episodes are converted with ``to_dict(include_paths=True)``.  Exact
    mappings must include ``daily_path`` and the same enriched fields.
    """

    checked_identity = validate_identity(identity)
    campaign_id = str(checked_identity["campaign_id"])
    replay_ids = set(causal_replays)
    spec_ids = set(sleeve_specs)
    policy_ids = set(policies)
    if replay_ids != spec_ids:
        raise CausalSalvageEvidenceError(
            "causal replay and sleeve specification inventories disagree"
        )
    if replay_ids != set(checked_identity["component_fingerprints"]):
        raise CausalSalvageEvidenceError(
            "causal replay inventory disagrees with component fingerprints"
        )
    if policy_ids != set(checked_identity["policy_fingerprints"]):
        raise CausalSalvageEvidenceError(
            "policy inventory disagrees with immutable policy fingerprints"
        )

    component_rows = _component_ledgers(
        campaign_id=campaign_id,
        replays=causal_replays,
        specs=sleeve_specs,
        expected_fingerprints=checked_identity["component_fingerprints"],
    )
    membership = _membership_rows(
        campaign_id=campaign_id,
        policies=policies,
        specs=sleeve_specs,
        expected_fingerprints=checked_identity["policy_fingerprints"],
    )
    episodes, daily_paths = _episode_ledgers(
        campaign_id=campaign_id,
        records=evaluated_policy_records,
        policy_ids=policy_ids,
        component_ids=replay_ids,
    )
    _require_expected_episode_coverage(checked_identity, episodes)
    provenance_row = _provenance_row(
        campaign_id=campaign_id,
        identity=checked_identity,
        replays=causal_replays,
        policies=policies,
        value=provenance,
    )
    records = {
        **component_rows,
        "account_policy_membership": membership,
        "account_daily_paths": daily_paths,
        "episodes": episodes,
        "provenance": [provenance_row],
    }
    if set(records) != set(REQUIRED_DATASETS):
        raise CausalSalvageEvidenceError("required EvidenceBundle dataset drift")
    empty = sorted(name for name, rows in records.items() if not rows)
    if empty:
        raise CausalSalvageEvidenceError(
            "causal salvage cannot seal empty required datasets: " + ", ".join(empty)
        )
    compact = _compact_outputs(
        campaign_id=campaign_id,
        replays=causal_replays,
        episodes=episodes,
        context=compact_context or {},
    )
    return CausalSalvageMaterialization(records=records, compact_outputs=compact)


def finalize_causal_salvage_evidence_bundle(
    *,
    base_dir: str | Path,
    lightweight_manifest_path: str | Path,
    identity: Mapping[str, Any],
    causal_replays: Mapping[str, CausalSleeveReplay],
    evaluated_policy_records: Sequence[Mapping[str, Any]],
    policies: Mapping[str, Any],
    sleeve_specs: Mapping[str, Any],
    provenance: Mapping[str, Any],
    compact_context: Mapping[str, Any] | None = None,
    writer_id: str | None = None,
) -> EvidenceBundleReceipt:
    """Write and atomically finalize fresh causal development evidence."""

    checked_identity = validate_identity(identity)
    campaign_id = str(checked_identity["campaign_id"])
    materialized = materialize_causal_salvage_evidence(
        identity=checked_identity,
        causal_replays=causal_replays,
        evaluated_policy_records=evaluated_policy_records,
        policies=policies,
        sleeve_specs=sleeve_specs,
        provenance=provenance,
        compact_context=compact_context,
    )
    base = Path(base_dir).resolve()
    resolved_writer_id = writer_id or f"causal-salvage:{campaign_id}"
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
    try:
        for dataset in REQUIRED_DATASETS:
            rows = materialized.records[dataset]
            for index in range(0, len(rows), 5_000):
                writer.append_records(
                    dataset,
                    rows[index : index + 5_000],
                    batch_id=(
                        f"causal-salvage:{dataset}:{index // 5_000:06d}"
                    ),
                )
        for name, value in materialized.compact_outputs.items():
            writer.write_compact_output(name, value)
        writer.checkpoint(
            {
                "stage": "CAUSAL_SALVAGE_EVIDENCE_COMPLETE",
                "signal_count": len(materialized.records["component_signals"]),
                "trade_count": len(materialized.records["component_trades"]),
                "episode_count": len(materialized.records["episodes"]),
                "censored_signal_count": sum(
                    row.get("outcome_status") == CENSORED_FUTURE_COVERAGE
                    for row in materialized.records["component_signals"]
                ),
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
        raise CausalSalvageEvidenceError("sealed causal evidence status drift")
    guard_campaign_completion(
        "COMPLETE", receipt.bundle_path, campaign_id=campaign_id
    )
    return receipt


def finalize_causal_salvage_evidence_bundle_streaming(
    *,
    base_dir: str | Path,
    lightweight_manifest_path: str | Path,
    identity: Mapping[str, Any],
    causal_replays: Mapping[str, CausalSleeveReplay],
    evaluated_policy_record_chunks: Iterable[
        Mapping[str, Any] | Iterable[Mapping[str, Any]]
    ],
    policies: Mapping[str, Any],
    sleeve_specs: Mapping[str, Any],
    provenance: Mapping[str, Any],
    compact_context: Mapping[str, Any] | None = None,
    writer_id: str | None = None,
    episode_batch_size: int = 2_000,
    daily_path_batch_size: int = 5_000,
) -> EvidenceBundleReceipt:
    """Seal causal evidence without retaining account daily paths in memory.

    The input may be an iterable of individual evaluated-policy records, an
    iterable of record chunks, or a mixture of both.  Records are normalized
    one episode at a time and only bounded output batches are retained.  A
    repeated ``(policy_id, episode_id, horizon, scenario)`` is accepted only
    when the canonical episode-plus-daily-path hash is identical; otherwise
    the writer fails closed before finalization.

    Resumption is deterministic when the caller replays the same logical
    record sequence from the beginning.  Batch boundaries are based on output
    row counts rather than caller chunk boundaries, so changing only the chunk
    sizes does not change the EvidenceBundle partitions.
    """

    if episode_batch_size <= 0 or daily_path_batch_size <= 0:
        raise CausalSalvageEvidenceError(
            "streaming EvidenceBundle batch sizes must be positive"
        )
    checked_identity = validate_identity(identity)
    campaign_id = str(checked_identity["campaign_id"])
    replay_ids = set(causal_replays)
    policy_ids = set(policies)
    if replay_ids != set(sleeve_specs):
        raise CausalSalvageEvidenceError(
            "causal replay and sleeve specification inventories disagree"
        )
    if replay_ids != set(checked_identity["component_fingerprints"]):
        raise CausalSalvageEvidenceError(
            "causal replay inventory disagrees with component fingerprints"
        )
    if policy_ids != set(checked_identity["policy_fingerprints"]):
        raise CausalSalvageEvidenceError(
            "policy inventory disagrees with immutable policy fingerprints"
        )

    component_rows = _component_ledgers(
        campaign_id=campaign_id,
        replays=causal_replays,
        specs=sleeve_specs,
        expected_fingerprints=checked_identity["component_fingerprints"],
    )
    membership = _membership_rows(
        campaign_id=campaign_id,
        policies=policies,
        specs=sleeve_specs,
        expected_fingerprints=checked_identity["policy_fingerprints"],
    )
    provenance_row = _provenance_row(
        campaign_id=campaign_id,
        identity=checked_identity,
        replays=causal_replays,
        policies=policies,
        value=provenance,
    )
    small_datasets: dict[str, list[dict[str, Any]]] = {
        **component_rows,
        "account_policy_membership": membership,
        "provenance": [provenance_row],
    }
    empty = sorted(name for name, rows in small_datasets.items() if not rows)
    if empty:
        raise CausalSalvageEvidenceError(
            "causal salvage cannot seal empty required datasets: " + ", ".join(empty)
        )

    base = Path(base_dir).resolve()
    resolved_writer_id = writer_id or f"causal-salvage-streaming:{campaign_id}"
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
    episode_part_index = 0
    daily_part_index = 0
    duplicate_count = 0
    unique_daily_path_count = 0
    input_record_count = 0
    input_sequence_hasher = hashlib.sha256()
    accumulator = _StreamingCompactAccumulator(
        campaign_id=campaign_id,
        replays=causal_replays,
    )

    def flush_episodes() -> None:
        nonlocal episode_part_index
        if not episode_buffer:
            return
        writer.append_records(
            "episodes",
            episode_buffer,
            batch_id=(
                f"causal-salvage-streaming:episodes:{episode_part_index:08d}"
            ),
        )
        episode_part_index += 1
        episode_buffer.clear()

    def flush_daily_paths() -> None:
        nonlocal daily_part_index
        if not daily_buffer:
            return
        writer.append_records(
            "account_daily_paths",
            daily_buffer,
            batch_id=(
                "causal-salvage-streaming:account_daily_paths:"
                f"{daily_part_index:08d}"
            ),
        )
        daily_part_index += 1
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
            for index in range(0, len(rows), 5_000):
                writer.append_records(
                    dataset,
                    rows[index : index + 5_000],
                    batch_id=(
                        "causal-salvage-streaming:"
                        f"{dataset}:{index // 5_000:08d}"
                    ),
                )

        for record in _iter_evaluated_records(evaluated_policy_record_chunks):
            episodes, paths = _episode_ledgers(
                campaign_id=campaign_id,
                records=[record],
                policy_ids=policy_ids,
                component_ids=replay_ids,
            )
            if len(episodes) != 1 or not paths:
                raise CausalSalvageEvidenceError(
                    "one streaming input must materialize one non-empty episode"
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
            input_record_count += 1
            input_sequence_hasher.update(
                json.dumps(
                    [*key, digest],
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
                + b"\n"
            )
            prior = seen_hashes.get(key)
            if prior is not None:
                if prior != digest:
                    raise CausalSalvageEvidenceError(
                        "duplicate causal episode key has divergent evidence: "
                        + repr(key)
                    )
                duplicate_count += 1
                continue
            seen_hashes[key] = digest
            base_key = key[:3]
            observed_base_keys.add(base_key)
            scenario_coverage[base_key].add(key[3])
            accumulator.add(episode)
            episode_buffer.append(episode)
            daily_buffer.extend(paths)
            unique_daily_path_count += len(paths)
            if len(episode_buffer) >= episode_batch_size:
                flush_episodes()
            if len(daily_buffer) >= daily_path_batch_size:
                flush_daily_paths()

        flush_episodes()
        flush_daily_paths()
        _validate_streaming_coverage(
            checked_identity,
            observed_base_keys=observed_base_keys,
            scenario_coverage=scenario_coverage,
        )
        expected_counts = {
            **{name: len(rows) for name, rows in small_datasets.items()},
            "episodes": len(seen_hashes),
            "account_daily_paths": unique_daily_path_count,
        }
        observed_counts = writer.dataset_row_counts
        count_drift = {
            name: {
                "expected": expected,
                "observed": observed_counts.get(name, 0),
            }
            for name, expected in expected_counts.items()
            if observed_counts.get(name, 0) != expected
        }
        if count_drift:
            raise CausalSalvageEvidenceError(
                "streaming EvidenceBundle staging row counts disagree with the "
                "replayed logical input: " + json.dumps(count_drift, sort_keys=True)
            )
        context = {
            **dict(compact_context or {}),
            "streaming_adapter": True,
            "input_episode_record_count": input_record_count,
            "input_episode_sequence_sha256": input_sequence_hasher.hexdigest(),
            "unique_episode_record_count": len(seen_hashes),
            "duplicate_episode_record_count": duplicate_count,
            "duplicate_policy": "IDENTICAL_HASH_ONLY",
        }
        compact = accumulator.outputs(context=context)
        for name, value in compact.items():
            writer.write_compact_output(name, value)
        writer.checkpoint(
            {
                "stage": "CAUSAL_SALVAGE_STREAMING_EVIDENCE_COMPLETE",
                "signal_count": len(component_rows["component_signals"]),
                "trade_count": len(component_rows["component_trades"]),
                "unique_episode_record_count": len(seen_hashes),
                "duplicate_episode_record_count": duplicate_count,
                "input_episode_record_count": input_record_count,
                "input_episode_sequence_sha256": input_sequence_hasher.hexdigest(),
                "episode_partition_count": episode_part_index,
                "daily_path_partition_count": daily_part_index,
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
        raise CausalSalvageEvidenceError("sealed causal evidence status drift")
    guard_campaign_completion(
        "COMPLETE", receipt.bundle_path, campaign_id=campaign_id
    )
    return receipt


def _component_ledgers(
    *,
    campaign_id: str,
    replays: Mapping[str, CausalSleeveReplay],
    specs: Mapping[str, Any],
    expected_fingerprints: Mapping[str, str],
) -> dict[str, list[dict[str, Any]]]:
    signals: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for component_id in sorted(replays):
        replay = replays[component_id]
        spec = specs[component_id]
        if str(_field(replay, "sleeve_id")) != component_id:
            raise CausalSalvageEvidenceError("causal replay component identity drift")
        specification_hash = str(_field(replay, "specification_hash"))
        if specification_hash != str(expected_fingerprints[component_id]):
            raise CausalSalvageEvidenceError(
                f"causal specification fingerprint drift: {component_id}"
            )
        spec_id = str(_field(spec, "sleeve_id"))
        if spec_id != component_id:
            raise CausalSalvageEvidenceError("sleeve specification identity drift")
        market = str(_field(spec, "market"))
        contract = str(_field(spec, "execution_market"))
        timeframe = str(_field(spec, "timeframe"))
        role = _enum_value(_field(spec, "role"))
        side = int(_field(spec, "side"))
        holding_bars = int(_field(spec, "holding_bars"))
        replay_signals = tuple(_field(replay, "signals"))
        if int(_field(replay, "signal_count")) != len(replay_signals):
            raise CausalSalvageEvidenceError(
                f"causal signal count drift: {component_id}"
            )
        completed = [
            row
            for row in replay_signals
            if str(_field(row, "outcome_status")) == TARGET_OBSERVED
        ]
        censored = [
            row
            for row in replay_signals
            if str(_field(row, "outcome_status")) == CENSORED_FUTURE_COVERAGE
        ]
        unknown = set(
            str(_field(row, "outcome_status")) for row in replay_signals
        ) - {TARGET_OBSERVED, CENSORED_FUTURE_COVERAGE}
        if unknown:
            raise CausalSalvageEvidenceError(
                f"unknown causal signal outcome status: {sorted(unknown)}"
            )
        normal_events = tuple(_field(replay, "normal_events"))
        stressed_events = tuple(_field(replay, "stressed_events"))
        if len(completed) != len(normal_events):
            raise CausalSalvageEvidenceError(
                f"completed signal/trade count drift: {component_id}"
            )
        if len(completed) != len(stressed_events):
            raise CausalSalvageEvidenceError(
                f"completed signal/stressed-trade count drift: {component_id}"
            )
        if int(_field(replay, "completed_trade_count")) != len(completed):
            raise CausalSalvageEvidenceError(
                f"declared completed-trade count drift: {component_id}"
            )
        if not completed:
            raise CausalSalvageEvidenceError(
                "EvidenceBundle V1 requires at least one completed causal trade "
                f"for every component: {component_id}"
            )
        if int(_field(replay, "censored_signal_count")) != len(censored):
            raise CausalSalvageEvidenceError(
                f"censored signal count drift: {component_id}"
            )
        event_by_signal: dict[str, Any] = {}
        for event in normal_events:
            event_id = str(_field(event, "event_id"))
            if not event_id.endswith(":NORMAL"):
                raise CausalSalvageEvidenceError("normal causal event ID drift")
            signal_id = event_id.removesuffix(":NORMAL")
            if signal_id in event_by_signal:
                raise CausalSalvageEvidenceError("duplicate causal normal event")
            event_by_signal[signal_id] = event
        stressed_event_by_signal: dict[str, Any] = {}
        for event in stressed_events:
            event_id = str(_field(event, "event_id"))
            if not event_id.endswith(":STRESSED_1_5X"):
                raise CausalSalvageEvidenceError("stressed causal event ID drift")
            signal_id = event_id.removesuffix(":STRESSED_1_5X")
            if signal_id in stressed_event_by_signal:
                raise CausalSalvageEvidenceError("duplicate causal stressed event")
            stressed_event_by_signal[signal_id] = event
        observed_signal_ids: set[str] = set()
        replay_fill_policy_hash = str(_field(replay, "fill_policy_hash"))
        for signal in replay_signals:
            signal_id = str(_field(signal, "signal_id"))
            if not signal_id or signal_id in observed_signal_ids:
                raise CausalSalvageEvidenceError(
                    f"empty or duplicate causal signal ID: {component_id}"
                )
            observed_signal_ids.add(signal_id)
            if str(_field(signal, "sleeve_id")) != component_id:
                raise CausalSalvageEvidenceError(
                    f"causal signal sleeve identity drift: {component_id}"
                )
            if str(_field(signal, "fill_policy_hash")) != replay_fill_policy_hash:
                raise CausalSalvageEvidenceError(
                    f"causal signal fill-policy hash drift: {component_id}"
                )
            status = str(_field(signal, "outcome_status"))
            signal_time = _ns_iso(int(_field(signal, "signal_time_ns")))
            extension = _signal_time_extension(signal)
            signal_row = {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "signal_id": signal_id,
                "event_time": signal_time,
                "market": market,
                "contract": contract,
                "timeframe": timeframe,
                "signal": {
                    "direction": int(_field(signal, "direction")),
                    "trigger_value": float(_field(signal, "trigger_value")),
                    "context_value": _optional_float(_field(signal, "context_value")),
                    "causal_decision_kernel": str(_field(signal, "kernel_version")),
                },
                "sizing": float(_field(signal, "quantity")),
                "stop": None,
                "target": None,
                "veto": False,
                "component_role": role,
                "outcome_status": status,
                "censor_reason": _field(signal, "censor_reason"),
                "trade_materialized": status == TARGET_OBSERVED,
                "segment_code": int(_field(signal, "segment_code")),
                "contract_code": int(_field(signal, "contract_code")),
                "fill_policy_id": str(_field(signal, "fill_policy_id")),
                "fill_policy_hash": str(_field(signal, "fill_policy_hash")),
                "causal_event_fingerprint": str(_field(signal, "fingerprint")),
                "raw_entry_open": _optional_float(_field(signal, "raw_entry_open")),
                "normal_entry_fill_price": _optional_float(
                    _field(signal, "normal_entry_fill_price")
                ),
                "stressed_entry_fill_price": _optional_float(
                    _field(signal, "stressed_entry_fill_price")
                ),
                "raw_exit_open": _optional_float(_field(signal, "raw_exit_open")),
                "normal_exit_fill_price": _optional_float(
                    _field(signal, "normal_exit_fill_price")
                ),
                "stressed_exit_fill_price": _optional_float(
                    _field(signal, "stressed_exit_fill_price")
                ),
                **extension,
            }
            signals.append(signal_row)
            if status == CENSORED_FUTURE_COVERAGE:
                if signal_id in event_by_signal:
                    raise CausalSalvageEvidenceError(
                        "censored signal incorrectly owns a completed trade"
                    )
                if signal_id in stressed_event_by_signal:
                    raise CausalSalvageEvidenceError(
                        "censored signal incorrectly owns a stressed completed trade"
                    )
                censored_fill_ns = _field(signal, "fill_time_ns")
                if _field(signal, "exit_fill_time_ns") is not None:
                    raise CausalSalvageEvidenceError(
                        "censored causal signal may not contain a completed exit fill"
                    )
                if any(
                    _field(signal, name) is not None
                    for name in (
                        "raw_exit_open",
                        "normal_exit_fill_price",
                        "stressed_exit_fill_price",
                    )
                ):
                    raise CausalSalvageEvidenceError(
                        "censored causal signal contains exit-price evidence"
                    )
                if censored_fill_ns is not None:
                    quantity = int(_field(signal, "quantity"))
                    if quantity <= 0:
                        raise CausalSalvageEvidenceError(
                            "filled censored causal entry quantity is non-positive"
                        )
                    entries.append(
                        {
                            "campaign_id": campaign_id,
                            "component_id": component_id,
                            "trade_id": signal_id,
                            "entry_time": _ns_iso(int(censored_fill_ns)),
                            "market": market,
                            "contract": contract,
                            "side": "LONG" if side > 0 else "SHORT",
                            "quantity": quantity,
                            "entry_price": _required_float(
                                signal, "normal_entry_fill_price"
                            ),
                            "sizing": float(quantity),
                            "stop_price": None,
                            "target_price": None,
                            "source_signal_id": signal_id,
                            "outcome_status": CENSORED_FUTURE_COVERAGE,
                            "censor_reason": _field(signal, "censor_reason"),
                            "trade_materialized": False,
                            "open_position_unresolved": True,
                            **extension,
                        }
                    )
                elif any(
                    _field(signal, name) is not None
                    for name in (
                        "raw_entry_open",
                        "normal_entry_fill_price",
                        "stressed_entry_fill_price",
                    )
                ):
                    raise CausalSalvageEvidenceError(
                        "pre-fill causal censor contains entry-price evidence"
                    )
                continue
            event = event_by_signal.pop(signal_id, None)
            if event is None:
                raise CausalSalvageEvidenceError(
                    "observed causal signal lacks its normal trade event"
                )
            stressed_event = stressed_event_by_signal.pop(signal_id, None)
            if stressed_event is None:
                raise CausalSalvageEvidenceError(
                    "observed causal signal lacks its stressed trade event"
                )
            entry_ns = _required_time(signal, "fill_time_ns")
            exit_ns = _required_time(signal, "exit_fill_time_ns")
            entry_price = _required_float(signal, "normal_entry_fill_price")
            exit_price = _required_float(signal, "normal_exit_fill_price")
            quantity = int(_field(signal, "quantity"))
            if quantity <= 0:
                raise CausalSalvageEvidenceError("causal trade quantity is non-positive")
            gross = float(_field(event, "gross_pnl"))
            net = float(_field(event, "net_pnl"))
            costs = gross - net
            if costs < -1e-9:
                raise CausalSalvageEvidenceError("causal trade costs are negative")
            costs = max(costs, 0.0)
            stressed_gross = float(_field(stressed_event, "gross_pnl"))
            stressed_net = float(_field(stressed_event, "net_pnl"))
            stressed_costs = stressed_gross - stressed_net
            if stressed_costs < -1e-9:
                raise CausalSalvageEvidenceError("stressed causal trade costs are negative")
            stressed_costs = max(stressed_costs, 0.0)
            side_name = "LONG" if side > 0 else "SHORT"
            common = {
                "campaign_id": campaign_id,
                "component_id": component_id,
                "trade_id": signal_id,
            }
            entries.append(
                {
                    **common,
                    "entry_time": _ns_iso(entry_ns),
                    "market": market,
                    "contract": contract,
                    "side": side_name,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "sizing": float(quantity),
                    "stop_price": None,
                    "target_price": None,
                    "source_signal_id": signal_id,
                    **extension,
                }
            )
            exits.append(
                {
                    **common,
                    "exit_time": _ns_iso(exit_ns),
                    "exit_price": exit_price,
                    "exit_reason": f"CAUSAL_FROZEN_TIME_EXIT_{holding_bars}",
                    "exit_decision_time": extension["exit_decision_time"],
                    "exit_order_submit_time": extension["exit_order_submit_time"],
                    "exit_earliest_executable_time": extension[
                        "exit_earliest_executable_time"
                    ],
                    "exit_fill_time": extension["exit_fill_time"],
                }
            )
            trades.append(
                {
                    **common,
                    "entry_time": _ns_iso(entry_ns),
                    "exit_time": _ns_iso(exit_ns),
                    "market": market,
                    "contract": contract,
                    "side": side_name,
                    "quantity": quantity,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "gross_pnl": gross,
                    "costs": costs,
                    "net_pnl": net,
                    "source_normal_event_id": str(_field(event, "event_id")),
                    "source_stressed_event_id": str(
                        _field(stressed_event, "event_id")
                    ),
                    "raw_entry_open": _required_float(signal, "raw_entry_open"),
                    "raw_exit_open": _required_float(signal, "raw_exit_open"),
                    "stressed_entry_fill_price": _required_float(
                        signal, "stressed_entry_fill_price"
                    ),
                    "stressed_exit_fill_price": _required_float(
                        signal, "stressed_exit_fill_price"
                    ),
                    "stressed_gross_pnl": stressed_gross,
                    "stressed_costs": stressed_costs,
                    "stressed_net_pnl": stressed_net,
                    **extension,
                }
            )
        if event_by_signal:
            raise CausalSalvageEvidenceError("unclaimed causal normal events remain")
        if stressed_event_by_signal:
            raise CausalSalvageEvidenceError("unclaimed causal stressed events remain")
    return {
        "component_signals": signals,
        "component_entries": entries,
        "component_exits": exits,
        "component_trades": trades,
    }


def _membership_rows(
    *,
    campaign_id: str,
    policies: Mapping[str, Any],
    specs: Mapping[str, Any],
    expected_fingerprints: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    represented: set[str] = set()
    for policy_id in sorted(policies):
        policy = policies[policy_id]
        if str(_field(policy, "policy_id")) != policy_id:
            raise CausalSalvageEvidenceError("policy identity drift")
        actual_fingerprint = str(_field(policy, "structural_fingerprint"))
        if actual_fingerprint != str(expected_fingerprints[policy_id]):
            raise CausalSalvageEvidenceError(
                f"causal policy fingerprint drift: {policy_id}"
            )
        components = tuple(
            str(value)
            for value in _field_any(policy, "component_ids", "component_priority")
        )
        if not components or len(set(components)) != len(components):
            raise CausalSalvageEvidenceError("policy membership is empty or duplicated")
        if not set(components) <= set(specs):
            raise CausalSalvageEvidenceError("policy references an unknown causal sleeve")
        risk = float(_field(policy, "static_risk_tier"))
        policy_value = _to_mapping(policy)
        for component_id in components:
            represented.add(component_id)
            rows.append(
                {
                    "campaign_id": campaign_id,
                    "policy_id": policy_id,
                    "component_id": component_id,
                    "risk_allocation": risk,
                    "component_role": _enum_value(_field(specs[component_id], "role")),
                    "causal_policy_specification": policy_value,
                    "future_outcome_fields_used": False,
                    "inactive_sleeve_reserves_risk": False,
                }
            )
    if represented != set(specs):
        raise CausalSalvageEvidenceError(
            "every causal component must occur in at least one policy membership"
        )
    return rows


def _episode_ledgers(
    *,
    campaign_id: str,
    records: Sequence[Mapping[str, Any]],
    policy_ids: set[str],
    component_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    episodes: list[dict[str, Any]] = []
    paths: list[dict[str, Any]] = []
    observed: set[tuple[str, str, str, str]] = set()
    for record in records:
        if not isinstance(record, Mapping):
            raise CausalSalvageEvidenceError("evaluated policy records must be objects")
        policy_id = str(record.get("policy_id") or "")
        if policy_id not in policy_ids:
            raise CausalSalvageEvidenceError("episode references an unknown policy")
        scenario = str(record.get("scenario") or record.get("cost_scenario") or "")
        if scenario not in COST_SCENARIOS:
            raise CausalSalvageEvidenceError("causal episode scenario drift")
        horizon = str(record.get("horizon") or "")
        temporal_block = str(record.get("temporal_block") or "")
        if not horizon or not temporal_block:
            raise CausalSalvageEvidenceError(
                "causal episode requires horizon and temporal block"
            )
        raw_episode = record.get("episode", record)
        episode = _episode_mapping(raw_episode)
        if str(episode.get("policy_id") or "") != policy_id:
            raise CausalSalvageEvidenceError("evaluated episode policy identity drift")
        start_day = int(episode["start_day"])
        episode_id = str(record.get("episode_id") or f"{policy_id}:{start_day}")
        key = (policy_id, episode_id, horizon, scenario)
        if key in observed:
            raise CausalSalvageEvidenceError("duplicate causal episode evidence")
        observed.add(key)
        terminal = _terminal_state(
            episode,
            horizon=horizon,
            requested_duration=record.get("requested_duration_trading_days"),
        )
        duration = int(episode["eligible_days"])
        daily = list(episode.get("daily_path") or ())
        if duration <= 0 or len(daily) != duration:
            raise CausalSalvageEvidenceError(
                "causal episode daily path does not match eligible-day duration"
            )
        explicit_future_censor = (
            terminal == "DATA_CENSORED"
            and str(episode.get("terminal_reason") or "")
            == CENSORED_FUTURE_COVERAGE
        )
        source_consistency_ok = bool(episode["consistency_ok"])
        source_best_day_concentration = float(
            episode.get("best_day_concentration", 0.0)
        )
        # Phase-A raw parts were durably written before the bounded terminal
        # summary defect was found.  Their daily account path is complete and
        # authoritative; only the duplicate episode-level consistency fields
        # were computed from a stale best-day accumulator.  Normalize this one
        # explicit censor representation without accepting any non-censored
        # path mismatch.
        terminal_consistency_ok = (
            bool(daily[-1]["consistency_ok"])
            if explicit_future_censor
            else source_consistency_ok
        )
        terminal_best_day_concentration = (
            float(daily[-1]["consistency"])
            if explicit_future_censor
            else source_best_day_concentration
        )
        total_cost = float(episode["total_cost"])
        net_pnl = float(episode["net_pnl"])
        contribution = {
            str(name): float(value)
            for name, value in (episode.get("component_contribution") or {}).items()
        }
        if not set(contribution) <= component_ids:
            raise CausalSalvageEvidenceError(
                "causal episode contribution references an unknown component"
            )
        failure = list(record.get("failure_vector") or _failure_vector(terminal, episode))
        episode_row = {
            "campaign_id": campaign_id,
            "policy_id": policy_id,
            "episode_id": episode_id,
            "episode_start": _day_iso(start_day),
            "horizon": horizon,
            "temporal_block": temporal_block,
            "duration_trading_days": duration,
            "target_reached": terminal == "TARGET_REACHED",
            "mll_breached": terminal == "MLL_BREACHED",
            "censored_state": terminal
            in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"},
            "cost_scenario": scenario,
            "costs": total_cost,
            "net_pnl": net_pnl,
            "target_progress": float(episode["target_progress"]),
            "minimum_mll_buffer": float(episode["minimum_mll_buffer"]),
            "consistency_ok": terminal_consistency_ok,
            "days_to_target": (
                None
                if episode.get("days_to_target") is None
                else float(episode["days_to_target"])
            ),
            "failure_vector": failure,
            "terminal_state": terminal,
            "component_contribution": contribution,
            "terminal_reason": str(episode.get("terminal_reason") or ""),
            "causal_account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
            "accepted_events": int(episode.get("accepted_events", 0)),
            "skipped_events": int(episode.get("skipped_events", 0)),
            "traded_days": int(episode.get("traded_days", 0)),
            "conflict_count": int(episode.get("conflict_count", 0)),
            "maximum_target_progress": float(
                episode.get("maximum_target_progress", episode["target_progress"])
            ),
            "best_day_concentration": terminal_best_day_concentration,
            "source_episode_consistency_ok": source_consistency_ok,
            "source_episode_best_day_concentration": (
                source_best_day_concentration
            ),
            "consistency_representation_source": (
                "TERMINAL_REALIZED_ACCOUNT_PATH"
                if explicit_future_censor
                else "ACCOUNT_POLICY_EPISODE"
            ),
            "maximum_mini_equivalent": float(
                episode.get("maximum_mini_equivalent", 0.0)
            ),
            "maximum_net_directional_exposure": float(
                episode.get("maximum_net_directional_exposure", 0.0)
            ),
            "shared_loss_days": int(episode.get("shared_loss_days", 0)),
            "skipped_reasons": dict(episode.get("skipped_reasons") or {}),
            "episode_end_day": int(episode.get("end_day", start_day + duration - 1)),
        }
        episodes.append(episode_row)
        allocation_by_day: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for decision in episode.get("risk_allocation_path") or ():
            allocation_by_day[int(decision["session_day"])].append(dict(decision))
        for raw_day in daily:
            if not isinstance(raw_day, Mapping):
                raise CausalSalvageEvidenceError("causal daily path row is not an object")
            missing = sorted(_REQUIRED_DAILY_FIELDS - set(raw_day))
            if missing:
                raise CausalSalvageEvidenceError(
                    "causal daily path lacks exact enriched fields: "
                    + ", ".join(missing)
                )
            day = int(raw_day["session_day"])
            attribution = {
                str(name): float(value)
                for name, value in raw_day["component_attribution"].items()
            }
            if not set(attribution) <= component_ids:
                raise CausalSalvageEvidenceError(
                    "causal daily attribution references an unknown component"
                )
            path_row = {
                "campaign_id": campaign_id,
                "policy_id": policy_id,
                "episode_id": episode_id,
                "trading_day": _day_date(day),
                "cost_scenario": scenario,
                "horizon": horizon,
                "realized_pnl": float(raw_day["realized_pnl"]),
                "unrealized_pnl": float(raw_day["unrealized_pnl"]),
                "daily_pnl": float(raw_day["day_pnl"]),
                "equity": float(raw_day["balance"]),
                "mll": float(raw_day["mll_floor"]),
                "mll_buffer": float(raw_day["mll_buffer"]),
                "minimum_mll_buffer": float(raw_day["minimum_mll_buffer"]),
                "consistency": float(raw_day["consistency"]),
                "consistency_ok": bool(raw_day["consistency_ok"]),
                "target_progress": float(raw_day["target_progress"]),
                "costs": float(raw_day["costs"]),
                "conflicts": _json_object_or_list(raw_day["conflicts"], "conflicts"),
                "exposure": _numeric_mapping(raw_day["exposure"], "exposure"),
                "component_attribution": attribution,
                "open_positions": _nonnegative_int(
                    raw_day["open_positions"], "open_positions"
                ),
                "routing_decisions": sorted(
                    allocation_by_day.get(day, ()),
                    key=lambda value: (
                        int(value.get("decision_ns", 0)),
                        str(value.get("component_id", "")),
                        str(value.get("event_id", "")),
                    ),
                ),
                "causal_account_replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
            }
            paths.append(path_row)
        _reconcile_episode_path(episode_row, [row for row in paths if (
            row["policy_id"], row["episode_id"], row["horizon"], row["cost_scenario"]
        ) == key])
    return episodes, paths


def _provenance_row(
    *,
    campaign_id: str,
    identity: Mapping[str, Any],
    replays: Mapping[str, CausalSleeveReplay],
    policies: Mapping[str, Any],
    value: Mapping[str, Any],
) -> dict[str, Any]:
    access_sha = str(value.get("access_ledger_sha256") or "")
    recorded = str(value.get("recorded_at_utc") or "")
    market_role = str(value.get("market_data_role") or "")
    if len(access_sha) != 64 or not recorded or not market_role:
        raise CausalSalvageEvidenceError(
            "causal provenance requires access hash, timestamp, and market-data role"
        )
    checksums = {
        "configuration": str(identity["configuration_sha256"]),
        **{
            f"data:{name}": str(digest)
            for name, digest in identity["data_fingerprints"].items()
        },
        **{
            f"causal_replay:{component_id}:decision": str(
                _field(replay, "decision_hash")
            )
            for component_id, replay in sorted(replays.items())
        },
        **{
            f"causal_replay:{component_id}:normal_events": str(
                _field(replay, "normal_event_hash")
            )
            for component_id, replay in sorted(replays.items())
        },
        **{
            f"causal_replay:{component_id}:stressed_events": str(
                _field(replay, "stressed_event_hash")
            )
            for component_id, replay in sorted(replays.items())
        },
        **{
            f"policy:{policy_id}": str(_field(policy, "structural_fingerprint"))
            for policy_id, policy in sorted(policies.items())
        },
    }
    supplied = value.get("immutable_checksums") or {}
    if not isinstance(supplied, Mapping):
        raise CausalSalvageEvidenceError("provenance immutable_checksums must be a map")
    for name, digest in supplied.items():
        if name in checksums and checksums[name] != str(digest):
            raise CausalSalvageEvidenceError(
                f"supplied provenance checksum conflicts with causal identity: {name}"
            )
        checksums[str(name)] = str(digest)
    return {
        "campaign_id": campaign_id,
        "validator_version": "hydra_evidence_bundle_validator_v1",
        "replay_version": CAUSAL_ACCOUNT_REPLAY_VERSION,
        "market_data_role": market_role,
        "access_ledger_sha256": access_sha,
        "reconstruction_flag": False,
        "immutable_checksums": checksums,
        "recorded_at_utc": recorded,
        "adapter_version": CAUSAL_SALVAGE_ADAPTER_VERSION,
        "decision_kernel_version": CAUSAL_DECISION_KERNEL_VERSION,
        "causal_salvage_only": True,
        "q4_access_count": 0,
        "broker_connections": 0,
        "orders": 0,
    }


@dataclass(slots=True)
class _StreamingScenarioStats:
    count: int = 0
    pass_count: int = 0
    mll_breach_count: int = 0
    consistency_count: int = 0
    minimum_mll_buffer: float = math.inf
    net_pnl: list[float] | None = None
    target_progress: list[float] | None = None

    def __post_init__(self) -> None:
        if self.net_pnl is None:
            self.net_pnl = []
        if self.target_progress is None:
            self.target_progress = []

    def add(self, row: Mapping[str, Any]) -> None:
        self.count += 1
        self.pass_count += int(row["terminal_state"] == "TARGET_REACHED")
        self.mll_breach_count += int(row["terminal_state"] == "MLL_BREACHED")
        self.consistency_count += int(bool(row["consistency_ok"]))
        self.minimum_mll_buffer = min(
            self.minimum_mll_buffer,
            float(row["minimum_mll_buffer"]),
        )
        assert self.net_pnl is not None
        assert self.target_progress is not None
        self.net_pnl.append(float(row["net_pnl"]))
        self.target_progress.append(float(row["target_progress"]))

    def summary(self, *, include_mll_count: bool) -> dict[str, Any]:
        if self.count <= 0:
            raise CausalSalvageEvidenceError("empty streaming scenario statistics")
        assert self.net_pnl is not None
        assert self.target_progress is not None
        output = {
            "episode_count": self.count,
            "pass_count": self.pass_count,
            "net_pnl_total": math.fsum(self.net_pnl),
            "minimum_mll_buffer": self.minimum_mll_buffer,
        }
        if include_mll_count:
            output.update(
                {
                    "mll_breach_count": self.mll_breach_count,
                    "median_target_progress": statistics.median(
                        self.target_progress
                    ),
                }
            )
        else:
            output.update(
                {
                    "target_progress_median": statistics.median(
                        self.target_progress
                    ),
                    "consistency_rate": self.consistency_count / self.count,
                }
            )
        return output


class _StreamingCompactAccumulator:
    def __init__(
        self,
        *,
        campaign_id: str,
        replays: Mapping[str, CausalSleeveReplay],
    ) -> None:
        self.campaign_id = campaign_id
        self.replays = replays
        self.episode_count = 0
        self.by_scenario: dict[str, _StreamingScenarioStats] = {}
        self.by_policy: dict[str, dict[str, _StreamingScenarioStats]] = {}
        self.failure_by_policy: dict[str, Counter[str]] = defaultdict(Counter)

    def add(self, row: Mapping[str, Any]) -> None:
        scenario = str(row["cost_scenario"])
        if scenario not in COST_SCENARIOS:
            raise CausalSalvageEvidenceError(
                "streaming compact accumulator scenario drift"
            )
        policy_id = str(row["policy_id"])
        self.episode_count += 1
        self.by_scenario.setdefault(scenario, _StreamingScenarioStats()).add(row)
        self.by_policy.setdefault(policy_id, {}).setdefault(
            scenario, _StreamingScenarioStats()
        ).add(row)
        self.failure_by_policy[policy_id].update(
            str(value) for value in row["failure_vector"]
        )

    def outputs(self, *, context: Mapping[str, Any]) -> dict[str, Any]:
        if self.episode_count <= 0 or not self.by_policy:
            raise CausalSalvageEvidenceError(
                "causal salvage cannot seal without episode evidence"
            )
        if set(self.by_scenario) != set(COST_SCENARIOS):
            raise CausalSalvageEvidenceError(
                "streaming campaign lacks paired cost-scenario evidence"
            )
        summary = {
            "schema": "hydra_causal_salvage_campaign_summary_v1",
            "campaign_id": self.campaign_id,
            "component_count": len(self.replays),
            "policy_count": len(self.by_policy),
            "signal_count": sum(
                int(_field(row, "signal_count")) for row in self.replays.values()
            ),
            "completed_trade_count": sum(
                int(_field(row, "completed_trade_count"))
                for row in self.replays.values()
            ),
            "censored_signal_count": sum(
                int(_field(row, "censored_signal_count"))
                for row in self.replays.values()
            ),
            "episode_count": self.episode_count,
            "scenario_results": {
                scenario: stats.summary(include_mll_count=True)
                for scenario, stats in sorted(self.by_scenario.items())
            },
            "fresh_development_evidence": True,
            "independently_confirmed": False,
            "reconstruction_flag": False,
            "context": dict(context),
        }
        frontier = []
        for policy_id, scenarios_by_name in sorted(self.by_policy.items()):
            missing = set(COST_SCENARIOS) - set(scenarios_by_name)
            if missing:
                raise CausalSalvageEvidenceError(
                    f"policy {policy_id} lacks paired scenario evidence: "
                    + ", ".join(sorted(missing))
                )
            frontier.append(
                {
                    "policy_id": policy_id,
                    "scenarios": {
                        scenario: scenarios_by_name[scenario].summary(
                            include_mll_count=False
                        )
                        for scenario in COST_SCENARIOS
                    },
                }
            )
        return {
            "campaign_summary": summary,
            "failure_vectors": {
                "schema": "hydra_causal_salvage_failure_vectors_v1",
                "campaign_id": self.campaign_id,
                "by_policy": {
                    policy_id: dict(sorted(counter.items()))
                    for policy_id, counter in sorted(
                        self.failure_by_policy.items()
                    )
                },
            },
            "pareto_archive": {
                "schema": "hydra_causal_salvage_pareto_archive_v1",
                "campaign_id": self.campaign_id,
                "frontier": frontier,
                "opaque_score_used": False,
            },
            "next_campaign_recommendations": {
                "schema": "hydra_causal_salvage_next_action_v1",
                "campaign_id": self.campaign_id,
                "action": "APPLY_FROZEN_CAUSAL_SALVAGE_GATE",
                "mutation_authorized": False,
                "q4_access_authorized": False,
                "data_purchase_authorized": False,
            },
        }


def _compact_outputs(
    *,
    campaign_id: str,
    replays: Mapping[str, CausalSleeveReplay],
    episodes: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    accumulator = _StreamingCompactAccumulator(
        campaign_id=campaign_id,
        replays=replays,
    )
    for row in episodes:
        accumulator.add(row)
    return accumulator.outputs(context=context)


def _iter_evaluated_records(
    values: Iterable[Mapping[str, Any] | Iterable[Mapping[str, Any]]],
) -> Iterable[Mapping[str, Any]]:
    for value in values:
        if isinstance(value, Mapping):
            yield value
            continue
        if isinstance(value, (str, bytes)):
            raise CausalSalvageEvidenceError(
                "evaluated-policy chunks may not be strings"
            )
        try:
            chunk = iter(value)
        except TypeError as exc:
            raise CausalSalvageEvidenceError(
                "evaluated-policy input must contain records or record iterables"
            ) from exc
        for record in chunk:
            if not isinstance(record, Mapping):
                raise CausalSalvageEvidenceError(
                    "evaluated-policy chunks must contain objects"
                )
            yield record


def _validate_streaming_coverage(
    identity: Mapping[str, Any],
    *,
    observed_base_keys: set[tuple[str, str, str]],
    scenario_coverage: Mapping[tuple[str, str, str], set[str]],
) -> None:
    expected = {
        (str(row["policy_id"]), str(row["episode_id"]), str(row["horizon"]))
        for row in identity["expected_coverage"]["required_episode_keys"]
    }
    missing = expected - observed_base_keys
    if missing:
        raise CausalSalvageEvidenceError(
            f"streaming causal evidence misses {len(missing)} required base keys"
        )
    if not bool(
        identity["expected_coverage"]["allow_additional_episode_keys"]
    ):
        unexpected = observed_base_keys - expected
        if unexpected:
            raise CausalSalvageEvidenceError(
                f"streaming causal evidence has {len(unexpected)} undeclared base keys"
            )
    expected_scenarios = set(COST_SCENARIOS)
    incomplete = {
        key: sorted(expected_scenarios - set(scenarios))
        for key, scenarios in scenario_coverage.items()
        if set(scenarios) != expected_scenarios
    }
    if incomplete:
        first_key = sorted(incomplete)[0]
        raise CausalSalvageEvidenceError(
            "streaming causal evidence lacks paired scenarios for "
            f"{first_key}: {', '.join(incomplete[first_key])}"
        )
    covered_policies = {key[0] for key in observed_base_keys}
    missing_policies = set(identity["policy_fingerprints"]) - covered_policies
    if missing_policies:
        raise CausalSalvageEvidenceError(
            "streaming causal evidence lacks policies: "
            + ", ".join(sorted(missing_policies)[:10])
        )


def _canonical_hash(value: Any) -> str:
    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CausalSalvageEvidenceError(
            "causal streaming evidence is not canonical JSON"
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def _terminal_state(
    episode: Mapping[str, Any], *, horizon: str, requested_duration: Any
) -> str:
    raw = _enum_value(episode["terminal"])
    if raw in {"PASSED", "TARGET_REACHED"}:
        return "TARGET_REACHED"
    if raw in {"MLL_BREACH", "MLL_BREACHED"}:
        return "MLL_BREACHED"
    if raw in {"COMPLIANCE_FAILURE", "HARD_RULE_FAILURE"}:
        return "HARD_RULE_FAILURE"
    if raw not in {"TIMEOUT", "DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}:
        raise CausalSalvageEvidenceError(f"unknown causal terminal state: {raw}")
    if raw in {"DATA_CENSORED", "OPERATIONAL_HORIZON_NOT_REACHED"}:
        return raw
    # The causal replay can discover missing required future coverage on the
    # nominal last day of a fixed horizon.  Duration equality does not turn an
    # explicit data censor into an operational timeout.
    if str(episode.get("terminal_reason") or "") == CENSORED_FUTURE_COVERAGE:
        return "DATA_CENSORED"
    if "FULL" in horizon:
        return "DATA_CENSORED"
    duration = (
        int(requested_duration)
        if requested_duration is not None
        else _horizon_days(horizon)
    )
    return (
        "DATA_CENSORED"
        if duration is not None and int(episode["eligible_days"]) < duration
        else "OPERATIONAL_HORIZON_NOT_REACHED"
    )


def _failure_vector(terminal: str, episode: Mapping[str, Any]) -> list[str]:
    if terminal == "TARGET_REACHED":
        return []
    if terminal == "MLL_BREACHED":
        return ["MLL_BREACH"]
    if terminal == "HARD_RULE_FAILURE":
        return ["HARD_RULE_FAILURE", str(episode.get("terminal_reason") or "UNKNOWN")]
    if float(episode.get("target_progress", 0.0)) <= 0.0:
        return ["INSUFFICIENT_OPPORTUNITIES"]
    return ["TARGET_TOO_SLOW"]


def _reconcile_episode_path(
    episode: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> None:
    if len(rows) != int(episode["duration_trading_days"]):
        raise CausalSalvageEvidenceError("causal daily path length drift")
    if not _close(math.fsum(float(row["daily_pnl"]) for row in rows), episode["net_pnl"]):
        raise CausalSalvageEvidenceError("causal daily PnL does not reconcile")
    if not _close(math.fsum(float(row["costs"]) for row in rows), episode["costs"]):
        raise CausalSalvageEvidenceError("causal daily costs do not reconcile")
    if not _close(
        math.fsum(
            float(value)
            for row in rows
            for value in row["component_attribution"].values()
        ),
        episode["net_pnl"],
    ):
        raise CausalSalvageEvidenceError("causal daily attribution does not reconcile")
    if not _close(
        min(float(row["minimum_mll_buffer"]) for row in rows),
        episode["minimum_mll_buffer"],
    ):
        raise CausalSalvageEvidenceError("causal daily MLL path does not reconcile")
    final = rows[-1]
    for field in ("target_progress",):
        if not _close(final[field], episode[field], tolerance=1e-9):
            raise CausalSalvageEvidenceError(f"causal terminal {field} drift")
    if bool(final["consistency_ok"]) != bool(episode["consistency_ok"]):
        raise CausalSalvageEvidenceError("causal terminal consistency drift")


def _require_expected_episode_coverage(
    identity: Mapping[str, Any], episodes: Sequence[Mapping[str, Any]]
) -> None:
    expected = {
        (str(row["policy_id"]), str(row["episode_id"]), str(row["horizon"]))
        for row in identity["expected_coverage"]["required_episode_keys"]
    }
    observed = {
        (str(row["policy_id"]), str(row["episode_id"]), str(row["horizon"]))
        for row in episodes
    }
    missing = expected - observed
    if missing:
        raise CausalSalvageEvidenceError(
            f"causal episode records miss {len(missing)} required coverage keys"
        )


def _signal_time_extension(value: Any) -> dict[str, Any]:
    return {
        "signal_time": _ns_iso_or_none(_field(value, "signal_time_ns")),
        "decision_time": _ns_iso_or_none(_field(value, "decision_time_ns")),
        "order_submit_time": _ns_iso_or_none(_field(value, "order_submit_time_ns")),
        "earliest_executable_time": _ns_iso_or_none(
            _field(value, "earliest_executable_time_ns")
        ),
        "fill_time": _ns_iso_or_none(_field(value, "fill_time_ns")),
        "exit_decision_time": _ns_iso_or_none(
            _field(value, "exit_decision_time_ns")
        ),
        "exit_order_submit_time": _ns_iso_or_none(
            _field(value, "exit_order_submit_time_ns")
        ),
        "exit_earliest_executable_time": _ns_iso_or_none(
            _field(value, "exit_earliest_executable_time_ns")
        ),
        "exit_fill_time": _ns_iso_or_none(_field(value, "exit_fill_time_ns")),
    }


def _episode_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if not callable(to_dict):
        raise CausalSalvageEvidenceError("episode is neither mapping nor serializable object")
    try:
        row = to_dict(include_paths=True)
    except TypeError:
        row = to_dict()
    if not isinstance(row, Mapping):
        raise CausalSalvageEvidenceError("episode serialization did not return an object")
    return dict(row)


def _to_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    method = getattr(value, "to_dict", None)
    if not callable(method):
        raise CausalSalvageEvidenceError("frozen specification is not serializable")
    row = method()
    if not isinstance(row, Mapping):
        raise CausalSalvageEvidenceError("frozen specification serialization drift")
    return dict(row)


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        if name not in value:
            raise CausalSalvageEvidenceError(f"required field is absent: {name}")
        return value[name]
    if not hasattr(value, name):
        raise CausalSalvageEvidenceError(f"required attribute is absent: {name}")
    result = getattr(value, name)
    return result() if name == "fingerprint" and callable(result) else result


def _field_any(value: Any, *names: str) -> Any:
    for name in names:
        if (isinstance(value, Mapping) and name in value) or hasattr(value, name):
            return _field(value, name)
    raise CausalSalvageEvidenceError(
        "none of the required alternative fields is present: " + ", ".join(names)
    )


def _required_time(value: Any, name: str) -> int:
    result = _field(value, name)
    if result is None:
        raise CausalSalvageEvidenceError(f"completed causal trade lacks {name}")
    return int(result)


def _required_float(value: Any, name: str) -> float:
    result = _field(value, name)
    if result is None or not math.isfinite(float(result)):
        raise CausalSalvageEvidenceError(f"completed causal trade lacks finite {name}")
    return float(result)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _numeric_mapping(value: Any, label: str) -> dict[str, float]:
    if not isinstance(value, Mapping):
        raise CausalSalvageEvidenceError(f"causal daily {label} must be an object")
    result = {str(name): float(raw) for name, raw in value.items()}
    if any(not name or not math.isfinite(raw) for name, raw in result.items()):
        raise CausalSalvageEvidenceError(
            f"causal daily {label} must contain finite numeric values"
        )
    return result


def _nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise CausalSalvageEvidenceError(f"causal daily {label} must be an integer")
    result = int(value)
    if float(value) != float(result) or result < 0:
        raise CausalSalvageEvidenceError(
            f"causal daily {label} must be a non-negative integer"
        )
    return result


def _json_object_or_list(value: Any, label: str) -> dict[str, Any] | list[Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, list):
        return list(value)
    raise CausalSalvageEvidenceError(f"causal daily {label} must be object or list")


def _horizon_days(value: str) -> int | None:
    prefix = value.split("_", 1)[0]
    return int(prefix) if prefix.isdigit() else None


def _ns_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _ns_iso_or_none(value: Any) -> str | None:
    return None if value is None else _ns_iso(int(value))


def _day_date(epoch_day: int) -> str:
    return (date(1970, 1, 1) + timedelta(days=epoch_day)).isoformat()


def _day_iso(epoch_day: int) -> str:
    day = date(1970, 1, 1) + timedelta(days=epoch_day)
    return datetime(day.year, day.month, day.day, tzinfo=UTC).isoformat().replace(
        "+00:00", "Z"
    )


def _close(left: Any, right: Any, *, tolerance: float = 1e-6) -> bool:
    return math.isclose(
        float(left), float(right), rel_tol=0.0, abs_tol=float(tolerance)
    )


__all__ = [
    "CAUSAL_SALVAGE_ADAPTER_VERSION",
    "CausalSalvageEvidenceError",
    "CausalSalvageMaterialization",
    "finalize_causal_salvage_evidence_bundle",
    "finalize_causal_salvage_evidence_bundle_streaming",
    "materialize_causal_salvage_evidence",
]
