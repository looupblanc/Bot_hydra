from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from hydra.data.budget import (
    AUTO_UNDER_HARD_CAP,
    DatabentoBudgetConfig,
    DatabentoBudgetError,
    DatabentoSpendRecord,
    append_spend_record,
    cumulative_spend,
    enforce_budget,
    request_id_for,
    sha256_file,
    utc_now,
)
from hydra.data.current_contract_map import ensure_current_contract_map
from hydra.data.databento_loader import load_api_key
from hydra.shadow.contract_resolver import (
    ContractResolution,
    discover_roll_maps,
    resolve_current_contracts,
)
from hydra.shadow.data_heartbeat import ForwardDataHeartbeat, HeartbeatPublisher
from hydra.shadow.feed_health import assess_feed_health
from hydra.shadow.forward_bar_store import ForwardBar, ForwardBarStore
from hydra.shadow.forward_feed_manifest import (
    build_cme_calendar_manifest,
    build_read_only_source_manifest,
    calendar_from_manifest,
    stable_hash,
    validate_forward_boundary_manifest,
    write_manifest,
)
from hydra.utils.config import project_path


DATASET = "GLBX.MDP3"
SCHEMA = "ohlcv-1m"
SOURCE_ID = "databento_historical_delayed_v1"
RESULT_SCHEMA = "hydra_databento_delayed_forward_update_v1"


class DatabentoForwardFeedError(RuntimeError):
    pass


def run_databento_forward_update(
    output_dir: str | Path,
    *,
    boundary_manifest_path: str | Path,
    boundary_manifest_sha256: str,
    state_dir: str | Path,
    contract_map_dir: str | Path,
    budget: DatabentoBudgetConfig,
    code_commit: str,
    minimum_reserve_usd: float = 30.0,
    maximum_incremental_cost_usd: float = 0.10,
    now: datetime | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    observed = _utc(now or datetime.now(timezone.utc))
    boundary_path = Path(boundary_manifest_path)
    if not boundary_path.is_file() or sha256_file(boundary_path) != str(
        boundary_manifest_sha256
    ):
        raise DatabentoForwardFeedError("Frozen forward-boundary artifact changed.")
    boundaries = json.loads(boundary_path.read_text(encoding="utf-8"))
    validate_forward_boundary_manifest(boundaries)
    candidates = [dict(value) for value in boundaries["candidates"]]
    roots = tuple(
        sorted(
            {
                str(root)
                for candidate in candidates
                for root in candidate["required_roots"]
            }
        )
    )
    freeze_by_root = {
        root: min(
            _utc(str(candidate["freeze_timestamp_utc"]))
            for candidate in candidates
            if root in set(candidate["required_roots"])
        )
        for root in roots
    }
    earliest_freeze = min(freeze_by_root.values())

    historical = client or _historical_client()
    dataset_range = dict(historical.metadata.get_dataset_range(dataset=DATASET))
    schema_range = dict((dataset_range.get("schema") or {}).get(SCHEMA) or {})
    available_end = _utc(str(schema_range.get("end") or dataset_range.get("end")))
    manifest_root = Path(state_dir) / "forward_data" / "manifests"
    source_manifest = build_read_only_source_manifest(
        dataset=DATASET,
        checked_at=observed,
        valid_through=observed.date() + timedelta(days=1),
        dataset_range=dataset_range,
    )
    source_path = write_manifest(
        manifest_root / f"source_{source_manifest['manifest_hash'][:16]}.json",
        source_manifest,
    )
    calendar_manifest = build_cme_calendar_manifest(
        checked_at=observed,
        valid_through=observed.date() + timedelta(days=31),
    )
    calendar_path = write_manifest(
        manifest_root / f"calendar_{calendar_manifest['manifest_hash'][:16]}.json",
        calendar_manifest,
    )
    calendar = calendar_from_manifest(calendar_manifest, now=observed)

    map_paths = discover_roll_maps(contract_map_dir)
    resolution_time = min(available_end, observed) - timedelta(microseconds=1)
    resolution = resolve_current_contracts(map_paths, roots, as_of=resolution_time)
    map_receipt: dict[str, Any] | None = None
    definitions_spend = 0.0
    if not resolution.ready:
        # A multi-week lookback prevents the symbology API from clipping the
        # current segment's start to the request boundary.  That preserves the
        # observed roll date instead of mistaking today's request end for a
        # roll transition.
        mapping_start = (resolution_time - timedelta(days=45)).isoformat()
        mapping_end = resolution_time.isoformat()
        receipt = ensure_current_contract_map(
            historical,
            roots=roots,
            start=mapping_start,
            end=mapping_end,
            budget=budget,
            cache_root=Path(contract_map_dir) / "forward_definitions",
            minimum_reserve_usd=minimum_reserve_usd,
            dataset=DATASET,
            schema=SCHEMA,
        )
        map_receipt = receipt.to_dict()
        definitions_spend = float(receipt.incremental_spend_usd)
        map_paths = discover_roll_maps(contract_map_dir)
        resolution = resolve_current_contracts(
            map_paths, roots, as_of=resolution_time
        )
    if not resolution.ready:
        raise DatabentoForwardFeedError(
            f"Current explicit-contract resolution failed: {resolution.reason}"
        )

    store = ForwardBarStore(
        Path(state_dir) / "forward_data" / "forward_bars.db",
        calendar=calendar,
    )
    latest_before = store.latest_by_root()
    query_start_by_root: dict[str, datetime] = {}
    for root in roots:
        start = freeze_by_root[root]
        previous = latest_before.get(root)
        if previous:
            start = max(start, _utc(str(previous["bar_close_at_utc"])))
        start = max(start, _utc(resolution.contract_for(root).active_start))
        query_start_by_root[root] = start
    query_start = min(query_start_by_root.values())
    query_end = min(available_end, observed - timedelta(seconds=1))
    raw_symbols = [resolution.contract_for(root).contract for root in roots]
    record_count = 0
    estimated_cost = 0.0
    data_path: Path | None = None
    data_sha256: str | None = None
    data_spend = 0.0
    accepted = 0
    duplicates = 0
    candidate_heartbeats = 0
    heartbeat_paths: dict[str, str] = {}

    if query_end > query_start and query_end > earliest_freeze:
        record_count = int(
            historical.metadata.get_record_count(
                dataset=DATASET,
                start=query_start.isoformat(),
                end=query_end.isoformat(),
                symbols=raw_symbols,
                schema=SCHEMA,
                stype_in="raw_symbol",
            )
        )
    if record_count > 0:
        estimated_cost = float(
            historical.metadata.get_cost(
                dataset=DATASET,
                start=query_start.isoformat(),
                end=query_end.isoformat(),
                symbols=raw_symbols,
                schema=SCHEMA,
                stype_in="raw_symbol",
            )
        )
        if definitions_spend + estimated_cost > maximum_incremental_cost_usd:
            raise DatabentoBudgetError(
                "Forward update exceeds its bounded incremental-cost authorization."
            )
        data_path, data_spend = _acquire_ohlcv(
            historical,
            roots=roots,
            raw_symbols=raw_symbols,
            start=query_start,
            end=query_end,
            estimate=estimated_cost,
            budget=budget,
            minimum_reserve_usd=minimum_reserve_usd,
        )
        data_sha256 = sha256_file(data_path)
        frame = _load_forward_frame(data_path, resolution)
        if not frame.empty:
            latest_sequences = {
                root: int((latest_before.get(root) or {}).get("source_sequence") or 0)
                for root in roots
            }
            with store.writer(writer_id=SOURCE_ID) as writer:
                for root, group in frame.groupby("root", sort=True):
                    lower = query_start_by_root[str(root)]
                    selected = group[
                        group["timestamp"].add(pd.Timedelta(minutes=1)) > lower
                    ].sort_values("timestamp")
                    for row in selected.itertuples(index=False):
                        latest_sequences[str(root)] += 1
                        bar = ForwardBar(
                            source_id=SOURCE_ID,
                            root=str(root),
                            contract=str(row.contract),
                            timeframe="1m",
                            bar_start_at_utc=_utc(row.timestamp),
                            bar_close_at_utc=_utc(row.timestamp)
                            + timedelta(minutes=1),
                            availability_at_utc=observed,
                            open=float(row.open),
                            high=float(row.high),
                            low=float(row.low),
                            close=float(row.close),
                            volume=float(row.volume),
                            source_sequence=latest_sequences[str(root)],
                        )
                        receipt = writer.append(
                            bar, observed_at=observed, resolution=resolution
                        )
                        accepted += receipt["status"] == "ACCEPTED"
                        duplicates += receipt["status"] == "DUPLICATE_IGNORED"

    latest_after = store.latest_by_root()
    source_checksum = data_sha256 or stable_hash(
        {"available_end": available_end.isoformat(), "record_count": record_count}
    )
    publisher = HeartbeatPublisher(Path(state_dir) / "forward_data")
    with publisher.writer(writer_id=SOURCE_ID) as heartbeat_writer:
        for candidate in candidates:
            required = tuple(sorted(str(value) for value in candidate["required_roots"]))
            freeze = _utc(str(candidate["freeze_timestamp_utc"]))
            if any(
                root not in latest_after
                or _utc(str(latest_after[root]["bar_close_at_utc"])) <= freeze
                for root in required
            ):
                continue
            subset = _subset_resolution(resolution, required)
            health = assess_feed_health(
                subset,
                store,
                now=observed,
                max_age_seconds=int(candidate["stale_data_seconds"]),
                source_authorization_proven=True,
            )
            if not health.can_publish_candidate_heartbeat:
                continue
            latest_completed = min(
                _utc(str(latest_after[root]["bar_close_at_utc"]))
                for root in required
            )
            sequence = max(
                int(latest_after[root]["source_sequence"]) for root in required
            )
            checkpoint = stable_hash(
                {
                    "store": store.summary(),
                    "latest": {root: latest_after[root] for root in required},
                }
            )
            heartbeat = ForwardDataHeartbeat.build(
                source_id=SOURCE_ID,
                dataset=DATASET,
                contracts={root: subset.contract_for(root) for root in required},
                latest_completed_bar_at=latest_completed,
                observed_at=observed,
                source_sequence=sequence,
                source_payload_checksum=source_checksum,
                store_checkpoint=checkpoint,
            )
            heartbeat_path = heartbeat_writer.publish(
                candidate_id=str(candidate["candidate_id"]),
                heartbeat=heartbeat,
                required_roots=required,
                stale_data_seconds=int(candidate["stale_data_seconds"]),
                health_status="READY",
                now=observed,
            )
            heartbeat_paths[str(candidate["candidate_id"])] = str(
                heartbeat_path.resolve()
            )
            candidate_heartbeats += 1

    market_state = calendar.market_state(observed)
    if accepted and candidate_heartbeats:
        conclusion = "FORWARD_BARS_INGESTED_AND_HEARTBEATS_PUBLISHED"
    elif accepted:
        conclusion = "FORWARD_BARS_INGESTED_FAIL_CLOSED_STALE_OR_INCOMPLETE"
    else:
        conclusion = "WAITING_FOR_FIRST_POST_FREEZE_FORWARD_BAR"
    next_check = _next_check(observed, calendar, active=market_state == "OPEN")
    actual_spend = cumulative_spend(project_path(budget.ledger_path))[1]
    result: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "scientific_conclusion": conclusion,
        "code_commit": code_commit,
        "checked_at_utc": observed.isoformat(),
        "available_end_utc": available_end.isoformat(),
        "market_state": market_state,
        "boundary_manifest_path": str(boundary_path.resolve()),
        "boundary_manifest_sha256": boundary_manifest_sha256,
        "source_authorization_manifest_path": str(source_path.resolve()),
        "source_authorization_manifest_sha256": sha256_file(source_path),
        "session_calendar_manifest_path": str(calendar_path.resolve()),
        "session_calendar_manifest_sha256": sha256_file(calendar_path),
        "contract_resolution": resolution.to_dict(),
        "current_contract_map": map_receipt,
        "query": {
            "start": query_start.isoformat(),
            "end": query_end.isoformat(),
            "raw_symbols": raw_symbols,
            "official_record_count": record_count,
            "official_estimated_cost_usd": estimated_cost,
            "data_path": str(data_path.resolve()) if data_path else None,
            "data_sha256": data_sha256,
        },
        "fresh_forward_bars_processed": int(accepted),
        "duplicates_ignored": int(duplicates),
        "candidate_heartbeats_published": candidate_heartbeats,
        "heartbeat_paths": heartbeat_paths,
        "forward_store": store.summary(),
        "incremental_databento_spend_usd": float(
            definitions_spend + data_spend
        ),
        "cumulative_databento_spend_usd": float(actual_spend),
        "remaining_databento_budget_usd": float(budget.hard_cap_usd - actual_spend),
        "protected_reserve_usd": float(minimum_reserve_usd),
        "next_check_at_utc": next_check.isoformat(),
        "broker_connections": 0,
        "outbound_orders": 0,
        "automatic_order_capability": False,
        "paper_shadow_ready": 0,
        "q4_access_delta": 0,
    }
    result["result_hash"] = stable_hash(result)
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    result_path = root / "forward_update_result.json"
    _atomic_json(result_path, result)
    report_path = root / "forward_update_report.md"
    _atomic_text(
        report_path,
        "\n".join(
            [
                "# HYDRA delayed forward update",
                "",
                f"- Conclusion: `{conclusion}`",
                f"- Fresh post-freeze bars: `{accepted}`",
                f"- Candidate heartbeats: `{candidate_heartbeats}`",
                f"- Incremental cost USD: `{definitions_spend + data_spend:.9f}`",
                "- Broker connections: `0`",
                "- Outbound orders: `0`",
                "",
            ]
        ),
    )
    result["result_path"] = str(result_path.resolve())
    result["result_sha256"] = sha256_file(result_path)
    result["report_path"] = str(report_path.resolve())
    return result


def _acquire_ohlcv(
    client: Any,
    *,
    roots: tuple[str, ...],
    raw_symbols: list[str],
    start: datetime,
    end: datetime,
    estimate: float,
    budget: DatabentoBudgetConfig,
    minimum_reserve_usd: float,
) -> tuple[Path, float]:
    payload = {
        "dataset": DATASET,
        "schema": SCHEMA,
        "symbols": raw_symbols,
        "stype_in": "raw_symbol",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "purpose": "strictly post-freeze append-only shadow observations",
    }
    request_id = request_id_for(payload)
    target = project_path(
        "data", "cache", "databento", "forward", f"{request_id}.dbn.zst"
    )
    if target.is_file():
        return target, 0.0
    projected, actual = enforce_budget(budget, estimate)
    if budget.hard_cap_usd - (actual + estimate) < minimum_reserve_usd:
        raise DatabentoBudgetError(
            "Forward bars would consume the protected final-lockbox reserve."
        )
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=SCHEMA,
            symbols=raw_symbols,
            stype_in="raw_symbol",
            start=start.isoformat(),
            end=end.isoformat(),
            estimated_cost_usd=estimate,
            actual_cost_usd=None,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual,
            cache_hit=False,
            research_purpose="strictly post-freeze append-only shadow observations",
            candidate_tier="SHADOW_FORWARD_EVIDENCE",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=None,
            checksum=None,
            download_status="ESTIMATED_ONLY",
        ),
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    client.timeseries.get_range(
        dataset=DATASET,
        start=start.isoformat(),
        end=end.isoformat(),
        symbols=raw_symbols,
        schema=SCHEMA,
        stype_in="raw_symbol",
        stype_out="instrument_id",
        path=target,
    )
    append_spend_record(
        budget,
        DatabentoSpendRecord(
            request_id=request_id,
            timestamp_utc=utc_now(),
            dataset=DATASET,
            schema=SCHEMA,
            symbols=raw_symbols,
            stype_in="raw_symbol",
            start=start.isoformat(),
            end=end.isoformat(),
            estimated_cost_usd=0.0,
            actual_cost_usd=estimate,
            cumulative_estimated_spend_usd=projected,
            cumulative_actual_spend_usd=actual + estimate,
            cache_hit=False,
            research_purpose="strictly post-freeze append-only shadow observations",
            candidate_tier="SHADOW_FORWARD_EVIDENCE",
            approval_mode=AUTO_UNDER_HARD_CAP,
            resulting_file=str(target.resolve()),
            checksum=sha256_file(target),
            download_status="DOWNLOADED",
        ),
    )
    return target, estimate


def _load_forward_frame(
    path: Path, resolution: ContractResolution
) -> pd.DataFrame:
    db = _import_databento()
    store = db.DBNStore.from_file(path)
    frame = store.to_df(price_type="float", pretty_ts=True, map_symbols=False)
    if frame.index.name:
        frame = frame.reset_index()
    frame = frame.rename(columns={"ts_event": "timestamp"})
    required = {"timestamp", "instrument_id", "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise DatabentoForwardFeedError(
            f"Forward DBN columns missing: {sorted(missing)}"
        )
    instrument_map = {
        str(value.instrument_id): (value.root, value.contract)
        for value in resolution.contracts
    }
    frame["instrument_id"] = frame["instrument_id"].astype(str)
    frame["root"] = frame["instrument_id"].map(
        {key: value[0] for key, value in instrument_map.items()}
    )
    frame["contract"] = frame["instrument_id"].map(
        {key: value[1] for key, value in instrument_map.items()}
    )
    if frame[["root", "contract"]].isna().any().any():
        raise DatabentoForwardFeedError("Forward DBN contains unmapped instruments.")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if frame.duplicated(["root", "timestamp"]).any():
        raise DatabentoForwardFeedError("Duplicate forward root/timestamp bars detected.")
    return frame.sort_values(["root", "timestamp"]).reset_index(drop=True)


def _subset_resolution(
    resolution: ContractResolution, roots: tuple[str, ...]
) -> ContractResolution:
    required = tuple(sorted(roots))
    return ContractResolution(
        status="READY",
        as_of_utc=resolution.as_of_utc,
        required_roots=required,
        contracts=tuple(
            value for value in resolution.contracts if value.root in set(required)
        ),
        missing_roots=(),
        unsafe_roll_roots=(),
        reason="candidate_exact_contract_subset",
        next_action=None,
        inspected_maps=resolution.inspected_maps,
    )


def _next_check(
    now: datetime, calendar: Any, *, active: bool
) -> datetime:
    current = _utc(now)
    if active:
        return current + timedelta(minutes=2)
    probe = current + timedelta(minutes=1)
    for _ in range(8 * 24 * 60):
        if calendar.market_state(probe) == "OPEN":
            return probe + timedelta(minutes=2)
        probe += timedelta(minutes=1)
    raise DatabentoForwardFeedError("Could not locate the next CME open.")


def _historical_client() -> Any:
    key = load_api_key()
    if not key:
        raise DatabentoForwardFeedError("DATABENTO_API_KEY is unavailable.")
    return _import_databento().Historical(key)


def _import_databento() -> Any:
    try:
        import databento as db
    except ImportError as exc:
        raise DatabentoForwardFeedError("Databento dependency is unavailable.") from exc
    return db


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _utc(value: datetime | pd.Timestamp | str) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise DatabentoForwardFeedError("Timezone-aware forward timestamps are required.")
    return parsed.tz_convert("UTC").to_pydatetime()


__all__ = [
    "DATASET",
    "RESULT_SCHEMA",
    "SCHEMA",
    "SOURCE_ID",
    "DatabentoForwardFeedError",
    "run_databento_forward_update",
]
