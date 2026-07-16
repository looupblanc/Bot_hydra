"""Fail-closed append-only processor for frozen active-risk shadow books.

The processor consumes only canonical closed 1-minute bars already present in
``forward_bars.db``.  It never fetches data and never submits an order.  The
current adapter proves post-freeze bar identity, closed-bar resampling and the
frozen feature/binding contract.  Exact online feature/signal equivalence has
not yet been established, so every persisted decision remains fail-closed:
either ``WARMUP_PENDING`` or ``SIGNAL_ENGINE_NOT_PROVEN_FAIL_CLOSED`` with zero
signals, fills and account mutation.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from hydra.data.multitimeframe import resample_closed_bars
from hydra.research.turbo_feature_builder import (
    FEATURE_BUNDLE_VERSION,
    FEATURE_DAG_HASH,
)
from hydra.shadow.active_risk_forward_boundary import (
    validate_active_risk_forward_boundary,
)
from hydra.shadow.active_risk_package import (
    reconstruct_active_risk_shadow_package,
)
from hydra.shadow.forward_bar_store import (
    STORE_SCHEMA,
    CmeSessionCalendar,
    ForwardBar,
)
from hydra.shadow.forward_feed_manifest import stable_hash


EVENT_SCHEMA = "hydra_active_risk_forward_evidence_event_v1"
RESULT_SCHEMA = "hydra_active_risk_forward_processor_result_v1"
PROCESSOR_VERSION = "hydra_active_risk_forward_warmup_adapter_v1"
MINIMUM_CONTIGUOUS_1M_BARS = 181
MINIMUM_COMPLETE_60M_BARS = 11
SUPPORTED_RESAMPLING_MINUTES = (5, 15, 30, 60)


class ActiveRiskForwardProcessorError(RuntimeError):
    """Forward input, event chain or frozen package failed validation."""


def run_active_risk_forward_processor(
    *,
    repository_root: str | Path,
    boundary_manifest_path: str | Path,
    boundary_manifest_sha256: str,
    forward_store_path: str | Path,
    state_dir: str | Path,
    observed_at: datetime,
) -> dict[str, Any]:
    """Process all unseen complete post-freeze decision minutes once."""

    root = Path(repository_root).resolve()
    boundary_path = _inside(root, boundary_manifest_path, label="forward boundary")
    if not boundary_path.is_file() or _sha256(boundary_path) != str(
        boundary_manifest_sha256
    ):
        raise ActiveRiskForwardProcessorError("frozen forward boundary changed")
    boundary = _json(boundary_path, label="forward boundary")
    validate_active_risk_forward_boundary(boundary, repository_root=root)
    observed = _utc(observed_at)
    state_root = _inside(root, state_dir, label="shadow state directory")
    store_path = _inside(root, forward_store_path, label="forward bar store")
    package_rows = [dict(row) for row in boundary["candidates"]]
    packages = {
        str(row["candidate_id"]): reconstruct_active_risk_shadow_package(
            _json(
                _inside(root, row["package_path"], label="boundary package"),
                label="boundary package",
            )
        )
        for row in package_rows
    }
    all_roots = tuple(
        sorted(
            {
                str(value)
                for row in package_rows
                for value in row["required_roots"]
            }
        )
    )
    earliest_freeze = min(_utc(row["freeze_timestamp_utc"]) for row in package_rows)
    bars, store_audit = _read_verified_bars(
        store_path,
        roots=all_roots,
        after_close=earliest_freeze,
        observed_at=observed,
    )

    candidate_results: list[dict[str, Any]] = []
    total_appended = 0
    for row in package_rows:
        candidate_id = str(row["candidate_id"])
        reconstructed = packages[candidate_id]
        package = reconstructed.package
        freeze = _utc(package.freeze_timestamp_utc)
        required_roots = tuple(str(value) for value in row["required_roots"])
        candidate_bars = [
            value
            for value in bars
            if value["root"] in set(required_roots)
            and _utc(value["bar_close_at_utc"]) > freeze
        ]
        common = _complete_decision_minutes(candidate_bars, required_roots)
        ledger_path = _ledger_path(root, state_root, package.observability)
        existing = _validate_event_ledger(
            ledger_path,
            candidate_id=candidate_id,
            package_hash=package.package_hash,
            freeze_timestamp_utc=package.freeze_timestamp_utc,
        )
        last_decision = (
            _utc(existing[-1]["decision_at_utc"]) if existing else None
        )
        unseen = [
            (decision, values)
            for decision, values in common
            if last_decision is None or decision > last_decision
        ]
        resampled = _resampled_closed_bars(candidate_bars, observed_at=observed)
        contiguous = _contiguous_counts([decision for decision, _ in common])
        previous_hash = (
            str(existing[-1]["event_hash"]) if existing else "0" * 64
        )
        events: list[dict[str, Any]] = []
        for decision, minute_bars in unseen:
            mtf = _resampling_snapshot(
                resampled,
                required_roots=required_roots,
                decision_at=decision,
            )
            min_60m = min(
                int((mtf["60m"][root])["complete_bar_count"])
                for root in required_roots
            )
            contiguous_count = int(contiguous[decision])
            market_data_ready = bool(
                contiguous_count >= MINIMUM_CONTIGUOUS_1M_BARS
                and min_60m >= MINIMUM_COMPLETE_60M_BARS
            )
            decision_status = (
                "SIGNAL_ENGINE_NOT_PROVEN_FAIL_CLOSED"
                if market_data_ready
                else "WARMUP_PENDING"
            )
            event: dict[str, Any] = {
                "schema": EVENT_SCHEMA,
                "processor_version": PROCESSOR_VERSION,
                "candidate_id": candidate_id,
                "package_hash": package.package_hash,
                "freeze_timestamp_utc": package.freeze_timestamp_utc,
                "decision_at_utc": decision.isoformat(),
                "observed_at_utc": observed.isoformat(),
                "decision_status": decision_status,
                "raw_decision_bars": {
                    value["root"]: _raw_bar_view(value)
                    for value in sorted(minute_bars, key=lambda item: item["root"])
                },
                "closed_bar_resampling": mtf,
                "causal_warmup": {
                    "pre_freeze_rows_used": 0,
                    "contiguous_common_1m_bars": contiguous_count,
                    "minimum_contiguous_1m_bars": MINIMUM_CONTIGUOUS_1M_BARS,
                    "minimum_complete_60m_bars_per_root": (
                        MINIMUM_COMPLETE_60M_BARS
                    ),
                    "observed_minimum_complete_60m_bars": min_60m,
                    "market_data_warmup_complete": market_data_ready,
                },
                "frozen_feature_contract": {
                    "feature_bundle_version": FEATURE_BUNDLE_VERSION,
                    "feature_dag_hash": FEATURE_DAG_HASH,
                    "binding_count": len(reconstructed.frozen_signal_bindings),
                    "threshold_recalibration_performed": False,
                    "online_feature_equivalence_proven": False,
                    "reason": (
                        "EXACT_ONLINE_FEATURE_SIGNAL_AND_SESSION_EQUIVALENCE_"
                        "NOT_YET_PROVEN"
                    ),
                },
                "signal": {
                    "evaluated": False,
                    "emitted": False,
                    "signal_count": 0,
                    "reason": decision_status,
                },
                "fill": {
                    "evaluated": False,
                    "created": False,
                    "virtual_fill_count": 0,
                    "real_fill_count": 0,
                },
                "account": {
                    "mutated": False,
                    "realized_pnl_delta_usd": 0.0,
                    "unrealized_pnl_delta_usd": 0.0,
                    "mll_path_updated": False,
                    "consistency_path_updated": False,
                    "target_progress_path_updated": False,
                },
                "safety": {
                    "broker_connections": 0,
                    "outbound_orders": 0,
                    "automatic_order_capability": False,
                    "q4_access_delta": 0,
                    "market_data_purchase_delta_usd": 0.0,
                },
                "previous_event_hash": previous_hash,
            }
            event["event_hash"] = stable_hash(event)
            previous_hash = str(event["event_hash"])
            events.append(event)
        if events:
            _append_events(ledger_path, events)
            # Re-read the complete chain after the append; a partial or racing
            # writer is an integrity failure, never a reason to continue.
            _validate_event_ledger(
                ledger_path,
                candidate_id=candidate_id,
                package_hash=package.package_hash,
                freeze_timestamp_utc=package.freeze_timestamp_utc,
            )
        total_appended += len(events)
        candidate_results.append(
            {
                "candidate_id": candidate_id,
                "freeze_timestamp_utc": package.freeze_timestamp_utc,
                "required_roots": list(required_roots),
                "post_freeze_bar_count": len(candidate_bars),
                "complete_decision_minute_count": len(common),
                "events_preexisting": len(existing),
                "events_appended": len(events),
                "latest_decision_at_utc": (
                    common[-1][0].isoformat() if common else None
                ),
                "latest_status": (
                    events[-1]["decision_status"]
                    if events
                    else (existing[-1]["decision_status"] if existing else "WAITING_FOR_DATA")
                ),
                "ledger_path": str(ledger_path),
                "signals_emitted": 0,
                "virtual_fills_created": 0,
                "account_mutations": 0,
            }
        )
    conclusion = (
        "FAIL_CLOSED_CAUSAL_WARMUP_EVENTS_APPENDED"
        if total_appended
        else "WAITING_FOR_GENUINE_POST_FREEZE_COMPLETE_ROOT_BARS"
    )
    result: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "processor_version": PROCESSOR_VERSION,
        "scientific_conclusion": conclusion,
        "observed_at_utc": observed.isoformat(),
        "boundary_manifest_path": str(boundary_path),
        "boundary_manifest_sha256": boundary_manifest_sha256,
        "forward_store": store_audit,
        "candidate_count": len(candidate_results),
        "candidates": candidate_results,
        "events_appended": total_appended,
        "signals_emitted": 0,
        "virtual_fills_created": 0,
        "account_mutations": 0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "q4_access_delta": 0,
        "market_data_purchase_delta_usd": 0.0,
    }
    result["result_hash"] = stable_hash(result)
    return result


def _read_verified_bars(
    path: Path,
    *,
    roots: Sequence[str],
    after_close: datetime,
    observed_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.is_file():
        return [], {
            "schema": STORE_SCHEMA,
            "exists": False,
            "sqlite_integrity": None,
            "eligible_bar_count": 0,
        }
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        schema = connection.execute(
            "SELECT value FROM metadata WHERE key='schema'"
        ).fetchone()
        if integrity != "ok" or schema is None or str(schema[0]) != STORE_SCHEMA:
            raise ActiveRiskForwardProcessorError("forward bar store integrity/schema failure")
        placeholders = ",".join("?" for _ in roots)
        query = f"""
            SELECT source_id, root, contract, timeframe, bar_start_at_utc,
                   bar_close_at_utc, availability_at_utc, open, high, low,
                   close, volume, source_sequence, payload_hash
            FROM bars
            WHERE root IN ({placeholders})
            ORDER BY bar_close_at_utc, root, contract, source_sequence
        """
        raw_rows = [
            dict(value)
            for value in connection.execute(
                query,
                tuple(roots),
            ).fetchall()
        ]
        # SQLite stores canonical ISO strings, but historical rows may use
        # either ``Z`` or ``+00:00``.  Compare parsed instants, not text.
        rows = [
            row
            for row in raw_rows
            if _utc(row["bar_close_at_utc"]) > after_close
            and _utc(row["availability_at_utc"]) <= observed_at
        ]
    except sqlite3.Error as exc:
        raise ActiveRiskForwardProcessorError("forward bar store is unreadable") from exc
    finally:
        connection.close()
    seen: set[tuple[str, str]] = set()
    previous_sequence: dict[tuple[str, str, str], int] = {}
    for row in rows:
        bar = ForwardBar(
            source_id=str(row["source_id"]),
            root=str(row["root"]),
            contract=str(row["contract"]),
            timeframe=str(row["timeframe"]),
            bar_start_at_utc=_utc(row["bar_start_at_utc"]),
            bar_close_at_utc=_utc(row["bar_close_at_utc"]),
            availability_at_utc=_utc(row["availability_at_utc"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            source_sequence=int(row["source_sequence"]),
        )
        # Weekly-only validation is sufficient here because ingestion already
        # applied the versioned calendar.  This pass independently rechecks the
        # closed 1m/OHLC/availability/payload contract.
        bar.validate(observed_at=observed_at, calendar=CmeSessionCalendar.weekly_only())
        if bar.payload_hash != row["payload_hash"]:
            raise ActiveRiskForwardProcessorError("forward bar payload hash drift")
        identity = (str(row["root"]), str(row["bar_close_at_utc"]))
        if identity in seen:
            raise ActiveRiskForwardProcessorError(
                "multiple explicit contracts/sources occupy one root decision minute"
            )
        seen.add(identity)
        stream = (str(row["source_id"]), str(row["root"]), str(row["contract"]))
        sequence = int(row["source_sequence"])
        if sequence <= previous_sequence.get(stream, 0):
            raise ActiveRiskForwardProcessorError("forward source sequence is not monotonic")
        previous_sequence[stream] = sequence
    return rows, {
        "schema": STORE_SCHEMA,
        "exists": True,
        "sqlite_integrity": integrity,
        "eligible_bar_count": len(rows),
        "earliest_eligible_close_at_utc": (
            rows[0]["bar_close_at_utc"] if rows else None
        ),
        "latest_eligible_close_at_utc": (
            rows[-1]["bar_close_at_utc"] if rows else None
        ),
    }


def _complete_decision_minutes(
    bars: Sequence[Mapping[str, Any]], required_roots: Sequence[str]
) -> list[tuple[datetime, list[dict[str, Any]]]]:
    required = set(required_roots)
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for raw in bars:
        row = dict(raw)
        grouped.setdefault(_utc(row["bar_close_at_utc"]), []).append(row)
    output = []
    for decision, values in sorted(grouped.items()):
        roots = {str(value["root"]) for value in values}
        if roots == required and len(values) == len(required):
            output.append((decision, values))
    return output


def _resampled_closed_bars(
    bars: Sequence[Mapping[str, Any]], *, observed_at: datetime
) -> dict[int, pd.DataFrame]:
    if not bars:
        return {minutes: pd.DataFrame() for minutes in SUPPORTED_RESAMPLING_MINUTES}
    frame = pd.DataFrame(
        {
            "symbol": [str(row["root"]) for row in bars],
            "active_contract": [str(row["contract"]) for row in bars],
            "timestamp": [row["bar_start_at_utc"] for row in bars],
            "open": [float(row["open"]) for row in bars],
            "high": [float(row["high"]) for row in bars],
            "low": [float(row["low"]) for row in bars],
            "close": [float(row["close"]) for row in bars],
            "volume": [float(row["volume"]) for row in bars],
        }
    )
    output: dict[int, pd.DataFrame] = {}
    for minutes in SUPPORTED_RESAMPLING_MINUTES:
        resampled = resample_closed_bars(frame, minutes, as_of=observed_at)
        complete = (
            resampled["source_row_count"].eq(minutes)
            & resampled["source_last_timestamp"].eq(
                resampled["source_bar_close"] - pd.Timedelta(minutes=1)
            )
        )
        output[minutes] = resampled.loc[complete].reset_index(drop=True)
    return output


def _resampling_snapshot(
    resampled: Mapping[int, pd.DataFrame],
    *,
    required_roots: Sequence[str],
    decision_at: datetime,
) -> dict[str, dict[str, dict[str, Any]]]:
    snapshot: dict[str, dict[str, dict[str, Any]]] = {}
    cutoff = pd.Timestamp(decision_at)
    for minutes in SUPPORTED_RESAMPLING_MINUTES:
        by_root: dict[str, dict[str, Any]] = {}
        frame = resampled[minutes]
        for root in required_roots:
            if frame.empty:
                selected = frame
            else:
                selected = frame.loc[
                    frame["symbol"].astype(str).eq(str(root))
                    & (frame["availability_timestamp"] <= cutoff)
                ]
            latest = (
                pd.Timestamp(selected.iloc[-1]["availability_timestamp"]).isoformat()
                if not selected.empty
                else None
            )
            by_root[str(root)] = {
                "complete_bar_count": int(len(selected)),
                "latest_complete_bar_close_at_utc": latest,
                "incomplete_bar_used": False,
            }
        snapshot[f"{minutes}m"] = by_root
    return snapshot


def _contiguous_counts(decisions: Sequence[datetime]) -> dict[datetime, int]:
    output: dict[datetime, int] = {}
    previous: datetime | None = None
    count = 0
    for current in sorted(decisions):
        count = count + 1 if previous and current - previous == timedelta(minutes=1) else 1
        output[current] = count
        previous = current
    return output


def _raw_bar_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": str(row["source_id"]),
        "contract": str(row["contract"]),
        "bar_start_at_utc": str(row["bar_start_at_utc"]),
        "bar_close_at_utc": str(row["bar_close_at_utc"]),
        "availability_at_utc": str(row["availability_at_utc"]),
        "source_sequence": int(row["source_sequence"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "payload_hash": str(row["payload_hash"]),
    }


def _ledger_path(
    repository_root: Path,
    state_dir: Path,
    observability: Mapping[str, Any],
) -> Path:
    path = _inside(
        repository_root,
        str(observability.get("ledger_path") or ""),
        label="package forward evidence ledger",
    )
    allowed = (state_dir / "forward").resolve()
    try:
        path.relative_to(allowed)
    except ValueError as exc:
        raise ActiveRiskForwardProcessorError(
            "package forward ledger escapes the configured shadow state"
        ) from exc
    return path


def _validate_event_ledger(
    path: Path,
    *,
    candidate_id: str,
    package_hash: str,
    freeze_timestamp_utc: str,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    previous_hash = "0" * 64
    previous_decision: datetime | None = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ActiveRiskForwardProcessorError("forward evidence ledger is unreadable") from exc
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ActiveRiskForwardProcessorError(
                "forward evidence ledger contains a partial/invalid row"
            ) from exc
        if not isinstance(row, dict):
            raise ActiveRiskForwardProcessorError("forward evidence row is not an object")
        claimed = str(row.get("event_hash") or "")
        body = dict(row)
        body.pop("event_hash", None)
        decision = _utc(str(row.get("decision_at_utc") or ""))
        if (
            row.get("schema") != EVENT_SCHEMA
            or row.get("processor_version") != PROCESSOR_VERSION
            or row.get("candidate_id") != candidate_id
            or row.get("package_hash") != package_hash
            or row.get("freeze_timestamp_utc") != freeze_timestamp_utc
            or row.get("previous_event_hash") != previous_hash
            or stable_hash(body) != claimed
            or decision <= _utc(freeze_timestamp_utc)
            or (previous_decision is not None and decision <= previous_decision)
            or (row.get("signal") or {}).get("emitted") is not False
            or (row.get("fill") or {}).get("created") is not False
            or (row.get("account") or {}).get("mutated") is not False
            or int((row.get("safety") or {}).get("outbound_orders", -1)) != 0
            or int((row.get("safety") or {}).get("broker_connections", -1)) != 0
        ):
            raise ActiveRiskForwardProcessorError(
                "forward evidence append-only chain or safety contract drift"
            )
        previous_hash = claimed
        previous_decision = decision
        rows.append(row)
    return rows


def _append_events(path: Path, events: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ActiveRiskForwardProcessorError(
                "another active-risk evidence writer holds the lock"
            ) from exc
        with path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(
                    json.dumps(
                        dict(event),
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    + "\n"
                )
            handle.flush()
            os.fsync(handle.fileno())
        fcntl.flock(lock, fcntl.LOCK_UN)


def _json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskForwardProcessorError(f"{label} is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise ActiveRiskForwardProcessorError(f"{label} is not an object: {path}")
    return value


def _inside(root: Path, raw: str | Path, *, label: str) -> Path:
    path = Path(raw)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ActiveRiskForwardProcessorError(f"{label} escapes repository root") from exc
    return resolved


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc(value: datetime | str) -> datetime:
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise ActiveRiskForwardProcessorError("invalid forward UTC timestamp") from exc
    if parsed.tzinfo is None:
        raise ActiveRiskForwardProcessorError("naive forward timestamps are prohibited")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "EVENT_SCHEMA",
    "PROCESSOR_VERSION",
    "RESULT_SCHEMA",
    "ActiveRiskForwardProcessorError",
    "run_active_risk_forward_processor",
]
