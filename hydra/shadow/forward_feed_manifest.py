from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.shadow.forward_bar_store import (
    CmeSessionCalendar,
    MarketClosure,
)


SOURCE_SCHEMA = "hydra_read_only_market_data_authorization_v1"
CALENDAR_SCHEMA = "hydra_cme_session_calendar_v1"
BOUNDARY_SCHEMA = "hydra_shadow_forward_boundary_manifest_v1"


class ForwardFeedManifestError(RuntimeError):
    pass


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def build_read_only_source_manifest(
    *,
    dataset: str,
    checked_at: datetime,
    valid_through: date,
    dataset_range: Mapping[str, Any],
) -> dict[str, Any]:
    observed = _utc(checked_at)
    payload: dict[str, Any] = {
        "schema": SOURCE_SCHEMA,
        "dataset": str(dataset),
        "market_data_read_only": True,
        "historical_metadata_preflight": True,
        "symbology_read_preflight": True,
        "credential_material_persisted": False,
        "credentials_read_by_shadow_runtime": False,
        "broker_connections": 0,
        "outbound_orders": 0,
        "checked_at_utc": observed.isoformat(),
        "valid_through": valid_through.isoformat(),
        "available_range": dict(dataset_range),
    }
    payload["manifest_hash"] = stable_hash(payload)
    validate_read_only_source_manifest(payload, now=observed)
    return payload


def validate_read_only_source_manifest(
    payload: Mapping[str, Any], *, now: datetime
) -> None:
    semantic = dict(payload)
    supplied = str(semantic.pop("manifest_hash", ""))
    if supplied != stable_hash(semantic):
        raise ForwardFeedManifestError("Read-only source manifest hash mismatch.")
    if semantic.get("schema") != SOURCE_SCHEMA:
        raise ForwardFeedManifestError("Unsupported read-only source manifest.")
    if semantic.get("dataset") != "GLBX.MDP3":
        raise ForwardFeedManifestError("Only the frozen GLBX.MDP3 source is allowed.")
    if semantic.get("market_data_read_only") is not True:
        raise ForwardFeedManifestError("Forward source must be market-data read only.")
    if semantic.get("historical_metadata_preflight") is not True:
        raise ForwardFeedManifestError("Historical metadata preflight is required.")
    if semantic.get("symbology_read_preflight") is not True:
        raise ForwardFeedManifestError("Symbology preflight is required.")
    if semantic.get("credential_material_persisted") is not False:
        raise ForwardFeedManifestError("Credential persistence is prohibited.")
    if int(semantic.get("broker_connections", -1)) != 0 or int(
        semantic.get("outbound_orders", -1)
    ) != 0:
        raise ForwardFeedManifestError("Broker/order capability is prohibited.")
    if date.fromisoformat(str(semantic["valid_through"])) < _utc(now).date():
        raise ForwardFeedManifestError("Read-only source manifest is expired.")


def build_cme_calendar_manifest(
    *,
    checked_at: datetime,
    valid_through: date,
    closures: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": CALENDAR_SCHEMA,
        "version": f"cme_globex_2026_checked_{_utc(checked_at).date().isoformat()}",
        "checked_at_utc": _utc(checked_at).isoformat(),
        "valid_through": valid_through.isoformat(),
        "source": "https://www.cmegroup.com/trading-hours.html",
        "weekly_session": {
            "timezone": "America/Chicago",
            "sunday_open_local": "17:00",
            "friday_close_local": "16:00",
            "daily_maintenance_local": ["16:00", "17:00"],
        },
        "closures": [dict(value) for value in closures],
        "broker_connections": 0,
        "outbound_orders": 0,
    }
    payload["manifest_hash"] = stable_hash(payload)
    calendar_from_manifest(payload, now=checked_at)
    return payload


def calendar_from_manifest(
    payload: Mapping[str, Any], *, now: datetime | None = None
) -> CmeSessionCalendar:
    semantic = dict(payload)
    supplied = str(semantic.pop("manifest_hash", ""))
    if supplied != stable_hash(semantic):
        raise ForwardFeedManifestError("CME calendar manifest hash mismatch.")
    if semantic.get("schema") != CALENDAR_SCHEMA:
        raise ForwardFeedManifestError("Unsupported CME calendar manifest.")
    if int(semantic.get("broker_connections", -1)) != 0 or int(
        semantic.get("outbound_orders", -1)
    ) != 0:
        raise ForwardFeedManifestError("Calendar manifest cannot authorize orders.")
    valid_through = date.fromisoformat(str(semantic["valid_through"]))
    if now is not None and valid_through < _utc(now).date():
        raise ForwardFeedManifestError("CME calendar manifest is expired.")
    return CmeSessionCalendar(
        version=str(semantic["version"]),
        holiday_schedule_through=valid_through,
        closures=tuple(
            MarketClosure(
                start_at_utc=_utc(str(value["start_at_utc"])),
                end_at_utc=_utc(str(value["end_at_utc"])),
                reason=str(value["reason"]),
            )
            for value in semantic.get("closures") or []
        ),
    )


def build_forward_boundary_manifest(
    candidates: Sequence[Mapping[str, Any]], *, created_at: datetime
) -> dict[str, Any]:
    rows = [dict(value) for value in candidates]
    rows.sort(key=lambda value: str(value.get("candidate_id") or ""))
    payload: dict[str, Any] = {
        "schema": BOUNDARY_SCHEMA,
        "created_at_utc": _utc(created_at).isoformat(),
        "candidate_count": len(rows),
        "candidates": rows,
        "forward_only": True,
        "pre_freeze_backfill_prohibited": True,
        "broker_connections": 0,
        "outbound_orders": 0,
    }
    payload["manifest_hash"] = stable_hash(payload)
    validate_forward_boundary_manifest(payload)
    return payload


def validate_forward_boundary_manifest(payload: Mapping[str, Any]) -> None:
    semantic = dict(payload)
    supplied = str(semantic.pop("manifest_hash", ""))
    if supplied != stable_hash(semantic):
        raise ForwardFeedManifestError("Forward boundary manifest hash mismatch.")
    if semantic.get("schema") != BOUNDARY_SCHEMA:
        raise ForwardFeedManifestError("Unsupported forward boundary manifest.")
    rows = [dict(value) for value in semantic.get("candidates") or []]
    if int(semantic.get("candidate_count") or 0) != len(rows) or not rows:
        raise ForwardFeedManifestError("Forward boundary candidate set is empty/incomplete.")
    identifiers = [str(value.get("candidate_id") or "") for value in rows]
    if not all(identifiers) or len(identifiers) != len(set(identifiers)):
        raise ForwardFeedManifestError("Forward candidates must be uniquely identified.")
    if semantic.get("forward_only") is not True or semantic.get(
        "pre_freeze_backfill_prohibited"
    ) is not True:
        raise ForwardFeedManifestError("Strict post-freeze semantics are required.")
    if int(semantic.get("broker_connections", -1)) != 0 or int(
        semantic.get("outbound_orders", -1)
    ) != 0:
        raise ForwardFeedManifestError("Forward boundaries cannot authorize orders.")
    for row in rows:
        freeze = _utc(str(row.get("freeze_timestamp_utc") or ""))
        roots = tuple(sorted({str(value) for value in row.get("required_roots") or []}))
        if not roots or not str(row.get("configuration_hash") or ""):
            raise ForwardFeedManifestError("Candidate boundary lacks roots/config hash.")
        if int(row.get("stale_data_seconds") or 0) <= 0:
            raise ForwardFeedManifestError("Candidate stale-data policy is invalid.")
        if freeze >= _utc(str(semantic["created_at_utc"])):
            raise ForwardFeedManifestError("Candidate freeze must precede manifest creation.")


def write_manifest(path: str | Path, payload: Mapping[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    if target.exists():
        if target.read_text(encoding="utf-8") != encoded:
            raise ForwardFeedManifestError(f"Immutable manifest drift: {target}")
        return target
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def _utc(value: datetime | str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ForwardFeedManifestError("Timezone-aware timestamps are required.")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "BOUNDARY_SCHEMA",
    "CALENDAR_SCHEMA",
    "SOURCE_SCHEMA",
    "ForwardFeedManifestError",
    "build_cme_calendar_manifest",
    "build_forward_boundary_manifest",
    "build_read_only_source_manifest",
    "calendar_from_manifest",
    "stable_hash",
    "validate_forward_boundary_manifest",
    "validate_read_only_source_manifest",
    "write_manifest",
]
