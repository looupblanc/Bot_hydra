from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hydra.mission.calibration_retest_execution import _stable_hash
from hydra.shadow.specification import ShadowSpecification


class ShadowPipelineIntegrityError(RuntimeError):
    pass


def tick_shadow_pipeline(
    state_dir: str | Path,
    active_registry: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Refresh fail-closed shadow liveness without touching mission SQLite.

    The mission controller remains the only registry writer. This tick validates
    immutable activation/configuration contracts and never synthesizes a signal
    when a fresh forward-data heartbeat is absent.
    """
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    stopped = (root / "stop.request").exists()
    candidates: dict[str, dict[str, Any]] = {}
    for candidate_id, entry in sorted(active_registry.items()):
        candidates[candidate_id] = _candidate_runtime_state(
            candidate_id, entry, root=root, now=current, stopped=stopped
        )
    active = sum(
        item["operational_classification"] == "SHADOW_RESEARCH_ACTIVE"
        for item in candidates.values()
    )
    waiting = sum(
        item["operational_classification"] == "SHADOW_WAITING_FOR_FEED"
        for item in candidates.values()
    )
    complete = sum(
        item["operational_classification"] == "SHADOW_CONFIG_COMPLETE"
        for item in candidates.values()
    )
    status = {
        "schema": "hydra_shadow_pipeline_status_v1",
        "pipeline": "SHADOW",
        "status": "STOPPED_FAIL_CLOSED" if stopped else "RUNNING_FAIL_CLOSED",
        "updated_at_utc": current.isoformat(),
        "registered_candidates": len(candidates),
        "shadow_config_complete": complete,
        "shadow_waiting_for_feed": waiting,
        "shadow_research_active": active,
        "candidates": candidates,
        "forward_signals": 0,
        "virtual_fills": 0,
        "outbound_orders": 0,
        "broker_connections": 0,
        "one_writer": True,
    }
    _atomic_json(root / "status.json", status)
    _atomic_json(
        root / "heartbeat.json",
        {
            "pipeline": "SHADOW",
            "updated_at_utc": current.isoformat(),
            "status": status["status"],
            "shadow_research_active": active,
            "shadow_waiting_for_feed": waiting,
            "outbound_orders": 0,
        },
    )
    return status


def registry_entry_from_activation(result: dict[str, Any]) -> dict[str, Any]:
    manifest = dict(result.get("activation_manifest") or {})
    candidate_id = str(result.get("candidate_id") or manifest.get("candidate_id") or "")
    if not candidate_id or manifest.get("candidate_id") != candidate_id:
        raise ShadowPipelineIntegrityError("Activation result identity is incomplete.")
    expected = str(manifest.get("activation_manifest_hash") or "")
    unhashed = dict(manifest)
    unhashed.pop("activation_manifest_hash", None)
    if not expected or _stable_hash(unhashed) != expected:
        raise ShadowPipelineIntegrityError("Activation manifest hash does not recompute.")
    return {
        "candidate_id": candidate_id,
        "activation_manifest_hash": expected,
        "configuration_path": manifest["configuration_path"],
        "configuration_sha256": manifest["configuration_sha256"],
        "configuration_hash": manifest["configuration_hash"],
        "stale_data_seconds": int(manifest["stale_data_seconds"]),
        "operational_classification": "SHADOW_CONFIG_COMPLETE",
        "outbound_orders_enabled": False,
    }


def _candidate_runtime_state(
    candidate_id: str,
    entry: dict[str, Any],
    *,
    root: Path,
    now: datetime,
    stopped: bool,
) -> dict[str, Any]:
    if entry.get("candidate_id") != candidate_id or entry.get("outbound_orders_enabled"):
        raise ShadowPipelineIntegrityError("Active registry identity/order guard failed.")
    config_path = Path(str(entry.get("configuration_path") or ""))
    expected_sha = str(entry.get("configuration_sha256") or "")
    if not config_path.is_file() or hashlib.sha256(config_path.read_bytes()).hexdigest() != expected_sha:
        raise ShadowPipelineIntegrityError(f"Immutable configuration changed: {candidate_id}")
    specification = _load_specification(config_path)
    if specification.configuration_hash != entry.get("configuration_hash"):
        raise ShadowPipelineIntegrityError(f"Configuration semantic hash changed: {candidate_id}")
    heartbeat_path = root / "forward_data" / f"{candidate_id}.heartbeat.json"
    feed = _fresh_feed_status(
        heartbeat_path,
        now=now,
        stale_data_seconds=int(entry.get("stale_data_seconds") or specification.stale_data_seconds),
    )
    if stopped:
        runtime_state = "KILL_SWITCH_ACTIVE"
        classification = "SHADOW_STOPPED"
    elif not feed["fresh"]:
        runtime_state = "WAITING_FOR_FRESH_FORWARD_DATA"
        classification = "SHADOW_WAITING_FOR_FEED"
    else:
        runtime_state = "READY_FOR_VIRTUAL_SIGNALS"
        classification = "SHADOW_RESEARCH_ACTIVE"
    return {
        "operational_classification": classification,
        # The historical admission tier is retained as provenance; it is not an
        # operational liveness claim. Only a fresh feed earns ACTIVE above.
        "registry_evidence_tier": "SHADOW_ACTIVE",
        "runtime_state": runtime_state,
        "configuration_hash": specification.configuration_hash,
        "feed": feed,
        "signals": 0,
        "virtual_fills": 0,
        "outbound_orders": 0,
        "broker_connections": 0,
    }


def _fresh_feed_status(
    path: Path, *, now: datetime, stale_data_seconds: int
) -> dict[str, Any]:
    if not path.is_file():
        return {"fresh": False, "reason": "missing_forward_data_heartbeat", "age_seconds": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        observed = datetime.fromisoformat(str(payload["latest_completed_bar_at_utc"]))
        observed = observed.astimezone(timezone.utc)
        age = max(0.0, (now - observed).total_seconds())
    except (OSError, KeyError, ValueError, TypeError, json.JSONDecodeError):
        return {"fresh": False, "reason": "invalid_forward_data_heartbeat", "age_seconds": None}
    fresh = age <= stale_data_seconds
    return {
        "fresh": fresh,
        "reason": "fresh" if fresh else "stale_forward_data",
        "age_seconds": age,
        "latest_completed_bar_at_utc": observed.isoformat(),
    }


def _load_specification(path: Path) -> ShadowSpecification:
    payload = json.loads(path.read_text(encoding="utf-8"))
    supplied = payload.pop("configuration_hash", None)
    for key in ("feature_versions", "markets", "timeframes", "kill_conditions"):
        payload[key] = tuple(payload[key])
    specification = ShadowSpecification(**payload)
    specification.validate()
    if supplied != specification.configuration_hash:
        raise ShadowPipelineIntegrityError("Configuration file hash does not recompute.")
    return specification


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
