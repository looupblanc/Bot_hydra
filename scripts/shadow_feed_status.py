#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.shadow.contract_resolver import discover_roll_maps, resolve_current_contracts
from hydra.shadow.feed_health import assess_feed_health
from hydra.shadow.forward_bar_store import (
    CmeSessionCalendar,
    ForwardBarStore,
    MarketClosure,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit the local fail-closed HYDRA forward shadow feed without networking."
    )
    parser.add_argument("--state-dir", default="shadow/state")
    parser.add_argument("--contract-map-dir", default="data/cache/contract_maps")
    parser.add_argument("--activation-root", default="reports/mission_experiments")
    parser.add_argument("--roots", default="", help="Comma-separated override for offline tests.")
    parser.add_argument("--as-of", default=None, help="Aware ISO timestamp; defaults to current UTC.")
    parser.add_argument("--max-age-seconds", type=int, default=75)
    parser.add_argument("--source-authorization-manifest", default=None)
    parser.add_argument("--session-calendar-manifest", default=None)
    args = parser.parse_args()

    now = _utc(args.as_of) if args.as_of else datetime.now(timezone.utc)
    state_dir = Path(args.state_dir)
    explicit_roots = tuple(
        sorted({value.strip().upper() for value in args.roots.split(",") if value.strip()})
    )
    active = _discover_active_markets(
        state_dir / "status.json", Path(args.activation_root)
    )
    roots = explicit_roots or tuple(sorted({root for values in active.values() for root in values}))
    maps = discover_roll_maps(args.contract_map_dir)
    resolution = resolve_current_contracts(maps, roots, as_of=now)
    authorization = _authorization_status(args.source_authorization_manifest, now=now)
    calendar, calendar_status = _load_calendar(args.session_calendar_manifest)
    store = ForwardBarStore(state_dir / "forward_data" / "forward_bars.db", calendar=calendar)
    health = assess_feed_health(
        resolution,
        store,
        now=now,
        max_age_seconds=args.max_age_seconds,
        source_authorization_proven=authorization["proven"],
    )
    conclusion = (
        "FORWARD_DATA_SOURCE_REQUIRED"
        if health.status == "SOURCE_REQUIRED"
        else f"FORWARD_FEED_{health.status}"
    )
    payload: dict[str, Any] = {
        "schema": "hydra_shadow_forward_feed_status_v1",
        "scientific_conclusion": conclusion,
        "checked_at_utc": now.isoformat(),
        "status": health.status,
        "mission_blocker": health.mission_blocker,
        "active_shadow_candidates": active,
        "required_roots": list(roots),
        "roll_maps_inspected": len(maps),
        "source_authorization": authorization,
        "session_calendar": calendar_status,
        "feed_health": health.to_dict(),
        "candidate_heartbeats_published": 0,
        "network_requests": 0,
        "incremental_databento_spend_usd": 0.0,
        "broker_connections": 0,
        "outbound_orders": 0,
        "shadow_policy": "FAIL_CLOSED_NO_SIGNAL_NO_FILL",
        "next_action": (
            resolution.next_action
            if resolution.status == "SOURCE_REQUIRED"
            else (
                "Create a checksummed read-only entitlement manifest without exposing credentials."
                if not authorization["proven"]
                else health.reason
            )
        ),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _discover_active_markets(
    status_path: Path, activation_root: Path
) -> dict[str, list[str]]:
    if not status_path.is_file() or not activation_root.is_dir():
        return {}
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    candidate_ids = set((status.get("candidates") or {}).keys())
    discovered: dict[str, set[str]] = {candidate_id: set() for candidate_id in candidate_ids}
    for path in sorted(activation_root.glob("**/*activation_manifest*.json")):
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        candidate_id = str(manifest.get("candidate_id") or "")
        if candidate_id in discovered:
            discovered[candidate_id].update(str(item) for item in manifest.get("markets") or [])
    return {
        candidate_id: sorted(markets)
        for candidate_id, markets in sorted(discovered.items())
    }


def _authorization_status(path: str | None, *, now: datetime) -> dict[str, Any]:
    if not path:
        return {
            "proven": False,
            "reason": "no_offline_read_only_entitlement_manifest",
            "credentials_read": False,
        }
    target = Path(path)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        supplied_hash = str(payload.pop("manifest_hash"))
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        recomputed = hashlib.sha256(raw.encode()).hexdigest()
        valid_through = date.fromisoformat(str(payload["valid_through"]))
        passed = bool(
            supplied_hash == recomputed
            and payload.get("schema") == "hydra_read_only_market_data_authorization_v1"
            and payload.get("dataset") == "GLBX.MDP3"
            and payload.get("market_data_read_only") is True
            and payload.get("outbound_orders") == 0
            and payload.get("broker_connections") == 0
            and valid_through >= now.date()
        )
        return {
            "proven": passed,
            "reason": "verified_offline_manifest" if passed else "invalid_or_expired_manifest",
            "manifest_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            "credentials_read": False,
        }
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return {
            "proven": False,
            "reason": "invalid_offline_read_only_entitlement_manifest",
            "credentials_read": False,
        }


def _load_calendar(path: str | None) -> tuple[CmeSessionCalendar, dict[str, Any]]:
    if not path:
        calendar = CmeSessionCalendar.weekly_only()
        return calendar, {
            "verified": False,
            "version": calendar.version,
            "reason": "no_current_holiday_calendar_manifest",
        }
    target = Path(path)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        supplied_hash = str(payload.pop("manifest_hash"))
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if supplied_hash != hashlib.sha256(raw.encode()).hexdigest():
            raise ValueError("calendar manifest hash mismatch")
        if payload.get("schema") != "hydra_cme_session_calendar_v1":
            raise ValueError("calendar schema mismatch")
        calendar = CmeSessionCalendar(
            version=str(payload["version"]),
            holiday_schedule_through=date.fromisoformat(str(payload["valid_through"])),
            closures=tuple(
                MarketClosure(
                    start_at_utc=_utc(item["start_at_utc"]),
                    end_at_utc=_utc(item["end_at_utc"]),
                    reason=str(item["reason"]),
                )
                for item in payload.get("closures") or []
            ),
        )
        return calendar, {
            "verified": True,
            "version": calendar.version,
            "valid_through": calendar.holiday_schedule_through.isoformat(),
            "manifest_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        calendar = CmeSessionCalendar.weekly_only()
        return calendar, {
            "verified": False,
            "version": calendar.version,
            "reason": "invalid_current_holiday_calendar_manifest",
        }


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("--as-of must include an explicit timezone.")
    return parsed.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
