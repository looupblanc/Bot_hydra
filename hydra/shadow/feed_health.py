from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hydra.shadow.contract_resolver import ContractResolution
from hydra.shadow.forward_bar_store import ForwardBarStore


@dataclass(frozen=True)
class FeedHealth:
    status: str
    checked_at_utc: str
    reason: str
    mission_blocker: str | None
    market_state: str
    source_authorization_proven: bool
    contract_resolution: dict[str, Any]
    store: dict[str, Any]
    roots: tuple[dict[str, Any], ...]
    can_publish_candidate_heartbeat: bool
    outbound_orders: int = 0
    broker_connections: int = 0
    network_requests: int = 0
    incremental_data_spend_usd: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_feed_health(
    resolution: ContractResolution,
    store: ForwardBarStore,
    *,
    now: datetime,
    max_age_seconds: int,
    source_authorization_proven: bool,
) -> FeedHealth:
    current = _utc(now)
    contract_payload = resolution.to_dict()
    store_payload = store.summary()
    market_state = store.calendar.market_state(current)
    base = {
        "checked_at_utc": current.isoformat(),
        "market_state": market_state,
        "source_authorization_proven": bool(source_authorization_proven),
        "contract_resolution": contract_payload,
        "store": store_payload,
    }

    if resolution.status == "SOURCE_REQUIRED":
        return FeedHealth(
            status="SOURCE_REQUIRED",
            reason=resolution.reason,
            mission_blocker="FORWARD_DATA_SOURCE_REQUIRED",
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if resolution.status != "READY":
        return FeedHealth(
            status="INTEGRITY_BLOCKED",
            reason=f"contract_resolution:{resolution.status}:{resolution.reason}",
            mission_blocker="FORWARD_DATA_INTEGRITY_BLOCKED",
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if not source_authorization_proven:
        return FeedHealth(
            status="SOURCE_REQUIRED",
            reason="read_only_current_market_data_entitlement_not_proven_offline",
            mission_blocker="FORWARD_DATA_SOURCE_REQUIRED",
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if not store.calendar.holiday_coverage_verified or not store.calendar.covers(current):
        return FeedHealth(
            status="SOURCE_REQUIRED",
            reason="current_cme_holiday_calendar_coverage_not_proven",
            mission_blocker="FORWARD_DATA_SOURCE_REQUIRED",
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if market_state != "OPEN":
        return FeedHealth(
            status="MARKET_CLOSED",
            reason=market_state.lower(),
            mission_blocker=None,
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if not store_payload.get("exists"):
        return FeedHealth(
            status="WAITING_FOR_FIRST_BAR",
            reason="forward_bar_store_missing",
            mission_blocker=None,
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )
    if store_payload.get("sqlite_integrity") != "ok" or int(
        store_payload.get("missing_bar_count") or 0
    ):
        return FeedHealth(
            status="INTEGRITY_BLOCKED",
            reason=(
                "forward_store_integrity_failed"
                if store_payload.get("sqlite_integrity") != "ok"
                else "missing_forward_bars_detected"
            ),
            mission_blocker="FORWARD_DATA_INTEGRITY_BLOCKED",
            roots=(),
            can_publish_candidate_heartbeat=False,
            **base,
        )

    latest = store.latest_by_root()
    root_status: list[dict[str, Any]] = []
    stale_or_missing = False
    integrity_failure = False
    for expected in resolution.contracts:
        observed = latest.get(expected.root)
        if not observed:
            stale_or_missing = True
            root_status.append(
                {
                    "root": expected.root,
                    "contract": expected.contract,
                    "status": "MISSING",
                    "age_seconds": None,
                }
            )
            continue
        if observed.get("contract") != expected.contract:
            integrity_failure = True
            root_status.append(
                {
                    "root": expected.root,
                    "contract": expected.contract,
                    "observed_contract": observed.get("contract"),
                    "status": "WRONG_CONTRACT",
                    "age_seconds": None,
                }
            )
            continue
        completed = _utc(str(observed["bar_close_at_utc"]))
        age = (current - completed).total_seconds()
        fresh = 0 <= age <= max_age_seconds
        stale_or_missing = stale_or_missing or not fresh
        root_status.append(
            {
                "root": expected.root,
                "contract": expected.contract,
                "status": "FRESH" if fresh else "STALE_OR_FUTURE",
                "age_seconds": age,
                "latest_completed_bar_at_utc": completed.isoformat(),
                "source_sequence": int(observed["source_sequence"]),
            }
        )
    if integrity_failure:
        status = "INTEGRITY_BLOCKED"
        reason = "forward_bar_contract_differs_from_current_explicit_map"
        blocker = "FORWARD_DATA_INTEGRITY_BLOCKED"
    elif stale_or_missing:
        status = "STALE"
        reason = "one_or_more_required_markets_lack_a_fresh_completed_bar"
        blocker = None
    else:
        status = "READY"
        reason = "all_required_explicit_contracts_have_fresh_complete_bars"
        blocker = None
    return FeedHealth(
        status=status,
        reason=reason,
        mission_blocker=blocker,
        roots=tuple(root_status),
        can_publish_candidate_heartbeat=status == "READY",
        **base,
    )


def write_feed_health(path: str | Path, health: FeedHealth) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(health.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)
    return target


def _utc(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Feed-health timestamps must be timezone aware.")
    return parsed.astimezone(timezone.utc)
