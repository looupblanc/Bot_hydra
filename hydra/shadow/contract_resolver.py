from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from hydra.data.contract_mapping import (
    ContractInfo,
    RollMap,
    is_unsafe_roll_window,
    load_roll_map,
    valid_outright_future_symbol,
)


EXPLICIT_MAP_PREFIX = "EXPLICIT_"


@dataclass(frozen=True)
class ResolvedContract:
    root: str
    contract: str
    instrument_id: str
    active_start: str
    active_end: str
    expiry_date: str
    tick_size: float
    point_value: float
    map_path: str
    map_sha256: str
    roll_map_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractResolution:
    status: str
    as_of_utc: str
    required_roots: tuple[str, ...]
    contracts: tuple[ResolvedContract, ...]
    missing_roots: tuple[str, ...]
    unsafe_roll_roots: tuple[str, ...]
    reason: str
    next_action: str | None
    inspected_maps: tuple[dict[str, Any], ...]

    @property
    def ready(self) -> bool:
        return self.status == "READY"

    def contract_for(self, root: str) -> ResolvedContract:
        matches = [item for item in self.contracts if item.root == root]
        if not self.ready or len(matches) != 1:
            raise RuntimeError(
                f"No safe current explicit contract is available for {root}: {self.status}"
            )
        return matches[0]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ready"] = self.ready
        return payload


def discover_roll_maps(folder: str | Path) -> tuple[Path, ...]:
    root = Path(folder)
    if not root.is_dir():
        return ()
    return tuple(sorted(root.glob("roll_map_*.json")))


def resolve_current_contracts(
    map_paths: Iterable[str | Path],
    roots: Iterable[str],
    *,
    as_of: datetime | str,
    block_unsafe_roll_window: bool = True,
) -> ContractResolution:
    """Resolve dated raw contracts without ever extrapolating a historical map.

    The historical ``active_contract`` helper intentionally returns the nearest
    segment outside coverage. That behavior is useful for diagnostics but is
    unsafe for a forward feed. This resolver requires an exact dated segment,
    explicit symbology, an instrument id and an unexpired outright contract.
    """

    timestamp = _utc(as_of)
    required = tuple(sorted({str(root).strip().upper() for root in roots if str(root).strip()}))
    if not required:
        return ContractResolution(
            status="SOURCE_REQUIRED",
            as_of_utc=timestamp.isoformat(),
            required_roots=(),
            contracts=(),
            missing_roots=(),
            unsafe_roll_roots=(),
            reason="no_required_roots",
            next_action="Provide the exact markets from immutable active shadow configurations.",
            inspected_maps=(),
        )

    inspected: list[dict[str, Any]] = []
    valid_maps: list[tuple[Path, RollMap, str, str]] = []
    integrity_errors: list[str] = []
    for raw_path in sorted({Path(path) for path in map_paths}, key=lambda item: str(item)):
        record: dict[str, Any] = {"path": str(raw_path)}
        try:
            raw = raw_path.read_bytes()
            document = json.loads(raw)
            roll_map = load_roll_map(raw_path)
            supplied_hash = str(document.get("roll_map_hash") or "")
            semantic_hash = roll_map.roll_map_hash()
            if not supplied_hash or supplied_hash != semantic_hash:
                overlaps_decision = any(
                    item.root in required
                    and _utc(item.active_start) <= timestamp < _utc(item.active_end)
                    for item in roll_map.contracts
                )
                record.update(
                    status=("INVALID_IN_SCOPE" if overlaps_decision else "INVALID_OUT_OF_SCOPE"),
                    error="ValueError:roll_map_hash_mismatch",
                    map_type=roll_map.map_type,
                    symbols=sorted(roll_map.symbols),
                )
                if overlaps_decision:
                    integrity_errors.append(str(raw_path))
                inspected.append(record)
                continue
            if not str(roll_map.map_type).startswith(EXPLICIT_MAP_PREFIX):
                record.update(
                    status="REJECTED_NON_EXPLICIT",
                    map_type=roll_map.map_type,
                    symbols=sorted(roll_map.symbols),
                )
            else:
                digest = hashlib.sha256(raw).hexdigest()
                record.update(
                    status="INSPECTED_EXPLICIT",
                    map_type=roll_map.map_type,
                    symbols=sorted(roll_map.symbols),
                    roll_map_hash=semantic_hash,
                    file_sha256=digest,
                )
                valid_maps.append((raw_path, roll_map, digest, semantic_hash))
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            record.update(status="INVALID", error=f"{type(exc).__name__}:{exc}")
            integrity_errors.append(str(raw_path))
        inspected.append(record)

    candidates: dict[str, list[ResolvedContract]] = {root: [] for root in required}
    unsafe_roots: set[str] = set()
    for path, roll_map, file_hash, semantic_hash in valid_maps:
        for root in required:
            exact = [
                item
                for item in roll_map.contracts
                if item.root == root
                and _utc(item.active_start) <= timestamp < _utc(item.active_end)
            ]
            for item in exact:
                if not _contract_is_forward_safe(item, root=root, as_of=timestamp):
                    integrity_errors.append(f"{path}:{root}:{item.contract}")
                    continue
                if block_unsafe_roll_window and is_unsafe_roll_window(
                    roll_map, root, timestamp
                ):
                    unsafe_roots.add(root)
                candidates[root].append(
                    ResolvedContract(
                        root=root,
                        contract=item.contract,
                        instrument_id=str(item.instrument_id),
                        active_start=_utc(item.active_start).isoformat(),
                        active_end=_utc(item.active_end).isoformat(),
                        expiry_date=item.expiry_date,
                        tick_size=float(item.tick_size),
                        point_value=float(item.point_value),
                        map_path=str(path),
                        map_sha256=file_hash,
                        roll_map_hash=semantic_hash,
                    )
                )

    resolved: list[ResolvedContract] = []
    ambiguous: list[str] = []
    for root in required:
        options = candidates[root]
        identities = {
            (item.contract, item.instrument_id, item.active_start, item.active_end)
            for item in options
        }
        if len(identities) > 1:
            ambiguous.append(root)
        elif options:
            # Several immutable maps can carry the same exact segment. Prefer
            # the lexicographically latest semantic map while retaining hashes.
            resolved.append(sorted(options, key=lambda item: (item.roll_map_hash, item.map_path))[-1])

    missing = tuple(root for root in required if not candidates[root])
    if ambiguous or integrity_errors:
        return ContractResolution(
            status="INTEGRITY_BLOCKED",
            as_of_utc=timestamp.isoformat(),
            required_roots=required,
            contracts=tuple(sorted(resolved, key=lambda item: item.root)),
            missing_roots=missing,
            unsafe_roll_roots=tuple(sorted(unsafe_roots)),
            reason=(
                "ambiguous_explicit_contracts:" + ",".join(sorted(ambiguous))
                if ambiguous
                else "invalid_explicit_contract_artifact"
            ),
            next_action="Repair or replace the conflicting dated definition/symbology artifact.",
            inspected_maps=tuple(inspected),
        )
    if missing:
        names = ",".join(missing)
        return ContractResolution(
            status="SOURCE_REQUIRED",
            as_of_utc=timestamp.isoformat(),
            required_roots=required,
            contracts=tuple(sorted(resolved, key=lambda item: item.root)),
            missing_roots=missing,
            unsafe_roll_roots=tuple(sorted(unsafe_roots)),
            reason=f"no_exact_dated_explicit_contract_coverage:{names}",
            next_action=(
                "Acquire or cache current dated definitions plus continuous-to-raw symbology "
                f"for {names}; verify read-only market-data entitlement and cost before connecting."
            ),
            inspected_maps=tuple(inspected),
        )
    if unsafe_roots:
        return ContractResolution(
            status="ROLL_TRANSITION_BLOCKED",
            as_of_utc=timestamp.isoformat(),
            required_roots=required,
            contracts=tuple(sorted(resolved, key=lambda item: item.root)),
            missing_roots=(),
            unsafe_roll_roots=tuple(sorted(unsafe_roots)),
            reason="explicit_contract_is_inside_preregistered_unsafe_roll_window",
            next_action="Wait for or explicitly validate the current roll transition before publishing bars.",
            inspected_maps=tuple(inspected),
        )
    return ContractResolution(
        status="READY",
        as_of_utc=timestamp.isoformat(),
        required_roots=required,
        contracts=tuple(sorted(resolved, key=lambda item: item.root)),
        missing_roots=(),
        unsafe_roll_roots=(),
        reason="exact_dated_explicit_contracts_resolved",
        next_action=None,
        inspected_maps=tuple(inspected),
    )


def _contract_is_forward_safe(
    contract: ContractInfo, *, root: str, as_of: datetime
) -> bool:
    if not contract.instrument_id or not valid_outright_future_symbol(root, contract.contract):
        return False
    if float(contract.tick_size) <= 0 or float(contract.point_value) <= 0:
        return False
    expiry = _utc(contract.expiry_date)
    return expiry.date() >= as_of.date()


def _utc(value: datetime | str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
