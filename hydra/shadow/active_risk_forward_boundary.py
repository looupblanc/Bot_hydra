"""Immutable forward boundary for the six selected active-risk books.

This is deliberately separate from the legacy ``ShadowSpecification``
boundary.  An active-risk book is a sealed, reconstructible package, not a
single-strategy configuration.  The boundary binds the exact package bytes,
package semantic hash, freeze time and complete signal/execution root set.
It grants no market-data purchase, broker or order authority.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from hydra.research.turbo_feature_builder import (
    FEATURE_BUNDLE_VERSION,
    FEATURE_DAG_HASH,
)
from hydra.shadow.active_risk_binding_loader import (
    EXPORT_RECEIPT_NAME,
    verify_active_risk_shadow_export,
)
from hydra.shadow.active_risk_package import (
    reconstruct_active_risk_shadow_package,
)
from hydra.shadow.forward_feed_manifest import stable_hash
from hydra.shadow.forward_feed_manifest import build_forward_boundary_manifest


BOUNDARY_SCHEMA = "hydra_active_risk_forward_boundary_v1"
SELECTED_BOOK_COUNT = 6


class ActiveRiskForwardBoundaryError(RuntimeError):
    """The active-risk forward boundary is incomplete or has drifted."""


def build_active_risk_forward_boundary(
    *,
    repository_root: str | Path,
    package_paths: Sequence[str | Path],
    created_at: datetime,
) -> dict[str, Any]:
    """Build a hash-bound boundary from exactly six reconstructible packages.

    When the package lives in a sealed export directory, the existing export
    receipt is verified and bound too.  The package hash remains mandatory in
    either case, so a boundary can also be exercised in isolated tests without
    fabricating a production export receipt.
    """

    root = Path(repository_root).resolve()
    rows: list[dict[str, Any]] = []
    for raw_path in package_paths:
        package_path = _inside(root, raw_path, label="active-risk package")
        if package_path.is_dir():
            package_path = package_path / "shadow_package.json"
        if not package_path.is_file():
            raise ActiveRiskForwardBoundaryError(
                f"active-risk package is missing: {package_path}"
            )
        payload = _json(package_path, label="active-risk package")
        reconstructed = reconstruct_active_risk_shadow_package(payload)
        package = reconstructed.package
        freeze = _utc(package.freeze_timestamp_utc)
        if freeze >= _utc(created_at):
            raise ActiveRiskForwardBoundaryError(
                "package freeze must precede boundary creation"
            )
        roots = _required_roots(package.market_policy)
        receipt_path = package_path.parent / EXPORT_RECEIPT_NAME
        receipt_binding: dict[str, Any] | None = None
        if receipt_path.is_file():
            receipt = verify_active_risk_shadow_export(package_path.parent)
            if (
                receipt.get("policy_id") != package.candidate_id
                or receipt.get("package_hash") != package.package_hash
                or receipt.get("freeze_timestamp_utc")
                != package.freeze_timestamp_utc
            ):
                raise ActiveRiskForwardBoundaryError(
                    f"sealed export differs from package: {package.candidate_id}"
                )
            receipt_binding = {
                "path": _relative(root, receipt_path),
                "sha256": _sha256(receipt_path),
                "receipt_hash": str(receipt["receipt_hash"]),
            }
        rows.append(
            {
                "candidate_id": package.candidate_id,
                "package_path": _relative(root, package_path),
                "package_sha256": _sha256(package_path),
                "package_hash": package.package_hash,
                "freeze_timestamp_utc": package.freeze_timestamp_utc,
                "required_roots": list(roots),
                "stale_data_seconds": int(package.data_policy["stale_after_seconds"]),
                "expected_bar_seconds": int(
                    package.data_policy["expected_bar_seconds"]
                ),
                "feature_bundle_version": FEATURE_BUNDLE_VERSION,
                "feature_dag_hash": FEATURE_DAG_HASH,
                "sealed_export_receipt": receipt_binding,
            }
        )
    rows.sort(key=lambda value: str(value["candidate_id"]))
    payload: dict[str, Any] = {
        "schema": BOUNDARY_SCHEMA,
        "created_at_utc": _utc(created_at).isoformat(),
        "candidate_count": len(rows),
        "candidates": rows,
        "package_contract": "HASH_BOUND_RECONSTRUCTIBLE_ACTIVE_RISK_PACKAGE",
        "all_sealed_export_receipts_verified": all(
            row["sealed_export_receipt"] is not None for row in rows
        ),
        "strictly_post_freeze_only": True,
        "pre_freeze_backfill_prohibited": True,
        "feature_threshold_recalibration_prohibited": True,
        "market_data_purchase_authorized": False,
        "q4_access_authorized": False,
        "broker_connections": 0,
        "outbound_orders": 0,
    }
    payload["manifest_hash"] = stable_hash(payload)
    validate_active_risk_forward_boundary(payload, repository_root=root)
    return payload


def validate_active_risk_forward_boundary(
    payload: Mapping[str, Any],
    *,
    repository_root: str | Path,
) -> None:
    """Recompute every manifest, package and optional export binding."""

    root = Path(repository_root).resolve()
    semantic = dict(payload)
    claimed = str(semantic.pop("manifest_hash", ""))
    rows = semantic.get("candidates")
    if (
        not claimed
        or claimed != stable_hash(semantic)
        or semantic.get("schema") != BOUNDARY_SCHEMA
        or semantic.get("package_contract")
        != "HASH_BOUND_RECONSTRUCTIBLE_ACTIVE_RISK_PACKAGE"
        or not isinstance(rows, list)
        or len(rows) != SELECTED_BOOK_COUNT
        or int(semantic.get("candidate_count", -1)) != SELECTED_BOOK_COUNT
        or semantic.get("strictly_post_freeze_only") is not True
        or semantic.get("pre_freeze_backfill_prohibited") is not True
        or semantic.get("feature_threshold_recalibration_prohibited") is not True
        or semantic.get("market_data_purchase_authorized") is not False
        or semantic.get("q4_access_authorized") is not False
        or int(semantic.get("broker_connections", -1)) != 0
        or int(semantic.get("outbound_orders", -1)) != 0
    ):
        raise ActiveRiskForwardBoundaryError(
            "active-risk forward boundary contract/hash drift"
        )
    created = _utc(str(semantic["created_at_utc"]))
    identifiers: set[str] = set()
    receipt_claims: list[bool] = []
    for raw_row in rows:
        if not isinstance(raw_row, Mapping):
            raise ActiveRiskForwardBoundaryError("boundary candidate row is malformed")
        row = dict(raw_row)
        candidate_id = str(row.get("candidate_id") or "")
        package_path = _inside(
            root, str(row.get("package_path") or ""), label="boundary package"
        )
        if not candidate_id or candidate_id in identifiers or not package_path.is_file():
            raise ActiveRiskForwardBoundaryError(
                "boundary package identity is missing or duplicated"
            )
        identifiers.add(candidate_id)
        if _sha256(package_path) != str(row.get("package_sha256") or ""):
            raise ActiveRiskForwardBoundaryError(
                f"boundary package bytes drifted: {candidate_id}"
            )
        reconstructed = reconstruct_active_risk_shadow_package(
            _json(package_path, label="boundary package")
        )
        package = reconstructed.package
        roots = _required_roots(package.market_policy)
        if (
            package.candidate_id != candidate_id
            or package.package_hash != row.get("package_hash")
            or package.freeze_timestamp_utc != row.get("freeze_timestamp_utc")
            or _utc(package.freeze_timestamp_utc) >= created
            or list(roots) != row.get("required_roots")
            or int(row.get("stale_data_seconds", 0))
            != int(package.data_policy["stale_after_seconds"])
            or int(row.get("expected_bar_seconds", 0)) != 60
            or row.get("feature_bundle_version") != FEATURE_BUNDLE_VERSION
            or row.get("feature_dag_hash") != FEATURE_DAG_HASH
        ):
            raise ActiveRiskForwardBoundaryError(
                f"boundary package semantic drift: {candidate_id}"
            )
        receipt_binding = row.get("sealed_export_receipt")
        receipt_claims.append(receipt_binding is not None)
        if receipt_binding is not None:
            if not isinstance(receipt_binding, Mapping):
                raise ActiveRiskForwardBoundaryError("sealed export binding is malformed")
            receipt_path = _inside(
                root,
                str(receipt_binding.get("path") or ""),
                label="sealed export receipt",
            )
            if (
                receipt_path != package_path.parent / EXPORT_RECEIPT_NAME
                or not receipt_path.is_file()
                or _sha256(receipt_path) != receipt_binding.get("sha256")
            ):
                raise ActiveRiskForwardBoundaryError(
                    f"sealed export receipt bytes drifted: {candidate_id}"
                )
            receipt = verify_active_risk_shadow_export(package_path.parent)
            if (
                receipt.get("receipt_hash") != receipt_binding.get("receipt_hash")
                or receipt.get("package_hash") != package.package_hash
            ):
                raise ActiveRiskForwardBoundaryError(
                    f"sealed export receipt semantic drift: {candidate_id}"
                )
    if bool(semantic.get("all_sealed_export_receipts_verified")) != all(
        receipt_claims
    ):
        raise ActiveRiskForwardBoundaryError(
            "sealed export coverage claim differs from candidate rows"
        )


def build_databento_ingestion_boundary(
    payload: Mapping[str, Any],
    *,
    repository_root: str | Path,
    created_at: datetime,
) -> dict[str, Any]:
    """Project the sealed active-risk boundary onto the generic bar ingestor.

    The generic Databento updater knows only configuration hashes and roots.
    This projection keeps the active-risk manifest as the richer authoritative
    boundary while binding the exact same package bytes/hash/freeze for the
    existing append-only bar-store writer.  It grants no broker/order path.
    Market-data spending authority remains an external operating-package guard.
    """

    root = Path(repository_root).resolve()
    validate_active_risk_forward_boundary(payload, repository_root=root)
    rows = []
    for raw in payload["candidates"]:
        row = dict(raw)
        rows.append(
            {
                "candidate_id": str(row["candidate_id"]),
                "configuration_path": str(
                    _inside(root, row["package_path"], label="ingestion package")
                ),
                "configuration_sha256": str(row["package_sha256"]),
                "configuration_hash": str(row["package_hash"]),
                "freeze_timestamp_utc": str(row["freeze_timestamp_utc"]),
                "required_roots": list(row["required_roots"]),
                "stale_data_seconds": int(row["stale_data_seconds"]),
            }
        )
    return build_forward_boundary_manifest(rows, created_at=created_at)


def _required_roots(market_policy: Mapping[str, Any]) -> tuple[str, ...]:
    sleeves = market_policy.get("sleeves")
    if not isinstance(sleeves, Mapping) or not sleeves:
        raise ActiveRiskForwardBoundaryError("package market policy has no sleeves")
    roots: set[str] = set()
    for raw in sleeves.values():
        if not isinstance(raw, Mapping):
            raise ActiveRiskForwardBoundaryError("package sleeve market row is malformed")
        for field in ("signal_market", "execution_market"):
            value = str(raw.get(field) or "").strip().upper()
            if not value:
                raise ActiveRiskForwardBoundaryError(
                    f"package sleeve lacks {field}"
                )
            roots.add(value)
    return tuple(sorted(roots))


def _json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActiveRiskForwardBoundaryError(f"{label} is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise ActiveRiskForwardBoundaryError(f"{label} is not an object: {path}")
    return value


def _inside(root: Path, raw: str | Path, *, label: str) -> Path:
    path = Path(raw)
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ActiveRiskForwardBoundaryError(f"{label} escapes repository root") from exc
    return resolved


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root).as_posix()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc(value: datetime | str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ActiveRiskForwardBoundaryError("boundary timestamps must be aware")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "BOUNDARY_SCHEMA",
    "SELECTED_BOOK_COUNT",
    "ActiveRiskForwardBoundaryError",
    "build_active_risk_forward_boundary",
    "build_databento_ingestion_boundary",
    "validate_active_risk_forward_boundary",
]
