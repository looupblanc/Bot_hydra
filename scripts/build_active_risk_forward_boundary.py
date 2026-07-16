#!/usr/bin/env python3
"""Build the immutable six-book active-risk forward boundary."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from hydra.shadow.active_risk_forward_boundary import (
    build_active_risk_forward_boundary,
    build_databento_ingestion_boundary,
)
from hydra.shadow.forward_feed_manifest import write_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--exports-root",
        type=Path,
        default=Path(
            "reports/economic_evolution/"
            "active_risk_pool_target_velocity_0026_revision_02/forward_shadow"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("mission/state/forward_feed_manifests/active_risk_0026.json"),
    )
    parser.add_argument(
        "--ingestion-output",
        type=Path,
        default=Path(
            "mission/state/forward_feed_manifests/"
            "active_risk_0026_ingestion.json"
        ),
    )
    parser.add_argument("--created-at", default=None)
    args = parser.parse_args()

    root = args.root.resolve()
    exports = (root / args.exports_root).resolve()
    packages = sorted(exports.glob("*/shadow_package.json"))
    created = (
        datetime.fromisoformat(args.created_at.replace("Z", "+00:00"))
        if args.created_at
        else datetime.now(timezone.utc)
    )
    manifest = build_active_risk_forward_boundary(
        repository_root=root,
        package_paths=packages,
        created_at=created,
    )
    output = (root / args.output).resolve()
    write_manifest(output, manifest)
    ingestion = build_databento_ingestion_boundary(
        manifest,
        repository_root=root,
        created_at=created,
    )
    ingestion_output = (root / args.ingestion_output).resolve()
    write_manifest(ingestion_output, ingestion)
    print(
        json.dumps(
            {
                "path": str(output),
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "manifest_hash": manifest["manifest_hash"],
                "ingestion_path": str(ingestion_output),
                "ingestion_sha256": hashlib.sha256(
                    ingestion_output.read_bytes()
                ).hexdigest(),
                "ingestion_manifest_hash": ingestion["manifest_hash"],
                "candidate_count": manifest["candidate_count"],
                "all_sealed_export_receipts_verified": manifest[
                    "all_sealed_export_receipts_verified"
                ],
                "market_data_purchase_authorized": False,
                "broker_connections": 0,
                "outbound_orders": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
