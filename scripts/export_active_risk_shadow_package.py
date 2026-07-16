#!/usr/bin/env python3
"""Export one selected active-risk book after both immutable seals exist."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.shadow.active_risk_binding_loader import (
    EXPORT_RECEIPT_NAME,
    build_active_risk_package_from_sealed_selection,
    seal_active_risk_shadow_export,
    verify_active_risk_shadow_export,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--campaign-manifest",
        type=Path,
        default=Path("config/v7/active_risk_pool_target_velocity_0026_revision_02.json"),
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
        ),
    )
    parser.add_argument(
        "--selection-dir",
        type=Path,
        default=Path(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02"
        ),
    )
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    root = args.root.resolve()
    output_root = (root / args.output_dir).resolve()
    if (output_root / EXPORT_RECEIPT_NAME).is_file():
        receipt = verify_active_risk_shadow_export(output_root)
        if receipt.get("policy_id") != args.policy_id:
            raise RuntimeError("existing forward export belongs to another policy")
        print(json.dumps(receipt, sort_keys=True))
        return 0
    package, audit = build_active_risk_package_from_sealed_selection(
        repository_root=root,
        campaign_manifest_path=args.campaign_manifest,
        report_dir=args.report_dir,
        selection_dir=args.selection_dir,
        policy_id=args.policy_id,
    )
    receipt = seal_active_risk_shadow_export(
        package,
        audit,
        output_dir=output_root,
    )
    print(
        json.dumps(
            {
                "policy_id": args.policy_id,
                "binding_count": audit["binding_count"],
                "binding_audit_hash": audit["audit_hash"],
                "package_hash": package.package_hash,
                "freeze_timestamp_utc": package.freeze_timestamp_utc,
                "export_receipt_hash": receipt["receipt_hash"],
                "output_dir": str(output_root),
                "broker_connectivity": False,
                "outbound_order_capability": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
