#!/usr/bin/env python3
"""Seal HYDRA Operating Package V1 from already-frozen evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.operating.package_v1 import (  # noqa: E402
    build_operating_package_v1,
    sha256_file,
    stable_hash,
    write_operating_package,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--selection",
        default=(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_"
            "revision_02/frozen_book_selection_revision_02.json"
        ),
    )
    parser.add_argument(
        "--decision-report",
        default=(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_"
            "revision_02/decision_report_revision_02.json"
        ),
    )
    parser.add_argument(
        "--forward-root",
        default=(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_"
            "revision_02/forward_shadow"
        ),
    )
    parser.add_argument(
        "--redundancy-audit",
        default="reports/operating/hydra_operating_package_v1/redundancy_audit.json",
    )
    parser.add_argument(
        "--post-payout-frontier",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "xfa_post_payout_frontier.json"
        ),
    )
    parser.add_argument(
        "--active-boundary",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "active_risk_forward_boundary.json"
        ),
    )
    parser.add_argument(
        "--ingestion-boundary",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "active_risk_ingestion_boundary.json"
        ),
    )
    parser.add_argument(
        "--forward-update",
        default=(
            "reports/operating/hydra_operating_package_v1/forward_append_0001/"
            "forward_update_result.json"
        ),
    )
    parser.add_argument(
        "--forward-processor",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "forward_processor_result.json"
        ),
    )
    parser.add_argument(
        "--output",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "OPERATING_PACKAGE_V1.json"
        ),
    )
    parser.add_argument(
        "--report",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "OPERATING_PACKAGE_V1.md"
        ),
    )
    parser.add_argument(
        "--receipt",
        default=(
            "reports/operating/hydra_operating_package_v1/"
            "OPERATING_PACKAGE_V1_seal_receipt.json"
        ),
    )
    parser.add_argument("--created-at", default=None)
    parser.add_argument("--source-commit", default=None)
    args = parser.parse_args()

    root = args.root.resolve()
    read = lambda value: _json(_resolve(root, value))
    redundancy = read(args.redundancy_audit)
    frontier = read(args.post_payout_frontier)
    active_boundary_path = _resolve(root, args.active_boundary)
    ingestion_boundary_path = _resolve(root, args.ingestion_boundary)
    forward_update_path = _resolve(root, args.forward_update)
    forward_processor_path = _resolve(root, args.forward_processor)
    active_boundary = _json(active_boundary_path)
    ingestion_boundary = _json(ingestion_boundary_path)
    forward_update = _json(forward_update_path)
    forward_processor = _json(forward_processor_path)
    created = (
        datetime.fromisoformat(args.created_at.replace("Z", "+00:00"))
        if args.created_at
        else datetime.now(timezone.utc)
    )
    source_commit = args.source_commit or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    binding = {
        "active_risk_boundary": _binding(root, active_boundary_path),
        "active_risk_boundary_manifest_hash": active_boundary["manifest_hash"],
        "ingestion_boundary": _binding(root, ingestion_boundary_path),
        "ingestion_boundary_manifest_hash": ingestion_boundary["manifest_hash"],
        "required_roots": sorted(
            {
                root_name
                for row in active_boundary["candidates"]
                for root_name in row["required_roots"]
            }
        ),
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "post_freeze_only": True,
        "pre_freeze_price_backfill_authorized": False,
        "maximum_initial_spend_usd": 10.0,
        "minimum_budget_reserve_usd": 25.0,
        "latest_update": {
            **_binding(root, forward_update_path),
            "scientific_conclusion": forward_update["scientific_conclusion"],
            "checked_at_utc": forward_update["checked_at_utc"],
            "available_end_utc": forward_update["available_end_utc"],
            "fresh_forward_bars_processed": int(
                forward_update["fresh_forward_bars_processed"]
            ),
            "incremental_spend_usd": float(
                forward_update["incremental_databento_spend_usd"]
            ),
            "remaining_budget_usd": float(
                forward_update["remaining_databento_budget_usd"]
            ),
        },
        "latest_processor": {
            **_binding(root, forward_processor_path),
            "scientific_conclusion": forward_processor["scientific_conclusion"],
            "events_appended": int(forward_processor["events_appended"]),
            "signals_emitted": int(forward_processor["signals_emitted"]),
            "virtual_fills_created": int(
                forward_processor["virtual_fills_created"]
            ),
            "account_mutations": int(forward_processor["account_mutations"]),
        },
        "signal_engine_status": (
            "FAIL_CLOSED_UNTIL_ONLINE_FEATURE_EQUIVALENCE_IS_PROVEN"
        ),
    }
    package = build_operating_package_v1(
        project_root=root,
        selection_path=args.selection,
        decision_report_path=args.decision_report,
        forward_root=args.forward_root,
        redundancy_audit=redundancy,
        post_payout_frontier=frontier,
        forward_boundary=binding,
        source_commit=source_commit,
        created_at=created,
    )
    output = _resolve(root, args.output)
    write_operating_package(output, package)
    report_path = _resolve(root, args.report)
    _atomic_text(report_path, _render_report(package, frontier))
    receipt = {
        "schema": "hydra_operating_package_v1_seal_receipt",
        "sealed_at_utc": created.astimezone(timezone.utc).isoformat(),
        "manifest_path": output.relative_to(root).as_posix(),
        "manifest_sha256": sha256_file(output),
        "manifest_hash": package["manifest_hash"],
        "report_path": report_path.relative_to(root).as_posix(),
        "report_sha256": sha256_file(report_path),
        "artifacts": {
            "OPERATING_PACKAGE_V1.json": {
                "relative_path": "OPERATING_PACKAGE_V1.json",
                "sha256": sha256_file(output),
                "size_bytes": output.stat().st_size,
            },
            "OPERATING_PACKAGE_V1.md": {
                "relative_path": "OPERATING_PACKAGE_V1.md",
                "sha256": sha256_file(report_path),
                "size_bytes": report_path.stat().st_size,
            },
        },
        "publication_contract": {
            "manifest_and_report_written_before_receipt": True,
            "receipt_is_commit_marker": True,
            "immutable": True,
        },
        "source_commit": source_commit,
        "book_count": 6,
        "broker_connections": 0,
        "outbound_orders": 0,
        "paper_shadow_ready": False,
    }
    receipt["receipt_hash"] = stable_hash(receipt)
    receipt_path = _resolve(root, args.receipt)
    _atomic_json(receipt_path, receipt)
    print(
        json.dumps(
            {
                "manifest_path": str(output),
                "manifest_sha256": sha256_file(output),
                "manifest_hash": package["manifest_hash"],
                "report_path": str(report_path),
                "receipt_path": str(receipt_path),
                "receipt_hash": receipt["receipt_hash"],
                "roles": {
                    row["role"]: row["policy_id"]
                    for row in package["role_rule"]["assignments"]
                },
                "selected_xfa_paths": {
                    row["policy_id"]: row["selected_xfa_path"]
                    for row in package["books"]
                },
            },
            sort_keys=True,
        )
    )
    return 0


def _render_report(package: Mapping[str, Any], frontier: Mapping[str, Any]) -> str:
    roles = package["role_rule"]["assignments"]
    lines = [
        "# HYDRA Operating Package V1",
        "",
        f"- Manifest hash: `{package['manifest_hash']}`",
        "- Evidence status: development-selected; append-only confirmation pending",
        "- Complete-book stacking: prohibited",
        "- Broker connections / orders: `0 / 0`",
        "",
        "## Frozen roles",
        "",
    ]
    lines.extend(
        f"- `{row['role']}`: `{row['policy_id']}` — {row['reason']}"
        for row in roles
    )
    lines.extend(
        [
            "",
            "## Book evidence and selected XFA alternative",
            "",
            "| Book | Role | N passes | S passes | S net USD | Min S MLL buffer | Std EV/attempt | Cons EV/attempt | XFA | Post-payout profile |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in package["books"]:
        normal = row["combine_evidence"]["normal"]
        stressed = row["combine_evidence"]["stressed_1_5x"]
        standard = row["xfa_path_comparison_stressed"]["standard"]
        consistency = row["xfa_path_comparison_stressed"]["consistency"]
        profile = row["selected_post_payout_profile"]
        lines.append(
            "| `{}` | {} | {}/{} | {}/{} | {:.2f} | {:.2f} | {:.2f} | {:.2f} | {} | {} |".format(
                row["policy_id"],
                row["operating_role"],
                normal["passes"], normal["starts"],
                stressed["passes"], stressed["starts"],
                stressed["net_pnl_usd"],
                stressed["minimum_mll_buffer_usd"],
                standard["expected_trader_payout_per_new_combine_attempt_usd"],
                consistency["expected_trader_payout_per_new_combine_attempt_usd"],
                row["selected_xfa_path"],
                profile.get("profile_id") or profile.get("profile_family"),
            )
        )
    lines.extend(
        [
            "",
            "## Selected post-payout frontier (stressed 1.5x)",
            "",
            "| Book | Profile | P(>=2|XFA) | P(>=2)/attempt | Cycles/XFA | Cycles/attempt | Survival 30d | 60d | 90d | EV/XFA | EV/new attempt |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in package["books"]:
        profile = row["selected_post_payout_profile"]
        stressed = profile["summary"]["stressed_1_5x"]
        per_attempt = profile["derived_per_new_combine_attempt"]["stressed_1_5x"]
        horizons = stressed["post_payout_survival_horizons"]
        lines.append(
            "| `{}` | {} | {:.2%} | {:.2%} | {:.3f} | {:.3f} | {} | {} | {} | {:.2f} | {:.2f} |".format(
                row["policy_id"],
                profile["profile_family"],
                stressed["probability_at_least_two_payouts"],
                per_attempt["probability_at_least_two_payouts"],
                stressed["payout_cycles_per_transition"],
                per_attempt["expected_payout_cycles"],
                _rate(horizons["30"]["survival_rate"]),
                _rate(horizons["60"]["survival_rate"]),
                _rate(horizons["90"]["survival_rate"]),
                stressed["expected_trader_net_payout_per_transition"],
                per_attempt["expected_trader_net_payout_usd"],
            )
        )
    forward = package["forward_data_binding"]
    update = forward["latest_update"]
    processor = forward["latest_processor"]
    lines.extend(
        [
            "",
            "## Redundancy qualification",
            "",
            "All six books share the same 18 immutable sleeves. Their differences are governor/path differences, not six independent alpha sources.",
            "",
            "## Append-only forward state",
            "",
            f"- Feed: `{update['scientific_conclusion']}`",
            f"- Available end: `{update['available_end_utc']}`",
            f"- Fresh bars / signals / virtual fills: `{update['fresh_forward_bars_processed']} / {processor['signals_emitted']} / {processor['virtual_fills_created']}`",
            f"- Initial spend / remaining budget: `${update['incremental_spend_usd']:.9f}` / `${update['remaining_budget_usd']:.6f}`",
            f"- Processor: `{processor['scientific_conclusion']}`",
            "- Exact blocker: genuine post-freeze bars and proven online feature/signal equivalence.",
            "",
            "## Safety classification",
            "",
            "No book is PAPER_SHADOW_READY. All launch designations are research selections pending sequential forward gates.",
            "",
            "## B2 specialist lane",
            "",
            "Reserved at <=10% compute; no specialist is admitted until positive B2, low-overlap, outside-B2 and new-book-simulation gates all pass. It does not alter or delay the six frozen books.",
            "",
        ]
    )
    return "\n".join(lines)


def _rate(value: Any) -> str:
    return "n/e" if value is None else f"{float(value):.2%}"


def _binding(root: Path, path: Path) -> dict[str, str]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": sha256_file(path),
    }


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"JSON artifact is not an object: {path}")
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise RuntimeError(f"Immutable operating artifact drift: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
