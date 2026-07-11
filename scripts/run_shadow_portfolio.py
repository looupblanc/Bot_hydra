#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.shadow.specification import ShadowSpecification
from hydra.shadow.runner import ShadowRunner


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start a zero-order HYDRA shadow portfolio.")
    parser.add_argument("--configuration", required=True)
    parser.add_argument("--state-dir", default="shadow/state")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(Path(args.configuration).read_text(encoding="utf-8"))
    supplied_hash = payload.pop("configuration_hash", None)
    specification = ShadowSpecification(**_tuples(payload))
    specification.validate()
    if supplied_hash != specification.configuration_hash:
        raise RuntimeError("Shadow configuration hash mismatch.")
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    if (state_dir / "stop.request").exists():
        raise RuntimeError("Shadow stop request is active; fail-closed startup.")
    guard_state_path = state_dir / f"{specification.strategy_id}.prior_trade_guard.json"
    guard_marker_path = state_dir / f"{specification.strategy_id}.prior_trade_guard_initialized.json"
    guarded = "prior_trade_guard" in specification.entry_rules
    initialize_guard = bool(guarded and not guard_state_path.exists() and not guard_marker_path.exists())
    runner = ShadowRunner(
        specification,
        prior_trade_guard_state_path=guard_state_path if guarded else None,
        initialize_prior_trade_guard_genesis=initialize_guard,
    )
    if initialize_guard:
        marker = {
            "schema": "hydra_prior_trade_guard_initialization_v1",
            "strategy_id": specification.strategy_id,
            "configuration_hash": specification.configuration_hash,
            "guard_state_path": str(guard_state_path),
            "guard_state_sha256": hashlib.sha256(guard_state_path.read_bytes()).hexdigest(),
            "initialized_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        marker["marker_hash"] = hashlib.sha256(
            json.dumps(marker, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        guard_marker_path.write_text(
            json.dumps(marker, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    elif guarded and guard_marker_path.exists():
        marker = json.loads(guard_marker_path.read_text(encoding="utf-8"))
        supplied_marker_hash = str(marker.pop("marker_hash", ""))
        actual_marker_hash = hashlib.sha256(
            json.dumps(marker, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if supplied_marker_hash != actual_marker_hash:
            raise RuntimeError("Prior-trade guard initialization marker hash mismatch.")
        if marker.get("configuration_hash") != specification.configuration_hash:
            raise RuntimeError("Prior-trade guard initialization marker belongs to another config.")
    status = {
        "status": "SHADOW_ACTIVE",
        "strategy_id": specification.strategy_id,
        "configuration_hash": specification.configuration_hash,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "outbound_orders_enabled": False,
        "broker_connections": 0,
        "virtual_fills": 0,
        "prior_trade_guard_required": guarded,
        "prior_trade_guard_reconciliation": runner.prior_trade_guard_reconciliation,
        "prior_trade_guard_fail_closed": bool(
            guarded
            and runner.prior_trade_guard_reconciliation
            in {"MISSING_STATE_FAIL_CLOSED", "INVALID_RESTART_STATE_FAIL_CLOSED", "INVALID_AUDIT_CHAIN_FAIL_CLOSED"}
        ),
    }
    (state_dir / "status.json").write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(status, sort_keys=True))
    return 0


def _tuples(payload: dict[str, object]) -> dict[str, object]:
    for key in ("feature_versions", "markets", "timeframes", "kill_conditions"):
        payload[key] = tuple(payload[key])
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
