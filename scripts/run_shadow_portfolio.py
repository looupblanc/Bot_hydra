#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from hydra.shadow.specification import ShadowSpecification


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
    status = {
        "status": "SHADOW_ACTIVE",
        "strategy_id": specification.strategy_id,
        "configuration_hash": specification.configuration_hash,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "outbound_orders_enabled": False,
        "broker_connections": 0,
        "virtual_fills": 0,
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
