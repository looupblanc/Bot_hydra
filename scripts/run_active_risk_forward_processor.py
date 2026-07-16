#!/usr/bin/env python3
"""Run one local, zero-order active-risk forward processing pass."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from hydra.shadow.active_risk_forward_processor import (
    run_active_risk_forward_processor,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--boundary-manifest", required=True)
    parser.add_argument("--boundary-manifest-sha256", required=True)
    parser.add_argument(
        "--forward-store", default="shadow/state/forward_data/forward_bars.db"
    )
    parser.add_argument("--state-dir", default="shadow/state")
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    observed = (
        datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        if args.as_of
        else datetime.now(timezone.utc)
    )
    result = run_active_risk_forward_processor(
        repository_root=args.root,
        boundary_manifest_path=args.boundary_manifest,
        boundary_manifest_sha256=args.boundary_manifest_sha256,
        forward_store_path=args.forward_store,
        state_dir=args.state_dir,
        observed_at=observed,
    )
    if args.output:
        output = (Path(args.root).resolve() / args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, output)
        finally:
            temporary.unlink(missing_ok=True)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
