#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail-close a HYDRA shadow portfolio.")
    parser.add_argument("--state-dir", default="shadow/state")
    args = parser.parse_args()
    state = Path(args.state_dir)
    state.mkdir(parents=True, exist_ok=True)
    (state / "stop.request").write_text(datetime.now(timezone.utc).isoformat() + "\n")
    print("SHADOW_STOP_REQUESTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
