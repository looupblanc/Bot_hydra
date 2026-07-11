#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Show HYDRA shadow status.")
    parser.add_argument("--state-dir", default="shadow/state")
    args = parser.parse_args()
    path = Path(args.state_dir) / "status.json"
    print(path.read_text(encoding="utf-8") if path.exists() else '{"status":"INACTIVE"}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
