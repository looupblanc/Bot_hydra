#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.shadow.monitoring import shadow_summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize HYDRA shadow events.")
    parser.add_argument("--events", default="shadow/state/events.jsonl")
    args = parser.parse_args()
    path = Path(args.events)
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []
    print(json.dumps(shadow_summary(rows), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
