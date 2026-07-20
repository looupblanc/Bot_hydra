#!/usr/bin/env python3
"""Run and atomically persist the frozen CFTC grain crowding tripwire."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.cftc_grain_positioning_crowding import run_tripwire


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output",
        default="reports/research_tripwires/cftc_grain_positioning_crowding_v1/economic_result.json",
    )
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    result = run_tripwire(root)
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, output)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
