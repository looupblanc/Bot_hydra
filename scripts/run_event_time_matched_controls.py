#!/usr/bin/env python3
"""Run and atomically persist the bounded event-time matched controls."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hydra.production.autonomous_event_time_matched_controls import (
    DEFAULT_SOURCE_COMPOSITE,
    run_event_time_matched_controls,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--source", default=str(DEFAULT_SOURCE_COMPOSITE))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    result = run_event_time_matched_controls(
        root, source_composite_path=args.source
    )
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output)
    print(
        json.dumps(
            {
                "status": result["status"],
                "control_verdict": result["control_verdict"],
                "result_hash": result["result_hash"],
                "output": str(output),
                "counts": result["counts"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
