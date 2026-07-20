#!/usr/bin/env python3
"""Run the frozen no-purchase scheduled-release OCO preflight."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hydra.research.direction_neutral_release_bracket import run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output-dir",
        default="reports/research_tripwires/direction_neutral_release_bracket_preflight_v1",
    )
    args = parser.parse_args()
    for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ.setdefault(name, "1")
    result = run(Path(args.project_root), output_dir=args.output_dir)
    print(
        json.dumps(
            {
                "status": result["status"],
                "counts": result["counts"],
                "runtime_seconds": result["runtime_seconds"],
                "result_hash": result["result_hash"],
                "next_action": result["next_action"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
