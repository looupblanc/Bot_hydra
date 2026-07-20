#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.research.fx_causal_ecology import run


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the frozen FX causal-ecology pilot")
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--output-dir",
        default="reports/research_tripwires/fx_causal_ecology_pilot_v1",
    )
    args = parser.parse_args()
    result = run(args.project_root, output_dir=args.output_dir)
    print(json.dumps({"status": result["status"], "counts": result["counts"], "result_hash": result["result_hash"], "runtime_seconds": result["runtime_seconds"]}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
