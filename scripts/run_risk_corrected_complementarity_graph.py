#!/usr/bin/env python3
"""Run the bounded read-only risk-corrected complementarity graph."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.production.risk_corrected_complementarity_graph import (
    DEFAULT_MANIFEST,
    run,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument(
        "--output",
        default=(
            "reports/economic_evolution/"
            "risk_corrected_complementarity_graph_v1/economic_result.json"
        ),
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()
    result = run(root, manifest_path=args.manifest)
    output = (root / args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": result["status"],
        "result_hash": result["result_hash"],
        "qualified_policy_count": result["qualified_policy_count"],
        "best_held_out_policy_id": result["best_held_out_policy_id"],
        "counts": result["counts"],
        "output": str(output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
