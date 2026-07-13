#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v72_crossfit_baskets import run_v72_crossfit_baskets


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--proof-registry", default="mission/state/proof_registry.json")
    parser.add_argument("--output-dir", default="reports/v7_2/crossfit_0001")
    args = parser.parse_args()
    result = run_v72_crossfit_baskets(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "structure_count": result["structure_count"],
                "cross_fit_rotation_count": result["cross_fit_rotation_count"],
                "held_out_basket_evaluation_count": result[
                    "held_out_basket_evaluation_count"
                ],
                "status_counts": result["status_counts"],
                "cross_fit_survivor_count": result["cross_fit_survivor_count"],
                "promotion_to_48_starts_count": result[
                    "promotion_to_48_starts_count"
                ],
                "result_path": result["result_path"],
                "result_sha256": result["result_sha256"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
