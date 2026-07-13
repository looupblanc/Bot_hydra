#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v72_component_bank import build_v72_component_bank


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze the V7.2 component bank.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="reports/v7_2/component_bank")
    parser.add_argument(
        "--worm-output",
        default="WORM/v7.2-component-bank-0001-2026-07-13.json",
    )
    args = parser.parse_args()
    result = build_v72_component_bank(
        project_root=args.project_root,
        output_dir=args.output_dir,
        worm_output_path=args.worm_output,
    )
    print(
        json.dumps(
            {
                "source_walk_forward_positive_count": result[
                    "source_walk_forward_positive_count"
                ],
                "unaccounted_candidate_count": result[
                    "unaccounted_candidate_count"
                ],
                "behavioral_cluster_count": result["behavioral_cluster_count"],
                "primary_component_count": result["primary_component_count"],
                "backup_component_count": result["backup_component_count"],
                "status_counts": result["status_counts"],
                "component_bank_path": result["component_bank_path"],
                "component_bank_sha256": result["component_bank_sha256"],
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
