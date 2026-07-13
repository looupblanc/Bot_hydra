from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_cross_clock_speed_leadership_funnel import (
    run_speed_leadership_funnel,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the limited V7.1 speed-leadership funnel."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7_1/discovery_0005")
    args = parser.parse_args()
    result = run_speed_leadership_funnel(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "candidate_count",
                    "stage1_pass_count",
                    "walk_forward_positive_count",
                    "classification_counts",
                    "result_path",
                    "result_sha256",
                )
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
