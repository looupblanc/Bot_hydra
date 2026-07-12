from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.calibration.v71_candidate_specific_power_calibration import (
    run_candidate_power_calibration,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Calibrate the frozen V7.1 candidate-specific power policy."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument(
        "--output-dir", default="reports/v7_1/power_aware_0001"
    )
    args = parser.parse_args()
    result = run_candidate_power_calibration(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "world_summaries": result["world_summaries"],
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
