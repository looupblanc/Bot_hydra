from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_cross_clock_flow_power_audit import (
    run_cross_clock_flow_power_audit,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen V7.1 cross-clock power-aware audit."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7_1/discovery_0004")
    args = parser.parse_args()
    result = run_cross_clock_flow_power_audit(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status_counts",
                    "powered_candidate_ids",
                    "underpowered_candidate_ids",
                    "fragile_candidate_ids",
                    "rolling_combine_research_eligible_ids",
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
