from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_power_aware_candidate_audit import (
    run_power_aware_candidate_audit,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit the 16 frozen V7.1 walk-forward-positive candidates."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument(
        "--output-dir", default="reports/v7_1/power_aware_0001"
    )
    args = parser.parse_args()
    result = run_power_aware_candidate_audit(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status_counts": result["status_counts"],
                "powered_candidate_ids": result["powered_candidate_ids"],
                "rolling_combine_research_eligible_ids": result[
                    "rolling_combine_research_eligible_ids"
                ],
                "principal_named_bounded_diagnostic_ids": result[
                    "principal_named_bounded_diagnostic_ids"
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
