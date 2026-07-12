from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_phase2_multiplicity import run_phase2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run WORM-preregistered HYDRA V7 Phase 2 deduplication."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--preregistration",
        default="WORM/phase2-multiplicity-dedup-2026-07-12.json",
    )
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument("--output-dir", default="reports/v7/phase2")
    args = parser.parse_args()
    result = run_phase2(
        project_root=args.project_root,
        preregistration_path=args.preregistration,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "candidate_count": result["candidate_count"],
                "behavioral_cluster_count": result["behavioral_cluster_count"],
                "DSR_positive_count": result["DSR_positive_count"],
                "BH_rejection_count": result["BH_rejection_count"],
                "SIM_EXPLOIT_count": result["SIM_EXPLOIT_count"],
                "promotion_eligible_count": result["promotion_eligible_count"],
                "selected_representative_ids": result[
                    "selected_representative_ids"
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
