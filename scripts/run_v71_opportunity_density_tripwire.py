from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_opportunity_density_tripwire import (
    run_opportunity_density_tripwire,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen V7.1 opportunity-density tripwire."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument(
        "--output-dir", default="reports/v7_1/discovery_0002"
    )
    args = parser.parse_args()
    result = run_opportunity_density_tripwire(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "NULL_RATIO": result["NULL_RATIO"],
                "raw_pass_counts": result["raw_pass_counts"],
                "evidence_strength": result["evidence_strength"],
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
