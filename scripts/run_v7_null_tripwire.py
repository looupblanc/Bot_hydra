from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v7_null_tripwire import run_null_tripwire


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the WORM-preregistered HYDRA V7 Phase 1 null tripwire."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--preregistration",
        default="WORM/phase1-null-tripwire-2026-07-12.json",
    )
    parser.add_argument("--output-dir", default="reports/v7/phase1")
    args = parser.parse_args()
    result = run_null_tripwire(
        project_root=args.project_root,
        preregistration_path=args.preregistration,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "experiment_id": result["experiment_id"],
                "verdict": result["verdict"],
                "NULL_RATIO": result.get("NULL_RATIO"),
                "real": result.get("real"),
                "pooled_null": result.get("pooled_null"),
                "controls": result.get("controls"),
                "result_path": result["result_path"],
                "result_sha256": result["result_sha256"],
                "report_path": result["report_path"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result["verdict"] != "BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
