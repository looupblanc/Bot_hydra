#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from hydra.mission.experiment_runner import run_experiment


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an already authorized Q4 experiment specification outside the controller only for recovery testing."
    )
    parser.add_argument("--experiment-spec", required=True)
    parser.add_argument("--output-root", default="reports/mission_experiments")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    specification = json.loads(Path(args.experiment_spec).read_text(encoding="utf-8"))
    safe = bool(
        specification.get("experiment_type") == "q4_atomic_one_shot"
        and specification.get("q4_one_shot") is True
        and specification.get("max_attempts") == 1
        and specification.get("live_or_broker_allowed") is False
    )
    if not safe:
        raise RuntimeError("Specification is not an exact fail-closed Q4 one-shot.")
    if not args.execute:
        print(
            json.dumps(
                {
                    "status": "VALIDATION_ONLY_Q4_NOT_OPENED",
                    "experiment_id": specification.get("experiment_id"),
                    "cohort_manifest_hash": specification.get("cohort_manifest_hash"),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    result = run_experiment(specification, output_root=Path(args.output_root))
    print(
        json.dumps(
            {
                "status": "Q4_ONE_SHOT_FINISHED",
                "result_path": result.get("result_path"),
                "result_hash": result.get("result_hash"),
                "status_counts": result.get("status_counts"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
