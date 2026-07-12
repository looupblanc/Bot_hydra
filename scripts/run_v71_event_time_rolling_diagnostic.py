from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_event_time_rolling_diagnostic import (
    run_event_time_rolling_diagnostic,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen V7.1 event-time Rolling Combine diagnostic."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument(
        "--proof-registry", default="mission/state/proof_registry.json"
    )
    parser.add_argument(
        "--output-dir", default="reports/v7_1/power_aware_0001"
    )
    args = parser.parse_args()
    result = run_event_time_rolling_diagnostic(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "episode_start_count": result["episode_start_count"],
                "episode_power_status": result["episode_power_status"],
                "candidate_results": {
                    candidate_id: row["quantity_results"]["1"][
                        "eod_level_rt_breach"
                    ]
                    for candidate_id, row in result["candidate_results"].items()
                },
                "basket": result["basket"]["mode_results"][
                    "eod_level_rt_breach"
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
