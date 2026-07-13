from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.validation.v71_underpowered_combine_selection import (
    build_underpowered_combine_selection,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the frozen V7.1 underpowered Combine selection."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="reports/v7_1/combine_research_0001")
    args = parser.parse_args()
    result = build_underpowered_combine_selection(
        project_root=args.project_root,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "selected_count": result["selected_count"],
                "selected_candidate_ids": [
                    row["candidate_id"] for row in result["selected_candidates"]
                ],
                "population_reconciliation": result["population_reconciliation"],
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
