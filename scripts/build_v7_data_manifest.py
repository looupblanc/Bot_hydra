from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.v7_manifest import (
    render_data_manifest_report,
    verify_v7_data_manifest,
    write_v7_data_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the pre-ingestion HYDRA V7 data-lake manifest."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output", default="data/manifest.json")
    parser.add_argument(
        "--report", default="reports/v7/bootstrap/data_manifest_report.md"
    )
    parser.add_argument("--generated-at-utc")
    args = parser.parse_args()

    root = Path(args.project_root).resolve()
    output = root / args.output
    payload = write_v7_data_manifest(
        root, output, generated_at_utc=args.generated_at_utc
    )
    verification = verify_v7_data_manifest(root, output)
    report = root / args.report
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(render_data_manifest_report(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                **verification,
                "output": str(output),
                "report": str(report),
                "forward_bar_file_count": payload["forward_data_audit"][
                    "fresh_bar_file_count"
                ],
                "product_cutoffs": payload["product_cutoffs"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
