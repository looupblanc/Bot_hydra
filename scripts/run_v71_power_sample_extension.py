from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.calibration.v71_power_sample_extension import run_power_sample_extension


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen V7.1 sample-size controls.")
    parser.add_argument("--project-root", default=".")
    args = parser.parse_args()
    result = run_power_sample_extension(project_root=args.project_root)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["verdict"] == "GREEN" else 2


if __name__ == "__main__":
    raise SystemExit(main())
