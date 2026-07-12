from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.calibration.v71_power_audit import run_v71_power_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen HYDRA V7.1 power controls.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--proof-registry", default="mission/state/proof_registry.json")
    parser.add_argument("--output-dir", default="reports/v7_1/calibration")
    args = parser.parse_args()
    result = run_v71_power_audit(
        project_root=args.project_root,
        proof_registry_path=args.proof_registry,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["verdict"] == "GREEN" else 2


if __name__ == "__main__":
    raise SystemExit(main())
