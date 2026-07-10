from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.calibration.validator_benchmark import benchmark_validator, write_calibration_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA validator calibration controls.")
    parser.add_argument("--seed", type=int, default=9050)
    parser.add_argument(
        "--previous-report",
        default="reports/edge_atom_lab/edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
    )
    parser.add_argument("--report-tag", default="validator_calibration_v1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = benchmark_validator(seed=args.seed, previous_report=args.previous_report)
    path = write_calibration_report(result, tag=args.report_tag)
    print(json.dumps({"passed": result.passed, "false_positive_rate": result.false_positive_rate, "power": result.power_on_meaningful_effects, "report_path": str(path)}, sort_keys=True))
    return 0 if result.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
