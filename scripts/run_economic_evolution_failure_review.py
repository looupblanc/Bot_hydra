#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from hydra.research.economic_evolution_failure_review import (
    run_economic_evolution_failure_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen HYDRA economic-evolution failure review."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    args = parser.parse_args()
    result = run_economic_evolution_failure_review(
        args.output_dir,
        preregistration_path=args.preregistration,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
