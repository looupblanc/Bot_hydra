#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from hydra.validation.economic_evolution_expensive_validation import (
    run_economic_evolution_expensive_validation,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the frozen HYDRA economic-evolution expensive validation."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--contract-map", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()
    result = run_economic_evolution_expensive_validation(
        args.output_dir,
        preregistration_path=args.preregistration,
        contract_map_path=args.contract_map,
        cache_root=args.cache_root,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
