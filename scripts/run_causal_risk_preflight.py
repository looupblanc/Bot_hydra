#!/usr/bin/env python3
"""Run the preregistered bounded risk frontier for campaign 0028."""

from __future__ import annotations

import argparse
import json

from hydra.production.causal_risk_preflight import run_causal_risk_preflight


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        default="config/v7/causal_target_velocity_0028.json",
    )
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()
    result = run_causal_risk_preflight(
        args.manifest,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "result_hash": result["result_hash"],
                "survivor_count": result["gate"]["survivor_count"],
                "runtime_seconds": result["runtime_seconds"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
