#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.successor_seed import build_successor_seed_archive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--output-name", default="economic_evolution_successor_seed.json"
    )
    args = parser.parse_args()
    payload = build_successor_seed_archive(
        args.run_dir, project_root=args.project_root
    )
    receipt = AtomicResultWriter(args.output_dir).write_json(
        args.output_name, payload
    )
    print(
        json.dumps(
            {
                "archive_hash": payload["archive_hash"],
                "component_count": payload["component_count"],
                "policy_count": payload["policy_count"],
                "sha256": receipt.sha256,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
