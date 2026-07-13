#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.compute.result_writer import AtomicResultWriter
from hydra.economic_evolution.seed_archive import build_seed_archive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    payload = build_seed_archive(args.run_dir)
    receipt = AtomicResultWriter(args.output_dir).write_json(
        "seed_archive.json", payload
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
