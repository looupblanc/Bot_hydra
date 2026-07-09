#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.databento_loader import (
    DatabentoConfigError,
    DatabentoDependencyError,
    DatabentoMissingKeyError,
    test_connection,
)
from hydra.utils.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Databento historical API configuration.")
    parser.add_argument("--dry-run", action="store_true", help="Validate configuration without creating a Databento client or making a request.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    try:
        result = test_connection(cfg, dry_run=args.dry_run)
    except (DatabentoConfigError, DatabentoMissingKeyError, DatabentoDependencyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.dry_run:
        print("Databento dry-run passed. No real API request was made.")
    else:
        print("Databento connectivity check completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
