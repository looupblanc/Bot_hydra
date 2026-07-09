#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.data.databento_loader import (
    DatabentoConfigError,
    DatabentoCostLimitError,
    DatabentoDependencyError,
    DatabentoMissingKeyError,
    download_historical_ohlcv,
)
from hydra.utils.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Databento historical futures OHLCV data.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print the request plan without making a Databento request.")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--schema")
    parser.add_argument("--dataset")
    parser.add_argument("--max-cost-usd", type=float, default=5.0, help="Abort if Databento estimates the request above this USD cost.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    try:
        result = download_historical_ohlcv(
            cfg,
            symbols=args.symbols,
            start=args.start,
            end=args.end,
            schema=args.schema,
            dataset=args.dataset,
            dry_run=args.dry_run,
            max_cost_usd=args.max_cost_usd,
        )
    except (DatabentoConfigError, DatabentoMissingKeyError, DatabentoDependencyError, DatabentoCostLimitError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.dry_run:
        print("Databento download dry-run passed. No real API request was made.")
    elif result.get("cache_hit"):
        print(f"Databento historical data cache hit at {result['output_path']}. No real API request was made.")
    else:
        print(f"Databento historical data saved to {result['output_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
