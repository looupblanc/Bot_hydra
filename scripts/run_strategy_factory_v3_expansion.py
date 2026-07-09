#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.factory.candidate_factory import generate_candidates
from hydra.factory.expansion import evaluate_and_log
from hydra.registry.db import connect
from hydra.utils.config import load_config
from hydra.utils.logging import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA V3-style candidate expansion.")
    parser.add_argument("--candidates", type=int, default=500)
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    setup_logging(cfg["logging"]["folder"], cfg["logging"]["level"])
    if not args.synthetic:
        raise SystemExit("Real data is not configured yet. Use --synthetic for smoke tests.")
    symbols = args.symbols or cfg["markets"]["symbols"]
    timeframes = cfg["markets"]["timeframes"]
    conn = connect(cfg["registry"]["path"])
    candidates = generate_candidates(args.candidates, symbols, timeframes, args.seed)
    counts: Counter[str] = Counter()
    existing_curves: dict = {}
    for i, candidate in enumerate(candidates, start=1):
        status = evaluate_and_log(conn, candidate, cfg, args.synthetic, args.seed + i, existing_curves)
        counts[status] += 1
    print("HYDRA V3 expansion smoke run complete")
    print("Synthetic data only: results are pipeline diagnostics, not trading edge.")
    print(f"Generated candidates: {len(candidates)}")
    for status, count in counts.most_common():
        print(f"{status}: {count}")


if __name__ == "__main__":
    main()
