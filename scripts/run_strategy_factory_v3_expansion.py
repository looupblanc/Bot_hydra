#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.factory.candidate_factory import generate_candidates
from hydra.factory.diagnostics import DIAGNOSTIC_WARNING, apply_diagnostic_relaxed_config, diagnostic_bars
from hydra.factory.expansion import evaluate_and_log
from hydra.registry.db import connect
from hydra.utils.config import load_config
from hydra.utils.logging import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA V3-style candidate expansion.")
    parser.add_argument("--candidates", type=int, default=500)
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--diagnostic-relaxed", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    setup_logging(cfg["logging"]["folder"], cfg["logging"]["level"])
    if not args.synthetic:
        raise SystemExit("Real data is not configured yet. Use --synthetic for smoke tests.")
    if args.strict and args.diagnostic_relaxed:
        raise SystemExit("--strict and --diagnostic-relaxed are mutually exclusive.")
    diagnostic_relaxed = bool(args.synthetic and args.diagnostic_relaxed and not args.strict)
    if diagnostic_relaxed:
        cfg = apply_diagnostic_relaxed_config(cfg)
    symbols = args.symbols or cfg["markets"]["symbols"]
    timeframes = cfg["markets"]["timeframes"]
    conn = connect(cfg["registry"]["path"])
    candidates = generate_candidates(args.candidates, symbols, timeframes, args.seed, diagnostic_relaxed=diagnostic_relaxed)
    counts: Counter[str] = Counter()
    existing_curves: dict = {}
    bars = diagnostic_bars(cfg) if diagnostic_relaxed else 1500
    for i, candidate in enumerate(candidates, start=1):
        status = evaluate_and_log(conn, candidate, cfg, args.synthetic, args.seed + i, existing_curves, diagnostic_relaxed, bars)
        counts[status] += 1
    print("HYDRA V3 expansion smoke run complete")
    if args.synthetic:
        print(DIAGNOSTIC_WARNING)
    print(f"Generated candidates: {len(candidates)}")
    for status, count in counts.most_common():
        print(f"{status}: {count}")


if __name__ == "__main__":
    main()
