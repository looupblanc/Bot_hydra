#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.factory.risk_compression import run_risk_compression
from hydra.registry.db import connect
from hydra.registry.reports import build_markdown_report
from hydra.utils.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA V4 risk compression.")
    parser.add_argument("--min-buffer", type=float, default=500)
    parser.add_argument("--target-buffer", type=float, default=2500)
    parser.add_argument("--max-strategies", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    conn = connect(cfg["registry"]["path"])
    selected = run_risk_compression(conn, args.min_buffer, args.target_buffer, args.max_strategies)
    report = build_markdown_report(conn, cfg["reports"]["folder"])
    print("HYDRA V4 risk compression complete")
    print(f"Portfolio selections/promotions: {len(selected)}")
    for cid in selected:
        print(f"- {cid}")
    print(f"Report: {report}")


if __name__ == "__main__":
    main()
