#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import signal
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.factory.candidate_factory import generate_candidates
from hydra.factory.continuous_service import sleep_cycle
from hydra.factory.expansion import evaluate_and_log
from hydra.factory.risk_compression import run_risk_compression
from hydra.registry.db import connect
from hydra.registry.reports import build_markdown_report
from hydra.utils.config import load_config
from hydra.utils.logging import setup_logging


STOP = False


def _stop(_signum, _frame) -> None:
    global STOP
    STOP = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA continuous factory loop.")
    parser.add_argument("--sleep", type=int, default=300)
    parser.add_argument("--target-qualified", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--synthetic", action="store_true", help="Use synthetic data for smoke-mode factory cycles.")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config()
    setup_logging(cfg["logging"]["folder"], cfg["logging"]["level"])
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    conn = connect(cfg["registry"]["path"])
    cycle = 0
    while not STOP:
        cycle += 1
        qualified = conn.execute("SELECT COUNT(*) FROM candidates WHERE validation_status IN ('QUALIFIED','PROMOTED_TO_PORTFOLIO')").fetchone()[0]
        if qualified < args.target_qualified:
            if not args.synthetic:
                raise SystemExit("Continuous generation requires --synthetic until real data is configured.")
            candidates = generate_candidates(args.batch_size, cfg["markets"]["symbols"], cfg["markets"]["timeframes"], args.seed + cycle)
            counts: Counter[str] = Counter()
            existing_curves: dict = {}
            for i, candidate in enumerate(candidates, start=1):
                status = evaluate_and_log(conn, candidate, cfg, True, args.seed + cycle + i, existing_curves)
                counts[status] += 1
            logging.info("cycle=%s generated=%s statuses=%s", cycle, len(candidates), dict(counts))
        else:
            selected = run_risk_compression(conn, cfg["propfirm"]["min_mll_buffer"], cfg["propfirm"]["target_mll_buffer"], cfg["risk_compression"]["max_strategies"])
            logging.info("cycle=%s monitoring/risk-compression selected=%s", cycle, len(selected))
        if cycle % cfg["continuous"]["report_every_cycles"] == 0:
            logging.info("report=%s", build_markdown_report(conn, cfg["reports"]["folder"]))
        sleep_cycle(args.sleep)
    logging.info("Continuous factory stopped gracefully.")


if __name__ == "__main__":
    main()
