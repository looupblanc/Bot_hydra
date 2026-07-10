from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.mission.controller import MissionControllerConfig, run_controller


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the persistent autonomous HYDRA mission controller.")
    parser.add_argument("--mission-id", required=True)
    parser.add_argument("--baseline-commit", required=True)
    parser.add_argument("--objective-config", required=True)
    parser.add_argument("--remaining-databento-budget-usd", type=float, required=True)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--checkpoint-every-minutes", type=float, default=20.0)
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--sleep-seconds", type=float, default=15.0)
    parser.add_argument("--max-cycles", type=int)
    parser.add_argument("--single-writer", action="store_true")
    parser.add_argument("--autonomous-engineering", action="store_true")
    parser.add_argument("--information-gain-planning", action="store_true")
    parser.add_argument("--validator-calibration-required", action="store_true")
    parser.add_argument("--persistent", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-live-trading", action="store_true")
    args = parser.parse_args()
    if not args.no_live_trading:
        parser.error("--no-live-trading is required")
    if not args.single_writer:
        parser.error("--single-writer is required")
    return args


def main() -> int:
    args = parse_args()
    config = MissionControllerConfig(
        mission_id=args.mission_id,
        baseline_commit=args.baseline_commit,
        objective_config=args.objective_config,
        remaining_databento_budget_usd=args.remaining_databento_budget_usd,
        workers=args.workers,
        checkpoint_every_minutes=args.checkpoint_every_minutes,
        persistent=args.persistent,
        resume=args.resume,
        no_live_trading=args.no_live_trading,
        state_dir=args.state_dir,
        sleep_seconds=args.sleep_seconds,
        max_cycles=args.max_cycles,
    )
    return run_controller(config)


if __name__ == "__main__":
    raise SystemExit(main())
