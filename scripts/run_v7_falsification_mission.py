from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.mission.v7_falsification_controller import (
    V7ControllerConfig,
    run_v7_controller,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the persistent HYDRA V7 falsification controller."
    )
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--sleep-seconds", type=float, default=15.0)
    parser.add_argument("--checkpoint-every-steps", type=int, default=25)
    parser.add_argument("--maximum-steps", type=int)
    parser.add_argument("--persistent", action="store_true")
    parser.add_argument("--single-writer", action="store_true")
    parser.add_argument("--no-live-trading", action="store_true")
    args = parser.parse_args()
    if not args.single_writer:
        parser.error("--single-writer is required")
    if not args.no_live_trading:
        parser.error("--no-live-trading is required")
    return args


def main() -> int:
    args = parse_args()
    return run_v7_controller(
        V7ControllerConfig(
            project_root=args.project_root,
            state_dir=args.state_dir,
            sleep_seconds=args.sleep_seconds,
            checkpoint_every_steps=args.checkpoint_every_steps,
            persistent=args.persistent,
            maximum_steps=args.maximum_steps,
            no_live_trading=args.no_live_trading,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
