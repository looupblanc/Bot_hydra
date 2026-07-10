from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.mission.mission_state import mission_paths, request_stop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Request a clean HYDRA autonomous mission stop.")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--reason", default="manual_stop")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    request_stop(paths, args.reason)
    print(f"stop requested: {paths.stop_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
