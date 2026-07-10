from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.mission.mission_state import clear_stop, mission_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clear stop state and optionally start HYDRA mission service.")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--start-service", action="store_true")
    parser.add_argument("--service-name", default="hydra-autonomous-mission.service")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    clear_stop(paths)
    print(f"stop cleared: {paths.stop_path}")
    if args.start_service:
        subprocess.run(["systemctl", "start", args.service_name], check=True)
        print(f"service started: {args.service_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
