from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.mission.mission_state import connect_state, mission_paths, state_snapshot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show HYDRA autonomous mission status.")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    heartbeat = {}
    if paths.heartbeat_path.exists():
        heartbeat = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
    snapshot = {}
    if paths.db_path.exists():
        conn = connect_state(paths)
        try:
            snapshot = state_snapshot(conn)
        finally:
            conn.close()
    status = {"heartbeat_path": str(paths.heartbeat_path), "heartbeat": heartbeat, "state": snapshot}
    if args.json:
        print(json.dumps(status, indent=2, sort_keys=True, default=str))
    else:
        print(f"mission_id: {heartbeat.get('mission_id') or snapshot.get('mission_id')}")
        print(f"phase: {heartbeat.get('current_phase') or snapshot.get('current_phase')}")
        print(f"action: {heartbeat.get('current_action') or snapshot.get('current_action')}")
        print(f"cycle_count: {heartbeat.get('cycle_count') or snapshot.get('cycle_count')}")
        print(f"heartbeat_at_utc: {heartbeat.get('heartbeat_at_utc')}")
        print(f"pid: {heartbeat.get('pid')}")
        print(f"checkpoint: {heartbeat.get('latest_checkpoint') or snapshot.get('last_successful_checkpoint')}")
        print(f"q4_access_count: {heartbeat.get('q4_access_count')}")
        print(f"remaining_budget: {heartbeat.get('remaining_databento_budget_usd') or snapshot.get('remaining_databento_budget_usd')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
