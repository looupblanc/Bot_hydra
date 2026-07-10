from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.governance.kernel import check_governance_kernel
from hydra.mission.experiment_queue import experiment_counts
from hydra.mission.mission_state import connect_state_readonly, mission_paths, state_snapshot
from hydra.mission.watchdog import heartbeat_status, scheduler_health


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYDRA autonomous mission health checks.")
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--baseline-commit", default="b56c98b8179d67e87d0290690fd8b73f70040dbe")
    parser.add_argument("--remaining-databento-budget-usd", type=float, default=77.036754)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    governance = check_governance_kernel(
        baseline_commit=args.baseline_commit,
        remaining_budget_usd=args.remaining_databento_budget_usd,
    )
    heartbeat = heartbeat_status(paths)
    snapshot = {}
    counts = {"TOTAL": 0, "QUEUED": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0, "BLOCKED": 0}
    if paths.db_path.exists():
        conn = connect_state_readonly(paths)
        try:
            snapshot = state_snapshot(conn)
            counts = experiment_counts(conn)
        finally:
            conn.close()
    scheduler = scheduler_health(heartbeat, snapshot, counts)
    result = {
        "governance": governance.to_dict(),
        "heartbeat": heartbeat.to_dict(),
        "state": snapshot,
        "experiments": counts,
        "scheduler": scheduler,
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        print(f"governance_passed: {governance.passed}")
        print(f"registry_integrity: {governance.details.get('registry_integrity_result')}")
        print(f"q4_access_count: {governance.details.get('q4_access_count')}")
        print(f"heartbeat_fresh: {heartbeat.fresh}")
        print(f"heartbeat_age_seconds: {heartbeat.age_seconds}")
        print(f"current_phase: {snapshot.get('current_phase')}")
        print(f"scheduler_classification: {scheduler.get('classification')}")
    return 0 if governance.passed and scheduler.get("healthy") else 2


if __name__ == "__main__":
    raise SystemExit(main())
