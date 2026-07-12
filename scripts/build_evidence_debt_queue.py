from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.mission.mission_state import (
    connect_state_readonly,
    mission_paths,
    state_snapshot,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the latest immutable HYDRA Evidence Debt queue."
    )
    parser.add_argument("--state-dir", default="mission/state")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = mission_paths(args.state_dir)
    conn = connect_state_readonly(paths)
    try:
        snapshot = state_snapshot(conn)
    finally:
        conn.close()
    metrics = dict(snapshot.get("evidence_conversion_v3_latest_metrics") or {})
    report_path = Path(str(metrics.get("report_path") or ""))
    queue_path = report_path.with_name("evidence_debt_queue.json")
    if not report_path.is_file() or not queue_path.is_file():
        print("No completed immutable Evidence Conversion V3 queue is available.")
        return 2
    rows = json.loads(queue_path.read_text(encoding="utf-8"))
    rows.sort(
        key=lambda row: (
            -float(row.get("evidence_conversion_priority") or 0.0),
            str(row.get("candidate_id") or ""),
        )
    )
    payload = {
        "queue_path": str(queue_path),
        "inventory_count": len(rows),
        "remaining_representatives": int(
            metrics.get("evidence_debt_queue_count") or 0
        ),
        "latest_cohort": metrics.get("cohort_id"),
        "top": rows[: max(int(args.limit), 0)],
    }
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(f"queue_path: {payload['queue_path']}")
        print(f"inventory_count: {payload['inventory_count']}")
        print(f"remaining_representatives: {payload['remaining_representatives']}")
        for row in payload["top"]:
            print(
                f"{row['candidate_id']}\t"
                f"{float(row['evidence_conversion_priority']):.8f}\t"
                f"{row['identity'].get('strategy_role')}\t"
                f"{len(row.get('missing_evidence') or [])} gaps"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
