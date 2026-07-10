#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hydra.mission.experiment_queue import (
    claim_next_experiment,
    complete_experiment,
    enqueue_experiment,
    experiment_counts,
    experiment_record,
)
from hydra.mission.controller import AutonomousMissionController, MissionControllerConfig
from hydra.mission.mission_state import connect_state, mission_paths, set_kv
from hydra.utils.config import project_path


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hydra-mission-smoke-") as temporary:
        root = Path(temporary)
        paths = mission_paths(str(root / "state"))
        conn = connect_state(paths)
        try:
            experiment_id = "calibration_affected_atom_retest_design_smoke_v1"
            specification = {
                "experiment_type": "calibration_affected_atom_retest_design",
                "priority": 100.0,
                "max_attempts": 1,
                "historical_report_path": str(
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_lab_20260710T101052+0000_edge_atom_discovery_replication_v1_final_corrected.md",
                    )
                ),
                "historical_preregistration_path": str(
                    project_path(
                        "reports",
                        "edge_atom_lab",
                        "edge_atom_preregistration_20260710T101052+0000_edge_atom_discovery_replication_v1_final.json",
                    )
                ),
                "code_commit": "deterministic-smoke",
                "worker_output_root": str(root / "artifacts"),
                "q4_access_allowed": False,
                "paid_data_allowed": False,
            }
            enqueue_experiment(conn, experiment_id, specification)
            claimed = claim_next_experiment(conn, claimed_by="scheduler-smoke", lease_seconds=1.0)
            if claimed is None:
                raise RuntimeError("Smoke experiment was not claimable.")
            controller = AutonomousMissionController(
                MissionControllerConfig(
                    mission_id="scheduler_smoke",
                    baseline_commit="deterministic-smoke",
                    objective_config="deterministic-smoke",
                    remaining_databento_budget_usd=77.036754,
                    persistent=False,
                    state_dir=str(paths.state_dir),
                    sleep_seconds=0.1,
                )
            )
            set_kv(conn, "current_phase", "RUNNING_EXPERIMENT")
            set_kv(
                conn,
                "current_experiment",
                {
                    "experiment_id": experiment_id,
                    "claim_token": claimed["claim_token"],
                    "lease_expires_at": claimed["lease_expires_at"],
                },
            )
            result = controller._run_experiment_with_heartbeat(conn, claimed)
            running = experiment_record(conn, experiment_id)
            heartbeat = json.loads(paths.heartbeat_path.read_text(encoding="utf-8"))
            complete_experiment(conn, experiment_id, result, claim_token=str(claimed["claim_token"]))
            record = experiment_record(conn, experiment_id)
            if record is None or record["status"] != "COMPLETED":
                raise RuntimeError("Smoke experiment did not complete.")
            if result["selection"]["historical_atom_retest_count"] != 6:
                raise RuntimeError("Smoke design did not retain the bounded six-atom contract.")
            output = {
                "smoke_status": "PASSED",
                "experiment_id": experiment_id,
                "experiment_counts": experiment_counts(conn),
                "design_hash": result["design_hash"],
                "preregistration_hash": result["preregistration"]["preregistration_hash"],
                "selected_retests": result["selection"]["historical_atom_retest_count"],
                "worker_heartbeat_written": bool(heartbeat.get("current_experiment", {}).get("worker_pid")),
                "lease_renewed": bool(running and running.get("lease_expires_at") > claimed["lease_expires_at"]),
                "q4_access_count_delta": 0,
                "databento_spend_delta_usd": 0.0,
            }
            if not output["worker_heartbeat_written"] or not output["lease_renewed"]:
                raise RuntimeError("Smoke did not exercise subprocess heartbeat and lease renewal.")
            print(json.dumps(output, indent=2, sort_keys=True))
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
