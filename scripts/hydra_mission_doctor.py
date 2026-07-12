from __future__ import annotations

import argparse
import os
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hydra.governance.kernel import check_governance_kernel
from hydra.mission.experiment_queue import experiment_counts
from hydra.mission.mission_state import connect_state_readonly, mission_paths, state_snapshot
from hydra.mission.watchdog import heartbeat_status, scheduler_health
from hydra.governance.proof_registry import burned_window_ids, load_and_verify


V7_CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)


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
    bootstrap = _v7_clean_stop_bootstrap_health(paths, snapshot, governance.passed)
    if bootstrap["eligible"]:
        scheduler = {
            "classification": "HEALTHY_STOPPED_FOR_V7_BOOTSTRAP",
            "healthy": True,
            "reason": "intentional_clean_stop_before_G0_G1",
        }
    result = {
        "governance": governance.to_dict(),
        "heartbeat": heartbeat.to_dict(),
        "state": snapshot,
        "experiments": counts,
        "scheduler": scheduler,
        "v7_bootstrap_clean_stop": bootstrap,
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
        print(f"v7_bootstrap_clean_stop_eligible: {bootstrap['eligible']}")
    return 0 if governance.passed and scheduler.get("healthy") else 2


def _v7_clean_stop_bootstrap_health(
    paths: object, snapshot: dict[str, object], governance_passed: bool
) -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    contract = root / "MISSION_CONTRACT.md"
    contract_hash = _sha256(contract) if contract.is_file() else None
    phase = str(snapshot.get("current_phase") or "")
    service_state = str(snapshot.get("service_state") or "")
    lock_path = Path(getattr(paths, "lock_path", root / "mission/state/hydra_mission.lock"))
    lock_pid = _read_pid(lock_path)
    lock_owner_alive = _pid_alive(lock_pid)
    main_pid = _service_main_pid()
    controller_pids = _controller_pids()
    proof_path = root / "mission/state/proof_registry.json"
    try:
        proof = load_and_verify(proof_path)
        q4_burned = "Q4_2024" in burned_window_ids(proof)
        proof_valid = True
    except (FileNotFoundError, ValueError, RuntimeError):
        proof_valid = False
        q4_burned = False
    eligible = bool(
        contract_hash == V7_CONTRACT_SHA256
        and phase == "STOPPED_CLEANLY"
        and service_state == "STOPPED_CLEANLY"
        and governance_passed
        and main_pid == 0
        and not lock_owner_alive
        and not controller_pids
        and proof_valid
        and q4_burned
    )
    return {
        "eligible": eligible,
        "contract_hash": contract_hash,
        "phase": phase,
        "service_state": service_state,
        "service_main_pid": main_pid,
        "lock_pid": lock_pid,
        "lock_owner_alive": lock_owner_alive,
        "controller_pids": controller_pids,
        "registry_writer_count": len(controller_pids),
        "proof_registry_valid": proof_valid,
        "q4_burned": q4_burned,
        "scope": "bootstrap_acceptance_only_not_runtime_autonomy",
    }


def _service_main_pid() -> int:
    completed = subprocess.run(
        [
            "systemctl",
            "show",
            "hydra-autonomous-mission.service",
            "--property=MainPID",
            "--value",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        return int(completed.stdout.strip() or 0)
    except ValueError:
        return -1


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def _pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _controller_pids() -> list[int]:
    current = os.getpid()
    pids: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == current:
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if (
            b"hydra.mission.controller" in command
            or b"run_hydra_autonomous_mission.py" in command
        ):
            pids.append(int(entry.name))
    return sorted(pids)


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
