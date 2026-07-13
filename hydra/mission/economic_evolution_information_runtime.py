from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.mission.economic_evolution_successor_runtime import (
    CAMPAIGN_RESULT_NAME as SOURCE_RESULT_NAME,
    CAMPAIGN_OUTPUT_RELATIVE_PATH as SOURCE_OUTPUT_RELATIVE_PATH,
    load_and_verify_successor_result,
    verify_successor_freeze,
)
from hydra.research.economic_evolution_information_review import (
    REVIEW_SCHEMA,
    load_information_review_preregistration,
)


REVIEW_ID = "hydra_economic_evolution_information_review_0004"
REVIEW_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_information_review_0004.json"
)
REVIEW_CONFIG_SHA256 = (
    "b4db22f4600cb0b5bb9b9215b3c157e71959def4fed9f27011e5f67960d8fef2"
)
REVIEW_WORM_TAG = "worm/economic-evolution-information-review-0004-2026-07-13"
REVIEW_WORM_COMMIT = "42d9281450ddbbb4a718954edbfb1e8e904ec1f6"
REVIEW_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/information_review_0004"
)
REVIEW_RESULT_NAME = "information_review_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_information_review_0004_multiplicity_reservation"
)
MULTIPLICITY_DELTA = 50
SOURCE_RESULT_SHA256 = (
    "85d2b600a0ea76d4aaf3ee65dc4a6a77017fd89587708da31321cdbe12705de0"
)


class EconomicEvolutionInformationRuntime:
    """Launch the frozen, development-only censored-horizon review.

    The worker writes immutable report files only.  This controller-owned
    runtime remains the sole proof-registry writer and never opens a market-data
    or broker connection.
    """

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / REVIEW_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / REVIEW_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_information_review_0004.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_information_review_0004.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_information_review_freeze(self.root)
        if self.result_path.is_file():
            result = load_and_verify_information_review_result(
                self.result_path, config
            )
            return information_review_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_information_review_result(
                    self.result_path, config
                )
                return information_review_action_from_result(predecessor, result)
            if return_code != 0:
                self._record_runtime_state(
                    "WORKER_FAILED", worker_exit_code=int(return_code)
                )

        self._quarantine_incomplete_attempt()
        self._start_worker()
        return self._running_action(predecessor, reservation)

    def stop(self) -> None:
        process = self._process
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)
        self._record_runtime_state(
            "WORKER_STOPPED_WITH_CONTROLLER", worker_exit_code=process.returncode
        )
        self._process = None

    def snapshot(self) -> dict[str, Any]:
        if self.result_path.is_file():
            state = "COMPLETE"
        elif self._process is not None and self._process.poll() is None:
            state = "RUNNING"
        else:
            state = str(self._load_runtime_state().get("state") or "READY")
        return {
            "review_id": REVIEW_ID,
            "state": state,
            "worker_pid": (
                self._process.pid
                if self._process is not None and self._process.poll() is None
                else None
            ),
            "attempt": self._attempt,
            "result_path": str(self.result_path),
            "review_stage": self._review_stage(),
            "compute_worker_count": 3,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "orders": 0,
        }

    def _ensure_multiplicity_reservation(
        self, config: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        proof_path = self.state_dir / "proof_registry.json"
        registry = load_and_verify(proof_path)
        if burned_window_ids(registry) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError("unexpected proof-window state")
        existing = next(
            (
                row
                for row in registry["entries"]
                if row["event_id"] == MULTIPLICITY_EVENT_ID
            ),
            None,
        )
        if existing is not None:
            if (
                int(existing["multiplicity"]["delta_trials"])
                != MULTIPLICITY_DELTA
                or existing["evidence"]["worm_sha256"]
                != REVIEW_CONFIG_SHA256
            ):
                raise EconomicEvolutionRuntimeError(
                    "existing information-review multiplicity reservation drift"
                )
            return existing
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "information-review artifacts exist before multiplicity reservation"
            )
        prior = multiplicity_trial_count(registry)
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_INFORMATION_REVIEW_OUTCOMES",
                "scientific_role": (
                    "DEVELOPMENT_DIAGNOSTIC_MULTIPLICITY_NO_PROOF_WINDOW"
                ),
                "evidence": {
                    "review_id": REVIEW_ID,
                    "worm_path": str(REVIEW_CONFIG_RELATIVE_PATH),
                    "worm_sha256": REVIEW_CONFIG_SHA256,
                    "worm_commit": REVIEW_WORM_COMMIT,
                    "source_result_sha256": SOURCE_RESULT_SHA256,
                    "selected_policy_count": len(config["selected_policies"]),
                    "outcomes_seen_for_this_review": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "proof_window_consumed": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": prior,
                    "delta_trials": MULTIPLICITY_DELTA,
                    "cumulative_N_trials": prior + MULTIPLICITY_DELTA,
                    "selected_policy_count": len(config["selected_policies"]),
                    "horizon_count": 5,
                    "cost_profile_count": 2,
                    "method": (
                        "Five frozen policies times five frozen horizons times "
                        "two cost profiles, reserved before review outcomes."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "information_review_0004_multiplicity_reservation.json",
            {
                "schema": "hydra_economic_evolution_multiplicity_reservation_v2",
                "event_id": MULTIPLICITY_EVENT_ID,
                "previous_N_trials": prior,
                "delta_trials": MULTIPLICITY_DELTA,
                "cumulative_N_trials": prior + MULTIPLICITY_DELTA,
                "entry_hash": entry["entry_hash"],
                "burned_windows": ["Q4_2024"],
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "outbound_order_count": 0,
                "CONTRE": (
                    "These are correlated development diagnostics; the global "
                    "counter records them without turning them into proof."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "information-review worker exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(
                self.root
                / "scripts/run_economic_evolution_information_review.py"
            ),
            "--output-dir",
            str(self.output_dir),
            "--preregistration",
            str(self.root / REVIEW_CONFIG_RELATIVE_PATH),
            "--contract-map",
            str(self.root / CONTRACT_MAP_RELATIVE_PATH),
            "--cache-root",
            str(self.root / FEATURE_CACHE_RELATIVE_PATH),
        ]
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONHASHSEED": "0",
                "OMP_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
            }
        )
        with self.log_path.open("ab") as log:
            self._process = subprocess.Popen(
                command,
                cwd=self.root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=False,
            )
        self._record_runtime_state("RUNNING")

    def _quarantine_incomplete_attempt(self) -> None:
        if not self.output_dir.exists() or not any(self.output_dir.iterdir()):
            return
        if self.result_path.is_file():
            return
        quarantine = (
            self.root
            / "reports/economic_evolution/quarantine"
            / f"information_review_0004_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "information-review quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_information_review_id": REVIEW_ID,
            "economic_information_review_state": self._review_stage(),
            "economic_information_review_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_information_review_attempt": self._attempt,
            "economic_information_review_selected_policies": 5,
            "economic_information_review_horizons": [20, 40, 60, 90, "FULL"],
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "Five immutable development policies are being replayed on "
                "frozen starts with explicit censoring; no status promotion, "
                "Q4, new data, proof window or orders are permitted."
            ),
        }

    def _review_stage(self) -> str:
        state_path = self.output_dir / "review_state.json"
        if not state_path.is_file():
            return "WORKER_STARTING"
        try:
            return str(json.loads(state_path.read_text())["stage"])
        except (OSError, ValueError, KeyError, TypeError):
            return "STATE_READ_RETRY"

    def _load_runtime_state(self) -> dict[str, Any]:
        if not self.runtime_state_path.is_file():
            return {}
        value = json.loads(self.runtime_state_path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}

    def _record_runtime_state(self, state: str, **extra: Any) -> None:
        _atomic_json(
            self.runtime_state_path,
            {
                "schema": "hydra_economic_evolution_information_runtime_state_v1",
                "review_id": REVIEW_ID,
                "state": state,
                "attempt": self._attempt,
                "worker_pid": (
                    None if self._process is None else self._process.pid
                ),
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "orders": 0,
                **extra,
            },
        )


def verify_information_review_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    successor_config = verify_successor_freeze(project)
    source_result = project / SOURCE_OUTPUT_RELATIVE_PATH / SOURCE_RESULT_NAME
    if _sha256(source_result) != SOURCE_RESULT_SHA256:
        raise EconomicEvolutionRuntimeError("information-review source result drift")
    load_and_verify_successor_result(source_result, successor_config)
    config_path = project / REVIEW_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != REVIEW_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("information-review WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", REVIEW_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != REVIEW_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("information-review WORM tag drift")
    config = load_information_review_preregistration(config_path)
    if (
        int(config["multiplicity"]["prospective_diagnostic_comparisons"])
        != MULTIPLICITY_DELTA
    ):
        raise EconomicEvolutionRuntimeError(
            "information-review multiplicity policy drift"
        )
    return config


def load_and_verify_information_review_result(
    path: str | Path, config: Mapping[str, Any]
) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if value.get("schema") != REVIEW_SCHEMA or value.get("review_id") != REVIEW_ID:
        raise EconomicEvolutionRuntimeError("information-review result schema drift")
    if value.get("preregistration_hash") != config["preregistration_hash"]:
        raise EconomicEvolutionRuntimeError(
            "information-review result preregistration drift"
        )
    if value.get("source_campaign_result_sha256") != SOURCE_RESULT_SHA256:
        raise EconomicEvolutionRuntimeError("information-review source hash drift")
    if (
        value.get("development_only") is not True
        or int(value.get("validated_policy_count") or 0) != 0
        or int(value.get("pre_holdout_ready_count") or 0) != 0
        or int(value.get("paper_shadow_ready_count") or 0) != 0
        or value.get("proof_window_consumed") is not False
        or int(value.get("new_data_purchase_count") or 0) != 0
        or int(value.get("q4_access_delta") or 0) != 0
        or int(value.get("broker_connections") or 0) != 0
        or int(value.get("orders") or 0) != 0
        or value.get("outbound_order_capability") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "information-review result integrity drift"
        )
    if int(value.get("selected_policy_count") or 0) != len(
        config["selected_policies"]
    ):
        raise EconomicEvolutionRuntimeError(
            "information-review result policy-count drift"
        )
    return value


def information_review_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    eligible = int(result["expensive_validation_queue_eligible_count"])
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_information_review_id": REVIEW_ID,
        "economic_information_review_state": "COMPLETE",
        "economic_information_review_selected_policies": int(
            result["selected_policy_count"]
        ),
        "economic_information_review_base_passes": int(
            result["full_available_base_pass_count"]
        ),
        "economic_information_review_stressed_passes": int(
            result["full_available_stressed_pass_count"]
        ),
        "economic_expensive_validation_queue_eligible_count": eligible,
        "economic_expensive_validation_queue_eligible_ids": list(
            result["expensive_validation_queue_eligible_ids"]
        ),
        "economic_information_review_scientific_status": result[
            "scientific_status"
        ],
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": (
            "hydra_economic_evolution_expensive_validation_freeze_0005"
            if eligible
            else "hydra_economic_evolution_representation_review_0005"
        ),
        "next_experiment_state": (
            "WORM_PREREGISTRATION_REQUIRED"
            if eligible
            else "AUTONOMOUS_INFORMATION_GAIN_REVIEW_REQUIRED"
        ),
        "principal_blocker": (
            "Queued development paths still require powered, null-adjusted and "
            "independent confirmation."
            if eligible
            else "Frozen policies still do not convert positive economics into "
            "sufficient target velocity."
        ),
        "reason": (
            "The censored development review completed atomically without status "
            "inheritance, proof consumption, Q4, new data or orders."
        ),
    }


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EconomicEvolutionInformationRuntime",
    "MULTIPLICITY_DELTA",
    "MULTIPLICITY_EVENT_ID",
    "REVIEW_CONFIG_RELATIVE_PATH",
    "REVIEW_ID",
    "REVIEW_OUTPUT_RELATIVE_PATH",
    "information_review_action_from_result",
    "load_and_verify_information_review_result",
    "verify_information_review_freeze",
]
