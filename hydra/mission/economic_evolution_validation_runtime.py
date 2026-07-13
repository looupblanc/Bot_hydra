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

from hydra.economic_evolution.schema import stable_hash
from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_information_runtime import (
    REVIEW_CONFIG_RELATIVE_PATH,
    REVIEW_ID,
    REVIEW_OUTPUT_RELATIVE_PATH,
    REVIEW_RESULT_NAME,
    information_review_action_from_result,
    load_and_verify_information_review_result,
    verify_information_review_freeze,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.validation.economic_evolution_expensive_validation import (
    VALIDATION_SCHEMA,
    load_expensive_validation_preregistration,
)


VALIDATION_ID = "hydra_economic_evolution_expensive_validation_0005"
VALIDATION_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_expensive_validation_0005.json"
)
VALIDATION_CONFIG_SHA256 = (
    "90501ba0e2e519a5c1cf0a30a67e5bc682f6797303555257190cbcbb0b0fbc78"
)
VALIDATION_WORM_TAG = (
    "worm/economic-evolution-expensive-validation-0005-2026-07-13"
)
VALIDATION_WORM_COMMIT = "6f582e6decbb573b44fb9b1c7a6e87f084f1263b"
VALIDATION_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/expensive_validation_0005"
)
VALIDATION_RESULT_NAME = "expensive_validation_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_expensive_validation_0005_multiplicity_reservation"
)
MULTIPLICITY_DELTA = 24


class EconomicEvolutionValidationRuntime:
    """Run the WORM-frozen development validation without adding a writer.

    The controller reserves multiplicity prospectively.  The worker is allowed
    to write immutable report artifacts only; it cannot write mission state,
    proof state, market-data ledgers, shadow state, or broker state.
    """

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / VALIDATION_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / VALIDATION_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_expensive_validation_0005.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_expensive_validation_0005.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_expensive_validation_freeze(self.root)
        self._verify_predecessor(predecessor, config)
        if self.result_path.is_file():
            result = load_and_verify_expensive_validation_result(
                self.result_path, config
            )
            return expensive_validation_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_expensive_validation_result(
                    self.result_path, config
                )
                return expensive_validation_action_from_result(predecessor, result)
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
            "validation_id": VALIDATION_ID,
            "state": state,
            "worker_pid": (
                self._process.pid
                if self._process is not None and self._process.poll() is None
                else None
            ),
            "attempt": self._attempt,
            "result_path": str(self.result_path),
            "validation_stage": self._validation_stage(),
            "exact_worker_count": 3,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "broker_connections": 0,
            "orders": 0,
        }

    def _verify_predecessor(
        self, predecessor: Mapping[str, Any], config: Mapping[str, Any]
    ) -> None:
        candidate_id = str(config["candidate"]["policy_id"])
        if (
            predecessor.get("action_type")
            != "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_COMPLETE"
            or int(
                predecessor.get(
                    "economic_expensive_validation_queue_eligible_count", 0
                )
            )
            != 1
            or list(
                predecessor.get(
                    "economic_expensive_validation_queue_eligible_ids", []
                )
            )
            != [candidate_id]
        ):
            raise EconomicEvolutionRuntimeError(
                "expensive validation predecessor is not the frozen eligible path"
            )

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
                != VALIDATION_CONFIG_SHA256
                or int(existing["multiplicity"]["cumulative_N_trials"])
                != int(config["statistics_policy"]["raw_global_N_trials_at_freeze"])
                + MULTIPLICITY_DELTA
            ):
                raise EconomicEvolutionRuntimeError(
                    "existing expensive-validation multiplicity reservation drift"
                )
            return existing
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "expensive-validation artifacts exist before multiplicity reservation"
            )
        prior = multiplicity_trial_count(registry)
        frozen_prior = int(
            config["statistics_policy"]["raw_global_N_trials_at_freeze"]
        )
        if prior != frozen_prior:
            raise EconomicEvolutionRuntimeError(
                f"expensive-validation trial-count drift: {prior} != {frozen_prior}"
            )
        candidate_id = str(config["candidate"]["policy_id"])
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_EXPENSIVE_VALIDATION_OUTCOMES",
                "scientific_role": (
                    "DEVELOPMENT_EXPENSIVE_VALIDATION_NO_PROOF_WINDOW"
                ),
                "evidence": {
                    "validation_id": VALIDATION_ID,
                    "candidate_id": candidate_id,
                    "candidate_specification_hash": config["candidate"][
                        "policy_specification_hash"
                    ],
                    "worm_path": str(VALIDATION_CONFIG_RELATIVE_PATH),
                    "worm_sha256": VALIDATION_CONFIG_SHA256,
                    "worm_commit": VALIDATION_WORM_COMMIT,
                    "outcomes_seen_for_this_validation": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "proof_window_consumed": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": prior,
                    "delta_trials": MULTIPLICITY_DELTA,
                    "cumulative_N_trials": prior + MULTIPLICITY_DELTA,
                    "candidate_count": 1,
                    "prospective_comparisons": MULTIPLICITY_DELTA,
                    "method": (
                        "Twenty-four frozen account-level profile, null, ablation, "
                        "fragility, power and decision comparisons reserved before "
                        "any validation outcome. Resampling iterations are excluded."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "expensive_validation_0005_multiplicity_reservation.json",
            {
                "schema": "hydra_economic_evolution_multiplicity_reservation_v2",
                "event_id": MULTIPLICITY_EVENT_ID,
                "candidate_id": candidate_id,
                "previous_N_trials": prior,
                "delta_trials": MULTIPLICITY_DELTA,
                "cumulative_N_trials": prior + MULTIPLICITY_DELTA,
                "entry_hash": entry["entry_hash"],
                "burned_windows": ["Q4_2024"],
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "outbound_order_count": 0,
                "CONTRE": (
                    "This remains post-selection development evidence and cannot "
                    "serve as independent confirmation."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        self._recover_pre_execution_import_failures()
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "expensive-validation worker exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(
                self.root
                / "scripts/run_economic_evolution_expensive_validation.py"
            ),
            "--output-dir",
            str(self.output_dir),
            "--preregistration",
            str(self.root / VALIDATION_CONFIG_RELATIVE_PATH),
            "--contract-map",
            str(self.root / CONTRACT_MAP_RELATIVE_PATH),
            "--cache-root",
            str(self.root / FEATURE_CACHE_RELATIVE_PATH),
        ]
        environment = _worker_environment(self.root)
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

    def _recover_pre_execution_import_failures(self) -> None:
        """Permit one audited retry after the original launcher never imported.

        This is deliberately narrower than a scientific retry.  Recovery is
        possible only when all three old attempts ended at Python import, no
        output artifact exists, and no prior recovery was consumed.  The
        frozen validation implementation, inputs, gates, and proof reservation
        remain unchanged.
        """

        if self._attempt < 3:
            return
        recovery_path = (
            self.root
            / "reports/economic_evolution/"
            "expensive_validation_0005_bootstrap_recovery.json"
        )
        if recovery_path.exists():
            raise EconomicEvolutionRuntimeError(
                "expensive-validation bootstrap recovery was already consumed"
            )
        if self.result_path.exists() or (
            self.output_dir.exists() and any(self.output_dir.iterdir())
        ):
            raise EconomicEvolutionRuntimeError(
                "expensive-validation attempts cannot recover after outcome artifacts"
            )
        if not self.log_path.is_file():
            raise EconomicEvolutionRuntimeError(
                "expensive-validation worker exhausted three deterministic attempts"
            )
        log_text = self.log_path.read_text(encoding="utf-8", errors="replace")
        signature = "ModuleNotFoundError: No module named 'hydra'"
        if log_text.count(signature) != 3:
            raise EconomicEvolutionRuntimeError(
                "expensive-validation worker exhausted three deterministic attempts"
            )
        log_sha256 = _sha256(self.log_path)
        _atomic_json(
            recovery_path,
            {
                "schema": (
                    "hydra_economic_evolution_pre_execution_bootstrap_recovery_v1"
                ),
                "validation_id": VALIDATION_ID,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "failure_signature": signature,
                "pre_execution_failure_count": 3,
                "log_sha256": log_sha256,
                "worker_import_completed": False,
                "validation_outcomes_seen": False,
                "result_artifacts_seen": False,
                "proof_reservation_reused": True,
                "multiplicity_delta_added": 0,
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "broker_connections": 0,
                "orders": 0,
                "CONTRE": (
                    "This engineering recovery is valid only because Python never "
                    "imported the frozen validator and therefore observed no outcome."
                ),
            },
        )
        self._attempt = 0
        self._record_runtime_state(
            "PRE_EXECUTION_BOOTSTRAP_RECOVERED",
            pre_execution_failure_count=3,
            prior_log_sha256=log_sha256,
        )

    def _quarantine_incomplete_attempt(self) -> None:
        if not self.output_dir.exists() or not any(self.output_dir.iterdir()):
            return
        if self.result_path.is_file():
            return
        quarantine = (
            self.root
            / "reports/economic_evolution/quarantine"
            / f"expensive_validation_0005_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "expensive-validation quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_EXPENSIVE_VALIDATION_0005_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_expensive_validation_id": VALIDATION_ID,
            "economic_expensive_validation_state": self._validation_stage(),
            "economic_expensive_validation_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_expensive_validation_attempt": self._attempt,
            "economic_expensive_validation_candidate_id": reservation["evidence"][
                "candidate_id"
            ],
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "The sole frozen development policy is undergoing bounded null, "
                "fragility, power and account-control validation. No proof window, "
                "Q4, new data, shadow promotion or order path is permitted."
            ),
        }

    def _validation_stage(self) -> str:
        state_path = self.output_dir / "validation_state.json"
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
                "schema": "hydra_economic_evolution_validation_runtime_state_v1",
                "validation_id": VALIDATION_ID,
                "state": state,
                "attempt": self._attempt,
                "worker_pid": (
                    None if self._process is None else self._process.pid
                ),
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "broker_connections": 0,
                "orders": 0,
                **extra,
            },
        )


def verify_expensive_validation_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    review_config = verify_information_review_freeze(project)
    review_result_path = (
        project / REVIEW_OUTPUT_RELATIVE_PATH / REVIEW_RESULT_NAME
    )
    review_result = load_and_verify_information_review_result(
        review_result_path, review_config
    )
    review_action = information_review_action_from_result(
        {"action_type": "ECONOMIC_EVOLUTION_PREDECESSOR_COMPLETE", "phase": "4"},
        review_result,
    )
    config_path = project / VALIDATION_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != VALIDATION_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("expensive-validation WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", VALIDATION_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != VALIDATION_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("expensive-validation WORM tag drift")
    config = load_expensive_validation_preregistration(config_path)
    candidate_id = str(config["candidate"]["policy_id"])
    if (
        int(config["multiplicity"]["prospective_comparisons"])
        != MULTIPLICITY_DELTA
        or review_action["economic_expensive_validation_queue_eligible_ids"]
        != [candidate_id]
        or review_action["economic_expensive_validation_queue_eligible_count"]
        != 1
    ):
        raise EconomicEvolutionRuntimeError(
            "expensive-validation selection or multiplicity policy drift"
        )
    return config


def load_and_verify_expensive_validation_result(
    path: str | Path, config: Mapping[str, Any]
) -> dict[str, Any]:
    result_path = Path(path).resolve()
    value = json.loads(result_path.read_text(encoding="utf-8"))
    frozen_hash = str(value.get("result_sha256") or "")
    semantic = dict(value)
    semantic.pop("result_sha256", None)
    if not frozen_hash or stable_hash(semantic) != frozen_hash:
        raise EconomicEvolutionRuntimeError(
            "expensive-validation result hash drift"
        )
    candidate = config["candidate"]
    if (
        value.get("schema") != VALIDATION_SCHEMA
        or value.get("validation_id") != VALIDATION_ID
        or value.get("candidate_id") != candidate["policy_id"]
        or value.get("candidate_specification_hash")
        != candidate["policy_specification_hash"]
    ):
        raise EconomicEvolutionRuntimeError(
            "expensive-validation result identity drift"
        )
    gates = value.get("gates")
    if not isinstance(gates, dict) or not gates or not all(
        isinstance(row, bool) for row in gates.values()
    ):
        raise EconomicEvolutionRuntimeError(
            "expensive-validation result gates are invalid"
        )
    all_gates = all(gates.values())
    allowed_failures = set(config["decision_policy"]["failure_statuses"])
    supported = str(config["decision_policy"]["supported_status"])
    status = str(value.get("scientific_status") or "")
    if (all_gates and status != supported) or (
        not all_gates and status not in allowed_failures
    ):
        raise EconomicEvolutionRuntimeError(
            "expensive-validation result decision drift"
        )
    if (
        value.get("development_only") is not True
        or value.get("validated") is not False
        or value.get("status_inheritance") is not False
        or value.get("all_frozen_gates_passed") is not all_gates
        or value.get("independent_confirmation_queue_eligible") is not all_gates
        or int(value.get("pre_holdout_ready_count") or 0) != 0
        or int(value.get("paper_shadow_ready_count") or 0) != 0
        or value.get("proof_window_consumed") is not False
        or int(value.get("q4_access_delta") or 0) != 0
        or int(value.get("new_data_purchase_count") or 0) != 0
        or int(value.get("broker_connections") or 0) != 0
        or int(value.get("orders") or 0) != 0
        or value.get("outbound_order_capability") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "expensive-validation protected-state drift"
        )
    expected_auxiliary = {
        "profile_results_path": "account_profile_results.json",
        "matched_controls_path": "matched_controls.json",
        "statistical_validation_path": "statistical_validation.json",
    }
    for key, filename in expected_auxiliary.items():
        auxiliary = Path(str(value.get(key) or "")).resolve()
        if auxiliary != result_path.parent / filename or not auxiliary.is_file():
            raise EconomicEvolutionRuntimeError(
                f"expensive-validation auxiliary result drift: {key}"
            )
        payload = json.loads(auxiliary.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise EconomicEvolutionRuntimeError(
                f"expensive-validation auxiliary result invalid: {key}"
            )
    return value


def expensive_validation_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    eligible = bool(result["independent_confirmation_queue_eligible"])
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_EXPENSIVE_VALIDATION_0005_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_expensive_validation_id": VALIDATION_ID,
        "economic_expensive_validation_state": "COMPLETE",
        "economic_expensive_validation_candidate_id": result["candidate_id"],
        "economic_expensive_validation_scientific_status": result[
            "scientific_status"
        ],
        "economic_expensive_validation_all_gates_passed": bool(
            result["all_frozen_gates_passed"]
        ),
        "economic_expensive_validation_gates": dict(result["gates"]),
        "economic_independent_confirmation_queue_eligible_count": int(eligible),
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": (
            "hydra_economic_evolution_independent_confirmation_freeze_0006"
            if eligible
            else "hydra_economic_evolution_failure_directed_review_0006"
        ),
        "next_experiment_state": (
            "WORM_PREREGISTRATION_REQUIRED_NO_DATA_ACCESS"
            if eligible
            else "AUTONOMOUS_INFORMATION_GAIN_REVIEW_REQUIRED"
        ),
        "principal_blocker": (
            "An untouched confirmation manifest and decision policy must be "
            "frozen before any independent data access."
            if eligible
            else "The sole queued policy failed at least one frozen development "
            "gate and cannot consume independent proof."
        ),
        "reason": (
            "The bounded development validation completed atomically without "
            "status inheritance, proof consumption, Q4, new data or orders."
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


def _worker_environment(root: Path) -> dict[str, str]:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = str(root) + (
        os.pathsep + existing if existing else ""
    )
    return environment


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "EconomicEvolutionValidationRuntime",
    "MULTIPLICITY_DELTA",
    "MULTIPLICITY_EVENT_ID",
    "VALIDATION_CONFIG_RELATIVE_PATH",
    "VALIDATION_ID",
    "VALIDATION_OUTPUT_RELATIVE_PATH",
    "expensive_validation_action_from_result",
    "load_and_verify_expensive_validation_result",
    "verify_expensive_validation_freeze",
]
