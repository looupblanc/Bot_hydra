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
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.mission.economic_evolution_validation_runtime import (
    VALIDATION_CONFIG_RELATIVE_PATH,
    VALIDATION_ID,
    VALIDATION_OUTPUT_RELATIVE_PATH,
    VALIDATION_RESULT_NAME,
    expensive_validation_action_from_result,
    load_and_verify_expensive_validation_result,
    verify_expensive_validation_freeze,
)
from hydra.research.economic_evolution_failure_review import (
    REVIEW_SCHEMA,
    load_failure_review_preregistration,
)


REVIEW_ID = (
    "hydra_economic_evolution_failure_directed_review_0006_revision_02"
)
REVIEW_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_failure_review_0006_revision_02.json"
)
REVIEW_CONFIG_SHA256 = (
    "d973e23584440e50df7d6334c49d55b55538d7ec5a275fefb1540562e0fe8f4c"
)
REVIEW_WORM_TAG = (
    "worm/economic-evolution-failure-review-0006-revision-02-2026-07-13"
)
REVIEW_WORM_COMMIT = "98f14c12ad016feb4c221827122324fe65360bbb"
REVIEW_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/failure_review_0006"
)
REVIEW_RESULT_NAME = "failure_directed_review_result.json"
EXPECTED_N_TRIALS = 452628


class EconomicEvolutionFailureReviewRuntime:
    """Run the zero-multiplicity class review after validation 0005.

    This worker can only read already-produced development artifacts and write
    an immutable report.  It runs no simulation, bootstrap, null, market-data,
    proof, database, registry, shadow, broker or order path.
    """

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / REVIEW_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / REVIEW_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_failure_review_0006.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_failure_review_0006.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_failure_review_freeze(self.root)
        self._verify_predecessor(predecessor, config)
        if self.result_path.is_file():
            result = load_and_verify_failure_review_result(self.result_path, config)
            self._verify_static_protections(review_complete=True)
            return failure_review_action_from_result(predecessor, result)

        self._verify_static_protections(review_complete=False)

        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_failure_review_result(
                    self.result_path, config
                )
                return failure_review_action_from_result(predecessor, result)
            self._record_runtime_state(
                "WORKER_FAILED", worker_exit_code=int(return_code)
            )

        self._quarantine_incomplete_attempt()
        self._start_worker()
        return self._running_action(predecessor)

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
            "new_statistical_comparisons": 0,
            "multiplicity_delta": 0,
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
            != "ECONOMIC_EVOLUTION_EXPENSIVE_VALIDATION_0005_COMPLETE"
            or predecessor.get("economic_expensive_validation_scientific_status")
            != "EXPENSIVE_VALIDATION_UNDERPOWERED"
            or predecessor.get("economic_expensive_validation_candidate_id")
            != candidate_id
            or int(
                predecessor.get(
                    "economic_independent_confirmation_queue_eligible_count", 0
                )
            )
            != 0
        ):
            raise EconomicEvolutionRuntimeError(
                "failure-review predecessor is not the frozen 0005 underpowered path"
            )

    def _verify_static_protections(self, *, review_complete: bool = False) -> None:
        registry = load_and_verify(self.state_dir / "proof_registry.json")
        if burned_window_ids(registry) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError("unexpected proof-window state")
        current_trials = multiplicity_trial_count(registry)
        if not review_complete and current_trials != EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "failure-review multiplicity changed despite zero comparisons"
            )
        if review_complete and current_trials < EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "failure-review multiplicity predecessor regressed"
            )
        review_reservations = [
            row
            for row in registry["entries"]
            if row.get("event_type") == MULTIPLICITY_EVENT
            and REVIEW_ID in str(row.get("event_id") or "")
            and int(row.get("multiplicity", {}).get("delta_trials", 0)) != 0
        ]
        if review_reservations:
            raise EconomicEvolutionRuntimeError(
                "failure-review unexpectedly reserved multiplicity"
            )

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "failure-review worker exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.root / "scripts/run_economic_evolution_failure_review.py"),
            "--output-dir",
            str(self.output_dir),
            "--preregistration",
            str(self.root / REVIEW_CONFIG_RELATIVE_PATH),
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

    def _quarantine_incomplete_attempt(self) -> None:
        if not self.output_dir.exists() or not any(self.output_dir.iterdir()):
            return
        if self.result_path.is_file():
            return
        quarantine = (
            self.root
            / "reports/economic_evolution/quarantine"
            / f"failure_review_0006_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "failure-review quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_FAILURE_REVIEW_0006_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_failure_review_id": REVIEW_ID,
            "economic_failure_review_state": "RUNNING",
            "economic_failure_review_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_failure_review_attempt": self._attempt,
            "economic_failure_review_new_statistical_comparisons": 0,
            "economic_failure_review_multiplicity_delta": 0,
            "raw_global_N_trials": EXPECTED_N_TRIALS,
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "The controller is classifying already-observed 0005 failures. "
                "It executes no new comparison, simulation, proof, data, Q4, "
                "shadow, broker or order path."
            ),
        }

    def _load_runtime_state(self) -> dict[str, Any]:
        if not self.runtime_state_path.is_file():
            return {}
        value = json.loads(self.runtime_state_path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}

    def _record_runtime_state(self, state: str, **extra: Any) -> None:
        _atomic_json(
            self.runtime_state_path,
            {
                "schema": "hydra_economic_evolution_failure_review_runtime_v1",
                "review_id": REVIEW_ID,
                "state": state,
                "attempt": self._attempt,
                "worker_pid": (
                    None if self._process is None else self._process.pid
                ),
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "new_statistical_comparisons": 0,
                "multiplicity_delta": 0,
                "broker_connections": 0,
                "orders": 0,
                **extra,
            },
        )


def verify_failure_review_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    validation_config = verify_expensive_validation_freeze(project)
    validation_result_path = (
        project / VALIDATION_OUTPUT_RELATIVE_PATH / VALIDATION_RESULT_NAME
    )
    validation_result = load_and_verify_expensive_validation_result(
        validation_result_path, validation_config
    )
    validation_action = expensive_validation_action_from_result(
        {
            "action_type": "ECONOMIC_EVOLUTION_INFORMATION_REVIEW_0004_COMPLETE",
            "phase": "4",
        },
        validation_result,
    )
    if (
        validation_action["economic_expensive_validation_scientific_status"]
        != "EXPENSIVE_VALIDATION_UNDERPOWERED"
        or validation_action[
            "economic_independent_confirmation_queue_eligible_count"
        ]
        != 0
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review source validation is not the frozen failure path"
        )
    config_path = project / REVIEW_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != REVIEW_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("failure-review WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", REVIEW_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != REVIEW_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("failure-review WORM tag drift")
    config = load_failure_review_preregistration(config_path)
    if (
        config["review_id"] != REVIEW_ID
        or config["candidate"]["policy_id"]
        != validation_result["candidate_id"]
        or int(config["multiplicity_delta"]) != 0
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review frozen identity or multiplicity drift"
        )
    return config


def load_and_verify_failure_review_result(
    path: str | Path, config: Mapping[str, Any]
) -> dict[str, Any]:
    result_path = Path(path).resolve()
    value = json.loads(result_path.read_text(encoding="utf-8"))
    frozen_hash = str(value.get("result_sha256") or "")
    semantic = dict(value)
    semantic.pop("result_sha256", None)
    if not frozen_hash or stable_hash(semantic) != frozen_hash:
        raise EconomicEvolutionRuntimeError("failure-review result hash drift")
    if (
        value.get("schema") != REVIEW_SCHEMA
        or value.get("review_id") != REVIEW_ID
        or value.get("candidate_id") != config["candidate"]["policy_id"]
        or value.get("candidate_specification_hash")
        != config["candidate"]["policy_specification_hash"]
        or value.get("candidate_exact_status")
        != "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF"
        or value.get("candidate_validated") is not False
        or value.get("class_status")
        != "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
        or value.get("dominant_failure") != "INSUFFICIENT_STATISTICAL_POWER"
        or value.get("retrospective_only") is not True
        or int(value.get("new_statistical_comparisons_executed") or 0) != 0
        or int(value.get("multiplicity_delta") or 0) != 0
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review scientific decision drift"
        )
    decision = value.get("decision") or {}
    if (
        decision.get("class_level_reformulation") is not True
        or decision.get("new_ids_required") is not True
        or any(
            decision.get(key) is not False
            for key in (
                "consume_independent_proof",
                "reuse_q4",
                "purchase_new_data",
                "admit_shadow",
                "mutate_exact_policy",
                "replay_exact_policy_unchanged",
                "remove_tombstones",
                "inherit_status",
            )
        )
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review protected decision drift"
        )
    if (
        int(value.get("pre_holdout_ready_count") or 0) != 0
        or int(value.get("paper_shadow_ready_count") or 0) != 0
        or value.get("proof_window_consumed") is not False
        or int(value.get("q4_access_delta") or 0) != 0
        or int(value.get("new_data_purchase_count") or 0) != 0
        or int(value.get("broker_connections") or 0) != 0
        or int(value.get("orders") or 0) != 0
        or value.get("outbound_order_capability") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review protected-state drift"
        )
    if value.get("next_experiment_id") != config["next_research_class"][
        "next_experiment_id"
    ]:
        raise EconomicEvolutionRuntimeError(
            "failure-review next experiment drift"
        )
    report_path = result_path.parent / "failure_directed_review_report.md"
    if not report_path.is_file() or "## CONTRE" not in report_path.read_text(
        encoding="utf-8"
    ):
        raise EconomicEvolutionRuntimeError(
            "failure-review human report is absent or incomplete"
        )
    return value


def failure_review_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    evidence = result["observed_evidence"]
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_FAILURE_REVIEW_0006_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_failure_review_id": REVIEW_ID,
        "economic_failure_review_state": "COMPLETE",
        "economic_failure_review_candidate_status": result[
            "candidate_exact_status"
        ],
        "economic_failure_review_class_status": result["class_status"],
        "economic_failure_review_dominant_failure": result["dominant_failure"],
        "economic_failure_review_ranked_failures": list(
            result["ranked_failure_dimensions"]
        ),
        "economic_failure_review_new_statistical_comparisons": 0,
        "economic_failure_review_multiplicity_delta": 0,
        "economic_failure_review_stress_2x_net_usd": float(
            evidence["stress_2x_net_usd"]
        ),
        "economic_failure_review_positive_blocks": int(
            evidence["positive_blocks"]
        ),
        "economic_failure_review_block_count": int(evidence["block_count"]),
        "economic_failure_review_consistency_pass_rate": float(
            evidence["consistency_pass_rate_1_5x"]
        ),
        "economic_failure_review_validator_power": float(
            evidence["validator_power"]
        ),
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "economic_independent_confirmation_queue_eligible_count": 0,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": result["next_experiment_id"],
        "next_experiment_state": result["next_experiment_state"],
        "principal_blocker": (
            "The exact 0005 policy remains statistically underpowered; a new "
            "class-level density/diversification campaign must be frozen before "
            "any outcome and cannot inherit its status."
        ),
        "reason": (
            "The zero-multiplicity retrospective review froze the exact policy "
            "and selected a new class-level representation without proof, Q4, "
            "new data, shadow admission or orders."
        ),
    }


def _worker_environment(root: Path) -> dict[str, str]:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = str(root) + (
        os.pathsep + existing if existing else ""
    )
    return environment


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
    "EXPECTED_N_TRIALS",
    "EconomicEvolutionFailureReviewRuntime",
    "REVIEW_CONFIG_RELATIVE_PATH",
    "REVIEW_ID",
    "REVIEW_OUTPUT_RELATIVE_PATH",
    "failure_review_action_from_result",
    "load_and_verify_failure_review_result",
    "verify_failure_review_freeze",
]
