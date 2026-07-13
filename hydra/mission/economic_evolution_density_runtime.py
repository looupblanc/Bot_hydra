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
from hydra.mission.economic_evolution_failure_runtime import (
    REVIEW_CONFIG_RELATIVE_PATH,
    REVIEW_ID,
    REVIEW_OUTPUT_RELATIVE_PATH,
    REVIEW_RESULT_NAME,
    failure_review_action_from_result,
    load_and_verify_failure_review_result,
    verify_failure_review_freeze,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.research.economic_evolution_density_campaign import (
    load_and_verify_density_preregistration,
    load_and_verify_density_result,
)


CAMPAIGN_ID = "hydra_economic_evolution_density_diversification_0007"
CAMPAIGN_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_density_diversification_0007.json"
)
CAMPAIGN_CONFIG_SHA256 = (
    "447a655673ff4e44810f9c0753663ba8a11b05e32d5182c5f668c614ea6a1942"
)
CAMPAIGN_WORM_TAG = (
    "worm/economic-evolution-density-diversification-0007-2026-07-13"
)
CAMPAIGN_WORM_COMMIT = "802879db39598bd6562d1feb005003afc94e70d9"
CAMPAIGN_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/density_diversification_0007"
)
CAMPAIGN_RESULT_NAME = "density_diversification_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_density_diversification_0007_"
    "multiplicity_reservation"
)
PRIOR_N_TRIALS = 452_628
MULTIPLICITY_DELTA = 1_875
EXPECTED_N_TRIALS = PRIOR_N_TRIALS + MULTIPLICITY_DELTA


class EconomicEvolutionDensityRuntime:
    """Controller-owned launcher for the WORM 0007 development campaign."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / CAMPAIGN_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_density_runtime_0007.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_density_0007.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_density_freeze(self.root)
        self._verify_predecessor(predecessor)
        if self.result_path.is_file():
            result = load_and_verify_density_result(self.result_path, config)
            return density_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_density_result(self.result_path, config)
                return density_action_from_result(predecessor, result)
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
            "campaign_id": CAMPAIGN_ID,
            "state": state,
            "worker_pid": (
                self._process.pid
                if self._process is not None and self._process.poll() is None
                else None
            ),
            "attempt": self._attempt,
            "result_path": str(self.result_path),
            "exact_worker_count": 3,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "reserved_multiplicity_delta": MULTIPLICITY_DELTA,
            "broker_connections": 0,
            "orders": 0,
        }

    def _verify_predecessor(self, predecessor: Mapping[str, Any]) -> None:
        if (
            predecessor.get("action_type")
            != "ECONOMIC_EVOLUTION_FAILURE_REVIEW_0006_COMPLETE"
            or predecessor.get("economic_failure_review_candidate_status")
            != "FROZEN_DEVELOPMENT_UNDERPOWERED_NO_PROOF"
            or predecessor.get("economic_failure_review_class_status")
            != "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
            or predecessor.get("next_experiment_id") != CAMPAIGN_ID
            or int(predecessor.get("economic_pre_holdout_ready_count", 0)) != 0
            or int(predecessor.get("economic_paper_shadow_ready_count", 0)) != 0
        ):
            raise EconomicEvolutionRuntimeError(
                "density campaign predecessor is not the frozen 0006 class review"
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
                != CAMPAIGN_CONFIG_SHA256
                or int(existing["multiplicity"]["cumulative_N_trials"])
                != EXPECTED_N_TRIALS
            ):
                raise EconomicEvolutionRuntimeError(
                    "existing density multiplicity reservation drift"
                )
            return existing
        if multiplicity_trial_count(registry) != PRIOR_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "density campaign multiplicity predecessor drift"
            )
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "density artifacts exist before multiplicity reservation"
            )
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_DENSITY_CAMPAIGN_OUTCOMES",
                "scientific_role": (
                    "DEVELOPMENT_MULTIPLICITY_ONLY_NO_PROOF_WINDOW_CONSUMED"
                ),
                "evidence": {
                    "campaign_id": CAMPAIGN_ID,
                    "class_id": config["class_id"],
                    "worm_path": str(CAMPAIGN_CONFIG_RELATIVE_PATH),
                    "worm_sha256": CAMPAIGN_CONFIG_SHA256,
                    "worm_commit": CAMPAIGN_WORM_COMMIT,
                    "candidate_manifest_hash": config["structural_population"]
                    ["candidate_manifest_hash"],
                    "feature_results_seen": False,
                    "signal_results_seen": False,
                    "pnl_results_seen": False,
                    "account_results_seen": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": PRIOR_N_TRIALS,
                    "delta_trials": MULTIPLICITY_DELTA,
                    "cumulative_N_trials": EXPECTED_N_TRIALS,
                    "prospective_comparisons": int(
                        config["multiplicity"]["prospective_comparisons"]
                    ),
                    "campaign_inflation_factor": float(
                        config["multiplicity"]["campaign_specific_inflation"]
                    ),
                    "real_component_candidates": int(
                        config["structural_population"]["real_sleeve_count"]
                    ),
                    "matched_null_candidates": int(
                        config["structural_population"]["matched_null_sleeve_count"]
                    ),
                    "frozen_account_policy_candidates": int(
                        config["structural_population"]["account_policy_count"]
                    ),
                    "maximum_leave_one_out_controls": int(
                        config["funnel"]["maximum_account_policy_evaluations"]
                    )
                    * int(
                        config["funnel"]["maximum_leave_one_out_controls_per_policy"]
                    ),
                    "method": (
                        "WORM upper-bound reservation for components, matched family "
                        "nulls, account policies and matched add-one/leave-one-out "
                        "controls before any 0007 feature or outcome access."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "density_diversification_0007_multiplicity_reservation.json",
            {
                "schema": "hydra_density_diversification_multiplicity_v1",
                "event_id": MULTIPLICITY_EVENT_ID,
                "previous_N_trials": PRIOR_N_TRIALS,
                "delta_trials": MULTIPLICITY_DELTA,
                "cumulative_N_trials": EXPECTED_N_TRIALS,
                "entry_hash": entry["entry_hash"],
                "burned_windows": ["Q4_2024"],
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "outbound_order_count": 0,
                "CONTRE": (
                    "The reservation is conservative and includes development "
                    "candidates that may be killed before account replay."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "density campaign worker exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.root / "scripts/run_economic_evolution_density_campaign.py"),
            "--output-dir",
            str(self.output_dir),
            "--preregistration",
            str(self.root / CAMPAIGN_CONFIG_RELATIVE_PATH),
            "--contract-map",
            str(self.root / CONTRACT_MAP_RELATIVE_PATH),
            "--cache-root",
            str(self.root / FEATURE_CACHE_RELATIVE_PATH),
        ]
        environment = dict(os.environ)
        existing = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = str(self.root) + (
            os.pathsep + existing if existing else ""
        )
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
            / f"density_diversification_0007_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "density campaign quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self, predecessor: Mapping[str, Any], reservation: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_DENSITY_0007_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_density_campaign_id": CAMPAIGN_ID,
            "economic_density_campaign_state": self._campaign_stage(),
            "economic_density_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_density_attempt": self._attempt,
            "economic_density_multiplicity_delta": MULTIPLICITY_DELTA,
            "economic_density_frozen_source_count": 22,
            "economic_density_frozen_policy_count": 192,
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "next_experiment_id": CAMPAIGN_ID,
            "next_experiment_state": "RUNNING_WORM_DEVELOPMENT_CAMPAIGN",
            "reason": (
                "The controller is evaluating new density-gated component and "
                "account-policy identities under a preregistered matched family "
                "null. It consumes no proof, Q4, new data, shadow or order path."
            ),
        }

    def _campaign_stage(self) -> str:
        if self.result_path.is_file():
            return "COMPLETE"
        state_path = self.output_dir / "density_campaign_state.json"
        if state_path.is_file():
            try:
                return str(json.loads(state_path.read_text())["stage"])
            except (json.JSONDecodeError, KeyError, TypeError):
                return "INVALID_STATE"
        return "STARTING" if self._process is not None else "READY"

    def _load_runtime_state(self) -> dict[str, Any]:
        if not self.runtime_state_path.is_file():
            return {}
        value = json.loads(self.runtime_state_path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}

    def _record_runtime_state(self, state: str, **extra: Any) -> None:
        _atomic_json(
            self.runtime_state_path,
            {
                "schema": "hydra_density_diversification_runtime_v1",
                "campaign_id": CAMPAIGN_ID,
                "state": state,
                "attempt": self._attempt,
                "worker_pid": (
                    None if self._process is None else self._process.pid
                ),
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "multiplicity_delta": MULTIPLICITY_DELTA,
                "broker_connections": 0,
                "orders": 0,
                **extra,
            },
        )


def verify_density_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    review_config = verify_failure_review_freeze(project)
    review_result_path = (
        project / REVIEW_OUTPUT_RELATIVE_PATH / REVIEW_RESULT_NAME
    )
    review_result = load_and_verify_failure_review_result(
        review_result_path, review_config
    )
    review_action = failure_review_action_from_result(
        {
            "action_type": "ECONOMIC_EVOLUTION_EXPENSIVE_VALIDATION_0005_COMPLETE",
            "phase": "4",
        },
        review_result,
    )
    if (
        review_result["review_id"] != REVIEW_ID
        or review_action["next_experiment_id"] != CAMPAIGN_ID
        or review_action["economic_failure_review_class_status"]
        != "CLASS_REFORMULATION_ALLOWED_NEW_IDS_ONLY"
    ):
        raise EconomicEvolutionRuntimeError(
            "density source review is not the frozen 0006 reformulation path"
        )
    config_path = project / CAMPAIGN_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("density campaign WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", CAMPAIGN_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != CAMPAIGN_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("density campaign WORM tag drift")
    config = load_and_verify_density_preregistration(config_path)
    if (
        config["campaign_id"] != CAMPAIGN_ID
        or int(config["multiplicity"]["reserved_delta_trials"])
        != MULTIPLICITY_DELTA
        or int(config["budget"]["N_trials_before_reservation"])
        != PRIOR_N_TRIALS
        or int(config["budget"]["N_trials_after_reservation"])
        != EXPECTED_N_TRIALS
    ):
        raise EconomicEvolutionRuntimeError(
            "density campaign frozen identity or multiplicity drift"
        )
    return config


def density_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    tripwire = result["family_tripwire"]
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_DENSITY_0007_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_density_campaign_id": CAMPAIGN_ID,
        "economic_density_campaign_state": "COMPLETE",
        "economic_density_scientific_status": result["scientific_status"],
        "economic_density_source_count": int(result["population"]["source_count"]),
        "economic_density_real_component_count": int(
            result["population"]["real_sleeve_count"]
        ),
        "economic_density_matched_null_count": int(
            result["population"]["matched_null_sleeve_count"]
        ),
        "economic_density_frozen_policy_count": int(
            result["population"]["account_policy_count"]
        ),
        "economic_density_account_policy_evaluated_count": int(
            result["account_policy_evaluated_count"]
        ),
        "economic_density_account_research_candidate_count": int(
            result["account_research_candidate_count"]
        ),
        "economic_density_combine_path_diagnostic_count": int(
            result["combine_path_diagnostic_count"]
        ),
        "economic_density_real_pass_count": int(tripwire["real_pass_count"]),
        "economic_density_null_pass_count": int(tripwire["null_pass_count"]),
        "economic_density_NULL_RATIO": tripwire["NULL_RATIO"],
        "economic_density_tripwire_verdict": tripwire["verdict"],
        "economic_density_tripwire_evidence_strength": tripwire[
            "evidence_strength"
        ],
        "economic_density_multiplicity_delta": MULTIPLICITY_DELTA,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "economic_independent_confirmation_queue_eligible_count": 0,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": result["next_action"],
        "next_experiment_state": "AUTONOMOUS_CLASS_DECISION_REQUIRED",
        "principal_blocker": (
            "0007 remains development-only and cannot consume independent proof "
            "or shadow admission regardless of its family/account result."
        ),
        "reason": (
            "The WORM density/diversification class completed with matched family "
            "nulls and account chronology, without proof, Q4, new data or orders."
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
    "CAMPAIGN_CONFIG_RELATIVE_PATH",
    "CAMPAIGN_ID",
    "CAMPAIGN_OUTPUT_RELATIVE_PATH",
    "EXPECTED_N_TRIALS",
    "EconomicEvolutionDensityRuntime",
    "density_action_from_result",
    "verify_density_freeze",
]
