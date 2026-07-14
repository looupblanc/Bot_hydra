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
from hydra.mission.economic_evolution_account_state_runtime import (
    CAMPAIGN_OUTPUT_RELATIVE_PATH as ACCOUNT_STATE_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME as ACCOUNT_STATE_RESULT_NAME,
    verify_account_state_freeze,
)
from hydra.mission.economic_evolution_account_state_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    load_and_verify_account_state_terminal_verdict,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.research.economic_evolution_account_state_campaign import (
    load_and_verify_account_state_result,
)
from hydra.research.economic_evolution_account_timeline_campaign import (
    load_and_verify_account_timeline_preregistration,
    load_and_verify_account_timeline_result,
)


CAMPAIGN_ID = NEXT_CAMPAIGN_ID
CAMPAIGN_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_account_timeline_0012.json"
)
CAMPAIGN_CONFIG_SHA256 = (
    "d067d3bfbc92d7d1c75a9c728e3d8277618268d3986fb4bc6334ea4aa9c5b8d9"
)
CAMPAIGN_PREREGISTRATION_HASH = (
    "2145256c8264e181a89f19ba3c8ddfd42590a2ec92f34d3f3d1608c57463b63d"
)
CAMPAIGN_WORM_TAG = "worm/economic-evolution-account-timeline-0012-2026-07-14"
CAMPAIGN_WORM_COMMIT = "65a713b376722e6bf7adfb4942231eed49f895d1"
CAMPAIGN_IMPLEMENTATION_COMMIT = "f9869fa48ce683a4a937e08583ae2d2b0e7bbe5e"
CAMPAIGN_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/account_timeline_0012"
)
CAMPAIGN_RESULT_NAME = "account_timeline_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_account_timeline_0012_multiplicity_reservation"
)
PRIOR_N_TRIALS = 467_703
MULTIPLICITY_DELTA = 3_600
EXPECTED_N_TRIALS = PRIOR_N_TRIALS + MULTIPLICITY_DELTA


class EconomicEvolutionAccountTimelineRuntime:
    """Controller-owned launcher for WORM account-timeline campaign 0012."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / CAMPAIGN_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_account_timeline_runtime_0012.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_account_timeline_0012.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_account_timeline_freeze(self.root)
        self._verify_predecessor(predecessor)
        if self.result_path.is_file():
            result = load_and_verify_account_timeline_result(
                self.result_path, config
            )
            return account_timeline_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_account_timeline_result(
                    self.result_path, config
                )
                return account_timeline_action_from_result(predecessor, result)
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
            process.wait(timeout=20.0)
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
            "component_worker_count": 3,
            "account_worker_count": 3,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "reserved_multiplicity_delta": MULTIPLICITY_DELTA,
            "broker_connections": 0,
            "orders": 0,
        }

    def _verify_predecessor(self, predecessor: Mapping[str, Any]) -> None:
        if (
            predecessor.get("action_type")
            != "ECONOMIC_EVOLUTION_ACCOUNT_STATE_0011_TOMBSTONED"
            or predecessor.get("economic_account_state_terminal_state")
            != "COMPLETE"
            or predecessor.get("economic_account_state_terminal_verdict")
            != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
            or predecessor.get("economic_account_state_parameter_rescue_allowed")
            is not False
            or predecessor.get("economic_account_state_same_class_relaunch_allowed")
            is not False
            or predecessor.get("economic_account_state_status_inheritance_allowed")
            is not False
            or int(
                predecessor.get(
                    "economic_account_state_graveyard_class_signature_count", -1
                )
            )
            != 99
            or int(
                predecessor.get(
                    "economic_account_state_graveyard_indexed_object_count", -1
                )
            )
            != 117_204
            or predecessor.get("next_experiment_id") != CAMPAIGN_ID
            or int(predecessor.get("raw_global_N_trials", -1)) != PRIOR_N_TRIALS
            or int(predecessor.get("economic_pre_holdout_ready_count", 0)) != 0
            or int(predecessor.get("economic_paper_shadow_ready_count", 0)) != 0
        ):
            raise EconomicEvolutionRuntimeError(
                "account-timeline predecessor is not the frozen 0011 tombstone"
            )

    def _ensure_multiplicity_reservation(
        self, config: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        proof_path = self.state_dir / "proof_registry.json"
        registry = load_and_verify(proof_path)
        if burned_window_ids(registry) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError(
                "account-timeline campaign unexpected proof-window state"
            )
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
                or existing["evidence"]["preregistration_hash"]
                != CAMPAIGN_PREREGISTRATION_HASH
                or int(existing["multiplicity"]["cumulative_N_trials"])
                != EXPECTED_N_TRIALS
            ):
                raise EconomicEvolutionRuntimeError(
                    "existing account-timeline multiplicity reservation drift"
                )
            return existing
        if multiplicity_trial_count(registry) != PRIOR_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "account-timeline multiplicity predecessor drift"
            )
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "account-timeline artifacts exist before multiplicity reservation"
            )
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_ACCOUNT_TIMELINE_OUTCOMES",
                "scientific_role": (
                    "DEVELOPMENT_MULTIPLICITY_ONLY_NO_PROOF_WINDOW_CONSUMED"
                ),
                "evidence": {
                    "campaign_id": CAMPAIGN_ID,
                    "class_id": config["class_id"],
                    "worm_path": str(CAMPAIGN_CONFIG_RELATIVE_PATH),
                    "worm_sha256": CAMPAIGN_CONFIG_SHA256,
                    "worm_commit": CAMPAIGN_WORM_COMMIT,
                    "preregistration_hash": CAMPAIGN_PREREGISTRATION_HASH,
                    "policy_manifest_hash": config["structural_population"]
                    ["policy_manifest_hash"],
                    "feature_results_seen": False,
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
                    "real_account_policies": int(
                        config["structural_population"]["policy_pair_count"]
                    ),
                    "matched_control_policies": int(
                        config["structural_population"]["policy_pair_count"]
                    ),
                    "method": (
                        "WORM upper-bound reservation for same-membership own-"
                        "timeline versus deranged-history comparisons before "
                        "any 0012 feature or account outcome."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "account_timeline_0012_multiplicity_reservation.json",
            {
                "schema": "hydra_account_timeline_multiplicity_v1",
                "campaign_id": CAMPAIGN_ID,
                "event_id": MULTIPLICITY_EVENT_ID,
                "previous_N_trials": PRIOR_N_TRIALS,
                "reserved_delta_trials": MULTIPLICITY_DELTA,
                "cumulative_N_trials": EXPECTED_N_TRIALS,
                "proof_window_consumed": False,
                "new_data_purchase_count": 0,
                "q4_access_delta": 0,
                "orders": 0,
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "account-timeline campaign exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(
                self.root
                / "scripts/run_economic_evolution_account_timeline_campaign.py"
            ),
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
            / f"account_timeline_0012_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "account-timeline campaign quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self, predecessor: Mapping[str, Any], reservation: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_account_timeline_campaign_id": CAMPAIGN_ID,
            "economic_account_timeline_campaign_state": self._campaign_stage(),
            "economic_account_timeline_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_account_timeline_attempt": self._attempt,
            "economic_account_timeline_multiplicity_delta": MULTIPLICITY_DELTA,
            "economic_account_timeline_component_count": 48,
            "economic_account_timeline_real_policy_count": 512,
            "economic_account_timeline_matched_control_count": 512,
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "next_experiment_id": CAMPAIGN_ID,
            "next_experiment_state": "RUNNING_WORM_ACCOUNT_TIMELINE_CAMPAIGN",
            "reason": (
                "The controller is replaying 512 own-sleeve completed-timeline "
                "routers against 512 deranged-history controls without proof, "
                "Q4, new data or orders."
            ),
        }

    def _campaign_stage(self) -> str:
        if self.result_path.is_file():
            return "COMPLETE"
        state_path = self.output_dir / "account_timeline_campaign_state.json"
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
                "schema": "hydra_account_timeline_runtime_v1",
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


def verify_account_timeline_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    account_state_config = verify_account_state_freeze(project)
    account_state_result = load_and_verify_account_state_result(
        project / ACCOUNT_STATE_OUTPUT_RELATIVE_PATH / ACCOUNT_STATE_RESULT_NAME,
        account_state_config,
    )
    terminal = load_and_verify_account_state_terminal_verdict(
        project, result=account_state_result
    )
    if (
        terminal["terminal_decision"]["verdict"]
        != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
        or terminal["next_action"] != "PREREGISTER_ACCOUNT_TIMELINE_MECHANISM_0012"
    ):
        raise EconomicEvolutionRuntimeError(
            "account-timeline source terminal verdict drift"
        )
    config_path = project / CAMPAIGN_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("account-timeline WORM file drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-parse", f"{CAMPAIGN_WORM_TAG}^{{commit}}"],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != CAMPAIGN_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("account-timeline WORM tag drift")
    tagged_blob = subprocess.check_output(
        ["git", "show", f"{CAMPAIGN_WORM_TAG}:{CAMPAIGN_CONFIG_RELATIVE_PATH}"],
        cwd=project,
    )
    if hashlib.sha256(tagged_blob).hexdigest() != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("account-timeline tagged blob drift")
    config = load_and_verify_account_timeline_preregistration(config_path)
    structural = config["structural_population"]
    multiplicity = config["multiplicity"]
    if (
        config["campaign_id"] != CAMPAIGN_ID
        or config["source_commit"] != CAMPAIGN_IMPLEMENTATION_COMMIT
        or config["preregistration_hash"] != CAMPAIGN_PREREGISTRATION_HASH
        or int(structural["component_count"]) != 48
        or int(structural["policy_pair_count"]) != 512
        or structural["same_ordered_membership_within_pair"] is not True
        or structural["same_component_event_paths_within_pair"] is not True
        or structural["same_completed_history_values_within_pair"] is not True
        or structural["same_history_source_multiset_within_pair"] is not True
        or structural["past_only_completed_outcomes"] is not True
        or int(multiplicity["reserved_delta_trials"]) != MULTIPLICITY_DELTA
        or int(multiplicity["prior_global_N_trials"]) != PRIOR_N_TRIALS
        or int(multiplicity["expected_global_N_trials_after_reservation"])
        != EXPECTED_N_TRIALS
        or structural["same_class_0011_rescue"] is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "account-timeline frozen identity, population or multiplicity drift"
        )
    return config


def account_timeline_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    tripwire = result["family_tripwire"]
    policies = result["account_policy_economics"]
    controls = result["matched_control_economics"]
    paired = result["paired_account_economics"]
    pass_probability = policies["combine_pass_probability"]
    progress = policies["median_target_progress_distribution"]
    mll = policies["mll_breach_rate_distribution"]
    consistency = policies["stressed_consistency_pass_rate_distribution"]
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_account_timeline_campaign_id": CAMPAIGN_ID,
        "economic_account_timeline_campaign_state": "COMPLETE",
        "economic_account_timeline_scientific_status": result["scientific_status"],
        "economic_account_timeline_component_count": int(
            result["population"]["component_count"]
        ),
        "economic_account_timeline_real_policy_count": int(
            result["population"]["real_policy_count"]
        ),
        "economic_account_timeline_matched_control_count": int(
            result["population"]["matched_control_policy_count"]
        ),
        "economic_account_timeline_policy_pair_evaluated_count": int(
            result["policy_pair_evaluated_count"]
        ),
        "economic_account_timeline_real_win_count": int(tripwire["real_win_count"]),
        "economic_account_timeline_matched_control_win_count": int(
            tripwire["matched_control_win_count"]
        ),
        "economic_account_timeline_NULL_RATIO": tripwire["NULL_RATIO"],
        "economic_account_timeline_tripwire_verdict": tripwire["verdict"],
        "economic_account_timeline_tripwire_evidence_strength": tripwire[
            "evidence_strength"
        ],
        "economic_account_timeline_account_research_candidate_count": int(
            result["account_research_candidate_count"]
        ),
        "economic_account_timeline_combine_path_diagnostic_count": int(
            result["combine_path_diagnostic_count"]
        ),
        "economic_account_timeline_rolling_combine_episode_count": int(
            policies["primary_rolling_combine_episode_count"]
        ),
        "economic_account_timeline_policies_with_combine_pass_count": int(
            policies["policies_passing_at_least_one_combine_episode"]
        ),
        "economic_account_timeline_median_combine_pass_probability": (
            pass_probability["median"]
        ),
        "economic_account_timeline_best_combine_pass_probability": (
            pass_probability["maximum"]
        ),
        "economic_account_timeline_median_target_progress": progress["median"],
        "economic_account_timeline_maximum_target_progress": policies[
            "maximum_target_progress"
        ],
        "economic_account_timeline_median_mll_breach_rate": mll["median"],
        "economic_account_timeline_maximum_mll_breach_rate": mll["maximum"],
        "economic_account_timeline_median_stressed_consistency_pass_rate": (
            consistency["median"]
        ),
        "economic_account_timeline_normal_positive_policy_count": int(
            policies["normal_positive_policy_count"]
        ),
        "economic_account_timeline_stressed_positive_policy_count": int(
            policies["stressed_positive_policy_count"]
        ),
        "economic_account_timeline_control_stressed_positive_policy_count": int(
            controls["stressed_positive_policy_count"]
        ),
        "economic_account_timeline_behaviorally_distinct_policy_count": int(
            policies["behaviorally_distinct_policy_count"]
        ),
        "economic_account_timeline_paired_stressed_net_delta_median_usd": paired[
            "stressed_median_net_delta_usd"
        ]["median"],
        "economic_account_timeline_paired_target_progress_delta_median": paired[
            "stressed_target_progress_delta"
        ]["median"],
        "economic_account_timeline_failure_vector_distribution": policies[
            "failure_vector_distribution"
        ],
        "economic_account_timeline_targeted_mutations_selected": policies[
            "targeted_mutations_selected"
        ],
        "economic_account_timeline_wall_clock_accounting": result[
            "wall_clock_accounting"
        ],
        "economic_account_timeline_multiplicity_delta": MULTIPLICITY_DELTA,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "economic_independent_confirmation_queue_eligible_count": 0,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": result["next_action"],
        "next_experiment_state": "AUTONOMOUS_ACCOUNT_DECISION_REQUIRED",
        "principal_blocker": (
            "0012 remains correlated development evidence and cannot consume "
            "fresh proof or shadow admission regardless of account result."
        ),
        "reason": (
            "The WORM account-timeline class completed same-membership paired "
            "replay without proof, Q4, new data, broker or orders."
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
    "CAMPAIGN_RESULT_NAME",
    "EXPECTED_N_TRIALS",
    "MULTIPLICITY_DELTA",
    "PRIOR_N_TRIALS",
    "EconomicEvolutionAccountTimelineRuntime",
    "account_timeline_action_from_result",
    "verify_account_timeline_freeze",
]
