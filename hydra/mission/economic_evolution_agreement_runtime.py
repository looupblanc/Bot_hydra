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
from hydra.mission.economic_evolution_density_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
    TERMINAL_VERDICT_RELATIVE_PATH,
    load_and_verify_density_terminal_verdict,
)
from hydra.mission.economic_evolution_density_runtime import (
    CAMPAIGN_OUTPUT_RELATIVE_PATH as DENSITY_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME as DENSITY_RESULT_NAME,
    verify_density_freeze,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.research.economic_evolution_agreement_campaign import (
    load_and_verify_agreement_preregistration,
    load_and_verify_agreement_result,
)
from hydra.research.economic_evolution_density_campaign import (
    load_and_verify_density_result,
)


CAMPAIGN_ID = NEXT_CAMPAIGN_ID
CAMPAIGN_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_directional_agreement_0008_revision_02.json"
)
CAMPAIGN_CONFIG_SHA256 = (
    "29dab8df0f370caf3baaf2cc9923491f16cccd9825a8c74c78f2934eda2f333c"
)
CAMPAIGN_PREREGISTRATION_HASH = (
    "e3c7126b182928c44db8c277037212f6d162448c7f8d2feb0abc56ae267688f8"
)
CAMPAIGN_WORM_TAG = (
    "worm/economic-evolution-directional-agreement-0008-revision-02-2026-07-13"
)
CAMPAIGN_WORM_COMMIT = "a6a3a67132f1db8d8e8a9d2508dcb04ba2edec86"
CAMPAIGN_IMPLEMENTATION_COMMIT = "e1779adc51686919cd53192d3c38ffd7a572a22b"
CAMPAIGN_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/directional_agreement_0008"
)
CAMPAIGN_RESULT_NAME = "directional_agreement_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_multi_horizon_agreement_0008_"
    "multiplicity_reservation"
)
PRIOR_N_TRIALS = 454_503
MULTIPLICITY_DELTA = 2_400
EXPECTED_N_TRIALS = PRIOR_N_TRIALS + MULTIPLICITY_DELTA


class EconomicEvolutionAgreementRuntime:
    """Controller-owned launcher for WORM development campaign 0008."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / CAMPAIGN_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_agreement_runtime_0008.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_agreement_0008.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_agreement_freeze(self.root)
        self._verify_predecessor(predecessor)
        if self.result_path.is_file():
            result = load_and_verify_agreement_result(self.result_path, config)
            return agreement_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_agreement_result(
                    self.result_path, config
                )
                return agreement_action_from_result(predecessor, result)
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
            != "ECONOMIC_EVOLUTION_DENSITY_0007_TOMBSTONED"
            or predecessor.get("economic_density_terminal_state") != "COMPLETE"
            or predecessor.get("economic_density_terminal_verdict")
            != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
            or predecessor.get("economic_density_parameter_rescue_allowed")
            is not False
            or predecessor.get("economic_density_same_class_relaunch_allowed")
            is not False
            or predecessor.get("economic_density_status_inheritance_allowed")
            is not False
            or int(
                predecessor.get(
                    "economic_density_graveyard_class_signature_count", -1
                )
            )
            != 95
            or int(
                predecessor.get(
                    "economic_density_graveyard_indexed_object_count", -1
                )
            )
            != 115_624
            or predecessor.get("next_experiment_id") != CAMPAIGN_ID
            or int(predecessor.get("raw_global_N_trials", -1))
            != PRIOR_N_TRIALS
            or int(predecessor.get("economic_pre_holdout_ready_count", 0)) != 0
            or int(predecessor.get("economic_paper_shadow_ready_count", 0)) != 0
        ):
            raise EconomicEvolutionRuntimeError(
                "agreement campaign predecessor is not the frozen 0007 tombstone"
            )

    def _ensure_multiplicity_reservation(
        self, config: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        proof_path = self.state_dir / "proof_registry.json"
        registry = load_and_verify(proof_path)
        if burned_window_ids(registry) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError(
                "agreement campaign unexpected proof-window state"
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
                    "existing agreement multiplicity reservation drift"
                )
            return existing
        if multiplicity_trial_count(registry) != PRIOR_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "agreement campaign multiplicity predecessor drift"
            )
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "agreement artifacts exist before multiplicity reservation"
            )
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_AGREEMENT_CAMPAIGN_OUTCOMES",
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
                    "candidate_manifest_hash": config["structural_population"]
                    ["candidate_manifest_hash"],
                    "source_outcomes_from_0007_used": False,
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
                        config["structural_population"]
                        ["matched_null_sleeve_count"]
                    ),
                    "frozen_account_policy_candidates": int(
                        config["structural_population"]["account_policy_count"]
                    ),
                    "maximum_leave_one_out_controls": int(
                        config["funnel"]["maximum_account_policy_evaluations"]
                    )
                    * int(
                        config["funnel"]
                        ["maximum_leave_one_out_controls_per_policy"]
                    ),
                    "method": (
                        "WORM upper-bound reservation for paired agreement/null "
                        "components, account policies and leave-one-out controls "
                        "before any 0008 feature or outcome access."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "directional_agreement_0008_multiplicity_reservation.json",
            {
                "schema": "hydra_directional_agreement_multiplicity_v1",
                "event_id": MULTIPLICITY_EVENT_ID,
                "previous_N_trials": PRIOR_N_TRIALS,
                "delta_trials": MULTIPLICITY_DELTA,
                "cumulative_N_trials": EXPECTED_N_TRIALS,
                "entry_hash": entry["entry_hash"],
                "burned_windows": ["Q4_2024"],
                "feature_results_seen_before_reservation": False,
                "signal_results_seen_before_reservation": False,
                "pnl_results_seen_before_reservation": False,
                "account_results_seen_before_reservation": False,
                "new_data_purchase_count": 0,
                "q4_access_count_delta": 0,
                "outbound_order_count": 0,
                "CONTRE": (
                    "The reservation is conservative and includes candidates "
                    "that may be killed before account replay."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "agreement campaign exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.root / "scripts/run_economic_evolution_agreement_campaign.py"),
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
            / f"directional_agreement_0008_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "agreement campaign quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self, predecessor: Mapping[str, Any], reservation: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_AGREEMENT_0008_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_agreement_campaign_id": CAMPAIGN_ID,
            "economic_agreement_campaign_state": self._campaign_stage(),
            "economic_agreement_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_agreement_attempt": self._attempt,
            "economic_agreement_multiplicity_delta": MULTIPLICITY_DELTA,
            "economic_agreement_frozen_source_count": 22,
            "economic_agreement_real_component_count": 44,
            "economic_agreement_matched_null_count": 44,
            "economic_agreement_frozen_policy_count": 256,
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
                "The controller is evaluating a new closed-bar directional veto "
                "class against paired opposite-state nulls. It consumes no proof, "
                "Q4, new data, shadow or order path."
            ),
        }

    def _campaign_stage(self) -> str:
        if self.result_path.is_file():
            return "COMPLETE"
        state_path = self.output_dir / "agreement_campaign_state.json"
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
                "schema": "hydra_directional_agreement_runtime_v1",
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


def verify_agreement_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    density_config = verify_density_freeze(project)
    density_result = load_and_verify_density_result(
        project / DENSITY_OUTPUT_RELATIVE_PATH / DENSITY_RESULT_NAME,
        density_config,
    )
    terminal = load_and_verify_density_terminal_verdict(
        project, result=density_result
    )
    if (
        terminal["terminal_decision"]["verdict"]
        != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
        or terminal["next_action"]
        != "PREREGISTER_STRUCTURALLY_DISTINCT_0008_REPRESENTATION"
    ):
        raise EconomicEvolutionRuntimeError(
            "agreement campaign source terminal verdict drift"
        )
    config_path = project / CAMPAIGN_CONFIG_RELATIVE_PATH
    if _sha256(config_path) != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("agreement campaign WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-parse", f"{CAMPAIGN_WORM_TAG}^{{commit}}"],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != CAMPAIGN_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("agreement campaign WORM tag drift")
    tagged_blob = subprocess.check_output(
        ["git", "show", f"{CAMPAIGN_WORM_TAG}:{CAMPAIGN_CONFIG_RELATIVE_PATH}"],
        cwd=project,
    )
    if hashlib.sha256(tagged_blob).hexdigest() != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError(
            "agreement campaign tagged blob drift"
        )
    config = load_and_verify_agreement_preregistration(config_path)
    if (
        config["campaign_id"] != CAMPAIGN_ID
        or config["implementation_commit"] != CAMPAIGN_IMPLEMENTATION_COMMIT
        or config["preregistration_hash"] != CAMPAIGN_PREREGISTRATION_HASH
        or int(config["structural_population"]["actual_frozen_source_count"])
        != 22
        or int(config["structural_population"]["real_sleeve_count"]) != 44
        or int(config["structural_population"]["matched_null_sleeve_count"])
        != 44
        or int(config["structural_population"]["account_policy_count"]) != 256
        or int(config["funnel"]["maximum_account_policy_evaluations"]) != 256
        or config["funnel"][
            "all_frozen_account_policies_receive_diagnostic_replay"
        ]
        is not True
        or config["funnel"][
            "scientific_promotion_requires_green_family_tripwire"
        ]
        is not True
        or int(config["multiplicity"]["reserved_delta_trials"])
        != MULTIPLICITY_DELTA
        or int(config["budget"]["N_trials_before_reservation"])
        != PRIOR_N_TRIALS
        or int(config["budget"]["N_trials_after_reservation"])
        != EXPECTED_N_TRIALS
        or config["source_outcomes_from_0007_used"] is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "agreement frozen identity, population or multiplicity drift"
        )
    return config


def agreement_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    tripwire = result["family_tripwire"]
    components = result["component_economics"]
    policies = result["account_policy_economics"]
    pass_probability = policies["combine_pass_probability"]
    progress = policies["median_target_progress_distribution"]
    mll = policies["mll_breach_rate_distribution"]
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_AGREEMENT_0008_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_agreement_campaign_id": CAMPAIGN_ID,
        "economic_agreement_campaign_state": "COMPLETE",
        "economic_agreement_scientific_status": result["scientific_status"],
        "economic_agreement_source_count": int(result["population"]["source_count"]),
        "economic_agreement_real_component_count": int(
            result["population"]["real_sleeve_count"]
        ),
        "economic_agreement_matched_null_count": int(
            result["population"]["matched_null_sleeve_count"]
        ),
        "economic_agreement_frozen_policy_count": int(
            result["population"]["account_policy_count"]
        ),
        "economic_agreement_account_policy_evaluated_count": int(
            result["account_policy_evaluated_count"]
        ),
        "economic_agreement_account_research_candidate_count": int(
            result["account_research_candidate_count"]
        ),
        "economic_agreement_combine_path_diagnostic_count": int(
            result["combine_path_diagnostic_count"]
        ),
        "economic_agreement_real_pass_count": int(tripwire["real_pass_count"]),
        "economic_agreement_null_pass_count": int(tripwire["null_pass_count"]),
        "economic_agreement_positive_after_cost_count": int(
            components["real_positive_after_normal_cost_count"]
        ),
        "economic_agreement_positive_after_stress_count": int(
            components["real_positive_after_stressed_cost_count"]
        ),
        "economic_agreement_matched_null_winner_count": int(
            components["matched_null_component_gate_winner_count"]
        ),
        "economic_agreement_rolling_combine_episode_count": int(
            policies["primary_rolling_combine_episode_count"]
        ),
        "economic_agreement_policies_with_combine_pass_count": int(
            policies["policies_passing_at_least_one_combine_episode"]
        ),
        "economic_agreement_median_combine_pass_probability": pass_probability[
            "median"
        ],
        "economic_agreement_best_combine_pass_probability": pass_probability[
            "maximum"
        ],
        "economic_agreement_median_target_progress": progress["median"],
        "economic_agreement_maximum_target_progress": policies[
            "maximum_target_progress"
        ],
        "economic_agreement_median_mll_breach_rate": mll["median"],
        "economic_agreement_maximum_mll_breach_rate": mll["maximum"],
        "economic_agreement_behaviorally_distinct_policy_count": int(
            policies["behaviorally_distinct_policy_count"]
        ),
        "economic_agreement_failure_vector_distribution": policies[
            "failure_vector_distribution"
        ],
        "economic_agreement_targeted_mutations_selected": policies[
            "targeted_mutations_selected"
        ],
        "economic_agreement_wall_clock_accounting": result[
            "wall_clock_accounting"
        ],
        "economic_agreement_NULL_RATIO": tripwire["NULL_RATIO"],
        "economic_agreement_tripwire_verdict": tripwire["verdict"],
        "economic_agreement_tripwire_evidence_strength": tripwire[
            "evidence_strength"
        ],
        "economic_agreement_real_exact_replay_missing_count": int(
            tripwire["real_exact_replay_missing_count"]
        ),
        "economic_agreement_null_exact_replay_missing_count": int(
            tripwire["null_exact_replay_missing_count"]
        ),
        "economic_agreement_multiplicity_delta": MULTIPLICITY_DELTA,
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
            "0008 remains development-only and cannot consume independent proof "
            "or shadow admission regardless of its family/account result."
        ),
        "reason": (
            "The WORM directional-agreement class completed with paired family "
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
    "MULTIPLICITY_DELTA",
    "PRIOR_N_TRIALS",
    "EconomicEvolutionAgreementRuntime",
    "agreement_action_from_result",
    "verify_agreement_freeze",
]
