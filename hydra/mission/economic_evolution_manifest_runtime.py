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
from hydra.mission.economic_evolution_account_timeline_terminal_runtime import (
    NEXT_CAMPAIGN_ID,
)
from hydra.mission.economic_evolution_runtime import (
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
)
from hydra.research.economic_evolution_opportunity_density_campaign import (
    load_and_verify_opportunity_density_preregistration,
    load_and_verify_opportunity_density_result,
)
from hydra.research.v7_graveyard import (
    ClassTombstone,
    append_class_tombstone,
    audit_graveyard,
    verify_class_tombstone,
)


QUEUE_RELATIVE_PATH = Path("config/v7/economic_evolution_production_queue.json")
QUEUE_REVISION_GLOB = "economic_evolution_production_queue_*.json"
SUPPORTED_ENGINES = {"opportunity_density_v1", "manifest_account_pair_v1"}


class EconomicEvolutionManifestRuntime:
    """Reloadable single-process manifest queue for economic campaigns.

    The controller owns this runtime once. Adding another campaign that uses a
    registered engine requires a frozen manifest/queue update, not controller
    source code or another service. Workers remain read-only and all proof-
    registry mutations occur synchronously in the controller process.
    """

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_manifest_runtime.json"
        )
        self.graveyard_path = self.state_dir / "graveyard.db"
        state = self._load_runtime_state()
        self._attempts = {
            str(key): int(value)
            for key, value in (state.get("attempts") or {}).items()
        }
        self._process: subprocess.Popen[bytes] | None = None
        self._active_campaign_id: str | None = None

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        queue = load_and_verify_manifest_queue(self.root)
        action = dict(predecessor)
        enabled = [row for row in queue["entries"] if row.get("enabled") is True]
        if not enabled:
            return self._idle_action(action, "MANIFEST_QUEUE_EMPTY")
        for index, entry in enumerate(enabled):
            config = self._verify_entry(entry)
            campaign_id = str(config["campaign_id"])
            if index == 0:
                self._verify_first_predecessor(action, campaign_id)
            output_dir, result_path = self._paths(config)
            if result_path.is_file():
                result = self._load_result(config, result_path)
                action = self._complete_action(action, config, result)
                action = self._terminalize(action, config, result, output_dir)
                continue

            reservation = self._ensure_multiplicity_reservation(config, output_dir)
            if self._process is not None:
                if self._active_campaign_id != campaign_id:
                    if self._process.poll() is None:
                        raise EconomicEvolutionRuntimeError(
                            "manifest runtime found two active campaigns"
                        )
                    self._process = None
                    self._active_campaign_id = None
                else:
                    return_code = self._process.poll()
                    if return_code is None:
                        return self._running_action(action, config, reservation)
                    self._process = None
                    self._active_campaign_id = None
                    if result_path.is_file():
                        result = self._load_result(config, result_path)
                        action = self._complete_action(action, config, result)
                        action = self._terminalize(
                            action, config, result, output_dir
                        )
                        continue
                    self._record_runtime_state(
                        "WORKER_FAILED",
                        campaign_id=campaign_id,
                        worker_exit_code=int(return_code),
                    )

            self._quarantine_incomplete_attempt(config, output_dir, result_path)
            self._start_worker(config, output_dir)
            return self._running_action(action, config, reservation)

        return self._idle_action(action, "MANIFEST_QUEUE_AWAITING_APPEND")

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
            "WORKER_STOPPED_WITH_CONTROLLER",
            campaign_id=self._active_campaign_id,
            worker_exit_code=process.returncode,
        )
        self._process = None
        self._active_campaign_id = None

    def snapshot(self) -> dict[str, Any]:
        state = self._load_runtime_state()
        return {
            "queue_path": str(_latest_manifest_queue_path(self.root)),
            "state": (
                "RUNNING"
                if self._process is not None and self._process.poll() is None
                else str(state.get("state") or "READY")
            ),
            "active_campaign_id": self._active_campaign_id,
            "worker_pid": (
                self._process.pid
                if self._process is not None and self._process.poll() is None
                else None
            ),
            "attempts": dict(self._attempts),
            "component_worker_count": 3,
            "account_worker_count": 3,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "broker_connections": 0,
            "orders": 0,
        }

    def _verify_first_predecessor(
        self, predecessor: Mapping[str, Any], campaign_id: str
    ) -> None:
        if (
            predecessor.get("action_type")
            != "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_TOMBSTONED"
            or predecessor.get("economic_account_timeline_terminal_state")
            != "COMPLETE"
            or predecessor.get("economic_account_timeline_parameter_rescue_allowed")
            is not False
            or predecessor.get("economic_account_timeline_same_class_relaunch_allowed")
            is not False
            or predecessor.get("next_experiment_id") != campaign_id
            or campaign_id != NEXT_CAMPAIGN_ID
        ):
            raise EconomicEvolutionRuntimeError(
                "manifest runtime predecessor is not terminal 0012"
            )

    def _verify_entry(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        engine = str(entry.get("engine") or "")
        if engine not in SUPPORTED_ENGINES:
            raise EconomicEvolutionRuntimeError(
                f"unsupported manifest engine: {engine}"
            )
        path = self.root / str(entry["preregistration_path"])
        if _sha256(path) != str(entry["preregistration_file_sha256"]):
            raise EconomicEvolutionRuntimeError("campaign manifest checksum drift")
        config = (
            load_and_verify_opportunity_density_preregistration(path)
            if engine == "opportunity_density_v1"
            else _load_and_verify_generic_account_pair_preregistration(path)
        )
        if (
            config.get("preregistration_hash")
            != entry.get("preregistration_semantic_hash")
            or config.get("campaign_id") != entry.get("campaign_id")
            or (config.get("runtime_manifest") or {}).get("engine") != engine
        ):
            raise EconomicEvolutionRuntimeError("campaign queue entry drift")
        tagged_commit = subprocess.check_output(
            ["git", "rev-parse", f"{entry['worm_tag']}^{{commit}}"],
            cwd=self.root,
            text=True,
        ).strip()
        expected_commit = subprocess.check_output(
            ["git", "rev-parse", str(entry["worm_commit"])],
            cwd=self.root,
            text=True,
        ).strip()
        if tagged_commit != expected_commit:
            raise EconomicEvolutionRuntimeError("campaign WORM tag drift")
        tagged_blob = subprocess.check_output(
            ["git", "show", f"{entry['worm_tag']}:{entry['preregistration_path']}"],
            cwd=self.root,
        )
        if hashlib.sha256(tagged_blob).hexdigest() != str(
            entry["preregistration_file_sha256"]
        ):
            raise EconomicEvolutionRuntimeError("campaign tagged blob drift")
        runtime_config = dict(config)
        runtime_config["_runtime_preregistration_path"] = str(path)
        return runtime_config

    def _paths(self, config: Mapping[str, Any]) -> tuple[Path, Path]:
        runtime = config["runtime_manifest"]
        output = (self.root / str(runtime["output_dir"])).resolve()
        allowed = (self.root / "reports/economic_evolution").resolve()
        if output != allowed and allowed not in output.parents:
            raise EconomicEvolutionRuntimeError("campaign output escapes reports")
        result_name = str(runtime["result_name"])
        if Path(result_name).name != result_name:
            raise EconomicEvolutionRuntimeError("campaign result name is unsafe")
        return output, output / result_name

    def _load_result(
        self, config: Mapping[str, Any], path: Path
    ) -> dict[str, Any]:
        engine = str(config["runtime_manifest"]["engine"])
        if engine == "opportunity_density_v1":
            return load_and_verify_opportunity_density_result(path, config)
        if engine == "manifest_account_pair_v1":
            return _load_and_verify_generic_account_pair_result(path, config)
        raise EconomicEvolutionRuntimeError(f"no result loader for {engine}")

    def _ensure_multiplicity_reservation(
        self, config: Mapping[str, Any], output_dir: Path
    ) -> dict[str, Any]:
        proof_path = self.state_dir / "proof_registry.json"
        proof = load_and_verify(proof_path)
        if burned_window_ids(proof) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError(
                "manifest runtime unexpected proof-window state"
            )
        campaign_id = str(config["campaign_id"])
        multiplicity = config["multiplicity"]
        prior = int(multiplicity["prior_global_N_trials"])
        delta = int(multiplicity["reserved_delta_trials"])
        expected = int(multiplicity["expected_global_N_trials_after_reservation"])
        if prior + delta != expected:
            raise EconomicEvolutionRuntimeError("manifest multiplicity arithmetic drift")
        event_id = f"{campaign_id}_multiplicity_reservation"
        reservations = [
            row
            for row in proof["entries"]
            if row.get("event_type") == MULTIPLICITY_EVENT
            and row.get("event_id") == event_id
        ]
        current = multiplicity_trial_count(proof)
        if reservations:
            if len(reservations) != 1 or current < expected:
                raise EconomicEvolutionRuntimeError(
                    "manifest multiplicity reservation drift"
                )
            return reservations[0]
        if output_dir.exists() and any(output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "campaign artifacts exist before multiplicity reservation"
            )
        if current != prior:
            raise EconomicEvolutionRuntimeError(
                f"manifest expected N_trials={prior}, observed {current}"
            )
        entry = append_entry(
            proof_path,
            {
                "event_id": event_id,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED",
                "scientific_role": "DEVELOPMENT_ONLY",
                "evidence": {
                    "campaign_id": campaign_id,
                    "class_id": config["class_id"],
                    "preregistration_hash": config["preregistration_hash"],
                    "feature_results_seen": False,
                    "account_results_seen": False,
                    "proof_window_consumed": False,
                    "new_data_purchase_count": 0,
                    "q4_access_delta": 0,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": prior,
                    "delta_trials": delta,
                    "cumulative_N_trials": expected,
                    "prospective_comparisons": int(
                        multiplicity["prospective_comparisons"]
                    ),
                    "campaign_inflation_factor": float(
                        multiplicity["campaign_specific_inflation"]
                    ),
                    "method": "WORM manifest reservation before campaign outcomes",
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution"
            / f"{campaign_id}_multiplicity_reservation.json",
            {
                "schema": "hydra_manifest_campaign_multiplicity_v1",
                "campaign_id": campaign_id,
                "event_id": event_id,
                "previous_N_trials": prior,
                "reserved_delta_trials": delta,
                "cumulative_N_trials": expected,
                "proof_window_consumed": False,
                "new_data_purchase_count": 0,
                "q4_access_delta": 0,
                "orders": 0,
            },
        )
        return entry

    def _start_worker(self, config: Mapping[str, Any], output_dir: Path) -> None:
        campaign_id = str(config["campaign_id"])
        attempt_key = self._attempt_key(config)
        attempt = self._attempts.get(attempt_key, 0)
        if attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                f"{campaign_id} manifest {config['preregistration_hash']} "
                "exhausted three deterministic attempts"
            )
        attempt += 1
        self._attempts[attempt_key] = attempt
        runtime = config["runtime_manifest"]
        runner = (self.root / str(runtime["runner"])).resolve()
        scripts = (self.root / "scripts").resolve()
        if runner != scripts and scripts not in runner.parents:
            raise EconomicEvolutionRuntimeError("manifest runner escapes scripts")
        config_path = Path(str(config["_runtime_preregistration_path"]))
        manifest_revision = str(config["preregistration_hash"])[:12]
        log_path = (
            self.state_dir
            / "logs"
            / f"{campaign_id}.{manifest_revision}.log"
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(runner),
            "--output-dir",
            str(output_dir),
            "--preregistration",
            str(config_path),
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
        with log_path.open("ab") as log:
            self._process = subprocess.Popen(
                command,
                cwd=self.root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=False,
            )
        self._active_campaign_id = campaign_id
        self._record_runtime_state("RUNNING", campaign_id=campaign_id)

    def _quarantine_incomplete_attempt(
        self,
        config: Mapping[str, Any],
        output_dir: Path,
        result_path: Path,
    ) -> None:
        if not output_dir.exists() or not any(output_dir.iterdir()):
            return
        if result_path.is_file():
            return
        campaign_id = str(config["campaign_id"])
        attempt_key = self._attempt_key(config)
        attempt = self._attempts.get(attempt_key, 0)
        manifest_revision = str(config["preregistration_hash"])[:12]
        quarantine = (
            self.root
            / "reports/economic_evolution/quarantine"
            / f"{campaign_id}_{manifest_revision}_attempt_{attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "manifest campaign quarantine path collision"
            )
        shutil.move(str(output_dir), str(quarantine))

    @staticmethod
    def _attempt_key(config: Mapping[str, Any]) -> str:
        """Identify retries by immutable manifest, not mutable campaign label.

        A technical WORM revision may keep the scientific campaign ID while
        repairing a pre-result integration defect.  Its retry budget must not
        erase or inherit the exhausted budget of the superseded manifest.
        """

        return (
            f"{config['campaign_id']}:"
            f"{config['preregistration_hash']}"
        )

    def _terminalize(
        self,
        action: Mapping[str, Any],
        config: Mapping[str, Any],
        result: Mapping[str, Any],
        output_dir: Path,
    ) -> dict[str, Any]:
        tripwire = result["family_tripwire"]
        if bool(tripwire["family_green"]):
            return {
                **dict(action),
                "manifest_campaign_terminal_state": "SURVIVORS_REQUIRE_NEXT_MANIFEST",
                "next_experiment_id": "FAILURE_GUIDED_SURVIVOR_MUTATION_MANIFEST",
                "next_experiment_state": "WORM_MANIFEST_REQUIRED",
            }
        result_path = output_dir / str(config["runtime_manifest"]["result_name"])
        period = config["data"]["period"]
        tombstone = ClassTombstone(
            mechanism_class=str(config["class_id"]),
            regime=(
                f"DEVELOPMENT_{period[0]}_TO_{period[1]}_"
                "MULTI_MARKET_PAST_ONLY_MANIFEST_RUNTIME"
            ),
            death_cause=(
                "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8"
                if tripwire["verdict"] == "ARTEFACT_GEOMETRY_ONLY"
                else "NO_ECONOMIC_SIGNAL"
            ),
            candidate_count=int(
                config["structural_population"]["policy_pair_count"]
            ),
            source_scope=str(config["campaign_id"]).upper(),
            evidence_sha256=_sha256(result_path),
        )
        receipt_path = output_dir / "graveyard_append_receipt.json"
        if receipt_path.is_file():
            appended = verify_class_tombstone(self.graveyard_path, tombstone)
            receipt = _load_json(receipt_path)
            if receipt.get("class_signature_hash") != tombstone.signature_hash:
                raise EconomicEvolutionRuntimeError(
                    "manifest terminal receipt drift"
                )
        else:
            appended = append_class_tombstone(self.graveyard_path, tombstone)
            receipt = {
                "schema": "hydra_manifest_campaign_graveyard_receipt_v1",
                "campaign_id": config["campaign_id"],
                "class_signature_hash": tombstone.signature_hash,
                "mechanism_class": tombstone.mechanism_class,
                "regime": tombstone.regime,
                "death_cause": tombstone.death_cause,
                "candidate_count": tombstone.candidate_count,
                "evidence_sha256": tombstone.evidence_sha256,
                "class_signature_count": int(appended["class_signature_count"]),
                "indexed_object_count": int(appended["indexed_object_count"]),
                "parameter_level_feedback": False,
                "matched_controls_counted_as_candidates": False,
                "proof_windows_consumed": 0,
                "new_data_purchase_count": 0,
                "q4_access_delta": 0,
                "broker_connections": 0,
                "orders": 0,
            }
            receipt["receipt_hash"] = stable_hash(receipt)
            _atomic_json(receipt_path, receipt)
        return {
            **dict(action),
            "manifest_campaign_terminal_state": "EXACT_CLASS_TOMBSTONED",
            "manifest_campaign_tombstone_signature_hash": tombstone.signature_hash,
            "manifest_campaign_graveyard_class_signature_count": int(
                receipt["class_signature_count"]
            ),
            "manifest_campaign_graveyard_indexed_object_count": int(
                receipt["indexed_object_count"]
            ),
            "manifest_campaign_parameter_rescue_allowed": False,
            "manifest_campaign_same_class_relaunch_allowed": False,
            "next_experiment_id": "NEXT_STRUCTURALLY_DISTINCT_WORM_MANIFEST",
            "next_experiment_state": "MANIFEST_QUEUE_AWAITING_APPEND",
        }

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        config: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_CAMPAIGN_RUNNING",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": config["runtime_manifest"]["engine"],
            "manifest_campaign_state": "RUNNING",
            "manifest_campaign_worker_pid": (
                self._process.pid if self._process is not None else None
            ),
            "manifest_campaign_worker_count": int(
                config["compute"]["account_worker_count"]
            ),
            "manifest_campaign_real_policy_count": int(
                config["structural_population"]["policy_pair_count"]
            ),
            "manifest_campaign_matched_control_count": int(
                config["structural_population"]["policy_pair_count"]
            ),
            "manifest_campaign_reserved_multiplicity_delta": int(
                reservation["multiplicity"]["delta_trials"]
            ),
            "raw_global_N_trials": int(
                config["multiplicity"]["expected_global_N_trials_after_reservation"]
            ),
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "next_experiment_id": config["campaign_id"],
            "next_experiment_state": "RUNNING_MANIFEST_CAMPAIGN",
            "reason": (
                "The stable V17 manifest runtime is executing the frozen "
                "campaign with three read-only research workers."
            ),
            "progressed": True,
        }

    def _complete_action(
        self,
        predecessor: Mapping[str, Any],
        config: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        economics = result["account_policy_economics"]
        tripwire = result["family_tripwire"]
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_CAMPAIGN_COMPLETE",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": config["runtime_manifest"]["engine"],
            "manifest_campaign_state": "COMPLETE",
            "manifest_campaign_scientific_status": result["scientific_status"],
            "manifest_campaign_real_policy_count": int(
                result["population"]["real_policy_count"]
            ),
            "manifest_campaign_matched_control_count": int(
                result["population"]["matched_control_policy_count"]
            ),
            "manifest_campaign_policy_pair_evaluated_count": int(
                result["policy_pair_evaluated_count"]
            ),
            "manifest_campaign_rolling_combine_episode_count": int(
                economics["primary_rolling_combine_episode_count"]
            ),
            "manifest_campaign_policies_with_combine_pass_count": int(
                economics["policies_passing_at_least_one_combine_episode"]
            ),
            "manifest_campaign_best_combine_pass_probability": float(
                economics["combine_pass_probability"]["maximum"]
            ),
            "manifest_campaign_median_combine_pass_probability": float(
                economics["combine_pass_probability"]["median"]
            ),
            "manifest_campaign_median_target_progress": float(
                economics["median_target_progress_distribution"]["median"]
            ),
            "manifest_campaign_maximum_target_progress": float(
                economics["maximum_target_progress"]
            ),
            "manifest_campaign_median_mll_breach_rate": float(
                economics["mll_breach_rate_distribution"]["median"]
            ),
            "manifest_campaign_maximum_mll_breach_rate": float(
                economics["mll_breach_rate_distribution"]["maximum"]
            ),
            "manifest_campaign_normal_positive_policy_count": int(
                economics["normal_positive_policy_count"]
            ),
            "manifest_campaign_stressed_positive_policy_count": int(
                economics["stressed_positive_policy_count"]
            ),
            "manifest_campaign_real_win_count": int(tripwire["real_win_count"]),
            "manifest_campaign_matched_control_win_count": int(
                tripwire["matched_control_win_count"]
            ),
            "manifest_campaign_NULL_RATIO": tripwire["NULL_RATIO"],
            "manifest_campaign_tripwire_verdict": tripwire["verdict"],
            "manifest_campaign_failure_vector_distribution": dict(
                economics["failure_vector_distribution"]
            ),
            "manifest_campaign_targeted_mutations_selected": list(
                economics["targeted_mutations_selected"]
            ),
            "manifest_campaign_wall_clock_accounting": dict(
                result["wall_clock_accounting"]
            ),
            "raw_global_N_trials": int(
                config["multiplicity"]["expected_global_N_trials_after_reservation"]
            ),
            "economic_independent_confirmation_queue_eligible_count": 0,
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "progressed": True,
        }

    def _idle_action(
        self, predecessor: Mapping[str, Any], state: str
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "manifest_campaign_runtime_state": state,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
        }

    def _record_runtime_state(
        self,
        state: str,
        *,
        campaign_id: str | None,
        worker_exit_code: int | None = None,
    ) -> None:
        _atomic_json(
            self.runtime_state_path,
            {
                "schema": "hydra_manifest_campaign_runtime_v1",
                "state": state,
                "campaign_id": campaign_id,
                "attempts": dict(self._attempts),
                "worker_pid": (
                    self._process.pid if self._process is not None else None
                ),
                "worker_exit_code": worker_exit_code,
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "broker_connections": 0,
                "orders": 0,
            },
        )

    def _load_runtime_state(self) -> dict[str, Any]:
        if not self.runtime_state_path.is_file():
            return {}
        return _load_json(self.runtime_state_path)


def load_and_verify_manifest_queue(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    queue = _load_json(_latest_manifest_queue_path(project))
    claimed = queue.get("queue_hash")
    payload = dict(queue)
    payload.pop("queue_hash", None)
    runtime = queue.get("runtime_policy") or {}
    governance = queue.get("governance") or {}
    entries = queue.get("entries") or []
    if (
        queue.get("schema") != "hydra_manifest_campaign_queue_v1"
        or claimed != stable_hash(payload)
        or not entries
        or [int(row["ordinal"]) for row in entries]
        != sorted(int(row["ordinal"]) for row in entries)
        or len({str(row["campaign_id"]) for row in entries}) != len(entries)
        or runtime.get("reload_queue_each_controller_step") is not True
        or runtime.get("controller_source_change_for_new_manifest") is not False
        or runtime.get("single_active_campaign") is not True
        or runtime.get("single_authoritative_mission_writer") is not True
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
        or governance.get("proof_window_consumption_allowed") is not False
    ):
        raise EconomicEvolutionRuntimeError("invalid manifest campaign queue")
    return queue


def _latest_manifest_queue_path(root: str | Path) -> Path:
    """Return the newest immutable queue revision without rewriting a WORM file."""

    project = Path(root).resolve()
    base = project / QUEUE_RELATIVE_PATH
    revisions = sorted(
        (project / QUEUE_RELATIVE_PATH.parent).glob(QUEUE_REVISION_GLOB)
    )
    return revisions[-1] if revisions else base


def _load_and_verify_generic_account_pair_preregistration(path: Path) -> dict[str, Any]:
    config = _load_json(path)
    claimed = config.get("preregistration_hash")
    payload = dict(config)
    payload.pop("preregistration_hash", None)
    structural = config.get("structural_population") or {}
    runtime = config.get("runtime_manifest") or {}
    compute = config.get("compute") or {}
    governance = config.get("governance") or {}
    statuses = config.get("statuses") or {}
    data = config.get("data") or {}
    implementation_files = config.get("implementation_files") or {}
    if (
        config.get("schema") != "hydra_manifest_account_pair_preregistration_v1"
        or claimed != stable_hash(payload)
        or not str(config.get("campaign_id") or "")
        or not str(config.get("class_id") or "")
        or not 1 <= int(structural.get("policy_pair_count", -1)) <= 512
        or not 24
        <= int(
            config.get("rolling_episode_policy", {}).get(
                "maximum_starts", -1
            )
        )
        <= 60
        or int(compute.get("account_worker_count", -1)) != 3
        or runtime.get("engine") != "manifest_account_pair_v1"
        or not str(runtime.get("result_schema") or "")
        or runtime.get("controller_source_change_required") is not False
        or not isinstance(implementation_files, dict)
        or not implementation_files
        or str(runtime.get("runner") or "") not in implementation_files
        or not str(structural.get("policy_manifest_hash") or "")
        or statuses.get("development_only") is not True
        or statuses.get("validated_allowed") is not False
        or statuses.get("pre_holdout_ready_allowed") is not False
        or statuses.get("paper_shadow_ready_allowed") is not False
        or statuses.get("status_inheritance") is not False
        or "Q4_EXCLUDED" not in str(data.get("role") or "")
        or governance.get("q4_access_allowed") is not False
        or governance.get("new_data_purchase_allowed") is not False
        or governance.get("broker_or_orders_allowed") is not False
    ):
        raise EconomicEvolutionRuntimeError("invalid generic account-pair manifest")
    root = _project_root(path)
    for relative, expected in implementation_files.items():
        if _sha256(root / str(relative)) != str(expected):
            raise EconomicEvolutionRuntimeError(
                f"generic account-pair implementation drift: {relative}"
            )
    return config


def _load_and_verify_generic_account_pair_result(
    path: Path,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    result = _load_json(path)
    claimed = result.get("result_sha256")
    payload = dict(result)
    payload.pop("result_sha256", None)
    population = result.get("population") or {}
    economics = result.get("account_policy_economics") or {}
    governance = result.get("governance") or {}
    structural = config["structural_population"]
    expected_pairs = int(structural["policy_pair_count"])
    expected_episodes = expected_pairs * int(
        config["rolling_episode_policy"]["maximum_starts"]
    )
    if (
        result.get("schema") != config["runtime_manifest"]["result_schema"]
        or result.get("campaign_id") != config.get("campaign_id")
        or result.get("class_id") != config.get("class_id")
        or claimed != stable_hash(payload)
        or population.get("manifest_hash") != structural.get("policy_manifest_hash")
        or int(population.get("real_policy_count", -1)) != expected_pairs
        or int(population.get("matched_control_policy_count", -1)) != expected_pairs
        or int(result.get("policy_pair_evaluated_count", -1)) != expected_pairs
        or int(economics.get("primary_rolling_combine_episode_count", -1))
        != expected_episodes
        or int(result.get("pre_holdout_ready_count", -1)) != 0
        or int(result.get("paper_shadow_ready_count", -1)) != 0
        or int(governance.get("proof_windows_consumed", -1)) != 0
        or int(governance.get("new_data_purchase_count", -1)) != 0
        or int(governance.get("q4_access_delta", -1)) != 0
        or int(governance.get("broker_connections", -1)) != 0
        or int(governance.get("orders", -1)) != 0
    ):
        raise EconomicEvolutionRuntimeError(
            "generic account-pair result integrity failure"
        )
    return result


def _project_root(path: Path) -> Path:
    for parent in (path.parent, *path.parents):
        if (parent / "MISSION_CONTRACT.md").is_file():
            return parent
    raise EconomicEvolutionRuntimeError("project root not found")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise EconomicEvolutionRuntimeError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise EconomicEvolutionRuntimeError(f"expected JSON object: {path}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


__all__ = [
    "EconomicEvolutionManifestRuntime",
    "QUEUE_RELATIVE_PATH",
    "load_and_verify_manifest_queue",
]
