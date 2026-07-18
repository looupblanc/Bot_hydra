from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence import require_complete_evidence_bundle
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
from hydra.production import (
    PRODUCTION_KPI_SCHEMA,
    PRODUCTION_STATE_SCHEMA,
    load_and_validate_production_manifest,
    load_and_verify_production_result,
)
from hydra.production.causal_target_velocity_manifest import (
    CAUSAL_TARGET_VELOCITY_ENGINE,
    load_and_validate_causal_target_velocity_manifest,
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
PRODUCTION_ENGINE = "production_kernel_v1"
PRODUCTION_STATE_NAME = "production_state.json"
PRODUCTION_KPI_NAME = "production_kpis.json"
SUPPORTED_ENGINES = {
    "opportunity_density_v1",
    "manifest_account_pair_v1",
    PRODUCTION_ENGINE,
    CAUSAL_TARGET_VELOCITY_ENGINE,
}
PRODUCTION_LIKE_ENGINES = frozenset(
    {PRODUCTION_ENGINE, CAUSAL_TARGET_VELOCITY_ENGINE}
)
_PRODUCTION_RESUMABLE_STATES = {
    "STARTING",
    "POPULATION_FROZEN",
    "FAST_SCREEN_COMPLETE",
    "COMPONENT_LEDGER_COMPILED",
    "COMPONENT_LEDGER_COMPLETE",
    "EXACT_REPLAY_ACTIVE",
    "FIRST_HALVING_COMPLETE",
    "ROBUSTNESS_ACTIVE",
    "EXPANDED_EPISODES_ACTIVE",
    "FINALIZING",
}
_PRODUCTION_SAFETY_ZERO_FIELDS = {
    "broker_connections",
    "orders",
    "q4_access_count_delta",
    "q4_access_delta",
    "data_purchase_count",
    "new_data_purchase_count",
    "proof_windows_consumed",
}


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
        self._production_no_result_exits = {
            str(key): dict(value)
            for key, value in (state.get("production_no_result_exits") or {}).items()
            if isinstance(value, Mapping)
        }
        self._production_successor_handoffs = (
            self._load_production_successor_handoffs(state)
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._active_campaign_id: str | None = None
        self._active_config: dict[str, Any] | None = None
        self._external_worker_pid: int | None = None
        self._verified_production_results: dict[
            str, tuple[str, dict[str, Any]]
        ] = {}

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
                if (
                    self._active_campaign_id == campaign_id
                    and self._live_worker_pid() is not None
                ):
                    # The runner publishes the terminal result atomically before
                    # exiting. Do not overlap the next campaign during that
                    # short final process window.
                    return action
                if self._active_campaign_id == campaign_id:
                    self._process = None
                    self._external_worker_pid = None
                    self._active_campaign_id = None
                    self._active_config = None
                continue

            reservation = self._ensure_multiplicity_reservation(config, output_dir)
            if (
                self._live_worker_pid() is not None
                and self._active_campaign_id != campaign_id
            ):
                raise EconomicEvolutionRuntimeError(
                    "manifest runtime found two active campaigns"
                )
            if self._process is not None:
                if self._active_campaign_id != campaign_id:
                    if self._process.poll() is None:
                        raise EconomicEvolutionRuntimeError(
                            "manifest runtime found two active campaigns"
                        )
                    self._process = None
                    self._active_campaign_id = None
                    self._active_config = None
                else:
                    return_code = self._process.poll()
                    if return_code is None:
                        return self._running_action(action, config, reservation)
                    self._process = None
                    if result_path.is_file():
                        result = self._load_result(config, result_path)
                        self._active_campaign_id = None
                        self._active_config = None
                        action = self._complete_action(action, config, result)
                        action = self._terminalize(
                            action, config, result, output_dir
                        )
                        continue
                    if _is_production_like(config):
                        repeated = self._record_production_no_result_exit(
                            config,
                            output_dir,
                            worker_exit_code=int(return_code),
                        )
                    else:
                        repeated = False
                        self._record_runtime_state(
                            "WORKER_FAILED",
                            campaign_id=campaign_id,
                            worker_exit_code=int(return_code),
                        )
                    self._active_campaign_id = None
                    self._active_config = None
                    if repeated:
                        raise EconomicEvolutionRuntimeError(
                            "production runner exited twice at the same economic "
                            "checkpoint without a terminal result; relaunch loop stopped"
                        )

            if self._adopt_running_production_worker(config, output_dir):
                return self._running_action(action, config, reservation)

            self._quarantine_incomplete_attempt(config, output_dir, result_path)
            self._start_worker(config, output_dir)
            return self._running_action(action, config, reservation)

        return self._idle_action(action, "MANIFEST_QUEUE_AWAITING_APPEND")

    def stop(self) -> None:
        process = self._process
        if process is None or process.poll() is not None:
            if (
                self._external_worker_pid is not None
                and self._active_config is not None
                and self._production_pid_matches(
                    self._external_worker_pid, self._active_config
                )
            ):
                os.kill(self._external_worker_pid, 15)
                self._record_runtime_state(
                    "ADOPTED_WORKER_STOPPED_WITH_CONTROLLER",
                    campaign_id=self._active_campaign_id,
                )
            self._external_worker_pid = None
            self._active_campaign_id = None
            self._active_config = None
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
        self._active_config = None

    def snapshot(self) -> dict[str, Any]:
        state = self._load_runtime_state()
        worker_pid = self._live_worker_pid()
        production_kpis: dict[str, Any] | None = None
        production_state_path: str | None = None
        production_kpi_path: str | None = None
        if self._active_config is not None and _is_production_like(self._active_config):
            output_dir, _ = self._paths(self._active_config)
            production_state_path = str(output_dir / PRODUCTION_STATE_NAME)
            production_kpi_path = str(output_dir / PRODUCTION_KPI_NAME)
            production_kpis = self._load_production_kpis(
                self._active_config, output_dir
            )
        return {
            "queue_path": str(_latest_manifest_queue_path(self.root)),
            "state": (
                "RUNNING"
                if worker_pid is not None
                else str(state.get("state") or "READY")
            ),
            "active_campaign_id": self._active_campaign_id,
            "worker_pid": worker_pid,
            "attempts": dict(self._attempts),
            "component_worker_count": 3,
            "account_worker_count": 3,
            "production_research_worker_count": (
                3
                if self._active_config is not None
                and _is_production_like(self._active_config)
                else 0
            ),
            "production_evidence_writer_count": (
                1
                if self._active_config is not None
                and _is_production_like(self._active_config)
                else 0
            ),
            "authoritative_mission_writer_count": 1,
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "production_state_path": production_state_path,
            "production_kpi_path": production_kpi_path,
            "production_kpis": production_kpis,
            "production_successor_handoff_count": len(
                self._production_successor_handoffs
            ),
            "latest_production_successor_handoff": (
                dict(self._production_successor_handoffs[-1])
                if self._production_successor_handoffs
                else None
            ),
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
        }

    def _live_worker_pid(self) -> int | None:
        if self._process is not None and self._process.poll() is None:
            return int(self._process.pid)
        if (
            self._external_worker_pid is not None
            and self._active_config is not None
            and self._production_pid_matches(
                self._external_worker_pid, self._active_config
            )
        ):
            return self._external_worker_pid
        return None

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
        path_key = (
            "manifest_path"
            if engine in PRODUCTION_LIKE_ENGINES
            else "preregistration_path"
        )
        hash_key = (
            "manifest_file_sha256"
            if engine in PRODUCTION_LIKE_ENGINES
            else "preregistration_file_sha256"
        )
        semantic_key = (
            "manifest_semantic_hash"
            if engine in PRODUCTION_LIKE_ENGINES
            else "preregistration_semantic_hash"
        )
        # The queue schema historically calls every frozen campaign document a
        # preregistration.  Accept that spelling for production entries too,
        # while rejecting entries that provide neither complete form.
        if path_key not in entry and engine in PRODUCTION_LIKE_ENGINES:
            path_key = "preregistration_path"
            hash_key = "preregistration_file_sha256"
            semantic_key = "preregistration_semantic_hash"
        path = self.root / str(entry[path_key])
        if _sha256(path) != str(entry[hash_key]):
            raise EconomicEvolutionRuntimeError("campaign manifest checksum drift")
        if engine == "opportunity_density_v1":
            config = load_and_verify_opportunity_density_preregistration(path)
        elif engine == "manifest_account_pair_v1":
            config = _load_and_verify_generic_account_pair_preregistration(path)
        elif engine == PRODUCTION_ENGINE:
            config = load_and_validate_production_manifest(path)
        else:
            config = load_and_validate_causal_target_velocity_manifest(path)
        if (
            _manifest_revision(config) != entry.get(semantic_key)
            or config.get("campaign_id") != entry.get("campaign_id")
            or _runtime_manifest(config).get("engine") != engine
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
            ["git", "show", f"{entry['worm_tag']}:{entry[path_key]}"],
            cwd=self.root,
        )
        if hashlib.sha256(tagged_blob).hexdigest() != str(
            entry[hash_key]
        ):
            raise EconomicEvolutionRuntimeError("campaign tagged blob drift")
        if engine in PRODUCTION_LIKE_ENGINES:
            self._verify_production_deployment_ancestry(config, tagged_commit)
        runtime_config = dict(config)
        runtime_config["_runtime_preregistration_path"] = str(path)
        return runtime_config

    def _verify_production_deployment_ancestry(
        self,
        config: Mapping[str, Any],
        worm_commit: str,
    ) -> None:
        source_commit = str(config.get("source_commit") or "")
        live_head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=self.root,
            text=True,
        ).strip()
        relationships = (
            (source_commit, worm_commit, "WORM commit is not descended from source_commit"),
            (worm_commit, live_head, "live HEAD is not descended from the WORM commit"),
        )
        for ancestor, descendant, error in relationships:
            completed = subprocess.run(
                ["git", "merge-base", "--is-ancestor", ancestor, descendant],
                cwd=self.root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if completed.returncode != 0:
                raise EconomicEvolutionRuntimeError(
                    f"unsafe production deployment ancestry: {error}"
                )

    def _paths(self, config: Mapping[str, Any]) -> tuple[Path, Path]:
        runtime = _runtime_manifest(config)
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
        if _is_production_like(config):
            result_file_sha256 = _sha256(path)
            cached = self._verified_production_results.get(str(path.resolve()))
            if cached is not None and cached[0] == result_file_sha256:
                return dict(cached[1])
            result = load_and_verify_production_result(path, config)
            self._require_production_terminal_evidence(config, result)
            self._verified_production_results[str(path.resolve())] = (
                result_file_sha256,
                dict(result),
            )
            return result
        engine = _engine(config)
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
                    "preregistration_hash": _manifest_revision(config),
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
        production_resume = self._production_resume_state(config, output_dir)
        if attempt >= 3 and production_resume is None:
            raise EconomicEvolutionRuntimeError(
                f"{campaign_id} manifest {_manifest_revision(config)} "
                "exhausted three deterministic attempts"
            )
        # Controller/service restarts resume a verified production checkpoint;
        # they are not new deterministic research attempts and must not consume
        # the three-attempt integration budget.
        if production_resume is None:
            attempt += 1
            self._attempts[attempt_key] = attempt
        runtime = _runtime_manifest(config)
        runner = (self.root / str(runtime["runner"])).resolve()
        scripts = (self.root / "scripts").resolve()
        if runner != scripts and scripts not in runner.parents:
            raise EconomicEvolutionRuntimeError("manifest runner escapes scripts")
        config_path = Path(str(config["_runtime_preregistration_path"]))
        manifest_revision = _manifest_revision(config)[:12]
        log_path = (
            self.state_dir
            / "logs"
            / f"{campaign_id}.{manifest_revision}.log"
        )
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if _is_production_like(config):
            command = [
                sys.executable,
                str(runner),
                "--manifest",
                str(config_path),
                "--contract-map",
                str(self.root / CONTRACT_MAP_RELATIVE_PATH),
                "--cache-root",
                str(self.root / FEATURE_CACHE_RELATIVE_PATH),
            ]
        else:
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
                "HYDRA_Q4_ACCESS_ALLOWED": "0",
                "HYDRA_NEW_DATA_PURCHASE_ALLOWED": "0",
                "HYDRA_BROKER_CONNECTION_ALLOWED": "0",
                "HYDRA_ORDERS_ALLOWED": "0",
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
        self._active_config = dict(config)
        self._external_worker_pid = None
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
        if _is_production_like(config):
            # A production output directory is a resumable checkpoint, not a
            # disposable failed attempt. Validation is deliberately performed
            # before relaunch; invalid or FAILED_CLOSED states stop the runtime
            # without moving authoritative evidence out from under provenance.
            self._production_resume_state(config, output_dir, required=True)
            return
        campaign_id = str(config["campaign_id"])
        attempt_key = self._attempt_key(config)
        attempt = self._attempts.get(attempt_key, 0)
        manifest_revision = _manifest_revision(config)[:12]
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
            f"{_manifest_revision(config)}"
        )

    def _production_resume_state(
        self,
        config: Mapping[str, Any],
        output_dir: Path,
        *,
        required: bool = False,
    ) -> dict[str, Any] | None:
        if not _is_production_like(config):
            return None
        state_path = output_dir / PRODUCTION_STATE_NAME
        if not state_path.is_file():
            if required:
                raise EconomicEvolutionRuntimeError(
                    "production output exists without a resumable state checkpoint"
                )
            return None
        state = self._load_production_live_file(
            state_path,
            schema=PRODUCTION_STATE_SCHEMA,
            hash_field="state_hash",
            config=config,
        )
        production_state = str(state.get("state") or "")
        if production_state == "FAILED_CLOSED":
            raise EconomicEvolutionRuntimeError(
                "production checkpoint is FAILED_CLOSED; automatic retry forbidden"
            )
        if production_state == "COMPLETE":
            raise EconomicEvolutionRuntimeError(
                "production state is COMPLETE but the atomic terminal result is missing"
            )
        if production_state not in _PRODUCTION_RESUMABLE_STATES:
            raise EconomicEvolutionRuntimeError(
                f"production checkpoint state is not resumable: {production_state}"
            )
        for field in (
            "checkpoint_sequence",
            "runner_pid",
            "worker_count",
            "evidence_writer_count",
            "policies_proposed",
            "unique_policies_screened",
            "exact_account_replays",
            "combine_episodes_completed",
        ):
            value = state.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise EconomicEvolutionRuntimeError(
                    f"invalid production state counter: {field}"
                )
        # Historical production manifests predate an explicit topology field
        # and are canonically three-worker campaigns.  New manifests may
        # freeze another positive count (0032 freezes two CPU workers).
        expected_workers = int(_runtime_manifest(config).get("worker_count", 3))
        expected_writer = int(
            _runtime_manifest(config).get(
                "asynchronous_evidence_writer_count",
                _runtime_manifest(config).get("authoritative_writer_count", 1),
            )
        )
        if (
            expected_workers < 1
            or int(state["worker_count"]) != expected_workers
            or int(state["evidence_writer_count"]) != expected_writer
        ):
            raise EconomicEvolutionRuntimeError(
                "production checkpoint worker topology drift"
            )
        self._verify_production_evidence_paths(config, state)
        return state

    def _record_production_no_result_exit(
        self,
        config: Mapping[str, Any],
        output_dir: Path,
        *,
        worker_exit_code: int,
    ) -> bool:
        state = self._production_resume_state(config, output_dir, required=True)
        assert state is not None
        progress = {
            field: state.get(field)
            for field in (
                "state",
                "stage",
                "policies_proposed",
                "unique_policies_screened",
                "exact_account_replays",
                "combine_episodes_completed",
                "next_action",
                "last_completed_policy_id",
            )
        }
        fingerprint = stable_hash(progress)
        key = self._attempt_key(config)
        prior = self._production_no_result_exits.get(key) or {}
        count = (
            int(prior.get("identical_exit_count", 0)) + 1
            if prior.get("progress_fingerprint") == fingerprint
            else 1
        )
        self._production_no_result_exits[key] = {
            "progress_fingerprint": fingerprint,
            "identical_exit_count": count,
            "checkpoint_sequence": int(state["checkpoint_sequence"]),
            "state": str(state["state"]),
            "stage": str(state["stage"]),
        }
        self._record_runtime_state(
            "PRODUCTION_WORKER_EXITED_WITHOUT_RESULT",
            campaign_id=str(config["campaign_id"]),
            worker_exit_code=worker_exit_code,
        )
        return count >= 2

    def _load_production_kpis(
        self,
        config: Mapping[str, Any],
        output_dir: Path,
    ) -> dict[str, Any] | None:
        path = output_dir / PRODUCTION_KPI_NAME
        if not path.is_file():
            return None
        snapshot = self._load_production_live_file(
            path,
            schema=PRODUCTION_KPI_SCHEMA,
            hash_field="kpi_hash",
            config=config,
        )
        rates = snapshot.get("rates_per_hour")
        workers = snapshot.get("workers")
        required_counters = {
            "checkpoint_sequence",
            "policies_proposed",
            "unique_policies_screened",
            "exact_account_replays",
            "combine_episodes_completed",
            "normal_episodes_completed",
            "stressed_episodes_completed",
            "positive_stressed_net_candidates",
            "candidates_with_normal_pass",
            "candidates_with_stressed_pass",
            "near_pass_count",
            "candidates_promoted_96",
            "confirmation_ready_candidates",
        }
        required_unit_metrics = {
            "best_normal_pass_rate",
            "best_stressed_pass_rate",
            "median_normal_pass_rate",
            "median_stressed_pass_rate",
            "duplicate_rejection_rate",
            "cache_hit_rate",
            "economic_research_wall_clock_fraction",
            "cpu_utilization_fraction",
        }
        required_rates = {
            "policies_proposed",
            "unique_policies_screened",
            "exact_account_replays",
            "combine_episodes",
        }
        if (
            not isinstance(rates, Mapping)
            or not required_rates.issubset(rates)
            or not isinstance(workers, Mapping)
            or int(workers.get("compute", -1))
            != int(_runtime_manifest(config).get("worker_count", 3))
            or int(workers.get("evidence_writer", -1)) != 1
            or snapshot.get("state") not in _PRODUCTION_RESUMABLE_STATES
            | {"COMPLETE", "FAILED_CLOSED"}
            or not isinstance(snapshot.get("admin_overhead_alert"), bool)
            or not str(snapshot.get("matched_controls_status") or "")
            or not str(snapshot.get("null_status") or "")
        ):
            raise EconomicEvolutionRuntimeError("production KPI topology drift")
        for field in required_counters:
            number = snapshot.get(field)
            if not isinstance(number, int) or isinstance(number, bool) or number < 0:
                raise EconomicEvolutionRuntimeError(
                    f"invalid production KPI counter: {field}"
                )
        for field in required_unit_metrics:
            number = snapshot.get(field)
            if (
                not isinstance(number, (int, float))
                or isinstance(number, bool)
                or not math.isfinite(float(number))
                or not 0.0 <= float(number) <= 1.0
            ):
                raise EconomicEvolutionRuntimeError(
                    f"invalid production KPI fraction: {field}"
                )
        if (
            int(snapshot["combine_episodes_completed"])
            != int(snapshot["normal_episodes_completed"])
            + int(snapshot["stressed_episodes_completed"])
            or int(snapshot["normal_episodes_completed"])
            != int(snapshot["stressed_episodes_completed"])
            or int(snapshot["unique_policies_screened"])
            > int(snapshot["policies_proposed"])
            or int(snapshot["exact_account_replays"])
            > int(snapshot["unique_policies_screened"])
            or int(snapshot["candidates_promoted_96"])
            > int(snapshot["exact_account_replays"])
            or int(snapshot["confirmation_ready_candidates"])
            > int(snapshot["exact_account_replays"])
        ):
            raise EconomicEvolutionRuntimeError(
                "production KPI economic counter reconciliation failed"
            )
        self._require_finite_nonnegative_numbers(rates, "rates_per_hour")
        return snapshot

    def _load_production_live_file(
        self,
        path: Path,
        *,
        schema: str,
        hash_field: str,
        config: Mapping[str, Any],
    ) -> dict[str, Any]:
        snapshot = _load_json(path)
        claimed = snapshot.get(hash_field)
        payload = dict(snapshot)
        payload.pop(hash_field, None)
        if (
            snapshot.get("schema") != schema
            or snapshot.get("campaign_id") != config.get("campaign_id")
            or snapshot.get("manifest_hash") != _manifest_revision(config)
            or snapshot.get("source_commit") != config.get("source_commit")
            or not isinstance(claimed, str)
            or claimed != stable_hash(payload)
        ):
            raise EconomicEvolutionRuntimeError(
                f"production live snapshot integrity failure: {path.name}"
            )
        self._verify_zero_safety_fields(snapshot, path.name)
        return snapshot

    @staticmethod
    def _require_finite_nonnegative_numbers(
        value: Mapping[str, Any], label: str
    ) -> None:
        for field, number in value.items():
            if (
                not isinstance(number, (int, float))
                or isinstance(number, bool)
                or not math.isfinite(float(number))
                or float(number) < 0.0
            ):
                raise EconomicEvolutionRuntimeError(
                    f"invalid production KPI number: {label}.{field}"
                )

    @staticmethod
    def _verify_zero_safety_fields(
        value: Mapping[str, Any], label: str
    ) -> None:
        containers = [value]
        for key in ("governance", "safety"):
            nested = value.get(key)
            if isinstance(nested, Mapping):
                containers.append(nested)
        observed: set[str] = set()
        for container in containers:
            for field in _PRODUCTION_SAFETY_ZERO_FIELDS:
                if field not in container:
                    continue
                observed.add(field)
                number = container[field]
                if (
                    not isinstance(number, (int, float))
                    or isinstance(number, bool)
                    or float(number) != 0.0
                ):
                    raise EconomicEvolutionRuntimeError(
                        f"production safety invariant violated: {label}.{field}"
                    )
        # State snapshots are the controller's live safety proof and must carry
        # the four invariants emitted by the runner. KPI snapshots may mirror
        # them, but are permitted to omit duplicates.
        required_safety = (
            {
                "broker_connections",
                "orders",
                "q4_access_count_delta",
                "data_purchase_count",
            }
            if label in {PRODUCTION_STATE_NAME, PRODUCTION_KPI_NAME}
            else {
                "broker_connections",
                "orders",
                "q4_access_delta",
                "new_data_purchase_count",
            }
            if label == "production_result"
            else set()
        )
        if not required_safety.issubset(observed):
            raise EconomicEvolutionRuntimeError(
                f"{label} omits mandatory production safety counters"
            )

    def _verify_production_evidence_paths(
        self,
        config: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> None:
        evidence = config.get("evidence_bundle") or {}
        if not isinstance(evidence, Mapping):
            raise EconomicEvolutionRuntimeError("production EvidenceBundle config missing")
        base = (self.root / str(evidence.get("destination") or "")).resolve()
        allowed = (self.root / "data/cache/evidence_bundles").resolve()
        if base != allowed:
            raise EconomicEvolutionRuntimeError(
                "production EvidenceBundle destination drift"
            )
        for field in ("evidence_staging_path", "evidence_final_path"):
            raw = str(state.get(field) or "")
            if not raw:
                continue
            path = Path(raw)
            resolved = (self.root / path).resolve() if not path.is_absolute() else path.resolve()
            if resolved != allowed and allowed not in resolved.parents:
                raise EconomicEvolutionRuntimeError(
                    f"production {field} escapes the evidence cache"
                )

    def _adopt_running_production_worker(
        self,
        config: Mapping[str, Any],
        output_dir: Path,
    ) -> bool:
        if not _is_production_like(config):
            return False
        state = self._production_resume_state(config, output_dir)
        if state is None:
            return False
        pid = int(state["runner_pid"])
        if not self._production_pid_matches(pid, config):
            return False
        if self._active_campaign_id not in (None, str(config["campaign_id"])):
            raise EconomicEvolutionRuntimeError(
                "manifest runtime found two active production campaigns"
            )
        self._external_worker_pid = pid
        self._active_campaign_id = str(config["campaign_id"])
        self._active_config = dict(config)
        self._record_runtime_state("RUNNING_ADOPTED", campaign_id=self._active_campaign_id)
        return True

    def _production_pid_matches(
        self, pid: int, config: Mapping[str, Any]
    ) -> bool:
        if pid <= 0:
            return False
        try:
            command = (Path("/proc") / str(pid) / "cmdline").read_bytes()
        except OSError:
            return False
        runner = str((self.root / str(_runtime_manifest(config)["runner"])).resolve())
        manifest = str(Path(str(config["_runtime_preregistration_path"])).resolve())
        arguments = [part.decode(errors="replace") for part in command.split(b"\0") if part]
        return runner in arguments and manifest in arguments

    def _require_production_terminal_evidence(
        self,
        config: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> None:
        receipt = result.get("evidence_bundle")
        if not isinstance(receipt, Mapping):
            raise EconomicEvolutionRuntimeError(
                "production COMPLETE forbidden without an EvidenceBundle receipt"
            )
        raw_path = str(receipt.get("bundle_path") or "")
        if not raw_path:
            raise EconomicEvolutionRuntimeError(
                "production EvidenceBundle receipt omits bundle_path"
            )
        path = Path(raw_path)
        bundle_path = (self.root / path).resolve() if not path.is_absolute() else path.resolve()
        allowed = (self.root / "data/cache/evidence_bundles").resolve()
        if bundle_path == allowed or allowed not in bundle_path.parents:
            raise EconomicEvolutionRuntimeError(
                "production terminal EvidenceBundle escapes the evidence cache"
            )
        try:
            verified = require_complete_evidence_bundle(
                bundle_path,
                campaign_id=str(config["campaign_id"]),
                deep=True,
            )
        except Exception as exc:
            raise EconomicEvolutionRuntimeError(
                "production terminal EvidenceBundle is incomplete"
            ) from exc
        manifest_path = bundle_path / "evidence_bundle_manifest.json"
        receipt_manifest = Path(str(receipt.get("manifest_path") or ""))
        receipt_manifest = (
            (self.root / receipt_manifest).resolve()
            if not receipt_manifest.is_absolute()
            else receipt_manifest.resolve()
        )
        evidence_contract = config.get("evidence_bundle")
        evidence_contract = (
            evidence_contract if isinstance(evidence_contract, Mapping) else {}
        )
        expected_status = str(
            evidence_contract.get(
                "evidence_status", "FRESH_DEVELOPMENT_EVIDENCE"
            )
        )
        expected_reconstruction = evidence_contract.get(
            "reconstruction_flag", False
        )
        if (expected_status, expected_reconstruction) not in {
            ("FRESH_DEVELOPMENT_EVIDENCE", False),
            ("AUTHORITATIVE_DEVELOPMENT_RECONSTRUCTION", True),
        }:
            raise EconomicEvolutionRuntimeError(
                "production manifest declares an invalid evidence class"
            )
        if (
            receipt.get("contract") != "HYDRA_EVIDENCE_BUNDLE_V1"
            or int(receipt.get("schema_version", -1)) != 1
            or receipt.get("campaign_id") != config.get("campaign_id")
            or receipt_manifest != manifest_path.resolve()
            or receipt.get("manifest_sha256") != _sha256(manifest_path)
            or receipt.get("bundle_content_sha256")
            != verified.get("bundle_content_sha256")
            or receipt.get("dataset_row_counts")
            != verified.get("dataset_row_counts")
            or receipt.get("evidence_status") != verified.get("evidence_status")
            or receipt.get("reconstruction_flag")
            is not verified.get("reconstruction_flag")
            or result.get("evidence_verification_manifest_sha256")
            != receipt.get("manifest_sha256")
            or receipt.get("evidence_status") != expected_status
            or receipt.get("reconstruction_flag")
            is not expected_reconstruction
        ):
            raise EconomicEvolutionRuntimeError(
                "production terminal EvidenceBundle receipt drift"
            )

    @staticmethod
    def _load_production_successor_handoffs(
        state: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        raw = state.get("production_successor_handoffs") or []
        if not isinstance(raw, list):
            raise EconomicEvolutionRuntimeError(
                "production successor handoff chain is invalid"
            )
        verified: list[dict[str, Any]] = []
        previous_hash: str | None = None
        for sequence, item in enumerate(raw, start=1):
            if not isinstance(item, Mapping):
                raise EconomicEvolutionRuntimeError(
                    "production successor handoff entry is invalid"
                )
            entry = dict(item)
            claimed = str(entry.pop("handoff_hash", ""))
            recommendation = entry.get("recommendation")
            if (
                entry.get("schema") != "hydra_production_successor_handoff_v1"
                or entry.get("sequence") != sequence
                or entry.get("previous_handoff_hash") != previous_hash
                or not claimed
                or stable_hash(entry) != claimed
                or not isinstance(recommendation, Mapping)
            ):
                raise EconomicEvolutionRuntimeError(
                    "production successor handoff hash-chain drift"
                )
            entry["handoff_hash"] = claimed
            verified.append(entry)
            previous_hash = claimed
        return verified

    @staticmethod
    def _verify_production_successor_recommendation(
        recommendation: Mapping[str, Any],
    ) -> None:
        action = recommendation.get("action")
        if (
            not isinstance(action, str)
            or not action.strip()
            or not isinstance(recommendation.get("manifest_required"), bool)
            or recommendation.get("q4_access_authorized") is not False
            or recommendation.get("new_data_purchase_authorized") is not False
        ):
            raise EconomicEvolutionRuntimeError(
                "production successor recommendation is incomplete or unsafe"
            )

    def _record_production_successor_handoff(
        self,
        config: Mapping[str, Any],
        result: Mapping[str, Any],
        recommendation: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist a hash-chained request; never invent a successor manifest."""

        self._verify_production_successor_recommendation(recommendation)
        result_hash = str(result.get("result_hash") or "")
        if len(result_hash) != 64 or any(
            character not in "0123456789abcdef" for character in result_hash
        ):
            raise EconomicEvolutionRuntimeError(
                "production successor handoff requires the immutable result hash"
            )
        identity = {
            "campaign_id": str(config["campaign_id"]),
            "manifest_hash": _manifest_revision(config),
            "result_hash": result_hash,
            "recommendation": dict(recommendation),
        }
        handoff_id = stable_hash(identity)
        for existing in self._production_successor_handoffs:
            if existing.get("handoff_id") == handoff_id:
                return dict(existing)
            if (
                existing.get("campaign_id") == identity["campaign_id"]
                and existing.get("manifest_hash") == identity["manifest_hash"]
            ):
                raise EconomicEvolutionRuntimeError(
                    "completed production campaign successor recommendation drift"
                )
        entry: dict[str, Any] = {
            "schema": "hydra_production_successor_handoff_v1",
            "sequence": len(self._production_successor_handoffs) + 1,
            "handoff_id": handoff_id,
            **identity,
            "previous_handoff_hash": (
                self._production_successor_handoffs[-1]["handoff_hash"]
                if self._production_successor_handoffs
                else None
            ),
            "recorded_at_utc": datetime.now(UTC).isoformat(),
            "handoff_state": (
                "WORM_MANIFEST_REQUIRED"
                if recommendation["manifest_required"] is True
                else "NO_SUCCESSOR_MANIFEST_REQUIRED"
            ),
        }
        entry["handoff_hash"] = stable_hash(entry)
        self._production_successor_handoffs.append(entry)
        self._record_runtime_state(
            "SUCCESSOR_HANDOFF_RECORDED",
            campaign_id=str(config["campaign_id"]),
        )
        return dict(entry)

    def _tombstone_exhausted_production_class(
        self,
        config: Mapping[str, Any],
        result: Mapping[str, Any],
        output_dir: Path,
    ) -> tuple[ClassTombstone, dict[str, Any]]:
        result_hash = str(result.get("result_hash") or "")
        if len(result_hash) != 64 or any(
            character not in "0123456789abcdef" for character in result_hash
        ):
            raise EconomicEvolutionRuntimeError(
                "production class tombstone requires the immutable result hash"
            )
        summary = result.get("economic_results")
        kpis = result.get("kpis")
        if not isinstance(summary, Mapping) or not isinstance(kpis, Mapping):
            raise EconomicEvolutionRuntimeError(
                "production class tombstone lacks economic evidence"
            )
        counters, _, _ = self._production_terminal_views(summary, kpis)
        successive = result.get("successive_halving")
        decisions = (
            successive.get("stage_decisions")
            if isinstance(successive, Mapping)
            else None
        )
        final_decision = decisions[-1] if isinstance(decisions, list) and decisions else None
        if (
            not isinstance(final_decision, Mapping)
            or int(final_decision.get("output_count", -1)) != 0
            or list(final_decision.get("selected_policy_ids") or ())
            or list(summary.get("confirmation_ready_candidate_ids") or ())
        ):
            raise EconomicEvolutionRuntimeError(
                "production class tombstone forbidden while finalists survive"
            )
        candidate_count = int(counters["serious_exact_account_replays"])
        if candidate_count <= 0:
            raise EconomicEvolutionRuntimeError(
                "production class tombstone requires completed exact policy replays"
            )
        tombstone = ClassTombstone(
            mechanism_class=str(config["class_id"]),
            regime="DEVELOPMENT_MANIFEST_DRIVEN_COMPLETE_EVIDENCE",
            death_cause="NO_SUCCESSIVE_HALVING_SURVIVOR",
            candidate_count=candidate_count,
            source_scope=str(config["campaign_id"]).upper(),
            evidence_sha256=result_hash,
        )
        receipt_path = output_dir / "production_exact_class_graveyard_receipt.json"
        if receipt_path.is_file():
            receipt = _load_json(receipt_path)
            claimed = str(receipt.get("receipt_hash") or "")
            payload = dict(receipt)
            payload.pop("receipt_hash", None)
            if (
                not claimed
                or stable_hash(payload) != claimed
                or receipt.get("campaign_id") != config.get("campaign_id")
                or receipt.get("result_hash") != result_hash
                or receipt.get("class_signature_hash")
                != tombstone.signature_hash
                or receipt.get("candidate_count") != candidate_count
            ):
                raise EconomicEvolutionRuntimeError(
                    "production exact-class graveyard receipt drift"
                )
            appended = verify_class_tombstone(self.graveyard_path, tombstone)
        else:
            appended = append_class_tombstone(self.graveyard_path, tombstone)
            receipt = {
                "schema": "hydra_production_exact_class_graveyard_receipt_v1",
                "campaign_id": config["campaign_id"],
                "manifest_hash": _manifest_revision(config),
                "result_hash": result_hash,
                "class_signature_hash": tombstone.signature_hash,
                "mechanism_class": tombstone.mechanism_class,
                "regime": tombstone.regime,
                "death_cause": tombstone.death_cause,
                "candidate_count": candidate_count,
                "class_signature_count": int(appended["class_signature_count"]),
                "indexed_object_count": int(appended["indexed_object_count"]),
                "parameter_level_feedback": False,
                "same_class_relaunch_allowed": False,
                "q4_access_delta": 0,
                "new_data_purchase_count": 0,
                "broker_connections": 0,
                "orders": 0,
            }
            receipt["receipt_hash"] = stable_hash(receipt)
            _atomic_json(receipt_path, receipt)
        return tombstone, receipt

    def _terminalize(
        self,
        action: Mapping[str, Any],
        config: Mapping[str, Any],
        result: Mapping[str, Any],
        output_dir: Path,
    ) -> dict[str, Any]:
        if _is_production_like(config):
            evidence = result["evidence_bundle"]
            next_action = result.get("autonomous_next_action")
            if not isinstance(next_action, Mapping):
                raise EconomicEvolutionRuntimeError(
                    "production result omits autonomous next action"
                )
            handoff = self._record_production_successor_handoff(
                config, result, next_action
            )
            terminal = {
                **dict(action),
                "manifest_campaign_terminal_state": (
                    "PRODUCTION_EVIDENCE_BUNDLE_COMPLETE"
                ),
                "manifest_campaign_evidence_status": evidence["evidence_status"],
                "manifest_campaign_evidence_bundle_sha256": evidence[
                    "bundle_content_sha256"
                ],
                "manifest_campaign_evidence_reconstruction": bool(
                    evidence.get("reconstruction_flag", False)
                ),
                "manifest_campaign_summary_only_evidence": False,
                "manifest_campaign_independently_confirmed": False,
                "manifest_campaign_status_inheritance_allowed": False,
                "manifest_campaign_successor_handoff_id": handoff["handoff_id"],
                "manifest_campaign_successor_handoff_hash": handoff[
                    "handoff_hash"
                ],
                "manifest_campaign_successor_handoff_state": handoff[
                    "handoff_state"
                ],
                "next_experiment_id": str(next_action["action"]),
                "next_experiment_state": (
                    "WORM_MANIFEST_REQUIRED"
                    if next_action.get("manifest_required") is True
                    else "DEVELOPMENT_FINALISTS_FROZEN"
                ),
                "manifest_campaign_autonomous_next_action": dict(next_action),
            }
            if (
                next_action.get("action")
                == "QUEUE_MATERIALLY_DISTINCT_MECHANISM_MANIFEST"
            ):
                if next_action.get("manifest_required") is not True:
                    raise EconomicEvolutionRuntimeError(
                        "materially distinct successor must require a WORM manifest"
                    )
                tombstone, receipt = self._tombstone_exhausted_production_class(
                    config, result, output_dir
                )
                terminal.update(
                    manifest_campaign_terminal_state=(
                        "PRODUCTION_EVIDENCE_COMPLETE_EXACT_CLASS_TOMBSTONED"
                    ),
                    manifest_campaign_tombstone_signature_hash=(
                        tombstone.signature_hash
                    ),
                    manifest_campaign_graveyard_class_signature_count=int(
                        receipt["class_signature_count"]
                    ),
                    manifest_campaign_graveyard_indexed_object_count=int(
                        receipt["indexed_object_count"]
                    ),
                    manifest_campaign_parameter_rescue_allowed=False,
                    manifest_campaign_same_class_relaunch_allowed=False,
                    next_experiment_state="WORM_MANIFEST_REQUIRED",
                )
            return terminal
        tripwire = result["family_tripwire"]
        if bool(tripwire["family_green"]):
            if config.get("class_id") == "NESTED_STATIC_BASKET_SELECTOR_PROCEDURE_V1":
                final = result.get("final_development") or {}
                confirmation_ready = list(
                    final.get("basket_confirmation_ready") or []
                )
                if result.get("development_only") is not True or result.get(
                    "independently_confirmed"
                ) is not False:
                    raise EconomicEvolutionRuntimeError(
                        "nested selector GREEN escaped development-only status"
                    )
                if result.get("account_policy_economics", {}).get(
                    "targeted_mutations_selected"
                ) != []:
                    raise EconomicEvolutionRuntimeError(
                        "nested selector GREEN cannot launch neighboring mutations"
                    )
                return {
                    **dict(action),
                    "manifest_campaign_terminal_state": (
                        "SELECTOR_GREEN_DEVELOPMENT_FINALISTS_FROZEN"
                    ),
                    "manifest_campaign_confirmation_ready_count": len(
                        confirmation_ready
                    ),
                    "manifest_campaign_independently_confirmed": False,
                    "manifest_campaign_parameter_neighbour_mutation_allowed": False,
                    "next_experiment_id": (
                        "INDEPENDENT_CONFIRMATION_DATA_AVAILABILITY_AUDIT"
                        if confirmation_ready
                        else "NESTED_SELECTOR_GREEN_FINALISTS_PRESERVED"
                    ),
                    "next_experiment_state": (
                        "BASKET_CONFIRMATION_READY_DEVELOPMENT_ONLY"
                        if confirmation_ready
                        else "AWAITING_INDEPENDENT_CONFIRMATION_AUTHORITY"
                    ),
                }
            return {
                **dict(action),
                "manifest_campaign_terminal_state": "SURVIVORS_REQUIRE_NEXT_MANIFEST",
                "next_experiment_id": "FAILURE_GUIDED_SURVIVOR_MUTATION_MANIFEST",
                "next_experiment_state": "WORM_MANIFEST_REQUIRED",
            }
        result_path = output_dir / str(_runtime_manifest(config)["result_name"])
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

    def _production_running_action(
        self,
        predecessor: Mapping[str, Any],
        config: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        output_dir, _ = self._paths(config)
        state_path = output_dir / PRODUCTION_STATE_NAME
        state = (
            self._production_resume_state(config, output_dir)
            if state_path.is_file()
            else None
        )
        kpis = self._load_production_kpis(config, output_dir)
        counters = {
            "policies_proposed": int(
                (kpis or {}).get(
                    "policies_proposed",
                    (state or {}).get("policies_proposed", 0),
                )
            ),
            "unique_policies_screened": int(
                (kpis or {}).get(
                    "unique_policies_screened",
                    (state or {}).get("unique_policies_screened", 0),
                )
            ),
            "exact_account_replays": int(
                (kpis or {}).get(
                    "exact_account_replays",
                    (state or {}).get("exact_account_replays", 0),
                )
            ),
            "combine_episodes_completed": int(
                (kpis or {}).get(
                    "combine_episodes_completed",
                    (state or {}).get("combine_episodes_completed", 0),
                )
            ),
        }
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_PRODUCTION_RUNNING",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": _engine(config),
            "manifest_campaign_state": str((state or {}).get("state") or "STARTING"),
            "manifest_campaign_stage": str((state or {}).get("stage") or "STARTING"),
            "manifest_campaign_checkpoint_sequence": int(
                (state or {}).get("checkpoint_sequence", 0)
            ),
            "manifest_campaign_worker_pid": self._live_worker_pid(),
            "manifest_campaign_worker_count": int(
                _runtime_manifest(config).get("worker_count", 3)
            ),
            "manifest_campaign_evidence_writer_count": 1,
            "manifest_campaign_state_path": str(state_path),
            "manifest_campaign_kpi_path": str(output_dir / PRODUCTION_KPI_NAME),
            "manifest_campaign_live_kpis": dict(kpis or {}),
            "manifest_campaign_policies_proposed": counters["policies_proposed"],
            "manifest_campaign_unique_policies_screened": counters[
                "unique_policies_screened"
            ],
            "manifest_campaign_exact_account_replays": counters[
                "exact_account_replays"
            ],
            "manifest_campaign_rolling_combine_episode_count": counters[
                "combine_episodes_completed"
            ],
            "manifest_campaign_reserved_multiplicity_delta": int(
                reservation["multiplicity"]["delta_trials"]
            ),
            "raw_global_N_trials": int(
                config["multiplicity"]["expected_global_N_trials_after_reservation"]
            ),
            "authoritative_mission_writer_count": 1,
            "production_research_worker_count": int(
                _runtime_manifest(config).get("worker_count", 3)
            ),
            "production_evidence_writer_count": 1,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "next_experiment_id": config["campaign_id"],
            "next_experiment_state": "RUNNING_PRODUCTION_MANIFEST",
            "reason": (
                "Stable V17 is executing the frozen manifest-declared research "
                "worker topology with one asynchronous EvidenceBundle writer."
            ),
            "progressed": True,
        }

    @staticmethod
    def _production_nonnegative_int(
        container: Mapping[str, Any], field: str, label: str
    ) -> int:
        value = container.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise EconomicEvolutionRuntimeError(
                f"{label} has invalid non-negative integer: {field}"
            )
        return value

    @staticmethod
    def _production_finite_number(
        container: Mapping[str, Any],
        field: str,
        label: str,
        *,
        unit_interval: bool = False,
        nonnegative: bool = True,
    ) -> float:
        value = container.get(field)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            raise EconomicEvolutionRuntimeError(
                f"{label} has invalid finite number: {field}"
            )
        number = float(value)
        if nonnegative and number < 0.0:
            raise EconomicEvolutionRuntimeError(
                f"{label} has negative number: {field}"
            )
        if unit_interval and not 0.0 <= number <= 1.0:
            raise EconomicEvolutionRuntimeError(
                f"{label} has out-of-range fraction: {field}"
            )
        return number

    @classmethod
    def _production_terminal_views(
        cls, summary: Mapping[str, Any], result_kpis: Mapping[str, Any]
    ) -> tuple[dict[str, int], dict[str, Any], dict[str, Any]]:
        """Return canonical terminal views without interpreting omissions as zero.

        Production-kernel v1 stores terminal truth in three nested campaign-summary
        objects.  A fully populated legacy flat payload is still readable, but a
        partial payload fails closed instead of silently manufacturing zero results.
        """

        nested_fields = (
            "production_counters",
            "production_kpis",
            "economic_frontier",
        )
        nested_present = [field in summary for field in nested_fields]
        if any(nested_present) and not all(nested_present):
            raise EconomicEvolutionRuntimeError(
                "production campaign summary has a partial nested terminal payload"
            )
        if all(nested_present):
            counters_raw = summary["production_counters"]
            kpis_raw = summary["production_kpis"]
            frontier_raw = summary["economic_frontier"]
            if not all(
                isinstance(value, Mapping)
                for value in (counters_raw, kpis_raw, frontier_raw)
            ):
                raise EconomicEvolutionRuntimeError(
                    "production campaign summary nested terminal payload is invalid"
                )
            counters = dict(counters_raw)
            production_kpis = dict(kpis_raw)
            frontier = dict(frontier_raw)
        else:
            legacy_required = {
                "exact_account_replays",
                "predeclared_control_policy_replays",
                "combine_episodes_completed",
                "normal_episodes_completed",
                "stressed_episodes_completed",
                "best_normal_pass_rate",
                "best_stressed_pass_rate",
                "median_normal_pass_rate",
                "median_stressed_pass_rate",
                "target_progress_frontier",
                "mll_frontier",
            }
            missing = sorted(legacy_required.difference(summary))
            if missing:
                raise EconomicEvolutionRuntimeError(
                    "production legacy campaign summary is incomplete: "
                    + ", ".join(missing)
                )
            target = summary["target_progress_frontier"]
            mll = summary["mll_frontier"]
            if not isinstance(target, Mapping) or not isinstance(mll, Mapping):
                raise EconomicEvolutionRuntimeError(
                    "production legacy frontier payload is invalid"
                )
            counters = {
                "serious_exact_account_replays": summary["exact_account_replays"],
                "predeclared_control_policy_replays": summary[
                    "predeclared_control_policy_replays"
                ],
                "combine_episodes_completed": summary["combine_episodes_completed"],
                "normal_episodes_completed": summary["normal_episodes_completed"],
                "stressed_episodes_completed": summary[
                    "stressed_episodes_completed"
                ],
            }
            production_kpis = {
                field: result_kpis.get(field)
                for field in (
                    "rates_per_hour",
                    "economic_research_wall_clock_fraction",
                    "cpu_utilization_fraction",
                    "workers",
                    "duplicate_rejection_rate",
                    "cache_hit_rate",
                )
            }
            frontier = {
                "candidate_count": summary.get("candidate_count"),
                "normal_pass_fraction_best": summary["best_normal_pass_rate"],
                "normal_pass_fraction_median": summary[
                    "median_normal_pass_rate"
                ],
                "stressed_pass_fraction_best": summary[
                    "best_stressed_pass_rate"
                ],
                "stressed_pass_fraction_median": summary[
                    "median_stressed_pass_rate"
                ],
                "stressed_target_progress_median_best": target.get(
                    "stressed_target_progress_median_best"
                ),
                "stressed_target_progress_median_population": target.get(
                    "stressed_target_progress_median_population"
                ),
                "stressed_mll_breach_rate_minimum": mll.get(
                    "stressed_mll_breach_rate_minimum"
                ),
                "stressed_mll_breach_rate_maximum": mll.get(
                    "stressed_mll_breach_rate_maximum"
                ),
                "positive_stressed_net_count": summary.get(
                    "positive_stressed_net_count"
                ),
            }

        counter_fields = (
            "serious_exact_account_replays",
            "predeclared_control_policy_replays",
            "combine_episodes_completed",
            "normal_episodes_completed",
            "stressed_episodes_completed",
        )
        canonical_counters = {
            field: cls._production_nonnegative_int(
                counters, field, "production counters"
            )
            for field in counter_fields
        }
        if (
            canonical_counters["normal_episodes_completed"]
            + canonical_counters["stressed_episodes_completed"]
            != canonical_counters["combine_episodes_completed"]
        ):
            raise EconomicEvolutionRuntimeError(
                "production episode scenario counters do not reconcile"
            )

        rates = production_kpis.get("rates_per_hour")
        workers = production_kpis.get("workers")
        if not isinstance(rates, Mapping) or not isinstance(workers, Mapping):
            raise EconomicEvolutionRuntimeError(
                "production campaign summary KPI topology is incomplete"
            )
        for field in (
            "policies_proposed",
            "unique_policies_screened",
            "exact_account_replays",
            "combine_episodes",
        ):
            cls._production_finite_number(
                rates, field, "production rates per hour"
            )
        result_workers = result_kpis.get("workers")
        expected_compute = (
            int(result_workers.get("compute", -1))
            if isinstance(result_workers, Mapping)
            else -1
        )
        expected_writer = (
            int(result_workers.get("evidence_writer", -1))
            if isinstance(result_workers, Mapping)
            else -1
        )
        if (
            expected_compute < 1
            or workers.get("compute") != expected_compute
            or expected_writer != 1
            or workers.get("evidence_writer") != expected_writer
        ):
            raise EconomicEvolutionRuntimeError(
                "production campaign summary worker topology drift"
            )
        for field in (
            "economic_research_wall_clock_fraction",
            "cpu_utilization_fraction",
            "duplicate_rejection_rate",
            "cache_hit_rate",
        ):
            cls._production_finite_number(
                production_kpis,
                field,
                "production campaign summary KPIs",
                unit_interval=True,
            )

        candidate_count = cls._production_nonnegative_int(
            frontier, "candidate_count", "production economic frontier"
        )
        positive_count = cls._production_nonnegative_int(
            frontier,
            "positive_stressed_net_count",
            "production economic frontier",
        )
        if positive_count > candidate_count:
            raise EconomicEvolutionRuntimeError(
                "production economic frontier positive count exceeds candidates"
            )
        frontier_fields = (
            "normal_pass_fraction_best",
            "normal_pass_fraction_median",
            "stressed_pass_fraction_best",
            "stressed_pass_fraction_median",
            "stressed_target_progress_median_best",
            "stressed_target_progress_median_population",
            "stressed_mll_breach_rate_minimum",
            "stressed_mll_breach_rate_maximum",
        )
        canonical_frontier: dict[str, Any] = {
            "candidate_count": candidate_count,
            "positive_stressed_net_count": positive_count,
        }
        if candidate_count == 0:
            for field in frontier_fields:
                if field in frontier and frontier[field] is not None:
                    raise EconomicEvolutionRuntimeError(
                        "empty production frontier contains economic observations"
                    )
                canonical_frontier[field] = None
        else:
            for field in frontier_fields:
                canonical_frontier[field] = cls._production_finite_number(
                    frontier,
                    field,
                    "production economic frontier",
                    unit_interval=(
                        "pass_fraction" in field or "mll_breach_rate" in field
                    ),
                )
        return canonical_counters, production_kpis, canonical_frontier

    def _production_complete_action(
        self,
        predecessor: Mapping[str, Any],
        config: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        kpis = result.get("kpis")
        summary = result.get("economic_results")
        if not isinstance(kpis, Mapping) or not isinstance(summary, Mapping):
            raise EconomicEvolutionRuntimeError(
                "production result KPI/economic payload invalid"
            )
        scientific_status = result.get("scientific_status")
        if not isinstance(scientific_status, str) or not scientific_status.strip():
            raise EconomicEvolutionRuntimeError(
                "production result scientific_status is missing"
            )
        counters, production_kpis, frontier = self._production_terminal_views(
            summary, kpis
        )
        policies_proposed = self._production_nonnegative_int(
            kpis, "policies_proposed", "production result kpis"
        )
        unique_screened = self._production_nonnegative_int(
            kpis, "unique_policies_screened", "production result kpis"
        )
        near_pass_count = self._production_nonnegative_int(
            kpis, "near_pass_count", "production result kpis"
        )
        normal_pass_count = self._production_nonnegative_int(
            summary, "normal_pass_candidate_count", "production campaign summary"
        )
        stressed_pass_count = self._production_nonnegative_int(
            summary, "stressed_pass_candidate_count", "production campaign summary"
        )
        positive_stressed_count = self._production_nonnegative_int(
            summary, "positive_stressed_net_count", "production campaign summary"
        )
        confirmation_ready = summary.get("confirmation_ready_candidate_ids")
        if not isinstance(confirmation_ready, list) or any(
            not isinstance(value, str) or not value for value in confirmation_ready
        ):
            raise EconomicEvolutionRuntimeError(
                "production campaign summary confirmation-ready IDs are invalid"
            )

        candidate_count = int(frontier["candidate_count"])
        if any(
            count > candidate_count
            for count in (
                normal_pass_count,
                stressed_pass_count,
                positive_stressed_count,
                len(confirmation_ready),
            )
        ):
            raise EconomicEvolutionRuntimeError(
                "production campaign summary counts exceed its economic frontier"
            )
        if positive_stressed_count != int(frontier["positive_stressed_net_count"]):
            raise EconomicEvolutionRuntimeError(
                "production stressed-positive count disagrees with economic frontier"
            )
        if summary.get("development_only") is not True or summary.get(
            "independently_confirmed"
        ) is not False:
            raise EconomicEvolutionRuntimeError(
                "production campaign summary escaped development-only evidence"
            )

        target_progress_frontier = {
            "stressed_target_progress_median_best": frontier[
                "stressed_target_progress_median_best"
            ],
            "stressed_target_progress_median_population": frontier[
                "stressed_target_progress_median_population"
            ],
        }
        mll_frontier = {
            "stressed_mll_breach_rate_minimum": frontier[
                "stressed_mll_breach_rate_minimum"
            ],
            "stressed_mll_breach_rate_maximum": frontier[
                "stressed_mll_breach_rate_maximum"
            ],
        }
        self._verify_zero_safety_fields(result, "production_result")
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_PRODUCTION_COMPLETE",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": _engine(config),
            "manifest_campaign_state": "COMPLETE",
            "manifest_campaign_scientific_status": scientific_status,
            "manifest_campaign_live_kpis": dict(kpis),
            "manifest_campaign_production_counters": dict(counters),
            "manifest_campaign_production_kpis": dict(production_kpis),
            "manifest_campaign_economic_frontier": dict(frontier),
            "manifest_campaign_policies_proposed": policies_proposed,
            "manifest_campaign_unique_policies_screened": unique_screened,
            "manifest_campaign_exact_account_replays": int(
                counters["serious_exact_account_replays"]
            ),
            "manifest_campaign_predeclared_control_policy_replays": int(
                counters["predeclared_control_policy_replays"]
            ),
            "manifest_campaign_rolling_combine_episode_count": int(
                counters["combine_episodes_completed"]
            ),
            "manifest_campaign_normal_episode_count": int(
                counters["normal_episodes_completed"]
            ),
            "manifest_campaign_stressed_episode_count": int(
                counters["stressed_episodes_completed"]
            ),
            "manifest_campaign_stressed_positive_policy_count": (
                positive_stressed_count
            ),
            "manifest_campaign_policies_with_normal_pass_count": normal_pass_count,
            "manifest_campaign_policies_with_stressed_pass_count": (
                stressed_pass_count
            ),
            "manifest_campaign_best_normal_pass_rate": frontier[
                "normal_pass_fraction_best"
            ],
            "manifest_campaign_best_stressed_pass_rate": frontier[
                "stressed_pass_fraction_best"
            ],
            "manifest_campaign_median_normal_pass_rate": frontier[
                "normal_pass_fraction_median"
            ],
            "manifest_campaign_median_stressed_pass_rate": frontier[
                "stressed_pass_fraction_median"
            ],
            "manifest_campaign_target_progress_frontier": target_progress_frontier,
            "manifest_campaign_mll_frontier": mll_frontier,
            "manifest_campaign_near_pass_count": near_pass_count,
            "manifest_campaign_promoted_to_96_start_ids": list(
                summary.get("stage5_96_start_candidate_ids") or []
            ),
            "manifest_campaign_development_finalist_ids": list(
                summary.get("development_finalist_ids") or []
            ),
            "manifest_campaign_confirmation_ready_ids": confirmation_ready,
            "manifest_campaign_successive_halving": dict(
                result["successive_halving"]
            ),
            "manifest_campaign_failure_vectors": dict(result["failure_vectors"]),
            "manifest_campaign_matched_controls": dict(result["matched_controls"]),
            "manifest_campaign_evidence_bundle": dict(result["evidence_bundle"]),
            "raw_global_N_trials": int(
                config["multiplicity"]["expected_global_N_trials_after_reservation"]
            ),
            "economic_independent_confirmation_queue_eligible_count": 0,
            "economic_pre_holdout_ready_count": 0,
            "economic_paper_shadow_ready_count": 0,
            "q4_access_delta": 0,
            "new_data_purchase_count": 0,
            "broker_connections": 0,
            "orders": 0,
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "progressed": True,
        }

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        config: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        if _is_production_like(config):
            return self._production_running_action(
                predecessor, config, reservation
            )
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_CAMPAIGN_RUNNING",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": _engine(config),
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
        if _is_production_like(config):
            return self._production_complete_action(predecessor, config, result)
        economics = result["account_policy_economics"]
        tripwire = result["family_tripwire"]
        return {
            **dict(predecessor),
            "action_type": "MANIFEST_ECONOMIC_CAMPAIGN_COMPLETE",
            "manifest_campaign_id": config["campaign_id"],
            "manifest_campaign_engine": _engine(config),
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
        if self._live_worker_pid() is not None:
            raise EconomicEvolutionRuntimeError(
                "manifest runtime cannot enter IDLE while a worker is live"
            )
        self._process = None
        self._external_worker_pid = None
        self._active_campaign_id = None
        self._active_config = None
        self._record_runtime_state("IDLE", campaign_id=None)
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
                "production_no_result_exits": dict(
                    self._production_no_result_exits
                ),
                "production_successor_handoffs": [
                    dict(row) for row in self._production_successor_handoffs
                ],
                "worker_pid": self._live_worker_pid(),
                "worker_exit_code": worker_exit_code,
                "engine": (
                    _engine(self._active_config)
                    if self._active_config is not None
                    else None
                ),
                "production_state_path": (
                    str(self._paths(self._active_config)[0] / PRODUCTION_STATE_NAME)
                    if self._active_config is not None
                    and _is_production_like(self._active_config)
                    else None
                ),
                "production_kpi_path": (
                    str(self._paths(self._active_config)[0] / PRODUCTION_KPI_NAME)
                    if self._active_config is not None
                    and _is_production_like(self._active_config)
                    else None
                ),
                "updated_at_utc": datetime.now(UTC).isoformat(),
                "q4_access_delta": 0,
                "new_data_purchase_count": 0,
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
    _verify_terminal_disable_entries(project, entries)
    return queue


def _verify_terminal_disable_entries(
    project: Path, entries: list[Mapping[str, Any]]
) -> None:
    """Fail closed on new terminal-disable receipts without breaking old queues."""

    for entry in entries:
        terminal_disable = entry.get("terminal_disable")
        if terminal_disable is None:
            # Historical disabled entries predate terminal-disable receipts.
            continue
        if entry.get("enabled") is not False or not isinstance(
            terminal_disable, Mapping
        ):
            raise EconomicEvolutionRuntimeError(
                "terminal-disable declaration is invalid"
            )
        _verify_terminal_disable_entry(project, entry, terminal_disable)


def _verify_terminal_disable_entry(
    project: Path,
    entry: Mapping[str, Any],
    terminal_disable: Mapping[str, Any],
) -> None:
    campaign_id = str(entry.get("campaign_id") or "")
    status = str(terminal_disable.get("status") or "")
    reason = str(terminal_disable.get("reason") or "")
    if (
        not campaign_id
        or not status
        or not reason
        or terminal_disable.get("automatic_retry_allowed") is not False
        or terminal_disable.get("evidence_finalization_allowed") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "terminal-disable safety boundary is invalid"
        )

    receipt_path = _terminal_disable_path(
        project, terminal_disable.get("receipt_path"), "receipt"
    )
    verdict_path = _terminal_disable_path(
        project, terminal_disable.get("verdict_path"), "verdict"
    )
    receipt = _verify_terminal_disable_document(
        receipt_path,
        expected_file_sha256=terminal_disable.get("receipt_file_sha256"),
        expected_self_hash=terminal_disable.get("receipt_hash"),
        self_hash_key="receipt_hash",
        label="terminal-disable receipt",
    )
    verdict = _verify_terminal_disable_document(
        verdict_path,
        expected_file_sha256=terminal_disable.get("verdict_file_sha256"),
        expected_self_hash=terminal_disable.get("verdict_hash"),
        self_hash_key="verdict_hash",
        label="terminal-disable verdict",
    )
    if any(
        document.get("campaign_id") != campaign_id
        or document.get("terminal_status") != status
        for document in (receipt, verdict)
    ):
        raise EconomicEvolutionRuntimeError(
            "terminal-disable campaign or status drift"
        )
    if receipt.get("failure_code") != reason or verdict.get("failure_code") != reason:
        raise EconomicEvolutionRuntimeError(
            "terminal-disable failure-code drift"
        )
    expected_receipt_reference = {
        "path": str(terminal_disable.get("receipt_path")),
        "file_sha256": str(terminal_disable.get("receipt_file_sha256")),
        "receipt_hash": str(terminal_disable.get("receipt_hash")),
    }
    if verdict.get("source_terminal_receipt") != expected_receipt_reference:
        raise EconomicEvolutionRuntimeError(
            "terminal-disable WORM verdict does not anchor the receipt"
        )

    tag = str(terminal_disable.get("verdict_tag") or "")
    commit = _terminal_disable_hex(
        terminal_disable.get("verdict_commit"), 40, "verdict commit"
    )
    if not tag.startswith("worm/"):
        raise EconomicEvolutionRuntimeError("terminal-disable verdict tag is missing")
    try:
        tagged_commit = subprocess.check_output(
            ["git", "rev-parse", f"{tag}^{{commit}}"],
            cwd=project,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        expected_commit = subprocess.check_output(
            ["git", "rev-parse", commit],
            cwd=project,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        tagged_blob = subprocess.check_output(
            ["git", "show", f"{tag}:{verdict_path.relative_to(project).as_posix()}"],
            cwd=project,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
        raise EconomicEvolutionRuntimeError(
            "terminal-disable WORM reference is invalid"
        ) from exc
    if tagged_commit != expected_commit or expected_commit != commit:
        raise EconomicEvolutionRuntimeError("terminal-disable WORM tag drift")
    if hashlib.sha256(tagged_blob).hexdigest() != str(
        terminal_disable["verdict_file_sha256"]
    ):
        raise EconomicEvolutionRuntimeError(
            "terminal-disable tagged verdict blob drift"
        )


def _terminal_disable_path(project: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise EconomicEvolutionRuntimeError(
            f"terminal-disable {label} path is invalid"
        )
    path = (project / value).resolve()
    try:
        path.relative_to(project)
    except ValueError as exc:
        raise EconomicEvolutionRuntimeError(
            f"terminal-disable {label} path escapes the project root"
        ) from exc
    if not path.is_file():
        raise EconomicEvolutionRuntimeError(
            f"terminal-disable {label} is missing"
        )
    return path


def _verify_terminal_disable_document(
    path: Path,
    *,
    expected_file_sha256: Any,
    expected_self_hash: Any,
    self_hash_key: str,
    label: str,
) -> dict[str, Any]:
    file_sha256 = _terminal_disable_hex(
        expected_file_sha256, 64, f"{label} file checksum"
    )
    self_hash = _terminal_disable_hex(
        expected_self_hash, 64, f"{label} self-hash"
    )
    if _sha256(path) != file_sha256:
        raise EconomicEvolutionRuntimeError(f"{label} checksum drift")
    document = _load_json(path)
    claimed = document.get(self_hash_key)
    payload = dict(document)
    payload.pop(self_hash_key, None)
    if claimed != self_hash or stable_hash(payload) != self_hash:
        raise EconomicEvolutionRuntimeError(f"{label} self-hash drift")
    return document


def _terminal_disable_hex(value: Any, length: int, label: str) -> str:
    candidate = str(value or "")
    if len(candidate) != length or any(
        character not in "0123456789abcdef" for character in candidate
    ):
        raise EconomicEvolutionRuntimeError(f"terminal-disable {label} is invalid")
    return candidate


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


def _runtime_manifest(config: Mapping[str, Any]) -> Mapping[str, Any]:
    value = (
        config.get("runtime")
        if config.get("schema")
        in {
            "hydra_economic_production_manifest_v1",
            "hydra_causal_target_velocity_manifest_v1",
        }
        else config.get("runtime_manifest")
    )
    if not isinstance(value, Mapping):
        raise EconomicEvolutionRuntimeError("campaign runtime declaration missing")
    return value


def _engine(config: Mapping[str, Any]) -> str:
    return str(_runtime_manifest(config).get("engine") or "")


def _is_production_like(config: Mapping[str, Any]) -> bool:
    return _engine(config) in PRODUCTION_LIKE_ENGINES


def _manifest_revision(config: Mapping[str, Any]) -> str:
    field = (
        "manifest_hash"
        if config.get("schema")
        in {
            "hydra_economic_production_manifest_v1",
            "hydra_causal_target_velocity_manifest_v1",
        }
        or (
            "manifest_hash" in config
            and "preregistration_hash" not in config
        )
        else "preregistration_hash"
    )
    value = str(config.get(field) or "")
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise EconomicEvolutionRuntimeError("campaign semantic hash is invalid")
    return value


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
