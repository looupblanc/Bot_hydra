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
from hydra.economic_evolution.seed_archive import load_and_verify_seed_archive
from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    append_entry,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_runtime import (
    AMENDMENT_RELATIVE_PATH,
    AMENDMENT_SHA256,
    CONTRACT_MAP_RELATIVE_PATH,
    FEATURE_CACHE_RELATIVE_PATH,
    EconomicEvolutionRuntimeError,
    load_and_verify_campaign_result,
    verify_economic_evolution_freeze,
)


CAMPAIGN_ID = "hydra_economic_evolution_persistent_0003_revision_01"
CAMPAIGN_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_persistent_0003_revision_01.json"
)
CAMPAIGN_CONFIG_SHA256 = (
    "5640ad2de8dc587bf99d7e19a7d2a3101ea699c9f023546163f71963c5df3693"
)
CAMPAIGN_WORM_TAG = "worm/economic-evolution-persistent-0003-r1-2026-07-13"
CAMPAIGN_WORM_COMMIT = "1a13064bb376cbded499747d4f7af124a8cac4ee"
CAMPAIGN_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/persistent_0003_revision_01"
)
CAMPAIGN_RESULT_NAME = "economic_evolution_campaign_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_persistent_0003_revision_01_"
    "multiplicity_reservation"
)
SUPERSEDED_MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_persistent_0003_multiplicity_reservation"
)
SUPERSESSION_ANNOTATION_EVENT_ID = (
    "hydra_economic_evolution_persistent_0003_pre_outcome_abort_annotation"
)
MULTIPLICITY_DELTA = 53_500
SOURCE_RESULT_RELATIVE_PATH = Path(
    "reports/economic_evolution/persistent_0002/"
    "economic_evolution_campaign_result.json"
)
SOURCE_RESULT_SHA256 = (
    "cc28a45b776d8d565809c6aeef1171c95ec6167b2d235289b41bdf35b0872d9d"
)


class EconomicEvolutionSuccessorRuntime:
    """Controller-owned, one-writer launcher for the WORM successor campaign."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / CAMPAIGN_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_runtime_0003_revision_01.json"
        )
        self.log_path = (
            self.state_dir / "logs/economic_evolution_0003_revision_01.log"
        )
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_successor_freeze(self.root)
        if self.result_path.is_file():
            result = load_and_verify_successor_result(self.result_path, config)
            return successor_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_successor_result(self.result_path, config)
                return successor_action_from_result(predecessor, result)
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
            "campaign_id": CAMPAIGN_ID,
            "state": state,
            "worker_pid": (
                self._process.pid
                if self._process is not None and self._process.poll() is None
                else None
            ),
            "attempt": self._attempt,
            "result_path": str(self.result_path),
            "campaign_stage": self._campaign_stage(),
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
        superseded = next(
            (
                row
                for row in registry["entries"]
                if row["event_id"] == SUPERSEDED_MULTIPLICITY_EVENT_ID
            ),
            None,
        )
        annotation = next(
            (
                row
                for row in registry["entries"]
                if row["event_id"] == SUPERSESSION_ANNOTATION_EVENT_ID
            ),
            None,
        )
        if (
            superseded is None
            or annotation is None
            or annotation.get("references_event_id")
            != SUPERSEDED_MULTIPLICITY_EVENT_ID
            or annotation.get("correction", {}).get("scientific_outcomes_seen")
            is not False
        ):
            raise EconomicEvolutionRuntimeError(
                "superseded pre-outcome reservation is not fully annotated"
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
            ):
                raise EconomicEvolutionRuntimeError(
                    "existing successor multiplicity reservation drift"
                )
            return existing
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "successor artifacts exist before multiplicity reservation"
            )
        prior = multiplicity_trial_count(registry)
        entry = append_entry(
            proof_path,
            {
                "event_id": MULTIPLICITY_EVENT_ID,
                "event_type": MULTIPLICITY_EVENT,
                "recorded_at_utc": datetime.now(UTC).isoformat(),
                "status": "RESERVED_BEFORE_CAMPAIGN_OUTCOMES",
                "scientific_role": (
                    "MULTIPLICITY_RESERVATION_ONLY_NO_PROOF_WINDOW_CONSUMED"
                ),
                "evidence": {
                    "campaign_id": CAMPAIGN_ID,
                    "worm_path": str(CAMPAIGN_CONFIG_RELATIVE_PATH),
                    "worm_sha256": CAMPAIGN_CONFIG_SHA256,
                    "worm_commit": CAMPAIGN_WORM_COMMIT,
                    "source_result_sha256": SOURCE_RESULT_SHA256,
                    "candidate_manifest_hash": config["structural_population"][
                        "candidate_manifest_hash"
                    ],
                    "feature_results_seen": False,
                    "signal_results_seen": False,
                    "pnl_results_seen": False,
                    "account_results_seen": False,
                    "new_data_purchase": False,
                    "q4_access": False,
                    "outbound_orders": 0,
                },
                "multiplicity": {
                    "previous_N_trials": prior,
                    "delta_trials": MULTIPLICITY_DELTA,
                    "cumulative_N_trials": prior + MULTIPLICITY_DELTA,
                    "maximum_structural_proposals": int(
                        config["funnel"]["raw_proposals"]
                    ),
                    "maximum_exact_component_replays": int(
                        config["funnel"]["maximum_exact_component_replays"]
                    ),
                    "maximum_incremental_value_evaluations": int(
                        config["funnel"]["incremental_value_evaluations"]
                    ),
                    "maximum_account_policy_structures": int(
                        config["funnel"]["structural_account_policies"]
                    ),
                    "maximum_failure_directed_children": int(
                        config["funnel"]["failure_directed_policy_children"]
                    ),
                    "maximum_exact_account_policy_evaluations": int(
                        config["funnel"]["exact_account_policy_evaluations"]
                    ),
                    "maximum_rolling_elites": int(
                        config["funnel"]["rolling_combine_elite_count"]
                    ),
                    "campaign_inflation_factor": float(
                        config["multiplicity"]["campaign_specific_inflation"]
                    ),
                    "method": (
                        "Conservative WORM upper-bound reservation before any "
                        "successor feature, PnL or account outcome."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "persistent_0003_revision_01_multiplicity_reservation.json",
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
                    "The reservation is deliberately conservative and includes "
                    "development structures killed before formal inference."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "successor worker exhausted three deterministic attempts"
            )
        self._attempt += 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.root / "scripts/run_economic_evolution_campaign.py"),
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
            / f"persistent_0003_revision_01_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError(
                "successor quarantine path collision"
            )
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_CAMPAIGN_0003_R1_RUNNING",
            "phase": "4",
            "progressed": True,
            "economic_evolution_engine": "hydra_economic_evolution_engine_v2",
            "economic_campaign_id": CAMPAIGN_ID,
            "economic_campaign_state": self._campaign_stage(),
            "economic_campaign_worker_pid": (
                None if self._process is None else self._process.pid
            ),
            "economic_campaign_attempt": self._attempt,
            "economic_campaign_compute_workers": 3,
            "economic_campaign_reserved_trials": MULTIPLICITY_DELTA,
            "economic_campaign_failure_target": "LONG_RECOVERY_TIME",
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "The WORM successor is testing failure-directed target-velocity "
                "repairs against identical development episode semantics without "
                "Q4, new data, status inheritance or orders."
            ),
        }

    def _campaign_stage(self) -> str:
        state_path = self.output_dir / "campaign_state.json"
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
                "schema": "hydra_economic_evolution_runtime_state_v1",
                "campaign_id": CAMPAIGN_ID,
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


def verify_successor_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    predecessor_config = verify_economic_evolution_freeze(project)
    source_result = project / SOURCE_RESULT_RELATIVE_PATH
    if _sha256(source_result) != SOURCE_RESULT_SHA256:
        raise EconomicEvolutionRuntimeError("successor source result drift")
    load_and_verify_campaign_result(source_result, predecessor_config)
    amendment = project / AMENDMENT_RELATIVE_PATH
    config_path = project / CAMPAIGN_CONFIG_RELATIVE_PATH
    if _sha256(amendment) != AMENDMENT_SHA256:
        raise EconomicEvolutionRuntimeError("economic-evolution amendment drift")
    if _sha256(config_path) != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("successor campaign WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", CAMPAIGN_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != CAMPAIGN_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("successor WORM tag drift")
    value = json.loads(config_path.read_text(encoding="utf-8"))
    payload = dict(value)
    frozen_hash = str(payload.pop("preregistration_hash", ""))
    if stable_hash(payload) != frozen_hash:
        raise EconomicEvolutionRuntimeError("successor preregistration hash drift")
    if (
        value.get("campaign_id") != CAMPAIGN_ID
        or int(value["multiplicity"]["prospective_global_reservation"])
        != MULTIPLICITY_DELTA
        or value.get("q4_access_allowed") is not False
        or value.get("new_data_purchase_allowed") is not False
        or value.get("network_access_allowed") is not False
        or value.get("broker_or_orders_allowed") is not False
        or value["successor_basis"].get("status_inheritance") is not False
    ):
        raise EconomicEvolutionRuntimeError("successor governance drift")
    for relative, expected in value["implementation_files"].items():
        if _sha256(project / relative) != str(expected):
            raise EconomicEvolutionRuntimeError(
                f"successor implementation drift: {relative}"
            )
    implementation_commit = str(value["implementation_commit"])
    if (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", implementation_commit, "HEAD"],
            cwd=project,
            check=False,
        ).returncode
        != 0
    ):
        raise EconomicEvolutionRuntimeError(
            "successor implementation commit is not an ancestor"
        )
    seed_path = project / str(value["seed_archive"]["path"])
    if _sha256(seed_path) != str(value["seed_archive"]["file_sha256"]):
        raise EconomicEvolutionRuntimeError("successor seed file drift")
    seed = load_and_verify_seed_archive(seed_path)
    if seed["archive_hash"] != str(value["seed_archive"]["archive_hash"]):
        raise EconomicEvolutionRuntimeError("successor seed archive drift")
    return value


def load_and_verify_successor_result(
    path: str | Path, config: Mapping[str, Any]
) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    funnel = dict(value.get("funnel") or {})
    governance = dict(value.get("governance") or {})
    configured = dict(config["funnel"])
    bounded_counts = {
        "raw_structural_proposals": int(configured["raw_proposals"]),
        "exact_component_replays": int(
            configured["maximum_exact_component_replays"]
        ),
        "incremental_value_evaluations": int(
            configured["incremental_value_evaluations"]
        ),
        "component_bank": int(configured["maximum_component_bank"]),
        "structural_account_policies": int(
            configured["structural_account_policies"]
        ),
        "failure_directed_policy_children": int(
            configured["failure_directed_policy_children"]
        ),
        "exact_account_policies": int(
            configured["exact_account_policy_evaluations"]
        ),
        "rolling_combine_elites": int(
            configured["rolling_combine_elite_count"]
        ),
    }
    if any(
        int(funnel.get(field) or 0) < 0
        or int(funnel.get(field) or 0) > maximum
        for field, maximum in bounded_counts.items()
    ):
        raise EconomicEvolutionRuntimeError("successor funnel exceeded WORM bounds")
    if (
        value.get("schema") != "hydra_economic_evolution_campaign_result_v1"
        or value.get("campaign_id") != CAMPAIGN_ID
        or value.get("preregistration_hash") != config["preregistration_hash"]
        or int(funnel.get("raw_structural_proposals") or 0)
        != int(config["funnel"]["raw_proposals"])
        or governance.get("development_only") is not True
        or governance.get("expensive_validation_executed") is not False
        or governance.get("single_authoritative_mission_writer_preserved")
        is not True
        or bool(governance.get("protected_holdout_accessed"))
        or bool(governance.get("q4_accessed"))
        or bool(governance.get("outbound_order_capability"))
        or int(governance.get("broker_connections") or 0) != 0
        or int(governance.get("orders") or 0) != 0
        or governance.get("status_inheritance") is not False
    ):
        raise EconomicEvolutionRuntimeError("successor result integrity drift")
    if int(funnel.get("pre_holdout_ready") or 0) != 0 or int(
        funnel.get("paper_shadow_ready") or 0
    ) != 0:
        raise EconomicEvolutionRuntimeError(
            "development successor attempted an unauthorized promotion"
        )
    return value


def successor_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    funnel = dict(result["funnel"])
    rolling = dict(result["rolling_combine"])
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_CAMPAIGN_0003_R1_COMPLETE",
        "phase": "4",
        "progressed": True,
        "economic_evolution_engine": result["engine_version"],
        "economic_campaign_id": CAMPAIGN_ID,
        "economic_campaign_state": "COMPLETE",
        "economic_raw_proposals": int(funnel["raw_structural_proposals"]),
        "economic_unique_sleeves": int(funnel["unique_sleeves"]),
        "economic_cheap_survivors": int(funnel["cheap_screen_survivors"]),
        "economic_micro_edge_useful": int(funnel["micro_edge_useful"]),
        "economic_account_policy_count": int(funnel["exact_account_policies"]),
        "economic_account_research_candidate_count": int(
            funnel["account_policy_research_candidates"]
        ),
        "economic_combine_path_count": int(funnel["combine_path_candidates"]),
        "economic_combine_pass_count": int(rolling["pass_count"]),
        "economic_median_target_progress": rolling["median_target_progress"],
        "economic_maximum_target_progress": rolling["maximum_target_progress"],
        "economic_median_mll_breach_rate": rolling["median_mll_breach_rate"],
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_economic_evolution_information_review_0004",
        "next_experiment_state": "AUTONOMOUS_INFORMATION_GAIN_REVIEW_REQUIRED",
        "principal_blocker": (
            "Development-only successors still require expensive validation and "
            "untouched confirmation before any shadow promotion."
        ),
        "reason": (
            "The failure-directed successor completed atomically without status "
            "inheritance, Q4, new data, broker access or orders."
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
    "CAMPAIGN_RESULT_NAME",
    "EconomicEvolutionSuccessorRuntime",
    "load_and_verify_successor_result",
    "successor_action_from_result",
    "verify_successor_freeze",
]
