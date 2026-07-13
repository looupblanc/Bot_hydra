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


CAMPAIGN_ID = "hydra_economic_evolution_persistent_0002"
CAMPAIGN_CONFIG_RELATIVE_PATH = Path(
    "config/v7/economic_evolution_persistent_0002.json"
)
CAMPAIGN_CONFIG_SHA256 = (
    "12b1a797bcda92fcc8943cb6975a07022a07e86990545e78c2ad72983441a863"
)
CAMPAIGN_WORM_TAG = "worm/economic-evolution-persistent-0002-2026-07-13"
CAMPAIGN_WORM_COMMIT = "85c0cd3359c742609061efa80c534c81bc6df828"
AMENDMENT_RELATIVE_PATH = Path(
    "MISSION_CONTRACT_AMENDMENT_004_ECONOMIC_EVOLUTION.md"
)
AMENDMENT_SHA256 = (
    "0e7c9da13f6f04b5cb0ae7deffca23a9af3af8143c9e4ba8bc17807bccd6747a"
)
SEED_ARCHIVE_RELATIVE_PATH = Path(
    "reports/economic_evolution/pilot_0001/seed_archive.json"
)
CAMPAIGN_OUTPUT_RELATIVE_PATH = Path(
    "reports/economic_evolution/persistent_0002"
)
CAMPAIGN_RESULT_NAME = "economic_evolution_campaign_result.json"
MULTIPLICITY_EVENT_ID = (
    "hydra_economic_evolution_persistent_0002_multiplicity_reservation"
)
MULTIPLICITY_DELTA = 51_600
CONTRACT_MAP_RELATIVE_PATH = Path(
    "data/cache/contract_maps/"
    "roll_map_GLBX-MDP3_ohlcv-1m_705ce6fe27bac7de.json"
)
FEATURE_CACHE_RELATIVE_PATH = Path("data/cache/economic_evolution/features")


class EconomicEvolutionRuntimeError(RuntimeError):
    pass


class EconomicEvolutionRuntime:
    """Controller-owned launcher for one preregistered compute-plane campaign."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.output_dir = self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH
        self.result_path = self.output_dir / CAMPAIGN_RESULT_NAME
        self.runtime_state_path = (
            self.state_dir / "economic_evolution_runtime_0002.json"
        )
        self.log_path = self.state_dir / "logs/economic_evolution_0002.log"
        self._process: subprocess.Popen[bytes] | None = None
        self._attempt = int(self._load_runtime_state().get("attempt", 0))

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        config = verify_economic_evolution_freeze(self.root)
        if self.result_path.is_file():
            result = load_and_verify_campaign_result(self.result_path, config)
            return campaign_action_from_result(predecessor, result)

        reservation = self._ensure_multiplicity_reservation(config)
        if self._process is not None:
            return_code = self._process.poll()
            if return_code is None:
                return self._running_action(predecessor, reservation)
            self._process = None
            if self.result_path.is_file():
                result = load_and_verify_campaign_result(self.result_path, config)
                return campaign_action_from_result(predecessor, result)
            if return_code != 0:
                self._record_runtime_state(
                    "WORKER_FAILED",
                    worker_exit_code=int(return_code),
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
                    "existing campaign multiplicity reservation drift"
                )
            return existing
        if self.output_dir.exists() and any(self.output_dir.iterdir()):
            raise EconomicEvolutionRuntimeError(
                "campaign artifacts exist before multiplicity reservation"
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
                    "maximum_rolling_elites": int(
                        config["funnel"]["rolling_combine_elite_count"]
                    ),
                    "campaign_inflation_factor": float(
                        config["multiplicity"]["campaign_specific_inflation"]
                    ),
                    "method": (
                        "Conservative preregistered upper-bound reservation before "
                        "any campaign feature, PnL or account outcome."
                    ),
                },
            },
        )
        _atomic_json(
            self.root
            / "reports/economic_evolution/"
            "persistent_0002_multiplicity_reservation.json",
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
                    "The upper-bound reservation includes structures killed before "
                    "formal inference and is deliberately conservative."
                ),
            },
        )
        return entry

    def _start_worker(self) -> None:
        if self._attempt >= 3:
            raise EconomicEvolutionRuntimeError(
                "campaign worker exhausted three deterministic engineering attempts"
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
            / f"persistent_0002_attempt_{self._attempt:02d}"
        )
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if quarantine.exists():
            raise EconomicEvolutionRuntimeError("campaign quarantine path collision")
        shutil.move(str(self.output_dir), str(quarantine))

    def _running_action(
        self,
        predecessor: Mapping[str, Any],
        reservation: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            **dict(predecessor),
            "action_type": "ECONOMIC_EVOLUTION_CAMPAIGN_0002_RUNNING",
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
            "raw_global_N_trials": int(
                reservation["multiplicity"]["cumulative_N_trials"]
            ),
            "new_data_purchase_authorized": False,
            "protected_holdout_access_authorized": False,
            "shadow_admission_authorized": False,
            "reason": (
                "The preregistered economic-evolution campaign is actively "
                "generating, screening, assembling and replaying development-only "
                "account policies under the controller-owned compute plane."
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


def verify_economic_evolution_freeze(root: str | Path) -> dict[str, Any]:
    project = Path(root).resolve()
    amendment = project / AMENDMENT_RELATIVE_PATH
    config_path = project / CAMPAIGN_CONFIG_RELATIVE_PATH
    if _sha256(amendment) != AMENDMENT_SHA256:
        raise EconomicEvolutionRuntimeError("economic-evolution amendment drift")
    if _sha256(config_path) != CAMPAIGN_CONFIG_SHA256:
        raise EconomicEvolutionRuntimeError("economic-evolution campaign WORM drift")
    tag_commit = subprocess.check_output(
        ["git", "rev-list", "-n1", CAMPAIGN_WORM_TAG],
        cwd=project,
        text=True,
    ).strip()
    if tag_commit != CAMPAIGN_WORM_COMMIT:
        raise EconomicEvolutionRuntimeError("campaign WORM tag drift")
    value = json.loads(config_path.read_text(encoding="utf-8"))
    if (
        value.get("campaign_id") != CAMPAIGN_ID
        or int(value["multiplicity"]["prospective_global_reservation"])
        != MULTIPLICITY_DELTA
        or value.get("q4_access_allowed") is not False
        or value.get("new_data_purchase_allowed") is not False
        or value.get("network_access_allowed") is not False
        or value.get("broker_or_orders_allowed") is not False
    ):
        raise EconomicEvolutionRuntimeError("campaign governance drift")
    return value


def classify_economic_evolution_action(
    root: str | Path,
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    project = Path(root).resolve()
    config = verify_economic_evolution_freeze(project)
    result_path = project / CAMPAIGN_OUTPUT_RELATIVE_PATH / CAMPAIGN_RESULT_NAME
    if result_path.is_file():
        return campaign_action_from_result(
            predecessor, load_and_verify_campaign_result(result_path, config)
        )
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_CAMPAIGN_0002_PREREGISTERED",
        "phase": "4",
        "progressed": True,
        "economic_evolution_engine": "hydra_economic_evolution_engine_v2",
        "economic_campaign_id": CAMPAIGN_ID,
        "economic_campaign_state": "READY_FOR_CONTROLLER_OWNED_LAUNCH",
        "economic_campaign_raw_proposals": int(config["funnel"]["raw_proposals"]),
        "economic_campaign_exact_policy_limit": int(
            config["funnel"]["exact_account_policy_evaluations"]
        ),
        "economic_campaign_rolling_elite_limit": int(
            config["funnel"]["rolling_combine_elite_count"]
        ),
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "reason": (
            "The outcome-free campaign, thresholds, data fingerprints and typed "
            "population are WORM-frozen; the controller must reserve multiplicity "
            "before launching its compute workers."
        ),
    }


def load_and_verify_campaign_result(
    path: str | Path,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    funnel = dict(value.get("funnel") or {})
    governance = dict(value.get("governance") or {})
    if (
        value.get("schema") != "hydra_economic_evolution_campaign_result_v1"
        or value.get("campaign_id") != CAMPAIGN_ID
        or value.get("preregistration_hash") != config["preregistration_hash"]
        or int(funnel.get("raw_structural_proposals") or 0)
        != int(config["funnel"]["raw_proposals"])
        or bool(governance.get("protected_holdout_accessed"))
        or bool(governance.get("q4_accessed"))
        or bool(governance.get("outbound_order_capability"))
        or int(governance.get("broker_connections") or 0) != 0
        or int(governance.get("orders") or 0) != 0
        or governance.get("status_inheritance") is not False
    ):
        raise EconomicEvolutionRuntimeError("campaign result integrity drift")
    if int(funnel.get("pre_holdout_ready") or 0) != 0 or int(
        funnel.get("paper_shadow_ready") or 0
    ) != 0:
        raise EconomicEvolutionRuntimeError(
            "development campaign attempted an unauthorized promotion"
        )
    return value


def campaign_action_from_result(
    predecessor: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    funnel = dict(result["funnel"])
    rolling = dict(result["rolling_combine"])
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_CAMPAIGN_0002_COMPLETE",
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
        "economic_median_mll_breach_rate": rolling["median_mll_breach_rate"],
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": "hydra_economic_evolution_failure_review_0003",
        "next_experiment_state": "WORM_ADAPTIVE_POLICY_INSTANCE_REQUIRED",
        "principal_blocker": (
            "Development-only account behavior still requires expensive validation "
            "and untouched confirmation before any shadow promotion."
        ),
        "reason": (
            "The persistent economic-evolution campaign completed atomically; "
            "its diagnostic outcomes are recorded without status inheritance, "
            "Q4, new data, broker access or orders."
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
    "CAMPAIGN_ID",
    "CAMPAIGN_CONFIG_RELATIVE_PATH",
    "CAMPAIGN_RESULT_NAME",
    "EconomicEvolutionRuntime",
    "EconomicEvolutionRuntimeError",
    "campaign_action_from_result",
    "classify_economic_evolution_action",
    "load_and_verify_campaign_result",
    "verify_economic_evolution_freeze",
]
