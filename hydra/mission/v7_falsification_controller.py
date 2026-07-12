from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from hydra.governance.proof_registry import burned_window_ids, load_and_verify
from hydra.mission.mission_state import (
    append_event,
    append_jsonl,
    clear_stop,
    connect_state,
    get_kv,
    mission_lock,
    mission_paths,
    set_kv,
    stop_requested,
    write_heartbeat,
)
from hydra.utils.time import utc_now_iso


CONTRACT_SHA256 = (
    "35cca36324e24425fbff369c2cec864c90b612508436c13902fed5901c6ad9ab"
)
CONTROLLER_SCHEMA = "hydra_v7_1_falsification_controller_v2"
EXPERIMENT_ID = "hydra_v7_1_falsification_20260712_0001"
G0_RELATIVE_PATH = Path("reports/v7/phase0_v2/g0_result.json")
G1_RELATIVE_PATH = Path("reports/v7/phase1/g1_result.json")
D1_TRIBUNAL_RELATIVE_PATH = Path(
    "reports/v7/data/d1_candidate_tribunal_result.json"
)
V71_POLICY_RELATIVE_PATH = Path(
    "WORM/v7.1-hierarchical-validation-policy-2026-07-12.json"
)
V71_POWER_RELATIVE_PATH = Path(
    "reports/v7_1/calibration/v71_power_audit_result.json"
)
V71_POWER_EXTENSION_RELATIVE_PATH = Path(
    "reports/v7_1/calibration/v71_power_sample_extension_result.json"
)
V71_SIGNAL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery/v71_signal_manifest.json"
)
V71_FUNNEL_RELATIVE_PATH = Path(
    "reports/v7_1/discovery/v71_development_funnel_result.json"
)
V71_FORENSICS_RELATIVE_PATH = Path(
    "reports/v7_1/forensics/v71_mechanism_forensics_result.json"
)
V71_FROZEN_HASHES = {
    "MISSION_CONTRACT_AMENDMENT_001_ORDERFLOW.md": "981523c00831fac4dee02aa9bd908be6781ecec63a2a3fa573832206ea173eeb",
    str(V71_POLICY_RELATIVE_PATH): "d745ac9ca51049ccc2f7f1f97d3593cf49231c92a8873737e350e380170f916c",
    "WORM/v7.1-event-mechanism-grammar-0001-2026-07-12.json": "e1c8de955302da2be836bbcebf2bfedc07768b2d9b987ea32258a85a2b0caf8a",
    "WORM/v7.1-powered-promotion-minimum-2026-07-12.json": "3e0211c6a5acea81713431802fc1576da4d5be2a0cc37bf900cd02eabd68c6fa",
}


class V7ControllerIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class V7ControllerConfig:
    project_root: str = "."
    state_dir: str = "mission/state"
    sleep_seconds: float = 15.0
    checkpoint_every_steps: int = 25
    persistent: bool = True
    maximum_steps: int | None = None
    no_live_trading: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def classify_v7_action(project_root: str | Path) -> dict[str, Any]:
    root = Path(project_root).resolve()
    if (root / V71_POLICY_RELATIVE_PATH).is_file():
        return _classify_v71_action(root)
    tribunal_path = root / D1_TRIBUNAL_RELATIVE_PATH
    if not tribunal_path.is_file():
        return {
            "action_type": "D1_CANDIDATE_TRIBUNAL_PENDING",
            "phase": "D",
            "progressed": False,
            "reason": "The frozen D1 tribunal has no atomic result yet.",
        }
    tribunal = _load_json(tribunal_path)
    verdict = str(tribunal.get("verdict") or "")
    selected = tuple(
        str(value)
        for value in tribunal.get("selected_shadow_queue_candidate_ids") or ()
    )
    if verdict == "GREEN" and selected:
        fiche_root = root / "WORM" / "candidates"
        missing = [
            candidate_id
            for candidate_id in selected
            if not (fiche_root / f"{candidate_id}.json").is_file()
        ]
        if missing:
            return {
                "action_type": "CANDIDATE_FICHE_FREEZE_REQUIRED",
                "phase": "3",
                "progressed": False,
                "candidate_ids": list(selected),
                "missing_candidate_fiches": missing,
                "reason": "WORM fiches must precede any forward-gap ingestion.",
            }
        boundary = root / "mission/state/v7_forward_boundary_manifest.json"
        if not boundary.is_file():
            return {
                "action_type": "FORWARD_BOUNDARY_MANIFEST_REQUIRED",
                "phase": "3",
                "progressed": False,
                "candidate_ids": list(selected),
                "reason": "Candidate fiches exist but the append-only boundary is absent.",
            }
        return {
            "action_type": "FORWARD_FEED_READY",
            "phase": "3",
            "progressed": True,
            "candidate_ids": list(selected),
            "boundary_manifest": str(boundary),
            "reason": "Frozen candidates may enter the post-fiche feed path.",
        }
    if verdict == "NULL" and not selected:
        graveyard = root / "mission/state/graveyard.db"
        source_scope = (
            "HYDRA_V7_GRAMMAR:hydra_v7_d1_microstructure_grammar_0001"
        )
        indexed = False
        if graveyard.is_file():
            conn = sqlite3.connect(f"file:{graveyard}?mode=ro", uri=True)
            try:
                indexed = (
                    conn.execute(
                        "SELECT COUNT(*) FROM class_tombstones WHERE source_scope=?",
                        (source_scope,),
                    ).fetchone()[0]
                    > 0
                )
            finally:
                conn.close()
        if not indexed:
            return {
                "action_type": "D1_CLASS_TOMBSTONE_REQUIRED",
                "phase": "4",
                "progressed": False,
                "reason": "The null D1 classes are not yet indexed in the class-only graveyard.",
            }
        return {
            "action_type": "NEW_HYPOTHESIS_GRAMMAR_REQUIRED",
            "phase": "4",
            "progressed": False,
            "reason": "D1 classes are tombstoned; the next economic hypothesis must be WORM before generation.",
        }
    raise V7ControllerIntegrityError(
        "D1 tribunal has an unsupported or internally inconsistent verdict"
    )


def _classify_v71_action(root: Path) -> dict[str, Any]:
    required = (
        (V71_POWER_RELATIVE_PATH, "V71_POWER_AUDIT_REQUIRED"),
        (V71_POWER_EXTENSION_RELATIVE_PATH, "V71_POWER_EXTENSION_REQUIRED"),
        (V71_SIGNAL_RELATIVE_PATH, "V71_SIGNAL_MANIFEST_REQUIRED"),
        (V71_FUNNEL_RELATIVE_PATH, "V71_DEVELOPMENT_FUNNEL_REQUIRED"),
        (V71_FORENSICS_RELATIVE_PATH, "V71_FORENSICS_REQUIRED"),
    )
    for path, action in required:
        if not (root / path).is_file():
            return {
                "action_type": action,
                "phase": "4",
                "progressed": False,
                "required_path": str(path),
                "reason": "The preregistered V7.1 evidence sequence is incomplete.",
            }
    power = _load_json(root / V71_POWER_RELATIVE_PATH)
    extension = _load_json(root / V71_POWER_EXTENSION_RELATIVE_PATH)
    signal = _load_json(root / V71_SIGNAL_RELATIVE_PATH)
    funnel = _load_json(root / V71_FUNNEL_RELATIVE_PATH)
    forensics = _load_json(root / V71_FORENSICS_RELATIVE_PATH)
    if power.get("verdict") != "RED" or extension.get("verdict") != "GREEN":
        raise V7ControllerIntegrityError("V7.1 power evidence sequence is inconsistent")
    if int(signal.get("candidate_count") or 0) != 256:
        raise V7ControllerIntegrityError("V7.1 signal manifest candidate count drift")
    powered = int(funnel.get("powered_walk_forward_candidate_count") or 0)
    positive = int(funnel.get("walk_forward_positive_count") or 0)
    if powered > 0:
        return {
            "action_type": "V71_STAGE3_COHORT_FREEZE_REQUIRED",
            "phase": "4",
            "progressed": True,
            "powered_candidate_count": powered,
            "walk_forward_positive_count": positive,
            "reason": "Powered walk-forward candidates must be frozen before nulls and DSR/BH.",
        }
    if forensics.get("MINI_MICRO_DIVERGENCE", {}).get("mechanism") != "MECHANISM_CONFIRMED_DEAD":
        raise V7ControllerIntegrityError("V7.1 intra-product artifact status drift")
    return {
        "action_type": "V71_OPPORTUNITY_DENSITY_GRAMMAR_REQUIRED",
        "phase": "4",
        "progressed": True,
        "walk_forward_positive_count": positive,
        "powered_candidate_count": powered,
        "minimum_powered_events": int(extension["minimum_required_event_count"]),
        "next_experiment_id": "hydra_v7_1_opportunity_density_grammar_0002",
        "next_experiment_state": "PREREGISTRATION_REQUIRED",
        "new_data_purchase_authorized": False,
        "reason": (
            "Eleven distinct formulations are walk-forward positive but below "
            "the frozen 320-event power minimum; expand opportunity coverage "
            "structurally without parameter tuning or new data."
        ),
    }


class V7FalsificationController:
    def __init__(self, config: V7ControllerConfig) -> None:
        if not config.no_live_trading:
            raise V7ControllerIntegrityError("V7 requires no_live_trading=True")
        if config.checkpoint_every_steps <= 0 or config.sleep_seconds < 0.0:
            raise ValueError("invalid V7 controller cadence")
        self.config = config
        self.root = Path(config.project_root).resolve()
        state_dir = Path(config.state_dir)
        if not state_dir.is_absolute():
            state_dir = self.root / state_dir
        self.paths = mission_paths(str(state_dir))
        self._shutdown = False

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        with mission_lock(self.paths):
            conn = connect_state(self.paths)
            try:
                self._initialize(conn)
                completed = 0
                while not self._shutdown:
                    if stop_requested(self.paths):
                        self._stop_cleanly(conn, "manual_stop_file")
                        return 0
                    self._step(conn)
                    completed += 1
                    if (
                        self.config.maximum_steps is not None
                        and completed >= self.config.maximum_steps
                    ):
                        self._stop_cleanly(conn, "maximum_steps")
                        return 0
                    if not self.config.persistent:
                        self._stop_cleanly(conn, "non_persistent")
                        return 0
                    if self.config.sleep_seconds:
                        time.sleep(self.config.sleep_seconds)
                self._stop_cleanly(conn, "signal")
                return 0
            except Exception as exc:
                set_kv(conn, "service_state", "V7_INTEGRITY_BLOCKED")
                set_kv(conn, "current_phase", "INTEGRITY_BLOCKED")
                set_kv(conn, "current_blocker", f"{type(exc).__name__}:{exc}"[:4000])
                write_heartbeat(
                    self.paths,
                    self._heartbeat(
                        conn,
                        action={
                            "action_type": "V7_INTEGRITY_BLOCKED",
                            "reason": str(exc),
                        },
                    ),
                )
                raise
            finally:
                conn.close()

    def _initialize(self, conn: sqlite3.Connection) -> None:
        self._verify_constitution()
        _verify_database_integrity(conn)
        legacy_active = conn.execute(
            "SELECT experiment_id FROM experiments WHERE status IN ('QUEUED','RUNNING')"
        ).fetchall()
        if legacy_active and any(str(row[0]) != EXPERIMENT_ID for row in legacy_active):
            raise V7ControllerIntegrityError(
                "legacy queued/running work must not coexist with V7"
            )
        payload = {
            "schema": CONTROLLER_SCHEMA,
            "experiment_id": EXPERIMENT_ID,
            "contract_sha256": CONTRACT_SHA256,
            "source_commit": _git_head(self.root),
            "no_live_trading": True,
            "outbound_order_capability": False,
            "config": self.config.to_dict(),
        }
        now = utc_now_iso()
        conn.execute(
            "INSERT INTO experiments(experiment_id,status,payload,updated_at,"
            "experiment_type,specification_hash,result,priority,attempt_count,"
            "max_attempts,created_at,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(experiment_id) DO UPDATE SET status='RUNNING',"
            "payload=excluded.payload,updated_at=excluded.updated_at,"
            "started_at=COALESCE(experiments.started_at,excluded.started_at),"
            "last_error=NULL,claim_token=NULL,claimed_by=NULL,lease_expires_at=NULL",
            (
                EXPERIMENT_ID,
                "RUNNING",
                json.dumps(payload, sort_keys=True),
                now,
                "v7_falsification_perpetual",
                CONTRACT_SHA256,
                None,
                1000.0,
                0,
                1,
                now,
                now,
            ),
        )
        conn.commit()
        set_kv(conn, "mission_id", EXPERIMENT_ID)
        set_kv(conn, "mission_contract", CONTROLLER_SCHEMA)
        set_kv(conn, "mission_contract_sha256", CONTRACT_SHA256)
        set_kv(conn, "service_state", "RUNNING_V7_FALSIFICATION")
        set_kv(conn, "last_shutdown", None)
        set_kv(conn, "live_trading_enabled", False)
        set_kv(conn, "broker_order_capability", False)
        set_kv(conn, "v7_controller_version", CONTROLLER_SCHEMA)
        append_event(conn, "V7_CONTROLLER_INITIALIZED", payload)
        append_jsonl(
            self.paths.decision_ledger,
            {
                "created_at_utc": now,
                "decision_type": "V7_CONTROLLER_INITIALIZED",
                "experiment_id": EXPERIMENT_ID,
                "contract_sha256": CONTRACT_SHA256,
                "outbound_orders": 0,
            },
        )

    def _step(self, conn: sqlite3.Connection) -> None:
        contract_text = self._verify_constitution()
        _verify_database_integrity(conn)
        action = classify_v7_action(self.root)
        previous = get_kv(conn, "v7_current_action", {})
        step = int(get_kv(conn, "v7_step", 0)) + 1
        set_kv(conn, "v7_step", step)
        set_kv(conn, "v7_current_action", action)
        set_kv(conn, "current_action", action)
        set_kv(conn, "current_phase", f"V7_PHASE_{action['phase']}")
        set_kv(conn, "current_blocker", None)
        set_kv(conn, "service_state", "RUNNING_V7_FALSIFICATION")
        if _stable_json(previous) != _stable_json(action):
            append_event(
                conn,
                "V7_ACTION_TRANSITION",
                {"step": step, "previous": previous, "current": action},
            )
            append_jsonl(
                self.paths.decision_ledger,
                {
                    "created_at_utc": utc_now_iso(),
                    "decision_type": "V7_ACTION_TRANSITION",
                    "experiment_id": EXPERIMENT_ID,
                    "step": step,
                    "previous": previous,
                    "current": action,
                    "outbound_orders": 0,
                },
            )
        checkpoint = str(get_kv(conn, "v7_latest_checkpoint", ""))
        if step % self.config.checkpoint_every_steps == 0:
            checkpoint = str(
                self._checkpoint(conn, step=step, action=action, contract_text=contract_text)
            )
            set_kv(conn, "v7_latest_checkpoint", checkpoint)
        write_heartbeat(self.paths, self._heartbeat(conn, action=action))

    def _verify_constitution(self) -> str:
        contract = self.root / "MISSION_CONTRACT.md"
        if not contract.is_file():
            raise V7ControllerIntegrityError("MISSION_CONTRACT.md is absent")
        text = contract.read_text(encoding="utf-8")
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != CONTRACT_SHA256:
            raise V7ControllerIntegrityError("MISSION_CONTRACT.md hash drift")
        g0 = _load_json(self.root / G0_RELATIVE_PATH)
        g1 = _load_json(self.root / G1_RELATIVE_PATH)
        if g0.get("verdict") != "GREEN" or g1.get("verdict") != "GREEN":
            raise V7ControllerIntegrityError("G0 and G1 must both be frozen GREEN")
        drift = [
            path
            for path, expected in V71_FROZEN_HASHES.items()
            if _sha256(self.root / path) != expected
        ]
        if drift:
            raise V7ControllerIntegrityError(
                "V7.1 frozen constitutional input drift: " + ",".join(drift)
            )
        proof = load_and_verify(self.root / "mission/state/proof_registry.json")
        if burned_window_ids(proof) != ("Q4_2024",):
            raise V7ControllerIntegrityError("unexpected proof-window state")
        return text

    def _checkpoint(
        self,
        conn: sqlite3.Connection,
        *,
        step: int,
        action: Mapping[str, Any],
        contract_text: str,
    ) -> Path:
        if not contract_text.startswith("# MISSION HYDRA V7"):
            raise V7ControllerIntegrityError("full contract reread failed")
        proof = load_and_verify(self.root / "mission/state/proof_registry.json")
        path = (
            self.root
            / "reports/v7/checkpoints"
            / f"hydra_v7_persistent_step_{step:06d}.md"
        )
        content = "\n".join(
            [
                f"[HYDRA-V7] phase={action['phase']} step={step} verdict=GREEN",
                f"gate=V7_PERSISTENCE preuve=MISSION_CONTRACT.md#{CONTRACT_SHA256[:8]} tests=deploiement_persistant",
                f"budget_llm=usage_API_non_exposee/solde budget_data=registre_persistant N_trials={_multiplicity(proof)} burned={len(burned_window_ids(proof))}",
                "diff_validation=aucun CONTRE=un_controleur_sain_ne_prouve_pas_un_edge_et_ne_doit_jamais_etre_compte_comme_resultat_scientifique",
                f"prochaine_action={action['action_type']}",
                "",
                "Justification : clauses 1, 5 et 8 — préserver les verdicts, le registre de preuve et zéro ordre broker.",
                "Auto-audit : le risque principal est de confondre continuité opérationnelle et progression scientifique.",
                "",
            ]
        )
        _atomic_text(path, content)
        append_event(
            conn,
            "V7_CONSTITUTIONAL_CHECKPOINT",
            {"step": step, "path": str(path), "sha256": _sha256(path)},
        )
        return path

    def _heartbeat(
        self, conn: sqlite3.Connection, *, action: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            "controller_version": CONTROLLER_SCHEMA,
            "mission_id": EXPERIMENT_ID,
            "service_state": get_kv(conn, "service_state", "UNKNOWN"),
            "phase": get_kv(conn, "current_phase", "UNKNOWN"),
            "step": int(get_kv(conn, "v7_step", 0)),
            "current_action": dict(action),
            "latest_checkpoint": get_kv(conn, "v7_latest_checkpoint", ""),
            "process_lock": str(self.paths.lock_path),
            "single_writer": True,
            "broker_connections": 0,
            "outbound_orders": 0,
            "automatic_order_capability": False,
        }

    def _stop_cleanly(self, conn: sqlite3.Connection, reason: str) -> None:
        now = utc_now_iso()
        set_kv(conn, "service_state", "STOPPED_CLEANLY_V7")
        set_kv(conn, "current_phase", "STOPPED_CLEANLY")
        set_kv(conn, "last_shutdown", "clean")
        set_kv(conn, "last_stop_reason", reason)
        conn.execute(
            "UPDATE experiments SET status='COMPLETED',updated_at=?,completed_at=?,"
            "result=? WHERE experiment_id=?",
            (
                now,
                now,
                json.dumps({"status": "STOPPED_CLEANLY", "reason": reason}),
                EXPERIMENT_ID,
            ),
        )
        conn.commit()
        write_heartbeat(
            self.paths,
            self._heartbeat(
                conn,
                action={"action_type": "STOPPED_CLEANLY", "reason": reason},
            ),
        )

    def _handle_signal(self, _signum: int, _frame: Any) -> None:
        self._shutdown = True


def run_v7_controller(config: V7ControllerConfig) -> int:
    return V7FalsificationController(config).run()


def _verify_database_integrity(conn: sqlite3.Connection) -> None:
    result = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
    if result != "ok":
        raise V7ControllerIntegrityError(f"mission DB integrity failed: {result}")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise V7ControllerIntegrityError(f"required artifact is absent: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise V7ControllerIntegrityError(f"artifact must be an object: {path}")
    return payload


def _multiplicity(proof: Mapping[str, Any]) -> int:
    values = [
        int(entry.get("multiplicity", {}).get("cumulative_N_trials", 0))
        for entry in proof.get("entries", [])
        if isinstance(entry, Mapping)
    ]
    return max(values, default=0)


def _git_head(root: Path) -> str:
    import subprocess

    source_root = root if (root / ".git").exists() else Path(__file__).resolve().parents[2]
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=source_root, text=True
    ).strip()


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "CONTROLLER_SCHEMA",
    "EXPERIMENT_ID",
    "V7ControllerConfig",
    "V7ControllerIntegrityError",
    "V7FalsificationController",
    "classify_v7_action",
    "run_v7_controller",
]
