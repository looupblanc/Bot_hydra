from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping

from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_density_runtime import (
    CAMPAIGN_ID,
    CAMPAIGN_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME,
    EXPECTED_N_TRIALS,
    verify_density_freeze,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.economic_evolution_density_campaign import (
    load_and_verify_density_result,
)
from hydra.research.v7_graveyard import (
    ClassTombstone,
    append_class_tombstone,
    audit_graveyard,
    class_feedback,
    verify_class_tombstone,
)


TERMINAL_VERDICT_RELATIVE_PATH = Path(
    "WORM/economic-evolution-density-diversification-0007-verdict-2026-07-13.json"
)
TERMINAL_VERDICT_SHA256 = (
    "985cd71ac0f9ea1c2f5e4c30b65983abfe77e99497e88e30479b14cb71f728a8"
)
TERMINAL_VERDICT_SEMANTIC_HASH = (
    "d67d5d01404980b9e6310f035ca7be619dba5847d9ca9ba9418e0328460e90ad"
)
TERMINAL_VERDICT_TAG = (
    "worm/economic-evolution-density-diversification-0007-verdict-2026-07-13"
)
TERMINAL_VERDICT_COMMIT = "e5518e8cd21f0e14bbec8536281f07261c14eef7"
RESULT_SHA256 = "88cc32dc9032d85defc8bde98d0c5b73bb09d4cf488d3a7183f739cf74c60ccd"
RESULT_SEMANTIC_HASH = (
    "bd64d7e4e70596a5fca62502f394559b597c231504053ef504d3b69b7a28bbc7"
)
NEXT_CAMPAIGN_ID = "hydra_economic_evolution_multi_horizon_agreement_0008"
RECEIPT_RELATIVE_PATH = (
    CAMPAIGN_OUTPUT_RELATIVE_PATH / "graveyard_append_receipt.json"
)


class EconomicEvolutionDensityTerminalRuntime:
    """Controller-owned, class-only terminal persistence for campaign 0007."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.graveyard_path = self.state_dir / "graveyard.db"
        self.receipt_path = self.root / RECEIPT_RELATIVE_PATH

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        self._verify_predecessor(predecessor)
        config = verify_density_freeze(self.root)
        result_path = (
            self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH / CAMPAIGN_RESULT_NAME
        )
        result = load_and_verify_density_result(result_path, config)
        verdict = load_and_verify_density_terminal_verdict(
            self.root, result=result
        )
        self._verify_proof_state()
        tombstone = _tombstone_from_verdict(verdict)
        receipt_exists = self.receipt_path.is_file()
        if receipt_exists:
            append_result = verify_class_tombstone(
                self.graveyard_path, tombstone
            )
        else:
            append_result = append_class_tombstone(
                self.graveyard_path, tombstone
            )
        _verify_append_result(
            verdict,
            append_result,
            allow_downstream_growth=receipt_exists,
        )
        receipt = self._write_or_verify_receipt(verdict, tombstone)
        return density_terminal_action(predecessor, verdict, receipt)

    def snapshot(self) -> dict[str, Any]:
        feedback = class_feedback(self.graveyard_path)
        present = any(
            row["mechanism_class"]
            == "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1"
            and row["regime"] == "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET"
            and row["death_cause"] == "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8"
            for row in feedback
        )
        audit = audit_graveyard(self.graveyard_path)
        return {
            "campaign_id": CAMPAIGN_ID,
            "state": "COMPLETE" if present else "READY",
            "class_tombstone_present": present,
            "class_signature_count": audit["class_signature_count"],
            "indexed_object_count": audit["indexed_object_count"],
            "receipt_path": str(self.receipt_path),
            "mission_db_writer_count": 0,
            "registry_writer_count": 0,
            "broker_connections": 0,
            "orders": 0,
        }

    def _verify_predecessor(self, predecessor: Mapping[str, Any]) -> None:
        if (
            predecessor.get("action_type")
            != "ECONOMIC_EVOLUTION_DENSITY_0007_COMPLETE"
            or predecessor.get("economic_density_campaign_id") != CAMPAIGN_ID
            or predecessor.get("economic_density_campaign_state") != "COMPLETE"
            or predecessor.get("economic_density_scientific_status")
            != "ARTEFACT_GEOMETRY_ONLY"
            or predecessor.get("economic_density_tripwire_verdict")
            != "ARTEFACT_GEOMETRY_ONLY"
            or int(predecessor.get("economic_density_real_component_count", -1))
            != 22
            or int(predecessor.get("economic_density_matched_null_count", -1))
            != 22
            or int(
                predecessor.get(
                    "economic_density_account_policy_evaluated_count", -1
                )
            )
            != 0
            or int(predecessor.get("raw_global_N_trials", -1))
            != EXPECTED_N_TRIALS
            or predecessor.get("next_experiment_id")
            != "CLASS_TOMBSTONE_AND_NEW_REPRESENTATION"
        ):
            raise EconomicEvolutionRuntimeError(
                "density terminal predecessor is not the frozen 0007 null verdict"
            )

    def _verify_proof_state(self) -> None:
        proof = load_and_verify(self.state_dir / "proof_registry.json")
        if burned_window_ids(proof) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError(
                "density terminal unexpected proof-window state"
            )
        current_trials = multiplicity_trial_count(proof)
        if not self.receipt_path.is_file() and current_trials != EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "density terminal multiplicity drift"
            )
        if self.receipt_path.is_file() and current_trials < EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "density terminal multiplicity regressed after completion"
            )
        density_reservations = [
            row
            for row in proof["entries"]
            if row.get("event_type") == MULTIPLICITY_EVENT
            and (row.get("evidence") or {}).get("campaign_id") == CAMPAIGN_ID
        ]
        if len(density_reservations) != 1:
            raise EconomicEvolutionRuntimeError(
                "density terminal found a late self-attributed reservation"
            )

    def _write_or_verify_receipt(
        self,
        verdict: Mapping[str, Any],
        tombstone: ClassTombstone,
    ) -> dict[str, Any]:
        frozen_append = verdict["graveyard_append"]
        receipt = {
            "schema": "hydra_density_terminal_graveyard_receipt_v1",
            "campaign_id": CAMPAIGN_ID,
            "worm_verdict_path": str(TERMINAL_VERDICT_RELATIVE_PATH),
            "worm_verdict_sha256": TERMINAL_VERDICT_SHA256,
            "worm_verdict_hash": TERMINAL_VERDICT_SEMANTIC_HASH,
            "class_signature_hash": tombstone.signature_hash,
            "mechanism_class": tombstone.mechanism_class,
            "regime": tombstone.regime,
            "death_cause": tombstone.death_cause,
            "candidate_count": tombstone.candidate_count,
            "evidence_sha256": tombstone.evidence_sha256,
            "class_signature_count": int(
                frozen_append["class_signature_count_after"]
            ),
            "indexed_object_count": int(
                frozen_append["indexed_object_count_after"]
            ),
            "parameter_level_feedback": False,
            "matched_null_controls_counted_as_candidates": False,
            "unevaluated_account_policies_counted_as_candidates": False,
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "next_experiment_id": NEXT_CAMPAIGN_ID,
        }
        receipt["receipt_hash"] = _semantic_hash(receipt)
        if self.receipt_path.is_file():
            prior = _load_json(self.receipt_path)
            if prior != receipt:
                raise EconomicEvolutionRuntimeError(
                    "density terminal receipt drift"
                )
            return prior
        _atomic_json(self.receipt_path, receipt)
        return receipt


def load_and_verify_density_terminal_verdict(
    root: str | Path, *, result: Mapping[str, Any]
) -> dict[str, Any]:
    project = Path(root).resolve()
    path = project / TERMINAL_VERDICT_RELATIVE_PATH
    if _sha256(path) != TERMINAL_VERDICT_SHA256:
        raise EconomicEvolutionRuntimeError("density terminal WORM file drift")
    tagged_commit = subprocess.run(
        ["git", "rev-parse", f"{TERMINAL_VERDICT_TAG}^{{commit}}"],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if tagged_commit != TERMINAL_VERDICT_COMMIT:
        raise EconomicEvolutionRuntimeError("density terminal WORM tag drift")
    tagged_blob = subprocess.run(
        ["git", "show", f"{TERMINAL_VERDICT_TAG}:{TERMINAL_VERDICT_RELATIVE_PATH}"],
        cwd=project,
        check=True,
        capture_output=True,
    ).stdout
    if hashlib.sha256(tagged_blob).hexdigest() != TERMINAL_VERDICT_SHA256:
        raise EconomicEvolutionRuntimeError("density terminal tagged blob drift")
    verdict = _load_json(path)
    claimed_hash = verdict.get("verdict_hash")
    without_hash = dict(verdict)
    without_hash.pop("verdict_hash", None)
    if (
        claimed_hash != TERMINAL_VERDICT_SEMANTIC_HASH
        or _semantic_hash(without_hash) != TERMINAL_VERDICT_SEMANTIC_HASH
    ):
        raise EconomicEvolutionRuntimeError("density terminal semantic hash drift")
    frozen_result = verdict.get("result") or {}
    terminal = verdict.get("terminal_decision") or {}
    append = verdict.get("graveyard_append") or {}
    tripwire = result.get("family_tripwire") or {}
    if (
        verdict.get("campaign_id") != CAMPAIGN_ID
        or verdict.get("class_id")
        != "INDEPENDENT_OPPORTUNITY_DENSITY_CONSISTENCY_ASSEMBLY_V1"
        or frozen_result.get("file_sha256") != RESULT_SHA256
        or frozen_result.get("semantic_hash") != RESULT_SEMANTIC_HASH
        or frozen_result.get("scientific_status") != result.get("scientific_status")
        or int(frozen_result.get("real_pass_count", -1))
        != int(tripwire.get("real_pass_count", -2))
        or int(frozen_result.get("matched_null_pass_count", -1))
        != int(tripwire.get("null_pass_count", -2))
        or float(frozen_result.get("NULL_RATIO", -1.0))
        != float(tripwire.get("NULL_RATIO", -2.0))
        or terminal.get("verdict") != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
        or terminal.get("parameter_rescue_allowed") is not False
        or terminal.get("same_class_relaunch_allowed") is not False
        or terminal.get("candidate_status_inheritance_allowed") is not False
        or append.get("candidate_count") != 22
        or append.get("parameter_level_feedback") is not False
        or append.get("matched_null_controls_counted_as_candidates") is not False
        or append.get("unevaluated_account_policies_counted_as_candidates") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "density terminal WORM does not match the frozen result"
        )
    return verdict


def density_terminal_action(
    predecessor: Mapping[str, Any],
    verdict: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_DENSITY_0007_TOMBSTONED",
        "economic_density_terminal_state": "COMPLETE",
        "economic_density_terminal_verdict": verdict["terminal_decision"][
            "verdict"
        ],
        "economic_density_tombstone_signature_hash": receipt[
            "class_signature_hash"
        ],
        "economic_density_graveyard_class_signature_count": int(
            receipt["class_signature_count"]
        ),
        "economic_density_graveyard_indexed_object_count": int(
            receipt["indexed_object_count"]
        ),
        "economic_density_tombstoned_real_component_count": int(
            receipt["candidate_count"]
        ),
        "economic_density_parameter_rescue_allowed": False,
        "economic_density_same_class_relaunch_allowed": False,
        "economic_density_status_inheritance_allowed": False,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "economic_independent_confirmation_queue_eligible_count": 0,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": NEXT_CAMPAIGN_ID,
        "next_experiment_state": "WORM_PREREGISTRATION_REQUIRED_BEFORE_OUTCOMES",
        "reason": (
            "The exact 0007 geometry-only class was appended once by the "
            "controller writer; a structurally distinct representation with new "
            "IDs is required."
        ),
        "principal_blocker": (
            "No 0008 outcome may be generated before its immutable population, "
            "null family and multiplicity reservation are frozen."
        ),
    }


def _tombstone_from_verdict(verdict: Mapping[str, Any]) -> ClassTombstone:
    row = verdict["graveyard_append"]
    return ClassTombstone(
        mechanism_class=str(row["mechanism_class"]),
        regime=str(row["regime"]),
        death_cause=str(row["death_cause"]),
        candidate_count=int(row["candidate_count"]),
        source_scope=str(row["source_scope"]),
        evidence_sha256=str(row["evidence_sha256"]),
    )


def _verify_append_result(
    verdict: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    allow_downstream_growth: bool = False,
) -> None:
    frozen = verdict["graveyard_append"]
    actual_classes = int(result.get("class_signature_count", -1))
    actual_objects = int(result.get("indexed_object_count", -1))
    expected_classes = int(frozen["class_signature_count_after"])
    expected_objects = int(frozen["indexed_object_count_after"])
    if allow_downstream_growth:
        count_mismatch = (
            actual_classes < expected_classes
            or actual_objects < expected_objects
        )
    else:
        count_mismatch = (
            actual_classes != expected_classes
            or actual_objects != expected_objects
        )
    if (
        result.get("append_status")
        not in {"APPENDED", "ALREADY_PRESENT_IDENTICAL"}
        or count_mismatch
        or result.get("parameter_level_columns") != []
    ):
        raise EconomicEvolutionRuntimeError(
            "density terminal graveyard append invariant failed"
        )


def _semantic_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EconomicEvolutionRuntimeError("expected JSON object")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


__all__ = [
    "NEXT_CAMPAIGN_ID",
    "RECEIPT_RELATIVE_PATH",
    "TERMINAL_VERDICT_RELATIVE_PATH",
    "EconomicEvolutionDensityTerminalRuntime",
    "density_terminal_action",
    "load_and_verify_density_terminal_verdict",
]
