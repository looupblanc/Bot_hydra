from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping

from hydra.economic_evolution.schema import stable_hash
from hydra.governance.proof_registry import (
    MULTIPLICITY_EVENT,
    burned_window_ids,
    load_and_verify,
    multiplicity_trial_count,
)
from hydra.mission.economic_evolution_account_timeline_runtime import (
    CAMPAIGN_ID,
    CAMPAIGN_OUTPUT_RELATIVE_PATH,
    CAMPAIGN_RESULT_NAME,
    EXPECTED_N_TRIALS,
    verify_account_timeline_freeze,
)
from hydra.mission.economic_evolution_runtime import EconomicEvolutionRuntimeError
from hydra.research.economic_evolution_account_timeline_campaign import (
    load_and_verify_account_timeline_result,
)
from hydra.research.v7_graveyard import (
    ClassTombstone,
    append_class_tombstone,
    audit_graveyard,
    class_feedback,
    verify_class_tombstone,
)


TERMINAL_VERDICT_RELATIVE_PATH = Path(
    "WORM/economic-evolution-account-timeline-0012-verdict-2026-07-14.json"
)
TERMINAL_VERDICT_SHA256 = (
    "5baf8a668145257ccd0a62b1c40b9da1d8e586d558c97434961190085bf382fd"
)
TERMINAL_VERDICT_SEMANTIC_HASH = (
    "88ba7bcc4472019858d917b378ceafd535f413d918aa9bce3c7cd0bfb5b9960d"
)
TERMINAL_VERDICT_TAG = (
    "worm/economic-evolution-account-timeline-0012-verdict-2026-07-14"
)
TERMINAL_VERDICT_COMMIT = "7a3221773d09fc5eb04be44ecee01bb0d143ea16"
RESULT_SHA256 = "8db2525265fffb187faf47aabb402c016860d97ca8f9adbc41a058f457cd00f7"
RESULT_SEMANTIC_HASH = (
    "1d0779d36c345919f7433d7f55b2ba319a483c0df888cc8dee6e3d5b7f1e8e10"
)
NEXT_CAMPAIGN_ID = "hydra_economic_evolution_opportunity_density_0013"
RECEIPT_RELATIVE_PATH = (
    CAMPAIGN_OUTPUT_RELATIVE_PATH / "graveyard_append_receipt.json"
)


class EconomicEvolutionAccountTimelineTerminalRuntime:
    """Persist the exact 0012 class verdict before manifest production."""

    def __init__(self, project_root: str | Path, state_dir: str | Path) -> None:
        self.root = Path(project_root).resolve()
        self.state_dir = Path(state_dir).resolve()
        self.graveyard_path = self.state_dir / "graveyard.db"
        self.receipt_path = self.root / RECEIPT_RELATIVE_PATH

    def advance(self, predecessor: Mapping[str, Any]) -> dict[str, Any]:
        self._verify_predecessor(predecessor)
        config = verify_account_timeline_freeze(self.root)
        result = load_and_verify_account_timeline_result(
            self.root / CAMPAIGN_OUTPUT_RELATIVE_PATH / CAMPAIGN_RESULT_NAME,
            config,
        )
        verdict = load_and_verify_account_timeline_terminal_verdict(
            self.root, result=result
        )
        self._verify_proof_state()
        tombstone = _tombstone_from_verdict(verdict)
        receipt_exists = self.receipt_path.is_file()
        if receipt_exists:
            appended = verify_class_tombstone(self.graveyard_path, tombstone)
        else:
            appended = append_class_tombstone(self.graveyard_path, tombstone)
        _verify_append_result(
            verdict,
            appended,
            allow_downstream_growth=receipt_exists,
        )
        receipt = self._write_or_verify_receipt(verdict, tombstone)
        return account_timeline_terminal_action(predecessor, verdict, receipt)

    def snapshot(self) -> dict[str, Any]:
        present = any(
            row["mechanism_class"] == "SLEEVE_VIRTUAL_PERSISTENCE_ROUTER_V1"
            and row["regime"]
            == (
                "DEVELOPMENT_2023Q3_TO_2024Q3_MULTI_MARKET_"
                "PAST_ONLY_COMPLETED_OUTCOME_TIMELINE"
            )
            and row["death_cause"] == "GEOMETRY_ONLY_NULL_RATIO_GTE_0_8"
            for row in class_feedback(self.graveyard_path)
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
            != "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_COMPLETE"
            or predecessor.get("economic_account_timeline_campaign_id")
            != CAMPAIGN_ID
            or predecessor.get("economic_account_timeline_campaign_state")
            != "COMPLETE"
            or predecessor.get("economic_account_timeline_scientific_status")
            != "ARTEFACT_GEOMETRY_ONLY"
            or predecessor.get("economic_account_timeline_tripwire_verdict")
            != "ARTEFACT_GEOMETRY_ONLY"
            or int(predecessor.get("economic_account_timeline_real_policy_count", -1))
            != 512
            or int(
                predecessor.get("economic_account_timeline_matched_control_count", -1)
            )
            != 512
            or int(
                predecessor.get(
                    "economic_account_timeline_policies_with_combine_pass_count", -1
                )
            )
            != 0
            or int(predecessor.get("raw_global_N_trials", -1))
            != EXPECTED_N_TRIALS
            or predecessor.get("next_experiment_id")
            != "TOMBSTONE_EXACT_0012_AND_CHANGE_ACCOUNT_REPRESENTATION"
        ):
            raise EconomicEvolutionRuntimeError(
                "account-timeline terminal predecessor is not frozen 0012"
            )

    def _verify_proof_state(self) -> None:
        proof = load_and_verify(self.state_dir / "proof_registry.json")
        if burned_window_ids(proof) != ("Q4_2024",):
            raise EconomicEvolutionRuntimeError(
                "account-timeline terminal unexpected proof-window state"
            )
        current_trials = multiplicity_trial_count(proof)
        if not self.receipt_path.is_file() and current_trials != EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "account-timeline terminal multiplicity drift"
            )
        if self.receipt_path.is_file() and current_trials < EXPECTED_N_TRIALS:
            raise EconomicEvolutionRuntimeError(
                "account-timeline terminal multiplicity regressed"
            )
        reservations = [
            row
            for row in proof["entries"]
            if row.get("event_type") == MULTIPLICITY_EVENT
            and (row.get("evidence") or {}).get("campaign_id") == CAMPAIGN_ID
        ]
        if len(reservations) != 1:
            raise EconomicEvolutionRuntimeError(
                "account-timeline terminal found invalid reservation count"
            )

    def _write_or_verify_receipt(
        self,
        verdict: Mapping[str, Any],
        tombstone: ClassTombstone,
    ) -> dict[str, Any]:
        frozen = verdict["graveyard_append"]
        receipt = {
            "schema": "hydra_account_timeline_terminal_graveyard_receipt_v1",
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
            "class_signature_count": int(frozen["class_signature_count_after"]),
            "indexed_object_count": int(frozen["indexed_object_count_after"]),
            "parameter_level_feedback": False,
            "matched_controls_counted_as_candidates": False,
            "proof_windows_consumed": 0,
            "new_data_purchase_count": 0,
            "q4_access_delta": 0,
            "broker_connections": 0,
            "orders": 0,
            "next_experiment_id": NEXT_CAMPAIGN_ID,
        }
        receipt["receipt_hash"] = stable_hash(receipt)
        if self.receipt_path.is_file():
            prior = _load_json(self.receipt_path)
            if prior != receipt:
                raise EconomicEvolutionRuntimeError(
                    "account-timeline terminal receipt drift"
                )
            return prior
        _atomic_json(self.receipt_path, receipt)
        return receipt


def load_and_verify_account_timeline_terminal_verdict(
    root: str | Path,
    *,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    project = Path(root).resolve()
    path = project / TERMINAL_VERDICT_RELATIVE_PATH
    if _sha256(path) != TERMINAL_VERDICT_SHA256:
        raise EconomicEvolutionRuntimeError("account-timeline terminal WORM drift")
    tagged_commit = subprocess.check_output(
        ["git", "rev-parse", f"{TERMINAL_VERDICT_TAG}^{{commit}}"],
        cwd=project,
        text=True,
    ).strip()
    if tagged_commit != TERMINAL_VERDICT_COMMIT:
        raise EconomicEvolutionRuntimeError("account-timeline terminal tag drift")
    tagged_blob = subprocess.check_output(
        ["git", "show", f"{TERMINAL_VERDICT_TAG}:{TERMINAL_VERDICT_RELATIVE_PATH}"],
        cwd=project,
    )
    if hashlib.sha256(tagged_blob).hexdigest() != TERMINAL_VERDICT_SHA256:
        raise EconomicEvolutionRuntimeError(
            "account-timeline terminal tagged blob drift"
        )
    verdict = _load_json(path)
    claimed = verdict.get("verdict_hash")
    payload = dict(verdict)
    payload.pop("verdict_hash", None)
    frozen = verdict.get("result") or {}
    terminal = verdict.get("terminal_decision") or {}
    append = verdict.get("graveyard_append") or {}
    tripwire = result.get("family_tripwire") or {}
    policies = result.get("account_policy_economics") or {}
    population = result.get("population") or {}
    paired = result.get("paired_account_economics") or {}
    if (
        claimed != TERMINAL_VERDICT_SEMANTIC_HASH
        or stable_hash(payload) != TERMINAL_VERDICT_SEMANTIC_HASH
        or verdict.get("campaign_id") != CAMPAIGN_ID
        or frozen.get("file_sha256") != RESULT_SHA256
        or frozen.get("semantic_hash") != RESULT_SEMANTIC_HASH
        or frozen.get("scientific_status") != result.get("scientific_status")
        or int(frozen.get("real_policy_count", -1))
        != int(population.get("real_policy_count", -2))
        or int(frozen.get("matched_control_policy_count", -1))
        != int(population.get("matched_control_policy_count", -2))
        or int(frozen.get("informative_pair_count", -1))
        != int(tripwire.get("informative_pair_count", -2))
        or int(frozen.get("real_win_count", -1))
        != int(tripwire.get("real_win_count", -2))
        or int(frozen.get("matched_control_win_count", -1))
        != int(tripwire.get("matched_control_win_count", -2))
        or int(frozen.get("rolling_combine_episode_count", -1))
        != int(policies.get("primary_rolling_combine_episode_count", -2))
        or int(frozen.get("policies_with_at_least_one_combine_pass", -1))
        != int(policies.get("policies_passing_at_least_one_combine_episode", -2))
        or float(frozen.get("paired_stressed_net_delta_median_usd", -1.0))
        != float((paired.get("stressed_median_net_delta_usd") or {}).get("median", -2.0))
        or terminal.get("verdict") != "CLASS_TOMBSTONE_EXACT_GRAMMAR"
        or terminal.get("parameter_rescue_allowed") is not False
        or terminal.get("same_class_relaunch_allowed") is not False
        or int(append.get("candidate_count", -1)) != 512
        or append.get("parameter_level_feedback") is not False
    ):
        raise EconomicEvolutionRuntimeError(
            "account-timeline terminal WORM does not match frozen result"
        )
    return verdict


def account_timeline_terminal_action(
    predecessor: Mapping[str, Any],
    verdict: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(predecessor),
        "action_type": "ECONOMIC_EVOLUTION_ACCOUNT_TIMELINE_0012_TOMBSTONED",
        "economic_account_timeline_terminal_state": "COMPLETE",
        "economic_account_timeline_terminal_verdict": verdict[
            "terminal_decision"
        ]["verdict"],
        "economic_account_timeline_tombstone_signature_hash": receipt[
            "class_signature_hash"
        ],
        "economic_account_timeline_graveyard_class_signature_count": int(
            receipt["class_signature_count"]
        ),
        "economic_account_timeline_graveyard_indexed_object_count": int(
            receipt["indexed_object_count"]
        ),
        "economic_account_timeline_tombstoned_real_policy_count": int(
            receipt["candidate_count"]
        ),
        "economic_account_timeline_parameter_rescue_allowed": False,
        "economic_account_timeline_same_class_relaunch_allowed": False,
        "economic_account_timeline_status_inheritance_allowed": False,
        "raw_global_N_trials": EXPECTED_N_TRIALS,
        "economic_independent_confirmation_queue_eligible_count": 0,
        "economic_pre_holdout_ready_count": 0,
        "economic_paper_shadow_ready_count": 0,
        "new_data_purchase_authorized": False,
        "protected_holdout_access_authorized": False,
        "shadow_admission_authorized": False,
        "next_experiment_id": NEXT_CAMPAIGN_ID,
        "next_experiment_state": "MANIFEST_QUEUE_READY",
        "reason": (
            "The controller tombstoned only completed-outcome persistence "
            "routing and handed control to the manifest production queue."
        ),
        "principal_blocker": None,
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
    append_result: Mapping[str, Any],
    *,
    allow_downstream_growth: bool = False,
) -> None:
    expected = verdict["graveyard_append"]
    actual_classes = int(append_result.get("class_signature_count", -1))
    actual_objects = int(append_result.get("indexed_object_count", -1))
    expected_classes = int(expected["class_signature_count_after"])
    expected_objects = int(expected["indexed_object_count_after"])
    if allow_downstream_growth:
        mismatch = (
            actual_classes < expected_classes or actual_objects < expected_objects
        )
    else:
        mismatch = (
            actual_classes != expected_classes
            or actual_objects != expected_objects
        )
    if (
        append_result.get("append_status")
        not in {"APPENDED", "ALREADY_PRESENT_IDENTICAL"}
        or mismatch
        or append_result.get("parameter_level_columns") != []
    ):
        raise EconomicEvolutionRuntimeError(
            "account-timeline graveyard append failed frozen expectations"
        )


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
    "EconomicEvolutionAccountTimelineTerminalRuntime",
    "NEXT_CAMPAIGN_ID",
    "TERMINAL_VERDICT_RELATIVE_PATH",
    "account_timeline_terminal_action",
    "load_and_verify_account_timeline_terminal_verdict",
]
