from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


EVIDENCE_CONVERSION_ALLOCATION = {
    "promotion": 0.70,
    "shadow_feed_engineering": 0.15,
    "targeted_mutation": 0.10,
    "discovery": 0.05,
}
EVIDENCE_CONVERSION_STATUSES = frozenset(
    {"PROMOTION_FAILED", "SHADOW_RESEARCH_ONLY", "PRE_HOLDOUT_READY"}
)
EVIDENCE_CONVERSION_ELIGIBLE_STATUSES = frozenset(
    {
        "PROMISING_RESEARCH_CANDIDATE",
        "ROBUST_RESEARCH_CANDIDATE",
        "SHADOW_RESEARCH_CANDIDATE",
        "SHADOW_ACTIVE",
    }
)
STRUCTURAL_EXHAUSTION_MARKERS = (
    "insufficient_structural_diversity",
    "only ",
    "unique structures are available",
    "family/lineage caps permit only",
)


class EvidenceConversionContractError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FrozenEvidenceSources:
    promotion_result_paths: tuple[str, ...]
    promotion_result_sha256s: dict[str, str]
    exact_result_paths: tuple[str, ...]
    exact_result_sha256s: dict[str, str]
    source_experiment_ids: tuple[str, ...]
    source_fingerprint: str

    @classmethod
    def build(
        cls,
        *,
        promotion_sources: Sequence[Mapping[str, str]],
        exact_sources: Sequence[Mapping[str, str]],
    ) -> "FrozenEvidenceSources":
        promotion_by_path = _validated_source_map(promotion_sources, "promotion")
        exact_by_path = _validated_source_map(exact_sources, "exact replay")
        if not promotion_by_path or not exact_by_path:
            raise EvidenceConversionContractError(
                "Evidence conversion requires frozen promotion and exact-replay sources."
            )
        source_ids = tuple(
            sorted(
                {
                    str(row.get("experiment_id") or "")
                    for row in promotion_sources
                    if str(row.get("experiment_id") or "")
                }
            )
        )
        payload = {
            "promotion_result_sha256s": promotion_by_path,
            "exact_result_sha256s": exact_by_path,
            "source_experiment_ids": source_ids,
        }
        return cls(
            promotion_result_paths=tuple(sorted(promotion_by_path)),
            promotion_result_sha256s=promotion_by_path,
            exact_result_paths=tuple(sorted(exact_by_path)),
            exact_result_sha256s=exact_by_path,
            source_experiment_ids=source_ids,
            source_fingerprint=_stable_hash(payload),
        )

    def to_specification_fields(self) -> dict[str, Any]:
        return {
            "source_result_paths": list(self.promotion_result_paths),
            "source_result_sha256s": dict(self.promotion_result_sha256s),
            "source_exact_result_paths": list(self.exact_result_paths),
            "source_exact_result_sha256s": dict(self.exact_result_sha256s),
            "source_experiment_ids": list(self.source_experiment_ids),
            "source_fingerprint": self.source_fingerprint,
        }


def is_turbo_structural_exhaustion(error: BaseException | str) -> bool:
    value = str(error).lower()
    if "insufficient_structural_diversity" in value:
        return True
    return all(marker in value for marker in STRUCTURAL_EXHAUSTION_MARKERS[1:3]) or (
        STRUCTURAL_EXHAUSTION_MARKERS[3] in value
    )


def candidate_bank_manifest(
    bank: Mapping[str, Mapping[str, Any]],
    *,
    cohort_id: str,
    code_commit: str,
    sources: FrozenEvidenceSources,
    killed_candidate_ids: Sequence[str] = (),
) -> dict[str, Any]:
    killed = {str(value) for value in killed_candidate_ids}
    candidates: list[dict[str, Any]] = []
    excluded_status_counts: dict[str, int] = {}
    killed_excluded = 0
    for candidate_id in sorted(bank):
        row = dict(bank[candidate_id] or {})
        status = str(row.get("status") or "")
        if candidate_id in killed:
            killed_excluded += 1
            continue
        if status not in EVIDENCE_CONVERSION_ELIGIBLE_STATUSES:
            excluded_status_counts[status or "MISSING_STATUS"] = (
                excluded_status_counts.get(status or "MISSING_STATUS", 0) + 1
            )
            continue
        row["candidate_id"] = candidate_id
        candidates.append(row)
    payload: dict[str, Any] = {
        "schema": "hydra_evidence_conversion_candidate_bank_manifest_v1",
        "cohort_id": cohort_id,
        "code_commit": code_commit,
        "source_candidate_bank_count": len(bank),
        "candidate_count": len(candidates),
        "eligible_statuses": sorted(EVIDENCE_CONVERSION_ELIGIBLE_STATUSES),
        "excluded_status_counts": dict(sorted(excluded_status_counts.items())),
        "killed_candidates_excluded": killed_excluded,
        "source_experiment_ids": list(sources.source_experiment_ids),
        "source_fingerprint": sources.source_fingerprint,
        "candidates": candidates,
        "q4_access_allowed": False,
        "paid_data_allowed": False,
        "network_allowed": False,
        "live_or_broker_allowed": False,
    }
    payload["manifest_hash"] = _stable_hash(payload)
    return payload


def validate_evidence_conversion_result(result: Mapping[str, Any]) -> None:
    required = {
        "schema",
        "cohort_id",
        "candidates_before_clustering",
        "behavioral_clusters",
        "representative_count",
        "role_distribution",
        "evidence_debt_queue_count",
        "evidence_debt_inventory_count",
        "full_economic_replay_count",
        "full_risk_replay_count",
        "full_promotion_validation_count",
        "promotion_decisions_count",
        "complete_validation_candidate_ids",
        "status_counts",
        "pre_holdout_candidate_ids",
        "q4_access_count",
        "paper_shadow_ready",
        "report_path",
        "artifacts",
        "artifact_sha256s",
        "result_hash",
        "scientific_conclusion",
    }
    missing = sorted(required - set(result))
    if missing:
        raise EvidenceConversionContractError(
            f"Evidence-conversion result is missing required fields: {missing}"
        )
    if str(result.get("schema") or "") != "hydra_evidence_conversion_foundry_v3":
        raise EvidenceConversionContractError(
            "Unexpected evidence-conversion result schema."
        )
    status_counts = dict(result.get("status_counts") or {})
    if set(status_counts) != EVIDENCE_CONVERSION_STATUSES:
        raise EvidenceConversionContractError(
            "Evidence-conversion statuses must be exactly "
            f"{sorted(EVIDENCE_CONVERSION_STATUSES)}."
        )
    if int(result.get("q4_access_count") or 0) != 0:
        raise EvidenceConversionContractError("Evidence conversion accessed protected Q4.")
    if int(result.get("paper_shadow_ready") or 0) != 0:
        raise EvidenceConversionContractError(
            "A pre-holdout cohort cannot claim PAPER_SHADOW_READY."
        )
    representatives = int(result.get("representative_count") or 0)
    if representatives < 0 or representatives > 40:
        raise EvidenceConversionContractError("Representative count is outside the frozen cap.")
    validated = int(result.get("full_promotion_validation_count") or 0)
    if validated < 0 or validated > 20 or validated > representatives:
        raise EvidenceConversionContractError("Full-validation count violates the frozen cap.")
    decisions = int(result.get("promotion_decisions_count") or 0)
    if decisions < validated or decisions > representatives:
        raise EvidenceConversionContractError(
            "Promotion decision count is inconsistent with complete validation and representatives."
        )
    if sum(int(value) for value in status_counts.values()) != decisions:
        raise EvidenceConversionContractError(
            "Role-specific status counts must cover every promotion decision."
        )
    decided_ids = [
        str(value) for value in result.get("complete_validation_candidate_ids") or []
    ]
    if len(decided_ids) != len(set(decided_ids)) or len(decided_ids) != decisions:
        raise EvidenceConversionContractError(
            "Complete-validation candidate IDs must uniquely cover every promotion decision."
        )
    remaining = int(result.get("evidence_debt_queue_count") or 0)
    inventory = int(result.get("evidence_debt_inventory_count") or 0)
    if remaining < 0 or inventory < decisions or remaining > inventory:
        raise EvidenceConversionContractError(
            "Evidence-debt queue and inventory counts are inconsistent."
        )
    pre_holdout = [str(value) for value in result.get("pre_holdout_candidate_ids") or []]
    if len(pre_holdout) != len(set(pre_holdout)):
        raise EvidenceConversionContractError("Pre-holdout candidate IDs are not unique.")
    if len(pre_holdout) != int(status_counts.get("PRE_HOLDOUT_READY", 0)):
        raise EvidenceConversionContractError(
            "PRE_HOLDOUT_READY count does not match the immutable candidate list."
        )
    if len(pre_holdout) > validated:
        raise EvidenceConversionContractError(
            "PRE_HOLDOUT_READY cannot exceed fully validated candidates."
        )
    candidates = [dict(value) for value in result.get("candidates") or []]
    if int(result.get("candidate_count") or 0) != decisions or len(candidates) != decisions:
        raise EvidenceConversionContractError(
            "Candidate payload does not cover every promotion decision."
        )
    candidate_ids = [str(value.get("candidate_id") or "") for value in candidates]
    if set(candidate_ids) != set(decided_ids) or len(candidate_ids) != len(set(candidate_ids)):
        raise EvidenceConversionContractError(
            "Candidate payload IDs differ from the immutable decision list."
        )
    observed_statuses = {
        status: sum(str(value.get("status") or "") == status for value in candidates)
        for status in EVIDENCE_CONVERSION_STATUSES
    }
    if observed_statuses != {key: int(value) for key, value in status_counts.items()}:
        raise EvidenceConversionContractError(
            "Candidate statuses differ from the aggregate result counts."
        )
    if any(
        str(value.get("status") or "") == "PRE_HOLDOUT_READY"
        and str(value.get("candidate_id") or "") not in set(pre_holdout)
        for value in candidates
    ):
        raise EvidenceConversionContractError(
            "Pre-holdout candidate payload differs from the frozen cohort list."
        )

    semantic = dict(result)
    for key in ("result_hash", "artifacts", "artifact_sha256s", "report_path"):
        semantic.pop(key, None)
    expected_result_hash = _stable_hash(semantic)
    if str(result.get("result_hash") or "") != expected_result_hash:
        raise EvidenceConversionContractError(
            "Evidence-conversion semantic result hash is invalid."
        )

    artifacts = dict(result.get("artifacts") or {})
    artifact_hashes = dict(result.get("artifact_sha256s") or {})
    required_artifacts = {
        "evidence_debt_queue_path",
        "behavioral_clusters_path",
        "representatives_path",
        "complete_validation_path",
        "result_path",
        "report_path",
    }
    if not required_artifacts <= set(artifacts) or not required_artifacts <= set(artifact_hashes):
        raise EvidenceConversionContractError(
            "Evidence-conversion artifact receipt set is incomplete."
        )
    for key in sorted(required_artifacts | {"pre_holdout_manifest_path"}):
        path_value = artifacts.get(key)
        digest = artifact_hashes.get(key)
        if path_value is None and digest is None and key == "pre_holdout_manifest_path":
            continue
        path = Path(str(path_value))
        if not path.is_file() or len(str(digest or "")) != 64:
            raise EvidenceConversionContractError(
                f"Evidence-conversion artifact is missing: {key}."
            )
        if hashlib.sha256(path.read_bytes()).hexdigest() != str(digest):
            raise EvidenceConversionContractError(
                f"Evidence-conversion artifact hash mismatch: {key}."
            )
    persisted = json.loads(Path(str(artifacts["result_path"])).read_text(encoding="utf-8"))
    if str(persisted.get("result_hash") or "") != str(result.get("result_hash") or ""):
        raise EvidenceConversionContractError(
            "Persisted semantic result differs from the routed result."
        )


def should_schedule_followup(result: Mapping[str, Any]) -> bool:
    validate_evidence_conversion_result(result)
    return int(result.get("evidence_debt_queue_count") or 0) > 0


def _validated_source_map(
    rows: Sequence[Mapping[str, str]], label: str
) -> dict[str, str]:
    output: dict[str, str] = {}
    for row in rows:
        path = str(row.get("path") or "")
        digest = str(row.get("sha256") or "")
        if not path or len(digest) != 64:
            raise EvidenceConversionContractError(
                f"Frozen {label} source lacks path or SHA-256."
            )
        previous = output.get(path)
        if previous is not None and previous != digest:
            raise EvidenceConversionContractError(
                f"Frozen {label} source has conflicting hashes: {path}"
            )
        output[path] = digest
    return dict(sorted(output.items()))


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


__all__ = [
    "EVIDENCE_CONVERSION_ALLOCATION",
    "EVIDENCE_CONVERSION_ELIGIBLE_STATUSES",
    "EVIDENCE_CONVERSION_STATUSES",
    "EvidenceConversionContractError",
    "FrozenEvidenceSources",
    "candidate_bank_manifest",
    "is_turbo_structural_exhaustion",
    "should_schedule_followup",
    "validate_evidence_conversion_result",
]
