from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


STATUS_POLICY_VERSION = "candidate_null_policy_v1"

MANDATORY_NULLS = (
    "random_matched_effect",
    "block_shuffled_effect",
    "delayed_effect",
    "sign_flipped_effect",
    "momentum_baseline_effect",
    "mean_reversion_baseline_effect",
)


@dataclass(frozen=True)
class CandidateNullDecision:
    policy_version: str
    candidate_id: str
    tested_null_set: tuple[str, ...]
    null_tests: int
    null_tests_passed: int
    required_tests: tuple[str, ...]
    effect_size: float
    event_count: int
    adjusted_probability: float
    effective_trial_count: int
    passed: bool
    decision_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_level_null_decision(
    null_result: dict[str, Any],
    *,
    candidate_id: str,
    effective_trial_count: int,
    min_events: int = 30,
    min_adjusted_probability: float = 0.60,
) -> CandidateNullDecision:
    real = abs(float(null_result.get("real_effect", 0.0)))
    tested = tuple(key for key in MANDATORY_NULLS if key in null_result)
    passed_tests = tuple(key for key in tested if real > abs(float(null_result.get(key, 0.0))))
    probability = len(passed_tests) / max(len(tested), 1)
    adjusted = max(0.0, probability - (max(effective_trial_count, 1) - 1) * 0.01)
    event_count = int(null_result.get("event_count", 0))
    passed = bool(event_count >= min_events and set(MANDATORY_NULLS).issubset(passed_tests) and adjusted >= min_adjusted_probability)
    if event_count < min_events:
        reason = "insufficient_candidate_events"
    elif not set(MANDATORY_NULLS).issubset(passed_tests):
        missing = sorted(set(MANDATORY_NULLS) - set(passed_tests))
        reason = "mandatory_null_failed:" + ",".join(missing)
    elif adjusted < min_adjusted_probability:
        reason = "multiple_testing_adjusted_probability_below_threshold"
    else:
        reason = "candidate_level_matched_null_policy_passed"
    return CandidateNullDecision(
        policy_version=STATUS_POLICY_VERSION,
        candidate_id=candidate_id,
        tested_null_set=tested,
        null_tests=len(tested),
        null_tests_passed=len(passed_tests),
        required_tests=MANDATORY_NULLS,
        effect_size=float(null_result.get("real_effect", 0.0)),
        event_count=event_count,
        adjusted_probability=float(round(adjusted, 6)),
        effective_trial_count=int(effective_trial_count),
        passed=passed,
        decision_reason=reason,
    )


def stable_policy_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def previous_status_semantics() -> dict[str, Any]:
    return {
        "MATCHED_NULL_BEATEN": "Prototype label assigned when any component in the prototype overlapped a lane-level component-level null comparison that passed.",
        "REPRESENTATION_EVIDENCE_PASS": "Prototype label assigned when the shared component-level null condition was true and sampled Q1-Q3 net was positive in at least two periods.",
        "TOPSTEP_PATH_CANDIDATE": "Prototype label assigned from sampled pooled net PnL above 1500 plus shared component-level null evidence.",
        "TOPSTEP_COMPATIBLE": "Prototype label assigned from sampled pooled net PnL above 9000 plus shared component-level null evidence; no candidate-level Topstep replay was performed.",
        "defect": "Previous labels were not candidate-level complete-null-suite decisions and cannot support Q4 freeze or promotion.",
    }
