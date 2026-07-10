from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
from itertools import combinations
from typing import Any


@dataclass(frozen=True)
class ClusterThresholds:
    overlap_threshold: float = 0.90
    pnl_correlation_threshold: float = 0.92
    tail_overlap_threshold: float = 0.80
    holding_similarity_threshold: float = 0.85


@dataclass(frozen=True)
class PairDecision:
    left: str
    right: str
    should_cluster: bool
    did_cluster: bool
    similarity: float
    control_type: str


def calibrate_clustering_controls(sketches: list[dict[str, Any]], thresholds: ClusterThresholds | None = None) -> dict[str, Any]:
    thresholds = thresholds or ClusterThresholds()
    controls = build_controls(sketches)
    decisions = [evaluate_pair(left, right, expected, label, thresholds) for left, right, expected, label in controls]
    positives = [item for item in decisions if item.should_cluster]
    negatives = [item for item in decisions if not item.should_cluster]
    tp = sum(1 for item in positives if item.did_cluster)
    fn = sum(1 for item in positives if not item.did_cluster)
    tn = sum(1 for item in negatives if not item.did_cluster)
    fp = sum(1 for item in negatives if item.did_cluster)
    return {
        "thresholds": asdict(thresholds),
        "control_pairs": len(decisions),
        "positive_controls": len(positives),
        "negative_controls": len(negatives),
        "precision_known_clones": round(tp / max(tp + fp, 1), 6),
        "recall_known_clones": round(tp / max(tp + fn, 1), 6),
        "false_merge_rate": round(fp / max(len(negatives), 1), 6),
        "false_split_rate": round(fn / max(len(positives), 1), 6),
        "cluster_stability": stability_proxy(sketches, thresholds),
        "recommended_thresholds": asdict(thresholds),
        "uncertainty": "Controls are synthetic/local until a larger trade-ledger set exists.",
        "pair_decisions": [asdict(item) for item in decisions[:50]],
    }


def cluster_sketches(sketches: list[dict[str, Any]], thresholds: ClusterThresholds | None = None) -> list[dict[str, Any]]:
    thresholds = thresholds or ClusterThresholds()
    clusters: list[list[dict[str, Any]]] = []
    for sketch in sketches:
        placed = False
        for cluster in clusters:
            if any(should_cluster(sketch, member, thresholds) for member in cluster):
                cluster.append(sketch)
                placed = True
                break
        if not placed:
            clusters.append([sketch])
    out = []
    for idx, members in enumerate(clusters, start=1):
        out.append(
            {
                "cluster_id": f"behavior_cluster_{idx:04d}",
                "level": cluster_level(members),
                "member_count": len(members),
                "representative": members[0].get("candidate_id"),
                "members": [item.get("candidate_id") for item in members],
                "portfolio_role": _portfolio_role(members),
            }
        )
    return sorted(out, key=lambda item: item["member_count"], reverse=True)


def should_cluster(left: dict[str, Any], right: dict[str, Any], thresholds: ClusterThresholds) -> bool:
    if left.get("candidate_id") == right.get("candidate_id"):
        return True
    exact_signatures = (
        left.get("daily_pnl_hash") == right.get("daily_pnl_hash")
        and left.get("trade_timestamp_signature") == right.get("trade_timestamp_signature")
        and left.get("direction_signature") == right.get("direction_signature")
    )
    if exact_signatures:
        return True
    score = sketch_similarity(left, right)
    return score >= min(thresholds.overlap_threshold, thresholds.pnl_correlation_threshold)


def sketch_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    components = [
        1.0 if left.get("trade_timestamp_signature") == right.get("trade_timestamp_signature") else 0.0,
        1.0 if left.get("direction_signature") == right.get("direction_signature") else 0.0,
        1.0 if left.get("tail_event_signature") == right.get("tail_event_signature") else 0.0,
        histogram_similarity(left.get("holding_time_histogram", {}), right.get("holding_time_histogram", {})),
        histogram_similarity(left.get("session_histogram", {}), right.get("session_histogram", {})),
        histogram_similarity(left.get("symbol_exposure", {}), right.get("symbol_exposure", {})),
    ]
    return round(sum(components) / len(components), 6)


def build_controls(sketches: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any], bool, str]]:
    if not sketches:
        return []
    controls: list[tuple[dict[str, Any], dict[str, Any], bool, str]] = []
    base = sketches[0]
    exact = copy.deepcopy(base)
    exact["candidate_id"] = f"{base.get('candidate_id')}_metadata_variant"
    exact["parent_candidate_id"] = "metadata_only_change"
    controls.append((base, exact, True, "exact_duplicate_metadata_change"))
    tiny_stop = copy.deepcopy(base)
    tiny_stop["candidate_id"] = f"{base.get('candidate_id')}_tiny_stop_variant"
    tiny_stop["validation_hash"] = "tiny_stop_variant"
    controls.append((base, tiny_stop, True, "tiny_stop_target_neighbor"))
    for left, right in combinations(sketches[: min(len(sketches), 20)], 2):
        different_symbol = left.get("symbol_exposure") != right.get("symbol_exposure")
        different_session = left.get("session_histogram") != right.get("session_histogram")
        different_tail = left.get("tail_event_signature") != right.get("tail_event_signature")
        if different_symbol or (different_session and different_tail):
            controls.append((left, right, False, "negative_behavioral_difference"))
        if len(controls) >= 30:
            break
    return controls


def evaluate_pair(
    left: dict[str, Any],
    right: dict[str, Any],
    expected: bool,
    label: str,
    thresholds: ClusterThresholds,
) -> PairDecision:
    similarity = sketch_similarity(left, right)
    did = should_cluster(left, right, thresholds)
    return PairDecision(
        left=str(left.get("candidate_id")),
        right=str(right.get("candidate_id")),
        should_cluster=expected,
        did_cluster=did,
        similarity=similarity,
        control_type=label,
    )


def stability_proxy(sketches: list[dict[str, Any]], thresholds: ClusterThresholds) -> float:
    if len(sketches) < 4:
        return 1.0
    base = cluster_sketches(sketches, thresholds)
    relaxed = cluster_sketches(sketches, ClusterThresholds(overlap_threshold=thresholds.overlap_threshold - 0.05, pnl_correlation_threshold=thresholds.pnl_correlation_threshold - 0.05))
    strict = cluster_sketches(sketches, ClusterThresholds(overlap_threshold=thresholds.overlap_threshold + 0.03, pnl_correlation_threshold=thresholds.pnl_correlation_threshold + 0.03))
    base_count = len(base)
    deltas = abs(len(relaxed) - base_count) + abs(len(strict) - base_count)
    return round(max(0.0, 1.0 - deltas / max(base_count, 1)), 6)


def histogram_similarity(left: dict[str, int], right: dict[str, int]) -> float:
    keys = set(left) | set(right)
    if not keys:
        return 1.0
    overlap = sum(min(int(left.get(k, 0)), int(right.get(k, 0))) for k in keys)
    total = sum(max(int(left.get(k, 0)), int(right.get(k, 0))) for k in keys)
    return overlap / max(total, 1)


def cluster_level(members: list[dict[str, Any]]) -> str:
    if len(members) <= 1:
        return "LEVEL_4_DISTINCT_PORTFOLIO_ROLE"
    signatures = {(m.get("daily_pnl_hash"), m.get("trade_timestamp_signature"), m.get("direction_signature")) for m in members}
    if len(signatures) == 1:
        return "LEVEL_1_EXECUTION_EQUIVALENT"
    mechanisms = {(tuple(sorted((m.get("symbol_exposure") or {}).items())), tuple(sorted((m.get("session_histogram") or {}).items()))) for m in members}
    if len(mechanisms) == 1:
        return "LEVEL_2_SAME_ECONOMIC_MECHANISM"
    return "LEVEL_3_SAME_BROAD_FAMILY_BEHAVIORALLY_DIFFERENT"


def _portfolio_role(members: list[dict[str, Any]]) -> str:
    exposure = members[0].get("symbol_exposure") or {}
    if set(exposure) <= {"MES", "MNQ"}:
        return "micro_risk_control"
    if "NQ" in exposure or "MNQ" in exposure:
        return "nasdaq_momentum_or_relative_value"
    if "ES" in exposure or "MES" in exposure:
        return "sp500_index_exposure"
    return "unknown"
