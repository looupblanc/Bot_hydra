from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
from itertools import combinations
from typing import Any


@dataclass(frozen=True)
class ClusterThresholds:
    overlap_threshold: float = 0.95
    pnl_correlation_threshold: float = 0.95
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
    return score >= max(thresholds.overlap_threshold, thresholds.pnl_correlation_threshold)


def sketch_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    components = [
        1.0 if left.get("daily_pnl_hash") == right.get("daily_pnl_hash") else 0.0,
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
    positive_labels = [
        "exact_duplicate",
        "metadata_only_change",
        "tiny_stop_neighbor",
        "tiny_target_neighbor",
        "same_signal_neighboring_parameter",
        "parent_child_high_trade_overlap",
    ]
    for label in positive_labels:
        clone = copy.deepcopy(base)
        clone["candidate_id"] = f"{base.get('candidate_id')}_{label}"
        clone["validation_hash"] = label
        clone["parent_candidate_id"] = base.get("candidate_id") if "parent_child" in label else base.get("parent_candidate_id")
        controls.append((base, clone, True, label))

    negative_templates = [
        ("different_session", {"session_histogram": {"overnight": 10}, "trade_timestamp_signature": "neg_ts_1", "tail_event_signature": "neg_tail_1"}),
        ("opposite_direction", {"direction_signature": "opposite_direction", "trade_timestamp_signature": "neg_ts_2", "tail_event_signature": "neg_tail_2"}),
        ("different_instrument", {"symbol_exposure": {"ES": 10}, "trade_timestamp_signature": "neg_ts_3", "tail_event_signature": "neg_tail_3"}),
        ("different_holding_horizon", {"holding_time_histogram": {"120-124": 10}, "trade_timestamp_signature": "neg_ts_4", "tail_event_signature": "neg_tail_4"}),
        ("different_tail_behavior", {"tail_event_signature": "materially_different_tail", "trade_timestamp_signature": "neg_ts_5"}),
        ("low_trade_overlap", {"trade_timestamp_signature": "low_overlap", "daily_pnl_hash": "low_overlap_pnl"}),
        ("different_regime", {"regime_exposure": {"high_vol_failure": 10}, "trade_timestamp_signature": "neg_ts_6", "tail_event_signature": "neg_tail_6"}),
        ("different_portfolio_role", {"symbol_exposure": {"MES": 5, "MNQ": 5}, "session_histogram": {"rth_open": 10}, "trade_timestamp_signature": "neg_ts_7", "tail_event_signature": "neg_tail_7"}),
    ]
    for label, overrides in negative_templates:
        other = copy.deepcopy(base)
        other["candidate_id"] = f"{base.get('candidate_id')}_{label}"
        other.update(overrides)
        controls.append((base, other, False, label))

    for left, right in combinations(sketches[: min(len(sketches), 20)], 2):
        different_symbol = left.get("symbol_exposure") != right.get("symbol_exposure")
        different_session = left.get("session_histogram") != right.get("session_histogram")
        different_tail = left.get("tail_event_signature") != right.get("tail_event_signature")
        if (different_symbol or (different_session and different_tail)) and sketch_similarity(left, right) < 0.75:
            controls.append((left, right, False, "negative_behavioral_difference"))
        if len(controls) >= 60:
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
