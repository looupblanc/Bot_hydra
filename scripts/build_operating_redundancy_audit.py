#!/usr/bin/env python3
"""Build the read-only redundancy audit for the six frozen HYDRA books.

The audit deliberately reads the sealed selection/report plus the compact
EvidenceBundle daily-path parts.  It never opens the large replay caches and it
never mutates an account book, ledger, or service state.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
from collections import Counter
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping

from hydra.economic_evolution.schema import stable_hash


SCHEMA = "hydra_operating_redundancy_audit_v1"
HORIZON = "90_TRADING_DAYS"
FROZEN_BOOK_IDS = (
    "active_pool_070e391c7586ba1fac2f5494",
    "active_pool_186a4177401aab223b0a21fa",
    "active_pool_14e275fa8d869c28b1f27f78",
    "active_pool_2287bfb0b1c6f07930150102",
    "active_pool_2377af7025aadf9aaf456a7e",
    "active_pool_014dffb40e99814612d78c51",
)
ROLE_BY_BOOK = {
    "active_pool_070e391c7586ba1fac2f5494": "SAFETY_CAP",
    "active_pool_186a4177401aab223b0a21fa": "CORE",
    "active_pool_14e275fa8d869c28b1f27f78": "SAFETY_BUFFER",
    "active_pool_2287bfb0b1c6f07930150102": "DIVERSIFIER_GOVERNOR_RELATIVE",
    "active_pool_2377af7025aadf9aaf456a7e": "CORE_AUXILIARY",
    "active_pool_014dffb40e99814612d78c51": "BACKUP_DIVERSE",
}
STAGES = ("stage3", "stage4", "stage5")


class RedundancyAuditError(RuntimeError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RedundancyAuditError(message)


def _jaccard(left: set[Any], right: set[Any]) -> float:
    union = left | right
    return float(len(left & right) / len(union)) if union else 1.0


def _pearson(left: list[float], right: list[float]) -> float:
    _require(len(left) == len(right) and bool(left), "correlation vectors drifted")
    left_mean = math.fsum(left) / len(left)
    right_mean = math.fsum(right) / len(right)
    left_delta = [value - left_mean for value in left]
    right_delta = [value - right_mean for value in right]
    denominator = math.sqrt(
        math.fsum(value * value for value in left_delta)
        * math.fsum(value * value for value in right_delta)
    )
    if denominator == 0.0:
        _require(left == right, "undefined non-identical constant correlation")
        return 1.0
    return float(
        math.fsum(a * b for a, b in zip(left_delta, right_delta, strict=True))
        / denominator
    )


def _histogram(rows: Iterable[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        if field == "holding_bars":
            value = row["sleeve_specification"][field]
        else:
            value = row[field]
        counts[str(value)] += 1
    return dict(sorted(counts.items()))


def _discover_parts(dataset_root: Path) -> tuple[dict[str, dict[str, Path]], list[dict[str, Any]]]:
    found = {policy_id: {} for policy_id in FROZEN_BOOK_IDS}
    sources: list[dict[str, Any]] = []
    for path in sorted(dataset_root.glob("part-*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            header = json.loads(next(handle))
            try:
                first_row = json.loads(next(handle))
            except StopIteration:
                continue
        policy_id = str(first_row.get("policy_id") or "")
        if policy_id not in found:
            continue
        part = header.get("_evidence_part") or {}
        batch_id = str(part.get("batch_id") or "")
        stage = next((stage for stage in STAGES if f":{stage}:" in batch_id), None)
        if stage is None:
            continue
        _require(stage not in found[policy_id], f"duplicate {stage} part for {policy_id}")
        found[policy_id][stage] = path
        sources.append(
            {
                "policy_id": policy_id,
                "stage": stage,
                "relative_path": str(path),
                "batch_id": batch_id,
                "payload_sha256": str(part.get("payload_sha256") or ""),
                "row_count": int(part.get("row_count", 0)),
            }
        )
    for policy_id, stages in found.items():
        _require(set(stages) == set(STAGES), f"missing Stage3/4/5 parts for {policy_id}")
    return found, sorted(sources, key=lambda row: (row["policy_id"], row["stage"]))


def _empty_observations() -> dict[str, Any]:
    return {
        "daily": {},
        "emitted": set(),
        "accepted": set(),
        "accepted_exact": set(),
        "suppressed": set(),
        "suppressed_exact": set(),
        "conflict": set(),
        "size_reduced": set(),
    }


def _load_observations(parts: Mapping[str, Mapping[str, Path]]) -> dict[str, dict[str, Any]]:
    observations = {policy_id: _empty_observations() for policy_id in FROZEN_BOOK_IDS}
    horizon_marker = f'"horizon":"{HORIZON}"'
    for policy_id in FROZEN_BOOK_IDS:
        target = observations[policy_id]
        for stage in STAGES:
            path = parts[policy_id][stage]
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                next(handle)
                for raw_line in handle:
                    # Most rows belong to the other four frozen horizons.  The
                    # lexical guard avoids parsing their large routing arrays.
                    if horizon_marker not in raw_line:
                        continue
                    row = json.loads(raw_line)
                    _require(row["policy_id"] == policy_id, f"mixed policy part: {path}")
                    start_id = str(row["episode_id"]).split(":", 1)[1]
                    day_key = (
                        stage,
                        str(row["cost_scenario"]),
                        start_id,
                        str(row["trading_day"]),
                    )
                    _require(day_key not in target["daily"], f"duplicate daily key {day_key}")
                    target["daily"][day_key] = (
                        float(row["daily_pnl"]),
                        float(row["closing_mll_buffer"]),
                        float(row["target_progress"]),
                    )
                    for decision in row.get("risk_allocation") or ():
                        event_key = (
                            stage,
                            str(row["cost_scenario"]),
                            start_id,
                            str(decision["event_id"]),
                        )
                        if decision.get("emitted"):
                            target["emitted"].add(event_key)
                        if decision.get("accepted"):
                            target["accepted"].add(event_key)
                            target["accepted_exact"].add(
                                event_key
                                + (
                                    float(decision.get("quantity") or 0.0),
                                    float(decision.get("mini_equivalent") or 0.0),
                                )
                            )
                        if decision.get("rejected") or decision.get("size_reduced"):
                            target["suppressed"].add(event_key)
                            target["suppressed_exact"].add(
                                event_key
                                + (
                                    str(decision.get("decision_status") or ""),
                                    str(decision.get("reason") or ""),
                                    float(decision.get("quantity") or 0.0),
                                )
                            )
                        if decision.get("conflict_rejected"):
                            target["conflict"].add(event_key)
                        if decision.get("size_reduced"):
                            target["size_reduced"].add(event_key)
    return observations


def _pair_row(
    left_id: str,
    right_id: str,
    observations: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    left = observations[left_id]
    right = observations[right_id]
    common_days = sorted(set(left["daily"]) & set(right["daily"]))
    union_days = set(left["daily"]) | set(right["daily"])
    left_values = [left["daily"][key] for key in common_days]
    right_values = [right["daily"][key] for key in common_days]
    left_pnl = [row[0] for row in left_values]
    right_pnl = [row[0] for row in right_values]
    left_mll = [row[1] for row in left_values]
    right_mll = [row[1] for row in right_values]
    left_target = [row[2] for row in left_values]
    right_target = [row[2] for row in right_values]
    left_wins = {key for key, value in left["daily"].items() if value[0] > 0.0}
    right_wins = {key for key, value in right["daily"].items() if value[0] > 0.0}
    left_losses = {key for key, value in left["daily"].items() if value[0] < 0.0}
    right_losses = {key for key, value in right["daily"].items() if value[0] < 0.0}
    sign_discordant = sum(
        (a > 0.0 > b) or (b > 0.0 > a)
        for a, b in zip(left_pnl, right_pnl, strict=True)
    )
    return {
        "left_policy_id": left_id,
        "right_policy_id": right_id,
        "membership_jaccard": 1.0,
        "source_signal_ledger_jaccard": 1.0,
        "source_trade_ledger_jaccard": 1.0,
        "market_histogram_intersection": 1.0,
        "session_histogram_intersection": 1.0,
        "timeframe_histogram_intersection": 1.0,
        "holding_bars_histogram_intersection": 1.0,
        "common_episode_day_observations": len(common_days),
        "union_episode_day_observations": len(union_days),
        "episode_day_coverage_jaccard": len(common_days) / len(union_days),
        "emitted_signal_path_jaccard": _jaccard(left["emitted"], right["emitted"]),
        "accepted_trade_identity_jaccard": _jaccard(left["accepted"], right["accepted"]),
        "accepted_trade_quantity_jaccard": _jaccard(
            left["accepted_exact"], right["accepted_exact"]
        ),
        "daily_pnl_pearson": _pearson(left_pnl, right_pnl),
        "closing_mll_buffer_pearson": _pearson(left_mll, right_mll),
        "target_progress_pearson": _pearson(left_target, right_target),
        "shared_winning_episode_days": sum(
            a > 0.0 and b > 0.0
            for a, b in zip(left_pnl, right_pnl, strict=True)
        ),
        "shared_losing_episode_days": sum(
            a < 0.0 and b < 0.0
            for a, b in zip(left_pnl, right_pnl, strict=True)
        ),
        "sign_discordant_episode_days": sign_discordant,
        "winning_episode_day_jaccard": _jaccard(left_wins, right_wins),
        "losing_episode_day_jaccard": _jaccard(left_losses, right_losses),
        "suppressed_event_identity_jaccard": _jaccard(
            left["suppressed"], right["suppressed"]
        ),
        "suppressed_event_action_jaccard": _jaccard(
            left["suppressed_exact"], right["suppressed_exact"]
        ),
        "conflict_rejection_jaccard": _jaccard(left["conflict"], right["conflict"]),
        "size_reduction_jaccard": _jaccard(
            left["size_reduced"], right["size_reduced"]
        ),
    }


def _role_rows(
    selected_by_id: Mapping[str, Mapping[str, Any]],
    finalist_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    reasons = {
        "active_pool_070e391c7586ba1fac2f5494": "STRICTEST_10_MINI_3000_OPEN_RISK_AND_500_PROTECTED_BUFFER",
        "active_pool_186a4177401aab223b0a21fa": "BEST_STRESSED_NET_AND_CONSISTENCY_EV_WITH_LOWEST_FOREGONE_PNL",
        "active_pool_14e275fa8d869c28b1f27f78": "BEST_PRIMARY_MINIMUM_MLL_BUFFER_AND_SCALE_50_TARGET_PROTECTION",
        "active_pool_2287bfb0b1c6f07930150102": "DISTINCT_FIVE_SLEEVE_4500_OPEN_RISK_ROUTING_NOT_INDEPENDENT_ALPHA",
        "active_pool_2377af7025aadf9aaf456a7e": "STRONG_ECONOMICS_BUT_HIGH_REDUNDANCY_WITH_CORE",
        "active_pool_014dffb40e99814612d78c51": "GREATEST_TRAJECTORY_DISTANCE_WITH_WEAKER_ECONOMICS",
    }
    rows = []
    for policy_id in FROZEN_BOOK_IDS:
        selected = selected_by_id[policy_id]
        finalist = finalist_by_id[policy_id]
        policy = selected["frozen_policy_specification"]["active_risk_policy"]
        rows.append(
            {
                "policy_id": policy_id,
                "derived_operating_role": ROLE_BY_BOOK[policy_id],
                "selection_role": selected["selection_role"],
                "reason_code": reasons[policy_id],
                "governor": {
                    key: policy[key]
                    for key in (
                        "aggregate_open_risk_ceiling",
                        "daily_consistency_profit_guard",
                        "daily_loss_guard",
                        "maximum_concurrent_sleeves",
                        "maximum_mini_equivalent",
                        "protected_mll_buffer",
                        "target_protection_distance",
                        "target_protection_mode",
                    )
                },
                "normal_net_pnl_usd": float(finalist["normal"]["net_total"]),
                "stressed_net_pnl_usd": float(finalist["stressed"]["net_total"]),
                "normal_passes_out_of_192": int(finalist["normal"]["pass_count"]),
                "stressed_passes_out_of_192": int(finalist["stressed"]["pass_count"]),
                "normal_minimum_mll_buffer_usd": float(
                    finalist["normal"]["minimum_mll_buffer"]
                ),
                "stressed_minimum_mll_buffer_usd": float(
                    finalist["stressed"]["minimum_mll_buffer"]
                ),
            }
        )
    return rows


def validate_redundancy_audit(audit: Mapping[str, Any]) -> None:
    _require(audit.get("schema") == SCHEMA, "redundancy audit schema drift")
    pairs = audit.get("pairwise_redundancy") or []
    _require(len(pairs) == 15, "exactly 15 pairwise rows are required")
    pair_ids = {
        tuple(sorted((str(row["left_policy_id"]), str(row["right_policy_id"]))))
        for row in pairs
    }
    _require(len(pair_ids) == 15, "duplicate pairwise rows")
    _require(
        all(float(row["membership_jaccard"]) == 1.0 for row in pairs),
        "frozen membership overlap must remain one",
    )
    roles = {
        str(row["policy_id"]): str(row["derived_operating_role"])
        for row in audit.get("roles") or []
    }
    _require(roles == ROLE_BY_BOOK, "derived operating roles drifted")
    claimed = str(audit.get("audit_hash") or "")
    unhashed = dict(audit)
    unhashed.pop("audit_hash", None)
    _require(bool(claimed) and stable_hash(unhashed) == claimed, "audit hash drifted")


def build_redundancy_audit(
    *,
    selection_path: Path,
    decision_report_path: Path,
    evidence_dataset_root: Path,
) -> dict[str, Any]:
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    report = json.loads(decision_report_path.read_text(encoding="utf-8"))
    selected_by_id = {
        str(row["policy_id"]): row for row in selection["selected_books"]
    }
    _require(set(selected_by_id) == set(FROZEN_BOOK_IDS), "frozen six-book selection drifted")
    finalist_by_id = {
        str(row["policy_id"]): row
        for row in report["expanded_development_finalists"]["rows"]
    }
    _require(
        set(FROZEN_BOOK_IDS) <= set(finalist_by_id), "finalist report is incomplete"
    )

    memberships = {
        policy_id: selected_by_id[policy_id]["frozen_policy_specification"]["membership"]
        for policy_id in FROZEN_BOOK_IDS
    }
    membership_ids = {
        policy_id: {str(row["component_id"]) for row in rows}
        for policy_id, rows in memberships.items()
    }
    canonical_ids = membership_ids[FROZEN_BOOK_IDS[0]]
    _require(len(canonical_ids) == 18, "canonical sleeve count drifted")
    _require(
        all(value == canonical_ids for value in membership_ids.values()),
        "six books no longer share exact sleeve membership",
    )
    canonical_membership = memberships[FROZEN_BOOK_IDS[0]]
    inventory = {
        "sleeve_count": 18,
        "shared_by_all_six_books": True,
        "membership_sha256": selected_by_id[FROZEN_BOOK_IDS[0]][
            "membership_sha256"
        ],
        "component_ids": sorted(canonical_ids),
        "signal_ledger_sha256": sorted(
            str(row["signal_ledger_sha256"]) for row in canonical_membership
        ),
        "trade_ledger_sha256": sorted(
            str(row["trade_ledger_sha256"]) for row in canonical_membership
        ),
        "histograms": {
            "market": _histogram(canonical_membership, "market"),
            "session": _histogram(canonical_membership, "session"),
            "timeframe": _histogram(canonical_membership, "timeframe"),
            "holding_bars": _histogram(canonical_membership, "holding_bars"),
        },
    }

    parts, source_parts = _discover_parts(evidence_dataset_root)
    observations = _load_observations(parts)
    pairwise = [
        _pair_row(left_id, right_id, observations)
        for left_id, right_id in combinations(FROZEN_BOOK_IDS, 2)
    ]
    suppressions = []
    for policy_id in FROZEN_BOOK_IDS:
        value = finalist_by_id[policy_id]["suppression"]
        suppressions.append(
            {
                "policy_id": policy_id,
                "signals_emitted": int(value["signals_emitted"]),
                "signals_accepted": int(value["signals_accepted"]),
                "signals_rejected": int(value["signals_rejected"]),
                "size_reduced": int(value["size_reduced"]),
                "conflict_rejected": int(value["conflict_rejected"]),
                "contract_limit_rejected": int(value["contract_limit_rejected"]),
                "mll_risk_rejected": int(value["mll_risk_rejected"]),
                "foregone_realized_pnl_ex_post_usd": float(
                    value["foregone_realized_pnl_ex_post"]
                ),
                "foregone_realized_pnl_used_for_routing": bool(
                    value["foregone_realized_pnl_used_for_routing"]
                ),
            }
        )

    audit: dict[str, Any] = {
        "schema": SCHEMA,
        "campaign_id": str(report["campaign_id"]),
        "research_only": True,
        "book_semantics": "SIX_ALTERNATIVE_GOVERNORS_OF_ONE_SHARED_18_SLEEVE_BOOK",
        "scope": {
            "stages": list(STAGES),
            "horizon": HORIZON,
            "cost_scenarios": ["NORMAL", "STRESSED"],
            "episode_day_unit": "OVERLAPPING_ROLLING_START_EPISODE_DAY;NOT_INDEPENDENT_OBSERVATION",
            "correlation": "PEARSON_ON_COMMON_STAGE_SCENARIO_START_TRADING_DAY_KEYS",
        },
        "source": {
            "selection_path": str(selection_path),
            "selection_manifest_hash": str(selection["selection_manifest_hash"]),
            "decision_report_path": str(decision_report_path),
            "decision_report_hash": str(report["report_hash"]),
            "evidence_dataset_root": str(evidence_dataset_root),
            "parts": source_parts,
        },
        "frozen_book_ids": list(FROZEN_BOOK_IDS),
        "inventory": inventory,
        "pairwise_redundancy": pairwise,
        "suppressions": suppressions,
        "roles": _role_rows(selected_by_id, finalist_by_id),
        "conclusion": {
            "independent_alpha_book_count_claimed": 1,
            "governor_alternative_count": 6,
            "all_books_share_source_alpha": True,
            "complete_books_must_not_be_stacked": True,
        },
    }
    audit["audit_hash"] = stable_hash(audit)
    validate_redundancy_audit(audit)
    return audit


def write_atomic(path: Path, value: Mapping[str, Any]) -> None:
    validate_redundancy_audit(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--selection",
        type=Path,
        default=Path(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/"
            "frozen_book_selection_revision_02.json"
        ),
    )
    parser.add_argument(
        "--decision-report",
        type=Path,
        default=Path(
            "reports/economic_evolution/active_risk_pool_target_velocity_0026_revision_02/"
            "decision_report_revision_02.json"
        ),
    )
    parser.add_argument(
        "--evidence-dataset-root",
        type=Path,
        default=Path(
            "data/cache/evidence_bundles/"
            "hydra_active_risk_pool_target_velocity_0026.evidence-v1/"
            "datasets/account_daily_paths"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/operating/hydra_operating_package_v1/redundancy_audit.json"),
    )
    args = parser.parse_args()
    root = args.root.resolve()
    resolve = lambda path: path if path.is_absolute() else root / path
    audit = build_redundancy_audit(
        selection_path=resolve(args.selection),
        decision_report_path=resolve(args.decision_report),
        evidence_dataset_root=resolve(args.evidence_dataset_root),
    )
    output = resolve(args.output)
    write_atomic(output, audit)
    print(
        json.dumps(
            {
                "output": str(output),
                "audit_hash": audit["audit_hash"],
                "pair_count": len(audit["pairwise_redundancy"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
