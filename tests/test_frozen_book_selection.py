from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.production.active_risk_report_seal import (
    seal_active_risk_decision_report,
)
from hydra.production.frozen_book_selection import (
    FrozenBookSelectionError,
    SELECTION_JSON_NAME,
    build_frozen_book_selection,
    seal_frozen_book_selection,
    stable_hash,
    verify_frozen_book_selection_seal,
)


def _distribution(value: float) -> dict[str, float | int]:
    return {
        "count": 48,
        "minimum": value,
        "p25": value,
        "median": value,
        "p75": value,
        "maximum": value,
        "mean": value,
    }


def _scenario(index: int, *, stressed: bool) -> dict:
    diverse = index == 1
    pass_blocks = {"B1", "B2", "B3", "B4"} if diverse else {"B4"}
    passes_per_block = 2 if diverse else max(1, 40 - index)
    blocks = {
        block_id: {
            "episode_count": 48,
            "pass_count": passes_per_block if block_id in pass_blocks else 0,
            "net_pnl": _distribution(100.0 + index),
            "target_progress": _distribution(0.30 + index / 100.0),
        }
        for block_id in ("B1", "B2", "B3", "B4")
    }
    # Candidate 0 deliberately has the highest aggregate pass count, but only
    # one contributing block.  Candidate 1 must outrank it on block diversity.
    pass_count = sum(block["pass_count"] for block in blocks.values())
    return {
        "episode_count": 192,
        "pass_count": pass_count,
        "net_total": 100_000.0 + index * 1_000.0 - (5_000.0 if stressed else 0.0),
        "target_progress_p25": 0.30 + index / 100.0,
        "minimum_mll_buffer": 3_500.0 + index,
        "mll_breach_rate": 0.0,
        "consistency_rate": 0.95,
        "block_evidence_exact": blocks,
        "day_concentration_exact": {
            "maximum_positive_session_day_aggregate_share": 0.10 + index / 1000.0
        },
        "concentration": {
            "maximum_block_positive_profit_share": 0.30,
            "maximum_sleeve_positive_profit_share": 0.20,
            "maximum_market_positive_profit_share": 0.40,
            "trade_concentration": {
                "maximum_positive_source_trade_observation_share": 0.08,
                "maximum_positive_single_account_trade_observation_share": 0.09,
            },
        },
    }


def _path(index: int, path: str) -> dict:
    started = 120 + index
    first_payouts = started - 1
    post_payout_survival = started - 2
    payout = 1_000.0 + index * 10.0 + (20.0 if path == "standard" else 0.0)
    return {
        "combine_attempts": 192,
        "xfa_paths_started": started,
        "first_payouts": first_payouts,
        "payout_cycles": started * 2,
        "closure_before_first_payout_count": 1,
        "post_payout_survival_count": post_payout_survival,
        "trader_net_payout": payout * 192,
        "unconditional_lower_bound": {
            "combine_pass_probability": started / 192,
            "first_payout_probability_conditional_on_combine_pass": (
                first_payouts / started
            ),
            "first_payout_probability_per_combine_attempt": first_payouts / 192,
            "expected_trader_payout_per_combine_attempt": payout,
            "post_payout_survival_probability_conditional_on_first_payout": (
                post_payout_survival / first_payouts
            ),
            "denominators": {
                "combine_attempts": 192,
                "xfa_paths_started": started,
                "first_payout_paths": first_payouts,
            },
        },
    }


def _control_delta(index: int) -> dict:
    return {
        control: {
            scenario: {
                "target_progress_p25": 0.01 + index / 1000.0,
                "net_total": 100.0 + index,
                "pass_rate": 0.01,
            }
            for scenario in ("normal", "stressed")
        }
        for control in (
            "static_partition",
            "best_individual_sleeve",
            "equal_risk_active_pool",
            "matched_random_priority",
        )
    }


def _row(index: int) -> dict:
    policy_id = f"candidate-{index}"
    cumulative_fingerprint = stable_hash(
        {"candidate": policy_id, "cumulative_raw_behavior": True}
    )
    cumulative_behavior = {
        "scope": (
            "EXACT_CUMULATIVE_STAGE3_PLUS_STAGE4_PLUS_STAGE5_CANONICAL_90_DAY_"
            "ACCOUNT_TRAJECTORY_ROUTING_SUPPRESSION_AND_ADMITTED_TRADES"
        ),
        "fingerprint_schema": (
            "hydra_expanded_finalist_cumulative_account_trade_behavior_v1"
        ),
        "feature_schema": (
            "hydra_expanded_finalist_cumulative_behavior_features_v1"
        ),
        "authoritative_raw_account_trade_behavior_fingerprint": (
            cumulative_fingerprint
        ),
        "runtime_legacy_cumulative_behavior_fingerprint_rederived": stable_hash(
            {"candidate": policy_id, "runtime": True}
        ),
        "runtime_stage_behavior_fingerprints_rederived": {
            stage: stable_hash({"candidate": policy_id, "stage": stage})
            for stage in ("stage3", "stage4", "stage5")
        },
        "observation_count": 384,
        "per_scenario_observation_count": {"normal": 192, "stressed": 192},
        "stage_start_counts": {
            "stage3": {"normal": 48, "stressed": 48},
            "stage4": {"normal": 48, "stressed": 48},
            "stage5": {"normal": 96, "stressed": 96},
        },
        "episode_key_sha256": stable_hash(
            {"shared_finalist_episode_keys": True, "count": 384}
        ),
        "feature_width_per_episode": 5,
        "feature_vector_sha256": stable_hash(
            {"candidate": policy_id, "features": True}
        ),
        "routing_decision_tuple_count": 100,
        "routing_decision_tuple_sha256": stable_hash(
            {"candidate": policy_id, "routing": True}
        ),
        "admitted_trade_tuple_count": 80,
        "admitted_trade_tuple_sha256": stable_hash(
            {"candidate": policy_id, "trades": True}
        ),
        "full_daily_account_trajectory_bound": True,
        "emitted_routing_and_suppression_bound": True,
        "admitted_source_trade_contribution_bound": True,
        "policy_id_excluded_from_behavior_fingerprint": True,
    }
    return {
        "policy_id": policy_id,
        "structural_fingerprint": f"structural-{index}",
        "starts_per_scenario": 192,
        "effective_independent_source_block_count": 4,
        "source_block_ids": ["B1", "B2", "B3", "B4"],
        "expanded_exact_account_behavior_cluster": f"exact-{index}",
        "expanded_economic_behavior_cluster": "PENDING_TEST_CLUSTER",
        "stage3_posthoc_behavioral_cluster": f"stage3-{index % 4}",
        "sealed_cumulative_account_behavior_fingerprint": cumulative_fingerprint,
        "authoritative_cumulative_account_trade_behavior_fingerprint": (
            cumulative_fingerprint
        ),
        "legacy_frontier_behavior_fingerprint_rederived_exactly": True,
        "cumulative_account_trade_behavior": cumulative_behavior,
        "normal": _scenario(index, stressed=False),
        "stressed": _scenario(index, stressed=True),
        "horizons": {
            scenario: {
                "FULL_CHRONOLOGICAL_HORIZON": {
                    "episode_count": 192,
                    "pass_count": 120 + index,
                }
            }
            for scenario in ("normal", "stressed")
        },
        "stage3_matched_control_deltas": {
            "scope": "STAGE3_ONLY_48_MATCHED_STARTS",
            "matched_starts_per_scenario": 48,
            "expanded_192_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
            "deltas": _control_delta(index),
        },
        "expanded_standard_consistency_xfa_lifecycle_exact": {
            scenario: {
                path: _path(index, path)
                for path in ("standard", "consistency")
            }
            for scenario in ("normal", "stressed")
        }
        | {"paths_are_alternative_not_additive": True},
    }


def _economic_clustering(
    rows: list[dict], group_labels: dict[str, str] | None = None
) -> dict:
    groups: dict[str, list[str]] = {}
    labels: dict[str, str] = {}
    for row in rows:
        label = (group_labels or {}).get(row["policy_id"], row["policy_id"])
        labels[row["policy_id"]] = label
        groups.setdefault(label, []).append(row["policy_id"])
    by_id = {row["policy_id"]: row for row in rows}
    cluster_rows = []
    membership = {}
    for members in groups.values():
        members = sorted(members)
        cluster_id = "expanded_economic_behavior_" + stable_hash(
            {
                "members": members,
                "fingerprints": [
                    by_id[member]["cumulative_account_trade_behavior"][
                        "authoritative_raw_account_trade_behavior_fingerprint"
                    ]
                    for member in members
                ],
            }
        )[:20]
        representative = min(
            members,
            key=lambda member: (
                -by_id[member]["stressed"]["pass_count"] / 192,
                -by_id[member]["stressed"]["target_progress_p25"],
                -by_id[member]["stressed"]["net_total"],
                by_id[member]["stressed"]["mll_breach_rate"],
                member,
            ),
        )
        for member in members:
            by_id[member]["expanded_economic_behavior_cluster"] = cluster_id
            membership[member] = cluster_id
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "member_ids": members,
                "member_count": len(members),
                "representative_id": representative,
                "minimum_pair_correlation": 1.0 if len(members) == 1 else 0.999,
                "maximum_pair_rmse": 0.0 if len(members) == 1 else 0.01,
                "minimum_terminal_agreement": 1.0,
                "minimum_routing_jaccard": 1.0 if len(members) == 1 else 0.95,
                "minimum_admitted_trade_jaccard": (
                    1.0 if len(members) == 1 else 0.95
                ),
                "complete_link_thresholds": {
                    "minimum_account_vector_correlation": 0.995,
                    "maximum_account_vector_rmse": 0.05,
                    "minimum_terminal_agreement": 0.95,
                    "minimum_routing_jaccard": 0.90,
                    "minimum_admitted_trade_jaccard": 0.90,
                },
            }
        )
    candidate_ids = sorted(by_id)
    pairwise = []
    for index, left in enumerate(candidate_ids):
        for right in candidate_ids[index + 1 :]:
            similar = labels[left] == labels[right]
            pairwise.append(
                {
                    "left_policy_id": left,
                    "right_policy_id": right,
                    "account_vector_correlation": 0.999 if similar else 0.90,
                    "account_vector_rmse": 0.01 if similar else 0.10,
                    "terminal_agreement": 1.0 if similar else 0.80,
                    "routing_jaccard": 0.95 if similar else 0.80,
                    "admitted_trade_jaccard": 0.95 if similar else 0.80,
                    "similar": similar,
                }
            )
    partition = sorted(
        (sorted(members) for members in groups.values()),
        key=lambda members: tuple(members),
    )
    thresholds = {
        "minimum_account_vector_correlation": 0.995,
        "maximum_account_vector_rmse": 0.05,
        "minimum_terminal_agreement": 0.95,
        "minimum_routing_jaccard": 0.90,
        "minimum_admitted_trade_jaccard": 0.90,
    }
    return {
        "scope": (
            "CUMULATIVE_192_STARTS_PER_SCENARIO_STAGE3_48_PLUS_STAGE4_48_"
            "PLUS_STAGE5_96_ACCOUNT_TRAJECTORY_ROUTING_SUPPRESSION_AND_"
            "ADMITTED_TRADE_BEHAVIOR"
        ),
        "algorithm": "DETERMINISTIC_COMPLETE_LINK_FIXED_THRESHOLDS_V1",
        "complete_link_thresholds": thresholds,
        "cluster_count": len(cluster_rows),
        "clusters": sorted(cluster_rows, key=lambda value: value["cluster_id"]),
        "membership": dict(sorted(membership.items())),
        "pairwise_diagnostics": pairwise,
        "pairwise_diagnostic_count": len(pairwise),
        "expected_pairwise_diagnostic_count": 28,
        "pairwise_diagnostics_sha256": stable_hash(pairwise),
        "pairwise_coverage_complete": True,
        "pairwise_similarity_decisions_metric_rederived": True,
        "complete_link_partition_sha256": stable_hash(partition),
        "complete_link_partition_rederived_from_published_pairwise": True,
        "source_signal_or_trade_ledger_summary_only": False,
        "overlapping_starts_claimed_independent": False,
        "full_192_start_contract_satisfied": True,
    }


def _spec(index: int) -> dict:
    policy_id = f"candidate-{index}"
    policy = {
        "policy_id": policy_id,
        "structural_fingerprint": f"structural-{index}",
        "maximum_concurrent_sleeves": 2 + index % 2,
        "concurrency_scaling": "PRIORITY",
        "same_instrument_conflict_rule": "PRIORITY",
        "target_protection_mode": "NONE",
    }
    membership = [{"component_id": "sleeve-a", "immutable_fingerprint": "a" * 64}]
    combine = {
        "book": "COMBINE_BOOK",
        "policy_id": policy_id,
        "book_frozen_before_outcomes": True,
    }
    standard = {
        "book": "XFA_STANDARD_BOOK",
        "policy_id": policy_id,
        "book_frozen_before_outcomes": True,
    }
    consistency = {
        "book": "XFA_CONSISTENCY_BOOK",
        "policy_id": policy_id,
        "book_frozen_before_outcomes": True,
    }
    return {
        "policy_id": policy_id,
        "structural_fingerprint": f"structural-{index}",
        "membership_row_count": 1,
        "membership_rows_all_contain_identical_policy": True,
        "active_risk_policy": policy,
        "active_risk_policy_sha256": stable_hash(policy),
        "membership": membership,
        "membership_sha256": stable_hash(membership),
        "combine_book": combine,
        "combine_book_sha256": stable_hash(combine),
        "xfa_standard_book": standard,
        "xfa_standard_book_sha256": stable_hash(standard),
        "xfa_consistency_book": consistency,
        "xfa_consistency_book_sha256": stable_hash(consistency),
    }


def _sealed_report(tmp_path: Path) -> Path:
    rows = [_row(index) for index in range(8)]
    specs = [_spec(index) for index in range(8)]
    clustering = _economic_clustering(rows)
    report = {
        "schema": "hydra_active_risk_decision_report_v1",
        "revision": "revision_02",
        "campaign_id": "hydra_active_risk_pool_target_velocity_0026",
        "development_only": True,
        "promotion_or_selection_mutated": False,
        "integrity": {
            "expanded_finalist_decision_metrics_rederived_from_raw_caches": True,
            "expanded_finalist_runtime_behavior_merge_hash_rederived": True,
            "expanded_finalist_authoritative_account_trade_behavior_rederived": True,
            "expanded_finalist_cumulative_economic_behavior_clusters_rederived": True,
        },
        "expanded_development_finalists": {
            "finalist_count": 8,
            "rows": rows,
            "cumulative_192_economic_behavioral_clustering": clustering,
            "cumulative_192_economic_behavior_cluster_count": clustering[
                "cluster_count"
            ],
            "controls_scope": "STAGE3_ONLY_48_STARTS",
            "expanded_matched_controls_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
            "independent_evidence_status": "NOT_INDEPENDENT_CONFIRMATION",
        },
        "frozen_finalist_policy_specs": {
            "finalist_count": 8,
            "policy_specs": specs,
        },
        "campaign_wide_sealed_xfa_lifecycle_totals": {
            "transition_and_alternative_path_audit": {
                "alternative_path_multiplier": 2,
                "transition_to_alternative_path_identity_valid": True,
                "first_payout_observations_within_alternative_path_bound": True,
                "duplicate_transition_inflation_detected": False,
                "first_payout_observations_are_combine_to_xfa_transitions": False,
            }
        },
    }
    report["report_hash"] = stable_hash(report)
    report_dir = tmp_path / "report"
    seal_active_risk_decision_report(
        report,
        markdown_text=f"# sealed\n\n{report['report_hash']}\n",
        output_dir=report_dir,
    )
    return report_dir


def _reseal_mutated_report(
    tmp_path: Path, report: dict, *, name: str
) -> Path:
    report.pop("report_hash", None)
    report["report_hash"] = stable_hash(report)
    output = tmp_path / name
    seal_active_risk_decision_report(
        report,
        markdown_text=report["report_hash"],
        output_dir=output,
    )
    return output


def test_selects_five_primaries_and_distinct_backup_from_sealed_report(
    tmp_path: Path,
) -> None:
    report_dir = _sealed_report(tmp_path)
    selection = build_frozen_book_selection(
        report_dir, selection_completed_at_utc="2026-07-15T15:00:00Z"
    )
    assert selection["primary_count"] == 5
    assert selection["backup_count"] == 1
    assert selection["paper_shadow_ready_assigned"] is False
    assert selection["control_contract"] == {
        "scope": "STAGE3_ONLY_48_STARTS",
        "expanded_192_status": "192_CONTROL_SUPERIORITY_NOT_ESTABLISHED",
        "control_superiority_used_at_192": False,
    }
    selected = selection["selected_books"]
    assert len(selected) == 6
    assert len({row["expanded_economic_behavior_cluster"] for row in selected}) == 6
    assert sum(row["selection_role"] == "PRIMARY" for row in selected) == 5
    assert sum(row["selection_role"] == "BACKUP" for row in selected) == 1
    assert all(row["status"] == "FORWARD_SHADOW_CANDIDATE" for row in selected)
    assert all(stable_hash({k: v for k, v in row.items() if k != "entry_hash"}) == row["entry_hash"] for row in selected)
    assert selection["selection_manifest_hash"] == stable_hash(
        {k: v for k, v in selection.items() if k != "selection_manifest_hash"}
    )


def test_block_diversity_precedes_aggregate_pass_count(tmp_path: Path) -> None:
    selection = build_frozen_book_selection(
        _sealed_report(tmp_path),
        selection_completed_at_utc="2026-07-15T15:00:00Z",
    )
    representatives = selection["cluster_representatives"]
    assert representatives.index("candidate-1") < representatives.index("candidate-0")
    assert selection["ranking_policy"]["aggregate_pass_count_primary_rank"] is False
    assert selection["ranking_policy"][
        "standard_and_consistency_cash_aggregated_as_realisable"
    ] is False


def test_cumulative_economic_cluster_keeps_at_most_one_representative(
    tmp_path: Path,
) -> None:
    report_dir = _sealed_report(tmp_path)
    report_path = report_dir / "decision_report_revision_02.json"
    # Recreate a separate valid seal with two cumulative-economic equivalents.
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rows = report["expanded_development_finalists"]["rows"]
    clustering = _economic_clustering(
        rows,
        group_labels={
            **{f"candidate-{index}": f"candidate-{index}" for index in range(7)},
            "candidate-7": "candidate-6",
        },
    )
    report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ] = clustering
    report["expanded_development_finalists"][
        "cumulative_192_economic_behavior_cluster_count"
    ] = clustering["cluster_count"]
    report.pop("report_hash")
    report["report_hash"] = stable_hash(report)
    equivalent_dir = tmp_path / "equivalent"
    seal_active_risk_decision_report(
        report,
        markdown_text=report["report_hash"],
        output_dir=equivalent_dir,
    )
    selection = build_frozen_book_selection(
        equivalent_dir, selection_completed_at_utc="2026-07-15T15:00:00Z"
    )
    assert selection["cumulative_economic_behavior_cluster_count"] == 7
    selected_ids = {row["policy_id"] for row in selection["selected_books"]}
    assert len({"candidate-6", "candidate-7"} & selected_ids) <= 1


def test_fails_closed_when_not_exactly_eight_finalists(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"].pop()
    report["expanded_development_finalists"]["finalist_count"] = 7
    report.pop("report_hash")
    report["report_hash"] = stable_hash(report)
    bad = tmp_path / "bad"
    seal_active_risk_decision_report(report, markdown_text=report["report_hash"], output_dir=bad)
    with pytest.raises(FrozenBookSelectionError, match="expanded finalist"):
        build_frozen_book_selection(
            bad, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_atomic_receipt_binds_report_and_selection(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    manifest = build_frozen_book_selection(
        report_dir, selection_completed_at_utc="2026-07-15T15:00:00Z"
    )
    output = tmp_path / "selection"
    receipt = seal_frozen_book_selection(
        manifest, report_dir=report_dir, output_dir=output
    )
    assert receipt["selection_manifest_hash"] == manifest["selection_manifest_hash"]
    assert receipt["sealed_at_utc"].endswith("Z")
    assert verify_frozen_book_selection_seal(output, report_dir=report_dir) == receipt
    payload = json.loads((output / SELECTION_JSON_NAME).read_text())
    payload["paper_shadow_ready_assigned"] = True
    (output / SELECTION_JSON_NAME).write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(FrozenBookSelectionError, match="binding drift"):
        verify_frozen_book_selection_seal(output, report_dir=report_dir)


def test_rejects_cumulative_cluster_scope_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ]["scope"] = "CUMULATIVE_AVAILABLE_FROZEN_STARTS"
    tampered = _reseal_mutated_report(tmp_path, report, name="scope-tamper")
    with pytest.raises(FrozenBookSelectionError, match="clustering contract"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_raw_cumulative_fingerprint_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0][
        "cumulative_account_trade_behavior"
    ]["authoritative_raw_account_trade_behavior_fingerprint"] = "f" * 64
    tampered = _reseal_mutated_report(tmp_path, report, name="fingerprint-tamper")
    with pytest.raises(FrozenBookSelectionError, match="behavior identity drift"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_identical_cumulative_fingerprint_split(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    rows = report["expanded_development_finalists"]["rows"]
    fingerprint = rows[0]["sealed_cumulative_account_behavior_fingerprint"]
    rows[1]["sealed_cumulative_account_behavior_fingerprint"] = fingerprint
    rows[1]["authoritative_cumulative_account_trade_behavior_fingerprint"] = fingerprint
    rows[1]["cumulative_account_trade_behavior"][
        "authoritative_raw_account_trade_behavior_fingerprint"
    ] = fingerprint
    clustering = _economic_clustering(rows)  # Deliberately leaves them split.
    report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ] = clustering
    report["expanded_development_finalists"][
        "cumulative_192_economic_behavior_cluster_count"
    ] = clustering["cluster_count"]
    tampered = _reseal_mutated_report(tmp_path, report, name="split-tamper")
    with pytest.raises(FrozenBookSelectionError, match="split across"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_block_episode_reconciliation_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0]["stressed"][
        "block_evidence_exact"
    ]["B1"]["episode_count"] = 47
    tampered = _reseal_mutated_report(tmp_path, report, name="block-tamper")
    with pytest.raises(FrozenBookSelectionError, match="block counts"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_aggregate_pass_reconciliation_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0]["normal"][
        "pass_count"
    ] += 1
    tampered = _reseal_mutated_report(tmp_path, report, name="aggregate-tamper")
    with pytest.raises(FrozenBookSelectionError, match="block counts"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_xfa_transition_denominator_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0][
        "expanded_standard_consistency_xfa_lifecycle_exact"
    ]["stressed"]["standard"]["xfa_paths_started"] -= 1
    tampered = _reseal_mutated_report(tmp_path, report, name="xfa-tamper")
    with pytest.raises(FrozenBookSelectionError, match="XFA transitions"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_divergent_cumulative_episode_key_hash(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][1][
        "cumulative_account_trade_behavior"
    ]["episode_key_sha256"] = "e" * 64
    tampered = _reseal_mutated_report(tmp_path, report, name="episode-key-tamper")
    with pytest.raises(FrozenBookSelectionError, match="episode-key"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_xfa_expected_payout_ranking_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0][
        "expanded_standard_consistency_xfa_lifecycle_exact"
    ]["stressed"]["standard"]["unconditional_lower_bound"][
        "expected_trader_payout_per_combine_attempt"
    ] += 1.0
    tampered = _reseal_mutated_report(tmp_path, report, name="xfa-ev-tamper")
    with pytest.raises(FrozenBookSelectionError, match="denominator arithmetic"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_xfa_survival_ranking_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    report["expanded_development_finalists"]["rows"][0][
        "expanded_standard_consistency_xfa_lifecycle_exact"
    ]["normal"]["consistency"]["unconditional_lower_bound"][
        "post_payout_survival_probability_conditional_on_first_payout"
    ] = 1.0
    tampered = _reseal_mutated_report(tmp_path, report, name="xfa-survival-tamper")
    with pytest.raises(FrozenBookSelectionError, match="denominator arithmetic"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_pairwise_metric_to_similarity_tamper(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    clustering = report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ]
    pair = clustering["pairwise_diagnostics"][0]
    pair.update(
        {
            "account_vector_correlation": 0.999,
            "account_vector_rmse": 0.01,
            "terminal_agreement": 1.0,
            "routing_jaccard": 0.95,
            "admitted_trade_jaccard": 0.95,
            "similar": False,
        }
    )
    clustering["pairwise_diagnostics_sha256"] = stable_hash(
        clustering["pairwise_diagnostics"]
    )
    tampered = _reseal_mutated_report(tmp_path, report, name="pair-similar-tamper")
    with pytest.raises(FrozenBookSelectionError, match="similarity decision"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_missing_pairwise_diagnostic_even_when_rehashed(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    clustering = report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ]
    clustering["pairwise_diagnostics"].pop()
    clustering["pairwise_diagnostic_count"] = 27
    clustering["pairwise_diagnostics_sha256"] = stable_hash(
        clustering["pairwise_diagnostics"]
    )
    tampered = _reseal_mutated_report(tmp_path, report, name="pair-missing-tamper")
    with pytest.raises(FrozenBookSelectionError, match="matrix count"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )


def test_rejects_arbitrary_split_against_pairwise_complete_link(tmp_path: Path) -> None:
    report_dir = _sealed_report(tmp_path)
    report = json.loads((report_dir / "decision_report_revision_02.json").read_text())
    clustering = report["expanded_development_finalists"][
        "cumulative_192_economic_behavioral_clustering"
    ]
    pair = clustering["pairwise_diagnostics"][0]
    pair.update(
        {
            "account_vector_correlation": 0.999,
            "account_vector_rmse": 0.01,
            "terminal_agreement": 1.0,
            "routing_jaccard": 0.95,
            "admitted_trade_jaccard": 0.95,
            "similar": True,
        }
    )
    clustering["pairwise_diagnostics_sha256"] = stable_hash(
        clustering["pairwise_diagnostics"]
    )
    derived_partition = [
        ["candidate-0", "candidate-1"],
        *[[f"candidate-{index}"] for index in range(2, 8)],
    ]
    clustering["complete_link_partition_sha256"] = stable_hash(derived_partition)
    # Rehash the derived pairwise partition but deliberately retain split
    # published cluster rows and membership.
    tampered = _reseal_mutated_report(tmp_path, report, name="pair-split-tamper")
    with pytest.raises(FrozenBookSelectionError, match="published economic clusters"):
        build_frozen_book_selection(
            tampered, selection_completed_at_utc="2026-07-15T15:00:00Z"
        )
