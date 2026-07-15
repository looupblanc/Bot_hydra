from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from hydra.evidence import RECORD_SPECS
from hydra.production.portfolio_books import BookPair, SleeveRecord, stable_hash
from hydra.production.portfolio_runtime import (
    PortfolioFirstRun,
    PortfolioRuntimeError,
    _assert_authoritative_episode_counters,
    _combine_stage_metrics,
    _compact_combine_metric,
    _development_finalist_roles,
    _halving_stage_name,
    _lifecycle_account_behavior_fingerprint,
    _merge_summary,
    _metrics_excluding_pairs,
    _portfolio_lifecycle_evidence,
    _portfolio_membership_rows,
    _portfolio_provenance_checksums,
    _quantile,
    _select_pairs,
)


def test_quantile_is_deterministic() -> None:
    assert _quantile([0.0, 1.0, 2.0, 3.0], 0.25) == 0.75


def test_merge_summary_reconciles_passes_and_starts() -> None:
    base = {
        "episode_count": 48,
        "pass_count": 5,
        "pass_rate": 5 / 48,
        "net_total": 100.0,
        "net_median": 2.0,
        "target_progress_median": 0.5,
        "target_progress_p25": 0.2,
        "maximum_target_progress": 1.0,
        "mll_breach_rate": 0.0,
        "minimum_mll_buffer": 1000.0,
        "consistency_rate": 0.8,
        "pass_block_count": 2,
        "maximum_sleeve_profit_share": 0.4,
    }
    merged = _merge_summary(base, {**base, "pass_count": 3})
    assert merged["episode_count"] == 96
    assert merged["pass_count"] == 8
    assert merged["pass_rate"] == 8 / 96


def _lifecycle_metric(*, payout: float, payouts: int, survived: int) -> dict:
    scenario = {
        "episode_count": 48,
        "pass_count": 5,
        "pass_rate": 5 / 48,
        "net_total": 100.0,
        "net_median": 2.0,
        "net_values": [2.0] * 48,
        "target_progress_median": 0.5,
        "target_progress_p25": 0.2,
        "target_progress_values": [0.5] * 48,
        "maximum_target_progress": 1.0,
        "mll_breach_rate": 0.0,
        "mll_breach_count": 0,
        "minimum_mll_buffer": 1000.0,
        "consistency_rate": 1.0,
        "consistency_ok_count": 48,
        "pass_block_count": 2,
        "pass_block_ids": ["B1", "B2"],
        "by_block_net": {"B1": 50.0, "B2": 50.0},
        "component_contribution": {"s1": 60.0, "s2": 40.0},
        "maximum_block_profit_share": 0.5,
        "maximum_sleeve_profit_share": 0.6,
    }
    path = {
        "path_count": 5,
        "first_payouts": payouts,
        "payout_cycles": payouts,
        "trader_net_payout": payout,
        "post_payout_survived_count": survived,
        "post_payout_survival_rate": survived / payouts if payouts else 0.0,
    }
    return {
        "pair_id": "pair-1",
        "normal": dict(scenario),
        "stressed": dict(scenario),
        "combine_episode_count": 96,
        "normal_combine_passes": 5,
        "stressed_combine_passes": 5,
        "xfa_paths_started": 10,
        "xfa_standard_paths": 10,
        "xfa_consistency_paths": 10,
        "first_payouts": payouts * 4,
        "payout_cycles": payouts * 4,
        "trader_net_payout": payout * 4,
        "ranking_trader_net_payout": payout,
        "expected_trader_net_payout_per_attempt": payout / 48,
        "post_payout_survival_rate": path["post_payout_survival_rate"],
        "path_metrics": {
            scenario_name: {
                path_name: dict(path)
                for path_name in ("STANDARD", "CONSISTENCY")
            }
            for scenario_name in ("NORMAL", "STRESSED_1_5X")
        },
        "ranking_path": "STRESSED_1_5X:XFA_STANDARD",
        "failure_vectors": [],
    }


def test_lifecycle_batch_merge_preserves_float_payout_and_weighted_survival() -> None:
    first = _lifecycle_metric(payout=125.5, payouts=2, survived=1)
    second = _lifecycle_metric(payout=200.25, payouts=3, survived=3)

    merged = _combine_stage_metrics([first], [second])[0]

    assert merged["ranking_trader_net_payout"] == 325.75
    assert merged["expected_trader_net_payout_per_attempt"] == 325.75 / 96
    assert merged["post_payout_survival_rate"] == 4 / 5
    assert (
        merged["path_metrics"]["STRESSED_1_5X"]["STANDARD"][
            "post_payout_survived_count"
        ]
        == 4
    )


def test_lifecycle_batch_merge_recomputes_transition_probabilities_and_payout_time() -> None:
    first = _lifecycle_metric(payout=125.5, payouts=2, survived=1)
    second = _lifecycle_metric(payout=200.25, payouts=3, survived=3)
    for value, attempts, passes, days in (
        (first, 48, 5, [3, 7]),
        (second, 48, 5, [4, 6, 8]),
    ):
        for scenario in ("NORMAL", "STRESSED_1_5X"):
            for path in ("STANDARD", "CONSISTENCY"):
                metric = value["path_metrics"][scenario][path]
                metric["combine_attempt_count"] = attempts
                metric["combine_pass_count"] = passes
                metric["first_payout_day_values"] = list(days)

    merged = _combine_stage_metrics([first], [second])[0]
    metric = merged["path_metrics"]["STRESSED_1_5X"]["STANDARD"]

    assert metric["combine_attempt_count"] == 96
    assert metric["combine_pass_count"] == 10
    assert metric["path_count"] == 10
    assert metric["first_payouts"] == 5
    assert metric["xfa_entry_probability"] == pytest.approx(10 / 96)
    assert metric["first_payout_probability_conditional_on_combine_pass"] == 0.5
    assert metric["first_payout_probability_unconditional"] == pytest.approx(5 / 96)
    assert metric["median_trading_days_to_first_payout"] == 6
    assert metric["expected_payout_cycles_per_combine_attempt"] == pytest.approx(5 / 96)


def test_halving_stage_names_are_v17_resume_compatible() -> None:
    assert (
        _halving_stage_name("stage3")
        == "STAGE_3_48_START_COMBINE_TO_XFA_LIFECYCLE"
    )
    assert _halving_stage_name("stage4") == "STAGE_4_EXPANDED_96_STARTS"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sleeve(sleeve_id: str) -> SleeveRecord:
    return SleeveRecord(
        sleeve_id=sleeve_id,
        immutable_fingerprint=_sha(f"immutable:{sleeve_id}"),
        behavioral_fingerprint=_sha(f"behavior:{sleeve_id}"),
        signal_ledger_sha256=_sha(f"source-signal:{sleeve_id}"),
        trade_ledger_sha256=_sha(f"source-trade:{sleeve_id}"),
        market="ES",
        contract="MES",
        timeframe="15m",
        session="OPEN",
        economic_role="TARGET_VELOCITY",
        source_campaign="hydra_economic_production_0024",
        family_id="source-family",
    )


def test_membership_rows_reconstruct_both_books_without_max_allocation_loss() -> None:
    sleeves = (_sleeve("sleeve-a"), _sleeve("sleeve-b"))
    pair = BookPair.create(
        combine_sleeves=sleeves,
        combine_allocation_units=(1, 3),
        combine_risk_tier=1.15,
        xfa_sleeves=(sleeves[1],),
        xfa_allocation_units=(2,),
        xfa_risk_tier=0.75,
        conflict_policy="PRIORITY",
        behaviorally_novel=True,
        generator_seed=25,
        proposal_index=0,
    )

    rows = _portfolio_membership_rows("portfolio-campaign", (pair,), sleeves)
    declarations = {
        (
            tuple(row["combine_book_sleeve_ids"]),
            tuple(sorted(row["combine_book_allocation_units"].items())),
            row["combine_risk_tier"],
            tuple(row["xfa_book_sleeve_ids"]),
            tuple(sorted(row["xfa_book_allocation_units"].items())),
            row["xfa_risk_tier"],
            row["conflict_policy"],
        )
        for row in rows
    }
    assert declarations == {
        (
            pair.combine_sleeve_ids,
            tuple(sorted(zip(pair.combine_sleeve_ids, (1, 3), strict=True))),
            1.15,
            pair.xfa_sleeve_ids,
            (("sleeve-b", 2),),
            0.75,
            "PRIORITY",
        )
    }
    by_component = {row["component_id"]: row for row in rows}
    assert by_component["sleeve-a"]["xfa_member"] is False
    assert by_component["sleeve-a"]["xfa_allocation_units"] is None
    assert by_component["sleeve-b"]["combine_effective_risk_multiplier"] == pytest.approx(3.45)
    assert by_component["sleeve-b"]["xfa_effective_risk_multiplier"] == 1.5


def test_provenance_names_source_and_current_bundle_ledgers_separately() -> None:
    campaign_id = "portfolio-campaign"
    sleeve = _sleeve("sleeve-a")
    signal = {
        "campaign_id": campaign_id,
        "component_id": sleeve.sleeve_id,
        "signal_id": "signal-1",
        "event_time": "2026-07-15T00:00:00Z",
        "market": "ES",
        "contract": "MES",
        "timeframe": "15m",
        "signal": 1,
        "sizing": 1.0,
        "stop": None,
        "target": None,
        "veto": False,
        "component_role": "TARGET_VELOCITY",
    }
    entry = {
        "campaign_id": campaign_id,
        "component_id": sleeve.sleeve_id,
        "trade_id": "trade-1",
        "entry_time": "2026-07-15T00:00:00Z",
        "market": "ES",
        "contract": "MES",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 100.0,
        "sizing": 1.0,
        "stop_price": None,
        "target_price": None,
    }
    exit_row = {
        "campaign_id": campaign_id,
        "component_id": sleeve.sleeve_id,
        "trade_id": "trade-1",
        "exit_time": "2026-07-15T01:00:00Z",
        "exit_price": 101.0,
        "exit_reason": "TARGET",
    }
    trade = {
        "campaign_id": campaign_id,
        "component_id": sleeve.sleeve_id,
        "trade_id": "trade-1",
        "entry_time": "2026-07-15T00:00:00Z",
        "exit_time": "2026-07-15T01:00:00Z",
        "market": "ES",
        "contract": "MES",
        "side": "LONG",
        "quantity": 1.0,
        "entry_price": 100.0,
        "exit_price": 101.0,
        "gross_pnl": 10.0,
        "costs": 1.0,
        "net_pnl": 9.0,
    }
    identity = {
        "configuration_sha256": _sha("configuration"),
        "data_fingerprints": {
            "contract_map": _sha("contract-map"),
            "data_access_ledger": _sha("access-ledger"),
        },
    }
    checksums = _portfolio_provenance_checksums(
        identity,
        {
            "component_signals": [signal],
            "component_entries": [entry],
            "component_exits": [exit_row],
            "component_trades": [trade],
        },
        (sleeve,),
        campaign_id=campaign_id,
    )

    assert checksums["data:contract_map"] == _sha("contract-map")
    assert checksums["data:data_access_ledger"] == _sha("access-ledger")
    assert checksums["source:component_signals:sleeve-a"] == (
        sleeve.signal_ledger_sha256
    )
    assert checksums["source:component_trades:sleeve-a"] == (
        sleeve.trade_ledger_sha256
    )
    assert checksums["bundle:component_signals:sleeve-a"] != (
        sleeve.signal_ledger_sha256
    )
    assert checksums["bundle:component_trades:sleeve-a"] != (
        sleeve.trade_ledger_sha256
    )


def test_raw_xfa_payout_and_post_payout_paths_are_sealed_in_episode_payload() -> None:
    def path(name: str) -> dict:
        return {
            "path": name,
            "observed_days": 2,
            "payout_cycles": 1,
            "payout_eligible": True,
            "post_payout_survived": True,
            "trader_net_payout": 450.0,
            "daily_ledger": [
                {
                    "session_day": 20,
                    "payout_requested": True,
                    "gross_payout": 500.0,
                    "trader_net_payout": 450.0,
                    "post_payout_mll_locked_at_zero": True,
                },
                {
                    "session_day": 21,
                    "payout_requested": False,
                    "post_payout_mll_locked_at_zero": True,
                },
            ],
        }

    raw = {
        "lifecycle_version": "hydra_portfolio_combine_to_xfa_v1",
        "start_day": 10,
        "combine_status": "TARGET_REACHED",
        "combine_book": {"fingerprint": _sha("combine-book")},
        "xfa_book": {"fingerprint": _sha("xfa-book")},
        "xfa_started": True,
        "xfa_start_day": 12,
        "xfa_standard": path("XFA_STANDARD"),
        "xfa_consistency": path("XFA_CONSISTENCY"),
        "rule_snapshot": {"fingerprint": _sha("rules")},
        "union_timeline_hash": _sha("timeline"),
        "evidence_hash": _sha("lifecycle"),
        "combine_profit_transferred_to_xfa": False,
        "books_frozen_before_replay": True,
        "xfa_book_selected_from_outcomes": False,
        "unrealized_aggregation_semantics": (
            "CONSERVATIVE_SUM_OF_OPEN_TRADE_EXTREMA_BOUND_V1"
        ),
    }
    source_payload = dict(raw)
    source_payload.pop("evidence_hash")
    raw["evidence_hash"] = stable_hash(source_payload)

    sealed = _portfolio_lifecycle_evidence(raw)
    assert sealed["schema"] == "hydra_portfolio_lifecycle_evidence_v1"
    assert sealed["xfa_standard"]["daily_ledger"][0]["payout_requested"] is True
    assert sealed["xfa_consistency"]["post_payout_survived"] is True
    assert sealed["combine_profit_transferred_to_xfa"] is False
    assert sealed["xfa_book_selected_from_outcomes"] is False
    assert len(sealed["sealed_lifecycle_sha256"]) == 64
    episode = {
        "campaign_id": "portfolio-campaign",
        "policy_id": "pair-1",
        "episode_id": "pair-1:10",
        "episode_start": "1970-01-11T00:00:00Z",
        "horizon": "90_TRADING_DAYS",
        "temporal_block": "B1",
        "duration_trading_days": 2,
        "target_reached": True,
        "mll_breached": False,
        "censored_state": False,
        "cost_scenario": "NORMAL",
        "costs": 1.0,
        "net_pnl": 9_000.0,
        "target_progress": 1.0,
        "minimum_mll_buffer": 1_000.0,
        "consistency_ok": True,
        "days_to_target": 2.0,
        "failure_vector": ["NO_INCREMENTAL_VALUE"],
        "terminal_state": "TARGET_REACHED",
        "portfolio_lifecycle": sealed,
    }
    assert RECORD_SPECS["episodes"].validate(
        episode, campaign_id="portfolio-campaign"
    )["portfolio_lifecycle"] == sealed

    broken = dict(raw)
    broken["xfa_standard"] = {**path("XFA_STANDARD"), "payout_cycles": 2}
    broken["evidence_hash"] = stable_hash(
        {key: value for key, value in broken.items() if key != "evidence_hash"}
    )
    with pytest.raises(PortfolioRuntimeError, match="payout ledger does not reconcile"):
        _portfolio_lifecycle_evidence(broken)


def test_existing_result_replays_pending_forward_anchor_before_terminal_reconcile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second restart must heal a crash after result write, before anchoring."""

    import hydra.production.portfolio_runtime as runtime_module

    result_path = tmp_path / "portfolio_result.json"
    result_path.write_text("{}", encoding="utf-8")
    original = {
        "economic_results": {
            "forward_shadow_anchor_receipts": [],
            "production_counters": {
                "combine_episodes_completed": 2,
                "normal_episodes_completed": 1,
                "stressed_episodes_completed": 1,
            },
        },
        "evidence_bundle": {
            "bundle_path": "/sealed/bundle",
            "manifest_sha256": _sha("manifest"),
            "bundle_content_sha256": _sha("content"),
            "dataset_row_counts": {"episodes": 2},
        },
        "scientific_status": "DEVELOPMENT_WAVE_COMPLETE",
        "result_hash": _sha("old-result"),
    }
    written: dict[str, object] = {}
    sequence: list[str] = []
    run = PortfolioFirstRun.__new__(PortfolioFirstRun)
    run.output_dir = tmp_path
    run.manifest = {"runtime": {"result_name": result_path.name}}
    run.output_writer = SimpleNamespace(
        write_json=lambda name, payload: written.update(name=name, payload=payload)
    )
    run._load_forward_package_receipts = lambda: [{"candidate_id": "candidate-1"}]
    run._anchor_forward_packages = lambda receipt: [
        {
            "candidate_id": "candidate-1",
            "evidence_bundle_manifest_sha256": receipt["manifest_sha256"],
        }
    ]
    run._reconcile_completed_result_snapshots = lambda result: sequence.append(
        str(result["economic_results"]["forward_shadow_anchor_receipts"][0]["candidate_id"])
    )

    def fake_load(path: Path, manifest: dict) -> dict:
        del path, manifest
        payload = written.get("payload")
        return dict(payload) if isinstance(payload, dict) else dict(original)

    monkeypatch.setattr(runtime_module, "load_and_verify_production_result", fake_load)

    recovered = run.execute()

    assert written["name"] == result_path.name
    assert recovered["economic_results"]["forward_shadow_anchor_receipts"] == [
        {
            "candidate_id": "candidate-1",
            "evidence_bundle_manifest_sha256": _sha("manifest"),
        }
    ]
    assert recovered["result_hash"] == stable_hash(
        {key: value for key, value in recovered.items() if key != "result_hash"}
    )
    assert recovered["scientific_status"] == "PORTFOLIO_FIRST_DEVELOPMENT_COMPLETE"
    assert sequence == ["candidate-1", "candidate-1"]


def test_hot_progress_checkpoints_do_not_double_count_publish_overhead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hydra.production.portfolio_runtime as runtime_module

    run = PortfolioFirstRun.__new__(PortfolioFirstRun)
    run.clock = SimpleNamespace(hot_seconds=0.0)
    published: list[str] = []
    run._publish = lambda **updates: published.append(str(updates["stage"]))
    ticks = iter((2.0, 5.0, 8.0, 10.0))
    monkeypatch.setattr(runtime_module.time, "perf_counter", lambda: next(ticks))

    marker = run._publish_hot_progress(0.0, stage="FIRST")
    marker = run._publish_hot_progress(marker, stage="SECOND")

    assert marker == 10.0
    assert run.clock.hot_seconds == 5.0
    assert published == ["FIRST", "SECOND"]


def test_lifecycle_behavior_fingerprint_includes_xfa_daily_paths() -> None:
    base_path = {
        "path": "XFA_STANDARD",
        "terminal_state": "DATA_CENSORED",
        "observed_days": 1,
        "payout_eligible": False,
        "payout_cycles": 0,
        "first_payout_day": None,
        "trader_net_payout": 0.0,
        "post_payout_observed_days": 0,
        "post_payout_survived": False,
        "post_payout_censored": False,
        "daily_ledger": [{"session_day": 11, "balance": 100.0}],
    }
    row = {
        "cost_scenario": "NORMAL",
        "start_day": 10,
        "combine_status": "TARGET_REACHED",
        "xfa_started": True,
        "xfa_start_day": 11,
        "xfa_standard": base_path,
        "xfa_consistency": {**base_path, "path": "XFA_CONSISTENCY"},
    }

    first = _lifecycle_account_behavior_fingerprint(_sha("combine"), [row])
    changed = {
        **row,
        "xfa_standard": {
            **base_path,
            "daily_ledger": [{"session_day": 11, "balance": 125.0}],
        },
    }
    second = _lifecycle_account_behavior_fingerprint(_sha("combine"), [changed])

    assert first != second


def test_finalist_roles_freeze_three_to_five_primaries_and_distinct_backup() -> None:
    sleeves = tuple(_sleeve(f"sleeve-{index}") for index in range(2))
    pairs = tuple(
        BookPair.create(
            combine_sleeves=sleeves,
            combine_allocation_units=(1, 1),
            combine_risk_tier=1.0,
            xfa_sleeves=(sleeves[index % 2],),
            xfa_allocation_units=(1,),
            xfa_risk_tier=(0.75, 1.0, 1.15, 1.3, 0.75, 1.0)[index],
            conflict_policy="PRIORITY",
            behaviorally_novel=True,
            generator_seed=25_000_501,
            proposal_index=index,
        )
        for index in range(6)
    )

    roles = _development_finalist_roles(pairs)

    assert sum(row["role"] == "PRIMARY_DEVELOPMENT_BOOK" for row in roles) == 5
    assert sum(row["role"] == "BEHAVIORALLY_DISTINCT_BACKUP" for row in roles) == 1


def test_stage2_authoritative_counter_excludes_rows_replaced_by_stage3() -> None:
    sleeves = tuple(_sleeve(f"counter-sleeve-{index}") for index in range(2))
    advancing = BookPair.create(
        combine_sleeves=sleeves,
        combine_allocation_units=(1, 1),
        combine_risk_tier=1.0,
        xfa_sleeves=(sleeves[0],),
        xfa_allocation_units=(1,),
        xfa_risk_tier=0.75,
        conflict_policy="PRIORITY",
        behaviorally_novel=True,
        generator_seed=25_000_501,
        proposal_index=1,
    )
    eliminated = BookPair.create(
        combine_sleeves=sleeves,
        combine_allocation_units=(1, 2),
        combine_risk_tier=1.0,
        xfa_sleeves=(sleeves[1],),
        xfa_allocation_units=(1,),
        xfa_risk_tier=1.0,
        conflict_policy="PRIORITY",
        behaviorally_novel=True,
        generator_seed=25_000_501,
        proposal_index=2,
    )
    rows = [
        {
            "pair_id": advancing.pair_id,
            "combine_episode_count": 8,
            "normal": {"episode_count": 4},
            "stressed": {"episode_count": 4},
        },
        {
            "pair_id": eliminated.pair_id,
            "combine_episode_count": 8,
            "normal": {"episode_count": 4},
            "stressed": {"episode_count": 4},
        },
    ]

    persisted = _metrics_excluding_pairs(rows, (advancing,))

    assert [row["pair_id"] for row in persisted] == [eliminated.pair_id]
    assert sum(row["combine_episode_count"] for row in persisted) == 8


def test_authoritative_episode_counter_must_equal_scenarios_and_bundle_rows() -> None:
    economic = {
        "production_counters": {
            "combine_episodes_completed": 16,
            "normal_episodes_completed": 8,
            "stressed_episodes_completed": 8,
        }
    }
    receipt = {"dataset_row_counts": {"episodes": 16}}

    _assert_authoritative_episode_counters(economic, receipt)

    with pytest.raises(PortfolioRuntimeError, match="diverge"):
        _assert_authoritative_episode_counters(
            economic, {"dataset_row_counts": {"episodes": 24}}
        )


def test_stage2_ranking_metric_is_compact_but_cache_payload_retains_raw_evidence() -> None:
    payload = {
        "pair_id": "pair-1",
        "combine_episode_count": 8,
        "normal": {"episode_count": 4},
        "stressed": {"episode_count": 4},
        "evidence_raw": {
            "NORMAL": [{"start_day": 1}],
            "STRESSED_1_5X": [{"start_day": 1}],
        },
    }
    compact = _compact_combine_metric(payload)
    assert "evidence_raw" not in compact
    assert "evidence_raw" in payload


def test_scientific_null_uses_bounded_diagnostic_selection_and_preserves_bank_coverage() -> None:
    sleeves = tuple(_sleeve(f"sleeve-{index}") for index in range(4))
    pairs = (
        BookPair.create(
            combine_sleeves=(sleeves[0], sleeves[1]),
            combine_allocation_units=(1, 1),
            combine_risk_tier=1.0,
            xfa_sleeves=(sleeves[0],),
            xfa_allocation_units=(1,),
            xfa_risk_tier=0.75,
            conflict_policy="PRIORITY",
            behaviorally_novel=True,
            generator_seed=25,
            proposal_index=0,
        ),
        BookPair.create(
            combine_sleeves=(sleeves[2], sleeves[3]),
            combine_allocation_units=(1, 1),
            combine_risk_tier=1.0,
            xfa_sleeves=(sleeves[3],),
            xfa_allocation_units=(1,),
            xfa_risk_tier=0.75,
            conflict_policy="PRIORITY",
            behaviorally_novel=True,
            generator_seed=25,
            proposal_index=1,
        ),
    )
    metrics = [
        {
            "pair_id": pair.pair_id,
            "normal": {
                "net_total": -10.0 - index,
                "mll_breach_rate": 0.0,
                "pass_count": 0,
                "target_progress_p25": 0.1,
                "target_progress_median": 0.2,
            },
            "stressed": {
                "net_total": -20.0 - index,
                "mll_breach_rate": 0.0,
                "pass_count": 0,
                "target_progress_p25": 0.05,
                "target_progress_median": 0.1,
            },
        }
        for index, pair in enumerate(pairs)
    ]

    selected = _select_pairs(
        pairs,
        metrics,
        limit=2,
        require_stress=True,
        required_sleeve_ids={row.sleeve_id for row in sleeves},
    )

    assert len(selected) == 2
    assert {
        sleeve_id
        for pair in selected
        for sleeve_id in set(pair.combine_sleeve_ids) | set(pair.xfa_sleeve_ids)
    } == {row.sleeve_id for row in sleeves}
