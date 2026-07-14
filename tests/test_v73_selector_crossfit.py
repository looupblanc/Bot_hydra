from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from hydra.account_policy.basket import RoutedTrade
from hydra.data.contract_mapping import build_rule_based_roll_map
from hydra.economic_evolution.account_evaluation import ExactSleeveRuntime
from hydra.economic_evolution.schema import EconomicRole
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.selection.selector_crossfit import (
    RecoveredCampaignLedger,
    SelectorCrossfitError,
    assert_exact_runtime_metadata,
    build_purged_temporal_block_plan,
    canonical_hash,
    file_sha256,
    isolate_outer_fold_ledger,
    leave_one_block_out_folds,
    load_recovered_event_ledger,
    write_recovered_event_ledger,
)


def _session_days() -> tuple[int, ...]:
    start = date(2023, 7, 3)
    output: list[int] = []
    current = start
    epoch = date(1970, 1, 1)
    while len(output) < 240:
        if current.weekday() < 5:
            output.append((current - epoch).days)
        current += timedelta(days=1)
    return tuple(output)


def _runtime(
    sleeve_id: str,
    days: tuple[int, ...],
    *,
    regime: str,
) -> ExactSleeveRuntime:
    events = tuple(
        RoutedTrade(
            component_id=sleeve_id,
            market="MES",
            side=1,
            event=TradePathEvent(
                event_id=f"{sleeve_id}:{day}",
                decision_ns=day * 1_000_000_000,
                exit_ns=day * 1_000_000_000 + 60_000_000,
                session_day=day,
                net_pnl=10.0,
                gross_pnl=11.0,
                worst_unrealized_pnl=-2.0,
                best_unrealized_pnl=12.0,
                quantity=1,
                mini_equivalent=0.1,
                regime=regime,
            ),
        )
        for day in days
    )
    return ExactSleeveRuntime(
        sleeve_id=sleeve_id,
        signal_market="ES",
        execution_market="MES",
        role=EconomicRole.PRIMARY_ALPHA,
        source_campaign="frozen_source",
        specification_hash=canonical_hash({"sleeve_id": sleeve_id}),
        eligible_session_days=days,
        events=events,
        event_count=len(events),
        net_pnl=10.0 * len(events),
        cost_stress_1_5x_net=9.5 * len(events),
        maximum_drawdown=0.0,
        best_positive_event_share=1.0 / len(events),
        exit_implementation="EXACT_TIME_EXIT",
    )


def _roll_map_path(tmp_path: Path, days: tuple[int, ...]) -> Path:
    epoch = date(1970, 1, 1)
    start = epoch + timedelta(days=days[0])
    end = epoch + timedelta(days=days[-1] + 1)
    roll_map = build_rule_based_roll_map(
        ["MES"], start=start.isoformat(), end=end.isoformat(), unsafe_window_days=3
    )
    path = tmp_path / "frozen_roll_map.json"
    path.write_text(
        json.dumps(roll_map.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _ledger(tmp_path: Path) -> tuple[RecoveredCampaignLedger, Path]:
    days = _session_days()
    roll_map_path = _roll_map_path(tmp_path, days)
    runtimes = {
        "sleeve_a": _runtime("sleeve_a", days, regime="LOW_VOL"),
        "sleeve_b": _runtime("sleeve_b", days, regime="HIGH_VOL"),
    }
    ledger = write_recovered_event_ledger(
        runtimes,
        output_dir=tmp_path / "ledger",
        provenance={
            "campaign_id": "hydra_economic_evolution_static_parent_basket_0023",
            "contract_map_sha256": file_sha256(roll_map_path),
            "controlled_reconstruction_due_to_0023_ledger_omission": True,
            "feature_builder_called": False,
            "q4_access_count_delta": 0,
        },
        component_specs={
            "sleeve_a": {"session_code": 0, "market": "ES"},
            "sleeve_b": {"session_code": 2, "market": "ES"},
        },
    )
    return ledger, roll_map_path


def _contamination() -> dict[str, list[dict[str, object]]]:
    return {
        f"V73_B{index}": [
            {
                "data_role": "DEVELOPMENT_ONLY_Q4_EXCLUDED",
                "prior_campaigns_saw_period": True,
                "independent_confirmation": False,
            }
        ]
        for index in range(1, 5)
    }


def test_content_addressed_ledger_is_stable_and_load_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger, _ = _ledger(tmp_path)
    second = write_recovered_event_ledger(
        ledger.runtimes,
        output_dir=tmp_path / "ledger",
        provenance=ledger.manifest["provenance"],
        component_specs=ledger.manifest["component_specs"],
    )
    assert second.manifest_path == ledger.manifest_path
    assert second.ledger_path == ledger.ledger_path
    assert second.ledger_sha256 == ledger.ledger_sha256

    # Loading the immutable event ledger has no route to the feature builder.
    import hydra.research.turbo_feature_builder as builder

    monkeypatch.setattr(
        builder,
        "build_or_open_turbo_feature_bundles",
        lambda **_: (_ for _ in ()).throw(AssertionError("feature rebuild attempted")),
    )
    loaded = load_recovered_event_ledger(ledger.manifest_path)
    assert loaded.common_session_days == _session_days()
    assert sum(row.event_count for row in loaded.runtimes.values()) == 480
    assert loaded.manifest["load_recomputes_features_or_signals"] is False


def test_ledger_tampering_fails_closed(tmp_path: Path) -> None:
    ledger, _ = _ledger(tmp_path)
    content = ledger.ledger_path.read_text(encoding="utf-8")
    ledger.ledger_path.write_text(content.replace('"net_pnl":10.0', '"net_pnl":9.0', 1))
    with pytest.raises(SelectorCrossfitError, match="checksum drift"):
        load_recovered_event_ledger(ledger.manifest_path)


def test_purged_blocks_are_chronological_embargoed_and_audited(tmp_path: Path) -> None:
    ledger, roll_map_path = _ledger(tmp_path)
    plan = build_purged_temporal_block_plan(
        ledger,
        contract_map_path=roll_map_path,
        contamination_history_by_block=_contamination(),
    )
    assert len(plan.blocks) == 4
    assert [len(block.session_days) for block in plan.blocks] == [52] * 4
    assert [len(block.episode_start_days) for block in plan.blocks] == [12] * 4
    assert [len(gap) for gap in plan.embargo_gaps] == [10, 10, 10]
    assert len(plan.trailing_unused_days) == 2
    assert canonical_hash(
        {key: value for key, value in plan.to_dict().items() if key != "plan_hash"}
    ) == plan.plan_hash

    for index, block in enumerate(plan.blocks):
        assert block.episode_start_days == block.session_days[:12]
        last_start_index = block.session_days.index(block.episode_start_days[-1])
        assert last_start_index + block.primary_horizon_sessions <= len(block.session_days)
        assert block.within_block_starts_independent is False
        assert block.trading_day_count == 52
        assert block.event_count == 104
        assert block.volatility_regime_counts == {"HIGH_VOL": 52, "LOW_VOL": 52}
        assert block.contracts_by_market["MES"]
        assert block.contamination_history
        if index:
            gap = plan.embargo_gaps[index - 1]
            assert plan.blocks[index - 1].session_days[-1] < gap[0]
            assert gap[-1] < block.session_days[0]


def test_outer_fold_views_are_disjoint_and_heldout_mutation_cannot_change_design(
    tmp_path: Path,
) -> None:
    ledger, roll_map_path = _ledger(tmp_path)
    plan = build_purged_temporal_block_plan(
        ledger,
        contract_map_path=roll_map_path,
        contamination_history_by_block=_contamination(),
    )
    folds = leave_one_block_out_folds(plan)
    assert len(folds) == 4
    assert all(
        canonical_hash(
            {key: value for key, value in fold.to_dict().items() if key != "fold_hash"}
        )
        == fold.fold_hash
        for fold in folds
    )
    assert {fold.held_out_block_id for fold in folds} == {
        "V73_B1",
        "V73_B2",
        "V73_B3",
        "V73_B4",
    }
    fold = folds[0]
    isolated = isolate_outer_fold_ledger(ledger, plan, fold)
    assert not set(isolated.design.eligible_session_days) & set(
        isolated.held_out.eligible_session_days
    )
    assert len(isolated.design.eligible_session_days) == 156
    assert len(isolated.held_out.eligible_session_days) == 52

    runtime = ledger.runtimes["sleeve_a"]
    changed_events = list(runtime.events)
    first = changed_events[0]
    changed_events[0] = replace(
        first,
        event=replace(first.event, net_pnl=first.event.net_pnl + 999.0),
    )
    mutated_runtimes = dict(ledger.runtimes)
    mutated_runtimes["sleeve_a"] = replace(runtime, events=tuple(changed_events))
    mutated = replace(ledger, runtimes=mutated_runtimes)
    changed = isolate_outer_fold_ledger(mutated, plan, fold)
    assert changed.design.view_hash == isolated.design.view_hash
    assert changed.held_out.view_hash != isolated.held_out.view_hash


def test_runtime_metadata_reconciliation_is_exact_and_names_drift() -> None:
    expected = [{"sleeve_id": f"sleeve_{index:02d}", "net_pnl": float(index)} for index in range(48)]
    assert_exact_runtime_metadata(expected, list(expected))
    changed = [dict(row) for row in expected]
    changed[17]["net_pnl"] += 0.01
    with pytest.raises(SelectorCrossfitError, match="sleeve_17.*net_pnl"):
        assert_exact_runtime_metadata(expected, changed)
