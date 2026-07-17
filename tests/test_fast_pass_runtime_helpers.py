from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict
from datetime import date
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.evidence.causal_salvage_adapter import _episode_ledgers
from hydra.production.fast_pass_runtime_helpers import (
    FastPassHelperError,
    _active_policy_from_spec,
    _book_spec,
    _control_delta,
    _event_block_economics,
    _governor_profiles,
    _install_sprint_globals,
    _proposal_qd_cell,
    _quality_trajectories,
    _quality_multiplier,
    _read_event_receipt,
    _resize_quality_trajectory,
    _selection_receipt,
    _sprint_batch_worker,
    _sprint_metrics,
    _verify_snapshot,
)
from hydra.portfolio.marginal_contribution_builder import GovernorProfile
from hydra.propfirm.combine_to_xfa import official_rule_snapshot_2026_07_15
from hydra.propfirm.combine_episode import TradePathEvent
from hydra.research.causal_sleeve_replay import (
    CausalTradeMark,
    CausalTradeTrajectory,
)


def _trajectory(day: int, *, pnl: float, component_id: str = "sleeve_a") -> CausalTradeTrajectory:
    decision = int((day * 86_400 + 3_600) * 1_000_000_000)
    exit_time = decision + 60_000_000_000
    event = TradePathEvent(
        event_id=f"{component_id}:{day}",
        decision_ns=decision,
        exit_ns=exit_time,
        session_day=day,
        net_pnl=pnl,
        gross_pnl=pnl + 10.0,
        worst_unrealized_pnl=-100.0,
        best_unrealized_pnl=pnl,
        quantity=4,
        mini_equivalent=0.4,
        regime="TEST",
    )
    return CausalTradeTrajectory(
        component_id=component_id,
        market="ES",
        side=1,
        event=event,
        marks=(
            CausalTradeMark(
                availability_time_ns=exit_time,
                worst_unrealized_pnl=-100.0,
                best_unrealized_pnl=pnl,
                current_unrealized_pnl=pnl,
            ),
        ),
        initial_unrealized_pnl=-10.0,
    )


def _profile() -> GovernorProfile:
    return GovernorProfile(
        profile_id="profile_a",
        signal_quality_tiers=(0.0, 0.5, 1.0, 1.5, 2.0),
        open_risk_ceiling_fraction=0.75,
        daily_loss_budget_fraction=0.5,
        daily_profit_lock_fraction=0.75,
        maximum_concurrent_sleeves=2,
        target_protection_fraction=0.9,
        same_instrument_conflict_policy="priority",
    )


def _starts() -> dict[int, tuple[tuple[int, str], ...]]:
    return {
        5: ((19_500, "B1"), (19_505, "B2"), (19_510, "B3"), (19_515, "B4")),
        10: ((19_500, "B1"), (19_510, "B3")),
        20: ((19_500, "B1"),),
    }


def _replay(
    *,
    censored_day: int | None = None,
    missing_eligible_day: int | None = None,
) -> SimpleNamespace:
    days = tuple(range(19_500, 19_525))
    normal = tuple(_trajectory(day, pnl=3_000.0) for day in days)
    stressed = tuple(_trajectory(day, pnl=2_500.0) for day in days)
    events = (
        ()
        if censored_day is None
        else (
            SimpleNamespace(
                session_day=censored_day,
                outcome="CENSORED_FUTURE_COVERAGE",
            ),
        )
    )
    return SimpleNamespace(
        events=events,
        eligible_session_days=tuple(
            day for day in days if day != missing_eligible_day
        ),
        normal_trajectories=normal,
        stressed_trajectories=stressed,
    )


def _account_rule_snapshot() -> dict[str, object]:
    official = official_rule_snapshot_2026_07_15()
    return {
        "account_size_usd": official.account_size,
        "profit_target_usd": official.combine_profit_target,
        "maximum_loss_limit_usd": official.maximum_loss_limit,
        "best_day_consistency_fraction": official.combine_consistency_limit,
        "maximum_mini_contracts": official.combine_maximum_mini_equivalent,
        "maximum_micro_contracts": official.combine_maximum_micros,
        "rule_snapshot_version": official.rule_version,
        "rule_snapshot_hash": official.fingerprint,
        "official_snapshot": official.to_dict(),
    }


def _governor_manifest() -> dict[str, object]:
    profiles = [
        {
            "profile_id": f"fast_pass_governor_{maximum:02d}",
            "signal_quality_tiers": [0.0, 0.5, 1.0, 1.5, 2.0],
            "open_risk_ceiling_fraction": (0.25, 0.5, 0.75, 0.75)[maximum - 1],
            "daily_loss_budget_fraction": (0.25, 0.25, 0.5, 0.5)[maximum - 1],
            "daily_profit_lock_fraction": (0.5, 0.75, 0.5, 0.75)[maximum - 1],
            "maximum_concurrent_sleeves": maximum,
            "target_protection_fraction": (0.8, 0.9, 0.8, 0.9)[maximum - 1],
            "same_instrument_conflict_policy": "priority",
        }
        for maximum in (1, 2, 3, 4)
    ]
    return {
        "risk_governor": {
            "signal_quality_tiers": [0.0, 0.5, 1.0, 1.5, 2.0],
            "maximum_concurrent_sleeves": [1, 2, 3, 4],
            "open_risk_ceiling_mll_buffer_fractions": [0.25, 0.5, 0.75],
            "daily_loss_budget_mll_fractions": [0.25, 0.5],
            "daily_profit_lock_consistency_fractions": [0.5, 0.75],
            "frozen_profiles": profiles,
            "frozen_profiles_hash": stable_hash(profiles),
            "executable_quality_quantization": (
                "CAUSAL_EVENT_IDENTITY_HASH_INTEGER_MICRO_QUANTIZATION_V1"
            ),
        }
    }


def _policy_spec() -> dict[str, object]:
    profile = _profile()
    return {
        "schema": "hydra_fast_pass_sprint_policy_v1",
        "policy_id": "fast_policy_a",
        "component_ids": ["sleeve_a"],
        "quality_multipliers": {"sleeve_a": 1.0},
        "governor_profile_id": profile.profile_id,
        "governor_profile": {
            **asdict(profile),
            "signal_quality_tiers": list(profile.signal_quality_tiers),
        },
        "policy_role": "TEST",
        "wave": 1,
        "predecessor_policy_id": None,
    }


def test_quality_resizing_changes_only_executable_size_and_economics() -> None:
    source = _trajectory(19_500, pnl=100.0)

    half_rows, half_receipt = _quality_trajectories((source,), 0.5)
    double = _resize_quality_trajectory(source, 2.0)

    assert len(half_rows) in {0, 1}
    if half_rows:
        assert half_rows[0].event.quantity == source.event.quantity
        assert half_rows[0].event.net_pnl == source.event.net_pnl
    assert half_receipt["uses_outcome_fields"] is False
    assert half_receipt["fractional_contracts_created"] is False
    assert double.event.quantity == 8
    assert double.event.net_pnl == pytest.approx(200.0)
    assert source.event.quantity == 4
    assert double.event.decision_ns == source.event.decision_ns


def test_quality_tier_1_5_uses_only_deterministic_one_or_two_x_events() -> None:
    rows = tuple(_trajectory(day, pnl=100.0) for day in range(19_500, 19_510))

    resized, receipt = _quality_trajectories(rows, 1.5)

    assert len(resized) == len(rows)
    assert {row.event.quantity for row in resized}.issubset({4, 8})
    assert set(receipt["resolved_multiplier_counts"]).issubset({"1", "2"})
    assert len(receipt["selection_scaling_hash"]) == 64


def test_sprint_worker_uses_union_calendar_and_produces_nested_summaries() -> None:
    calendar = tuple(range(19_500, 19_525))
    _install_sprint_globals(
        replays={"sleeve_a": _replay()},
        calendar=calendar,
        starts=_starts(),
        governors=(),
        account_rule_snapshot=_account_rule_snapshot(),
    )

    row = _sprint_batch_worker([_policy_spec()])[0]

    assert row["status"] == "SPRINT_COMPLETE"
    assert row["signal_recomputation_performed"] is False
    assert row["summaries"]["NORMAL"]["5"]["episode_count"] == 4
    assert row["summaries"]["NORMAL"]["5"]["pass_count"] == 4
    assert row["summaries"]["STRESSED_1_5X"]["5"]["pass_count"] == 4
    assert row["summaries"]["NORMAL"]["10"]["episode_count"] == 2
    assert row["summaries"]["NORMAL"]["20"]["episode_count"] == 1
    assert row["summaries"]["NORMAL"]["5"]["block_pass_counts"] == {
        "B1": 1,
        "B2": 1,
        "B3": 1,
        "B4": 1,
    }
    assert row["summaries"]["NORMAL"]["5"]["maximum_mini_equivalent_max"] == pytest.approx(0.4)
    assert row["summaries"]["NORMAL"]["5"]["mean_daily_contract_utilization"] == pytest.approx(0.4 / 15.0)
    assert set(row["by_block"]) == {"B1", "B2", "B3", "B4"}
    assert row["summaries_by_role"]["DESIGN"]["NORMAL"]["5"]["episode_count"] == 2
    assert row["summaries_by_role"]["HELD_OUT_DEVELOPMENT"]["NORMAL"]["5"]["episode_count"] == 2
    assert row["quality_scaling"]["sleeve_a"]["uses_outcome_fields"] is False
    assert len(row["_episode_evidence"]) == 14
    assert all(
        evidence["coverage_state"] == "TARGET_REACHED"
        for evidence in row["_episode_evidence"]
    )
    episodes, daily_paths = _episode_ledgers(
        campaign_id="hydra_fast_pass_factory_0029",
        records=row["_episode_evidence"],
        policy_ids={"fast_policy_a"},
        component_ids={"sleeve_a"},
    )
    assert len(episodes) == 14
    assert daily_paths


def test_censored_signal_removes_only_affected_headline_denominators() -> None:
    _install_sprint_globals(
        replays={"sleeve_a": _replay(censored_day=19_502)},
        calendar=tuple(range(19_500, 19_525)),
        starts=_starts(),
        governors=(),
        account_rule_snapshot=_account_rule_snapshot(),
    )

    row = _sprint_batch_worker([_policy_spec()])[0]

    five = row["summaries"]["NORMAL"]["5"]
    assert five["requested_start_count"] == 4
    assert five["episode_count"] == 3
    assert five["data_censored_count"] == 1
    assert row["by_block"]["B1"]["NORMAL"]["5"]["data_censored_count"] == 1
    # Censored starts remain explicit in coverage summaries but are excluded
    # from the paired normal/stressed EvidenceBundle episode denominator.
    assert len(row["_episode_evidence"]) == 8
    assert all(
        evidence["episode"] is not None for evidence in row["_episode_evidence"]
    )
    assert row["data_censored_episode_count"] == 6


def test_missing_component_eligible_day_censors_affected_windows() -> None:
    _install_sprint_globals(
        replays={"sleeve_a": _replay(missing_eligible_day=19_503)},
        calendar=tuple(range(19_500, 19_525)),
        starts=_starts(),
        governors=(),
        account_rule_snapshot=_account_rule_snapshot(),
    )

    row = _sprint_batch_worker([_policy_spec()])[0]

    assert row["summaries"]["NORMAL"]["5"]["data_censored_count"] == 1
    assert row["summaries"]["NORMAL"]["10"]["data_censored_count"] == 1
    assert row["summaries"]["NORMAL"]["20"]["data_censored_count"] == 1
    assert row["summaries"]["NORMAL"]["5"]["episode_count"] == 3


def test_sprint_globals_require_exact_official_rule_snapshot() -> None:
    drifted = _account_rule_snapshot()
    drifted["rule_snapshot_hash"] = "0" * 64

    with pytest.raises(FastPassHelperError, match="official account rule snapshot drift"):
        _install_sprint_globals(
            replays={"sleeve_a": _replay()},
            calendar=tuple(range(19_500, 19_525)),
            starts=_starts(),
            governors=(),
            account_rule_snapshot=drifted,
        )


def test_profile_book_spec_and_policy_are_frozen_and_bounded() -> None:
    profile = _profile()
    bank = {
        "a": {"quality_multiplier": 1.5},
        "b": {"quality_multiplier": 2.0},
    }
    spec = _book_spec(
        policy_id="book",
        members=("a", "b"),
        bank_by_id=bank,
        profile=profile,
        role="BOOK",
        wave=1,
        predecessor_id="smaller",
    )

    policy = _active_policy_from_spec(spec)

    assert spec["quality_multipliers"] == {"a": 1.5, "b": 2.0}
    assert spec["predecessor_policy_id"] == "smaller"
    assert policy.maximum_concurrent_sleeves == 2
    assert policy.maximum_mini_equivalent == 15.0
    assert policy.static_risk_tier == 1.0
    assert policy.outbound_order_capability is False


def test_stage_cells_quality_and_block_economics_are_deterministic() -> None:
    row = {
        "candidate_id": "c",
        "candidate": {
            "market": "ES",
            "session_code": 0,
            "timeframe": "5m",
            "horizon": 15,
            "mechanism": "VOLATILITY_EXPANSION",
            "direction_rule": "PAST_RETURN_SIGN",
            "risk_level": 1.0,
        },
        "screen": {
            "independent_events_per_20_sessions": 6.0,
            "favorable_before_adverse_rate": 0.61,
            "cost_adjusted_target_velocity": 0.1,
            "stressed_net_pnl": 1.0,
        },
    }
    events = [
        {
            "session_day": (date(2024, 1, 2) - date(1970, 1, 1)).days,
            "outcome": "FAVORABLE_FIRST",
            "normal_net_pnl": 100.0,
            "stressed_net_pnl": 80.0,
        }
    ]

    assert _proposal_qd_cell(row) == _proposal_qd_cell(dict(row))
    assert _quality_multiplier(row["screen"]) == 2.0
    block = _event_block_economics(
        events,
        ({"block_id": "B", "start": "2024-01-01", "end": "2024-01-31"},),
    )["B"]
    assert block["event_count"] == 1
    assert block["stressed_net"] == 80.0


def test_event_receipt_selection_and_snapshot_integrity(tmp_path) -> None:
    rows = [{"candidate_id": "a", "value": 1}]
    plain = (json.dumps(rows[0], sort_keys=True, separators=(",", ":")) + "\n").encode()
    compressed = gzip.compress(plain, mtime=0)
    evidence = tmp_path / "events.jsonl.gz"
    evidence.write_bytes(compressed)
    receipt = {
        "base_dir": str(tmp_path),
        "relative_path": evidence.name,
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "uncompressed_sha256": hashlib.sha256(plain).hexdigest(),
        "record_count": 1,
    }
    assert _read_event_receipt(receipt) == rows

    selection = _selection_receipt(
        campaign_id="c",
        wave=1,
        stage="S",
        rows=rows,
        selected_ids=("a",),
        policy="P",
    )
    assert selection["selection_hash"] == stable_hash(
        {key: value for key, value in selection.items() if key != "selection_hash"}
    )
    manifest = {"campaign_id": "c", "manifest_hash": "m", "source_commit": "s"}
    snapshot = {**manifest, "state": "RUNNING"}
    snapshot["state_hash"] = stable_hash(snapshot)
    _verify_snapshot(snapshot, "state_hash", manifest)
    with pytest.raises(FastPassHelperError):
        _verify_snapshot({**snapshot, "state": "ALTERED"}, "state_hash", manifest)


def test_sprint_metric_conversion_and_control_delta() -> None:
    def summary(scale: float) -> dict[str, dict[str, float]]:
        return {
            horizon: {
                "pass_rate": scale * rate,
                "target_progress_p25": scale * 0.1,
                "target_progress_median": scale * 0.2,
                "mll_breach_rate": 0.0,
                "consistency_rate": 1.0,
                "net_total": scale * 100.0,
            }
            for horizon, rate in (("5", 0.05), ("10", 0.10), ("20", 0.20))
        }

    metrics = _sprint_metrics(summary(1.0))
    assert metrics.pass_rate_5d == 0.05
    assert metrics.pass_rate_20d == 0.20
    source = {
        "policy_id": "source",
        "summaries": {
            "NORMAL": summary(1.0),
            "STRESSED_1_5X": summary(0.8),
        },
    }
    control = {
        "policy_id": "control",
        "summaries": {
            "NORMAL": summary(0.5),
            "STRESSED_1_5X": summary(0.4),
        },
    }
    delta = _control_delta(source, control)
    assert delta["by_scenario_horizon"]["NORMAL"]["5"]["pass_rate_delta"] == pytest.approx(0.025)


def test_manifest_governor_frontier_is_bounded_and_deterministic() -> None:
    manifest = _governor_manifest()

    profiles = _governor_profiles(manifest)

    assert profiles == _governor_profiles(manifest)
    assert len(profiles) == 4
    assert {row.maximum_concurrent_sleeves for row in profiles} == {1, 2, 3, 4}
    assert all(row.signal_quality_tiers == (0.0, 0.5, 1.0, 1.5, 2.0) for row in profiles)

    drifted = _governor_manifest()
    drifted["risk_governor"]["frozen_profiles_hash"] = "0" * 64
    with pytest.raises(FastPassHelperError, match="profile hash drift"):
        _governor_profiles(drifted)
