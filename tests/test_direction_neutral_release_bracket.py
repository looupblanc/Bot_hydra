from __future__ import annotations

import pandas as pd

import hydra.research.direction_neutral_release_bracket as release_bracket
from hydra.research.fx_causal_ecology import RawTrade
from hydra.research.direction_neutral_release_bracket import (
    BracketPolicy,
    ReplayDiagnostics,
    ReleaseEvent,
    frozen_policies,
    materialize_brackets,
)


def _panel(*, roll_in_lookback: bool = False):
    timestamps = pd.date_range("2023-01-06T13:25:00Z", periods=40, freq="1min")
    roots = ("6E", "6B", "6J", "6A")
    frame = lambda value: pd.DataFrame({root: [value] * len(timestamps) for root in roots}, index=timestamps)
    opened = frame(1.05)
    high = frame(1.08)
    low = frame(1.02)
    close = frame(1.05)
    high.loc[pd.Timestamp("2023-01-06T13:30:00Z"), "6E"] = 1.10
    low.loc[pd.Timestamp("2023-01-06T13:30:00Z"), "6E"] = 1.00
    contracts = frame(101)
    if roll_in_lookback:
        contracts.loc[pd.Timestamp("2023-01-06T13:25:00Z"), "6E"] = 100
    return {
        "timestamps": timestamps,
        "timestamp_ns": timestamps.as_unit("ns").view("i8"),
        "open": opened,
        "high": high,
        "low": low,
        "close": close,
        "contract_id": contracts,
        "session_day": pd.Index([20230106] * len(timestamps)).to_numpy(),
        "flatten": pd.Index([False] * len(timestamps)).to_numpy(),
    }


def _policy():
    return BracketPolicy(
        root="6E",
        release_scope="BLS_EMPLOYMENT_SITUATION",
        lookback_minutes=5,
        trigger_buffer_fraction=0.0,
        trigger_window_minutes=1,
        stop_range_fraction=0.5,
        target_r=2.0,
        holding_minutes=5,
    )


def test_dual_touch_is_charged_as_loss_not_discarded():
    event = ReleaseEvent(
        family="BLS_EMPLOYMENT_SITUATION",
        release_ns=int(pd.Timestamp("2023-01-06T13:30:00Z").value),
        event_id="nfp",
    )
    trades, diagnostics = materialize_brackets(
        _policy(), (event,), _panel(), ("2023-01-01", "2023-02-01")
    )
    assert len(trades) == 1
    assert diagnostics.dual_touch_losses == 1
    assert trades[0].same_bar_ambiguous is True
    assert trades[0].gross_one_contract < 0.0


def test_contract_roll_in_feature_window_fails_closed():
    event = ReleaseEvent(
        family="BLS_EMPLOYMENT_SITUATION",
        release_ns=int(pd.Timestamp("2023-01-06T13:30:00Z").value),
        event_id="nfp",
    )
    trades, diagnostics = materialize_brackets(
        _policy(), (event,), _panel(roll_in_lookback=True), ("2023-01-01", "2023-02-01")
    )
    assert trades == ()
    assert diagnostics.roll_rejected == 1


def test_frozen_lattice_is_bounded_and_direction_neutral():
    policies = frozen_policies()
    assert len(policies) == 1536
    assert {policy.root for policy in policies} == {"6E", "6B", "6J", "6A"}
    assert not any(hasattr(policy, "direction") for policy in policies)


def test_empty_preflight_finalizes_with_python_boolean_literals(tmp_path, monkeypatch):
    manifest = {
        "manifest_hash": "m" * 64,
        "source_commit": "c" * 40,
        "data": {"release_calendar_sha256": "d" * 64},
    }
    monkeypatch.setattr(
        release_bracket,
        "load_contract",
        lambda _root: (manifest, (), {"artifact_id": "calendar"}, {}),
    )
    monkeypatch.setattr(
        release_bracket,
        "load_inputs",
        lambda _root: ({}, {"receipt_hash": "r" * 64}, None),
    )
    monkeypatch.setattr(release_bracket, "build_panel", lambda _bars: {})
    monkeypatch.setattr(release_bracket, "frozen_policies", lambda: ())
    monkeypatch.setattr(release_bracket, "_rule_configs", lambda _rules: {})
    result = release_bracket.run(tmp_path, output_dir="result")
    assert result["status"] == "RELEASE_BRACKET_PREFLIGHT_FALSIFIED"
    assert result["confirmation_opened"] is False
    assert (tmp_path / "result/economic_result.json").is_file()


def test_behavior_hash_excludes_candidate_identity():
    values = dict(
        root="6E",
        direction=1,
        decision_ns=1,
        entry_ns=2,
        exit_ns=3,
        session_day=20230101,
        entry_price=1.0,
        exit_price=1.1,
        stop_distance=0.1,
        gross_one_contract=10.0,
        normal_net_one_contract=8.0,
        stressed_net_one_contract=7.0,
        normal_worst_one_contract=-5.0,
        stressed_worst_one_contract=-6.0,
        normal_best_one_contract=12.0,
        stressed_best_one_contract=11.0,
        same_bar_ambiguous=False,
    )
    diagnostics = ReplayDiagnostics(1, 1, 1, 0, 0, 0, 0)
    left = release_bracket.summarize_trades(
        _policy(), (RawTrade(trade_id="candidate-a", **values),), diagnostics
    )
    right = release_bracket.summarize_trades(
        _policy(), (RawTrade(trade_id="candidate-b", **values),), diagnostics
    )
    assert left["trade_hash"] == right["trade_hash"]
