from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import autonomous_tier_g_xfa_diagnostic as diagnostic


CANDIDATE = "hazard-test"
COMBINE_BOOK_HASH = "1" * 64
XFA_BOOK_HASH = "2" * 64
XFA_PROFILE_HASH = "3" * 64


def _source() -> dict:
    candidate = {
        "candidate_id": CANDIDATE,
        "account_label": "50K",
        "account_size_usd": 50_000,
        "combine_book_hash": COMBINE_BOOK_HASH,
        "xfa_book": {
            "component_ids": [CANDIDATE],
            "xfa_book_hash": XFA_BOOK_HASH,
        },
        "xfa_profile": {
            "xfa_profile_id": "test-profile",
            "xfa_profile_hash": XFA_PROFILE_HASH,
            "risk_multiplier": 1.0,
            "maximum_concurrent_sleeves": 1,
            "maximum_mini_equivalent": 5.0,
            "standard_and_consistency_are_alternative_paths": True,
            "outbound_order_capability": False,
        },
    }
    transitions = []
    for scenario, salt in (("NORMAL", "4"), ("STRESSED", "5")):
        transitions.append(
            {
                "transition_id": f"transition-{scenario.lower()}",
                "candidate_id": CANDIDATE,
                "scenario": scenario,
                "temporal_block": "B1",
                "combine_start_id": f"start-{scenario.lower()}",
                "combine_path_hash": salt * 64,
                "combine_book_hash": COMBINE_BOOK_HASH,
                "xfa_book_hash": XFA_BOOK_HASH,
                "xfa_profile_hash": XFA_PROFILE_HASH,
                "account_label": "50K",
                "account_size_usd": 50_000,
                "eligible_session_days": [102, 103, 104],
                "xfa_start_day": 102,
            }
        )
    return {
        "result_hash": "6" * 64,
        "source_tier_g_graduation_hash": "7" * 64,
        "candidate_handoffs": [candidate],
        "transitions": transitions,
        "counts": {
            "exact_combine_episode_reconstruction_count": 2,
            "source_tape_count": 2,
        },
    }


def _path(path: str, *, net: float) -> dict:
    payload = {
        "path": f"XFA_{path}",
        "terminal": "DATA_CENSORED",
        "terminal_reason": "available_chronology_ended",
        "start_day": 102,
        "end_day": 104,
        "requested_horizon_days": 120,
        "observed_days": 3,
        "traded_days": 3,
        "accepted_event_count": 3,
        "skipped_event_count": 0,
        "payout_cycles": 1,
        "first_payout_day": 3,
        "first_payout_count": 1,
        "gross_payout": net / 0.9,
        "trader_net_payout": net,
        "ending_balance": 500.0,
        "ending_mll_floor": 0.0,
        "minimum_mll_buffer": 500.0,
        "post_payout_survived": False,
        "qualifying_winning_days": 3,
        "maximum_consistency_ratio": 0.4,
        "maximum_mini_equivalent": 1.0,
        "skipped_reasons": {},
        "component_contribution": {CANDIDATE: 1_000.0},
        "daily_ledger": [],
    }
    return {**payload, "path_hash": stable_hash(payload)}


def _engine_result(transition_id: str, combine_hash: str) -> dict:
    payload = {
        "schema": "hydra_account_size_xfa_alternatives_v1",
        "engine_version": "hydra_account_size_xfa_causal_v1",
        "transition_id": transition_id,
        "combine_path_hash": combine_hash,
        "source_trajectory_hash": "8" * 64,
        "handoff": {"handoff_hash": "9" * 64},
        "rules": {"fingerprint": "a" * 64},
        "alternatives": {
            "STANDARD": _path("STANDARD", net=100.0),
            "CONSISTENCY": _path("CONSISTENCY", net=200.0),
        },
        "standard_and_consistency_are_alternatives": True,
        "sum_standard_and_consistency_ev_allowed": False,
        "selected_path": None,
        "broker_connection_count": 0,
        "order_count": 0,
        "outbound_order_capability": False,
    }
    return {**payload, "result_hash": stable_hash(payload)}


def _run(monkeypatch: pytest.MonkeyPatch) -> dict:
    source = _source()
    monkeypatch.setattr(
        diagnostic,
        "verify_tier_g_combine_xfa_handoffs",
        lambda _value: source,
    )
    monkeypatch.setattr(
        diagnostic,
        "materialize_transition_trajectories",
        lambda _source, _transition_id: {CANDIDATE: ()},
    )
    monkeypatch.setattr(
        diagnostic,
        "load_account_size_xfa_rules",
        lambda _label, snapshot_path: SimpleNamespace(fingerprint="a" * 64),
    )
    monkeypatch.setattr(
        diagnostic,
        "freeze_account_size_xfa_handoff",
        lambda **_kwargs: SimpleNamespace(handoff_hash="9" * 64),
    )

    def fake_run(
        _trajectories,
        _days,
        *,
        handoff,
        rules,
        transition_id,
        combine_path_hash,
        start_day,
        horizon_days,
    ):
        assert handoff.handoff_hash == "9" * 64
        assert rules.fingerprint == "a" * 64
        assert start_day == 102
        assert horizon_days == 120
        return SimpleNamespace(
            to_dict=lambda: _engine_result(transition_id, combine_path_hash)
        )

    monkeypatch.setattr(diagnostic, "run_account_size_xfa_alternatives", fake_run)
    return diagnostic.build_autonomous_tier_g_xfa_diagnostic(source)


def test_diagnostic_keeps_standard_and_consistency_ev_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run(monkeypatch)
    assert result["counts"]["combine_transition_count"] == 2
    assert result["counts"]["alternative_path_count"] == 4
    assert result["counts"]["standard_first_payout_count"] == 2
    assert result["counts"]["consistency_first_payout_count"] == 2
    normal_standard = result["path_totals_by_scenario"]["STANDARD"][
        "by_scenario"
    ]["NORMAL"]
    normal_consistency = result["path_totals_by_scenario"]["CONSISTENCY"][
        "by_scenario"
    ]["NORMAL"]
    assert normal_standard["expected_trader_payout_per_new_combine_attempt_usd"] == 100
    assert normal_consistency[
        "expected_trader_payout_per_new_combine_attempt_usd"
    ] == 200
    assert result["alternative_path_audit"][
        "standard_and_consistency_ev_summed"
    ] is False
    assert "combined_expected_value" not in result


def test_verifier_rejects_duplicate_path_for_one_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run(monkeypatch)
    tampered = copy.deepcopy(result)
    transition = tampered["path_records"][0]["transition_id"]
    rows = [
        row
        for row in tampered["path_records"]
        if row["transition_id"] == transition
    ]
    assert len(rows) == 2
    consistency = next(row for row in rows if row["path"] == "CONSISTENCY")
    consistency["path"] = "STANDARD"
    consistency["path_record_id"] = "xfa-path-" + stable_hash(
        {
            "transition_id": consistency["transition_id"],
            "path": consistency["path"],
            "engine_result_hash": consistency["engine_result_hash"],
        }
    )[:24]
    consistency["path_record_hash"] = stable_hash(
        {
            key: value
            for key, value in consistency.items()
            if key != "path_record_hash"
        }
    )
    tampered["result_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "result_hash"}
    )
    with pytest.raises(
        diagnostic.AutonomousTierGXfaDiagnosticError,
        match="identity/hash drift or duplication",
    ):
        diagnostic.verify_autonomous_tier_g_xfa_diagnostic(tampered)


def test_verifier_rejects_more_than_one_first_payout_per_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run(monkeypatch)
    tampered = copy.deepcopy(result)
    row = tampered["path_records"][0]
    row["first_payout_count"] = 2
    row["path_record_hash"] = stable_hash(
        {key: value for key, value in row.items() if key != "path_record_hash"}
    )
    tampered["result_hash"] = stable_hash(
        {key: value for key, value in tampered.items() if key != "result_hash"}
    )
    with pytest.raises(
        diagnostic.AutonomousTierGXfaDiagnosticError,
        match="first-payout uniqueness",
    ):
        diagnostic.verify_autonomous_tier_g_xfa_diagnostic(tampered)
