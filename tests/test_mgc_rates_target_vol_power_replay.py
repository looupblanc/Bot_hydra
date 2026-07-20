from __future__ import annotations

import json
import inspect
from pathlib import Path

import pandas as pd
import pytest

from hydra.data.contract_mapping import ContractInfo, RollMap
from hydra.research import mgc_rates_target_vol_power_replay as replay


def _summary(*, net: float, passes: int = 0, mll: float = 0.0) -> dict[str, object]:
    return {
        "net_total_usd": net,
        "pass_count": passes,
        "mll_breach_rate": mll,
        "all_passing_paths_consistency_compliant": passes > 0,
    }


def _candidate(
    *,
    validation_net: float,
    final_net: float,
    normal_passes: int,
    stressed_passes: int,
    complete_gate: bool,
) -> dict[str, object]:
    return {
        "evaluations": {
            "VALIDATION": {
                replay.PRIMARY: {
                    "stressed": _summary(net=validation_net),
                }
            },
            "FINAL_DEVELOPMENT": {
                replay.PRIMARY: {
                    "normal": _summary(net=final_net, passes=normal_passes),
                    "stressed": _summary(net=final_net, passes=stressed_passes),
                }
            },
        },
        "gate": {"passed": complete_gate},
    }


def _power(passed: bool = True) -> dict[str, object]:
    return {"passed": passed}


def _control_power(passed: bool = True) -> dict[str, object]:
    return {"passed": passed}


def _contract(
    root: str,
    contract: str,
    instrument_id: str,
    start: str,
    end: str,
) -> ContractInfo:
    return ContractInfo(
        root=root,
        contract=contract,
        month_code="Z",
        year=2020,
        expiry_date=end,
        last_trade_date=end,
        active_start=start,
        active_end=end,
        roll_date=start,
        tick_size=0.1,
        tick_value=1.0,
        point_value=10.0,
        contract_multiplier=10.0,
        is_micro=True,
        instrument_id=instrument_id,
    )


def _control_event(day: int, primary_id: str, decision: int) -> dict[str, object]:
    return {
        "session_day": day,
        "matched_primary_event_id": primary_id,
        "decision_ns": decision,
        "entry_ns": decision + 1,
        "exit_ns": decision + 2,
        "side": 1,
        "entry_price": 100.0,
        "exit_price": 101.0,
        "target_contract": "MGCZ0",
    }


def _raw_match_event(
    *, day: int, event_id: str, decision: int, minute: int = 500
) -> dict[str, object]:
    return {
        **_control_event(day, "", decision),
        "event_id": event_id,
        "block": f"{str(day)[:4]}Q3",
        "session_role": "OPEN",
        "local_minute": minute,
    }


def test_card_freezes_exact_candidate_power_gate_and_governance() -> None:
    card = replay.load_replay_card()
    assert card["frozen_candidate"]["candidate_id"] == replay.CANDIDATE_ID
    assert (
        card["frozen_candidate"]["structural_fingerprint"]
        == replay.CANDIDATE_FINGERPRINT
    )
    assert card["power_preflight"]["minimum_independent_events_per_target"] == {
        "DISCOVERY": 60,
        "VALIDATION": 12,
        "FINAL_DEVELOPMENT": 20,
    }
    assert card["frozen_gate"]["minimum_combined_final_normal_passes"] == 2
    assert card["frozen_gate"]["minimum_combined_final_stressed_passes"] == 1
    assert card["governance"]["promotion_allowed"] is False
    assert card["governance"]["tier_q_allowed"] is False
    assert card["governance"]["xfa_allowed"] is False
    assert card["governance"]["network_access_allowed"] is False
    assert card["governance"]["q4_access_allowed"] is False


def test_card_hash_and_semantics_fail_closed(tmp_path: Path) -> None:
    card = replay.load_replay_card()
    card["frozen_candidate"]["candidate_id"] = "neighbor"
    path = tmp_path / "card.json"
    path.write_text(json.dumps(card), encoding="utf-8")
    with pytest.raises(replay.MGCPowerReplayError, match="semantic or hash drift"):
        replay.load_replay_card(path)


def test_frozen_proposal_reproduces_original_fingerprint() -> None:
    card = replay.load_replay_card()
    proposal = replay._frozen_proposal(card["causal_contract"]["mechanism"])
    assert proposal == {
        "candidate_id": replay.CANDIDATE_ID,
        "mechanism": "RATES_TARGET_VOL_GAP_OCO",
        "source_markets": ["ZN", "TN"],
        "execution_market": "MGC",
        "session_role": "OPEN",
        "structural_fingerprint": replay.CANDIDATE_FINGERPRINT,
    }


def test_receipt_semantics_forbid_outcomes_q4_and_promotion() -> None:
    receipt = {
        "schema": replay.acquisition.RECEIPT_SCHEMA,
        "bundle_id": "bundle",
        "candidate_ids": [replay.CANDIDATE_ID],
        "download_status": "DOWNLOADED",
        "outcomes_read": 0,
        "economic_replay_started": False,
        "promotion_changes": 0,
        "q4_access_count_delta": 0,
        "protected_data_access_count_delta": 0,
        "broker_connections": 0,
        "orders": 0,
        "power_thresholds": replay.POWER_THRESHOLDS,
        "official_total_cost_usd": replay.acquisition.EXPECTED_TOTAL_COST_USD,
    }
    replay._validate_receipt_semantics(receipt, expected_bundle_id="bundle")
    for key, bad in (
        ("outcomes_read", 1),
        ("economic_replay_started", True),
        ("promotion_changes", 1),
        ("q4_access_count_delta", 1),
        ("protected_data_access_count_delta", 1),
        ("broker_connections", 1),
        ("orders", 1),
    ):
        changed = {**receipt, key: bad}
        with pytest.raises(replay.MGCPowerReplayError, match="receipt semantic drift"):
            replay._validate_receipt_semantics(changed, expected_bundle_id="bundle")


def test_power_gate_precedes_economic_outcomes() -> None:
    card = replay.load_replay_card()
    candidate = _candidate(
        validation_net=1.0,
        final_net=1.0,
        normal_passes=2,
        stressed_passes=1,
        complete_gate=True,
    )
    gate = replay.power_replay_gate(
        candidate,
        power=_power(False),
        control_power=_control_power(True),
        card=card,
    )
    assert gate["status"] == "MGC_POWER_REPLAY_UNDERPOWERED_NO_THRESHOLD_RELAXATION"
    assert gate["passed"] is False


def test_unmatched_controls_are_underpowered_not_an_artificial_win() -> None:
    card = replay.load_replay_card()
    candidate = _candidate(
        validation_net=1.0,
        final_net=1.0,
        normal_passes=2,
        stressed_passes=1,
        complete_gate=True,
    )
    gate = replay.power_replay_gate(
        candidate,
        power=_power(True),
        control_power=_control_power(False),
        card=card,
    )
    assert gate["status"] == "MGC_POWER_REPLAY_CONTROL_UNDERPOWERED"
    assert gate["passed"] is False


def test_complete_gate_is_tier_e_only() -> None:
    card = replay.load_replay_card()
    candidate = _candidate(
        validation_net=100.0,
        final_net=200.0,
        normal_passes=2,
        stressed_passes=1,
        complete_gate=True,
    )
    gate = replay.power_replay_gate(
        candidate,
        power=_power(True),
        control_power=_control_power(True),
        card=card,
    )
    assert gate["status"] == "MGC_RATES_TARGET_VOL_GREEN_TIER_E_DIAGNOSTIC"
    assert gate["passed"] is True
    assert gate["promotion_status"] is None
    assert gate["xfa_status"] is None


def test_partial_economics_are_weak_not_green() -> None:
    card = replay.load_replay_card()
    candidate = _candidate(
        validation_net=100.0,
        final_net=-1.0,
        normal_passes=0,
        stressed_passes=0,
        complete_gate=False,
    )
    gate = replay.power_replay_gate(
        candidate,
        power=_power(True),
        control_power=_control_power(True),
        card=card,
    )
    assert gate["status"] == "MGC_RATES_TARGET_VOL_WEAK_DIAGNOSTIC"
    assert gate["passed"] is False


def test_role_local_control_power_requires_exact_primary_cardinality() -> None:
    card = replay.load_replay_card()
    validation_primary = {"event_id": "p-v", "session_day": 20191001}
    final_primary = {"event_id": "p-f", "session_day": 20200701}
    event_sets = {
        replay.PRIMARY: (validation_primary, final_primary),
        **{
            control: (
                _control_event(20191001, "p-v", 100 + index * 10),
                _control_event(20200701, "p-f", 200 + index * 10),
            )
            for index, control in enumerate(replay.CONTROLS)
        },
    }
    assert replay.control_power_audit(event_sets, card=card)["passed"] is True
    event_sets[replay.CONTROLS[0]] = (_control_event(20191001, "p-v", 100),)
    audit = replay.control_power_audit(event_sets, card=card)
    assert audit["passed"] is False
    assert (
        audit["roles"]["FINAL_DEVELOPMENT"]["exact_cardinality_checks"][
            replay.CONTROLS[0]
        ]
        is False
    )


def test_cross_role_control_match_is_rejected_even_if_cardinality_balances() -> None:
    card = replay.load_replay_card()
    primary = (
        {"event_id": "p-v", "session_day": 20200623},
        {"event_id": "p-f", "session_day": 20200624},
    )
    event_sets = {
        replay.PRIMARY: primary,
        **{
            control: (
                _control_event(20200623, "p-f", 100 + index * 10),
                _control_event(20200624, "p-v", 200 + index * 10),
            )
            for index, control in enumerate(replay.CONTROLS)
        },
    }
    assert replay.control_power_audit(event_sets, card=card)["passed"] is False


def test_exact_account_replay_requires_both_cheap_gates() -> None:
    assert replay.should_run_exact_account_replay(_power(True), _control_power(True))
    assert not replay.should_run_exact_account_replay(_power(False), _control_power(True))
    assert not replay.should_run_exact_account_replay(_power(True), _control_power(False))


def test_trigger_power_gate_prevents_outcome_materialization(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def forbidden(*args: object, **kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("outcome materializer must remain unreachable")

    monkeypatch.setattr(replay, "materialize_frozen_candidate_event_sets", forbidden)
    result = replay.materialize_after_trigger_power(
        {},
        pd.DataFrame(),
        {},
        power={"passed": False},
        card=replay.load_replay_card(),
    )
    assert result is None
    assert called is False


def test_runtime_orders_trigger_gate_before_materialization_and_exact_account() -> None:
    source = inspect.getsource(replay.run_power_replay)
    assert source.index("trigger_power_preflight") < source.index(
        "materialize_after_trigger_power"
    )
    assert source.index("materialize_after_trigger_power") < source.index(
        "shared._power_preflight"
    )
    assert source.index("shared._power_preflight") < source.index(
        "shared.evaluate_candidate"
    )


def test_matching_never_crosses_discovery_validation_boundary() -> None:
    card = replay.load_replay_card()
    primary = _raw_match_event(
        day=20190911, event_id="primary-validation", decision=1_000
    )
    discovery = _raw_match_event(
        day=20190909, event_id="control-discovery", decision=900
    )
    validation = _raw_match_event(
        day=20190912, event_id="control-validation", decision=1_100
    )
    matched = replay._match_control_events_role_local(
        [primary], [discovery, validation], used=set(), card=card
    )
    assert len(matched) == 1
    assert matched[0]["event_id"] == "control-validation"
    assert matched[0]["matched_evidence_role"] == "VALIDATION"


def test_event_fingerprint_clone_controls_fail_closed() -> None:
    card = replay.load_replay_card()
    primary = {"event_id": "p-v", "session_day": 20191001}
    clone = _control_event(20191001, "p-v", 100)
    event_sets = {
        replay.PRIMARY: (primary,),
        **{control: (dict(clone),) for control in replay.CONTROLS},
    }
    audit = replay.control_power_audit(event_sets, card=card)
    assert audit["passed"] is False
    assert audit["roles"]["VALIDATION"]["clone_pairs"]


def test_role_hashed_permutation_is_reproducible_and_not_a_shift_clone() -> None:
    card = replay.load_replay_card()
    timestamps = pd.date_range("2016-01-04T14:00:00Z", periods=17, freq="1min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "session_day": 20160104,
            "rates_vol_score": [float(value) for value in range(17)],
        }
    )
    first, audit_first = replay._attach_role_hashed_magnitude_permutation(
        frame, card=card
    )
    second, audit_second = replay._attach_role_hashed_magnitude_permutation(
        frame, card=card
    )
    assert audit_first == audit_second
    assert first["rates_vol_score_role_permutation"].tolist() == second[
        "rates_vol_score_role_permutation"
    ].tolist()
    assert sorted(first["rates_vol_score_role_permutation"].tolist()) == sorted(
        frame["rates_vol_score"].tolist()
    )
    assert first["rates_vol_score_role_permutation"].tolist() != frame[
        "rates_vol_score"
    ].tolist()
    assert audit_first["roles"]["DISCOVERY"]["affine_step"] != 1


def test_output_is_restricted_to_exact_governed_folder(tmp_path: Path) -> None:
    with pytest.raises(replay.MGCPowerReplayError, match="frozen diagnostic artifact root"):
        replay.persist_replay_artifacts(
            tmp_path,
            {},
            output_root="mission/state",
        )


def test_mgc_mapping_uses_explicit_instrument_and_date() -> None:
    roll = RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type=replay.acquisition.MGC_MAP_TYPE,
        symbols=["MGC"],
        contracts=[
            _contract("MGC", "MGCZ0", "10", "2020-01-01", "2020-02-01"),
            _contract("MGC", "MGCG1", "11", "2020-02-01", "2020-03-01"),
        ],
        unsafe_window_days=1,
        notes=[],
    )
    timestamp = pd.to_datetime(
        ["2020-01-02T14:00:00Z", "2020-02-03T14:00:00Z"], utc=True
    )
    frame = pd.DataFrame(
        {
            "timestamp": timestamp,
            "instrument_id": [10, 11],
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [1, 1],
        }
    )
    mapped = replay._map_mgc_rows(frame, roll)
    assert mapped["active_contract"].tolist() == ["MGCZ0", "MGCG1"]
    assert mapped["symbol"].tolist() == ["MGC", "MGC"]


def test_roll_payload_must_reconcile_its_native_hash() -> None:
    roll = RollMap(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        map_type=replay.acquisition.MGC_MAP_TYPE,
        symbols=["MGC"],
        contracts=[
            _contract("MGC", "MGCZ0", "10", "2020-01-01", "2020-02-01")
        ],
        unsafe_window_days=1,
        notes=[],
    )
    payload = roll.to_dict()
    assert replay._roll_map_from_payload(payload) == roll
    payload["contracts"][0]["point_value"] = 999.0
    with pytest.raises(replay.MGCPowerReplayError, match="roll-map semantic drift"):
        replay._roll_map_from_payload(payload)


def test_treasury_mapping_keeps_only_same_delivery_interval() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2020-01-02T14:00:00Z", "2020-01-02T14:00:00Z"], utc=True
            ),
            "instrument_id": [20, 30],
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [1, 1],
        }
    )
    sync = {
        "root_rolls": {
            "ZN": {"contracts": [{"instrument_id": "20"}]},
            "TN": {"contracts": [{"instrument_id": "30"}]},
        },
        "delivery_sync_intervals": [
            {
                "start": "2020-01-01",
                "end": "2020-02-01",
                "zn_instrument_id": "20",
                "zn_contract": "ZNH0",
                "tn_instrument_id": "30",
                "tn_contract": "TNH0",
            }
        ]
    }
    zn = replay._map_treasury_rows(frame, sync, "ZN")
    tn = replay._map_treasury_rows(frame, sync, "TN")
    assert zn["contract"].tolist() == ["ZNH0"]
    assert tn["contract"].tolist() == ["TNH0"]
