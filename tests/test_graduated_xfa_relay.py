from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production.graduated_xfa_relay import (
    GraduatedXfaRelayError,
    assert_graduated_xfa_relay_ready,
    prepare_graduated_xfa_relay,
)


RULES = Path("config/rulesets/topstep_official_2026-07-19.json")


def _graduation(
    *,
    account_label: str = "150K",
    account_size: int = 150_000,
    markets: list[str] | None = None,
) -> dict[str, object]:
    core: dict[str, object] = {
        "candidate_id": "tier_g_book_0001",
        "evidence_tier": "G",
        "graduation_status": "GRADUATED_DEVELOPMENT_BOOK",
        "tier_g_gate_cleared": True,
        "frozen_before_xfa": True,
        "promotion_status": "TIER_G",
        "account_label": account_label,
        "account_size_usd": account_size,
        "markets": list(markets or ["NQ"]),
        "combine_book_hash": "1" * 64,
        "xfa_book_hash": "2" * 64,
        "xfa_profile_hash": "3" * 64,
        "graduation_evidence_hash": "4" * 64,
    }
    return {**core, "result_hash": stable_hash(core)}


def _combine_path(
    graduation: dict[str, object],
    *,
    start_id: str = "combine_start_0001",
    status: str = "TARGET_REACHED",
    passed: bool = True,
    immutable: bool = True,
) -> dict[str, object]:
    core: dict[str, object] = {
        "candidate_id": graduation["candidate_id"],
        "combine_start_id": start_id,
        "combine_status": status,
        "passed": passed,
        "immutable": immutable,
        "combine_book_hash": graduation["combine_book_hash"],
        "account_label": graduation["account_label"],
        "account_size_usd": graduation["account_size_usd"],
        "source_ledger_hash": "5" * 64,
        "combine_evidence_hash": "6" * 64,
    }
    return {**core, "path_hash": stable_hash(core)}


def _rehash(payload: dict[str, object], hash_field: str) -> dict[str, object]:
    core = dict(payload)
    core.pop(hash_field, None)
    return {**core, hash_field: stable_hash(core)}


def _rules_with_well_formed_source_hashes(tmp_path: Path) -> Path:
    """Exercise executable binding without mutating the authoritative snapshot.

    Two source hashes in the repository snapshot contain 63 hexadecimal
    characters.  The production adapter must fail closed on that file.  This
    isolated fixture pads those fields solely to test the later binding path;
    it does not assert that the padded values are authentic document hashes.
    """

    payload = json.loads(RULES.read_text(encoding="utf-8"))
    for row in payload["sources"]:
        if len(row["document_sha256"]) == 63:
            row["document_sha256"] = f"0{row['document_sha256']}"
    target = tmp_path / "well_formed_provenance_rules.json"
    target.write_text(json.dumps(payload), encoding="utf-8")
    return target


def test_non_tier_g_candidate_is_rejected_before_any_xfa_plan() -> None:
    graduation = _graduation()
    graduation["evidence_tier"] = "E"
    graduation = _rehash(graduation, "result_hash")

    with pytest.raises(GraduatedXfaRelayError, match="only a frozen Tier-G"):
        prepare_graduated_xfa_relay(
            graduation,
            [_combine_path(graduation)],
            rule_snapshot_path=RULES,
        )


@pytest.mark.parametrize(
    ("account_label", "account_size"),
    (("50K", 50_000), ("100K", 100_000)),
)
def test_unverified_account_size_rules_fail_closed_without_simulation(
    account_label: str, account_size: int
) -> None:
    graduation = _graduation(
        account_label=account_label,
        account_size=account_size,
    )
    plan = prepare_graduated_xfa_relay(
        graduation,
        [_combine_path(graduation)],
        rule_snapshot_path=RULES,
    )

    assert plan["status"] == "XFA_RULES_UNVERIFIED_FAIL_CLOSED"
    assert plan["blocked"] is True
    assert plan["blocked_reason_code"] == (
        f"NO_VERSIONED_EXECUTABLE_{account_label}_XFA_RULE_SNAPSHOT"
    )
    assert plan["simulation_started"] is False
    assert plan["xfa_paths_started"] == 0
    assert plan["alternative_runs"] == {"STANDARD": [], "CONSISTENCY": []}
    assert plan["registry_writes"] == plan["database_writes"] == 0
    with pytest.raises(GraduatedXfaRelayError, match=account_label):
        assert_graduated_xfa_relay_ready(plan)


def test_verified_150k_plan_keeps_alternatives_separate_and_does_not_run(
    tmp_path: Path,
) -> None:
    graduation = _graduation()
    rules = _rules_with_well_formed_source_hashes(tmp_path)
    paths = [
        _combine_path(graduation, start_id="combine_start_0002"),
        _combine_path(graduation, start_id="combine_start_0001"),
    ]

    plan = prepare_graduated_xfa_relay(
        graduation,
        paths,
        rule_snapshot_path=rules,
    )

    assert plan == prepare_graduated_xfa_relay(
        graduation,
        list(reversed(paths)),
        rule_snapshot_path=rules,
    )
    assert plan["status"] == "READY_FOR_EXPLICIT_LATER_XFA_SIMULATION"
    assert plan["blocked"] is False
    assert plan["simulation_started"] is False
    assert plan["xfa_paths_started"] == 0
    assert plan["standard_and_consistency_are_alternatives"] is True
    assert plan["sum_standard_and_consistency_ev_allowed"] is False
    assert "combined_ev" not in plan
    assert plan["alternative_run_counts"] == {
        "STANDARD": 2,
        "CONSISTENCY": 2,
    }
    standard = plan["alternative_runs"]["STANDARD"]
    consistency = plan["alternative_runs"]["CONSISTENCY"]
    assert {row["xfa_path"] for row in standard} == {"STANDARD"}
    assert {row["xfa_path"] for row in consistency} == {"CONSISTENCY"}
    assert {row["combine_transition_hash"] for row in standard} == {
        row["combine_transition_hash"] for row in consistency
    }
    assert all(row["account_size_usd"] == 150_000 for row in standard)
    assert all(row["simulation_started"] is False for row in standard + consistency)
    assert plan["registry_writes"] == plan["database_writes"] == 0
    assert_graduated_xfa_relay_ready(plan)


@pytest.mark.parametrize(
    ("status", "passed", "immutable"),
    (
        ("MLL_BREACHED", False, True),
        ("TARGET_REACHED", False, True),
        ("TARGET_REACHED", True, False),
    ),
)
def test_only_successful_immutable_combine_paths_are_accepted(
    status: str, passed: bool, immutable: bool
) -> None:
    graduation = _graduation()
    path = _combine_path(
        graduation,
        status=status,
        passed=passed,
        immutable=immutable,
    )

    with pytest.raises(GraduatedXfaRelayError, match="non-passing, mutable"):
        prepare_graduated_xfa_relay(
            graduation,
            [path],
            rule_snapshot_path=RULES,
        )


def test_combine_path_hash_drift_is_rejected() -> None:
    graduation = _graduation()
    path = _combine_path(graduation)
    path["combine_start_id"] = "tampered_after_freeze"

    with pytest.raises(GraduatedXfaRelayError, match="identity/hash drift"):
        prepare_graduated_xfa_relay(
            graduation,
            [path],
            rule_snapshot_path=RULES,
        )


@pytest.mark.parametrize("market", ("CL", "CLZ6", "MCLZ6", "GC", "MGCZ6"))
def test_unverified_restricted_market_scaling_is_rejected_for_150k(
    market: str, tmp_path: Path,
) -> None:
    graduation = _graduation(markets=[market])
    rules = _rules_with_well_formed_source_hashes(tmp_path)

    with pytest.raises(
        GraduatedXfaRelayError,
        match="restricted-market XFA balance-tier scaling",
    ):
        prepare_graduated_xfa_relay(
            graduation,
            [_combine_path(graduation)],
            rule_snapshot_path=rules,
        )


def test_authoritative_snapshot_with_malformed_source_hashes_fails_closed() -> None:
    graduation = _graduation()
    plan = prepare_graduated_xfa_relay(
        graduation,
        [_combine_path(graduation)],
        rule_snapshot_path=RULES,
    )

    assert plan["status"] == "XFA_RULES_UNVERIFIED_FAIL_CLOSED"
    assert (
        plan["blocked_reason_code"]
        == "OFFICIAL_RULE_SNAPSHOT_SOURCE_PROVENANCE_INVALID"
    )
    assert plan["rule_snapshot"]["source_provenance_status"] == (
        "INVALID_FAIL_CLOSED"
    )
    assert plan["rule_snapshot"]["source_provenance_issues"] == [
        "OFFICIAL_SOURCE_5_DOCUMENT_SHA256_INVALID",
        "OFFICIAL_SOURCE_8_DOCUMENT_SHA256_INVALID",
    ]
    assert plan["alternative_runs"] == {"STANDARD": [], "CONSISTENCY": []}
    assert plan["simulation_started"] is False


def test_tampered_official_parsed_rules_fail_closed(tmp_path: Path) -> None:
    payload = json.loads(RULES.read_text(encoding="utf-8"))
    payload["xfa"]["standard"]["winning_days_required"] = 4
    tampered = tmp_path / "tampered_rules.json"
    tampered.write_text(json.dumps(payload), encoding="utf-8")
    graduation = _graduation()

    with pytest.raises(GraduatedXfaRelayError, match="identity/status drift"):
        prepare_graduated_xfa_relay(
            graduation,
            [_combine_path(graduation)],
            rule_snapshot_path=tampered,
        )


def test_dispatch_guard_rejects_tampered_alternative_run(tmp_path: Path) -> None:
    graduation = _graduation()
    rules = _rules_with_well_formed_source_hashes(tmp_path)
    plan = prepare_graduated_xfa_relay(
        graduation,
        [_combine_path(graduation)],
        rule_snapshot_path=rules,
    )
    run = plan["alternative_runs"]["STANDARD"][0]
    run["xfa_path"] = "CONSISTENCY"
    plan = _rehash(plan, "result_hash")

    with pytest.raises(GraduatedXfaRelayError, match="STANDARD run identity drift"):
        assert_graduated_xfa_relay_ready(plan)
