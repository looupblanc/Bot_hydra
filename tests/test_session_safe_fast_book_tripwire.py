from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hydra.economic_evolution.schema import stable_hash
from hydra.production import session_safe_fast_book_tripwire as tripwire


ROOT = Path(__file__).resolve().parents[1]
DECISION_CARD = (
    ROOT / "config/research/post_treasury_next_branch_decision_card_v1.json"
)


def _ns(value: str) -> int:
    return int(datetime.fromisoformat(value).astimezone(UTC).timestamp() * 1e9)


def test_decision_card_selects_two_shard_repair_without_status_inheritance() -> None:
    card = json.loads(DECISION_CARD.read_text(encoding="utf-8"))
    assert card["selected_branch"] == tripwire.BRANCH_ID
    experiment = card["smallest_decisive_falsification_experiment"]
    assert tuple(experiment["repair_variants"]) == tripwire.REPAIR_VARIANTS
    assert experiment["maximum_cpu_shards"] == 2
    assert experiment["promotion_allowed"] is False
    assert experiment["q4_access_allowed"] is False
    assert experiment["data_purchase_allowed"] is False
    assert card["alternative_audit"]["materially_distinct"] is True
    assert card["branch_rule"]["status_inheritance_allowed"] is False


def test_causal_session_cutoff_uses_declared_horizon_not_realized_exit() -> None:
    # 14:40 CT + 30 minutes is exactly the official 15:10 flatten.
    assert tripwire.causal_horizon_safe_entry(
        _ns("2024-02-14T14:40:00-06:00"), 30
    )
    assert not tripwire.causal_horizon_safe_entry(
        _ns("2024-02-14T14:41:00-06:00"), 30
    )
    # Overnight is mapped to the following trading-day flatten, including DST.
    assert tripwire.causal_horizon_safe_entry(
        _ns("2024-03-11T17:00:00-05:00"), 120
    )
    assert not tripwire.causal_horizon_safe_entry(
        _ns("2024-03-12T15:00:00-05:00"), 30
    )
    with pytest.raises(tripwire.SessionSafeFastBookError):
        tripwire.causal_horizon_safe_entry(0, 30)


@pytest.fixture(scope="module", params=tripwire.REPAIR_VARIANTS)
def real_result(request: pytest.FixtureRequest) -> dict:
    if not (ROOT / tripwire.source.DEFAULT_BANK_PATH).is_file():
        pytest.skip("immutable 0029 bank is not installed")
    first = tripwire.run_session_safe_fast_book_tripwire(
        ROOT, repair_variant=str(request.param)
    )
    second = tripwire.run_session_safe_fast_book_tripwire(
        ROOT, repair_variant=str(request.param)
    )
    assert first == second
    return first


def test_real_repair_is_exact_clean_read_only_and_nonpromotional(
    real_result: dict,
) -> None:
    assert real_result["schema"] == tripwire.SCHEMA
    core = dict(real_result)
    claimed = core.pop("result_hash")
    assert stable_hash(core) == claimed
    assert real_result["status_inherited"] is False
    assert real_result["promotion_status"] is None
    assert real_result["evidence_tier"] == "E"
    assert sum(
        real_result["original_session_violation_count_by_component"].values()
    ) == 8
    assert sum(
        real_result["repaired_session_violation_count_by_component"].values()
    ) == 0
    assert real_result["counters"]["exact_account_replays"] == 318
    assert real_result["counters"]["maximum_parallel_cpu_shards"] == 2
    assert real_result["counters"]["data_purchase_count"] == 0
    assert real_result["counters"]["q4_access_count_delta"] == 0
    assert real_result["counters"]["broker_connections"] == 0
    assert real_result["counters"]["orders"] == 0
    assert real_result["counters"]["authoritative_writes"] == 0
    assert real_result["counters"]["promotion_count"] == 0
    assert all(
        account["hard_execution_contract_clean"] is True
        and account["promotion_status"] is None
        for account in real_result["account_results"].values()
    )
    adapter = real_result["evidence_bundle_adapter"]
    assert len(adapter["evaluated_policy_records"]) == 318
    assert adapter["sealing_performed"] is False
    assert adapter["authoritative_writer_required_for_sealing"] is True

    # The repair preserves the only observed exact signal, but the denominator
    # is deliberately too small for promotion: it remains a Tier-E tripwire.
    selected = real_result["account_results"]["50K"]["horizon_results"]["20"]
    assert selected["full_coverage_start_count"] == 4
    assert selected["normal"]["pass_count"] == 2
    assert selected["stressed"]["pass_count"] == 2
    assert selected["normal"]["target_progress_median"] == pytest.approx(
        0.9546162873194699
    )
    assert selected["stressed"]["target_progress_median"] == pytest.approx(
        0.9441701119185189
    )
    assert selected["stressed"]["mll_breach_count"] == 0
    assert real_result["repair_signal_gate"]["passed"] is True
    assert real_result["repair_signal_gate"]["passing_blocks"] == ["B3", "B4"]
    assert real_result["decision"] == (
        "SESSION_SAFE_REPAIR_SIGNAL_REQUIRES_FROZEN_VALIDATION"
    )


def test_variants_are_structurally_distinct_and_frozen(real_result: dict) -> None:
    source_components = set(real_result["source_component_ids"])
    active = set(real_result["active_component_ids"])
    rejected = real_result["causal_pre_entry_rejection_count_by_component"]
    if real_result["repair_variant"] == "HORIZON_SAFE_ENTRY_CUTOFF":
        assert active == source_components
        assert sum(rejected.values()) == 17
    else:
        assert len(active) == len(source_components) - 1
        assert sum(rejected.values()) == 82
    assert real_result["repair_contract"]["outcome_fields_used"] is False
    assert real_result["repair_contract"]["future_label_eligibility_used"] is False
