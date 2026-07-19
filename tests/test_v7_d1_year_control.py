from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.data.budget import (
    DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD,
    DATABENTO_AUTOMATIC_SAFETY_CEILING_USD,
    DatabentoBudgetConfig,
)
from hydra.data.v7_d1_year_control import (
    D1YearControlError,
    EXPECTED_BILLABLE_SIZE_BYTES,
    EXPECTED_COST_USD,
    EXPECTED_RECORD_COUNT,
    OfficialEstimate,
    authorize_estimate,
    verify_frozen_plan,
)


def test_constitution_data_budget_is_the_default() -> None:
    budget = DatabentoBudgetConfig()

    assert budget.hard_cap_usd == DATABENTO_AUTHORIZED_CUMULATIVE_CAP_USD
    assert budget.safety_ceiling_usd == DATABENTO_AUTOMATIC_SAFETY_CEILING_USD


def test_year_control_plan_is_frozen_development_only() -> None:
    plan = verify_frozen_plan(".")

    assert plan["request"]["data_role"] == "DEVELOPMENT_ONLY"
    assert not plan["request"]["q4_included"]
    assert not plan["request"]["forward_gap_included"]
    assert plan["authorization"]["projected_d1_spend_usd"] < 60.0


def test_exact_estimate_preserves_phase_cap_and_reserve(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "candidate_tier": "V7_PREREGISTERED_HYPOTHESIS_CLASS_PILOT",
                "actual_cost_usd": 40.401062786579,
                "estimated_cost_usd": 0.0,
                "download_status": "DOWNLOADED",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    authorization = authorize_estimate(
        _estimate(),
        ledger_path=ledger,
        raw_output_path=tmp_path / "absent.dbn.zst",
    )

    assert authorization.allowed
    assert authorization.reason == "AUTHORIZED"
    assert authorization.projected_d1_spend_usd < 60.0
    assert authorization.projected_remaining_usd > 30.0


def test_phase_cap_cannot_be_weakened(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "candidate_tier": "V7_PREREGISTERED_HYPOTHESIS_CLASS_PILOT",
                "actual_cost_usd": 41.0,
                "estimated_cost_usd": 0.0,
                "download_status": "DOWNLOADED",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    authorization = authorize_estimate(
        _estimate(),
        ledger_path=ledger,
        raw_output_path=tmp_path / "absent.dbn.zst",
    )

    assert not authorization.allowed
    assert authorization.reason == "D1_PHASE_CAP_EXCEEDED"


def test_cost_drift_fails_closed(tmp_path: Path) -> None:
    estimate = OfficialEstimate(
        record_count=EXPECTED_RECORD_COUNT,
        estimated_cost_usd=EXPECTED_COST_USD + 0.01,
        billable_size_bytes=EXPECTED_BILLABLE_SIZE_BYTES,
    )

    authorization = authorize_estimate(
        estimate,
        ledger_path=tmp_path / "empty.jsonl",
        raw_output_path=tmp_path / "absent.dbn.zst",
    )

    assert not authorization.allowed
    assert authorization.reason == "OFFICIAL_COST_DRIFT"


def test_orphan_cache_fails_closed(tmp_path: Path) -> None:
    raw = tmp_path / "orphan.dbn.zst"
    raw.write_bytes(b"orphan")

    with pytest.raises(D1YearControlError, match="disagree"):
        authorize_estimate(
            _estimate(),
            ledger_path=tmp_path / "empty.jsonl",
            raw_output_path=raw,
        )


def _estimate() -> OfficialEstimate:
    return OfficialEstimate(
        record_count=EXPECTED_RECORD_COUNT,
        estimated_cost_usd=EXPECTED_COST_USD,
        billable_size_bytes=EXPECTED_BILLABLE_SIZE_BYTES,
    )
