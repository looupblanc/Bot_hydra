from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydra.data.v7_d1_trades import (
    EXPECTED_BILLABLE_SIZE_BYTES,
    EXPECTED_RECORD_COUNT,
    D1TradesError,
    OfficialEstimate,
    authorize_estimate,
    request_kwargs,
    verify_frozen_plan,
)


def _estimate(cost: float = 40.401062786579) -> OfficialEstimate:
    return OfficialEstimate(
        record_count=EXPECTED_RECORD_COUNT,
        estimated_cost_usd=cost,
        billable_size_bytes=EXPECTED_BILLABLE_SIZE_BYTES,
    )


def test_frozen_d1_plan_and_request_are_exact() -> None:
    plan = verify_frozen_plan(".")
    request = request_kwargs()

    assert plan["request"]["data_role"] == "DEVELOPMENT_ONLY"
    assert request["schema"] == "trades"
    assert request["symbols"] == ["ES.c.0", "MES.c.0"]
    assert request["end"] == "2024-10-01T00:00:00Z"
    assert "DATABENTO_API_KEY" not in json.dumps(plan)


def test_authorization_preserves_thirty_dollar_reserve(tmp_path: Path) -> None:
    ledger = tmp_path / "spend.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "request_id": "prior",
                "download_status": "DOWNLOADED",
                "estimated_cost_usd": 0.0,
                "actual_cost_usd": 27.986516065895,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    decision = authorize_estimate(
        _estimate(),
        ledger_path=ledger,
        raw_output_path=tmp_path / "missing.dbn.zst",
    )

    assert decision.allowed
    assert decision.reason == "AUTHORIZED"
    assert decision.projected_remaining_usd > 30.0


def test_authorization_blocks_cost_or_metadata_drift(tmp_path: Path) -> None:
    ledger = tmp_path / "empty.jsonl"
    ledger.write_text("", encoding="utf-8")

    too_expensive = authorize_estimate(
        _estimate(40.42),
        ledger_path=ledger,
        raw_output_path=tmp_path / "missing.dbn.zst",
    )
    changed_count = authorize_estimate(
        OfficialEstimate(
            record_count=EXPECTED_RECORD_COUNT + 1,
            estimated_cost_usd=40.0,
            billable_size_bytes=EXPECTED_BILLABLE_SIZE_BYTES,
        ),
        ledger_path=ledger,
        raw_output_path=tmp_path / "missing.dbn.zst",
    )

    assert not too_expensive.allowed
    assert too_expensive.reason == "REQUEST_COST_EXCEEDS_WORM_LIMIT"
    assert not changed_count.allowed
    assert changed_count.reason == "HISTORICAL_RECORD_COUNT_DRIFT"


def test_cache_and_ledger_must_agree(tmp_path: Path) -> None:
    ledger = tmp_path / "empty.jsonl"
    ledger.write_text("", encoding="utf-8")
    raw = tmp_path / "unledgered.dbn.zst"
    raw.write_bytes(b"data")

    with pytest.raises(D1TradesError, match="disagree"):
        authorize_estimate(
            _estimate(), ledger_path=ledger, raw_output_path=raw
        )
